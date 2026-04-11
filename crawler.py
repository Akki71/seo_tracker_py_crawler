"""
crawler.py — Core SEO crawl + analyze logic.
Called by run_audit() from the API background task.

Merged from crawler_1.py (old monolith) into this API-compatible version.
Key additions vs original ZIP:
  - body_copy_guidance AI call (per page, first 50 pages)
  - ai_alt_recommendations for missing-ALT images
  - ai_new_page_suggestions  (new page gaps)
  - Full spam/malware detection on each page
  - SSL expiry, WWW resolve, sitemap size checks
  - Full _build_site_analysis (crawl depth, hreflang, sitemap comparison)
  - Full _generate_seo_files (robots, sitemap, llms.txt, .htaccess, nginx, broken-link report)
  - db_insert_new_page_suggestions DB save
  - Scorecard google_search_console check fixed
  - _analyze_pages includes body_copy_guidance
  - ai_mode logged correctly in run_audit
"""

import os, re, json, time, hashlib, logging, sys, base64, io
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, urldefrag
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)

CRAWL_TIMEOUT       = 25    # Reduced to prevent SSL/connection drops
BROKEN_LINK_TIMEOUT = 8
MAX_WORKERS         = 8     # Reduced concurrency to prevent rate-limiting

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SKIP_EXTENSIONS = {
    '.png','.jpg','.jpeg','.gif','.webp','.svg','.ico','.bmp','.tiff','.avif',
    '.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.csv','.zip','.rar',
    '.gz','.tar','.mp3','.mp4','.avi','.mov','.wmv','.flv','.ogg','.wav',
    '.webm','.mkv','.woff','.woff2','.ttf','.eot','.otf','.css','.js',
    '.json','.xml','.map','.exe','.dmg','.apk','.swf',
}

SKIP_PATTERNS = [
    '/wp-content/uploads/','/wp-includes/','/wp-json/',
    '/assets/','/static/','/media/','/images/','/img/',
    '/fonts/','/dist/','/build/',
    '/wp-admin/','/wp-login','/admin/','/administrator/',
    '/login','/signin','/signup','/register',
    '/dashboard/','/cpanel/','/phpmyadmin/',
    '/wp-cron','/xmlrpc.php','/feed/','/rss/',
]

PAGESPEED_API_KEY = os.environ.get("PAGESPEED_API_KEY", "")

# ── Lazy session ──────────────────────────────────────────────────────────────

_session = None

def _get_session():
    global _session
    if _session is None:
        import requests as _req
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        s = _req.Session()
        retries = Retry(total=2, backoff_factor=1,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=["HEAD", "GET"])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30)
        s.mount("https://", adapter)
        s.mount("http://",  adapter)
        s.headers.update(HEADERS)
        _session = s
    return _session


def _safe_get(url, timeout=CRAWL_TIMEOUT, **kw):
    """GET with SSL fallback + one retry on connection reset."""
    import time as _time
    s = _get_session()
    for _attempt in range(2):
        try:
            return s.get(url, timeout=timeout, allow_redirects=True, **kw)
        except requests.exceptions.SSLError:
            return s.get(url, timeout=timeout, allow_redirects=True, verify=False, **kw)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as _ce:
            if _attempt == 0:
                logger.debug(f"Connection reset {url[:60]} — retry in 3s")
                _time.sleep(3)
                continue
            raise
        except Exception:
            raise
    raise requests.exceptions.ConnectionError(f"Failed after 2 attempts: {url}")


def _safe_head(url, timeout=BROKEN_LINK_TIMEOUT, **kw):
    s = _get_session()
    try:
        return s.head(url, timeout=timeout, allow_redirects=True, **kw)
    except Exception:
        return s.head(url, timeout=timeout, allow_redirects=True, verify=False, **kw)


# ── Utilities ─────────────────────────────────────────────────────────────────

def _clean(text):
    return re.sub(r'\s+', ' ', text).strip() if text else ""


def _normalize(url):
    url = urldefrag(url)[0]
    p = urlparse(url)
    path = p.path.rstrip("/") or "/"
    return f"{p.scheme}://{p.netloc}{path}"


def _canonical_check(url, canonical):
    if not canonical:
        return "Missing"
    try:
        if _normalize(url) != _normalize(canonical):
            return f"Mismatch -> {canonical}"
    except Exception:
        return "Invalid"
    return "Correct"


def _url_cleanup(url):
    p = urlparse(url)
    path = p.path
    path = re.sub(r'/index\.(html?|php|aspx?)', '/', path)
    path = re.sub(r'/page/\d+', '', path)
    path = re.sub(r'\?.*$', '', path)
    path = re.sub(r'/{2,}', '/', path)
    path = path.rstrip('/') or '/'
    return f"{p.scheme}://{p.netloc}{path}"


def _is_200(p):
    s = p.get("status", "") if isinstance(p, dict) else p
    return str(s).startswith("200") or s == 200


def _is_404(p):
    s = p.get("status", "") if isinstance(p, dict) else p
    return str(s) == "404" or s == 404


def _calculate_seo_score(pd):
    s = 0
    if _is_200(pd): s += 15
    if pd.get("canonical_status") == "Correct": s += 10
    if pd.get("thin_content") == "No": s += 10
    if pd.get("duplicate_status") == "Unique": s += 10
    title = pd.get("current_title", "") or ""
    if title: s += 5
    if 30 <= len(title) <= 60: s += 5
    meta = pd.get("current_meta_description", "") or ""
    if meta: s += 5
    if 70 <= len(meta) <= 160: s += 5
    if pd.get("current_h1"): s += 5
    if pd.get("og_tags") == "Present": s += 5
    if pd.get("schema_markup") == "Present": s += 5
    if pd.get("google_analytics") == "Yes": s += 5
    ic = pd.get("total_images", 0) or 0
    im = pd.get("images_missing_alt", 0) or 0
    if ic > 0 and im == 0: s += 5
    elif ic == 0: s += 5
    return min(s, 100)


def _seo_grade(score):
    if not isinstance(score, (int, float)): return "N/A"
    if score >= 90: return "A+"
    if score >= 80: return "A"
    if score >= 70: return "B"
    if score >= 60: return "C"
    if score >= 50: return "D"
    return "F"


def _serp_preview(url, title, desc):
    display = urlparse(url).netloc + urlparse(url).path
    return f"{(title or '(No Title)')[:60]}\n{display}\n{(desc or '(No Description)')[:160]}"


def _check_file(domain, path, sitemap_urls_out=None):
    """Validate robots.txt, sitemap.xml, and llms.txt with content checks."""
    for scheme in ["https", "http"]:
        try:
            r = _safe_get(f"{scheme}://{domain}/{path}", timeout=12)
            if r.status_code == 200:
                content = r.text.strip()
                if not content:
                    return "Present but Empty"
                if path == "sitemap.xml":
                    if "<urlset" not in content and "<sitemapindex" not in content:
                        return "Invalid (no <urlset>)"
                    import xml.etree.ElementTree as ET
                    try:
                        root = ET.fromstring(content)
                        locs = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                        if not locs:
                            locs = root.findall('.//loc')
                        if sitemap_urls_out is not None:
                            for loc in locs:
                                if loc.text:
                                    sitemap_urls_out.append(loc.text.strip())
                        return f"Valid ({len(locs)} URLs)"
                    except ET.ParseError:
                        return "Invalid (XML parse error)"
                elif path == "robots.txt":
                    if "user-agent" not in content.lower():
                        return "Invalid (no User-agent directive)"
                    has_sitemap = "sitemap:" in content.lower()
                    # Check for blocking
                    is_blocking_all = False
                    current_agent = ""
                    for line in content.lower().split('\n'):
                        line = line.strip()
                        if line.startswith("user-agent:"):
                            current_agent = line.split(":", 1)[1].strip()
                        elif line.startswith("disallow:") and current_agent:
                            disallow_path = line.split(":", 1)[1].strip()
                            if disallow_path == "/" and current_agent == "*":
                                is_blocking_all = True
                    if is_blocking_all:
                        status = "BLOCKING ALL BOTS (Disallow: /)"
                    else:
                        status = "Valid"
                    status += " + Sitemap ref" if has_sitemap else " (no Sitemap ref)"
                    return status
                elif path == "llms.txt":
                    has_heading = content.startswith('#') or '##' in content
                    has_links = 'http' in content.lower()
                    if has_heading and has_links:
                        return f"Present ({len(content)} chars)"
                    else:
                        return f"Present ({len(content)} chars, no headings)" if has_links else f"Present ({len(content)} chars)"
                return "Present"
            elif r.status_code == 403:
                return "Blocked (403)"
        except Exception:
            continue
    return "Not Found"


def _get_pagespeed(url, strategy):
    """Fetch PageSpeed Insights. Works without API key (free tier); key adds higher quota."""
    try:
        import requests as _req
        params = {"url": url, "strategy": strategy}
        if PAGESPEED_API_KEY:
            params["key"] = PAGESPEED_API_KEY
        r = _req.get(
            "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
            params=params,
            timeout=90,
        )
        data = r.json()
        if "error" in data:
            logger.warning(f"PageSpeed [{strategy}] API error for {url}: {data['error'].get('message','')}")
            return {"score": "N/A", "lcp": "N/A", "cls": "N/A", "fcp": "N/A"}
        lh = data.get("lighthouseResult", {})
        audits = lh.get("audits", {})
        perf = lh.get("categories", {}).get("performance", {}).get("score")
        score = int(perf * 100) if perf is not None else "N/A"
        return {
            "score": score,
            "lcp": audits.get("largest-contentful-paint", {}).get("displayValue", "N/A"),
            "cls": audits.get("cumulative-layout-shift", {}).get("displayValue", "N/A"),
            "fcp": audits.get("first-contentful-paint", {}).get("displayValue", "N/A"),
        }
    except Exception as e:
        logger.error(f"PageSpeed [{strategy}] {url}: {e}")
        return {"score": "Error", "lcp": "Error", "cls": "Error", "fcp": "Error"}


# ── Page Crawler ──────────────────────────────────────────────────────────────


def _analyze_page_schema(url: str, html_text: str) -> dict:
    """Deep schema markup analysis per page — detects existing and recommends missing schemas."""
    from bs4 import BeautifulSoup as _BS
    soup = _BS(html_text, "html.parser")
    found = []; snippets = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    found.append(item.get("@type", "Unknown"))
                    snippets.append(json.dumps(item, indent=2)[:2000])
        except Exception:
            pass
    for el in soup.find_all(attrs={"itemtype": True}):
        it = el.get("itemtype", "")
        if "schema.org" in it:
            nm = it.split("/")[-1]
            if nm not in found:
                found.append(f"Microdata:{nm}")
    # Rule-based schema recommendations
    recommended = ["WebPage"]
    path = url.lower()
    if path.count("/") <= 3:
        recommended.extend(["Organization", "WebSite"])
    if any(w in path for w in ["/about", "/team", "/company"]):
        recommended.extend(["Organization", "AboutPage"])
    if any(w in path for w in ["/contact", "/get-in-touch"]):
        recommended.extend(["ContactPage", "LocalBusiness"])
    if any(w in path for w in ["/service", "/product", "/solution"]):
        recommended.extend(["Service", "Product"])
    if any(w in path for w in ["/blog", "/article", "/news", "/post"]):
        recommended.extend(["Article", "BlogPosting", "BreadcrumbList"])
    if any(w in path for w in ["/faq", "/frequently"]):
        recommended.append("FAQPage")
    if any(w in path for w in ["/pricing", "/plans"]):
        recommended.extend(["PriceSpecification", "Offer"])
    if path.count("/") > 3 and "BreadcrumbList" not in recommended:
        recommended.append("BreadcrumbList")
    recommended = list(set(recommended))
    missing = [s for s in recommended if s not in found]
    status = "Missing" if not found else ("Partial" if missing else "Complete")
    return {
        "page_url":             url,
        "schema_types_found":   found,
        "schema_snippets":      snippets,
        "recommended_schemas":  recommended,
        "recommended_snippets": [],
        "schema_status":        status,
        "missing_schemas":      missing,
    }

def _crawl_page(url, base_url, domain, pages_list, images_list,
                pending_links, content_hash_map, pages_lock, images_lock, broken_lock):
    from bs4 import BeautifulSoup

    pd = OrderedDict()
    pd["url"] = url
    pd["url_cleaned"] = _url_cleanup(url)
    new_links = []

    try:
        r = _safe_get(url, timeout=CRAWL_TIMEOUT)
        status = r.status_code
        html_text = r.text
        response_headers = dict(r.headers)
    except Exception as e:
        err_str = str(e)
        if "Timeout" in err_str or "timed out" in err_str.lower():
            pd["status"] = "Timeout — server took too long to respond"
        elif "ConnectionError" in err_str or "Connection" in err_str:
            pd["status"] = "Connection Error - server refused/DNS failed/SSL issue"
        else:
            pd["status"] = f"Error: {err_str[:80]}"
        with pages_lock:
            pages_list.append(pd)
        return []

    pd["status"] = status

    if status == 404:
        pd["redirect_suggestion"] = base_url
        pd["redirect_type"] = "301"
        with pages_lock:
            pages_list.append(pd)
        return []

    if isinstance(status, int) and status >= 500:
        with pages_lock:
            pages_list.append(pd)
        return []

    if status != 200:
        if isinstance(status, int) and status in (301, 302, 307, 308):
            pd["redirect_target"] = r.url
        with pages_lock:
            pages_list.append(pd)
        return []

    # ── 200 OK: full parse ────────────────────────────────────────────────────
    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception:
        try:
            soup = BeautifulSoup(html_text, "html5lib")
        except Exception:
            pd["status"] = "Parse Error"
            with pages_lock:
                pages_list.append(pd)
            return []

    text_content = _clean(soup.get_text())
    word_count = len(text_content.split())
    page_source = html_text

    # Canonical
    can_tag = soup.find("link", rel="canonical")
    can_href = can_tag.get("href") if can_tag else None
    pd["canonical_status"] = _canonical_check(url, can_href)
    pd["canonical_url"]    = can_href or "Not Set"

    # Duplicate
    ch = hashlib.md5(text_content.encode("utf-8", errors="ignore")).hexdigest()
    if ch in content_hash_map:
        pd["duplicate_status"] = f"Duplicate of {content_hash_map[ch]}"
    else:
        content_hash_map[ch] = url
        pd["duplicate_status"] = "Unique"

    pd["word_count"]  = word_count
    pd["thin_content"] = "Yes" if word_count < 300 else "No"

    # Title / Meta / H1 / H2
    title = _clean(soup.title.string) if soup.title and soup.title.string else ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    meta_desc = _clean(meta_tag.get("content", "")) if meta_tag else ""
    h1_tag = soup.find("h1")
    h1 = _clean(h1_tag.text) if h1_tag else ""
    h2_list = [_clean(h.text) for h in soup.find_all("h2")[:5]]

    pd["current_title"]           = title
    pd["title_length"]            = len(title)
    pd["current_meta_description"]= meta_desc
    pd["meta_desc_length"]        = len(meta_desc)
    pd["current_h1"]              = h1
    pd["h2_tags"]                 = " | ".join(h2_list) if h2_list else "None"

    # GA
    ga_markers = ["gtag(", "G-", "UA-", "google-analytics.com", "googletagmanager.com", "analytics.js", "gtm.js"]
    pd["google_analytics"] = "Yes" if any(m in page_source for m in ga_markers) else "No"

    # GSC (homepage only)
    is_hp = (_normalize(url) == _normalize(base_url) or
             urlparse(url).path in ("", "/", "/index.html", "/index.php"))
    if is_hp:
        gsc_meta = soup.find("meta", attrs={"name": "google-site-verification"})
        pd["google_search_console"] = "Yes" if gsc_meta or "google-site-verification" in page_source else "No"
    else:
        pd["google_search_console"] = "Homepage Only"

    # OG
    og_t = soup.find("meta", property="og:title")
    og_d = soup.find("meta", property="og:description")
    og_i = soup.find("meta", property="og:image")
    pd["og_tags"]                 = "Present" if og_t else "Missing"
    pd["og_title_current"]        = _clean(og_t.get("content", "")) if og_t else "Missing"
    pd["og_description_current"]  = _clean(og_d.get("content", "")) if og_d else "Missing"
    pd["og_image_current"]        = og_i.get("content", "Missing") if og_i else "Missing"

    # Schema
    schema_types = []
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            sd = json.loads(sc.string or "")
            if isinstance(sd, dict): schema_types.append(sd.get("@type", "Unknown"))
            elif isinstance(sd, list):
                for item in sd:
                    if isinstance(item, dict): schema_types.append(item.get("@type", "Unknown"))
        except Exception:
            pass
    pd["schema_markup"]      = "Present" if schema_types else "Missing"
    pd["schema_types_found"] = ", ".join(schema_types) if schema_types else "None"
    # Per-page deep schema analysis (stored in schema_markup_analysis table)
    try:
        pd["_schema_analysis"] = _analyze_page_schema(url, html_text)
    except Exception:
        pd["_schema_analysis"] = None

    # Hreflang
    hl_links = soup.find_all("link", rel="alternate", hreflang=True)
    pd["hreflang_tags"] = " | ".join(
        f"{hl.get('hreflang','')}:{hl.get('href','')}" for hl in hl_links
    ) if hl_links else ""

    # Images
    img_total = 0
    img_missing = 0
    page_imgs = []
    for img in soup.find_all("img"):
        img_total += 1
        alt = (img.get("alt") or "").strip()
        raw_src = img.get("src") or img.get("data-src") or ""
        full_src = urljoin(url, raw_src.strip()) if raw_src else "(no src)"
        has_alt = bool(alt)
        if not has_alt:
            img_missing += 1
        page_imgs.append({
            "page": url, "src": full_src[:2000],
            "alt": alt, "alt_status": "Present" if has_alt else "Missing"
        })
    with images_lock:
        images_list.extend(page_imgs)
    pd["total_images"]       = img_total
    pd["images_missing_alt"] = img_missing
    pd["image_alt_status"]   = (
        "All Present" if img_missing == 0 and img_total > 0
        else f"{img_missing}/{img_total} Missing" if img_total > 0
        else "No Images"
    )

    # Technical checks
    pd["viewport_configured"] = "Yes" if soup.find("meta", attrs={"name": "viewport"}) else "No"
    pd["html_size_kb"]        = round(len(html_text) / 1024, 1)
    pd["html_size_issue"]     = "Yes" if pd["html_size_kb"] > 100 else "No"
    pd["is_secure"]           = "Yes" if url.startswith("https://") else "No"

    mixed = []
    if url.startswith("https://"):
        for tn, attr in [("script","src"),("link","href"),("img","src"),("iframe","src")]:
            for tag in soup.find_all(tn):
                res = tag.get(attr, "")
                if res.startswith("http://"):
                    mixed.append(f"{tn}:{res[:80]}")
    pd["mixed_content"]         = "Yes" if mixed else "No"
    pd["mixed_content_details"] = " | ".join(mixed[:10])

    unmin_js = [
        sc.get("src", "")[:100]
        for sc in soup.find_all("script")
        if sc.get("src", "") and ".min." not in sc.get("src", "")
        and not any(c in sc.get("src", "") for c in ["cdn.", "cdnjs.", "googleapis.", "gstatic."])
    ]
    pd["unminified_js"]         = "Yes" if unmin_js else "No"
    pd["unminified_js_details"] = " | ".join(unmin_js[:5])

    unmin_css = [
        lk.get("href", "")[:100]
        for lk in soup.find_all("link", rel="stylesheet")
        if lk.get("href", "") and ".min." not in lk.get("href", "")
        and not any(c in lk.get("href", "") for c in ["cdn.", "cdnjs.", "googleapis.", "gstatic."])
    ]
    pd["unminified_css"]         = "Yes" if unmin_css else "No"
    pd["unminified_css_details"] = " | ".join(unmin_css[:5])

    amp = soup.find("link", rel="amphtml")
    pd["amp_link"] = amp.get("href", "Present") if amp else "None"

    # OG Validation
    og_issues = []
    if not og_t: og_issues.append("Missing og:title")
    elif len(og_t.get("content", "")) > 90: og_issues.append(f"og:title too long ({len(og_t.get('content',''))} chars)")
    if not og_d: og_issues.append("Missing og:description")
    if not og_i: og_issues.append("Missing og:image")
    if not soup.find("meta", property="og:url"):  og_issues.append("Missing og:url")
    if not soup.find("meta", property="og:type"): og_issues.append("Missing og:type")
    pd["og_validation"] = " | ".join(og_issues) if og_issues else "Valid"

    x_robots = response_headers.get("X-Robots-Tag", response_headers.get("x-robots-tag", ""))
    pd["x_robots_noindex"]   = "Yes" if "noindex" in x_robots.lower() else "No"
    pd["page_cache_control"] = response_headers.get("Cache-Control",
                               response_headers.get("cache-control", "Not Set"))

    # Spam / Malware detection
    spam_flags = []
    text_lower = text_content.lower()
    page_source_lower = page_source.lower()
    spam_markers = [
        ("viagra", "Pharmaceutical spam"), ("cialis", "Pharmaceutical spam"),
        ("casino", "Casino/gambling spam"), ("poker online", "Gambling spam"),
        ("buy cheap", "Spam commercial content"), ("payday loan", "Financial spam"),
        ("cryptocurrency invest", "Crypto spam"), ("click here to win", "Phishing/spam"),
    ]
    for marker, label in spam_markers:
        idx = text_lower.find(marker)
        if idx >= 0:
            start = max(0, idx - 40)
            end = min(len(text_content), idx + len(marker) + 40)
            context = text_content[start:end].strip()
            spam_flags.append(f"{label} — found: ...{context}...")
    hidden_patterns = [
        ('display:none', 'display:none'), ('visibility:hidden', 'visibility:hidden'),
        ('font-size:0', 'font-size:0'), ('text-indent:-9999', 'text-indent off-screen'),
        ('position:absolute;left:-9999', 'off-screen positioning'),
    ]
    for hp, hp_label in hidden_patterns:
        if hp.replace(" ", "") in page_source_lower.replace(" ", ""):
            spam_flags.append(f"Hidden text/cloaking ({hp_label})")
    if "eval(atob(" in page_source_lower or "eval(base64_decode" in page_source_lower:
        spam_flags.append("Base64 encoded script execution - possible malware")
    pd["spam_malware_flags"] = " | ".join(spam_flags) if spam_flags else "Clean"

    # Store content for AI (trimmed for memory)
    pd["_content"] = text_content[:5000]

    # Discover internal links
    domain_variants = {domain}
    if domain.startswith("www."):
        domain_variants.add(domain[4:])
    else:
        domain_variants.add("www." + domain)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        try:
            full = _normalize(urljoin(url, href))
        except Exception:
            continue
        with broken_lock:
            pending_links.append((url, full))
        if urlparse(full).netloc in domain_variants:
            path_lower = urlparse(full).path.lower()
            is_asset = (
                any(path_lower.endswith(ext) for ext in SKIP_EXTENSIONS) or
                any(pat in path_lower for pat in SKIP_PATTERNS)
            )
            if not is_asset:
                new_links.append(full)

    with pages_lock:
        pages_list.append(pd)
    return new_links


# ── Main Orchestrator ─────────────────────────────────────────────────────────

def run_audit(input_url: str, brand_id: int, target_location: str = "",
              business_type: str = "", ai_mode: str = "1", crawl_limit: int = 100,
              run_pagespeed: bool = False) -> dict:
    """
    Entry point called by the API background task.
    Returns {"audit_id": int, "excel_file": str, "pdf_file": str}
    """
    logger.info(f"run_audit start: brand_id={brand_id} url={input_url} "
                f"ai_mode={ai_mode} limit={crawl_limit}")

    from ai_helpers import setup_ai_clients
    from db import (
        get_db_conn, release_db_conn,
        db_create_audit, db_update_audit_complete,
        db_insert_page, db_update_page_ai,
        db_insert_images_batch, db_insert_broken_links_batch,
        db_insert_keywords, db_insert_blog_topics, db_insert_backlinks,
        db_insert_plan, db_insert_internal_linking, db_insert_kw_url_map,
        db_insert_axo, db_insert_scorecard, db_insert_aeo_faq,
        db_insert_site_analysis, db_insert_generated_files,
        db_mark_url_progress,
        db_insert_new_page_suggestions, db_insert_keyword_planner,
        db_insert_schema_analysis, db_insert_llm_prompts, db_insert_depth_analysis,
    )
    from scorecard import build_scorecard

    setup_ai_clients(ai_mode)

    # Resolve URL
    if not input_url.startswith("http"):
        input_url = "https://" + input_url
    base_url = _resolve_base_url(input_url)
    parsed   = urlparse(base_url)
    domain   = parsed.netloc
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(f"Resolved URL: {base_url} domain: {domain}")

    # Create DB record
    conn = get_db_conn()
    try:
        audit_id = db_create_audit(conn, brand_id, {
            "domain": domain, "base_url": base_url,
            "target_location": target_location or "Global",
            "business_type": business_type or "",
            "ai_mode": ai_mode,
        })
    finally:
        release_db_conn(conn)
    logger.info(f"Created audit #{audit_id}")

    # Global file checks
    sitemap_urls_found = []
    robots_status  = _check_file(domain, "robots.txt")
    sitemap_status = _check_file(domain, "sitemap.xml", sitemap_urls_out=sitemap_urls_found)
    llm_status     = _check_file(domain, "llms.txt")
    gbp_status     = _check_gbp(base_url)
    logger.info(f"Files: robots={robots_status} sitemap={sitemap_status} "
                f"llm={llm_status} gbp={gbp_status}")

    # SSL + WWW checks (site-level, stored in audit final update)
    ssl_status        = _check_ssl(domain)
    www_resolve       = _check_www_resolve(base_url, domain)
    sitemap_size      = _check_sitemap_size(domain)

    # State
    visited          = set()
    pages_list       = []
    images_list      = []
    broken_links_list = []
    pending_links    = []
    content_hash_map = {}
    crawl_depth_map  = {}
    pages_lock       = Lock()
    images_lock      = Lock()
    broken_lock      = Lock()

    # ── BFS Crawl ─────────────────────────────────────────────────────────────
    logger.info(f"Starting crawl (limit={crawl_limit})...")
    queue = [(_normalize(base_url), 0)]
    crawl_depth_map[_normalize(base_url)] = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while queue and len(visited) < crawl_limit:
            batch = []
            batch_depths = {}
            while queue and len(batch) < MAX_WORKERS:
                u, depth = queue.pop(0)
                if u not in visited:
                    visited.add(u)
                    batch.append(u)
                    batch_depths[u] = depth
            if not batch:
                break

            futures = {
                executor.submit(
                    _crawl_page, u, base_url, domain,
                    pages_list, images_list, pending_links, content_hash_map,
                    pages_lock, images_lock, broken_lock
                ): u for u in batch
            }
            for f in as_completed(futures):
                crawled_url = futures[f]
                try:
                    new_links = f.result()
                    parent_depth = batch_depths.get(crawled_url, 0)
                    for u in new_links:
                        if u not in visited:
                            child_depth = parent_depth + 1
                            if u not in crawl_depth_map or crawl_depth_map[u] > child_depth:
                                crawl_depth_map[u] = child_depth
                            queue.append((u, child_depth))
                except Exception as e:
                    logger.error(f"Crawl error {crawled_url}: {e}")

            logger.info(f"  Crawled: {len(visited)} | Queue: {len(queue)}")

    # Assign depths
    for p in pages_list:
        p["crawl_depth"] = crawl_depth_map.get(
            _normalize(p.get("url", "")),
            crawl_depth_map.get(p.get("url", ""), -1)
        )

    logger.info(f"Crawl done: {len(pages_list)} pages, {len(images_list)} images")

    # ── Broken Link Check ──────────────────────────────────────────────────────
    unique_links = {}
    for source, target in pending_links:
        if target not in unique_links:
            unique_links[target] = source

    logger.info(f"Checking {len(unique_links)} unique links...")
    found_broken = []

    def _check_link(target, source):
        try:
            r = _safe_head(target, timeout=BROKEN_LINK_TIMEOUT)
            if r.status_code in (404, 410, 500, 502, 503):
                found_broken.append({
                    "source_page": source, "broken_url": target,
                    "status": r.status_code,
                    "redirect_suggestion": base_url, "redirect_type": "301",
                })
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=8) as executor:
        futs = [executor.submit(_check_link, t, s) for t, s in unique_links.items()]
        for f in as_completed(futs):
            try: f.result(timeout=15)
            except Exception: pass

    broken_links_list.extend(found_broken)
    logger.info(f"Broken links: {len(broken_links_list)}")

    # ── Location detection ─────────────────────────────────────────────────────
    detected_location = target_location or _detect_location(domain, pages_list, ai_mode)

    # ── Save crawled pages to DB ───────────────────────────────────────────────
    saved_pages = 0
    failed_pages = 0
    for p in pages_list:
        _conn = get_db_conn()
        try:
            db_insert_page(_conn, audit_id, p)
            db_mark_url_progress(_conn, audit_id, p.get("url", ""), "crawled",
                                 str(p.get("status", "")))
            saved_pages += 1
        except Exception as e:
            failed_pages += 1
            logger.error(f"DB page insert error ({p.get('url','')[:60]}): {e}")
            try: _conn.rollback()
            except Exception: pass
        finally:
            release_db_conn(_conn)

    logger.info(f"Pages saved to DB: {saved_pages} OK, {failed_pages} failed")

    if images_list:
        _conn = get_db_conn()
        try:
            db_insert_images_batch(_conn, audit_id, images_list)
        except Exception as e:
            logger.error(f"DB images batch error: {e}")
            try: _conn.rollback()
            except Exception: pass
        finally:
            release_db_conn(_conn)

    if broken_links_list:
        _conn = get_db_conn()
        try:
            db_insert_broken_links_batch(_conn, audit_id, broken_links_list)
        except Exception as e:
            logger.error(f"DB broken links batch error: {e}")
            try: _conn.rollback()
            except Exception: pass
        finally:
            release_db_conn(_conn)

    # ── AI + PageSpeed ─────────────────────────────────────────────────────────
    pages_200 = [p for p in pages_list if _is_200(p)]

    if ai_mode != "4":
        logger.info(f"Running AI analysis on {len(pages_200)} pages...")
        _analyze_pages(pages_200, base_url, audit_id, detected_location, run_pagespeed)
    else:
        for p in pages_list:
            if _is_200(p):
                p["seo_score"] = _calculate_seo_score(p)
                p["seo_grade"] = _seo_grade(p["seo_score"])
            _conn = get_db_conn()
            try:
                db_update_page_ai(_conn, audit_id, p.get("url", ""), p)
            except Exception: pass
            finally:
                release_db_conn(_conn)

    # ── AI alt text for missing-ALT images ────────────────────────────────────
    if ai_mode != "4":
        from ai_helpers import ai_alt_recommendations
        missing_imgs = [i for i in images_list if i.get("alt_status") == "Missing"]
        if missing_imgs:
            alt_recs = ai_alt_recommendations(missing_imgs)
            for img in images_list:
                if img.get("src") in alt_recs:
                    img["ai_alt_recommendation"] = alt_recs[img["src"]]
            # Update image records in DB
            if alt_recs:
                _conn = get_db_conn()
                try:
                    db_insert_images_batch(_conn, audit_id, images_list)
                except Exception as e:
                    logger.error(f"DB alt recs update error: {e}")
                finally:
                    release_db_conn(_conn)

    # ── Site-wide AI ───────────────────────────────────────────────────────────
    site_recommendation_text = ""
    keyword_data = {}; blog_topics_data = []; backlink_strategy_data = {}
    six_month_plan_data = {}; internal_linking_data = {}
    keyword_url_map_data = []; axo_data = {}
    new_page_suggestions = []
    keyword_planner_data = []
    llm_prompts_data = []
    depth_analysis_data = []
    schema_analysis_data = []
    # New: keyword planner, schema, LLM prompts, depth analysis
    keyword_planner_data = []
    schema_analysis_data = []
    llm_prompts_data = []
    depth_analysis_data = []

    if ai_mode != "4":
        from ai_helpers import (
            ai_site_recommendations, ai_keyword_analysis, ai_blog_topics,
            ai_backlink_strategy, ai_six_month_plan, ai_internal_linking_strategy,
            ai_keyword_url_mapping, ai_axo_recommendations,
        )
        logger.info("Running site-wide AI analysis...")
        summary = {
            "total_pages": len(pages_list), "pages_200": len(pages_200),
            "pages_404": len([p for p in pages_list if _is_404(p)]),
            "broken_links": len(broken_links_list),
            "robots_txt": robots_status, "sitemap": sitemap_status,
            "llm_txt": llm_status, "gbp": gbp_status,
            "detected_location": detected_location,
            "thin_pages": len([p for p in pages_200 if p.get("thin_content") == "Yes"]),
            "missing_schema": len([p for p in pages_200 if p.get("schema_markup") == "Missing"]),
            "missing_og": len([p for p in pages_200 if p.get("og_tags") == "Missing"]),
            "images_missing_alt": len([i for i in images_list if i.get("alt_status") == "Missing"]),
        }
        site_recommendation_text = ai_site_recommendations(domain, summary, pages_200)

        all_content = " ".join(
            f"TITLE:{p.get('current_title','')} H1:{p.get('current_h1','')} "
            f"H2:{p.get('h2_tags','')} META:{p.get('current_meta_description','')}"
            for p in pages_200
        )
        brand_name = domain.replace("www.", "").split(".")[0]
        keyword_data = ai_keyword_analysis(all_content, brand_name, detected_location)

        # Auto-detect business_type from AI if not supplied via API param
        if not business_type and keyword_data.get("business_type"):
            business_type = keyword_data["business_type"]
            logger.info(f"business_type auto-detected: {business_type}")
        elif business_type:
            # API param overrides AI — sync back into keyword_data
            keyword_data["business_type"] = business_type
            logger.info(f"business_type from request param: {business_type}")

        if keyword_data.get("services"):
            blog_topics_data       = ai_blog_topics(keyword_data, brand_name, detected_location)
            backlink_strategy_data = ai_backlink_strategy(keyword_data, brand_name, domain, detected_location)
            plan_summary = {
                "total_pages": len(pages_list), "pages_200": len(pages_200),
                "broken_links": len(broken_links_list),
                "thin_pages": len([p for p in pages_200 if p.get("thin_content") == "Yes"]),
                "missing_schema": len([p for p in pages_200 if p.get("schema_markup") == "Missing"]),
                "blog_topics_count": sum(len(s.get("topics", [])) for s in blog_topics_data),
            }
            six_month_plan_data   = ai_six_month_plan(keyword_data, backlink_strategy_data,
                                                        brand_name, domain, plan_summary)
            internal_linking_data = ai_internal_linking_strategy(pages_200, domain)
            keyword_url_map_data  = ai_keyword_url_mapping(pages_200, keyword_data, domain, detected_location)
            axo_data              = ai_axo_recommendations(pages_200, keyword_data, domain, detected_location)

            # ── Keyword Planner (search volume, CPC, competition ranking) ──
            logger.info("Running keyword planner pipeline...")
            try:
                from ai_helpers import ai_keyword_planner_pipeline
                keyword_planner_data = ai_keyword_planner_pipeline(keyword_data, brand_name, detected_location)
                logger.info(f"keyword_planner: {len(keyword_planner_data)} keywords ranked")
            except ImportError:
                logger.warning("ai_keyword_planner_pipeline not in ai_helpers — skipping")
            except Exception as kp_err:
                logger.error(f"keyword_planner FAILED: {kp_err}")

            # ── LLM Prompts (AI search engine prompt generation) ──
            logger.info("Generating LLM prompts...")
            try:
                from ai_helpers import ai_generate_llm_prompts
                llm_prompts_data = ai_generate_llm_prompts(keyword_data, keyword_planner_data, brand_name, detected_location)
                logger.info(f"llm_prompts: {len(llm_prompts_data)} prompts generated")
            except ImportError:
                logger.warning("ai_generate_llm_prompts not in ai_helpers — skipping")
            except Exception as lp_err:
                logger.error(f"llm_prompts FAILED: {lp_err}")

            # ── New Page Suggestions ──
            logger.info("Generating new page suggestions...")
            try:
                from ai_helpers import ai_new_page_suggestions
                new_page_suggestions = ai_new_page_suggestions(pages_200, keyword_data, domain, brand_name, detected_location)
                logger.info(f"new_page_suggestions: {len(new_page_suggestions)} suggestions")
            except ImportError:
                logger.warning("ai_new_page_suggestions not in ai_helpers — skipping")
            except Exception as nps_err:
                logger.error(f"new_page_suggestions FAILED: {nps_err}")

    # depth_analysis and schema_analysis built below before scorecard

    # ── Depth Analysis — built from all crawled pages ──
    depth_analysis_data = []
    for p in pages_list:
        if not p.get("url"):
            continue
        depth_analysis_data.append({
            "depth_level":          int(p.get("crawl_depth", 0) or 0),
            "page_url":             p.get("url", ""),
            "page_title":           p.get("current_title", "") or "",
            "parent_url":           "",
            "seo_score":            int(p.get("seo_score", 0) or 0),
            "status_code":          str(p.get("status", "")),
            "word_count":           int(p.get("word_count", 0) or 0),
            "has_schema":           p.get("schema_markup") == "Present",
            "internal_links_count": 0,
        })
    logger.info(f"depth_analysis: {len(depth_analysis_data)} records built")

    # ── Schema Analysis — built from _schema_analysis collected during crawl ──
    schema_analysis_data = []
    for p in pages_list:
        sa = p.get("_schema_analysis")
        if sa and sa.get("page_url"):
            schema_analysis_data.append(sa)
        elif str(p.get("status", "")).startswith("200") or p.get("status") == 200:
            stypes = p.get("schema_types_found", "")
            schema_analysis_data.append({
                "page_url":             p.get("url", ""),
                "schema_types_found":   stypes.split(", ") if stypes and stypes != "None" else [],
                "schema_snippets":      [],
                "recommended_schemas":  [],
                "recommended_snippets": [],
                "schema_status":        p.get("schema_markup", "Missing"),
                "missing_schemas":      [],
            })
    logger.info(f"schema_analysis: {len(schema_analysis_data)} records built")

    # ── Scorecard ──────────────────────────────────────────────────────────────
    scorecard_results, global_checks = build_scorecard(
        pages_list, robots_status, sitemap_status,
        llm_status, gbp_status, broken_links_list
    )

    # ── Site analysis data ─────────────────────────────────────────────────────
    site_analysis_data = _build_site_analysis(pages_list, sitemap_urls_found)

    # ── Generated SEO files ────────────────────────────────────────────────────
    generated_files = _generate_seo_files(base_url, domain, pages_list, broken_links_list)

    # ── DB — save site-wide data ───────────────────────────────────────────────
    def _safe_db_insert(label, func, *args):
        _c = get_db_conn()
        try:
            func(_c, *args)
            logger.info(f"  DB saved: {label}")
        except Exception as _e:
            logger.error(f"  DB FAILED {label}: {_e}")
            try: _c.rollback()
            except Exception: pass
        finally:
            release_db_conn(_c)

    logger.info(f"Saving site-wide data to DB (audit #{audit_id})...")
    _safe_db_insert("scorecard",            db_insert_scorecard,          audit_id, scorecard_results, global_checks)
    _safe_db_insert("aeo_faq",              db_insert_aeo_faq,            audit_id, pages_list)
    _safe_db_insert("site_analysis",        db_insert_site_analysis,      audit_id, site_analysis_data)
    _safe_db_insert("generated_files",      db_insert_generated_files,    audit_id, generated_files)
    _safe_db_insert("seo_keywords",         db_insert_keywords,           audit_id, keyword_data)
    _safe_db_insert("blog_topics",          db_insert_blog_topics,        audit_id, blog_topics_data)
    _safe_db_insert("backlink_strategies",  db_insert_backlinks,          audit_id, backlink_strategy_data)
    _safe_db_insert("six_month_plan",       db_insert_plan,               audit_id, six_month_plan_data)
    _safe_db_insert("internal_linking",     db_insert_internal_linking,   audit_id, internal_linking_data)
    _safe_db_insert("keyword_url_mapping",  db_insert_kw_url_map,         audit_id, keyword_url_map_data)
    _safe_db_insert("axo_recommendations",  db_insert_axo,                audit_id, axo_data)

    # ── New tables — always save, log counts so empty tables are visible ──
    logger.info(f"  new_page_suggestions={len(new_page_suggestions)} "                f"keyword_planner={len(keyword_planner_data)} "                f"schema_analysis={len(schema_analysis_data)} "                f"llm_prompts={len(llm_prompts_data)} "                f"depth_analysis={len(depth_analysis_data)}")
    _safe_db_insert("new_page_suggestions",   db_insert_new_page_suggestions, audit_id, new_page_suggestions)
    _safe_db_insert("keyword_planner",        db_insert_keyword_planner,      audit_id, keyword_planner_data)
    _safe_db_insert("schema_markup_analysis", db_insert_schema_analysis,      audit_id, schema_analysis_data)
    _safe_db_insert("llm_prompts",            db_insert_llm_prompts,          audit_id, llm_prompts_data)
    _safe_db_insert("depth_analysis",         db_insert_depth_analysis,       audit_id, depth_analysis_data)

    logger.info("All site-wide DB saves complete.")

    # ── Excel & PDF exports ────────────────────────────────────────────────────
    from excel_export import generate_excel
    from pdf_export   import generate_pdf

    logger.info("Generating Excel...")
    excel_file = generate_excel(
        pages=pages_list, broken_links=broken_links_list, images=images_list,
        scorecard_results=scorecard_results, global_checks=global_checks,
        keyword_data=keyword_data, blog_topics_data=blog_topics_data,
        backlink_strategy_data=backlink_strategy_data,
        six_month_plan_data=six_month_plan_data,
        internal_linking_data=internal_linking_data,
        keyword_url_map_data=keyword_url_map_data, axo_data=axo_data,
        base_url=base_url, domain=domain, timestamp=timestamp,
        robots_status=robots_status, sitemap_status=sitemap_status,
        llm_status=llm_status, gbp_status=gbp_status,
    )

    logger.info("Generating PDF...")
    pdf_file = generate_pdf(
        pages=pages_list, broken_links=broken_links_list, images=images_list,
        scorecard_results=scorecard_results, global_checks=global_checks,
        keyword_data=keyword_data, blog_topics_data=blog_topics_data,
        backlink_strategy_data=backlink_strategy_data,
        six_month_plan_data=six_month_plan_data,
        internal_linking_data=internal_linking_data,
        keyword_url_map_data=keyword_url_map_data, axo_data=axo_data,
        base_url=base_url, domain=domain, timestamp=timestamp,
        site_recommendation_text=site_recommendation_text,
        detected_location=detected_location,
        robots_status=robots_status, sitemap_status=sitemap_status,
        llm_status=llm_status, gbp_status=gbp_status,
    )

    # ── DB — mark complete ─────────────────────────────────────────────────────
    conn = get_db_conn()
    try:
        db_update_audit_complete(conn, audit_id, {
            "total_pages":       len(pages_list),
            "pages_200":         len(pages_200),
            "pages_404":         len([p for p in pages_list if _is_404(p)]),
            "broken_links":      len(broken_links_list),
            "images_missing_alt": len([i for i in images_list if i.get("alt_status") == "Missing"]),
            "robots_status":     robots_status,
            "sitemap_status":    sitemap_status,
            "llm_status":        llm_status,
            "gbp_status":        gbp_status,
            "site_recommendation": site_recommendation_text,
            "detected_location": detected_location,
            "business_type":     business_type or keyword_data.get("business_type", ""),
            "ssl_status":        ssl_status,
            "www_resolve":       www_resolve,
            "sitemap_size":      sitemap_size,
            "excel_file":        excel_file,
            "pdf_file":          pdf_file,
        })
    finally:
        release_db_conn(conn)

    logger.info(f"Audit #{audit_id} complete — Excel: {excel_file} PDF: {pdf_file}")
    return {"audit_id": audit_id, "excel_file": excel_file, "pdf_file": pdf_file}


# ── Per-page AI analysis ──────────────────────────────────────────────────────

def _analyze_pages(pages_200, base_url, audit_id, detected_location, run_pagespeed):
    from ai_helpers import ai_analysis, ai_aeo_faq, ai_body_copy_guidance
    from db import get_db_conn, release_db_conn, db_update_page_ai, db_mark_url_progress

    total = len(pages_200)

    def _one(idx, pd):
        url = pd.get("url", "")
        content = pd.get("_content", "") or f"{pd.get('current_title','')} {pd.get('current_meta_description','')}"

        try:
            with ThreadPoolExecutor(max_workers=3) as sub:
                ai_f   = sub.submit(ai_analysis, url,
                                    pd.get("current_title", ""),
                                    pd.get("current_meta_description", ""),
                                    pd.get("current_h1", ""), content)
                # PageSpeed always runs — free tier works without API key
                mob_f  = sub.submit(_get_pagespeed, url, "mobile")
                desk_f = sub.submit(_get_pagespeed, url, "desktop")

                ai      = ai_f.result(timeout=120) or {}
                mobile  = mob_f.result(timeout=120)
                desktop = desk_f.result(timeout=120)

            pd.update({
                "primary_keyword":          ai.get("primary_keyword", ""),
                "secondary_keywords":       ", ".join(ai.get("secondary_keywords", [])),
                "short_tail_keywords":      ", ".join(ai.get("short_tail_keywords", [])),
                "long_tail_keywords":       ", ".join(ai.get("long_tail_keywords", [])),
                "ai_meta_title":            ai.get("meta_title", ""),
                "ai_meta_description":      ai.get("meta_description", ""),
                "ai_h1":                    ai.get("h1", ""),
                "ai_og_title":              ai.get("og_title", ""),
                "ai_og_description":        ai.get("og_description", ""),
                "ai_og_image_url":          ai.get("og_image_url", ""),
                "ai_schema_recommendation": ai.get("schema_type", ""),
                "ai_schema_code_snippet":   ai.get("schema_code_snippet", ""),
                "ai_optimized_url":         ai.get("optimized_url", ""),
                "image_optimization_tips":  ai.get("image_optimization_tips", ""),
                "serp_preview": _serp_preview(url, ai.get("meta_title", ""), ai.get("meta_description", "")),
                "mobile_score":  str(mobile.get("score",  "N/A")),
                "mobile_lcp":    str(mobile.get("lcp",    "N/A")),
                "mobile_cls":    str(mobile.get("cls",    "N/A")),
                "mobile_fcp":    str(mobile.get("fcp",    "N/A")),
                "desktop_score": str(desktop.get("score", "N/A")),
                "desktop_lcp":   str(desktop.get("lcp",   "N/A")),
                "desktop_cls":   str(desktop.get("cls",   "N/A")),
                "desktop_fcp":   str(desktop.get("fcp",   "N/A")),
            })

            # AEO FAQ + body copy guidance — first 50 pages only (cost control)
            if idx < 50:
                faq = ai_aeo_faq(url, pd.get("current_title", ""),
                                 pd.get("current_h1", ""), content,
                                 pd.get("primary_keyword", ""), detected_location)
                pd["aeo_faq"]       = json.dumps(faq) if faq else ""
                pd["_aeo_faq_list"] = faq or []

                body_guidance = ai_body_copy_guidance(
                    url, pd.get("current_title", ""), pd.get("current_h1", ""),
                    content, pd.get("primary_keyword", ""),
                    pd.get("word_count", 0), detected_location
                )
                pd["body_copy_guidance"] = json.dumps(body_guidance) if body_guidance else ""
            else:
                pd["aeo_faq"]            = ""
                pd["_aeo_faq_list"]      = []
                pd["body_copy_guidance"] = ""

            pd["seo_score"] = _calculate_seo_score(pd)
            pd["seo_grade"] = _seo_grade(pd["seo_score"])

        except Exception as e:
            logger.error(f"Analysis error {url}: {e}")
            pd["seo_score"] = _calculate_seo_score(pd)
            pd["seo_grade"] = _seo_grade(pd["seo_score"])

        conn = get_db_conn()
        try:
            db_update_page_ai(conn, audit_id, url, pd)
            db_mark_url_progress(conn, audit_id, url, "analyzed", str(pd.get("status", "")))
        except Exception as e:
            logger.error(f"DB update error {url}: {e}")
        finally:
            release_db_conn(conn)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futs = {executor.submit(_one, i, p): p for i, p in enumerate(pages_200)}
        done = 0
        for f in as_completed(futs):
            done += 1
            try: f.result(timeout=300)
            except Exception as e: logger.error(f"Analyze future error: {e}")
            if done % 5 == 0 or done == total:
                logger.info(f"  AI analyzed: {done}/{total}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_base_url(input_url: str) -> str:
    try:
        r = _safe_get(input_url, timeout=12)
        if r.status_code < 500:
            return input_url
    except Exception:
        pass
    parsed   = urlparse(input_url)
    host     = parsed.netloc
    alt_host = host[4:] if host.startswith("www.") else "www." + host
    alt_url  = f"{parsed.scheme}://{alt_host}{parsed.path}"
    try:
        r = _safe_get(alt_url, timeout=12)
        if r.status_code < 500:
            return alt_url
    except Exception:
        pass
    return input_url


def _check_gbp(base_url: str) -> str:
    try:
        r = _safe_get(base_url, timeout=12)
        if r.status_code == 200:
            markers = ["google.com/maps", "maps.google.com", "goo.gl/maps",
                       "business.google.com", "LocalBusiness", "schema.org/LocalBusiness",
                       "google.com/maps/place"]
            if any(m in r.text for m in markers):
                return "Present"
    except Exception:
        pass
    return "Not Found"


def _check_ssl(domain: str) -> str:
    """Check SSL certificate validity and expiry days."""
    try:
        import ssl, socket
        clean_host = domain.replace("www.", "") if not domain.startswith("www.") else domain
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(socket.socket(), server_hostname=clean_host) as s:
            s.settimeout(10)
            s.connect((clean_host, 443))
            cert = s.getpeercert()
            exp = datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z')
            days = (exp - datetime.now()).days
            if days < 0:
                return f"EXPIRED ({abs(days)} days ago)"
            elif days < 30:
                return f"Expiring soon ({days} days)"
            else:
                return f"Valid ({days} days remaining)"
    except Exception as e:
        return f"Check failed: {str(e)[:50]}"


def _check_www_resolve(base_url: str, domain: str) -> str:
    """Check that www and non-www both resolve to the same canonical URL."""
    try:
        if domain.startswith("www."):
            non_www = domain[4:]
            r = _safe_get(f"https://{non_www}/", timeout=10)
            if r.url.replace("http://", "https://").rstrip("/") != base_url.rstrip("/"):
                return f"Non-www does not redirect to www ({r.url})"
        else:
            r = _safe_get(f"https://www.{domain}/", timeout=10)
            if r.url.replace("http://", "https://").rstrip("/") != base_url.rstrip("/"):
                return f"www does not redirect to non-www ({r.url})"
        return "OK"
    except Exception:
        return "Could not test"


def _check_sitemap_size(domain: str) -> str:
    """Return sitemap size status string."""
    try:
        r = _safe_get(f"https://{domain}/sitemap.xml", timeout=12)
        if r.status_code == 200:
            size_kb = round(len(r.content) / 1024, 1)
            if size_kb > 50000:
                return f"Too large ({size_kb} KB — max 50MB)"
            elif size_kb > 10000:
                return f"Large ({size_kb} KB — consider splitting)"
            else:
                return f"OK ({size_kb} KB)"
    except Exception:
        pass
    return "Could not check"


def _detect_location(domain: str, pages_list: list = None, ai_mode: str = "4") -> str:
    """Detect target location from TLD or AI analysis of page content."""
    tld_map = {
        ".uk": "United Kingdom", ".co.uk": "United Kingdom",
        ".au": "Australia", ".ca": "Canada", ".in": "India",
        ".de": "Germany", ".fr": "France", ".sg": "Singapore",
        ".ae": "UAE", ".nz": "New Zealand", ".za": "South Africa",
        ".ie": "Ireland", ".nl": "Netherlands", ".it": "Italy",
        ".es": "Spain", ".jp": "Japan", ".br": "Brazil", ".mx": "Mexico",
    }
    for tld, loc in tld_map.items():
        if domain.endswith(tld):
            return loc

    # Try AI detection if enabled and pages available
    if ai_mode != "4" and pages_list:
        try:
            from ai_helpers import ai_chat
            loc_sample = " ".join(
                f"{p.get('current_title','')} {p.get('current_meta_description','')}"
                for p in pages_list[:20] if _is_200(p)
            )[:3000]
            if loc_sample.strip():
                prompt = ("From this website content, identify the PRIMARY COUNTRY this business "
                          "operates in or targets. Return COUNTRY name only. If unclear, return 'Global'.\n\n"
                          f"Content: {loc_sample}")
                result = ai_chat(prompt, max_tokens=50, temperature=0.1).strip().strip('"').strip("'")
                if result and result.lower() != "global":
                    return result
        except Exception:
            pass
    return "Global"


def _build_site_analysis(pages_list, sitemap_urls):
    """Build comprehensive site analysis: HTTP status, crawl depth, hreflang, sitemap comparison."""
    data = []

    # HTTP Status Distribution
    status_counts = {}
    for p in pages_list:
        st = str(p.get("status", "Unknown"))
        if st.startswith("200"): key = "200"
        elif st == "301": key = "301"
        elif st == "302": key = "302"
        elif st == "404": key = "404"
        elif st.startswith("5"): key = "5xx"
        elif any(st.startswith(x) for x in ["Error", "Connection", "Timeout"]): key = "Error"
        else: key = st
        status_counts[key] = status_counts.get(key, 0) + 1
    for code, count in sorted(status_counts.items()):
        data.append({"type": "http_status", "key": code, "value": f"{count} pages", "count": count})

    # Crawl Depth Distribution
    depth_counts = {}
    for p in pages_list:
        d = p.get("crawl_depth", -1)
        key = str(d) if d >= 0 else "Unknown"
        depth_counts[key] = depth_counts.get(key, 0) + 1
    for depth, count in sorted(depth_counts.items(),
                               key=lambda x: (x[0] == "Unknown", int(x[0]) if x[0].isdigit() else 999)):
        data.append({"type": "crawl_depth", "key": depth, "value": f"{count} pages", "count": count})

    # Hreflang Summary
    hreflang_pages = [p for p in pages_list if p.get("hreflang_tags")]
    hreflang_langs = {}
    for p in hreflang_pages:
        for part in (p.get("hreflang_tags", "")).split(" | "):
            if ":" in part:
                lang = part.split(":")[0].strip()
                hreflang_langs[lang] = hreflang_langs.get(lang, 0) + 1
    data.append({"type": "hreflang_summary", "key": "total_pages_with_hreflang",
                 "value": str(len(hreflang_pages)), "count": len(hreflang_pages)})
    data.append({"type": "hreflang_summary", "key": "total_languages",
                 "value": str(len(hreflang_langs)), "count": len(hreflang_langs)})
    for lang, count in sorted(hreflang_langs.items()):
        data.append({"type": "hreflang_lang", "key": lang, "value": f"{count} pages", "count": count})

    # Sitemap vs Crawled Pages comparison
    crawled_norm = {p.get("url", "").rstrip("/").lower() for p in pages_list if p.get("url")}
    sitemap_norm = {u.rstrip("/").lower() for u in sitemap_urls}
    in_both         = crawled_norm & sitemap_norm
    in_sitemap_only = sitemap_norm - crawled_norm
    in_crawl_only   = crawled_norm - sitemap_norm
    data.append({"type": "sitemap_comparison", "key": "sitemap_total",  "value": str(len(sitemap_norm)),  "count": len(sitemap_norm)})
    data.append({"type": "sitemap_comparison", "key": "crawled_total",  "value": str(len(crawled_norm)),  "count": len(crawled_norm)})
    data.append({"type": "sitemap_comparison", "key": "in_both",        "value": str(len(in_both)),       "count": len(in_both)})
    data.append({"type": "sitemap_comparison", "key": "sitemap_only",   "value": str(len(in_sitemap_only)),"count": len(in_sitemap_only)})
    data.append({"type": "sitemap_comparison", "key": "crawl_only",     "value": str(len(in_crawl_only)), "count": len(in_crawl_only)})
    for u in list(in_sitemap_only)[:100]:
        data.append({"type": "sitemap_only_url", "key": "url", "value": u, "count": 0})
    for u in list(in_crawl_only)[:100]:
        data.append({"type": "crawl_only_url", "key": "url", "value": u, "count": 0})

    return data


def _generate_seo_files(base_url, domain, pages_list, broken_links):
    """Generate sitemap.xml, robots.txt, llms.txt, .htaccess, nginx redirects, broken link report."""
    files = []
    urls_200 = [p["url"] for p in pages_list if _is_200(p) and p.get("url")]

    # sitemap.xml
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for u in urls_200:
        xml += f'  <url><loc>{u}</loc></url>\n'
    xml += '</urlset>'
    files.append({"file_name": "sitemap.xml", "file_type": "application/xml",
                  "file_content": xml, "file_size": len(xml.encode())})

    # robots.txt
    robots = f"User-agent: *\nAllow: /\n\nSitemap: {base_url.rstrip('/')}/sitemap.xml\n"
    files.append({"file_name": "robots.txt", "file_type": "text/plain",
                  "file_content": robots, "file_size": len(robots.encode())})

    # llms.txt
    brand = domain.replace("www.", "").split(".")[0].title()
    keywords = set()
    for p in pages_list:
        if p.get("primary_keyword"):
            keywords.add(p["primary_keyword"])
    llms = f"# {brand}\n\n## About\n{brand} is a business at {base_url}\n\n## Services\n"
    for kw in list(keywords)[:20]:
        llms += f"- {kw}\n"
    llms += f"\n## Contact\nWebsite: {base_url}\n"
    files.append({"file_name": "llms.txt", "file_type": "text/plain",
                  "file_content": llms, "file_size": len(llms.encode())})

    # .htaccess
    htaccess = "# AquilTechLabs SEO .htaccess\nRewriteEngine On\n\n"
    htaccess += "# Force HTTPS\nRewriteCond %{HTTPS} off\nRewriteRule ^(.*)$ https://%{HTTP_HOST}%{REQUEST_URI} [L,R=301]\n\n"
    if domain.startswith("www."):
        htaccess += "# Force www\nRewriteCond %{HTTP_HOST} !^www\\.\nRewriteRule ^(.*)$ https://www.%{HTTP_HOST}/$1 [L,R=301]\n\n"
    htaccess += "# Remove trailing slash\nRewriteCond %{REQUEST_FILENAME} !-d\nRewriteRule ^(.*)/$ /$1 [L,R=301]\n\n"
    htaccess += "<IfModule mod_deflate.c>\nAddOutputFilterByType DEFLATE text/html text/css application/javascript\n</IfModule>\n\n"
    htaccess += "<IfModule mod_expires.c>\nExpiresActive On\nExpiresByType image/jpeg \"access plus 1 year\"\nExpiresByType image/png \"access plus 1 year\"\nExpiresByType text/css \"access plus 1 month\"\nExpiresByType application/javascript \"access plus 1 month\"\n</IfModule>\n"
    files.append({"file_name": ".htaccess", "file_type": "text/plain",
                  "file_content": htaccess, "file_size": len(htaccess.encode())})

    # .htaccess_redirects
    redirects_ht = "# Broken link redirects (auto-generated)\n"
    for bl in broken_links[:200]:
        broken_path = urlparse(bl.get("broken_url", "")).path or "/"
        target = bl.get("redirect_suggestion", base_url) or base_url
        target_path = urlparse(target).path or "/"
        if broken_path != target_path:
            redirects_ht += f"Redirect 301 {broken_path} {target}\n"
    files.append({"file_name": ".htaccess_redirects", "file_type": "text/plain",
                  "file_content": redirects_ht, "file_size": len(redirects_ht.encode())})

    # nginx_redirects.conf
    redirects_ng = "# Nginx broken link redirects\n"
    for bl in broken_links[:200]:
        broken_path = urlparse(bl.get("broken_url", "")).path or "/"
        target_path = urlparse(bl.get("redirect_suggestion", "/") or "/").path or "/"
        if broken_path != target_path:
            redirects_ng += f"rewrite ^{broken_path}$ {target_path} permanent;\n"
    files.append({"file_name": "nginx_redirects.conf", "file_type": "text/plain",
                  "file_content": redirects_ng, "file_size": len(redirects_ng.encode())})

    # broken_links_report.txt
    report = f"Broken Links Report — {domain}\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
    for bl in broken_links:
        report += f"SOURCE: {bl.get('source_page','')}\n  BROKEN: {bl.get('broken_url','')} ({bl.get('status','')})\n  SUGGEST: {bl.get('redirect_suggestion','')}\n\n"
    if not broken_links:
        report += "No broken links found.\n"
    files.append({"file_name": "broken_links_report.txt", "file_type": "text/plain",
                  "file_content": report, "file_size": len(report.encode())})

    return files