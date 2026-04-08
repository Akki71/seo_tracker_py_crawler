"""
crawler.py — Core SEO crawl + analyze logic.
Called by the API. All inputs come as parameters (no stdin prompts).
"""

import os, re, json, time, hashlib, logging, sys, base64, io, subprocess, random
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, urldefrag
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from db import (
    get_db_conn, release_db_conn,
    db_create_audit, db_update_audit_complete,
    db_insert_page, db_update_page_ai,
    db_insert_images_batch, db_insert_broken_links_batch,
    db_insert_keywords, db_insert_blog_topics, db_insert_backlinks,
    db_insert_plan, db_insert_internal_linking, db_insert_kw_url_map,
    db_insert_axo, db_insert_scorecard, db_insert_aeo_faq,
    db_insert_site_analysis, db_insert_generated_files,
    db_mark_url_progress, db_get_processed_urls, db_get_crawled_urls,
    db_find_existing_audit, db_query_pages,
)
from ai_helpers import (
    ai_chat, ai_analysis, ai_site_recommendations,
    ai_keyword_analysis, ai_blog_topics, ai_backlink_strategy,
    ai_six_month_plan, ai_aeo_faq, ai_body_copy_guidance,
    ai_internal_linking_strategy, ai_keyword_url_mapping, ai_axo_recommendations,
    ai_alt_recommendations, setup_ai_clients,
)
from excel_export import generate_excel
from pdf_export import generate_pdf

logger = logging.getLogger(__name__)

CRAWL_TIMEOUT       = 30
BROKEN_LINK_TIMEOUT = 8
MAX_WORKERS         = 20
SCREENSHOT_DIR      = "screenshots"

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

# ── Session ───────────────────────────────────────────────────────────────────

def _create_session():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=2,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["HEAD","GET"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

_session = _create_session()

def _safe_get(url, timeout=CRAWL_TIMEOUT, **kw):
    try:
        return _session.get(url, timeout=timeout, allow_redirects=True, **kw)
    except requests.exceptions.SSLError:
        return _session.get(url, timeout=timeout, allow_redirects=True, verify=False, **kw)

def _safe_head(url, timeout=BROKEN_LINK_TIMEOUT, **kw):
    try:
        return _session.head(url, timeout=timeout, allow_redirects=True, **kw)
    except requests.exceptions.SSLError:
        return _session.head(url, timeout=timeout, allow_redirects=True, verify=False, **kw)

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
    s = p.get("status","") if isinstance(p, dict) else p
    return str(s).startswith("200") or s == 200

def _is_404(p):
    s = p.get("status","") if isinstance(p, dict) else p
    return str(s) == "404" or s == 404

def _calculate_seo_score(pd):
    s = 0
    if _is_200(pd): s += 15
    if pd.get("canonical_status") == "Correct": s += 10
    if pd.get("thin_content") == "No": s += 10
    if pd.get("duplicate_status") == "Unique": s += 10
    if pd.get("current_title"): s += 5
    if 30 <= len(pd.get("current_title","")) <= 60: s += 5
    if pd.get("current_meta_description"): s += 5
    if 70 <= len(pd.get("current_meta_description","")) <= 160: s += 5
    if pd.get("current_h1"): s += 5
    if pd.get("og_tags") == "Present": s += 5
    if pd.get("schema_markup") == "Present": s += 5
    if pd.get("google_analytics") == "Yes": s += 5
    ic = pd.get("total_images", 0) or 0
    im = pd.get("images_missing_alt", 0) or 0
    if ic > 0 and im == 0: s += 5
    elif ic == 0: s += 5
    if pd.get("google_search_console") == "Yes": s += 5
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
    display_url = urlparse(url).netloc + urlparse(url).path
    return f"{(title or '(No Title)')[:60]}\n{display_url}\n{(desc or '(No Description)')[:160]}"

def _check_file(domain, path, sitemap_urls_out=None):
    for scheme in ["https","http"]:
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
                    has_sitemap = "sitemap:" in content.lower()
                    return "Valid" + (" + Sitemap ref" if has_sitemap else "")
                elif path == "llms.txt":
                    return f"Present ({len(content)} chars)"
                return "Present"
            elif r.status_code == 403:
                return "Blocked (403)"
        except Exception:
            continue
    return "Not Found"

def _get_pagespeed(url, strategy):
    if not PAGESPEED_API_KEY:
        return {"score":"N/A","lcp":"N/A","cls":"N/A","fcp":"N/A","screenshot":None}
    try:
        r = requests.get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
            params={"url":url,"strategy":strategy,"key":PAGESPEED_API_KEY}, timeout=60)
        data = r.json()
        lh = data.get("lighthouseResult",{})
        audits = lh.get("audits",{})
        perf = lh.get("categories",{}).get("performance",{}).get("score")
        score = int(perf*100) if perf is not None else "N/A"
        ss_data = audits.get("final-screenshot",{}).get("details",{}).get("data","")
        screenshot = ss_data.split(",")[1] if ss_data and "," in ss_data else None
        return {
            "score": score,
            "lcp": audits.get("largest-contentful-paint",{}).get("displayValue","N/A"),
            "cls": audits.get("cumulative-layout-shift",{}).get("displayValue","N/A"),
            "fcp": audits.get("first-contentful-paint",{}).get("displayValue","N/A"),
            "screenshot": screenshot,
        }
    except Exception as e:
        logger.error(f"PageSpeed [{strategy}] {url}: {e}")
        return {"score":"Error","lcp":"Error","cls":"Error","fcp":"Error","screenshot":None}

# ── Crawl Function ─────────────────────────────────────────────────────────────

def _crawl_page(url, base_url, domain, visited_set,
                pages_list, images_list, pending_links,
                content_hash_map, pages_lock, images_lock, broken_lock):
    pd = OrderedDict()
    pd["url"] = url
    pd["url_cleaned"] = _url_cleanup(url)
    new_links = []

    try:
        r = _safe_get(url, timeout=CRAWL_TIMEOUT)
        status = r.status_code
        html_text = r.text
        response_headers = dict(r.headers)
    except requests.exceptions.Timeout:
        pd["status"] = "Timeout"
        with pages_lock:
            pages_list.append(pd)
        return []
    except requests.exceptions.ConnectionError as e:
        pd["status"] = f"Connection Error"
        with pages_lock:
            pages_list.append(pd)
        return []
    except Exception as e:
        pd["status"] = f"Error: {str(e)[:80]}"
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
        if isinstance(status, int) and status in (301,302,307,308):
            pd["redirect_target"] = r.url
        with pages_lock:
            pages_list.append(pd)
        return []

    # ── 200 OK: full parse ──
    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception:
        soup = BeautifulSoup(html_text, "html5lib")

    text_content = _clean(soup.get_text())
    word_count = len(text_content.split())

    # Canonical
    can_tag = soup.find("link", rel="canonical")
    can_href = can_tag["href"] if can_tag and can_tag.get("href") else None
    pd["canonical_status"] = _canonical_check(url, can_href)
    pd["canonical_url"] = can_href or "Not Set"

    # Duplicate
    ch = hashlib.md5(text_content.encode()).hexdigest()
    if ch in content_hash_map:
        pd["duplicate_status"] = f"Duplicate of {content_hash_map[ch]}"
    else:
        content_hash_map[ch] = url
        pd["duplicate_status"] = "Unique"

    pd["word_count"] = word_count
    pd["thin_content"] = "Yes" if word_count < 300 else "No"

    title = _clean(soup.title.string) if soup.title and soup.title.string else ""
    meta_tag = soup.find("meta", attrs={"name":"description"})
    meta_desc = _clean(meta_tag["content"]) if meta_tag and meta_tag.get("content") else ""
    h1_tag = soup.find("h1")
    h1 = _clean(h1_tag.text) if h1_tag else ""
    h2_list = [_clean(h.text) for h in soup.find_all("h2")[:5]]

    pd["current_title"] = title
    pd["title_length"] = len(title)
    pd["current_meta_description"] = meta_desc
    pd["meta_desc_length"] = len(meta_desc)
    pd["current_h1"] = h1
    pd["h2_tags"] = " | ".join(h2_list) if h2_list else "None"

    ga_markers = ["gtag(","G-","UA-","google-analytics.com","googletagmanager.com"]
    pd["google_analytics"] = "Yes" if any(m in html_text for m in ga_markers) else "No"

    is_homepage = (_normalize(url) == _normalize(base_url) or
                   urlparse(url).path in ("","","/","/index.html","/index.php"))
    if is_homepage:
        gsc_meta = soup.find("meta", attrs={"name":"google-site-verification"})
        pd["google_search_console"] = "Yes" if gsc_meta else "No"
    else:
        pd["google_search_console"] = "Homepage Only"

    og_t = soup.find("meta", property="og:title")
    og_d = soup.find("meta", property="og:description")
    og_i = soup.find("meta", property="og:image")
    pd["og_tags"] = "Present" if og_t else "Missing"
    pd["og_title_current"] = _clean(og_t["content"]) if og_t and og_t.get("content") else "Missing"
    pd["og_description_current"] = _clean(og_d["content"]) if og_d and og_d.get("content") else "Missing"
    pd["og_image_current"] = og_i["content"] if og_i and og_i.get("content") else "Missing"

    schema_scripts = soup.find_all("script", type="application/ld+json")
    schema_types = []
    for sc in schema_scripts:
        try:
            sd = json.loads(sc.string)
            if isinstance(sd, dict): schema_types.append(sd.get("@type","Unknown"))
            elif isinstance(sd, list):
                for item in sd:
                    if isinstance(item, dict): schema_types.append(item.get("@type","Unknown"))
        except Exception: pass
    pd["schema_markup"] = "Present" if schema_types else "Missing"
    pd["schema_types_found"] = ", ".join(schema_types) if schema_types else "None"

    hreflang_links = soup.find_all("link", rel="alternate", hreflang=True)
    pd["hreflang_tags"] = " | ".join(
        f"{hl.get('hreflang','')}:{hl.get('href','')}" for hl in hreflang_links
    ) if hreflang_links else ""

    page_imgs = soup.find_all("img")
    img_total = len(page_imgs)
    img_missing = 0
    page_image_records = []
    for img in page_imgs:
        alt = img.get("alt","").strip()
        raw_src = img.get("src","") or img.get("data-src","") or ""
        full_src = urljoin(url, raw_src.strip()) if raw_src else "(no src)"
        has_alt = bool(alt)
        if not has_alt: img_missing += 1
        page_image_records.append({
            "page": url, "src": full_src,
            "alt": alt, "alt_status": "Present" if has_alt else "Missing"
        })
    with images_lock:
        images_list.extend(page_image_records)
    pd["total_images"] = img_total
    pd["images_missing_alt"] = img_missing
    pd["image_alt_status"] = (
        "All Present" if img_missing == 0 and img_total > 0
        else f"{img_missing}/{img_total} Missing" if img_total > 0
        else "No Images"
    )

    # Technical checks
    viewport_tag = soup.find("meta", attrs={"name":"viewport"})
    pd["viewport_configured"] = "Yes" if viewport_tag else "No"
    pd["html_size_kb"] = round(len(html_text)/1024, 1)
    pd["html_size_issue"] = "Yes" if pd["html_size_kb"] > 100 else "No"
    pd["is_secure"] = "Yes" if url.startswith("https://") else "No"

    mixed = []
    if url.startswith("https://"):
        for tn, attr in [("script","src"),("link","href"),("img","src"),("iframe","src")]:
            for tag in soup.find_all(tn):
                res = tag.get(attr,"")
                if res.startswith("http://"): mixed.append(f"{tn}:{res[:80]}")
    pd["mixed_content"] = "Yes" if mixed else "No"
    pd["mixed_content_details"] = " | ".join(mixed[:10])

    unmin_js = []
    for script in soup.find_all("script"):
        src = script.get("src","")
        if src and ".min." not in src and not any(c in src for c in ["cdn.","cdnjs.","googleapis."]):
            unmin_js.append(src[:100])
    pd["unminified_js"] = "Yes" if unmin_js else "No"
    pd["unminified_js_details"] = " | ".join(unmin_js[:5])

    unmin_css = []
    for link in soup.find_all("link", rel="stylesheet"):
        href = link.get("href","")
        if href and ".min." not in href and not any(c in href for c in ["cdn.","cdnjs.","googleapis."]):
            unmin_css.append(href[:100])
    pd["unminified_css"] = "Yes" if unmin_css else "No"
    pd["unminified_css_details"] = " | ".join(unmin_css[:5])

    amp_link = soup.find("link", rel="amphtml")
    pd["amp_link"] = amp_link.get("href","Present") if amp_link else "None"

    og_issues = []
    if not og_t: og_issues.append("Missing og:title")
    if not og_d: og_issues.append("Missing og:description")
    if not og_i: og_issues.append("Missing og:image")
    if not soup.find("meta", property="og:url"): og_issues.append("Missing og:url")
    if not soup.find("meta", property="og:type"): og_issues.append("Missing og:type")
    pd["og_validation"] = " | ".join(og_issues) if og_issues else "Valid"

    x_robots = response_headers.get("X-Robots-Tag", response_headers.get("x-robots-tag",""))
    pd["x_robots_noindex"] = "Yes" if "noindex" in x_robots.lower() else "No"
    cache_control = response_headers.get("Cache-Control", response_headers.get("cache-control",""))
    pd["page_cache_control"] = cache_control or "Not Set"

    pd["spam_malware_flags"] = "Clean"  # Simplified for API mode

    pd["_content"] = text_content

    # Internal links for discovery
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("javascript:","mailto:","tel:","#")): continue
        full = _normalize(urljoin(url, href))
        with broken_lock:
            pending_links.append((url, full))
        if urlparse(full).netloc == domain:
            path_lower = urlparse(full).path.lower()
            is_asset = any(path_lower.endswith(ext) for ext in SKIP_EXTENSIONS) or \
                       any(pat in path_lower for pat in SKIP_PATTERNS)
            if not is_asset:
                new_links.append(full)

    with pages_lock:
        pages_list.append(pd)
    return new_links

# ── Main Audit Orchestrator ────────────────────────────────────────────────────

def run_audit(input_url: str, brand_id: int, target_location: str = "",
              ai_mode: str = "1", crawl_limit: int = 100,
              run_pagespeed: bool = True) -> dict:
    """
    Run a full SEO audit. Called by the API.
    Returns dict with audit_id, excel_file, pdf_file.
    """
    logger.info(f"Starting audit: brand_id={brand_id} url={input_url} ai_mode={ai_mode}")

    # Setup AI clients
    setup_ai_clients(ai_mode)

    # Resolve URL
    if not input_url.startswith("http"):
        input_url = "https://" + input_url
    base_url = _resolve_base_url(input_url)
    parsed_base = urlparse(base_url)
    domain = parsed_base.netloc
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # DB setup
    conn = get_db_conn()
    audit_id = db_create_audit(conn, brand_id, {
        "domain": domain, "base_url": base_url,
        "target_location": target_location or "Global",
        "ai_mode": ai_mode,
    })
    release_db_conn(conn)
    logger.info(f"Created audit #{audit_id} for brand {brand_id}")

    # Global file checks
    sitemap_urls_found = []
    robots_status  = _check_file(domain, "robots.txt")
    sitemap_status = _check_file(domain, "sitemap.xml", sitemap_urls_out=sitemap_urls_found)
    llm_status     = _check_file(domain, "llms.txt")
    gbp_status     = _check_gbp(base_url)
    logger.info(f"robots={robots_status} sitemap={sitemap_status} llm={llm_status} gbp={gbp_status}")

    # State
    visited = set()
    pages_list = []
    images_list = []
    broken_links_list = []
    pending_links = []
    content_hash_map = {}
    crawl_depth_map = {}
    pages_lock = Lock()
    images_lock = Lock()
    broken_lock = Lock()

    # ── BFS Crawl ──────────────────────────────────────────────────────────────
    logger.info("Starting BFS crawl...")
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
            futures = {executor.submit(
                _crawl_page, u, base_url, domain, visited,
                pages_list, images_list, pending_links, content_hash_map,
                pages_lock, images_lock, broken_lock
            ): u for u in batch}
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
                    logger.error(f"Crawl future error {crawled_url}: {e}")
            logger.info(f"Crawled: {len(visited)} | Queue: {len(queue)}")

    # Assign depths
    for p in pages_list:
        p["crawl_depth"] = crawl_depth_map.get(_normalize(p.get("url","")),
                           crawl_depth_map.get(p.get("url",""), -1))

    logger.info(f"Crawl done. {len(pages_list)} pages, {len(images_list)} images")

    # ── Broken Link Check ──────────────────────────────────────────────────────
    unique_links = {}
    for source, target in pending_links:
        if target not in unique_links:
            unique_links[target] = source

    logger.info(f"Checking {len(unique_links)} links...")
    found_broken = []

    def _check_link(target, source):
        try:
            r = _safe_head(target, timeout=BROKEN_LINK_TIMEOUT)
            if r.status_code in (404,410,500,502,503):
                found_broken.append({
                    "source_page": source, "broken_url": target,
                    "status": r.status_code, "redirect_suggestion": base_url,
                    "redirect_type": "301",
                })
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=10) as executor:
        futs = [executor.submit(_check_link, t, s) for t, s in unique_links.items()]
        for f in as_completed(futs):
            try: f.result(timeout=20)
            except Exception: pass

    broken_links_list.extend(found_broken)

    # ── Location Detection ─────────────────────────────────────────────────────
    if target_location:
        detected_location = target_location
    else:
        detected_location = _detect_location(domain, pages_list, ai_mode)
    logger.info(f"Target location: {detected_location}")

    # ── Save crawled pages to DB ───────────────────────────────────────────────
    pages_200 = [p for p in pages_list if _is_200(p)]
    conn = get_db_conn()
    try:
        logger.info("Saving crawled pages to DB...")
        for p in pages_list:
            try:
                db_insert_page(conn, audit_id, p)
                db_mark_url_progress(conn, audit_id, p.get("url",""), "crawled",
                                     str(p.get("status","")))
            except Exception as e:
                logger.error(f"DB page insert error: {e}")

        if images_list:
            db_insert_images_batch(conn, audit_id, images_list)
        if broken_links_list:
            db_insert_broken_links_batch(conn, audit_id, broken_links_list)
    finally:
        release_db_conn(conn)

    # ── AI + PageSpeed Analysis ────────────────────────────────────────────────
    if ai_mode != "4":
        logger.info(f"Running AI analysis on {len(pages_200)} pages...")
        _analyze_pages(pages_200, base_url, audit_id, detected_location,
                       run_pagespeed, ai_mode)

        # SEO score for non-200 pages
        for p in pages_list:
            if not _is_200(p):
                p["seo_score"] = 0
                p["seo_grade"] = "F"
    else:
        for p in pages_list:
            p["seo_score"] = _calculate_seo_score(p) if _is_200(p) else 0
            p["seo_grade"] = _seo_grade(p["seo_score"])

    # ── Site-wide AI ───────────────────────────────────────────────────────────
    site_recommendation_text = ""
    keyword_data = {}
    blog_topics_data = []
    backlink_strategy_data = {}
    six_month_plan_data = {}
    internal_linking_data = {}
    keyword_url_map_data = []
    axo_data = {}

    if ai_mode != "4":
        logger.info("Running site-wide AI analysis...")
        summary_for_ai = {
            "total_pages": len(pages_list),
            "pages_200": len(pages_200),
            "pages_404": len([p for p in pages_list if _is_404(p)]),
            "broken_links": len(broken_links_list),
            "robots_txt": robots_status, "sitemap": sitemap_status,
            "llm_txt": llm_status, "gbp": gbp_status,
            "detected_location": detected_location,
        }
        site_recommendation_text = ai_site_recommendations(domain, summary_for_ai, pages_200)

        all_content = " ".join(
            f"TITLE:{p.get('current_title','')} H1:{p.get('current_h1','')} "
            f"H2:{p.get('h2_tags','')} META:{p.get('current_meta_description','')}"
            for p in pages_200
        )
        brand_name = domain.replace("www.","").split(".")[0]
        keyword_data = ai_keyword_analysis(all_content, brand_name, detected_location)

        if keyword_data.get("services"):
            blog_topics_data      = ai_blog_topics(keyword_data, brand_name, detected_location)
            backlink_strategy_data = ai_backlink_strategy(keyword_data, brand_name, domain, detected_location)
            plan_summary = {
                "total_pages": len(pages_list), "pages_200": len(pages_200),
                "broken_links": len(broken_links_list),
                "thin_pages": len([p for p in pages_200 if p.get("thin_content")=="Yes"]),
                "missing_schema": len([p for p in pages_200 if p.get("schema_markup")=="Missing"]),
            }
            six_month_plan_data   = ai_six_month_plan(keyword_data, backlink_strategy_data,
                                                       brand_name, domain, plan_summary)
            internal_linking_data = ai_internal_linking_strategy(pages_200, domain)
            keyword_url_map_data  = ai_keyword_url_mapping(pages_200, keyword_data, domain, detected_location)
            axo_data              = ai_axo_recommendations(pages_200, keyword_data, domain, detected_location)

    # ── Scorecard ──────────────────────────────────────────────────────────────
    from scorecard import build_scorecard
    scorecard_results, global_checks = build_scorecard(
        pages_list, robots_status, sitemap_status, llm_status, gbp_status,
        broken_links_list
    )

    # ── Site Analysis ──────────────────────────────────────────────────────────
    site_analysis_data = _build_site_analysis(pages_list, sitemap_urls_found)

    # ── Generated Files ────────────────────────────────────────────────────────
    generated_files = _generate_seo_files(base_url, domain, pages_list, broken_links_list)

    # ── DB Final Save ──────────────────────────────────────────────────────────
    conn = get_db_conn()
    try:
        logger.info("Saving site-wide data to DB...")
        db_insert_keywords(conn, audit_id, keyword_data)
        db_insert_blog_topics(conn, audit_id, blog_topics_data)
        db_insert_backlinks(conn, audit_id, backlink_strategy_data)
        db_insert_plan(conn, audit_id, six_month_plan_data)
        db_insert_internal_linking(conn, audit_id, internal_linking_data)
        db_insert_kw_url_map(conn, audit_id, keyword_url_map_data)
        db_insert_axo(conn, audit_id, axo_data)
        db_insert_scorecard(conn, audit_id, scorecard_results, global_checks)
        db_insert_aeo_faq(conn, audit_id, pages_list)
        db_insert_site_analysis(conn, audit_id, site_analysis_data)
        db_insert_generated_files(conn, audit_id, generated_files)
    finally:
        release_db_conn(conn)

    # ── Excel Export ───────────────────────────────────────────────────────────
    logger.info("Generating Excel...")
    excel_file = generate_excel(
        pages=pages_list, broken_links=broken_links_list,
        images=images_list, scorecard_results=scorecard_results,
        global_checks=global_checks, keyword_data=keyword_data,
        blog_topics_data=blog_topics_data, backlink_strategy_data=backlink_strategy_data,
        six_month_plan_data=six_month_plan_data, internal_linking_data=internal_linking_data,
        keyword_url_map_data=keyword_url_map_data, axo_data=axo_data,
        base_url=base_url, domain=domain, timestamp=timestamp,
        robots_status=robots_status, sitemap_status=sitemap_status,
        llm_status=llm_status, gbp_status=gbp_status,
    )

    # ── PDF Export ─────────────────────────────────────────────────────────────
    logger.info("Generating PDF...")
    pdf_file = generate_pdf(
        pages=pages_list, broken_links=broken_links_list,
        images=images_list, scorecard_results=scorecard_results,
        global_checks=global_checks, keyword_data=keyword_data,
        blog_topics_data=blog_topics_data, backlink_strategy_data=backlink_strategy_data,
        six_month_plan_data=six_month_plan_data, internal_linking_data=internal_linking_data,
        keyword_url_map_data=keyword_url_map_data, axo_data=axo_data,
        base_url=base_url, domain=domain, timestamp=timestamp,
        site_recommendation_text=site_recommendation_text,
        detected_location=detected_location,
        robots_status=robots_status, sitemap_status=sitemap_status,
        llm_status=llm_status, gbp_status=gbp_status,
    )

    # ── DB Complete ────────────────────────────────────────────────────────────
    conn = get_db_conn()
    try:
        db_update_audit_complete(conn, audit_id, {
            "total_pages": len(pages_list),
            "pages_200": len(pages_200),
            "pages_404": len([p for p in pages_list if _is_404(p)]),
            "broken_links": len(broken_links_list),
            "images_missing_alt": len([i for i in images_list if i.get("alt_status")=="Missing"]),
            "robots_status": robots_status, "sitemap_status": sitemap_status,
            "llm_status": llm_status, "gbp_status": gbp_status,
            "site_recommendation": site_recommendation_text,
            "detected_location": detected_location,
            "excel_file": excel_file, "pdf_file": pdf_file,
        })
    finally:
        release_db_conn(conn)

    logger.info(f"Audit #{audit_id} complete. Excel={excel_file} PDF={pdf_file}")
    return {"audit_id": audit_id, "excel_file": excel_file, "pdf_file": pdf_file}

# ── Per-page AI Analysis ───────────────────────────────────────────────────────

def _analyze_pages(pages_200, base_url, audit_id, detected_location, run_pagespeed, ai_mode):
    total = len(pages_200)

    def _analyze_one(idx, pd):
        url = pd["url"]
        content = pd.get("_content","")
        if not content:
            content = f"{pd.get('current_title','')} {pd.get('current_meta_description','')}"

        try:
            with ThreadPoolExecutor(max_workers=3) as sub:
                ai_f    = sub.submit(ai_analysis, url, pd.get("current_title",""),
                                     pd.get("current_meta_description",""),
                                     pd.get("current_h1",""), content)
                mob_f   = sub.submit(_get_pagespeed, url, "mobile") if run_pagespeed else None
                desk_f  = sub.submit(_get_pagespeed, url, "desktop") if run_pagespeed else None

                ai = ai_f.result(timeout=120) or {}
                mobile  = mob_f.result(timeout=120) if mob_f else {"score":"N/A","lcp":"N/A","cls":"N/A","fcp":"N/A"}
                desktop = desk_f.result(timeout=120) if desk_f else {"score":"N/A","lcp":"N/A","cls":"N/A","fcp":"N/A"}

            pd.update({
                "primary_keyword": ai.get("primary_keyword",""),
                "secondary_keywords": ", ".join(ai.get("secondary_keywords",[])),
                "short_tail_keywords": ", ".join(ai.get("short_tail_keywords",[])),
                "long_tail_keywords": ", ".join(ai.get("long_tail_keywords",[])),
                "ai_meta_title": ai.get("meta_title",""),
                "ai_meta_description": ai.get("meta_description",""),
                "ai_h1": ai.get("h1",""),
                "ai_og_title": ai.get("og_title",""),
                "ai_og_description": ai.get("og_description",""),
                "ai_og_image_url": ai.get("og_image_url",""),
                "ai_schema_recommendation": ai.get("schema_type",""),
                "ai_schema_code_snippet": ai.get("schema_code_snippet",""),
                "ai_optimized_url": ai.get("optimized_url",""),
                "image_optimization_tips": ai.get("image_optimization_tips",""),
                "serp_preview": _serp_preview(url, ai.get("meta_title",""), ai.get("meta_description","")),
                "mobile_score": str(mobile.get("score","N/A")),
                "mobile_lcp": str(mobile.get("lcp","N/A")),
                "mobile_cls": str(mobile.get("cls","N/A")),
                "mobile_fcp": str(mobile.get("fcp","N/A")),
                "desktop_score": str(desktop.get("score","N/A")),
                "desktop_lcp": str(desktop.get("lcp","N/A")),
                "desktop_cls": str(desktop.get("cls","N/A")),
                "desktop_fcp": str(desktop.get("fcp","N/A")),
            })

            if idx < 50:
                faq = ai_aeo_faq(url, pd.get("current_title",""), pd.get("current_h1",""),
                                 content, pd.get("primary_keyword",""), detected_location)
                pd["aeo_faq"] = json.dumps(faq) if faq else ""
                pd["_aeo_faq_list"] = faq or []
            else:
                pd["aeo_faq"] = ""; pd["_aeo_faq_list"] = []

            pd["seo_score"] = _calculate_seo_score(pd)
            pd["seo_grade"]  = _seo_grade(pd["seo_score"])

        except Exception as e:
            logger.error(f"Analysis error {url}: {e}")
            pd["seo_score"] = _calculate_seo_score(pd)
            pd["seo_grade"]  = _seo_grade(pd["seo_score"])

        # Update DB
        conn = get_db_conn()
        try:
            db_update_page_ai(conn, audit_id, url, pd)
            db_mark_url_progress(conn, audit_id, url, "analyzed", str(pd.get("status","")))
        except Exception as e:
            logger.error(f"DB update error {url}: {e}")
        finally:
            release_db_conn(conn)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futs = {executor.submit(_analyze_one, i, p): p for i, p in enumerate(pages_200)}
        done = 0
        for f in as_completed(futs):
            done += 1
            try: f.result(timeout=300)
            except Exception as e: logger.error(f"Analyze future error: {e}")
            if done % 10 == 0: logger.info(f"Analyzed {done}/{total}")

# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_base_url(input_url: str) -> str:
    for url in [input_url]:
        try:
            r = _safe_get(url, timeout=12)
            if r.status_code < 500:
                return url
        except Exception:
            pass
    parsed = urlparse(input_url)
    host = parsed.netloc
    alt_host = host[4:] if host.startswith("www.") else "www." + host
    alt_url = f"{parsed.scheme}://{alt_host}{parsed.path}"
    try:
        r = _safe_get(alt_url, timeout=12)
        if r.status_code < 500:
            return alt_url
    except Exception:
        pass
    return input_url


def _check_gbp(base_url: str) -> str:
    try:
        r = _safe_get(base_url, timeout=15)
        if r.status_code == 200:
            markers = ["google.com/maps","maps.google.com","goo.gl/maps",
                       "business.google.com","LocalBusiness","schema.org/LocalBusiness"]
            if any(m in r.text for m in markers):
                return "Present"
    except Exception:
        pass
    return "Not Found"


def _detect_location(domain: str, pages: list, ai_mode: str) -> str:
    tld_map = {"uk":"United Kingdom","au":"Australia","ca":"Canada",
               "in":"India","de":"Germany","fr":"France","sg":"Singapore"}
    for tld, loc in tld_map.items():
        if domain.endswith(f".{tld}") or domain.endswith(f".co.{tld}"):
            return loc
    return "Global"


def _build_site_analysis(pages_list, sitemap_urls):
    data = []
    status_counts = {}
    for p in pages_list:
        st = str(p.get("status","Unknown"))
        key = st if st in ("200","301","302","404") else ("5xx" if st.startswith("5") else "Other")
        status_counts[key] = status_counts.get(key, 0) + 1
    for code, count in sorted(status_counts.items()):
        data.append({"type":"http_status","key":code,"value":f"{count} pages","count":count})
    return data


def _generate_seo_files(base_url, domain, pages_list, broken_links):
    files = []
    # sitemap.xml
    urls = [p["url"] for p in pages_list if _is_200(p)]
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for u in urls:
        xml += f'  <url><loc>{u}</loc></url>\n'
    xml += '</urlset>'
    files.append({"file_name":"sitemap.xml","file_type":"application/xml","file_content":xml,"file_size":len(xml.encode())})

    # robots.txt
    robots = f"User-agent: *\nAllow: /\n\nSitemap: {base_url.rstrip('/')}/sitemap.xml\n"
    files.append({"file_name":"robots.txt","file_type":"text/plain","file_content":robots,"file_size":len(robots.encode())})

    return files
