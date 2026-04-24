"""
brand_scraper.py — Multi-Platform Brand Intelligence Engine
────────────────────────────────────────────────────────────
Platforms : Reddit (Playwright) | Quora / Medium / Tumblr (Claude web_search)
Analysis  : 7 AI sections per platform (HTML format)
Storage   : PostgreSQL — same DB as SEO audit, linked by brand_id

All credentials loaded from environment variables (never hardcoded).
"""

import asyncio, json, logging, os, random, re, time
from datetime import datetime
from urllib.parse import quote_plus, unquote, urlparse, parse_qs

import requests as http_req

logger = logging.getLogger("brand_scraper")

# ── Config from env ────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_URL   = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL     = os.getenv("CLAUDE_SONNET_MODEL", "claude-sonnet-4-20250514")
OPENAI_MODEL        = "gpt-4o-mini"

# Which provider to use for analysis (scraping always uses Anthropic web_search)
AI_PROVIDER_ANALYSIS = os.getenv("SCRAPER_ANALYSIS_PROVIDER", "openai")  # "openai" | "anthropic"

HEADLESS    = os.getenv("SCRAPER_HEADLESS", "true").lower() != "false"
MAX_REDDIT  = int(os.getenv("SCRAPER_MAX_REDDIT", "50"))
DELAY       = (3, 6)
UA          = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"]

PLATFORMS = ["reddit", "quora", "medium", "tumblr"]

ANALYSIS_SECTIONS = {
    "reddit":  [("demand","Community Demand & Intent",""),
                ("visibility","Brand vs Competitor Visibility",""),
                ("scoring","Thread Opportunity Scoring",""),
                ("content","Content Strategy",""),
                ("answers","Answer Strategy",""),
                ("playbook","Competitor Playbook",""),
                ("trends","Trend Insights","")],
    "quora":   [("demand","Demand & Intent Analysis",""),
                ("visibility","Brand vs Competitor Visibility",""),
                ("scoring","Opportunity Scoring",""),
                ("content","Content Strategy",""),
                ("answers","Answer Strategy",""),
                ("playbook","Competitor Playbook",""),
                ("trends","Trend Insights","")],
    "medium":  [("demand","Content Demand & Intent",""),
                ("visibility","Brand vs Competitor Visibility",""),
                ("scoring","Article Opportunity Scoring",""),
                ("content","Content Strategy",""),
                ("answers","Publishing Strategy",""),
                ("playbook","Competitor Playbook",""),
                ("trends","Trend Insights","")],
    "tumblr":  [("demand","Niche Demand & Aesthetic Intent",""),
                ("visibility","Brand Presence in Communities",""),
                ("scoring","Post Opportunity Scoring",""),
                ("content","Tumblr Content Strategy",""),
                ("answers","Reblog & Engagement Strategy",""),
                ("playbook","Competitor Playbook",""),
                ("trends","Cultural & Trend Insights","")],
}


# ── Anthropic / OpenAI helpers ─────────────────────────────────────────────────

def _api_call(messages, tools=None, max_tokens=8000):
    """Anthropic API — used for scraping (web_search tool)."""
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set")
        return None
    body = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens, "messages": messages}
    if tools:
        body["tools"] = tools
    try:
        r = http_req.post(
            ANTHROPIC_API_URL,
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json=body, timeout=180
        )
        if r.status_code != 200:
            logger.error(f"Anthropic API HTTP {r.status_code}: {r.text[:300]}")
            return None
        return r.json()
    except Exception as e:
        logger.error(f"Anthropic API error: {e}")
        return None


def _openai_call(prompt, max_tokens=4096):
    """OpenAI — cheaper option for analysis generation."""
    if not OPENAI_API_KEY:
        return None
    try:
        import openai
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.7,
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.warning(f"OpenAI error: {e}")
        return None


def _analysis_call(prompt, max_tokens=4096):
    """Route analysis to cheapest available provider."""
    if AI_PROVIDER_ANALYSIS == "openai" and OPENAI_API_KEY:
        result = _openai_call(prompt, max_tokens)
        if result:
            return result
    # Fallback to Anthropic
    if ANTHROPIC_API_KEY:
        resp = _api_call([{"role": "user", "content": prompt}], max_tokens=max_tokens)
        if resp:
            return _get_text(resp)
    return None


def _get_text(data):
    if not data:
        return ""
    return "".join(b["text"] for b in data.get("content", []) if b.get("type") == "text")


def _extract_json(text):
    if not text:
        return []
    try:
        m = re.search(r"\[[\s\S]*\]", text)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    return []


def _api_search(prompt, name, fields_hint):
    """Call Claude web_search and extract JSON array from response."""
    logger.info(f"  Claude web_search → {name}")
    data = _api_call(
        [{"role": "user", "content": prompt}],
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        max_tokens=8000,
    )
    if not data:
        logger.error(f"  API returned None for {name}")
        return []

    full = ""
    for blk in data.get("content", []):
        if blk.get("type") == "text":
            full += blk.get("text", "")

    if not full:
        logger.warning(f"  No text in response for {name}")
        return []

    logger.info(f"  Got {len(full)} chars")
    items = _extract_json(full)
    if items:
        logger.info(f"  ✓ {len(items)} items extracted")
        return items

    # Retry extraction
    logger.info(f"  No JSON found — retrying extraction...")
    retry = _api_call(
        [{"role": "user", "content":
          f"Extract ALL items from the text below as a JSON array.\n"
          f"Required fields: {fields_hint}\n\nText:\n{full[:6000]}\n\n"
          f"Return ONLY a valid JSON array. No markdown, no explanation."}],
        max_tokens=4000,
    )
    if retry:
        items = _extract_json(_get_text(retry))
        if items:
            logger.info(f"  ✓ Retry got {len(items)} items")
            return items

    logger.warning(f"  ⚠ No items extracted for {name}")
    return []


# ── Scrapers ───────────────────────────────────────────────────────────────────

def scrape_quora(brand, kws, comps, location=""):
    logger.info("═══ QUORA ═══")
    loc_tag = f" {location}" if location else ""
    queries = (
        [f"site:quora.com {brand} {k}{loc_tag}" for k in kws] +
        [f"site:quora.com {brand} vs {c}{loc_tag}" for c in comps[:3]] +
        [f"site:quora.com {brand} review{loc_tag}"]
    )
    if location:
        queries += [f"site:quora.com {brand} {location}",
                    f"site:quora.com best {kws[0]} {location}"]
    loc_inst = f"\nFocus on questions relevant to the {location} market." if location else ""
    prompt = (
        f'Search for REAL Quora questions about "{brand}" using:\n'
        f"{json.dumps(queries[:10], indent=2)}\n{loc_inst}\n"
        "Return JSON array with: url, question, answer_count, snippet, topics, "
        "brands_mentioned, user_sentiment, funnel_stage.\n"
        "Only real https://www.quora.com/ URLs. Find 15-25 questions."
    )
    items = _api_search(prompt, "quora",
                        "url,question,answer_count,snippet,topics,brands_mentioned,user_sentiment,funnel_stage")
    results = []
    seen = set()
    for q in items:
        url = (q.get("url") or "").split("?")[0].rstrip("/")
        if not url.startswith("https://www.quora.com/") or url in seen:
            continue
        seen.add(url)
        try:
            ac = int(q.get("answer_count", 1))
        except Exception:
            ac = 1
        results.append({
            "url": url, "platform": "quora",
            "title": re.sub(r"\s*[-–—|]\s*Quora\s*$", "",
                            q.get("question", ""), flags=re.I).strip()[:500],
            "subreddit": ", ".join(q.get("topics", [])[:3])
                         if isinstance(q.get("topics"), list) else "",
            "author": "", "comment_count": max(0, ac), "claps": 0,
            "content": (q.get("snippet") or "")[:1000],
            "brands_raw": q.get("brands_mentioned", [])
                          if isinstance(q.get("brands_mentioned"), list) else [],
            "sentiment_raw": q.get("user_sentiment", "neutral"),
        })
    logger.info(f"  ✅ QUORA: {len(results)} questions")
    return results


def scrape_medium(brand, kws, comps, location=""):
    logger.info("═══ MEDIUM ═══")
    loc_tag = f" {location}" if location else ""
    queries = (
        [f"site:medium.com {brand} {k}{loc_tag}" for k in kws] +
        [f"site:medium.com {brand} vs {c}" for c in comps[:3]] +
        [f"site:medium.com {brand} review{loc_tag}"]
    )
    if location:
        queries.append(f"site:medium.com {brand} {location}")
    loc_inst = f"\nFocus on articles relevant to the {location} market." if location else ""
    prompt = (
        f'Search for REAL Medium articles about "{brand}" using:\n'
        f"{json.dumps(queries[:10], indent=2)}\n{loc_inst}\n"
        "Return JSON array with: url, title, author, publication, reading_time, "
        "claps_estimate, snippet, topics, brands_mentioned, content_type, "
        "sentiment_toward_brand, funnel_stage.\n"
        "Only real article URLs (NOT policy/help/about). Find 15-25 articles."
    )
    items = _api_search(prompt, "medium",
                        "url,title,author,publication,reading_time,claps_estimate,"
                        "snippet,brands_mentioned,sentiment_toward_brand")
    results = []
    seen = set()
    bl = ["policy.medium", "help.medium", "about.medium",
          "/tag/", "/search", "/me/", "/plans"]
    for a in items:
        url = (a.get("url") or "").split("?")[0].rstrip("/")
        if "medium.com" not in url or not url.startswith("http") or url in seen:
            continue
        if any(x in url.lower() for x in bl):
            continue
        seen.add(url)
        try:
            rt = int(a.get("reading_time", 5))
        except Exception:
            rt = 5
        results.append({
            "url": url, "platform": "medium",
            "title": a.get("title", "")[:500],
            "subreddit": (a.get("publication") or "Personal")[:128],
            "author": (a.get("author") or "Unknown")[:255],
            "comment_count": max(0, rt),
            "claps": {"high": 500, "medium": 100, "low": 20}.get(
                a.get("claps_estimate", "medium"), 100),
            "content": (a.get("snippet") or "")[:1000],
            "brands_raw": a.get("brands_mentioned", [])
                          if isinstance(a.get("brands_mentioned"), list) else [],
            "sentiment_raw": a.get("sentiment_toward_brand", "neutral"),
        })
    logger.info(f"  ✅ MEDIUM: {len(results)} articles")
    return results


def scrape_tumblr(brand, kws, comps, location=""):
    logger.info("═══ TUMBLR ═══")
    loc_tag = f" {location}" if location else ""
    queries = (
        [f"site:tumblr.com {brand} {k}{loc_tag}" for k in kws] +
        [f"site:tumblr.com {brand} vs {c}" for c in comps[:3]] +
        [f"site:tumblr.com {brand}{loc_tag}"]
    )
    if location:
        queries.append(f"site:tumblr.com {kws[0]} {location}")
    loc_inst = (f"\nFocus on posts relevant to the {location} market/community."
                if location else "")
    prompt = (
        f'Search for REAL Tumblr posts about "{brand}" using:\n'
        f"{json.dumps(queries[:10], indent=2)}\n{loc_inst}\n"
        "Return JSON array with: url, title, blog_name, notes_estimate, post_type, "
        "tags, snippet, topics, brands_mentioned, sentiment_toward_brand, funnel_stage.\n"
        "Only real tumblr.com URLs. Find 15-25 posts."
    )
    items = _api_search(prompt, "tumblr",
                        "url,title,blog_name,notes_estimate,snippet,"
                        "brands_mentioned,sentiment_toward_brand")
    results = []
    seen = set()
    for t in items:
        url = (t.get("url") or "").split("?")[0].rstrip("/")
        if "tumblr.com" not in url or not url.startswith("http") or url in seen:
            continue
        if any(x in url for x in ["/search", "/explore", "/dashboard", "/login"]):
            continue
        seen.add(url)
        blog = t.get("blog_name", "") or "Unknown"
        results.append({
            "url": url, "platform": "tumblr",
            "title": t.get("title", "")[:500],
            "subreddit": blog[:128], "author": blog[:255],
            "comment_count": {"viral": 500, "high": 100, "medium": 30, "low": 5}.get(
                t.get("notes_estimate", "medium"), 30),
            "claps": 0,
            "content": (t.get("snippet") or "")[:1000],
            "brands_raw": t.get("brands_mentioned", [])
                          if isinstance(t.get("brands_mentioned"), list) else [],
            "sentiment_raw": t.get("sentiment_toward_brand", "neutral"),
        })
    logger.info(f"  ✅ TUMBLR: {len(results)} posts")
    return results


# ── Reddit via Playwright ──────────────────────────────────────────────────────

REDDIT_RE = re.compile(
    r"https?://(?:(?:www|old)\.)?reddit\.com/r/\w+/comments/\w+"
)


def _val_reddit(raw):
    if "duckduckgo.com/l/" in raw:
        p = parse_qs(urlparse(raw).query)
        if "uddg" in p:
            raw = unquote(p["uddg"][0])
    m = REDDIT_RE.search(raw)
    return (m.group(0)
            .replace("old.reddit.com", "www.reddit.com")
            .split("?")[0].rstrip("/")) if m else None


def _parse_rjson(raw, url):
    try:
        dl = json.loads(raw)
        if not isinstance(dl, list):
            return None
        p = dl[0]["data"]["children"][0]["data"]
        r = {
            "url": url, "platform": "reddit",
            "title": p.get("title", ""),
            "subreddit": p.get("subreddit", ""),
            "author": p.get("author", ""),
            "comment_count": p.get("num_comments", 0),
            "content": p.get("selftext", ""),
        }
        if len(dl) > 1:
            cs = []
            def w(ch, d=0):
                if d > 3:
                    return
                for c in ch:
                    if c.get("kind") == "t1":
                        b = c["data"].get("body", "")
                        if len(b) > 15:
                            cs.append(b)
                        rp = c["data"].get("replies")
                        if isinstance(rp, dict):
                            w(rp.get("data", {}).get("children", []), d + 1)
            w(dl[1]["data"]["children"])
            if cs:
                r["content"] += "\n---\n" + "\n---\n".join(cs[:40])
        return r
    except Exception:
        return None


async def _reddit_pipeline(page, queries, brand="", comps=None, kws=None, location=""):
    if comps is None:
        comps = []
    if kws is None:
        kws = []
    logger.info("═══ REDDIT ═══")
    # Build relevance terms
    rel = {brand.lower()} | {c.strip().lower() for c in comps} | {
        w for k in kws for w in k.strip().lower().split() if len(w) > 3
    }
    if location:
        rel |= {w for w in location.lower().split() if len(w) > 2}
    rel.discard("")

    urls = set()
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("beautifulsoup4 not installed — run: pip install beautifulsoup4")
        return []

    for kw in queries:
        if len(urls) >= MAX_REDDIT:
            break
        try:
            await page.goto(
                f"https://old.reddit.com/search?q={quote_plus(kw)}&sort=relevance&t=all",
                timeout=18000, wait_until="domcontentloaded"
            )
            await asyncio.sleep(random.uniform(2, 4))
            for a in BeautifulSoup(await page.content(), "html.parser").find_all("a", href=True):
                href = a["href"]
                if "/comments/" in href:
                    if not href.startswith("http"):
                        href = "https://old.reddit.com" + href
                    n = _val_reddit(href)
                    if n:
                        urls.add(n)
        except Exception:
            pass
        await asyncio.sleep(random.uniform(*DELAY))

    logger.info(f"  Found {len(urls)} Reddit URLs")
    results = []
    skipped = 0
    for i, url in enumerate(list(urls)[:MAX_REDDIT]):
        logger.info(f"  [{i+1}/{min(len(urls), MAX_REDDIT)}] {url[:70]}")
        d = None
        try:
            jurl = url.rstrip("/") + ".json"
            jurl = jurl.replace("www.reddit.com", "old.reddit.com")
            await page.goto(jurl, timeout=15000, wait_until="domcontentloaded")
            await asyncio.sleep(1.5)
            pre = BeautifulSoup(await page.content(), "html.parser").find("pre")
            if pre:
                d = _parse_rjson(pre.get_text(), url)
        except Exception:
            pass
        await asyncio.sleep(random.uniform(2, 4))

        if not d or len(d.get("content", "")) < 50:
            continue

        check_text = f"{d.get('title', '')} {d.get('content', '')[:500]}".lower()
        if not any(term in check_text for term in rel):
            skipped += 1
            continue

        # Filter comments to relevant ones only
        if "\n---\n" in d.get("content", ""):
            parts = d["content"].split("\n---\n")
            filtered = [parts[0]] if parts else []
            for comment in parts[1:]:
                if any(t in comment.lower() for t in rel):
                    filtered.append(comment)
            d["content"] = "\n---\n".join(filtered[:16])

        results.append(d)
        logger.info(f"    ✓ {d['title'][:50]}")

    logger.info(f"  ✅ REDDIT: {len(results)} threads (skipped {skipped} irrelevant)")
    return results


# ── NLP processing ─────────────────────────────────────────────────────────────

POS = {"love","great","awesome","amazing","excellent","best","fantastic","perfect",
       "wonderful","outstanding","impressive","helpful","solid","reliable","recommend",
       "worth","happy","enjoy","superior","favorite","brilliant","superb","useful"}
NEG = {"hate","terrible","awful","worst","bad","horrible","poor","useless","garbage",
       "trash","broken","disappointing","frustrating","expensive","overpriced","scam",
       "avoid","sucks","annoying","buggy","slow","crash","unreliable","waste"}
REC_KW = ["i recommend","you should try","check out","best option","go with",
           "switched to","works great","highly recommend"]
CMP_KW = [" vs "," versus ","compared to","better than","worse than",
           "alternative to","switch from","instead of"]
COM_KW = ["doesn't work","stopped working","wasted money","customer support",
          "refund","getting worse","rip off"]


def _process(t, brand, comps):
    text = f"{t.get('title', '')} {t.get('content', '')}"
    tl = text.lower()
    t["has_brand"] = brand.lower() in tl
    br = t.get("brands_raw", [])
    cf = [c for c in comps if c.strip().lower() in tl]
    for b in br:
        if b.strip().lower() != brand.lower() and b not in cf:
            cf.append(b)
    t["has_competitor"] = len(cf) > 0
    t["competitors_found"] = cf
    sr = t.get("sentiment_raw", "")
    if sr in ("positive", "negative", "neutral"):
        t["sentiment"] = sr
    elif sr in ("mixed", "not_mentioned"):
        t["sentiment"] = "neutral"
    else:
        w = set(re.findall(r"\b[a-z]+\b", tl))
        p, n = len(w & POS), len(w & NEG)
        t["sentiment"] = "positive" if p > n else ("negative" if n > p else "neutral")
    s = {
        "complaint":      sum(1 for x in COM_KW if x in tl),
        "recommendation": sum(1 for x in REC_KW if x in tl),
        "comparison":     sum(1 for x in CMP_KW if x in tl),
    }
    best = max(s, key=s.get)
    t["intent"] = best if s[best] > 0 else "general"
    t["brand_name"] = brand
    sc = 0
    if t["has_brand"]:       sc += 50
    if t["has_competitor"]:  sc += 25
    if t["intent"] in ("recommendation", "comparison"): sc += 30
    elif t["intent"] == "complaint":                    sc += 20
    sc += min(t.get("comment_count", 0), 50)
    sc += min(t.get("claps", 0) // 10, 30)
    if t["sentiment"] == "negative" and t["has_brand"]: sc += 15
    t["score"] = sc
    return t


def _mk_drafts(t, brand):
    i = t.get("intent", "general")
    if i == "complaint":
        return ("That's frustrating. Have you tried their support?",
                "Had same issue. Different approach worked.",
                f"Switched to {brand}, smooth so far.")
    if i == "recommendation":
        return ("Depends on needs. What's your use case?",
                "Tried several. Happy to share.",
                f"Good results with {brand}.")
    if i == "comparison":
        return ("Both have tradeoffs. Try each.",
                "Tested both. Depends on priorities.",
                f"Chose {brand} after comparing.")
    return ("Good discussion.", "Test a few options.", f"Using {brand}, handles this well.")


def _mk_insight(t, brand):
    i  = t.get("intent")
    hb = t.get("has_brand")
    hc = t.get("has_competitor")
    pl = t.get("platform", "").title()
    if i == "complaint" and hb:
        return {"w": f"Negative {pl} thread.", "o": "defense",  "a": "Address publicly."}
    if i == "comparison":
        return {"w": f"Decision-stage {pl} thread.", "o": "lead", "a": "Share comparison."}
    if i == "recommendation" and not hb:
        return {"w": f"Rec on {pl}, brand absent.", "o": "awareness", "a": "Join with helpful reply."}
    if hc and not hb:
        return {"w": f"Competitor on {pl}, no brand.", "o": "awareness", "a": "Engage."}
    return {"w": f"Relevant {pl} discussion.", "o": "general", "a": "Monitor."}


# ── AI analysis (7 sections per platform) ─────────────────────────────────────

def _build_analysis_prompt(platform, section_id, brand, comps, data_summary, location=""):
    loc_ctx = f"\nTarget Market/Location: {location}" if location else "\nTarget Market: Global"
    loc_hint = f" in {location}" if location else ""
    loc_label = f" the {location} market" if location else " this category"

    base = (
        f"You are a senior brand strategist analyzing REAL {platform.title()} data for {brand}.\n"
        f"Competitors: {', '.join(comps)}{loc_ctx}\n\n"
        f"Here is the REAL scraped data summary:\n{data_summary}\n\n"
        "IMPORTANT: Format your response as clean HTML for an accordion panel.\n"
        "Use: <h2> headers, <h3> subsections, <table> for data, "
        "<div class=\"li\"> for bullets, <strong> for emphasis.\n"
        "Do NOT use markdown. Use HTML only. Be specific and reference real data.\n"
    )
    if location:
        base += f"Tailor ALL analysis specifically for the {location} market.\n\n"

    sections = {
        "demand": (
            f"Analyze DEMAND & INTENT from the real {platform} data{loc_hint}:\n"
            f"1. Ranked table of top posts by engagement (Title, Engagement, Sentiment, Funnel Stage)\n"
            f"2. Demand distribution across funnel stages with percentages\n"
            f"3. Topic cluster analysis\n"
            f"4. Intent signals — what users{loc_hint} are actually looking for\n"
            f"5. Top 5 underserved content gaps where {brand} can dominate{loc_hint}"
        ),
        "visibility": (
            f"Analyze BRAND VS COMPETITOR VISIBILITY on {platform}{loc_hint}:\n"
            f"1. Scorecard table: Brand | Mentions | Sentiment | Presence Score\n"
            f"2. Where {brand} appears vs where competitors appear{loc_hint}\n"
            f"3. Threads where {brand} is MISSING but competitors are present\n"
            f"4. Real user quotes about each brand\n"
            f"5. Visibility gaps and opportunities"
        ),
        "scoring": (
            f"Create OPPORTUNITY SCORING for each scraped {platform} item{loc_hint}:\n"
            f"1. Score each: Relevance (1-10) | Competition | Sentiment Gap | Overall | Tier (GOLD/SILVER/BRONZE)\n"
            f"2. Ranked table with all items\n"
            f"3. For TOP 5 GOLD opportunities: specific response strategies\n"
            f"4. Include actual URL and recommended action per GOLD item\n"
            f"5. Key insights summary"
        ),
        "content": (
            f"Create CONTENT STRATEGY based on real {platform} data{loc_hint}:\n"
            f"A. 15 content ideas (Title | Source Post | Target Keyword | Format | Priority)\n"
            f"B. Topic clusters (5 pillars from real patterns){loc_hint}\n"
            f"C. Long-tail keywords from actual post wordings\n"
            f"D. Content addressing sentiment issues found\n"
            f"E. 3-month content calendar for{loc_label}"
        ),
        "answers": (
            f"Create ENGAGEMENT STRATEGY for {platform}{loc_hint}:\n"
            f"A. Top 10 posts to respond to THIS WEEK (with URLs)\n"
            f"For each: URL | Opening Hook | Key Points | {brand} Positioning | Word Count\n"
            f"B. Rebuttal frameworks for negative sentiments found\n"
            f"C. Response templates by post type{loc_hint}\n"
            f"D. Authority building strategy{loc_hint}\n"
            f"E. Promotion vs information balance guide (90/10 rule)"
        ),
        "playbook": (
            f"Create COMPETITOR PLAYBOOK from real {platform} data{loc_hint}:\n"
            f"A. Competitor frequency in real discussions (ranked with mention counts)\n"
            f"B. What real users{loc_hint} say about each competitor\n"
            f"C. Market gaps — unanswered posts where {brand} can win{loc_hint}\n"
            f"D. Counter-positioning strategy using real user language\n"
            f"E. 7-day quick-win action plan with specific post URLs"
        ),
        "trends": (
            f"Analyze TRENDS from real {platform} data{loc_hint}:\n"
            f"A. Topics users{loc_hint} are ACTUALLY asking about (ranked by frequency)\n"
            f"B. Common user concerns from real snippets\n"
            f"C. Rising topics table: Topic | Evidence | Business Implication\n"
            f"D. Audience segments{loc_hint} — personas with percentages\n"
            f"E. 6-month predictions for{loc_label} based on real patterns"
        ),
    }
    return base + "\n" + sections.get(
        section_id, "Provide comprehensive analysis with tables and actionable insights."
    )


def _run_analysis(brand_id, brand, comps, platform, threads, location="", conn=None):
    """Generate 7 AI analysis sections and save to DB."""
    from db import db_insert_brand_analysis
    loc_label = f" ({location})" if location else ""
    logger.info(f"\n  🤖 AI analysis — {platform.upper()}{loc_label}")

    summary_items = []
    for t in threads[:25]:
        comp_text = ",".join(t.get("competitors_found", []))
        summary_items.append(
            f"- [{t.get('sentiment', 'neutral')}] \"{t.get('title', '')[:100]}\" "
            f"(engagement:{t.get('comment_count', 0)}, brands:{comp_text}, "
            f"intent:{t.get('intent', 'general')}, url:{t.get('url', '')})"
        )
    data_summary = "\n".join(summary_items)

    for i, (sid, title, icon) in enumerate(ANALYSIS_SECTIONS.get(platform, [])):
        logger.info(f"    [{i+1}/7] {icon} {title}")
        prompt = _build_analysis_prompt(platform, sid, brand, comps, data_summary, location)
        content = "<p>Analysis generation failed. Please retry.</p>"
        result = _analysis_call(prompt, max_tokens=4096)
        if result:
            content = result.replace("```html", "").replace("```", "")
        if conn:
            db_insert_brand_analysis(conn, brand_id, platform, sid, title, icon, i, content, brand)
        logger.info(f"       ✅ {len(content):,} chars saved")
        time.sleep(1)


# ── Main pipeline (called by API) ─────────────────────────────────────────────

async def run_scraper(brand_id: int, brand: str, competitors: list, keywords: list,
                      location: str = "", platforms: list = None) -> dict:
    """
    Full pipeline entry point — called by the FastAPI background task.
    Returns summary dict with counts per platform.
    """
    from db import (get_db_conn, release_db_conn,
                    db_insert_brand_thread, db_insert_brand_mention,
                    db_insert_drafts, db_insert_brand_insight,
                    db_insert_brand_analysis)

    if platforms is None:
        platforms = list(PLATFORMS)

    loc_tag = f" {location}" if location else ""
    logger.info(f"Brand scraper start: brand_id={brand_id} brand={brand} "
                f"platforms={platforms} location={location or 'Global'}")

    # ── Build Reddit queries ───────────────────────────────────────────────────
    rq = []
    for k in keywords:
        rq += [f"{brand} {k}{loc_tag}", f"{brand} {k} review{loc_tag}",
               f"{brand} {k} reddit{loc_tag}"]
    for c in competitors:
        rq += [f"{brand} vs {c}{loc_tag}", f"{brand} or {c}"]
    for c in competitors:
        for k in keywords[:3]:
            rq.append(f"{c} {k}{loc_tag}")
    rq += [f"{brand} review{loc_tag}", f"{brand} experience{loc_tag}",
           f"{brand} recommendation{loc_tag}", f"{brand} problems{loc_tag}"]
    if location:
        rq.append(f"{brand} {location}")
        for k in keywords[:3]:
            rq.append(f"{k} {location}")
    # Dedup
    seen_q = set()
    rq_dedup = []
    for q in rq:
        ql = q.lower().strip()
        if ql not in seen_q:
            seen_q.add(ql)
            rq_dedup.append(q)
    rq = rq_dedup[:40]

    # ── Scrape platforms ───────────────────────────────────────────────────────
    all_threads = []
    plat_threads = {}

    if "reddit" in platforms:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                br = await p.chromium.launch(headless=HEADLESS)
                ctx = await br.new_context(
                    user_agent=random.choice(UA),
                    viewport={"width": 1280, "height": 720},
                    locale="en-US"
                )
                await ctx.route(
                    "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
                    lambda r: r.abort()
                )
                pg = await ctx.new_page()
                rt = await _reddit_pipeline(pg, rq, brand=brand,
                                            comps=competitors, kws=keywords,
                                            location=location)
                all_threads.extend(rt)
                plat_threads["reddit"] = rt
                await br.close()
        except ImportError:
            logger.error("Playwright not installed — Reddit scraping skipped. "
                         "Run: pip install playwright && playwright install chromium")
        except Exception as e:
            logger.error(f"Reddit scraper error: {e}")

    if "quora" in platforms:
        qt = scrape_quora(brand, keywords, competitors, location)
        all_threads.extend(qt)
        plat_threads["quora"] = qt

    if "medium" in platforms:
        mt = scrape_medium(brand, keywords, competitors, location)
        all_threads.extend(mt)
        plat_threads["medium"] = mt

    if "tumblr" in platforms:
        tt = scrape_tumblr(brand, keywords, competitors, location)
        all_threads.extend(tt)
        plat_threads["tumblr"] = tt

    if not all_threads:
        logger.warning("No threads scraped from any platform.")
        return {"status": "no_data", "total": 0, "by_platform": {}}

    # ── Process & score ────────────────────────────────────────────────────────
    logger.info(f"Processing {len(all_threads)} threads...")
    for t in all_threads:
        _process(t, brand, competitors)
    all_threads.sort(key=lambda x: x["score"], reverse=True)

    # ── Store to PostgreSQL ────────────────────────────────────────────────────
    conn = get_db_conn()
    stored = 0
    try:
        for t in all_threads:
            tid = db_insert_brand_thread(conn, brand_id, t)
            if not tid:
                continue
            stored += 1
            db_insert_brand_mention(conn, tid,
                                    brand if t.get("has_brand") else "",
                                    t.get("competitors_found", []))
            d1, d2, d3 = _mk_drafts(t, brand)
            db_insert_drafts(conn, tid, d1, d2, d3)
            ins = _mk_insight(t, brand)
            ot = ins["o"]
            if ot not in ("lead", "awareness", "defense", "general"):
                ot = "general"
            db_insert_brand_insight(conn, tid, ot, ins["w"], ins["a"], ins["a"])

        logger.info(f"Stored {stored}/{len(all_threads)} threads to DB")

        # ── AI analysis per platform ───────────────────────────────────────────
        for plat in platforms:
            pt = plat_threads.get(plat, [])
            if not pt:
                logger.info(f"  ⚠ No scraped data for {plat.upper()} — "
                            f"generating analysis from brand context only")
                loc_ctx = f", Location: {location}" if location else ""
                pt = [{
                    "title": f"{brand} discussion",
                    "content": (f"Brand: {brand}, Keywords: {', '.join(keywords)}, "
                                f"Competitors: {', '.join(competitors)}{loc_ctx}"),
                    "sentiment": "neutral", "intent": "general",
                    "comment_count": 0, "competitors_found": competitors,
                    "url": "", "platform": plat,
                }]
            _run_analysis(brand_id, brand, competitors, plat, pt, location, conn)

    finally:
        release_db_conn(conn)

    # ── Summary ───────────────────────────────────────────────────────────────
    by_plat = {}
    for t in all_threads:
        pl = t["platform"]
        by_plat.setdefault(pl, {"total": 0, "brand": 0, "competitor": 0})
        by_plat[pl]["total"] += 1
        if t.get("has_brand"):
            by_plat[pl]["brand"] += 1
        if t.get("has_competitor"):
            by_plat[pl]["competitor"] += 1

    logger.info(f"Brand scraper complete — {stored} stored, "
                f"{7 * len(platforms)} analysis sections generated")
    return {
        "status":      "complete",
        "total":       len(all_threads),
        "stored":      stored,
        "by_platform": by_plat,
        "analysis_sections": 7 * len(platforms),
    }
