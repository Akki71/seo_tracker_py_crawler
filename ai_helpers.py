"""
ai_helpers.py — All AI provider calls.
Compatible with: openai==1.59.0 + anthropic==0.40.0 + httpx==0.28.1
"""

import os, json, re, logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── State ──────────────────────────────────────────────────────────────────────
openai_client    = None
anthropic_client = None
_ai_mode         = "4"

OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

CLAUDE_HAIKU  = os.environ.get("CLAUDE_HAIKU_MODEL",  "claude-haiku-4-5-20251001")
CLAUDE_SONNET = os.environ.get("CLAUDE_SONNET_MODEL", "claude-sonnet-4-20250514")
GPT4O_MINI    = "gpt-4o-mini"


def setup_ai_clients(mode: str):
    global openai_client, anthropic_client, _ai_mode, OPENAI_API_KEY, ANTHROPIC_API_KEY
    _ai_mode = mode
    OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", OPENAI_API_KEY)
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
    if mode == "4":
        logger.info("AI mode=4: skipping AI"); return
    if mode in ("1","3") and OPENAI_API_KEY:
        try:
            from openai import OpenAI
            openai_client = OpenAI(api_key=OPENAI_API_KEY)
            logger.info("OpenAI client ready")
        except Exception as e: logger.error(f"OpenAI init: {e}")
    if mode in ("2","3") and ANTHROPIC_API_KEY:
        try:
            import anthropic as _ant
            anthropic_client = _ant.Anthropic(api_key=ANTHROPIC_API_KEY)
            logger.info("Anthropic client ready")
        except Exception as e: logger.error(f"Anthropic init: {e}")
    if not openai_client and not anthropic_client:
        logger.warning(f"AI mode={mode} but NO clients initialized. pip install openai==1.59.0 anthropic==0.40.0 httpx==0.28.1")


def _has_client() -> bool:
    return bool(openai_client or anthropic_client)


def ai_chat(prompt: str, max_tokens: int = 1024, temperature: float = 0.3,
            use_sonnet: bool = False) -> str:
    if not _has_client(): return ""
    try:
        if _ai_mode == "1":
            if not openai_client: return ""
            r = openai_client.chat.completions.create(
                model=GPT4O_MINI, messages=[{"role":"user","content":prompt}],
                temperature=temperature, max_tokens=max_tokens)
            return r.choices[0].message.content or ""
        elif _ai_mode == "2":
            if not anthropic_client: return ""
            r = anthropic_client.messages.create(
                model=CLAUDE_SONNET if use_sonnet else CLAUDE_HAIKU, max_tokens=max_tokens,
                messages=[{"role":"user","content":prompt}], temperature=temperature)
            return r.content[0].text if r.content else ""
        elif _ai_mode == "3":
            if use_sonnet and anthropic_client:
                try:
                    r = anthropic_client.messages.create(
                        model=CLAUDE_SONNET, max_tokens=max_tokens,
                        messages=[{"role":"user","content":prompt}], temperature=temperature)
                    return r.content[0].text if r.content else ""
                except Exception as ant_err:
                    logger.warning(f"Anthropic unavailable: {str(ant_err)[:100]} — fallback to OpenAI")
                    if openai_client:
                        r = openai_client.chat.completions.create(
                            model=GPT4O_MINI, messages=[{"role":"user","content":prompt}],
                            temperature=temperature, max_tokens=max_tokens)
                        return r.choices[0].message.content or ""
                    return ""
            elif openai_client:
                r = openai_client.chat.completions.create(
                    model=GPT4O_MINI, messages=[{"role":"user","content":prompt}],
                    temperature=temperature, max_tokens=max_tokens)
                return r.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"ai_chat error (mode={_ai_mode}): {e}")
    return ""


# ── JSON helpers ───────────────────────────────────────────────────────────────

def _repair_json(s: str) -> str:
    s = re.sub(r'^```(?:json)?\s*\n?', '', s.strip())
    s = re.sub(r'\n?```\s*$', '', s).strip()
    s = re.sub(r',\s*([}\]])', r'\1', s)
    try: return json.dumps(json.loads(s))
    except Exception: pass
    ob = s.count("{") - s.count("}")
    ab = s.count("[") - s.count("]")
    s += "]" * max(0, ab) + "}" * max(0, ob)
    s = re.sub(r',\s*([}\]])', r'\1', s)
    return s

def _parse_obj(raw: str) -> dict:
    i = raw.find("{"); j = raw.rfind("}") + 1
    if i >= 0 and j > i:
        for attempt in [raw[i:j], _repair_json(raw[i:j])]:
            try: return json.loads(attempt)
            except Exception: pass
    return {}

def _parse_arr(raw: str) -> list:
    i = raw.find("["); j = raw.rfind("]") + 1
    if i >= 0 and j > i:
        for attempt in [raw[i:j], _repair_json(raw[i:j])]:
            try: return json.loads(attempt)
            except Exception: pass
    return []


# ── Per-page AI ────────────────────────────────────────────────────────────────

def ai_analysis(url, title, meta_desc, h1, content) -> dict:
    if not _has_client(): return {}
    title_status = "MISSING — you MUST create one" if not title.strip() else f"({len(title)} chars): {title}"
    desc_status  = "MISSING — you MUST create one" if not meta_desc.strip() else f"({len(meta_desc)} chars): {meta_desc[:200]}"
    h1_status    = "MISSING — you MUST create one" if not h1.strip() else f": {h1}"
    prompt = f"""You are an expert SEO analyst. Analyze this page and generate optimized metadata.

STRICT RULES — FOLLOW EXACTLY:
1. meta_title: MUST be 30-60 characters. MUST be DIFFERENT and BETTER than current. Include primary keyword near the start. If current is missing or too short, CREATE a compelling one based on the page content and URL.
2. meta_description: MUST be 120-155 characters. MUST be DIFFERENT from current. Include primary keyword. Add call-to-action like "Learn more", "Discover", "Get started". If current is missing, CREATE one.
3. h1: MUST be DIFFERENT from current H1 and from title. Include primary keyword. Make it user-friendly and descriptive. If missing, CREATE one based on page content.
4. og_title: 40-60 characters. Engaging for social media sharing. Can differ from meta_title.
5. og_description: 100-150 characters. Compelling social description with emoji-free CTA.
6. NEVER repeat the current title/description/H1 verbatim — always IMPROVE them.
7. If page content is thin or empty, infer the page purpose from the URL path and create appropriate metadata.

Return ONLY valid JSON (no markdown, no backticks):
{{"primary_keyword":"main keyword for this page","secondary_keywords":["kw1","kw2","kw3"],"short_tail_keywords":["1-2 word keyword","another"],"long_tail_keywords":["4-8 word specific phrase","another phrase"],"meta_title":"NEW title 30-60 chars with keyword","meta_description":"NEW description 120-155 chars with keyword and CTA","h1":"NEW H1 different from title","og_title":"Social title 40-60 chars","og_description":"Social description 100-150 chars","og_image_url":"recommended OG image description","schema_type":"Schema.org type","schema_code_snippet":"JSON-LD example max 200 chars","optimized_url":"seo-friendly-slug","image_optimization_tips":"image SEO tips"}}

CURRENT PAGE DATA:
URL: {url}
Current Title {title_status}
Current Description {desc_status}
Current H1 {h1_status}
Content: {content[:1500]}
"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=800, temperature=0.3))
    except Exception as e: logger.error(f"ai_analysis {url}: {e}"); return {}


def ai_aeo_faq(url, title, h1, content, primary_keyword, location) -> list:
    if not _has_client(): return []
    prompt = f"""Generate 5 FAQ Q&A pairs for AEO (Answer Engine Optimization) for this page. Optimize for Google AI Overviews and ChatGPT.
Use natural question types (What/How/Why). Answers: 2-3 sentences, direct. Include primary keyword in 2 questions. Be page-specific.

Return ONLY JSON array (no markdown): [{{"question":"...","answer":"..."}}]

URL: {url} | Title: {title} | H1: {h1} | Keyword: {primary_keyword} | Location: {location}
Content: {content[:1000]}
"""
    try: return _parse_arr(ai_chat(prompt, max_tokens=800, temperature=0.3))
    except Exception as e: logger.error(f"ai_aeo_faq {url}: {e}"); return []


def ai_body_copy_guidance(url, title, h1, content, keyword, word_count, location) -> dict:
    if not _has_client(): return {}
    is_thin = int(word_count) < 300 if word_count else True
    thin_request = ""
    thin_field   = ""
    if is_thin:
        thin_request = f"""
CRITICAL: This page has only {word_count} words — it is THIN CONTENT. Generate 300-500 words of ORIGINAL body copy.

CONTENT RULES:
- Write with different sentence structures, natural tone, and improved readability.
- Ensure content is highly unique and not traceable to any existing content.
- Match the website's professional tonality.
- Include the primary keyword "{keyword}" naturally 2-3 times.
- Be specific and actionable — not vague filler text.
- Include relevant details for {location} if applicable.
- Structure with 2-3 short paragraphs and 1-2 bullet point lists.
- Do NOT use cliché phrases like "In today's digital landscape", "look no further", "it's important to note".

Put the generated content in the "suggested_body_copy" field."""
        thin_field = ',"suggested_body_copy":"FULL 300-500 word original body copy here"'

    json_template = '{"ideal_word_count":1200,"content_gap":"what topics are missing","opening_hook":"compelling opening 2-3 sentences","recommended_sections":["Section 1","Section 2","Section 3","Section 4"],"cta_recommendation":"CTA text and placement","tone_guidance":"recommended tone","keyword_placement":"where to place keywords","readability_tips":"readability improvements","e_e_a_t_signals":"E-E-A-T recommendations","internal_link_anchors":["anchor 1","anchor 2","anchor 3"]' + thin_field + '}'

    prompt = f"""SEO content strategist. Provide body copy guidance for this page.{thin_request}

Return ONLY JSON (no markdown, no backticks):
{json_template}

URL: {url} | Title: {title} | H1: {h1} | Keyword: {keyword} | Words: {word_count} | Location: {location}
Content: {content[:1500]}
"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=2000 if is_thin else 800, temperature=0.4))
    except Exception as e: logger.error(f"ai_body_copy {url}: {e}"); return {}


# ── Site-wide AI ───────────────────────────────────────────────────────────────

def ai_site_recommendations(domain, summary, pages_sample) -> str:
    if not _has_client(): return ""
    sample_text = "\n".join(
        f"- URL: {p.get('url','')} | Title: {p.get('current_title','')} | H1: {p.get('current_h1','')} | Schema: {p.get('schema_markup','')} | OG: {p.get('og_tags','')} | Words: {p.get('word_count',0)} | SEO Score: {p.get('seo_score','N/A')}"
        for p in pages_sample[:10]
    )
    prompt = f"""You are a senior SEO strategist. Based on this website audit data, provide a comprehensive final recommendation report.

Website: {domain}
Summary: {json.dumps(summary, default=str)}
Sample Pages: {sample_text}

Write a detailed report (600-800 words) covering these sections:

1. OVERALL SITE HEALTH ASSESSMENT
   - Summary of critical issues found
   - Priority fixes needed immediately

2. GOOGLE'S LATEST ALGORITHM COMPLIANCE
   - E-E-A-T (Experience, Expertise, Authoritativeness, Trustworthiness) gaps
   - Core Web Vitals recommendations
   - Helpful Content system alignment
   - Spam policies compliance

3. AEO (Answer Engine Optimization) RECOMMENDATIONS
   - How to optimize for AI-powered search (Google SGE/AI Overviews, Perplexity, ChatGPT)
   - FAQ schema implementation needs
   - Featured snippet optimization
   - Conversational content structure

4. GEO (Generative Engine Optimization) RECOMMENDATIONS
   - Structured data improvements for AI crawlers
   - Content clarity and citation-worthiness
   - Entity optimization and knowledge graph presence
   - llms.txt / robots.txt AI crawler directives

5. CONTENT STRATEGY IMPROVEMENTS
   - Thin content remediation
   - Content gap analysis
   - Internal linking structure

6. TECHNICAL SEO PRIORITIES
   - Critical technical fixes (canonical, redirects, schema)
   - Mobile optimization
   - Page speed improvements

Return the report as plain text with section headers. Be specific and actionable.
"""
    try:
        result = ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True)
        if not result:
            logger.warning("ai_site_recommendations: empty response")
            return "Error: AI returned empty response."
        logger.info(f"Site recommendations: {len(result)} chars")
        return result
    except Exception as e:
        logger.error(f"ai_site_recommendations: {e}")
        return "Error generating AI recommendations."


def ai_keyword_analysis(content, brand, location="Global") -> dict:
    if not _has_client(): return {}
    location_instruction = ""
    if location and location != "Global":
        location_instruction = f"""
LOCATION CONTEXT: This business operates in {location}.
- Use this as COUNTRY-LEVEL context for keyword relevance - do NOT stuff location into every keyword.
- Primary keywords: should be GENERIC (no location). E.g. "organic skincare products" not "organic skincare products UK".
- Secondary keywords: mostly generic, at most 1 out of 3 can include country name if natural.
- Service keywords: keep generic. Only add country/city if it's a clearly local service (e.g. plumber, restaurant).
- Short-tail keywords: 1 out of 2 can include CITY name if a city is identified, otherwise keep generic.
- Long-tail keywords: 1 out of 2 should include city or country naturally (e.g. "best organic skincare for dry skin in London").
"""
    prompt = f"""You are a world-class SEO expert. Analyze the following website content and identify ALL distinct services or product categories offered by this business.

This could be ANY type of business. Adapt your analysis to the specific business vertical.
{location_instruction}
For EACH service/product category you detect, generate:
1. 5 service-based keywords (mostly generic, location only if clearly local service)
2. Primary keyword (GENERIC - no location)
3. 3 secondary keywords (mostly generic)
4. 2 short-tail keywords (1-2 words; one can include city if known)
5. 2 long-tail keywords (4-8 words; one should include location naturally)

Rules:
- Detect MINIMUM 3 services, MAXIMUM 8
- Country context: {location} - use for relevance, NOT for keyword stuffing
- Most keywords should work globally. Only sprinkle location into a FEW keywords.
- Include transactional and informational intent keywords

Return ONLY valid JSON (no markdown, no backticks):
{{
  "business_type": "detected business category",
  "target_location": "{location}",
  "services": [
    {{
      "service": "Service Name",
      "keywords": ["k1","k2","k3","k4","k5"],
      "primary": "main keyword",
      "secondary": ["s1","s2","s3"],
      "short_tail": ["st1","st2"],
      "long_tail": ["lt1","lt2"]
    }}
  ]
}}

Brand: {brand}
Target Country: {location}
Website Content:
{content[:12000]}"""
    try:
        raw = ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True)
        if not raw:
            logger.warning("ai_keyword_analysis: empty response. Check API key and mode.")
            return {}
        result = _parse_obj(raw)
        if result.get("services"):
            logger.info(f"Keyword analysis: {len(result['services'])} services detected")
        else:
            logger.warning("ai_keyword_analysis: no services found in response")
        return result
    except Exception as e:
        logger.error(f"ai_keyword_analysis: {e}"); return {}


def ai_blog_topics(kw_json, brand, location="Global", existing_pages=None) -> list:
    """Generate blog topics with duplicate-check against existing site pages."""
    if not _has_client() or not kw_json.get("services"): return []
    current_year = datetime.now().year
    all_services = kw_json["services"]
    existing_pages = existing_pages or []
    existing_titles = [(p.get("current_title","") or "").lower().strip() for p in existing_pages if p.get("current_title")]
    existing_content_summary = "\n".join(f"- {t}" for t in existing_titles[:50])
    location_instruction = f"LOCATION: {location}. 1 local topic per service should reference {location}." if location and location != "Global" else ""
    all_topics = []
    for i in range(0, len(all_services), 3):
        batch = all_services[i:i+3]
        services_summary = "\n".join(f"- {svc['service']}: {', '.join(svc.get('keywords',[])[:5])}" for svc in batch)
        prompt = f"""Generate blog topics for these services. Year: {current_year}. Brand: {brand}. {location_instruction}

EXISTING CONTENT ON THE WEBSITE (DO NOT DUPLICATE):
{existing_content_summary if existing_content_summary else "None"}

RULES:
1. Per service generate: 3 informational, 2 commercial, 1 local topic.
2. CRITICAL: Check each topic against the existing content list above. If a similar topic (60%+ meaning overlap) already exists on the website, DO NOT suggest it. Only suggest NEW topics that fill content gaps.
3. Analyze the website's keywords, services, and content to suggest topics that complement existing coverage.
4. Keep titles concise and SEO-optimized. No year unless it adds value.

Services:{services_summary}

Return ONLY valid JSON array (no markdown):
[{{"service":"Name","topics":[{{"title":"Blog Title","type":"informational|commercial|local","target_keyword":"keyword","description":"2-3 sentence summary"}}]}}]"""
        try:
            parsed = _parse_arr(ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True))
            if parsed and isinstance(parsed, list):
                all_topics.extend(parsed)
                logger.info(f"Blog topics batch {i//3+1}: {sum(len(s.get('topics',[])) for s in parsed)} topics")
        except Exception as e: logger.error(f"ai_blog_topics batch {i}: {e}")
    total = sum(len(s.get("topics",[])) for s in all_topics)
    logger.info(f"Blog topics complete: {total} topics across {len(all_topics)} services")
    return all_topics


def generate_blog_ideas_with_keywords(keyword_data: dict, keywords_ranked: list,
                                       brand_name: str, location: str = "Global",
                                       existing_pages: list = None) -> list:
    """Generate blog ideas enriched with keyword planner data (search volume, CPC, competition)."""
    if not _has_client() or not keywords_ranked: return []
    top_kws = sorted(keywords_ranked, key=lambda x: x.get("search_volume",0), reverse=True)[:40]
    kw_by_service = {}
    for kw in top_kws:
        svc = kw.get("service_name","General")
        kw_by_service.setdefault(svc, []).append(kw)
    existing_titles = [p.get("current_title","").lower() for p in (existing_pages or []) if p.get("current_title")]
    all_ideas = []
    for svc_name, svc_kws in kw_by_service.items():
        kw_details = "\n".join(
            f"- {k['keyword']} (Vol:{k['search_volume']}, CPC:${k['cpc']}, Comp:{k.get('competition_level','?')})"
            for k in svc_kws[:10]
        )
        prompt = f"""Generate 5 SEO blog topics for "{svc_name}" using keyword data.
Brand: {brand_name} | Location: {location}
Keywords:
{kw_details}
Existing (don't duplicate): {chr(10).join(existing_titles[:20])}
Per topic: title (50-70 chars), primary_keyword, secondary_keywords (3-5), short_tail (2-3), long_tail (2-3), type, description, content_outline (5-7 sections).
Return ONLY JSON: [{{"service":"{svc_name}","topics":[{{"title":"Title","type":"informational|commercial|comparison|howto|listicle","target_keyword":"kw","description":"summary","primary_keyword":"kw","secondary_keywords":["kw1"],"short_tail_keywords":["st1"],"long_tail_keywords":["lt1"],"content_outline":["Section 1"]}}]}}]"""
        try:
            parsed = _parse_arr(ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True))
            if parsed: all_ideas.extend(parsed)
        except Exception as e: logger.error(f"generate_blog_ideas_with_keywords {svc_name}: {e}")
    logger.info(f"generate_blog_ideas_with_keywords: {sum(len(s.get('topics',[])) for s in all_ideas)} topics")
    return all_ideas


def ai_backlink_strategy(kw_json, brand, domain, location="Global") -> dict:
    if not _has_client() or not kw_json.get("services"): return {}
    svcs = ", ".join(s["service"] for s in kw_json["services"])
    all_kws = []
    for svc in kw_json["services"]: all_kws.extend(svc.get("keywords", []))
    keywords_list = ", ".join(all_kws[:20])
    prompt = f"""You are an expert link building strategist. Create a comprehensive backlinking strategy for this business.

Business: {brand}
Domain: {domain}
Business Type: {kw_json.get('business_type', 'Unknown')}
Services: {svcs}
Target Keywords: {keywords_list}
Target Location: {location}

LOCATION-BASED DOMAIN REQUIREMENT:
- Include REGION-SPECIFIC directories and platforms relevant to {location}.
- For example if UK: include yell.com, freeindex.co.uk, scoot.co.uk, uk.trustpilot.com
- For example if US: include yelp.com, bbb.org, yellowpages.com, manta.com
- For example if India: include justdial.com, sulekha.com, indiamart.com, tradeindia.com
- For example if Australia: include yellowpages.com.au, truelocal.com.au, hotfrog.com.au
- Mix both GLOBAL platforms (medium.com, linkedin.com) and LOCAL platforms specific to {location}.

STRICT RULES FOR TARGET DOMAINS:
- NEVER recommend competitor websites, SEO agency sites, or digital marketing blogs (e.g. neilpatel.com, moz.com, semrush.com, ahrefs.com, searchenginejournal.com, hubspot.com, backlinko.com). These must be excluded.
- ONLY recommend platforms where the business can CREATE a profile, SUBMIT content, GET listed, or POST articles.
- Every domain must be a platform that ACCEPTS external contributions, listings, or profiles.

Generate backlinking strategies with SPECIFIC target domains in these categories:

1. SEO BACKLINKS:
   a) Guest Post Websites — platforms that accept guest articles (e.g. medium.com, vocal.media, hackernoon.com, dev.to, tumblr.com)
   b) Business Directory Listings — submit business profile (e.g. yelp.com, yellowpages.com, bbb.org, hotfrog.com, brownbook.net, foursquare.com, manta.com, cylex.com)
   c) Article Submission Sites — publish informative articles (e.g. ezinearticles.com, articlebiz.com, hubpages.com, issuu.com, scribd.com, slideshare.net)
   d) Web 2.0 Profile Backlinks — create branded profiles (e.g. wordpress.com, blogger.com, weebly.com, wix.com, strikingly.com, sites.google.com)
   e) Social Bookmarking — bookmark and share content (e.g. mix.com, digg.com, slashdot.org, folkd.com, scoop.it)

2. AEO (Answer Engine Optimization) BACKLINKS:
   a) Q&A Platforms — answer questions with links (e.g. quora.com, reddit.com, stackexchange.com, answers.com)
   b) Wiki & Knowledge Bases — contribute knowledge (e.g. wikipedia.org, wikihow.com, fandom.com, everipedia.org)
   c) Forum Participation — join niche forums (e.g. warriorforum.com, digitalpoint.com, sitepoint.com, relevant industry forums)
   d) Dictionary & Reference Sites — submit definitions/terms (e.g. dictionary.com, investopedia.com, techopedia.com, webopedia.com)

3. GEO (Generative Engine Optimization) BACKLINKS:
   a) Business Data Platforms — create company profiles (e.g. crunchbase.com, owler.com, zoominfo.com, dnb.com, glassdoor.com)
   b) Academic & Research — publish research/whitepapers (e.g. researchgate.net, academia.edu, ssrn.com, arxiv.org)
   c) Government & Institutional — get listed on .gov/.edu directories
   d) Data & Statistics — contribute industry data (e.g. statista.com, data.gov, kaggle.com)

4. PR (Public Relations) BACKLINKS:
   a) Press Release Distribution — distribute news (e.g. prnewswire.com, businesswire.com, globenewswire.com, prweb.com, openpr.com, einpresswire.com)
   b) News & Media — submit stories (e.g. medium.com, substack.com, linkedin.com/pulse, patch.com)
   c) Podcast Directories — list podcast/interviews (e.g. podcasts.apple.com, spotify.com, podbean.com, buzzsprout.com, anchor.fm)
   d) Social Profile Backlinks — create official profiles (e.g. linkedin.com, x.com, facebook.com, instagram.com, pinterest.com, youtube.com, tiktok.com, github.com, behance.net, dribbble.com)

5. BACKLINKS TO AVOID:
   - PBN (Private Blog Networks), link farms, paid link schemes, irrelevant forum spam, low-quality article spinners, fiverr gig backlinks, automated link building tools, comment spam, expired domain link networks

For EACH strategy include 3-5 SPECIFIC domains with DA scores. Only recommend domains where the business can actually get a backlink.

Return ONLY valid JSON (no markdown, no backticks):
{{
  "seo_backlinks": [
    {{"strategy": "Strategy name", "description": "How to execute", "priority": "High|Medium|Low", "difficulty": "Easy|Medium|Hard", "target_domains": ["domain.com (DA XX)", "domain2.com (DA XX)"]}}
  ],
  "aeo_backlinks": [
    {{"strategy": "Strategy name", "description": "How to execute", "priority": "High|Medium|Low", "difficulty": "Easy|Medium|Hard", "target_domains": ["domain.com (DA XX)", "domain2.com (DA XX)"]}}
  ],
  "geo_backlinks": [
    {{"strategy": "Strategy name", "description": "How to execute", "priority": "High|Medium|Low", "difficulty": "Easy|Medium|Hard", "target_domains": ["domain.com (DA XX)", "domain2.com (DA XX)"]}}
  ],
  "pr_backlinks": [
    {{"strategy": "Strategy name", "description": "How to execute", "priority": "High|Medium|Low", "difficulty": "Easy|Medium|Hard", "target_domains": ["domain.com (DA XX)", "domain2.com (DA XX)"]}}
  ],
  "avoid_backlinks": [
    {{"type": "Type to avoid", "reason": "Why harmful", "risk_level": "High|Critical", "examples": ["example-spam-site.com", "fiverr-gig-backlinks.xyz"]}}
  ]
}}"""
    try:
        raw = ai_chat(prompt, max_tokens=4096, temperature=0.3, use_sonnet=True)
        if not raw: logger.warning("ai_backlink_strategy: empty response"); return {}
        result = _parse_obj(raw)
        total = sum(len(result.get(k,[])) for k in ["seo_backlinks","aeo_backlinks","geo_backlinks","pr_backlinks","avoid_backlinks"])
        logger.info(f"Backlink strategy: {total} strategies generated")
        return result
    except Exception as e: logger.error(f"ai_backlink_strategy: {e}"); return {}


def ai_six_month_plan(kw_json, bl_data, brand, domain, summary) -> dict:
    if not _has_client(): return {}
    current_year  = datetime.now().year
    current_month = datetime.now().month
    svcs = ", ".join(s["service"] for s in kw_json.get("services", []))
    prompt = f"""You are a senior SEO project manager. Create a detailed 6-month SEO execution plan.

Business: {brand}
Domain: {domain}
Business Type: {kw_json.get('business_type', 'Unknown')}
Services: {svcs}
Current Audit Summary: {json.dumps(summary, default=str)}
Start Date: Month {current_month}/{current_year}

Create a month-by-month plan for 6 months. For EACH month include:
- Key focus areas and tasks
- Content deliverables (blog posts, pages to create/optimize)
- Technical SEO tasks
- Backlink building activities
- Expected measurable outputs/results for that month

Be realistic with expectations - SEO takes time. Show progressive improvement.

Return ONLY valid JSON (no markdown, no backticks):
{{
  "plan_start": "{current_month}/{current_year}",
  "months": [
    {{
      "month_number": 1,
      "month_label": "Month 1 (MMM YYYY)",
      "focus": "Primary focus area",
      "tasks": [
        {{"task": "Task description", "category": "Technical|Content|Backlinks|Analytics"}}
      ],
      "deliverables": ["Deliverable 1", "Deliverable 2"],
      "expected_output": {{
        "organic_traffic_change": "+X%",
        "keywords_improved": "X keywords move up",
        "backlinks_target": "X new backlinks",
        "pages_optimized": "X pages",
        "content_published": "X blog posts",
        "technical_fixes": "X issues resolved",
        "summary": "Brief summary of expected outcome"
      }}
    }}
  ]
}}"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.4, use_sonnet=True)
        if not raw: logger.warning("ai_six_month_plan: empty response"); return {}
        result = _parse_obj(raw)
        logger.info(f"6-month plan: {len(result.get('months',[]))} months generated")
        return result
    except Exception as e: logger.error(f"ai_six_month_plan: {e}"); return {}


def ai_internal_linking_strategy(pages, domain) -> dict:
    if not _has_client() or not pages: return {}
    pages_summary = "\n".join(
        f"- URL: {p.get('url','')} | Title: {p.get('current_title','')} | H1: {p.get('current_h1','')} | Primary KW: {p.get('primary_keyword','')} | Words: {p.get('word_count',0)}"
        for p in pages[:30]
    )
    prompt = f"""You are an expert in internal linking for SEO. Analyze these website pages and create a comprehensive internal linking strategy.

Website: {domain}
Pages:
{pages_summary}

Create an internal linking strategy covering:

1. HUB PAGES: Identify 3-5 pages that should serve as pillar/hub pages (most important topic clusters)
2. LINKING MAP: For each page, suggest 2-3 OTHER pages it should link TO, with recommended anchor text
3. ORPHAN PAGES: Identify pages that likely receive no internal links and suggest where to link from
4. SILOING STRATEGY: Group pages into topic silos/clusters for logical internal linking
5. ANCHOR TEXT RECOMMENDATIONS: Best anchor text phrases for each internal link (avoid generic "click here")
6. LINK DEPTH: Pages that need to be closer to homepage (reduce click depth)
7. NAVIGATION IMPROVEMENTS: Suggestions for main menu, footer, or sidebar link additions

Return ONLY valid JSON (no markdown fences):
{{
  "hub_pages": [
    {{"url": "page URL", "topic_cluster": "cluster name", "reason": "why this is a hub page"}}
  ],
  "linking_map": [
    {{"from_url": "source page", "to_url": "target page", "anchor_text": "recommended anchor text", "context": "where in the content to place this link"}}
  ],
  "orphan_pages": [
    {{"url": "orphan page URL", "link_from": "suggested source page", "anchor_text": "anchor text"}}
  ],
  "topic_silos": [
    {{"silo_name": "Topic Cluster Name", "pages": ["url1", "url2"], "hub_url": "main hub page URL"}}
  ],
  "navigation_suggestions": ["suggestion 1", "suggestion 2"],
  "overall_score": "Good/Needs Work/Poor",
  "priority_actions": ["action 1", "action 2", "action 3"]
}}
"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=4096, temperature=0.3, use_sonnet=True))
    except Exception as e: logger.error(f"ai_internal_linking: {e}"); return {}


def ai_keyword_url_mapping(pages, kw_data, domain, location="Global") -> list:
    if not _has_client() or not pages: return []
    pages_summary = "\n".join(
        f"- URL: {p.get('url','')} | Title: {p.get('current_title','')} | H1: {p.get('current_h1','')} | Content Keywords: {p.get('primary_keyword','')} | Words: {p.get('word_count',0)}"
        for p in pages[:50]
    )
    all_kw_rows = []
    for s in kw_data.get("services", []):
        svc = s.get("service","")
        for kw in [s.get("primary","")] + s.get("keywords",[]) + s.get("short_tail",[]) + s.get("long_tail",[]):
            if kw:
                ktype = "primary" if kw == s.get("primary") else "secondary"
                all_kw_rows.append({"service": svc, "keyword": kw, "keyword_type": ktype})
    if not all_kw_rows: return []
    kw_list = json.dumps(all_kw_rows[:60], default=str)
    prompt = f"""You are an SEO keyword mapping expert. Based on the actual website content, map each keyword to the BEST matching URL on this website.

Website: {domain}
Target Location: {location}

Pages Available:
{pages_summary}

Keywords by Service:
{kw_list}

For EACH keyword, analyze the page content and assign it to the most relevant URL. If no page is a good match, suggest creating a new page.

Return ONLY valid JSON (no markdown fences):
[
  {{
    "keyword": "keyword phrase",
    "keyword_type": "primary|secondary|short_tail|long_tail",
    "service_name": "Service Name",
    "mapped_url": "best matching URL from the website",
    "match_confidence": "High|Medium|Low",
    "reason": "Why this page is the best match",
    "on_page_action": "Specific action to optimize this page for this keyword",
    "create_new_page": false,
    "suggested_new_url": ""
  }}
]
"""
    try:
        result = _parse_arr(ai_chat(prompt, max_tokens=4096, temperature=0.3, use_sonnet=True))
        logger.info(f"ai_keyword_url_mapping: {len(result)} mappings generated")
        return result
    except Exception as e: logger.error(f"ai_keyword_url_mapping: {e}"); return []


def ai_axo_recommendations(pages, kw_data, domain, location="Global") -> dict:
    if not _has_client() or not pages: return {}
    pages_summary = "\n".join(
        f"- URL: {p.get('url','')} | Title: {p.get('current_title','')} | Schema: {p.get('schema_types_found','')} | OG: {p.get('og_tags','')} | Words: {p.get('word_count',0)}"
        for p in pages[:20]
    )
    svcs = json.dumps([s.get("service","") for s in kw_data.get("services",[])])
    prompt = f"""You are an AXO (AI Experience Optimization) expert. AXO is about optimizing websites for ALL AI-powered platforms — not just search engines.

AXO covers:
- AEO (Answer Engine Optimization): Google AI Overviews, Perplexity, ChatGPT search
- GEO (Generative Engine Optimization): AI content generation citing your website
- Voice Search Optimization: Alexa, Siri, Google Assistant
- Conversational AI: ChatGPT, Claude, Gemini referencing your content
- AI Aggregators: AI-powered comparison sites, recommendation engines
- Multimodal AI: Image and video AI understanding your brand

Website: {domain}
Location: {location}
Pages: {pages_summary}
Services: {svcs}

Provide comprehensive AXO recommendations:

Return ONLY valid JSON (no markdown fences):
{{
  "axo_score": 65,
  "axo_grade": "C",
  "aeo_recommendations": [
    {{"action": "Specific action", "priority": "Critical|High|Medium|Low", "impact": "Expected impact", "implementation": "How to implement"}}
  ],
  "geo_recommendations": [
    {{"action": "Specific action", "priority": "Critical|High|Medium|Low", "impact": "Expected impact", "implementation": "How to implement"}}
  ],
  "voice_search_recommendations": [
    {{"action": "Specific action", "priority": "Critical|High|Medium|Low", "impact": "Expected impact", "implementation": "How to implement"}}
  ],
  "conversational_ai_recommendations": [
    {{"action": "Specific action", "priority": "Critical|High|Medium|Low", "impact": "Expected impact", "implementation": "How to implement"}}
  ],
  "structured_data_actions": [
    {{"page_type": "Page type", "required_schema": "Schema types needed", "action": "What to add/fix"}}
  ],
  "content_format_actions": [
    {{"action": "Content format change", "reason": "Why AI platforms need this", "pages_affected": "Which pages"}}
  ],
  "llms_txt_recommendation": "Specific recommendation for llms.txt file content",
  "entity_optimization": "How to establish stronger entity presence for AI knowledge graphs",
  "citation_worthiness": "How to make content more likely to be cited by AI platforms",
  "priority_roadmap": ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"]
}}
"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=4096, temperature=0.3, use_sonnet=True))
    except Exception as e: logger.error(f"ai_axo_recommendations: {e}"); return {}


def ai_alt_recommendations(images_missing: list) -> dict:
    if not _has_client() or not images_missing: return {}
    batch = images_missing[:50]
    image_list = "\n".join(f"{i+1}. Page: {img['page']}\n   Image URL: {img['src']}" for i,img in enumerate(batch))
    prompt = f"""You are an SEO image optimization expert. Based on the image URLs and their page context, suggest descriptive, keyword-rich ALT text for each image.

Rules for good ALT text:
- Descriptive: explain what the image shows
- Keyword-rich but natural: include relevant keywords without stuffing
- Concise: 8-15 words maximum
- Contextual: relate to the page content
- Accessible: useful for screen readers

Images needing ALT text:
{image_list}

Return ONLY valid JSON (no markdown fences). Return a JSON object where keys are the image numbers (as strings) and values are the recommended ALT text.
Example: {{"1": "Professional web development team collaborating on software project", "2": "Modern office interior with standing desks and natural lighting"}}
"""
    try:
        result = {}
        for k,v in _parse_obj(ai_chat(prompt, max_tokens=800, temperature=0.4)).items():
            idx = int(k) - 1
            if 0 <= idx < len(batch): result[batch[idx]["src"]] = v
        return result
    except Exception as e: logger.error(f"ai_alt_recommendations: {e}"); return {}


def ai_new_page_suggestions(pages_data: list, keyword_data: dict,
                              domain: str, brand: str, location: str = "Global") -> list:
    if not _has_client() or not pages_data: return []
    existing_urls   = [p.get("url","") for p in pages_data[:50]]
    existing_titles = [p.get("current_title","") for p in pages_data[:50]]
    services = [s.get("service","") for s in keyword_data.get("services",[])]
    all_kws = []
    for svc in keyword_data.get("services",[]): all_kws.extend(svc.get("keywords",[])[:5])
    prompt = f"""You are an SEO strategist. Analyze this website's existing pages and suggest NEW pages that should be created to improve SEO coverage and user experience.

Website: {domain}
Brand: {brand}
Location: {location}
Services: {', '.join(services)}
Target Keywords: {', '.join(all_kws[:15])}

Existing pages (summarized):
{chr(10).join(f'- {u}' for u in existing_urls[:30])}

Based on gaps in the site structure, suggest 5-10 NEW pages that should be created. Consider:
1. Missing service/product pages for keywords without dedicated landing pages
2. Location-specific pages (e.g., "services in {location}")
3. Comparison/alternative pages (e.g., "{brand} vs competitor")
4. FAQ or resource pages
5. About/team/process pages if missing
6. Case study or portfolio pages
7. Industry-specific landing pages

For each suggested page, provide:
- A SEO-optimized URL slug
- Title (30-60 chars)
- Page type (service/location/blog/resource/landing)
- Why it's needed
- Target keyword
- Content outline (5-7 section headings)
- Priority (high/medium/low)

Return ONLY valid JSON array (no markdown):
[{{"url":"/suggested-url-slug","title":"Page Title 30-60 chars","page_type":"service|location|blog|resource|landing","reason":"why this page is needed","target_keyword":"main keyword","content_outline":["Section 1","Section 2","Section 3","Section 4","Section 5"],"priority":"high|medium|low"}}]"""
    try:
        result = _parse_arr(ai_chat(prompt, max_tokens=4096, temperature=0.4, use_sonnet=True))
        logger.info(f"ai_new_page_suggestions: {len(result)} pages recommended")
        return result if isinstance(result, list) else []
    except Exception as e: logger.error(f"ai_new_page_suggestions: {e}"); return []


def ai_keyword_planner_pipeline(keyword_data: dict, brand_name: str, location: str = "Global") -> list:
    """Generate 100 keywords with AI-estimated search volume, CPC, competition, intent."""
    if not _has_client() or not keyword_data.get("services"): return []
    all_keywords = set(); service_map = {}; type_map = {}
    for svc in keyword_data["services"]:
        svc_name = svc.get("service","")
        for ktype, kfield in [("primary","primary"),("secondary","secondary"),("short_tail","short_tail"),("long_tail","long_tail")]:
            vals = svc.get(kfield,[]) if isinstance(svc.get(kfield), list) else [svc.get(kfield,"")]
            for kw in vals:
                if kw: all_keywords.add(kw); service_map[kw] = svc_name; type_map[kw] = ktype
        for kw in svc.get("keywords",[]):
            if kw: all_keywords.add(kw); service_map[kw] = svc_name; type_map.setdefault(kw, "service")
    for bkw in [brand_name, f"{brand_name} services", f"{brand_name} reviews", f"{brand_name} pricing",
                f"{brand_name} near me", f"{brand_name} {location}", f"{brand_name} alternatives", f"best {brand_name} services"]:
        if bkw.strip(): all_keywords.add(bkw); service_map[bkw] = "Brand"; type_map[bkw] = "brand"
    # AI expand to 100
    if len(all_keywords) < 100 and _has_client():
        needed = 100 - len(all_keywords)
        services_list = ", ".join([s["service"] for s in keyword_data["services"]])
        existing_list = "\n".join(list(all_keywords)[:50])
        prompt = f"""Generate {needed} additional SEO keywords. Mix high/medium/low competition.
Business: {brand_name} | Location: {location} | Services: {services_list}
Existing (do NOT duplicate):
{existing_list}
Include: transactional, informational, comparison, how-to, best, near-me, questions.
Return ONLY JSON: [{{"keyword":"text","type":"primary|secondary|short_tail|long_tail|brand","service":"Name","competition_estimate":"HIGH|MEDIUM|LOW"}}]"""
        try:
            for item in _parse_arr(ai_chat(prompt, max_tokens=3000, temperature=0.4, use_sonnet=True)):
                kw = item.get("keyword","")
                if kw and kw not in all_keywords:
                    all_keywords.add(kw); service_map[kw] = item.get("service","General"); type_map[kw] = item.get("type","secondary")
        except Exception as e: logger.error(f"ai_keyword_planner expansion: {e}")
    keyword_list = list(all_keywords)[:100]
    # AI estimate metrics in batches
    all_metrics = {}
    for batch_start in range(0, len(keyword_list), 25):
        batch = keyword_list[batch_start:batch_start+25]
        kw_list_str = "\n".join(f"{i+1}. {kw}" for i,kw in enumerate(batch))
        prompt = f"""Estimate monthly search volume, CPC (USD), competition level, and intent for each keyword for {location}.
Keywords:
{kw_list_str}
Return ONLY valid JSON (no markdown):
{{"keywords":[{{"keyword":"text","search_volume":1000,"cpc":1.50,"competition":"HIGH","competition_index":0.75,"intent":"informational"}}]}}
Rules: search_volume=int, cpc=decimal, competition=HIGH|MEDIUM|LOW, intent=informational|transactional|navigational|commercial"""
        try:
            for item in _parse_obj(ai_chat(prompt, max_tokens=2000, temperature=0.3, use_sonnet=False)).get("keywords",[]):
                all_metrics[item.get("keyword","").lower()] = item
        except Exception as e: logger.error(f"ai_keyword_planner batch: {e}")
    keywords_full = []
    for kw in keyword_list:
        m = all_metrics.get(kw.lower(), {})
        is_brand = type_map.get(kw,"") == "brand" or brand_name.lower() in kw.lower()
        keywords_full.append({
            "keyword": kw, "keyword_type": type_map.get(kw,"secondary"),
            "competition_level": m.get("competition","MEDIUM"),
            "search_volume": int(m.get("search_volume",0) or 0),
            "cpc": float(m.get("cpc",0.0) or 0.0),
            "competition_index": float(m.get("competition_index",0.5) or 0.5),
            "is_brand_keyword": 1 if is_brand else 0,
            "service_name": service_map.get(kw,""),
            "intent": m.get("intent","informational"),
        })
    max_vol = max((k["search_volume"] for k in keywords_full), default=1) or 1
    max_cpc = max((k["cpc"] for k in keywords_full), default=1) or 1
    intent_scores = {"transactional":1.0,"commercial":0.8,"informational":0.5,"navigational":0.3}
    for kw in keywords_full:
        kw["rank_score"] = round(
            (kw["search_volume"]/max_vol)*40 + (kw["cpc"]/max_cpc)*20 +
            {"LOW":20,"MEDIUM":10,"HIGH":5}.get(kw["competition_level"],10) +
            intent_scores.get(kw["intent"],0.5)*20, 2)
    keywords_full.sort(key=lambda x: x.get("rank_score",0), reverse=True)
    for i, kw in enumerate(keywords_full, 1):
        kw["keyword_rank"] = i
        kw["keyword_difficulty"] = ("Hard" if kw["competition_level"]=="HIGH" and kw["search_volume"]>1000
                                    else "Easy" if kw["competition_level"]=="LOW" or kw["search_volume"]<100
                                    else "Medium")
        is_br = kw.get("is_brand_keyword",0); comp = kw["competition_level"]; intent = kw["intent"]
        if is_br:                                                           kw["keyword_category"] = "branded"
        elif comp=="HIGH" and intent in ("transactional","commercial"):     kw["keyword_category"] = "high_competition_commercial"
        elif comp=="HIGH":                                                  kw["keyword_category"] = "high_competition"
        elif intent in ("transactional","commercial"):                      kw["keyword_category"] = "commercial"
        elif intent=="informational":                                       kw["keyword_category"] = "informational"
        elif comp=="LOW":                                                   kw["keyword_category"] = "low_competition_opportunity"
        else:                                                               kw["keyword_category"] = "medium_competition"
    logger.info(f"ai_keyword_planner_pipeline: {len(keywords_full)} keywords ranked")
    return keywords_full


def ai_generate_llm_prompts(keyword_data: dict, keywords_ranked: list,
                             brand_name: str, location: str = "Global") -> list:
    """
    Generate LLM prompts (questions users type into ChatGPT/Perplexity/Gemini).
    5 categories: high_competition, informational, commercial, local, branded.
    FIX: Restored full indentation and all 5 categories inside the function.
    """
    if not _has_client() or not keywords_ranked:
        return []

    services      = [s.get("service","") for s in keyword_data.get("services", [])]
    business_type = keyword_data.get("business_type", "")
    brand_lower   = brand_name.lower().strip()

    non_branded  = [k for k in keywords_ranked
                    if not k.get("is_brand_keyword") and brand_lower not in k.get("keyword","").lower()]
    branded      = [k for k in keywords_ranked
                    if k.get("is_brand_keyword") or brand_lower in k.get("keyword","").lower()]
    high_vol     = sorted([k for k in non_branded if k.get("search_volume",0) > 0],
                           key=lambda x: x.get("search_volume",0), reverse=True)[:30]
    high_comp    = [k for k in high_vol if k.get("competition_level") == "HIGH"][:15]
    info_kws     = [k for k in high_vol if k.get("intent") == "informational"][:10]
    commercial_kws = [k for k in high_vol if k.get("intent") in ("commercial","transactional")][:10]

    all_prompts = []

    def _call(prompt_text, batch_name):
        try:
            raw = ai_chat(prompt_text, max_tokens=3000, temperature=0.3, use_sonnet=True)
            result = _parse_arr(raw)
            logger.info(f"llm_prompts {batch_name}: {len(result)} prompts")
            return result
        except Exception as e:
            logger.error(f"llm_prompts {batch_name} error: {e}")
            return []

    # ── CATEGORY 1: High Competition ──────────────────────────────────────────
    if high_comp:
        kw_list = "\n".join(
            f"- \"{k['keyword']}\" (Vol:{k['search_volume']}, Service:{k.get('service_name','')})"
            for k in high_comp
        )
        all_prompts.extend(_call(f"""Generate search prompts for HIGH COMPETITION keywords. These are prompts users type into ChatGPT, Perplexity, Google AI, Gemini.

Location: {location} | Business Type: {business_type}

HIGH COMPETITION KEYWORDS:
{kw_list}

RULES:
1. Generate 2 prompts per keyword = {min(len(high_comp)*2, 30)} total prompts
2. DO NOT include the brand name "{brand_name}" in any prompt
3. Combine keyword with location "{location}" in 60% of prompts
4. Use natural question/phrase format that real users type
5. Focus on comparison, best-of, and decision-making queries

Examples: "best [keyword] in [location]", "top [keyword] companies [location]", "[keyword] vs [alternative] which is better", "how to choose [keyword] in [location]"

Return ONLY JSON array (no markdown, no explanation):
[{{"prompt_text":"exact prompt","prompt_type":"high_competition","target_keyword":"keyword used","search_volume":1000,"ai_engine":"All","service_name":"Service","priority":"high","keyword_category":"high_competition"}}]""", "high_competition"))

    # ── CATEGORY 2: Informational ─────────────────────────────────────────────
    if info_kws:
        kw_list = "\n".join(f"- \"{k['keyword']}\" (Vol:{k['search_volume']})" for k in info_kws)
        all_prompts.extend(_call(f"""Generate INFORMATIONAL search prompts. Users seeking knowledge/education about these topics.

Location: {location}

INFORMATIONAL KEYWORDS:
{kw_list}

RULES:
1. Generate 2 prompts per keyword
2. DO NOT include "{brand_name}" in any prompt
3. Use "what is", "how does", "why", "explain", "guide to" phrasing
4. Include location in 40% of prompts

Return ONLY JSON array:
[{{"prompt_text":"text","prompt_type":"informational","target_keyword":"kw","search_volume":500,"ai_engine":"All","service_name":"service","priority":"medium","keyword_category":"informational"}}]""", "informational"))

    # ── CATEGORY 3: Commercial / Transactional ────────────────────────────────
    if commercial_kws:
        kw_list = "\n".join(f"- \"{k['keyword']}\" (Vol:{k['search_volume']})" for k in commercial_kws)
        all_prompts.extend(_call(f"""Generate COMMERCIAL/TRANSACTIONAL search prompts. Users ready to buy or hire.

Location: {location} | Business: {business_type}

COMMERCIAL KEYWORDS:
{kw_list}

RULES:
1. Generate 2 prompts per keyword
2. DO NOT include "{brand_name}" in any prompt
3. Use "best", "top", "affordable", "hire", "buy", "pricing", "cost", "near me" phrasing
4. Include "{location}" in 70% of prompts — these are purchase-intent queries

Return ONLY JSON array:
[{{"prompt_text":"text","prompt_type":"commercial","target_keyword":"kw","search_volume":500,"ai_engine":"All","service_name":"service","priority":"high","keyword_category":"commercial"}}]""", "commercial"))

    # ── CATEGORY 4: Local / Geo ───────────────────────────────────────────────
    if location and location != "Global" and high_vol[:8]:
        local_kws = high_vol[:8]
        kw_list = "\n".join(f"- \"{k['keyword']}\"" for k in local_kws)
        all_prompts.extend(_call(f"""Generate LOCAL search prompts for {location}. Users in {location} asking AI for local recommendations.

Keywords:
{kw_list}

RULES:
1. Generate 8-10 prompts
2. DO NOT include "{brand_name}"
3. Every prompt MUST include "{location}" or a city/area within {location}
4. Use "near me", "in [location]", "best [keyword] [location]"

Return ONLY JSON array:
[{{"prompt_text":"text","prompt_type":"local","target_keyword":"kw","search_volume":300,"ai_engine":"All","service_name":"service","priority":"medium","keyword_category":"local"}}]""", "local"))

    # ── CATEGORY 5: Branded ───────────────────────────────────────────────────
    if branded[:10]:
        branded_list = "\n".join(f"- \"{k['keyword']}\" (Vol:{k.get('search_volume',0)})" for k in branded[:10])
        all_prompts.extend(_call(f"""Generate BRANDED search prompts for "{brand_name}". These are prompts where users specifically search for this brand on AI platforms.

Brand: {brand_name} | Location: {location} | Services: {", ".join(services[:6])}

BRANDED KEYWORDS:
{branded_list}

Generate 10-15 branded prompts:
- "{brand_name} reviews" / "is {brand_name} good"
- "{brand_name} vs [competitor]"
- "{brand_name} pricing/cost"
- "{brand_name} services in {location}"
- "{brand_name} alternatives"
- "how to contact {brand_name}"
- "{brand_name} [service] experience"

Every prompt MUST contain "{brand_name}".

Return ONLY JSON array:
[{{"prompt_text":"text with {brand_name}","prompt_type":"branded","target_keyword":"kw","search_volume":100,"ai_engine":"All","service_name":"service","priority":"high","keyword_category":"branded"}}]""", "branded"))

    # ── Deduplicate + enforce brand-safety rules ───────────────────────────────
    seen = set()
    final = []
    for p in all_prompts:
        text = p.get("prompt_text","").strip()
        text_lower = text.lower()
        if not text or text_lower in seen:
            continue
        seen.add(text_lower)
        cat = p.get("keyword_category", p.get("prompt_type","general"))
        if cat != "branded" and brand_lower in text_lower:
            continue  # non-branded must not leak brand name
        p["suggested_answer"] = ""
        vol = p.get("search_volume", 0)
        try: vol = int(vol)
        except: vol = 0
        p["search_volume"] = vol
        p["priority"]      = "high" if vol >= 1000 else ("medium" if vol >= 200 else "low")
        p["prompt_type"]   = cat
        final.append(p)

    cats = {}
    for p in final:
        c = p.get("prompt_type","other"); cats[c] = cats.get(c,0)+1
    logger.info(f"ai_generate_llm_prompts: {len(final)} total | {', '.join(f'{k}:{v}' for k,v in sorted(cats.items()))}")
    return final
