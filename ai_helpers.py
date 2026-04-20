"""
ai_helpers.py — All AI provider calls.
Compatible with: openai==1.59.0 + anthropic==0.40.0 + httpx==0.28.1
"""

import os, json, re, logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

logger = logging.getLogger(__name__)

# ── State ──────────────────────────────────────────────────────────────────────
openai_client    = None
anthropic_client = None
_ai_mode         = "4"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError("Missing OPENAI_API_KEY in .env")

if not ANTHROPIC_API_KEY:
    raise ValueError("Missing ANTHROPIC_API_KEY in .env")

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
4. h2_suggestions: Generate 3-5 SEO-optimized H2 subheadings that naturally structure the page content. Each H2 should target a secondary keyword or long-tail variation. Keep each H2 under 70 characters.
5. og_title: 40-60 characters. Engaging for social media sharing. Can differ from meta_title.
6. og_description: 100-150 characters. Compelling social description with emoji-free CTA.
7. NEVER repeat the current title/description/H1 verbatim — always IMPROVE them.
8. If page content is thin or empty, infer the page purpose from the URL path and create appropriate metadata.

Return ONLY valid JSON (no markdown, no backticks):
{{"primary_keyword":"main keyword for this page","secondary_keywords":["kw1","kw2","kw3"],"short_tail_keywords":["1-2 word keyword","another"],"long_tail_keywords":["4-8 word specific phrase","another phrase"],"meta_title":"NEW title 30-60 chars with keyword","meta_description":"NEW description 120-155 chars with keyword and CTA","h1":"NEW H1 different from title","h2_suggestions":["H2 subheading 1","H2 subheading 2","H2 subheading 3"],"og_title":"Social title 40-60 chars","og_description":"Social description 100-150 chars","og_image_url":"recommended OG image description","schema_type":"Schema.org type","schema_code_snippet":"JSON-LD example max 200 chars","optimized_url":"seo-friendly-slug","image_optimization_tips":"image SEO tips"}}

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
    try:
        result = _parse_arr(ai_chat(prompt, max_tokens=800, temperature=0.3))
        return result[:5] if result else []  # Hard limit: max 5 FAQs
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
1. Per service generate EXACTLY 5 topics total: 3 informational, 1 commercial, 1 local topic. HARD LIMIT: 5 topics per service, no more.
2. CRITICAL: Check each topic against the existing content list above. If a similar topic (60%+ meaning overlap) already exists on the website, DO NOT suggest it. Only suggest NEW topics that fill content gaps.
3. Analyze the website's keywords, services, and content to suggest topics that complement existing coverage.
4. Keep titles concise and SEO-optimized. No year unless it adds value.

Services:{services_summary}

Return ONLY valid JSON array (no markdown):
[{{"service":"Name","topics":[{{"title":"Blog Title","type":"informational|commercial|local","target_keyword":"keyword","description":"2-3 sentence summary"}}]}}]"""
        try:
            parsed = _parse_arr(ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True))
            if parsed and isinstance(parsed, list):
                # Enforce hard limit of 5 topics per service
                for svc_entry in parsed:
                    if isinstance(svc_entry, dict) and "topics" in svc_entry:
                        svc_entry["topics"] = svc_entry["topics"][:5]
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
            if parsed:
                # Enforce hard limit of 5 topics per service
                for svc_entry in parsed:
                    if isinstance(svc_entry, dict) and "topics" in svc_entry:
                        svc_entry["topics"] = svc_entry["topics"][:5]
                all_ideas.extend(parsed)
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
    from calendar import month_name as _month_name
    current_year  = datetime.now().year
    current_month = datetime.now().month
    svcs = ", ".join(s["service"] for s in kw_json.get("services", []))

    # Pre-compute real calendar month names for all 6 months
    month_labels = []
    for i in range(1, 7):
        raw_m = current_month + i - 1
        y = current_year + (raw_m - 1) // 12
        m = ((raw_m - 1) % 12) + 1
        month_labels.append(f"Month {i} — {_month_name[m]} {y}")
    month_labels_str = "\n".join(f"  {lbl}" for lbl in month_labels)

    prompt = f"""You are a senior SEO project manager. Create a detailed 6-month SEO execution plan.

Business: {brand}
Domain: {domain}
Business Type: {kw_json.get('business_type', 'Unknown')}
Services: {svcs}
Current Audit Summary: {json.dumps(summary, default=str)}

The 6 months are (USE THESE EXACT BASE LABELS):
{month_labels_str}

CRITICAL RULES FOR month_label:
- Format MUST be: "Month N — MonthName YYYY: Short Focus Theme"
- The Short Focus Theme after the colon must be 4-7 words describing that month focus.
- NEVER use placeholder text like "MMM YYYY" or "Month 1 (MMM YYYY)".
- ALWAYS use the real month name from the list above.
- Examples:
    "Month 1 — {month_labels[0].split(" — ")[1]}: Technical Foundation & Quick Wins"
    "Month 2 — {month_labels[1].split(" — ")[1]}: Content Creation & On-Page SEO"
    "Month 3 — {month_labels[2].split(" — ")[1]}: Link Building & Authority Growth"
    "Month 4 — {month_labels[3].split(" — ")[1]}: Keyword Expansion & Local SEO"
    "Month 5 — {month_labels[4].split(" — ")[1]}: Conversion Optimization & Schema"
    "Month 6 — {month_labels[5].split(" — ")[1]}: Scaling & Performance Review"

For EACH month include:
- focus: one clear sentence describing the primary SEO goal this month
- tasks: 5-8 specific actionable tasks with categories (Technical|Content|Backlinks|Analytics)
- deliverables: 3-5 concrete outputs the client receives
- expected_output: realistic measurable results

Progression guide: Month 1-2 = Foundation, Month 3-4 = Growth, Month 5-6 = Scaling.

Return ONLY valid JSON (no markdown, no backticks):
{{
  "plan_start": "{month_labels[0]}",
  "months": [
    {{
      "month_number": 1,
      "month_label": "Month 1 — {month_labels[0].split(" — ")[1]}: Technical Foundation & Quick Wins",
      "focus": "One sentence describing this month primary SEO goal",
      "tasks": [
        {{"task": "Specific actionable task", "category": "Technical|Content|Backlinks|Analytics"}}
      ],
      "deliverables": ["Deliverable 1", "Deliverable 2", "Deliverable 3"],
      "expected_output": {{
        "organic_traffic_change": "+X% or baseline established",
        "keywords_improved": "X keywords indexed/tracked",
        "backlinks_target": "X new quality backlinks",
        "pages_optimized": "X pages fixed/optimized",
        "content_published": "X blog posts/pages published",
        "technical_fixes": "X technical issues resolved",
        "summary": "One sentence summary of expected outcome"
      }}
    }}
  ]
}}"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.4, use_sonnet=True)
        if not raw: logger.warning("ai_six_month_plan: empty response"); return {}
        result = _parse_obj(raw)
        # Post-process: guarantee every month_label has real month name (AI fallback safety)
        for i, month in enumerate(result.get("months", [])):
            lbl  = month.get("month_label", "")
            base = month_labels[i] if i < len(month_labels) else f"Month {i+1}"
            real_month_name = base.split(" — ")[1].split(" ")[0]  # e.g. "April"
            if not lbl or "MMM" in lbl or "YYYY" in lbl or real_month_name not in lbl:
                focus = month.get("focus", "")[:50]
                month["month_label"] = f"{base}: {focus}" if focus else base
        labels_preview = [m.get("month_label", "")[:45] for m in result.get("months", [])]
        logger.info(f"6-month plan: {len(result.get('months',[]))} months | {labels_preview}")
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

    def _img_filename(src: str) -> str:
        """Extract a readable filename from image URL for context."""
        try:
            from urllib.parse import urlparse as _up
            name = _up(src).path.split("/")[-1]
            name = name.rsplit(".", 1)[0].replace("-", " ").replace("_", " ")
            return name[:80] if name else ""
        except Exception:
            return ""

    # Build rich image list with page title, primary keyword, and filename
    image_list_lines = []
    for i, img in enumerate(batch):
        page_url   = img.get("page", "")
        src        = img.get("src", "")
        page_title = img.get("page_title", "")
        primary_kw = img.get("primary_keyword", "")
        filename   = _img_filename(src)
        line = f"{i+1}. Page URL: {page_url}"
        if page_title:  line += f"\n   Page Title: {page_title}"
        if primary_kw:  line += f"\n   Page Keyword: {primary_kw}"
        line += f"\n   Image URL: {src}"
        if filename:    line += f"\n   Image Filename: {filename}"
        image_list_lines.append(line)
    image_list = "\n".join(image_list_lines)

    prompt = f"""You are an SEO image optimization expert. Generate highly specific, descriptive ALT text for each image below.

RULES FOR GOOD ALT TEXT:
1. Descriptive and specific: describe exactly what the image shows — people, objects, actions, settings.
2. Keyword-rich but natural: weave in the page keyword where it fits naturally. Do NOT keyword-stuff.
3. Length: 8-15 words. Never a single word. Never more than 20 words.
4. Context-aware: use the Page Title and Page Keyword to infer what the image likely depicts.
5. Use the Image Filename as a clue — filenames like "team-meeting.jpg" or "seo-audit-dashboard.png"
   tell you what the image shows. Expand them into a full descriptive phrase.
6. Accessible: write as if describing to a visually impaired person.
7. No "image of" or "photo of" — start directly with the description.
8. Each ALT text must be UNIQUE — never repeat the same phrase.

EXAMPLES OF GOOD ALT TEXT:
- Filename: team-working.jpg, Page: Digital Marketing Agency → "Digital marketing team collaborating on SEO strategy in modern office"
- Filename: logo.png, Page: About Us → "AcmeCorp official company logo in blue and white"
- Filename: dashboard-screenshot.png, Page: SEO Tools → "SEO audit dashboard showing keyword rankings and traffic metrics"
- Filename: hero-banner.jpg, Page: Web Design Services → "Professional web designer creating responsive website layout on laptop"

Images needing ALT text:
{image_list}

Return ONLY valid JSON (no markdown fences, no extra text).
Keys are image numbers as strings, values are the ALT text.
Example: {{"1": "Professional web development team reviewing SEO audit results on monitor", "2": "Company logo featuring blue geometric design on white background"}}
"""
    try:
        result = {}
        parsed = _parse_obj(ai_chat(prompt, max_tokens=1500, temperature=0.4))
        for k, v in parsed.items():
            try:
                idx = int(k) - 1
                if 0 <= idx < len(batch) and v and isinstance(v, str):
                    alt_text = v.strip().strip("'\"")
                    if 3 <= len(alt_text.split()) <= 25:
                        result[batch[idx]["src"]] = alt_text
            except (ValueError, TypeError):
                continue
        logger.info(f"ai_alt_recommendations: {len(result)} ALT texts generated")
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


# =============================================================================
# GOOGLE ADS KEYWORD PLANNER
# =============================================================================

def get_env(key):
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing environment variable: {key}")
    return value

GOOGLE_ADS_CONFIG = {
    "developer_token": get_env("GOOGLE_ADS_DEVELOPER_TOKEN"),
    "client_id":       get_env("GOOGLE_ADS_CLIENT_ID"),
    "client_secret":   get_env("GOOGLE_ADS_CLIENT_SECRET"),
    "refresh_token":   get_env("GOOGLE_ADS_REFRESH_TOKEN"),
    "customer_id":     get_env("GOOGLE_ADS_CUSTOMER_ID"),
}

_google_ads_available = False
_google_ads_client    = None


def setup_google_ads() -> bool:
    """Initialize Google Ads client for Keyword Planner API."""
    global _google_ads_available, _google_ads_client
    if _google_ads_available:
        return True
    try:
        from google.ads.googleads.client import GoogleAdsClient
        config = {
            "developer_token":   GOOGLE_ADS_CONFIG["developer_token"],
            "client_id":         GOOGLE_ADS_CONFIG["client_id"],
            "client_secret":     GOOGLE_ADS_CONFIG["client_secret"],
            "refresh_token":     GOOGLE_ADS_CONFIG["refresh_token"],
            # FIX 1: login_customer_id is REQUIRED for KeywordPlanIdeaService
            "login_customer_id": GOOGLE_ADS_CONFIG["customer_id"].replace("-", ""),
            "use_proto_plus":    True,
        }
        _google_ads_client    = GoogleAdsClient.load_from_dict(config)
        _google_ads_available = True
        logger.info("Google Ads Keyword Planner: Connected.")
        return True
    except ImportError:
        logger.warning("google-ads not installed. pip install google-ads")
        return False
    except Exception as e:
        logger.error(f"Google Ads setup error: {e}")
        return False


def get_keyword_metrics_google(keywords_list: list,
                                language_id: str = "1000",
                                geo_target:  str = "2356") -> dict:
    """Fetch search volume, CPC, competition from Google Ads Keyword Planner API."""
    if not _google_ads_available or not _google_ads_client:
        return {}
    customer_id = GOOGLE_ADS_CONFIG["customer_id"].replace("-", "")
    results: dict = {}
    try:
        kp_service = _google_ads_client.get_service("KeywordPlanIdeaService")
        kp_network = _google_ads_client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
        comp_map   = {0: "UNSPECIFIED", 1: "UNKNOWN", 2: "LOW", 3: "MEDIUM", 4: "HIGH"}

        for batch_start in range(0, len(keywords_list), 20):
            batch = keywords_list[batch_start: batch_start + 20]
            try:
                # FIX 2: Build a fresh request per batch; use extend() not clear()+append()
                request = _google_ads_client.get_type("GenerateKeywordIdeasRequest")
                request.customer_id          = customer_id
                request.language             = f"languageConstants/{language_id}"
                request.keyword_plan_network = kp_network
                request.include_adult_keywords = False
                request.geo_target_constants.append(f"geoTargetConstants/{geo_target}")
                # FIX 3: extend() works correctly across all proto-plus versions
                request.keyword_seed.keywords.extend(batch)

                response = kp_service.generate_keyword_ideas(request=request)
                for idea in response.results:
                    m = idea.keyword_idea_metrics
                    results[idea.text.lower()] = {
                        "search_volume":     int(m.avg_monthly_searches or 0),
                        "cpc":               round(m.average_cpc_micros / 1_000_000, 2) if m.average_cpc_micros else 0.0,
                        "competition":       comp_map.get(int(m.competition), "UNKNOWN"),
                        "competition_index": round(m.competition_index / 100, 4) if m.competition_index else 0.0,
                    }
                logger.info(f"KW Planner batch {batch_start//20+1}: {len(batch)} keywords processed.")
                import time; time.sleep(1)

            except Exception as be:
                err = str(be)
                # FIX 4: Detect GRPC version mismatch — abort all batches immediately
                if "GRPC target method" in err:
                    logger.error(
                        "Google Ads GRPC version mismatch — your library routes to a different "
                        "API version than installed. Fix: pip3 install --upgrade google-ads"
                    )
                    return {}
                logger.error(f"KW Planner batch error: {err[:200]}")
                continue

    except Exception as e:
        logger.error(f"Google Ads Keyword Planner error: {e}")
    return results


# =============================================================================
# KEYWORD PLANNER PIPELINE — tries Google Ads first, falls back to AI
# =============================================================================

GOOGLE_ADS_GEO_MAP = {
    "india":         "2356",
    "us":            "2840",
    "usa":           "2840",
    "united states": "2840",
    "uk":            "2826",
    "united kingdom":"2826",
    "australia":     "2036",
    "canada":        "2124",
    "germany":       "2276",
    "france":        "2250",
    "singapore":     "2702",
    "uae":           "2784",
    "south africa":  "2710",
}


def ai_keyword_planner_pipeline(keyword_data: dict, brand_name: str, location: str = "Global") -> list:
    if not _has_client() and not keyword_data.get("services"):
        return []

    all_keywords: set = set()
    service_map: dict = {}
    type_map:    dict = {}

    for svc in keyword_data.get("services", []):
        svc_name = svc.get("service", "")
        for ktype, kfield in [("primary","primary"),("secondary","secondary"),
                               ("short_tail","short_tail"),("long_tail","long_tail")]:
            vals = svc.get(kfield, [])
            if not isinstance(vals, list):
                vals = [vals] if vals else []
            for kw in vals:
                if kw:
                    all_keywords.add(kw)
                    service_map[kw] = svc_name
                    type_map[kw]    = ktype
        for kw in svc.get("keywords", []):
            if kw:
                all_keywords.add(kw)
                service_map[kw] = svc_name
                type_map.setdefault(kw, "service")

    for bkw in [brand_name, f"{brand_name} services", f"{brand_name} reviews",
                f"{brand_name} pricing", f"{brand_name} near me",
                f"{brand_name} {location}", f"{brand_name} alternatives",
                f"best {brand_name} services"]:
        if bkw.strip():
            all_keywords.add(bkw)
            service_map[bkw] = "Brand"
            type_map[bkw]    = "brand"

    if len(all_keywords) < 100 and _has_client():
        needed        = 100 - len(all_keywords)
        services_list = ", ".join([s["service"] for s in keyword_data.get("services", [])])
        existing_list = "\n".join(list(all_keywords)[:50])
        prompt = f"""Generate {needed} additional SEO keywords. Mix high/medium/low competition.
Business: {brand_name} | Location: {location} | Services: {services_list}
Existing (do NOT duplicate):
{existing_list}
Include: transactional, informational, comparison, how-to, best, near-me, questions.
Return ONLY JSON: [{{"keyword":"text","type":"primary|secondary|short_tail|long_tail|brand","service":"Name","competition_estimate":"HIGH|MEDIUM|LOW"}}]"""
        try:
            for item in _parse_arr(ai_chat(prompt, max_tokens=3000, temperature=0.4, use_sonnet=True)):
                kw = item.get("keyword", "")
                if kw and kw not in all_keywords:
                    all_keywords.add(kw)
                    service_map[kw] = item.get("service", "General")
                    type_map[kw]    = item.get("type", "secondary")
        except Exception as e:
            logger.error(f"ai_keyword_planner expansion: {e}")

    keyword_list = list(all_keywords)[:100]
    logger.info(f"ai_keyword_planner_pipeline: {len(keyword_list)} keywords collected")

    all_metrics: dict = {}
    google_ads_used   = False

    setup_google_ads()

    if _google_ads_available:
        geo_target = "2356"
        loc_lower  = location.lower()
        for loc_key, geo_id in GOOGLE_ADS_GEO_MAP.items():
            if loc_key in loc_lower:
                geo_target = geo_id
                break
        google_metrics = get_keyword_metrics_google(keyword_list, geo_target=geo_target)
        if google_metrics:
            all_metrics    = google_metrics
            google_ads_used = True
            logger.info(f"Google Ads metrics fetched for {len(google_metrics)} keywords")

    if not google_ads_used:
        logger.info("Google Ads unavailable — using AI metric estimation fallback")
        for batch_start in range(0, len(keyword_list), 25):
            batch       = keyword_list[batch_start: batch_start + 25]
            kw_list_str = "\n".join(f"{i+1}. {kw}" for i, kw in enumerate(batch))
            prompt = f"""Estimate monthly search volume, CPC (USD), competition level, and intent for each keyword for {location}.
Keywords:
{kw_list_str}
Return ONLY valid JSON (no markdown):
{{"keywords":[{{"keyword":"text","search_volume":1000,"cpc":1.50,"competition":"HIGH","competition_index":0.75,"intent":"informational"}}]}}
Rules: search_volume=int, cpc=decimal, competition=HIGH|MEDIUM|LOW, intent=informational|transactional|navigational|commercial"""
            try:
                for item in _parse_obj(ai_chat(prompt, max_tokens=2000, temperature=0.3,
                                               use_sonnet=False)).get("keywords", []):
                    kw_key = item.get("keyword", "").lower()
                    if kw_key:
                        all_metrics[kw_key] = {
                            "search_volume":     int(item.get("search_volume", 0) or 0),
                            "cpc":               float(item.get("cpc", 0.0) or 0.0),
                            "competition":       item.get("competition", "MEDIUM"),
                            "competition_index": float(item.get("competition_index", 0.5) or 0.5),
                            "intent":            item.get("intent", "informational"),
                        }
            except Exception as e:
                logger.error(f"ai_keyword_planner batch: {e}")

    keywords_full = []
    for kw in keyword_list:
        m        = all_metrics.get(kw.lower(), {})
        is_brand = type_map.get(kw, "") == "brand" or brand_name.lower() in kw.lower()
        keywords_full.append({
            "keyword":           kw,
            "keyword_type":      type_map.get(kw, "secondary"),
            "competition_level": m.get("competition", "MEDIUM"),
            "search_volume":     int(m.get("search_volume", 0) or 0),
            "cpc":               float(m.get("cpc", 0.0) or 0.0),
            "competition_index": float(m.get("competition_index", 0.5) or 0.5),
            "is_brand_keyword":  1 if is_brand else 0,
            "service_name":      service_map.get(kw, ""),
            "intent":            m.get("intent", "informational"),
        })

    max_vol   = max((k["search_volume"] for k in keywords_full), default=1) or 1
    max_cpc   = max((k["cpc"]           for k in keywords_full), default=1) or 1
    intent_sc = {"transactional": 1.0, "commercial": 0.8, "informational": 0.5, "navigational": 0.3}

    for kw in keywords_full:
        kw["rank_score"] = round(
            (kw["search_volume"] / max_vol) * 40 +
            (kw["cpc"]           / max_cpc) * 20 +
            {"LOW": 20, "MEDIUM": 10, "HIGH": 5}.get(kw["competition_level"], 10) +
            intent_sc.get(kw["intent"], 0.5) * 20, 2
        )

    keywords_full.sort(key=lambda x: x.get("rank_score", 0), reverse=True)

    for i, kw in enumerate(keywords_full, 1):
        kw["keyword_rank"] = i
        kw["keyword_difficulty"] = (
            "Hard"   if kw["competition_level"] == "HIGH" and kw["search_volume"] > 1000 else
            "Easy"   if kw["competition_level"] == "LOW"  or  kw["search_volume"] < 100  else
            "Medium"
        )
        is_br  = kw.get("is_brand_keyword", 0)
        comp   = kw["competition_level"]
        intent = kw["intent"]
        if   is_br:                                                         kw["keyword_category"] = "branded"
        elif comp == "HIGH" and intent in ("transactional","commercial"):   kw["keyword_category"] = "high_competition_commercial"
        elif comp == "HIGH":                                                kw["keyword_category"] = "high_competition"
        elif intent in ("transactional","commercial"):                      kw["keyword_category"] = "commercial"
        elif intent == "informational":                                     kw["keyword_category"] = "informational"
        elif comp == "LOW":                                                 kw["keyword_category"] = "low_competition_opportunity"
        else:                                                               kw["keyword_category"] = "medium_competition"

    source = "Google Ads" if google_ads_used else "AI-estimated"
    high = sum(1 for k in keywords_full if k["competition_level"] == "HIGH")
    med  = sum(1 for k in keywords_full if k["competition_level"] == "MEDIUM")
    low  = sum(1 for k in keywords_full if k["competition_level"] == "LOW")
    logger.info(f"keyword_planner done [{source}]: {len(keywords_full)} | High:{high} Med:{med} Low:{low}")
    return keywords_full


def ai_generate_llm_prompts(keyword_data: dict, keywords_ranked: list,
                             brand_name: str, location: str = "Global") -> list:
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
            continue
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