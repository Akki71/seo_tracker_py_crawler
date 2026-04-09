"""
ai_helpers.py — All AI provider calls.
Compatible with: openai==1.59.0 + anthropic==0.40.0 + httpx==0.28.1
"""
import os, json, re, logging
from typing import Optional

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
    """Initialize AI clients. Must be called before any ai_chat calls."""
    global openai_client, anthropic_client, _ai_mode, OPENAI_API_KEY, ANTHROPIC_API_KEY
    _ai_mode = mode

    # Re-read keys — env may have been loaded after module import
    OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", OPENAI_API_KEY)
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)

    if mode == "4":
        logger.info("AI mode=4: skipping AI")
        return

    if mode in ("1", "3") and OPENAI_API_KEY:
        try:
            from openai import OpenAI
            openai_client = OpenAI(api_key=OPENAI_API_KEY)
            logger.info("OpenAI client ready ✓")
        except TypeError as e:
            logger.error(f"OpenAI TypeError — upgrade: pip install openai==1.59.0 httpx==0.28.1 — {e}")
        except Exception as e:
            logger.error(f"OpenAI init: {e}")

    if mode in ("2", "3") and ANTHROPIC_API_KEY:
        try:
            import anthropic as _ant
            anthropic_client = _ant.Anthropic(api_key=ANTHROPIC_API_KEY)
            logger.info("Anthropic client ready ✓")
        except TypeError as e:
            logger.error(f"Anthropic TypeError — upgrade: pip install anthropic==0.40.0 httpx==0.28.1 — {e}")
        except Exception as e:
            logger.error(f"Anthropic init: {e}")

    if not openai_client and not anthropic_client:
        logger.warning(
            f"AI mode={mode} but NO clients initialized.\n"
            f"  Fix: pip install openai==1.59.0 anthropic==0.40.0 httpx==0.28.1\n"
            f"  Then restart the server."
        )


def _has_client() -> bool:
    return bool(openai_client or anthropic_client)


def ai_chat(prompt: str, max_tokens: int = 1024, temperature: float = 0.3,
            use_sonnet: bool = False) -> str:
    if not _has_client():
        return ""
    try:
        if _ai_mode == "1":
            if not openai_client: return ""
            r = openai_client.chat.completions.create(
                model=GPT4O_MINI,
                messages=[{"role":"user","content":prompt}],
                temperature=temperature, max_tokens=max_tokens,
            )
            return r.choices[0].message.content or ""

        elif _ai_mode == "2":
            if not anthropic_client: return ""
            import anthropic as _ant
            r = anthropic_client.messages.create(
                model=CLAUDE_SONNET if use_sonnet else CLAUDE_HAIKU,
                max_tokens=max_tokens,
                messages=[{"role":"user","content":prompt}],
                temperature=temperature,
            )
            return r.content[0].text if r.content else ""

        elif _ai_mode == "3":
            if use_sonnet and anthropic_client:
                import anthropic as _ant
                r = anthropic_client.messages.create(
                    model=CLAUDE_SONNET, max_tokens=max_tokens,
                    messages=[{"role":"user","content":prompt}],
                    temperature=temperature,
                )
                return r.content[0].text if r.content else ""
            elif openai_client:
                r = openai_client.chat.completions.create(
                    model=GPT4O_MINI,
                    messages=[{"role":"user","content":prompt}],
                    temperature=temperature, max_tokens=max_tokens,
                )
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
    prompt = f"""SEO analyst. Return ONLY valid JSON (no markdown):
{{"primary_keyword":"kw","secondary_keywords":["k1","k2","k3"],"short_tail_keywords":["s1","s2"],"long_tail_keywords":["l1","l2"],"meta_title":"title 30-60 chars","meta_description":"desc 120-155 chars","h1":"H1 text","og_title":"og title","og_description":"og desc","og_image_url":"description","schema_type":"Schema type","schema_code_snippet":"JSON-LD","optimized_url":"url-slug","image_optimization_tips":"tips"}}
URL:{url} Title({len(title)}ch):{title} Meta({len(meta_desc)}ch):{meta_desc[:150]} H1:{h1}
Content:{content[:1000]}"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=800, temperature=0.3))
    except Exception as e: logger.error(f"ai_analysis {url}: {e}"); return {}


def ai_aeo_faq(url, title, h1, content, primary_keyword, location) -> list:
    if not _has_client(): return []
    prompt = f"""5 FAQ pairs for AEO. Return ONLY JSON array (no markdown):
[{{"question":"...","answer":"..."}}]
URL:{url} Title:{title} KW:{primary_keyword} Loc:{location}
Content:{content[:800]}"""
    try: return _parse_arr(ai_chat(prompt, max_tokens=600, temperature=0.3))
    except Exception as e: logger.error(f"ai_aeo_faq {url}: {e}"); return []


def ai_body_copy_guidance(url, title, h1, content, keyword, word_count, location) -> dict:
    if not _has_client(): return {}
    prompt = f"""SEO body copy guidance. Return ONLY JSON (no markdown):
{{"ideal_word_count":1200,"content_gap":"gaps","opening_hook":"hook","recommended_sections":["H1","H2"],"cta_recommendation":"cta","tone_guidance":"tone","keyword_placement":"placement","readability_tips":"tips","e_e_a_t_signals":"signals","internal_link_anchors":"anchors"}}
URL:{url} KW:{keyword} Words:{word_count} Location:{location}
Content:{content[:800]}"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=600, temperature=0.3))
    except Exception as e: logger.error(f"ai_body_copy {url}: {e}"); return {}


# ── Site-wide AI ───────────────────────────────────────────────────────────────

def ai_site_recommendations(domain, summary, pages_sample) -> str:
    if not _has_client(): return ""
    sample = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | score:{p.get('seo_score','N/A')}"
        for p in pages_sample[:10]
    )
    prompt = f"""Senior SEO strategist. 600-800 word report covering:
1. Overall Site Health  2. Google Algorithm Compliance (E-E-A-T, CWV)
3. AEO Recommendations  4. GEO Recommendations  5. Content Strategy  6. Technical SEO

Website:{domain}  Summary:{json.dumps(summary, default=str)}
Pages:{sample}"""
    try: return ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True)
    except Exception as e: logger.error(f"ai_site_recommendations: {e}"); return ""


def ai_keyword_analysis(content, brand, location="Global") -> dict:
    if not _has_client(): return {}
    prompt = f"""World-class SEO expert. Detect 3-8 services. Return ONLY valid JSON (no markdown):
{{"business_type":"category","target_location":"{location}","services":[{{"service":"Name","keywords":["k1","k2","k3","k4","k5"],"primary":"main kw","secondary":["s1","s2","s3"],"short_tail":["st1","st2"],"long_tail":["lt1","lt2"]}}]}}
Brand:{brand} Location:{location}
Content:{content[:8000]}"""
    try:
        raw = ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True)
        result = _parse_obj(raw)
        if result.get("services"):
            logger.info(f"Keyword analysis: {len(result['services'])} services detected")
        return result
    except Exception as e: logger.error(f"ai_keyword_analysis: {e}"); return {}


def ai_blog_topics(kw_json, brand, location="Global") -> list:
    if not _has_client() or not kw_json.get("services"): return []
    all_topics = []
    for i in range(0, len(kw_json["services"]), 3):
        batch = kw_json["services"][i:i+3]
        svc_summary = "\n".join(
            f"- {s['service']}: {', '.join(s.get('keywords',[])[:4])}" for s in batch
        )
        prompt = f"""Blog topics for services. Per service: 3 informational, 2 commercial, 1 local.
Return ONLY JSON array (no markdown):
[{{"service":"Name","topics":[{{"title":"Title","type":"informational|commercial|local","target_keyword":"kw","description":"brief"}}]}}]
Services:{svc_summary} Location:{location}"""
        try:
            parsed = _parse_arr(ai_chat(prompt, max_tokens=1500, temperature=0.4, use_sonnet=True))
            if parsed: all_topics.extend(parsed)
        except Exception as e: logger.error(f"ai_blog_topics batch: {e}")
    logger.info(f"Blog topics: {sum(len(s.get('topics',[])) for s in all_topics)} topics generated")
    return all_topics


def ai_backlink_strategy(kw_json, brand, domain, location="Global") -> dict:
    if not _has_client() or not kw_json.get("services"): return {}
    svcs = ", ".join(s["service"] for s in kw_json["services"])
    prompt = f"""Expert link building strategist. Return ONLY valid JSON (no markdown):
{{"seo_backlinks":[{{"strategy":"name","description":"how","priority":"High|Medium|Low","difficulty":"Easy|Medium|Hard","target_domains":["domain.com (DA XX)"]}}],"aeo_backlinks":[...],"geo_backlinks":[...],"pr_backlinks":[...],"avoid_backlinks":[{{"type":"type","reason":"why","risk_level":"High|Critical"}}]}}
Business:{brand} Domain:{domain} Location:{location}
Services:{svcs}"""
    try:
        result = _parse_obj(ai_chat(prompt, max_tokens=3500, temperature=0.3, use_sonnet=True))
        total = sum(len(result.get(k,[])) for k in ["seo_backlinks","aeo_backlinks","geo_backlinks","pr_backlinks","avoid_backlinks"])
        logger.info(f"Backlink strategy: {total} strategies generated")
        return result
    except Exception as e: logger.error(f"ai_backlink_strategy: {e}"); return {}


def ai_six_month_plan(kw_json, bl_data, brand, domain, summary) -> dict:
    if not _has_client(): return {}
    from datetime import datetime
    svcs = ", ".join(s["service"] for s in kw_json.get("services", []))
    m = datetime.now().month; y = datetime.now().year
    prompt = f"""Senior SEO project manager. 6-month SEO plan. Return ONLY valid JSON (no markdown):
{{"plan_start":"{m}/{y}","months":[{{"month_number":1,"month_label":"Month 1","focus":"focus","tasks":[{{"task":"task","category":"Technical|Content|Backlinks|Analytics"}}],"expected_output":{{"organic_traffic_change":"+X%","keywords_improved":"X kw","backlinks_target":"X","pages_optimized":"X","content_published":"X","technical_fixes":"X","summary":"summary"}}}}]}}
Business:{brand} Domain:{domain} Services:{svcs}
Audit summary:{json.dumps(summary, default=str)}"""
    try:
        result = _parse_obj(ai_chat(prompt, max_tokens=3500, temperature=0.4, use_sonnet=True))
        logger.info(f"6-month plan: {len(result.get('months',[]))} months generated")
        return result
    except Exception as e: logger.error(f"ai_six_month_plan: {e}"); return {}


def ai_internal_linking_strategy(pages, domain) -> dict:
    if not _has_client() or not pages: return {}
    ps = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | KW:{p.get('primary_keyword','')} | words:{p.get('word_count',0)}"
        for p in pages[:25]
    )
    prompt = f"""Internal linking expert. Return ONLY valid JSON (no markdown):
{{"hub_pages":[{{"url":"url","topic_cluster":"cluster","reason":"why"}}],"linking_map":[{{"from_url":"from","to_url":"to","anchor_text":"anchor","context":"where"}}],"orphan_pages":[{{"url":"url","link_from":"source","anchor_text":"anchor"}}],"topic_silos":[{{"silo_name":"name","pages":["url"],"hub_url":"url"}}],"navigation_suggestions":["sug"],"overall_score":"Good|Needs Work|Poor","priority_actions":["action"]}}
Website:{domain}
Pages:{ps}"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=2500, temperature=0.3, use_sonnet=True))
    except Exception as e: logger.error(f"ai_internal_linking: {e}"); return {}


def ai_keyword_url_mapping(pages, kw_data, domain, location="Global") -> list:
    if not _has_client() or not pages: return []
    ps = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | words:{p.get('word_count',0)}"
        for p in pages[:20]
    )
    kw = json.dumps([{"service":s.get("service",""),"primary":s.get("primary",""),
                       "short_tail":s.get("short_tail",[]),"long_tail":s.get("long_tail",[])}
                      for s in kw_data.get("services",[])], default=str)[:2000]
    prompt = f"""SEO keyword mapping expert. Return ONLY JSON array (no markdown):
[{{"keyword":"kw","keyword_type":"primary|secondary|short_tail|long_tail","service":"svc","mapped_url":"url","match_confidence":"High|Medium|Low","reason":"why","on_page_action":"action","create_new_page":false,"suggested_new_url":""}}]
Website:{domain} Location:{location}
Pages:{ps}
Keywords:{kw}"""
    try: return _parse_arr(ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True))
    except Exception as e: logger.error(f"ai_keyword_url_mapping: {e}"); return []


def ai_axo_recommendations(pages, kw_data, domain, location="Global") -> dict:
    if not _has_client() or not pages: return {}
    ps = "\n".join(
        f"- {p.get('url','')} | schema:{p.get('schema_types_found','')} | words:{p.get('word_count',0)}"
        for p in pages[:15]
    )
    svcs = json.dumps([s.get("service","") for s in kw_data.get("services",[])])
    prompt = f"""AXO expert. Return ONLY valid JSON (no markdown):
{{"axo_score":65,"axo_grade":"C","aeo_recommendations":[{{"action":"action","priority":"Critical|High|Medium|Low","impact":"impact","implementation":"how"}}],"geo_recommendations":[...],"voice_search_recommendations":[...],"conversational_ai_recommendations":[...],"llms_txt_recommendation":"rec","entity_optimization":"how","citation_worthiness":"how","priority_roadmap":["step1","step2"]}}
Website:{domain} Location:{location}
Pages:{ps}
Services:{svcs}"""
    try: return _parse_obj(ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True))
    except Exception as e: logger.error(f"ai_axo_recommendations: {e}"); return {}


def ai_alt_recommendations(images_missing: list) -> dict:
    if not _has_client() or not images_missing: return {}
    batch = images_missing[:40]
    img_list = "\n".join(f"{i+1}. Page:{img['page']}\n   URL:{img['src']}" for i,img in enumerate(batch))
    prompt = f"""ALT text for images (8-15 words each). Return ONLY JSON object (no markdown):
{{"1":"alt text","2":"alt text"}}
Images:{img_list}"""
    try:
        result = {}
        for k,v in _parse_obj(ai_chat(prompt, max_tokens=800, temperature=0.4)).items():
            idx = int(k) - 1
            if 0 <= idx < len(batch): result[batch[idx]["src"]] = v
        return result
    except Exception as e: logger.error(f"ai_alt_recommendations: {e}"); return {}
