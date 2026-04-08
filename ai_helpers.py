"""
ai_helpers.py — All AI provider calls extracted from the original seo_crawler.py.
Supports OpenAI, Claude (Anthropic), Hybrid, or Skip.
"""

import os, json, re, logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Client state ──────────────────────────────────────────────────────────────
openai_client    = None
anthropic_client = None
_ai_mode         = "4"   # default: skip

GPT4O_MINI     = "gpt-4o-mini"
GPT4O          = "gpt-4o"
CLAUDE_HAIKU   = os.environ.get("CLAUDE_HAIKU_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_SONNET  = os.environ.get("CLAUDE_SONNET_MODEL", "claude-sonnet-4-20250514")

OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

def setup_ai_clients(mode: str):
    global openai_client, anthropic_client, _ai_mode
    _ai_mode = mode

    if mode in ("1","3") and OPENAI_API_KEY:
        try:
            from openai import OpenAI
            openai_client = OpenAI(api_key=OPENAI_API_KEY)
            logger.info("OpenAI client ready.")
        except ImportError:
            logger.warning("openai package not installed.")

    if mode in ("2","3") and ANTHROPIC_API_KEY:
        try:
            import anthropic
            anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            logger.info("Anthropic client ready.")
        except ImportError:
            logger.warning("anthropic package not installed.")


def ai_chat(prompt: str, max_tokens: int = 1024, temperature: float = 0.3,
             use_sonnet: bool = False) -> str:
    mode = _ai_mode
    if mode == "4" or (not openai_client and not anthropic_client):
        return ""

    try:
        if mode == "1":
            if not openai_client: return ""
            resp = openai_client.chat.completions.create(
                model=GPT4O_MINI,
                messages=[{"role":"user","content":prompt}],
                temperature=temperature, max_tokens=max_tokens,
            )
            return resp.choices[0].message.content or ""

        elif mode == "2":
            if not anthropic_client: return ""
            model = CLAUDE_SONNET if use_sonnet else CLAUDE_HAIKU
            resp = anthropic_client.messages.create(
                model=model, max_tokens=max_tokens,
                messages=[{"role":"user","content":prompt}],
                temperature=temperature,
            )
            return resp.content[0].text if resp.content else ""

        elif mode == "3":
            if use_sonnet and anthropic_client:
                resp = anthropic_client.messages.create(
                    model=CLAUDE_SONNET, max_tokens=max_tokens,
                    messages=[{"role":"user","content":prompt}],
                    temperature=temperature,
                )
                return resp.content[0].text if resp.content else ""
            elif openai_client:
                resp = openai_client.chat.completions.create(
                    model=GPT4O_MINI,
                    messages=[{"role":"user","content":prompt}],
                    temperature=temperature, max_tokens=max_tokens,
                )
                return resp.choices[0].message.content or ""

    except Exception as e:
        logger.error(f"AI API error (mode={mode}, sonnet={use_sonnet}): {e}")
    return ""


def _repair_json(json_str: str) -> str:
    repaired = json_str.strip()
    repaired = re.sub(r'^```(?:json)?\s*\n?', '', repaired)
    repaired = re.sub(r'\n?```\s*$', '', repaired)
    repaired = repaired.strip()
    repaired = re.sub(r',\s*([}\]])', r'\1', repaired)
    try:
        return json.dumps(json.loads(repaired))
    except Exception:
        pass
    open_braces   = repaired.count("{") - repaired.count("}")
    open_brackets = repaired.count("[") - repaired.count("]")
    repaired += "]" * max(0, open_brackets)
    repaired += "}" * max(0, open_braces)
    repaired = re.sub(r',\s*([}\]])', r'\1', repaired)
    return repaired


def _parse_json_obj(raw: str) -> dict:
    start = raw.find("{"); end = raw.rfind("}") + 1
    if start >= 0 and end > start:
        try: return json.loads(raw[start:end])
        except Exception:
            try: return json.loads(_repair_json(raw[start:end]))
            except Exception: pass
    return {}


def _parse_json_arr(raw: str) -> list:
    start = raw.find("["); end = raw.rfind("]") + 1
    if start >= 0 and end > start:
        try: return json.loads(raw[start:end])
        except Exception:
            try: return json.loads(_repair_json(raw[start:end]))
            except Exception: pass
    return []


# ── Per-page AI ────────────────────────────────────────────────────────────────

def ai_analysis(url, title, meta_desc, h1, content_snippet) -> dict:
    if not (openai_client or anthropic_client): return {}
    prompt = f"""You are an expert SEO analyst. Return ONLY valid JSON (no markdown):

{{"primary_keyword":"main keyword","secondary_keywords":["kw1","kw2","kw3"],"short_tail_keywords":["short1","short2"],"long_tail_keywords":["lt1","lt2"],"meta_title":"optimized title 30-60 chars","meta_description":"optimized desc 120-155 chars","h1":"compelling H1","og_title":"social title 40-60 chars","og_description":"social desc 100-150 chars","og_image_url":"ideal OG image 1200x630","schema_type":"Schema.org type","schema_code_snippet":"minimal JSON-LD","optimized_url":"seo-slug","image_optimization_tips":"brief tips"}}

URL: {url}
Title ({len(title)} chars): {title}
Meta ({len(meta_desc)} chars): {meta_desc[:200]}
H1: {h1}
Content: {content_snippet[:1200]}
"""
    try:
        raw = ai_chat(prompt, max_tokens=800, temperature=0.3)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_analysis error {url}: {e}")
        return {}


def ai_aeo_faq(url, title, h1, content, primary_keyword, location) -> list:
    if not (openai_client or anthropic_client): return []
    prompt = f"""Generate 5 FAQ pairs for AEO. Return ONLY JSON array:
[{{"question":"...","answer":"..."}}]

URL: {url} | Title: {title} | H1: {h1} | KW: {primary_keyword} | Location: {location}
Content: {content[:1000]}
"""
    try:
        raw = ai_chat(prompt, max_tokens=800, temperature=0.3)
        return _parse_json_arr(raw)
    except Exception as e:
        logger.error(f"ai_aeo_faq error {url}: {e}")
        return []


def ai_body_copy_guidance(url, title, h1, content, primary_keyword, word_count, location) -> dict:
    if not (openai_client or anthropic_client): return {}
    prompt = f"""SEO content guidance. Return ONLY JSON:
{{"ideal_word_count":1200,"content_gap":"gaps","opening_hook":"hook","recommended_sections":["H1","H2"],"cta_recommendation":"CTA","tone_guidance":"tone","keyword_placement":"placement","readability_tips":"tips","e_e_a_t_signals":"signals","internal_link_anchors":"anchors"}}

URL: {url} | Title: {title} | KW: {primary_keyword} | Words: {word_count}
"""
    try:
        raw = ai_chat(prompt, max_tokens=800, temperature=0.3)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_body_copy error {url}: {e}")
        return {}


# ── Site-wide AI ───────────────────────────────────────────────────────────────

def ai_site_recommendations(domain, summary_data, pages_sample) -> str:
    if not (openai_client or anthropic_client): return ""
    sample = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | score:{p.get('seo_score','N/A')}"
        for p in pages_sample[:10]
    )
    prompt = f"""Senior SEO strategist. Write a 600-800 word site recommendation report covering:
1. Overall Site Health
2. Google Algorithm Compliance (E-E-A-T, Core Web Vitals)
3. AEO Recommendations (AI search, FAQ schema, featured snippets)
4. GEO Recommendations (structured data, entity optimization, llms.txt)
5. Content Strategy
6. Technical SEO Priorities

Website: {domain}
Summary: {json.dumps(summary_data, default=str)}
Sample Pages: {sample}
"""
    try:
        return ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True)
    except Exception as e:
        logger.error(f"ai_site_recommendations error: {e}")
        return ""


def ai_keyword_analysis(content_text, brand_name, location="Global") -> dict:
    if not (openai_client or anthropic_client): return {}
    prompt = f"""World-class SEO expert. Detect 3-8 services from the content. Return ONLY valid JSON:
{{"business_type":"category","target_location":"{location}","services":[{{"service":"Name","keywords":["k1","k2","k3","k4","k5"],"primary":"main kw","secondary":["s1","s2","s3"],"short_tail":["st1","st2"],"long_tail":["lt1","lt2"]}}]}}

Brand: {brand_name} | Location: {location}
Content: {content_text[:10000]}
"""
    try:
        raw = ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_keyword_analysis error: {e}")
        return {}


def ai_blog_topics(keyword_json, brand_name, location="Global") -> list:
    if not (openai_client or anthropic_client) or not keyword_json.get("services"): return []
    all_topics = []
    for i in range(0, len(keyword_json["services"]), 3):
        batch = keyword_json["services"][i:i+3]
        services_summary = "\n".join(
            f"- {s['service']}: {', '.join(s.get('keywords',[])[:5])}" for s in batch
        )
        prompt = f"""Blog topics for: {services_summary}
Per service: 3 informational, 2 commercial, 1 local.
Return ONLY JSON array: [{{"service":"Name","topics":[{{"title":"Title","type":"informational|commercial|local","target_keyword":"kw","description":"brief"}}]}}]
"""
        try:
            raw = ai_chat(prompt, max_tokens=2000, temperature=0.4, use_sonnet=True)
            parsed = _parse_json_arr(raw)
            if parsed: all_topics.extend(parsed)
        except Exception as e:
            logger.error(f"ai_blog_topics batch error: {e}")
    return all_topics


def ai_backlink_strategy(keyword_json, brand_name, domain_name, location="Global") -> dict:
    if not (openai_client or anthropic_client) or not keyword_json.get("services"): return {}
    services = ", ".join(s["service"] for s in keyword_json["services"])
    prompt = f"""Backlink strategy. Return ONLY valid JSON:
{{"seo_backlinks":[{{"strategy":"name","description":"how","priority":"High|Medium|Low","difficulty":"Easy|Medium|Hard","target_domains":["domain.com (DA XX)"]}}],"aeo_backlinks":[...],"geo_backlinks":[...],"pr_backlinks":[...],"avoid_backlinks":[{{"type":"type","reason":"why","risk_level":"High|Critical"}}]}}

Business: {brand_name} | Domain: {domain_name} | Location: {location}
Services: {services}
"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.3, use_sonnet=True)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_backlink_strategy error: {e}")
        return {}


def ai_six_month_plan(keyword_json, backlink_data, brand_name, domain_name, audit_summary) -> dict:
    if not (openai_client or anthropic_client): return {}
    from datetime import datetime
    services = ", ".join(s["service"] for s in keyword_json.get("services", []))
    prompt = f"""6-month SEO plan. Return ONLY valid JSON:
{{"plan_start":"MM/YYYY","months":[{{"month_number":1,"month_label":"Month 1 (Mon YYYY)","focus":"focus area","tasks":[{{"task":"task","category":"Technical|Content|Backlinks|Analytics"}}],"expected_output":{{"organic_traffic_change":"+X%","keywords_improved":"X keywords","backlinks_target":"X","pages_optimized":"X","content_published":"X","technical_fixes":"X","summary":"summary"}}}}]}}

Business: {brand_name} | Domain: {domain_name} | Services: {services}
Audit: {json.dumps(audit_summary, default=str)}
"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.4, use_sonnet=True)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_six_month_plan error: {e}")
        return {}


def ai_internal_linking_strategy(pages_data, domain_name) -> dict:
    if not (openai_client or anthropic_client) or not pages_data: return {}
    pages_summary = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | KW:{p.get('primary_keyword','')} | words:{p.get('word_count',0)}"
        for p in pages_data[:25]
    )
    prompt = f"""Internal linking strategy. Return ONLY valid JSON:
{{"hub_pages":[{{"url":"url","topic_cluster":"cluster","reason":"why"}}],"linking_map":[{{"from_url":"from","to_url":"to","anchor_text":"anchor","context":"where"}}],"orphan_pages":[{{"url":"url","link_from":"source","anchor_text":"anchor"}}],"topic_silos":[{{"silo_name":"name","pages":["url1"],"hub_url":"url"}}],"navigation_suggestions":["suggestion"],"overall_score":"Good|Needs Work|Poor","priority_actions":["action"]}}

Website: {domain_name}
Pages: {pages_summary}
"""
    try:
        raw = ai_chat(prompt, max_tokens=3000, temperature=0.3, use_sonnet=True)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_internal_linking error: {e}")
        return {}


def ai_keyword_url_mapping(pages_data, keyword_data, domain_name, location="Global") -> list:
    if not (openai_client or anthropic_client) or not pages_data: return []
    pages_summary = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | {p.get('current_h1','')} | words:{p.get('word_count',0)}"
        for p in pages_data[:25]
    )
    kw_summary = json.dumps([
        {"service": s.get("service",""), "primary": s.get("primary",""),
         "secondary": s.get("secondary",[]), "short_tail": s.get("short_tail",[]),
         "long_tail": s.get("long_tail",[])}
        for s in keyword_data.get("services", [])
    ], default=str)[:3000]
    prompt = f"""Map keywords to URLs. Return ONLY JSON array:
[{{"keyword":"kw","keyword_type":"primary|secondary|short_tail|long_tail","service":"name","mapped_url":"url","match_confidence":"High|Medium|Low","reason":"why","on_page_action":"action","create_new_page":false,"suggested_new_url":""}}]

Website: {domain_name} | Location: {location}
Pages: {pages_summary}
Keywords: {kw_summary}
"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.3, use_sonnet=True)
        return _parse_json_arr(raw)
    except Exception as e:
        logger.error(f"ai_keyword_url_mapping error: {e}")
        return []


def ai_axo_recommendations(pages_data, keyword_data, domain_name, location="Global") -> dict:
    if not (openai_client or anthropic_client) or not pages_data: return {}
    pages_summary = "\n".join(
        f"- {p.get('url','')} | schema:{p.get('schema_types_found','')} | og:{p.get('og_tags','')} | words:{p.get('word_count',0)}"
        for p in pages_data[:15]
    )
    services = json.dumps([s.get("service","") for s in keyword_data.get("services",[])])
    prompt = f"""AXO recommendations. Return ONLY valid JSON:
{{"axo_score":65,"axo_grade":"C","aeo_recommendations":[{{"action":"action","priority":"Critical|High|Medium|Low","impact":"impact","implementation":"how"}}],"geo_recommendations":[...],"voice_search_recommendations":[...],"conversational_ai_recommendations":[...],"llms_txt_recommendation":"recommendation","entity_optimization":"how","citation_worthiness":"how","priority_roadmap":["step1","step2"]}}

Website: {domain_name} | Location: {location}
Pages: {pages_summary} | Services: {services}
"""
    try:
        raw = ai_chat(prompt, max_tokens=4000, temperature=0.3, use_sonnet=True)
        return _parse_json_obj(raw)
    except Exception as e:
        logger.error(f"ai_axo_recommendations error: {e}")
        return {}


def ai_alt_recommendations(images_missing: list) -> dict:
    if not (openai_client or anthropic_client) or not images_missing: return {}
    batch = images_missing[:50]
    image_list = "\n".join(f"{i+1}. Page: {img['page']}\n   URL: {img['src']}" for i, img in enumerate(batch))
    prompt = f"""Suggest ALT text for images (8-15 words each). Return ONLY JSON object:
{{"1":"alt text for image 1","2":"alt text for image 2"}}

Images: {image_list}
"""
    try:
        raw = ai_chat(prompt, max_tokens=1000, temperature=0.4)
        result = {}
        for idx_str, alt_text in _parse_json_obj(raw).items():
            idx = int(idx_str) - 1
            if 0 <= idx < len(batch):
                result[batch[idx]["src"]] = alt_text
        return result
    except Exception as e:
        logger.error(f"ai_alt_recommendations error: {e}")
        return {}
