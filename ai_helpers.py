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
    """Universal AI call with automatic fallback.
    mode=1: OpenAI only
    mode=2: Claude only
    mode=3: Hybrid — Claude Sonnet for strategy, GPT-4o-mini for bulk.
            If Anthropic fails (credit/rate limit), automatically falls back to OpenAI.
    """
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
            r = anthropic_client.messages.create(
                model=CLAUDE_SONNET if use_sonnet else CLAUDE_HAIKU,
                max_tokens=max_tokens,
                messages=[{"role":"user","content":prompt}],
                temperature=temperature,
            )
            return r.content[0].text if r.content else ""

        elif _ai_mode == "3":
            # Hybrid: Anthropic for strategy (use_sonnet=True), OpenAI for bulk
            if use_sonnet and anthropic_client:
                try:
                    r = anthropic_client.messages.create(
                        model=CLAUDE_SONNET, max_tokens=max_tokens,
                        messages=[{"role":"user","content":prompt}],
                        temperature=temperature,
                    )
                    return r.content[0].text if r.content else ""
                except Exception as _ant_err:
                    # Anthropic unavailable (credit/rate limit) — fall back to OpenAI
                    logger.warning(f"Anthropic unavailable: {str(_ant_err)[:100]} — using OpenAI fallback")
                    if openai_client:
                        r = openai_client.chat.completions.create(
                            model=GPT4O_MINI,
                            messages=[{"role":"user","content":prompt}],
                            temperature=temperature, max_tokens=max_tokens,
                        )
                        return r.choices[0].message.content or ""
                    return ""
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
    """Map every keyword to its best matching page. Returns one row per keyword."""
    if not _has_client() or not pages: return []
    ps = "\n".join(
        f"- {p.get('url','')} | {p.get('current_title','')} | words:{p.get('word_count',0)}"
        for p in pages[:50]
    )
    # Flatten ALL keywords across all services
    all_kw_rows = []
    for s in kw_data.get("services", []):
        svc = s.get("service", "")
        for kw in [s.get("primary", "")] + s.get("keywords", []) + s.get("short_tail", []) + s.get("long_tail", []):
            if kw:
                ktype = "primary" if kw == s.get("primary") else "secondary"
                all_kw_rows.append({"service": svc, "keyword": kw, "keyword_type": ktype})
    if not all_kw_rows:
        return []
    kw_list = json.dumps(all_kw_rows[:60], default=str)
    prompt = f"""SEO keyword-to-URL mapping expert.
Map each keyword to the best existing page URL. Return ONLY a JSON array (no markdown):
[{{"keyword":"kw","keyword_type":"primary|secondary|short_tail|long_tail","service_name":"service","mapped_url":"full_url","match_confidence":"High|Medium|Low","reason":"1 sentence why","on_page_action":"what to optimize on that page","create_new_page":false,"suggested_new_url":""}}]
Rules: Every keyword must map to a URL. If no match, set create_new_page=true.
Website:{domain} Location:{location}
Pages:
{ps}
Keywords: {kw_list}"""
    try:
        result = _parse_arr(ai_chat(prompt, max_tokens=4000, temperature=0.2, use_sonnet=True))
        logger.info(f"ai_keyword_url_mapping: {len(result)} mappings generated")
        return result
    except Exception as e:
        logger.error(f"ai_keyword_url_mapping: {e}")
        return []


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



def ai_new_page_suggestions(pages_data: list, keyword_data: dict,
                              domain: str, brand: str, location: str = "Global") -> list:
    """Suggest NEW pages to create based on keyword gaps and site structure."""
    if not _has_client() or not pages_data:
        return []
    existing_urls = [p.get("url", "") for p in pages_data[:50]]
    services = [s.get("service", "") for s in keyword_data.get("services", [])]
    all_kws = []
    for svc in keyword_data.get("services", []):
        all_kws.extend(svc.get("keywords", [])[:5])
    existing_list = "\n".join(f"- {u}" for u in existing_urls[:30])
    svc_str = ", ".join(services)
    kw_str  = ", ".join(all_kws[:15])
    prompt = (
        f"SEO strategist. Suggest 5-10 NEW pages to create to fill SEO gaps.\n"
        f"Return ONLY JSON array (no markdown):\n"
        f'[{{"url":"/slug","title":"Title 30-60 chars","page_type":"service|location|blog|resource|landing",'
        f'"reason":"why needed","target_keyword":"keyword","content_outline":["S1","S2","S3","S4","S5"],'
        f'"priority":"high|medium|low"}}]\n'
        f"Website:{domain} Brand:{brand} Location:{location}\n"
        f"Services:{svc_str}\n"
        f"Keywords:{kw_str}\n"
        f"Existing pages:\n{existing_list}"
    )
    try:
        result = _parse_arr(ai_chat(prompt, max_tokens=3000, temperature=0.4, use_sonnet=True))
        logger.info(f"ai_new_page_suggestions: {len(result)} pages recommended")
        return result if isinstance(result, list) else []
    except Exception as e:
        logger.error(f"ai_new_page_suggestions: {e}")
        return []



def ai_keyword_planner_pipeline(keyword_data: dict, brand_name: str, location: str = "Global") -> list:
    """
    Generate 100 keywords with AI-estimated search volume, CPC, competition, intent.
    Mirrors crawler_2.py keyword_planner_pipeline but uses AI estimation (no Google Ads required).
    Returns list of keyword dicts ranked by composite score.
    """
    if not _has_client() or not keyword_data.get("services"):
        return []

    # Collect all keywords from services
    all_keywords = set()
    service_map = {}
    type_map = {}

    for svc in keyword_data["services"]:
        svc_name = svc.get("service", "")
        for ktype, kfield in [("primary","primary"),("secondary","secondary"),
                               ("short_tail","short_tail"),("long_tail","long_tail")]:
            vals = svc.get(kfield, []) if isinstance(svc.get(kfield), list) else [svc.get(kfield, "")]
            for kw in vals:
                if kw:
                    all_keywords.add(kw)
                    service_map[kw] = svc_name
                    type_map[kw] = ktype
        for kw in svc.get("keywords", []):
            if kw:
                all_keywords.add(kw)
                service_map[kw] = svc_name
                if kw not in type_map:
                    type_map[kw] = "service"

    # Brand keywords
    for bkw in [brand_name, f"{brand_name} services", f"{brand_name} reviews",
                 f"{brand_name} pricing", f"{brand_name} near me"]:
        if bkw.strip():
            all_keywords.add(bkw)
            service_map[bkw] = "Brand"
            type_map[bkw] = "brand"

    keyword_list = list(all_keywords)[:100]

    # AI estimate metrics in batches of 25
    all_metrics = {}
    for batch_start in range(0, len(keyword_list), 25):
        batch = keyword_list[batch_start:batch_start + 25]
        kw_list_str = "\n".join(f"{i+1}. {kw}" for i, kw in enumerate(batch))
        prompt = f"""Estimate monthly search volume, CPC (USD), competition level, and intent for each keyword for {location}.
Keywords:
{kw_list_str}
Return ONLY valid JSON (no markdown):
{{"keywords":[{{"keyword":"text","search_volume":1000,"cpc":1.50,"competition":"HIGH","competition_index":0.75,"intent":"informational"}}]}}
Rules: search_volume=int, cpc=decimal, competition=HIGH|MEDIUM|LOW, intent=informational|transactional|navigational|commercial"""
        try:
            raw = ai_chat(prompt, max_tokens=2000, temperature=0.3, use_sonnet=False)
            result = _parse_obj(raw)
            for item in result.get("keywords", []):
                all_metrics[item.get("keyword","").lower()] = item
        except Exception as e:
            logger.error(f"ai_keyword_planner_pipeline batch error: {e}")

    # Build ranked keyword list
    keywords_full = []
    for kw in keyword_list:
        m = all_metrics.get(kw.lower(), {})
        is_brand = type_map.get(kw,"") == "brand" or brand_name.lower() in kw.lower()
        keywords_full.append({
            "keyword":          kw,
            "keyword_type":     type_map.get(kw, "secondary"),
            "competition_level":m.get("competition", "MEDIUM"),
            "search_volume":    int(m.get("search_volume", 0) or 0),
            "cpc":              float(m.get("cpc", 0.0) or 0.0),
            "competition_index":float(m.get("competition_index", 0.5) or 0.5),
            "is_brand_keyword": 1 if is_brand else 0,
            "service_name":     service_map.get(kw, ""),
            "intent":           m.get("intent", "informational"),
        })

    # Rank by composite score
    max_vol = max((k["search_volume"] for k in keywords_full), default=1) or 1
    max_cpc = max((k["cpc"] for k in keywords_full), default=1) or 1
    intent_scores = {"transactional":1.0,"commercial":0.8,"informational":0.5,"navigational":0.3}
    for kw in keywords_full:
        vol_score   = (kw["search_volume"] / max_vol) * 40
        cpc_score   = (kw["cpc"] / max_cpc) * 20
        comp_score  = {"LOW":20,"MEDIUM":10,"HIGH":5}.get(kw["competition_level"], 10)
        intent_score= intent_scores.get(kw["intent"], 0.5) * 20
        kw["rank_score"] = round(vol_score + cpc_score + comp_score + intent_score, 2)

    keywords_full.sort(key=lambda x: x.get("rank_score", 0), reverse=True)
    for i, kw in enumerate(keywords_full, 1):
        kw["keyword_rank"] = i
        if kw["competition_level"] == "HIGH" and kw["search_volume"] > 1000:
            kw["keyword_difficulty"] = "Hard"
        elif kw["competition_level"] == "LOW" or kw["search_volume"] < 100:
            kw["keyword_difficulty"] = "Easy"
        else:
            kw["keyword_difficulty"] = "Medium"
        # keyword_category
        is_brand = kw.get("is_brand_keyword", 0)
        comp = kw["competition_level"]
        intent = kw["intent"]
        if is_brand:
            kw["keyword_category"] = "branded"
        elif comp == "HIGH" and intent in ("transactional", "commercial"):
            kw["keyword_category"] = "high_competition_commercial"
        elif comp == "HIGH":
            kw["keyword_category"] = "high_competition"
        elif intent in ("transactional", "commercial"):
            kw["keyword_category"] = "commercial"
        elif intent == "informational":
            kw["keyword_category"] = "informational"
        elif comp == "LOW":
            kw["keyword_category"] = "low_competition_opportunity"
        else:
            kw["keyword_category"] = "medium_competition"

    logger.info(f"ai_keyword_planner_pipeline: {len(keywords_full)} keywords ranked")
    return keywords_full


def ai_generate_llm_prompts(keyword_data: dict, keywords_ranked: list,
                             brand_name: str, location: str = "Global") -> list:
    """
    Generate LLM prompts (questions users type into ChatGPT/Perplexity/Gemini)
    based on keyword data. Mirrors crawler_2.py generate_llm_prompts().
    """
    if not _has_client() or not keywords_ranked:
        return []

    services = [s.get("service","") for s in keyword_data.get("services", [])]
    business_type = keyword_data.get("business_type", "")
    brand_lower = brand_name.lower().strip()

    non_branded = [k for k in keywords_ranked
                   if not k.get("is_brand_keyword") and brand_lower not in k.get("keyword","").lower()]
    branded     = [k for k in keywords_ranked
                   if k.get("is_brand_keyword") or brand_lower in k.get("keyword","").lower()]

    high_vol = sorted([k for k in non_branded if k.get("search_volume",0) > 0],
                      key=lambda x: x.get("search_volume",0), reverse=True)[:30]

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

    # High competition prompts
    if high_vol[:15]:
        kw_list = "\n".join(f"- \"{k['keyword']}\" (Vol:{k['search_volume']}, Service:{k['service_name']})"
                             for k in high_vol[:15])
        all_prompts.extend(_call(f"""Generate search prompts for HIGH COMPETITION keywords. Location: {location}
Business Type: {business_type}
Keywords:
{kw_list}
Rules: 2 prompts per keyword. DO NOT include brand name "{brand_name}". Use natural question format.
Return ONLY JSON array:
[{{"prompt_text":"prompt","prompt_type":"high_competition","target_keyword":"kw","search_volume":1000,"ai_engine":"All","service_name":"svc","priority":"high"}}]""", "high_competition"))

    # Informational prompts
    info_kws = [k for k in high_vol if k.get("intent") == "informational"][:10]
    if info_kws:
        kw_list = "\n".join(f"- \"{k['keyword']}\""  for k in info_kws)
        all_prompts.extend(_call(f"""Generate INFORMATIONAL search prompts. Location: {location}
Keywords:
{kw_list}
Rules: 2 prompts per keyword. DO NOT include "{brand_name}". Use what/how/why format.
Return ONLY JSON array:
[{{"prompt_text":"prompt","prompt_type":"informational","target_keyword":"kw","search_volume":500,"ai_engine":"All","service_name":"svc","priority":"medium"}}]""", "informational"))

    # Branded prompts
    if branded[:10]:
        branded_list = "\n".join(f"- \"{k['keyword']}\"" for k in branded[:10])
        all_prompts.extend(_call(f"""Generate BRANDED search prompts for "{brand_name}".
Brand: {brand_name} | Location: {location} | Services: {", ".join(services[:6])}
Branded keywords:
{branded_list}
Generate 10 prompts. Every prompt MUST contain "{brand_name}".
Return ONLY JSON array:
[{{"prompt_text":"prompt with {brand_name}","prompt_type":"branded","target_keyword":"kw","search_volume":100,"ai_engine":"All","service_name":"Brand","priority":"high"}}]""", "branded"))

    # Deduplicate
    seen = set()
    final = []
    for p in all_prompts:
        text = p.get("prompt_text","").strip()
        if text and text.lower() not in seen:
            seen.add(text.lower())
            p["suggested_answer"] = ""
            vol = p.get("search_volume", 0)
            try: vol = int(vol)
            except: vol = 0
            p["search_volume"] = vol
            p["priority"] = "high" if vol >= 1000 else ("medium" if vol >= 200 else "low")
            final.append(p)

    logger.info(f"ai_generate_llm_prompts: {len(final)} total prompts generated")
    return final