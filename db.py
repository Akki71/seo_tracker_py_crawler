"""
PostgreSQL database layer — replaces MySQL.
Uses psycopg2 with a simple connection pool.
All DB_CONFIG values are read from environment variables (set in Coolify).
"""

import os, json, logging
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# ── Config (all from env vars set in Coolify) ──────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "user":     os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "dbname":   os.environ.get("DB_NAME", "seo_crawler"),
}

_pool: pg_pool.SimpleConnectionPool | None = None

def _get_pool() -> pg_pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.SimpleConnectionPool(minconn=1, maxconn=10, **DB_CONFIG)
        logger.info("PostgreSQL connection pool created.")
    return _pool

def get_db_conn():
    return _get_pool().getconn()

def release_db_conn(conn):
    _get_pool().putconn(conn)

def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None

# ── Schema Creation ────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
-- Main audit record (one per crawl run)
CREATE TABLE IF NOT EXISTS audits (
    id                    SERIAL PRIMARY KEY,
    brand_id              INTEGER NOT NULL,
    domain                TEXT,
    base_url              TEXT,
    target_location       TEXT DEFAULT 'Global',
    business_type         TEXT,
    ai_mode               TEXT DEFAULT '1',
    total_pages_crawled   INTEGER DEFAULT 0,
    pages_200             INTEGER DEFAULT 0,
    pages_404             INTEGER DEFAULT 0,
    broken_links_count    INTEGER DEFAULT 0,
    images_missing_alt    INTEGER DEFAULT 0,
    robots_txt_status     TEXT,
    sitemap_status        TEXT,
    llm_txt_status        TEXT,
    gbp_status            TEXT,
    site_recommendation   TEXT,
    detected_location     TEXT,
    excel_file            TEXT,
    pdf_file              TEXT,
    audit_status          TEXT DEFAULT 'in_progress',
    audit_timestamp       TIMESTAMP DEFAULT NOW()
);

-- Per-page data
CREATE TABLE IF NOT EXISTS pages (
    id                        SERIAL PRIMARY KEY,
    audit_id                  INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    url                       TEXT,
    url_cleaned               TEXT,
    status                    TEXT,
    redirect_suggestion       TEXT,
    redirect_type             TEXT,
    redirect_target           TEXT,
    canonical_status          TEXT,
    canonical_url             TEXT,
    duplicate_status          TEXT,
    word_count                INTEGER DEFAULT 0,
    thin_content              TEXT,
    current_title             TEXT,
    title_length              INTEGER DEFAULT 0,
    current_meta_description  TEXT,
    meta_desc_length          INTEGER DEFAULT 0,
    current_h1                TEXT,
    h2_tags                   TEXT,
    google_analytics          TEXT,
    google_search_console     TEXT,
    og_tags                   TEXT,
    og_title_current          TEXT,
    og_description_current    TEXT,
    og_image_current          TEXT,
    schema_markup             TEXT,
    schema_types_found        TEXT,
    total_images              INTEGER DEFAULT 0,
    images_missing_alt        INTEGER DEFAULT 0,
    image_alt_status          TEXT,
    primary_keyword           TEXT,
    secondary_keywords        TEXT,
    short_tail_keywords       TEXT,
    long_tail_keywords        TEXT,
    ai_meta_title             TEXT,
    ai_meta_description       TEXT,
    ai_h1                     TEXT,
    ai_og_title               TEXT,
    ai_og_description         TEXT,
    ai_og_image_url           TEXT,
    ai_schema_recommendation  TEXT,
    ai_schema_code_snippet    TEXT,
    ai_optimized_url          TEXT,
    image_optimization_tips   TEXT,
    serp_preview              TEXT,
    mobile_score              TEXT,
    mobile_lcp                TEXT,
    mobile_cls                TEXT,
    mobile_fcp                TEXT,
    desktop_score             TEXT,
    desktop_lcp               TEXT,
    desktop_cls               TEXT,
    desktop_fcp               TEXT,
    seo_score                 INTEGER DEFAULT 0,
    seo_grade                 TEXT,
    spam_malware_flags        TEXT,
    aeo_faq                   TEXT,
    body_copy_guidance        TEXT,
    viewport_configured       TEXT,
    html_size_kb              NUMERIC(10,2),
    html_size_issue           TEXT,
    is_secure                 TEXT,
    mixed_content             TEXT,
    mixed_content_details     TEXT,
    unminified_js             TEXT,
    unminified_js_details     TEXT,
    unminified_css            TEXT,
    unminified_css_details    TEXT,
    amp_link                  TEXT,
    og_validation             TEXT,
    x_robots_noindex          TEXT,
    page_cache_control        TEXT,
    crawl_depth               INTEGER DEFAULT -1,
    hreflang_tags             TEXT,
    created_at                TIMESTAMP DEFAULT NOW()
);

-- Broken links
CREATE TABLE IF NOT EXISTS broken_links (
    id                  SERIAL PRIMARY KEY,
    audit_id            INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    source_page         TEXT,
    broken_url          TEXT,
    status              TEXT,
    redirect_suggestion TEXT,
    redirect_type       TEXT DEFAULT '301',
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Images
CREATE TABLE IF NOT EXISTS images (
    id                      SERIAL PRIMARY KEY,
    audit_id                INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    page_url                TEXT,
    image_src               TEXT,
    alt_status              TEXT,
    current_alt             TEXT,
    ai_alt_recommendation   TEXT,
    created_at              TIMESTAMP DEFAULT NOW()
);

-- SEO keywords
CREATE TABLE IF NOT EXISTS seo_keywords (
    id                  SERIAL PRIMARY KEY,
    audit_id            INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    service_name        TEXT,
    keyword             TEXT,
    keyword_type        TEXT,
    primary_keyword     TEXT,
    secondary_keywords  TEXT,
    short_tail_keywords TEXT,
    long_tail_keywords  TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Blog topics
CREATE TABLE IF NOT EXISTS blog_topics (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    service_name    TEXT,
    title           TEXT,
    topic_type      TEXT,
    target_keyword  TEXT,
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Backlink strategies
CREATE TABLE IF NOT EXISTS backlink_strategies (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    category        TEXT,
    strategy_name   TEXT,
    description     TEXT,
    priority        TEXT,
    difficulty      TEXT,
    target_domains  TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- 6-month plan
CREATE TABLE IF NOT EXISTS six_month_plan (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    month_number    INTEGER,
    month_label     TEXT,
    theme           TEXT,
    tasks           TEXT,
    expected_output TEXT,
    kpis            TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Internal linking
CREATE TABLE IF NOT EXISTS internal_linking (
    id          SERIAL PRIMARY KEY,
    audit_id    INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    entry_type  TEXT,
    from_url    TEXT,
    to_url      TEXT,
    anchor_text TEXT,
    context     TEXT,
    silo_name   TEXT,
    reason      TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Keyword URL mapping
CREATE TABLE IF NOT EXISTS keyword_url_mapping (
    id                  SERIAL PRIMARY KEY,
    audit_id            INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    keyword             TEXT,
    keyword_type        TEXT,
    service_name        TEXT,
    mapped_url          TEXT,
    match_confidence    TEXT,
    reason              TEXT,
    on_page_action      TEXT,
    create_new_page     BOOLEAN DEFAULT FALSE,
    suggested_new_url   TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- AXO recommendations
CREATE TABLE IF NOT EXISTS axo_recommendations (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    axo_score       INTEGER DEFAULT 0,
    axo_grade       TEXT,
    category        TEXT,
    action_text     TEXT,
    priority        TEXT,
    impact          TEXT,
    implementation  TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Scorecard
CREATE TABLE IF NOT EXISTS scorecard (
    id          SERIAL PRIMARY KEY,
    audit_id    INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    parameter   TEXT,
    pass_count  INTEGER DEFAULT 0,
    fail_count  INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    pass_rate   NUMERIC(5,2),
    status      TEXT,
    check_type  TEXT DEFAULT 'per_page',
    created_at  TIMESTAMP DEFAULT NOW()
);

-- AEO FAQ
CREATE TABLE IF NOT EXISTS aeo_faq (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    page_url        TEXT,
    primary_keyword TEXT,
    question        TEXT,
    answer          TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Crawl progress (for resume support)
CREATE TABLE IF NOT EXISTS audit_progress (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    url             TEXT,
    phase           TEXT,
    status_code     TEXT,
    processed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(audit_id, url)
);

-- Site analysis
CREATE TABLE IF NOT EXISTS site_analysis (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    analysis_type   TEXT,
    analysis_key    TEXT,
    analysis_value  TEXT,
    count_value     INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Generated files
CREATE TABLE IF NOT EXISTS generated_files (
    id           SERIAL PRIMARY KEY,
    audit_id     INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    file_name    TEXT,
    file_type    TEXT,
    file_content TEXT,
    file_size    INTEGER DEFAULT 0,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_audits_brand_id   ON audits(brand_id);
CREATE INDEX IF NOT EXISTS idx_audits_domain      ON audits(domain);
CREATE INDEX IF NOT EXISTS idx_pages_audit_id     ON pages(audit_id);
CREATE INDEX IF NOT EXISTS idx_pages_url          ON pages(url);
CREATE INDEX IF NOT EXISTS idx_broken_audit_id    ON broken_links(audit_id);
CREATE INDEX IF NOT EXISTS idx_images_audit_id    ON images(audit_id);
CREATE INDEX IF NOT EXISTS idx_progress_audit_url ON audit_progress(audit_id, url);
"""

def init_db():
    """Create all tables if they don't exist. Call once on startup."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        logger.info("PostgreSQL schema initialized.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Schema init error: {e}")
        raise
    finally:
        release_db_conn(conn)

# ── DB Helper Functions ────────────────────────────────────────────────────────

def db_create_audit(conn, brand_id: int, audit_meta: dict) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audits (brand_id, domain, base_url, target_location,
                business_type, ai_mode, audit_status)
            VALUES (%s,%s,%s,%s,%s,%s,'in_progress')
            RETURNING id
        """, (
            brand_id,
            audit_meta.get("domain", ""),
            audit_meta.get("base_url", ""),
            audit_meta.get("target_location", "Global"),
            audit_meta.get("business_type", ""),
            audit_meta.get("ai_mode", "1"),
        ))
        audit_id = cur.fetchone()[0]
    conn.commit()
    return audit_id


def db_update_audit_complete(conn, audit_id: int, stats: dict):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE audits SET
                total_pages_crawled = %s, pages_200 = %s, pages_404 = %s,
                broken_links_count  = %s, images_missing_alt = %s,
                robots_txt_status   = %s, sitemap_status = %s,
                llm_txt_status      = %s, gbp_status = %s,
                site_recommendation = %s, detected_location = %s,
                excel_file          = %s, pdf_file = %s,
                audit_status        = 'complete'
            WHERE id = %s
        """, (
            stats.get("total_pages", 0), stats.get("pages_200", 0),
            stats.get("pages_404", 0), stats.get("broken_links", 0),
            stats.get("images_missing_alt", 0),
            stats.get("robots_status", ""), stats.get("sitemap_status", ""),
            stats.get("llm_status", ""), stats.get("gbp_status", ""),
            stats.get("site_recommendation", "")[:60000],
            stats.get("detected_location", ""),
            stats.get("excel_file", ""), stats.get("pdf_file", ""),
            audit_id,
        ))
    conn.commit()


def _safe(val, max_len=10000):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        val = json.dumps(val, default=str)
    s = str(val)
    return s[:max_len]


def db_insert_page(conn, audit_id: int, p: dict):
    cols = [
        "audit_id","url","url_cleaned","status",
        "redirect_suggestion","redirect_type","redirect_target",
        "canonical_status","canonical_url","duplicate_status",
        "word_count","thin_content","current_title","title_length",
        "current_meta_description","meta_desc_length","current_h1","h2_tags",
        "google_analytics","google_search_console",
        "og_tags","og_title_current","og_description_current","og_image_current",
        "schema_markup","schema_types_found","total_images","images_missing_alt",
        "image_alt_status","primary_keyword","secondary_keywords",
        "short_tail_keywords","long_tail_keywords",
        "ai_meta_title","ai_meta_description","ai_h1",
        "ai_og_title","ai_og_description","ai_og_image_url",
        "ai_schema_recommendation","ai_schema_code_snippet",
        "ai_optimized_url","image_optimization_tips","serp_preview",
        "mobile_score","mobile_lcp","mobile_cls","mobile_fcp",
        "desktop_score","desktop_lcp","desktop_cls","desktop_fcp",
        "seo_score","seo_grade","spam_malware_flags","aeo_faq","body_copy_guidance",
        "viewport_configured","html_size_kb","html_size_issue","is_secure",
        "mixed_content","mixed_content_details","unminified_js","unminified_js_details",
        "unminified_css","unminified_css_details","amp_link","og_validation",
        "x_robots_noindex","page_cache_control","crawl_depth","hreflang_tags",
    ]
    placeholders = ",".join(["%s"] * len(cols))
    col_str = ",".join(cols)
    int_cols = {"word_count","title_length","meta_desc_length","total_images",
                "images_missing_alt","seo_score","crawl_depth"}
    float_cols = {"html_size_kb"}

    row = [audit_id]
    for c in cols[1:]:
        val = p.get(c, "")
        if c in int_cols:
            try: val = int(val)
            except: val = 0
        elif c in float_cols:
            try: val = float(val)
            except: val = 0.0
        else:
            val = _safe(val)
        row.append(val)

    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO pages ({col_str}) VALUES ({placeholders})", row)
    conn.commit()


def db_update_page_ai(conn, audit_id: int, url: str, p: dict):
    """Update AI + PageSpeed fields for an existing page row."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pages SET
                primary_keyword=%s, secondary_keywords=%s,
                short_tail_keywords=%s, long_tail_keywords=%s,
                ai_meta_title=%s, ai_meta_description=%s, ai_h1=%s,
                ai_og_title=%s, ai_og_description=%s, ai_og_image_url=%s,
                ai_schema_recommendation=%s, ai_schema_code_snippet=%s,
                ai_optimized_url=%s, image_optimization_tips=%s, serp_preview=%s,
                mobile_score=%s, mobile_lcp=%s, mobile_cls=%s, mobile_fcp=%s,
                desktop_score=%s, desktop_lcp=%s, desktop_cls=%s, desktop_fcp=%s,
                seo_score=%s, seo_grade=%s, aeo_faq=%s, body_copy_guidance=%s
            WHERE audit_id=%s AND url=%s
        """, (
            _safe(p.get("primary_keyword",""), 500),
            _safe(p.get("secondary_keywords","")),
            _safe(p.get("short_tail_keywords","")),
            _safe(p.get("long_tail_keywords","")),
            _safe(p.get("ai_meta_title","")),
            _safe(p.get("ai_meta_description","")),
            _safe(p.get("ai_h1","")),
            _safe(p.get("ai_og_title","")),
            _safe(p.get("ai_og_description","")),
            _safe(p.get("ai_og_image_url","")),
            _safe(p.get("ai_schema_recommendation",""), 255),
            _safe(p.get("ai_schema_code_snippet","")),
            _safe(p.get("ai_optimized_url",""), 2000),
            _safe(p.get("image_optimization_tips","")),
            _safe(p.get("serp_preview","")),
            _safe(p.get("mobile_score",""), 20),
            _safe(p.get("mobile_lcp",""), 50),
            _safe(p.get("mobile_cls",""), 50),
            _safe(p.get("mobile_fcp",""), 50),
            _safe(p.get("desktop_score",""), 20),
            _safe(p.get("desktop_lcp",""), 50),
            _safe(p.get("desktop_cls",""), 50),
            _safe(p.get("desktop_fcp",""), 50),
            int(p.get("seo_score", 0) or 0),
            _safe(p.get("seo_grade",""), 10),
            _safe(p.get("aeo_faq",""), 60000),
            _safe(p.get("body_copy_guidance",""), 60000),
            audit_id, url[:2000],
        ))
    conn.commit()


def db_insert_images_batch(conn, audit_id: int, images: list):
    if not images:
        return
    rows = [(audit_id, i.get("page",""), i.get("src","")[:2000],
             i.get("alt_status",""), i.get("alt","")[:500],
             i.get("ai_alt_recommendation","")[:500]) for i in images]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO images (audit_id, page_url, image_src, alt_status, current_alt, ai_alt_recommendation)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_broken_links_batch(conn, audit_id: int, broken: list):
    if not broken:
        return
    rows = [(audit_id, b.get("source_page",""), b.get("broken_url",""),
             str(b.get("status","")), b.get("redirect_suggestion",""),
             b.get("redirect_type","301")) for b in broken]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO broken_links (audit_id, source_page, broken_url, status, redirect_suggestion, redirect_type)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_keywords(conn, audit_id: int, kw_data: dict):
    if not kw_data.get("services"):
        return
    rows = []
    for svc in kw_data["services"]:
        for kw in svc.get("keywords", []):
            rows.append((audit_id, svc.get("service",""), kw, "service",
                         svc.get("primary",""),
                         ", ".join(svc.get("secondary",[])),
                         ", ".join(svc.get("short_tail",[])),
                         ", ".join(svc.get("long_tail",[]))))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO seo_keywords (audit_id, service_name, keyword, keyword_type,
                    primary_keyword, secondary_keywords, short_tail_keywords, long_tail_keywords)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_blog_topics(conn, audit_id: int, blog_data: list):
    if not blog_data:
        return
    rows = []
    for svc in blog_data:
        for t in svc.get("topics", []):
            rows.append((audit_id, svc.get("service",""), t.get("title",""),
                         t.get("type",""), t.get("target_keyword",""), t.get("description","")))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO blog_topics (audit_id, service_name, title, topic_type, target_keyword, description)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_backlinks(conn, audit_id: int, bl_data: dict):
    if not bl_data:
        return
    rows = []
    for cat in ["seo_backlinks","aeo_backlinks","geo_backlinks","pr_backlinks"]:
        for item in bl_data.get(cat, []):
            rows.append((audit_id, cat, item.get("strategy",""),
                         item.get("description",""), item.get("priority",""),
                         item.get("difficulty",""), json.dumps(item.get("target_domains",[]))))
    for item in bl_data.get("avoid_backlinks", []):
        rows.append((audit_id, "avoid", item.get("type",""),
                     item.get("reason",""), item.get("risk_level",""),
                     "", json.dumps(item.get("examples",[]))))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO backlink_strategies (audit_id, category, strategy_name,
                    description, priority, difficulty, target_domains)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_plan(conn, audit_id: int, plan_data: dict):
    if not plan_data or not plan_data.get("months"):
        return
    rows = [(audit_id, m.get("month_number", i+1), m.get("month_label",""),
             m.get("theme",""), json.dumps(m.get("tasks",[]), default=str)[:10000],
             json.dumps(m.get("expected_output",{}), default=str)[:10000],
             json.dumps(m.get("kpis",[]), default=str))
            for i, m in enumerate(plan_data["months"])]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO six_month_plan (audit_id, month_number, month_label, theme, tasks, expected_output, kpis)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_internal_linking(conn, audit_id: int, il_data: dict):
    if not il_data:
        return
    rows = []
    for hp in il_data.get("hub_pages", []):
        rows.append((audit_id, "hub_page", "", hp.get("url",""), "", "", "", hp.get("reason","")))
    for lm in il_data.get("linking_map", []):
        rows.append((audit_id, "link", lm.get("from_url",""), lm.get("to_url",""),
                     lm.get("anchor_text",""), lm.get("context",""), "", ""))
    for s in il_data.get("topic_silos", []):
        rows.append((audit_id, "silo", "", s.get("hub_url",""), "", "",
                     s.get("silo_name",""), json.dumps(s.get("pages",[]))))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO internal_linking (audit_id, entry_type, from_url, to_url,
                    anchor_text, context, silo_name, reason)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_kw_url_map(conn, audit_id: int, kum_data: list):
    if not kum_data:
        return
    rows = [(audit_id, k.get("keyword",""), k.get("keyword_type",""),
             k.get("service",""), k.get("mapped_url",""),
             k.get("match_confidence",""), k.get("reason",""),
             k.get("on_page_action",""), bool(k.get("create_new_page")),
             k.get("suggested_new_url","")) for k in kum_data]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO keyword_url_mapping (audit_id, keyword, keyword_type, service_name,
                mapped_url, match_confidence, reason, on_page_action,
                create_new_page, suggested_new_url)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_axo(conn, audit_id: int, axo_data: dict):
    if not axo_data:
        return
    rows = []
    for cat in ["aeo_recommendations","geo_recommendations",
                "voice_search_recommendations","conversational_ai_recommendations"]:
        for item in axo_data.get(cat, []):
            rows.append((audit_id, axo_data.get("axo_score", 0),
                         axo_data.get("axo_grade",""), cat,
                         item.get("action",""), item.get("priority",""),
                         item.get("impact",""), item.get("implementation","")))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO axo_recommendations (audit_id, axo_score, axo_grade, category,
                    action_text, priority, impact, implementation)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_scorecard(conn, audit_id: int, sc_results: list, gl_checks: list):
    rows = [(audit_id, s[0], s[1], s[2], s[3], float(s[4]), s[5], "per_page")
            for s in sc_results]
    for label, passed in gl_checks:
        rows.append((audit_id, label,
                     1 if passed else 0, 0 if passed else 1, 1,
                     100.0 if passed else 0.0,
                     "PASSED" if passed else "FAILED", "global"))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO scorecard (audit_id, parameter, pass_count, fail_count,
                total_count, pass_rate, status, check_type)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_aeo_faq(conn, audit_id: int, pages: list):
    rows = []
    for p in pages:
        faq_list = p.get("_aeo_faq_list", [])
        if not faq_list and p.get("aeo_faq"):
            try:
                faq_list = json.loads(p["aeo_faq"])
            except Exception:
                faq_list = []
        for fq in faq_list:
            rows.append((audit_id, p.get("url",""), p.get("primary_keyword",""),
                         fq.get("question",""), fq.get("answer","")))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO aeo_faq (audit_id, page_url, primary_keyword, question, answer)
                VALUES %s
            """, rows)
        conn.commit()


def db_insert_site_analysis(conn, audit_id: int, data: list):
    if not data:
        return
    rows = [(audit_id, a["type"], a.get("key",""),
             str(a.get("value",""))[:10000], int(a.get("count", 0))) for a in data]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO site_analysis (audit_id, analysis_type, analysis_key, analysis_value, count_value)
            VALUES %s
        """, rows)
    conn.commit()


def db_insert_generated_files(conn, audit_id: int, files: list):
    if not files:
        return
    with conn.cursor() as cur:
        for f in files:
            cur.execute("""
                INSERT INTO generated_files (audit_id, file_name, file_type, file_content, file_size)
                VALUES (%s,%s,%s,%s,%s)
            """, (audit_id, f["file_name"], f["file_type"], f["file_content"], f["file_size"]))
    conn.commit()


def db_mark_url_progress(conn, audit_id: int, url: str, phase: str, status_code: str = ""):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit_progress (audit_id, url, phase, status_code)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (audit_id, url)
            DO UPDATE SET phase=%s, status_code=%s, processed_at=NOW()
        """, (audit_id, url[:2000], phase, str(status_code), phase, str(status_code)))
    conn.commit()


def db_get_processed_urls(conn, audit_id: int) -> set:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT url FROM audit_progress WHERE audit_id=%s AND phase='analyzed'
        """, (audit_id,))
        return {row[0] for row in cur.fetchall()}


def db_get_crawled_urls(conn, audit_id: int) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM audit_progress WHERE audit_id=%s", (audit_id,))
        return {row[0] for row in cur.fetchall()}


def db_find_existing_audit(conn, domain: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, domain, base_url, target_location, business_type, ai_mode, audit_timestamp
            FROM audits WHERE domain=%s AND audit_status='in_progress'
            ORDER BY id DESC LIMIT 1
        """, (domain,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def db_query_pages(conn, audit_id: int) -> list:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pages WHERE audit_id=%s ORDER BY id", (audit_id,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]
