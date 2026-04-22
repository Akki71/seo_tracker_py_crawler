"""
PostgreSQL database layer — replaces MySQL.
Uses psycopg2 with a simple connection pool.
All DB_CONFIG values are read from environment variables (set in Coolify).
"""

import os, json, logging
from typing import Optional
import time
import psycopg2
import psycopg2.extensions
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

# ── Connection layer — direct connect with retry (no pool) ────────────────────
# SimpleConnectionPool is NOT used because Coolify/Postgres kills idle connections
# server-side (idle timeout, TCP timeout) without notifying the pool.
# Direct connections with keepalives + retry are far more reliable.

_CONNECT_KWARGS = None  # built once from DB_CONFIG

def _connect_kwargs() -> dict:
    global _CONNECT_KWARGS
    if _CONNECT_KWARGS is None:
        _CONNECT_KWARGS = {
            **DB_CONFIG,
            "connect_timeout":            10,
            "keepalives":                 1,
            "keepalives_idle":            30,   # send keepalive after 30s idle
            "keepalives_interval":        10,   # retry every 10s
            "keepalives_count":           5,    # drop after 5 missed keepalives
            "options":                    "-c statement_timeout=300000",  # 5 min max query
        }
    return _CONNECT_KWARGS


def _new_conn() -> psycopg2.extensions.connection:
    """Open a fresh direct connection (not from pool)."""
    conn = psycopg2.connect(**_connect_kwargs())
    conn.autocommit = False
    return conn


def get_db_conn() -> psycopg2.extensions.connection:
    """
    Return a working psycopg2 connection.
    Tries up to 3 times with exponential back-off so transient
    Postgres restarts / Coolify container recycles don't crash the audit.
    """
    import time
    last_err = None
    for attempt in range(1, 4):
        try:
            conn = _new_conn()
            conn.cursor().execute("SELECT 1")   # verify it's alive
            return conn
        except Exception as e:
            last_err = e
            wait = attempt * 2          # 2s, 4s, 6s
            logger.warning(f"DB connect attempt {attempt}/3 failed: {e} — retrying in {wait}s")
            time.sleep(wait)
    raise psycopg2.OperationalError(f"Could not connect to PostgreSQL after 3 attempts: {last_err}")


def release_db_conn(conn):
    """Close the connection (no pool to return to)."""
    try:
        if conn and not conn.closed:
            conn.close()
    except Exception as e:
        logger.debug(f"release_db_conn close error (ignored): {e}")


def close_pool():
    """No-op — kept for API compatibility."""
    pass

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
    ai_h2                     TEXT,
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
    id                   SERIAL PRIMARY KEY,
    audit_id             INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    service_name         TEXT,
    title                TEXT,
    topic_type           TEXT,
    target_keyword       TEXT,
    description          TEXT,
    blog_content         TEXT,
    primary_keyword      TEXT,
    secondary_keywords   TEXT,
    short_tail_keywords  TEXT,
    long_tail_keywords   TEXT,
    created_at           TIMESTAMP DEFAULT NOW()
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

-- Keyword planner (100 keywords with search volume, CPC, competition)
CREATE TABLE IF NOT EXISTS keyword_planner (
    id                  SERIAL PRIMARY KEY,
    audit_id            INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    keyword             TEXT,
    keyword_type        TEXT,
    keyword_category    TEXT,
    competition_level   TEXT,
    search_volume       INTEGER DEFAULT 0,
    cpc                 NUMERIC(10,2) DEFAULT 0.00,
    competition_index   NUMERIC(5,4) DEFAULT 0.0000,
    is_brand_keyword    BOOLEAN DEFAULT FALSE,
    service_name        TEXT,
    keyword_rank        INTEGER DEFAULT 0,
    keyword_difficulty  TEXT,
    intent              TEXT,
    mapped_url          TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Schema markup analysis (per-page deep schema analysis)
CREATE TABLE IF NOT EXISTS schema_markup_analysis (
    id                  SERIAL PRIMARY KEY,
    audit_id            INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    page_url            TEXT,
    schema_types_found  TEXT,
    schema_snippets     TEXT,
    recommended_schemas TEXT,
    recommended_snippets TEXT,
    schema_status       TEXT,
    missing_schemas     TEXT,
    UNIQUE (audit_id, page_url),
    validation_errors   TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- LLM prompts (AI engine prompts with keyword targeting)
CREATE TABLE IF NOT EXISTS llm_prompts (
    id              SERIAL PRIMARY KEY,
    audit_id        INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    prompt_text     TEXT,
    prompt_type     TEXT,
    target_keyword  TEXT,
    search_volume   INTEGER DEFAULT 0,
    ai_engine       TEXT,
    suggested_answer TEXT,
    service_name    TEXT,
    mapped_url      TEXT,
    priority        TEXT DEFAULT 'medium',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Depth analysis (per-page crawl depth records)
CREATE TABLE IF NOT EXISTS depth_analysis (
    id                    SERIAL PRIMARY KEY,
    audit_id              INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    depth_level           INTEGER DEFAULT 0,
    page_url              TEXT,
    page_title            TEXT,
    parent_url            TEXT,
    seo_score             INTEGER DEFAULT 0,
    status_code           TEXT,
    word_count            INTEGER DEFAULT 0,
    has_schema            BOOLEAN DEFAULT FALSE,
    internal_links_count  INTEGER DEFAULT 0,
    created_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (audit_id, page_url)
);

-- New page suggestions
CREATE TABLE IF NOT EXISTS new_page_suggestions (
    id               SERIAL PRIMARY KEY,
    audit_id         INTEGER REFERENCES audits(id) ON DELETE CASCADE,
    suggested_url    TEXT,
    page_title       TEXT,
    page_type        TEXT,
    reason           TEXT,
    target_keyword   TEXT,
    content_outline  TEXT,
    priority         TEXT DEFAULT 'medium',
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Additional indexes
CREATE INDEX IF NOT EXISTS idx_keyword_planner_audit  ON keyword_planner(audit_id);
CREATE INDEX IF NOT EXISTS idx_schema_analysis_audit  ON schema_markup_analysis(audit_id);
CREATE INDEX IF NOT EXISTS idx_llm_prompts_audit       ON llm_prompts(audit_id);
CREATE INDEX IF NOT EXISTS idx_depth_analysis_audit    ON depth_analysis(audit_id);
CREATE INDEX IF NOT EXISTS idx_new_page_sugg_audit     ON new_page_suggestions(audit_id);
"""

# All columns the pages table MUST have (column_name, column_definition)
PAGES_REQUIRED_COLUMNS = [
    ("viewport_configured",  "TEXT"),
    ("html_size_kb",         "NUMERIC(10,2)"),
    ("html_size_issue",      "TEXT"),
    ("is_secure",            "TEXT"),
    ("mixed_content",        "TEXT"),
    ("mixed_content_details","TEXT"),
    ("unminified_js",        "TEXT"),
    ("unminified_js_details","TEXT"),
    ("unminified_css",       "TEXT"),
    ("unminified_css_details","TEXT"),
    ("amp_link",             "TEXT"),
    ("og_validation",        "TEXT"),
    ("x_robots_noindex",     "TEXT"),
    ("page_cache_control",   "TEXT"),
    ("crawl_depth",          "INTEGER DEFAULT -1"),
    ("hreflang_tags",        "TEXT"),
    ("body_copy_guidance",   "TEXT"),
    ("aeo_faq",              "TEXT"),
    ("spam_malware_flags",   "TEXT"),
    ("serp_preview",         "TEXT"),
    ("image_optimization_tips", "TEXT"),
    ("ai_h2",                 "TEXT"),
]

AUDITS_REQUIRED_COLUMNS = [
    ("brand_id",             "INTEGER"),
    ("detected_location",    "TEXT"),
    ("site_recommendation",  "TEXT"),
    ("audit_status",         "TEXT DEFAULT 'in_progress'"),
    ("llm_txt_status",       "TEXT"),
    ("gbp_status",           "TEXT"),
]

# blog_topics was created before these columns existed — migrate them in
BLOG_TOPICS_REQUIRED_COLUMNS = [
    ("blog_content",         "TEXT"),
    ("primary_keyword",      "TEXT"),
    ("secondary_keywords",   "TEXT"),
    ("short_tail_keywords",  "TEXT"),
    ("long_tail_keywords",   "TEXT"),
]



def _fix_serial_sequences(conn):
    """Ensure all id columns that should be SERIAL have proper sequences attached.
    This fixes tables created from MySQL dumps that land without auto-increment."""
    tables = [
        "audits", "pages", "broken_links", "images", "seo_keywords",
        "blog_topics", "backlink_strategies", "six_month_plan", "internal_linking",
        "keyword_url_mapping", "axo_recommendations", "scorecard", "aeo_faq",
        "audit_progress", "site_analysis", "generated_files",
        "keyword_planner", "schema_markup_analysis", "llm_prompts",
        "depth_analysis", "new_page_suggestions",
    ]
    for table in tables:
        try:
            with conn.cursor() as cur:
                # Check if id column exists and if it already has a default
                cur.execute("""
                    SELECT column_default FROM information_schema.columns
                    WHERE table_name = %s AND column_name = 'id' AND table_schema = 'public'
                """, (table,))
                row = cur.fetchone()
                if row and (row[0] is None or 'nextval' not in str(row[0])):
                    # id exists but has no sequence — create and attach one
                    seq_name = f"{table}_id_seq"
                    cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
                    cur.execute(f"""
                        SELECT setval('{seq_name}',
                            COALESCE((SELECT MAX(id) FROM {table}), 0) + 1, false)
                    """)
                    cur.execute(f"""
                        ALTER TABLE {table}
                        ALTER COLUMN id SET DEFAULT nextval('{seq_name}')
                    """)
                    conn.commit()
                    logger.info(f"Fixed SERIAL sequence for {table}.id")
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            logger.debug(f"_fix_serial_sequences {table}: {str(e)[:80]}")


def init_db():
    """
    Create all tables one statement at a time to avoid SSL/timeout drops
    on large CREATE_TABLES_SQL batches. Each statement gets its own commit.
    """
    conn = get_db_conn()
    try:
        _sql_kw = ("CREATE", "ALTER", "INSERT", "DROP", "GRANT")
        executed = 0
        for raw_stmt in CREATE_TABLES_SQL.split(";"):
            # Strip comment lines, get clean SQL
            lines = [l for l in raw_stmt.splitlines() if not l.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if not stmt or not any(stmt.upper().startswith(k) for k in _sql_kw):
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
                executed += 1
            except Exception as stmt_err:
                try: conn.rollback()
                except Exception: pass
                logger.debug(f"DDL skipped (already exists?): {str(stmt_err)[:80]}")

        logger.info(f"init_db: {executed} DDL statements executed OK")

        # Safe column migrations
        _migrate_columns(conn, "pages",       PAGES_REQUIRED_COLUMNS)
        _migrate_columns(conn, "audits",      AUDITS_REQUIRED_COLUMNS)
        _migrate_columns(conn, "blog_topics", BLOG_TOPICS_REQUIRED_COLUMNS)

        # Fix any tables that may have been created without SERIAL sequences
        _fix_serial_sequences(conn)

        # Ensure UNIQUE constraints exist on tables that need them for upserts
        _migrate_unique_constraints(conn)

        logger.info("PostgreSQL schema initialized ✓")
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        logger.error(f"Schema init error: {e}")
        raise
    finally:
        release_db_conn(conn)


def _migrate_columns(conn, table: str, required_cols: list):
    """Add any columns missing from table — safe to run repeatedly."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
            """, (table,))
            existing = {row[0] for row in cur.fetchall()}

        added = []
        for col_name, col_def in required_cols:
            if col_name not in existing:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}')
                    conn.commit()
                    added.append(col_name)
                    logger.info(f"Migration: added column {table}.{col_name}")
                except Exception as e:
                    conn.rollback()
                    logger.warning(f"Migration: could not add {table}.{col_name}: {e}")

        if added:
            logger.info(f"Migration complete: added {len(added)} columns to {table}: {added}")
        else:
            logger.info(f"Migration: {table} schema is up to date")
    except Exception as e:
        logger.error(f"Migration error for {table}: {e}")


def _migrate_unique_constraints(conn):
    """Safely add UNIQUE constraints needed for ON CONFLICT upserts.
    Safe to run on existing DBs — skips if constraint already exists.
    """
    constraints = [
        # (table, constraint_name, columns)
        ("depth_analysis",       "depth_analysis_audit_url_unique",      "audit_id, page_url"),
        ("schema_markup_analysis","schema_markup_analysis_audit_url_uq", "audit_id, page_url"),
    ]
    for table, constraint_name, columns in constraints:
        try:
            with conn.cursor() as cur:
                # Check if constraint already exists
                cur.execute("""
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = %s AND constraint_name = %s
                    AND table_schema = 'public'
                """, (table, constraint_name))
                if cur.fetchone():
                    continue  # already exists — skip
                # Add the constraint
                cur.execute(f"""
                    ALTER TABLE {table}
                    ADD CONSTRAINT {constraint_name} UNIQUE ({columns})
                """)
            conn.commit()
            logger.info(f"Migration: added UNIQUE({columns}) to {table}")
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            # Might fail if duplicate rows already exist — log and continue
            logger.warning(f"Migration: could not add UNIQUE to {table}: {str(e)[:120]}")

# ── DB Helper Functions ────────────────────────────────────────────────────────

def db_create_audit(conn, brand_id: int, audit_meta: dict) -> int:
    """Insert audit row. Retries with a fresh connection on OperationalError."""
    def _do(c):
        with c.cursor() as cur:
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
        c.commit()
        return audit_id

    try:
        return _do(conn)
    except psycopg2.OperationalError:
        logger.warning("db_create_audit: connection lost — reconnecting...")
        release_db_conn(conn)
        fresh = get_db_conn()
        try:
            return _do(fresh)
        finally:
            release_db_conn(fresh)


def db_update_audit_complete(conn, audit_id: int, stats: dict):
    # Ensure new columns exist (safe migration)
    _ensure_audit_columns(conn)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE audits SET
                total_pages_crawled = %s, pages_200 = %s, pages_404 = %s,
                broken_links_count  = %s, images_missing_alt = %s,
                robots_txt_status   = %s, sitemap_status = %s,
                llm_txt_status      = %s, gbp_status = %s,
                site_recommendation = %s, detected_location = %s,
                business_type       = %s,
                ssl_status          = %s, www_resolve = %s, sitemap_size = %s,
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
            stats.get("business_type", ""),
            stats.get("ssl_status", ""), stats.get("www_resolve", ""),
            stats.get("sitemap_size", ""),
            stats.get("excel_file", ""), stats.get("pdf_file", ""),
            audit_id,
        ))
    conn.commit()


def _ensure_audit_columns(conn):
    """Add any missing columns to audits table (one-time safe migration)."""
    extra_cols = [
        ("business_type", "TEXT"),
        ("ssl_status",    "TEXT"),
        ("www_resolve",   "TEXT"),
        ("sitemap_size",  "TEXT"),
    ]
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='audits' AND table_schema='public'
            """)
            existing = {r[0] for r in cur.fetchall()}
        for col, coltype in extra_cols:
            if col not in existing:
                with conn.cursor() as cur:
                    cur.execute(f"ALTER TABLE audits ADD COLUMN IF NOT EXISTS {col} {coltype}")
                conn.commit()
                logger.info(f"Migration: added audits.{col}")
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        logger.warning(f"_ensure_audit_columns error (non-fatal): {e}")


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
        "ai_meta_title","ai_meta_description","ai_h1","ai_h2",
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

    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO pages ({col_str}) VALUES ({placeholders})", row)
        conn.commit()
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        raise


def db_update_page_ai(conn, audit_id: int, url: str, p: dict):
    """Update AI + PageSpeed fields for an existing page row."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pages SET
                primary_keyword=%s, secondary_keywords=%s,
                short_tail_keywords=%s, long_tail_keywords=%s,
                ai_meta_title=%s, ai_meta_description=%s, ai_h1=%s, ai_h2=%s,
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
            _safe(p.get("ai_h2","")),
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
        logger.info(f"  db_insert_keywords: skipped (no services data)")
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
        logger.info(f"  db_insert_blog_topics: skipped (no blog data)")
        return
    rows = []
    for svc in blog_data:
        for t in svc.get("topics", []):
            secondary = t.get("secondary_keywords", [])
            short_tail = t.get("short_tail_keywords", [])
            long_tail  = t.get("long_tail_keywords", [])
            rows.append((
                audit_id,
                str(svc.get("service", ""))[:255],
                str(t.get("title", ""))[:2000],
                str(t.get("type", ""))[:50],
                str(t.get("target_keyword", ""))[:500],
                str(t.get("description", ""))[:5000],
                str(t.get("blog_content", ""))[:60000],
                str(t.get("primary_keyword", ""))[:500],
                ", ".join(secondary) if isinstance(secondary, list) else str(secondary),
                ", ".join(short_tail) if isinstance(short_tail, list) else str(short_tail),
                ", ".join(long_tail)  if isinstance(long_tail, list)  else str(long_tail),
            ))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO blog_topics
                    (audit_id, service_name, title, topic_type, target_keyword,
                     description, blog_content, primary_keyword,
                     secondary_keywords, short_tail_keywords, long_tail_keywords)
                VALUES %s
            """, rows)
        conn.commit()
        logger.info(f"  db_insert_blog_topics: inserted {len(rows)} topics for audit #{audit_id}")


def db_insert_backlinks(conn, audit_id: int, bl_data: dict):
    if not bl_data:
        logger.info(f"  db_insert_backlinks: skipped (no backlink data)")
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
        logger.info(f"  db_insert_plan: skipped (no plan data)")
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
    rows = []
    for k in kum_data:
        service = k.get("service_name") or k.get("service") or ""
        rows.append((
            audit_id,
            str(k.get("keyword", ""))[:500],
            str(k.get("keyword_type", ""))[:100],
            str(service)[:255],
            str(k.get("mapped_url", ""))[:2000],
            str(k.get("match_confidence", ""))[:50],
            str(k.get("reason", ""))[:2000],
            str(k.get("on_page_action", ""))[:2000],
            bool(k.get("create_new_page", False)),
            str(k.get("suggested_new_url", ""))[:2000],
        ))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO keyword_url_mapping (audit_id, keyword, keyword_type, service_name,
                mapped_url, match_confidence, reason, on_page_action,
                create_new_page, suggested_new_url)
            VALUES %s
        """, rows)
    conn.commit()
    logger.info(f"keyword_url_mapping: inserted {len(rows)} rows for audit {audit_id}")


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


 


def db_insert_new_page_suggestions(conn, audit_id: int, suggestions: list):
    """Insert AI-generated new page suggestions."""
    if not suggestions:
        return
    rows = []
    for s in suggestions:
        outline = s.get("content_outline", [])
        rows.append((
            audit_id,
            str(s.get("url", s.get("suggested_url", "")))[:2000],
            str(s.get("title", s.get("page_title", "")))[:500],
            str(s.get("page_type", ""))[:100],
            str(s.get("reason", ""))[:2000],
            str(s.get("target_keyword", ""))[:500],
            json.dumps(outline) if isinstance(outline, list) else str(outline)[:5000],
            str(s.get("priority", "medium"))[:20],
        ))
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO new_page_suggestions
                (audit_id, suggested_url, page_title, page_type, reason,
                 target_keyword, content_outline, priority)
            VALUES %s
        """, rows)
    conn.commit()
    logger.info(f"Inserted {len(rows)} new page suggestions for audit #{audit_id}")


def db_insert_keyword_planner(conn, audit_id: int, keywords_ranked: list):
    """Insert keyword planner data (100 keywords with search volume, CPC, competition)."""
    if not keywords_ranked:
        return
    rows = [
        (
            audit_id,
            str(k.get("keyword", ""))[:500],
            str(k.get("keyword_type", ""))[:50],
            str(k.get("keyword_category", ""))[:100],
            str(k.get("competition_level", ""))[:20],
            int(k.get("search_volume", 0) or 0),
            float(k.get("cpc", 0.0) or 0.0),
            float(k.get("competition_index", 0.0) or 0.0),
            bool(k.get("is_brand_keyword", False)),
            str(k.get("service_name", ""))[:255],
            int(k.get("keyword_rank", 0) or 0),
            str(k.get("keyword_difficulty", ""))[:20],
            str(k.get("intent", ""))[:50],
            str(k.get("mapped_url", ""))[:2000],
        )
        for k in keywords_ranked
    ]
    # Use ON CONFLICT DO NOTHING to handle duplicate id sequences gracefully
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO keyword_planner
                    (audit_id, keyword, keyword_type, keyword_category, competition_level,
                     search_volume, cpc, competition_index, is_brand_keyword, service_name,
                     keyword_rank, keyword_difficulty, intent, mapped_url)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """, rows)
        conn.commit()
        logger.info(f"keyword_planner: inserted {len(rows)} keywords for audit #{audit_id}")
    except Exception as _ke:
        try: conn.rollback()
        except Exception: pass
        logger.warning(f"keyword_planner batch failed ({_ke}) — trying row-by-row")
        inserted = 0
        for row in rows:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO keyword_planner
                            (audit_id, keyword, keyword_type, keyword_category, competition_level,
                             search_volume, cpc, competition_index, is_brand_keyword, service_name,
                             keyword_rank, keyword_difficulty, intent, mapped_url)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, row)
                conn.commit()
                inserted += 1
            except Exception as _re:
                try: conn.rollback()
                except Exception: pass
                logger.error(f"keyword_planner row error: {_re}")
        logger.info(f"keyword_planner: row-by-row inserted {inserted}/{len(rows)} for audit #{audit_id}")


def db_insert_schema_analysis(conn, audit_id: int, schema_results: list):
    """Insert per-page schema markup deep analysis."""
    if not schema_results:
        return

    def _jdump(val):
        if isinstance(val, (list, dict)):
            return json.dumps(val, default=str)[:10000]
        return str(val or "")[:10000]

    rows = []
    for sr in schema_results:
        if not sr or not sr.get("page_url"):
            continue
        rows.append((
            audit_id,
            str(sr.get("page_url", ""))[:2000],
            _jdump(sr.get("schema_types_found", [])),
            _jdump(sr.get("schema_snippets", [])),
            _jdump(sr.get("recommended_schemas", [])),
            _jdump(sr.get("recommended_snippets", [])),
            str(sr.get("schema_status", ""))[:50],
            _jdump(sr.get("missing_schemas", [])),
            "",  # validation_errors
        ))
    if not rows:
        return
    inserted = 0
    for row in rows:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_markup_analysis
                        (audit_id, page_url, schema_types_found, schema_snippets,
                         recommended_schemas, recommended_snippets, schema_status,
                         missing_schemas, validation_errors)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (audit_id, page_url) DO UPDATE SET
                        schema_types_found   = EXCLUDED.schema_types_found,
                        schema_snippets      = EXCLUDED.schema_snippets,
                        recommended_schemas  = EXCLUDED.recommended_schemas,
                        recommended_snippets = EXCLUDED.recommended_snippets,
                        schema_status        = EXCLUDED.schema_status,
                        missing_schemas      = EXCLUDED.missing_schemas,
                        validation_errors    = EXCLUDED.validation_errors
                """, row)
            conn.commit()
            inserted += 1
        except Exception as row_err:
            try: conn.rollback()
            except Exception: pass
            logger.error(f"schema_markup_analysis row insert error: {row_err}")
    logger.info(f"schema_markup_analysis: inserted {inserted}/{len(rows)} pages for audit #{audit_id}")


def db_insert_llm_prompts(conn, audit_id: int, prompts: list):
    """Insert LLM prompts (AI engine question prompts with keyword data)."""
    if not prompts:
        return
    rows = [
        (
            audit_id,
            str(p.get("prompt_text", ""))[:5000],
            str(p.get("prompt_type", ""))[:50],
            str(p.get("target_keyword", ""))[:500],
            int(p.get("search_volume", 0) or 0),
            str(p.get("ai_engine", ""))[:100],
            str(p.get("suggested_answer", ""))[:10000],
            str(p.get("service_name", ""))[:255],
            str(p.get("mapped_url", ""))[:2000],
            str(p.get("priority", "medium"))[:20],
        )
        for p in prompts
    ]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO llm_prompts
                (audit_id, prompt_text, prompt_type, target_keyword, search_volume,
                 ai_engine, suggested_answer, service_name, mapped_url, priority)
            VALUES %s
        """, rows)
    conn.commit()
    logger.info(f"llm_prompts: inserted {len(rows)} prompts for audit #{audit_id}")


def db_insert_depth_analysis(conn, audit_id: int, depth_records: list):
    """Insert per-page crawl depth analysis records."""
    if not depth_records:
        return
    rows = []
    for d in depth_records:
        if not d.get("page_url"):
            continue
        rows.append((
            audit_id,
            int(d.get("depth_level", 0) or 0),
            str(d.get("page_url", ""))[:2000],
            str(d.get("page_title") or "")[:500],
            str(d.get("parent_url") or "")[:2000],
            int(d.get("seo_score", 0) or 0),
            str(d.get("status_code") or "")[:50],
            int(d.get("word_count", 0) or 0),
            bool(d.get("has_schema", False)),
            int(d.get("internal_links_count", 0) or 0),
        ))
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO depth_analysis
                (audit_id, depth_level, page_url, page_title, parent_url,
                 seo_score, status_code, word_count, has_schema, internal_links_count)
            VALUES %s
            ON CONFLICT (audit_id, page_url) DO UPDATE SET
                depth_level          = EXCLUDED.depth_level,
                page_title           = EXCLUDED.page_title,
                parent_url           = EXCLUDED.parent_url,
                seo_score            = EXCLUDED.seo_score,
                status_code          = EXCLUDED.status_code,
                word_count           = EXCLUDED.word_count,
                has_schema           = EXCLUDED.has_schema,
                internal_links_count = EXCLUDED.internal_links_count
        """, rows)
    conn.commit()
    logger.info(f"depth_analysis: upserted {len(rows)} records for audit #{audit_id}")


def db_insert_blog_topics_full(conn, audit_id: int, blog_data: list):
    """Enhanced blog topics insert with full keyword fields (primary, secondary, etc.)."""
    if not blog_data:
        return
    rows = []
    for svc in blog_data:
        for t in svc.get("topics", []):
            secondary = t.get("secondary_keywords", [])
            short_tail = t.get("short_tail_keywords", [])
            long_tail  = t.get("long_tail_keywords", [])
            rows.append((
                audit_id,
                str(svc.get("service", ""))[:255],
                str(t.get("title", ""))[:2000],
                str(t.get("type", ""))[:50],
                str(t.get("target_keyword", ""))[:500],
                str(t.get("description", ""))[:5000],
                str(t.get("blog_content", ""))[:60000],
                str(t.get("primary_keyword", ""))[:500],
                ", ".join(secondary) if isinstance(secondary, list) else str(secondary),
                ", ".join(short_tail) if isinstance(short_tail, list) else str(short_tail),
                ", ".join(long_tail)  if isinstance(long_tail, list)  else str(long_tail),
            ))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO blog_topics
                    (audit_id, service_name, title, topic_type, target_keyword,
                     description, blog_content, primary_keyword,
                     secondary_keywords, short_tail_keywords, long_tail_keywords)
                VALUES %s
            """, rows)
        conn.commit()
        logger.info(f"blog_topics (full): inserted {len(rows)} topics for audit #{audit_id}")



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
    """Find an in-progress audit for this domain (crash-resume)."""
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


def db_find_last_completed_audit(conn, domain: str, brand_id: int = None):
    """Find the most recent COMPLETED audit for this domain.
    Used to continue crawling above the previous URL count.
    Searches: (1) audit_status=complete, (2) any audit with crawled pages as fallback.
    """
    with conn.cursor() as cur:
        # Primary: find completed audit
        if brand_id:
            cur.execute("""
                SELECT id, domain, base_url, total_pages_crawled,
                       target_location, business_type, ai_mode, audit_timestamp
                FROM audits
                WHERE domain=%s AND brand_id=%s AND audit_status='complete'
                ORDER BY id DESC LIMIT 1
            """, (domain, brand_id))
        else:
            cur.execute("""
                SELECT id, domain, base_url, total_pages_crawled,
                       target_location, business_type, ai_mode, audit_timestamp
                FROM audits
                WHERE domain=%s AND audit_status='complete'
                ORDER BY id DESC LIMIT 1
            """, (domain,))
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

        # Fallback: find any audit (any status) that has actual crawled pages
        # This handles cases where audit_status was not updated correctly
        if brand_id:
            cur.execute("""
                SELECT a.id, a.domain, a.base_url, a.total_pages_crawled,
                       a.target_location, a.business_type, a.ai_mode, a.audit_timestamp
                FROM audits a
                WHERE a.domain=%s AND a.brand_id=%s
                  AND a.audit_status != 'in_progress'
                  AND EXISTS (
                      SELECT 1 FROM pages p WHERE p.audit_id=a.id LIMIT 1
                  )
                ORDER BY a.id DESC LIMIT 1
            """, (domain, brand_id))
        else:
            cur.execute("""
                SELECT a.id, a.domain, a.base_url, a.total_pages_crawled,
                       a.target_location, a.business_type, a.ai_mode, a.audit_timestamp
                FROM audits a
                WHERE a.domain=%s
                  AND a.audit_status != 'in_progress'
                  AND EXISTS (
                      SELECT 1 FROM pages p WHERE p.audit_id=a.id LIMIT 1
                  )
                ORDER BY a.id DESC LIMIT 1
            """, (domain,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        logger.info(f"db_find_last_completed_audit: using fallback for {domain} — "
                    f"found audit #{result['id']} with status!=in_progress and pages")
        return result


def db_get_all_crawled_urls_for_domain(conn, domain: str, brand_id: int = None) -> set:
    """Get ALL URLs ever crawled for this domain across all prior audits.
    Includes completed + any other non-in_progress audits that have pages.
    Used to seed the 'already visited' set so re-runs only crawl NEW pages.
    """
    with conn.cursor() as cur:
        if brand_id:
            cur.execute("""
                SELECT DISTINCT p.url
                FROM pages p
                JOIN audits a ON p.audit_id = a.id
                WHERE a.domain = %s AND a.brand_id = %s
                  AND a.audit_status != 'in_progress'
            """, (domain, brand_id))
        else:
            cur.execute("""
                SELECT DISTINCT p.url
                FROM pages p
                JOIN audits a ON p.audit_id = a.id
                WHERE a.domain = %s
                  AND a.audit_status != 'in_progress'
            """, (domain,))
        urls = {row[0] for row in cur.fetchall()}
        logger.info(f"db_get_all_crawled_urls_for_domain: {len(urls)} unique URLs "
                    f"found for {domain} (brand_id={brand_id})")
        return urls


def db_query_pages(conn, audit_id: int) -> list:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pages WHERE audit_id=%s ORDER BY id", (audit_id,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]