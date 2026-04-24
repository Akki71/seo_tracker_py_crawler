"""
AquilTechLabs SEO Crawler API v2.0
All heavy imports (crawler, AI, reports) happen ONLY inside background tasks.
The server starts in under 1 second with zero risk of import-crash.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import uuid, os, logging, sys, traceback

# ── Load .env early so all modules see the vars ──────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("seo_api")

# ── In-memory job store ────────────────────────────────────────────────────────
# Fine for single worker. For multiple workers, swap to Redis.
jobs: dict = {}

# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI startup: checking DB connection...")
    try:
        from db import init_db
        init_db()
        logger.info("DB schema initialized ✓")
    except Exception as e:
        logger.warning(f"DB init failed (non-fatal): {e}")
    yield
    logger.info("FastAPI shutdown complete.")

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="AquilTechLabs SEO Crawler API",
    description="Full SEO audit — crawl, AI analysis, Excel + PDF export, PostgreSQL storage.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Models ─────────────────────────────────────────────────────────────────────
class AuditRequest(BaseModel):
    url: str
    brand_id: int
    domain: Optional[str] = ""          # optional override; auto-detected from url if blank
    target_location: Optional[str] = ""
    business_type: Optional[str] = ""   # e.g. "agency", "ecommerce", "saas", "local"
    ai_mode: Optional[str] = "1"        # 1=OpenAI 2=Claude 3=Hybrid 4=Skip AI
    crawl_limit: Optional[int] = 500
    run_pagespeed: Optional[bool] = False

class AuditStatusResponse(BaseModel):
    job_id: str
    status: str


class ScraperRequest(BaseModel):
    brand_id:    int
    brand:       str
    competitors: list = []
    keywords:    list
    location:    str  = ""
    platforms:   list = ["reddit", "quora", "medium", "tumblr"]
    message: Optional[str] = ""
    audit_id: Optional[int] = None
    excel_file: Optional[str] = None
    pdf_file: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    """Root — service info."""
    return {
        "service": "AquilTechLabs SEO Crawler API",
        "version": "2.0.0",
        "status":  "running",
        "docs":    "/docs",
        "health":  "/health",
        "endpoints": {
            "POST /audit/start":                    "Start a new audit",
            "GET  /audit/status/{job_id}":          "Poll job status",
            "GET  /audit/download/{job_id}/excel":  "Download Excel report",
            "GET  /audit/download/{job_id}/pdf":    "Download PDF report",
            "GET  /brand/{brand_id}/audits":        "List audits for a brand",
            "GET  /audit/{audit_id}":               "Get audit detail from DB",
            "GET  /jobs":                           "List all active jobs",
        }
    }

@app.get("/health")
def health():
    """Health check — tests DB connectivity."""
    db_status = "unknown"
    db_error  = None
    try:
        from db import get_db_conn, release_db_conn
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        release_db_conn(conn)
        db_status = "connected"
    except Exception as e:
        db_status = "disconnected"
        db_error  = str(e)

    return {
        "status":      "ok",
        "version":     "2.0.0",
        "python":      sys.version.split()[0],
        "database":    db_status,
        "db_error":    db_error,
        "active_jobs": len(jobs),
        "timestamp":   datetime.utcnow().isoformat(),
    }

@app.get("/jobs")
def list_jobs():
    """List all in-memory jobs for this server session."""
    return {
        "total": len(jobs),
        "jobs": [
            {
                "job_id":     k,
                "status":     v["status"],
                "message":    v["message"],
                "brand_id":   v.get("brand_id"),
                "url":        v.get("url"),
                "started_at": v["started_at"],
                "completed_at": v.get("completed_at"),
                "audit_id":   v.get("audit_id"),
            }
            for k, v in jobs.items()
        ]
    }

@app.post("/audit/start", response_model=AuditStatusResponse)
async def start_audit(req: AuditRequest, background_tasks: BackgroundTasks):
    """
    Start a full SEO audit in the background.

    **Returns immediately** with a `job_id`.
    Poll `GET /audit/status/{job_id}` to check progress.
    Download files via `GET /audit/download/{job_id}/excel` or `/pdf` when status = `completed`.
    """
    url = req.url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    if req.brand_id <= 0:
        raise HTTPException(status_code=400, detail="brand_id must be a positive integer")

    crawl_limit = min(max(req.crawl_limit or 500, 1), 10000)

    job_id = str(uuid.uuid4())
    now    = datetime.utcnow().isoformat()

    jobs[job_id] = {
        "status":       "queued",
        "message":      f"Audit queued for {url}",
        "audit_id":     None,
        "excel_file":   None,
        "pdf_file":     None,
        "started_at":   now,
        "completed_at": None,
        "error":        None,
        # Extra context (not in response model but useful for /jobs)
        "brand_id":     req.brand_id,
        "url":          url,
    }

    background_tasks.add_task(
        _run_audit_task,
        job_id          = job_id,
        url             = url,
        brand_id        = req.brand_id,
        target_location = req.target_location or "",
        business_type   = req.business_type or "",
        ai_mode         = req.ai_mode or "1",
        crawl_limit     = crawl_limit,
        run_pagespeed   = bool(req.run_pagespeed),
    )

    logger.info(f"[{job_id}] Queued: brand={req.brand_id} url={url} "
                f"ai={req.ai_mode} limit={crawl_limit} biz={req.business_type}")

    return AuditStatusResponse(job_id=job_id, **{
        k: v for k, v in jobs[job_id].items()
        if k in AuditStatusResponse.model_fields
    })

@app.get("/audit/status/{job_id}", response_model=AuditStatusResponse)
def audit_status(job_id: str):
    """Poll job status. Statuses: queued → running → completed | failed"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found. "
                            "Note: jobs reset on server restart.")
    return AuditStatusResponse(job_id=job_id, **{
        k: v for k, v in jobs[job_id].items()
        if k in AuditStatusResponse.model_fields
    })

@app.get("/audit/download/{job_id}/excel")
def download_excel(job_id: str):
    """Download Excel report. Only available when status = completed."""
    j = _get_job_or_404(job_id)
    if j["status"] != "completed":
        raise HTTPException(status_code=400,
                            detail=f"Audit status is '{j['status']}' — not yet completed.")
    path = j.get("excel_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Excel file not found on disk.")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(path)
    )

@app.get("/audit/download/{job_id}/pdf")
def download_pdf(job_id: str):
    """Download PDF report. Only available when status = completed."""
    j = _get_job_or_404(job_id)
    if j["status"] != "completed":
        raise HTTPException(status_code=400,
                            detail=f"Audit status is '{j['status']}' — not yet completed.")
    path = j.get("pdf_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="PDF file not found on disk.")
    return FileResponse(path, media_type="application/pdf",
                        filename=os.path.basename(path))

@app.get("/brand/{brand_id}/audits")
def list_brand_audits(brand_id: int):
    """Return last 50 audit records for a brand from PostgreSQL."""
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, domain, base_url, audit_status, ai_mode,
                       total_pages_crawled, pages_200, pages_404,
                       broken_links_count, audit_timestamp, excel_file, pdf_file
                FROM audits
                WHERE brand_id = %s
                ORDER BY id DESC LIMIT 50
            """, (brand_id,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            result = []
            for row in rows:
                r = dict(zip(cols, row))
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
                result.append(r)
        return {"brand_id": brand_id, "total": len(result), "audits": result}
    except Exception as e:
        logger.error(f"DB error listing brand audits: {e}")
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        release_db_conn(conn)

@app.get("/audit/{audit_id}")
def get_audit(audit_id: int):
    """Get full audit record from PostgreSQL."""
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM audits WHERE id = %s", (audit_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Audit {audit_id} not found.")
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
            for k, v in result.items():
                if hasattr(v, "isoformat"):
                    result[k] = v.isoformat()
            return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DB error fetching audit {audit_id}: {e}")
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        release_db_conn(conn)

@app.get("/audit/{audit_id}/files")
def list_generated_files(audit_id: int):
    """List all generated SEO files available for an audit."""
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_name, file_type, file_size, created_at
                FROM generated_files WHERE audit_id = %s ORDER BY file_name
            """, (audit_id,))
            rows = cur.fetchall()
        files = [
            {
                "file_name":    r[0],
                "file_type":    r[1],
                "file_size":    r[2],
                "created_at":   r[3].isoformat() if r[3] else None,
                "download_url": f"/audit/{audit_id}/file/{r[0]}"
            }
            for r in rows
        ]
        return {"audit_id": audit_id, "total": len(files), "files": files}
    except Exception as e:
        logger.error(f"list_generated_files error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)


@app.get("/audit/{audit_id}/file/{file_name:path}")
def get_generated_file(audit_id: int, file_name: str):
    """
    Download a generated SEO file by name.
    file_name: llms.txt | sitemap.xml | robots.txt | .htaccess |
               .htaccess_redirects | nginx_redirects.conf | broken_links_report.txt
    """
    from db import get_db_conn, release_db_conn
    from fastapi.responses import PlainTextResponse
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_content, file_type FROM generated_files
                WHERE audit_id = %s AND file_name = %s
                ORDER BY id DESC LIMIT 1
            """, (audit_id, file_name))
            row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"File '{file_name}' not found for audit #{audit_id}. "
                       "Use GET /audit/{audit_id}/files to list available files."
            )
        file_content, file_type = row
        if file_name.endswith(".xml"):
            media_type = "application/xml"
        else:
            media_type = "text/plain; charset=utf-8"
        return PlainTextResponse(
            content=file_content,
            media_type=media_type,
            headers={"Content-Disposition": f'inline; filename="{file_name}"'}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_generated_file error ({file_name}): {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)


# ── Brand Intelligence Scraper ─────────────────────────────────────────────────

_scraper_jobs: dict = {}   # job_id → status dict


@app.post("/scraper/start")
def scraper_start(req: ScraperRequest):
    """
    Start a multi-platform brand intelligence scrape.

    Payload:
        brand_id    : int   — brand identifier (links all tables)
        brand       : str   — brand name to search for
        competitors : list  — competitor names e.g. ["Nike","Adidas"]
        keywords    : list  — keywords e.g. ["running shoes","sports wear"]
        location    : str   — optional e.g. "India", "Dubai", "UK"
        platforms   : list  — ["reddit","quora","medium","tumblr"] or subset
    """
    if req.brand_id <= 0:
        raise HTTPException(status_code=400, detail="brand_id must be positive")
    if not req.brand or not req.brand.strip():
        raise HTTPException(status_code=400, detail="brand name is required")
    if not req.keywords:
        raise HTTPException(status_code=400, detail="at least one keyword is required")

    valid_platforms = {"reddit", "quora", "medium", "tumblr"}
    platforms = [p.lower().strip() for p in req.platforms if p.lower().strip() in valid_platforms]
    if not platforms:
        platforms = ["reddit", "quora", "medium", "tumblr"]

    import uuid
    job_id = str(uuid.uuid4())
    _scraper_jobs[job_id] = {
        "job_id":    job_id,
        "status":    "queued",
        "brand_id":  req.brand_id,
        "brand":     req.brand,
        "platforms": platforms,
        "location":  req.location,
        "queued_at": datetime.now().isoformat(),
        "result":    None,
        "error":     None,
    }

    import asyncio, threading
    def _run_in_thread():
        _scraper_jobs[job_id]["status"] = "running"
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            from brand_scraper import run_scraper
            result = loop.run_until_complete(run_scraper(
                brand_id    = req.brand_id,
                brand       = req.brand.strip(),
                competitors = [c.strip() for c in req.competitors if c.strip()],
                keywords    = [k.strip() for k in req.keywords    if k.strip()],
                location    = req.location.strip(),
                platforms   = platforms,
            ))
            _scraper_jobs[job_id]["status"] = "complete"
            _scraper_jobs[job_id]["result"] = result
            logger.info(f"[{job_id}] Scraper complete: {result}")
        except Exception as e:
            _scraper_jobs[job_id]["status"] = "failed"
            _scraper_jobs[job_id]["error"]  = str(e)
            logger.error(f"[{job_id}] Scraper failed: {e}")
        finally:
            try: loop.close()
            except Exception: pass

    t = threading.Thread(target=_run_in_thread, daemon=True)
    t.start()

    logger.info(f"[{job_id}] Scraper queued: brand_id={req.brand_id} "
                f"brand={req.brand} platforms={platforms}")
    return {"job_id": job_id, "status": "queued",
            "brand_id": req.brand_id, "brand": req.brand,
            "platforms": platforms, "location": req.location}


@app.get("/scraper/status/{job_id}")
def scraper_status(job_id: str):
    """Get status of a brand intelligence scraper job."""
    job = _scraper_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Scraper job {job_id} not found")
    return job


@app.get("/scraper/jobs")
def scraper_jobs_list():
    """List all scraper jobs."""
    return {"total": len(_scraper_jobs),
            "jobs": list(_scraper_jobs.values())}


@app.get("/brand/{brand_id}/intelligence")
def get_brand_intelligence(brand_id: int, platform: str = None, limit: int = 50):
    """
    Retrieve scraped brand intelligence for a brand.
    Optional: ?platform=reddit|quora|medium|tumblr
    """
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            if platform:
                cur.execute("""
                    SELECT id, platform, url, title, subreddit, author,
                           comment_count, claps, sentiment, intent,
                           has_brand, has_competitor, score, created_at
                    FROM threads
                    WHERE brand_id=%s AND platform=%s
                    ORDER BY score DESC LIMIT %s
                """, (brand_id, platform, limit))
            else:
                cur.execute("""
                    SELECT id, platform, url, title, subreddit, author,
                           comment_count, claps, sentiment, intent,
                           has_brand, has_competitor, score, created_at
                    FROM threads
                    WHERE brand_id=%s
                    ORDER BY score DESC LIMIT %s
                """, (brand_id, limit))
            cols  = [d[0] for d in cur.description]
            rows  = cur.fetchall()
        threads = [dict(zip(cols, r)) for r in rows]
        # Serialize datetime fields
        for t in threads:
            if t.get("created_at"):
                t["created_at"] = t["created_at"].isoformat()
        return {"brand_id": brand_id, "platform": platform,
                "total": len(threads), "threads": threads}
    except Exception as e:
        logger.error(f"get_brand_intelligence error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)


@app.get("/brand/{brand_id}/intelligence/analysis")
def get_brand_analysis(brand_id: int, platform: str = None):
    """Retrieve AI analysis sections for a brand."""
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            if platform:
                cur.execute("""
                    SELECT platform, section_id, section_title, section_icon,
                           section_order, content, brand_name, created_at
                    FROM ai_analyses
                    WHERE brand_id=%s AND platform=%s
                    ORDER BY platform, section_order
                """, (brand_id, platform))
            else:
                cur.execute("""
                    SELECT platform, section_id, section_title, section_icon,
                           section_order, content, brand_name, created_at
                    FROM ai_analyses
                    WHERE brand_id=%s
                    ORDER BY platform, section_order
                """, (brand_id,))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        sections = [dict(zip(cols, r)) for r in rows]
        for s in sections:
            if s.get("created_at"):
                s["created_at"] = s["created_at"].isoformat()
        return {"brand_id": brand_id, "platform": platform,
                "total": len(sections), "sections": sections}
    except Exception as e:
        logger.error(f"get_brand_analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_job_or_404(job_id: str) -> dict:
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return jobs[job_id]

# ── Background Task ────────────────────────────────────────────────────────────

def _run_audit_task(job_id, url, brand_id, target_location,
                    business_type, ai_mode, crawl_limit, run_pagespeed):
    """Runs the full audit. All imports happen here — never at module level."""
    jobs[job_id]["status"]  = "running"
    jobs[job_id]["message"] = "Crawl in progress..."
    logger.info(f"[{job_id}] Starting audit: {url}")

    try:
        # Import inside task — safe, no startup crash risk
        from crawler import run_audit

        result = run_audit(
            input_url       = url,
            brand_id        = brand_id,
            target_location = target_location,
            business_type   = business_type,
            ai_mode         = ai_mode,
            crawl_limit     = crawl_limit,
            run_pagespeed   = run_pagespeed,
        )

        jobs[job_id].update({
            "status":       "completed",
            "message":      "Audit completed successfully",
            "audit_id":     result.get("audit_id"),
            "excel_file":   result.get("excel_file"),
            "pdf_file":     result.get("pdf_file"),
            "completed_at": datetime.utcnow().isoformat(),
        })
        logger.info(f"[{job_id}] Done ✓ audit_id={result.get('audit_id')}")

    except Exception as e:
        err_detail = traceback.format_exc()
        logger.error(f"[{job_id}] FAILED:\n{err_detail}")
        jobs[job_id].update({
            "status":       "failed",
            "message":      "Audit failed — see error field",
            "error":        f"{type(e).__name__}: {str(e)[:1000]}",
            "completed_at": datetime.utcnow().isoformat(),
        })