"""
AquilTechLabs SEO Crawler API v2.0
Accepts brand_id + url via REST API.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import uuid, os, logging, sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Lifespan: init DB on startup ───────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI startup: initializing DB schema...")
    try:
        from db import init_db
        init_db()
        logger.info("DB schema ready.")
    except Exception as e:
        logger.error(f"DB init failed (API still running): {e}")
    yield
    logger.info("FastAPI shutdown.")


app = FastAPI(
    title="AquilTechLabs SEO Crawler API",
    description="Full SEO audit — crawl, AI analysis, Excel + PDF, PostgreSQL storage.",
    version="2.0.0",
    lifespan=lifespan,
)

# In-memory job store (sufficient for single-worker; swap for Redis if scaling)
jobs: dict = {}

# ── Models ─────────────────────────────────────────────────────────────────────

class AuditRequest(BaseModel):
    url: str
    brand_id: int
    target_location: Optional[str] = ""
    ai_mode: Optional[str] = "1"        # 1=OpenAI, 2=Claude, 3=Hybrid, 4=Skip
    crawl_limit: Optional[int] = 100
    run_pagespeed: Optional[bool] = False  # default OFF — expensive & slow

class AuditStatusResponse(BaseModel):
    job_id: str
    status: str
    message: str
    audit_id: Optional[int] = None
    excel_file: Optional[str] = None
    pdf_file: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None

# ── Root / Health ──────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "AquilTechLabs SEO Crawler API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "health":       "GET  /health",
            "start_audit":  "POST /audit/start",
            "audit_status": "GET  /audit/status/{job_id}",
            "download_excel":"GET  /audit/download/{job_id}/excel",
            "download_pdf": "GET  /audit/download/{job_id}/pdf",
            "brand_audits": "GET  /brand/{brand_id}/audits",
            "audit_detail": "GET  /audit/{audit_id}",
            "docs":         "GET  /docs",
        }
    }

@app.get("/health")
def health():
    db_ok = False
    try:
        from db import get_db_conn, release_db_conn
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        release_db_conn(conn)
        db_ok = True
    except Exception as e:
        logger.warning(f"DB health check failed: {e}")

    return {
        "status": "ok",
        "service": "AquilTechLabs SEO Crawler API",
        "version": "2.0.0",
        "database": "connected" if db_ok else "disconnected",
        "active_jobs": len(jobs),
    }

# ── Start Audit ────────────────────────────────────────────────────────────────

@app.post("/audit/start", response_model=AuditStatusResponse)
async def start_audit(req: AuditRequest, background_tasks: BackgroundTasks):
    """
    Start a full SEO audit in the background.
    Returns a job_id immediately — poll /audit/status/{job_id} for progress.
    """
    url = req.url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    # Validate brand_id is positive
    if req.brand_id <= 0:
        raise HTTPException(status_code=400, detail="brand_id must be a positive integer")

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    jobs[job_id] = {
        "status": "queued",
        "message": f"Audit queued for {url}",
        "audit_id": None,
        "excel_file": None,
        "pdf_file": None,
        "started_at": now,
        "completed_at": None,
        "error": None,
    }

    background_tasks.add_task(
        _run_audit_task,
        job_id=job_id,
        url=url,
        brand_id=req.brand_id,
        target_location=req.target_location or "",
        ai_mode=req.ai_mode or "1",
        crawl_limit=min(req.crawl_limit or 100, 500),   # hard cap at 500
        run_pagespeed=req.run_pagespeed or False,
    )

    logger.info(f"[{job_id}] Queued: brand_id={req.brand_id} url={url} ai_mode={req.ai_mode} limit={req.crawl_limit}")
    return AuditStatusResponse(job_id=job_id, **jobs[job_id])

# ── Status ─────────────────────────────────────────────────────────────────────

@app.get("/audit/status/{job_id}", response_model=AuditStatusResponse)
def audit_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return AuditStatusResponse(job_id=job_id, **jobs[job_id])

# ── List all jobs (debug) ──────────────────────────────────────────────────────

@app.get("/jobs")
def list_jobs():
    """List all in-memory jobs (current server session only)."""
    return {
        "total": len(jobs),
        "jobs": [{"job_id": k, "status": v["status"], "started_at": v["started_at"]} for k, v in jobs.items()]
    }

# ── Download ───────────────────────────────────────────────────────────────────

@app.get("/audit/download/{job_id}/excel")
def download_excel(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    j = jobs[job_id]
    if j["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Audit is '{j['status']}', not yet completed")
    path = j.get("excel_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Excel file not found on disk")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(path)
    )

@app.get("/audit/download/{job_id}/pdf")
def download_pdf(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    j = jobs[job_id]
    if j["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Audit is '{j['status']}', not yet completed")
    path = j.get("pdf_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="PDF file not found on disk")
    return FileResponse(path, media_type="application/pdf", filename=os.path.basename(path))

# ── Brand audits from DB ───────────────────────────────────────────────────────

@app.get("/brand/{brand_id}/audits")
def list_brand_audits(brand_id: int):
    """Return all audit records for a given brand_id from PostgreSQL."""
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, domain, base_url, audit_status, ai_mode,
                       total_pages_crawled, pages_200, pages_404,
                       broken_links_count, audit_timestamp,
                       excel_file, pdf_file
                FROM audits
                WHERE brand_id = %s
                ORDER BY id DESC
                LIMIT 50
            """, (brand_id,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            result = [dict(zip(cols, row)) for row in rows]
            for r in result:
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
        return {"brand_id": brand_id, "total": len(result), "audits": result}
    except Exception as e:
        logger.error(f"DB error listing brand audits: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)

# ── Single audit from DB ───────────────────────────────────────────────────────

@app.get("/audit/{audit_id}")
def get_audit(audit_id: int):
    from db import get_db_conn, release_db_conn
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM audits WHERE id = %s", (audit_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Audit {audit_id} not found")
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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_db_conn(conn)

# ── Background task ────────────────────────────────────────────────────────────

def _run_audit_task(job_id, url, brand_id, target_location, ai_mode, crawl_limit, run_pagespeed):
    jobs[job_id]["status"] = "running"
    jobs[job_id]["message"] = "Crawl in progress..."
    logger.info(f"[{job_id}] Starting audit: {url}")

    try:
        from crawler import run_audit
        result = run_audit(
            input_url=url,
            brand_id=brand_id,
            target_location=target_location,
            ai_mode=ai_mode,
            crawl_limit=crawl_limit,
            run_pagespeed=run_pagespeed,
        )
        jobs[job_id].update({
            "status": "completed",
            "message": "Audit completed successfully",
            "audit_id": result.get("audit_id"),
            "excel_file": result.get("excel_file"),
            "pdf_file": result.get("pdf_file"),
            "completed_at": datetime.utcnow().isoformat(),
        })
        logger.info(f"[{job_id}] Completed. audit_id={result.get('audit_id')}")

    except Exception as e:
        logger.exception(f"[{job_id}] Audit failed: {e}")
        jobs[job_id].update({
            "status": "failed",
            "message": "Audit failed — check error field",
            "error": str(e)[:2000],
            "completed_at": datetime.utcnow().isoformat(),
        })
