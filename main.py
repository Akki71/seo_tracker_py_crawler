"""
AquilTechLabs SEO Crawler API
FastAPI wrapper for the SEO crawler — accepts brand_id + url via REST API.
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Header
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import uuid, os, logging, json
from datetime import datetime

from db import get_db_conn, release_db_conn
from crawler import run_audit

app = FastAPI(
    title="AquilTechLabs SEO Crawler API",
    description="Full SEO audit: crawl, AI analysis, Excel + PDF export, PostgreSQL storage.",
    version="2.0.0",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# In-memory job tracker (use Redis in production for multi-worker)
jobs: dict = {}

# ── Request / Response Models ──────────────────────────────────────────────────

class AuditRequest(BaseModel):
    url: str                                   # e.g. "https://example.com"
    brand_id: int                              # your brands table primary key
    target_location: Optional[str] = ""        # e.g. "Mumbai, India"
    ai_mode: Optional[str] = "1"              # "1"=OpenAI, "2"=Claude, "3"=Hybrid, "4"=Skip
    crawl_limit: Optional[int] = 100
    run_pagespeed: Optional[bool] = True

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

# ── Health Check ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "AquilTechLabs SEO Crawler API", "version": "2.0.0"}

# ── Start Audit ────────────────────────────────────────────────────────────────

@app.post("/audit/start", response_model=AuditStatusResponse)
async def start_audit(req: AuditRequest, background_tasks: BackgroundTasks):
    """
    Start a full SEO audit in the background.
    Returns a job_id to poll for status.
    """
    # Basic validation
    url = req.url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "queued",
        "message": "Audit queued",
        "audit_id": None,
        "excel_file": None,
        "pdf_file": None,
        "started_at": datetime.utcnow().isoformat(),
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
        crawl_limit=req.crawl_limit or 100,
        run_pagespeed=req.run_pagespeed,
    )

    logger.info(f"Audit job {job_id} queued: brand_id={req.brand_id} url={url}")
    return AuditStatusResponse(job_id=job_id, status="queued", message="Audit started", **jobs[job_id])

# ── Audit Status ───────────────────────────────────────────────────────────────

@app.get("/audit/status/{job_id}", response_model=AuditStatusResponse)
def audit_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    j = jobs[job_id]
    return AuditStatusResponse(job_id=job_id, **j)

# ── Download Files ─────────────────────────────────────────────────────────────

@app.get("/audit/download/{job_id}/excel")
def download_excel(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    path = jobs[job_id].get("excel_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Excel file not ready yet")
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=os.path.basename(path))

@app.get("/audit/download/{job_id}/pdf")
def download_pdf(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    path = jobs[job_id].get("pdf_file")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="PDF file not ready yet")
    return FileResponse(path, media_type="application/pdf", filename=os.path.basename(path))

# ── List Audits for a Brand ────────────────────────────────────────────────────

@app.get("/brand/{brand_id}/audits")
def list_brand_audits(brand_id: int):
    """Return all audit records for a given brand_id from PostgreSQL."""
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
            # Serialize datetime
            for r in result:
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
        return {"brand_id": brand_id, "audits": result}
    finally:
        release_db_conn(conn)

# ── Get Single Audit Detail ────────────────────────────────────────────────────

@app.get("/audit/{audit_id}")
def get_audit(audit_id: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM audits WHERE id = %s", (audit_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Audit not found")
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
            for k, v in result.items():
                if hasattr(v, "isoformat"):
                    result[k] = v.isoformat()
        return result
    finally:
        release_db_conn(conn)

# ── Background Task ────────────────────────────────────────────────────────────

def _run_audit_task(job_id, url, brand_id, target_location, ai_mode, crawl_limit, run_pagespeed):
    jobs[job_id]["status"] = "running"
    jobs[job_id]["message"] = "Crawl in progress..."
    try:
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
        logger.info(f"Job {job_id} completed. audit_id={result.get('audit_id')}")
    except Exception as e:
        logger.exception(f"Job {job_id} failed: {e}")
        jobs[job_id].update({
            "status": "failed",
            "message": "Audit failed",
            "error": str(e),
            "completed_at": datetime.utcnow().isoformat(),
        })
