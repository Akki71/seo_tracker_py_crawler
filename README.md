# AquilTechLabs SEO Crawler API v2.0

Full SEO audit system converted to a REST API with PostgreSQL storage.  
Accepts `brand_id` + `url` → crawls, runs AI analysis, exports Excel + PDF, saves everything to Postgres.

---

## 📁 Project Structure

```
seo_crawler_api/
├── main.py            ← FastAPI app (routes, job tracking)
├── crawler.py         ← Core crawl + analyze orchestrator
├── db.py              ← PostgreSQL layer (schema, all insert/update helpers)
├── ai_helpers.py      ← All OpenAI / Claude AI calls
├── excel_export.py    ← Excel report generator
├── pdf_export.py      ← PDF report generator
├── scorecard.py       ← Pass/fail scorecard builder
├── startup.py         ← Init DB schema + start uvicorn
├── requirements.txt
├── Dockerfile
├── docker-compose.yml ← For local testing
└── README.md
```

---

## 🔑 Environment Variables

Set these in Coolify → Service → Environment Variables:

| Variable             | Required | Description                          |
|----------------------|----------|--------------------------------------|
| `DB_HOST`            | ✅       | PostgreSQL host                      |
| `DB_PORT`            | ✅       | PostgreSQL port (default: 5432)      |
| `DB_USER`            | ✅       | PostgreSQL username                  |
| `DB_PASSWORD`        | ✅       | PostgreSQL password                  |
| `DB_NAME`            | ✅       | PostgreSQL database name             |
| `OPENAI_API_KEY`     | ⚠️ one  | For ai_mode 1 or 3                   |
| `ANTHROPIC_API_KEY`  | ⚠️ one  | For ai_mode 2 or 3                   |
| `PAGESPEED_API_KEY`  | optional | Google PageSpeed Insights key        |

---

## 🚀 Coolify Deployment

### Step 1 — Push to Git
Push this folder to a GitHub/GitLab repo.

### Step 2 — New Service in Coolify
1. Coolify Dashboard → **New Service** → **Dockerfile**
2. Connect your Git repo
3. Set **Port**: `8000`
4. Add all environment variables (see table above)
5. **Deploy**

### Step 3 — Verify
```
GET https://your-coolify-domain.com/health
→ {"status":"ok","service":"AquilTechLabs SEO Crawler API","version":"2.0.0"}
```

### Step 4 — Database
The API auto-creates all tables on first startup via `startup.py → init_db()`.  
No manual SQL migration needed.

---

## 📡 API Reference

### Base URL
```
https://your-coolify-domain.com
```

---

### `GET /health`
Health check.

**Response:**
```json
{"status": "ok", "service": "AquilTechLabs SEO Crawler API", "version": "2.0.0"}
```

---

### `POST /audit/start`
Start a full SEO audit in the background.

**Request Body:**
```json
{
  "url": "https://example.com",
  "brand_id": 42,
  "target_location": "Mumbai, India",
  "ai_mode": "1",
  "crawl_limit": 100,
  "run_pagespeed": true
}
```

| Field             | Type    | Required | Description                                              |
|-------------------|---------|----------|----------------------------------------------------------|
| `url`             | string  | ✅       | Website URL to audit                                     |
| `brand_id`        | integer | ✅       | Your brands table primary key                            |
| `target_location` | string  | ❌       | e.g. "Mumbai, India" — used for keyword context          |
| `ai_mode`         | string  | ❌       | `"1"`=OpenAI, `"2"`=Claude, `"3"`=Hybrid, `"4"`=Skip AI |
| `crawl_limit`     | integer | ❌       | Max pages to crawl (default: 100)                        |
| `run_pagespeed`   | boolean | ❌       | Run Google PageSpeed for each page (default: true)       |

**Response:**
```json
{
  "job_id": "a1b2c3d4-...",
  "status": "queued",
  "message": "Audit started",
  "audit_id": null,
  "excel_file": null,
  "pdf_file": null,
  "started_at": "2024-01-15T10:30:00",
  "completed_at": null,
  "error": null
}
```

---

### `GET /audit/status/{job_id}`
Poll job status.

**Status values:** `queued` → `running` → `completed` | `failed`

**Response (completed):**
```json
{
  "job_id": "a1b2c3d4-...",
  "status": "completed",
  "message": "Audit completed successfully",
  "audit_id": 7,
  "excel_file": "output/example.com_20240115_103045_SEO.xlsx",
  "pdf_file": "output/example.com_20240115_103045_SEO.pdf",
  "started_at": "2024-01-15T10:30:00",
  "completed_at": "2024-01-15T10:45:22",
  "error": null
}
```

---

### `GET /audit/download/{job_id}/excel`
Download the Excel report once status is `completed`.

### `GET /audit/download/{job_id}/pdf`
Download the PDF report once status is `completed`.

---

### `GET /brand/{brand_id}/audits`
List all audits for a brand.

**Response:**
```json
{
  "brand_id": 42,
  "audits": [
    {
      "id": 7,
      "domain": "example.com",
      "base_url": "https://example.com",
      "audit_status": "complete",
      "total_pages_crawled": 45,
      "pages_200": 40,
      "pages_404": 3,
      "broken_links_count": 2,
      "audit_timestamp": "2024-01-15T10:30:00"
    }
  ]
}
```

---

### `GET /audit/{audit_id}`
Get full audit record from PostgreSQL.

---

## 🔄 Typical Workflow (Client Code)

```python
import requests, time

API = "https://your-coolify-domain.com"

# 1. Start audit
resp = requests.post(f"{API}/audit/start", json={
    "url": "https://example.com",
    "brand_id": 42,
    "target_location": "London, UK",
    "ai_mode": "3",
    "crawl_limit": 200,
})
job_id = resp.json()["job_id"]
print(f"Job started: {job_id}")

# 2. Poll until done
while True:
    status = requests.get(f"{API}/audit/status/{job_id}").json()
    print(f"Status: {status['status']} — {status['message']}")
    if status["status"] in ("completed", "failed"):
        break
    time.sleep(15)

# 3. Download files
if status["status"] == "completed":
    excel = requests.get(f"{API}/audit/download/{job_id}/excel")
    with open("report.xlsx", "wb") as f:
        f.write(excel.content)

    pdf = requests.get(f"{API}/audit/download/{job_id}/pdf")
    with open("report.pdf", "wb") as f:
        f.write(pdf.content)

    print(f"Reports downloaded! audit_id={status['audit_id']}")
```

---

## 🗄️ Database Schema

All tables are auto-created on startup. Key tables:

| Table                  | Description                              |
|------------------------|------------------------------------------|
| `audits`               | One row per audit run (has `brand_id`)   |
| `pages`                | One row per crawled page                 |
| `broken_links`         | Broken link records                      |
| `images`               | Image alt audit records                  |
| `seo_keywords`         | AI-detected keywords per service         |
| `blog_topics`          | AI blog topic ideas                      |
| `backlink_strategies`  | Backlink strategy by category            |
| `six_month_plan`       | Month-by-month execution plan            |
| `internal_linking`     | Internal link strategy                   |
| `keyword_url_mapping`  | Keyword to page mapping                  |
| `axo_recommendations`  | AXO (AI Experience Optimization)         |
| `scorecard`            | Pass/fail scorecard results              |
| `aeo_faq`              | FAQ schema content                       |
| `audit_progress`       | Crawl resume tracking                    |
| `site_analysis`        | HTTP status distribution, depth maps     |
| `generated_files`      | sitemap.xml, robots.txt etc.             |

---

## 🤖 AI Modes

| Mode | Description                              | Best For           |
|------|------------------------------------------|--------------------|
| `1`  | OpenAI only (GPT-4o-mini)               | Cost-efficient     |
| `2`  | Claude only (Haiku bulk + Sonnet strat.) | Quality            |
| `3`  | Hybrid: GPT-4o-mini bulk + Sonnet strat.| Best cost/quality  |
| `4`  | Skip AI — crawl only                     | Speed / free       |

---

## 🧪 Local Testing

```bash
# Start postgres + api
docker compose up --build

# Test health
curl http://localhost:8000/health

# Start audit
curl -X POST http://localhost:8000/audit/start \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","brand_id":1,"ai_mode":"4","crawl_limit":10}'
```
