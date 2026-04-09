# AquilTechLabs SEO Crawler API v2.0

Full SEO audit REST API — crawl + AI analysis + Excel/PDF export + PostgreSQL storage.

---

## 🖥️ Run Locally (Your Computer)

### Prerequisites
- Python 3.10 or newer → https://python.org/downloads
- PostgreSQL database (local or remote)

---

### Option A — Mac / Linux / WSL (easiest)

```bash
# 1. Open terminal in the project folder
cd seo_crawler_api

# 2. Run the setup script
./run_local.sh
```

The script will:
- Create a Python virtual environment
- Install all packages
- Create `.env` from `.env.example`
- Ask you to fill in DB credentials
- Start the server at http://localhost:8000

---

### Option B — Windows

```cmd
# Double-click run_local.bat
# OR open Command Prompt in the folder and run:
run_local.bat
```

---

### Option C — Manual steps (any OS)

```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate it
# Mac/Linux:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 3. Install packages
pip install -r requirements.txt

# 4. Create .env file
cp .env.example .env
# Edit .env with your DB credentials

# 5. Load env vars
# Mac/Linux:
export $(grep -v '^#' .env | xargs)
# Windows CMD: set vars manually or use the .bat script

# 6. Start
python startup.py
```

---

### Testing Locally

After starting, open a new terminal:

```bash
# Health check
curl http://localhost:8000/health

# Start an audit (ai_mode=4 = skip AI, fastest for testing)
curl -X POST http://localhost:8000/audit/start \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","brand_id":1,"ai_mode":"4","crawl_limit":5}'

# You'll get back a job_id — poll status:
curl http://localhost:8000/audit/status/YOUR_JOB_ID_HERE

# Interactive API docs (open in browser):
open http://localhost:8000/docs
```

---

## 🌐 Coolify Deployment (GoDaddy Domain)

### Step 1 — Push to GitHub

```bash
cd seo_crawler_api
git init
git add .
git commit -m "SEO Crawler API v2.0"
# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USER/seo-crawler-api.git
git push -u origin main
```

### Step 2 — Coolify Setup

1. Log in to your Coolify dashboard
2. Click **+ New Resource** → **Application**
3. Connect your GitHub repo
4. **Build Pack**: Select **Nixpacks** (it auto-detects via `nixpacks.toml`)
5. **Port**: `8000`

### Step 3 — Environment Variables in Coolify

In Coolify → your service → **Environment Variables**, add:

```
DB_HOST        = your-postgres-host
DB_PORT        = 5432
DB_USER        = your_db_user
DB_PASSWORD    = your_db_password
DB_NAME        = seo_crawler
OPENAI_API_KEY = sk-...          (for ai_mode 1 or 3)
ANTHROPIC_API_KEY = sk-ant-...   (for ai_mode 2 or 3)
PAGESPEED_API_KEY = AIza...      (optional)
PORT           = 8000
```

### Step 4 — GoDaddy Domain

1. In Coolify → your service → **Domains**, add your domain:
   ```
   seo-crawler.yourdomain.com
   ```
2. In GoDaddy DNS → add a **CNAME** record:
   ```
   Type:  CNAME
   Name:  seo-crawler          (subdomain you want)
   Value: your-coolify-server-ip-or-hostname
   TTL:   600
   ```
   OR add an **A** record pointing to your server IP.
3. Enable **HTTPS** in Coolify (it auto-provisions Let's Encrypt certificate)
4. Deploy — wait ~2 min for DNS to propagate

### Step 5 — Verify Deployment

```bash
# Replace with your actual domain
curl https://seo-crawler.yourdomain.com/health
```

Expected response:
```json
{
  "status": "ok",
  "version": "2.0.0",
  "database": "connected",
  "active_jobs": 0
}
```

---

## 📡 API Reference

### POST /audit/start

Start a full SEO audit. Returns instantly with a `job_id`.

**Request:**
```json
{
  "url": "https://gulfpharmacy.com/",
  "brand_id": 103,
  "ai_mode": "3",
  "crawl_limit": 100,
  "target_location": "Dubai, UAE",
  "run_pagespeed": false
}
```

**ai_mode values:**
| Value | Description |
|-------|-------------|
| `"1"` | OpenAI only (GPT-4o-mini) — needs OPENAI_API_KEY |
| `"2"` | Claude only (Haiku + Sonnet) — needs ANTHROPIC_API_KEY |
| `"3"` | Hybrid: OpenAI bulk + Claude strategy — needs both keys |
| `"4"` | **Skip AI** — crawl only, fastest, free |

**Response:**
```json
{
  "job_id": "a1b2c3d4-e5f6-...",
  "status": "queued",
  "message": "Audit queued for https://gulfpharmacy.com/",
  "started_at": "2024-01-15T10:30:00"
}
```

---

### GET /audit/status/{job_id}

Poll until `status` = `completed` or `failed`.

| Status | Meaning |
|--------|---------|
| `queued` | Waiting to start |
| `running` | Crawling / analyzing |
| `completed` | Done — download files |
| `failed` | Error — check `error` field |

---

### GET /audit/download/{job_id}/excel
### GET /audit/download/{job_id}/pdf

Download reports. Only works when status = `completed`.

---

### GET /brand/{brand_id}/audits

List all audits for a brand from PostgreSQL.

---

## 🔄 Complete Usage Example

```python
import requests, time

API = "https://seo-crawler.yourdomain.com"

# 1. Start audit
r = requests.post(f"{API}/audit/start", json={
    "url": "https://gulfpharmacy.com/",
    "brand_id": 103,
    "ai_mode": "3",
    "crawl_limit": 100,
    "run_pagespeed": False,
})
job_id = r.json()["job_id"]
print(f"Started: {job_id}")

# 2. Poll status every 15 seconds
while True:
    s = requests.get(f"{API}/audit/status/{job_id}").json()
    print(f"Status: {s['status']} — {s['message']}")
    if s["status"] in ("completed", "failed"):
        break
    time.sleep(15)

# 3. Download files
if s["status"] == "completed":
    # Excel
    r = requests.get(f"{API}/audit/download/{job_id}/excel")
    open("report.xlsx", "wb").write(r.content)
    # PDF
    r = requests.get(f"{API}/audit/download/{job_id}/pdf")
    open("report.pdf", "wb").write(r.content)
    print(f"Downloaded! audit_id={s['audit_id']}")
else:
    print(f"Failed: {s['error']}")
```

---

## 🐛 Troubleshooting

### 503 No Available Server
- Check Coolify logs → your service → **Logs** tab
- Most common cause: missing DB env vars
- Try: set `ai_mode=4` to skip AI and test crawl only

### DB Connection Failed
- Ensure PostgreSQL is accessible from Coolify's network
- Check `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- Tables are auto-created on first successful connection

### Job stays in "running" forever
- Check server logs for traceback
- Reduce `crawl_limit` (try 10 first)
- Use `ai_mode=4` (skip AI) to isolate the issue
