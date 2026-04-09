# Coolify Deployment Guide

## Step 1 — Push to GitHub

```bash
# In your seo_crawler_api folder:
git init
git add .
git commit -m "SEO Crawler API v2.0 - production ready"
git remote add origin https://github.com/YOUR_USER/seo-crawler-api.git
git push -u origin main
```

---

## Step 2 — Create Service in Coolify

1. Coolify Dashboard → **+ New Resource** → **Application**
2. Connect your GitHub repo
3. **Build Pack**: Choose **Dockerfile** (recommended) or Nixpacks
4. **Port**: `8000`
5. **Branch**: `main`

---

## Step 3 — Environment Variables

In Coolify → your service → **Environment Variables**, add ALL of these:

```
# PostgreSQL (your remote DB)
DB_HOST        = 147.93.154.209
DB_PORT        = 5432
DB_USER        = seo_tracker_user
DB_PASSWORD    = your_actual_password
DB_NAME        = seo_tracker

# AI providers
OPENAI_API_KEY    = sk-proj-...
ANTHROPIC_API_KEY = sk-ant-...
PAGESPEED_API_KEY = AIza...

# Server
PORT = 8000
```

---

## Step 4 — Volume Mount (for Excel/PDF files)

In Coolify → your service → **Storages** → Add:
```
Container Path:  /app/output
```
This keeps Excel/PDF files after container restarts.

---

## Step 5 — Domain Setup

### In Coolify:
1. Your service → **Domains** → Add your domain:
   ```
   seo-crawler.yourdomain.com
   ```
2. Enable **HTTPS** (auto Let's Encrypt)
3. Deploy

### In GoDaddy DNS:
Go to GoDaddy → DNS Management → Add record:
```
Type:  CNAME
Name:  seo-crawler        (the subdomain you want)
Value: your-coolify-server.com   (your Coolify server hostname)
TTL:   600
```

OR if using IP:
```
Type:  A
Name:  seo-crawler
Value: YOUR_COOLIFY_SERVER_IP
TTL:   600
```

Wait 2-10 minutes for DNS to propagate.

---

## Step 6 — Verify

```bash
# Health check
curl https://seo-crawler.yourdomain.com/health

# Expected response:
# {"status":"ok","database":"connected","active_jobs":0,...}
```

Open in browser: `https://seo-crawler.yourdomain.com/docs`

---

## Step 7 — Call the API

```bash
curl -X POST https://seo-crawler.yourdomain.com/audit/start \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://gulfpharmacy.com/",
    "brand_id": 103,
    "ai_mode": "3",
    "crawl_limit": 100,
    "run_pagespeed": false
  }'

# Returns:
# {"job_id": "abc123...", "status": "queued", ...}

# Poll status:
curl https://seo-crawler.yourdomain.com/audit/status/abc123...

# Download Excel when completed:
curl https://seo-crawler.yourdomain.com/audit/download/abc123.../excel -o report.xlsx
```
