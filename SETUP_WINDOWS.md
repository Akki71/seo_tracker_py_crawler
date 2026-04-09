# Windows Local Setup Guide

## The Problem You Saw
```
DB: WARNING: Schema init failed — fe_sendauth: no password supplied
```
This means your `.env` file credentials were not loaded. Follow these steps exactly.

---

## Step 1 — Edit Your .env File

Open `.env` in Notepad (NOT `.env.example` — the actual `.env`):

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=YOUR_ACTUAL_PASSWORD_HERE
DB_NAME=seo_crawler

OPENAI_API_KEY=sk-proj-...your-key...
ANTHROPIC_API_KEY=sk-ant-...your-key...

PORT=8000
```

**Save the file.**

---

## Step 2 — Install PostgreSQL (if not installed)

Download from: https://www.postgresql.org/download/windows/

During install:
- Set a password for user `postgres` — **remember this password**
- Default port: `5432` — leave as is
- After install, PostgreSQL runs as a Windows service automatically

---

## Step 3 — Create the Database

Open **pgAdmin** (installed with PostgreSQL) or use Command Prompt:

```cmd
# Open Command Prompt as Administrator
psql -U postgres
```

Then type:
```sql
CREATE DATABASE seo_crawler;
\q
```

Or in pgAdmin: right-click "Databases" → Create → Database → name it `seo_crawler`

---

## Step 4 — Install Python packages

Open Command Prompt in the `seo_crawler_api` folder:

```cmd
# Activate virtual environment (if not already active)
venv\Scripts\activate

# Install python-dotenv (needed to read .env)
pip install python-dotenv

# Install all requirements
pip install -r requirements.txt
```

---

## Step 5 — Run the server

```cmd
python startup.py
```

You should see:
```
[5] Initializing PostgreSQL schema...
    Schema ready ✓

[6] Starting server on port 8000...
  ┌─────────────────────────────────────────────┐
  │  Local URL:  http://localhost:8000          │
  │  DB Status:  Connected ✓                    │
  └─────────────────────────────────────────────┘
```

---

## Step 6 — Test It

Open a NEW Command Prompt window:

```cmd
# Health check
curl http://localhost:8000/health

# Start a test audit (ai_mode=4 = no AI, fastest)
curl -X POST http://localhost:8000/audit/start ^
  -H "Content-Type: application/json" ^
  -d "{\"url\":\"https://gulfpharmacy.com/\",\"brand_id\":103,\"ai_mode\":\"4\",\"crawl_limit\":10}"
```

Or open your browser:
- http://localhost:8000/docs  ← Interactive API docs
- http://localhost:8000/health ← Health check

---

## Common Errors

### `fe_sendauth: no password supplied`
→ Your `.env` has `DB_PASSWORD=` empty or the file isn't being read.
→ Make sure your `.env` has `DB_PASSWORD=yourpassword` (no quotes needed)

### `connection refused` on port 5432
→ PostgreSQL isn't running.
→ Open Windows Services (Win+R → `services.msc`) → find `postgresql-x64-16` → Start

### `database "seo_crawler" does not exist`
→ Run: `psql -U postgres -c "CREATE DATABASE seo_crawler;"`

### urllib3 version warning
→ This is just a warning, not an error. The server still works fine. Ignore it.

---

## Using with Coolify (Production)

For Coolify, set all the env vars in the Coolify dashboard under:
Service → Environment Variables

The `.env` file is only for local development.
