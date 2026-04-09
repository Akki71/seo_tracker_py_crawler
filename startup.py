#!/usr/bin/env python3
"""
startup.py — Load .env, verify packages, init DB schema, start uvicorn.
Works in: local Windows/Mac/Linux, Docker (Coolify), Nixpacks.
"""
import sys, os, traceback, subprocess

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)

print("=" * 60)
print("AquilTechLabs SEO Crawler API v2.0")
print(f"Python: {sys.version.split()[0]}  |  Platform: {sys.platform}")
print("=" * 60)

# ── 1. Load .env (local dev only — Coolify sets vars directly) ──
print("\n[1] Environment setup...")
_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env, override=False)  # Don't override vars already set by Coolify
        print(f"    ✓ .env loaded")
    except ImportError:
        with open(_env, encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip().replace('\r','')
                if _line and not _line.startswith("#") and "=" in _line:
                    k, _, v = _line.partition("=")
                    k = k.strip(); v = v.strip().strip('"').strip("'")
                    if k and v: os.environ.setdefault(k, v)
        print("    ✓ .env loaded manually")
else:
    print("    (no .env file — using environment variables from Coolify/system)")

# ── 2. Show config ─────────────────────────────────────────────
print("\n[2] Configuration:")
_db = {k: os.environ.get(k,"") for k in ["DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME"]}
for k, v in _db.items():
    print(f"    {k:15s} = {'****' if k=='DB_PASSWORD' and v else (v or '⚠ NOT SET')}")
_miss = [k for k,v in _db.items() if not v]
if _miss:
    print(f"\n    ⚠ Missing DB vars: {_miss}")
else:
    print("    DB ✓")
print(f"    OPENAI_API_KEY    = {'SET ✓' if os.environ.get('OPENAI_API_KEY') else 'not set (ai_mode=4 will work)'}")
print(f"    ANTHROPIC_API_KEY = {'SET ✓' if os.environ.get('ANTHROPIC_API_KEY') else 'not set'}")
print(f"    PAGESPEED_API_KEY = {'SET ✓' if os.environ.get('PAGESPEED_API_KEY') else 'not set (pagespeed disabled)'}")

# ── 3. Verify AI packages ──────────────────────────────────────
print("\n[3] AI package check...")
_ai_ok = False

def _test_openai():
    try:
        from openai import OpenAI
        OpenAI(api_key="test-startup-check")
        return True
    except TypeError:
        return False  # proxies error
    except Exception:
        return True   # auth error is fine — client initialized OK

def _test_anthropic():
    try:
        import anthropic
        anthropic.Anthropic(api_key="test-startup-check")
        return True
    except TypeError:
        return False
    except Exception:
        return True

_oai_ok = _test_openai()
_ant_ok = _test_anthropic()

try:
    import openai
    print(f"    openai {openai.__version__}: {'✓' if _oai_ok else '✗ proxies error'}")
except: print("    openai: not installed")

try:
    import anthropic as _ant
    print(f"    anthropic {_ant.__version__}: {'✓' if _ant_ok else '✗ proxies error'}")
except: print("    anthropic: not installed")

try:
    import httpx
    print(f"    httpx {httpx.__version__}: ✓")
except: print("    httpx: not installed")

if not _oai_ok or not _ant_ok:
    print("\n    ⚠ AI packages have version conflict.")
    print("    Attempting auto-fix: pip install openai==1.58.1 anthropic==0.40.0 httpx==0.27.2")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install",
         "openai==1.58.1", "anthropic==0.40.0", "httpx==0.27.2", "-q"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("    ✓ Fixed! Server will use updated packages.")
        _ai_ok = True
    else:
        print(f"    ✗ Auto-fix failed. Run manually: pip install openai==1.58.1 anthropic==0.40.0 httpx==0.27.2")
else:
    _ai_ok = True
    print("    ✓ AI packages OK")

# ── 4. Package check ───────────────────────────────────────────
print("\n[4] All packages:")
_bad = []
for label, mod in [("fastapi","fastapi"),("uvicorn","uvicorn"),("psycopg2","psycopg2"),
                    ("requests","requests"),("bs4","bs4"),("openpyxl","openpyxl"),
                    ("reportlab","reportlab"),("Pillow","PIL")]:
    try:
        m = __import__(mod)
        ver = getattr(m, '__version__', '')
        print(f"    ✓ {label} {ver}".rstrip())
    except ImportError as e:
        print(f"    ✗ {label}: {e}")
        _bad.append(label)

if _bad:
    print(f"\n    ⚠ Missing: {_bad}")
    print("    Run: pip install -r requirements.txt")
else:
    print("    ✓ All OK")

# ── 5. Directories ─────────────────────────────────────────────
print("\n[5] Directories...")
for d in ["output", "screenshots", "logs"]:
    os.makedirs(d, exist_ok=True)
    print(f"    ✓ {d}/")

# ── 6. DB Schema ───────────────────────────────────────────────
print("\n[6] PostgreSQL schema...")
_db_ready = False
if _miss:
    print("    SKIPPED — set DB env vars first")
else:
    try:
        from db import init_db
        init_db()
        print("    ✓ Schema ready (all tables exist)")
        _db_ready = True
    except Exception as e:
        print(f"    ✗ {e}")
        print("    → Check DB credentials in Coolify environment variables")

# ── 7. Start ───────────────────────────────────────────────────
_port = int(os.environ.get("PORT", 8000))
print(f"\n[7] Starting server on port {_port}...")
print()
print(f"  ┌────────────────────────────────────────────────────┐")
print(f"  │  Local:  http://localhost:{_port}                    │")
print(f"  │  Docs:   http://localhost:{_port}/docs               │")
print(f"  │  Health: http://localhost:{_port}/health             │")
print(f"  │  DB:     {'Connected ✓' if _db_ready else 'NOT connected ⚠':30s}│")
print(f"  │  AI:     {'Ready ✓' if _ai_ok else 'Check packages ⚠':30s}│")
print(f"  └────────────────────────────────────────────────────┘")
print()
print("  Press Ctrl+C to stop")
print("=" * 60)

try:
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=_port,
        workers=1,        # 1 worker — jobs dict is in-memory
        log_level="info",
        access_log=True,
    )
except KeyboardInterrupt:
    print("\nServer stopped.")
except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
