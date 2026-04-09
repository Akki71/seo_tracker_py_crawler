#!/usr/bin/env python3
"""
startup.py — Loads .env, verifies packages, auto-fixes AI versions, inits DB, starts server.
"""
import sys, os, traceback, subprocess

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)

print("=" * 60)
print("AquilTechLabs SEO Crawler API v2.0")
print(f"Python: {sys.version.split()[0]}  |  Platform: {sys.platform}")
print("=" * 60)

# ── 1. Load .env ───────────────────────────────────────────────
print("\n[1] Loading .env...")
_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env, override=True)
        print(f"    ✓ {_env}")
    except ImportError:
        with open(_env, encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip().replace('\r','')
                if _line and not _line.startswith("#") and "=" in _line:
                    k, _, v = _line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        print("    ✓ .env loaded manually")
else:
    print("    WARNING: No .env file — copy .env.example to .env")

# ── 2. Environment check ───────────────────────────────────────
print("\n[2] Environment:")
_db = {k: os.environ.get(k,"") for k in ["DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME"]}
for k,v in _db.items():
    print(f"    {k:15s} = {'****' if k=='DB_PASSWORD' and v else (v or 'NOT SET ⚠')}")
_miss = [k for k,v in _db.items() if not v]
if _miss:
    print(f"\n    ⚠ Missing: {_miss} — edit .env")
else:
    print("    DB config ✓")
print(f"    OPENAI_API_KEY    = {'SET ✓' if os.environ.get('OPENAI_API_KEY') else 'not set'}")
print(f"    ANTHROPIC_API_KEY = {'SET ✓' if os.environ.get('ANTHROPIC_API_KEY') else 'not set'}")

# ── 3. Auto-fix AI packages ────────────────────────────────────
print("\n[3] Checking AI package versions...")
_need_fix = False
try:
    import openai
    _ov = openai.__version__
    print(f"    openai {_ov}")
    # Test if client actually works (this is what fails with proxies error)
    if os.environ.get("OPENAI_API_KEY"):
        try:
            _c = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            print("    ✓ OpenAI client OK")
        except TypeError:
            print("    ✗ OpenAI client has 'proxies' error — will fix")
            _need_fix = True
    else:
        # Test with dummy key
        try:
            _c = openai.OpenAI(api_key="test")
            print("    ✓ OpenAI client OK")
        except TypeError:
            print("    ✗ OpenAI has 'proxies' error — will fix")
            _need_fix = True
        except Exception:
            print("    ✓ OpenAI client OK (auth error expected with test key)")
except ImportError:
    print("    ✗ openai not installed — will install")
    _need_fix = True

try:
    import anthropic as _ant
    _av = _ant.__version__
    print(f"    anthropic {_av}")
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            _ac = _ant.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            print("    ✓ Anthropic client OK")
        except TypeError:
            print("    ✗ Anthropic has 'proxies' error — will fix")
            _need_fix = True
    else:
        try:
            _ac = _ant.Anthropic(api_key="test")
            print("    ✓ Anthropic client OK")
        except TypeError:
            print("    ✗ Anthropic has 'proxies' error — will fix")
            _need_fix = True
        except Exception:
            print("    ✓ Anthropic client OK (auth error expected with test key)")
except ImportError:
    print("    ✗ anthropic not installed — will install")
    _need_fix = True

if _need_fix:
    print("\n    Auto-fixing AI packages...")
    print("    Installing: openai==1.58.1 anthropic==0.40.0 httpx==0.27.2")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install",
         "openai==1.58.1", "anthropic==0.40.0", "httpx==0.27.2",
         "--upgrade", "-q"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("    ✓ Packages upgraded successfully!")
        print("    NOTE: Restart server for changes to take effect if this is first run.")
        # Try importing again after upgrade
        import importlib
        try:
            import openai; importlib.reload(openai)
        except Exception: pass
        try:
            import anthropic as _ant2; importlib.reload(_ant2)
        except Exception: pass
    else:
        print(f"    ✗ Auto-install failed: {result.stderr[:200]}")
        print("    Manual fix (run in PowerShell):")
        print("    pip install openai==1.58.1 anthropic==0.40.0 httpx==0.27.2")
else:
    print("    ✓ AI packages OK")

# ── 4. Test all packages ───────────────────────────────────────
print("\n[4] Package check...")
_bad = []
def _chk(label, mod):
    try:
        m = __import__(mod)
        ver = getattr(m, '__version__', '')
        print(f"    ✓ {label} {ver}".rstrip())
        return True
    except ImportError as e:
        print(f"    ✗ {label}: {e}")
        _bad.append(label)
        return False

_chk("fastapi","fastapi"); _chk("uvicorn","uvicorn")
_chk("psycopg2","psycopg2"); _chk("requests","requests")
_chk("bs4","bs4"); _chk("openpyxl","openpyxl")
_chk("reportlab","reportlab"); _chk("Pillow","PIL")
_chk("openai","openai"); _chk("anthropic","anthropic")

if _bad:
    print(f"\n    ⚠ Missing: {_bad} — run: pip install -r requirements.txt")
else:
    print("    All packages OK ✓")

# ── 5. Directories ─────────────────────────────────────────────
print("\n[5] Directories...")
for d in ["output","screenshots","logs"]:
    os.makedirs(d, exist_ok=True)
    print(f"    ✓ {d}/")

# ── 6. DB schema ───────────────────────────────────────────────
print("\n[6] PostgreSQL schema init...")
_db_ready = False
if _miss:
    print("    SKIPPED — fill in .env DB credentials first")
else:
    try:
        from db import init_db
        init_db()
        print("    ✓ Schema ready")
        _db_ready = True
    except Exception as e:
        print(f"    ⚠ {e}")
        print("    → Check DB_PASSWORD in .env")
        print("    → Run: python setup_db.py  to diagnose")

# ── 7. Start ───────────────────────────────────────────────────
_port = int(os.environ.get("PORT", 8000))
print(f"\n[7] Starting server on port {_port}...")
print()
print("  ┌──────────────────────────────────────────────────────┐")
print(f"  │  API:    http://localhost:{_port}                      │")
print(f"  │  Docs:   http://localhost:{_port}/docs                 │")
print(f"  │  Health: http://localhost:{_port}/health               │")
print(f"  │  DB:     {'Connected ✓' if _db_ready else 'NOT connected ⚠  → fix .env':30s}│")
print("  └──────────────────────────────────────────────────────┘")
if _need_fix:
    print()
    print("  ⚠ AI packages were just upgraded.")
    print("  ⚠ If AI still fails, press Ctrl+C and restart: python startup.py")
print()
print("  Ctrl+C to stop")
print("=" * 60)

try:
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=_port,
                workers=1, log_level="info", access_log=True)
except KeyboardInterrupt:
    print("\nStopped.")
except Exception as e:
    print(f"\nFATAL: {e}")
    traceback.print_exc()
    sys.exit(1)
