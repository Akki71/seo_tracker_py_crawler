#!/bin/bash
# ────────────────────────────────────────────────────────────────
# run_local.sh — Run the SEO Crawler API locally (Mac/Linux/WSL)
# ────────────────────────────────────────────────────────────────
set -e

echo ""
echo "=============================================="
echo " AquilTechLabs SEO Crawler — Local Setup"
echo "=============================================="

# ── Step 1: Check Python ──────────────────────────────────────
echo ""
echo "[1/6] Checking Python..."
if command -v python3 &>/dev/null; then
    PY=$(python3 --version)
    echo "      Found: $PY"
    PYTHON=python3
elif command -v python &>/dev/null; then
    PY=$(python --version)
    echo "      Found: $PY"
    PYTHON=python
else
    echo "      ERROR: Python not found. Install Python 3.10+ from https://python.org"
    exit 1
fi

# ── Step 2: Create virtualenv ──────────────────────────────────
echo ""
echo "[2/6] Setting up virtual environment..."
if [ ! -d "venv" ]; then
    $PYTHON -m venv venv
    echo "      Created venv/"
else
    echo "      Using existing venv/"
fi

# Activate
source venv/bin/activate 2>/dev/null || source venv/Scripts/activate 2>/dev/null || {
    echo "      ERROR: Could not activate venv. Try manually: source venv/bin/activate"
    exit 1
}
echo "      Activated ✓"

# ── Step 3: Install packages ───────────────────────────────────
echo ""
echo "[3/6] Installing packages..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
echo "      Packages installed ✓"

# ── Step 4: Check .env file ────────────────────────────────────
echo ""
echo "[4/6] Checking .env file..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo ""
    echo "      ┌─────────────────────────────────────────────────────┐"
    echo "      │  .env file created from .env.example               │"
    echo "      │  PLEASE EDIT .env with your actual DB credentials  │"
    echo "      │  before continuing!                                  │"
    echo "      └─────────────────────────────────────────────────────┘"
    echo ""
    read -p "      Press ENTER after editing .env to continue..."
else
    echo "      .env found ✓"
fi

# ── Step 5: Load .env ──────────────────────────────────────────
echo ""
echo "[5/6] Loading environment from .env..."
export $(grep -v '^#' .env | grep -v '^$' | xargs)
echo "      DB_HOST=${DB_HOST:-NOT SET}"
echo "      DB_NAME=${DB_NAME:-NOT SET}"
echo "      OpenAI:    $([ -n "$OPENAI_API_KEY" ] && echo SET || echo NOT SET)"
echo "      Anthropic: $([ -n "$ANTHROPIC_API_KEY" ] && echo SET || echo NOT SET)"

# ── Step 6: Start server ───────────────────────────────────────
echo ""
echo "[6/6] Starting server..."
echo ""
echo "      ✓ API will be available at:  http://localhost:8000"
echo "      ✓ Interactive docs at:       http://localhost:8000/docs"
echo "      ✓ Health check:              http://localhost:8000/health"
echo ""
echo "      To test: curl http://localhost:8000/health"
echo ""
echo "      Press Ctrl+C to stop."
echo "=============================================="
echo ""

$PYTHON startup.py
