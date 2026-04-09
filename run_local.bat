@echo off
REM ──────────────────────────────────────────────────────────────
REM run_local.bat — Run the SEO Crawler API locally on Windows
REM ──────────────────────────────────────────────────────────────

echo.
echo ==============================================
echo  AquilTechLabs SEO Crawler - Windows Setup
echo ==============================================

REM Step 1: Check Python
echo.
echo [1/6] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo       ERROR: Python not found. Install from https://python.org
    pause
    exit /b 1
)
python --version
echo       Python found OK

REM Step 2: Virtual environment
echo.
echo [2/6] Setting up virtual environment...
if not exist "venv\" (
    python -m venv venv
    echo       Created venv\
) else (
    echo       Using existing venv\
)
call venv\Scripts\activate.bat
echo       Activated OK

REM Step 3: Install packages
echo.
echo [3/6] Installing packages (this may take a few minutes)...
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
echo       Packages installed OK

REM Step 4: Check .env
echo.
echo [4/6] Checking .env file...
if not exist ".env" (
    copy .env.example .env
    echo.
    echo       .env file created!
    echo       PLEASE EDIT .env with your PostgreSQL credentials
    echo       Then press any key to continue...
    pause
) else (
    echo       .env found OK
)

REM Step 5: Create output dirs
echo.
echo [5/6] Creating output directories...
if not exist "output\" mkdir output
if not exist "screenshots\" mkdir screenshots
if not exist "logs\" mkdir logs
echo       Directories ready

REM Step 6: Start
echo.
echo [6/6] Starting server...
echo.
echo       API:    http://localhost:8000
echo       Docs:   http://localhost:8000/docs
echo       Health: http://localhost:8000/health
echo.
echo       Press Ctrl+C to stop.
echo ==============================================
echo.

REM Load .env vars
for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" (
        set "%%A=%%B"
    )
)

python startup.py
pause
