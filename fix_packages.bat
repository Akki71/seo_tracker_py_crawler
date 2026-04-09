@echo off
echo ============================================
echo  Fix AI Package Versions
echo  openai 1.58.1 + anthropic 0.40.0 + httpx 0.27.2
echo ============================================
echo.
pip install openai==1.58.1 anthropic==0.40.0 httpx==0.27.2 --upgrade -q
if %errorlevel% equ 0 (
    echo.
    echo Done! Now restart: python startup.py
) else (
    echo.
    echo ERROR - Try running as Administrator
)
echo ============================================
pause
