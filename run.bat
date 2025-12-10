@echo off
echo ========================================
echo   Attendance Manager - Starting Server
echo ========================================
echo.

cd /d "%~dp0backend"

echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://python.org
    pause
    exit /b 1
)

echo Installing dependencies...
pip install -r requirements.txt

echo.
echo Starting server...
echo.
echo ========================================
echo   Server running at: http://localhost:5000
echo   Press Ctrl+C to stop
echo ========================================
echo.

python app.py
