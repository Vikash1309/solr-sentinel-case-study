@echo off
REM Solr Sentinel - Windows launcher. Double-click, or run from a terminal in this folder.
cd /d "%~dp0"
echo Checking Python...
python --version || (echo Python 3.10+ is not installed or not on PATH. Install it from https://www.python.org/downloads/ and tick "Add python.exe to PATH". & pause & exit /b 1)
echo Installing requirements (first run only takes a minute)...
python -m pip install -r requirements.txt || (echo pip install failed - see the message above. & pause & exit /b 1)
echo.
echo Starting Solr Sentinel. Keep this window open.
echo   Dashboard: http://localhost:8501
echo   API docs:  http://127.0.0.1:8000/docs
python run.py demo
pause
