@echo off
rem Morning weather ingestion. Windows Task Scheduler runs this at 06:00 (see setup_scheduler.ps1).
rem You can also double-click it, or run:  run_daily.bat --force
setlocal
cd /d "%~dp0"

set "PY=python"
where python >nul 2>nul || set "PY=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"

rem --trigger is set to "scheduled" only when Task Scheduler passes it; double-click = manual
"%PY%" -X utf8 scheduler.py %*
exit /b %ERRORLEVEL%
