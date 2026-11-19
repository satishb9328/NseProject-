@echo off
setlocal

rem ============================================================================
rem  Signal Validation System - Logger launcher (long-lived foreground process)
rem
rem  Runs the READ-ONLY live logger. It subscribes to gamma:live_options_updated
rem  and writes raw + enriched JSONL under D:/Signal_Validation_Data/ (override
rem  the base dir with the SIGVAL_DATA_DIR environment variable). It never
rem  mutates the live platform. See register_scheduled_tasks.ps1 for a scheduled
rem  startup at 09:05 IST.
rem
rem  One-time dependency install (uses .venv, which carries redis + pyyaml):
rem      .venv\Scripts\python.exe -m pip install -r Nse_Project\Signal_Validation\requirements.txt
rem
rem  Stop it with Ctrl+C (manual) or Task Scheduler end. It exits cleanly on
rem  SIGINT/SIGTERM/SIGBREAK and drains/flushes before shutting down.
rem ============================================================================

set "PROJECT_DIR=C:\Users\ADMIN\Desktop\MyProject2"
set "LOG_FILE=%~dp0run_logger_log.txt"
set PYTHONIOENCODING=utf-8

echo.>>"%LOG_FILE%"
echo [%date% %time%] Starting Signal Validation logger.>>"%LOG_FILE%"

cd /d "%PROJECT_DIR%" >>"%LOG_FILE%" 2>&1 || goto :failed
call "C:\Users\ADMIN\Desktop\MyProject2\.venv\Scripts\activate.bat" >>"%LOG_FILE%" 2>&1 || goto :failed

python ".\Nse_Project\Signal_Validation\run_logger.py" >>"%LOG_FILE%" 2>&1
set "EXITCODE=%ERRORLEVEL%"

echo [%date% %time%] Logger stopped (exit code %EXITCODE%).>>"%LOG_FILE%"
exit /b %EXITCODE%

:failed
echo [%date% %time%] ERROR: logger failed to start. See log for details.>>"%LOG_FILE%"
exit /b 1
