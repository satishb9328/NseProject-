@echo off
setlocal

rem ============================================================================
rem  Signal Validation System - Evaluator launcher (one-shot batch job)
rem
rem  Runs the offline evaluator once: reads D:/Signal_Validation_Data and writes
rem  the validation report to D:/Signal_Validation_Data/reports. See
rem  register_scheduled_tasks.ps1 for a scheduled run at 15:50 IST.
rem
rem  NOTE: run_evaluator.py is Phase 4. Until that module exists this launcher
rem  exits with a clear message; no code change is needed once it is built.
rem
rem  One-time dependency install (uses .venv, which carries numpy + pandas):
rem      .venv\Scripts\python.exe -m pip install -r Nse_Project\Signal_Validation\requirements.txt
rem ============================================================================

set "PROJECT_DIR=C:\Users\ADMIN\Desktop\MyProject2"
set "LOG_FILE=%~dp0run_evaluator_log.txt"
set PYTHONIOENCODING=utf-8

echo.>>"%LOG_FILE%"
echo [%date% %time%] Starting Signal Validation evaluator.>>"%LOG_FILE%"

cd /d "%PROJECT_DIR%" >>"%LOG_FILE%" 2>&1 || goto :failed

if not exist ".\Nse_Project\Signal_Validation\run_evaluator.py" (
    echo [%date% %time%] ERROR: run_evaluator.py not found - Phase 4 (Evaluator) is not implemented yet.>>"%LOG_FILE%"
    exit /b 1
)

call "C:\Users\ADMIN\Desktop\MyProject2\.venv\Scripts\activate.bat" >>"%LOG_FILE%" 2>&1 || goto :failed

python ".\Nse_Project\Signal_Validation\run_evaluator.py" >>"%LOG_FILE%" 2>&1
set "EXITCODE=%ERRORLEVEL%"

echo [%date% %time%] Evaluator finished (exit code %EXITCODE%).>>"%LOG_FILE%"
exit /b %EXITCODE%

:failed
echo [%date% %time%] ERROR: evaluator failed. See log for details.>>"%LOG_FILE%"
exit /b 1
