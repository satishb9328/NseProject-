@echo off
REM ============================================================================
REM  Algorithmic Trading System - One-Click Startup (Windows)
REM
REM  PIPELINE ORDER (this is the fix):
REM    STEP 0  Nifty200_data_fetcher.py   -> INGEST  (blocking, gated)
REM    STEP 1  manage.py runserver        -> DASHBOARD
REM    STEP 2  upstox_live_feed.py        -> LIVE FEED
REM    STEP 3  nifty_advanced_strategy.py -> SCANNER (only if STEP 0 succeeded)
REM
REM  Previously STEP 0 was missing entirely, so the scanner consumed a parquet
REM  corpus that nothing in the system was responsible for refreshing.
REM
REM  Fetcher exit codes: 0=OK 1=init failed 2=no token 3=token expired 4=all failed
REM ============================================================================

setlocal

REM ---- 1) Base directory (folder where this script lives) --------------------
cd /d "%~dp0"
set "BASE_DIR=%CD%"

if errorlevel 1 (
    echo [ERROR] Could not change directory to "%BASE_DIR%".
    pause
    exit /b 1
)

REM ---- 2) Create the logs folder ---------------------------------------------
if not exist "logs" mkdir "logs"

REM ---- 3) Force unbuffered Python I/O + UTF-8 log encoding -------------------
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

REM ---- 3b) Shared data contract for fetcher AND scanner ----------------------
REM Both scripts honour UPSTOX_HISTORY_DIR, so they can never drift apart.
if not defined UPSTOX_HISTORY_DIR set "UPSTOX_HISTORY_DIR=D:\Api data\ML_HISTO_DATA\Upstox_History"

set "VENV_PY=%BASE_DIR%\venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] Virtualenv python not found at "%VENV_PY%".
    pause
    exit /b 1
)

echo [INFO] Base directory : %BASE_DIR%
echo [INFO] Logs folder    : %BASE_DIR%\logs
echo [INFO] History dir    : %UPSTOX_HISTORY_DIR%
echo.

REM ============================================================================
REM  STEP 0 - DATA INGESTION  (BLOCKING - must finish before we scan)
REM ============================================================================
echo ============================================================================
echo [STEP 0/3] Ingesting NIFTY 200 historical data from Upstox...
echo            BLOCKING - the scanner will not start until this completes.
echo            Progress log: logs\data_fetcher.log
echo ============================================================================
echo.

REM No pipe here: piping would clobber ERRORLEVEL. Redirect, then display.
"%VENV_PY%" -u "Nse_Project\Swing_stocks\Nifty200_data_fetcher.py" > "logs\data_fetcher.log" 2>&1
set "FETCH_RC=%ERRORLEVEL%"

type "logs\data_fetcher.log"
echo.
echo [INFO] Fetcher exit code: %FETCH_RC%
echo.

if "%FETCH_RC%"=="2" goto err_token
if "%FETCH_RC%"=="3" goto err_token
if "%FETCH_RC%"=="1" goto err_init
if "%FETCH_RC%"=="4" goto warn_allfailed
if not "%FETCH_RC%"=="0" goto err_unknown

echo [OK] STEP 0 complete - data ingestion succeeded.
echo.
goto launch

REM ============================================================================
REM  ERROR HANDLERS
REM ============================================================================

:err_token
echo ****************************************************************************
echo [FATAL] Upstox token is MISSING or EXPIRED (exit code %FETCH_RC%).
echo.
echo   Generate a fresh token, then re-run this launcher:
echo     "%VENV_PY%" Nse_Project\Brokers\upstox\get_upstox_token_manual.py
echo.
echo   The scanner will NOT be started, because with a stale data corpus it
echo   would skip every symbol and overwrite the dashboard with 0 results.
echo ****************************************************************************
echo.
pause
exit /b %FETCH_RC%

:err_init
echo ****************************************************************************
echo [FATAL] Fetcher initialisation failed (exit code 1).
echo   Could not reach the NSE symbol list / Upstox instrument master, or
echo   "%UPSTOX_HISTORY_DIR%" is not writable.
echo ****************************************************************************
echo.
pause
exit /b 1

:err_unknown
echo ****************************************************************************
echo [FATAL] Fetcher crashed unexpectedly (exit code %FETCH_RC%).
echo   See logs\data_fetcher.log for the traceback.
echo ****************************************************************************
echo.
pause
exit /b %FETCH_RC%

:warn_allfailed
echo ****************************************************************************
echo [WARN] Fetcher completed but updated ZERO symbols (exit code 4).
echo   The corpus may still be stale. Continuing anyway - the scanner will
echo   report 'stale_data' per symbol and write an empty result set.
echo ****************************************************************************
echo.
timeout /t 5 >nul

REM ============================================================================
REM  LAUNCH BACKGROUND PROCESSES
REM ============================================================================
:launch
echo [INFO] Launching background processes in minimized windows...
echo.

REM ---- STEP 1) Django development server (port 8080) -------------------------
start "Django Server" /min cmd /k "call venv\Scripts\activate.bat && python -u manage.py runserver 8080 > logs\django_server.log 2>&1"

REM ---- STEP 2) Upstox live feed ----------------------------------------------
start "Upstox Live Feed" /min cmd /k "call venv\Scripts\activate.bat && python -u Nse_Project\Live_dashboard\upstox_live_feed.py > logs\upstox_feed.log 2>&1"

REM ---- STEP 3) Nifty advanced strategy scanner (data now guaranteed fresh) ---
start "Advanced Strategy" /min cmd /k "call venv\Scripts\activate.bat && python -u Nse_Project\Swing_stocks\nifty_advanced_strategy.py > logs\advanced_strategy.log 2>&1"

REM ---- Wait and Auto-Open Browser -------------------------------------------
echo [INFO] Waiting 5 seconds for the Django server to start...
timeout /t 5 >nul

echo [INFO] Opening Main Dashboard...
start http://127.0.0.1:8080/live-signals/

echo.
echo [DONE] All systems are GO! Logs are streaming to the "logs" folder.
echo        Close any process by closing its minimized window.
echo.
echo        Ingestion log  : logs\data_fetcher.log
echo        Scanner log    : logs\advanced_strategy.log
echo        Scan status    : Nse_Project\Swing_stocks\scan_status.json
echo.

endlocal
exit /b 0
