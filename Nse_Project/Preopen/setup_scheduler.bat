@echo off
REM Batch file to easily setup the scheduled task
REM This will automatically request Administrator privileges

echo ============================================================
echo NSE Preopen Data Fetcher - Task Scheduler Setup
echo ============================================================
echo.
echo This will create a scheduled task that runs at 9:08 AM
echo every weekday (automatically skipping weekends and holidays)
echo.

REM Check for admin rights
net session >nul 2>&1
if %errorLevel% == 0 (
    echo Running with Administrator privileges...
    echo.
    PowerShell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_scheduler.ps1"
) else (
    echo Requesting Administrator privileges...
    echo.
    PowerShell.exe -NoProfile -ExecutionPolicy Bypass -Command "Start-Process PowerShell -ArgumentList '-NoProfile -ExecutionPolicy Bypass -File \"%~dp0setup_scheduler.ps1\"' -Verb RunAs"
    timeout /t 3 >nul
    exit /b
)

echo.
echo ============================================================
echo Setup complete!
echo ============================================================
echo.
pause

