@echo off
setlocal

set "PROJECT_DIR=C:\Users\ADMIN\Desktop\MyProject2"
set "LOG_FILE=%~dp0daily_eod_log.txt"

echo.>>"%LOG_FILE%"
echo [%date% %time%] Starting daily EOD update.>>"%LOG_FILE%"

cd /d "%PROJECT_DIR%" >>"%LOG_FILE%" 2>&1 || goto :failed
call "C:\Users\ADMIN\Desktop\MyProject2\venv\Scripts\activate" >>"%LOG_FILE%" 2>&1 || goto :failed
python .\Nse_Project\dw_upstox_data.py >>"%LOG_FILE%" 2>&1 || goto :failed

cd /d "%PROJECT_DIR%\Nse_Project\funda_fetch" >>"%LOG_FILE%" 2>&1 || goto :failed
python funda_data.py >>"%LOG_FILE%" 2>&1 || goto :failed

cd /d "%PROJECT_DIR%" >>"%LOG_FILE%" 2>&1 || goto :failed
python .\Nse_Project\funda_fetch\trend_ready.py >>"%LOG_FILE%" 2>&1 || goto :failed
python .\manage.py build_daily_trend_setups >>"%LOG_FILE%" 2>&1 || goto :failed

echo [%date% %time%] Daily EOD update completed successfully.>>"%LOG_FILE%"
exit /b 0

:failed
echo [%date% %time%] ERROR: Daily EOD update stopped because a command failed.>>"%LOG_FILE%"
pause
exit /b 1
