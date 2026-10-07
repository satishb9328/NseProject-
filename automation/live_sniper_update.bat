@echo off
setlocal

set "PROJECT_DIR=C:\Users\ADMIN\Desktop\MyProject2"
set "LOG_FILE=%~dp0live_sniper_log.txt"

echo.>>"%LOG_FILE%"
echo [%date% %time%] Starting live sniper update.>>"%LOG_FILE%"

cd /d "%PROJECT_DIR%" >>"%LOG_FILE%" 2>&1 || goto :failed
call "C:\Users\ADMIN\Desktop\MyProject2\venv\Scripts\activate" >>"%LOG_FILE%" 2>&1 || goto :failed
python .\manage.py update_live_sniper >>"%LOG_FILE%" 2>&1 || goto :failed

echo [%date% %time%] Live sniper update completed successfully.>>"%LOG_FILE%"
exit /b 0

:failed
echo [%date% %time%] ERROR: Live sniper update stopped because a command failed.>>"%LOG_FILE%"
pause
exit /b 1
