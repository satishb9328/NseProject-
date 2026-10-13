@echo off
setlocal

set "PROJECT_DIR=C:\Users\ADMIN\Desktop\MyProject2"
set "LOG_FILE=%~dp0live_sniper_log.txt"

:: ప్రస్తుత సమయం గంటను తీసుకోవడం (24-hour format)
set "hour=%time:~0,2%"
set "hour=%hour: =0%"

:: ఉదయం 9 గంటల కంటే ముందు అయితే రన్ అవ్వద్దు
if %hour% LSS 9 (
    echo [%date% %time%] Market not opened yet. Auto-stopping. >>"%LOG_FILE%"
    exit /b 0
)

:: సాయంత్రం 4 (16:00) లేదా ఆ తర్వాత అయితే దానంతట అదే ఆగిపోవాలి
if %hour% GEQ 16 (
    echo [%date% %time%] Market is closed. Auto-stopping the live sniper. >>"%LOG_FILE%"
    exit /b 0
)

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