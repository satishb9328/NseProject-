@echo off
setlocal

set "PROJECT_ROOT=C:\Users\ADMIN\Desktop\MyProject2"
set "PREOPEN_DIR=%PROJECT_ROOT%\Nse_Project\Preopen"
set "LOG_FILE=%~dp0preopen_log.txt"

echo.>>"%LOG_FILE%"
echo [%date% %time%] Starting NSE pre-open update.>>"%LOG_FILE%"

cd /d "%PREOPEN_DIR%" >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

:: ఇక్కడ మీ వర్చువల్ ఎన్విరాన్‌మెంట్ యాక్టివేట్ అవుతుంది (venv పేరు కరెక్ట్ గా ఉందో లేదో చూసుకోండి)
call "%PROJECT_ROOT%\venv\Scripts\activate"

:: ఆ తర్వాతే పైథాన్ రన్ అవుతుంది
python .\fetch_and_import_preopen.py --verbose >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

echo [%date% %time%] NSE pre-open update completed successfully.>>"%LOG_FILE%"
exit /b 0

:failed
echo [%date% %time%] ERROR: NSE pre-open update failed. See output above.>>"%LOG_FILE%"
:: విండో క్లోజ్ అవ్వకుండా ఇక్కడ ఆగుతుంది
pause
exit /b 1