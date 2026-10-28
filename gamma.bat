@echo off
set GAMMA_SCALP_MODE=on
set GAMMA_SCALP_ATR_SEED=on
call .venv\Scripts\activate.bat
python manage.py runserver 0.0.0.0:8080 --noreload --nostatic