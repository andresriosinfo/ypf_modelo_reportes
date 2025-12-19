@echo off
cd /d "%~dp0"
python procesar_tiempo_real.py --once --lookback 1
pause


