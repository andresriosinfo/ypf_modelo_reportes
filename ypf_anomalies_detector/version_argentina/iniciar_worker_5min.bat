@echo off
echo ========================================
echo WORKER DE DETECCION DE ANOMALIAS
echo Intervalo: 5 minutos
echo ========================================
echo.
cd /d "%~dp0"
python worker_procesamiento.py --interval 5
pause

