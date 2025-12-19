@echo off
echo ========================================
echo WORKER DE DETECCION DE ANOMALIAS
echo ========================================
echo.
cd /d "%~dp0"
python worker_procesamiento.py
pause

