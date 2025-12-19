@echo off
echo ========================================
echo WORKER DE REENTRENAMIENTO AUTOMATICO
echo Reentrena modelos todos los dias a las 2:00 AM
echo ========================================
echo.
cd /d "%~dp0"
python worker_reentrenamiento.py
pause

