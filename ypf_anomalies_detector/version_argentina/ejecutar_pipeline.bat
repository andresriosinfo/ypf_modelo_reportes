@echo off
echo ========================================
echo EJECUTANDO PIPELINE COMPLETO CON SQL
echo ========================================
echo.

cd /d %~dp0

echo Paso 1: Escribir datos de entrenamiento a SQL...
python write_training_data_to_sql.py
if errorlevel 1 (
    echo ERROR en paso 1
    pause
    exit /b 1
)

echo.
echo Paso 2: Entrenar modelos desde SQL...
python train_from_sql.py
if errorlevel 1 (
    echo ERROR en paso 2
    pause
    exit /b 1
)

echo.
echo Paso 3: Detectar anomalias y escribir resultados...
python detect_from_sql.py
if errorlevel 1 (
    echo ERROR en paso 3
    pause
    exit /b 1
)

echo.
echo ========================================
echo PIPELINE COMPLETADO EXITOSAMENTE
echo ========================================
pause


