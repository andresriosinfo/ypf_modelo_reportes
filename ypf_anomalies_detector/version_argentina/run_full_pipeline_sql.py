"""
Script para ejecutar el pipeline completo con SQL Server
"""

import sys
import traceback
from pathlib import Path

# Verificar dependencias
print("="*80)
print("VERIFICANDO DEPENDENCIAS")
print("="*80)

try:
    import pandas as pd
    print("[OK] pandas instalado")
except ImportError:
    print("[ERROR] pandas no está instalado. Ejecuta: pip install pandas")
    sys.exit(1)

try:
    import pyodbc
    print("[OK] pyodbc instalado")
except ImportError:
    print("[ERROR] pyodbc no está instalado. Ejecuta: pip install pyodbc")
    sys.exit(1)

try:
    from prophet import Prophet
    print("[OK] prophet instalado")
except ImportError:
    print("[ERROR] prophet no está instalado. Ejecuta: pip install prophet")
    sys.exit(1)

# Agregar path
sys.path.append(str(Path(__file__).parent))

print("\n" + "="*80)
print("PASO 1: ESCRIBIR DATOS DE ENTRENAMIENTO A SQL")
print("="*80)

try:
    from write_training_data_to_sql import main as write_main
    write_main()
except Exception as e:
    print(f"\n[ERROR] Error en paso 1: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("PASO 2: ENTRENAR MODELOS DESDE SQL")
print("="*80)

try:
    from train_from_sql import main as train_main
    train_main()
except Exception as e:
    print(f"\n[ERROR] Error en paso 2: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("PASO 3: DETECTAR ANOMALÍAS Y ESCRIBIR RESULTADOS")
print("="*80)

try:
    from detect_from_sql import main as detect_main
    detect_main()
except Exception as e:
    print(f"\n[ERROR] Error en paso 3: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("PIPELINE COMPLETO FINALIZADO EXITOSAMENTE")
print("="*80)
print("\nVerifica las tablas en SQL Server:")
print("  - dbo.datos_proceso (datos de entrenamiento)")
print("  - dbo.anomalies_detector (resultados de detección)")


