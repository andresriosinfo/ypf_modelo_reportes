"""
Script de prueba y ejecución del pipeline
"""

import sys
import os

# Forzar salida sin buffer
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
os.environ['PYTHONUNBUFFERED'] = '1'

print("="*80)
print("VERIFICANDO DEPENDENCIAS")
print("="*80)
sys.stdout.flush()

# Verificar pyodbc
try:
    import pyodbc
    print("[OK] pyodbc instalado")
except ImportError:
    print("[ERROR] pyodbc NO está instalado")
    print("Instala con: pip install pyodbc")
    sys.exit(1)

# Verificar pandas
try:
    import pandas as pd
    print("[OK] pandas instalado")
except ImportError:
    print("[ERROR] pandas NO está instalado")
    sys.exit(1)

# Verificar prophet
try:
    from prophet import Prophet
    print("[OK] prophet instalado")
except ImportError:
    print("[ERROR] prophet NO está instalado")
    sys.exit(1)

print("\n" + "="*80)
print("PRUEBA DE CONEXIÓN SQL")
print("="*80)
sys.stdout.flush()

try:
    from sql_utils import SQLConnection
    
    SQL_CONFIG = {
        'server': '10.147.17.105',
        'database': 'ypf_ai_pilot',
        'username': 'sa',
        'password': 'Gnomes1*.',
        'port': 14333
    }
    
    sql_conn = SQLConnection(**SQL_CONFIG)
    if sql_conn.connect():
        print("[OK] Conexión a SQL Server exitosa")
        sql_conn.disconnect()
    else:
        print("[ERROR] No se pudo conectar a SQL Server")
        sys.exit(1)
        
except Exception as e:
    print(f"[ERROR] Error en conexión: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("EJECUTANDO PIPELINE")
print("="*80)
sys.stdout.flush()

# Ejecutar scripts
scripts = [
    'write_training_data_to_sql.py',
    'train_from_sql.py', 
    'detect_from_sql.py'
]

for i, script in enumerate(scripts, 1):
    print(f"\n[{i}/3] Ejecutando {script}...")
    sys.stdout.flush()
    
    try:
        # Importar y ejecutar el main del script
        script_path = script.replace('.py', '')
        module = __import__(script_path)
        if hasattr(module, 'main'):
            module.main()
            print(f"[OK] {script} completado")
        else:
            print(f"[ERROR] {script} no tiene función main()")
    except Exception as e:
        print(f"[ERROR] Error ejecutando {script}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    sys.stdout.flush()

print("\n" + "="*80)
print("PIPELINE COMPLETADO EXITOSAMENTE")
print("="*80)
print("\nVerifica las tablas en SQL Server:")
print("  - dbo.datos_proceso")
print("  - dbo.anomalies_detector")


