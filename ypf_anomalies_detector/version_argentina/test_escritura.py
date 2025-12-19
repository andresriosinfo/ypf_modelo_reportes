"""Script para probar la escritura en otms_analytics"""
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

sys.path.append(str(Path(__file__).parent))
from sql_utils import SQLConnection
from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector

SQL_CONFIG_INPUT = {
    'server': '10.147.17.241',
    'database': 'otms_main',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

SQL_CONFIG_OUTPUT = {
    'server': '10.147.17.241',
    'database': 'otms_analytics',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

print("="*80)
print("PRUEBA DE ESCRITURA EN otms_analytics")
print("="*80)

# Conectar a base de datos de entrada
print("\n[1] Conectando a otms_main...")
conn_input = SQLConnection(**SQL_CONFIG_INPUT)
if not conn_input.connect():
    print("✗ Error conectando a otms_main")
    sys.exit(1)

# Leer algunos datos de prueba
print("[2] Leyendo datos de prueba...")
query = """
    SELECT TOP 100 datetime, variable_name, value 
    FROM dbo.ypf_process_data 
    ORDER BY datetime DESC
"""
df_long = conn_input.execute_query(query)
print(f"✓ Datos leídos: {len(df_long)} filas")

# Convertir a formato ancho
df_wide = df_long.pivot_table(
    index='datetime',
    columns='variable_name',
    values='value',
    aggfunc='first'
)
df_wide = df_wide.reset_index()
df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
print(f"✓ Datos convertidos: {df_wide.shape[0]} filas x {df_wide.shape[1]} columnas")

conn_input.disconnect()

# Cargar modelos
print("\n[3] Cargando modelos...")
models_dir = Path("pipeline/models/prophet")
detector = ProphetAnomalyDetector()
try:
    detector.load_models(str(models_dir))
    print(f"✓ {len(detector.models)} modelos cargados")
except Exception as e:
    print(f"✗ Error cargando modelos: {e}")
    sys.exit(1)

# Detectar anomalías en una variable de prueba
print("\n[4] Detectando anomalías (prueba con una variable)...")
available_vars = [v for v in detector.models.keys() if v in df_wide.columns]
if not available_vars:
    print("✗ No hay variables comunes")
    sys.exit(1)

test_var = available_vars[0]
print(f"  Probando con variable: {test_var}")

try:
    results = detector.detect_anomalies(
        detector.models[test_var],
        df_wide,
        test_var,
        'DATETIME'
    )
    print(f"✓ Anomalías detectadas: {results['is_anomaly'].sum()} de {len(results)} puntos")
    
    # Preparar datos para escribir
    if 'source_file' not in results.columns:
        results['source_file'] = 'test'
    
    bool_cols = ['outside_interval', 'high_residual', 'is_anomaly']
    for col in bool_cols:
        if col in results.columns:
            results[col] = results[col].astype(int)
    
    columns_to_write = [
        'ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'residual',
        'outside_interval', 'high_residual', 'is_anomaly', 'anomaly_score',
        'variable', 'prediction_error_pct', 'source_file'
    ]
    available_cols = [col for col in columns_to_write if col in results.columns]
    results_to_write = results[available_cols].copy()
    
    # Conectar a base de datos de salida
    print("\n[5] Escribiendo en otms_analytics...")
    conn_output = SQLConnection(**SQL_CONFIG_OUTPUT)
    if not conn_output.connect():
        print("✗ Error conectando a otms_analytics")
        sys.exit(1)
    
    # Crear tabla si no existe
    from detect_from_sql import create_anomalies_table
    create_anomalies_table(conn_output)
    
    # Escribir datos
    success = conn_output.write_dataframe(
        results_to_write,
        table_name='ypf_anomaly_detector',
        if_exists='append'
    )
    
    if success:
        print(f"✓ {len(results_to_write)} filas escritas exitosamente en otms_analytics.dbo.ypf_anomaly_detector")
    else:
        print("✗ Error escribiendo datos")
    
    conn_output.disconnect()
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("PRUEBA COMPLETADA EXITOSAMENTE")
print("="*80)

