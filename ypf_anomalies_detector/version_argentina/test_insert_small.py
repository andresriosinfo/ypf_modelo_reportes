"""
Script de prueba para insertar solo un pequeño chunk y verificar que funciona
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent))

from sql_utils import SQLConnection

SQL_CONFIG = {
    'server': '10.147.17.105',
    'database': 'ypf_ai_pilot',
    'username': 'sa',
    'password': 'Gnomes1*.',
    'port': 14333
}

print("="*80)
print("PRUEBA DE INSERCIÓN PEQUEÑA")
print("="*80)

# Leer solo las primeras 100 filas
data_file = Path("output/datos_proceso_N101_cleaned.csv")
df = pd.read_csv(data_file, parse_dates=['DATETIME'], nrows=100)
print(f"\n[INFO] Leyendo primeras 100 filas de {data_file.name}")

# Transformar a formato largo
from write_training_data_to_sql import transform_data_to_long_format
df_long = transform_data_to_long_format(df, datetime_col='DATETIME', source_file='test')
print(f"[INFO] Filas en formato largo: {len(df_long)}")

# Conectar y escribir
sql_conn = SQLConnection(**SQL_CONFIG)
if sql_conn.connect():
    print("\n[INFO] Insertando datos de prueba...")
    success = sql_conn.write_dataframe(
        df_long,
        table_name='datos_proceso',
        if_exists='append'
    )
    
    if success:
        print("\n[OK] Prueba exitosa! El método de inserción funciona.")
        print("     Ahora puedes ejecutar el script completo.")
    else:
        print("\n[ERROR] La inserción falló")
    
    sql_conn.disconnect()
else:
    print("\n[ERROR] No se pudo conectar a SQL Server")


