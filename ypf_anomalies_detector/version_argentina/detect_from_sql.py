"""
Script para detectar anomalías leyendo desde SQL y escribiendo resultados a SQL
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Agregar el directorio padre al path
sys.path.append(str(Path(__file__).parent))

from sql_utils import SQLConnection

from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector

# Configuración de conexión SQL - Base de datos de entrada
SQL_CONFIG_INPUT = {
    'server': '10.147.17.241',
    'database': 'otms_main',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

# Configuración de conexión SQL - Base de datos de salida
SQL_CONFIG_OUTPUT = {
    'server': '10.147.17.241',
    'database': 'otms_analytics',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}


def create_anomalies_table(sql_conn: SQLConnection):
    """Crea la tabla ypf_anomaly_detector si no existe"""
    print("\n[INFO] Verificando/creando tabla ypf_anomaly_detector...")
    
    columns = {
        'id': 'BIGINT IDENTITY(1,1) PRIMARY KEY',
        'ds': 'DATETIME NOT NULL',
        'y': 'DECIMAL(18,6)',
        'yhat': 'DECIMAL(18,6)',
        'yhat_lower': 'DECIMAL(18,6)',
        'yhat_upper': 'DECIMAL(18,6)',
        'residual': 'DECIMAL(18,6)',
        'outside_interval': 'BIT',
        'high_residual': 'BIT',
        'is_anomaly': 'BIT',
        'anomaly_score': 'DECIMAL(5,2)',
        'variable': 'VARCHAR(100) NOT NULL',
        'prediction_error_pct': 'DECIMAL(5,2)',
        'source_file': 'VARCHAR(255)',
        'processed_at': 'DATETIME DEFAULT GETDATE()'
    }
    
    sql_conn.create_table_if_not_exists('ypf_anomaly_detector', columns=columns)
    
    # Crear índices si no existen
    indexes = [
        ("idx_ds", "CREATE INDEX idx_ds ON dbo.ypf_anomaly_detector(ds)"),
        ("idx_variable", "CREATE INDEX idx_variable ON dbo.ypf_anomaly_detector(variable)"),
        ("idx_is_anomaly", "CREATE INDEX idx_is_anomaly ON dbo.ypf_anomaly_detector(is_anomaly)"),
        ("idx_anomaly_score", "CREATE INDEX idx_anomaly_score ON dbo.ypf_anomaly_detector(anomaly_score)"),
        ("idx_processed_at", "CREATE INDEX idx_processed_at ON dbo.ypf_anomaly_detector(processed_at)")
    ]
    
    for idx_name, idx_query in indexes:
        try:
            # Verificar si el índice ya existe
            check_query = f"""
                SELECT COUNT(*) 
                FROM sys.indexes 
                WHERE name = '{idx_name}' AND object_id = OBJECT_ID('dbo.ypf_anomaly_detector')
            """
            cursor = sql_conn._conn.cursor()
            cursor.execute(check_query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            
            if not exists:
                sql_conn.execute_non_query(idx_query)
        except Exception as e:
            # Ignorar errores de índices
            pass


def read_data_from_sql(sql_conn: SQLConnection, start_date: str = None, 
                       end_date: str = None) -> pd.DataFrame:
    """
    Lee datos desde SQL Server y los convierte a formato ancho
    """
    print("\n[INFO] Leyendo datos desde SQL Server...")
    
    query = "SELECT datetime, variable_name, value FROM dbo.ypf_process_data"
    
    conditions = []
    if start_date:
        conditions.append(f"datetime >= '{start_date}'")
    if end_date:
        conditions.append(f"datetime <= '{end_date}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY datetime, variable_name"
    
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        print("[ERROR] No se encontraron datos en SQL")
        return None
    
    print(f"  Filas leídas: {len(df_long):,}")
    
    # Convertir a formato ancho
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'
    )
    
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    print(f"  Dimensiones: {df_wide.shape[0]} filas x {df_wide.shape[1]} columnas")
    
    return df_wide


def main():
    print("="*80)
    print("DETECCIÓN DE ANOMALÍAS DESDE SQL SERVER")
    print("="*80)
    
    # Configuración
    models_dir = Path("pipeline/models/prophet")
    datetime_col = 'DATETIME'
    
    # Verificar que existan modelos
    if not models_dir.exists() or not list(models_dir.glob("prophet_model_*.pkl")):
        print(f"\n[ERROR] No se encontraron modelos entrenados en {models_dir}")
        print("   Entrena los modelos primero:")
        print("   python train_from_sql.py")
        return
    
    # Conectar a SQL - Base de datos de entrada (para leer datos)
    sql_conn_input = SQLConnection(**SQL_CONFIG_INPUT)
    if not sql_conn_input.connect():
        return
    
    # Conectar a SQL - Base de datos de salida (para escribir resultados)
    sql_conn_output = SQLConnection(**SQL_CONFIG_OUTPUT)
    if not sql_conn_output.connect():
        sql_conn_input.disconnect()
        return
    
    try:
        # Crear tabla de resultados si no existe (en base de datos de salida)
        create_anomalies_table(sql_conn_output)
        
        # Cargar detector y modelos
        print(f"\n[INFO] Cargando modelos desde: {models_dir}")
        detector = ProphetAnomalyDetector()
        
        try:
            detector.load_models(str(models_dir))
            print(f"[OK] {len(detector.models)} modelos cargados")
        except Exception as e:
            print(f"[ERROR] Error cargando modelos: {str(e)}")
            import traceback
            traceback.print_exc()
            return
        
        # Leer datos desde SQL
        # Puedes filtrar por fechas si quieres procesar solo datos nuevos
        # Por ejemplo, últimos 7 días:
        # from datetime import datetime, timedelta
        # end_date = datetime.now()
        # start_date = end_date - timedelta(days=7)
        # df = read_data_from_sql(sql_conn, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        df = read_data_from_sql(sql_conn_input)
        
        if df is None:
            return
        
        # Obtener variables disponibles en los modelos
        available_vars = [v for v in detector.models.keys() if v in df.columns]
        
        if not available_vars:
            print(f"\n[ADVERTENCIA] No hay variables comunes entre modelos y datos")
            return
        
        print(f"\n[INFO] Variables a analizar: {len(available_vars)}")
        
        # Detectar anomalías
        print("\n" + "="*80)
        print("DETECTANDO ANOMALÍAS")
        print("="*80)
        
        try:
            results = detector.detect_anomalies_multiple(
                df=df,
                variables=available_vars,
                datetime_col=datetime_col,
                combine_results=True
            )
            
            # Agregar source_file si no existe
            if 'source_file' not in results.columns:
                results['source_file'] = 'sql_ypf_process_data'
            
            # Convertir booleanos a 0/1 para SQL Server BIT
            bool_cols = ['outside_interval', 'high_residual', 'is_anomaly']
            for col in bool_cols:
                if col in results.columns:
                    results[col] = results[col].astype(int)
            
            # Renombrar 'ds' si es necesario (ya debería estar como 'ds')
            if 'ds' not in results.columns and datetime_col in results.columns:
                results.rename(columns={datetime_col: 'ds'}, inplace=True)
            
            print(f"\n[OK] Anomalías detectadas: {results['is_anomaly'].sum()} de {len(results)} puntos")
            print(f"     Tasa de anomalías: {results['is_anomaly'].mean()*100:.2f}%")
            
        except Exception as e:
            print(f"\n[ERROR] Error detectando anomalías: {str(e)}")
            import traceback
            traceback.print_exc()
            return
        
        # Escribir resultados a SQL
        print("\n" + "="*80)
        print("ESCRIBIENDO RESULTADOS A SQL")
        print("="*80)
        
        # Seleccionar solo las columnas que existen en la tabla
        columns_to_write = [
            'ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'residual',
            'outside_interval', 'high_residual', 'is_anomaly', 'anomaly_score',
            'variable', 'prediction_error_pct', 'source_file'
        ]
        
        # Filtrar columnas que existen
        available_cols = [col for col in columns_to_write if col in results.columns]
        results_to_write = results[available_cols].copy()
        
        success = sql_conn_output.write_dataframe(
            results_to_write,
            table_name='ypf_anomaly_detector',
            if_exists='append'
        )
        
        if success:
            print(f"\n[OK] {len(results_to_write)} filas escritas en otms_analytics.dbo.ypf_anomaly_detector")
        else:
            print(f"\n[ERROR] Error escribiendo resultados")
        
        # Resumen final
        print("\n" + "="*80)
        print("RESUMEN")
        print("="*80)
        print(f"[OK] Total de puntos analizados: {len(results):,}")
        print(f"[OK] Anomalías detectadas: {results['is_anomaly'].sum():,}")
        print(f"[OK] Tasa de anomalías: {results['is_anomaly'].mean()*100:.2f}%")
        
        # Top variables con más anomalías
        if len(results) > 0:
            summary = detector.get_anomaly_summary(results)
            print(f"\nTop 5 variables con más anomalías:")
            print(summary.head(5).to_string())
        
    finally:
        sql_conn_input.disconnect()
        sql_conn_output.disconnect()


if __name__ == '__main__':
    main()

