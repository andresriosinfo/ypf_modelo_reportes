"""
Procesador de anomalías para datos individuales en tiempo real
Procesa cada dato nuevo tan pronto como llega a la base de datos
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import argparse

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
        ("idx_processed_at", "CREATE INDEX idx_processed_at ON dbo.ypf_anomaly_detector(processed_at)")
    ]
    
    for idx_name, idx_query in indexes:
        try:
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
            pass


def get_processed_datetimes(sql_conn: SQLConnection) -> set:
    """Obtiene el conjunto de datetimes ya procesados"""
    query = "SELECT DISTINCT ds FROM dbo.ypf_anomaly_detector"
    result = sql_conn.execute_query(query)
    
    if result is None or result.empty:
        return set()
    
    return set(pd.to_datetime(result['ds']).dt.to_pydatetime())


def get_all_datetimes_from_source(sql_conn: SQLConnection) -> set:
    """Obtiene todos los datetimes disponibles en ypf_process_data"""
    query = "SELECT DISTINCT datetime FROM dbo.ypf_process_data ORDER BY datetime"
    result = sql_conn.execute_query(query)
    
    if result is None or result.empty:
        return set()
    
    return set(pd.to_datetime(result['datetime']).dt.to_pydatetime())


def get_data_for_datetime(sql_conn: SQLConnection, target_datetime: datetime) -> pd.DataFrame:
    """
    Obtiene todos los datos para un datetime específico
    """
    query = f"""
        SELECT datetime, variable_name, value, source_file
        FROM dbo.ypf_process_data
        WHERE datetime = '{target_datetime.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY variable_name
    """
    
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        return None
    
    return df_long


def convert_long_to_wide_single(df_long: pd.DataFrame) -> pd.DataFrame:
    """Convierte datos de formato largo a ancho para un solo datetime"""
    if df_long.empty:
        return None
    
    # Obtener el datetime único
    datetime_val = df_long['datetime'].iloc[0]
    
    # Crear un DataFrame ancho con una sola fila
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'
    )
    
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    return df_wide


def process_single_datetime(sql_conn_output: SQLConnection,
                           detector: ProphetAnomalyDetector,
                           target_datetime: datetime,
                           sql_conn_input: SQLConnection) -> int:
    """
    Procesa un datetime específico y detecta anomalías
    
    Parámetros:
    -----------
    sql_conn_output : SQLConnection
        Conexión para escribir resultados (base de datos de salida)
    detector : ProphetAnomalyDetector
        Detector de anomalías
    target_datetime : datetime
        DateTime a procesar
    sql_conn_input : SQLConnection
        Conexión para leer datos (base de datos de entrada)
    
    Retorna:
    --------
    int: Número de anomalías detectadas
    """
    # Leer datos para este datetime (de base de datos de entrada)
    df_long = get_data_for_datetime(sql_conn_input, target_datetime)
    
    if df_long is None or len(df_long) == 0:
        return 0
    
    # Convertir a formato ancho
    df_wide = convert_long_to_wide_single(df_long)
    
    if df_wide is None or df_wide.empty:
        return 0
    
    # Obtener variables disponibles en los modelos
    available_vars = [v for v in detector.models.keys() if v in df_wide.columns]
    
    if not available_vars:
        return 0
    
    # Detectar anomalías
    try:
        results = detector.detect_anomalies_multiple(
            df=df_wide,
            variables=available_vars,
            datetime_col='DATETIME',
            combine_results=True
        )
        
        if results is None or len(results) == 0:
            return 0
        
        # Agregar source_file si no existe
        if 'source_file' not in results.columns:
            source_file_map = df_long.groupby(['datetime', 'variable_name'])['source_file'].first().to_dict()
            results['source_file'] = results.apply(
                lambda row: source_file_map.get((row['ds'], row['variable']), 'unknown'),
                axis=1
            )
        
        # Convertir booleanos a 0/1 para SQL Server BIT
        bool_cols = ['outside_interval', 'high_residual', 'is_anomaly']
        for col in bool_cols:
            if col in results.columns:
                results[col] = results[col].astype(int)
        
        # Limpiar valores numéricos para SQL Server
        if 'anomaly_score' in results.columns:
            results['anomaly_score'] = results['anomaly_score'].fillna(0).clip(lower=0, upper=999.99)
        
        if 'prediction_error_pct' in results.columns:
            results['prediction_error_pct'] = results['prediction_error_pct'].replace([np.inf, -np.inf], np.nan)
            results['prediction_error_pct'] = results['prediction_error_pct'].fillna(0).clip(lower=0, upper=999.99)
        
        # Seleccionar columnas para escribir
        columns_to_write = [
            'ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'residual',
            'outside_interval', 'high_residual', 'is_anomaly', 'anomaly_score',
            'variable', 'prediction_error_pct', 'source_file'
        ]
        
        available_cols = [col for col in columns_to_write if col in results.columns]
        results_to_write = results[available_cols].copy()
        
        # Escribir a SQL (base de datos de salida)
        success = sql_conn_output.write_dataframe(
            results_to_write,
            table_name='ypf_anomaly_detector',
            if_exists='append'
        )
        
        if success:
            n_anomalies = results['is_anomaly'].sum()
            return n_anomalies
        else:
            return 0
            
    except Exception as e:
        print(f"[ERROR] Error procesando datetime {target_datetime}: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


def run_individual_processor(poll_interval: float = 2.0):
    """
    Ejecuta el procesador que monitorea y procesa cada dato nuevo individualmente
    
    Parámetros:
    -----------
    poll_interval : float
        Intervalo en segundos entre verificaciones de nuevos datos
    """
    print("="*80)
    print("PROCESADOR DE ANOMALÍAS - DATOS INDIVIDUALES")
    print("="*80)
    
    # Verificar modelos
    models_dir = Path("pipeline/models/prophet")
    if not models_dir.exists() or not list(models_dir.glob("prophet_model_*.pkl")):
        print(f"\n[ERROR] No se encontraron modelos entrenados en {models_dir}")
        print("   Entrena los modelos primero:")
        print("   python train_from_sql.py")
        return
    
    # Conectar a SQL - Base de datos de entrada
    sql_conn_input = SQLConnection(**SQL_CONFIG_INPUT)
    if not sql_conn_input.connect():
        return
    
    # Conectar a SQL - Base de datos de salida
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
        
        print(f"[INFO] Intervalo de polling: {poll_interval} segundos")
        print(f"[INFO] Presiona Ctrl+C para detener\n")
        
        iteration = 0
        total_processed = 0
        total_anomalies = 0
        
        # Obtener datetimes ya procesados inicialmente (de base de datos de salida)
        processed_datetimes = get_processed_datetimes(sql_conn_output)
        print(f"[INFO] Datetimes ya procesados: {len(processed_datetimes)}")
        
        try:
            while True:
                iteration += 1
                current_time = datetime.now()
                
                # Obtener todos los datetimes disponibles (de base de datos de entrada)
                all_datetimes = get_all_datetimes_from_source(sql_conn_input)
                
                # Encontrar datetimes nuevos (no procesados)
                new_datetimes = sorted([dt for dt in all_datetimes if dt not in processed_datetimes])
                
                if new_datetimes:
                    print(f"\n[{iteration}] {current_time.strftime('%Y-%m-%d %H:%M:%S')} - {len(new_datetimes)} datetime(s) nuevo(s) encontrado(s)")
                    
                    # Procesar cada datetime nuevo (escribir en base de datos de salida)
                    for dt in new_datetimes:
                        print(f"  [PROCESANDO] {dt.strftime('%Y-%m-%d %H:%M:%S')}...", end=' ', flush=True)
                        
                        n_anomalies = process_single_datetime(sql_conn_output, detector, dt, sql_conn_input)
                        
                        if n_anomalies > 0:
                            print(f"[ANOMALÍAS: {n_anomalies}]")
                            total_anomalies += n_anomalies
                        else:
                            print("[OK]")
                        
                        # Marcar como procesado
                        processed_datetimes.add(dt)
                        total_processed += 1
                    
                    print(f"  [RESUMEN] {len(new_datetimes)} datetime(s) procesado(s), {total_anomalies} anomalía(s) total(es)")
                else:
                    # Solo mostrar mensaje cada 10 iteraciones para no saturar la consola
                    if iteration % 10 == 0:
                        print(f"[{iteration}] {current_time.strftime('%Y-%m-%d %H:%M:%S')} - Esperando nuevos datos... (Total procesados: {total_processed})")
                
                # Esperar antes de la próxima verificación
                time.sleep(poll_interval)
                
        except KeyboardInterrupt:
            print(f"\n\n[ADVERTENCIA] Procesamiento interrumpido por el usuario")
        
        print(f"\n{'='*80}")
        print("RESUMEN FINAL")
        print(f"{'='*80}")
        print(f"[OK] Iteraciones completadas: {iteration}")
        print(f"[OK] Total de datetimes procesados: {total_processed}")
        print(f"[OK] Total de anomalías detectadas: {total_anomalies}")
        
    finally:
        sql_conn_input.disconnect()
        sql_conn_output.disconnect()


def main():
    parser = argparse.ArgumentParser(
        description='Procesador de anomalías para datos individuales en tiempo real'
    )
    parser.add_argument('--poll-interval', type=float, default=2.0,
                       help='Intervalo en segundos entre verificaciones de nuevos datos (default: 2.0)')
    
    args = parser.parse_args()
    
    run_individual_processor(poll_interval=args.poll_interval)


if __name__ == '__main__':
    main()

