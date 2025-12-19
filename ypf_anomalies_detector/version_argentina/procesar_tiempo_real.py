"""
Procesador de anomalías en tiempo real
Monitorea la tabla ypf_process_data y detecta anomalías en nuevos datos
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


def get_last_processed_datetime(sql_conn: SQLConnection) -> datetime:
    """Obtiene el último datetime procesado de ypf_anomaly_detector"""
    query = "SELECT MAX(ds) as last_ds FROM dbo.ypf_anomaly_detector"
    result = sql_conn.execute_query(query)
    
    if result is not None and not result.empty and result['last_ds'].iloc[0] is not None:
        return pd.to_datetime(result['last_ds'].iloc[0])
    else:
        return None


def get_new_data_from_sql(sql_conn: SQLConnection, since_datetime: datetime = None) -> pd.DataFrame:
    """
    Lee nuevos datos desde SQL Server desde un datetime específico
    
    Parámetros:
    -----------
    sql_conn : SQLConnection
        Conexión a SQL Server
    since_datetime : datetime
        Fecha desde la cual leer datos nuevos (None = últimos 1 hora)
    """
    if since_datetime is None:
        since_datetime = datetime.now() - timedelta(hours=1)
    
    query = f"""
        SELECT datetime, variable_name, value, source_file
        FROM dbo.ypf_process_data
        WHERE datetime > '{since_datetime.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY datetime, variable_name
    """
    
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        return None
    
    return df_long


def convert_long_to_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    """Convierte datos de formato largo a ancho"""
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'
    )
    
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    return df_wide


def process_new_anomalies(sql_conn_output: SQLConnection,
                         detector: ProphetAnomalyDetector,
                         since_datetime: datetime = None,
                         sql_conn_input: SQLConnection = None) -> int:
    """
    Procesa nuevos datos y detecta anomalías
    
    Parámetros:
    -----------
    sql_conn_output : SQLConnection
        Conexión para escribir resultados (base de datos de salida)
    detector : ProphetAnomalyDetector
        Detector de anomalías
    since_datetime : datetime
        Fecha desde la cual procesar
    sql_conn_input : SQLConnection, optional
        Conexión para leer datos (base de datos de entrada). Si no se proporciona, usa sql_conn_output.
    
    Retorna:
    --------
    int: Número de anomalías detectadas
    """
    # Leer nuevos datos (de base de datos de entrada)
    conn_to_read = sql_conn_input if sql_conn_input is not None else sql_conn_output
    df_long = get_new_data_from_sql(conn_to_read, since_datetime)
    
    if df_long is None or len(df_long) == 0:
        print(f"  [INFO] No se encontraron datos nuevos en SQL desde {since_datetime}")
        return 0
    
    print(f"  [INFO] Datos encontrados: {len(df_long)} filas")
    
    # Convertir a formato ancho
    df_wide = convert_long_to_wide(df_long)
    print(f"  [INFO] Datos convertidos a formato ancho: {df_wide.shape[0]} filas x {df_wide.shape[1]} columnas")
    
    # Obtener variables disponibles en los modelos
    available_vars = [v for v in detector.models.keys() if v in df_wide.columns]
    
    if not available_vars:
        print(f"  [ADVERTENCIA] No hay variables comunes entre modelos ({len(detector.models)} modelos) y datos ({len(df_wide.columns)-1} variables)")
        print(f"  [INFO] Variables en datos: {list(df_wide.columns)[:5]}...")
        print(f"  [INFO] Variables en modelos: {list(detector.models.keys())[:5]}...")
        return 0
    
    print(f"  [INFO] Variables a analizar: {len(available_vars)} de {len(detector.models)} modelos")
    
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
            # Intentar obtener source_file del df_long original
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
        # DECIMAL(5,2) permite valores de -999.99 a 999.99
        if 'anomaly_score' in results.columns:
            results['anomaly_score'] = results['anomaly_score'].fillna(0).clip(lower=0, upper=999.99)
        
        if 'prediction_error_pct' in results.columns:
            # Reemplazar infinitos y NaN, luego limitar al rango
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
        print(f"[ERROR] Error procesando anomalías: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


def run_realtime_processor(interval_seconds: int = 60,
                          lookback_hours: int = 1,
                          continuous: bool = True):
    """
    Ejecuta el procesador en tiempo real
    
    Parámetros:
    -----------
    interval_seconds : int
        Intervalo en segundos entre procesamientos
    lookback_hours : int
        Horas hacia atrás para buscar datos nuevos
    continuous : bool
        Si ejecutar en loop continuo o una sola vez
    """
    print("="*80)
    print("PROCESADOR DE ANOMALÍAS EN TIEMPO REAL")
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
        
        # Obtener último datetime procesado (de base de datos de salida)
        last_processed = get_last_processed_datetime(sql_conn_output)
        if last_processed:
            print(f"[INFO] Último datetime procesado: {last_processed}")
            since_datetime = last_processed
        else:
            print(f"[INFO] No hay datos procesados anteriormente. Buscando datos de las últimas {lookback_hours} horas")
            since_datetime = datetime.now() - timedelta(hours=lookback_hours)
        
        print(f"[INFO] Intervalo de procesamiento: {interval_seconds} segundos")
        print(f"[INFO] Modo: {'Continuo' if continuous else 'Una sola vez'}")
        print(f"\n[INFO] Presiona Ctrl+C para detener\n")
        
        iteration = 0
        total_anomalies = 0
        
        try:
            while True:
                iteration += 1
                current_time = datetime.now()
                
                print(f"[{iteration}] {current_time.strftime('%Y-%m-%d %H:%M:%S')} - Procesando nuevos datos desde {since_datetime.strftime('%Y-%m-%d %H:%M:%S')}...")
                
                # Procesar nuevos datos (escribir en base de datos de salida)
                n_anomalies = process_new_anomalies(sql_conn_output, detector, since_datetime, sql_conn_input)
                
                if n_anomalies > 0:
                    total_anomalies += n_anomalies
                    print(f"  [OK] {n_anomalies} anomalías detectadas (Total acumulado: {total_anomalies})")
                else:
                    # El mensaje específico ya se muestra en process_new_anomalies
                    pass
                
                # Actualizar since_datetime para la próxima iteración
                # Usar el último datetime en ypf_process_data para evitar procesar duplicados (de base de datos de entrada)
                query = "SELECT MAX(datetime) as last_datetime FROM dbo.ypf_process_data"
                result = sql_conn_input.execute_query(query)
                if result is not None and not result.empty and result['last_datetime'].iloc[0] is not None:
                    new_last = pd.to_datetime(result['last_datetime'].iloc[0])
                    if new_last > since_datetime:
                        since_datetime = new_last
                
                if not continuous:
                    break
                
                # Esperar antes de la próxima iteración
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print(f"\n\n[ADVERTENCIA] Procesamiento interrumpido por el usuario")
        
        print(f"\n{'='*80}")
        print("RESUMEN")
        print(f"{'='*80}")
        print(f"[OK] Iteraciones completadas: {iteration}")
        print(f"[OK] Total de anomalías detectadas: {total_anomalies}")
        
    finally:
        sql_conn_input.disconnect()
        sql_conn_output.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Procesador de anomalías en tiempo real')
    parser.add_argument('--interval', type=int, default=60,
                       help='Intervalo en segundos entre procesamientos (default: 60)')
    parser.add_argument('--lookback', type=int, default=1,
                       help='Horas hacia atrás para buscar datos nuevos si no hay procesamiento previo (default: 1)')
    parser.add_argument('--once', action='store_true',
                       help='Procesar una sola vez y salir (default: modo continuo)')
    
    args = parser.parse_args()
    
    run_realtime_processor(
        interval_seconds=args.interval,
        lookback_hours=args.lookback,
        continuous=not args.once
    )


if __name__ == '__main__':
    main()

