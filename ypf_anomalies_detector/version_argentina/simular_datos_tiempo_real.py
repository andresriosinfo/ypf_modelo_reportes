"""
Simulador de datos en tiempo real
Lee datos históricos y los inserta en SQL Server simulando llegada en tiempo real
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

# Configuración de conexión SQL
SQL_CONFIG = {
    'server': '10.147.17.241',
    'database': 'otms_main',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}


def get_last_datetime_in_sql(sql_conn: SQLConnection) -> datetime:
    """Obtiene el último datetime de la tabla ypf_process_data"""
    query = "SELECT MAX(datetime) as last_datetime FROM dbo.ypf_process_data"
    result = sql_conn.execute_query(query)
    
    if result is not None and not result.empty and result['last_datetime'].iloc[0] is not None:
        return pd.to_datetime(result['last_datetime'].iloc[0])
    else:
        # Si no hay datos, empezar desde una fecha por defecto
        return datetime(2024, 1, 1)


def read_historical_data(source: str = 'sql') -> pd.DataFrame:
    """
    Lee datos históricos desde SQL o CSV
    
    Parámetros:
    -----------
    source : str
        'sql' para leer desde SQL, 'csv' para leer desde archivos CSV
    """
    if source == 'sql':
        print("\n[INFO] Leyendo datos históricos desde SQL Server...")
        sql_conn = SQLConnection(**SQL_CONFIG)
        if not sql_conn.connect():
            return None
        
        try:
            query = """
                SELECT datetime, variable_name, value, source_file
                FROM dbo.ypf_process_data
                ORDER BY datetime, variable_name
            """
            df_long = sql_conn.execute_query(query)
            
            if df_long is None or len(df_long) == 0:
                print("[ERROR] No se encontraron datos en SQL")
                return None
            
            print(f"  Filas leídas: {len(df_long):,}")
            return df_long
            
        finally:
            sql_conn.disconnect()
    
    else:  # source == 'csv'
        print("\n[INFO] Leyendo datos históricos desde archivos CSV...")
        data_dir = Path("output")
        cleaned_files = list(data_dir.glob("*_cleaned.csv"))
        
        if not cleaned_files:
            print(f"[ERROR] No se encontraron archivos CSV en {data_dir}")
            return None
        
        # Leer y combinar archivos
        dfs = []
        for f in cleaned_files:
            df = pd.read_csv(f, parse_dates=['DATETIME'])
            # Convertir a formato largo
            variable_cols = [col for col in df.columns if col != 'DATETIME']
            df_long = df.melt(
                id_vars=['DATETIME'],
                value_vars=variable_cols,
                var_name='variable_name',
                value_name='value'
            )
            df_long['source_file'] = f.stem
            df_long.rename(columns={'DATETIME': 'datetime'}, inplace=True)
            dfs.append(df_long)
        
        df_combined = pd.concat(dfs, ignore_index=True)
        df_combined = df_combined.dropna(subset=['value'])
        print(f"  Filas leídas: {len(df_combined):,}")
        return df_combined


def simulate_realtime_data(df_historical: pd.DataFrame,
                          start_datetime: datetime = None,
                          interval_seconds: int = 60,
                          speed_multiplier: float = 1.0,
                          max_rows: int = None,
                          add_noise: bool = False,
                          noise_level: float = 0.01):
    """
    Simula inserción de datos en tiempo real
    
    Parámetros:
    -----------
    df_historical : pd.DataFrame
        Datos históricos en formato largo (datetime, variable_name, value)
    start_datetime : datetime
        Fecha/hora de inicio de la simulación (None = usar último datetime en SQL)
    interval_seconds : int
        Intervalo en segundos entre inserciones (default: 60 = 1 minuto)
    speed_multiplier : float
        Multiplicador de velocidad (1.0 = tiempo real, 10.0 = 10x más rápido)
    max_rows : int
        Número máximo de filas a insertar (None = todas)
    add_noise : bool
        Si agregar ruido aleatorio a los valores
    noise_level : float
        Nivel de ruido (0.01 = 1% de variación)
    """
    print("\n" + "="*80)
    print("SIMULACIÓN DE DATOS EN TIEMPO REAL")
    print("="*80)
    
    # Conectar a SQL
    sql_conn = SQLConnection(**SQL_CONFIG)
    if not sql_conn.connect():
        return
    
    try:
        # Determinar fecha de inicio
        if start_datetime is None:
            start_datetime = get_last_datetime_in_sql(sql_conn)
            print(f"\n[INFO] Último datetime en SQL: {start_datetime}")
            start_datetime = start_datetime + timedelta(seconds=interval_seconds)
        
        print(f"[INFO] Iniciando simulación desde: {start_datetime}")
        print(f"[INFO] Intervalo entre inserciones: {interval_seconds} segundos")
        print(f"[INFO] Velocidad: {speed_multiplier}x tiempo real")
        print(f"[INFO] Ruido: {'Sí' if add_noise else 'No'}")
        
        # Obtener fechas únicas de los datos históricos
        historical_dates = sorted(df_historical['datetime'].unique())
        
        if max_rows:
            # Limitar número de filas
            total_rows = len(df_historical)
            if max_rows < total_rows:
                # Seleccionar un subconjunto aleatorio
                df_historical = df_historical.sample(n=max_rows, random_state=42)
                historical_dates = sorted(df_historical['datetime'].unique())
                print(f"[INFO] Limitando a {max_rows:,} filas de {total_rows:,} totales")
        
        # Agrupar por datetime para insertar todos los valores de un timestamp juntos
        grouped = df_historical.groupby('datetime')
        
        current_sim_datetime = start_datetime
        total_inserted = 0
        n_groups = len(grouped)
        
        print(f"\n[INFO] Procesando {n_groups} timestamps históricos...")
        print(f"[INFO] Presiona Ctrl+C para detener la simulación\n")
        
        start_time = time.time()
        
        try:
            for i, (hist_datetime, group_df) in enumerate(grouped, 1):
                # Preparar datos para este timestamp
                df_to_insert = group_df.copy()
                
                # Actualizar datetime al tiempo simulado
                df_to_insert['datetime'] = current_sim_datetime
                
                # Agregar ruido si está habilitado
                if add_noise:
                    noise = np.random.normal(0, noise_level, len(df_to_insert))
                    df_to_insert['value'] = df_to_insert['value'] * (1 + noise)
                
                # Eliminar columnas que no están en la tabla SQL
                columns_to_write = ['datetime', 'variable_name', 'value', 'source_file']
                df_to_insert = df_to_insert[columns_to_write].copy()
                
                # Insertar en SQL
                success = sql_conn.write_dataframe(
                    df_to_insert,
                    table_name='ypf_process_data',
                    if_exists='append'
                )
                
                if success:
                    total_inserted += len(df_to_insert)
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    
                    print(f"[{i}/{n_groups}] {current_sim_datetime.strftime('%Y-%m-%d %H:%M:%S')} - "
                          f"{len(df_to_insert)} valores insertados | "
                          f"Total: {total_inserted:,} | "
                          f"Velocidad: {rate:.0f} filas/seg")
                    
                    # Avanzar al siguiente timestamp
                    current_sim_datetime += timedelta(seconds=interval_seconds)
                    
                    # Esperar el intervalo ajustado por el multiplicador de velocidad
                    wait_time = interval_seconds / speed_multiplier
                    if wait_time > 0:
                        time.sleep(wait_time)
                else:
                    print(f"[ERROR] Error insertando datos para {current_sim_datetime}")
                    break
                
        except KeyboardInterrupt:
            print(f"\n\n[ADVERTENCIA] Simulación interrumpida por el usuario")
        
        elapsed = time.time() - start_time
        print(f"\n{'='*80}")
        print("RESUMEN DE SIMULACIÓN")
        print(f"{'='*80}")
        print(f"[OK] Filas insertadas: {total_inserted:,}")
        print(f"[OK] Tiempo transcurrido: {elapsed:.1f} segundos")
        print(f"[OK] Velocidad promedio: {total_inserted/elapsed:.0f} filas/seg")
        print(f"[OK] Último datetime insertado: {current_sim_datetime}")
        
    finally:
        sql_conn.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Simulador de datos en tiempo real')
    parser.add_argument('--source', type=str, default='sql', choices=['sql', 'csv'],
                       help='Fuente de datos históricos (sql o csv)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Intervalo en segundos entre inserciones (default: 60)')
    parser.add_argument('--speed', type=float, default=1.0,
                       help='Multiplicador de velocidad (1.0 = tiempo real, 10.0 = 10x más rápido)')
    parser.add_argument('--max-rows', type=int, default=None,
                       help='Número máximo de filas a insertar (default: todas)')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Fecha de inicio (YYYY-MM-DD HH:MM:SS). Si no se especifica, usa el último datetime en SQL')
    parser.add_argument('--add-noise', action='store_true',
                       help='Agregar ruido aleatorio a los valores')
    parser.add_argument('--noise-level', type=float, default=0.01,
                       help='Nivel de ruido (default: 0.01 = 1%%)')
    
    args = parser.parse_args()
    
    # Leer datos históricos
    df_historical = read_historical_data(source=args.source)
    
    if df_historical is None:
        print("[ERROR] No se pudieron leer datos históricos")
        return
    
    # Parsear fecha de inicio si se proporciona
    start_datetime = None
    if args.start_date:
        try:
            start_datetime = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"[ERROR] Formato de fecha inválido. Use: YYYY-MM-DD HH:MM:SS")
            return
    
    # Ejecutar simulación
    simulate_realtime_data(
        df_historical=df_historical,
        start_datetime=start_datetime,
        interval_seconds=args.interval,
        speed_multiplier=args.speed,
        max_rows=args.max_rows,
        add_noise=args.add_noise,
        noise_level=args.noise_level
    )


if __name__ == '__main__':
    main()

