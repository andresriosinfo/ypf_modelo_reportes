"""
Worker para reentrenamiento automático de modelos
Reentrena los modelos todos los días a las 2:00 AM usando datos de SQL
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
import logging

sys.path.append(str(Path(__file__).parent))

from sql_utils import SQLConnection
from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector

# Configuración de conexión SQL
SQL_CONFIG = {
    'server': '10.147.17.241',
    'database': 'otms_main',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def read_data_from_sql(sql_conn: SQLConnection, start_date: str = None, 
                       end_date: str = None) -> pd.DataFrame:
    """
    Lee datos desde SQL Server y los convierte a formato ancho
    
    Parámetros:
    -----------
    sql_conn : SQLConnection
        Conexión a SQL
    start_date : str
        Fecha de inicio (opcional, formato: 'YYYY-MM-DD')
    end_date : str
        Fecha de fin (opcional, formato: 'YYYY-MM-DD')
        
    Retorna:
    --------
    pd.DataFrame en formato ancho (datetime + variables en columnas)
    """
    print("\n[INFO] Leyendo datos desde SQL Server...")
    sys.stdout.flush()
    
    # Construir query
    query = "SELECT datetime, variable_name, value FROM dbo.ypf_process_data"
    
    conditions = []
    if start_date:
        conditions.append(f"datetime >= '{start_date}'")
    if end_date:
        conditions.append(f"datetime <= '{end_date}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY datetime, variable_name"
    
    print(f"  Query: {query[:100]}...")
    sys.stdout.flush()
    
    # Ejecutar query
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        print("[ERROR] No se encontraron datos en SQL")
        sys.stdout.flush()
        return None
    
    print(f"  Filas leídas: {len(df_long):,}")
    sys.stdout.flush()
    
    # Convertir a formato ancho
    print("  Transformando a formato ancho...")
    sys.stdout.flush()
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'  # Si hay duplicados, tomar el primero
    )
    
    # Resetear índice para que datetime sea columna
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    print(f"  Dimensiones finales: {df_wide.shape[0]} filas x {df_wide.shape[1]} columnas")
    print(f"  Variables: {df_wide.shape[1] - 1}")
    print(f"  Rango temporal: {df_wide['DATETIME'].min()} a {df_wide['DATETIME'].max()}")
    sys.stdout.flush()
    
    return df_wide


def retrain_models(sql_conn: SQLConnection, models_dir: Path) -> bool:
    """
    Reentrena los modelos usando datos de SQL
    
    Retorna:
    --------
    bool: True si fue exitoso, False si hubo error
    """
    try:
        print("\n" + "="*80)
        print("INICIANDO REENTRENAMIENTO DE MODELOS")
        print("="*80)
        sys.stdout.flush()
        
        # Leer datos desde SQL
        df = read_data_from_sql(sql_conn)
        
        if df is None:
            print("[ERROR] No se pudieron leer datos de SQL")
            sys.stdout.flush()
            return False
        
        # Seleccionar variables (excluir datetime)
        variables = [col for col in df.columns if col != 'DATETIME']
        
        print(f"\n[INFO] Entrenando modelos para {len(variables)} variables")
        sys.stdout.flush()
        
        # Crear detector
        detector = ProphetAnomalyDetector(
            interval_width=0.95,
            changepoint_prior_scale=0.05,
            seasonality_mode='multiplicative',
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,
            anomaly_threshold=2.0
        )
        
        # Entrenar modelos
        print("\n" + "="*80)
        print("ENTRENANDO MODELOS")
        print("="*80)
        sys.stdout.flush()
        
        try:
            detector.train_multiple_variables(
                df=df,
                variables=variables,
                datetime_col='DATETIME',
                verbose=True
            )
        except Exception as e:
            print(f"\n[ERROR] Error durante el entrenamiento: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            return False
        
        # Guardar modelos
        print("\n" + "="*80)
        print("GUARDANDO MODELOS")
        print("="*80)
        sys.stdout.flush()
        
        try:
            detector.save_models(str(models_dir))
            print(f"\n[OK] Modelos guardados exitosamente en: {models_dir}")
            print(f"[OK] Total de modelos: {len(detector.models)}")
            sys.stdout.flush()
            return True
        except Exception as e:
            print(f"\n[ERROR] Error guardando modelos: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            return False
            
    except Exception as e:
        print(f"\n[ERROR] Error en reentrenamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        return False


class RetrainingWorker:
    """Worker para reentrenamiento automático de modelos"""
    
    def __init__(self, training_hour: int = 2, training_minute: int = 0):
        """
        Inicializa el worker
        
        Parámetros:
        -----------
        training_hour : int
            Hora del día para reentrenar (0-23, default: 2 = 2:00 AM)
        training_minute : int
            Minuto de la hora para reentrenar (0-59, default: 0)
        """
        self.training_hour = training_hour
        self.training_minute = training_minute
        self.sql_conn = None
        self.models_dir = Path("pipeline/models/prophet")
        self.last_training_date = None
        self.training_count = 0
        self.check_interval_seconds = 60  # Verificar cada minuto
        
    def initialize(self) -> bool:
        """Inicializa conexión a SQL"""
        print("[INFO] Conectando a SQL Server...")
        sys.stdout.flush()
        self.sql_conn = SQLConnection(**SQL_CONFIG)
        if not self.sql_conn.connect():
            print("[ERROR] No se pudo conectar a SQL Server")
            sys.stdout.flush()
            return False
        
        # Crear directorio de modelos si no existe
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        return True
    
    def should_retrain(self) -> bool:
        """
        Verifica si es hora de reentrenar
        
        Retorna:
        --------
        bool: True si es hora de reentrenar
        """
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        current_date = now.date()
        
        # Verificar si es la hora de entrenamiento
        if current_hour == self.training_hour and current_minute == self.training_minute:
            # Verificar que no se haya entrenado hoy ya
            if self.last_training_date != current_date:
                return True
        
        return False
    
    def retrain(self) -> bool:
        """Ejecuta el reentrenamiento"""
        if not self.sql_conn:
            print("[ERROR] No hay conexión a SQL")
            sys.stdout.flush()
            return False
        
        success = retrain_models(self.sql_conn, self.models_dir)
        
        if success:
            self.last_training_date = datetime.now().date()
            self.training_count += 1
            print(f"\n[OK] Reentrenamiento completado exitosamente (#{self.training_count})")
            sys.stdout.flush()
        else:
            print(f"\n[ERROR] Reentrenamiento falló")
            sys.stdout.flush()
        
        return success
    
    def run(self):
        """Ejecuta el worker en modo continuo"""
        if not self.initialize():
            print("[ERROR] No se pudo inicializar el worker")
            sys.stdout.flush()
            return
        
        print("\n" + "="*80)
        print("WORKER DE REENTRENAMIENTO AUTOMÁTICO")
        print("="*80)
        print(f"[INFO] Hora de reentrenamiento: {self.training_hour:02d}:{self.training_minute:02d}")
        print(f"[INFO] Verificando cada {self.check_interval_seconds} segundos")
        print(f"[INFO] Presiona Ctrl+C para detener")
        print()
        sys.stdout.flush()
        
        try:
            while True:
                now = datetime.now()
                
                # Verificar si es hora de reentrenar
                if self.should_retrain():
                    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] ¡Es hora de reentrenar!")
                    sys.stdout.flush()
                    
                    self.retrain()
                    
                    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Reentrenamiento completado. Próximo: mañana a las {self.training_hour:02d}:{self.training_minute:02d}")
                    print()
                    sys.stdout.flush()
                else:
                    # Solo mostrar mensaje cada hora para no saturar la consola
                    if now.minute == 0:
                        next_training = datetime.now().replace(
                            hour=self.training_hour,
                            minute=self.training_minute,
                            second=0,
                            microsecond=0
                        )
                        # Si ya pasó la hora de hoy, será mañana
                        if next_training < now:
                            next_training += timedelta(days=1)
                        
                        time_until = next_training - now
                        hours = int(time_until.total_seconds() // 3600)
                        minutes = int((time_until.total_seconds() % 3600) // 60)
                        
                        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Esperando reentrenamiento... Próximo en {hours}h {minutes}m (a las {self.training_hour:02d}:{self.training_minute:02d})")
                        sys.stdout.flush()
                
                # Esperar antes de la próxima verificación
                time.sleep(self.check_interval_seconds)
                
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print("WORKER DETENIDO POR EL USUARIO")
            print("="*80)
            print(f"[OK] Reentrenamientos completados: {self.training_count}")
            print(f"[OK] Último reentrenamiento: {self.last_training_date}")
            sys.stdout.flush()
        
        finally:
            if self.sql_conn:
                self.sql_conn.disconnect()
                print("[INFO] Conexión a SQL cerrada")
                sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description='Worker para reentrenamiento automático de modelos',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Reentrenar todos los días a las 2:00 AM (default)
  python worker_reentrenamiento.py
  
  # Reentrenar todos los días a las 3:30 AM
  python worker_reentrenamiento.py --hour 3 --minute 30
  
  # Reentrenar todos los días a medianoche
  python worker_reentrenamiento.py --hour 0 --minute 0
        """
    )
    parser.add_argument('--hour', type=int, default=2,
                       help='Hora del día para reentrenar (0-23, default: 2)')
    parser.add_argument('--minute', type=int, default=0,
                       help='Minuto de la hora para reentrenar (0-59, default: 0)')
    
    args = parser.parse_args()
    
    # Validar argumentos
    if not (0 <= args.hour <= 23):
        print("[ERROR] La hora debe estar entre 0 y 23")
        sys.exit(1)
    
    if not (0 <= args.minute <= 59):
        print("[ERROR] El minuto debe estar entre 0 y 59")
        sys.exit(1)
    
    worker = RetrainingWorker(training_hour=args.hour, training_minute=args.minute)
    worker.run()


if __name__ == '__main__':
    main()

