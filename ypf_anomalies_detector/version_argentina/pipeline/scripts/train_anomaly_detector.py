"""
Script para entrenar el detector de anomalías usando Prophet
Versión adaptada para datos de Argentina
"""

import sys
from pathlib import Path
import pandas as pd

# Agregar el directorio padre al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent.parent))

from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector


def main():
    print("="*80)
    print("ENTRENAMIENTO DE DETECTOR DE ANOMALÍAS CON PROPHET - ARGENTINA")
    print("="*80)
    
    # Configuración
    data_dir = Path("output")  # Datos limpios del protocolo
    models_dir = Path("pipeline/models/prophet")
    datetime_col = 'DATETIME'
    
    # Encontrar archivos de datos limpios
    cleaned_files = list(data_dir.glob("*_cleaned.csv"))
    
    if not cleaned_files:
        print(f"\n[ERROR] No se encontraron archivos de datos limpios en {data_dir}")
        print("   Ejecuta primero el protocolo de seleccion de variables:")
        print("   python variable_selection_protocol.py")
        return
    
    print(f"\n[INFO] Archivos de datos encontrados: {len(cleaned_files)}")
    for f in cleaned_files:
        print(f"   - {f.name}")
    
    # Usar el archivo más reciente o combinar múltiples archivos
    if len(cleaned_files) == 1:
        data_file = cleaned_files[0]
        print(f"\n[INFO] Cargando datos de: {data_file.name}")
        df = pd.read_csv(data_file, parse_dates=[datetime_col])
    else:
        # Combinar múltiples archivos
        print(f"\n[INFO] Combinando {len(cleaned_files)} archivos...")
        dfs = []
        for f in cleaned_files:
            df_temp = pd.read_csv(f, parse_dates=[datetime_col])
            dfs.append(df_temp)
        
        # Combinar y eliminar duplicados
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=[datetime_col], keep='last')
        df = df.sort_values(datetime_col).reset_index(drop=True)
        print(f"   Total de registros: {len(df)}")
    
    print(f"\n[INFO] Dimensiones del dataset: {df.shape[0]} filas x {df.shape[1]} columnas")
    print(f"   Rango temporal: {df[datetime_col].min()} a {df[datetime_col].max()}")
    
    # Seleccionar variables (excluir datetime)
    variables = [col for col in df.columns if col != datetime_col]
    
    print(f"\n[OK] Entrenando modelos para {len(variables)} variables")
    
    # Crear detector
    detector = ProphetAnomalyDetector(
        interval_width=0.95,           # 95% intervalo de confianza
        changepoint_prior_scale=0.05,  # Flexibilidad moderada
        seasonality_mode='multiplicative',  # Mejor para datos de proceso
        daily_seasonality=True,        # Estacionalidad diaria
        weekly_seasonality=True,       # Estacionalidad semanal
        yearly_seasonality=False,      # No estacionalidad anual
        anomaly_threshold=2.0          # 2 desviaciones estándar
    )
    
    # Entrenar modelos
    print("\n" + "="*80)
    print("ENTRENANDO MODELOS")
    print("="*80)
    
    try:
        detector.train_multiple_variables(
            df=df,
            variables=variables,
            datetime_col=datetime_col,
            verbose=True
        )
    except KeyboardInterrupt:
        print("\n\n[ADVERTENCIA] Entrenamiento interrumpido por el usuario")
        return
    except Exception as e:
        print(f"\n[ERROR] Error durante el entrenamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Guardar modelos
    print("\n" + "="*80)
    print("GUARDANDO MODELOS")
    print("="*80)
    
    try:
        detector.save_models(str(models_dir))
        print(f"\n[OK] Modelos guardados exitosamente en: {models_dir}")
    except Exception as e:
        print(f"\n[ERROR] Error guardando modelos: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Resumen final
    print("\n" + "="*80)
    print("RESUMEN")
    print("="*80)
    print(f"[OK] Modelos entrenados: {len(detector.models)}")
    print(f"[OK] Ubicacion: {models_dir}")
    print(f"\nPróximos pasos:")
    print(f"  1. Ejecuta 'python pipeline/scripts/detect_anomalies.py' para detectar anomalías")
    print(f"  2. Revisa los resultados en 'pipeline/results/'")


if __name__ == '__main__':
    main()

