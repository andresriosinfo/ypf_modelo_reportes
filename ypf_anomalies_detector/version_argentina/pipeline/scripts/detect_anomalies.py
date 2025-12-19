"""
Script para detectar anomalías usando modelos Prophet entrenados
Versión adaptada para datos de Argentina
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Agregar el directorio padre al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector


def main():
    print("="*80)
    print("DETECCIÓN DE ANOMALÍAS CON PROPHET - ARGENTINA")
    print("="*80)
    
    # Configuración
    models_dir = Path("pipeline/models/prophet")
    data_dir = Path("output")
    results_dir = Path("pipeline/results")
    datetime_col = 'DATETIME'
    
    # Verificar que existan modelos
    if not models_dir.exists() or not list(models_dir.glob("prophet_model_*.pkl")):
        print(f"\n[ERROR] No se encontraron modelos entrenados en {models_dir}")
        print("   Entrena los modelos primero:")
        print("   python pipeline/scripts/train_anomaly_detector.py")
        return
    
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
    
    # Cargar datos
    cleaned_files = list(data_dir.glob("*_cleaned.csv"))
    
    if not cleaned_files:
        print(f"\n[ERROR] No se encontraron archivos de datos en {data_dir}")
        return
    
    # Procesar cada archivo o el más reciente
    print(f"\n[INFO] Archivos de datos encontrados: {len(cleaned_files)}")
    
    all_results = []
    
    for data_file in cleaned_files:
        print(f"\n{'='*80}")
        print(f"Procesando: {data_file.name}")
        print(f"{'='*80}")
        
        # Cargar datos
        df = pd.read_csv(data_file, parse_dates=[datetime_col])
        print(f"  Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
        
        # Obtener variables disponibles en los modelos
        available_vars = [v for v in detector.models.keys() if v in df.columns]
        
        if not available_vars:
            print(f"  [ADVERTENCIA] No hay variables comunes entre modelos y datos")
            continue
        
        print(f"  Variables a analizar: {len(available_vars)}")
        
        # Detectar anomalías
        try:
            results = detector.detect_anomalies_multiple(
                df=df,
                variables=available_vars,
                datetime_col=datetime_col,
                combine_results=True
            )
            
            # Agregar información del archivo
            results['source_file'] = data_file.stem
            
            all_results.append(results)
            
            # Resumen por archivo
            n_anomalies = results['is_anomaly'].sum()
            n_total = len(results)
            print(f"\n  [OK] Anomalias detectadas: {n_anomalies} de {n_total} puntos ({n_anomalies/n_total*100:.2f}%)")
            
        except Exception as e:
            print(f"  [ERROR] Error detectando anomalias: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_results:
        print("\n[ERROR] No se generaron resultados")
        return
    
    # Combinar todos los resultados
    print(f"\n{'='*80}")
    print("COMBINANDO RESULTADOS")
    print(f"{'='*80}")
    
    combined_results = pd.concat(all_results, ignore_index=True)
    
    # Crear directorio de resultados
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Guardar resultados completos
    results_file = results_dir / f"anomalies_detected_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    combined_results.to_csv(results_file, index=False)
    print(f"\n[OK] Resultados guardados en: {results_file}")
    
    # Generar resumen
    summary = detector.get_anomaly_summary(combined_results)
    summary_file = results_dir / f"anomaly_summary_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    summary.to_csv(summary_file, index=True)
    print(f"[OK] Resumen guardado en: {summary_file}")
    
    # Mostrar resumen en consola
    print(f"\n{'='*80}")
    print("RESUMEN DE ANOMALÍAS")
    print(f"{'='*80}")
    print(f"\nTotal de puntos analizados: {len(combined_results):,}")
    print(f"Total de anomalías detectadas: {combined_results['is_anomaly'].sum():,}")
    print(f"Tasa de anomalías: {combined_results['is_anomaly'].mean()*100:.2f}%")
    
    print(f"\nTop 10 variables con más anomalías:")
    print(summary.head(10).to_string())
    
    # Anomalías por fecha
    anomalies_by_date = combined_results[combined_results['is_anomaly']].groupby(
        pd.to_datetime(combined_results[combined_results['is_anomaly']]['ds']).dt.date
    ).size().sort_values(ascending=False)
    
    print(f"\nTop 10 fechas con más anomalías:")
    for date, count in anomalies_by_date.head(10).items():
        print(f"  {date}: {count} anomalías")
    
    # Guardar solo anomalías
    anomalies_only = combined_results[combined_results['is_anomaly']].copy()
    if len(anomalies_only) > 0:
        anomalies_file = results_dir / f"anomalies_only_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        anomalies_only.to_csv(anomalies_file, index=False)
        print(f"\n[OK] Solo anomalias guardadas en: {anomalies_file}")
        print(f"  Total: {len(anomalies_only)} registros anomalos")
    
    print(f"\n{'='*80}")
    print("PROCESO COMPLETADO")
    print(f"{'='*80}")
    print(f"\nArchivos generados en: {results_dir}")
    print(f"  - Resultados completos")
    print(f"  - Resumen por variable")
    print(f"  - Solo anomalías")


if __name__ == '__main__':
    main()

