"""
Script para evaluar las métricas de rendimiento del modelo de detección de anomalías
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import json
from datetime import datetime

# Agregar el directorio al path
sys.path.append(str(Path(__file__).parent))

from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector


def calculate_prediction_metrics(y_true, y_pred):
    """Calcula métricas de calidad de predicción"""
    # Filtrar valores válidos
    mask = ~(np.isnan(y_true) | np.isnan(y_pred) | np.isinf(y_true) | np.isinf(y_pred))
    y_true_clean = y_true[mask]
    y_pred_clean = y_pred[mask]
    
    if len(y_true_clean) == 0:
        return {
            'mae': np.nan,
            'rmse': np.nan,
            'mape': np.nan,
            'r2': np.nan,
            'n_points': 0
        }
    
    # MAE (Mean Absolute Error)
    mae = mean_absolute_error(y_true_clean, y_pred_clean)
    
    # RMSE (Root Mean Squared Error)
    rmse = np.sqrt(mean_squared_error(y_true_clean, y_pred_clean))
    
    # MAPE (Mean Absolute Percentage Error)
    # Evitar división por cero
    mask_nonzero = np.abs(y_true_clean) > 1e-10
    if mask_nonzero.sum() > 0:
        mape = np.mean(np.abs((y_true_clean[mask_nonzero] - y_pred_clean[mask_nonzero]) / y_true_clean[mask_nonzero])) * 100
    else:
        mape = np.nan
    
    # R² (Coeficiente de determinación)
    r2 = r2_score(y_true_clean, y_pred_clean)
    
    return {
        'mae': mae,
        'rmse': rmse,
        'mape': mape,
        'r2': r2,
        'n_points': len(y_true_clean)
    }


def calculate_interval_coverage(y_true, y_lower, y_upper):
    """Calcula la cobertura del intervalo de confianza"""
    mask = ~(np.isnan(y_true) | np.isnan(y_lower) | np.isnan(y_upper) | 
             np.isinf(y_true) | np.isinf(y_lower) | np.isinf(y_upper))
    
    y_true_clean = y_true[mask]
    y_lower_clean = y_lower[mask]
    y_upper_clean = y_upper[mask]
    
    if len(y_true_clean) == 0:
        return {'coverage': np.nan, 'n_points': 0}
    
    # Calcular cuántos valores están dentro del intervalo
    inside_interval = (y_true_clean >= y_lower_clean) & (y_true_clean <= y_upper_clean)
    coverage = inside_interval.mean() * 100
    
    return {
        'coverage': coverage,
        'n_points': len(y_true_clean),
        'n_inside': inside_interval.sum(),
        'n_outside': (~inside_interval).sum()
    }


def calculate_residual_stats(residuals):
    """Calcula estadísticas de los residuales"""
    residuals_clean = residuals[~(np.isnan(residuals) | np.isinf(residuals))]
    
    if len(residuals_clean) == 0:
        return {
            'mean': np.nan,
            'std': np.nan,
            'median': np.nan,
            'q25': np.nan,
            'q75': np.nan,
            'min': np.nan,
            'max': np.nan,
            'n_points': 0
        }
    
    return {
        'mean': residuals_clean.mean(),
        'std': residuals_clean.std(),
        'median': np.median(residuals_clean),
        'q25': np.percentile(residuals_clean, 25),
        'q75': np.percentile(residuals_clean, 75),
        'min': residuals_clean.min(),
        'max': residuals_clean.max(),
        'n_points': len(residuals_clean)
    }


def evaluate_model(data_dir='output', models_dir='pipeline/models/prophet', results_dir='pipeline/results'):
    """Evalúa el modelo de detección de anomalías y calcula todas las métricas"""
    
    print("="*80)
    print("EVALUACIÓN DE MÉTRICAS DEL MODELO DE DETECCIÓN DE ANOMALÍAS")
    print("="*80)
    
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
    data_dir = Path(data_dir)
    cleaned_files = list(data_dir.glob("*_cleaned.csv"))
    
    if not cleaned_files:
        print(f"\n[ERROR] No se encontraron archivos de datos en {data_dir}")
        return
    
    # Usar el archivo más reciente
    data_file = cleaned_files[0]
    print(f"\n[INFO] Cargando datos de: {data_file.name}")
    df = pd.read_csv(data_file, parse_dates=['DATETIME'])
    
    # Obtener variables disponibles
    available_vars = [v for v in detector.models.keys() if v in df.columns]
    print(f"[INFO] Variables a evaluar: {len(available_vars)}")
    
    # Detectar anomalías para obtener predicciones
    print(f"\n[INFO] Generando predicciones y detectando anomalías...")
    results = detector.detect_anomalies_multiple(
        df=df,
        variables=available_vars,
        datetime_col='DATETIME',
        combine_results=True
    )
    
    print(f"[OK] {len(results)} puntos evaluados")
    
    # Calcular métricas por variable
    print(f"\n{'='*80}")
    print("CALCULANDO MÉTRICAS POR VARIABLE")
    print(f"{'='*80}")
    
    metrics_by_variable = []
    
    for var in available_vars:
        var_results = results[results['variable'] == var].copy()
        
        if len(var_results) == 0:
            continue
        
        # Métricas de predicción
        pred_metrics = calculate_prediction_metrics(
            var_results['y'].values,
            var_results['yhat'].values
        )
        
        # Cobertura del intervalo
        interval_metrics = calculate_interval_coverage(
            var_results['y'].values,
            var_results['yhat_lower'].values,
            var_results['yhat_upper'].values
        )
        
        # Estadísticas de residuales
        residual_stats = calculate_residual_stats(var_results['residual'].values)
        
        # Métricas de anomalías
        n_anomalies = var_results['is_anomaly'].sum()
        anomaly_rate = n_anomalies / len(var_results) * 100
        avg_anomaly_score = var_results[var_results['is_anomaly']]['anomaly_score'].mean() if n_anomalies > 0 else 0
        max_anomaly_score = var_results['anomaly_score'].max()
        
        # Combinar todas las métricas
        var_metrics = {
            'variable': var,
            'n_points': len(var_results),
            # Métricas de predicción
            'mae': pred_metrics['mae'],
            'rmse': pred_metrics['rmse'],
            'mape': pred_metrics['mape'],
            'r2': pred_metrics['r2'],
            # Cobertura del intervalo
            'interval_coverage_pct': interval_metrics['coverage'],
            'n_outside_interval': interval_metrics['n_outside'],
            # Estadísticas de residuales
            'residual_mean': residual_stats['mean'],
            'residual_std': residual_stats['std'],
            'residual_median': residual_stats['median'],
            # Métricas de anomalías
            'n_anomalies': n_anomalies,
            'anomaly_rate_pct': anomaly_rate,
            'avg_anomaly_score': avg_anomaly_score,
            'max_anomaly_score': max_anomaly_score,
        }
        
        metrics_by_variable.append(var_metrics)
    
    # Crear DataFrame con métricas
    metrics_df = pd.DataFrame(metrics_by_variable)
    
    # Guardar métricas
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = results_dir / f"model_metrics_{timestamp}.csv"
    metrics_df.to_csv(metrics_file, index=False)
    print(f"\n[OK] Métricas guardadas en: {metrics_file}")
    
    # Mostrar resumen general
    print(f"\n{'='*80}")
    print("RESUMEN GENERAL DE MÉTRICAS")
    print(f"{'='*80}")
    
    print(f"\n📊 MÉTRICAS DE CALIDAD DE PREDICCIÓN:")
    print(f"  • MAE promedio: {metrics_df['mae'].mean():.4f}")
    print(f"  • RMSE promedio: {metrics_df['rmse'].mean():.4f}")
    print(f"  • MAPE promedio: {metrics_df['mape'].mean():.2f}%")
    print(f"  • R² promedio: {metrics_df['r2'].mean():.4f}")
    print(f"  • R² mediano: {metrics_df['r2'].median():.4f}")
    
    print(f"\n📈 COBERTURA DEL INTERVALO DE CONFIANZA:")
    print(f"  • Cobertura promedio: {metrics_df['interval_coverage_pct'].mean():.2f}%")
    print(f"  • Cobertura esperada: 95.0% (basado en interval_width=0.95)")
    coverage_diff = metrics_df['interval_coverage_pct'].mean() - 95.0
    print(f"  • Diferencia: {coverage_diff:+.2f}%")
    
    print(f"\n📉 ESTADÍSTICAS DE RESIDUALES:")
    print(f"  • Media promedio: {metrics_df['residual_mean'].mean():.6f}")
    print(f"  • Desviación estándar promedio: {metrics_df['residual_std'].mean():.4f}")
    print(f"  • Mediana promedio: {metrics_df['residual_median'].mean():.6f}")
    
    print(f"\n🚨 MÉTRICAS DE DETECCIÓN DE ANOMALÍAS:")
    total_points = metrics_df['n_points'].sum()
    total_anomalies = metrics_df['n_anomalies'].sum()
    print(f"  • Total de puntos analizados: {total_points:,}")
    print(f"  • Total de anomalías detectadas: {total_anomalies:,}")
    print(f"  • Tasa global de anomalías: {total_anomalies/total_points*100:.2f}%")
    print(f"  • Score promedio de anomalías: {results[results['is_anomaly']]['anomaly_score'].mean():.2f}")
    print(f"  • Score máximo de anomalías: {results['anomaly_score'].max():.2f}")
    
    # Top variables por diferentes métricas
    print(f"\n{'='*80}")
    print("TOP 10 VARIABLES POR DIFERENTES MÉTRICAS")
    print(f"{'='*80}")
    
    print(f"\n🏆 Mejor R² (mejor ajuste del modelo):")
    top_r2 = metrics_df.nlargest(10, 'r2')[['variable', 'r2', 'mae', 'rmse']]
    print(top_r2.to_string(index=False))
    
    print(f"\n🎯 Menor MAE (mejor precisión):")
    top_mae = metrics_df.nsmallest(10, 'mae')[['variable', 'mae', 'r2', 'mape']]
    print(top_mae.to_string(index=False))
    
    print(f"\n📊 Mejor cobertura del intervalo (más cercano a 95%):")
    metrics_df['coverage_diff'] = np.abs(metrics_df['interval_coverage_pct'] - 95.0)
    top_coverage = metrics_df.nsmallest(10, 'coverage_diff')[['variable', 'interval_coverage_pct', 'coverage_diff']]
    print(top_coverage.to_string(index=False))
    
    print(f"\n🚨 Más anomalías detectadas:")
    top_anomalies = metrics_df.nlargest(10, 'n_anomalies')[['variable', 'n_anomalies', 'anomaly_rate_pct', 'avg_anomaly_score']]
    print(top_anomalies.to_string(index=False))
    
    # Distribución de R²
    print(f"\n{'='*80}")
    print("DISTRIBUCIÓN DE R² (Calidad del Ajuste)")
    print(f"{'='*80}")
    r2_ranges = [
        (0.95, 1.0, "Excelente (0.95-1.0)"),
        (0.90, 0.95, "Muy bueno (0.90-0.95)"),
        (0.80, 0.90, "Bueno (0.80-0.90)"),
        (0.70, 0.80, "Aceptable (0.70-0.80)"),
        (0.50, 0.70, "Regular (0.50-0.70)"),
        (0.0, 0.50, "Bajo (<0.50)"),
        (-np.inf, 0.0, "Muy bajo (<0.0)")
    ]
    
    for min_r2, max_r2, label in r2_ranges:
        count = ((metrics_df['r2'] >= min_r2) & (metrics_df['r2'] < max_r2)).sum()
        pct = count / len(metrics_df) * 100
        print(f"  {label}: {count} variables ({pct:.1f}%)")
    
    # Interpretación de métricas
    print(f"\n{'='*80}")
    print("INTERPRETACIÓN DE MÉTRICAS")
    print(f"{'='*80}")
    print("""
📊 MÉTRICAS DE PREDICCIÓN:
  • MAE (Mean Absolute Error): Error promedio absoluto. Menor es mejor.
  • RMSE (Root Mean Squared Error): Error cuadrático medio. Penaliza errores grandes. Menor es mejor.
  • MAPE (Mean Absolute Percentage Error): Error porcentual promedio. Menor es mejor.
  • R² (Coeficiente de determinación): Proporción de varianza explicada. 
    - R² = 1.0: Modelo perfecto
    - R² > 0.9: Excelente
    - R² > 0.8: Bueno
    - R² > 0.7: Aceptable
    - R² < 0.5: Modelo pobre

📈 COBERTURA DEL INTERVALO:
  • Debe estar cerca del 95% si interval_width=0.95
  • Si es mucho menor: el modelo es demasiado conservador
  • Si es mucho mayor: el modelo es demasiado optimista

📉 RESIDUALES:
  • Media cercana a 0: el modelo no tiene sesgo sistemático
  • Desviación estándar pequeña: predicciones más precisas

🚨 DETECCIÓN DE ANOMALÍAS:
  • Tasa de anomalías: porcentaje de puntos marcados como anómalos
  • Score de anomalía: qué tan anómalo es un punto (0-100)
  • Una tasa muy alta (>10%) puede indicar umbral muy sensible
  • Una tasa muy baja (<1%) puede indicar umbral muy estricto
    """)
    
    # Guardar resumen en JSON
    summary = {
        'timestamp': timestamp,
        'n_variables': len(metrics_df),
        'total_points': int(total_points),
        'total_anomalies': int(total_anomalies),
        'global_anomaly_rate': float(total_anomalies/total_points*100),
        'avg_metrics': {
            'mae': float(metrics_df['mae'].mean()),
            'rmse': float(metrics_df['rmse'].mean()),
            'mape': float(metrics_df['mape'].mean()),
            'r2': float(metrics_df['r2'].mean()),
            'interval_coverage': float(metrics_df['interval_coverage_pct'].mean()),
            'residual_std': float(metrics_df['residual_std'].mean())
        },
        'median_metrics': {
            'mae': float(metrics_df['mae'].median()),
            'rmse': float(metrics_df['rmse'].median()),
            'mape': float(metrics_df['mape'].median()),
            'r2': float(metrics_df['r2'].median()),
            'interval_coverage': float(metrics_df['interval_coverage_pct'].median()),
            'residual_std': float(metrics_df['residual_std'].median())
        }
    }
    
    summary_file = results_dir / f"model_metrics_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n[OK] Resumen guardado en: {summary_file}")
    print(f"\n{'='*80}")
    print("EVALUACIÓN COMPLETADA")
    print(f"{'='*80}")


if __name__ == '__main__':
    evaluate_model()



