"""
Script para evaluar el rendimiento del detector de anomalías
"""

import pandas as pd
import numpy as np
from pathlib import Path

def main():
    print("="*80)
    print("EVALUACION DEL DETECTOR DE ANOMALIAS CON PROPHET")
    print("="*80)
    
    results_dir = Path("pipeline/results")
    
    # Encontrar archivos más recientes
    summary_files = sorted(results_dir.glob("anomaly_summary_*.csv"), reverse=True)
    anomalies_files = sorted(results_dir.glob("anomalies_only_*.csv"), reverse=True)
    full_results_files = sorted(results_dir.glob("anomalies_detected_*.csv"), reverse=True)
    
    if not summary_files:
        print("\n[ERROR] No se encontraron archivos de resultados")
        return
    
    # Cargar resumen
    summary = pd.read_csv(summary_files[0], index_col=0)
    print(f"\n[INFO] Analizando resultados de: {summary_files[0].name}")
    
    # Estadísticas generales
    print("\n" + "="*80)
    print("ESTADISTICAS GENERALES")
    print("="*80)
    
    total_points = summary['n_points'].sum()
    total_anomalies = summary['n_anomalies'].sum()
    avg_rate = (total_anomalies / total_points * 100) if total_points > 0 else 0
    
    print(f"\nTotal de puntos analizados: {total_points:,}")
    print(f"Total de anomalias detectadas: {total_anomalies:,}")
    print(f"Tasa promedio de anomalias: {avg_rate:.2f}%")
    print(f"Numero de variables analizadas: {len(summary)}")
    
    # Análisis por variable
    print("\n" + "="*80)
    print("ANALISIS POR VARIABLE")
    print("="*80)
    
    # Variables con más anomalías
    print("\nTop 10 variables con MAS anomalias:")
    top_anomalies = summary.nlargest(10, 'n_anomalies')
    for idx, row in top_anomalies.iterrows():
        print(f"  {idx:30s} | {int(row['n_anomalies']):5d} anomalias | {row['anomaly_rate']*100:5.2f}% | Score avg: {row['avg_score']:.1f}")
    
    # Variables con menos anomalías (más estables)
    print("\nTop 10 variables con MENOS anomalias (mas estables):")
    stable = summary[summary['n_anomalies'] > 0].nsmallest(10, 'n_anomalies')
    for idx, row in stable.iterrows():
        print(f"  {idx:30s} | {int(row['n_anomalies']):5d} anomalias | {row['anomaly_rate']*100:5.2f}%")
    
    # Variables con scores altos (anomalías más severas)
    print("\nTop 10 variables con scores MAS ALTOS (anomalias mas severas):")
    high_scores = summary.nlargest(10, 'max_score')
    for idx, row in high_scores.iterrows():
        print(f"  {idx:30s} | Score max: {row['max_score']:.1f} | {int(row['n_anomalies']):5d} anomalias")
    
    # Análisis de distribución
    print("\n" + "="*80)
    print("DISTRIBUCION DE ANOMALIAS")
    print("="*80)
    
    print(f"\nTasa de anomalias por variable:")
    print(f"  Minimo: {summary['anomaly_rate'].min()*100:.2f}%")
    print(f"  Maximo: {summary['anomaly_rate'].max()*100:.2f}%")
    print(f"  Promedio: {summary['anomaly_rate'].mean()*100:.2f}%")
    print(f"  Mediana: {summary['anomaly_rate'].median()*100:.2f}%")
    print(f"  Desviacion estandar: {summary['anomaly_rate'].std()*100:.2f}%")
    
    # Categorización de variables
    print("\n" + "="*80)
    print("CATEGORIZACION DE VARIABLES")
    print("="*80)
    
    # Variables críticas (alta tasa de anomalías)
    critical = summary[summary['anomaly_rate'] > 0.10]
    print(f"\nVariables CRITICAS (tasa > 10%): {len(critical)}")
    if len(critical) > 0:
        print("  Estas variables requieren atencion inmediata:")
        for idx in critical.index[:10]:
            print(f"    - {idx}")
    
    # Variables normales (baja tasa)
    normal = summary[summary['anomaly_rate'] < 0.02]
    print(f"\nVariables NORMALES (tasa < 2%): {len(normal)}")
    
    # Variables moderadas
    moderate = summary[(summary['anomaly_rate'] >= 0.02) & (summary['anomaly_rate'] <= 0.10)]
    print(f"Variables MODERADAS (2% <= tasa <= 10%): {len(moderate)}")
    
    # Evaluación del detector
    print("\n" + "="*80)
    print("EVALUACION DEL DETECTOR")
    print("="*80)
    
    print("\n[CRITERIOS DE CALIDAD]")
    
    # 1. Cobertura
    variables_with_anomalies = (summary['n_anomalies'] > 0).sum()
    coverage = (variables_with_anomalies / len(summary) * 100) if len(summary) > 0 else 0
    print(f"  1. Cobertura: {variables_with_anomalies}/{len(summary)} variables tienen anomalias ({coverage:.1f}%)")
    
    # 2. Tasa de detección (no debería ser ni muy alta ni muy baja)
    if 2.0 <= avg_rate <= 10.0:
        print(f"  2. Tasa de deteccion: {avg_rate:.2f}% - EXCELENTE (rango ideal: 2-10%)")
    elif avg_rate < 2.0:
        print(f"  2. Tasa de deteccion: {avg_rate:.2f}% - BAJA (puede estar perdiendo anomalias)")
    else:
        print(f"  2. Tasa de deteccion: {avg_rate:.2f}% - ALTA (puede tener falsos positivos)")
    
    # 3. Variabilidad en detección
    std_rate = summary['anomaly_rate'].std() * 100
    if std_rate > 5.0:
        print(f"  3. Variabilidad: {std_rate:.2f}% - ALTA (deteccion muy variable entre variables)")
    else:
        print(f"  3. Variabilidad: {std_rate:.2f}% - MODERADA (deteccion consistente)")
    
    # 4. Variables problemáticas identificadas
    print(f"  4. Variables criticas identificadas: {len(critical)}")
    
    # Recomendaciones
    print("\n" + "="*80)
    print("RECOMENDACIONES")
    print("="*80)
    
    print("\n1. VARIABLES PRIORITARIAS PARA REVISION:")
    if len(critical) > 0:
        for idx in critical.index[:5]:
            row = summary.loc[idx]
            print(f"   - {idx}: {row['anomaly_rate']*100:.1f}% de anomalias, Score max: {row['max_score']:.1f}")
    
    print("\n2. AJUSTES SUGERIDOS:")
    if avg_rate < 2.0:
        print("   - Considera reducir 'anomaly_threshold' o 'interval_width' para detectar mas anomalias")
    elif avg_rate > 10.0:
        print("   - Considera aumentar 'anomaly_threshold' o 'interval_width' para reducir falsos positivos")
    else:
        print("   - La tasa de deteccion esta en un rango adecuado")
    
    print("\n3. VALIDACION:")
    print("   - Revisa manualmente las anomalias de las variables criticas")
    print("   - Valida con expertos del dominio si las anomalias son reales")
    print("   - Ajusta parametros segun feedback")
    
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    print(f"\nEl detector Prophet funciono EXITOSAMENTE:")
    print(f"  - Analizo {len(summary)} variables")
    print(f"  - Detecto {total_anomalies:,} anomalias en {total_points:,} puntos")
    print(f"  - Tasa de deteccion: {avg_rate:.2f}%")
    print(f"  - Identifico {len(critical)} variables criticas")
    
    if avg_rate >= 2.0 and avg_rate <= 10.0:
        print(f"\n[VEREDICTO] El detector esta funcionando BIEN con las variables seleccionadas.")
        print(f"           La tasa de deteccion esta en el rango ideal para procesos industriales.")
    else:
        print(f"\n[VEREDICTO] El detector funciona pero puede necesitar ajustes de parametros.")

if __name__ == '__main__':
    main()

