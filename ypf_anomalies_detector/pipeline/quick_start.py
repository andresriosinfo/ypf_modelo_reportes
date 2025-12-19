"""
Script de inicio rápido para el pipeline completo

Este script ejecuta todo el flujo:
1. Selección y limpieza de variables (si no existe)
2. Entrenamiento de modelos Prophet
3. Detección de anomalías
"""

import sys
from pathlib import Path
import subprocess

def run_step(script_name, description):
    """Ejecuta un paso del pipeline"""
    print("\n" + "="*80)
    print(description)
    print("="*80)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error ejecutando {script_name}")
        return False
    except KeyboardInterrupt:
        print(f"\n⚠️  Interrumpido por el usuario")
        return False

def main():
    print("="*80)
    print("PIPELINE COMPLETO DE DETECCIÓN DE ANOMALÍAS")
    print("="*80)
    
    # Verificar que existan datos limpios
    output_dir = Path("output")
    cleaned_files = list(output_dir.glob("*_cleaned.csv")) if output_dir.exists() else []
    
    if not cleaned_files:
        print("\n⚠️  No se encontraron datos limpios.")
        print("   Ejecutando protocolo de selección de variables...")
        
        if not run_step("run_protocol.py", "PASO 1: Selección y Limpieza de Variables"):
            print("\n❌ Falló la selección de variables. Revisa los errores.")
            return
        
        # Verificar nuevamente
        cleaned_files = list(output_dir.glob("*_cleaned.csv")) if output_dir.exists() else []
        if not cleaned_files:
            print("\n❌ No se generaron datos limpios. Revisa los errores.")
            return
    
    print(f"\n✓ Datos limpios encontrados: {len(cleaned_files)} archivos")
    
    # Paso 2: Entrenar modelos
    print("\n" + "="*80)
    print("PASO 2: Entrenamiento de Modelos Prophet")
    print("="*80)
    
    response = input("\n¿Entrenar modelos Prophet? (s/n): ").strip().lower()
    if response in ['s', 'si', 'sí', 'y', 'yes']:
        if not run_step("pipeline/scripts/train_anomaly_detector.py", "Entrenando Modelos"):
            print("\n⚠️  Falló el entrenamiento. Puedes continuar con modelos existentes si los hay.")
    else:
        print("   Saltando entrenamiento...")
    
    # Paso 3: Detectar anomalías
    print("\n" + "="*80)
    print("PASO 3: Detección de Anomalías")
    print("="*80)
    
    response = input("\n¿Detectar anomalías? (s/n): ").strip().lower()
    if response in ['s', 'si', 'sí', 'y', 'yes']:
        if not run_step("pipeline/scripts/detect_anomalies.py", "Detectando Anomalías"):
            print("\n❌ Falló la detección de anomalías.")
            return
    else:
        print("   Saltando detección...")
    
    # Resumen final
    print("\n" + "="*80)
    print("PIPELINE COMPLETADO")
    print("="*80)
    
    results_dir = Path("pipeline/results")
    if results_dir.exists():
        result_files = list(results_dir.glob("*.csv"))
        if result_files:
            print(f"\n✓ Resultados generados: {len(result_files)} archivos")
            print(f"  Ubicación: {results_dir}")
            for f in result_files[-3:]:  # Mostrar últimos 3
                print(f"    - {f.name}")
    
    models_dir = Path("pipeline/models/prophet")
    if models_dir.exists():
        model_files = list(models_dir.glob("*.pkl"))
        if model_files:
            print(f"\n✓ Modelos disponibles: {len(model_files)}")
            print(f"  Ubicación: {models_dir}")
    
    print("\n" + "="*80)
    print("PRÓXIMOS PASOS")
    print("="*80)
    print("1. Revisa los resultados en pipeline/results/")
    print("2. Analiza el archivo anomaly_summary_*.csv para identificar variables problemáticas")
    print("3. Valida las anomalías detectadas con expertos del dominio")
    print("4. Ajusta parámetros si es necesario (interval_width, anomaly_threshold)")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrumpido por el usuario")

