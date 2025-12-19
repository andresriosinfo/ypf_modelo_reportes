"""
Script de ejemplo para ejecutar el protocolo de selección y limpieza de variables
Versión adaptada para datos de Argentina
"""

from variable_selection_protocol import AnomalyDetectionVariableProtocol
from pathlib import Path
import pandas as pd

def main():
    print("="*80)
    print("PROTOCOLO DE SELECCIÓN Y LIMPIEZA DE VARIABLES PARA DETECCIÓN DE ANOMALÍAS")
    print("VERSIÓN ARGENTINA")
    print("="*80)
    
    # Configurar el protocolo
    protocol = AnomalyDetectionVariableProtocol(
        min_suitability_score=60.0,    # Score mínimo de adecuación (0-100)
        max_missing_pct=0.3,           # Máximo 30% de valores faltantes
        min_cv=0.01,                   # Coeficiente de variación mínimo
        handle_missing='forward_fill', # Método: forward_fill, backward_fill, interpolate
        remove_outliers=True           # Eliminar outliers extremos
    )
    
    # Archivo preprocesado
    file_path = 'datos/preprocesados/datos_proceso_N101.csv'
    
    if not Path(file_path).exists():
        print(f"\n[ERROR] Archivo no encontrado: {file_path}")
        print("   Ejecuta primero: python preprocesar_datos.py")
        return
    
    print(f"\n[INFO] Procesando archivo: {file_path}")
    
    # Procesar archivo
    cleaned_data, report = protocol.process_file(
        file_path,
        datetime_col='DATETIME',
        save_report=True,
        output_dir='output'
    )
    
    # Resumen final
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    
    print(f"\nArchivo procesado: {Path(file_path).name}")
    print(f"  - Variables seleccionadas: {len(protocol.selected_variables)}")
    print(f"  - Filas en datos limpios: {len(cleaned_data)}")
    
    if len(protocol.selected_variables) > 0:
        print(f"\nVariables seleccionadas:")
        for var in protocol.selected_variables[:20]:
            print(f"  - {var}")
        if len(protocol.selected_variables) > 20:
            print(f"  ... y {len(protocol.selected_variables) - 20} más")
    
    print("\n" + "="*80)
    print("PROCESO COMPLETADO")
    print("="*80)
    print("\nArchivos generados en el directorio 'output/':")
    print("  - [archivo]_cleaned.csv: Datos limpios y listos para ML")
    print("  - [archivo]_variable_selection_report.csv: Reporte detallado de selección")


if __name__ == '__main__':
    main()

