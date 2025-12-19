"""
Script de ejemplo para ejecutar el protocolo de selección y limpieza de variables
"""

from variable_selection_protocol import AnomalyDetectionVariableProtocol
from pathlib import Path
import pandas as pd

def main():
    print("="*80)
    print("PROTOCOLO DE SELECCIÓN Y LIMPIEZA DE VARIABLES PARA DETECCIÓN DE ANOMALÍAS")
    print("="*80)
    
    # Configurar el protocolo
    protocol = AnomalyDetectionVariableProtocol(
        min_suitability_score=60.0,    # Score mínimo de adecuación (0-100)
        max_missing_pct=0.3,           # Máximo 30% de valores faltantes
        min_cv=0.01,                   # Coeficiente de variación mínimo
        handle_missing='forward_fill', # Método: forward_fill, backward_fill, interpolate
        remove_outliers=True           # Eliminar outliers extremos
    )
    
    # Archivos a procesar (puedes modificar esta lista)
    files_to_process = [
        '2025-01.xlsx',
        '2024-12.xlsx',
        '2024-11.xlsx'
    ]
    
    # Filtrar archivos que existen
    existing_files = [f for f in files_to_process if Path(f).exists()]
    
    if not existing_files:
        print("\nNo se encontraron archivos para procesar.")
        print("Asegúrate de que los archivos .xlsx estén en el directorio actual.")
        return
    
    print(f"\nArchivos encontrados: {len(existing_files)}")
    for f in existing_files:
        print(f"  - {f}")
    
    # Procesar archivos
    results = protocol.process_multiple_files(
        existing_files,
        datetime_col='DATETIME',
        output_dir='output'
    )
    
    # Resumen final
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    
    # Analizar variables comunes
    all_vars_sets = []
    for file_path, (cleaned_df, report_df) in results.items():
        selected_vars = report_df[report_df['Selected']]['Variable'].tolist()
        all_vars_sets.append(set(selected_vars))
        print(f"\n{Path(file_path).name}:")
        print(f"  - Variables seleccionadas: {len(selected_vars)}")
        print(f"  - Filas en datos limpios: {len(cleaned_df)}")
    
    # Variables comunes
    if len(all_vars_sets) > 1:
        common_vars = set.intersection(*all_vars_sets)
        print(f"\n{'='*80}")
        print(f"VARIABLES COMUNES EN TODOS LOS ARCHIVOS: {len(common_vars)}")
        print(f"{'='*80}")
        
        if common_vars:
            # Categorizar variables comunes
            categories = {
                'Temperaturas (TE, TI, TIT)': [v for v in common_vars if any(v.startswith(pref) for pref in ['TE_', 'TI_', 'TIT_'])],
                'Presiones (PI, PIT)': [v for v in common_vars if any(v.startswith(pref) for pref in ['PI_', 'PIT_'])],
                'Flujos (FI, FIC)': [v for v in common_vars if any(v.startswith(pref) for pref in ['FI_', 'FIC_'])],
                'Niveles (LI, LIT)': [v for v in common_vars if any(v.startswith(pref) for pref in ['LI_', 'LIT_'])],
                'Control (BPC, SIC)': [v for v in common_vars if any(v.startswith(pref) for pref in ['BPC_', 'SIC_'])],
                'Proceso (BB, BL)': [v for v in common_vars if any(v.startswith(pref) for pref in ['BB_', 'BL_'])],
                'Otras': [v for v in common_vars if not any(v.startswith(pref) for pref in ['TE_', 'TI_', 'TIT_', 'PI_', 'PIT_', 'FI_', 'FIC_', 'LI_', 'LIT_', 'BPC_', 'SIC_', 'BB_', 'BL_'])]
            }
            
            for category, vars_list in categories.items():
                if vars_list:
                    print(f"\n{category}: {len(vars_list)} variables")
                    for var in sorted(vars_list)[:10]:
                        print(f"  - {var}")
                    if len(vars_list) > 10:
                        print(f"  ... y {len(vars_list) - 10} más")
            
            # Guardar lista de variables recomendadas
            recommended_vars_path = Path('output') / 'recommended_variables.txt'
            with open(recommended_vars_path, 'w', encoding='utf-8') as f:
                f.write("VARIABLES RECOMENDADAS PARA DETECCIÓN DE ANOMALÍAS\n")
                f.write("="*80 + "\n\n")
                f.write(f"Total de variables: {len(common_vars)}\n\n")
                for category, vars_list in categories.items():
                    if vars_list:
                        f.write(f"\n{category} ({len(vars_list)} variables):\n")
                        for var in sorted(vars_list):
                            f.write(f"  - {var}\n")
            
            print(f"\n\nLista completa de variables recomendadas guardada en: {recommended_vars_path}")
    
    print("\n" + "="*80)
    print("PROCESO COMPLETADO")
    print("="*80)
    print("\nArchivos generados en el directorio 'output/':")
    print("  - [archivo]_cleaned.csv: Datos limpios y listos para ML")
    print("  - [archivo]_variable_selection_report.csv: Reporte detallado de selección")
    print("  - recommended_variables.txt: Lista de variables recomendadas")


if __name__ == '__main__':
    main()

