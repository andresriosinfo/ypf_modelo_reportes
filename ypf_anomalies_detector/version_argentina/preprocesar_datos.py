"""
Script para preprocesar los datos de Argentina
Convierte el formato transpuesto a formato estándar (fechas en filas, variables en columnas)
"""

import pandas as pd
import numpy as np
from pathlib import Path

def preprocesar_datos_argentina(input_file: str, output_file: str):
    """
    Preprocesa los datos de Argentina que vienen en formato transpuesto
    
    Parámetros:
    -----------
    input_file : str
        Ruta al archivo Excel original
    output_file : str
        Ruta donde guardar el archivo preprocesado
    """
    print("="*80)
    print("PREPROCESAMIENTO DE DATOS ARGENTINA")
    print("="*80)
    
    print(f"\n[INFO] Cargando archivo: {input_file}")
    
    # Cargar datos (formato transpuesto: variables en filas, fechas en columnas)
    df_transpuesto = pd.read_excel(input_file)
    
    print(f"  Dimensiones originales: {df_transpuesto.shape[0]} filas x {df_transpuesto.shape[1]} columnas")
    print(f"  Variables encontradas: {df_transpuesto.shape[0]}")
    print(f"  Puntos temporales: {df_transpuesto.shape[1] - 1}")
    
    # La primera columna contiene los nombres de las variables
    variable_col = df_transpuesto.columns[0]
    variables = df_transpuesto[variable_col].tolist()
    
    print(f"\n[INFO] Variables encontradas:")
    for i, var in enumerate(variables, 1):
        print(f"  {i}. {var}")
    
    # Transponer: convertir filas a columnas
    print(f"\n[INFO] Transponiendo datos...")
    
    # Crear DataFrame con fechas como índice
    df_transpuesto = df_transpuesto.set_index(variable_col)
    
    # Transponer
    df = df_transpuesto.T
    
    # Resetear índice para que las fechas sean una columna
    df = df.reset_index()
    
    # Renombrar la columna de fechas
    df.rename(columns={'index': 'DATETIME'}, inplace=True)
    
    # Convertir DATETIME a datetime si no lo es
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    
    # Ordenar por fecha
    df = df.sort_values('DATETIME').reset_index(drop=True)
    
    print(f"  Dimensiones finales: {df.shape[0]} filas x {df.shape[1]} columnas")
    print(f"  Rango temporal: {df['DATETIME'].min()} a {df['DATETIME'].max()}")
    
    # Verificar valores faltantes
    missing_pct = (df.isna().sum() / len(df) * 100).round(2)
    print(f"\n[INFO] Valores faltantes por variable:")
    for var in variables:
        if var in df.columns:
            pct = missing_pct[var]
            if pct > 0:
                print(f"  {var}: {pct}%")
    
    # Guardar archivo preprocesado
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_file, index=False)
    print(f"\n[OK] Datos preprocesados guardados en: {output_file}")
    
    return df

if __name__ == '__main__':
    input_file = 'datos/Datos de proceso N-101.xlsx'
    output_file = 'datos/preprocesados/datos_proceso_N101.csv'
    
    preprocesar_datos_argentina(input_file, output_file)

