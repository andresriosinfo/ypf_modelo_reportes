"""
Protocolo de Selección y Limpieza de Variables para Detección de Anomalías con ML
Versión adaptada para datos de Argentina
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')


class VariableSelector:
    """Clase para seleccionar variables adecuadas para detección de anomalías"""
    
    def __init__(self, 
                 min_suitability_score: float = 60.0,
                 max_missing_pct: float = 0.3,
                 min_cv: float = 0.01,
                 exclude_binary: bool = True,
                 exclude_constant: bool = True):
        """
        Parámetros:
        -----------
        min_suitability_score : float
            Score mínimo de adecuación (0-100)
        max_missing_pct : float
            Porcentaje máximo de valores faltantes permitido
        min_cv : float
            Coeficiente de variación mínimo (std/mean)
        exclude_binary : bool
            Excluir variables binarias puras (0/1)
        exclude_constant : bool
            Excluir variables constantes
        """
        self.min_suitability_score = min_suitability_score
        self.max_missing_pct = max_missing_pct
        self.min_cv = min_cv
        self.exclude_binary = exclude_binary
        self.exclude_constant = exclude_constant
        self.selected_variables = []
        self.variable_stats = {}
        
    def calculate_suitability_score(self, series: pd.Series, var_name: str) -> Dict:
        """Calcula el score de adecuación de una variable"""
        stats = {
            'name': var_name,
            'missing_pct': series.isna().sum() / len(series),
            'nunique': series.nunique(),
            'dtype': str(series.dtype),
        }
        
        # Criterios de exclusión inmediata
        if stats['missing_pct'] > self.max_missing_pct:
            stats['suitability_score'] = 0
            stats['exclusion_reason'] = f'Demasiados valores faltantes ({stats["missing_pct"]*100:.1f}%)'
            return stats
            
        if self.exclude_constant and stats['nunique'] <= 2:
            stats['suitability_score'] = 0
            stats['exclusion_reason'] = 'Variable constante o casi constante'
            return stats
        
        # Variables binarias puras
        if self.exclude_binary:
            unique_vals = series.dropna().unique()
            if len(unique_vals) == 2 and set(unique_vals).issubset({0, 1, 0.0, 1.0}):
                stats['suitability_score'] = 0
                stats['exclusion_reason'] = 'Variable binaria pura'
                return stats
        
        # Calcular estadísticas para variables numéricas
        if series.dtype in ['float64', 'int64']:
            stats['mean'] = series.mean()
            stats['std'] = series.std()
            stats['min'] = series.min()
            stats['max'] = series.max()
            stats['median'] = series.median()
            stats['q25'] = series.quantile(0.25)
            stats['q75'] = series.quantile(0.75)
            
            # Coeficiente de variación
            if stats['mean'] != 0:
                stats['cv'] = abs(stats['std'] / stats['mean'])
            else:
                stats['cv'] = None
                
            stats['zeros_pct'] = (series == 0).sum() / len(series)
            stats['outliers_iqr'] = self._count_outliers_iqr(series)
        else:
            stats['cv'] = None
            stats['zeros_pct'] = None
        
        # Calcular score de adecuación
        score = 100
        
        # Penalizar por valores faltantes
        score -= stats['missing_pct'] * 30
        
        # Penalizar por baja variabilidad
        if stats['cv'] is not None:
            if stats['cv'] < self.min_cv:
                score -= 30
            elif stats['cv'] < 0.05:
                score -= 15
            elif 0.05 <= stats['cv'] <= 2.0:  # Rango óptimo
                score += 10
        
        # Penalizar por demasiados ceros
        if stats.get('zeros_pct') and stats['zeros_pct'] > 0.8:
            score -= 20
        
        # Bonificar variables con buena distribución
        if stats.get('cv') and 0.1 <= stats['cv'] <= 1.0:
            score += 5
        
        stats['suitability_score'] = max(0, min(100, score))
        stats['exclusion_reason'] = None
        
        return stats
    
    def _count_outliers_iqr(self, series: pd.Series) -> int:
        """Cuenta outliers usando método IQR"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return ((series < lower_bound) | (series > upper_bound)).sum()
    
    def select_variables(self, df: pd.DataFrame, datetime_col: str = 'DATETIME') -> List[str]:
        """
        Selecciona variables adecuadas para detección de anomalías
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame con los datos
        datetime_col : str
            Nombre de la columna de fecha/hora (se excluirá)
        
        Retorna:
        --------
        List[str] : Lista de nombres de variables seleccionadas
        """
        self.variable_stats = {}
        
        for col in df.columns:
            if col == datetime_col:
                continue
                
            stats = self.calculate_suitability_score(df[col], col)
            self.variable_stats[col] = stats
            
            if stats['suitability_score'] >= self.min_suitability_score:
                self.selected_variables.append(col)
        
        return self.selected_variables
    
    def get_selection_report(self) -> pd.DataFrame:
        """Genera un reporte de las variables seleccionadas"""
        if not self.variable_stats:
            return pd.DataFrame()
        
        report_data = []
        for var_name, stats in self.variable_stats.items():
            report_data.append({
                'Variable': var_name,
                'Score': stats['suitability_score'],
                'Missing %': stats['missing_pct'] * 100,
                'Unique Values': stats['nunique'],
                'CV': stats.get('cv', None),
                'Mean': stats.get('mean', None),
                'Std': stats.get('std', None),
                'Selected': var_name in self.selected_variables,
                'Exclusion Reason': stats.get('exclusion_reason', '')
            })
        
        report_df = pd.DataFrame(report_data)
        report_df = report_df.sort_values('Score', ascending=False)
        return report_df


class DataCleaner:
    """Clase para limpiar y preprocesar datos"""
    
    def __init__(self, 
                 handle_missing: str = 'forward_fill',
                 remove_outliers: bool = True,
                 outlier_method: str = 'iqr',
                 normalize: bool = False):
        """
        Parámetros:
        -----------
        handle_missing : str
            Método para manejar valores faltantes: 'forward_fill', 'backward_fill', 'interpolate', 'drop'
        remove_outliers : bool
            Si eliminar outliers extremos
        outlier_method : str
            Método para detectar outliers: 'iqr', 'zscore'
        normalize : bool
            Si normalizar los datos (útil para algunos algoritmos ML)
        """
        self.handle_missing = handle_missing
        self.remove_outliers = remove_outliers
        self.outlier_method = outlier_method
        self.normalize = normalize
        self.scalers = {}
        
    def clean_data(self, 
                   df: pd.DataFrame, 
                   selected_vars: List[str],
                   datetime_col: str = 'DATETIME') -> pd.DataFrame:
        """
        Limpia los datos según el protocolo configurado
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame original
        selected_vars : List[str]
            Lista de variables seleccionadas
        datetime_col : str
            Nombre de la columna de fecha/hora
        
        Retorna:
        --------
        pd.DataFrame : DataFrame limpio
        """
        # Filtrar variables que realmente existen en el DataFrame
        available_vars = [v for v in selected_vars if v in df.columns]
        missing_vars = [v for v in selected_vars if v not in df.columns]
        
        if missing_vars:
            print(f"   Advertencia: {len(missing_vars)} variables seleccionadas no están en este archivo")
        
        # Crear DataFrame con variables seleccionadas + datetime
        cols_to_use = [datetime_col] + available_vars
        clean_df = df[cols_to_use].copy()
        
        # Ordenar por fecha
        if datetime_col in clean_df.columns:
            clean_df = clean_df.sort_values(datetime_col).reset_index(drop=True)
        
        # 1. Manejar valores faltantes
        clean_df = self._handle_missing_values(clean_df, available_vars)
        
        # 2. Manejar outliers (solo marcar, no eliminar filas completas)
        if self.remove_outliers:
            clean_df = self._handle_outliers(clean_df, available_vars)
        
        # 3. Normalización (opcional)
        if self.normalize:
            clean_df = self._normalize_data(clean_df, available_vars)
        
        return clean_df
    
    def _handle_missing_values(self, df: pd.DataFrame, vars: List[str]) -> pd.DataFrame:
        """Maneja valores faltantes según el método configurado"""
        df_clean = df.copy()
        
        for var in vars:
            if var not in df_clean.columns:
                continue
                
            if self.handle_missing == 'forward_fill':
                df_clean[var] = df_clean[var].ffill()
                df_clean[var] = df_clean[var].bfill()  # Si quedan al inicio
            elif self.handle_missing == 'backward_fill':
                df_clean[var] = df_clean[var].bfill()
                df_clean[var] = df_clean[var].ffill()  # Si quedan al final
            elif self.handle_missing == 'interpolate':
                df_clean[var] = df_clean[var].interpolate(method='linear')
                df_clean[var] = df_clean[var].ffill()
                df_clean[var] = df_clean[var].bfill()
            elif self.handle_missing == 'drop':
                df_clean = df_clean.dropna(subset=[var])
        
        return df_clean
    
    def _handle_outliers(self, df: pd.DataFrame, vars: List[str]) -> pd.DataFrame:
        """Marca outliers extremos (los reemplaza con NaN para luego imputar)"""
        df_clean = df.copy()
        
        for var in vars:
            if var not in df_clean.columns:
                continue
                
            if self.outlier_method == 'iqr':
                Q1_val = df_clean[var].quantile(0.25)
                Q3_val = df_clean[var].quantile(0.75)
                # Convertir a escalar si es necesario
                Q1 = float(Q1_val) if not isinstance(Q1_val, pd.Series) else float(Q1_val.iloc[0])
                Q3 = float(Q3_val) if not isinstance(Q3_val, pd.Series) else float(Q3_val.iloc[0])
                IQR = Q3 - Q1
                if IQR > 0:  # Solo procesar si hay variabilidad
                    lower_bound = Q1 - 3 * IQR  # Más estricto que 1.5*IQR
                    upper_bound = Q3 + 3 * IQR
                    
                    # Marcar outliers extremos
                    outliers_mask = (df_clean[var] < lower_bound) | (df_clean[var] > upper_bound)
                    num_outliers = np.sum(outliers_mask.values) if hasattr(outliers_mask, 'values') else np.sum(outliers_mask)
                    if num_outliers > 0:
                        # Reemplazar outliers con NaN usando índice booleano
                        df_clean[var] = df_clean[var].where(~outliers_mask, np.nan)
                        # Re-imputar los outliers marcados
                        df_clean[var] = df_clean[var].interpolate(method='linear')
                        df_clean[var] = df_clean[var].ffill()
                        df_clean[var] = df_clean[var].bfill()
                    
            elif self.outlier_method == 'zscore':
                mean_val_raw = df_clean[var].mean()
                std_val_raw = df_clean[var].std()
                # Convertir a escalar si es necesario
                mean_val = float(mean_val_raw) if not isinstance(mean_val_raw, pd.Series) else float(mean_val_raw.iloc[0])
                std_val = float(std_val_raw) if not isinstance(std_val_raw, pd.Series) else float(std_val_raw.iloc[0])
                if std_val > 0:  # Solo procesar si hay variabilidad
                    z_scores = np.abs((df_clean[var] - mean_val) / std_val)
                    outliers_mask = z_scores > 3  # 3 desviaciones estándar
                    num_outliers = np.sum(outliers_mask.values) if hasattr(outliers_mask, 'values') else np.sum(outliers_mask)
                    if num_outliers > 0:
                        # Reemplazar outliers con NaN usando índice booleano
                        df_clean[var] = df_clean[var].where(~outliers_mask, np.nan)
                        df_clean[var] = df_clean[var].interpolate(method='linear')
                        df_clean[var] = df_clean[var].ffill()
                        df_clean[var] = df_clean[var].bfill()
        
        return df_clean
    
    def _normalize_data(self, df: pd.DataFrame, vars: List[str]) -> pd.DataFrame:
        """Normaliza los datos usando StandardScaler"""
        from sklearn.preprocessing import StandardScaler
        
        df_clean = df.copy()
        
        for var in vars:
            if var not in df_clean.columns:
                continue
                
            scaler = StandardScaler()
            df_clean[var] = scaler.fit_transform(df_clean[[var]])
            self.scalers[var] = scaler
        
        return df_clean


class AnomalyDetectionVariableProtocol:
    """Protocolo completo para selección y limpieza de variables para detección de anomalías"""
    
    def __init__(self, 
                 min_suitability_score: float = 60.0,
                 max_missing_pct: float = 0.3,
                 min_cv: float = 0.01,
                 handle_missing: str = 'forward_fill',
                 remove_outliers: bool = True):
        """
        Inicializa el protocolo con parámetros configurables
        """
        self.selector = VariableSelector(
            min_suitability_score=min_suitability_score,
            max_missing_pct=max_missing_pct,
            min_cv=min_cv
        )
        self.cleaner = DataCleaner(
            handle_missing=handle_missing,
            remove_outliers=remove_outliers
        )
        self.selected_variables = []
        self.cleaned_data = None
        
    def process_file(self, 
                    file_path: str,
                    datetime_col: str = 'DATETIME',
                    save_report: bool = True,
                    output_dir: str = 'output') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Procesa un archivo CSV: selecciona variables y limpia datos
        
        Parámetros:
        -----------
        file_path : str
            Ruta al archivo CSV
        datetime_col : str
            Nombre de la columna de fecha/hora
        save_report : bool
            Si guardar reportes en archivos
        output_dir : str
            Directorio para guardar outputs
        
        Retorna:
        --------
        Tuple[pd.DataFrame, pd.DataFrame] : (datos_limpios, reporte_seleccion)
        """
        print(f"\n{'='*80}")
        print(f"Procesando archivo: {file_path}")
        print(f"{'='*80}")
        
        # 1. Cargar datos
        print("\n1. Cargando datos...")
        df = pd.read_csv(file_path, parse_dates=[datetime_col])
        print(f"   Dimensiones originales: {df.shape[0]} filas x {df.shape[1]} columnas")
        
        # 2. Seleccionar variables
        print("\n2. Seleccionando variables adecuadas para ML...")
        self.selected_variables = self.selector.select_variables(df, datetime_col)
        print(f"   Variables seleccionadas: {len(self.selected_variables)} de {len(df.columns)-1}")
        
        # 3. Generar reporte
        report = self.selector.get_selection_report()
        print(f"\n   Top 10 variables seleccionadas:")
        top_vars = report[report['Selected']].head(10)
        for idx, row in top_vars.iterrows():
            cv_str = f"{row['CV']:.4f}" if pd.notna(row['CV']) else 'N/A'
            print(f"      - {row['Variable']}: Score={row['Score']:.1f}, CV={cv_str}")
        
        # 4. Limpiar datos
        print("\n3. Limpiando datos...")
        self.cleaned_data = self.cleaner.clean_data(df, self.selected_variables, datetime_col)
        print(f"   Dimensiones finales: {self.cleaned_data.shape[0]} filas x {self.cleaned_data.shape[1]} columnas")
        print(f"   Valores faltantes restantes: {self.cleaned_data.isna().sum().sum()}")
        
        # 5. Guardar reportes si se solicita
        if save_report:
            Path(output_dir).mkdir(exist_ok=True, parents=True)
            file_stem = Path(file_path).stem
            
            # Guardar reporte completo
            report_path = Path(output_dir) / f"{file_stem}_variable_selection_report.csv"
            report.to_csv(report_path, index=False)
            print(f"\n   Reporte guardado en: {report_path}")
            
            # Guardar datos limpios
            cleaned_path = Path(output_dir) / f"{file_stem}_cleaned.csv"
            self.cleaned_data.to_csv(cleaned_path, index=False)
            print(f"   Datos limpios guardados en: {cleaned_path}")
        
        return self.cleaned_data, report


def main():
    """Función principal para ejecutar el protocolo"""
    import sys
    
    # Configuración del protocolo
    protocol = AnomalyDetectionVariableProtocol(
        min_suitability_score=60.0,  # Score mínimo
        max_missing_pct=0.3,          # Máximo 30% de valores faltantes
        min_cv=0.01,                  # Coeficiente de variación mínimo
        handle_missing='forward_fill', # Método de imputación
        remove_outliers=True          # Eliminar outliers extremos
    )
    
    # Archivo preprocesado
    file_path = 'datos/preprocesados/datos_proceso_N101.csv'
    
    if not Path(file_path).exists():
        print(f"[ERROR] Archivo no encontrado: {file_path}")
        print("   Ejecuta primero: python preprocesar_datos.py")
        return
    
    # Procesar archivo
    cleaned_data, report = protocol.process_file(file_path, output_dir='output')
    
    print("\n" + "="*80)
    print("PROTOCOLO COMPLETADO")
    print("="*80)
    print(f"\nVariables seleccionadas: {len(protocol.selected_variables)}")
    print("\nLos datos limpios y reportes se encuentran en el directorio 'output/'")


if __name__ == '__main__':
    main()

