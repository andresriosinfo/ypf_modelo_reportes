# preprocesamiento.py

import pandas as pd
import numpy as np


# =========================
# 1. Cargar archivo y validar estructura
# =========================
def cargar_datos_proceso(
    ruta: str,
    tag_col: str = "Tag de PI",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Carga el archivo Excel de datos de proceso en formato wide.
    Retorna df_raw.
    """
    df_raw = pd.read_excel(ruta)

    if verbose:
        print("Shape original (filas, columnas):", df_raw.shape)
        print(df_raw.head())
        print(df_raw.info())

    assert tag_col in df_raw.columns, f"No encuentro la columna de tags '{tag_col}', revisa el nombre."

    return df_raw


# =========================
# 2. Convertir wide → long
# =========================
def wide_a_long(
    df_raw: pd.DataFrame,
    tag_col: str = "Tag de PI",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Convierte un dataframe en formato wide (una fila por tag, columnas de tiempo)
    a formato long con columnas:
        - tag_col (ej. 'Tag de PI')
        - 'timestamp'
        - 'value'
    """
    # Todas las demás columnas son timestamps
    time_cols = [c for c in df_raw.columns if c != tag_col]

    if verbose:
        print("Nº de variables (tags):", df_raw.shape[0])
        print("Nº de columnas de tiempo:", len(time_cols))

    # Convertimos los nombres de las columnas de tiempo a datetime
    time_cols_dt = pd.to_datetime(time_cols)
    rename_map = dict(zip(time_cols, time_cols_dt))
    df = df_raw.rename(columns=rename_map)

    if verbose:
        print("Tipo de las columnas de tiempo:", type(list(df.columns[1:5])[0]))

    # Melt: wide -> long
    df_long = df.melt(
        id_vars=tag_col,
        var_name="timestamp",
        value_name="value"
    )

    # Asegurar que timestamp es datetime
    df_long["timestamp"] = pd.to_datetime(df_long["timestamp"])

    if verbose:
        print("Shape long:", df_long.shape)
        print(df_long.head())
        print(df_long.info())

    return df_long


# =========================
# 3. Diagnóstico de nulos
# =========================
def diagnostico_nulos(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    verbose: bool = True
):
    """
    Imprime diagnóstico de nulos globales, por tag y por timestamp.
    No modifica el dataframe.
    """
    total_nulos = df_long["value"].isna().sum()
    pct_nulos = total_nulos / len(df_long) * 100

    if verbose:
        print(f"Nulos totales: {total_nulos} ({pct_nulos:.4f} %)")

        missing_by_tag = (
            df_long
            .groupby(tag_col)["value"]
            .apply(lambda s: s.isna().mean() * 100)
            .sort_values(ascending=False)
        )

        print("\nTop 10 tags con mayor % de nulos:")
        print(missing_by_tag.head(10))

        missing_by_time = (
            df_long
            .groupby("timestamp")["value"]
            .apply(lambda s: s.isna().mean() * 100)
            .sort_values(ascending=False)
        )

        print("\nTop 10 timestamps con mayor % de nulos:")
        print(missing_by_time.head(10))


# =========================
# 4. Diagnóstico de variabilidad
# =========================
def diagnostico_variabilidad(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    umbral_std: float = 1e-3,
    verbose: bool = True
):
    """
    Calcula la desviación estándar por tag y detecta tags casi constantes.
    Retorna (std_by_tag, tags_casi_constantes).
    """
    std_by_tag = (
        df_long
        .groupby(tag_col)["value"]
        .std()
        .sort_values()
    )

    tags_casi_constantes = std_by_tag[std_by_tag < umbral_std]

    if verbose:
        print("\nDesviación estándar mínima y máxima por tag:")
        print(std_by_tag.head(10))   # las 10 con menos variación
        print(std_by_tag.tail(10))   # las 10 con más variación

        print(f"\nTags con std < {umbral_std} (posibles sensores congelados):")
        print(tags_casi_constantes)

    return std_by_tag, tags_casi_constantes


# =========================
# 5. Outliers por IQR
# =========================
def marcar_outliers_iqr_serie(serie: pd.Series) -> pd.Series:
    """
    Marca outliers en una serie usando el criterio IQR.
    Retorna una serie booleana del mismo índice.
    """
    serie = serie.astype(float)
    q1 = serie.quantile(0.25)
    q3 = serie.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:  # para señales constantes
        return pd.Series(False, index=serie.index)
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return (serie < lower) | (serie > upper)


def agregar_outliers_iqr(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_valor: str = "value",
    col_salida: str = "is_outlier_iqr",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Agrega una columna booleana 'is_outlier_iqr' (por defecto) al df_long
    marcando outliers por tag usando IQR.
    """
    df = df_long.sort_values([tag_col, "timestamp"]).copy()

    df[col_salida] = (
        df
        .groupby(tag_col)[col_valor]
        .transform(marcar_outliers_iqr_serie)
    )

    if verbose:
        pct_outliers = df[col_salida].mean() * 100
        print("\nPorcentaje de puntos marcados como outlier (IQR):")
        print(f"{pct_outliers:.4f} %")

        print("\nEjemplos de puntos outlier:")
        print(df[df[col_salida]].head(20))

    return df


# =========================
# 6. Interpolación de nulos por tag
# =========================
def interpolar_nulos_por_tag(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_valor: str = "value",
    nueva_col: str = "value_interp",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Interpola nulos en 'col_valor' por tag a lo largo del tiempo.
    Crea una nueva columna con el nombre 'nueva_col'.
    """
    df = df_long.sort_values([tag_col, "timestamp"]).copy()

    df[nueva_col] = (
        df
        .groupby(tag_col)[col_valor]
        .transform(lambda s: s.interpolate(limit_direction="both"))
    )

    if verbose:
        nulos_despues = df[nueva_col].isna().sum()
        print(f"\nNulos después de interpolar en {nueva_col}: {nulos_despues}")

    return df


# =========================
# 7. Pipeline completo de preprocesamiento
# =========================
def preprocesar_datos_proceso(
    ruta: str,
    tag_col: str = "Tag de PI",
    usar_interpolacion: bool = True,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Orquesta todo el preprocesamiento:
        1) Carga df_raw (wide)
        2) Convierte a df_long
        3) Diagnóstico de nulos
        4) Diagnóstico de variabilidad
        5) Marca outliers IQR
        6) (Opcional) Interpola nulos y devuelve 'value' ya limpio

    Retorna un df_long listo para pasar al módulo de features de turno,
    con columnas:
        - tag_col (ej. 'Tag de PI')
        - 'timestamp'
        - 'value'  (ya limpio si usar_interpolacion=True)
        - 'is_outlier_iqr'
        - 'value_interp' (si usar_interpolacion=True)
    """
    # 1) Cargar
    df_raw = cargar_datos_proceso(ruta, tag_col=tag_col, verbose=verbose)

    # 2) Wide -> Long
    df_long = wide_a_long(df_raw, tag_col=tag_col, verbose=verbose)

    # 3) Diagnóstico de nulos
    diagnostico_nulos(df_long, tag_col=tag_col, verbose=verbose)

    # 4) Diagnóstico de variabilidad
    diagnostico_variabilidad(df_long, tag_col=tag_col, verbose=verbose)

    # 5) Outliers
    df_long = agregar_outliers_iqr(df_long, tag_col=tag_col, col_valor="value", verbose=verbose)

    # 6) Interpolación (opcional)
    if usar_interpolacion:
        df_long = interpolar_nulos_por_tag(
            df_long,
            tag_col=tag_col,
            col_valor="value",
            nueva_col="value_interp",
            verbose=verbose
        )
        # Para el resto del pipeline, usamos la serie interpolada como 'value'
        df_long["value"] = df_long["value_interp"]

    return df_long
