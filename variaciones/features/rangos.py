# rangos.py

import pandas as pd
import numpy as np


# =========================
# 1. Construir rangos normales desde el histórico
# =========================
def construir_rangos_desde_historico(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_valor: str = "value",
    p_low: float = 0.05,
    p_high: float = 0.95,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Construye límites "normales" por tag a partir del histórico usando percentiles.
    Por defecto: [p5, p95].

    Retorna un dataframe con columnas:
        - tag_col
        - lim_inf
        - lim_sup
    """
    df_rangos = (
        df_long
        .groupby(tag_col)[col_valor]
        .agg(
            lim_inf=lambda s: s.quantile(p_low),
            lim_sup=lambda s: s.quantile(p_high),
        )
        .reset_index()
    )

    if verbose:
        print("\nEjemplo de rangos por tag (primeros 10):")
        print(df_rangos.head(10))

    return df_rangos


# =========================
# 2. Asignar turno (duplicamos lógica para ser independientes)
# =========================
def _asignar_turno(ts: pd.Timestamp) -> str:
    h = ts.hour
    if 0 <= h < 8:
        return "T1_00_08"
    elif 8 <= h < 16:
        return "T2_08_16"
    else:
        return "T3_16_00"


def _asegurar_turno_y_fecha(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
) -> pd.DataFrame:
    """
    Asegura que df_long tiene columnas 'turno' y 'fecha'.
    Si no están, las crea.
    """
    df = df_long.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if "turno" not in df.columns:
        df["turno"] = df["timestamp"].apply(_asignar_turno)

    if "fecha" not in df.columns:
        df["fecha"] = df["timestamp"].dt.date

    return df


# =========================
# 3. Estimar intervalo de muestreo (en minutos)
# =========================
def estimar_intervalo_min(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    timestamp_col: str = "timestamp",
) -> float:
    """
    Estima el intervalo de muestreo (en minutos) a partir del primer tag.
    """
    df = df_long.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])

    primer_tag = df[tag_col].iloc[0]
    sub = df[df[tag_col] == primer_tag].sort_values(timestamp_col)

    deltas = sub[timestamp_col].diff().dropna().dt.total_seconds() / 60.0
    if len(deltas) == 0:
        # fallback
        return 10.0

    dt_min = deltas.median()
    return float(dt_min)


# =========================
# 4. Cálculo de % en rango / bajo / alto por turno
# =========================
def calcular_porcentajes_rango_por_turno(
    df_long: pd.DataFrame,
    df_rangos: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_valor: str = "value",
    sample_period_min: float | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Calcula, para cada (Tag de PI, fecha, turno):
        - pct_en_rango
        - pct_bajo
        - pct_alto
        - tiempo_en_rango_min
        - tiempo_bajo_min
        - tiempo_alto_min
        - n_muestras

    Asume df_long con:
        - tag_col
        - 'timestamp'
        - col_valor (limpio)
    y df_rangos con:
        - tag_col
        - 'lim_inf'
        - 'lim_sup'
    """
    # Asegurar turno y fecha
    df = _asegurar_turno_y_fecha(df_long, tag_col=tag_col)

    # Merge con rangos
    df = df.merge(df_rangos, on=tag_col, how="left")

    # Estimar periodo de muestreo si no se pasa
    if sample_period_min is None:
        sample_period_min = estimar_intervalo_min(df, tag_col=tag_col, timestamp_col="timestamp")
        if verbose:
            print(f"\nPeriodo de muestreo estimado: {sample_period_min:.3f} minutos")

    def _stats_rango_grupo(sub: pd.DataFrame) -> pd.Series:
        vals = sub[col_valor]
        lim_inf = sub["lim_inf"].iloc[0]
        lim_sup = sub["lim_sup"].iloc[0]

        n_total = len(vals)
        if n_total == 0:
            return pd.Series({
                "n_muestras": 0,
                "pct_en_rango": 0.0,
                "pct_bajo": 0.0,
                "pct_alto": 0.0,
                "tiempo_en_rango_min": 0.0,
                "tiempo_bajo_min": 0.0,
                "tiempo_alto_min": 0.0,
            })

        mask_bajo = vals < lim_inf
        mask_alto = vals > lim_sup
        n_bajo = mask_bajo.sum()
        n_alto = mask_alto.sum()
        n_en_rango = n_total - n_bajo - n_alto

        pct_bajo = n_bajo / n_total * 100.0
        pct_alto = n_alto / n_total * 100.0
        pct_en_rango = n_en_rango / n_total * 100.0

        tiempo_bajo_min = n_bajo * sample_period_min
        tiempo_alto_min = n_alto * sample_period_min
        tiempo_en_rango_min = n_en_rango * sample_period_min

        return pd.Series({
            "n_muestras": n_total,
            "pct_en_rango": pct_en_rango,
            "pct_bajo": pct_bajo,
            "pct_alto": pct_alto,
            "tiempo_en_rango_min": tiempo_en_rango_min,
            "tiempo_bajo_min": tiempo_bajo_min,
            "tiempo_alto_min": tiempo_alto_min,
        })

    df_pct = (
        df
        .groupby([tag_col, "fecha", "turno"])
        .apply(_stats_rango_grupo)
        .reset_index()
    )

    if verbose:
        print("\nEjemplo de % en rango por turno:")
        print(df_pct.head())

    return df_pct


# =========================
# 5. Enriquecer features de turno con info de rangos
# =========================
def enriquecer_features_turno_con_rangos(
    df_features_turno: pd.DataFrame,
    df_pct_rango: pd.DataFrame,
    tag_col: str = "Tag de PI",
) -> pd.DataFrame:
    """
    Hace un merge entre las estadísticas de turno (df_features_turno)
    y los porcentajes de tiempo en rango (df_pct_rango) usando:
        - tag_col
        - 'fecha'
        - 'turno'
    """
    cols_clave = [tag_col, "fecha", "turno"]
    df_merged = df_features_turno.merge(
        df_pct_rango,
        on=cols_clave,
        how="left",
        suffixes=("", "_rango")
    )
    return df_merged
