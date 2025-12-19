# dinamica_turno.py

import pandas as pd
import numpy as np


# =========================
# 1. Asegurar columnas de turno y fecha
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
    Se asegura de que df_long tenga columnas:
        - 'timestamp' (datetime)
        - 'turno'
        - 'fecha'
    """
    df = df_long.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if "turno" not in df.columns:
        df["turno"] = df["timestamp"].apply(_asignar_turno)

    if "fecha" not in df.columns:
        df["fecha"] = df["timestamp"].dt.date

    return df


# =========================
# 2. Estimar intervalo de muestreo (minutos)
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
        return 10.0  # fallback

    dt_min = deltas.median()
    return float(dt_min)


# =========================
# 3. Función auxiliar: dinámica dentro de un turno
# =========================
def _stats_dinamica_turno(
    sub: pd.DataFrame,
    col_valor: str,
) -> pd.Series:
    """
    Calcula:
        - cv_turno
        - slope_turno (pendiente regresión lineal valor ~ tiempo_min)
        - rate_mean_turno ((último - primero) / Δt)
        - osc_sign_changes_turno (n cambios de signo en derivada)
        - osc_sign_changes_norm_turno
        - n_muestras_turno
        - duracion_turno_min
    """
    vals = sub[col_valor].astype(float).values
    t = pd.to_datetime(sub["timestamp"])
    # tiempo en minutos relativo al inicio del turno
    t0 = t.iloc[0]
    t_min = (t - t0).dt.total_seconds().values / 60.0

    n = len(vals)
    if n < 2:
        # con una sola muestra no tiene sentido hablar de pendiente ni oscilaciones
        return pd.Series({
            "n_muestras_turno": n,
            "duracion_turno_min": 0.0,
            "cv_turno": np.nan,
            "slope_turno": np.nan,
            "rate_mean_turno": np.nan,
            "osc_sign_changes_turno": 0,
            "osc_sign_changes_norm_turno": 0.0,
        })

    mean = float(np.mean(vals))
    std = float(np.std(vals, ddof=1))  # similar a pandas std()

    cv_turno = std / mean if mean != 0 else np.nan

    # Pendiente por regresión lineal simple
    # y = a * t_min + b  -> slope = a
    try:
        slope, intercept = np.polyfit(t_min, vals, 1)
    except np.linalg.LinAlgError:
        slope = np.nan

    # Tasa de cambio promedio: (último - primero) / Δt
    dt_total_min = (t.iloc[-1] - t.iloc[0]).total_seconds() / 60.0
    if dt_total_min == 0:
        rate_mean = np.nan
    else:
        rate_mean = (vals[-1] - vals[0]) / dt_total_min

    # Oscilación: cambios de signo en la derivada
    diffs = np.diff(vals)
    signs = np.sign(diffs)  # -1, 0, +1
    # ignorar ceros para contar cambios de signo realmente
    signs_nonzero = signs[signs != 0]

    if len(signs_nonzero) < 2:
        osc_changes = 0
        osc_changes_norm = 0.0
    else:
        osc_changes = int(np.sum(np.diff(signs_nonzero) != 0))
        osc_changes_norm = osc_changes / max(len(signs_nonzero) - 1, 1)

    return pd.Series({
        "n_muestras_turno": n,
        "duracion_turno_min": dt_total_min,
        "cv_turno": cv_turno,
        "slope_turno": slope,
        "rate_mean_turno": rate_mean,
        "osc_sign_changes_turno": osc_changes,
        "osc_sign_changes_norm_turno": osc_changes_norm,
    })


# =========================
# 4. Cálculo de dinámica por turno
# =========================
def calcular_dinamica_por_turno(
    df_long: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_valor: str = "value",
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Calcula métricas de dinámica para cada (tag_col, fecha, turno):
        - n_muestras_turno
        - duracion_turno_min
        - cv_turno
        - slope_turno
        - rate_mean_turno
        - osc_sign_changes_turno
        - osc_sign_changes_norm_turno
    """
    df = _asegurar_turno_y_fecha(df_long, tag_col=tag_col)
    df = df.sort_values([tag_col, "fecha", "turno", "timestamp"])

    df_dinamica = (
        df
        .groupby([tag_col, "fecha", "turno"])
        .apply(lambda sub: _stats_dinamica_turno(sub, col_valor=col_valor))
        .reset_index()
    )

    if verbose:
        print("\nEjemplo de métricas de dinámica por turno:")
        print(df_dinamica.head())

    return df_dinamica


# =========================
# 5. Merge con features de turno
# =========================
def enriquecer_features_turno_con_dinamica(
    df_features_turno: pd.DataFrame,
    df_dinamica: pd.DataFrame,
    tag_col: str = "Tag de PI",
) -> pd.DataFrame:
    """
    Hace un merge de las features de turno (estadísticas, rangos, etc.)
    con las métricas de dinámica, usando:
        - tag_col
        - 'fecha'
        - 'turno'
    """
    claves = [tag_col, "fecha", "turno"]
    df_merged = df_features_turno.merge(
        df_dinamica,
        on=claves,
        how="left",
        suffixes=("", "_din")
    )
    return df_merged
