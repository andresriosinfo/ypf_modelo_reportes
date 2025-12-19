import pandas as pd
import numpy as np

# =========================
# 1. Función para asignar turno a un timestamp
# =========================
def asignar_turno(ts: pd.Timestamp) -> str:
    """
    Asigna el turno a partir de la hora de un timestamp:
    - T1_00_08 : 00:00 a 08:00
    - T2_08_16 : 08:00 a 16:00
    - T3_16_00 : 16:00 a 00:00 (del día siguiente)
    """
    h = ts.hour
    if 0 <= h < 8:
        return "T1_00_08"
    elif 8 <= h < 16:
        return "T2_08_16"
    else:
        return "T3_16_00"


# =========================
# 2. Enriquecer df_long con columnas de fecha y turno
# =========================
def agregar_info_turno(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe un df_long con columnas:
        - 'Tag de PI'
        - 'timestamp'
        - 'value'
    y añade:
        - 'turno'
        - 'fecha' (solo la fecha, sin hora)
    """
    df = df_long.copy()

    # Asegurarnos de que timestamp es datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Asignar turno
    df["turno"] = df["timestamp"].apply(asignar_turno)

    # Fecha del día (sin tiempo)
    df["fecha"] = df["timestamp"].dt.date

    return df


# =========================
# 3. Cálculo de estadísticas por turno
# =========================
def calcular_stats_por_turno(df_long_con_turno: pd.DataFrame) -> pd.DataFrame:
    """
    A partir de un df_long que ya tiene columnas:
        - 'Tag de PI'
        - 'fecha'
        - 'turno'
        - 'value'
    calcula las estadísticas por (Tag de PI, fecha, turno).
    """
    cols_necesarias = {"Tag de PI", "fecha", "turno", "value"}
    if not cols_necesarias.issubset(df_long_con_turno.columns):
        raise ValueError(f"Faltan columnas necesarias en df_long: {cols_necesarias}")

    df_turno_stats = (
        df_long_con_turno
        .groupby(["Tag de PI", "fecha", "turno"])["value"]
        .agg(
            mean_turno="mean",
            std_turno="std",
            min_turno="min",
            max_turno="max",
            p10_turno=lambda x: x.quantile(0.10),
            p50_turno="median",
            p90_turno=lambda x: x.quantile(0.90),
        )
        .reset_index()
    )

    return df_turno_stats


# =========================
# 4. Cálculo de estadísticas mensuales por variable
# =========================
def calcular_stats_mensuales(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula estadísticas mensuales (de todo el periodo disponible) por Tag de PI.
    De momento solo devuelve la media mensual (mean_mes), pero se puede ampliar.
    """
    if "Tag de PI" not in df_long.columns or "value" not in df_long.columns:
        raise ValueError("df_long debe contener las columnas 'Tag de PI' y 'value'")

    df_month_stats = (
        df_long
        .groupby("Tag de PI")["value"]
        .agg(mean_mes="mean")
        .reset_index()
    )

    return df_month_stats


# =========================
# 5. Orquestador: construir features de turno completas
# =========================
def construir_features_turno(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline completo para construir features de turno:
        1) Añade columnas de turno y fecha
        2) Calcula estadísticas por turno
        3) Calcula estadísticas mensuales por tag
        4) Calcula diferencias entre el turno y el mes (gap_mean, gap_pct)

    Retorna un DataFrame con una fila por (Tag de PI, fecha, turno)
    y columnas:
        - mean_turno, std_turno, min_turno, max_turno
        - p10_turno, p50_turno, p90_turno
        - mean_mes, gap_mean, gap_pct
    """

    # 1) Añadir turno y fecha
    df_long_con_turno = agregar_info_turno(df_long)

    # 2) Stats por turno
    df_turno_stats = calcular_stats_por_turno(df_long_con_turno)

    # 3) Stats mensuales
    df_month_stats = calcular_stats_mensuales(df_long)

    # 4) Merge turno + mes
    df_turno_stats = df_turno_stats.merge(df_month_stats, on="Tag de PI", how="left")

    # 5) Comparaciones entre turno y mes
    df_turno_stats["gap_mean"] = df_turno_stats["mean_turno"] - df_turno_stats["mean_mes"]
    df_turno_stats["gap_pct"] = (
        df_turno_stats["gap_mean"] / df_turno_stats["mean_mes"]
    ) * 100

    return df_turno_stats
