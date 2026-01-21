# clasificacion_rangos_turno.py

import pandas as pd
import numpy as np


def _clasificar_estado_global_rango(
    pct_en_rango: float,
    umbral_ok: float,
    umbral_leve: float,
    umbral_moderada: float,
) -> str:
    """
    Clasifica el estado global del turno según % en rango.
    """
    if pd.isna(pct_en_rango):
        return "desconocido"

    if pct_en_rango >= umbral_ok:
        return "OK"
    elif pct_en_rango >= umbral_leve:
        return "Leve desviación"
    elif pct_en_rango >= umbral_moderada:
        return "Desviación moderada"
    else:
        return "Crítica"


def _clasificar_desviacion_predominante(
    pct_bajo: float,
    pct_alto: float,
    umbral_predominio: float,
) -> str:
    """
    Determina si la desviación predominante es:
        - por_debajo
        - por_encima
        - mixta
        - sin_desviacion
    según % de tiempo por debajo y por encima del rango.
    """
    if pd.isna(pct_bajo) or pd.isna(pct_alto):
        return "desconocida"

    total_fuera = pct_bajo + pct_alto

    if total_fuera < 1e-6:
        return "sin_desviacion"

    # porcentaje relativo de cada lado
    frac_bajo = pct_bajo / total_fuera
    frac_alto = pct_alto / total_fuera

    if frac_bajo >= umbral_predominio and frac_alto < (1 - umbral_predominio):
        return "por_debajo"
    elif frac_alto >= umbral_predominio and frac_bajo < (1 - umbral_predominio):
        return "por_encima"
    else:
        return "mixta"


def _clasificar_prioridad_atencion(
    estado_global: str,
    gap_pct: float | None,
    umbral_gap_medio: float,
    umbral_gap_alto: float,
) -> str:
    """
    Clasifica prioridad de atención combinando:
        - estado_global_rango
        - gap_pct (desviación relativa frente al mes)
    """
    if estado_global == "desconocido":
        return "Desconocida"

    # base según estado_global
    if estado_global == "OK":
        base = "Baja"
    elif estado_global == "Leve desviación":
        base = "Media"
    elif estado_global == "Desviación moderada":
        base = "Alta"
    else:  # Crítica
        base = "Crítica"

    # Ajuste según gap_pct
    if gap_pct is None or pd.isna(gap_pct):
        return base

    gap_abs = abs(gap_pct)

    if gap_abs >= umbral_gap_alto:
        # subimos un nivel de prioridad si no está ya en Crítica
        if base == "Baja":
            return "Media"
        elif base == "Media":
            return "Alta"
        elif base == "Alta":
            return "Crítica"
        else:
            return base
    elif gap_abs >= umbral_gap_medio:
        # subimos medio escalón: de Baja→Media o de Media→Alta
        if base == "Baja":
            return "Media"
        elif base == "Media":
            return "Alta"
        else:
            return base
    else:
        return base


def clasificar_rangos_turno(
    df_features: pd.DataFrame,
    col_pct_en_rango: str = "pct_en_rango",
    col_pct_bajo: str = "pct_bajo",
    col_pct_alto: str = "pct_alto",
    col_gap_pct: str = "gap_pct",
    # umbrales para estado_global_rango
    umbral_ok: float = 95.0,
    umbral_leve: float = 80.0,
    umbral_moderada: float = 50.0,
    # umbral para predominio de un lado (bajo/alto)
    umbral_predominio: float = 0.7,
    # umbrales para gap_pct
    umbral_gap_medio: float = 5.0,
    umbral_gap_alto: float = 10.0,
) -> pd.DataFrame:
    """
    Enriquecer df_features (que ya tiene % en rango y gap_pct) con columnas categóricas:

    - estado_global_rango:
        { "OK", "Leve desviación", "Desviación moderada", "Crítica", "desconocido" }

    - desviacion_predominante:
        { "por_debajo", "por_encima", "mixta", "sin_desviacion", "desconocida" }

    - prioridad_atencion:
        { "Baja", "Media", "Alta", "Crítica", "Desconocida" }

    Retorna un nuevo DataFrame con estas columnas añadidas.
    """
    df = df_features.copy()

    # 1) Estado global según % en rango
    df["estado_global_rango"] = df[col_pct_en_rango].apply(
        lambda x: _clasificar_estado_global_rango(
            x,
            umbral_ok=umbral_ok,
            umbral_leve=umbral_leve,
            umbral_moderada=umbral_moderada,
        )
    )

    # 2) Desviación predominante según % bajo / % alto
    df["desviacion_predominante"] = df.apply(
        lambda row: _clasificar_desviacion_predominante(
            row[col_pct_bajo],
            row[col_pct_alto],
            umbral_predominio=umbral_predominio,
        ),
        axis=1,
    )

    # 3) Prioridad de atención combinando estado_global_rango + gap_pct
    df["prioridad_atencion"] = df.apply(
        lambda row: _clasificar_prioridad_atencion(
            estado_global=row["estado_global_rango"],
            gap_pct=row[col_gap_pct],
            umbral_gap_medio=umbral_gap_medio,
            umbral_gap_alto=umbral_gap_alto,
        ),
        axis=1,
    )

    return df
