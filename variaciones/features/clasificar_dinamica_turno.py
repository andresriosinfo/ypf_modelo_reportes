# clasificacion_dinamica_turno.py

import pandas as pd
import numpy as np


def _clasificar_tendencia_row(
    row,
    slope_col: str,
    cv_col: str,
    std_col: str | None,
    umbral_slope_std_baja: float,
    umbral_slope_std_alta: float,
    umbral_cv_plano: float,
    osc_norm_col: str,
    umbral_osc_para_oscilando: float,
) -> str:
    """
    Regla para tendencia_turno_cat.
    """
    slope = row[slope_col]
    cv = row[cv_col]
    osc_norm = row[osc_norm_col]
    std_turno = row[std_col] if std_col is not None and std_col in row.index else np.nan

    # Si no tenemos datos suficientes
    if pd.isna(slope):
        return "desconocida"

    # Si está muy oscilante, priorizamos "oscilando"
    if not pd.isna(osc_norm) and osc_norm >= umbral_osc_para_oscilando:
        return "oscilando"

    # Normalizamos la pendiente por la desviación estándar (si está disponible)
    if not pd.isna(std_turno) and std_turno > 0:
        slope_norm = slope / std_turno
    else:
        slope_norm = slope  # fallback: usamos slope sin normalizar

    # Si la variación es casi nula, consideramos la señal plana
    if not pd.isna(cv) and cv < umbral_cv_plano:
        if abs(slope_norm) < umbral_slope_std_baja:
            return "plana"
        else:
            # ligera tendencia pero casi sin variación
            return "ligera_tendencia"

    # Reglas principales de tendencia
    if slope_norm >= umbral_slope_std_alta:
        return "subiendo_fuerte"
    elif slope_norm >= umbral_slope_std_baja:
        return "subiendo"
    elif slope_norm <= -umbral_slope_std_alta:
        return "bajando_fuerte"
    elif slope_norm <= -umbral_slope_std_baja:
        return "bajando"
    else:
        return "estable"


def _clasificar_oscilacion_row(
    row,
    osc_norm_col: str,
    umbral_baja: float,
    umbral_media: float,
    umbral_alta: float,
) -> str:
    """
    Clasifica nivel de oscilación a partir de osc_sign_changes_norm_turno.
    """
    osc_norm = row[osc_norm_col]

    if pd.isna(osc_norm):
        return "osc_desconocida"

    if osc_norm < umbral_baja:
        return "sin_oscilacion"
    elif osc_norm < umbral_media:
        return "oscilacion_baja"
    elif osc_norm < umbral_alta:
        return "oscilacion_media"
    else:
        return "oscilacion_alta"


def _clasificar_estabilidad_global_row(
    row,
    cv_col: str,
    osc_cat_col: str,
    umbral_cv_bajo: float,
    umbral_cv_medio: float,
) -> str:
    """
    Combina CV + nivel de oscilación para dar una etiqueta de estabilidad global.
    """
    cv = row[cv_col]
    osc_cat = row[osc_cat_col]

    if pd.isna(cv):
        return "estabilidad_desconocida"

    # Primero miramos cv
    if cv < umbral_cv_bajo:
        base = "muy_estable"
    elif cv < umbral_cv_medio:
        base = "estable"
    else:
        base = "variable"

    # Ajustamos según la oscilación
    if osc_cat in ("oscilacion_media", "oscilacion_alta"):
        if base == "muy_estable":
            return "estable_oscilante"
        elif base == "estable":
            return "variable_oscilante"
        else:
            return "inestable"
    else:
        return base


def clasificar_dinamica_turno(
    df_features: pd.DataFrame,
    tag_col: str = "Tag de PI",
    col_cv: str = "cv_turno",
    col_slope: str = "slope_turno",
    col_std: str = "std_turno",
    col_osc_norm: str = "osc_sign_changes_norm_turno",
    # umbrales tendencia
    umbral_slope_std_baja: float = 0.05,
    umbral_slope_std_alta: float = 0.20,
    umbral_cv_plano: float = 0.01,
    umbral_osc_para_oscilando: float = 0.7,
    # umbrales oscilación
    umbral_osc_baja: float = 0.1,
    umbral_osc_media: float = 0.4,
    umbral_osc_alta: float = 0.7,
    # umbrales estabilidad global
    umbral_cv_bajo: float = 0.02,
    umbral_cv_medio: float = 0.10,
) -> pd.DataFrame:
    """
    Enriquecer df_features (que ya tiene métricas dinámicas) con etiquetas discretas:

    - tendencia_turno_cat: {subiendo_fuerte, subiendo, bajando_fuerte, bajando, estable, plana, ligera_tendencia, oscilando, desconocida}
    - osc_turno_cat: {sin_oscilacion, oscilacion_baja, oscilacion_media, oscilacion_alta, osc_desconocida}
    - estabilidad_turno_cat: {muy_estable, estable, variable, inestable, estable_oscilante, variable_oscilante, estabilidad_desconocida}

    Requiere que df_features tenga columnas:
        - col_cv (cv_turno)
        - col_slope (slope_turno)
        - col_std (std_turno)   [para normalizar pendiente, si disponible]
        - col_osc_norm (osc_sign_changes_norm_turno)
        - 'fecha'
        - 'turno'
    """
    df = df_features.copy()

    # 1) Clasificar oscilación
    df["osc_turno_cat"] = df.apply(
        lambda row: _clasificar_oscilacion_row(
            row,
            osc_norm_col=col_osc_norm,
            umbral_baja=umbral_osc_baja,
            umbral_media=umbral_osc_media,
            umbral_alta=umbral_osc_alta,
        ),
        axis=1,
    )

    # 2) Clasificar tendencia
    df["tendencia_turno_cat"] = df.apply(
        lambda row: _clasificar_tendencia_row(
            row,
            slope_col=col_slope,
            cv_col=col_cv,
            std_col=col_std,
            umbral_slope_std_baja=umbral_slope_std_baja,
            umbral_slope_std_alta=umbral_slope_std_alta,
            umbral_cv_plano=umbral_cv_plano,
            osc_norm_col=col_osc_norm,
            umbral_osc_para_oscilando=umbral_osc_para_oscilando,
        ),
        axis=1,
    )

    # 3) Clasificar estabilidad global
    df["estabilidad_turno_cat"] = df.apply(
        lambda row: _clasificar_estabilidad_global_row(
            row,
            cv_col=col_cv,
            osc_cat_col="osc_turno_cat",
            umbral_cv_bajo=umbral_cv_bajo,
            umbral_cv_medio=umbral_cv_medio,
        ),
        axis=1,
    )

    return df
