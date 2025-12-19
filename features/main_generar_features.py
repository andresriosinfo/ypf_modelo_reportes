# main.py

import pandas as pd

from preprocesamiento import preprocesar_datos_proceso
from features_turno import construir_features_turno
from rangos import (
    construir_rangos_desde_historico,
    calcular_porcentajes_rango_por_turno,
    enriquecer_features_turno_con_rangos,
)
from features.calcular_dinamicas_por_turno import (
    calcular_dinamica_por_turno,
    enriquecer_features_turno_con_dinamica,
)
from features.clasificar_dinamica_turno import clasificar_dinamica_turno
from features.clasificar_rangos_turno import clasificar_rangos_turno


def main():
    ruta = r"datos\Datos de proceso N-101.xlsx"

    # 1) Preprocesamiento
    df_long = preprocesar_datos_proceso(
        ruta=ruta,
        tag_col="Tag de PI",
        usar_interpolacion=True,
        verbose=True,
    )

    # 2) Features de turno (estadísticas + comparación mensual)
    df_features_turno = construir_features_turno(df_long)

    # 3) Rangos y % en rango
    df_rangos = construir_rangos_desde_historico(
        df_long,
        tag_col="Tag de PI",
        col_valor="value",
        p_low=0.05,
        p_high=0.95,
        verbose=True,
    )

    df_pct_rango = calcular_porcentajes_rango_por_turno(
        df_long,
        df_rangos,
        tag_col="Tag de PI",
        col_valor="value",
        sample_period_min=None,
        verbose=True,
    )

    df_features_rangos = enriquecer_features_turno_con_rangos(
        df_features_turno,
        df_pct_rango,
        tag_col="Tag de PI",
    )

    # 4) Dinámica
    df_dinamica = calcular_dinamica_por_turno(
        df_long,
        tag_col="Tag de PI",
        col_valor="value",
        verbose=True,
    )

    df_features_dinamica = enriquecer_features_turno_con_dinamica(
        df_features_rangos,
        df_dinamica,
        tag_col="Tag de PI",
    )

    # 5) Etiquetas discretas de dinámica
    df_features_dinamica_cat = clasificar_dinamica_turno(
        df_features_dinamica,
        tag_col="Tag de PI",
        col_cv="cv_turno",
        col_slope="slope_turno",
        col_std="std_turno",
        col_osc_norm="osc_sign_changes_norm_turno",
    )

    # 6) Etiquetas discretas de estado global de rango
    df_features_final = clasificar_rangos_turno(
        df_features_dinamica_cat,
        col_pct_en_rango="pct_en_rango",
        col_pct_bajo="pct_bajo",
        col_pct_alto="pct_alto",
        col_gap_pct="gap_pct",
    )

    print("\nEjemplo de columnas categóricas para el LLM:")
    print(
        df_features_final[
            [
                "Tag de PI", "fecha", "turno",
                "pct_en_rango", "pct_bajo", "pct_alto", "gap_pct",
                "estado_global_rango", "desviacion_predominante",
                "prioridad_atencion",
                "tendencia_turno_cat", "osc_turno_cat", "estabilidad_turno_cat",
            ]
        ].head()
    )

    df_features_final.to_csv("features_turno_llm_ready.csv", index=False)


if __name__ == "__main__":
    main()
