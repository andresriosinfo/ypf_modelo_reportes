# llm/generate_per_variable.py

import pandas as pd
from typing import Dict, Any
from .model_gemma import GemmaClient


def build_prompt_reporte(row: pd.Series) -> str:
    """
    Construye un prompt tipo 'reporte' usando la info de la fila.
    Ajusta los nombres de columna a los que tengas en tu CSV.
    """

    fecha = row.get("fecha", "N/A")
    turno = row.get("turno", "N/A")
    variable = row.get("variable", row.get("tag", "variable"))
    valor = row.get("valor", row.get("value", "N/A"))
    unidad = row.get("unidad", row.get("unit", ""))

    prompt = f"""
Eres un ingeniero de procesos experto en oleoductos.

Te doy información de una variable de proceso y quiero que generes
un breve reporte técnico en español, claro pero riguroso, que incluya:
- Interpretación del valor actual.
- Posibles causas si el valor es anómalo.
- Recomendaciones operativas si aplica.

Datos:
- Fecha: {fecha}
- Turno: {turno}
- Variable: {variable}
- Valor actual: {valor} {unidad}

No repitas la lista de datos tal cual; intégralos en un texto fluido.
Responde en uno o dos párrafos.
"""
    return prompt.strip()


def generar_para_turno(
    ruta_csv_features: str,
    fecha: str | None = None,
    turno: str | None = None,
    tags: Any = None,
) -> pd.DataFrame:
    """
    Lee el CSV de features, filtra por fecha/turno (si existen esas columnas),
    y genera textos tipo reporte por cada fila.
    """

    print(f"Leyendo features desde: {ruta_csv_features}")
    df = pd.read_csv(ruta_csv_features)

    # Filtrar por fecha y turno sólo si las columnas existen
    if fecha is not None and "fecha" in df.columns:
        df = df[df["fecha"] == fecha]

    if turno is not None and "turno" in df.columns:
        df = df[df["turno"] == turno]

    print(f"Filas después del filtrado: {len(df)}")

    # Instanciar el modelo
    llm = GemmaClient()

    resultados: list[Dict[str, Any]] = []

    for _, row in df.iterrows():
        prompt_reporte = build_prompt_reporte(row)

        texto_reporte = llm.generate(prompt_reporte)

        resultados.append(
            {
                "fecha": row.get("fecha", fecha),
                "turno": row.get("turno", turno),
                "variable": row.get("variable", row.get("tag", None)),
                "valor": row.get("valor", row.get("value", None)),
                "reporte_llm": texto_reporte,
            }
        )

    df_out = pd.DataFrame(resultados)
    return df_out


if __name__ == "__main__":
    # Ajusta la ruta al CSV si está en otra carpeta
    RUTA_CSV = r"C:\Users\AndrésRios\Documents\ypf_modelo_lenguaje\llm\features_turno_llm_ready.csv"
    # O, si lo tienes en la misma carpeta donde vas a ejecutar:
    # RUTA_CSV = "features_turno_llm_ready.csv"

    FECHA = "2024-10-01"   # pon una fecha que exista en tu CSV
    TURNO = "T1_00_08"     # igual con el turno

    df_textos = generar_para_turno(RUTA_CSV, FECHA, TURNO, tags=None)

    print("\n=== PRIMERAS FILAS DEL REPORTE ===")
    print(df_textos.head())

    # Guardar a CSV para revisar en Excel
    df_textos.to_csv("reportes_llm_por_variable.csv", index=False)
    print("\nReportes guardados en: reportes_llm_por_variable.csv")
