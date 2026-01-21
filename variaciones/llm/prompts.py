# llm/prompts.py

import pandas as pd


def build_prompt_estado_rango(row: pd.Series) -> str:
    return f"""
Eres un ingeniero de procesos que analiza una sola variable de planta en un turno de 8 horas.

Datos de la variable:
- nombre_variable: {row['Tag de PI']}
- fecha: {row['fecha']}
- turno: {row['turno']}
- porcentaje_en_rango: {row['pct_en_rango']:.2f}%
- porcentaje_bajo: {row['pct_bajo']:.2f}%
- porcentaje_alto: {row['pct_alto']:.2f}%
- estado_global_rango: "{row['estado_global_rango']}"
- desviacion_predominante: "{row['desviacion_predominante']}"

Tarea:
Escribe UNA sola frase corta (máximo 2 líneas) que describa cómo estuvo la variable respecto al rango normal.
No inventes valores numéricos nuevos. No hagas listas. Responde en español.
""".strip()


def build_prompt_tendencia(row: pd.Series) -> str:
    return f"""
Eres un ingeniero de procesos que analiza la tendencia de una variable en un turno de 8 horas.

Datos:
- nombre_variable: {row['Tag de PI']}
- fecha: {row['fecha']}
- turno: {row['turno']}
- tendencia_turno_cat: "{row['tendencia_turno_cat']}"

Tarea:
Describe en UNA frase corta la tendencia de la variable durante el turno, interpretando la categoría dada.
No inventes datos nuevos. Responde en español.
""".strip()


def build_prompt_oscilacion(row: pd.Series) -> str:
    return f"""
Eres un ingeniero de procesos que analiza la oscilación de una variable en un turno de 8 horas.

Datos:
- nombre_variable: {row['Tag de PI']}
- fecha: {row['fecha']}
- turno: {row['turno']}
- osc_turno_cat: "{row['osc_turno_cat']}"

Tarea:
Escribe UNA frase que describa si la señal osciló o no durante el turno.
Responde en español y no agregues información que no esté en la categoría.
""".strip()


def build_prompt_estabilidad(row: pd.Series) -> str:
    cv = row.get("cv_turno", None)
    osc_norm = row.get("osc_sign_changes_norm_turno", None)
    return f"""
Eres un ingeniero de procesos que analiza la estabilidad de una variable en un turno de 8 horas.

Datos:
- nombre_variable: {row['Tag de PI']}
- fecha: {row['fecha']}
- turno: {row['turno']}
- estabilidad_turno_cat: "{row['estabilidad_turno_cat']}"
- cv_turno: {cv}
- osc_sign_changes_norm_turno: {osc_norm}

Tarea:
Escribe una frase corta que resuma qué tan estable o inestable fue la variable durante el turno.
No inventes números adicionales. Responde en español.
""".strip()


def build_prompt_resumen_corto(row: pd.Series) -> str:
    return f"""
Eres un ingeniero de procesos que debe resumir el comportamiento de UNA sola variable
durante un turno de 8 horas, para un informe operacional.

Datos:
- nombre_variable: {row['Tag de PI']}
- fecha: {row['fecha']}
- turno: {row['turno']}
- estado_global_rango: "{row['estado_global_rango']}"
- desviacion_predominante: "{row['desviacion_predominante']}"
- tendencia_turno_cat: "{row['tendencia_turno_cat']}"
- osc_turno_cat: "{row['osc_turno_cat']}"
- estabilidad_turno_cat: "{row['estabilidad_turno_cat']}"
- prioridad_atencion: "{row['prioridad_atencion']}"

Tarea:
Escribe un resumen muy breve (2 a 3 líneas) del comportamiento de esta variable durante el turno.
No inventes valores numéricos ni estados que no aparezcan arriba. Responde en español.
""".strip()
