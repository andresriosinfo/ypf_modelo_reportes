# Variaciones del Modelo - Chatbot

Esta carpeta contiene una **COPIA EXACTA** del proyecto del chatbot original para experimentar con diferentes modelos de lenguaje sin afectar el proyecto principal.

## ⚠️ IMPORTANTE

**NO se ha modificado el proyecto original.** Esta es una copia completa donde puedes experimentar libremente.

## Estructura

- `llm/`: Módulo de lenguaje de máquina (MODIFICADO para usar Qwen 2.5 7B)
  - `config.py`: Configuración del modelo (MODIFICADO: ahora usa Qwen 2.5 7B Instruct - más rápido y robusto)
  - `model_gemma.py`: Cliente para el modelo (MODIFICADO: compatible con Qwen)
  - `prompts.py`: Funciones para construir prompts (ORIGINAL)
  - `generate_per_variable.py`: Generación de reportes por variable (ORIGINAL)
  - `probar_gemma.py`: Script de prueba del modelo (ORIGINAL)

- `features/`: Módulo de procesamiento de features (COPIA EXACTA del original)
  - Scripts para calcular dinámicas, clasificar rangos, etc.

## Modelo actual

**Qwen 2.5 7B Instruct** - Más rápido que Gemma 2 9B y muy robusto:
- ✅ Más rápido: 7B parámetros vs 9B (aproximadamente 20-30% más rápido)
- ✅ Muy robusto: Supera a GPT-3.5 en benchmarks (74.2% MMLU)
- ✅ Sin restricciones: No requiere acceso especial
- ✅ Buen rendimiento en español

## Modificaciones sugeridas

Para cambiar el modelo de lenguaje y hacerlo más rápido:

1. Edita `llm/config.py` y cambia `MODEL_NAME` al modelo deseado
2. Si el nuevo modelo requiere una clase diferente, modifica `llm/model_gemma.py` o crea un nuevo archivo de modelo
3. Ajusta los parámetros de generación en `GENERATION_CONFIG` si es necesario

## Notas

- Este es un espacio de trabajo independiente del proyecto principal
- Los cambios aquí NO afectan el proyecto original
- Puedes experimentar libremente con diferentes modelos y configuraciones
- El proyecto original sigue intacto con Gemma 2 9B
