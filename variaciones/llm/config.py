# llm/config.py

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"  # Más rápido que Gemma 2 9B (7B vs 9B) y muy robusto

# Parámetros por defecto de generación
GENERATION_CONFIG = {
    "max_new_tokens": 200,
    "temperature": 0.4,
    "top_p": 0.9,
    "do_sample": True,
}
