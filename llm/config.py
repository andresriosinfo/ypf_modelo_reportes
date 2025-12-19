# llm/config.py

MODEL_NAME = "google/gemma-2-9b-it"  # versión instruct

# Parámetros por defecto de generación
GENERATION_CONFIG = {
    "max_new_tokens": 200,
    "temperature": 0.4,
    "top_p": 0.9,
    "do_sample": True,
}
