# probar_gemma.py
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

# Modelo más pequeño para probar (puedes cambiar luego a "google/gemma-2-9b-it")
MODEL_NAME = "google/gemma-2-2b-it"


def main():
    # Para que sea lo más estable posible: usamos CPU
    device = "cpu"
    print(f"Cargando modelo {MODEL_NAME} en {device}...")

    # Cargar tokenizer y modelo
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.float32,  # en CPU usamos float32
    ).to(device)

    # Prompt de prueba (cámbialo si quieres)
    prompt = (
        "Eres un ingeniero de procesos experto en oleoductos. "
        "Explica brevemente qué es una válvula de control."
    )

    # Tokenizar
    inputs = tokenizer(prompt, return_tensors="pt").to(device)

    # Generar sin gradientes (más eficiente)
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=150,
            temperature=0.4,
            top_p=0.9,
            do_sample=True,
        )

    # Decodificar
    text = tokenizer.decode(outputs[0], skip_special_tokens=True)

    print("\n=== RESPUESTA DEL MODELO ===")
    print(text)


if __name__ == "__main__":
    main()
