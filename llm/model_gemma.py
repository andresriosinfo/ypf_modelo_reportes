# llm/model_gemma.py

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from .config import MODEL_NAME, GENERATION_CONFIG


class GemmaClient:
    """
    Wrapper sencillo alrededor de Gemma 2 9B Instruct usando transformers.

    Usa un pipeline de text-generation. Carga el modelo una sola vez.
    """

    def __init__(self, device: str | None = None):
        # Detectar dispositivo si no se especifica
        if device is None:
            if torch.cuda.is_available():
                device = "cuda"
            else:
                device = "cpu"

        self.device = device

        print(f"Cargando modelo {MODEL_NAME} en {self.device}...")
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        self.model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            torch_dtype=torch.float16 if self.device == "cuda" else torch.float32,
            device_map="auto" if self.device == "cuda" else None,
        )

        self.pipe = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
        )

    def generate(self, prompt: str, **gen_kwargs) -> str:
        """
        Genera texto para un prompt dado.
        """
        kwargs = GENERATION_CONFIG.copy()
        kwargs.update(gen_kwargs)

        out = self.pipe(
            prompt,
            **kwargs
        )[0]["generated_text"]

        # En algunos modelos el prompt viene incluido en la salida: lo limpiamos si quieres.
        # Aquí asumimos que el texto útil viene después del prompt, así que:
        if out.startswith(prompt):
            out = out[len(prompt):].strip()

        return out
