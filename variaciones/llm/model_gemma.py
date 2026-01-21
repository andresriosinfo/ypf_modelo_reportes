# llm/model_gemma.py

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from .config import MODEL_NAME, GENERATION_CONFIG


class GemmaClient:
    """
    Wrapper para modelos de lenguaje usando transformers.
    Actualmente configurado para Qwen 2.5 7B Instruct (más rápido que Gemma 2 9B y muy robusto).

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
        
        # Configurar padding token si no existe (necesario para Qwen)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        
        # Configurar dtype
        dtype = torch.float16 if self.device == "cuda" else torch.float32
        
        # Verificar si accelerate está disponible para usar device_map
        # Importar accelerate explícitamente para asegurar que esté disponible
        try:
            import accelerate
            accelerate_available = True
        except ImportError:
            accelerate_available = False
        
        # Cargar el modelo
        # Usar device_map solo si accelerate está disponible y estamos en CUDA
        if accelerate_available and self.device == "cuda":
            try:
                self.model = AutoModelForCausalLM.from_pretrained(
                    MODEL_NAME,
                    dtype=dtype,
                    device_map="auto",
                )
            except Exception as e:
                # Si falla con device_map, cargar sin él
                print(f"Advertencia: No se pudo usar device_map. Cargando sin device_map: {e}")
                self.model = AutoModelForCausalLM.from_pretrained(
                    MODEL_NAME,
                    dtype=dtype,
                )
                self.model = self.model.to(self.device)
        else:
            # Cargar sin device_map y mover manualmente
            self.model = AutoModelForCausalLM.from_pretrained(
                MODEL_NAME,
                dtype=dtype,
            )
            self.model = self.model.to(self.device)

        self.pipe = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
        )

    def generate(self, prompt: str, **gen_kwargs) -> str:
        """
        Genera texto para un prompt dado.
        Para Qwen 2.5, el tokenizer maneja automáticamente el formato de chat.
        """
        kwargs = GENERATION_CONFIG.copy()
        kwargs.update(gen_kwargs)

        # Si el tokenizer tiene chat_template, usarlo para formatear el prompt
        if hasattr(self.tokenizer, 'apply_chat_template') and self.tokenizer.chat_template:
            # Formatear como mensaje de usuario para Qwen
            messages = [{"role": "user", "content": prompt}]
            formatted_prompt = self.tokenizer.apply_chat_template(
                messages, 
                tokenize=False, 
                add_generation_prompt=True
            )
        else:
            formatted_prompt = prompt

        out = self.pipe(
            formatted_prompt,
            **kwargs
        )[0]["generated_text"]

        # Limpiar el prompt de la salida
        if out.startswith(formatted_prompt):
            out = out[len(formatted_prompt):].strip()
        elif out.startswith(prompt):
            out = out[len(prompt):].strip()

        return out
