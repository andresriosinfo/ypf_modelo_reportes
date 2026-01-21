# YPF Modelo de Lenguaje - Sistema de Generación de Reportes

Sistema de análisis de procesos industriales que combina features engineering, detección de anomalías y generación de reportes en lenguaje natural usando modelos de lenguaje.

## Visión General

Este sistema es una pipeline de análisis de procesos industriales que combina:
1. Procesamiento de datos (features engineering)
2. Detección de anomalías (Prophet)
3. Generación de reportes con LLM (Gemma/Qwen)
4. Interfaz de chatbot (Gradio)

## Arquitectura en Capas

```
┌─────────────────────────────────────────────────────────────┐
│                    CAPA DE PRESENTACIÓN                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Chatbot    │  │   Informes   │  │ Visualizac.  │      │
│  │   (Gradio)   │  │   (LaTeX)    │  │  (Gráficas)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  CAPA DE GENERACIÓN LLM                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  GemmaClient │  │   Prompts    │  │  Generación  │      │
│  │  (Modelo)    │  │  (Templates) │  │  por Variable│      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              CAPA DE FEATURES ENGINEERING                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │Preprocesam.  │  │ Features     │  │ Clasificación│      │
│  │  (Datos)     │  │  (Turno)     │  │  (Categorías) │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Rangos     │  │  Dinámicas   │  │  Estadísticas│      │
│  │  (Normal)    │  │  (Tendencias)│  │  (Turno)     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    CAPA DE DATOS                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Excel/CSV  │  │     SQL      │  │  Archivos    │      │
│  │  (Datos Raw) │  │  (Tiempo Real)│  │  (Features)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Componentes Principales

### 1. Módulo de Features (`features/`)

**Propósito**: Transformar datos de proceso industrial en features estructuradas por turno.

**Flujo de procesamiento**:

```
Datos Excel/CSV (formato ancho)
    ↓
[Preprocesamiento]
    - Conversión a formato largo (long format)
    - Interpolación de valores faltantes
    - Normalización de timestamps
    ↓
[Features de Turno]
    - Estadísticas por turno (mean, std, min, max)
    - Comparación con mes anterior
    - Agregación por turnos de 8 horas
    ↓
[Rangos Normales]
    - Cálculo de percentiles (5%, 95%)
    - Porcentaje de tiempo en rango normal
    - Clasificación: "en_rango", "bajo", "alto"
    ↓
[Dinámicas]
    - Tendencias (slope)
    - Oscilaciones (cambios de signo)
    - Estabilidad (coeficiente de variación)
    ↓
[Clasificación]
    - Categorías discretas para LLM
    - Priorización de atención
    ↓
CSV: features_turno_llm_ready.csv
```

**Archivos clave**:
- `preprocesamiento.py`: Convierte datos de formato ancho a largo
- `features_turno.py`: Calcula estadísticas por turno
- `rangos.py`: Define rangos normales y calcula porcentajes
- `calcular_dinamicas_por_turno.py`: Analiza tendencias y oscilaciones
- `clasificar_*.py`: Convierte valores numéricos en categorías

### 2. Módulo LLM (`llm/`)

**Propósito**: Generar reportes en lenguaje natural usando modelos de lenguaje.

**Arquitectura**:

```
Features CSV (features_turno_llm_ready.csv)
    ↓
[GemmaClient / Modelo LLM]
    - Carga modelo (Gemma 2 9B o Qwen 2.5 7B)
    - Pipeline de text-generation
    ↓
[Prompts]
    - build_prompt_estado_rango()
    - build_prompt_tendencia()
    - build_prompt_oscilacion()
    - build_prompt_estabilidad()
    - build_prompt_resumen_corto()
    ↓
[Generación]
    - Por cada variable/turno
    - Genera texto en español
    - Interpretación técnica
    ↓
Reportes LLM (CSV con textos generados)
```

**Componentes**:
- `config.py`: Configuración del modelo (nombre, parámetros)
- `model_gemma.py`: Cliente del modelo (wrapper de transformers)
- `prompts.py`: Templates de prompts especializados
- `generate_per_variable.py`: Orquestador de generación

### 3. Variaciones (`variaciones/`)

Carpeta de experimentación para probar diferentes modelos de lenguaje sin afectar el proyecto principal. Actualmente configurada con Qwen 2.5 7B Instruct (más rápido que Gemma 2 9B).

## Flujo de Datos Completo

### Flujo Principal (Features → LLM)

```
1. DATOS RAW
   └─> Excel/CSV con datos de proceso industrial
   
2. PREPROCESAMIENTO
   └─> Conversión a formato largo
   └─> Interpolación y limpieza
   
3. FEATURES ENGINEERING
   └─> Estadísticas por turno
   └─> Rangos normales
   └─> Dinámicas (tendencias, oscilaciones)
   └─> Clasificaciones categóricas
   
4. CSV DE FEATURES
   └─> features_turno_llm_ready.csv
   
5. GENERACIÓN LLM
   └─> Lee CSV de features
   └─> Construye prompts especializados
   └─> Genera textos interpretativos
   
6. REPORTES
   └─> CSV con reportes generados
   └─> Informes LaTeX (opcional)
```

## Estructura del Proyecto

```
ypf_modelo_lenguaje/
├── features/              # Features engineering
│   ├── preprocesamiento.py
│   ├── features_turno.py
│   ├── rangos.py
│   ├── calcular_dinamicas_por_turno.py
│   ├── clasificar_dinamica_turno.py
│   ├── clasificar_rangos_turno.py
│   └── main_generar_features.py
├── llm/                   # Modelo de lenguaje
│   ├── config.py
│   ├── model_gemma.py
│   ├── prompts.py
│   ├── generate_per_variable.py
│   └── probar_gemma.py
├── variaciones/           # Experimentación (copia)
│   ├── features/
│   ├── llm/
│   └── README.md
└── plantilla/             # Templates LaTeX
    └── main.tex
```

## Requisitos

### Dependencias principales

```bash
pip install pandas numpy transformers torch gradio accelerate
```

### Modelos de lenguaje

El sistema soporta diferentes modelos de lenguaje:
- **Gemma 2 9B Instruct** (proyecto principal)
- **Qwen 2.5 7B Instruct** (variaciones - más rápido)

## Uso

### 1. Generar Features

```bash
cd features
python main_generar_features.py
```

Esto genera `features_turno_llm_ready.csv` con todas las features calculadas.

### 2. Generar Reportes con LLM

```bash
cd llm
python generate_per_variable.py
```

O usar la función programáticamente:

```python
from llm.generate_per_variable import generar_para_turno

df_reportes = generar_para_turno(
    ruta_csv_features="features_turno_llm_ready.csv",
    fecha="2024-10-01",
    turno="T1_00_08"
)
```

### 3. Experimentar con Modelos (Variaciones)

```bash
cd variaciones
# Editar variaciones/llm/config.py para cambiar el modelo
python -c "from llm.model_gemma import GemmaClient; client = GemmaClient()"
```

## Tecnologías Utilizadas

### Backend
- **Python 3.12**
- **Pandas**: Procesamiento de datos
- **Transformers**: Modelos de lenguaje (Gemma, Qwen)
- **PyTorch**: Framework de deep learning

### Frontend
- **Gradio**: Interfaz web del chatbot
- **LaTeX**: Generación de informes

## Características Arquitectónicas

### Modularidad
- Cada módulo es independiente
- Interfaces claras entre componentes
- Fácil de extender o modificar

### Separación de Responsabilidades
- **Features**: Procesamiento de datos
- **LLM**: Generación de texto
- **Variaciones**: Experimentación

### Pipeline ETL
- **Extract**: Datos de Excel/CSV/SQL
- **Transform**: Features engineering
- **Load**: CSV listos para LLM

### Configurabilidad
- Modelos intercambiables (Gemma, Qwen, etc.)
- Parámetros ajustables
- Prompts personalizables

## Puntos de Extensión

1. **Nuevos modelos LLM**: Cambiar en `llm/config.py`
2. **Nuevos tipos de features**: Agregar en `features/`
3. **Nuevos prompts**: Agregar en `llm/prompts.py`
4. **Nuevas visualizaciones**: Extender módulo de gráficas
5. **Integración con otras fuentes**: Extender preprocesamiento

## Notas

- Los datos (CSV, PKL, imágenes) están excluidos del repositorio por `.gitignore`
- El proyecto original usa Gemma 2 9B Instruct
- La carpeta `variaciones/` contiene una copia para experimentar con modelos más rápidos
- Los informes LaTeX se generan usando la plantilla en `plantilla/`

## Licencia

Este proyecto es privado y propiedad de YPF.
