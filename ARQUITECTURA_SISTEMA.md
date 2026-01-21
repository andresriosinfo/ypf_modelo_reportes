# Arquitectura Principal del Sistema YPF - Modelo de Lenguaje

## Visión General

El sistema es una **pipeline de análisis de procesos industriales** que combina:
1. **Procesamiento de datos** (features engineering)
2. **Detección de anomalías** (Prophet)
3. **Generación de reportes con LLM** (Gemma/Qwen)
4. **Interfaz de chatbot** (Gradio)

---

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
│              CAPA DE DETECCIÓN DE ANOMALÍAS                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Prophet    │  │  Detección   │  │  Visualización│      │
│  │  (Modelos)   │  │  (Anomalías) │  │  (Gráficas)   │      │
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

---

## Componentes Principales

### 1. **Módulo de Features** (`features/`)

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

---

### 2. **Módulo LLM** (`llm/`)

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

---

### 3. **Módulo de Detección de Anomalías** (`ypf_anomalies_detector/`)

**Propósito**: Detectar anomalías en series temporales usando Prophet.

**Pipeline**:

```
Datos Limpios (CSV)
    ↓
[Selección de Variables]
    - Protocolo de selección
    - Limpieza de datos
    ↓
[Entrenamiento Prophet]
    - Modelo por variable
    - Aprende tendencias y estacionalidad
    ↓
[Detección]
    - Predicción de valores esperados
    - Intervalos de confianza (95%)
    - Cálculo de scores de anomalía
    ↓
Resultados (CSV con anomalías detectadas)
```

**Tecnologías**:
- **Facebook Prophet**: Modelos de series temporales
- **SQL Server**: Integración para tiempo real (opcional)

---

### 4. **Interfaz de Chatbot** (Gradio)

**Propósito**: Interfaz web para interactuar con el modelo LLM.

**Componentes**:
- Chat general: Preguntas directas al modelo
- Análisis desde CSV: Análisis estructurado de variables

---

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

### Flujo Paralelo (Detección de Anomalías)

```
1. DATOS LIMPIOS
   └─> CSV procesado
   
2. ENTRENAMIENTO PROPHET
   └─> Modelos por variable
   
3. DETECCIÓN
   └─> Comparación real vs. predicho
   └─> Scores de anomalía
   
4. RESULTADOS
   └─> CSV con anomalías
   └─> Visualizaciones
```

---

## Tecnologías Utilizadas

### Backend
- **Python 3.12**
- **Pandas**: Procesamiento de datos
- **Transformers**: Modelos de lenguaje (Gemma, Qwen)
- **PyTorch**: Framework de deep learning
- **Prophet**: Series temporales
- **SQL Server**: Base de datos (opcional)

### Frontend
- **Gradio**: Interfaz web del chatbot
- **LaTeX**: Generación de informes
- **Matplotlib/Plotly**: Visualizaciones

---

## Características Arquitectónicas

### 1. **Modularidad**
- Cada módulo es independiente
- Interfaces claras entre componentes
- Fácil de extender o modificar

### 2. **Separación de Responsabilidades**
- **Features**: Procesamiento de datos
- **LLM**: Generación de texto
- **Detector**: Análisis estadístico
- **Chatbot**: Interfaz de usuario

### 3. **Pipeline ETL**
- **Extract**: Datos de Excel/CSV/SQL
- **Transform**: Features engineering
- **Load**: CSV listos para LLM

### 4. **Configurabilidad**
- Modelos intercambiables (Gemma, Qwen, etc.)
- Parámetros ajustables
- Prompts personalizables

---

## Puntos de Extensión

1. **Nuevos modelos LLM**: Cambiar en `llm/config.py`
2. **Nuevos tipos de features**: Agregar en `features/`
3. **Nuevos prompts**: Agregar en `llm/prompts.py`
4. **Nuevas visualizaciones**: Extender módulo de gráficas
5. **Integración con otras fuentes**: Extender preprocesamiento

---

## Estructura de Directorios

```
ypf_modelo_lenguaje/
├── features/              # Features engineering
│   ├── preprocesamiento.py
│   ├── features_turno.py
│   ├── rangos.py
│   └── ...
├── llm/                   # Modelo de lenguaje
│   ├── config.py
│   ├── model_gemma.py
│   ├── prompts.py
│   └── ...
├── ypf_anomalies_detector/ # Detección de anomalías
│   ├── pipeline/
│   └── version_argentina/
├── variaciones/           # Experimentación (copia)
└── plantilla/             # Templates LaTeX
```

---

## Ventajas de esta Arquitectura

✅ **Modular**: Cada componente puede desarrollarse independientemente  
✅ **Escalable**: Fácil agregar nuevos modelos o features  
✅ **Mantenible**: Separación clara de responsabilidades  
✅ **Testeable**: Cada módulo puede probarse por separado  
✅ **Flexible**: Fácil cambiar modelos o configuraciones  
