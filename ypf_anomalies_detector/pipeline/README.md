# Pipeline de Detección de Anomalías con Prophet

Este pipeline implementa un sistema completo de detección de anomalías usando Facebook Prophet, ideal para series temporales de procesos industriales.

## Estructura del Pipeline

```
pipeline/
├── data/
│   ├── raw/              # Datos originales (opcional)
│   └── processed/        # Datos procesados (opcional)
├── models/
│   └── prophet/         # Modelos Prophet entrenados
├── results/              # Resultados de detección de anomalías
└── scripts/
    ├── prophet_anomaly_detector.py  # Clase principal del detector
    ├── train_anomaly_detector.py    # Script de entrenamiento
    └── detect_anomalies.py           # Script de detección
```

## Flujo de Trabajo

### 1. Selección y Limpieza de Variables

Primero ejecuta el protocolo de selección de variables:

```bash
python run_protocol.py
```

Esto genera:
- Datos limpios en `output/*_cleaned.csv`
- Reportes de selección en `output/*_variable_selection_report.csv`

### 2. Entrenamiento de Modelos Prophet

Entrena modelos Prophet para cada variable seleccionada:

```bash
python pipeline/scripts/train_anomaly_detector.py
```

**Parámetros configurables:**
- `interval_width`: Ancho del intervalo de confianza (default: 0.95)
- `changepoint_prior_scale`: Flexibilidad para cambios de tendencia (default: 0.05)
- `seasonality_mode`: 'additive' o 'multiplicative' (default: 'multiplicative')
- `daily_seasonality`: Estacionalidad diaria (default: True)
- `weekly_seasonality`: Estacionalidad semanal (default: True)
- `anomaly_threshold`: Umbral de anomalía en desviaciones estándar (default: 2.0)

**Salida:**
- Modelos guardados en `pipeline/models/prophet/`
- Estadísticas de variables
- Configuración del detector

### 3. Detección de Anomalías

Detecta anomalías en nuevos datos usando los modelos entrenados:

```bash
python pipeline/scripts/detect_anomalies.py
```

**Salida:**
- `anomalies_detected_[timestamp].csv`: Todos los resultados
- `anomaly_summary_[timestamp].csv`: Resumen por variable
- `anomalies_only_[timestamp].csv`: Solo registros anómalos

## Características del Detector

### Ventajas de Prophet

1. **Robusto a Outliers**: Prophet maneja bien valores atípicos durante el entrenamiento
2. **Estacionalidad Automática**: Detecta y modela patrones estacionales automáticamente
3. **Intervalos de Confianza**: Proporciona intervalos de predicción confiables
4. **Estable**: Menos propenso a sobreajuste que otros métodos

### Método de Detección

Una anomalía se detecta cuando:

1. **Fuera del Intervalo de Confianza**: El valor real está fuera del intervalo de predicción (95% por defecto)
2. **Residual Alto**: El residual (diferencia entre real y predicho) está más de 2 desviaciones estándar del promedio

### Score de Anomalía

Cada punto recibe un score de 0-100:
- **0-50**: Normal o ligeramente desviado
- **50-75**: Moderadamente anómalo
- **75-100**: Altamente anómalo

El score se calcula basado en:
- Distancia al intervalo de confianza
- Magnitud del residual normalizado

## Uso Programático

### Entrenar un Modelo

```python
from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector
import pandas as pd

# Cargar datos
df = pd.read_csv('output/2025-01_cleaned.csv', parse_dates=['DATETIME'])

# Crear detector
detector = ProphetAnomalyDetector(
    interval_width=0.95,
    anomaly_threshold=2.0
)

# Entrenar para una variable
model = detector.train_model(df, 'PI_1412A', 'DATETIME')

# O entrenar múltiples
detector.train_multiple_variables(
    df, 
    variables=['PI_1412A', 'TE_1432B', 'FI_1442A'],
    datetime_col='DATETIME'
)

# Guardar modelos
detector.save_models('pipeline/models/prophet')
```

### Detectar Anomalías

```python
# Cargar modelos guardados
detector = ProphetAnomalyDetector()
detector.load_models('pipeline/models/prophet')

# Detectar anomalías
results = detector.detect_anomalies_multiple(
    df,
    variables=['PI_1412A', 'TE_1432B'],
    datetime_col='DATETIME'
)

# Filtrar solo anomalías
anomalies = results[results['is_anomaly']]

# Ver resumen
summary = detector.get_anomaly_summary(results)
print(summary)
```

## Interpretación de Resultados

### Columnas en el DataFrame de Resultados

- `ds`: Fecha/hora
- `y`: Valor real observado
- `yhat`: Valor predicho por Prophet
- `yhat_lower`: Límite inferior del intervalo de confianza
- `yhat_upper`: Límite superior del intervalo de confianza
- `residual`: Diferencia entre real y predicho
- `outside_interval`: Si está fuera del intervalo de confianza
- `high_residual`: Si el residual es alto
- `is_anomaly`: Si es una anomalía (True/False)
- `anomaly_score`: Score de anomalía (0-100)
- `prediction_error_pct`: Error de predicción en porcentaje
- `variable`: Nombre de la variable

### Análisis de Anomalías

1. **Revisar Resumen**: El archivo `anomaly_summary_*.csv` muestra qué variables tienen más anomalías
2. **Fechas Problemáticas**: Identifica períodos con alta concentración de anomalías
3. **Variables Críticas**: Prioriza variables con scores altos de anomalía
4. **Validación**: Siempre valida anomalías detectadas con expertos del dominio

## Configuración Avanzada

### Ajustar Sensibilidad

Para detectar más anomalías (menos estricto):
```python
detector = ProphetAnomalyDetector(
    interval_width=0.90,      # Intervalo más estrecho
    anomaly_threshold=1.5     # Umbral más bajo
)
```

Para detectar menos anomalías (más estricto):
```python
detector = ProphetAnomalyDetector(
    interval_width=0.99,      # Intervalo más amplio
    anomaly_threshold=3.0      # Umbral más alto
)
```

### Estacionalidad Personalizada

Para datos con estacionalidad anual:
```python
detector = ProphetAnomalyDetector(
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
)
```

## Troubleshooting

### Error: "Insuficientes datos"
- **Causa**: Menos de 10 puntos de datos
- **Solución**: Asegúrate de tener suficientes datos históricos

### Modelos no detectan anomalías
- **Causa**: Umbral muy alto o datos muy estables
- **Solución**: Reduce `anomaly_threshold` o `interval_width`

### Entrenamiento muy lento
- **Causa**: Muchas variables o muchos datos
- **Solución**: Limita el número de variables o usa submuestreo

## Requisitos

```bash
pip install prophet pandas numpy
```

Nota: Prophet requiere compiladores C++ en Windows. Considera usar conda:
```bash
conda install -c conda-forge prophet
```

## Próximos Pasos

1. **Visualización**: Agregar gráficos de series temporales con anomalías marcadas
2. **Alertas**: Sistema de notificaciones para anomalías críticas
3. **Re-entrenamiento Automático**: Actualizar modelos periódicamente
4. **Dashboard**: Interfaz web para monitoreo en tiempo real

