# Funcionamiento del Sistema de Detección de Anomalías

## ¿Qué hace el sistema?

El sistema de detección de anomalías utiliza **Facebook Prophet** para:

1. **Entrenar modelos de predicción** para cada variable de proceso (temperaturas, presiones, flujos, etc.)
2. **Predecir valores esperados** en cada punto temporal
3. **Calcular intervalos de confianza** (95% por defecto) alrededor de las predicciones
4. **Detectar anomalías** comparando valores reales vs. predicciones
5. **Calcular scores de severidad** (0-100) para cada punto analizado

## Proceso de Detección

### Paso 1: Entrenamiento (train_anomaly_detector.py)

Para cada variable:
- Toma los datos históricos limpios
- Entrena un modelo Prophet que aprende:
  - **Tendencias** (cambios a largo plazo)
  - **Estacionalidad diaria** (patrones que se repiten cada día)
  - **Estacionalidad semanal** (patrones que se repiten cada semana)
- Guarda el modelo entrenado en `pipeline/models/prophet/`

### Paso 2: Detección (detect_anomalies.py)

Para cada punto temporal de cada variable:
1. **Predice el valor esperado** usando el modelo entrenado → `yhat`
2. **Calcula el intervalo de confianza** → `yhat_lower` y `yhat_upper`
3. **Compara el valor real** (`y`) con la predicción
4. **Calcula el residual** → `residual = y - yhat`
5. **Determina si es anomalía**:
   - `outside_interval = True` si `y` está fuera del intervalo [yhat_lower, yhat_upper]
   - `high_residual = True` si `|residual| > 2 * desviación_estándar`
   - `is_anomaly = True` si cualquiera de las dos condiciones es verdadera
6. **Calcula el score de anomalía** (0-100) basado en:
   - Qué tan lejos está del intervalo de confianza
   - La magnitud del residual normalizado

## Variables que Escribe el Sistema

### Archivo: `anomalies_detected_[timestamp].csv`

**13 columnas** generadas por el sistema:

| Columna | Tipo | Cómo se Calcula | Descripción |
|---------|------|------------------|-------------|
| `ds` | datetime | Del DataFrame original | Fecha y hora del registro |
| `y` | float | Del DataFrame original | Valor real observado de la variable |
| `yhat` | float | **Predicción de Prophet** | Valor predicho por el modelo |
| `yhat_lower` | float | **Intervalo inferior de Prophet** | Límite inferior del intervalo de confianza (95%) |
| `yhat_upper` | float | **Intervalo superior de Prophet** | Límite superior del intervalo de confianza (95%) |
| `residual` | float | **y - yhat** | Diferencia entre valor real y predicho |
| `outside_interval` | boolean | **y < yhat_lower OR y > yhat_upper** | True si está fuera del intervalo |
| `high_residual` | boolean | **\|residual\| > 2 * std(residual)** | True si el residual es alto |
| `is_anomaly` | boolean | **outside_interval OR high_residual** | True si es una anomalía |
| `anomaly_score` | float | **Fórmula compleja (0-100)** | Score de severidad de la anomalía |
| `variable` | string | Nombre de la variable analizada | Identificador de la variable |
| `prediction_error_pct` | float | **\|residual / yhat\| * 100** | Error porcentual de predicción |
| `source_file` | string | Nombre del archivo origen | Archivo de datos procesado |

### Archivo: `anomaly_summary_[timestamp].csv`

**8 columnas** agregadas por variable:

| Columna | Tipo | Cómo se Calcula | Descripción |
|---------|------|------------------|-------------|
| `variable` | string | Nombre de la variable | Identificador (índice) |
| `n_anomalies` | int | **COUNT(is_anomaly = True)** | Total de anomalías detectadas |
| `anomaly_rate` | float | **n_anomalies / n_points** | Tasa de anomalías (0-1) |
| `avg_score` | float | **MEAN(anomaly_score WHERE is_anomaly)** | Score promedio de anomalías |
| `max_score` | float | **MAX(anomaly_score WHERE is_anomaly)** | Score máximo encontrado |
| `avg_residual` | float | **MEAN(residual WHERE is_anomaly)** | Residual promedio |
| `std_residual` | float | **STD(residual WHERE is_anomaly)** | Desviación estándar de residuales |
| `n_points` | int | **COUNT(*)** | Total de puntos analizados |

### Archivo: `anomalies_only_[timestamp].csv`

**Mismas 13 columnas** que `anomalies_detected`, pero **filtrado** para incluir solo filas donde `is_anomaly = True`.

## Fórmulas Clave

### Cálculo de `anomaly_score`

```python
# Si el valor está por encima del intervalo superior:
anomaly_score = ((y - yhat_upper) / (yhat_upper - yhat)) * 50
# Limitado a máximo 100

# Si el valor está por debajo del intervalo inferior:
anomaly_score = ((yhat_lower - y) / (yhat - yhat_lower)) * 50
# Limitado a máximo 100

# Ajuste adicional basado en residuales:
anomaly_score = MAX(anomaly_score, (|residual| / std_residual * 20))
# Limitado a máximo 100
```

### Cálculo de `prediction_error_pct`

```python
prediction_error_pct = |residual / yhat| * 100
# Maneja divisiones por cero (retorna NaN)
```

## Flujo de Datos

```
Datos Limpios (CSV)
    ↓
Cargar modelos Prophet entrenados
    ↓
Para cada variable:
    ↓
    Prophet predice → yhat, yhat_lower, yhat_upper
    ↓
    Calcular residual = y - yhat
    ↓
    Detectar si es anomalía
    ↓
    Calcular anomaly_score
    ↓
    Calcular prediction_error_pct
    ↓
Combinar resultados de todas las variables
    ↓
Guardar en CSV:
    - anomalies_detected_[timestamp].csv (todos los puntos)
    - anomaly_summary_[timestamp].csv (resumen por variable)
    - anomalies_only_[timestamp].csv (solo anomalías)
```

## Parámetros Configurables

El sistema permite ajustar:

- **`interval_width`**: Ancho del intervalo de confianza (default: 0.95 = 95%)
- **`anomaly_threshold`**: Número de desviaciones estándar para considerar residual alto (default: 2.0)
- **`seasonality_mode`**: 'additive' o 'multiplicative' (default: 'multiplicative')
- **`daily_seasonality`**: Incluir estacionalidad diaria (default: True)
- **`weekly_seasonality`**: Incluir estacionalidad semanal (default: True)

## Ejemplo de Cálculo

Para un punto con:
- `y = 52.8` (valor real)
- `yhat = 45.4` (predicción)
- `yhat_lower = 42.7` (límite inferior)
- `yhat_upper = 48.1` (límite superior)

**Cálculos:**
1. `residual = 52.8 - 45.4 = 7.4`
2. `outside_interval = True` (porque 52.8 > 48.1)
3. `high_residual = True` (si |7.4| > 2 * std_residual)
4. `is_anomaly = True` (porque outside_interval = True)
5. `anomaly_score = ((52.8 - 48.1) / (48.1 - 45.4)) * 50 = 87.0`
6. `prediction_error_pct = |7.4 / 45.4| * 100 = 16.3%`

