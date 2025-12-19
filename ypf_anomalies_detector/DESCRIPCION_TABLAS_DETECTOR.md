# Descripción de Tablas del Detector de Anomalías

## Tabla Principal: `anomalies_detected_[timestamp].csv`

### Estructura de la Tabla

Esta tabla contiene **todos los puntos analizados** por el detector de anomalías, con información completa de predicciones y detección.

### Columnas y Tipos de Datos

| Columna | Tipo | Descripción | Ejemplo |
|---------|------|-------------|---------|
| `ds` | datetime | Fecha y hora del registro | `2024-11-01 00:00:00` |
| `y` | float | Valor real observado | `45.2` |
| `yhat` | float | Valor predicho por Prophet | `44.8` |
| `yhat_lower` | float | Límite inferior del intervalo de confianza (95%) | `42.1` |
| `yhat_upper` | float | Límite superior del intervalo de confianza (95%) | `47.5` |
| `residual` | float | Diferencia entre valor real y predicho (y - yhat) | `0.4` |
| `outside_interval` | boolean | True si el valor está fuera del intervalo de confianza | `False` |
| `high_residual` | boolean | True si el residual es alto (>2 desviaciones estándar) | `False` |
| `is_anomaly` | boolean | True si es una anomalía (outside_interval OR high_residual) | `False` |
| `anomaly_score` | float | Score de anomalía de 0-100 (mayor = más anómalo) | `5.2` |
| `variable` | string | Nombre de la variable analizada | `PI_1412A` |
| `prediction_error_pct` | float | Error porcentual de predicción | `0.89` |
| `source_file` | string | Archivo origen de los datos | `2024-11_cleaned` |

### Características

- **Una fila por punto temporal** analizado
- **Múltiples variables** pueden estar en la misma tabla (identificadas por columna `variable`)
- **Formato CSV estándar** con separador de coma
- **Encoding UTF-8**
- **Sin índice** (primera fila es header)

### Interpretación de Valores

- **`is_anomaly = True`**: El punto fue marcado como anomalía
- **`anomaly_score`**:
  - 0-50: Normal o ligeramente desviado
  - 50-75: Moderadamente anómalo
  - 75-100: Altamente anómalo (crítico)
- **`outside_interval = True`**: El valor real está fuera del intervalo de confianza del 95%
- **`high_residual = True`**: El residual está más de 2 desviaciones estándar del promedio

---

## Tabla Resumen: `anomaly_summary_[timestamp].csv`

### Estructura de la Tabla

Esta tabla contiene un **resumen agregado por variable** de todas las anomalías detectadas.

### Columnas y Tipos de Datos

| Columna | Tipo | Descripción | Ejemplo |
|---------|------|-------------|---------|
| `variable` | string | Nombre de la variable (índice) | `PI_1412A` |
| `n_anomalies` | int | Cantidad total de anomalías detectadas | `5` |
| `anomaly_rate` | float | Tasa de anomalías (0-1) | `0.0625` |
| `avg_score` | float | Score promedio de anomalías | `87.6` |
| `max_score` | float | Score máximo encontrado | `100.0` |
| `avg_residual` | float | Residual promedio | `15.2` |
| `std_residual` | float | Desviación estándar de residuales | `28.4` |
| `n_points` | int | Total de puntos analizados | `80` |

### Características

- **Una fila por variable** monitoreada
- **Ordenada por `n_anomalies` descendente** por defecto
- **Formato CSV estándar** con separador de coma
- **Encoding UTF-8**

### Interpretación de Valores

- **`anomaly_rate`**: Porcentaje de puntos que son anomalías (ej: 0.0625 = 6.25%)
- **`avg_score`**: Severidad promedio de las anomalías en esa variable
- **`max_score`**: La anomalía más severa encontrada en esa variable

---

## Tabla Solo Anomalías: `anomalies_only_[timestamp].csv`

### Estructura de la Tabla

Esta tabla contiene **solo los registros marcados como anomalías** (`is_anomaly = True`).

### Columnas

**Mismas columnas que `anomalies_detected`**, pero filtrada para incluir solo filas donde `is_anomaly = True`.

### Características

- **Subconjunto de `anomalies_detected`**
- **Útil para análisis enfocado** en problemas detectados
- **Formato CSV estándar** con separador de coma
- **Encoding UTF-8**

---

## Operaciones Recomendadas con las Tablas

### Filtrado

```python
# Filtrar por variable
df[df['variable'] == 'PI_1412A']

# Filtrar por fecha
df[df['ds'] >= '2024-11-01']

# Filtrar por score mínimo
df[df['anomaly_score'] > 75]

# Filtrar solo anomalías críticas
df[(df['is_anomaly'] == True) & (df['anomaly_score'] > 75)]
```

### Agrupaciones

```python
# Anomalías por variable
df[df['is_anomaly']].groupby('variable').size()

# Anomalías por día
df[df['is_anomaly']].groupby(df['ds'].dt.date).size()

# Score promedio por variable
df.groupby('variable')['anomaly_score'].mean()
```

### Exportación

- **CSV**: Para análisis en Excel/Python
- **JSON**: Para consumo por APIs
- **PDF**: Para reportes ejecutivos

