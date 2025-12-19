# Sistema de Detección de Anomalías - Versión Argentina

Sistema completo de detección de anomalías adaptado para datos de proceso de Argentina.

## Estructura del Proyecto

```
version_argentina/
├── datos/
│   ├── Datos de proceso N-101.xlsx          # Archivo original
│   └── preprocesados/
│       └── datos_proceso_N101.csv           # Datos preprocesados
├── output/                                   # Datos limpios y reportes
├── pipeline/
│   ├── models/
│   │   └── prophet/                         # Modelos entrenados
│   ├── results/                             # Resultados de detección
│   └── scripts/
│       ├── prophet_anomaly_detector.py       # Clase principal
│       ├── train_anomaly_detector.py        # Entrenamiento
│       └── detect_anomalies.py              # Detección
├── preprocesar_datos.py                      # Preprocesamiento de datos
├── variable_selection_protocol.py            # Protocolo de selección
└── run_protocol.py                           # Ejecutar protocolo
```

## Flujo de Trabajo

### 1. Preprocesar Datos

Convierte el formato transpuesto del Excel a formato estándar:

```bash
python preprocesar_datos.py
```

**Salida**: `datos/preprocesados/datos_proceso_N101.csv`

### 2. Selección y Limpieza de Variables

Ejecuta el protocolo de selección de variables:

```bash
python run_protocol.py
```

O directamente:

```bash
python variable_selection_protocol.py
```

**Salida**:
- `output/datos_proceso_N101_cleaned.csv` - Datos limpios
- `output/datos_proceso_N101_variable_selection_report.csv` - Reporte

### 3. Entrenamiento de Modelos Prophet

Entrena modelos Prophet para cada variable seleccionada:

```bash
python pipeline/scripts/train_anomaly_detector.py
```

**Salida**: Modelos guardados en `pipeline/models/prophet/`

### 4. Detección de Anomalías

Detecta anomalías en los datos usando los modelos entrenados:

```bash
python pipeline/scripts/detect_anomalies.py
```

**Salida**:
- `pipeline/results/anomalies_detected_[timestamp].csv` - Todos los resultados
- `pipeline/results/anomaly_summary_[timestamp].csv` - Resumen por variable
- `pipeline/results/anomalies_only_[timestamp].csv` - Solo anomalías

## Variables de Salida

El sistema genera las mismas 13 columnas que la versión original:

1. `ds` - Fecha/hora
2. `y` - Valor real observado
3. `yhat` - Valor predicho
4. `yhat_lower` - Límite inferior del intervalo
5. `yhat_upper` - Límite superior del intervalo
6. `residual` - Diferencia entre real y predicho
7. `outside_interval` - Boolean: fuera del intervalo
8. `high_residual` - Boolean: residual alto
9. `is_anomaly` - Boolean: es anomalía
10. `anomaly_score` - Score 0-100
11. `variable` - Nombre de la variable
12. `prediction_error_pct` - Error porcentual
13. `source_file` - Archivo origen

## Diferencias con la Versión Original

- **Formato de entrada**: Los datos vienen en formato transpuesto (variables en filas, fechas en columnas)
- **Preprocesamiento**: Se requiere un paso adicional de transposición
- **Archivo único**: Se procesa un solo archivo en lugar de múltiples
- **Mismo formato de salida**: Las variables generadas son idénticas

## Requisitos

```bash
pip install pandas numpy prophet openpyxl scikit-learn
```

