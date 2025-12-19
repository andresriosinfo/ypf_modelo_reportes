# Integración con SQL Server

Este documento explica cómo usar el sistema de detección de anomalías con SQL Server.

## Configuración

### 1. Instalar dependencias

```bash
pip install -r requirements_sql.txt
```

**Nota**: Para Windows, es posible que necesites instalar el driver ODBC para SQL Server:
- Descargar desde: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### 2. Configuración de conexión

Los scripts usan las siguientes credenciales (definidas en cada script):

```python
SQL_CONFIG = {
    'server': '10.147.17.105',
    'database': 'ypf_ai_pilot',
    'username': 'sa',
    'password': 'Gnomes1*.',
    'port': 14333
}
```

## Flujo de trabajo

### Paso 1: Escribir datos de entrenamiento a SQL

Este script toma los datos limpios (CSV) y los escribe en la tabla `datos_proceso`:

```bash
python write_training_data_to_sql.py
```

**Qué hace:**
- Lee archivos `*_cleaned.csv` de la carpeta `output/`
- Transforma datos de formato ancho a formato largo (datetime, variable_name, value)
- Crea la tabla `dbo.datos_proceso` si no existe
- Escribe todos los datos a SQL

**Estructura de la tabla `datos_proceso`:**
- `id`: BIGINT (auto-incremental, PK)
- `datetime`: DATETIME
- `variable_name`: VARCHAR(100)
- `value`: DECIMAL(18,6)
- `source_file`: VARCHAR(255)
- `created_at`: DATETIME (default: GETDATE())

### Paso 2: Entrenar modelos desde SQL

Este script lee datos desde SQL y entrena los modelos:

```bash
python train_from_sql.py
```

**Qué hace:**
- Lee todos los datos de `dbo.datos_proceso`
- Transforma de formato largo a ancho (una columna por variable)
- Entrena modelos Prophet para cada variable
- Guarda modelos en `pipeline/models/prophet/`

**Opcional**: Puedes filtrar por fechas editando el script:
```python
df = read_data_from_sql(sql_conn, start_date='2024-10-01', end_date='2024-10-31')
```

### Paso 3: Detectar anomalías desde SQL

Este script lee datos desde SQL, detecta anomalías y escribe resultados:

```bash
python detect_from_sql.py
```

**Qué hace:**
- Lee datos de `dbo.datos_proceso`
- Carga modelos entrenados
- Detecta anomalías usando Prophet
- Escribe resultados en `dbo.anomalies_detector`

**Estructura de la tabla `anomalies_detector`:**
- `id`: BIGINT (auto-incremental, PK)
- `ds`: DATETIME (fecha del punto)
- `y`: DECIMAL(18,6) (valor real)
- `yhat`: DECIMAL(18,6) (valor predicho)
- `yhat_lower`: DECIMAL(18,6) (límite inferior)
- `yhat_upper`: DECIMAL(18,6) (límite superior)
- `residual`: DECIMAL(18,6) (error)
- `outside_interval`: BIT (fuera del intervalo)
- `high_residual`: BIT (residual alto)
- `is_anomaly`: BIT (es anomalía)
- `anomaly_score`: DECIMAL(5,2) (puntuación 0-100)
- `variable`: VARCHAR(100) (nombre de variable)
- `prediction_error_pct`: DECIMAL(5,2) (error porcentual)
- `source_file`: VARCHAR(255)
- `processed_at`: DATETIME (default: GETDATE())

**Opcional**: Puedes filtrar por fechas para procesar solo datos nuevos:
```python
from datetime import datetime, timedelta
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
df = read_data_from_sql(sql_conn, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
```

## Consultas útiles

### Ver datos de entrenamiento
```sql
SELECT TOP 100 * FROM dbo.datos_proceso ORDER BY datetime DESC
```

### Ver anomalías detectadas
```sql
SELECT TOP 100 * 
FROM dbo.anomalies_detector 
WHERE is_anomaly = 1 
ORDER BY anomaly_score DESC, ds DESC
```

### Resumen de anomalías por variable
```sql
SELECT 
    variable,
    COUNT(*) as n_anomalies,
    AVG(anomaly_score) as avg_score,
    MAX(anomaly_score) as max_score,
    MIN(ds) as first_anomaly,
    MAX(ds) as last_anomaly
FROM dbo.anomalies_detector
WHERE is_anomaly = 1
GROUP BY variable
ORDER BY n_anomalies DESC
```

### Anomalías en las últimas 24 horas
```sql
SELECT * 
FROM dbo.anomalies_detector
WHERE is_anomaly = 1 
  AND ds >= DATEADD(day, -1, GETDATE())
ORDER BY anomaly_score DESC
```

## Notas importantes

1. **Formato de datos**: Los datos se almacenan en formato largo (una fila por variable por timestamp) para facilitar el manejo de múltiples variables.

2. **Índices**: Los scripts crean índices automáticamente para optimizar las consultas.

3. **Duplicados**: Si ejecutas `write_training_data_to_sql.py` múltiples veces, los datos se agregarán (append). Para evitar duplicados, puedes:
   - Eliminar datos antes de escribir: `DELETE FROM dbo.datos_proceso`
   - O agregar lógica de deduplicación en el script

4. **Seguridad**: En producción, las credenciales deben estar en variables de entorno o archivos de configuración seguros, no hardcodeadas en el código.

5. **Performance**: Para grandes volúmenes de datos, considera procesar en chunks o filtrar por fechas.


