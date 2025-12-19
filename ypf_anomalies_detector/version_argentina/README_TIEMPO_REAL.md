# Simulación de Detección de Anomalías en Tiempo Real

Este documento explica cómo simular un sistema de detección de anomalías en tiempo real usando los scripts proporcionados.

## Componentes

### 1. `simular_datos_tiempo_real.py`
Simula la llegada de datos en tiempo real insertándolos en la tabla `datos_proceso` de SQL Server.

### 2. `procesar_tiempo_real.py`
Monitorea la tabla `datos_proceso` y detecta anomalías en nuevos datos, escribiendo los resultados en `anomalies_detector`.

## Uso

### Paso 1: Simular datos en tiempo real

El simulador lee datos históricos y los inserta en SQL Server con timestamps que avanzan en tiempo real (o acelerado).

**Ejemplos de uso:**

```bash
# Simulación básica: insertar datos cada 60 segundos a velocidad normal
python simular_datos_tiempo_real.py

# Insertar datos cada 30 segundos, 10x más rápido que tiempo real
python simular_datos_tiempo_real.py --interval 30 --speed 10.0

# Simular solo 1000 filas para pruebas rápidas
python simular_datos_tiempo_real.py --max-rows 1000 --speed 10.0

# Agregar ruido aleatorio a los valores (útil para probar detección)
python simular_datos_tiempo_real.py --add-noise --noise-level 0.02

# Leer datos desde CSV en lugar de SQL
python simular_datos_tiempo_real.py --source csv --interval 60

# Especificar fecha de inicio
python simular_datos_tiempo_real.py --start-date "2024-11-20 10:00:00" --interval 60
```

**Parámetros:**
- `--source`: Fuente de datos (`sql` o `csv`, default: `sql`)
- `--interval`: Intervalo en segundos entre inserciones (default: 60)
- `--speed`: Multiplicador de velocidad (1.0 = tiempo real, 10.0 = 10x más rápido)
- `--max-rows`: Número máximo de filas a insertar (default: todas)
- `--start-date`: Fecha de inicio en formato `YYYY-MM-DD HH:MM:SS`
- `--add-noise`: Agregar ruido aleatorio a los valores
- `--noise-level`: Nivel de ruido (default: 0.01 = 1%)

### Paso 2: Procesar anomalías en tiempo real

El procesador monitorea la tabla `datos_proceso` y detecta anomalías en nuevos datos.

**Ejemplos de uso:**

```bash
# Modo continuo: procesar cada 60 segundos
python procesar_tiempo_real.py

# Procesar cada 30 segundos
python procesar_tiempo_real.py --interval 30

# Procesar una sola vez y salir (útil para pruebas)
python procesar_tiempo_real.py --once

# Buscar datos de las últimas 2 horas si no hay procesamiento previo
python procesar_tiempo_real.py --lookback 2
```

**Parámetros:**
- `--interval`: Intervalo en segundos entre procesamientos (default: 60)
- `--lookback`: Horas hacia atrás para buscar datos nuevos si no hay procesamiento previo (default: 1)
- `--once`: Procesar una sola vez y salir (default: modo continuo)

## Escenario de Prueba Completo

### Terminal 1: Simulador de datos
```bash
cd version_argentina
python simular_datos_tiempo_real.py --interval 30 --speed 10.0 --max-rows 5000
```

### Terminal 2: Procesador de anomalías
```bash
cd version_argentina
python procesar_tiempo_real.py --interval 30
```

## Flujo de Datos

1. **Simulador** → Inserta datos en `dbo.datos_proceso` con timestamps progresivos
2. **Procesador** → Lee nuevos datos de `dbo.datos_proceso`
3. **Procesador** → Detecta anomalías usando modelos Prophet entrenados
4. **Procesador** → Escribe resultados en `dbo.anomalies_detector`

## Notas Importantes

- **Modelos entrenados**: Asegúrate de tener modelos entrenados antes de ejecutar el procesador:
  ```bash
  python train_from_sql.py
  ```

- **Velocidad de simulación**: Usa `--speed` para acelerar la simulación durante pruebas. En producción, usa `--speed 1.0` para tiempo real.

- **Datos históricos**: El simulador puede leer desde SQL (tabla `datos_proceso`) o desde archivos CSV en `output/`.

- **Persistencia**: El procesador recuerda el último datetime procesado, por lo que puedes detenerlo y reiniciarlo sin perder datos.

- **Interrupción**: Ambos scripts pueden ser interrumpidos con `Ctrl+C` de forma segura.

## Monitoreo

Puedes consultar las tablas en SQL Server para ver el progreso:

```sql
-- Ver últimos datos insertados
SELECT TOP 100 * FROM dbo.datos_proceso 
ORDER BY created_at DESC

-- Ver últimas anomalías detectadas
SELECT TOP 100 * FROM dbo.anomalies_detector 
WHERE is_anomaly = 1
ORDER BY processed_at DESC

-- Contar anomalías por variable
SELECT variable, COUNT(*) as n_anomalies
FROM dbo.anomalies_detector
WHERE is_anomaly = 1
GROUP BY variable
ORDER BY n_anomalies DESC
```

