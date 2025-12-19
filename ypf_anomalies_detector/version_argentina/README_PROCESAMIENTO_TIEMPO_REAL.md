# Procesamiento en Tiempo Real

Este sistema incluye dos scripts para procesar anomalías en tiempo real:

## 1. `procesar_tiempo_real.py` - Procesamiento por Lotes

Procesa datos en lotes, útil cuando llegan múltiples datos a la vez.

**Uso:**
```bash
# Modo continuo (verifica cada 60 segundos)
python procesar_tiempo_real.py

# Modo continuo con intervalo personalizado
python procesar_tiempo_real.py --interval 30

# Procesar una sola vez
python procesar_tiempo_real.py --once --lookback 1
```

**Parámetros:**
- `--interval`: Intervalo en segundos entre procesamientos (default: 60)
- `--lookback`: Horas hacia atrás para buscar datos nuevos si no hay procesamiento previo (default: 1)
- `--once`: Procesar una sola vez y salir (default: modo continuo)

## 2. `procesar_dato_individual.py` - Procesamiento Individual ⭐ RECOMENDADO PARA PRODUCCIÓN

Procesa cada dato nuevo **inmediatamente** cuando llega a la base de datos. Este es el script recomendado para producción ya que:

- ✅ Procesa cada dato tan pronto como llega
- ✅ No espera a acumular lotes
- ✅ Más eficiente para datos que llegan de forma continua
- ✅ Detecta nuevos datos cada 2 segundos (configurable)

**Uso:**
```bash
# Modo continuo (verifica cada 2 segundos)
python procesar_dato_individual.py

# Con intervalo personalizado (ej: cada 1 segundo)
python procesar_dato_individual.py --poll-interval 1.0

# Con intervalo más largo (ej: cada 5 segundos)
python procesar_dato_individual.py --poll-interval 5.0
```

**Parámetros:**
- `--poll-interval`: Intervalo en segundos entre verificaciones de nuevos datos (default: 2.0)

## Cómo Funciona

### `procesar_dato_individual.py`

1. **Carga los modelos** entrenados de Prophet
2. **Obtiene la lista de datetimes ya procesados** de `dbo.anomalies_detector`
3. **Monitorea continuamente** la tabla `dbo.datos_proceso`
4. **Cuando detecta un datetime nuevo**:
   - Lee todos los datos (todas las variables) para ese datetime
   - Convierte los datos al formato requerido
   - Ejecuta la detección de anomalías con todos los modelos
   - Escribe los resultados en `dbo.anomalies_detector`
5. **Marca el datetime como procesado** para no duplicarlo

### Ventajas del Procesamiento Individual

- **Baja latencia**: Los datos se procesan casi inmediatamente (2 segundos máximo)
- **Sin duplicados**: Cada datetime se procesa una sola vez
- **Eficiente**: Solo procesa datos nuevos, no re-procesa datos antiguos
- **Escalable**: Puede manejar datos que llegan a diferentes ritmos

## Requisitos Previos

1. **Modelos entrenados**: Debes haber ejecutado `train_from_sql.py` primero
2. **Datos en SQL**: Los datos deben estar en `dbo.datos_proceso`
3. **Conexión SQL**: Configuración correcta en el script

## Ejemplo de Flujo Completo

```bash
# 1. Escribir datos de entrenamiento a SQL
python write_training_data_to_sql.py

# 2. Entrenar modelos
python train_from_sql.py

# 3. Procesar datos en tiempo real (en producción)
python procesar_dato_individual.py
```

## Monitoreo

El script muestra información en tiempo real:
- Cada vez que detecta un datetime nuevo
- Número de anomalías detectadas por datetime
- Resumen de procesamiento

Ejemplo de salida:
```
[1] 2025-01-15 10:30:15 - 3 datetime(s) nuevo(s) encontrado(s)
  [PROCESANDO] 2025-01-15 10:30:00... [ANOMALÍAS: 2]
  [PROCESANDO] 2025-01-15 10:30:05... [OK]
  [PROCESANDO] 2025-01-15 10:30:10... [ANOMALÍAS: 1]
  [RESUMEN] 3 datetime(s) procesado(s), 3 anomalía(s) total(es)
```

## Detener el Procesador

Presiona `Ctrl+C` para detener el procesador de forma segura. El script mostrará un resumen final.

