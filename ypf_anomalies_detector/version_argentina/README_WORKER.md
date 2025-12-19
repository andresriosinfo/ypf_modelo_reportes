# Worker de Procesamiento de Anomalías

Worker optimizado que verifica periódicamente si hay nuevos datos y los procesa automáticamente.

## Características

- ✅ **Eficiente**: Verifica solo cada 10 minutos (configurable), no consume recursos constantemente
- ✅ **Automático**: Procesa automáticamente todos los datos nuevos encontrados
- ✅ **Robusto**: Maneja errores y reconecta automáticamente
- ✅ **Logging**: Registra todas las operaciones para monitoreo
- ✅ **Estadísticas**: Muestra resumen de procesamiento

## Uso

### Modo Básico (cada 10 minutos)

```bash
python worker_procesamiento.py
```

### Intervalo Personalizado

```bash
# Verificar cada 5 minutos
python worker_procesamiento.py --interval 5

# Verificar cada 30 minutos
python worker_procesamiento.py --interval 30

# Verificar cada 1 hora
python worker_procesamiento.py --interval 60
```

## Cómo Funciona

1. **Inicialización**:
   - Carga los modelos entrenados de Prophet
   - Conecta a SQL Server
   - Obtiene el último datetime procesado (o busca desde hace 24 horas si es la primera vez)

2. **Ciclo de Verificación** (cada 10 minutos por defecto):
   - Consulta la tabla `dbo.datos_proceso` por datos nuevos desde el último procesado
   - Si encuentra datos nuevos:
     - Los convierte al formato requerido
     - Ejecuta la detección de anomalías
     - Escribe los resultados en `dbo.anomalies_detector`
     - Actualiza el último datetime procesado
   - Si no hay datos nuevos, espera hasta la próxima verificación

3. **Manejo de Errores**:
   - Si hay un error, lo registra y continúa con la siguiente verificación
   - No se detiene por errores temporales

## Ejemplo de Salida

```
2025-01-15 10:00:00 - INFO - ================================================================================
2025-01-15 10:00:00 - INFO - WORKER DE DETECCIÓN DE ANOMALÍAS
2025-01-15 10:00:00 - INFO - ================================================================================
2025-01-15 10:00:00 - INFO - Intervalo de verificación: 10 minutos
2025-01-15 10:00:00 - INFO - Presiona Ctrl+C para detener
2025-01-15 10:00:00 - INFO - 
2025-01-15 10:00:00 - INFO - Cargando modelos desde: pipeline/models/prophet
2025-01-15 10:00:05 - INFO - 25 modelos cargados exitosamente
2025-01-15 10:00:05 - INFO - Último datetime procesado: 2025-01-15 09:45:00
2025-01-15 10:00:05 - INFO - [Iteración 1] 2025-01-15 10:00:05 - Verificando nuevos datos...
2025-01-15 10:00:05 - INFO - Datos encontrados: 150 filas
2025-01-15 10:00:05 - INFO - Datos convertidos: 5 datetime(s) únicos, 25 variables
2025-01-15 10:00:05 - INFO - Variables a analizar: 25 de 25 modelos
2025-01-15 10:00:12 - INFO - ✓ Procesados: 5 datetime(s), Anomalías: 3 (Total: 3)
2025-01-15 10:00:12 - INFO -   Esperando 10 minutos hasta la próxima verificación...
```

## Ejecutar como Servicio (Windows)

### Opción 1: Task Scheduler

1. Abre "Programador de tareas" (Task Scheduler)
2. Crea una tarea básica
3. Configura:
   - **Trigger**: "Al iniciar sesión" o "Al iniciar el equipo"
   - **Acción**: Iniciar programa
   - **Programa**: `python`
   - **Argumentos**: `C:\ruta\a\version_argentina\worker_procesamiento.py`
   - **Iniciar en**: `C:\ruta\a\version_argentina`

4. En la pestaña "Configuración":
   - Marca "Ejecutar la tarea tan pronto como sea posible después de una conexión programada que se pierda"
   - Marca "Si la tarea en ejecución no finaliza cuando se solicite, forzar su detención"

### Opción 2: NSSM (Non-Sucking Service Manager)

1. Descarga NSSM: https://nssm.cc/download
2. Instala el servicio:
```bash
nssm install AnomalyDetectionWorker "C:\Python\python.exe" "C:\ruta\a\version_argentina\worker_procesamiento.py"
nssm set AnomalyDetectionWorker AppDirectory "C:\ruta\a\version_argentina"
nssm start AnomalyDetectionWorker
```

## Ejecutar como Servicio (Linux)

### systemd Service

Crea el archivo `/etc/systemd/system/anomaly-worker.service`:

```ini
[Unit]
Description=Anomaly Detection Worker
After=network.target

[Service]
Type=simple
User=tu_usuario
WorkingDirectory=/ruta/a/version_argentina
ExecStart=/usr/bin/python3 /ruta/a/version_argentina/worker_procesamiento.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Luego:
```bash
sudo systemctl daemon-reload
sudo systemctl enable anomaly-worker
sudo systemctl start anomaly-worker
sudo systemctl status anomaly-worker
```

## Monitoreo

### Ver logs en tiempo real

```bash
# Windows PowerShell
Get-Content worker.log -Wait -Tail 50

# Linux
tail -f worker.log
```

### Verificar que está funcionando

```sql
-- Ver últimos datos procesados
SELECT TOP 10 * 
FROM dbo.anomalies_detector 
ORDER BY processed_at DESC;

-- Ver cuántos datetimes se han procesado hoy
SELECT COUNT(DISTINCT ds) as datetimes_procesados
FROM dbo.anomalies_detector
WHERE CAST(processed_at AS DATE) = CAST(GETDATE() AS DATE);
```

## Recomendaciones de Intervalo

- **Datos que llegan cada minuto o más frecuente**: `--interval 5` (5 minutos)
- **Datos que llegan cada 5-15 minutos**: `--interval 10` (10 minutos) - **RECOMENDADO**
- **Datos que llegan cada hora**: `--interval 30` (30 minutos)
- **Datos que llegan esporádicamente**: `--interval 60` (1 hora)

## Ventajas sobre Polling Constante

| Aspecto | Polling cada 2s | Worker cada 10min |
|---------|----------------|-------------------|
| **Uso de CPU** | Alto (constante) | Bajo (solo cuando verifica) |
| **Consultas SQL** | ~1800/hora | 6/hora |
| **Carga en BD** | Alta | Mínima |
| **Latencia** | ~2 segundos | ~10 minutos máximo |
| **Escalabilidad** | Limitada | Excelente |

## Troubleshooting

### El worker no encuentra datos nuevos

- Verifica que hay datos en `dbo.datos_proceso`:
```sql
SELECT MAX(datetime) as ultimo_dato FROM dbo.datos_proceso;
```

- Verifica el último datetime procesado:
```sql
SELECT MAX(ds) as ultimo_procesado FROM dbo.anomalies_detector;
```

### El worker se detiene

- Revisa los logs para ver errores
- Verifica que los modelos existen en `pipeline/models/prophet/`
- Verifica la conexión a SQL Server

### El worker procesa datos duplicados

- Esto no debería pasar, el worker usa el último datetime procesado
- Si ocurre, verifica que la tabla `anomalies_detector` tiene el índice correcto

