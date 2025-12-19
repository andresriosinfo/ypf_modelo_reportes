# Worker de Reentrenamiento Automático

Worker que reentrena los modelos de Prophet automáticamente todos los días a las 2:00 AM usando datos de SQL Server.

## Características

- ✅ **Automático**: Reentrena los modelos todos los días sin intervención
- ✅ **Configurable**: Puedes cambiar la hora de reentrenamiento
- ✅ **Robusto**: Maneja errores y continúa funcionando
- ✅ **Eficiente**: Solo verifica la hora cada minuto, no consume recursos constantemente
- ✅ **Logging**: Registra todas las operaciones

## Uso

### Modo Básico (2:00 AM)

```bash
python worker_reentrenamiento.py
```

### Hora Personalizada

```bash
# Reentrenar a las 3:30 AM
python worker_reentrenamiento.py --hour 3 --minute 30

# Reentrenar a medianoche
python worker_reentrenamiento.py --hour 0 --minute 0

# Reentrenar a las 1:15 AM
python worker_reentrenamiento.py --hour 1 --minute 15
```

## Cómo Funciona

1. **Inicialización**:
   - Conecta a SQL Server
   - Verifica que exista el directorio de modelos

2. **Monitoreo Continuo**:
   - Verifica cada minuto si es la hora de reentrenar
   - Cuando llega la hora configurada (default: 2:00 AM):
     - Lee todos los datos de `dbo.datos_proceso` desde SQL
     - Convierte los datos al formato requerido
     - Entrena modelos Prophet para todas las variables
     - Guarda los modelos en `pipeline/models/prophet/`
   - Evita reentrenar múltiples veces el mismo día

3. **Manejo de Errores**:
   - Si hay un error durante el reentrenamiento, lo registra y continúa
   - El worker sigue funcionando para el próximo día

## Ejemplo de Salida

```
================================================================================
WORKER DE REENTRENAMIENTO AUTOMÁTICO
================================================================================
[INFO] Hora de reentrenamiento: 02:00
[INFO] Verificando cada 60 segundos
[INFO] Presiona Ctrl+C para detener

[INFO] Conectando a SQL Server...
[OK] Conectado a SQL Server: 10.147.17.105

[2025-01-15 10:00:00] Esperando reentrenamiento... Próximo en 16h 0m (a las 02:00)

...

[2025-01-16 02:00:00] ¡Es hora de reentrenar!

================================================================================
INICIANDO REENTRENAMIENTO DE MODELOS
================================================================================

[INFO] Leyendo datos desde SQL Server...
  Filas leídas: 71,424
  Transformando a formato ancho...
  Dimensiones finales: 2,976 filas x 25 columnas
  Variables: 24

[INFO] Entrenando modelos para 24 variables

================================================================================
ENTRENANDO MODELOS
================================================================================
Entrenando modelo para variable: Variable1
...
[OK] Modelos guardados exitosamente en: pipeline/models/prophet
[OK] Total de modelos: 24

[OK] Reentrenamiento completado exitosamente (#1)

[2025-01-16 02:15:30] Reentrenamiento completado. Próximo: mañana a las 02:00
```

## Ejecutar como Servicio

### Windows - Task Scheduler

1. Abre "Programador de tareas" (Task Scheduler)
2. Crea una tarea básica
3. Configura:
   - **Trigger**: "Diariamente" a las 2:00 AM
   - **Acción**: Iniciar programa
   - **Programa**: `python`
   - **Argumentos**: `C:\ruta\a\version_argentina\worker_reentrenamiento.py`
   - **Iniciar en**: `C:\ruta\a\version_argentina`

**O mejor aún**: Ejecuta el worker continuamente (una sola vez) y él mismo manejará el horario:

1. **Trigger**: "Al iniciar sesión" o "Al iniciar el equipo"
2. El worker se ejecutará continuamente y reentrenará automáticamente a las 2:00 AM

### Windows - NSSM (Non-Sucking Service Manager)

```bash
nssm install ModelRetrainingWorker "C:\Python\python.exe" "C:\ruta\a\version_argentina\worker_reentrenamiento.py"
nssm set ModelRetrainingWorker AppDirectory "C:\ruta\a\version_argentina"
nssm start ModelRetrainingWorker
```

### Linux - systemd Service

Crea el archivo `/etc/systemd/system/model-retraining-worker.service`:

```ini
[Unit]
Description=Model Retraining Worker
After=network.target

[Service]
Type=simple
User=tu_usuario
WorkingDirectory=/ruta/a/version_argentina
ExecStart=/usr/bin/python3 /ruta/a/version_argentina/worker_reentrenamiento.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Luego:
```bash
sudo systemctl daemon-reload
sudo systemctl enable model-retraining-worker
sudo systemctl start model-retraining-worker
sudo systemctl status model-retraining-worker
```

## Recomendaciones

### Hora de Reentrenamiento

- **2:00 AM (default)**: Recomendado, generalmente hay menos actividad en el sistema
- **Medianoche (0:00)**: Si prefieres que se reentrene al inicio del día
- **3:00 AM o más tarde**: Si el sistema tiene procesos pesados a las 2:00 AM

### Consideraciones

1. **Tiempo de Reentrenamiento**: 
   - Depende de la cantidad de datos y variables
   - Puede tomar desde minutos hasta horas
   - El worker continúa funcionando durante el reentrenamiento

2. **Datos Nuevos**:
   - El reentrenamiento usa TODOS los datos disponibles en `dbo.datos_proceso`
   - Asegúrate de que los datos estén actualizados antes del reentrenamiento

3. **Modelos Anteriores**:
   - Los modelos antiguos se sobrescriben con los nuevos
   - Si quieres mantener backups, modifica el script para hacer backup antes de guardar

## Troubleshooting

### El worker no reentrena

- Verifica que el worker esté corriendo
- Verifica la hora del sistema
- Revisa los logs para ver errores

### Error durante el reentrenamiento

- Verifica que haya datos en `dbo.datos_proceso`
- Verifica la conexión a SQL Server
- Revisa los logs para ver el error específico

### El reentrenamiento tarda mucho

- Es normal, depende de la cantidad de datos
- Considera filtrar datos antiguos si no son necesarios
- Puedes ajustar los parámetros de Prophet para entrenamiento más rápido

## Integración con Worker de Procesamiento

Puedes ejecutar ambos workers simultáneamente:

1. **Worker de Procesamiento** (`worker_procesamiento.py`): Procesa datos nuevos cada 10 minutos
2. **Worker de Reentrenamiento** (`worker_reentrenamiento.py`): Reentrena modelos todos los días a las 2:00 AM

Ambos pueden ejecutarse en paralelo sin conflictos.

