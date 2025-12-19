"""Script de prueba rápida para el worker de reentrenamiento"""
import sys
from pathlib import Path
from datetime import datetime
sys.path.append(str(Path(__file__).parent))

from worker_reentrenamiento import RetrainingWorker

print("="*80)
print("PRUEBA DEL WORKER DE REENTRENAMIENTO")
print("="*80)
print()
sys.stdout.flush()

# Crear worker con hora actual + 1 minuto para probar
now = datetime.now()
test_hour = now.hour
test_minute = (now.minute + 1) % 60

print(f"Worker configurado para reentrenar a las {test_hour:02d}:{test_minute:02d}")
print("(Para probar, se configuró para el minuto siguiente)")
print()

worker = RetrainingWorker(training_hour=test_hour, training_minute=test_minute)
print("Worker creado. Inicializando...")
sys.stdout.flush()

if worker.initialize():
    print("\n✓ Worker inicializado correctamente")
    print(f"  - Directorio de modelos: {worker.models_dir}")
    print(f"  - Hora de reentrenamiento: {worker.training_hour:02d}:{worker.training_minute:02d}")
    print("\nVerificando si es hora de reentrenar...")
    sys.stdout.flush()
    
    should_retrain = worker.should_retrain()
    print(f"  ¿Es hora de reentrenar? {should_retrain}")
    
    if worker.sql_conn:
        worker.sql_conn.disconnect()
        print("\n✓ Conexión cerrada")
else:
    print("✗ Error inicializando el worker")
    sys.exit(1)

print("\n" + "="*80)
print("PRUEBA COMPLETADA")
print("="*80)
print("\nPara ejecutar el worker completo:")
print("  python worker_reentrenamiento.py")
print("\nPara cambiar la hora de reentrenamiento:")
print("  python worker_reentrenamiento.py --hour 3 --minute 30")

