"""Script de prueba rápida para el worker"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from worker_procesamiento import AnomalyDetectionWorker

print("="*80)
print("PRUEBA DEL WORKER")
print("="*80)
print()

worker = AnomalyDetectionWorker(check_interval_minutes=1)
print("Worker creado. Inicializando...")
sys.stdout.flush()

if worker.initialize():
    print("\n✓ Worker inicializado correctamente")
    print(f"  - Modelos cargados: {len(worker.detector.models)}")
    print(f"  - Último datetime procesado: {worker.last_processed_datetime}")
    print("\nEjecutando una verificación...")
    sys.stdout.flush()
    
    processed = worker.check_and_process()
    if processed:
        print("✓ Se procesaron datos nuevos")
    else:
        print("ℹ No había datos nuevos para procesar")
    
    if worker.sql_conn:
        worker.sql_conn.disconnect()
        print("\n✓ Conexión cerrada")
else:
    print("✗ Error inicializando el worker")
    sys.exit(1)

print("\n" + "="*80)
print("PRUEBA COMPLETADA")
print("="*80)

