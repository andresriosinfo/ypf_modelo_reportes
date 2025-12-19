"""
Ejecuta el pipeline completo y guarda la salida en un archivo de log
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

log_file = Path("pipeline_execution.log")

def run_script(script_name, step_name):
    """Ejecuta un script y captura la salida"""
    print(f"\n{'='*80}")
    print(f"EJECUTANDO: {step_name}")
    print(f"{'='*80}\n")
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {step_name}\n")
        f.write(f"{'='*80}\n")
        f.flush()
        
        try:
            result = subprocess.run(
                [sys.executable, script_name],
                cwd=Path(__file__).parent,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8'
            )
            
            output = result.stdout
            print(output)
            f.write(output)
            f.write(f"\nExit code: {result.returncode}\n")
            f.flush()
            
            if result.returncode != 0:
                print(f"\n[ERROR] {step_name} falló con código {result.returncode}")
                return False
            else:
                print(f"\n[OK] {step_name} completado exitosamente")
                return True
                
        except Exception as e:
            error_msg = f"Error ejecutando {script_name}: {str(e)}\n"
            print(error_msg)
            f.write(error_msg)
            import traceback
            f.write(traceback.format_exc())
            return False

if __name__ == '__main__':
    # Limpiar log anterior
    if log_file.exists():
        log_file.unlink()
    
    print("="*80)
    print("EJECUTANDO PIPELINE COMPLETO CON SQL SERVER")
    print("="*80)
    print(f"Log guardado en: {log_file.absolute()}")
    
    scripts = [
        ("write_training_data_to_sql.py", "Paso 1: Escribir datos de entrenamiento a SQL"),
        ("train_from_sql.py", "Paso 2: Entrenar modelos desde SQL"),
        ("detect_from_sql.py", "Paso 3: Detectar anomalías y escribir resultados")
    ]
    
    all_success = True
    
    for script, step_name in scripts:
        if not run_script(script, step_name):
            all_success = False
            break
    
    print("\n" + "="*80)
    if all_success:
        print("PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*80)
        print(f"\nVerifica las tablas en SQL Server:")
        print("  - dbo.datos_proceso (datos de entrenamiento)")
        print("  - dbo.anomalies_detector (resultados de detección)")
    else:
        print("PIPELINE FALLÓ")
        print("="*80)
        print(f"\nRevisa el log en: {log_file.absolute()}")
    
    print(f"\nLog completo disponible en: {log_file.absolute()}")


