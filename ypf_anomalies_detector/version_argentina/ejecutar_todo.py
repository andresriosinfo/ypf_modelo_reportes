"""
Ejecuta todo el pipeline y guarda la salida
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

def run_with_output(script_name):
    """Ejecuta script y retorna salida"""
    print(f"Ejecutando {script_name}...")
    sys.stdout.flush()
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), 1

# Archivo de log
log_file = Path("pipeline_log.txt")

with open(log_file, 'w', encoding='utf-8') as f:
    f.write(f"Pipeline ejecutado: {datetime.now()}\n")
    f.write("="*80 + "\n\n")
    
    scripts = [
        ("write_training_data_to_sql.py", "Paso 1: Escribir datos a SQL"),
        ("train_from_sql.py", "Paso 2: Entrenar modelos"),
        ("detect_from_sql.py", "Paso 3: Detectar anomalías")
    ]
    
    for script, desc in scripts:
        f.write(f"\n{'='*80}\n")
        f.write(f"{desc}\n")
        f.write(f"{'='*80}\n\n")
        f.flush()
        
        stdout, stderr, code = run_with_output(script)
        
        f.write(stdout)
        if stderr:
            f.write("\nSTDERR:\n")
            f.write(stderr)
        f.write(f"\nExit code: {code}\n")
        f.flush()
        
        print(f"{desc}: Exit code {code}")
        if code != 0:
            print(f"ERROR en {script}")
            print(f"Revisa {log_file} para más detalles")
        else:
            print(f"OK: {script}")

print(f"\nLog completo guardado en: {log_file.absolute()}")


