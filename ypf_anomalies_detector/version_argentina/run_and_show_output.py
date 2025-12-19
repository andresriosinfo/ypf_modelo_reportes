"""
Script auxiliar para ejecutar otros scripts y mostrar su salida
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_name):
    """Ejecuta un script y muestra su salida"""
    print(f"\n{'='*80}")
    print(f"EJECUTANDO: {script_name}")
    print(f"{'='*80}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
        
        print(f"\nExit code: {result.returncode}")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error ejecutando {script_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    scripts = [
        'write_training_data_to_sql.py',
        'train_from_sql.py',
        'detect_from_sql.py'
    ]
    
    for script in scripts:
        if not run_script(script):
            print(f"\n[ERROR] {script} falló. Deteniendo pipeline.")
            sys.exit(1)
        print("\n" + "="*80 + "\n")
    
    print("PIPELINE COMPLETADO EXITOSAMENTE")


