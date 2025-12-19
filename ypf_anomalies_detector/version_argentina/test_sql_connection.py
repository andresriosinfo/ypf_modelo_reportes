"""
Script de prueba para verificar conexión a SQL Server
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

try:
    from sql_utils import SQLConnection
    
    SQL_CONFIG = {
        'server': '10.147.17.105',
        'database': 'ypf_ai_pilot',
        'username': 'sa',
        'password': 'Gnomes1*.',
        'port': 14333
    }
    
    print("="*80)
    print("PRUEBA DE CONEXIÓN A SQL SERVER")
    print("="*80)
    
    sql_conn = SQLConnection(**SQL_CONFIG)
    if sql_conn.connect():
        print("\n[OK] Conexión exitosa!")
        
        # Probar una consulta simple
        result = sql_conn.execute_query("SELECT @@VERSION as version")
        if result is not None and len(result) > 0:
            print(f"[OK] Versión de SQL Server: {result['version'].iloc[0][:50]}...")
        
        sql_conn.disconnect()
        print("\n[OK] Conexión cerrada correctamente")
    else:
        print("\n[ERROR] No se pudo conectar")
        sys.exit(1)
        
except ImportError as e:
    print(f"\n[ERROR] Error importando módulos: {e}")
    print("Instala las dependencias: pip install pyodbc pandas")
    sys.exit(1)
except Exception as e:
    print(f"\n[ERROR] Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)


