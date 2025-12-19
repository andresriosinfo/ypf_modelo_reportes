"""Script para probar la configuración de bases de datos"""
from sql_utils import SQLConnection

SQL_CONFIG_INPUT = {
    'server': '10.147.17.241',
    'database': 'otms_main',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

SQL_CONFIG_OUTPUT = {
    'server': '10.147.17.241',
    'database': 'otms_analytics',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

print("="*80)
print("PRUEBA DE CONFIGURACIÓN DE BASES DE DATOS")
print("="*80)

# Probar conexión a base de datos de entrada
print("\n[1] Probando conexión a otms_main (entrada)...")
conn_input = SQLConnection(**SQL_CONFIG_INPUT)
if conn_input.connect():
    print("✓ Conexión exitosa a otms_main")
    
    # Verificar tabla de entrada
    query = "SELECT COUNT(*) as total FROM dbo.ypf_process_data"
    result = conn_input.execute_query(query)
    if result is not None:
        total = result['total'].iloc[0]
        print(f"✓ Tabla ypf_process_data encontrada: {total:,} registros")
    else:
        print("✗ No se pudo leer de ypf_process_data")
    
    conn_input.disconnect()
else:
    print("✗ Error conectando a otms_main")

# Probar conexión a base de datos de salida
print("\n[2] Probando conexión a otms_analytics (salida)...")
conn_output = SQLConnection(**SQL_CONFIG_OUTPUT)
if conn_output.connect():
    print("✓ Conexión exitosa a otms_analytics")
    
    # Verificar si existe la tabla
    query = """
        SELECT COUNT(*) as total 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ypf_anomaly_detector'
    """
    result = conn_output.execute_query(query)
    if result is not None and result['total'].iloc[0] > 0:
        print("✓ Tabla ypf_anomaly_detector existe")
        
        # Contar registros
        query = "SELECT COUNT(*) as total FROM dbo.ypf_anomaly_detector"
        result = conn_output.execute_query(query)
        if result is not None:
            total = result['total'].iloc[0]
            print(f"✓ Registros actuales en ypf_anomaly_detector: {total:,}")
            
            # Contar anomalías
            query = "SELECT COUNT(*) as total FROM dbo.ypf_anomaly_detector WHERE is_anomaly = 1"
            result = conn_output.execute_query(query)
            if result is not None:
                anomalies = result['total'].iloc[0]
                print(f"✓ Anomalías detectadas: {anomalies:,}")
    else:
        print("⚠ Tabla ypf_anomaly_detector no existe aún (se creará automáticamente)")
    
    conn_output.disconnect()
else:
    print("✗ Error conectando a otms_analytics")

print("\n" + "="*80)
print("PRUEBA COMPLETADA")
print("="*80)

