"""Script para verificar si hay resultados de detección de anomalías en la base de datos"""
from sql_utils import SQLConnection

SQL_CONFIG = {
    'server': '10.147.17.241',
    'database': 'otms_analytics',
    'username': 'sa',
    'password': 'OtmsSecure2024Dev123',
    'port': 1433
}

print("Verificando resultados de detección de anomalías...")
conn = SQLConnection(**SQL_CONFIG)
if conn.connect():
    # Verificar si existe la tabla
    query = """
        SELECT COUNT(*) as total 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ypf_anomaly_detector'
    """
    result = conn.execute_query(query)
    if result is not None and result['total'].iloc[0] > 0:
        print("✓ Tabla ypf_anomaly_detector existe")
        
        # Contar registros
        query = "SELECT COUNT(*) as total FROM dbo.ypf_anomaly_detector"
        result = conn.execute_query(query)
        if result is not None:
            total = result['total'].iloc[0]
            print(f"✓ Total de registros: {total:,}")
            
            # Contar anomalías
            query = "SELECT COUNT(*) as total FROM dbo.ypf_anomaly_detector WHERE is_anomaly = 1"
            result = conn.execute_query(query)
            if result is not None:
                anomalies = result['total'].iloc[0]
                print(f"✓ Anomalías detectadas: {anomalies:,}")
                if total > 0:
                    print(f"✓ Tasa de anomalías: {anomalies/total*100:.2f}%")
            
            # Mostrar algunas anomalías
            query = """
                SELECT TOP 5 ds, variable, y, yhat, anomaly_score, is_anomaly 
                FROM dbo.ypf_anomaly_detector 
                WHERE is_anomaly = 1 
                ORDER BY anomaly_score DESC
            """
            result = conn.execute_query(query)
            if result is not None and len(result) > 0:
                print(f"\nTop 5 anomalías (por score):")
                print(result.to_string(index=False))
    else:
        print("✗ La tabla ypf_anomaly_detector no existe aún")
        print("  Ejecuta: python detect_from_sql.py")
    
    conn.disconnect()
else:
    print("✗ Error de conexión")

