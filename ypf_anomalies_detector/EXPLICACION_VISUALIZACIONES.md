# Explicación de Visualizaciones del Detector de Anomalías

## Archivos Generados

El script `generar_visualizaciones_detector.py` genera dos imágenes:

1. **`visualizacion_detector_anomalias.png`** - Dashboard completo con múltiples visualizaciones
2. **`tabla_anomalias_detectadas.png`** - Tabla detallada de las top 10 anomalías

## Contenido de las Visualizaciones

### Dashboard Principal (`visualizacion_detector_anomalias.png`)

#### 1. Gráfico de Serie Temporal con Anomalías (Panel Superior Izquierdo)
- **Banda gris azulada**: Intervalo de confianza del 95% (yhat_lower a yhat_upper)
- **Línea verde punteada**: Valores predichos por Prophet (yhat)
- **Puntos azules pequeños**: Valores reales normales (y)
- **Puntos rojos grandes**: Anomalías detectadas (tamaño proporcional al score)
- **Etiquetas amarillas**: Anomalías críticas (score > 80) con su score

**Uso en Front-end**: 
- Gráfico interactivo con zoom y pan
- Click en anomalía para ver detalles
- Selector de variable para cambiar entre variables monitoreadas

#### 2. Tabla de Resumen por Variable (Panel Superior Derecho)
- **Columnas**: Variable, N° Anomalías, Tasa %, Score Promedio, Score Máximo
- **Colores**:
  - Rojo: Variables con 4+ anomalías
  - Amarillo: Variables con 2-3 anomalías
  - Verde: Variables con 1 anomalía

**Uso en Front-end**:
- Tabla ordenable y filtrable
- Click en variable para ver su serie temporal
- Exportar a CSV/Excel

#### 3. Distribución de Scores de Anomalía (Panel Medio Derecho)
- **Histograma** con tres categorías:
  - Verde: Normal (0-50)
  - Naranja: Moderado (50-75)
  - Rojo: Crítico (75-100)
- **Líneas verticales**: Umbrales de categorización

**Uso en Front-end**:
- Filtro interactivo: click en barra para filtrar por rango
- Tooltip con cantidad de puntos en cada categoría

#### 4. Métricas Principales (Panel Inferior Izquierdo)
- **Cards** con 5 métricas clave:
  - Total Analizado
  - Anomalías Detectadas
  - Tasa de Anomalías (%)
  - Score Promedio
  - Score Máximo

**Uso en Front-end**:
- Cards animados con actualización en tiempo real
- Cambio de color según umbrales configurados

#### 5. Timeline de Anomalías (Panel Inferior Derecho)
- **Gráfico de barras** mostrando cantidad de anomalías por día
- **Colores**:
  - Rojo oscuro: Días con score promedio >= 80
  - Rojo: Días con score promedio 60-80
  - Salmón: Días con score promedio < 60

**Uso en Front-end**:
- Click en barra para ver detalle del día
- Zoom para ver períodos específicos

### Tabla Detallada de Anomalías (`tabla_anomalias_detectadas.png`)

- **Top 10 anomalías** ordenadas por score descendente
- **Columnas**:
  - Fecha/Hora
  - Variable
  - Valor Real
  - Valor Predicho
  - Score de Anomalía
  - Error Porcentual

- **Colores por fila**:
  - Rojo oscuro: Score >= 80 (crítico)
  - Naranja: Score 60-80 (moderado)
  - Amarillo: Score < 60 (bajo)

**Uso en Front-end**:
- Tabla paginada y filtrable
- Búsqueda por variable o fecha
- Exportar a CSV/Excel/PDF
- Click en fila para ver gráfico de contexto

## Estructura de Datos para Front-end

### API Response Sugerida

```json
{
  "summary": {
    "total_points": 80,
    "total_anomalies": 5,
    "anomaly_rate": 6.25,
    "avg_score": 87.6,
    "max_score": 100.0
  },
  "variables": [
    {
      "name": "PI_1412A",
      "n_anomalies": 5,
      "anomaly_rate": 0.0625,
      "avg_score": 87.6,
      "max_score": 100.0
    }
  ],
  "time_series": [
    {
      "ds": "2024-11-01T00:00:00",
      "y": 45.2,
      "yhat": 44.8,
      "yhat_lower": 42.1,
      "yhat_upper": 47.5,
      "is_anomaly": false,
      "anomaly_score": 5.2
    }
  ],
  "anomalies": [
    {
      "ds": "2024-11-01T03:00:00",
      "y": 52.8,
      "yhat": 45.4,
      "anomaly_score": 78.5,
      "variable": "PI_1412A"
    }
  ]
}
```

## Componentes Front-end Recomendados

1. **Gráfico de Serie Temporal**: Plotly.js o Recharts
2. **Tablas**: React Table o Material-UI Table
3. **Cards de Métricas**: Componentes personalizados con animaciones
4. **Filtros**: Material-UI o Ant Design
5. **Exportación**: jsPDF para PDF, xlsx para Excel

