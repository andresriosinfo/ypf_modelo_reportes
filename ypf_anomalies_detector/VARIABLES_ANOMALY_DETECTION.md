# Protocolo de Selección de Variables para Detección de Anomalías con ML

## Resumen Ejecutivo

Este documento describe el protocolo implementado para identificar y seleccionar variables de proceso adecuadas para sistemas de detección de anomalías basados en Machine Learning.

## Variables Adecuadas para Detección de Anomalías

### Criterios de Selección

Una variable es adecuada para detección de anomalías con ML si cumple los siguientes criterios:

1. **Variabilidad Suficiente**
   - Coeficiente de variación (CV = std/mean) entre 0.01 y 2.0
   - No es constante ni casi constante
   - Muestra cambios significativos en el tiempo

2. **Calidad de Datos**
   - Menos del 30% de valores faltantes
   - No es una variable binaria pura (0/1)
   - Tiene suficientes valores únicos para capturar patrones

3. **Relevancia del Proceso**
   - Variable continua (no discreta)
   - Relacionada con operaciones críticas del proceso
   - Puede indicar estados anómalos cuando se desvía de lo normal

### Categorías de Variables Identificadas

#### 1. Variables de Temperatura
- **Prefijos**: `TE_`, `TI_`, `TIT_`
- **Ejemplos**: `TE_1412A`, `TI_1432B`, `TIT_1440`
- **Adecuadas para ML**: ✅ Sí
- **Razón**: Las temperaturas son variables continuas críticas que pueden indicar problemas de proceso

#### 2. Variables de Presión
- **Prefijos**: `PI_`, `PIT_`
- **Ejemplos**: `PI_1412A`, `PIT_1432B`
- **Adecuadas para ML**: ✅ Sí
- **Razón**: Las presiones son indicadores clave de estado del proceso

#### 3. Variables de Flujo
- **Prefijos**: `FI_`, `FIC_`
- **Ejemplos**: `FI_1412A`, `FIC_1432B`
- **Adecuadas para ML**: ✅ Sí
- **Razón**: Los flujos muestran actividad del proceso y pueden detectar bloqueos o fugas

#### 4. Variables de Nivel
- **Prefijos**: `LI_`, `LIT_`
- **Ejemplos**: `LI_1412A`, `LIT_1432B`
- **Adecuadas para ML**: ✅ Sí
- **Razón**: Los niveles indican estado de tanques y pueden detectar desbordamientos o vacíos

#### 5. Variables de Control
- **Prefijos**: `BPC_`, `SIC_`
- **Ejemplos**: `BPC_1410_P`, `SIC_1442A_SP`
- **Adecuadas para ML**: ✅ Sí (con cuidado)
- **Razón**: Variables de control pueden tener patrones complejos, útiles para detectar desajustes

#### 6. Variables de Proceso (BB, BL)
- **Prefijos**: `BB_`, `BL_`
- **Ejemplos**: `BB_1410_HAD`, `BL_1421A_ON`
- **Adecuadas para ML**: ✅ Sí
- **Razón**: Variables operacionales que reflejan estado del proceso

#### 7. Variables Binarias (0/1)
- **Prefijos**: Varios (alarmas, estados ON/OFF)
- **Adecuadas para ML**: ❌ No (para modelos continuos)
- **Razón**: Variables binarias puras no aportan información suficiente para modelos de anomalías continuas. Pueden usarse como features adicionales pero no como variables principales.

#### 8. Variables de Guía/Control (GUA)
- **Prefijos**: `GUA_`
- **Adecuadas para ML**: ⚠️ Depende
- **Razón**: Muchas variables GUA pueden ser binarias o tener baja variabilidad. Requieren análisis individual.

## Protocolo de Limpieza

### 1. Manejo de Valores Faltantes

**Método recomendado**: `forward_fill` (rellenar hacia adelante)
- Preserva la continuidad temporal
- Apropiado para datos de proceso con muestreo regular
- Alternativas: `interpolate` (interpolación lineal) para datos más complejos

### 2. Manejo de Outliers

**Método recomendado**: Detección IQR (Interquartile Range)
- Identifica valores fuera de Q1 - 3×IQR y Q3 + 3×IQR
- Reemplaza outliers extremos con valores imputados
- Preserva la estructura temporal de los datos

### 3. Normalización

**Recomendación**: Normalizar solo si se usan algoritmos sensibles a escala
- Algoritmos como Isolation Forest no requieren normalización
- Autoencoders y PCA sí se benefician de normalización
- Usar StandardScaler (media=0, std=1)

## Algoritmos de ML Recomendados

### 1. Isolation Forest
- ✅ No requiere normalización
- ✅ Maneja bien datos con muchas variables
- ✅ Rápido y escalable
- ⚠️ Puede tener problemas con variables muy correlacionadas

### 2. Autoencoders (Deep Learning)
- ✅ Captura patrones complejos y no lineales
- ✅ Requiere normalización
- ✅ Necesita más datos para entrenar
- ⚠️ Computacionalmente más costoso

### 3. One-Class SVM
- ✅ Efectivo para datos con estructura clara
- ✅ Requiere normalización
- ⚠️ No escala bien con muchas variables

### 4. Local Outlier Factor (LOF)
- ✅ Detecta outliers locales
- ✅ Útil para datos con clusters
- ⚠️ Computacionalmente costoso con muchos datos

## Uso del Protocolo

### Ejecución Básica

```python
from variable_selection_protocol import AnomalyDetectionVariableProtocol

# Inicializar protocolo
protocol = AnomalyDetectionVariableProtocol(
    min_suitability_score=60.0,
    max_missing_pct=0.3,
    min_cv=0.01,
    handle_missing='forward_fill',
    remove_outliers=True
)

# Procesar un archivo
cleaned_data, report = protocol.process_file('2025-01.xlsx')
```

### Procesamiento Múltiple

```python
# Procesar múltiples archivos y encontrar variables comunes
files = ['2025-01.xlsx', '2024-12.xlsx', '2024-11.xlsx']
results = protocol.process_multiple_files(files, output_dir='output')
```

### Ejecución desde Línea de Comandos

```bash
python run_protocol.py
```

## Estructura de Salida

El protocolo genera:

1. **Datos Limpios** (`[archivo]_cleaned.csv`)
   - Variables seleccionadas
   - Datos preprocesados
   - Listos para entrenar modelos ML

2. **Reporte de Selección** (`[archivo]_variable_selection_report.csv`)
   - Score de adecuación por variable
   - Estadísticas (CV, missing %, etc.)
   - Razones de exclusión

3. **Variables Recomendadas** (`recommended_variables.txt`)
   - Lista de variables comunes en múltiples archivos
   - Categorizadas por tipo
   - Ideales para modelos unificados

## Mejores Prácticas

1. **Selección de Variables**
   - Priorizar variables con score >= 70
   - Usar variables comunes en múltiples períodos
   - Balancear entre diferentes tipos de variables

2. **Preprocesamiento**
   - Siempre manejar valores faltantes
   - Considerar el contexto del proceso al manejar outliers
   - Documentar todas las transformaciones

3. **Validación**
   - Validar en múltiples períodos temporales
   - Verificar que las anomalías detectadas sean reales
   - Ajustar umbrales según feedback del dominio

4. **Monitoreo**
   - Re-entrenar modelos periódicamente
   - Monitorear drift en las distribuciones
   - Actualizar variables seleccionadas si el proceso cambia

## Ejemplo de Variables Seleccionadas

Basado en el análisis de los archivos, estas son categorías de variables típicamente seleccionadas:

- **Temperaturas críticas**: ~50-100 variables
- **Presiones de proceso**: ~30-50 variables
- **Flujos principales**: ~40-60 variables
- **Niveles de tanques**: ~20-30 variables
- **Variables de control**: ~50-80 variables
- **Variables operacionales (BB/BL)**: ~100-200 variables

**Total típico**: 300-500 variables de ~2000-2600 disponibles

## Notas Importantes

- El número de variables seleccionadas puede variar según el período
- Variables con nombres similares (ej: `PI_1412A` vs `PIT_1412A`) pueden tener comportamientos diferentes
- Siempre validar con expertos del dominio antes de implementar en producción
- Considerar la correlación entre variables al seleccionar el conjunto final

