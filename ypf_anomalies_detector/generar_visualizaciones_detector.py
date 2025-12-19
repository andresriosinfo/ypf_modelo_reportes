"""
Script para generar visualizaciones del detector de anomalías
Muestra cómo se verían los resultados en un front-end
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import numpy as np
from datetime import datetime

# Configurar estilo
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except:
    plt.style.use('seaborn-darkgrid')
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 10

def crear_visualizaciones():
    """Genera todas las visualizaciones del detector de anomalías"""
    
    # Cargar datos de ejemplo
    df = pd.read_csv('ejemplo_anomalies_detected.csv', parse_dates=['ds'])
    summary = pd.read_csv('ejemplo_anomaly_summary.csv')
    
    # Crear figura con múltiples subplots
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
    
    # ============================================
    # 1. GRÁFICO PRINCIPAL: Serie Temporal con Anomalías
    # ============================================
    ax1 = fig.add_subplot(gs[0:2, 0])
    
    # Filtrar datos normales y anomalías
    normal_data = df[df['is_anomaly'] == False]
    anomaly_data = df[df['is_anomaly'] == True]
    
    # Dibujar intervalo de confianza (banda)
    ax1.fill_between(df['ds'], df['yhat_lower'], df['yhat_upper'], 
                     alpha=0.3, color='lightblue', label='Intervalo de Confianza (95%)')
    
    # Línea de predicción
    ax1.plot(df['ds'], df['yhat'], 'g--', linewidth=2, alpha=0.7, label='Predicción (yhat)')
    
    # Valores reales normales
    ax1.scatter(normal_data['ds'], normal_data['y'], 
               color='blue', s=20, alpha=0.6, label='Valores Normales', zorder=3)
    
    # Valores reales anómalos (tamaño según score)
    if len(anomaly_data) > 0:
        sizes = anomaly_data['anomaly_score'] * 3  # Escalar para visualización
        ax1.scatter(anomaly_data['ds'], anomaly_data['y'], 
                   color='red', s=sizes, alpha=0.8, 
                   edgecolors='darkred', linewidths=1.5,
                   label='Anomalías Detectadas', zorder=5)
        
        # Añadir etiquetas para anomalías críticas (score > 80)
        critical = anomaly_data[anomaly_data['anomaly_score'] > 80]
        for idx, row in critical.iterrows():
            ax1.annotate(f"Score: {row['anomaly_score']:.1f}", 
                        xy=(row['ds'], row['y']),
                        xytext=(10, 10), textcoords='offset points',
                        fontsize=8, color='darkred', fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
    
    ax1.set_xlabel('Fecha y Hora', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Valor de la Variable', fontsize=12, fontweight='bold')
    ax1.set_title('Detección de Anomalías - Variable: PI_1412A', 
                 fontsize=14, fontweight='bold', pad=20)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # ============================================
    # 2. TABLA DE RESUMEN POR VARIABLE
    # ============================================
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.axis('tight')
    ax2.axis('off')
    
    # Preparar datos para tabla
    table_data = summary[['variable', 'n_anomalies', 'anomaly_rate', 'avg_score', 'max_score']].copy()
    table_data['anomaly_rate'] = (table_data['anomaly_rate'] * 100).round(2).astype(str) + '%'
    table_data['avg_score'] = table_data['avg_score'].round(1)
    table_data['max_score'] = table_data['max_score'].round(1)
    table_data.columns = ['Variable', 'N° Anomalías', 'Tasa %', 'Score Prom.', 'Score Máx.']
    
    table = ax2.table(cellText=table_data.values,
                     colLabels=table_data.columns,
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])
    
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Colorear header
    for i in range(len(table_data.columns)):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear filas según número de anomalías
    for i in range(1, len(table_data) + 1):
        n_anom = summary.iloc[i-1]['n_anomalies']
        if n_anom >= 4:
            color = '#FF6B6B'  # Rojo para muchas anomalías
        elif n_anom >= 2:
            color = '#FFD93D'  # Amarillo para algunas
        else:
            color = '#95E1D3'  # Verde para pocas
        for j in range(len(table_data.columns)):
            table[(i, j)].set_facecolor(color)
    
    ax2.set_title('Resumen de Anomalías por Variable', 
                 fontsize=12, fontweight='bold', pad=10)
    
    # ============================================
    # 3. DISTRIBUCIÓN DE SCORES DE ANOMALÍA
    # ============================================
    ax3 = fig.add_subplot(gs[1, 1])
    
    # Categorizar scores
    normal_scores = df[df['anomaly_score'] < 50]['anomaly_score']
    moderate_scores = df[(df['anomaly_score'] >= 50) & (df['anomaly_score'] < 75)]['anomaly_score']
    critical_scores = df[df['anomaly_score'] >= 75]['anomaly_score']
    
    bins = np.arange(0, 101, 5)
    ax3.hist(normal_scores, bins=bins, alpha=0.6, color='green', label='Normal (0-50)', edgecolor='black')
    ax3.hist(moderate_scores, bins=bins, alpha=0.6, color='orange', label='Moderado (50-75)', edgecolor='black')
    ax3.hist(critical_scores, bins=bins, alpha=0.6, color='red', label='Crítico (75-100)', edgecolor='black')
    
    # Líneas verticales para umbrales
    ax3.axvline(x=50, color='orange', linestyle='--', linewidth=2, alpha=0.7)
    ax3.axvline(x=75, color='red', linestyle='--', linewidth=2, alpha=0.7)
    
    ax3.set_xlabel('Score de Anomalía', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Frecuencia', fontsize=11, fontweight='bold')
    ax3.set_title('Distribución de Scores de Anomalía', fontsize=12, fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    # ============================================
    # 4. MÉTRICAS PRINCIPALES (Dashboard Cards)
    # ============================================
    ax4 = fig.add_subplot(gs[2, 0])
    ax4.axis('off')
    
    # Calcular métricas
    total_points = len(df)
    total_anomalies = df['is_anomaly'].sum()
    anomaly_rate = (total_anomalies / total_points) * 100
    avg_score = df[df['is_anomaly']]['anomaly_score'].mean() if total_anomalies > 0 else 0
    max_score = df['anomaly_score'].max()
    
    # Crear "cards" de métricas
    metrics = [
        ('Total Analizado', f'{total_points:,}', '#3498db'),
        ('Anomalías Detectadas', f'{total_anomalies:,}', '#e74c3c'),
        ('Tasa de Anomalías', f'{anomaly_rate:.2f}%', '#f39c12'),
        ('Score Promedio', f'{avg_score:.1f}', '#9b59b6'),
        ('Score Máximo', f'{max_score:.1f}', '#1abc9c')
    ]
    
    y_positions = [0.8, 0.6, 0.4, 0.2, 0.0]
    for i, (label, value, color) in enumerate(metrics):
        # Fondo del card
        rect = Rectangle((i*0.19, y_positions[0]), 0.17, 0.15, 
                       facecolor=color, alpha=0.2, edgecolor=color, linewidth=2)
        ax4.add_patch(rect)
        
        # Valor
        ax4.text(i*0.19 + 0.085, y_positions[0] + 0.1, value, 
               ha='center', va='center', fontsize=16, fontweight='bold', color=color)
        
        # Label
        ax4.text(i*0.19 + 0.085, y_positions[0] + 0.03, label, 
               ha='center', va='center', fontsize=9, color='black')
    
    ax4.set_xlim(0, 1)
    ax4.set_ylim(0, 1)
    ax4.set_title('Métricas Principales del Detector', 
                 fontsize=12, fontweight='bold', pad=10)
    
    # ============================================
    # 5. TIMELINE DE ANOMALÍAS
    # ============================================
    ax5 = fig.add_subplot(gs[2, 1])
    
    # Agrupar anomalías por día
    df['date'] = df['ds'].dt.date
    anomalies_by_date = df[df['is_anomaly']].groupby('date').agg({
        'anomaly_score': ['count', 'mean']
    }).reset_index()
    anomalies_by_date.columns = ['date', 'count', 'avg_score']
    anomalies_by_date['date'] = pd.to_datetime(anomalies_by_date['date'])
    
    if len(anomalies_by_date) > 0:
        # Gráfico de barras
        bars = ax5.bar(anomalies_by_date['date'], anomalies_by_date['count'],
                      color='red', alpha=0.7, edgecolor='darkred', linewidth=1.5)
        
        # Colorear según score promedio
        for i, (bar, score) in enumerate(zip(bars, anomalies_by_date['avg_score'])):
            if score >= 80:
                bar.set_color('darkred')
            elif score >= 60:
                bar.set_color('red')
            else:
                bar.set_color('salmon')
        
        # Añadir etiquetas
        for i, (date, count, score) in enumerate(zip(anomalies_by_date['date'], 
                                                     anomalies_by_date['count'],
                                                     anomalies_by_date['avg_score'])):
            ax5.text(date, count + 0.1, f'{int(count)}', 
                   ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    ax5.set_xlabel('Fecha', fontsize=11, fontweight='bold')
    ax5.set_ylabel('Cantidad de Anomalías', fontsize=11, fontweight='bold')
    ax5.set_title('Timeline de Anomalías por Día', fontsize=12, fontweight='bold')
    ax5.grid(True, alpha=0.3, axis='y')
    ax5.tick_params(axis='x', rotation=45)
    
    # ============================================
    # TÍTULO GENERAL
    # ============================================
    fig.suptitle('Dashboard de Detección de Anomalías - Sistema Prophet', 
                fontsize=16, fontweight='bold', y=0.98)
    
    # Guardar imagen
    plt.savefig('visualizacion_detector_anomalias.png', dpi=300, bbox_inches='tight', 
               facecolor='white', edgecolor='none')
    print("[OK] Visualizacion guardada en: visualizacion_detector_anomalias.png")
    
    # También crear una visualización adicional: tabla detallada de anomalías
    crear_tabla_anomalias(df)
    
    plt.close()

def crear_tabla_anomalias(df):
    """Crea una tabla detallada de las anomalías detectadas"""
    
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('tight')
    ax.axis('off')
    
    # Filtrar solo anomalías y seleccionar columnas relevantes
    anomalies = df[df['is_anomaly'] == True].copy()
    anomalies = anomalies.sort_values('anomaly_score', ascending=False).head(10)
    
    # Preparar datos para tabla
    table_data = anomalies[['ds', 'variable', 'y', 'yhat', 'anomaly_score', 'prediction_error_pct']].copy()
    table_data['ds'] = table_data['ds'].dt.strftime('%Y-%m-%d %H:%M')
    table_data['y'] = table_data['y'].round(2)
    table_data['yhat'] = table_data['yhat'].round(2)
    table_data['anomaly_score'] = table_data['anomaly_score'].round(1)
    table_data['prediction_error_pct'] = table_data['prediction_error_pct'].round(2)
    
    table_data.columns = ['Fecha/Hora', 'Variable', 'Valor Real', 'Valor Predicho', 
                         'Score Anomalía', 'Error %']
    
    table = ax.table(cellText=table_data.values,
                    colLabels=table_data.columns,
                    cellLoc='center',
                    loc='center',
                    bbox=[0, 0, 1, 1])
    
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2.5)
    
    # Colorear header
    for i in range(len(table_data.columns)):
        table[(0, i)].set_facecolor('#2C3E50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear filas según score
    for i in range(1, len(table_data) + 1):
        score = anomalies.iloc[i-1]['anomaly_score']
        if score >= 80:
            color = '#E74C3C'  # Rojo oscuro
        elif score >= 60:
            color = '#F39C12'  # Naranja
        else:
            color = '#F7DC6F'  # Amarillo
        
        for j in range(len(table_data.columns)):
            table[(i, j)].set_facecolor(color)
            if score >= 80:
                table[(i, j)].set_text_props(weight='bold')
    
    ax.set_title('Top 10 Anomalías Detectadas (Ordenadas por Score)', 
                fontsize=14, fontweight='bold', pad=20)
    
    plt.savefig('tabla_anomalias_detectadas.png', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    print("[OK] Tabla de anomalias guardada en: tabla_anomalias_detectadas.png")
    plt.close()

if __name__ == '__main__':
    print("Generando visualizaciones del detector de anomalías...")
    print("=" * 60)
    crear_visualizaciones()
    print("=" * 60)
    print("[OK] Proceso completado!")

