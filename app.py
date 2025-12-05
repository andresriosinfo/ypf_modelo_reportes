import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

# Configuración de la página
st.set_page_config(
    page_title="Informe de Turno - Unidad N-101",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos personalizados inspirados en Schneider Electric
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #00A859;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #1D1D1B;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .kpi-card {
        background-color: #F5F5F5;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #00A859;
        margin-bottom: 1rem;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1D1D1B;
    }
    .warning-box {
        background-color: #FFF3CD;
        border-left: 4px solid #FFC107;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #D1ECF1;
        border-left: 4px solid #0C5460;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)


def generate_dummy_data():
    """Genera datos dummy para la aplicación"""
    
    # Datos de resumen del turno
    df_resumen = pd.DataFrame({
        'Variable': ['Flujo F-101', 'Nivel L-201', 'Presión P-301', 'Temperatura T-401', 
                     'Flujo F-102', 'Nivel L-202', 'Presión P-302', 'Temperatura T-402'],
        'Valor_Actual': [1250.5, 68.3, 2.45, 185.2, 980.3, 45.7, 1.89, 172.5],
        'Valor_Objetivo': [1200.0, 70.0, 2.5, 180.0, 1000.0, 50.0, 2.0, 175.0],
        'Desvío_%': [4.2, -2.4, -2.0, 2.9, -2.0, -8.6, -5.5, -1.4],
        'Estado': ['Normal', 'Normal', 'Normal', 'Atención', 'Normal', 'Atención', 'Atención', 'Normal'],
        'Tipo': ['Flujo', 'Nivel', 'Presión', 'Temperatura', 'Flujo', 'Nivel', 'Presión', 'Temperatura']
    })
    
    # KPIs del turno
    kpis = {
        'Producción_Total': 12500.5,
        'Eficiencia': 94.2,
        'Tiempo_Activo': 7.8,
        'Paradas': 2,
        'Energía_Consumida': 12500.3,
        'Calidad_Producto': 98.5
    }
    
    # Series temporales para variables
    np.random.seed(42)
    horas = pd.date_range(start='2024-10-01 00:00', periods=48, freq='10min')
    
    series_data = {}
    variables = ['Flujo F-101', 'Nivel L-201', 'Presión P-301', 'Temperatura T-401']
    
    for var in variables:
        base_value = df_resumen[df_resumen['Variable'] == var]['Valor_Actual'].values[0]
        noise = np.random.normal(0, base_value * 0.05, len(horas))
        trend = np.linspace(0, base_value * 0.1, len(horas))
        values = base_value + noise + trend
        series_data[var] = pd.DataFrame({
            'Timestamp': horas,
            'Valor': values,
            'Variable': var
        })
    
    # Recomendaciones / Puntos a vigilar
    recomendaciones = [
        {
            'Prioridad': 'Alta',
            'Variable': 'Nivel L-202',
            'Descripción': 'El nivel está 8.6% por debajo del objetivo. Revisar válvulas de entrada.',
            'Acción': 'Ajustar válvula V-202 y verificar bomba B-201',
            'Tiempo_Estimado': '15 min'
        },
        {
            'Prioridad': 'Media',
            'Variable': 'Presión P-302',
            'Descripción': 'Presión 5.5% por debajo del objetivo. Posible fuga o desgaste.',
            'Acción': 'Inspección visual del sistema de presión P-302',
            'Tiempo_Estimado': '30 min'
        },
        {
            'Prioridad': 'Media',
            'Variable': 'Temperatura T-401',
            'Descripción': 'Temperatura 2.9% por encima del objetivo. Verificar sistema de enfriamiento.',
            'Acción': 'Revisar válvula de control de temperatura y intercambiador',
            'Tiempo_Estimado': '20 min'
        },
        {
            'Prioridad': 'Baja',
            'Variable': 'Flujo F-101',
            'Descripción': 'Flujo 4.2% por encima del objetivo. Monitorear continuamente.',
            'Acción': 'Ajuste fino de válvula de control F-101',
            'Tiempo_Estimado': '10 min'
        }
    ]
    
    return df_resumen, kpis, series_data, recomendaciones


def render_resumen_turno(df_resumen, kpis):
    """Renderiza el dashboard de resumen del turno"""
    
    st.markdown('<div class="main-header">Resumen del Turno</div>', unsafe_allow_html=True)
    
    # KPIs principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Producción Total (kg)",
            value=f"{kpis['Producción_Total']:,.1f}",
            delta=f"{kpis['Producción_Total'] * 0.02:,.1f}"
        )
    
    with col2:
        st.metric(
            label="Eficiencia (%)",
            value=f"{kpis['Eficiencia']:.1f}%",
            delta=f"{kpis['Eficiencia'] - 92:.1f}%"
        )
    
    with col3:
        st.metric(
            label="Tiempo Activo (h)",
            value=f"{kpis['Tiempo_Activo']:.1f}",
            delta=f"-{8 - kpis['Tiempo_Activo']:.1f}h"
        )
    
    with col4:
        st.metric(
            label="Paradas",
            value=f"{kpis['Paradas']}",
            delta=f"-{kpis['Paradas'] - 3}"
        )
    
    st.markdown('<div class="sub-header">Estado de Variables Principales</div>', unsafe_allow_html=True)
    
    # Tabla de resumen con formato
    df_display = df_resumen[['Variable', 'Valor_Actual', 'Valor_Objetivo', 'Desvío_%', 'Estado']].copy()
    df_display['Desvío_%'] = df_display['Desvío_%'].apply(lambda x: f"{x:.2f}%")
    
    # Aplicar colores según estado
    def color_estado(val):
        if val == 'Atención':
            return 'background-color: #FFF3CD'
        elif val == 'Normal':
            return 'background-color: #D4EDDA'
        else:
            return ''
    
    st.dataframe(
        df_display.style.applymap(color_estado, subset=['Estado']),
        use_container_width=True,
        hide_index=True
    )
    
    # Gráfico de barras de desvíos
    st.markdown('<div class="sub-header">Desvíos respecto al Objetivo</div>', unsafe_allow_html=True)
    
    fig = go.Figure()
    
    df_normal = df_resumen[df_resumen['Estado'] == 'Normal']
    df_atencion = df_resumen[df_resumen['Estado'] == 'Atención']
    
    if len(df_normal) > 0:
        fig.add_trace(go.Bar(
            x=df_normal['Variable'],
            y=df_normal['Desvío_%'],
            name='Normal',
            marker_color='#00A859'
        ))
    
    if len(df_atencion) > 0:
        fig.add_trace(go.Bar(
            x=df_atencion['Variable'],
            y=df_atencion['Desvío_%'],
            name='Atención',
            marker_color='#FFC107'
        ))
    
    fig.update_layout(
        title='Desvíos porcentuales respecto al objetivo',
        xaxis_title='Variable',
        yaxis_title='Desvío (%)',
        height=400,
        template='plotly_white',
        showlegend=True
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_variables(df_variables, series_data, filtros):
    """Renderiza la exploración de variables y series temporales"""
    
    st.markdown('<div class="main-header">Variables y Series Temporales</div>', unsafe_allow_html=True)
    
    # Aplicar filtros
    df_filtrado = df_variables.copy()
    
    if filtros['tipos_variable']:
        df_filtrado = df_filtrado[df_filtrado['Tipo'].isin(filtros['tipos_variable'])]
    
    if filtros['solo_bien_comportadas']:
        df_filtrado = df_filtrado[df_filtrado['Estado'] == 'Normal']
    
    if filtros['solo_desvios']:
        df_filtrado = df_filtrado[df_filtrado['Estado'] == 'Atención']
    
    # Selector de variable para visualización detallada
    variables_disponibles = df_filtrado['Variable'].tolist()
    
    if variables_disponibles:
        var_seleccionada = st.selectbox(
            "Seleccione una variable para visualizar su serie temporal:",
            variables_disponibles
        )
        
        # Mostrar información de la variable seleccionada
        var_info = df_filtrado[df_filtrado['Variable'] == var_seleccionada].iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Valor Actual", f"{var_info['Valor_Actual']:.2f}")
        with col2:
            st.metric("Valor Objetivo", f"{var_info['Valor_Objetivo']:.2f}")
        with col3:
            st.metric("Desvío", f"{var_info['Desvío_%']:.2f}%")
        with col4:
            estado_color = "#00A859" if var_info['Estado'] == 'Normal' else "#FFC107"
            st.markdown(f'<div style="text-align: center;"><span style="color: {estado_color}; font-weight: bold;">{var_info["Estado"]}</span></div>', unsafe_allow_html=True)
        
        # Gráfico de serie temporal
        if var_seleccionada in series_data:
            df_serie = series_data[var_seleccionada]
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_serie['Timestamp'],
                y=df_serie['Valor'],
                mode='lines',
                name=var_seleccionada,
                line=dict(color='#00A859', width=2)
            ))
            
            # Línea de objetivo
            valor_objetivo = var_info['Valor_Objetivo']
            fig.add_hline(
                y=valor_objetivo,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Objetivo: {valor_objetivo:.2f}",
                annotation_position="right"
            )
            
            fig.update_layout(
                title=f'Serie temporal: {var_seleccionada}',
                xaxis_title='Tiempo',
                yaxis_title='Valor',
                height=500,
                template='plotly_white',
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Estadísticas de la serie
        if var_seleccionada in series_data:
            df_serie = series_data[var_seleccionada]
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Mínimo", f"{df_serie['Valor'].min():.2f}")
            with col2:
                st.metric("Máximo", f"{df_serie['Valor'].max():.2f}")
            with col3:
                st.metric("Promedio", f"{df_serie['Valor'].mean():.2f}")
            with col4:
                st.metric("Desviación Estándar", f"{df_serie['Valor'].std():.2f}")
    
    # Tabla de todas las variables filtradas
    st.markdown('<div class="sub-header">Tabla de Variables</div>', unsafe_allow_html=True)
    
    df_display = df_filtrado[['Variable', 'Tipo', 'Valor_Actual', 'Valor_Objetivo', 'Desvío_%', 'Estado']].copy()
    df_display['Desvío_%'] = df_display['Desvío_%'].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Gráfico comparativo de todas las variables
    if len(df_filtrado) > 0:
        st.markdown('<div class="sub-header">Comparación de Variables</div>', unsafe_allow_html=True)
        
        fig = go.Figure()
        
        for idx, row in df_filtrado.iterrows():
            color = '#00A859' if row['Estado'] == 'Normal' else '#FFC107'
            fig.add_trace(go.Bar(
                x=[row['Variable']],
                y=[row['Valor_Actual']],
                name=row['Variable'],
                marker_color=color,
                showlegend=False
            ))
        
        fig.update_layout(
            title='Valores actuales de las variables',
            xaxis_title='Variable',
            yaxis_title='Valor',
            height=400,
            template='plotly_white'
        )
        
        st.plotly_chart(fig, use_container_width=True)


def render_puntos_vigilar(recomendaciones):
    """Renderiza la vista de puntos a vigilar / recomendaciones"""
    
    st.markdown('<div class="main-header">Puntos a Vigilar / Recomendaciones</div>', unsafe_allow_html=True)
    
    # Filtrar por prioridad
    prioridad_filtro = st.selectbox(
        "Filtrar por prioridad:",
        ["Todas", "Alta", "Media", "Baja"]
    )
    
    recomendaciones_filtradas = recomendaciones
    if prioridad_filtro != "Todas":
        recomendaciones_filtradas = [r for r in recomendaciones if r['Prioridad'] == prioridad_filtro]
    
    if not recomendaciones_filtradas:
        st.info("No hay recomendaciones para la prioridad seleccionada.")
        return
    
    # Mostrar cada recomendación
    for rec in recomendaciones_filtradas:
        prioridad_color = {
            'Alta': '#DC3545',
            'Media': '#FFC107',
            'Baja': '#17A2B8'
        }
        
        color = prioridad_color.get(rec['Prioridad'], '#6C757D')
        
        with st.container():
            st.markdown(f"""
                <div style="
                    border-left: 4px solid {color};
                    padding: 1rem;
                    margin: 1rem 0;
                    background-color: #F8F9FA;
                    border-radius: 4px;
                ">
                    <h3 style="color: {color}; margin-top: 0;">
                        {rec['Variable']} - Prioridad: {rec['Prioridad']}
                    </h3>
                    <p><strong>Descripción:</strong> {rec['Descripción']}</p>
                    <p><strong>Acción recomendada:</strong> {rec['Acción']}</p>
                    <p><strong>Tiempo estimado:</strong> {rec['Tiempo_Estimado']}</p>
                </div>
            """, unsafe_allow_html=True)
    
    # Resumen estadístico
    st.markdown('<div class="sub-header">Resumen de Recomendaciones</div>', unsafe_allow_html=True)
    
    df_rec = pd.DataFrame(recomendaciones_filtradas)
    conteo_prioridad = df_rec['Prioridad'].value_counts()
    
    fig = go.Figure(data=[
        go.Bar(
            x=conteo_prioridad.index,
            y=conteo_prioridad.values,
            marker_color=['#DC3545', '#FFC107', '#17A2B8']
        )
    ])
    
    fig.update_layout(
        title='Distribución de recomendaciones por prioridad',
        xaxis_title='Prioridad',
        yaxis_title='Cantidad',
        height=300,
        template='plotly_white'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_descargas(df_resumen, kpis, series_data, recomendaciones):
    """Renderiza la sección de descarga de informes y datos"""
    
    st.markdown('<div class="main-header">Descarga de Informes y Datos</div>', unsafe_allow_html=True)
    
    # Información del informe
    st.markdown('<div class="sub-header">Información del Informe</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Fecha de generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        st.info(f"**Unidad:** N-101")
    
    # Descarga de datos en CSV
    st.markdown('<div class="sub-header">Descargar Datos</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # CSV de resumen
        csv_resumen = df_resumen.to_csv(index=False)
        st.download_button(
            label="Descargar Resumen (CSV)",
            data=csv_resumen,
            file_name=f"resumen_turno_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with col2:
        # CSV de series temporales
        if series_data:
            # Combinar todas las series en un solo DataFrame
            df_series_combinado = pd.concat(series_data.values(), ignore_index=True)
            csv_series = df_series_combinado.to_csv(index=False)
            st.download_button(
                label="Descargar Series Temporales (CSV)",
                data=csv_series,
                file_name=f"series_temporales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Descarga de JSON
    st.markdown('<div class="sub-header">Descargar Informe Completo (JSON)</div>', unsafe_allow_html=True)
    
    informe_completo = {
        'fecha_generacion': datetime.now().isoformat(),
        'unidad': 'N-101',
        'kpis': kpis,
        'resumen_variables': df_resumen.to_dict('records'),
        'recomendaciones': recomendaciones,
        'series_temporales': {
            var: df.to_dict('records') for var, df in series_data.items()
        }
    }
    
    json_str = json.dumps(informe_completo, indent=2, default=str)
    st.download_button(
        label="Descargar Informe Completo (JSON)",
        data=json_str,
        file_name=f"informe_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json"
    )
    
    # Información sobre PDF (simulado)
    st.markdown('<div class="sub-header">Descarga de Informe en PDF</div>', unsafe_allow_html=True)
    
    st.warning("""
    **Nota:** La generación de PDF requiere librerías adicionales (reportlab, weasyprint, etc.).
    Por ahora, puede descargar los datos en formato CSV o JSON y generar el PDF externamente.
    """)
    
    # Vista previa del informe
    st.markdown('<div class="sub-header">Vista Previa del Informe</div>', unsafe_allow_html=True)
    
    with st.expander("Ver resumen del informe"):
        st.write("**KPIs del Turno:**")
        st.json(kpis)
        
        st.write("**Variables:**")
        st.dataframe(df_resumen, use_container_width=True, hide_index=True)
        
        st.write("**Recomendaciones:**")
        st.json(recomendaciones)


def main():
    """Función principal de la aplicación"""
    
    # Header principal
    st.markdown("""
        <div style="text-align: center; padding: 1.5rem 0; margin-bottom: 2rem;">
            <h1 style="color: #1D1D1B; margin: 0; font-size: 2.5rem; font-weight: 600;">Informe de Turno - Unidad N-101</h1>
            <p style="color: #666; margin: 0.5rem 0 0 0; font-size: 1.1rem;">Sistema de Monitoreo y Análisis de Procesos Industriales</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### Filtros del Informe")
        
        # Selector de fecha/turno
        turnos = [
            "Turno 1 – 2024-10-01 00:00 a 08:00",
            "Turno 2 – 2024-10-01 08:00 a 16:00",
            "Turno 3 – 2024-10-01 16:00 a 00:00",
            "Turno 1 – 2024-10-02 00:00 a 08:00"
        ]
        
        turno_seleccionado = st.selectbox(
            "Seleccionar Turno:",
            turnos
        )
        
        # Selector de unidad
        unidad_seleccionada = st.selectbox(
            "Seleccionar Unidad:",
            ["N-101", "N-102"]
        )
        
        st.markdown("---")
        
        # Filtro de tipo de variable
        tipos_variable = st.multiselect(
            "Tipo de Variable:",
            ["Flujo", "Nivel", "Presión", "Temperatura"],
            default=["Flujo", "Nivel", "Presión", "Temperatura"]
        )
        
        # Checkboxes
        solo_bien_comportadas = st.checkbox("Mostrar solo variables bien comportadas")
        solo_desvios = st.checkbox("Mostrar solo variables con desvíos")
        
        # Información adicional
        st.markdown("---")
        st.markdown("### Información")
        st.info(f"Turno seleccionado: {turno_seleccionado}")
        st.info(f"Unidad: {unidad_seleccionada}")
    
    # Generar datos dummy
    df_resumen, kpis, series_data, recomendaciones = generate_dummy_data()
    
    # Preparar filtros
    filtros = {
        'tipos_variable': tipos_variable,
        'solo_bien_comportadas': solo_bien_comportadas,
        'solo_desvios': solo_desvios
    }
    
    # Sistema de pestañas
    tab_resumen, tab_variables, tab_puntos, tab_descargas = st.tabs(
        ["Resumen del turno", "Variables y series temporales", "Puntos a vigilar", "Descarga de informes y datos"]
    )
    
    with tab_resumen:
        render_resumen_turno(df_resumen, kpis)
    
    with tab_variables:
        render_variables(df_resumen, series_data, filtros)
    
    with tab_puntos:
        render_puntos_vigilar(recomendaciones)
    
    with tab_descargas:
        render_descargas(df_resumen, kpis, series_data, recomendaciones)


if __name__ == "__main__":
    main()

