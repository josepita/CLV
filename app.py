import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io

# --- Funciones de Cálculo de Informes (a implementar) ---

def convert_excel_date(serial_date):
    """Convierte fecha serial de Excel a objeto datetime."""
    try:
        # Evitar convertir valores nulos o ya convertidos
        if pd.isna(serial_date) or isinstance(serial_date, (datetime, pd.Timestamp)):
            return serial_date
        # Si es numérico, tratar como serial de Excel
        if isinstance(serial_date, (int, float, np.integer, np.floating)):
            return datetime(1899, 12, 30) + timedelta(days=float(serial_date))
        # Si es string, intentar parsear como fecha (acepta ISO y DD/MM/AAAA)
        parsed = pd.to_datetime(serial_date, errors='coerce', dayfirst=True)
        return parsed
    except (ValueError, TypeError):
        return pd.NaT

def preprocess_data(df, logger=st.write):
    """Limpia y preprocesa el DataFrame."""
    logger("Iniciando preprocesamiento de datos...")
    
    # Validar columnas
    required_columns = ['fecha', 'cod_cliente', 'Total_pagado']
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        logger(f"Faltan columnas requeridas: {', '.join(missing)}")
        return None

    # Conversión de fechas
    logger("Convirtiendo fechas...")
    df['fecha_dt'] = df['fecha'].apply(convert_excel_date)
    df = df.dropna(subset=['fecha_dt']) # Eliminar filas con fechas inválidas
    logger(f"Filas después de limpiar fechas: {len(df):,}")
    df = df.sort_values('fecha_dt')

    # Conversión de total pagado
    df['Total_pagado_eur'] = pd.to_numeric(df['Total_pagado'], errors='coerce') / 100
    df = df.dropna(subset=['Total_pagado_eur'])
    logger(f"Filas después de limpiar importes: {len(df):,}")

    # Identificar primera compra
    logger("Identificando cohortes...")
    primera_compra = df.groupby('cod_cliente')['fecha_dt'].min().reset_index()
    primera_compra.columns = ['cod_cliente', 'primera_compra_dt']
    df = df.merge(primera_compra, on='cod_cliente')

    # Cohortes trimestrales y anuales
    df['cohorte_trimestral'] = df['primera_compra_dt'].dt.to_period('Q')
    df['periodo_trimestral'] = df['fecha_dt'].dt.to_period('Q')
    
    df['cohorte_anual'] = df['primera_compra_dt'].dt.to_period('Y')
    df['periodo_anual'] = df['fecha_dt'].dt.to_period('Y')

    logger("Preprocesamiento completado.")
    return df

def generate_retention_report(df):
    """Genera el informe de retención por trimestres."""
    retention_matrix = df.groupby(['cohorte_trimestral', 'periodo_trimestral'])['cod_cliente'].nunique().unstack(0)
    cohort_sizes = df.groupby('cohorte_trimestral')['cod_cliente'].nunique()
    
    retention_pct = retention_matrix.div(cohort_sizes, axis=1).T * 100
    retention_pct = retention_pct.fillna(0)  # mostrar churn explícito en vez de NaN
    
    # Formateo
    retention_pct.index = retention_pct.index.strftime('Y%Y-Q%q')
    retention_pct.columns = retention_pct.columns.strftime('Y%Y-Q%q')
    
    # Añadir total de clientes alineando índices para evitar errores de concatenación
    cohort_sizes.index = cohort_sizes.index.strftime('Y%Y-Q%q')
    cohort_sizes = cohort_sizes.reindex(retention_pct.index)  # asegurar alineación
    cohort_sizes = cohort_sizes.fillna(0)
    report_df = retention_pct.copy()
    report_df.insert(0, 'Total Clientes', cohort_sizes)

    return report_df

def generate_annual_retention_report(df):
    """Genera el informe de retención anual."""
    retention_matrix = df.groupby(['cohorte_anual', 'periodo_anual'])['cod_cliente'].nunique().unstack(0)
    cohort_sizes = df.groupby('cohorte_anual')['cod_cliente'].nunique()
    
    retention_pct = retention_matrix.div(cohort_sizes, axis=1).T * 100
    retention_pct = retention_pct.fillna(0)

    # Formateo
    retention_pct.index = retention_pct.index.strftime('%Y')
    retention_pct.columns = retention_pct.columns.strftime('%Y')

    cohort_sizes.index = cohort_sizes.index.strftime('%Y')
    cohort_sizes = cohort_sizes.reindex(retention_pct.index)  # asegurar alineación
    cohort_sizes = cohort_sizes.fillna(0)
    report_df = retention_pct.copy()
    report_df.insert(0, 'Total Clientes', cohort_sizes)

    return report_df

def generate_survival_analysis(df):
    """Genera el informe de análisis de supervivencia."""
    # Usar la fecha máxima del dataset como referencia temporal (evita depender del reloj actual)
    reference_date = df['fecha_dt'].max()
    if pd.isna(reference_date):
        reference_date = datetime.now()

    # Calcular meses desde la primera compra para cada pedido
    df['meses_desde_primera_compra'] = ((df['fecha_dt'].dt.year - df['primera_compra_dt'].dt.year) * 12 +
                                       (df['fecha_dt'].dt.month - df['primera_compra_dt'].dt.month))

    milestones = [0, 1, 3, 6, 9, 12, 18, 24, 36, 48, 60]
    
    # Pre-calcular el número de clientes únicos por cohorte
    cohort_sizes = df.groupby('cohorte_trimestral')['cod_cliente'].nunique()

    # Función para calcular supervivientes por cohorte
    def get_survivors_for_cohort(cohort_df):
        survivors = {}
        total_clients = cohort_df['cod_cliente'].nunique()
        for m in milestones:
            # Clientes que hicieron una compra EN O DESPUÉS del mes 'm'
            surviving_clients = cohort_df[cohort_df['meses_desde_primera_compra'] >= m]['cod_cliente'].nunique()
            survivors[f'Mes {m}'] = (surviving_clients / total_clients) * 100 if total_clients > 0 else 0
        return pd.Series(survivors)

    # Agrupar por cohorte y aplicar la función
    survival_table = df.groupby('cohorte_trimestral').apply(get_survivors_for_cohort)

    # Calcular métricas adicionales
    agg_stats = df.groupby('cod_cliente').agg(
        cohorte_trimestral=('cohorte_trimestral', 'first'),
        primera_compra=('primera_compra_dt', 'first'),
        ultima_compra=('fecha_dt', 'max'),
        total_pedidos=('cod_cliente', 'size'),
        total_revenue=('Total_pagado_eur', 'sum')
    ).reset_index()

    agg_stats['lifetime_dias'] = (agg_stats['ultima_compra'] - agg_stats['primera_compra']).dt.days
    
    cohort_stats = agg_stats.groupby('cohorte_trimestral').agg(
        Lifetime_Prom=('lifetime_dias', 'mean'),
        Pedidos_Prom=('total_pedidos', 'mean'),
        Revenue_Prom=('total_revenue', 'mean')
    ).reset_index()

    # Unir todo
    # Evitar duplicar columna 'cohorte_trimestral' si ya existe
    if 'cohorte_trimestral' in survival_table.columns:
        survival_table = survival_table.drop(columns=['cohorte_trimestral'])
    survival_table = survival_table.reset_index().rename(columns={'index': 'cohorte_trimestral'})
    report_df = pd.merge(survival_table, cohort_stats, on='cohorte_trimestral')
    
    # Añadir total de clientes y formatear
    report_df = pd.merge(report_df, cohort_sizes.rename('Total Clientes').reset_index(), on='cohorte_trimestral')
    report_df['cohorte_trimestral'] = report_df['cohorte_trimestral'].astype(str).str.replace('Q', '-Q')
    report_df = report_df.set_index('cohorte_trimestral')
    
    # Reordenar columnas
    cols = ['Total Clientes'] + [f'Mes {m}' for m in milestones] + ['Lifetime_Prom', 'Pedidos_Prom', 'Revenue_Prom']
    # Asegurar que todas las columnas existan; si falta alguna, crearla con NaN para evitar KeyError
    for col in cols:
        if col not in report_df.columns:
            report_df[col] = np.nan
    report_df = report_df[cols]
    
    # Calcular "Activos Hoy" (compras en los últimos 90 días)
    activos_hoy = df[df['fecha_dt'] >= (reference_date - timedelta(days=90))]['cod_cliente'].nunique()
    total_clientes = df['cod_cliente'].nunique()
    one_time_buyers = agg_stats[agg_stats['total_pedidos'] == 1]['cod_cliente'].nunique()

    summary = {
        "Total clientes analizados": total_clientes,
        "Clientes activos (últimos 90 días)": activos_hoy,
        "Tiempo de vida promedio (días)": agg_stats['lifetime_dias'].mean(),
        "Promedio de pedidos por cliente": agg_stats['total_pedidos'].mean(),
        "% de clientes con 1 sola compra": (one_time_buyers / total_clientes) * 100 if total_clientes > 0 else 0
    }
    
    return report_df, summary


def generate_frequency_report(df):
    """Genera el informe de frecuencia de compra con sus 4 secciones."""
    # Base para el análisis: clientes con 2+ compras
    df_sorted = df.sort_values(['cod_cliente', 'fecha_dt'])
    df_sorted['dias_desde_anterior'] = df_sorted.groupby('cod_cliente')['fecha_dt'].diff().dt.days
    clientes_multi_compra = df_sorted.dropna(subset=['dias_desde_anterior'])
    
    # --- 1. Distribución por Frecuencia de Compra ---
    bins = [0, 30, 60, 90, 180, 365, np.inf]
    labels = ['≤30 días', '31-60 días', '61-90 días', '91-180 días', '181-365 días', '>365 días']
    clientes_multi_compra['segmento_frecuencia'] = pd.cut(clientes_multi_compra['dias_desde_anterior'], bins=bins, labels=labels, right=True)
    
    distribucion_frecuencia = clientes_multi_compra.groupby('segmento_frecuencia').agg(
        Total_Intervalos=('dias_desde_anterior', 'count'),
        Dias_Promedio=('dias_desde_anterior', 'mean'),
        Dias_Mediana=('dias_desde_anterior', 'median')
    )
    total_intervalos = distribucion_frecuencia['Total_Intervalos'].sum()
    distribucion_frecuencia['% del Total'] = (distribucion_frecuencia['Total_Intervalos'] / total_intervalos) * 100
    distribucion_frecuencia = distribucion_frecuencia[['Total_Intervalos', '% del Total', 'Dias_Promedio', 'Dias_Mediana']]

    # --- 2. Tiempo hasta Segunda Compra ---
    df_sorted['num_compra'] = df_sorted.groupby('cod_cliente').cumcount() + 1
    compras_1_y_2 = df_sorted[df_sorted['num_compra'].isin([1, 2])]
    
    tiempo_segunda_compra_raw = compras_1_y_2.groupby('cod_cliente').agg(
        num_compras=('num_compra', 'max'),
        dias_hasta_segunda=('dias_desde_anterior', 'last')
    )
    tiempo_segunda_compra_raw = tiempo_segunda_compra_raw[tiempo_segunda_compra_raw['num_compras'] > 1]
    
    bins_2da = [-1, 30, 60, 90, 180, np.inf]
    labels_2da = ['Dentro de 30 días', '31-60 días', '61-90 días', '91-180 días', 'Más de 180 días']
    tiempo_segunda_compra_raw['periodo'] = pd.cut(tiempo_segunda_compra_raw['dias_hasta_segunda'], bins=bins_2da, labels=labels_2da, right=True)
    
    tiempo_segunda_compra = tiempo_segunda_compra_raw.groupby('periodo').agg(
        Clientes=('dias_hasta_segunda', 'count')
    )
    total_clientes_2da = tiempo_segunda_compra['Clientes'].sum()
    tiempo_segunda_compra['% del Total'] = (tiempo_segunda_compra['Clientes'] / total_clientes_2da) * 100

    # --- 3. Evolución de Frecuencia por Número de Compra ---
    max_compras = 10 # Limitar para legibilidad
    evolucion = df_sorted[df_sorted['num_compra'] <= max_compras]
    
    evolucion_frecuencia = evolucion.groupby('num_compra').agg(
        Numero_Clientes=('cod_cliente', 'nunique'),
        Dias_Promedio_Intervalo=('dias_desde_anterior', 'mean'),
        Dias_Mediana_Intervalo=('dias_desde_anterior', 'median')
    ).reset_index()
    evolucion_frecuencia = evolucion_frecuencia[evolucion_frecuencia['num_compra'] > 1] # El intervalo es para la compra N
    evolucion_frecuencia['Tendencia'] = evolucion_frecuencia['Dias_Promedio_Intervalo'].diff().apply(
        lambda x: '↓ Mejora' if x < 0 else ('↑ Empeora' if x > 0 else '→ Estable')
    )

    # --- 4. Velocidad de Compra (Compras por Mes) ---
    agg_stats = df.groupby('cod_cliente').agg(
        total_pedidos=('cod_cliente', 'size'),
        primera_compra=('primera_compra_dt', 'first'),
        ultima_compra=('fecha_dt', 'max'),
        total_revenue=('Total_pagado_eur', 'sum')
    ).reset_index()
    
    agg_stats = agg_stats[agg_stats['total_pedidos'] > 1]
    agg_stats['dias_actividad'] = (agg_stats['ultima_compra'] - agg_stats['primera_compra']).dt.days
    # Si primera y última compra son el mismo día, pero hay >1 pedido, considerar 1 día de actividad.
    agg_stats.loc[agg_stats['dias_actividad'] == 0, 'dias_actividad'] = 1
    
    agg_stats['compras_por_mes'] = agg_stats['total_pedidos'] / (agg_stats['dias_actividad'] / 30)

    bins_vel = [-np.inf, 0.1, 0.25, 0.5, 1, np.inf]
    labels_vel = ['Muy Baja (<0.1)', 'Baja (0.1-0.24)', 'Media (0.25-0.49)', 'Media-Alta (0.5-0.99)', 'Alta (≥1)']
    agg_stats['segmento_velocidad'] = pd.cut(agg_stats['compras_por_mes'], bins=bins_vel, labels=labels_vel, right=False)

    velocidad_compra = agg_stats.groupby('segmento_velocidad').agg(
        Numero_Clientes=('cod_cliente', 'count'),
        Compras_por_Mes_Promedio=('compras_por_mes', 'mean'),
        Pedidos_Promedio=('total_pedidos', 'mean'),
        Revenue_Promedio=('total_revenue', 'mean')
    )
    total_clientes_vel = velocidad_compra['Numero_Clientes'].sum()
    velocidad_compra['% del Total'] = (velocidad_compra['Numero_Clientes'] / total_clientes_vel) * 100

    return {
        "distribucion": distribucion_frecuencia,
        "segunda_compra": tiempo_segunda_compra,
        "evolucion": evolucion_frecuencia.set_index('num_compra'),
        "velocidad": velocidad_compra
    }


# Las funciones para los informes 3 y 4 son más complejas y se añadirán progresivamente.

def style_retention_table(df):
    """Aplica estilos de color a la tabla de retención."""
    def color_cells(val, row_idx, col_idx, df_values):
        cohorte_period = df_values.index[row_idx]
        current_period = df_values.columns[col_idx]
        
        # Convertir 'Y2024-Q1' a un objeto Period
        try:
            cohorte_period = pd.Period(cohorte_period.replace('Y', ''), freq='Q')
            current_period = pd.Period(current_period.replace('Y', ''), freq='Q')
        except:
             return '' # No aplicar estilo si el formato no es el esperado

        style = ''
        if pd.isna(val) or val == 0:
            style = 'background-color: #C0C0C0' # Gris
        elif current_period < cohorte_period:
             style = 'background-color: #C0C0C0' # Gris
        elif current_period == cohorte_period:
            style = 'background-color: #1E6B1E; color: white' # Verde oscuro
        elif val >= 8:
            style = 'background-color: #7CCD7C' # Verde claro
        elif 3 <= val < 8:
            style = 'background-color: #FFD700' # Amarillo
        else:
            style = 'background-color: #FF6B6B' # Rojo
        return style

    styled = df.style.format("{:.2f}%", na_rep="").apply(
        lambda r: [
            color_cells(r.iloc[c_idx], r.name, c_idx, r.to_frame().T)
            for c_idx in range(len(r))
        ],
        axis=1,
        subset=pd.IndexSlice[:, df.columns[1:]] # No aplicar a la columna 'Total Clientes'
    ).format({'Total Clientes': "{:,.0f}"})
    return styled

def style_heatmap(df, cmap="Greens"):
    """Aplica un gradiente de color a valores numéricos, mantiene la primera col sin formato."""
    if df is None or df.empty:
        return df
    first_col = df.columns[0]
    styled = df.style.format("{:.2f}", na_rep="-")
    
    # Filter to apply gradient only on numeric columns from the second column onwards
    numeric_cols = [col for col in df.columns[1:] if pd.api.types.is_numeric_dtype(df[col])]
    
    if numeric_cols:
        styled = styled.background_gradient(axis=None, cmap=cmap, subset=numeric_cols)

    if first_col in df.columns:
        styled = styled.format({first_col: "{:,.0f}"})
    return styled

def style_percent_heatmap(df, cmap="Blues"):
    """Gradiente para porcentajes: rojo solo en 0; resto verde claro→oscuro."""
    if df is None or df.empty:
        return df
    styled = df.style
    if df.columns[0].lower().startswith("total"):
        styled = styled.format({df.columns[0]: "{:,.0f}"})
        percent_cols = df.columns[1:]
    else:
        percent_cols = df.columns
    styled = styled.format({col: "{:.2f}%" for col in percent_cols}, na_rep="-")

    # Color mapping: 0 -> rojo, >0 -> gradiente verde
    def color_map(val):
        if pd.isna(val):
            return ""
        if val == 0:
            return "background-color: #ff6b6b; color: white"
        # Interpolar verde claro (#e8f5e9) a verde oscuro (#1b5e20)
        v = max(0.0, min(100.0, float(val)))
        t = v / 100.0
        start = np.array([0xE8, 0xF5, 0xE9])
        end = np.array([0x1B, 0x5E, 0x20])
        rgb = (start + (end - start) * t).astype(int)
        return f"background-color: rgb({rgb[0]},{rgb[1]},{rgb[2]}); color: {'white' if t>0.55 else 'black'}"

    styled = styled.applymap(color_map, subset=percent_cols)
    return styled

def style_currency(df, cols):
    if df is None or df.empty:
        return df
    fmt = {col: "€{:.2f}" for col in cols if col in df.columns}
    return df.style.format(fmt)

def styler_supported():
    """Devuelve True si la versión de Streamlit permite renderizar pandas.Styler en st.dataframe."""
    try:
        major, minor, *_ = map(int, st.__version__.split('.')[:2])
        return (major > 1) or (major == 1 and minor >= 31)
    except Exception:
        return False

def show_table(df, styler_fn=None, info_msg=None):
    """Renderiza df con estilos si la versión de Streamlit lo soporta; si no, muestra fallback plano."""
    if styler_supported() and styler_fn is not None:
        st.dataframe(styler_fn(df))
    else:
        if info_msg:
            st.info(info_msg)
        st.dataframe(df)

def filter_reports_by_date(reports, start_date, end_date):
    """Filtra tablas de informes solo para visualización según rango de fechas sin recalcular CSV."""
    if not reports:
        return None
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    def filter_period_table(df, freq):
        if df is None or df.empty:
            return df
        df_copy = df.copy()
        # Filtrar filas (cohortes)
        idx_period = df_copy.index.to_series().apply(lambda x: pd.Period(str(x).replace('Y',''), freq=freq))
        row_mask = (idx_period.dt.start_time >= start_ts) & (idx_period.dt.start_time <= end_ts)
        df_copy = df_copy.loc[row_mask]
        # Filtrar columnas (periodos calendario)
        col_period = pd.Series(df_copy.columns, index=df_copy.columns).apply(lambda x: pd.Period(str(x).replace('Y',''), freq=freq))
        col_keep = (col_period.dt.start_time >= start_ts) & (col_period.dt.start_time <= end_ts)
        # Siempre mantener la primera columna de totales
        col_keep.iloc[0] = True
        df_copy = df_copy.loc[:, col_keep]
        return df_copy

    filtered = {}
    filtered['report1'] = filter_period_table(reports['report1'], 'Q') if 'report1' in reports else None
    filtered['report2'] = filter_period_table(reports['report2'], 'Y') if 'report2' in reports else None
    # Supervivencia: filtrar filas por cohorte
    if 'report3' in reports and reports['report3'] is not None:
        df3 = reports['report3'].copy()
        idx_period = df3.index.to_series().apply(lambda x: pd.Period(str(x).replace('Q','-Q') if 'Q' not in str(x) else str(x), freq='Q'))
        row_mask = (idx_period.dt.start_time >= start_ts) & (idx_period.dt.start_time <= end_ts)
        df3 = df3.loc[row_mask]
        filtered['report3'] = df3
    else:
        filtered['report3'] = None
    filtered['report4'] = reports.get('report4')
    return filtered

from openpyxl.styles import PatternFill, Font

def export_to_excel(reports):
    """Crea un archivo Excel en memoria con los informes y sus estilos."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Escribir cada informe en una hoja
        reports['report1'].to_excel(writer, sheet_name='Retención Trimestral')
        reports['report2'].to_excel(writer, sheet_name='Retención Anual')
        reports['report3'].to_excel(writer, sheet_name='Análisis de Supervivencia')
        
        # Informes de frecuencia
        reports['report4']['distribucion'].to_excel(writer, sheet_name='Frecuencia - Distribución')
        reports['report4']['segunda_compra'].to_excel(writer, sheet_name='Frecuencia - 2da Compra')
        reports['report4']['evolucion'].to_excel(writer, sheet_name='Frecuencia - Evolución')
        reports['report4']['velocidad'].to_excel(writer, sheet_name='Frecuencia - Velocidad')

        # --- Aplicar Estilos (Ejemplo para Retención Trimestral) ---
        # Definir rellenos de color
        green_dark_fill = PatternFill(start_color="1E6B1E", end_color="1E6B1E", fill_type="solid")
        green_light_fill = PatternFill(start_color="7CCD7C", end_color="7CCD7C", fill_type="solid")
        yellow_fill = PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid")
        red_fill = PatternFill(start_color="FF6B6B", end_color="FF6B6B", fill_type="solid")
        gray_fill = PatternFill(start_color="C0C0C0", end_color="C0C0C0", fill_type="solid")
        white_font = Font(color="FFFFFF")

        ws = writer.sheets['Retención Trimestral']
        df = reports['report1']

        # Iterar sobre las celdas de datos para aplicar el formato condicional
        for r_idx, row in enumerate(df.index, 2): # 2 porque Excel es 1-based y hay cabecera
            for c_idx, col_name in enumerate(df.columns, 2): # Empieza en la 3a col del excel
                cell = ws.cell(row=r_idx, column=c_idx)
                val = df.loc[row, col_name]

                if pd.isna(val):
                    continue

                try:
                    cohorte_period = pd.Period(row.replace('Y', ''), freq='Q')
                    current_period = pd.Period(col_name.replace('Y', ''), freq='Q')
                    
                    if current_period < cohorte_period:
                        cell.fill = gray_fill
                    elif current_period == cohorte_period:
                        cell.fill = green_dark_fill
                        cell.font = white_font
                    elif val >= 8:
                        cell.fill = green_light_fill
                    elif 3 <= val < 8:
                        cell.fill = yellow_fill
                    elif val > 0: # Solo colorear si hay retención
                        cell.fill = red_fill

                except Exception as e:
                    # Ignorar errores de parseo de fechas para columnas no periódicas
                    pass
    
    output.seek(0)
    return output


# --- UI de la Aplicación Streamlit ---

st.set_page_config(layout="wide", page_title="Análisis CLV Pedalmoto")

# Paletas de color más contrastadas
RETENTION_CMAP = "RdYlGn"
SURVIVAL_CMAP = "YlOrRd"
FREQ_DISTRIB_CMAP = "BuGn"
FREQ_EVOL_CMAP = "PuBu"
FREQ_VEL_CMAP = "OrRd"

st.title("Aplicación de Análisis de Customer Lifetime Value (CLV)")
st.markdown("Sube un archivo CSV de pedidos para generar informes de retención y comportamiento de compra.")

if "history" not in st.session_state:
    st.session_state["history"] = []

preview_placeholder = st.empty()
status_placeholder = st.empty()

# ---- Histórico disponible incluso sin subir archivo ----
if st.session_state["history"]:
    st.sidebar.markdown("### Historial de Informes")
    labels = [f"{i+1}. {h['timestamp']}" for i, h in enumerate(st.session_state['history'])]
    choice = st.sidebar.selectbox("Ver / descargar informe previo", labels)
    idx = labels.index(choice)
    hist_item = st.session_state['history'][idx]
    st.sidebar.download_button(
        label="⬇️ Descargar selección",
        data=hist_item["excel_bytes"],
        file_name=f"Analisis_CLV_{hist_item['timestamp'].replace(' ', '_').replace(':','')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"download_hist_side_{idx}"
    )
    # Guardar como base para la visualización principal
    st.session_state["base_reports"] = hist_item["reports"]
    st.session_state["base_summary"] = hist_item["summary"]
    # Inferir rango de fechas disponible para filtros de vista
    def extract_dates_from_reports(reports):
        dates = []
        r1 = reports.get('report1')
        if r1 is not None and not r1.empty:
            idx_period = r1.index.to_series().apply(lambda x: pd.Period(str(x).replace('Y',''), freq='Q'))
            col_period = pd.Series(r1.columns[1:], index=r1.columns[1:]).apply(lambda x: pd.Period(str(x).replace('Y',''), freq='Q'))
            dates += list(idx_period.dt.start_time.dt.date)
            dates += list(col_period.dt.start_time.dt.date)
        r2 = reports.get('report2')
        if r2 is not None and not r2.empty:
            idx_period = r2.index.to_series().apply(lambda x: pd.Period(str(x), freq='Y'))
            col_period = pd.Series(r2.columns[1:], index=r2.columns[1:]).apply(lambda x: pd.Period(str(x), freq='Y'))
            dates += list(idx_period.dt.start_time.dt.date)
            dates += list(col_period.dt.start_time.dt.date)
        return dates
    dates_available = extract_dates_from_reports(st.session_state["base_reports"])
    if dates_available:
        st.session_state["data_date_min"] = min(dates_available)
        st.session_state["data_date_max"] = max(dates_available)
        st.session_state["view_date_range"] = (st.session_state["data_date_min"], st.session_state["data_date_max"])

# Estado inicial para mostrar u ocultar el panel de carga
if "show_controls" not in st.session_state:
    st.session_state["show_controls"] = False if st.session_state.get("base_reports") else True

# Botón para mostrar panel cuando está oculto
if not st.session_state["show_controls"]:
    if st.button("Mostrar panel de carga y filtros"):
        st.session_state["show_controls"] = True

# ---- Panel de carga y generación (ocultable) ----
if st.session_state["show_controls"]:
    with st.container():
        st.markdown("### Subir datos y generar informes")
        uploaded_file = st.file_uploader("Elige un archivo CSV", type="csv", key="uploader_panel")

        if uploaded_file is not None:
            df_raw = pd.read_csv(uploaded_file)
            st.session_state["df_raw"] = df_raw

            with preview_placeholder.container():
                st.info("Archivo subido correctamente. Mostrando primeras 5 filas:")
                st.dataframe(df_raw.head())

            fechas_preview = df_raw['fecha'].apply(convert_excel_date)
            min_fecha = fechas_preview.min()
            max_fecha = fechas_preview.max()
            if pd.isna(min_fecha) or pd.isna(max_fecha):
                min_fecha = datetime.now() - timedelta(days=365)
                max_fecha = datetime.now()
            st.markdown("#### Rango de fechas para procesar")
            date_range = st.date_input(
                "Rango (desde / hasta)",
                value=(min_fecha.date(), max_fecha.date()),
                min_value=min_fecha.date(),
                max_value=max_fecha.date(),
                key="process_date_range"
            )
            if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                date_start, date_end = date_range
            else:
                date_start = min_fecha.date()
                date_end = max_fecha.date()

            if st.button("Generar / Actualizar Informes"):
                log_box = status_placeholder.empty()
                log = log_box.write
                with st.spinner("Procesando datos y generando informes... Esto puede tardar unos minutos."):
                    df_processed = preprocess_data(df_raw, logger=log)
                    if df_processed is not None and not df_processed.empty:
                        start_ts = pd.Timestamp(date_start)
                        end_ts = pd.Timestamp(date_end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        df_processed = df_processed[
                            (df_processed['fecha_dt'] >= start_ts) & (df_processed['fecha_dt'] <= end_ts)
                        ]
                    
                    if df_processed is not None and not df_processed.empty:
                        report1_df = generate_retention_report(df_processed)
                        report2_df = generate_annual_retention_report(df_processed)
                        report3_df, report3_summary = generate_survival_analysis(df_processed)
                        report4_dfs = generate_frequency_report(df_processed)

                        all_reports = {
                            "report1": report1_df,
                            "report2": report2_df,
                            "report3": report3_df,
                            "report4": report4_dfs,
                        }
                        excel_file = export_to_excel(all_reports)
                        st.session_state["history"].insert(0, {
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "reports": all_reports,
                            "excel_bytes": excel_file.getvalue(),
                            "summary": report3_summary
                        })
                        # Guardar como base para vista y rango por defecto
                        st.session_state["base_reports"] = all_reports
                        st.session_state["base_summary"] = report3_summary
                        st.session_state["data_date_min"] = df_processed['fecha_dt'].min().date()
                        st.session_state["data_date_max"] = df_processed['fecha_dt'].max().date()
                        st.session_state["view_date_range"] = (st.session_state["data_date_min"], st.session_state["data_date_max"])

                        # Limpiar vista previa y logs; ocultar panel
                        log_box.empty()
                        preview_placeholder.empty()
                        status_placeholder.empty()
                        st.session_state["show_controls"] = False

                        st.sidebar.download_button(
                            label="📥 Descargar Informes en Excel",
                            data=excel_file,
                            file_name="Analisis_CLV_Pedalmoto.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_latest"
                        )
                    else:
                        st.error("No se pudieron procesar los datos. Revisa el CSV o el rango de fechas.")


if uploaded_file is not None:
    df_raw = pd.read_csv(uploaded_file)
    st.session_state["df_raw"] = df_raw

    # Vista previa (se limpiará tras generar informes)
    with preview_placeholder.container():
        st.info("Archivo subido correctamente. Mostrando primeras 5 filas:")
        st.dataframe(df_raw.head())

    # Rango de fechas para filtrar
    fechas_preview = df_raw['fecha'].apply(convert_excel_date)
    min_fecha = fechas_preview.min()
    max_fecha = fechas_preview.max()
    if pd.isna(min_fecha) or pd.isna(max_fecha):
        min_fecha = datetime.now() - timedelta(days=365)
        max_fecha = datetime.now()
    st.markdown("### Filtro de fechas")
    date_range = st.date_input(
        "Rango (desde / hasta)",
        value=(min_fecha.date(), max_fecha.date()),
        min_value=min_fecha.date(),
        max_value=max_fecha.date()
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_start, date_end = date_range
    else:
        date_start = min_fecha.date()
        date_end = max_fecha.date()

    if st.button("Generar / Actualizar Informes"):
        log_box = status_placeholder.empty()
        log = log_box.write
        with st.spinner("Procesando datos y generando informes... Esto puede tardar unos minutos."):
            # Preprocesamiento
            df_processed = preprocess_data(df_raw, logger=log)
            # Filtrar por rango seleccionado
            if df_processed is not None and not df_processed.empty:
                start_ts = pd.Timestamp(date_start)
                end_ts = pd.Timestamp(date_end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                df_processed = df_processed[
                    (df_processed['fecha_dt'] >= start_ts) & (df_processed['fecha_dt'] <= end_ts)
                ]
            
            if df_processed is not None and not df_processed.empty:
                # Generar informes
                report1_df = generate_retention_report(df_processed)
                report2_df = generate_annual_retention_report(df_processed)
                report3_df, report3_summary = generate_survival_analysis(df_processed)
                report4_dfs = generate_frequency_report(df_processed)

                # Guardar histórico
                all_reports = {
                    "report1": report1_df,
                    "report2": report2_df,
                    "report3": report3_df,
                    "report4": report4_dfs,
                }
                excel_file = export_to_excel(all_reports)
                st.session_state["history"].insert(0, {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "reports": all_reports,
                    "excel_bytes": excel_file.getvalue(),
                    "summary": report3_summary
                })

                # Limpiar vista previa y logs; dejar solo informes
                log_box.empty()
                preview_placeholder.empty()
                status_placeholder.empty()

                
                # --- Botón de Descarga ---
                st.sidebar.header("Exportar Informes")
                
                st.sidebar.download_button(
                    label="📥 Descargar Informes en Excel",
                    data=excel_file,
                    file_name="Analisis_CLV_Pedalmoto.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                # Histórico en sidebar
                st.sidebar.markdown("### Historial de Informes")
                if st.session_state["history"]:
                    labels = [f"{i+1}. {h['timestamp']}" for i, h in enumerate(st.session_state['history'])]
                    choice = st.sidebar.selectbox("Descargar informe previo", labels, index=0)
                    idx = labels.index(choice)
                    hist_item = st.session_state['history'][idx]
                    st.sidebar.download_button(
                        label="⬇️ Descargar selección",
                        data=hist_item["excel_bytes"],
                        file_name=f"Analisis_CLV_{hist_item['timestamp'].replace(' ', '_').replace(':','')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_hist_{idx}"
                    )
            else:
                st.error("No se pudieron procesar los datos. Revisa el CSV.")

base_reports = st.session_state.get("base_reports")
base_summary = st.session_state.get("base_summary")

if base_reports:
    data_min = st.session_state.get("data_date_min")
    data_max = st.session_state.get("data_date_max")
    if not data_min or not data_max:
        today = datetime.now().date()
        data_min = today - timedelta(days=365)
        data_max = today
    if "view_date_range" not in st.session_state:
        st.session_state["view_date_range"] = (data_min, data_max)

    with st.expander("Filtrar visualización por fechas", expanded=False):
        view_range = st.date_input(
            "Rango de visualización (no recalcula, solo oculta periodos fuera de rango)",
            value=st.session_state.get("view_date_range", (data_min, data_max)),
            min_value=data_min,
            max_value=data_max,
            key="view_date_range"
        )
    if isinstance(view_range, (list, tuple)) and len(view_range) == 2:
        view_start, view_end = view_range
    else:
        view_start, view_end = data_min, data_max

    display_reports = filter_reports_by_date(base_reports, view_start, view_end)
    tab1, tab2, tab3, tab4 = st.tabs([
        "Informe 1: Retención Trimestral",
        "Informe 2: Retención Anual",
        "Informe 3: Análisis de Supervivencia",
        "Informe 4: Frecuencia de Compra"
    ])

    with tab1:
        st.header("Retención por Trimestres")
        st.markdown("""
        **Descripción**: Análisis de cohortes que muestra qué porcentaje de clientes de cada cohorte trimestral (basada en su primera compra) realiza compras en trimestres subsiguientes.
        - **Filas**: Cohorte (trimestre de primera compra)
        - **Columnas**: Trimestres calendario
        - **Valores**: % de retención
        """)
        if display_reports['report1'] is not None and not display_reports['report1'].empty:
            show_table(
                display_reports['report1'],
                styler_fn=lambda d: style_percent_heatmap(d, cmap=RETENTION_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )

    with tab2:
        st.header("Retención Anual")
        st.markdown("Versión agregada del análisis de retención a nivel anual.")
        if display_reports['report2'] is not None and not display_reports['report2'].empty:
             show_table(
                 display_reports['report2'],
                 styler_fn=lambda d: style_percent_heatmap(d, cmap=RETENTION_CMAP),
                 info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
             )

    with tab3:
        st.header("Análisis de Supervivencia")
        st.markdown("""
        **Descripción**: Porcentaje de clientes de una cohorte que permanecen "activos" (realizando compras) después de un número específico de meses desde su primera compra.
        """)
        if display_reports['report3'] is not None and not display_reports['report3'].empty:
            st.subheader("Resumen Ejecutivo")
            cols = st.columns(len(base_summary))
            for i, (key, value) in enumerate(base_summary.items()):
                if "%" in key:
                    cols[i].metric(key, f"{value:.2f}%")
                elif "días" in key or "Promedio" in key:
                    cols[i].metric(key, f"{value:.2f}")
                else:
                    cols[i].metric(key, f"{int(value):,}")

            st.subheader("Tabla de Supervivencia por Cohorte")
            show_table(
                display_reports['report3'],
                styler_fn=lambda d: style_percent_heatmap(d, cmap=SURVIVAL_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )

    with tab4:
        st.header("Frecuencia de Compra")
        st.markdown("Análisis detallado del tiempo entre compras y patrones de recompra para clientes con 2 o más pedidos.")
        
        if display_reports and display_reports.get('report4'):
            st.subheader("1. Distribución por Frecuencia de Compra")
            show_table(
                display_reports['report4']['distribucion'],
                styler_fn=lambda d: style_heatmap(d, cmap=FREQ_DISTRIB_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )

            st.subheader("2. Tiempo hasta la Segunda Compra")
            show_table(
                display_reports['report4']['segunda_compra'],
                styler_fn=lambda d: style_percent_heatmap(d, cmap=RETENTION_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )
            
            st.subheader("3. Evolución de Frecuencia por Número de Compra")
            show_table(
                display_reports['report4']['evolucion'],
                styler_fn=lambda d: style_heatmap(d, cmap=FREQ_EVOL_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )
            
            st.subheader("4. Velocidad de Compra (Compras por Mes)")
            show_table(
                display_reports['report4']['velocidad'],
                styler_fn=lambda d: style_heatmap(d, cmap=FREQ_VEL_CMAP),
                info_msg="Tu versión de Streamlit no soporta estilos de pandas (<1.31). Se muestra la tabla sin colores."
            )
else:
    st.warning("Por favor, sube un archivo CSV o selecciona un histórico en la barra lateral para ver informes.")
