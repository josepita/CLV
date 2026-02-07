
# PROMPT PARA CLAUDE CODE: Aplicación Web de Análisis CLV para Pedalmoto

## CONTEXTO GENERAL

Necesito que desarrolles una aplicación web en Python (Flask o Streamlit) que permita subir un CSV de pedidos de ecommerce y genere automáticamente 4 informes avanzados de análisis de Customer Lifetime Value (CLV), retención y comportamiento de compra.

## ESTRUCTURA DE DATOS DE ENTRADA (CSV)

El CSV debe contener las siguientes columnas (nombres exactos):

```
- codigo: ID del pedido
- fecha: Fecha en formato Excel serial (ej. 45757)
- fecha_hora: Timestamp en formato Excel serial
- cod_cliente: ID único del cliente
- nombre: Nombre del cliente
- apellidos: Apellidos del cliente
- email: Email del cliente
- provincia: Provincia
- localidad: Ciudad
- pais: País
- forma_pago: Método de pago usado
- forma_envio: Método de envío
- estado: Estado del pedido (Entregado, Enviado, etc.)
- Total_pagado: Monto total del pedido en centavos (ej. 21978 = €219.78)
- agrupado: Flag si es pedido agrupado (s/n)
- pedidos_en_grupo: Número de pedidos en el grupo
- Tiempo_desde_ultimo_pedido: Días desde el último pedido del cliente
- Num_Pedidos_Cliente: Número total de pedidos del cliente
- cliente_nuevo: Flag si es cliente nuevo (s/n)
```

**IMPORTANTE**: Las si fechas vienen en formato serial de Excel (número de días desde 1900-01-01). Debes convertirlas usando: `datetime(1899, 12, 30) + timedelta(days=fecha_serial)`

## INFORME 1: RETENCIÓN POR TRIMESTRES (Hoja "Retención")

### Descripción
Análisis de cohortes mostrando qué porcentaje de clientes de cada cohorte trimestral realiza compras en trimestres subsiguientes.

### Cálculo Detallado

1. **Definir cohortes**: Agrupa clientes por su trimestre de primera compra (2018Q2, 2018Q3, ..., 2026Q1)

2. **Para cada combinación cohorte × trimestre**:
   - Contar cuántos clientes de esa cohorte hicieron al menos 1 compra en ese trimestre
   - Dividir por el total de clientes de la cohorte × 100

3. **Matriz resultante**:
   - Filas: Cohortes (trimestre de primera compra)
   - Columnas: Trimestres calendario
   - Valores: Porcentaje de retención
   - La diagonal (trimestre de cohorte) siempre es 100%
   - Valores antes del trimestre de cohorte son 0 o N/A

### Formato de Salida

**Tabla HTML/Excel** con:
- Columna A: Cohorte (formato "YYYYQX")
- Columna B: Total de clientes en esa cohorte
- Columnas C en adelante: Trimestres calendario con porcentajes

**Escala de colores condicional**:
- Verde oscuro (#1E6B1E): Diagonal (100%, trimestre de adquisición)
- Verde claro (#7CCD7C): ≥8% (muy buena retención)
- Amarillo (#FFD700): 3-8% (retención moderada)
- Rojo (#FF6B6B): <3% (baja retención)
- Gris (#C0C0C0): Período anterior a la cohorte

**Estadísticas resumen**:
- Total de clientes analizados
- Rango de períodos
- Insights clave (cohortes con mejor/peor retención)

**personalización**
El usuario podrá esteblecer el rango de fechas de inicio y fin para el análisis

## INFORME 2: RETENCIÓN ANUAL (Hoja "Retención Anual")

### Descripción
Versión agregada del análisis de retención pero a nivel anual (simplificado).

### Cálculo Detallado

1. **Definir cohortes anuales**: Agrupa clientes por su año de primera compra (2020, 2021, ..., 2025)

2. **Para cada combinación cohorte × año**:
   - Contar cuántos clientes de esa cohorte hicieron al menos 1 compra en ese año
   - Dividir por el total de clientes de la cohorte × 100

3. **Matriz resultante**:
   - Filas: Cohortes anuales
   - Columnas: Años calendario
   - Valores: Porcentaje de retención anual

### Formato de Salida

**Tabla similar** pero con años en lugar de trimestres. Misma lógica de colores.s

**personalización**
El usuario podrá esteblecer el rango de fechas de inicio y fin para el análisis

## INFORME 3: ANÁLISIS DE SUPERVIVENCIA (Hoja "Análisis de Supervivencia")

### Descripción
Análisis de cuánto tiempo los clientes permanecen "activos" después de su primera compra, usando cohortes trimestrales.

### Cálculo Detallado

1. **Para cada cliente**:
   - Identificar fecha de primera compra (cohorte)
   - Calcular "meses desde primera compra" para cada pedido subsiguiente
   - Determinar si el cliente sigue activo en cada milestone (mes 0, 1, 3, 6, 9, 12, 18, 24, 36, 48, 60)

2. **Por cohorte trimestral**:
   - **Mes 0**: 100% (todos los clientes)
   - **Mes X**: % de clientes que hicieron al menos 1 compra después de X meses desde su primera compra

3. **Métricas adicionales por cohorte**:
   - **Lifetime Promedio**: Tiempo promedio en días desde primera hasta última compra
   - **Pedidos Promedio**: Número medio de pedidos por cliente
   - **Revenue Promedio**: Ingresos totales promedio por cliente

### Formato de Salida

**Tabla con**:
- Columnas: Cohorte | Total Clientes | Activos Hoy | Mes 0 | Mes 1 | Mes 3 | Mes 6 | Mes 9 | Mes 12 | Mes 18 | Mes 24 | Mes 36 | Mes 48 | Mes 60 | Lifetime Prom | Pedidos Prom | Revenue Prom

**Escala de colores para % supervivencia**:
- Verde: >20%
- Amarillo: 10-20%
- Naranja: 5-10%
- Rojo: <5%

**Resumen ejecutivo**:
- Total clientes analizados
- Clientes activos (últimos 90 días)
- Tiempo de vida promedio
- Promedio de pedidos por cliente
- % de clientes con 1 sola compra (one-time buyers)

## INFORME 4: FRECUENCIA DE COMPRA (Hoja "Frecuencia de Compra")

### Descripción
Análisis detallado del tiempo entre compras y patrones de recompra.

### Cálculo Detallado

**Solo incluir clientes con 2+ compras**

#### Sección 1: Distribución por Frecuencia de Compra

Para cada cliente con múltiples compras:
1. Calcular intervalo promedio entre sus compras consecutivas
2. Clasificar en segmentos:
   - ≤30 días (Muy Frecuente)
   - 31-60 días (Frecuente)
   - 61-90 días (Regular)
   - 91-180 días (Ocasional)
   - 181-365 días (Poco Frecuente)
   - >365 días (Muy Poco Frecuente)

3. Por segmento calcular:
   - Total de intervalos (no clientes, sino pares de compras)
   - % del total
   - Días promedio
   - Días mediana

#### Sección 2: Tiempo hasta Segunda Compra

**CRÍTICO**: Este análisis es solo para el intervalo primera → segunda compra.

1. Filtrar todos los clientes que tienen al menos 2 compras
2. Calcular días entre compra 1 y compra 2 para cada cliente
3. Clasificar en períodos:
   - Dentro de 30 días
   - 31-60 días
   - 61-90 días
   - 91-180 días
   - Más de 180 días

4. Calcular:
   - Clientes por período
   - % del total

#### Sección 3: Evolución de Frecuencia por Número de Compra

Para cada transición de compra (2→3, 3→4, 4→5, etc.):

1. Filtrar clientes que tienen al menos N compras
2. Calcular intervalo entre compra N-1 y compra N
3. Reportar:
   - Número de clientes
   - Días promedio de intervalo
   - Días mediana de intervalo
   - Tendencia (↓ Mejora, ↑ Empeora, → Estable) comparando con la compra anterior

**Ejemplo**:
- Compra 2: Intervalo promedio 304 días (clientes que hicieron 2ª compra)
- Compra 3: Intervalo promedio 241 días (clientes que hicieron 3ª compra)
- Tendencia: ↓ Mejora (porque 241 < 304)

#### Sección 4: Velocidad de Compra (Compras por Mes)

1. Para cada cliente calcular: `compras_por_mes = total_pedidos / (días_entre_primera_y_última / 30)`
2. Clasificar en segmentos:
   - Alta (≥1 compra/mes)
   - Media-Alta (1 compra cada 2 meses, 0.5-0.99)
   - Media (1 compra cada 3-4 meses, 0.25-0.49)
   - Baja (1 compra cada 5-10 meses, 0.1-0.24)
   - Muy Baja (<1 compra cada 10 meses, <0.1)

3. Por segmento calcular:
   - Número de clientes
   - % del total
   - Compras/mes promedio
   - Pedidos promedio por cliente
   - Revenue promedio por cliente

### Formato de Salida

**4 tablas HTML**:
1. Distribución por Frecuencia
2. Tiempo hasta Segunda Compra (con interpretaciones: Excelente, Bueno, Aceptable, En Riesgo, Crítico)
3. Evolución por Número de Compra (con columna de Tendencia)
4. Velocidad de Compra

**Sección de Insights Clave** (texto formateado):
1. Ventana crítica de 30-90 días
   - % que hacen 2ª compra en <90 días
   - % que compran en <30 días
   - % que tardan >180 días
   - Recomendaciones de acción

2. La frecuencia mejora con el engagement
   - Comparación de intervalos por número de compra
   - Punto de inflexión

3. Segmentos de alta velocidad
   - % y revenue promedio por segmento

## REQUISITOS TÉCNICOS DE LA APLICACIÓN

### Stack Recomendado
- **Framework**: Streamlit (más simple) o Flask (más control)
- **Procesamiento**: Pandas, NumPy
- **Visualización**: Plotly o Matplotlib para gráficos opcionales
- **Exportación**: openpyxl para generar Excel con formato

### Flujo de la Aplicación

1. **Pantalla inicial**:
   - Uploader de archivo CSV
   - Validación de columnas requeridas
   - Previsualización de primeras 10 filas

2. **Procesamiento**:
   - Conversión de fechas de serial a datetime
   - Limpieza de datos (eliminar pedidos sin cliente, fechas inválidas)
   - Cálculo de métricas derivadas

3. **Generación de informes**:
   - Botón para generar los 4 informes
   - Barra de progreso

4. **Visualización**:
   - Tabs o secciones para cada informe
   - Tablas con formato y colores
   - Métricas destacadas (cards/KPIs)

5. **Exportación**:
   - Botón para descargar Excel con los 4 informes en hojas separadas
   - Mantener formato y colores

### Consideraciones Importantes

- **Performance**: Para datasets grandes (50k+ filas), usa operaciones vectorizadas de pandas
- **Manejo de errores**: Valida formatos de fecha, clientes duplicados, valores nulos
- **Formato de moneda**: Los valores de Total_pagado están en centavos, dividir por 100 para mostrar en euros
- **Fechas**: Manejar correctamente la conversión de serial de Excel a datetime
- **Cálculo de trimestres**: Usar `pd.PeriodIndex` con freq='Q' para agrupar por trimestres

### Ejemplo de Código Clave

```python
# Conversión de fecha serial de Excel
from datetime import datetime, timedelta
df['fecha_dt'] = df['fecha'].apply(lambda x: datetime(1899, 12, 30) + timedelta(days=x))

# Calcular trimestre
df['periodo'] = df['fecha_dt'].dt.to_period('Q')

# Identificar primera compra por cliente
primera_compra = df.groupby('cod_cliente')['fecha_dt'].min().reset_index()
primera_compra.columns = ['cod_cliente', 'primera_compra']
df = df.merge(primera_compra, on='cod_cliente')

# Cohorte del cliente
df['cohorte'] = df['primera_compra'].dt.to_period('Q')

# Análisis de retención: % de clientes de cohorte X que compraron en período Y
retention = df.groupby(['cohorte', 'periodo'])['cod_cliente'].nunique().unstack(fill_value=0)
cohort_sizes = df.groupby('cohorte')['cod_cliente'].nunique()
retention_pct = retention.div(cohort_sizes, axis=0) * 100
```

## ENTREGABLES ESPERADOS

1. **Código fuente** completo de la aplicación (Python)
2. **requirements.txt** con todas las dependencias
3. **README.md** con:
   - Instrucciones de instalación
   - Cómo ejecutar la aplicación
   - Formato esperado del CSV
   - Capturas de pantalla de ejemplo
4. **CSV de ejemplo** con datos ficticios para testing

## PRIORIDADES

1. **ALTA**: Cálculos correctos de los 4 informes (validar contra los resultados actuales de Excel)
2. **ALTA**: Exportación a Excel con formato y colores
3. **MEDIA**: Interfaz limpia y profesional
4. **BAJA**: Gráficos interactivos adicionales (opcional pero deseable)

## VALIDACIÓN

Para validar que los cálculos son correctos, puedo proporcionarte los valores esperados de algunas cohortes específicas:

- **Retención 2024Q1 en 2024Q2**: 7.18%
- **Supervivencia 2020Q1 Mes 12**: 40.3%
- **Frecuencia ≤30 días**: 969 intervalos, 22.65% del total, promedio 12.3 días

---

¿Este prompt es suficientemente detallado para que Claude Code pueda desarrollar la aplicación completa? Si necesitas que añada más especificaciones técnicas o ejemplos de cálculo, dímelo.