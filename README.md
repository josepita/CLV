# Aplicación de Análisis CLV para Pedalmoto

Esta es una aplicación web desarrollada en Streamlit que permite analizar datos de pedidos de un e-commerce para generar informes sobre el Customer Lifetime Value (CLV), la retención de clientes y la frecuencia de compra.

## Instrucciones de Instalación

1.  **Clonar el repositorio o descargar los archivos.**

2.  **Crear un entorno virtual (recomendado):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```

3.  **Instalar las dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

## Cómo Ejecutar la Aplicación

1.  Asegúrate de tener tus datos de pedidos en un archivo CSV.
2.  Ejecuta el siguiente comando en tu terminal:
    ```bash
    streamlit run app.py
    ```
3.  La aplicación se abrirá en tu navegador web. Sube el archivo CSV para comenzar el análisis.

## Formato del CSV Esperado

El archivo CSV de entrada debe contener, como mínimo, las siguientes columnas con los nombres exactos:

-   `codigo`: ID del pedido
-   `fecha`: Fecha en formato serial de Excel (ej. 45757)
-   `fecha_hora`: Timestamp en formato Excel serial
-   `cod_cliente`: ID único del cliente
-   `nombre`: Nombre del cliente
-   `apellidos`: Apellidos del cliente
-   `email`: Email del cliente
-   `provincia`: Provincia
-   `localidad`: Ciudad
-   `pais`: País
-   `forma_pago`: Método de pago usado
-   `forma_envio`: Método de envío
-   `estado`: Estado del pedido (Entregado, Enviado, etc.)
-   `Total_pagado`: Monto total del pedido en centavos (ej. 21978 = €219.78)
-   `agrupado`: Flag si es pedido agrupado (s/n)
-   `pedidos_en_grupo`: Número de pedidos en el grupo
-   `Tiempo_desde_ultimo_pedido`: Días desde el último pedido del cliente
-   `Num_Pedidos_Cliente`: Número total de pedidos del cliente
-   `cliente_nuevo`: Flag si es cliente nuevo (s/n)

**Importante**: El campo `fecha` debe ser un número que representa los días transcurridos desde el `1900-01-01` (formato de fecha serial de Excel). El campo `Total_pagado` debe estar en centavos.

## Capturas de Pantalla de Ejemplo

*(Se añadirán capturas de pantalla una vez que la interfaz esté más desarrollada)*
