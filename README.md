# bigDataFinalProject

## Descripción del Proyecto
[Breve descripción de qué hace tu proyecto de Big Data - por ejemplo: "Análisis de datos masivos utilizando Apache Spark para procesar y transformar datasets de gran escala"]

## Instrucciones de Despliegue

### Requisitos Previos
- Python 3.8 o superior
- Apache Spark / PySpark
- Databricks (opcional, si usas la plataforma en la nube)
- Java 8 o 11 (requerido por Spark)

### Instalación de Librerías

#### Opción 1: Usando pip (Ambiente Local)
```bash
pip install pyspark
pip install pandas
pip install numpy
pip install matplotlib
# Agrega otras librerías específicas de tu proyecto
```

#### Opción 2: Usando requirements.txt
```bash
pip install -r requirements.txt
```

### Ejecución del Código

#### En ambiente local:
```bash
# Opción 1: Usando spark-submit
spark-submit test.py

# Opción 2: Usando Python directamente
python test.py
```

#### En Databricks:
1. Subir el archivo al **Workspace** de Databricks
2. Crear un **Notebook** o importar el script
3. Adjuntar al **cluster activo**
4. Ejecutar cada celda secuencialmente o ejecutar todo el notebook

## Justificación Técnica

### Optimizaciones Aplicadas

#### 1. Broadcast Variables
**¿Por qué se utilizó?**
- Se utilizó broadcast para distribuir DataFrames pequeños (tablas de dimensiones o catálogos) a todos los workers del cluster.
- **Tamaño del dataset broadcast**: [Ejemplo: "Tabla de categorías de ~5 MB con 1,000 registros"]
- **Beneficio**: Evita operaciones shuffle costosas durante joins, manteniendo los datos pequeños en memoria de cada executor, lo que reduce significativamente el tiempo de procesamiento.
- **Implementación**:
  ```python
  from pyspark.sql.functions import broadcast
  df_result = df_large.join(broadcast(df_small), "key_column")
  ```

#### 2. Particionamiento
**Estrategia de particionamiento:**
- Se reparticionó el dataset principal por columna(s) clave: [Ejemplo: "`fecha`" o "`categoria`"]
- **Número de particiones**: [Ejemplo: "100 particiones basadas en el tamaño del cluster"]
- **Razón**: 
  - Optimizar operaciones de agregación y group by
  - Balancear la carga de trabajo entre todos los executors
  - Reducir el data skew (desbalance de datos)
  - Mejorar el paralelismo en operaciones subsecuentes
- **Código implementado**:
  ```python
  df = df.repartition(100, "column_name")
  # o para coalesce si reducimos particiones:
  df = df.coalesce(50)
  ```

#### 3. Otras Optimizaciones
- **Caché/Persist**: [Ejemplo: "Se aplicó cache() a DataFrames reutilizados múltiples veces en el pipeline"]
  ```python
  df_cached = df.cache()
  ```
- **Filtrado temprano**: [Ejemplo: "Filtros aplicados antes de joins para reducir volumen de datos"]
- **Selección de columnas específicas**: [Ejemplo: "Uso de select() para trabajar solo con columnas necesarias"]
- **Configuración de Spark**: [Ejemplo: "Ajuste de spark.sql.shuffle.partitions a 200"]

## Evidencia de Ejecución

### Captura de Pantalla en Databricks
![Ejecución en Databricks](./screenshots/databricks_execution.png)

*Figura 1: Resultado de la ejecución en Databricks mostrando [describe qué muestra: tiempos de ejecución, métricas de rendimiento, resultados del procesamiento, etc.]*

> **Nota**: Para agregar tu captura de pantalla:
> 1. Crea una carpeta llamada `screenshots` en el directorio del proyecto
> 2. Guarda tu captura de Databricks con un nombre descriptivo
> 3. Actualiza la ruta en la línea de arriba

## Resultados
[Describe los resultados obtenidos del procesamiento, por ejemplo:
- Tiempo de ejecución: X minutos
- Registros procesados: X millones
- Métricas de rendimiento
- Insights o hallazgos principales]

## Estructura del Proyecto
```
bigDataFinalProject/
│
├── README.md              # Este archivo
├── test.py                # Script principal de procesamiento
├── screenshots/           # Capturas de pantalla de evidencia
│   └── databricks_execution.png
└── requirements.txt       # Dependencias del proyecto (opcional)
```

## Autores
- [Tu nombre]

## Fecha
Diciembre 2024

---

## Notas Adicionales
[Cualquier información adicional relevante sobre el proyecto, limitaciones conocidas, trabajo futuro, etc.]