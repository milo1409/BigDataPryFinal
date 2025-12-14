# BigDataPryFinal

## 📊 Proyecto Final Big Data – Pipeline End-to-End con PySpark

Este proyecto implementa un **sistema integral End-to-End de Big Data** que cubre las fases de **ingesta, procesamiento optimizado y visualización**, utilizando **Apache Spark (PySpark)** como motor principal.

El caso de estudio corresponde al análisis de datos históricos de emergencias (Línea 123 – Bogotá), procesando grandes volúmenes de información de manera eficiente y escalable.  
El proyecto demuestra buenas prácticas de **ingeniería de datos**, **optimización en Spark** y **portabilidad a entornos empresariales**.

---

## 🎯 Objetivo del Proyecto

- Procesar hasta **36 meses de datos históricos** sin errores de memoria.
- Construir un pipeline escalable usando Spark.
- Aplicar optimizaciones reales (broadcast, particionamiento, cache).
- Validar la ejecución en **Google Colab**.
- Generar datasets optimizados para visualización.

---

## 🚀 Ejecución del Proyecto

### 💻 Ejecución en Google Colab

#### 1️⃣ Clonar el repositorio

```bash
git clone https://github.com/milo1409/BigDataPryFinal.git
cd BigDataPryFinal
```

#### 2️⃣ Instalar dependencias

```python
!pip install -r requirements.txt
```

#### 3️⃣ Ejecutar el notebook principal

```text
notebooks/PipeLinePlay.ipynb
```

El pipeline ejecuta:
- Ingesta de datos  
- Limpieza y estandarización  
- Procesamiento distribuido con Spark  
- Generación de datasets optimizados para visualización  

---

### ☁️ Ejecución en Databricks (PoC)

1. Importar el notebook `PipeLinePlay.ipynb` al Workspace de Databricks.
2. Adjuntar un **cluster activo**.
3. Ejecutar el pipeline completo.
4. Capturar evidencia visual de la ejecución exitosa en Databricks.

<img width="1913" height="496" alt="image" src="https://github.com/user-attachments/assets/0cb8ab1a-2b13-4782-822e-0f7fc715e9c9" />

<img width="1271" height="699" alt="image" src="https://github.com/user-attachments/assets/3c829228-23c8-4cb9-87b5-2f2656bdc083" />

<img width="1297" height="691" alt="image" src="https://github.com/user-attachments/assets/69a0e581-0a3f-4702-b930-a3d0c941a7e7" />


---

## 📦 Instalación de Dependencias

```bash
pip install -r requirements.txt
```

Dependencias principales:

- pyspark
- pandas
- numpy
- matplotlib
- plotly
- psutil

---

## ⚙️ Justificación Técnica de las Optimizaciones

### 🔹 Uso de Broadcast Join

Se utilizó `broadcast()` para joins entre datasets grandes y pequeños Geocodificar las localidades, evitando operaciones costosas de shuffle y reduciendo tiempos de ejecución.

```python
from pyspark.sql.functions import broadcast
df_resultado = df_grande.join(broadcast(df_pequeno), "clave", "left")
```

---

### 🔹 Particionamiento de Datos

Se aplicó `repartition()` sobre columnas clave para mejorar paralelismo y balancear carga.

```python
df = df.repartition(200, "FECHA")
```

En etapas finales se utilizó `coalesce()`.

---

### 🔹 Cache y Persistencia

```python
df_filtrado.cache()
```

Evita recomputaciones y mejora el rendimiento general.

---

### 🔹 Filtrado Temprano y Selección de Columnas

- Reducción del volumen de datos desde etapas iniciales.
- Selección de columnas necesarias para cada proceso.

---

## 📈 Resultados Obtenidos

- Procesamiento exitoso de **36 meses de datos históricos**.
- Ejecución estable en Google Colab.
- Validación en Databricks.
- Reducción significativa de tiempos de ejecución.

---
## 🗂️ Estructura del Proyecto

```
BigDataPryFinal/
│
├── config/                 # Archivos de configuración
├── data/
│   ├── cruda/              # Datos originales descargados
│   ├── procesada/          # Datos transformados por Spark
│   └── dashboard/          # Datos finales para visualización
│
├── PipeLinePlay.ipynb  # Notebook principal del pipeline
│
├── src/                    # Código fuente PySpark
├── utilities/              # Funciones utilitarias
├── requirements.txt        # Dependencias del proyecto
└── README.md
```



## 👤 Autores

- **Oscar Clavijo**
- **Edward Daniel Porras** 
- **Camilo Andres Porras**
Proyecto Final – Big Data  
Diciembre 2025

---
