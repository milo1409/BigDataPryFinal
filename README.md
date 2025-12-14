# BigDataPryFinal

## 📊 Proyecto Final Big Data – Pipeline End-to-End con PySpark

Este proyecto implementa un **sistema integral End-to-End de Big Data** que cubre las fases de **ingesta, procesamiento optimizado y visualización**, utilizando **Apache Spark (PySpark)** como motor principal.

El caso de estudio corresponde al análisis de datos históricos de emergencias (Línea 123 – Bogotá), procesando grandes volúmenes de información de manera eficiente y escalable.  
El proyecto demuestra buenas prácticas de **ingeniería de datos**, **optimización en Spark** y **portabilidad a entornos empresariales**.

---

## 🎯 Objetivo del Proyecto

- Procesar hasta **24 meses de datos históricos** sin errores de memoria.
- Construir un pipeline escalable usando Spark.
- Aplicar optimizaciones reales (broadcast, particionamiento, cache).
- Validar la ejecución tanto en **Google Colab** como en **Databricks**.
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
4. Capturar evidencia visual de la ejecución exitosa.

Este paso valida la **portabilidad del pipeline a un entorno empresarial**.

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

Se utilizó `broadcast()` para joins entre datasets grandes y pequeños, evitando operaciones costosas de shuffle y reduciendo tiempos de ejecución.

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

### 🔹 Ajustes de Configuración Spark

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

---

## 📈 Resultados Obtenidos

- Procesamiento exitoso de **24 meses de datos históricos**.
- Ejecución estable en Google Colab.
- Validación exitosa en Databricks Community Edition.
- Reducción significativa de tiempos de ejecución.

---

## 🗂️ Estructura del Proyecto

```
BigDataPryFinal/
│
├── config/
├── data/
├── notebooks/
│   └── PipeLinePlay.ipynb
├── src/
├── requirements.txt
└── README.md
```

---

## 👤 Autor

**Andrés Porras**  
Proyecto Final – Big Data  
Diciembre 2025

---

## 📝 Notas Finales

Este proyecto demuestra un enfoque profesional de ingeniería de datos, aplicando optimizaciones reales de Apache Spark y validando su ejecución en entornos académicos y empresariales.
