# Diagnóstico Distribuido de Capacidades Digitales y Resiliencia Operativa Post-Sismo (MinTIC)

Solución integral de procesamiento distribuido desarrollada para la **Tarea 3 del curso Big Data (UNAD)**, combinando almacenamiento tolerante a fallos en **Hadoop HDFS**, analítica por lotes en **Apache Spark (PySpark)** y monitoreo telemétrico en tiempo real con **Apache Kafka** y **Spark Structured Streaming**.

## 1. Conjunto de Datos (Fase Batch)
* **Nombre:** Resultados encuesta entidades Terremoto (*Diagnóstico para la Identificación de Afectaciones en Capacidades Digitales*).
* **Fuente:** Ministerio de Tecnologías de la Información y las Comunicaciones (MinTIC) – Datos Abiertos Colombia (`datos.gov.co`).
* **Dimensiones:** 409 registros de entidades territoriales (Instituciones Educativas, Alcaldías, Hospitales/ESE, Gobernaciones) y 61 variables.
* **Cobertura Geográfica:** Risaralda, Valle del Cauca, Caldas, Chocó y Quindío.

## 2. Estructura del Repositorio
* `tarea3_terremoto.py`: Script de procesamiento Batch en PySpark (4 pipelines analíticos con lectura multilínea, ingeniería de tiempos y `Window Functions`).
* `kafka_producer_terremoto.py`: Productor en Python que simula telemetría continua de nodos públicos hacia el topic `resiliencia_tic_sismo`.
* `spark_streaming_terremoto.py`: Consumidor en Spark Structured Streaming con ventanas de 1 minuto y clasificación automática de alertas operativas.

## 3. Despliegue y Ejecución

### A. Ingesta en Hadoop HDFS y Procesamiento Batch
```bash
hdfs dfs -mkdir -p /ProyectoTerremoto/
hdfs dfs -put /home/hadoop/encuesta_terremoto.csv /ProyectoTerremoto/
python3 tarea3_terremoto.py
