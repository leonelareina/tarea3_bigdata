from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Inicialización de la Sesión de Apache Spark
spark = SparkSession.builder \
    .appName("DiagnosticoCapacidadesDigitalesTerremoto2026") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================================================================
# PIPELINE 1: INGESTA MULTILÍNEA, NORMALIZACIÓN DE ESQUEMA E INGENIERÍA DE TIEMPO
# Descripción: Lee el CSV desde HDFS habilitando 'multiLine' para soportar saltos
# de línea en respuestas abiertas, renombra variables diagnósticas clave y calcula
# los minutos invertidos por cada entidad en diligenciar la encuesta.
# Muestra: Total de registros validados, esquema estructurado y muestra limpia.
# ==============================================================================
print("\n" + "="*85)
print(">>> PIPELINE 1: INGESTA MULTILÍNEA Y NORMALIZACIÓN DE ESQUEMA (HDFS) <<<")
print("="*85)

hdfs_path = "hdfs://localhost:9000/ProyectoTerremoto/encuesta_terremoto.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("encoding", "UTF-8") \
    .load(hdfs_path)

# Uso de backticks (` `) en nombres de columna que terminan en punto (.)
df_clean = df_raw.select(
    F.col("ID").cast("integer").alias("id_respuesta"),
    F.to_timestamp(F.col("Hora de inicio"), "yyyy MMM dd hh:mm:ss a").alias("ts_inicio"),
    F.to_timestamp(F.col("Hora de finalización"), "yyyy MMM dd hh:mm:ss a").alias("ts_fin"),
    F.upper(F.trim(F.col("`Seleccione el departamento.`"))).alias("departamento"),
    F.upper(F.trim(F.col("`Seleccione el municipio del departamento.`"))).alias("municipio"),
    F.trim(F.col("Seleccione Tipo de entidad")).alias("tipo_entidad"),
    F.trim(F.col("RAZON SOCIAL (Nombre) de la entidad")).alias("entidad"),
    F.split(F.col("¿Cuál es el nivel general de afectación de las capacidades digitales como consecuencia del evento?"), ":").getItem(0).alias("nivel_afectacion"),
    F.trim(F.col("¿La entidad cuenta actualmente con suministro estable de energía eléctrica?")).alias("estado_energia"),
    F.trim(F.col("¿Cuál es el estado actual de la conectividad a Internet?")).alias("estado_internet"),
    F.upper(F.trim(F.col("Indique el operador o los operadores de conectividad que proveen servicios a la entidad"))).alias("operador_red")
).withColumn(
    "minutos_diligenciamiento",
    F.round((F.unix_timestamp("ts_fin") - F.unix_timestamp("ts_inicio")) / 60.0, 2)
).filter(F.col("departamento").isNotNull() & F.col("nivel_afectacion").isNotNull())

df_clean.cache()
print(f"Total de entidades territoriales procesadas y validadas: {df_clean.count()}")
df_clean.printSchema()

# ==============================================================================
# PIPELINE 2: DIAGNÓSTICO DE SEVERIDAD POR SECTOR Y TIEMPOS DE RESPUESTA
# Descripción: Agrupa las entidades por tipo (Instituciones Educativas, Alcaldías,
# Hospitales/ESE, Gobernaciones) y nivel de afectación digital, calculando el
# conteo de entidades y métricas estadísticas sobre el tiempo de reporte.
# Muestra: Tabla cruzada de criticidad sectorial y tiempos promedio/medianos.
# ==============================================================================
print("\n" + "="*85)
print(">>> PIPELINE 2: SEVERIDAD OPERATIVA POR TIPO DE ENTIDAD Y TIEMPO DE REPORTE <<<")
print("="*85)

stats_sector = df_clean.groupBy("tipo_entidad", "nivel_afectacion").agg(
    F.count("id_respuesta").alias("total_entidades"),
    F.round(F.avg("minutos_diligenciamiento"), 2).alias("tiempo_prom_min"),
    F.round(F.expr("percentile_approx(minutos_diligenciamiento, 0.5)"), 2).alias("mediana_p50_min"),
    F.round(F.max("minutos_diligenciamiento"), 2).alias("tiempo_max_min")
).orderBy("tipo_entidad", F.col("total_entidades").desc())

stats_sector.show(20, truncate=False)

# ==============================================================================
# PIPELINE 3: RANKING TERRITORIAL DE MUNICIPIOS CRÍTICOS (WINDOW FUNCTIONS)
# Descripción: Aplica Window Functions (partitionBy departamento) para identificar
# el municipio con mayor cantidad de entidades en estado 'Crítico' o 'Alto' dentro
# de cada uno de los 5 departamentos evaluados tras el sismo.
# Muestra: Ranking Top 1 de municipios más afectados por departamento.
# ==============================================================================
print("\n" + "="*85)
print(">>> PIPELINE 3: MUNICIPIO MÁS AFECTADO POR DEPARTAMENTO (WINDOW FUNCTIONS) <<<")
print("="*85)

df_criticos = df_clean.filter(F.col("nivel_afectacion").isin("Crítico", "Alto")) \
    .groupBy("departamento", "municipio") \
    .agg(F.count("id_respuesta").alias("entidades_criticas_o_altas"))

windowSpec = Window.partitionBy("departamento").orderBy(F.col("entidades_criticas_o_altas").desc())

df_top_municipios = df_criticos.withColumn("rank", F.rank().over(windowSpec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank") \
    .orderBy(F.col("entidades_criticas_o_altas").desc())

df_top_municipios.show(15, truncate=False)

# ==============================================================================
# PIPELINE 4: BRECHA DE INFRAESTRUCTURA BASE (CONECTIVIDAD Y ENERGÍA ELÉCTRICA)
# Descripción: Cruza la disponibilidad de suministro eléctrico con el estado de
# conectividad a Internet por departamento para identificar fallas compuestas.
# Muestra: Matriz territorial de continuidad de servicios básicos TIC.
# ==============================================================================
print("\n" + "="*85)
print(">>> PIPELINE 4: DIAGNÓSTICO DE CONTINUIDAD ENERGÉTICA Y CONECTIVIDAD <<<")
print("="*85)

df_infra = df_clean.groupBy("departamento", "estado_energia", "estado_internet").agg(
    F.count("id_respuesta").alias("cantidad_entidades")
).orderBy("departamento", F.col("cantidad_entidades").desc())

df_infra.show(20, truncate=False)

spark.stop()
