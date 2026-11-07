from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Inicialización de Spark Structured Streaming
spark = SparkSession.builder \
    .appName("SparkStreamingResilienciaTICSismo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema tipado del payload JSON enviado por el productor
schema = StructType([
    StructField("nodo_id", StringType()),
    StructField("entidad", StringType()),
    StructField("tipo_entidad", StringType()),
    StructField("municipio", StringType()),
    StructField("departamento", StringType()),
    StructField("estado_energia", StringType()),
    StructField("latencia_red_ms", DoubleType()),
    StructField("disponibilidad_servicios_pct", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Lectura continua desde el Topic de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "resiliencia_tic_sismo") \
    .load()

# Parseo del JSON
parsed_df = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Clasificación en tiempo real de criticidad operativa
df_alertas = parsed_df.withColumn(
    "nivel_alerta_tic",
    when((col("disponibilidad_servicios_pct") < 50.0) | (col("estado_energia") == "Sin suministro"), "EMERGENCIA OPERATIVA TIC")
    .when((col("disponibilidad_servicios_pct") < 85.0) | (col("latencia_red_ms") > 150.0), "CONTINGENCIA PREVENTIVA")
    .otherwise("OPERACIÓN NORMAL")
)

# Agregación por ventanas de 1 minuto, departamento y nivel de alerta
windowed_analysis = df_alertas \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("departamento"),
        col("nivel_alerta_tic")
    ) \
    .agg(
        expr("count(nodo_id) as reportes_ventana"),
        expr("round(avg(disponibilidad_servicios_pct), 2) as prom_disponibilidad_pct"),
        expr("round(max(latencia_red_ms), 1) as max_latencia_ms")
    )

# Salida en consola en tiempo real
query = windowed_analysis.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
