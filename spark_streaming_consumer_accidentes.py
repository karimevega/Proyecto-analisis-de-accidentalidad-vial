from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("AccidentesSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
    StructField("gravedad", StringType()),
    StructField("clase", StringType()),
    StructField("heridos", IntegerType()),
    StructField("muertos", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidentes_tiempo_real") \
    .load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadísticas por ventana de tiempo (1 minuto)
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "gravedad", "clase") \
    .agg({"heridos": "sum", "muertos": "sum"})

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
