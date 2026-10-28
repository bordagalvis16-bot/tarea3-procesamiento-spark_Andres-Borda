from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

spark = SparkSession.builder \
    .appName("AnalisisStreamingColegiosBogota") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Estructura de los datos recibidos desde Kafka
schema = StructType([
    StructField("nombreestablecimiento", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("tipo_Establecimiento", StringType(), True),
    StructField("calendario", StringType(), True),
    StructField("año", StringType(), True)
])

# Lectura continua del tópico de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "colegios_bogota") \
    .option("startingOffsets", "earliest") \
    .load()

# Interpretar los mensajes JSON
colegios = df.select(
    from_json(col("value").cast("string"), schema).alias("datos"),
    col("timestamp").alias("fecha_evento")
).select("datos.*", "fecha_evento")

# Contar los registros recibidos por ventana de un minuto
resumen = colegios \
    .withWatermark("fecha_evento", "2 minutes") \
    .groupBy(
        window(col("fecha_evento"), "1 minute"),
        col("zona")
    ) \
    .agg(count("*").alias("cantidad_colegios"))

# Mostrar resultados en la terminal
consulta = resumen.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

consulta.awaitTermination()
