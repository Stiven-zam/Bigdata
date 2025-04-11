from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SensorRealTimeAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir esquema
schema = StructType([
    StructField("id_tienda", IntegerType()),
    StructField("transacciones", IntegerType()),
    StructField("total", FloatType()),
    StructField("timestamp", TimestampType())
])

# Leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos_tienda") \
    .load()

# Parsear JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Procesar datos en ventanas de 10 segundos
windowed_analysis = parsed_df \
    .groupBy(window(col("timestamp"), "10 seconds")) \
    .agg(
        count("*").alias("event_count"),
        avg("transacciones").alias("promedio_transacciones"),
        min("transacciones").alias("transacciones minimas"),  
        max("transacciones").alias("transacciones maximas"),
        avg("total").alias("promedio_total"),
        min("total").alias("total minimo"),
        max("total").alias("total maximo ")                   
    ) \
    .select(
        "window.start",
        "window.end",
        "numero_transacciones",
        "promedio_transacciones",
        "transacciones minimas",
        "transacciones maximas",
        "promedio_total",
        "total minimo",
        "total maximo"
    )

# Mostrar en consola cada 10 segundos
query = windowed_analysis \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
