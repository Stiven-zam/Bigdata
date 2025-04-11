
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, when
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName('Tarea3').getOrCreate()


file_path = 'hdfs://localhost:9000/Tarea3_practica/rows.csv.1'

try:
    df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)
    print("Archivo cargado correctamente.")
except Exception as e:
    print("Error al cargar el archivo:", e)
    exit()


df.printSchema()


df.show()

# Limpieza de datos: Eliminamos filas con valores nulos en las columnas importantes
df_clean = df.dropna(subset=['EDAD DE ATENCION (AÑOS)', 'AÑO REPORTADO', 'NOMBRE DEL DIAGNOSTICO'])

for col_name in ['EDAD DE ATENCION (AÑOS)', 'AÑO REPORTADO']:
    df_clean = df_clean.withColumn(col_name, F.regexp_replace(F.col(col_name), '[^0-9]', '').cast('int'))

print("Top 10 diagnósticos más comunes\n")
Diagnosticos_comunes = df_clean.groupBy('NOMBRE DEL DIAGNOSTICO') \
                         .count() \
                         .withColumnRenamed('count', 'total_pacientes') \
                         .orderBy(F.col('total_pacientes').desc()) \
                         .limit(10)

Diagnosticos_comunes.show(truncate=False)


df_distribucion = df_clean.withColumn(
    "rango_edad",
    F.when(F.col('EDAD DE ATENCION (AÑOS)') <= 5, "Primera infancia")
     .when((F.col('EDAD DE ATENCION (AÑOS)') >= 6) & (F.col('EDAD DE ATENCION (AÑOS)') <= 11), "Infancia")
     .when((F.col('EDAD DE ATENCION (AÑOS)') >= 12) & (F.col('EDAD DE ATENCION (AÑOS)') <= 18), "Adolescencia")
     .when((F.col('EDAD DE ATENCION (AÑOS)') >= 14) & (F.col('EDAD DE ATENCION (AÑOS)') <= 26), "Juventud")
     .when((F.col('EDAD DE ATENCION (AÑOS)') >= 27) & (F.col('EDAD DE ATENCION (AÑOS)') <= 59), "Adultez")
     .otherwise("Persona mayor")
)


ventana = Window.partitionBy("rango_edad").orderBy(F.col("count").desc())


df_distribucion = df_distribucion.groupBy("rango_edad", "NOMBRE DEL DIAGNOSTICO") \
                                 .count()


df_top3 = df_distribucion.withColumn("rank", row_number().over(ventana)) \
                         .filter(F.col("rank") <= 3)


df_top3 = df_top3.withColumn(
    "orden",
    when(col("rango_edad") == "Primera infancia", 1)
     .when(col("rango_edad") == "Infancia", 2)
     .when(col("rango_edad") == "Adolescencia", 3)
     .when(col("rango_edad") == "Juventud", 4)
     .when(col("rango_edad") == "Adultez", 5)
     .when(col("rango_edad") == "Persona mayor", 6)
)


print("Top 3 Diagnósticos más comunes por rango de edad (ordenado)")
df_top3.orderBy("orden", "rank").select("rango_edad", "NOMBRE DEL DIAGNOSTICO", "count", "rank").show(truncate=False)


try:
    output_path1 = 'hdfs://localhost:9000/Tarea3_practica/diagnosticos_comunes'
    output_path2 = 'hdfs://localhost:9000/Tarea3_practica/top3_por_rango_edad'

 
    Diagnosticos_comunes.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(output_path1)

 
    df_top3.orderBy("orden", "rank").select("rango_edad", "NOMBRE DEL DIAGNOSTICO", "count", "rank") \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(output_path2)

    print(f"Resultados guardados correctamente en:\n{output_path1}\n{output_path2}")

except Exception as e:
    print("Error al guardar los resultados:", e)
