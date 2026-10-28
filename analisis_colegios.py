from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, count,
    desc, explode, split
)

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisColegiosBogota") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ruta = "/home/vboxuser/colegios_bogota/LISTADO_COLEGIOS_BOGOTA_20261027.csv"
salida = "/home/vboxuser/colegios_bogota/resultados_batch"

print("\n========== 1. CARGA DE DATOS ==========")

# 2. Cargar el archivo CSV
df_original = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .csv(ruta)

print("Columnas encontradas:")
print(df_original.columns)

print("Cantidad de registros originales:", df_original.count())

print("\n========== 2. LIMPIEZA ==========")

# 3. Limpiar espacios en los campos de texto
df = df_original
for nombre in df.columns:
    df = df.withColumn(
        nombre,
        trim(col(nombre).cast("string"))
    )

# 4. Convertir cadenas vacías en valores nulos
for nombre in df.columns:
    df = df.withColumn(
        nombre,
        when(col(nombre) == "", None).otherwise(col(nombre))
    )

# 5. Eliminar registros completamente duplicados
cantidad_antes = df.count()
df = df.dropDuplicates()
cantidad_despues = df.count()

print("Registros antes de eliminar duplicados:", cantidad_antes)
print("Registros después de eliminar duplicados:", cantidad_despues)
print("Duplicados eliminados:", cantidad_antes - cantidad_despues)

# 6. Revisar campos esenciales
df_limpio = df.filter(
    col("nombreestablecimiento").isNotNull()
)

print(
    "Registros con nombre de establecimiento:",
    df_limpio.count()
)

print("\n========== 3. ANÁLISIS EXPLORATORIO ==========")

# 7. Estadísticas generales de campos importantes
for campo in [
    "sector",
    "zona",
    "tipo_Establecimiento",
    "genero",
    "estado",
    "calendario"
]:
    if campo in df_limpio.columns:
        print(f"\nDistribución por {campo}:")
        df_limpio.groupBy(campo) \
            .count() \
            .orderBy(desc("count")) \
            .show(20, truncate=False)

# 8. Cantidad de colegios por año
if "año" in df_limpio.columns:
    print("\nRegistros por año:")
    df_limpio.groupBy("año") \
        .count() \
        .orderBy("año") \
        .show(30, truncate=False)

# 9. Cantidad de establecimientos por zona
print("\n========== 4. ANÁLISIS POR ZONA ==========")
df_limpio.groupBy("zona") \
    .count() \
    .orderBy(desc("count")) \
    .show(truncate=False)

# 10. Analizar niveles educativos declarados
print("\n========== 5. NIVELES EDUCATIVOS ==========")

niveles = df_limpio.filter(
    col("niveles").isNotNull()
).withColumn(
    "nivel_educativo",
    explode(split(col("niveles"), ","))
)

niveles = niveles.withColumn(
    "nivel_educativo",
    trim(col("nivel_educativo"))
)

niveles.groupBy("nivel_educativo") \
    .count() \
    .orderBy(desc("count")) \
    .show(30, truncate=False)

print("\n========== 6. VALORES NULOS ==========")

# 11. Calcular valores vacíos por columna
for nombre in df.columns:
    total_nulos = df.filter(col(nombre).isNull()).count()
    print(f"{nombre}: {total_nulos} valores vacíos")

print("\n========== 7. GUARDAR RESULTADOS ==========")

# 12. Guardar datos limpios
df_limpio.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(salida + "/datos_limpios")

# 13. Guardar resumen por sector
df_limpio.groupBy("sector") \
    .count() \
    .orderBy(desc("count")) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(salida + "/resumen_sector")

# 14. Guardar resumen por zona
df_limpio.groupBy("zona") \
    .count() \
    .orderBy(desc("count")) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(salida + "/resumen_zona")

# 15. Guardar resumen de niveles educativos
niveles.groupBy("nivel_educativo") \
    .count() \
    .orderBy(desc("count")) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(salida + "/resumen_niveles")

print("\nAnálisis Batch finalizado.")
print("Resultados guardados en:", salida)

spark.stop()
