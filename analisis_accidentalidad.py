#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, count, avg, desc, hour, to_timestamp

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('AnalisisAccidentalidadVial').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/AccidentalidadVial/accidentalidad_barranquilla.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema
print("=== ESQUEMA DEL DATASET ===\n")
df.printSchema()

# Muestra las primeras filas del DataFrame
print("\n=== PRIMERAS FILAS DEL DATASET ===\n")
df.show(10, truncate=False)

# Estadísticas básicas
print("\n=== ESTADÍSTICAS BÁSICAS ===\n")
df.summary().show()

# Análisis 1: Accidentes por gravedad
print("\n=== DISTRIBUCIÓN POR GRAVEDAD DE ACCIDENTE ===\n")
gravedad = df.groupBy('GRAVEDAD_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy(desc('Total_Accidentes'))
gravedad.show()

# Análisis 2: Accidentes por tipo/clase
print("\n=== DISTRIBUCIÓN POR CLASE DE ACCIDENTE ===\n")
clase = df.groupBy('CLASE_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy(desc('Total_Accidentes'))
clase.show()

# Análisis 3: Accidentes por año
print("\n=== ACCIDENTES POR AÑO ===\n")
por_anio = df.groupBy('AÑO_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy('AÑO_ACCIDENTE')
por_anio.show()

# Análisis 4: Accidentes por mes
print("\n=== ACCIDENTES POR MES ===\n")
por_mes = df.groupBy('MES_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy(desc('Total_Accidentes'))
por_mes.show()

# Análisis 5: Accidentes por día de la semana
print("\n=== ACCIDENTES POR DÍA DE LA SEMANA ===\n")
por_dia = df.groupBy('DIA_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy(desc('Total_Accidentes'))
por_dia.show()

# Análisis 6: Zonas con mayor accidentalidad (top 10)
print("\n=== TOP 10 SITIOS CON MÁS ACCIDENTES ===\n")
zonas_peligrosas = df.groupBy('SITIO_EXACTO_ACCIDENTE').agg(count('*').alias('Total_Accidentes')).orderBy(desc('Total_Accidentes'))
zonas_peligrosas.show(10, truncate=False)

# Análisis 7: Accidentes con mayor cantidad de heridos
print("\n=== ACCIDENTES CON MÁS DE 2 HERIDOS ===\n")
con_heridos = df.filter(col('CANT_HERIDOS_EN _SITIO_ACCIDENTE') > 2).select('FECHA_ACCIDENTE', 'SITIO_EXACTO_ACCIDENTE', 'CANT_HERIDOS_EN _SITIO_ACCIDENTE', 'CLASE_ACCIDENTE').orderBy(desc('CANT_HERIDOS_EN _SITIO_ACCIDENTE'))
con_heridos.show(10, truncate=False)

# Análisis 8: Accidentes con muertes
print("\n=== ACCIDENTES CON MUERTES ===\n")
con_muertes = df.filter(col('CANT_MUERTOS_EN _SITIO_ACCIDENTE') > 0).select('FECHA_ACCIDENTE', 'SITIO_EXACTO_ACCIDENTE', 'CANT_MUERTOS_EN _SITIO_ACCIDENTE', 'CLASE_ACCIDENTE')
con_muertes.show(truncate=False)

# Análisis 9: Total de víctimas (heridos + muertos)
print("\n=== ESTADÍSTICAS DE VÍCTIMAS ===\n")
df.select(
    F.sum('CANT_HERIDOS_EN _SITIO_ACCIDENTE').alias('Total_Heridos'),
    F.sum('CANT_MUERTOS_EN _SITIO_ACCIDENTE').alias('Total_Muertos')
).show()

print("\n=== ANÁLISIS COMPLETADO ===\n")
