# Análisis de Accidentalidad Vial en Barranquilla con Big Data

## 📋 Descripción del Proyecto

Este proyecto implementa un sistema de análisis de datos de accidentalidad vial en Barranquilla, Colombia, utilizando tecnologías de Big Data para procesamiento tanto en batch como en tiempo real. El objetivo es identificar patrones de riesgo, zonas peligrosas y tendencias que puedan apoyar la toma de decisiones en seguridad vial.

## 🎯 Problemática

En las principales ciudades del país, los accidentes de tránsito son una de las causas más frecuentes de emergencias urbanas. Aunque existen reportes y bases de datos, muchas veces no se analizan en tiempo real, lo que impide detectar zonas peligrosas o patrones de riesgo que podrían prevenir futuros accidentes.

*Pregunta de investigación:* ¿Cómo analizar grandes volúmenes de datos sobre accidentes de tránsito para identificar patrones de riesgo y apoyar la toma de decisiones en seguridad vial?

## 📊 Dataset

- *Nombre:* Accidentalidad en Barranquilla
- *Fuente:* [Datos Abiertos de Colombia](https://www.datos.gov.co/)
- *URL:* https://www.datos.gov.co/api/views/yb9r-2dsi/rows.csv

### Columnas principales:
- Fecha y hora del accidente
- Gravedad del accidente
- Clase de accidente (Atropello, Choque, Volcamiento, etc.)
- Sitio exacto del accidente
- Cantidad de heridos y muertos
- Año, mes y día del accidente

## 🛠️ Tecnologías Utilizadas

- *Apache Hadoop 3.x:* Sistema de archivos distribuido (HDFS)
- *Apache Spark 3.5.3:* Procesamiento de datos en batch
- *Apache Kafka 3.6.2:* Sistema de mensajería distribuida
- *Spark Streaming:* Procesamiento de datos en tiempo real
- *PySpark:* API de Python para Spark
- *Python 3.x:* Lenguaje de programación
- *ZooKeeper:* Coordinación de servicios distribuidos

## 🏗️ Arquitectura de la solucion en spark 


### Procesamiento en Batch
1. Carga del dataset desde HDFS
2. Análisis exploratorio de datos (EDA)
3. Identificación de patrones y tendencias
4. Almacenamiento de resultados procesados

### Procesamiento en Tiempo Real
1. Simulación de reportes de accidentes con Kafka Producer
2. Consumo de datos con Spark Streaming
3. Análisis en ventanas de tiempo (1 minuto)
4. Visualización de estadísticas en tiempo real

## 📁 Estructura del Proyecto

├── analisis_accidentalidad.py          # Script de procesamiento en batch
├── kafka_producer_accidentes.py        # Productor de Kafka (datos simulados)
├── spark_streaming_consumer_accidentes.py  # Consumidor Spark Streaming
└── README.md                           # Este archivo

### Instalación de dependencias:
bash
sudo pip install pyspark
pip install kafka-python


## 🚀 Cómo Ejecutar el Proyecto

### Paso 1: Preparar el entorno

#### 1.1 Iniciar Hadoop
bash
# Conectarse como usuario hadoop
su - hadoop
# Password: hadoop

# Iniciar el clúster de Hadoop
start-all.sh


#### 1.2 Crear directorio en HDFS y cargar dataset
bash
# Crear carpeta en HDFS
hdfs dfs -mkdir /AccidentalidadVial

# Descargar el dataset
wget -O accidentalidad_barranquilla.csv https://www.datos.gov.co/api/views/yb9r-2dsi/rows.csv

# Copiar al HDFS
hdfs dfs -put accidentalidad_barranquilla.csv /AccidentalidadVial

# Verificar
hdfs dfs -ls /AccidentalidadVial


### Paso 2: Procesamiento en Batch
bash
# Cambiar a usuario vboxuser
# Password: bigdata

# Ejecutar análisis en batch
python3 analisis_accidentalidad.py


*Resultados del análisis batch:*
- Esquema del dataset
- Estadísticas básicas
- Distribución por gravedad y clase de accidente
- Accidentes por año, mes y día de la semana
- Top 10 sitios con más accidentes
- Accidentes con heridos y muertos
- Total de víctimas

### Paso 3: Procesamiento en Tiempo Real

#### 3.1 Iniciar ZooKeeper y Kafka

*Terminal 1 - ZooKeeper:*
bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties


*Terminal 2 - Kafka:*
bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties


#### 3.2 Crear topic de Kafka
*Terminal 3:*
bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic accidentes_tiempo_real

# Verificar
/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092


#### 3.3 Ejecutar Productor
*Terminal 4:*
bash
python3 kafka_producer_accidentes.py


#### 3.4 Ejecutar Consumidor con Spark Streaming
*Terminal 5:*
bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_accidentes.py


### Paso 4: Monitorear la ejecución
#### Interfaces Web:
- *Spark Jobs (context web UI):* http://192.168.1.20:4040

## 🔍 Análisis Realizados

1. *Análisis de gravedad:* Clasificación de accidentes por nivel de severidad
2. *Análisis de tipos:* Distribución por clase de accidente (atropello, choque, etc.)
3. *Análisis temporal:* Identificación de períodos de mayor riesgo
4. *Análisis geográfico:* Detección de puntos críticos (zonas peligrosas)
5. *Análisis de víctimas:* Cuantificación de impacto humano
6. *Análisis en tiempo real:* Procesamiento de eventos conforme ocurren

## 🎓 Conceptos Implementados

### RDDs y DataFrames de Spark
- Uso de DataFrames para análisis estructurado
- Transformaciones: filter, select, groupBy, agg
- Acciones: show, count, collect

## 👨‍💻 Autor

-**Karime Vega**
- Universidad:Universidad Nacional Abierta y a Distancia
- Curso: Big Data

## 📚 Referencias

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Datos Abiertos Colombia](https://www.datos.gov.co/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
