# tarea3-procesamiento-spark_Andres-Borda
Tarea 3 - Procesamiento de Datos con Apache Spark
# Tarea 3 - Procesamiento de Datos con Apache Spark

Descripción del proyecto

El presente proyecto tiene como finalidad aplicar herramientas de Big Data para procesar y analizar información de establecimientos educativos de Bogotá. Para ello, se utiliza Apache Spark para el procesamiento por lotes y la integración de Apache Kafka con Spark Structured Streaming para el procesamiento de datos en tiempo real.

Objetivo
Implementar un proceso de análisis de datos que permita realizar actividades de carga, limpieza, transformación y exploración de información educativa, utilizando tecnologías de procesamiento de datos para identificar características y distribuciones de los establecimientos educativos de Bogotá.

Tecnologías utilizadas

- Apache Spark 3.5.8.
- Apache Kafka 3.9.2.
- Python y PySpark.
- Spark Structured Streaming.
- Archivos de datos en formato CSV y mensajes en formato JSON.
- Sistema operativo Ubuntu.

Dataset utilizado

Para el desarrollo del proyecto se utiliza un conjunto de datos en formato CSV que contiene información de establecimientos educativos de Bogotá. Este archivo sirve como fuente para realizar el análisis por lotes y alimentar el flujo de datos enviado a Kafka.

El dataset utilizado se denomina LISTADO_COLEGIOS_BOGOTA_20261027.csv.

Procesamiento por lotes (Batch)

En esta etapa se realiza la lectura del conjunto de datos mediante Apache Spark. Posteriormente, se lleva a cabo la limpieza de los campos de texto, la identificación de registros duplicados, la revisión de valores nulos y el análisis exploratorio de diferentes características de los establecimientos educativos.

También se generan estadísticas relacionadas con las zonas, los tipos de establecimiento, los calendarios académicos y los niveles educativos declarados. Finalmente, los datos procesados y los resúmenes obtenidos se almacenan para su posterior consulta.

Procesamiento en tiempo real

Para el procesamiento en tiempo real se integra Apache Kafka con Spark Structured Streaming. El productor lee los registros del archivo CSV y los envía al tópico denominado colegios_bogota.

Posteriormente, el consumidor de Spark recibe los mensajes, interpreta la información transmitida y agrupa los registros por zona en ventanas de tiempo de un minuto. Los resultados del procesamiento se presentan en la consola, lo que permite observar el comportamiento del flujo de datos.

Organización del proyecto

El repositorio contiene los siguientes archivos principales:

- analisis_colegios.py: contiene el proceso de carga, limpieza y análisis de los datos mediante Batch.
- kafka_producer.py:se encarga de leer el dataset y enviar los registros al tópico de Kafka.
- spark_streaming_consumer.py: recibe los mensajes de Kafka y procesa los registros mediante Spark Structured Streaming.

Gracias por su atencion

Proyecto académico desarrollado en el marco de la actividad Tarea 3: Procesamiento de Datos con Apache Spark.
