from pyspark.sql import SparkSession
from pyspark.sql.functions import split, when

# Inisialisasi sesi Spark dengan nama "SensorStream"
spark = SparkSession.builder.appName("SensorStream").getOrCreate()

# Membaca data streaming dari topik Kafka "edodf-data-raw"
sensor_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092") \
    .option("subscribe", "edodf-data-raw") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.username","HZ353Z5ZYNCOECD5") \
    .option("kafka.sasl.password", "60NbFFdoYx9FbBQJ/yzctTzU1SxNz0bwgBOcThzUt1YwXyHdtVYpQY+UY1d2X8BR") \
    .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.plain.PlainLoginModule required username="HZ353Z5ZYNCOECD5" password="60NbFFdoYx9FbBQJ/yzctTzU1SxNz0bwgBOcThzUt1YwXyHdtVYpQY+UY1d2X8BR";""") \
    .load()

# Menggunakan fungsi selectExpr untuk memisahkan nilai yang masuk dari topik Kafka berdasarkan koma
raw_df = sensor_df.selectExpr("SPLIT(CAST(value AS STRING), ',' ) arr")

# Membuat DataFrame baru dengan mengekstrak kolom-kolom tertentu dari array yang dihasilkan oleh pemisahan sebelumnya
select_df = raw_df.withColumn("beach_name", raw_df['arr'][0]) \
    .withColumn("measurement_timestamp", raw_df['arr'][1]) \
    # ... (melanjutkan dengan menambahkan kolom lainnya)

# Menulis hasil transformasi kembali ke topik Kafka "edodf-data-clean"
query = select_df.selectExpr("CAST(measurement_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint") \
    .option("kafka.bootstrap.servers", "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092") \
    .option("topic", "edodf-data-clean") \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.username","HZ353Z5ZYNCOECD5") \
    .option("kafka.sasl.password", "60NbFFdoYx9FbBQJ/yzctTzU1SxNz0bwgBOcThzUt1YwXyHdtVYpQY+UY1d2X8BR") \
    .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.plain.PlainLoginModule required username="HZ353Z5ZYNCOECD5" password="60NbFFdoYx9FbBQJ/yzctTzU1SxNz0bwgBOcThzUt1YwXyHdtVYpQY+UY1d2X8BR";""") \
    .start()

# Menunggu hingga streaming berhenti (dapat dihentikan secara manual)
query.awaitTermination()
