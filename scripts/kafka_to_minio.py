from pyspark.sql import SparkSession

# --- 1. Spark session ---
spark = SparkSession.builder.appName("KafkaToMinIO").getOrCreate()

# --- 2. Настройка доступа к MinIO ---
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")

# --- 3. Чтение из Kafka ---
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "pg.public.orders")\
    .option("startingOffsets", "earliest")\
    .load()

# Сырые события Debezium сохраняем как есть (JSON-строка) — это "bronze"-слой.
# Разбор JSON и дедупликация — в minio_to_clickhouse_kafka.py.
df_str = df.selectExpr("CAST(value AS STRING) AS value").where("value IS NOT NULL")

# --- 4. Запись в MinIO ---
query_minio = df_str.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://bucket/checkpoints/kafka_orders") \
    .option("path", "s3a://bucket/kafka_output") \
    .start()

query_minio.awaitTermination()