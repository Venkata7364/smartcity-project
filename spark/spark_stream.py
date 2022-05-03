from pyspark.sql import SparkSession

print("Step 1: Starting script")

spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

print("Step 2: Spark session created")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-topic") \
    .load()

print("Step 3: Connected to Kafka")

df = df.selectExpr("CAST(value AS STRING)")

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("Step 4: Streaming started")

query.awaitTermination()