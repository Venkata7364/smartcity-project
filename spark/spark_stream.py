from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

print("Step 1: Starting script")

spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Step 2: Spark session created")

postgres_url = "jdbc:postgresql://postgres:5432/smartcity"

postgres_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

s3_base_path = "s3a://smartcity-data-lake-laksh/processed"

iot_schema = StructType([
    StructField("type", StringType(), True),
    StructField("vehicle_id", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("level", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("event", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("timestamp", DoubleType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "iot-topic") \
    .option("startingOffsets", "latest") \
    .load()

print("Step 3: Connected to Kafka")

raw_df = df.selectExpr("CAST(value AS STRING) AS raw_value")

parsed_df = raw_df.select(
    from_json(col("raw_value"), iot_schema).alias("event")
).select("event.*")

transformed_df = parsed_df \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumn(
        "speed_status",
        when((col("type") == "vehicle") & (col("speed") > 100), "overspeeding")
        .when(col("type") == "vehicle", "normal")
    ) \
    .withColumn(
        "priority",
        when((col("type") == "emergency") & (col("severity") == "high"), "critical")
        .when(col("type") == "emergency", "standard")
        .when((col("type") == "traffic") & (col("level") == "high"), "attention")
    )

valid_types = ["vehicle", "gps", "traffic", "weather", "emergency"]

clean_df = transformed_df.filter(
    col("timestamp").isNotNull()
    & col("type").isin(valid_types)
    & (
        (col("type") != "vehicle")
        | (col("speed").isNotNull() & (col("speed") >= 0))
    )
    & (
        (col("type") != "gps")
        | (
            col("latitude").between(-90, 90)
            & col("longitude").between(-180, 180)
        )
    )
    & (
        (col("type") != "emergency")
        | col("severity").isin(["low", "medium", "high"])
    )
)


def append_to_postgres(batch_df, table_name):
    batch_df.write.jdbc(
        url=postgres_url,
        table=table_name,
        mode="append",
        properties=postgres_properties
    )


def append_to_s3(batch_df, folder_name):
    batch_df.write \
        .mode("append") \
        .parquet(f"{s3_base_path}/{folder_name}")


def write_outputs(batch_df, batch_id):
    vehicle_df = batch_df.filter(col("type") == "vehicle")
    gps_df = batch_df.filter(col("type") == "gps")
    traffic_df = batch_df.filter(col("type") == "traffic")
    weather_df = batch_df.filter(col("type") == "weather")
    emergency_df = batch_df.filter(col("type") == "emergency")

    append_to_postgres(batch_df, "iot_events")
    append_to_postgres(vehicle_df, "vehicle_events")
    append_to_postgres(gps_df, "gps_events")
    append_to_postgres(traffic_df, "traffic_events")
    append_to_postgres(weather_df, "weather_events")
    append_to_postgres(emergency_df, "emergency_events")

    append_to_s3(batch_df, "iot_events")
    append_to_s3(vehicle_df, "vehicle_events")
    append_to_s3(gps_df, "gps_events")
    append_to_s3(traffic_df, "traffic_events")
    append_to_s3(weather_df, "weather_events")
    append_to_s3(emergency_df, "emergency_events")


query = clean_df.writeStream \
    .foreachBatch(write_outputs) \
    .outputMode("append") \
    .start()

print("Step 4: Streaming started and writing to PostgreSQL and S3")

query.awaitTermination()

