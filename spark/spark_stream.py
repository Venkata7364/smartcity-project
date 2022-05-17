# Import Spark session, Spark SQL functions, and schema data types.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

print("Step 1: Starting script")

# Stage 1: Create Spark session.
# This starts the Spark application and sets a writable Ivy cache path inside Docker.
# Ivy cache is used when Spark downloads external packages like Kafka, PostgreSQL, and S3 connectors.
spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .getOrCreate()

# Reduce unnecessary Spark logs and show only warnings/errors.
spark.sparkContext.setLogLevel("WARN")

print("Step 2: Spark session created")

# Stage 2: Define PostgreSQL connection.
# Spark uses this JDBC URL to connect to the PostgreSQL container.
# Since Spark runs inside Docker, it uses the Docker service name "postgres".
postgres_url = "jdbc:postgresql://postgres:5432/smartcity"

# PostgreSQL login details and JDBC driver.
postgres_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Stage 3: Define S3 data lake path.
# Spark writes processed data to this S3 location using the s3a protocol.
s3_base_path = "s3a://smartcity-data-lake-laksh/processed"

# Stage 4: Define JSON schema.
# Kafka sends JSON messages as strings.
# This schema tells Spark the expected fields and their data types.
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

# Stage 5: Read streaming data from Kafka.
# Spark connects to Kafka broker using broker:29092 because Spark runs inside Docker.
# It subscribes to the Kafka topic iot-topic and reads new messages.
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "iot-topic") \
    .option("startingOffsets", "latest") \
    .load()

print("Step 3: Connected to Kafka")

# Stage 6: Convert Kafka message value from binary to string.
# Kafka stores message values as bytes, so Spark first converts them into readable JSON strings.
raw_df = df.selectExpr("CAST(value AS STRING) AS raw_value")

# Stage 7: Parse JSON string into structured Spark columns.
# from_json uses the schema above to convert the raw JSON into columns like type, speed, latitude, etc.
parsed_df = raw_df.select(
    from_json(col("raw_value"), iot_schema).alias("event")
).select("event.*")

# Stage 8: Apply transformations.
# event_time converts Unix timestamp into readable timestamp.
# speed_status identifies overspeeding vehicles.
# priority identifies critical emergency or high traffic events.
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

# Stage 9: Define valid event types.
# Only these event types are allowed into the final clean dataset.
valid_types = ["vehicle", "gps", "traffic", "weather", "emergency"]

# Stage 10: Apply data quality checks.
# This filters invalid records before writing to PostgreSQL and S3.
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


# Stage 11: Write one micro-batch to PostgreSQL.
# Spark uses JDBC to append records into the given PostgreSQL table.
def append_to_postgres(batch_df, table_name):
    batch_df.write.jdbc(
        url=postgres_url,
        table=table_name,
        mode="append",
        properties=postgres_properties
    )


# Stage 12: Write one micro-batch to S3.
# Spark writes processed records as Parquet files into event-specific S3 folders.
def append_to_s3(batch_df, folder_name):
    batch_df.write \
        .mode("append") \
        .parquet(f"{s3_base_path}/{folder_name}")


# Stage 13: Split each streaming batch by event type and write outputs.
# This writes both a unified table/folder and separate event-specific tables/folders.
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


# Stage 14: Start the streaming query.
# foreachBatch sends every Spark micro-batch to the write_outputs function.
# This allows the same streaming data to be written to PostgreSQL and S3.
query = clean_df.writeStream \
    .foreachBatch(write_outputs) \
    .outputMode("append") \
    .start()

print("Step 4: Streaming started and writing to PostgreSQL and S3")

# Stage 15: Keep the streaming job running.
# Spark continues listening for new Kafka messages until the job is manually stopped.
query.awaitTermination()


