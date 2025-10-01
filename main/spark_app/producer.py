from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, to_date, to_timestamp, lit, when, expr
from pyspark.sql.types import StructType, StructField, StringType
import uuid, random
from datetime import datetime, timezone
from pyspark.sql.functions import udf

# Kafka schema
SCHEMA = StructType([
    StructField("accessed_date", StringType(), True),
    StructField("duration_(secs)", StringType(), True),
    StructField("network_protocol", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("bytes", StringType(), True),
    StructField("accessed_Ffom", StringType(), True),  # typo in raw
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("membership", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sales", StringType(), True),
    StructField("returned", StringType(), True),
    StructField("returned_amount", StringType(), True),
    StructField("pay_method", StringType(), True),
    StructField("item_category", StringType(), True),
])

UUID_EPOCH_START = datetime(1582, 10, 15, tzinfo=timezone.utc)

def to_timeuuid_from_date(ts_str: str) -> str:
    if ts_str is None:
        print("[DEBUG] accessed_date is None")
        return None
    try:
        print(f"[DEBUG] Raw accessed_date string: {ts_str}")

        # Try parsing with microseconds first (your data format)
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
            print(f"[DEBUG] Parsed datetime object: {dt}")
        except ValueError as e:
            print(f"[DEBUG] Failed microsecond parse: {e}")
            try:
                # Fallback to seconds only
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                print(f"[DEBUG] Parsed datetime (seconds only): {dt}")
            except ValueError as e2:
                print(f"[DEBUG] Failed second parse too: {e2}")
                return None

        # Make datetime timezone-aware (UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        intervals = int((dt - UUID_EPOCH_START).total_seconds() * 1e7)
        print(f"[DEBUG] Intervals since UUID epoch: {intervals}")

        time_low = intervals & 0xffffffff
        time_mid = (intervals >> 32) & 0xffff
        time_hi_and_version = ((intervals >> 48) & 0x0fff) | (1 << 12)

        clock_seq = random.getrandbits(14)
        clock_seq_hi = (clock_seq >> 8) | 0x80
        clock_seq_low = clock_seq & 0xff
        node = random.getrandbits(48)

        new_uuid = str(uuid.UUID(fields=(
            time_low, time_mid, time_hi_and_version,
            clock_seq_hi, clock_seq_low, node
        )))

        print(f"[DEBUG] Generated UUID: {new_uuid}")
        return new_uuid

    except Exception as e:
        print(f"[ERROR] Unexpected exception in to_timeuuid_from_date: {e}")
        return None


timeuuid_from_date_udf = udf(to_timeuuid_from_date, StringType())

def read_data(spark, kafka_servers, topic):
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", topic)
            .load()
    )

def transform_data(df):
    preprocessed_df = df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data"),
        col("value").alias("original_value")
    ).select("data.*", "original_value")

    preprocessed_df = (
        preprocessed_df
            .withColumnRenamed("duration_(secs)", "duration_secs")
            .withColumnRenamed("accessed_Ffom", "accessed_from")
    )

    # Generate UUID from the original string timestamp
    preprocessed_df = preprocessed_df.withColumn("accessed_at", timeuuid_from_date_udf(col("accessed_date").cast("string")))

    # Convert timestamp using correct format for microseconds
    # Use SSSSSS for microseconds (6 digits) instead of SSS for milliseconds (3 digits)
    preprocessed_df = (
        preprocessed_df
            .withColumn("log_date", to_date(to_timestamp(col("accessed_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS")))
            .withColumn("sales", col("sales").cast("double"))
            .withColumn("returned_amount", col("returned_amount").cast("double"))
            .withColumn("duration_secs", col("duration_secs").cast("int"))
            .withColumn("age", col("age").cast("int"))
            .withColumn("bytes", col("bytes").cast("bigint"))
            .withColumn("accessed_date", to_timestamp(col("accessed_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    )

    preprocessed_df = preprocessed_df.withColumn(
        "error_log",
        when(col("accessed_from").isNull(), lit("Error in Step 1 (Column Rename)."))
        .when(col("accessed_at").isNull(), lit("Error in Step 2 (UUID Generation)."))
        .when(col("log_date").isNull(), lit("Error in Step 3 (Date Parsing)."))
        .otherwise(lit("No errors detected."))
    )

    return preprocessed_df

def write_data(preprocessed_df, kafka_servers):
    valid_data = preprocessed_df.filter(col("error_log") == "No errors detected.")
    invalid_data = preprocessed_df.filter(col("error_log") != "No errors detected.")

    # ✅ logs_clean
    (
        valid_data.drop("original_value")
        .select(to_json(struct(col("*"))).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "logs_clean")
        .option("checkpointLocation", "/tmp/spark/logs_clean_checkpoint")
        .start()
    )

    # ✅ logs_dlq
    dlq_df = invalid_data.select(
        to_json(struct(col("original_value"), col("error_log"))).alias("value")
    )

    (
        dlq_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "logs_dlq")
        .option("checkpointLocation", "/tmp/spark/logs_dlq_checkpoint")
        .start()
    )

if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("ValidatorApp")
            .config("spark.executor.cores", "1")       # each executor = 1 core
            .config("spark.cores.max", "1")            # total cores max = 1
            .config("spark.executor.instances", "1")   # only 1 executor
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw_df = read_data(spark, "kafka:9092", "logs_raw")
    processed_df = transform_data(raw_df)
    write_data(processed_df, "kafka:9092")

    spark.streams.awaitAnyTermination()