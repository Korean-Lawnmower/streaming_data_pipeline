from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType
)
import sys

# === Schema aligned with Cassandra ===
SCHEMA = StructType([
    StructField("accessed_date", StringType(), True),     # will cast → Timestamp
    StructField("duration_secs", StringType(), True),     # will cast → Integer
    StructField("network_protocol", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("bytes", StringType(), True),             # will cast → Long
    StructField("accessed_from", StringType(), True),
    StructField("age", StringType(), True),               # will cast → Integer
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("membership", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sales", StringType(), True),             # will cast → Double
    StructField("returned", StringType(), True),
    StructField("returned_amount", StringType(), True),   # will cast → Double
    StructField("pay_method", StringType(), True),
    StructField("item_category", StringType(), True),
    StructField("accessed_at", StringType(), True),       # will cast → UUID
    StructField("log_date", StringType(), True),          # will cast → Date
    StructField("error_log", StringType(), True),
])

def main():
    print("🚀 Starting Spark Cassandra Consumer...", flush=True)

    spark = (
        SparkSession.builder
            .appName("CassandraSinkConsumer")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "1")
            .config("spark.executor.instances", "1")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Kafka topic
    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    SOURCE_TOPIC = "logs_clean"

    print(f"📡 Subscribing to Kafka topic: {SOURCE_TOPIC}", flush=True)

    raw_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", SOURCE_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
    )

    print("✅ Connected to Kafka. Parsing JSON...", flush=True)

    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) as json")
              .select(from_json(col("json"), SCHEMA).alias("data"))
              .select("data.*")
    )

    # === Cast types to match Cassandra ===
    parsed_df = (
        parsed_df
            .withColumn("sales", col("sales").cast(DoubleType()))
            .withColumn("returned_amount", col("returned_amount").cast(DoubleType()))
            .withColumn("duration_secs", col("duration_secs").cast(IntegerType()))
            .withColumn("age", col("age").cast(IntegerType()))
            .withColumn("bytes", col("bytes").cast(LongType()))
            .withColumn("accessed_date", to_timestamp(col("accessed_date")))
            .withColumn("log_date", to_date(col("log_date")))
            .withColumn("accessed_at", expr("uuid()"))  # overwrite with fresh UUID
    )

    print("📝 Writing stream into Cassandra tables...", flush=True)

    # logs_by_ip
    (
        parsed_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "ecommerce")
            .option("table", "logs_by_ip")
            .option("checkpointLocation", "/tmp/spark/checkpoints/logs_by_ip")
            .outputMode("append")
            .start()
    )

    # logs_by_day
    (
        parsed_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "ecommerce")
            .option("table", "logs_by_day")
            .option("checkpointLocation", "/tmp/spark/checkpoints/logs_by_day")
            .outputMode("append")
            .start()
    )

    # logs_by_country
    (
        parsed_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "ecommerce")
            .option("table", "logs_by_country")
            .option("checkpointLocation", "/tmp/spark/checkpoints/logs_by_country")
            .outputMode("append")
            .start()
    )

    # logs_by_category
    (
        parsed_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "ecommerce")
            .option("table", "logs_by_category")
            .option("checkpointLocation", "/tmp/spark/checkpoints/logs_by_category")
            .outputMode("append")
            .start()
    )

    print("👀 Also printing rows to console for debugging...", flush=True)

    (
        parsed_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("🛑 Stopping consumer...", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
