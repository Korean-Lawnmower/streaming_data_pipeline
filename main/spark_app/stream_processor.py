from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, udf, to_date, to_timestamp, lit, when, expr
from pyspark.sql.types import StructType, StructField, StringType
import uuid

# UUID 생성 UDF 정의 (삭제 가능: 이제 Spark expr("uuid()") 사용)
def generate_unique_uuid(ip, accessed_date):
    combined_data = f"{ip}-{accessed_date}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, combined_data))

# Kafka 메시지 스키마 정의 (모든 필드를 StringType으로 정의)
SCHEMA = StructType([
    StructField("accessed_date", StringType(), True),
    StructField("duration_(secs)", StringType(), True),
    StructField("network_protocol", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("bytes", StringType(), True),
    StructField("accessed_Ffom", StringType(), True),
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

def read_data(spark, kafka_servers, topic):
    """Kafka로부터 데이터를 읽어오는 함수."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .load()

def transform_data(df):
    """모든 전처리 단계를 수행하는 함수."""
    # 원본 데이터를 보존하여 DLQ로 보낼 준비
    preprocessed_df = df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data"),
        col("value").alias("original_value")
    ).select("data.*", "original_value")

    # 1. 칼럼명 수정 (`accessed_Ffom` -> `accessed_from`)
    preprocessed_df = preprocessed_df \
    .withColumnRenamed("duration_(secs)", "duration_secs") \
    .withColumnRenamed("accessed_Ffom", "accessed_from")

    # 2. 유효 ID(UUID) 생성 (Spark expr 이용)
    preprocessed_df = preprocessed_df.withColumn("accessed_at", expr("uuid()"))

    # 3. datetime 칼럼에서 날짜만 남겨 `log_date` 칼럼 생성
    preprocessed_df = preprocessed_df.withColumn("log_date", to_date(to_timestamp(col("accessed_date"), "yyyy-MM-dd HH:mm:ss.SSS")))

    # 4. 타입 캐스팅 추가
    preprocessed_df = preprocessed_df \
        .withColumn("sales", col("sales").cast("double")) \
        .withColumn("returned_amount", col("returned_amount").cast("double")) \
        .withColumn("duration_secs", col("duration_secs").cast("int")) \
        .withColumn("age", col("age").cast("int")) \
        .withColumn("bytes", col("bytes").cast("bigint")) \
        .withColumn("accessed_date", to_timestamp(col("accessed_date"), "yyyy-MM-dd HH:mm:ss.SSS"))

    # 오류 로그를 기록할 새로운 컬럼 생성
    preprocessed_with_errors_df = preprocessed_df.withColumn(
        "error_log",
        when(col("accessed_from").isNull(), lit("Error in Step 1 (Column Rename)."))
        .when(col("accessed_at").isNull(), lit("Error in Step 2 (UUID Generation)."))
        .when(col("log_date").isNull(), lit("Error in Step 3 (Date Parsing)."))
        .otherwise(lit("No errors detected."))
    )
    
    return preprocessed_with_errors_df

def write_data(preprocessed_df, kafka_servers):
    """유효성 검사를 수행하고 데이터를 Kafka와 Cassandra에 쓰는 함수."""
    is_valid = col("error_log") == "No errors detected."

    valid_data_stream = preprocessed_df.filter(is_valid)
    invalid_data_stream = preprocessed_df.filter(~is_valid)

    # ✅ Drop original_value for valid data
    valid_for_output = valid_data_stream.drop("original_value")

    # === 1) 유효한 데이터를 logs_clean 토픽에 쓰기
    valid_for_output.select(to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("topic", "logs_clean") \
        .option("checkpointLocation", "/tmp/spark/logs_clean_checkpoint") \
        .outputMode("append") \
        .start()

    # === 2) 유효한 데이터를 Cassandra 테이블들에 쓰기
    # logs_by_ip
    valid_for_output.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "ecommerce") \
        .option("table", "logs_by_ip") \
        .option("checkpointLocation", "/tmp/spark/cassandra_ip_checkpoint") \
        .outputMode("append") \
        .start()

    # logs_by_day
    valid_for_output.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "ecommerce") \
        .option("table", "logs_by_day") \
        .option("checkpointLocation", "/tmp/spark/cassandra_day_checkpoint") \
        .outputMode("append") \
        .start()

    # logs_by_country
    valid_for_output.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "ecommerce") \
        .option("table", "logs_by_country") \
        .option("checkpointLocation", "/tmp/spark/cassandra_country_checkpoint") \
        .outputMode("append") \
        .start()

    # logs_by_category
    valid_for_output.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "ecommerce") \
        .option("table", "logs_by_category") \
        .option("checkpointLocation", "/tmp/spark/cassandra_category_checkpoint") \
        .outputMode("append") \
        .start()

    # === 3) 유효하지 않은 데이터를 logs_dlq 토픽에 쓰기 (keep original_value)
    dlq_df = invalid_data_stream.select(
        to_json(
            struct(
                col("original_value"),
                col("error_log")
            )
        ).alias("value")
    )
    dlq_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("topic", "logs_dlq") \
        .option("checkpointLocation", "/tmp/spark/logs_dlq_checkpoint") \
        .outputMode("append") \
        .start()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LogProcessorProducer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()

    
    spark.sparkContext.setLogLevel("ERROR")

    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    SOURCE_TOPIC = "logs_raw"
    
    # 전체 파이프라인 실행
    raw_df = read_data(spark, KAFKA_BOOTSTRAP_SERVERS, SOURCE_TOPIC)
    processed_df = transform_data(raw_df)
    write_data(processed_df, KAFKA_BOOTSTRAP_SERVERS)
    
    spark.streams.awaitAnyTermination()
