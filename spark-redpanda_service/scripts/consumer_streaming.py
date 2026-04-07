import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("=" * 60)
print("Starting Spark Kafka Structured Streaming to PostgreSQL")
print("=" * 60)

# SparkSession with Kafka + PostgreSQL support
spark = SparkSession.builder \
    .appName("KafkaToPostgresStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.executor.memory", "512m") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/checkpoint") \
    .getOrCreate()

# Yellow taxi schema
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])

# PostgreSQL connection from environment variables
postgres_host = os.environ.get("POSTGRES_WAREHOUSE_HOST", "postgres-warehouse")
postgres_port = os.environ.get("POSTGRES_WAREHOUSE_PORT", "5432")
postgres_db = os.environ.get("POSTGRES_WAREHOUSE_DB", "mydb")
postgres_user = os.environ["POSTGRES_WAREHOUSE_USER"]
postgres_password = os.environ["POSTGRES_WAREHOUSE_PASSWORD"]

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
connection_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# Read from Kafka (STREAMING)
bootstrap_servers = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda-1:29092")
kafka_topic = os.environ.get("KAFKA_TOPIC_YELLOW", "yellow_taxi_bookings")
print(f"Connecting to Kafka stream at {bootstrap_servers}...")

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() 
    

# Parse JSON from Kafka value
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Function to write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    print(f"Processing micro-batch #{batch_id}")
    # Avoid calling count() - it's expensive and forces full scan
    # Instead, just try to write; if empty, JDBC will handle gracefully
    batch_df.write \
        .mode("append") \
        .jdbc(url=jdbc_url,
              table="nyc_taxi.yellow_taxi_bookings",
              properties=connection_properties)
    print(f"Batch #{batch_id} written to PostgreSQL successfully")

# Start streaming query
print("Starting streaming query...")
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .start()

print("Streaming started. Press Ctrl+C to stop.")
query.awaitTermination()
