import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("=" * 60)
print("Starting Spark Kafka Consumer")
print("=" * 60)

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

print("Spark session created")

# Yellow taxi schema (correct field names: tpep_* for yellow, not lpep_*)
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

bootstrap_servers = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda-1:29092")
print(f"Connecting to Kafka at {bootstrap_servers}...")

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", os.environ.get("KAFKA_TOPIC_YELLOW", "yellow_taxi_bookings")) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print("Connected to Kafka")
print("Parsing JSON data...")

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

record_count = parsed_df.count()
print(f"Total records found: {record_count}")

if record_count > 0:
    print("\nSample data (first 10 rows):")
    parsed_df.show(10, truncate=False)

    print("\nWriting to PostgreSQL...")

    # PostgreSQL connection properties from environment
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

    # Write to PostgreSQL table
    parsed_df.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table="nyc_taxi.yellow_taxi_bookings", properties=connection_properties)

    print("Data written successfully to PostgreSQL table 'yellow_taxi_bookings'")
else:
    print("No records found in Kafka topic")

spark.stop()
print("\n" + "=" * 60)
print("Processing Complete!")
print("=" * 60)
