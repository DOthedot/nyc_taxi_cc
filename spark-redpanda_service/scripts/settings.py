import os

# Path to the input parquet file
# Inside container: /opt/spark/data/yellow_taxi/yellow_tripdata_2024-01.parquet
# Can be overridden with INPUT_DATA_PATH environment variable
INPUT_DATA_PATH = os.environ.get(
    "INPUT_DATA_PATH",
    "/opt/spark/data/yellow_taxi/yellow_tripdata_2024-01.parquet"
)

# Kafka bootstrap servers (inside container: redpanda-1:29092, outside: localhost:9092)
BOOTSTRAP_SERVERS = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda-1:29092")

# Kafka topic for yellow taxi data
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_YELLOW", "yellow_taxi_bookings")