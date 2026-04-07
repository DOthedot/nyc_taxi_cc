"""
Shared Spark DataFrame Schemas for NYC Taxi Data

This module defines the canonical Spark StructType schemas for yellow and green taxi data.
All producers and consumers must use these schemas to prevent mismatch and ensure data consistency.

Any schema changes should be made here and propagated to all consumers/producers.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# ============================================================
# YELLOW TAXI SCHEMA
# ============================================================
# Used by: producer.py, consumer.py, consumer_streaming.py
# Kafka Topic: yellow_taxi_bookings

YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),  # Yellow uses tpep_* (not lpep_*)
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
    StructField("Airport_fee", DoubleType(), True),  # Yellow taxi only
])

# ============================================================
# GREEN TAXI SCHEMA
# ============================================================
# Used by: producer.py (unused currently), consumer variants (if implemented)
# Kafka Topic: green_taxi_bookings

GREEN_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", StringType(), True),  # Green uses lpep_* (not tpep_*)
    StructField("lpep_dropoff_datetime", StringType(), True),
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
])

__all__ = ["YELLOW_TAXI_SCHEMA", "GREEN_TAXI_SCHEMA"]
