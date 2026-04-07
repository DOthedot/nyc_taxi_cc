# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime

# program related modules
import logging
import os
import pyarrow.parquet as pq

# database related modules
from sqlalchemy import create_engine, text


def _postgres_config():
    """Load PostgreSQL config from environment variables."""
    return {
        'host': os.environ.get('POSTGRES_WAREHOUSE_HOST', 'postgres-warehouse'),
        'port': int(os.environ.get('POSTGRES_WAREHOUSE_PORT', '5432')),
        'database': os.environ.get('POSTGRES_WAREHOUSE_DB', 'mydb'),
        'user': os.environ['POSTGRES_WAREHOUSE_USER'],
        'password': os.environ['POSTGRES_WAREHOUSE_PASSWORD'],
    }

def _engine():
    cfg = _postgres_config()
    return create_engine(
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


@dag(
    dag_id='yellow_monthly_dump',
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "file_month": "2025-01",   # e.g. "2024-06", "2025-01"
    },
    tags=['taxi', 'nyc', 'yellow', 'data-engineering'],
)
def yellow_taxi_pipeline():

    @task
    def ingest_bronze(file_month: str) -> str:
        """Incremental-load the monthly parquet into bronze_yellow_taxi_data."""
        filepath = f"./data/yellow_taxi/yellow_tripdata_{file_month}.parquet"
        engine = _engine()

        parquet_file = pq.ParquetFile(filepath)
        is_first_batch = True

        for batch in parquet_file.iter_batches(batch_size=100_000):
            chunk_df = batch.to_pandas()

            if is_first_batch:
                chunk_df.head(n=0).to_sql(
                    name='bronze_yellow_taxi_data',
                    con=engine,
                    if_exists='replace',
                    schema='nyc_taxi',
                )
                is_first_batch = False
                logging.info("Bronze table (re)created")

            chunk_df.to_sql(
                name='bronze_yellow_taxi_data',
                con=engine,
                if_exists='append',
                schema='nyc_taxi',
            )

        logging.info(f"Bronze load complete for {file_month}")
        return file_month

    @task
    def build_silver(_file_month: str):
        """Drop-and-recreate silver_yellow_taxi_data from the bronze layer."""
        engine = _engine()
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS nyc_taxi.silver_yellow_taxi_data"))
            conn.execute(text("""
                CREATE TABLE nyc_taxi.silver_yellow_taxi_data AS
                SELECT DISTINCT
                    "VendorID"                                                                            AS vendor_id,
                    COALESCE("Borough", 'Unknown')                                                       AS borough,
                    COALESCE(service_zone, 'Unknown')                                                    AS service_zone,
                    DATE(tpep_pickup_datetime)                                                           AS pickup_date,
                    DATE_PART('month', tpep_pickup_datetime)                                             AS mnth,
                    ROUND(EXTRACT(EPOCH FROM AGE(tpep_dropoff_datetime, tpep_pickup_datetime)) / 60, 2) AS trip_time,
                    COALESCE(passenger_count, 0)                                                         AS passenger_count,
                    trip_distance,
                    total_amount                                                                         AS trip_amount,
                    COALESCE(payment_type, 5)                                                            AS payment_type
                FROM nyc_taxi.bronze_yellow_taxi_data AS yt
                LEFT JOIN nyc_taxi.bronze_taxi_zone_lookup AS lkp
                    ON yt."PULocationID" = lkp."LocationID"
                WHERE total_amount >= 0
                  AND DATE_TRUNC('month', tpep_pickup_datetime) = (
                      SELECT MAX(DATE_TRUNC('month', tpep_pickup_datetime))
                      FROM nyc_taxi.bronze_yellow_taxi_data
                  )
            """))
        logging.info("Silver table rebuilt")

    @task
    def build_gold():
        """Drop-and-recreate gold_yellow_taxi_data from the silver layer."""
        engine = _engine()
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS nyc_taxi.gold_yellow_taxi_data"))
            conn.execute(text("""
                CREATE TABLE nyc_taxi.gold_yellow_taxi_data AS
                SELECT
                    CASE
                        WHEN vendor_id = 1 THEN 'Creative Mobile Technologies, LLC'
                        WHEN vendor_id = 2 THEN 'Curb Mobility, LLC'
                        WHEN vendor_id = 6 THEN 'Myle Technologies Inc'
                    END                  AS vendor_id,
                    borough, service_zone, pickup_date,
                    mnth, trip_time, passenger_count, trip_distance, trip_amount,
                    CASE
                        WHEN payment_type = 0 THEN 'Flex Fare trip'
                        WHEN payment_type = 1 THEN 'Credit card'
                        WHEN payment_type = 2 THEN 'Cash'
                        WHEN payment_type = 3 THEN 'No charge'
                        WHEN payment_type = 4 THEN 'Dispute'
                        WHEN payment_type = 5 THEN 'Unknown'
                        WHEN payment_type = 6 THEN 'Voided trip'
                    END                  AS payment_type
                FROM nyc_taxi.silver_yellow_taxi_data
            """))
        logging.info("Gold table rebuilt")

    # Task dependencies: bronze → silver → gold
    month = ingest_bronze("{{ params.file_month }}")
    silver = build_silver(month)
    silver >> build_gold()


yellow_taxi_pipeline()
