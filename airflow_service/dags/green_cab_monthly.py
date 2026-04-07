# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime

# program related modules
import logging
import os
import pyarrow.parquet as pq

# data wrangling modules
import requests
import pandas as pd

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


@dag(
    dag_id='taxi_data_pipeline',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    params={
        "file_month": "2024-06",        # Pass different months: "2024-07", "2024-05"
        "top_zones": 10,                # Number of top zones to show
        "output_dir": "./data",       # Where to save results
        "min_trips": 100                # Filter zones with minimum trips
    },
    tags=['taxi', 'nyc', 'data-engineering']
)
def taxi_pipeline():
    
    @task
    def download_parquet(file_month: str) -> str:
        """Download parquet file and return local filepath"""
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{file_month}.parquet"
        
        
        filename = f"green_tripdata_{file_month}.parquet"
        
        logging.info(f"Downloading {url}")
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded: {filename}")
            return filename
        else:
            raise ValueError(f"Download failed: {response.status_code}")
    
    @task(multiple_outputs=True)
    def transform_data(filepath: str, top_zones: str, min_trips: str, output_dir: str, file_month: str):
        """Load, transform, and save analytics"""
        logging.info(f"Loading {filepath}")
        df = pd.read_parquet(filepath)

        # Data cleaning & transformations
        df['pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df = df.dropna(subset=['total_amount', 'PULocationID', 'passenger_count'])

        # Filter by month
        df = df[df['pickup_datetime'].dt.month == int(file_month.split('-')[1])]

        # Aggregate by pickup zone
        zone_stats = df.groupby('PULocationID', as_index=False)\
                .agg({'total_amount': ['mean', 'count', 'sum'], 'passenger_count': 'mean', 'trip_distance': 'mean'})\
                .round(2)

        zone_stats.columns = ['PULocationID', 'avg_fare', 'trip_count', 'total_revenue',
                            'avg_passengers', 'avg_distance']
        min_trips = int(min_trips)
        top_zones = int(top_zones)
        zone_stats = zone_stats.query("trip_count >= @min_trips")
        zone_stats = zone_stats.sort_values('trip_count', ascending=False).head(top_zones)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save results
        output_file = f"{output_dir}/top_zones_{file_month}.parquet"
        zone_stats.to_parquet(output_file)
        zone_stats.to_csv(output_file.replace('.parquet', '.csv'))

        logging.info(f"Saved {len(zone_stats)} zones to {output_file}")
        return {
            'result': zone_stats.to_dict('records'),
            'filepath': filepath
        }
       

    @task
    def postgre_cdl_dump(filepath: str) -> str:
        cfg = _postgres_config()
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        parquet_file = pq.ParquetFile(filepath)

        try:
            is_table_created = True
            for batch in parquet_file.iter_batches(batch_size=100000):
                chunk_df = batch.to_pandas()

                if is_table_created:
                    chunk_df.head(n=0).to_sql(
                        name='bronze_green_taxi_data',
                        con=engine,
                        if_exists='replace',
                        schema='nyc_taxi'
                    )
                    is_table_created = False
                    logging.info("Bronze green taxi table created successfully")

                chunk_df.to_sql(
                    name='bronze_green_taxi_data',
                    con=engine,
                    if_exists='append',
                    schema='nyc_taxi'
                )

            logging.info(f"Bronze load complete for {filepath}")
            return filepath
        except Exception as e:
            logging.error(f"Unable to ingest file '{filepath}' to Postgres: {e}")
            raise

    @task
    def archive_data(filepath: str):
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info(f"Deleted raw file: {filepath}")
        else:
            logging.warning(f"File not found: {filepath}")
        
    
    @task
    def build_silver_gold():
        """Drop-and-recreate silver and gold green taxi tables from the bronze layer."""
        cfg = _postgres_config()
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS nyc_taxi.gold_green_taxi_data"))
            conn.execute(text("DROP TABLE IF EXISTS nyc_taxi.silver_green_taxi_data"))

            conn.execute(text("""
                CREATE TABLE nyc_taxi.silver_green_taxi_data AS
                SELECT DISTINCT
                    "VendorID"                                                                              AS vendor_id,
                    COALESCE("Borough", 'Unknown')                                                         AS borough,
                    COALESCE(service_zone, 'Unknown')                                                      AS service_zone,
                    DATE(lpep_pickup_datetime)                                                             AS pickup_date,
                    DATE_PART('month', lpep_pickup_datetime)                                               AS mnth,
                    ROUND(EXTRACT(EPOCH FROM AGE(lpep_dropoff_datetime, lpep_pickup_datetime)) / 60, 2)   AS trip_time,
                    COALESCE(passenger_count, 0)                                                           AS passenger_count,
                    trip_distance,
                    total_amount                                                                           AS trip_amount,
                    COALESCE(payment_type, 5)                                                              AS payment_type
                FROM nyc_taxi.bronze_green_taxi_data AS gt
                LEFT JOIN nyc_taxi.bronze_taxi_zone_lookup AS lkp
                    ON gt."PULocationID" = lkp."LocationID"
                WHERE total_amount >= 0
                  AND DATE_TRUNC('month', lpep_pickup_datetime) = (
                      SELECT MAX(DATE_TRUNC('month', lpep_pickup_datetime))
                      FROM nyc_taxi.bronze_green_taxi_data
                  )
            """))
            logging.info("silver_green_taxi_data rebuilt")

            conn.execute(text("""
                CREATE TABLE nyc_taxi.gold_green_taxi_data AS
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
                FROM nyc_taxi.silver_green_taxi_data
            """))
            logging.info("gold_green_taxi_data rebuilt")

    @task
    def log_summary(transformed_data):
        """Log final summary"""
        logging.info(f"Pipeline complete! Analyzed {len(transformed_data)} zones")
        for zone in transformed_data[:3]:  # Top 3
            logging.info(f"Zone {zone['PULocationID']}: {zone['trip_count']:.0f} trips, "
                        f"${zone['avg_fare']:.2f} avg fare")


    # Download parquet first
    downloaded_filepath = download_parquet("{{ params.file_month }}")

    # Transform data and ingest to bronze run in parallel after download
    results = transform_data(
        downloaded_filepath,
        "{{ params.top_zones }}",
        "{{ params.min_trips }}",
        "{{ params.output_dir }}",
        "{{ params.file_month }}"
    )

    # Bronze load must complete before archive (prevents race condition)
    postgres_task = postgre_cdl_dump(downloaded_filepath)
    archive_task = archive_data(postgres_task)  # archive depends on postgres_task completing

    # Silver/Gold rebuild waits for bronze load
    silver_gold_task = build_silver_gold()
    postgres_task >> silver_gold_task

    # Log waits for everything to complete
    [silver_gold_task, archive_task, results] >> log_summary(results['result'])

    

# Initialize DAG
taxi_pipeline()
