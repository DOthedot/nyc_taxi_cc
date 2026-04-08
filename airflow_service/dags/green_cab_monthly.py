# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime, timedelta

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


def _engine():
    """Create SQLAlchemy engine with warehouse credentials."""
    cfg = _postgres_config()
    return create_engine(
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def _on_failure_callback(context):
    """Log pipeline failures to pipeline_run_log table."""
    try:
        engine = _engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO nyc_taxi.pipeline_run_log
                    (dag_id, task_id, execution_date, status, error_message)
                VALUES (:dag_id, :task_id, :exec_date, 'FAILED', :error)
            """), {
                "dag_id": context["dag"].dag_id,
                "task_id": context["task_instance"].task_id,
                "exec_date": context["execution_date"],
                "error": str(context.get("exception", "unknown error")),
            })
        logging.error(f"Task {context['task_instance'].task_id} failed. Logged to pipeline_run_log.")
    except Exception as e:
        logging.error(f"Failed to log failure to pipeline_run_log: {e}")


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_failure_callback,
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id='taxi_data_pipeline',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args=default_args,
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
    def validate_bronze(file_month: str) -> str:
        """Validate data quality in bronze_green_taxi_data layer."""
        cfg = _postgres_config()
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        with engine.connect() as conn:
            # Hard failure: no rows for this month
            row_count = conn.execute(text("""
                SELECT COUNT(*) FROM nyc_taxi.bronze_green_taxi_data
                WHERE DATE_TRUNC('month', lpep_pickup_datetime) =
                      DATE_TRUNC('month', CAST(:month_str AS DATE))
            """), {"month_str": f"{file_month}-01"}).scalar()

            if row_count == 0:
                raise ValueError(f"DQ FAIL: zero rows found for {file_month}. Halting pipeline.")

            # Hard failure: NULL on key timestamp
            null_count = conn.execute(text("""
                SELECT COUNT(*) FROM nyc_taxi.bronze_green_taxi_data
                WHERE lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL
            """)).scalar()
            if null_count > 0:
                raise ValueError(f"DQ FAIL: {null_count} rows with NULL pickup/dropoff datetime.")

            # Soft warning: anomalous negative amount rate
            result = conn.execute(text("""
                SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE total_amount < 0) / COUNT(*), 2) as neg_pct
                FROM nyc_taxi.bronze_green_taxi_data
            """)).fetchone()
            neg_pct = result[0] if result else 0

            if neg_pct and neg_pct > 5.0:
                logging.warning(f"DQ WARN: {neg_pct}% rows have negative total_amount (expected < 5%)")

            logging.info(f"DQ PASS (bronze): {row_count} rows, {neg_pct or 0}% negative amounts")
        return file_month

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
    def validate_gold():
        """Validate data quality in gold_green_taxi_data layer."""
        cfg = _postgres_config()
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        with engine.connect() as conn:
            # Hard failure: empty gold table
            silver_count = conn.execute(text(
                "SELECT COUNT(*) FROM nyc_taxi.silver_green_taxi_data"
            )).scalar()
            gold_count = conn.execute(text(
                "SELECT COUNT(*) FROM nyc_taxi.gold_green_taxi_data"
            )).scalar()

            if gold_count == 0:
                raise ValueError("DQ FAIL: gold_green_taxi_data is empty after transform. Halting.")

            # Soft warning: row loss from silver to gold
            if silver_count > 0:
                loss_pct = 100.0 * (silver_count - gold_count) / silver_count
            else:
                loss_pct = 0

            if loss_pct > 1.0:
                logging.warning(f"DQ WARN: {loss_pct:.1f}% data loss from silver to gold (expected < 1%)")

            logging.info(f"DQ PASS (gold): {gold_count} rows ({loss_pct:.1f}% loss from silver)")

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

    # Validate bronze before proceeding to silver/gold
    bronze_validated = validate_bronze("{{ params.file_month }}")
    postgres_task >> bronze_validated

    # Silver/Gold rebuild waits for validated bronze
    silver_gold_task = build_silver_gold()
    bronze_validated >> silver_gold_task

    # Validate gold layer
    gold_validated = validate_gold()
    silver_gold_task >> gold_validated

    # Log waits for everything to complete
    [gold_validated, archive_task, results] >> log_summary(results['result'])

    

# Initialize DAG
taxi_pipeline()
