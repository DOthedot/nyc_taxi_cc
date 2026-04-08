# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime, timedelta

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
    dag_id='yellow_monthly_dump',
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
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
    def validate_bronze(file_month: str) -> str:
        """Validate data quality in bronze_yellow_taxi_data layer."""
        engine = _engine()
        with engine.connect() as conn:
            # Hard failure: no rows for this month
            row_count = conn.execute(text("""
                SELECT COUNT(*) FROM nyc_taxi.bronze_yellow_taxi_data
                WHERE DATE_TRUNC('month', tpep_pickup_datetime) =
                      DATE_TRUNC('month', CAST(:month_str AS DATE))
            """), {"month_str": f"{file_month}-01"}).scalar()

            if row_count == 0:
                raise ValueError(f"DQ FAIL: zero rows found for {file_month}. Halting pipeline.")

            # Hard failure: NULL on key timestamp
            null_count = conn.execute(text("""
                SELECT COUNT(*) FROM nyc_taxi.bronze_yellow_taxi_data
                WHERE tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
            """)).scalar()
            if null_count > 0:
                raise ValueError(f"DQ FAIL: {null_count} rows with NULL pickup/dropoff datetime.")

            # Soft warning: anomalous negative amount rate
            result = conn.execute(text("""
                SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE total_amount < 0) / COUNT(*), 2) as neg_pct
                FROM nyc_taxi.bronze_yellow_taxi_data
            """)).fetchone()
            neg_pct = result[0] if result else 0

            if neg_pct and neg_pct > 5.0:
                logging.warning(f"DQ WARN: {neg_pct}% rows have negative total_amount (expected < 5%)")

            logging.info(f"DQ PASS (bronze): {row_count} rows, {neg_pct or 0}% negative amounts")
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

    @task
    def validate_gold():
        """Validate data quality in gold_yellow_taxi_data layer."""
        engine = _engine()
        with engine.connect() as conn:
            # Hard failure: empty gold table
            silver_count = conn.execute(text(
                "SELECT COUNT(*) FROM nyc_taxi.silver_yellow_taxi_data"
            )).scalar()
            gold_count = conn.execute(text(
                "SELECT COUNT(*) FROM nyc_taxi.gold_yellow_taxi_data"
            )).scalar()

            if gold_count == 0:
                raise ValueError("DQ FAIL: gold_yellow_taxi_data is empty after transform. Halting.")

            # Soft warning: row loss from silver to gold
            if silver_count > 0:
                loss_pct = 100.0 * (silver_count - gold_count) / silver_count
            else:
                loss_pct = 0

            if loss_pct > 1.0:
                logging.warning(f"DQ WARN: {loss_pct:.1f}% data loss from silver to gold (expected < 1%)")

            logging.info(f"DQ PASS (gold): {gold_count} rows ({loss_pct:.1f}% loss from silver)")

    # Task chain: ingest → validate_bronze → silver → gold → validate_gold
    month = ingest_bronze("{{ params.file_month }}")
    bronze_validated = validate_bronze(month)
    silver = build_silver(bronze_validated)
    gold = build_gold()
    silver >> gold
    gold >> validate_gold()


yellow_taxi_pipeline()
