# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime

# program related modules
import logging
import os

# database related modules
from sqlalchemy import create_engine, text
from psycopg.sql import SQL, Identifier


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
    dag_id='monthly_archival_pipeline',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['taxi', 'nyc', 'data-engineering']
)
def archival_pipeline():

    @task
    def monthly_cdl_backup(table_name='yellow_taxi_bookings', backup_suffix=None):
        """Create a timestamped backup of a table."""
        cfg = _postgres_config()
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )

        try:
            if backup_suffix is None:
                backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

            backup_table = f"{table_name}_backup_{backup_suffix}"

            with engine.begin() as conn:
                # Use SQL identifiers to prevent SQL injection
                sql = SQL(
                    "CREATE TABLE nyc_taxi.{} AS SELECT * FROM nyc_taxi.{}"
                ).format(
                    Identifier(backup_table),
                    Identifier(table_name)
                )
                conn.execute(sql)
                logging.info(f"Backup created: nyc_taxi.{backup_table}")

        except Exception as e:
            logging.error(f"Unable to create backup '{backup_table}': {e}")
            raise

    # Archive bronze application tables
    t1 = monthly_cdl_backup(table_name='yellow_taxi_bookings')
    t2 = monthly_cdl_backup(table_name='green_taxi_bookings')

    # Archive silver/gold tables for both taxi types
    t3 = monthly_cdl_backup(table_name='silver_yellow_taxi_data')
    t4 = monthly_cdl_backup(table_name='gold_yellow_taxi_data')
    t5 = monthly_cdl_backup(table_name='silver_green_taxi_data')
    t6 = monthly_cdl_backup(table_name='gold_green_taxi_data')

    # Yellow chain, then green chain (sequential to avoid lock contention)
    t1 >> t3 >> t4
    t2 >> t5 >> t6

# Initialize DAG
archival_pipeline()

