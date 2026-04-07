# airflow related modules
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone

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
    dag_id='monthly_archival_pipeline',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args=default_args,
    tags=['taxi', 'nyc', 'data-engineering']
)
def archival_pipeline():

    @task
    def preflight_check():
        """Abort archival if no successful pipeline run in the last 35 days."""
        engine = _engine()
        with engine.connect() as conn:
            last_success = conn.execute(text("""
                SELECT MAX(logged_at) FROM nyc_taxi.pipeline_run_log
                WHERE dag_id IN ('yellow_monthly_dump', 'taxi_data_pipeline')
                  AND status = 'SUCCESS'
            """)).scalar()

            if last_success is None:
                raise ValueError(
                    "No successful pipeline runs on record. Archival aborted. "
                    "Run yellow_monthly_dump or taxi_data_pipeline first."
                )

            days_since = (datetime.now(tz=timezone.utc) - last_success).days
            if days_since > 35:
                raise ValueError(
                    f"Last successful pipeline run was {days_since} days ago (> 35 days threshold). "
                    f"Archival aborted to prevent archiving stale data."
                )

            logging.info(f"Preflight check passed. Last success: {days_since} days ago.")

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

    # Preflight check before archival
    check = preflight_check()

    # Archive bronze application tables
    t1 = monthly_cdl_backup(table_name='yellow_taxi_bookings')
    t2 = monthly_cdl_backup(table_name='green_taxi_bookings')

    # Archive silver/gold tables for both taxi types
    t3 = monthly_cdl_backup(table_name='silver_yellow_taxi_data')
    t4 = monthly_cdl_backup(table_name='gold_yellow_taxi_data')
    t5 = monthly_cdl_backup(table_name='silver_green_taxi_data')
    t6 = monthly_cdl_backup(table_name='gold_green_taxi_data')

    # Preflight must pass before any backups
    check >> [t1, t2]

    # Yellow chain, then green chain (sequential to avoid lock contention)
    t1 >> t3 >> t4
    t2 >> t5 >> t6

# Initialize DAG
archival_pipeline()

