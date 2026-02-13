# airflow related modules 
from airflow.decorators import dag, task
from airflow import DAG
from datetime import datetime, timedelta

# program related modules 
import logging
import os
from pathlib import Path
import pyarrow.parquet as pq

# data wrangling modules 
import requests
import pandas as pd


# database related modules 
import psycopg
from sqlalchemy import create_engine


@dag(
    dag_id='monthly_archival_pipeline',
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['taxi', 'nyc', 'data-engineering']
)
def archival_pipeline():

    @task
    def monthly_cdl_backup(table_name = 'yellow_taxi_bookings', backup_suffix=None):
        POSTGRES_CONFIG = {'host': 'host.docker.internal','database': 'mydb','user': 'myuser', 'password': 'mysecretpassword','port': 5432}
    
        engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
        
        try:
            #checking for backup suffix
            if backup_suffix is None:
                backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

            backup_table = f"{table_name}_backup_{backup_suffix}"

            with engine.begin() as conn:
                # Create backup table
                result = conn.execute(f"""
                                      CREATE TABLE nyc_taxi.{backup_table} 
                                        AS SELECT * FROM nyc_taxi.{table_name}
                                    """)
                #conn.commit()
                print(f"Backup created: nyc_taxi.{backup_table}")   

        except Exception as e : 
            logging.info(f"""Unable to create backup : "{backup_table}" to Postgres : {e}""")
            raise

    monthly_cdl_backup(table_name='yellow_taxi_bookings') >> monthly_cdl_backup(table_name='green_taxi_bookings')

# Initialize DAG
archival_pipeline()

