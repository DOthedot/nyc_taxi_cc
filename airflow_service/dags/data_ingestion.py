import psycopg
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime

# for airflow relevant modulles 
from airflow import DAG
from airflow.operators.python import PythonOperator

def ingest_data():
    POSTGRES_CONFIG = {
        'host': 'host.docker.internal',
        'database': 'mydb',
        'user': 'myuser', 
        'password': 'mysecretpassword',
        'port': 5432}

    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    load_date = '2025-01'

    monthly_yellow_taxi_file_path  = f"./data/yellow_taxi/yellow_tripdata_{load_date}.parquet"


    # using incremental loading 

    parquet_file = pq.ParquetFile(monthly_yellow_taxi_file_path)

    is_table_created = True 

    for batch in parquet_file.iter_batches(batch_size=100000):

    # creating dataframe from the batch 
        chunk_df = batch.to_pandas()

        if is_table_created : 
            chunk_df.head(n=0).to_sql(name='bronze_yellow_taxi_data', con=engine, if_exists='replace' ,schema='nyc_taxi')
            is_table_created = False 
            print("Table created successfully")
       
        # Process chunk (datetimes already parsed correctly)
        chunk_df.to_sql(name='bronze_yellow_taxi_data', con=engine, if_exists='append' , schema='nyc_taxi')

        # here we want to sent that data to postgres server 
        #print("Inserted:", len(chunk_df))
    return "Success" 

with DAG(
    'yellow_monthly_dump',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    

    
    first_task = PythonOperator(
        task_id='yello_taxi_ingestion',
        python_callable=ingest_data,
    )
