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
    def transform_data(filepath: str, top_zones: str, min_trips: str, output_dir: str , file_month: str):
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
                .agg({'total_amount': ['mean', 'count', 'sum'],'passenger_count': 'mean','trip_distance': 'mean'})\
                .round(2)
        
        zone_stats.columns = ['PULocationID','avg_fare', 'trip_count', 'total_revenue', 
                            'avg_passengers', 'avg_distance']
        # zone_stats = zone_stats[zone_stats['trip_count'] >= min_trips]
        min_trips = int(min_trips)
        top_zones = int(top_zones)
        zone_stats = zone_stats.query("trip_count >= @min_trips")
        zone_stats = zone_stats.sort_values('trip_count', ascending=False).head(top_zones)
        
        # Save results
        output_file = f"{output_dir}/top_zones_{file_month}.parquet"
        zone_stats.to_parquet(output_file)
        zone_stats.to_csv(output_file.replace('.parquet', '.csv'))
        
        logging.info(f"Saved {len(zone_stats)} zones to {output_file}")
        return {
            'result': zone_stats.to_dict('records'),
            'filepath': filepath  # Pass filepath downstream
            }
       

    @task
    def postgre_cdl_dump(filepath: str) :
        POSTGRES_CONFIG = {'host': 'host.docker.internal','database': 'mydb','user': 'myuser', 'password': 'mysecretpassword','port': 5432}

        engine = create_engine(f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
        parquet_file = pq.ParquetFile(filepath)

        try : 
            is_table_created = True 
            for batch in parquet_file.iter_batches(batch_size=100000):
                # creating dataframe from the batch 
                chunk_df = batch.to_pandas()

                if is_table_created : 
                    chunk_df.head(n=0).to_sql(name='bronze_green_taxi_data', con=engine, if_exists='replace' ,schema='nyc_taxi')
                    is_table_created = False 
                    print("Table created successfully")
                # Process chunk (datetimes already parsed correctly)
                chunk_df.to_sql(name='bronze_green_taxi_data', con=engine, if_exists='append' , schema='nyc_taxi')

            return filepath
        except Exception as e : 
            logging.info(f"""Unable to ingest File: "{filepath}" to Postgres : {e}""")
            raise

    @task
    def archive_data(filepath: str):
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info(f"Deleted raw file: {filepath}")
        else:
            logging.warning(f"File not found: {filepath}")
        
    
    @task
    def log_summary(transformed_data):
        """Log final summary"""
        logging.info(f"Pipeline complete! Analyzed {len(transformed_data)} zones")
        for zone in transformed_data[:3]:  # Top 3
            logging.info(f"Zone {zone['PULocationID']}: {zone['trip_count']:.0f} trips, "
                        f"${zone['avg_fare']:.2f} avg fare")
    

    # download & transform run first (parallel inputs)
    downloaded_filepath = download_parquet("{{ params.file_month }}")
    results = transform_data(downloaded_filepath, 
                             "{{ params.top_zones }}", 
                             "{{ params.min_trips }}", 
                             "{{ params.output_dir }}", 
                             "{{ params.file_month }}")     


    # Both raw processing AND analytics complete, THEN log
    postgres_task = postgre_cdl_dump(downloaded_filepath)
    archive_task = archive_data(downloaded_filepath)

    # Log waits for BOTH raw processing AND analytics to complete
    [postgres_task, archive_task, results] >> log_summary(results['result'])

    

# Initialize DAG
taxi_pipeline()
