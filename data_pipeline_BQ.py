from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from google.cloud import bigquery

# My path/credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/gcp-creds.json"
bq_client = bigquery.Client()
BQ_TABLE = "project.mydataset.my_table"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('daily_etl_pipeline',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)

def extract_from_ftp(**kwargs):
    # Simulate large CSV extraction
    df = pd.read_csv("ftp://your_ftp_server/path/to/yourfile.csv", chunksize=500_000)
    extracted = pd.concat([chunk for chunk in df])
    extracted.to_parquet('/tmp/source1.parquet', index=False)

def extract_from_api(**kwargs):
    url = "https://api.example.com/data"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    df.to_parquet('/tmp/source2.parquet', index=False)

def transform(**kwargs):
    df1 = pd.read_parquet('/tmp/source1.parquet')
    df2 = pd.read_parquet('/tmp/source2.parquet')
    
    # Example transformations
    df = pd.concat([df1, df2])
    df['processed_at'] = pd.Timestamp.now()
    df.dropna(inplace=True)
    df.to_parquet('/tmp/final_data.parquet', index=False)

def load_to_bigquery(**kwargs):
    df = pd.read_parquet('/tmp/final_data.parquet')
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    bq_client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config).result()

extract_ftp = PythonOperator(
    task_id='extract_from_ftp',
    python_callable=extract_from_ftp,
    dag=dag)

extract_api = PythonOperator(
    task_id='extract_from_api',
    python_callable=extract_from_api,
    dag=dag)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag)

[extract_ftp, extract_api] >> transform_task >> load_task
