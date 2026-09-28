from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import boto3
import glob

# MinIO Connection Details — read from environment (set via .env / docker-compose)
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_BUCKET = os.environ.get("S3_BUCKET", "tidaline-lakehouse")

def upload_to_minio(local_dir, s3_prefix):
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    
    # Find all CSV files in the directory
    search_path = os.path.join("/opt/airflow/extractors/batch", "data", local_dir, "*.csv")
    files = glob.glob(search_path)
    
    if not files:
        print(f"No files found in {search_path}")
        return

    for file_path in files:
        file_name = os.path.basename(file_path)
        s3_key = f"bronze/{s3_prefix}/{file_name}"
        print(f"Uploading {file_path} to s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        
        # Clean up local file after successful upload to save space
        os.remove(file_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- VESSELS DAG (DAILY) ---
with DAG(
    'vessels_ingestion',
    default_args=default_args,
    description='Scrape vessels data and upload to MinIO',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'vessels', 'bronze'],
) as vessels_dag:

    scrape_vessels = BashOperator(
        task_id='scrape_vessels',
        bash_command='python /opt/airflow/extractors/batch/vessels.py',
    )

    upload_vessels_to_minio = PythonOperator(
        task_id='upload_vessels_to_minio',
        python_callable=upload_to_minio,
        op_kwargs={'local_dir': 'vessels', 's3_prefix': 'vessels'},
    )

    scrape_vessels >> upload_vessels_to_minio

# --- PORTS DAG (MONTHLY) ---
with DAG(
    'ports_ingestion',
    default_args=default_args,
    description='Download ports data and upload to MinIO',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'ports', 'bronze'],
) as ports_dag:

    download_ports = BashOperator(
        task_id='download_ports',
        bash_command='python /opt/airflow/extractors/batch/ports.py',
    )

    upload_ports_to_minio = PythonOperator(
        task_id='upload_ports_to_minio',
        python_callable=upload_to_minio,
        op_kwargs={'local_dir': 'ports', 's3_prefix': 'ports'},
    )

    download_ports >> upload_ports_to_minio
