from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from services.load.list_objects_in_minio_folder import list_objects_in_minio_folder
from services.load.get_raw_files_from_minio import get_raw_files_from_minio
from services.load.get_parquet_buffer_from_flattened_records import (
    get_parquet_buffer_from_flattened_records,
)
from services.load.write_processed_to_minio import write_processed_to_minio

# from time import time
from datetime import datetime
import logging

# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


# Função que combina todas as etapas do pipeline de ingestão de dados
def get_raw_files():
    source_bucket_name = "raw"

    now = datetime.now()

    year = now.year  # 2025
    month = now.month  # 12
    day = now.day  # 19
    hour = now.hour  # 14
        # start_time = time.time()
    prefix = f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    print(f"Listing objects in MinIO with prefix: {prefix}")
    logging.info(f"Listing objects in MinIO with prefix: {prefix}")
    object_names_to_be_transformed = list_objects_in_minio_folder(
        bucket_name=source_bucket_name,
        prefix=prefix,
    )
    print(f"Files to be transformed: {object_names_to_be_transformed}")
    logging.info(f"Files to be transformed: {object_names_to_be_transformed}")
    all_flattened_records = get_raw_files_from_minio(
        object_names=object_names_to_be_transformed,
        source_bucket_name=source_bucket_name,
    )
    return all_flattened_records


def process_files(ti):
    destination_bucket_name = "processed"
    now = datetime.now()
    year = now.year  # 2025
    month = now.month  # 12
    day = now.day  # 19
    hour = now.hour  # 14
    prefix = f"year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    data = ti.xcom_pull(task_ids="get_raw_files")
    if data:
        out_buffer = get_parquet_buffer_from_flattened_records(data)
        destination_object_name = (
            f"{prefix}consolidated-{year}{month}{day}{hour}.parquet"
        )
        write_processed_to_minio(
            buffer=out_buffer,
            destination_bucket_name=destination_bucket_name,
            destination_object_name=destination_object_name,
        )
    else:
        print("No records found to write to the destination bucket.")
        logging.info("No records found to write to the destination bucket.")


# Criando o DAG
with DAG(
    "lab_load",
    default_args=default_args,
    description="Ingestão de dados do lab",
    schedule_interval="* * * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    get_files_task = PythonOperator(
        task_id="get_raw_files", python_callable=get_raw_files
    )

    process_task = PythonOperator(
        task_id="process_files", python_callable=process_files
    )

    get_files_task >> process_task
