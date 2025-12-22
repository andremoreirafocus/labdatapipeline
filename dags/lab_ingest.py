from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from services.extract.fetch_data import fetch_data
from services.extract.write_data_to_minio import write_data_to_minio

from datetime import datetime

# Definindo os argumentos padrão para as tarefas do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


# Função que combina todas as etapas do pipeline de ingestão de dados
def extract():
    api_server = "https://randomuser.me/api/"
    return fetch_data(api_server)


def load(ti):   
    bucket_name = "raw"
    now = datetime.now()
    object_name = now.strftime("year=%Y/month=%m/day=%d/hour=%H/person_%M%S.json")
    data = ti.xcom_pull(task_ids="extract")
    write_data_to_minio(data, bucket_name, object_name)


# Criando o DAG
with DAG(
    "lab_ingest",
    default_args=default_args,
    description="Ingestão de dados do lab",
    schedule_interval="* * * * *",  # Use cron expression for every minute
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> load_task
    