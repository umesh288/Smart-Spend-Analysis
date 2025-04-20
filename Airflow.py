from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import StringIO

RAW_DATA_URI = "s3://smartspendanalysis/Raw_data/RAW SSA.csv"
TRANSFORMED_DATA_FOLDER = "s3://smartspendanalysis/transformed_data/"
TRANSFORMED_DATA_KEY = "transformed_data/transformed_data.csv"

def download_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='my_connection')
    bucket_name, key = RAW_DATA_URI.replace("s3://", "").split("/", 1)
    obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
    raw_data = obj.get()['Body'].read().decode('utf-8')
    return raw_data

def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='download_from_s3')
    df = pd.read_csv(StringIO(raw_data))
    df = df.dropna()
    numeric_columns = [
        "Rent", "Loan_Repayment", "Insurance", "Groceries", "Transport",
        "Eating_Out", "Entertainment", "Utilities"
    ]
    df[numeric_columns] = df[numeric_columns].round(2)
    transformed_data = df.to_csv(index=False)
    return transformed_data

def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    bucket_name, folder = TRANSFORMED_DATA_FOLDER.replace("s3://", "").split("/", 1)
    key = f"{folder}{TRANSFORMED_DATA_KEY}"

    s3_hook = S3Hook(aws_conn_id='my_connection')
    s3_hook.load_string(
        string_data=transformed_data,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )

with DAG(
    dag_id="smart_spent_etl_dag",
    default_args={"owner": "airflow", "retries": 1},
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download_task >> transform_task >> upload_task
