from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import StringIO


RAW_DATA_URI = "s3://smartspend-datapipeline/Raw_data/DE_DA.csv"
TRANSFORMED_DATA_FOLDER = "s3://smartspend-datapipeline/transformed_data/"
TRANSFORMED_DATA_KEY = "transformed_data/DE_DA_transformed.csv" 

def download_from_s3(**kwargs):
    
    s3_hook = S3Hook(aws_conn_id='aws_connection_faisal')
    bucket_name, key = RAW_DATA_URI.replace("s3://", "").split("/", 1)
    raw_data = s3_hook.read_key(bucket_name=bucket_name, key=key)
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
    key = f"{folder}DE_DA_transformed.csv" 

    s3_hook = S3Hook(aws_conn_id='aws_connection_faisal')
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
        provide_context=True, 
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,  
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True, 
    )

    download_task >> transform_task >> upload_task
