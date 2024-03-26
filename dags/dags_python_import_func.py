from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp


#DAG정의
with DAG(
    dag_id= 'dags_python_import_func',
    schedule= '30 6 * * *', 
    start_date= pendulum.datetime(2024, 3, 1, tz = 'Asia/Seoul'),
    catchup=False
)as dag:
    
    task_get_sftp= PythonOperator(
        task_id= 'task_get_sftp',
        python_callable= get_sftp
    )