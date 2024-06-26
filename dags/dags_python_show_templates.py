from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

# DAG 정의
with DAG(
    dag_id= 'dags_python_show_templates',
    schedule= '30 9 * * *',
    start_date= pendulum.datetime(2024, 3, 10, tz= 'Asia/Seoul'),
    catchup= True # 3월10일부터 오늘 날짜까지 모두 실행
) as dag:
    
    @task(task_id= 'python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()