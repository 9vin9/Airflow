from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

# DAG정의
with DAG(
    dag_id= 'dags_bash_with_template',
    schedule= '10 0 * * *',
    start_date= pendulum.datetime(2024, 3, 1, tz= 'Asia/Seoul'),
    catchup= False,
) as dag:
    
    # 첫 번째 BashOperator 설정
    bash_t1 = BashOperator(
        task_id= 'bash_t1',
        bash_command= 'echo "data_interval_end: {{ data_interval_end }} "'
    )

    # 두 번째 BashOperator 설정
    bash_t2= BashOperator(
        task_id= 'bash_t2',
        env= {
            'START_DATE': '{{data_interval_start | ds}}', # key value
            'END_DATE': '{{data_interval_end | ds}}'
        }, # dictionary
        bash_command= 'echo $START_DATE && echo $END_DATE'
    )

    # task 간 의존성 설정
    bash_t1 >> bash_t2