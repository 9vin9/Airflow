from airflow import DAG
from airflow.operators.bash import BashOperator

import datetime
import pendulum

# DAG 정의

with DAG(
    dag_id = "dag_bash_operator", # 파일명과 별개
    schedule = "0 0 * * *", # 분 시 일 월 요일 (매일 0시 0분에 작업)
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup = False, # start_date에서 앞에 누락된 구간을 돌릴건지 설정 False: 안돌림
    # dagrun_timeout = datetime.timedelta(minutes = 60), # 타임아웃 설정, 여기서 60분 이상 돌게 되면 실패하도록 설정
    # tags = ['example', 'example2'], # optional tag설정
    # params = {'example_key': 'example_value'}, # task에 넘겨줄 공통 parameter설정
)as dag:
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = 'echo whoami', # echo == print
    )
    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        bash_command = 'echo $HOSTNAME',
    )

# task간의 수행 순서
    
    bash_t1 >> bash_t2 