# DAG 정의
from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = 'dags_bash_select_fruit',
    schedule = '10 0 * * 6#1', # 매월 첫째주 토요일 0시 10분
    start_date = pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup = False
)as dag:
    
    t1_orange = BashOperator(
        task_id = 't1_orange',
        bash_command="/opt/airflow/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    t2_avocado = BashOperator(
        task_id = 't2_avocado',
        bash_command="/opt/airflow/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado