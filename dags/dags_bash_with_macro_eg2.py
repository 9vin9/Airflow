from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

# DAG 정의
with DAG(
    dag_id= 'dags_bash_with_macro_eg2',
    schedule='10 0 * * 6#2',
    start_date= pendulum.datetime(2024, 3, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    #START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id= 'bash_task_2',
        env= {
            # 10 0 * * 6#2 매월 둘째주 토요일에 실행되니 감안해서 days 날짜 계산
            'START_DATE':'{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}',    #in_timezone('Asia/Seoul') UTC기준에서 한국시간으로
            'END_DATE':'{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}'
        },
        bash_command= 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )