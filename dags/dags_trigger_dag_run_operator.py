from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

'''
DAG간의 의존관계, 선후행 관계를 주는 방법 1
'''

# DAG 정의
with DAG(
    dag_id= 'dags_trigger_dag_run_operator',
    start_date= pendulum.datetime(2024, 4, 1, tz= 'Asia/Seoul'),
    schedule= '30 9 * * *',
    catchup= False
) as dag:
    
    start_task = BashOperator(
        task_id= 'start_task',
        bash_command= 'echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id= 'trigger_dag_task',            # 필수 값
        trigger_dag_id= 'dags_python_operator', # 필수 값   
        # run_id: DAG의 수행방식과 시간을 유일하게 식별해주는 키
        #         같은 시간이라 해도 수행방식(Schedule, manual, Backfill)에 따라 키가 달라짐
        #         스케줄에 의해 실행된 경우 scheduled_{{data_interval_start}} 값을 가짐
        
        trigger_run_id= None,       # schedule__timestamp or manual__timestamp 템플릿 변수 사용
        execution_date= '{{data_interval_start}}',      # manual__{{excution_date}}로 수행
        reset_dag_run= True,        # 이미 run_id 값이 있는 경우에도 재수행 여부
        wait_for_completion= False,
        poke_interval= 60,
        allowed_states= ['success'],
        failed_states= None
    )

    start_task >> trigger_dag_task