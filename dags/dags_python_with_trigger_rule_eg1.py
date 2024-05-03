from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

# DAG 정의
with DAG(
    dag_id= 'dags_python_with_trigger_rule_eg1',
    start_date= pendulum.datetime(2024, 4, 1 ,tz= 'Asia/Seoul'),
    schedule= None,
    catchup= False
) as dag:
    
    # BashOperator 정의
    bash_upstream_1 = BashOperator(
        task_id= 'bash_upstream_1',
        bash_command= 'echo upstream1'
    )

    # PythonOperator를 사용하여 예외 발생 task 정의
    @task(task_id= 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    
    # 메시지 출력 task
    @task(task_id= 'python_upstream_2')
    def python_upstream_2():
        print('정상 처리')  
    
    @task(task_id= 'python_downstream_1', trigger_rule= 'all_done')
    def python_downstream_1():
        print('정상 처리')

    # task 간 의존성 설정
    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()


