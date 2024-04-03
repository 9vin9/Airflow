from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

# DAG정의
with DAG(
    dag_id= 'dags_python_with_xcom_eg1',
    schedule= '30 6 * * *',
    start_date= pendulum.datetime(2024, 3, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    # Task1: xcom을 사용하여 push
    @task(task_id= 'python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_1')
        ti.xcom_push(key='result2', value=[1,2,3])

    # Task2: xcom을 사용하여 push
    @task(task_id= 'python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_2')
        ti.xcom_push(key='result2', value=[1,2,3,4])
    
    # Task3: xcom을 사용하여 pull
    @task(task_id= 'python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key='result1')
        value2 = ti.xcom_pull(key='result2', task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)

    
    # 의존성 설정
    xcom_push1() >> xcom_push2() >> xcom_pull()