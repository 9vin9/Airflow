from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

import pendulum
import datetime

# DAG 정의
with DAG(
    dag_id= 'dags_python_with_task_group',
    schedule= None,
    start_date= pendulum.datetime(2024, 4, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    def inner_func(**kwargs):
        msg= kwargs.get('msg') or ''
        print (msg)

    @task_group(group_id= 'first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹'''      # docstring: 함수에 대한 설명, airflow내에서 확인가능
        
        # task decorator를 이용한 PythonOperator 
        @task(task_id= 'inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task 입니다.')
        
        # PythonOperator
        inner_function2 = PythonOperator(
            task_id= 'inner_function2',
            python_callable= inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번째 task 입니다.'}
        )

        # flow 정의
        inner_func1() >> inner_function2

    # class를 이용하여 TaskGroup
    with TaskGroup(group_id= 'second_group', tooltip= '두 번째 그룹입니다.') as group_2:
        ''' 여기에 적은 docstring은 표시되지 않습니다.'''       # docstring == tooltip
        @task(task_id= 'inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup내 첫 번째 task 입니다.')
        
        inner_function2 = PythonOperator(
            task_id= 'inner_function2',
            python_callable= inner_func,
            op_kwargs= {'msg': '두 번째 TaskGroup내 두 번째 task 입니다.'}
        )

        inner_func1() >> inner_function2
    
    group_1() >> group_2