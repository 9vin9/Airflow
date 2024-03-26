from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

#DAG정의
with DAG(
    dag_id = 'dags_python_operator',
    schedule = '30 6 * * *', # 매일 6:30
    start_date = pendulum.datetime(2024, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
)as dag:
    def select_fruit():
        fruit = ['Apple', 'Banana', 'Avovado']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable = select_fruit  # 어떤 python함수 실행할건지   
    )

    py_t1
