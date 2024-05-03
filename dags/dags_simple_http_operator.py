from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

# DAG 정의
with DAG(
    dag_id= 'dags_simple_http_operator',
    schedule= None,
    start_date= pendulum.datetime(2024, 4, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    ''' 걷고싶은 서울길 조회'''
    tb_seoul_gil_info = SimpleHttpOperator(
        task_id= 'tb_seoul_gil_info',
        http_conn_id= 'openapi.seoul.go.kr',
        endpoint= '{{var.value.apikey_openapi_seoul_go_kr}}/json/SeoulGilWalkCourse/1/10',
        method= 'GET',
        headers={'Content-Type':'application/json',
                 'charset':'utf-8',
                 'Accept':'*/*' }
    )

    @task(task_id= 'python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids= 'tb_seoul_gil_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_seoul_gil_info >> python_2()