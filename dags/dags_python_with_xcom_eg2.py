from airflow import DAG
import pendulum
from airflow.decorators import task

# DAG정의
with DAG(
    dag_id= 'dags_python_with_xcom_eg2',
    schedule= '30 6 * * *',
    start_date= pendulum.datetime(2024, 3, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    # Task1: return 값을 사용하여 xcom push
    @task(task_id= 'python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    # Task2: Task1의 return 값으로부터 xcom pull
    @task(task_id= 'python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids= 'python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값: ' + value1)

    # Task3: 함수 입력 값으로 받은 값을 출력
    @task(task_id= 'python_xcom_pull_2')
    def xcome_pull_2(status, **kwargs):     # status: 이전 task에서 반환된 값
        print('함수 입력값으로 받은 값: ' + status)

    # 의존성 설정
    python_xcom_push_by_return = xcom_push_result()
    xcome_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()