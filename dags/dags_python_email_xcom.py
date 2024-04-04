from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

# DAG정의
with DAG(
    dag_id= 'dags_python_email_operator',
    schedule= '@once',
    start_date= pendulum.datetime(2024, 4, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    # Python task 정의
    @task(task_id= 'something_task')
    def some_logic(**kwargs):
        from random import choice   # import choice: string, list, tuple중 random으로 값 꺼내기
        return choice(['Success', 'Fail'])
    
    # 이메일 보내는 task
    send_email = EmailOperator(
        task_id= 'send_email',
        to= 'luvin3994@gmail.com',
        subject= '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} some_logic 처리결과',
        # HTML형식으로 이메일 본문 작성
        html_content= '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} 처리 결과는 <br> \
                        {{ti.xcom_pull(task_ids= "something_task")}} 했습니다 <br>'
    )

    # 의존성 설정
    some_logic() >> send_email