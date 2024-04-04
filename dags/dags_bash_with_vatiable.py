'''
전역변수는 협업환경에서 표준화된 dag을 만들기 위해 주로 사용
주로 상수(CONST)로 지정해서 사용할 변수들 셋팅
'''
from airflow import DAG
import pendulum
from airflow.models import Variable     # Variable 라이브러리 이용하여 전역변수 사용
from airflow.operators.bash import BashOperator # Jinja 템플릿 이용하여 전역변수 사용

# DAG정의
with DAG(
    dag_id= 'dags_bash_with_variable',
    schedule='@monthly',
    start_date= pendulum.datetime(2024, 4, 1, tz= 'Asia/Seoul'),
    catchup= False
) as dag:
    
    # Variable 라이브러리 이용하여 전역변수 사용
    # 스케줄러의 주기적 DAG 파싱시 불필요한 부하 발생, 스케줄러 과부하 원인 중 하나
    var_value = Variable.get('sample_key')

    bash_var_1 = BashOperator(
        task_id= 'bash_var_1',
        bash_command= f'echo variable: {var_value}'
    )

    # Jinja 템플릿 이용하여 전역변수 사용, 권고되는 사항
    bash_var_2 = BashOperator(
        task_id= 'bash_var_2',
        bash_command= 'echo variable:{{var.value.sample_key}}'
    )