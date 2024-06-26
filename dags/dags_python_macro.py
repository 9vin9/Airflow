from airflow import DAG
import pendulum
from airflow.decorators import task

# DAG 정의
with DAG(
    dag_id= 'dags_python_macro',
    schedule= '10 0 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup= False
) as dag:
    
    # task1: macro를 사용하여 템플릿 값 계산
    @task(task_id='task_using_macros',
      templates_dict={'start_date': '{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds}}',
                      'end_date': '{{(data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}'
                      }
      )
    
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)
    

    # task2: dateutil 라이브러리를 사용하여 날짜 계산
    @task(task_id= 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta    # 스케줄러 부하 경감을 위해 아래에 작성

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    # 의존성 설정
    get_datetime_macro() >> get_datetime_calc()