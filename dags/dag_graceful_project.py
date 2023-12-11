from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.api_load_location import select_now_time_info,get_location_api,data_transform_result,make_text_message,send_to_slack
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
var_value=Variable.get("gonggong_api_key")


with DAG(
    dag_id="dag_graceful_project", ## airflow에들어왔을때 보이는 dag이름
    schedule="55 6,17 * * 1-5", ## 평일 (오전6시, 오후5시)시 50분에 진행
    start_date=pendulum.datetime(2023, 9, 20, tz="Asia/Seoul"), ## 서울로설정
    catchup=False ## 날짜 누락된 구간은 코드 실행x(start_date부터 어제까지의 구간은 코드실행X) 
) as dag:
    
    
    select_now_time_info=PythonOperator(
        task_id='select_now_time_info',
        python_callable=select_now_time_info
    ) 
    
    get_seoul_api=PythonOperator(
        task_id='get_seoul_api',
        python_callable=get_location_api,
        op_kwargs={'api_key':var_value,
                   'input_nx':58,
                   'input_ny':126,
                   'input_location':'seoul'}
    )
    
    get_buchon_api=PythonOperator(
        task_id='get_buchon_api',
        python_callable=get_location_api,
        op_kwargs={'api_key':var_value,
                   'input_nx':56,
                   'input_ny':125,
                   'input_location':'buchon'}
    )
    
    data_transform_result=PythonOperator(
        task_id='data_transform_result',
        python_callable=data_transform_result
    )
    
    make_text_message=PythonOperator(
        task_id='make_text_message',
        python_callable=make_text_message
    )
    
    send_to_slack=SlackWebhookOperator(
        task_id='send_to_slack',
        slack_webhook_conn_id='conn_slack_airflow_bot',
        message="{{ task_instance.xcom_pull(task_ids='make_text_message') }}"
    )

    
    select_now_time_info >> [get_seoul_api, get_buchon_api] >> data_transform_result >> make_text_message >> send_to_slack