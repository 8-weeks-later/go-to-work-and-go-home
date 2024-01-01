from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import glob



dag = DAG(
    'dag_cleanup_logs',
    schedule_interval='0 0 * * 6',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def dag_cleanup_logs():
    log_folder_path = '/opt/airflow/logs'
    retention_days = 7  # 로그 보존 기간 설정 (예: 7일)

    # 현재 날짜를 기준으로 로그 보존 기간 이전의 날짜 계산
    cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

    # 로그 폴더에서 보존 기간 이전의 로그 파일 찾기 및 삭제
    for root, dirs, files in os.walk(log_folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_modified_time = datetime.utcfromtimestamp(os.path.getmtime(file_path))

            # 파일 수정 시간이 cutoff_date 이전이면 삭제
            if file_modified_time < cutoff_date:
                os.remove(file_path)
                print(f'Deleted log file: {file_path}')

# PythonOperator를 사용하여 cleanup_logs 함수 실행
dag_cleanup_logs = PythonOperator(
    task_id='dag_cleanup_logs',
    python_callable=dag_cleanup_logs,
    dag=dag,
)

dag_cleanup_logs
