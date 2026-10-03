from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def hello_world():
    print("Hello Airflow! DAG работает корректно.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_dag',
    default_args=default_args,
    description='Простой тестовый DAG',
    schedule=None,  # DAG не будет запускаться по расписанию, только вручную
    start_date=datetime(2025, 9, 12),
    catchup=False,
    tags=['test'],
) as dag:

    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world
    )

    task1
