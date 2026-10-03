from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="raw_to_mart_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command="docker exec jupyter python /home/jovyan/scripts/generate_raw_data.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="docker exec dbt dbt run",
    )

    generate_raw_data >> run_dbt