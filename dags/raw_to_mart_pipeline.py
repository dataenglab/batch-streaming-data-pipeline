"""
ELT в Postgres: грязные сырые данные -> dbt staging -> dbt mart -> проверки качества.

generate_raw_data — scripts/generate_raw_data.py дописывает 500 "грязных" строк в raw_orders
                    (дубликаты order_id, пустые amount, даты в трёх форматах)
dbt_run           — stg_orders (view: очистка, типы, дедупликация) и mart_daily_sales (table)
dbt_test          — тесты dbt: unique / not_null / accepted_values + singular-тесты в dbt/tests

Задачи выполняются в контейнерах spark и dbt через docker exec
(Docker socket смонтирован в scheduler).
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "owner": "kamilla",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="raw_to_mart_pipeline",
    description="Raw orders in Postgres -> dbt staging -> dbt mart -> dbt tests",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "postgres"],
) as dag:

    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command="docker exec spark python /home/jovyan/scripts/generate_raw_data.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt dbt test",
    )

    generate_raw_data >> dbt_run >> dbt_test
