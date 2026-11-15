"""
CDC-ветка: MinIO (kafka_output) -> ClickHouse (orders_cdc + представление orders_current).

Сам стрим Kafka -> MinIO (kafka_to_minio.py) — бесконечный процесс, его не запускают из Airflow:
он работает отдельно и непрерывно складывает события Debezium в MinIO.
Этот DAG раз в час переносит накопившиеся события в ClickHouse.

Загрузка идемпотентна: ReplacingMergeTree(version, is_deleted) схлопывает одинаковые
(id, version), поэтому повторная обработка тех же файлов не создаёт дублей.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "owner": "kamilla",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "pool": "spark",
}

with DAG(
    dag_id="cdc_minio_to_clickhouse",
    description="CDC events from MinIO -> ClickHouse orders_cdc (ReplacingMergeTree)",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "spark", "clickhouse"],
) as dag:

    load_cdc = BashOperator(
        task_id="minio_to_clickhouse_cdc",
        bash_command="docker exec spark spark-submit /home/jovyan/scripts/minio_to_clickhouse_kafka.py",
    )

    check_current_state = BashOperator(
        task_id="check_current_state",
        bash_command=(
            "docker exec clickhouse clickhouse-client -q "
            "\"SELECT count() AS rows_now, max(source_ts) AS last_change FROM orders_current\""
        ),
        pool="default_pool",
    )

    load_cdc >> check_current_state
