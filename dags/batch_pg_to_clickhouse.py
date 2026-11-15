"""
Батч-ветка: Postgres -> MinIO -> ClickHouse (orders_batch).

pg_to_minio.py           — полный снимок таблицы orders в s3a://bucket/batch_output (Parquet)
minio_to_clickhouse.py   — снимок в ClickHouse orders_batch (TRUNCATE + INSERT, идемпотентно)

Задачи выполняются в контейнере spark через docker exec (Docker socket смонтирован в scheduler).
Пул "spark" (1 слот, создаётся в airflow-init) не даёт двум Spark-драйверам работать
одновременно — при 8 ГБ RAM это роняет Docker.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SPARK_SUBMIT = "docker exec spark spark-submit /home/jovyan/scripts/{script}"

default_args = {
    "owner": "kamilla",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "pool": "spark",
}

with DAG(
    dag_id="batch_pg_to_clickhouse",
    description="Full snapshot of orders: Postgres -> MinIO -> ClickHouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "spark", "clickhouse"],
) as dag:

    pg_to_minio = BashOperator(
        task_id="pg_to_minio",
        bash_command=SPARK_SUBMIT.format(script="pg_to_minio.py"),
    )

    minio_to_clickhouse = BashOperator(
        task_id="minio_to_clickhouse",
        bash_command=SPARK_SUBMIT.format(script="minio_to_clickhouse.py"),
    )

    pg_to_minio >> minio_to_clickhouse
