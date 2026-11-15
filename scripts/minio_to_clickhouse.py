"""
Батч-ветка: MinIO (batch_output) -> ClickHouse (orders_batch).

pg_to_minio.py каждый раз кладёт в batch_output ПОЛНЫЙ снимок таблицы orders.
Поэтому и в ClickHouse загружаем полный снимок: TRUNCATE + INSERT.
Так повторный запуск не плодит дубли, а строки, удалённые в Postgres,
исчезают и из ClickHouse.
"""
import urllib.request

from pyspark.sql import SparkSession

CLICKHOUSE_HTTP = "http://clickhouse:8123/"
CLICKHOUSE_JDBC = "jdbc:clickhouse://clickhouse:8123/default"
SOURCE_PATH = "s3a://bucket/batch_output/"
TARGET_TABLE = "orders_batch"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    id      Int32,
    name    Nullable(String),
    amount  Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id
"""


def clickhouse_exec(sql: str) -> None:
    """Выполнить DDL в ClickHouse через HTTP-интерфейс."""
    req = urllib.request.Request(CLICKHOUSE_HTTP, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        resp.read()


def main():
    spark = SparkSession.builder.appName("MinIO_to_ClickHouse_batch").getOrCreate()
    try:
        df = spark.read.parquet(SOURCE_PATH).select("id", "name", "amount")
        print(f"строк в снимке: {df.count()}")

        clickhouse_exec(DDL)
        clickhouse_exec(f"TRUNCATE TABLE {TARGET_TABLE}")  # полный снимок: TRUNCATE + INSERT

        (
            df.write
              .format("jdbc")
              .option("url", CLICKHOUSE_JDBC)
              .option("dbtable", TARGET_TABLE)
              .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
              .option("batchsize", "10000")
              .mode("append")
              .save()
        )
        print(f"снимок загружен в {TARGET_TABLE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
