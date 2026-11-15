"""
CDC-ветка: MinIO (kafka_output) -> ClickHouse (orders_cdc + представление orders_current).

В kafka_output лежат сырые события Debezium (колонка value с JSON):
каждое INSERT / UPDATE / DELETE в Postgres — отдельная строка.
Чтобы получить в ClickHouse ТЕКУЩЕЕ состояние таблицы orders:

1. Разбираем JSON на колонки (from_json).
2. Версия строки = LSN из Postgres (__lsn): чем позже изменение, тем больше LSN.
3. Пишем в ReplacingMergeTree(version, is_deleted) ORDER BY id:
   для каждого id ClickHouse оставляет строку с максимальной версией,
   а удалённые строки (is_deleted = 1) отбрасывает.
4. Представление orders_current читает таблицу с FINAL — это и есть текущее состояние.

Скрипт идемпотентен: его можно запускать повторно на тех же файлах —
одинаковые (id, version) схлопнутся в одну строку.
"""
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

CLICKHOUSE_HTTP = "http://clickhouse:8123/"
CLICKHOUSE_JDBC = "jdbc:clickhouse://clickhouse:8123/default"
SOURCE_PATH = "s3a://bucket/kafka_output/"
TARGET_TABLE = "orders_cdc"
CURRENT_VIEW = "orders_current"

# Формат сообщения задаётся в pg-connector.json:
# ExtractNewRecordState + delete.tombstone.handling.mode=rewrite + add.fields=op,lsn,source.ts_ms
EVENT_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType()),
    StructField("__op", StringType()),             # c = insert, u = update, d = delete, r = snapshot
    StructField("__lsn", LongType()),              # позиция в WAL Postgres
    StructField("__source_ts_ms", LongType()),     # время изменения в Postgres
    StructField("__deleted", StringType()),        # "true" для DELETE
])

DDL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    id          Int32,
    name        Nullable(String),
    amount      Nullable(Float64),
    op          String,
    source_ts   Nullable(DateTime64(3)),
    version     UInt64,
    is_deleted  UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id
"""

DDL_VIEW = f"""
CREATE VIEW IF NOT EXISTS {CURRENT_VIEW} AS
SELECT id, name, amount, source_ts
FROM {TARGET_TABLE} FINAL
WHERE is_deleted = 0
"""


def clickhouse_exec(sql: str) -> None:
    """Выполнить DDL в ClickHouse через HTTP-интерфейс."""
    req = urllib.request.Request(CLICKHOUSE_HTTP, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        resp.read()


def main():
    spark = SparkSession.builder.appName("MinIO_to_ClickHouse_cdc").getOrCreate()
    try:
        raw = spark.read.parquet(SOURCE_PATH)

        events = (
            raw.where(F.col("value").isNotNull())
               .select(F.from_json("value", EVENT_SCHEMA).alias("e"))
               .select("e.*")
               .where(F.col("id").isNotNull())
        )

        result = events.select(
            "id",
            "name",
            "amount",
            F.coalesce("__op", F.lit("unknown")).alias("op"),
            (F.col("__source_ts_ms") / 1000).cast("timestamp").alias("source_ts"),
            F.coalesce("__lsn", F.lit(0)).cast("long").alias("version"),
            F.when(F.col("__deleted") == "true", 1).otherwise(0).alias("is_deleted"),
        )

        print(f"событий CDC: {result.count()}")

        clickhouse_exec(DDL_TABLE)
        clickhouse_exec(DDL_VIEW)

        (
            result.write
                  .format("jdbc")
                  .option("url", CLICKHOUSE_JDBC)
                  .option("dbtable", TARGET_TABLE)
                  .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                  .option("batchsize", "10000")
                  .mode("append")
                  .save()
        )
        print(f"события загружены в {TARGET_TABLE}; текущее состояние — SELECT * FROM {CURRENT_VIEW}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
