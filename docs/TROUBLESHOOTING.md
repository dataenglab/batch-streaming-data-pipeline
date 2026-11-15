[← На главную](../README.md)

# Troubleshooting

Реальные проблемы, которые встретились при запуске проекта с чистого клона на Windows (Docker Desktop + WSL2, Git Bash), и как они решаются.

---

## Docker / окружение

### `docker compose up` падает: `Set SUPERSET_SECRET_KEY in .env`
Нет файла `.env`. Решение: `cp .env.example .env` и задать `SUPERSET_SECRET_KEY`.

### `Conflict. The container name "/minio" is already in use`
В `docker-compose.yml` заданы фиксированные `container_name`, а на машине запущен другой проект с теми же именами. Посмотреть, чьи контейнеры:
```bash
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Label \"com.docker.compose.project\"}}"
```
Остановить чужой проект по имени: `docker compose -p <project> down`.

### `request returned 500 Internal Server Error ... dockerDesktopLinuxEngine`
Упал движок Docker Desktop. Почти всегда это нехватка памяти: по умолчанию WSL2 отдаёт Docker половину RAM (4 ГБ на ноутбуке с 8 ГБ), а весь стек требует больше. Решение:
1. `C:\Users\<имя>\.wslconfig` с `memory=5GB` и `swap=8GB` (файл должен лежать в папке пользователя и не иметь расширения `.txt`);
2. Quit Docker Desktop → `wsl --shutdown` → запуск Docker Desktop;
3. поднимать стек частями через профили (`--profile streaming` и т.д.) и не запускать два Spark-драйвера одновременно.

### Git Bash: `No such file or directory: '/home/jovyan/D:/Git/home/...'` или `No FileSystem for scheme "D"`
Git Bash (MSYS) переписывает аргументы, похожие на Unix-пути, в пути Windows. Решение: `export MSYS_NO_PATHCONV=1` (лучше добавить в `~/.bashrc`).

---

## Airflow

### Задача `docker exec ...` падает: `client version 1.41 is too old. Minimum supported API version is 1.44`
В образ Airflow ставился Docker CLI из apt-репозитория Debian (`docker.io`, API 1.41), а Docker Desktop принимает только API ≥ 1.44. Решение — брать свежий CLI из официального образа multi-stage сборкой:
```dockerfile
FROM docker:27-cli AS dockercli
FROM apache/airflow:3.2.2
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
```

### `generate_raw_data` падает с `ModuleNotFoundError: faker`
Скрипт выполнялся в голом образе `jupyter/pyspark-notebook`, где нет `faker` и `psycopg2`. Теперь он запускается в контейнере `spark`, где зависимости ставятся из `requirements.txt`.

---

## dbt

### `Env var required but not provided: 'POSTGRES_PASSWORD'`
`profiles.yml` берёт пароль из переменной окружения, а у контейнера `dbt` её не было. Решение: `environment: POSTGRES_PASSWORD` в compose и значение по умолчанию `env_var('POSTGRES_PASSWORD', 'postgres')`.

---

## Spark

### `ClassNotFoundException: org.apache.spark.kafka010.KafkaConfigUpdater`
Конфликт загрузчиков классов. `spark-sql-kafka` лежал в системной папке `$SPARK_HOME/jars` (системный class loader), а его зависимости (`spark-token-provider-kafka`, `commons-pool2`) докачивались через `--packages` в дочерний class loader, который системный не видит. Решение: положить все зависимости в одно место — `spark/jars/` — и не использовать `--packages`.

### `Unable to infer schema for Parquet at .` при чтении `kafka_output`
Стрим создал `_spark_metadata`, но не записал ни одного батча (в логе `BatchIds found from listing:` пусто). Причина — упавший стрим; смотреть ошибку в логе `kafka_to_minio.py`.

### Стрим падает: `UnknownTopicOrPartitionException: This server does not host this topic-partition`
Checkpoint Structured Streaming привязан к offset'ам конкретного топика. Топик был пересоздан (после аварийного выключения), а checkpoint ссылался на старый. Решение: удалить checkpoint (`s3a://bucket/checkpoints/kafka_orders`) и вывод стрима — см. «Перерегистрация с нуля» в `ИНСТРУКЦИИ/регистрация debezium connector'а.txt`. Опция `failOnDataLoss=false` только маскирует проблему.

---

## Kafka / Debezium

### Debezium не стартует: `Topic 'debezium_status' ... is required to have 'cleanup.policy=compact'`
Служебные топики Kafka Connect (`debezium_config`, `debezium_offsets`, `debezium_status`) обязаны быть compacted. После аварийного выключения топик был создан заново автосозданием брокера (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`) с политикой по умолчанию `delete`. Решение:
```bash
for t in debezium_config debezium_offsets debezium_status; do
  docker exec kafka kafka-configs --bootstrap-server kafka:9092 --alter --entity-type topics --entity-name $t --add-config cleanup.policy=compact
done
docker start debezium
```

### Коннектор в статусе `UNASSIGNED`, REST API отвечает `Empty reply from server`
Kafka Connect не запустился (см. пункт выше) или ещё стартует — смотреть `docker logs debezium --tail 30`. После исправления — перерегистрировать коннектор.

---

## ClickHouse

### Дубли после повторной загрузки
- **Батч:** `pg_to_minio.py` пишет полный снимок, поэтому в ClickHouse он тоже грузится полным снимком (TRUNCATE + INSERT в `orders_batch`).
- **CDC:** Kafka хранит историю изменений, а не текущее состояние; плюс Debezium повторяет снимок таблицы при перерегистрации. События пишутся в `ReplacingMergeTree(version, is_deleted) ORDER BY id`, где `version` — LSN Postgres, а текущее состояние читается через представление `orders_current` (`FINAL` + `is_deleted = 0`).

---

## Superset

### `could not translate host name "postgres"` / не подключается к `localhost`
В поле Host нужно имя контейнера в сети Docker: `postgres-src`, `clickhouse`. `localhost` внутри контейнера Superset — это сам Superset.

### Какой URI у ClickHouse
Установлен драйвер `clickhouse-connect`, его схема — `clickhousedb://default:@clickhouse:8123/default` (а не `clickhouse+http://`).

---

## Spark JDBC → ClickHouse

### `DB::Exception: Table engine is not specified in CREATE query (ENGINE_REQUIRED)` при `mode("overwrite")`
Опция `truncate=true` работает только для баз, чей JDBC-диалект Spark знает (Postgres, MySQL и др.). Для ClickHouse встроенного диалекта нет, и Spark вместо TRUNCATE делает DROP TABLE + CREATE TABLE — без `ENGINE`, который ClickHouse обязателен. Решение: создавать таблицу и делать `TRUNCATE` самим (через HTTP-интерфейс ClickHouse), а писать из Spark в режиме `append` — тогда Spark не трогает структуру таблицы.
