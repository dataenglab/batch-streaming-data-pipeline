[← На главную](README.md)

<img width="1280" height="536" alt="image" src="https://github.com/user-attachments/assets/5c405bc8-a8c4-4d48-af8b-8f379d40de64" />

Проект разворачивает весь стек с картинки через `docker-compose.yml`.

❗ Прочитай этот файл до запуска — так будет меньше вопросов.

## 1. Клонирование

Открой терминал (Git Bash / PowerShell) в любой папке и выполни:

```bash
git clone https://github.com/dataenglab/batch-streaming-data-pipeline.git
cd batch-streaming-data-pipeline
```

## 2. JAR для Spark

❗ Скачай JAR, необходимый Spark и сборке образов, и положи его в `spark/jars/` (он весит ~300 МБ, поэтому его нет в репозитории):

https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.526/aws-java-sdk-bundle-1.12.526.jar

## 3. Файл .env

```bash
cp .env.example .env
```

Впиши в `.env` своё значение `SUPERSET_SECRET_KEY` (любая длинная случайная строка). Без этого файла `docker compose` не запустится.

## 4. Запуск

Убедись, что Docker запущен, и выполни (первая сборка долгая):

```bash
docker compose up -d --build
```

## Скрипты

❗ Все инструкции по запуску и настройке лежат в папке `ИНСТРУКЦИИ`. Прочитай их перед использованием скриптов из `scripts`.

| Скрипт | Что делает |
| --- | --- |
| `kafka_to_minio.py` | «верхняя ветка»: стриминг изменений Postgres (Debezium → Kafka → MinIO) |
| `pg_to_minio.py` | «нижняя ветка»: батчевая выгрузка Postgres → MinIO |
| `minio_to_clickhouse.py` | паркеты из `batch_output` → ClickHouse (`new_table`) |
| `minio_to_clickhouse_kafka.py` | паркеты из `kafka_output` → ClickHouse (`kafka_orders`) |
| `generate_raw_data.py` | генерирует «грязные» данные в `raw_orders` для dbt (запускается из DAG `raw_to_mart_pipeline`) |
| `kafka_producer.py` | генератор тестовых сообщений в топик `sensor_metrics` (создай его в Kafka UI, localhost:8082: 1 партиция, 1 реплика) |

❗ `kafka_producer.py` запускается на твоём компьютере (`pip install kafka-python`, затем `python scripts/kafka_producer.py`). Spark-скрипты запускаются через `docker exec ... spark-submit`, как описано в инструкциях.

Для `kafka_to_minio.py` нужно зарегистрировать Debezium-коннектор — инструкция прилагается.

«В терминале» в инструкциях означает: открой терминал в корневой папке проекта (там, где `docker-compose.yml`).

[← На главную](README.md)
