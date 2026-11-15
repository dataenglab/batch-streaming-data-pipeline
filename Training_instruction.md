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

Заполни в `.env` два значения, без них `docker compose` не запустится:

- `SUPERSET_SECRET_KEY` — любая длинная случайная строка;
- `AIRFLOW_FERNET_KEY` — ключ шифрования Airflow, сгенерируй его в Git Bash:

```bash
openssl rand -base64 32 | tr '+/' '-_'
```

## 4. Память для Docker (Windows)

Весь стек требует ~8 ГБ памяти для Docker. По умолчанию Docker Desktop на WSL2 получает половину RAM компьютера — на ноутбуке с 8 ГБ это 4 ГБ, и Docker падает с ошибкой `500 Internal Server Error`, как только стартует Spark.

Создай файл `C:\Users\<имя>\.wslconfig` (именно в папке пользователя, без `.txt` в конце):

```ini
[wsl2]
memory=5GB
processors=4
swap=8GB
```

Затем: Quit Docker Desktop → в PowerShell `wsl --shutdown` → запусти Docker Desktop. Проверка: `docker info --format "{{.MemTotal}}"`.

При 8 ГБ RAM запускай стек частями (см. шаг 6) и не держи два Spark-драйвера одновременно.

## 5. Git Bash

Git Bash переписывает пути вида `/home/jovyan/...` в пути Windows, и команды `docker exec` ломаются. Выполни один раз и открой терминал заново:

```bash
echo 'export MSYS_NO_PATHCONV=1' >> ~/.bashrc
```

## 6. Запуск

Убедись, что Docker запущен. Сервисы разбиты на профили, чтобы стек можно было поднимать частями:

| Команда | Что запускается |
| --- | --- |
| `docker compose up -d --build` | ядро: Postgres, MinIO, ClickHouse, Spark (батч-ветка) |
| `docker compose --profile streaming up -d` | + ZooKeeper, Kafka, Debezium, Kafka UI (CDC-ветка) |
| `docker compose --profile orchestration up -d` | + Airflow, dbt |
| `docker compose --profile bi up -d` | + Superset |
| `docker compose --profile dev up -d` | + Jupyter |
| `docker compose --profile streaming --profile orchestration --profile bi --profile dev up -d` | всё сразу (нужно 8+ ГБ для Docker) |

Остановить всё: `docker compose --profile streaming --profile orchestration --profile bi --profile dev down` (без `-v` данные в томах сохранятся).

Первая сборка образов долгая. Если упало что-то непонятное — см. [TROUBLESHOOTING](docs/TROUBLESHOOTING.md).

## Скрипты

❗ Все инструкции по запуску и настройке лежат в папке `ИНСТРУКЦИИ`. Прочитай их перед использованием скриптов из `scripts`.

| Скрипт | Что делает |
| --- | --- |
| `kafka_to_minio.py` | «верхняя ветка»: стриминг изменений Postgres (Debezium → Kafka → MinIO) |
| `pg_to_minio.py` | «нижняя ветка»: батчевая выгрузка Postgres → MinIO |
| `minio_to_clickhouse.py` | полный снимок из `batch_output` → ClickHouse (`orders_batch`, TRUNCATE + INSERT) |
| `minio_to_clickhouse_kafka.py` | события CDC из `kafka_output` → ClickHouse (`orders_cdc`, ReplacingMergeTree) + представление `orders_current` |
| `generate_raw_data.py` | генерирует «грязные» данные в `raw_orders` для dbt (запускается из DAG `raw_to_mart_pipeline`) |
| `kafka_producer.py` | генератор тестовых сообщений в топик `sensor_metrics` (создай его в Kafka UI, localhost:8082: 1 партиция, 1 реплика) |

❗ `kafka_producer.py` запускается на твоём компьютере (`pip install kafka-python`, затем `python scripts/kafka_producer.py`). Spark-скрипты запускаются через `docker exec spark spark-submit /home/jovyan/scripts/<скрипт>.py`, как описано в инструкциях.

Для `kafka_to_minio.py` нужно зарегистрировать Debezium-коннектор — инструкция прилагается.

«В терминале» в инструкциях означает: открой терминал в корневой папке проекта (там, где `docker-compose.yml`).

[← На главную](README.md)
