# Batch & Streaming Data Pipeline

Сквозной (end-to-end) проект по Data Engineering: **пакетная обработка данных, CDC-стриминг почти в реальном времени, оркестрация, трансформации и аналитическое хранилище**.

Проект объединяет PostgreSQL, Debezium, Apache Kafka, MinIO, Apache Spark, ClickHouse, Apache Airflow и dbt в одну платформу данных на Docker.

---

## Архитектура

```text
                        ┌────────────────────────────┐
                        │   PostgreSQL (postgres-src) │
                        │    исходная OLTP-база       │
                        └──────┬──────────────┬──────┘
                               │              │
        Пакетная ветка         │              │          CDC-ветка
  (полный снимок, по запросу)  │              │  (WAL → события, непрерывно)
                               ▼              ▼
                      Spark (pg_to_minio)   Debezium (Kafka Connect)
                               │              │
                               ▼              ▼
                     MinIO batch_output     Kafka-топик pg.public.orders
                               │              │
                               │              ▼
                               │     Spark Structured Streaming
                               │       (kafka_to_minio)
                               │              │
                               │              ▼
                               │     MinIO kafka_output (сырой JSON)
                               │              │
                               ▼              ▼
             Spark (minio_to_clickhouse)   Spark (minio_to_clickhouse_kafka)
                               │              │
                               ▼              ▼
                ClickHouse orders_batch    ClickHouse orders_cdc (ReplacingMergeTree)
                (TRUNCATE + INSERT)           └─► представление orders_current
                               │              │
                               └──────┬───────┘
                                      ▼
                                   Superset


   Оркестрируемые трансформации (DAG Airflow raw_to_mart_pipeline):

   generate_raw_data.py ─► PostgreSQL raw_orders ─► dbt stg_orders ─► dbt mart_daily_sales ─► dbt test ─► Superset
   («грязные» данные: дубли,   (view: очистка,          (таблица: продажи
    разные форматы дат)         дедупликация, типы)       по дням и категориям)
```

---

### Дашборд в Superset

![Дашборд Superset](docs/superset_dashboard.png)

---

## Обзор проекта

Проект показывает два основных подхода к обработке данных.

### Пакетная обработка (batch)

Spark забирает полный снимок таблицы `orders` из PostgreSQL, сохраняет его в Parquet в MinIO и загружает в ClickHouse. Так как каждый запуск даёт полный снимок, таблица в ClickHouse перезагружается через TRUNCATE + INSERT: повторный запуск идемпотентен, а строки, удалённые в источнике, исчезают и из ClickHouse.

```text
PostgreSQL → Spark → MinIO (Parquet) → Spark → ClickHouse orders_batch
```

### Стриминг / CDC

Debezium читает изменения PostgreSQL из WAL и публикует их в Kafka. Spark Structured Streaming сохраняет сырые события в MinIO; отдельная Spark-задача разбирает JSON и загружает его в таблицу `ReplacingMergeTree`, где версией строки служит LSN из Postgres, а удаления помечаются флагом `is_deleted`. Представление `orders_current` показывает текущее состояние исходной таблицы.

```text
PostgreSQL → Debezium → Kafka → Spark Structured Streaming → MinIO (сырой JSON)
           → Spark (разбор + версия) → ClickHouse orders_cdc → представление orders_current
```

### Оркестрируемые трансформации (Airflow + dbt)

DAG в Airflow генерирует в PostgreSQL намеренно «грязные» сырые данные и запускает поверх них модели dbt: staging-представление, которое очищает данные, приводит типы и убирает дубли, и витрину с ежедневными продажами по категориям, которая визуализируется в Superset. После каждого запуска тесты dbt проверяют ключи, допустимые значения и гранулярность витрины.

```text
Airflow: generate_raw_data → PostgreSQL raw_orders → dbt run (stg_orders, mart_daily_sales) → dbt test → Superset
```

Таким образом, в проекте есть пакетный ETL, событийный CDC и ELT на dbt.

---

## Технологии

| Технология          | Назначение                                              |
| ------------------- | ------------------------------------------------------- |
| **Python**          | Извлечение данных, продюсеры и скрипты пайплайнов       |
| **Apache Airflow**  | Оркестрация процессов                                   |
| **Apache Spark**    | Пакетные задачи и Structured Streaming (local mode)     |
| **Apache Kafka**    | Потоковая передача событий                              |
| **Debezium**        | Change Data Capture из PostgreSQL                       |
| **PostgreSQL**      | Исходная реляционная база                               |
| **MinIO**           | S3-совместимое объектное хранилище / слой data lake     |
| **ClickHouse**      | Аналитическая база данных                               |
| **dbt**             | SQL-трансформации и витрины данных                      |
| **Apache Superset** | Визуализация данных                                     |
| **Docker Compose**  | Локальная инфраструктура и управление сервисами         |
| **Git**             | Контроль версий                                         |

---

## Какие концепции Data Engineering показаны

* ETL / ELT
* Пакетная обработка
* Потоковая обработка данных
* Change Data Capture (CDC)
* Событийная (event-driven) архитектура
* Data lake / объектное хранилище
* Аналитические базы (ClickHouse MergeTree / ReplacingMergeTree)
* Пакетные задачи Spark и Spark Structured Streaming (local mode)
* Идемпотентные загрузки (полная перезагрузка снимка, версионные upsert по LSN)
* Оркестрация в Airflow (зависимости в DAG, повторы, пулы)
* SQL-трансформации в dbt (staging → mart)
* Тесты качества данных в dbt (generic и singular)
* Витрины данных
* Инфраструктура данных в Docker (профили Compose)
* S3-совместимое хранилище

---

## Компоненты пайплайна

### PostgreSQL

PostgreSQL — исходная OLTP-база.

Это начальный источник транзакционных данных и источник CDC-событий.

---

### Debezium

Debezium фиксирует изменения в PostgreSQL.

Вместо того чтобы периодически перечитывать всю таблицу, CDC получает изменения базы и публикует их как события в Kafka.

Схематично:

```text
INSERT / UPDATE / DELETE
          ↓
      PostgreSQL
          ↓
       Debezium
          ↓
         Kafka
```

Это событийный подход к интеграции данных.

---

### Apache Kafka

Kafka — слой потоковой передачи событий.

Она развязывает источники данных и потребителей ниже по потоку: они работают независимо друг от друга.

В проекте есть скрипты для отправки и обработки событий Kafka.

---

### MinIO

MinIO — S3-совместимое объектное хранилище.

Используется как локальный data lake / промежуточный слой для данных пайплайна.

Spark работает с MinIO через S3A-совместимую конфигурацию.

---

### Apache Spark

Spark выполняет пакетные задачи и задачу Structured Streaming. Задачи запускаются внутри контейнера `spark` в **local mode** (`local[*]`): отдельного Spark-кластера нет — для объёмов данных этого проекта этого достаточно, и стек помещается на ноутбук с 8 ГБ памяти. Spark UI запущенной задачи доступен по адресу http://localhost:4040.

В образ Spark включены коннекторы/JAR-файлы для интеграции с:

* Kafka
* PostgreSQL
* MinIO / S3
* ClickHouse

Все зависимости лежат в `spark/jars/` и копируются в `$SPARK_HOME/jars` при сборке образа (без `--packages`, см. [TROUBLESHOOTING](docs/TROUBLESHOOTING.md)).

---

### ClickHouse

ClickHouse — слой аналитического хранения.

Обе ветки приходят в ClickHouse: `orders_batch` (MergeTree, полная перезагрузка) и `orders_cdc` (`ReplacingMergeTree(version, is_deleted)`, дедупликация по LSN из Postgres) с представлением `orders_current` поверх.

Так аналитическая нагрузка отделена от транзакционного источника PostgreSQL.

---

### dbt

dbt выполняет SQL-трансформации сырых данных в PostgreSQL (`raw_orders`). Адаптер для ClickHouse установлен, поэтому модели позже можно перенести в ClickHouse.

* `sources.yml` — сырая таблица `raw_orders` объявлена как источник dbt (`{{ source('raw', 'raw_orders') }}`)
* `stg_orders` (view) — приведение типов, нормализация трёх форматов дат, дедупликация по `order_id`
* `mart_daily_sales` (table) — ежедневные продажи по категориям
* тесты — `unique` / `not_null` / `accepted_values` в `schema.yml` и singular-тесты в `dbt/tests/` (гранулярность витрины, положительные суммы); `dbt test` запускается отдельной задачей в DAG Airflow

Слой трансформаций построен по схеме staging → mart:

```text
Сырые данные
   ↓
staging-модели
   ↓
аналитические модели
   ↓
витрины данных
```

---

### Apache Airflow

Airflow отвечает за оркестрацию пайплайнов.

DAG `raw_to_mart_pipeline` запускается ежедневно:

```text
generate_raw_data (docker exec spark python ...)
        ↓
dbt_run  (docker exec dbt dbt run)
        ↓
dbt_test (docker exec dbt dbt test)
```

Ещё два DAG загружают пакетные и CDC-данные в ClickHouse — см. раздел [Оркестрация](#оркестрация).

Задачи обращаются к другим контейнерам через Docker-сокет, подключённый к scheduler.

Airflow даёт расписание, управление зависимостями и отслеживание запусков.

---

### Apache Superset

Superset — слой BI / визуализации.

Итоговые аналитические данные можно показать на дашбордах и графиках.

Таким образом, архитектура покрывает весь путь данных:

```text
Источник
  ↓
Загрузка (ingestion)
  ↓
Хранение
  ↓
Обработка
  ↓
Трансформация
  ↓
Витрина данных
  ↓
BI
```

---

# Структура репозитория

```text
batch-streaming-data-pipeline/
│
├── dags/
│   ├── batch_pg_to_clickhouse.py
│   ├── cdc_minio_to_clickhouse.py
│   └── raw_to_mart_pipeline.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   └── stg_orders.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       └── mart_daily_sales.sql
│   └── tests/
│       ├── assert_amount_positive.sql
│       └── assert_mart_daily_sales_grain.sql
│
├── scripts/
│   ├── generate_raw_data.py
│   ├── kafka_producer.py
│   ├── kafka_to_minio.py
│   ├── minio_to_clickhouse.py
│   ├── minio_to_clickhouse_kafka.py
│   └── pg_to_minio.py
│
├── spark/
│   ├── conf/
│   ├── jars/
│   ├── Dockerfile.spark
│   └── requirements.txt
│
├── docs/
│   ├── TROUBLESHOOTING.md
│   └── superset_dashboard.png
│
├── ИНСТРУКЦИИ/
│   ├── активация superset.txt
│   ├── запуск пайплайнов.txt
│   └── регистрация debezium connector'а.txt
│
├── .env.example
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.dbt
├── Dockerfile.superset
├── pg-connector.json
├── requirements.txt
└── Training_instruction.md
```

---

# Запуск проекта

## Что нужно заранее

* Docker + Docker Compose v2
* Git
* **Память:** полному стеку нужно около 8 ГБ RAM для Docker. Сервисы разделены на профили Compose, поэтому на ноутбуке с 8 ГБ стек можно запускать частями (см. ниже). В Windows/WSL2 Docker по умолчанию получает только половину памяти — задай `memory` и `swap` в `%UserProfile%\.wslconfig` (см. [Training_instruction.md](Training_instruction.md)).
* Git Bash в Windows: `export MSYS_NO_PATHCONV=1` (иначе пути в командах `docker exec` искажаются)

Склонируй репозиторий:

```bash
git clone https://github.com/dataenglab/batch-streaming-data-pipeline.git
cd batch-streaming-data-pipeline
```

Скачай JAR AWS SDK bundle (~300 МБ, в Git не хранится) в `spark/jars/` — он нужен для сборки образов Spark и Airflow:

```bash
curl -L -o spark/jars/aws-java-sdk-bundle-1.12.526.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.526/aws-java-sdk-bundle-1.12.526.jar
```

Создай локальный файл окружения (он исключён из Git) и заполни `SUPERSET_SECRET_KEY` и `AIRFLOW_FERNET_KEY` (команда для генерации есть в `.env.example`):

```bash
cp .env.example .env
```

---

## Запуск инфраструктуры

Сервисы сгруппированы в профили Compose:

| Профиль | Сервисы | Для чего |
| --- | --- | --- |
| *(без профиля)* | postgres-src, minio, clickhouse, spark | пакетная ветка |
| `streaming` | zookeeper, kafka, debezium, kafka-ui | CDC-ветка |
| `orchestration` | airflow (init, api-server, scheduler, dag-processor), postgres-airflow, dbt | Airflow + dbt |
| `bi` | superset | дашборды |
| `dev` | jupyter | исследование данных |

```bash
docker compose up -d --build                    # только ядро
docker compose --profile streaming up -d        # ядро + CDC
docker compose --profile orchestration up -d    # ядро + Airflow/dbt
docker compose --profile bi up -d               # ядро + Superset

# всё сразу (нужно 8+ ГБ для Docker)
docker compose --profile streaming --profile orchestration --profile bi --profile dev up -d

docker compose ps
```

## Адреса сервисов

| Сервис | URL | Доступ |
| --- | --- | --- |
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8082 | — |
| Kafka Connect (Debezium) REST | http://localhost:8083 | — |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Spark UI (пока задача выполняется) | http://localhost:4040 (вторая задача: 4041) | — |
| ClickHouse HTTP | http://localhost:8123 | default / (пустой пароль) |
| Superset | http://localhost:8088 | admin / admin (см. инструкции) |
| Jupyter | http://localhost:8889 | token: admin |

Пошаговые инструкции по запуску пайплайнов, регистрации коннектора Debezium и активации Superset — в [`Training_instruction.md`](Training_instruction.md) и в папке [`ИНСТРУКЦИИ/`](ИНСТРУКЦИИ/).

---

# Поток данных

Пошаговые команды: [`ИНСТРУКЦИИ/запуск пайплайнов.txt`](ИНСТРУКЦИИ/запуск%20пайплайнов.txt).

## Пакетная ветка

| Шаг | Скрипт | Результат |
| --- | --- | --- |
| Извлечение | `pg_to_minio.py` | полный снимок `orders` → `s3a://bucket/batch_output` (Parquet, перезапись) |
| Загрузка | `minio_to_clickhouse.py` | снимок → ClickHouse `orders_batch` (TRUNCATE + INSERT) |

## CDC-ветка

| Шаг | Компонент | Результат |
| --- | --- | --- |
| Захват | Debezium (`pg-connector.json`) | изменения строк → топик `pg.public.orders`; `ExtractNewRecordState`, удаления переписываются с полем `__deleted`, дополнительные поля `__op`, `__lsn`, `__source_ts_ms` |
| Приземление | `kafka_to_minio.py` (Structured Streaming) | сырые JSON-события → `s3a://bucket/kafka_output`, checkpoint в `s3a://bucket/checkpoints/kafka_orders` |
| Загрузка | `minio_to_clickhouse_kafka.py` | разобранные события → `orders_cdc` (`ReplacingMergeTree(version, is_deleted)`, version = LSN) |
| Выдача | представление ClickHouse `orders_current` | `FINAL` + `is_deleted = 0` → текущее состояние исходной таблицы |

Kafka хранит историю изменений, а не текущее состояние, а Debezium заново делает снимок таблицы при повторной регистрации коннектора. Версионирование по LSN делает загрузку идемпотентной: повторное воспроизведение тех же событий даёт тот же результат.

---

# Моделирование данных

Слой dbt состоит из staging-моделей и витрин.

### Staging

`stg_orders.sql`

Staging-слой готовит сырые данные из источника к аналитическим трансформациям.

Типичные задачи:

* переименование колонок
* приведение типов
* базовая очистка
* стандартизация
* подготовка данных для следующих моделей

### Витрина (mart)

`mart_daily_sales.sql`

Слой витрин содержит бизнес-ориентированные аналитические данные, подготовленные для отчётности.

Общая схема:

```text
Исходные данные
     ↓
Staging
     ↓
Бизнес-трансформации
     ↓
Аналитическая витрина
     ↓
BI / аналитика
```

---

# Оркестрация

| DAG | Расписание | Задачи |
| --- | --- | --- |
| `raw_to_mart_pipeline` | ежедневно | `generate_raw_data` → `dbt_run` → `dbt_test` (сырые данные Postgres → staging → витрина → тесты качества) |
| `batch_pg_to_clickhouse` | ежедневно | `pg_to_minio` → `minio_to_clickhouse` |
| `cdc_minio_to_clickhouse` | ежечасно | `minio_to_clickhouse_cdc` → `check_current_state` |

Задачи выполняют `docker exec` в контейнеры `spark`, `dbt` и `clickhouse` через Docker-сокет, подключённый к scheduler. Все Spark-задачи используют общий пул Airflow `spark` с одним слотом (создаётся в `airflow-init`), поэтому одновременно работает только один Spark-драйвер — стек рассчитан на ноутбук с 8 ГБ. Сам CDC-стрим (`kafka_to_minio.py`) — долгоживущий процесс, он запускается вне Airflow; ежечасный DAG переносит накопленные события в ClickHouse.

---

# Зачем этот проект

Проект создан как практическое обучение Data Engineering с упором на понимание того, как отдельные компоненты работают вместе как единая платформа данных.

Основные учебные цели:

* проектирование сквозных пайплайнов данных
* работа с пакетной и потоковой архитектурами
* понимание CDC
* интеграция Kafka и Debezium
* обработка данных в Spark
* работа с объектным хранилищем
* загрузка данных в аналитические базы
* оркестрация процессов в Airflow
* трансформация данных с помощью dbt
* построение аналитического слоя для BI

---

# Продемонстрированные навыки

### Data Engineering

* ETL / ELT
* Пакетная обработка
* Стриминг
* CDC
* Загрузка данных (ingestion)
* Трансформация данных
* Оркестрация данных
* Концепции хранилищ данных (DWH)
* Концепции data lake

### Технологии

* Python
* SQL
* PostgreSQL
* Apache Kafka
* Debezium
* Apache Spark
* Apache Airflow
* MinIO
* ClickHouse
* dbt
* Apache Superset
* Docker

### Инженерные практики

* Среда разработки в Docker
* Интеграция сервисов
* Управление конфигурацией
* Управление зависимостями
* Git / GitHub
* Диагностика проблем в пайплайнах

---

# Статус проекта

Проект задуман как локальная среда Data Engineering на Docker.

Сейчас реализованы:

* исходная база PostgreSQL
* Kafka
* Debezium CDC
* MinIO
* Spark
* ClickHouse
* Airflow
* dbt
* Superset
* компоненты пакетного и потокового пайплайнов

Все части проверены от начала до конца на чистом клоне репозитория. Проблемы, встреченные по пути, и их решения собраны в [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

Возможные улучшения:

* запуск CDC-стрима как управляемого сервиса вместо ручного `spark-submit`
* модели dbt поверх ClickHouse
* больше тестов dbt (relationships, source freshness) и проверки качества данных на стороне ClickHouse
* многоузловой Spark-кластер вместо local mode
* фиксированные версии Docker-образов вместо `:latest`
* инкрементальные модели dbt
* обработка эволюции схемы
* мониторинг и алерты
* CI/CD
* развёртывание в облаке
* инфраструктура как код
* дополнительные аналитические витрины

---

## Автор

**Kamilla**

Портфолио-проект по Data Engineering:

**Python · SQL · Spark · Airflow · Kafka · Debezium · PostgreSQL · ClickHouse · MinIO · dbt · Docker**
