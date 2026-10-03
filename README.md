# Batch & Streaming Data Pipeline

End-to-end data engineering project demonstrating **batch data processing, real-time CDC streaming, orchestration, data transformation and analytical storage**.

The project combines PostgreSQL, Debezium, Apache Kafka, MinIO, Apache Spark, ClickHouse, Apache Airflow and dbt into a Docker-based data platform.

---

## Architecture

```text
                         ┌──────────────────────┐
                         │      PostgreSQL       │
                         │     Source DB         │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
               Batch flow                       CDC flow
                    │                               │
                    ▼                               ▼
              Python / Spark                  Debezium
                    │                               │
                    ▼                               ▼
                  MinIO                         Kafka
                    │                               │
                    ▼                               ▼
                ClickHouse                 Kafka → MinIO
                    │                               │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
                               ClickHouse
                                    │
                                    ▼
                                  dbt
                                    │
                                    ▼
                              Data Marts
                                    │
                                    ▼
                                Superset


                    Apache Airflow
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
          Spark       Python        dbt
```

---

## Project Overview

The project demonstrates two main data processing approaches:

### Batch processing

Data is extracted from PostgreSQL and loaded into object storage and analytical storage.

```text
PostgreSQL
    ↓
Python / Spark
    ↓
MinIO
    ↓
ClickHouse
    ↓
dbt
    ↓
Data Mart
```

### Streaming / CDC

Changes in PostgreSQL are captured using Debezium and published to Kafka.

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Processing
    ↓
MinIO / ClickHouse
```

This allows the project to demonstrate both traditional batch ETL/ELT and event-driven CDC pipelines.

---

## Tech Stack

| Technology          | Purpose                                         |
| ------------------- | ----------------------------------------------- |
| **Python**          | Data extraction, producers and pipeline scripts |
| **Apache Airflow**  | Workflow orchestration                          |
| **Apache Spark**    | Distributed data processing                     |
| **Apache Kafka**    | Event streaming and message transport           |
| **Debezium**        | Change Data Capture from PostgreSQL             |
| **PostgreSQL**      | Source relational database                      |
| **MinIO**           | S3-compatible object storage / data lake layer  |
| **ClickHouse**      | Analytical database                             |
| **dbt**             | SQL transformations and data marts              |
| **Apache Superset** | Data visualization                              |
| **Docker Compose**  | Local infrastructure and service orchestration  |
| **Git**             | Version control                                 |

---

## Data Engineering Concepts Demonstrated

This project covers several practical Data Engineering concepts:

* ETL / ELT
* Batch processing
* Streaming data processing
* Change Data Capture (CDC)
* Event-driven architecture
* Data lake / object storage
* Analytical databases
* Distributed processing with Spark
* Workflow orchestration
* SQL transformations
* Data marts
* Dockerized data infrastructure
* Kafka producers and consumers
* PostgreSQL integration
* S3-compatible storage
* Monitoring and pipeline execution

---

## Pipeline Components

### PostgreSQL

PostgreSQL is used as the source OLTP database.

The project uses it as the initial source of transactional data and as the source for CDC events.

---

### Debezium

Debezium is used to capture changes made to PostgreSQL.

Instead of periodically querying the entire source table, CDC captures database changes and publishes events to Kafka.

Conceptually:

```text
INSERT / UPDATE / DELETE
          ↓
      PostgreSQL
          ↓
       Debezium
          ↓
         Kafka
```

This demonstrates an event-driven approach to data integration.

---

### Apache Kafka

Kafka acts as the event streaming layer.

It provides a decoupled communication layer between data producers and downstream consumers.

The project contains Kafka-related scripts for producing and processing events.

---

### MinIO

MinIO provides S3-compatible object storage.

It is used as a local data lake / staging layer for pipeline data.

The project uses Spark and S3A-compatible configuration to interact with MinIO.

---

### Apache Spark

Spark is used for distributed data processing.

The project includes Spark configuration and required connectors/JARs for integration with:

* Kafka
* PostgreSQL
* MinIO / S3
* ClickHouse

Spark is used where distributed processing and integration with the data platform are required.

---

### ClickHouse

ClickHouse is used as the analytical storage layer.

Processed data can be loaded into ClickHouse and queried for analytical workloads.

This separates the analytical workload from the transactional PostgreSQL source.

---

### dbt

dbt is used for SQL-based transformations.

The project contains:

```text
dbt/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── staging/
    │   └── stg_orders.sql
    └── marts/
        └── mart_daily_sales.sql
```

The transformation layer follows a staging → mart approach:

```text
Raw data
   ↓
staging models
   ↓
analytical models
   ↓
data marts
```

---

### Apache Airflow

Airflow is used for pipeline orchestration.

The repository contains DAGs responsible for coordinating data processing tasks.

Example high-level workflow:

```text
Extract
   ↓
Process
   ↓
Load
   ↓
Transform
   ↓
Data Mart
```

Airflow provides scheduling, dependency management and execution tracking.

---

### Apache Superset

Superset is used as the BI / visualization layer.

The final analytical data can be exposed through dashboards and charts.

The complete architecture therefore covers the path:

```text
Source
  ↓
Ingestion
  ↓
Storage
  ↓
Processing
  ↓
Transformation
  ↓
Data Mart
  ↓
BI
```

---

# Repository Structure

```text
batch-streaming-data-pipeline/
│
├── dags/
│   ├── raw_to_mart_pipeline.py
│   └── test_dag.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_orders.sql
│       └── marts/
│           └── mart_daily_sales.sql
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
├── config/
│   ├── jupyter_notebook_config.py
│   └── superset_config.py
│
├── ИНСТРУКЦИИ/
│   ├── активация superset.txt
│   ├── запуск пайплайнов.txt
│   └── регистрация debezium connector'а.txt
│
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.dbt
├── Dockerfile.superset
├── pg-connector.json
├── requirements.txt
└── Training_instruction.md
```

---

# Running the Project

## Prerequisites

The project requires:

* Docker
* Docker Compose
* Git
* At least 8 GB RAM available for Docker
* Internet connection for downloading Docker images and dependencies

Clone the repository:

```bash
git clone https://github.com/<YOUR_USERNAME>/batch-streaming-data-pipeline.git
cd batch-streaming-data-pipeline
```

Create the local environment file:

```bash
cp .env.example .env
```

Set the required local secrets in `.env`.

The `.env` file is intentionally excluded from Git.

---

## Start the infrastructure

Build the required images:

```bash
docker compose build
```

Start the services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

---

# Data Flow

## Batch pipeline

The batch flow demonstrates periodic data ingestion and transformation.

```text
PostgreSQL
     ↓
Python / Spark
     ↓
MinIO
     ↓
ClickHouse
     ↓
dbt
     ↓
Data Mart
```

The resulting analytical data can then be consumed by BI tools such as Superset.

---

## CDC pipeline

The streaming flow demonstrates Change Data Capture.

```text
PostgreSQL
     ↓
Debezium
     ↓
Kafka
     ↓
Consumer / Processing
     ↓
MinIO / ClickHouse
```

This approach avoids repeatedly extracting the entire source dataset and instead processes database changes as events.

---

# Data Modeling

The dbt layer contains staging and mart models.

### Staging

`stg_orders.sql`

The staging layer is responsible for preparing raw source data for analytical transformations.

Typical responsibilities include:

* renaming columns
* type conversion
* basic cleaning
* standardization
* preparing data for downstream models

### Mart

`mart_daily_sales.sql`

The mart layer contains business-oriented analytical data prepared for reporting.

The general concept is:

```text
Source data
     ↓
Staging
     ↓
Business transformations
     ↓
Analytical Mart
     ↓
BI / Analytics
```

---

# Orchestration

Airflow manages dependencies between pipeline tasks.

For example:

```text
Raw ingestion
      ↓
Data processing
      ↓
Loading
      ↓
dbt transformations
      ↓
Data mart
```

This allows individual pipeline stages to be monitored and rerun independently.

---

# Why This Project

The project was built as practical Data Engineering training with a focus on understanding how individual components work together as a complete data platform.

The main learning objectives were:

* designing end-to-end data pipelines
* working with batch and streaming architectures
* understanding CDC
* integrating Kafka and Debezium
* processing data with Spark
* working with object storage
* loading data into analytical databases
* orchestrating workflows with Airflow
* transforming data using dbt
* building an analytical layer for BI

---

# Skills Demonstrated

### Data Engineering

* ETL / ELT
* Batch processing
* Streaming
* CDC
* Data ingestion
* Data transformation
* Data orchestration
* Data warehousing concepts
* Data lake concepts

### Technologies

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

### Engineering

* Dockerized development environment
* Service integration
* Configuration management
* Dependency management
* Git / GitHub
* Pipeline troubleshooting

---

# Project Status

The project is designed as a local Docker-based Data Engineering environment.

Current implementation includes:

* PostgreSQL source database
* Kafka
* Debezium CDC
* MinIO
* Spark
* ClickHouse
* Airflow
* dbt
* Superset
* Batch and streaming pipeline components

Further improvements can include:

* automated data quality checks
* additional dbt tests
* incremental dbt models
* schema evolution handling
* monitoring and alerting
* CI/CD
* production cloud deployment
* infrastructure-as-code
* additional analytical marts

---

## Author

**Kamilla**

Data Engineering portfolio project focused on:

**Python · SQL · Spark · Airflow · Kafka · Debezium · PostgreSQL · ClickHouse · MinIO · dbt · Docker**
