# E-commerce Fraud Detection System

A production-style streaming data pipeline for detecting and analyzing e-commerce fraud. It ingests events via Kafka, orchestrates tasks with Airflow, transforms data using dbt, and stores results in Postgres.

## Overview

- Real-time ingestion: Kafka producer emits transaction events to topic `transactions_raw`; Kafka consumer writes messages into Postgres `raw.transactions_stream`.
- Orchestration: Airflow DAG `kafka_streaming_pipeline` runs consumer, checks raw data, waits for DB readiness, and executes dbt models for `raw`, `staging`, and `marts` layers.
- Transformations: dbt models flatten JSON payloads, build staging tables, and materialize dimensional and fact tables.
- Analytics: `vw_customer_fraud_summary` summarizes customer activity and fraud scores.

## Architecture

- Kafka producer → Kafka broker → Airflow-launched consumer → Postgres `raw` schema
- dbt `raw` models → dbt `staging` models (incremental) → dbt `marts` (dims + fact + view)
- Airflow tasks run as Docker containers on network `e-commercefrauddetectionsystem_infra_net`

## Prerequisites

- Docker and Docker Compose
- Ports available: Postgres `5432`, Kafka `9092`/`29092`, Airflow Webserver `8080`
- Environment variables in `.env` (examples):
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
  - `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_HOST`, `AIRFLOW_DB_PORT`, `AIRFLOW_DB_NAME`

## Quick Start

1. Build and start services:
   - `docker compose up -d --build`
2. Open Airflow UI:
   - `http://localhost:8080` (default)
3. Trigger the DAG:
   - `kafka_streaming_pipeline` (latest version id, e.g., `kafka_streaming_pipeline_v_1.31`)
4. Monitor task logs in Airflow; verify inserts into Postgres.

## Services

- `postgres`: database `fraud_db`; schemas `raw`, `staging`, `marts`
- `zookeeper` + `kafka`: broker reachable at `kafka:9092` (inside network) and `localhost:29092` (host)
- `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`: orchestrate DAG
- `kafka-producer` and `kafka-consumer` images for event flow

## Airflow DAG

Pipeline steps:
- `run_kafka_consumer`: Reads from Kafka (`transactions_raw`) and writes JSON into `raw.transactions_stream`
- `check_raw_transactions`: Validates row count > 0
- `wait_for_postgres`: Retries until DB is reachable
- `dbt run --select raw`: Materializes raw layer
- `dbt run --select staging`: Builds incremental staging tables
- `dbt run --select marts`: Builds dims and fact, plus summary view

All DockerOperator tasks use `network_mode=e-commercefrauddetectionsystem_infra_net` and pass necessary environment variables.

## dbt Project

- `models/raw`:
  - `raw_transactions` (table) sourced from `raw.transactions_stream`
- `models/staging` (incremental):
  - `stg_transactions`, `stg_customers`, `stg_devices`, `stg_merchants`
- `models/marts` (incremental):
  - `dim_customer`, `dim_device`, `dim_merchant`, `fct_transactions`
  - `vw_customer_fraud_summary` (view)

Schema mapping:
- `raw` → models in `models/raw`
- `staging` → models in `models/staging`
- `marts` → models in `models/marts`

## Running dbt Manually

- Install packages and run layers:
  - `dbt deps --project-dir dbt/dbt_project`
  - `dbt run --project-dir dbt/dbt_project --profiles-dir dbt/dbt_project --select raw`
  - `dbt run --project-dir dbt/dbt_project --profiles-dir dbt/dbt_project --select staging`
  - `dbt run --project-dir dbt/dbt_project --profiles-dir dbt/dbt_project --select marts`
- First-time incremental setup: consider `--full-refresh` for staging/marts.

## Data Model

- `raw.transactions_stream`: topic, message JSONB, received_at
- Raw flattened table: `raw_transactions`
- Staging: `stg_*` tables keyed on transaction/customer/device/merchant, with `received_at` for incremental filters
- Marts: `dim_*` and `fct_transactions` (joins on `customer_id`, `device_id`, `merchant_id`); `vw_customer_fraud_summary`

## Troubleshooting

- No messages consumed:
  - Ensure unique `GROUP_ID` for consumer; use `AUTO_OFFSET_RESET=latest`; verify topic `transactions_raw` and broker `kafka:9092`
- Name resolution errors (`postgres` not found):
  - Ensure all containers use network `e-commercefrauddetectionsystem_infra_net`; Airflow tasks must set `network_mode` accordingly
- dbt type mismatches in joins:
  - Align IDs to text in joins or standardize types across staging and dims
- Incremental filters failing:
  - Confirm `received_at` exists on targets; perform `--full-refresh` once to initialize

## Project Structure

```
E-commerce Fraud Detection System/
├─ airflow/
│  ├─ dags/
│  ├─ logs/
│  └─ plugins/
├─ kafka/
│  ├─ producer_scripts/
│  └─ consumer_scripts/
├─ postgres/
│  ├─ create_airflow_db.sql
│  ├─ create_ecommerce_db.sql
│  └─ data/
├─ dbt/
│  └─ dbt_project/
│     ├─ models/
│     ├─ macros/
│     ├─ profiles.yml
│     └─ dbt_project.yml
├─ utils/
│  └─ helpers/
├─ docker-compose.yml
└─ README.md
```

## License

MIT (or your preferred license).