import os
import logging
import psycopg2
import time
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

# ---------------------------
# Default DAG arguments
# ---------------------------
default_args = {
    'owner': 'Faraz',
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

# ---------------------------
# Centralized Config
# ---------------------------
POSTGRES_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
    'database': os.environ.get('POSTGRES_DB', 'fraud_db'),
    'user': os.environ.get('POSTGRES_USER', '<placeholder>'),
    'password': os.environ.get('POSTGRES_PASSWORD', '<placeholder>'),
}

KAFKA_CONFIG = {
    'broker': os.environ.get('KAFKA_BROKER', 'kafka:9092'),
    'topic': os.environ.get('KAFKA_TOPIC', 'transactions_raw'),
}

DBT_CONFIG = {
    'project_dir': '/app/dbt_project',
    'profile_dir': '/app/dbt_project',
}

DOCKER_NETWORK = os.environ.get('DOCKER_NETWORK', 'e-commercefrauddetectionsystem_infra_net')

# ---------------------------
# Define DAG
# ---------------------------
with DAG(
    dag_id='kafka_streaming_pipeline_v_1.31',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['ecommerce', 'fraud', 'streaming']
) as dag:

    # ---------------------------
    # Kafka Consumer task
    # ---------------------------
    kafka_consumer = DockerOperator(
        task_id='run_kafka_consumer',
        image='e-commercefrauddetectionsystem-kafka-consumer',
        api_version='auto',
        auto_remove=True,
        command='python consume_transactions.py',
        docker_url='unix://var/run/docker.sock',
        network_mode=DOCKER_NETWORK,
        environment={
            'KAFKA_BROKER': KAFKA_CONFIG['broker'],
            'TOPIC': KAFKA_CONFIG['topic'],
            'GROUP_ID': 'airflow-consumer-raw',
            'AUTO_OFFSET_RESET': 'latest',
            'MAX_RECORDS': "100", 
            'CONSUMER_TIMEOUT_MS': "20000",
            **POSTGRES_CONFIG
        },
        mount_tmp_dir=False,
        tty=True,
        do_xcom_push=False
    )

    # ---------------------------
    # Python task to check Postgres table
    # ---------------------------
    def check_raw_table():
        try:
            conn = psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password']
            )
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM raw.transactions_stream;")
            count = cursor.fetchone()[0]
            logging.info(f"Number of transactions in raw.transactions_stream: {count}")

            cursor.close()
            conn.close()

            if count == 0:
                raise ValueError("No data found in raw.transactions_stream!")

        except Exception as e:
            logging.error(f"Error checking raw.transactions_stream: {e}")
            raise

    check_raw = PythonOperator(
        task_id='check_raw_transactions',
        python_callable=check_raw_table
    )

    def wait_for_postgres():
        attempts = 0
        while attempts < 24:
            try:
                conn = psycopg2.connect(
                    host=POSTGRES_CONFIG['host'],
                    port=POSTGRES_CONFIG['port'],
                    database=POSTGRES_CONFIG['database'],
                    user=POSTGRES_CONFIG['user'],
                    password=POSTGRES_CONFIG['password']
                )
                conn.close()
                return
            except Exception as e:
                logging.warning(f"Postgres not reachable: {e}")
                time.sleep(5)
                attempts += 1
        raise RuntimeError("Postgres not reachable after retries")

    wait_pg = PythonOperator(
        task_id='wait_for_postgres',
        python_callable=wait_for_postgres
    )

    # ---------------------------
    # DBT: Bronze Layer (raw)
    # ---------------------------
    dbt_run_raw = DockerOperator(
        task_id='dbt_run_raw',
        image='e-commercefrauddetectionsystem-dbt',
        api_version='auto',
        auto_remove=True,
        command='bash -lc "dbt deps --project-dir /app/dbt_project && dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select raw"',
        docker_url='unix://var/run/docker.sock',
        network_mode=DOCKER_NETWORK,
        environment={
            'DBT_PROFILES_DIR': DBT_CONFIG["profile_dir"]
        },
        mount_tmp_dir=False,
        tty=True,
        do_xcom_push=False
    )

    # ---------------------------
    # DBT: Staging Layer (stg) - incremental
    # ---------------------------
    dbt_run_staging= DockerOperator(
        task_id='dbt_run_staging',
        image='e-commercefrauddetectionsystem-dbt',
        api_version='auto',
        auto_remove= True,
        command='bash -lc "dbt deps --project-dir /app/dbt_project && dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select staging"',
        network_mode=DOCKER_NETWORK,
        environment={
            'DBT_PROFILES_DIR': DBT_CONFIG['profile_dir']
        },
        mount_tmp_dir=False,
        tty=True,
        do_xcom_push=False
    )

    # ---------------------------
    # DBT: Marts Layer (dim + fct) - incremental + SCD
    # ---------------------------
    dbt_run_marts= DockerOperator(
        task_id='run_dbt_marts',
        image='e-commercefrauddetectionsystem-dbt',
        api_version='auto',
        auto_remove=True,
        command='bash -lc "dbt deps --project-dir /app/dbt_project && dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select marts"',
        docker_url='unix://var/run/docker.sock',
        network_mode=DOCKER_NETWORK,
        environment={
            'DBT_PROFILES_DIR': DBT_CONFIG['profile_dir']
        },
        mount_tmp_dir=False,
        tty=True,
        do_xcom_push=False
    )




    # ---------------------------
    # DAG dependencies
    # ---------------------------
    kafka_consumer >> check_raw >> wait_pg >> dbt_run_raw >> dbt_run_staging >> dbt_run_marts
