E-commerce Fraud Detection System/
│
├─ airflow/                     # Airflow main folder
│   ├─ dags/                    # Your DAGs go here
│   ├─ logs/                    # Airflow logs (volume mapped)
│   └─ plugins/                 # Custom operators, sensors, hooks
│
├─ kafka/                       # Kafka + Zookeeper folder
│   ├─ zookeeper_data/          # Zookeeper snapshot data (persistent)
│   ├─ zookeeper_datalog/       # Zookeeper transaction logs (persistent)
│   ├─ producer_scripts/        # Kafka producer Python scripts
│   └─ consumer_scripts/        # Kafka consumer Python scripts
│
├─ postgres/                    # Postgres DB scripts
│   └─ init.sql                 # Create databases, schemas, tables, users
│
├─ dbt/                         # dbt project folder
│   ├─ models/                  # Your dbt models
│   ├─ snapshots/               # Optional: dbt snapshots
│   ├─ macros/                  # dbt macros
│   └─ dbt_project.yml          # dbt project config
│
├─ utils/                       # Helper scripts / config
│   ├─ config/                  # Kafka, DB, Airflow configs
│   └─ helpers/                 # Reusable Python functions
│
├─ docker-compose.yml           # Main Docker compose file
├─ .env                         # Environment variables (passwords, ports)
└─ README.md                    # Project documentation












| Service Name                  | Docker Image / Notes                                               | Purpose / Role                                                                                                                                                    |
| ----------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Postgres (Raw + Metadata)** | `postgres:15`                                                      | Store your transactional data (raw, staged, curated) and Airflow metadata. We’ll have **two databases**: one for Airflow metadata and logs, one for project data. |
| **Zookeeper**                 | `zookeeper:3.8.0`                                                  | Kafka depends on Zookeeper to manage brokers. Handles leader election, cluster metadata, and coordination.                                                        |
| **Kafka Broker**              | `confluentinc/cp-kafka:7.4.1`                                      | Central message broker. Producers push events (transactions), consumers read events (to Postgres or staging).                                                     |
| **Airflow Webserver**         | `apache/airflow:2.7.1`                                             | Web UI for managing DAGs, monitoring workflows, triggering tasks.                                                                                                 |
| **Airflow Scheduler**         | `apache/airflow:2.7.1`                                             | Orchestrates DAGs and schedules tasks.                                                                                                                            |
| **Airflow Worker**            | `apache/airflow:2.7.1`                                             | Executes Python tasks (Kafka producer/consumer scripts, dbt runs).                                                                                                |
| **Airflow Triggerer**         | `apache/airflow:2.7.1`                                             | Handles deferrable operators and triggers. Needed for modern Airflow DAGs.                                                                                        |
| **Airflow Init**              | `apache/airflow:2.7.1`                                             | Initializes Airflow DB and directories on first startup.                                                                                                          |
| **Kafka Producer**            | Python script / Docker service                                     | Generates live e-commerce transaction events to Kafka topics.                                                                                                     |
| **Kafka Consumer**            | Python script / Docker service                                     | Reads events from Kafka topics and writes to Postgres staging/raw tables.                                                                                         |
| **dbt (Data Build Tool)**     | `fishtownanalytics/dbt:1.6.4` (or python image with dbt installed) | Transforms and models raw/staged data into curated tables using dbt workflow.                                                                                     |
