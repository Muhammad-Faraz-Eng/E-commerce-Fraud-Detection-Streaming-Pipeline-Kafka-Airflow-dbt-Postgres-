-- 1️⃣ Create Airflow user
CREATE USER airflow_user WITH ENCRYPTED PASSWORD '<placeholder>';

-- 2️⃣ Create Airflow DB owned by airflow_user
CREATE DATABASE airflow
    WITH 
    OWNER = airflow_user
    ENCODING = 'UTF8'
    TEMPLATE = template0;

-- 3️⃣ Connect to airflow DB
\connect airflow;

-- 4️⃣ Grant schema permissions
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;

GRANT USAGE ON SCHEMA public TO airflow_user;
GRANT CREATE ON SCHEMA public TO airflow_user;
ALTER SCHEMA public OWNER TO airflow_user;
