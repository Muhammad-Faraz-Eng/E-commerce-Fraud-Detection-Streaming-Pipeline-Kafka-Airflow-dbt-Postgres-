FROM python:3.10-slim

WORKDIR /app

# Install dbt-postgres
RUN pip install --no-cache-dir dbt-postgres psycopg2-binary

# Copy dbt project
COPY . /app

# Default CMD
CMD ["dbt", "run"]
