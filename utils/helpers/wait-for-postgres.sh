#!/bin/bash
# Wait until Postgres is ready
set -e

host="$1"
shift
cmd="$@"

until pg_isready -h "$host" -U "$AIRFLOW_DB_USER"; do
  echo "Waiting for postgres at $host..."
  sleep 2
done

exec $cmd
