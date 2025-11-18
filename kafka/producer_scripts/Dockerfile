FROM python:3.10-slim

WORKDIR /app


# Install dependencies
RUN pip install --no-cache-dir kafka-python faker psycopg2-binary

# Copy producer script
COPY produce_transactions.py /app/produce_transactions.py


# Run script
CMD ["python", "-u", "produce_transactions.py"]

