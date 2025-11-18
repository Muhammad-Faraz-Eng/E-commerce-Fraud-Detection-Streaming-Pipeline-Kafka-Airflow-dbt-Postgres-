FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python psycopg2-binary

# Copy consumer script
COPY consume_transactions.py /app/consume_transactions.py

# Default command
CMD ["python", "consume_transactions.py"]
