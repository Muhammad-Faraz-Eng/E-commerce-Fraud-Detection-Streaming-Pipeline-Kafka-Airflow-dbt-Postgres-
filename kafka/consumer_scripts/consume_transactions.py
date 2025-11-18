    import os
    import sys
    import json
    import psycopg2
    from datetime import datetime
    from kafka import KafkaConsumer

    # ------------------------------
    # Read Kafka + Postgres settings
    # ------------------------------
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
    TOPIC = os.environ.get("TOPIC", "transactions_raw")
    MAX_RECORDS = int(os.environ.get("MAX_RECORDS", 50))
    CONSUMER_TIMEOUT_MS = int(os.environ.get("CONSUMER_TIMEOUT_MS", 20000))
    GROUP_ID = os.environ.get("GROUP_ID", "consumer-group-1")
    AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")

    PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
    PG_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
    PG_DB = os.environ.get('POSTGRES_DB', 'fraud_db')
    PG_USER = os.environ.get('POSTGRES_USER', 'faraz')
    PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'Faraz123!')

    print(f"Starting Kafka consumer... (Limit: {MAX_RECORDS} records)")
    print(f"Connecting to Postgres at {PG_HOST}:{PG_PORT}/{PG_DB}")

    # ------------------------------
    # Connect Postgres
    # ------------------------------
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB,
                            user=PG_USER, password=PG_PASSWORD)
    cursor = conn.cursor()

    # ------------------------------
    # Kafka Consumer
    # ------------------------------
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS
    )

    print("Kafka consumer waiting for messages...")

    count = 0
    while count < MAX_RECORDS:
        batch = consumer.poll(timeout_ms=CONSUMER_TIMEOUT_MS)
        if not batch:
            print("No messages received within timeout, stopping consumer...")
            break

        for tp, messages in batch.items():
            for message in messages:
                msg = message.value
                try:
                    cursor.execute(
                        "INSERT INTO raw.transactions_stream (topic, message, received_at) VALUES (%s, %s, %s)",
                        (TOPIC, json.dumps(msg), datetime.utcnow())
                    )
                    conn.commit()
                    count += 1
                    print(f"[{count}/{MAX_RECORDS}] Inserted transaction: {msg.get('transaction_id')}")
                except Exception as e:
                    print(f"Insert error: {e}")
                    conn.rollback()

                if count >= MAX_RECORDS:
                    print("Reached MAX_RECORDS, stopping consumer...")
                    break
            if count >= MAX_RECORDS:
                break

    # ------------------------------
    # Clean up
    # ------------------------------
    cursor.close()
    conn.close()
    consumer.close()
    print("Kafka consumer finished successfully.")
    sys.exit(0)