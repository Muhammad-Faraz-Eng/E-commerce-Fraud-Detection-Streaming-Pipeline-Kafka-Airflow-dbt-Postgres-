import json
import time
from kafka import KafkaProducer
from faker import Faker
import random
from datetime import datetime

fake = Faker()
import os
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC = os.environ.get('TOPIC', 'transactions_raw')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": fake.random_int(min=1000, max=9999),
        "merchant_id": fake.random_int(min=100, max=999),
        "transaction_amount": round(random.uniform(5.0, 5000.0), 2),
        "currency": "USD",
        "timestamp": datetime.utcnow().isoformat(),
        "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
        "transaction_status": random.choice(["approved", "declined", "pending"]),
        "transaction_type": random.choice(["purchase", "refund", "withdrawal"]),
        "customer_email": fake.email(),
        "customer_phone": fake.phone_number(),
        "customer_location": fake.city(),
        "customer_account_created": fake.date_time_this_decade().isoformat(),
        "customer_risk_score": round(random.uniform(0, 100), 2),
        "customer_previous_fraud": fake.boolean(chance_of_getting_true=5),
        "ip_address": fake.ipv4_public(),
        "ip_geolocation": {"city": fake.city(), "country": fake.country()},
        "device_id": fake.uuid4(),
        "device_type": random.choice(["Mobile", "Desktop", "Tablet"]),
        "device_os": random.choice(["Windows", "iOS", "Android"]),
        "browser": fake.user_agent(),
        "is_vpn": fake.boolean(chance_of_getting_true=5),
        "login_attempts_last_24h": random.randint(0, 10),
        "failed_login_attempts": random.randint(0, 5),
        "password_change_recent": fake.boolean(chance_of_getting_true=20),
        "account_age_days": random.randint(1, 3650),
        "transaction_count_last_7d": random.randint(0, 20),
        "average_transaction_amount_last_7d": round(random.uniform(5.0, 2000.0), 2),
        "shipping_address_mismatch": fake.boolean(chance_of_getting_true=5),
        "is_fraud": fake.boolean(chance_of_getting_true=2),
        "fraud_score": round(random.uniform(0, 100), 2),
        "chargeback_flag": fake.boolean(chance_of_getting_true=2),
        "velocity_risk_flag": fake.boolean(chance_of_getting_true=5),
        "geolocation_risk_flag": fake.boolean(chance_of_getting_true=5),
        "device_risk_flag": fake.boolean(chance_of_getting_true=5),
        "payment_method_risk_flag": fake.boolean(chance_of_getting_true=5),
        "account_risk_flag": fake.boolean(chance_of_getting_true=5),
        "external_blacklist_flag": fake.boolean(chance_of_getting_true=2),
        "coupon_code_used": fake.lexify(text='??????'),
        "promotion_id": random.randint(1, 1000),
        "merchant_category": random.choice(["Electronics", "Fashion", "Food", "Gaming"]),
        "session_id": fake.uuid4(),
        "cart_value": round(random.uniform(5.0, 5000.0), 2),
        "referrer_url": fake.url(),
        "browser_fingerprint": fake.sha1()
    }

while True:
    msg = generate_transaction()
    producer.send(TOPIC, msg)
    print(f"Sent: {msg['transaction_id']}")
    time.sleep(1)  # 1 transaction per second
