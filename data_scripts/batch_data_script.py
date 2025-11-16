import pandas as pd
import json
import random
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET
from openpyxl import Workbook
from datetime import datetime, timedelta

# -------------------------
# Configuration
# -------------------------
NUM_RECORDS = 500  # number of transactions per batch file
FRAUD_PERCENT = 0.15  # 15% fraudulent transactions

transaction_types = ['Payment', 'Transfer', 'Withdrawal', 'Deposit']
merchant_categories = ['Electronics', 'Grocery', 'Travel', 'Luxury', 'Restaurants']
currencies = ['USD', 'EUR', 'GBP']
card_types = ['Debit', 'Credit', 'Prepaid']
channels = ['Mobile App', 'Web', 'POS']
locations = ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Houston']
fraud_scenarios = ['CNP Fraud', 'Account Takeover', 'Money Laundering', 'Synthetic Identity', 'High-Risk Merchant']

# -------------------------
# Function to generate transactions
# -------------------------
def generate_transaction(transaction_id):
    is_fraud = 1 if random.random() < FRAUD_PERCENT else 0
    scenario = random.choice(fraud_scenarios) if is_fraud else None
    risk_score = round(random.uniform(0.7, 1.0), 2) if is_fraud else round(random.uniform(0.0, 0.3), 2)
    
    return {
        "transaction_id": transaction_id,
        "user_id": random.randint(1000, 5000),
        "account_id": random.randint(10000, 99999),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(currencies),
        "transaction_type": random.choice(transaction_types),
        "merchant_id": random.randint(100, 500),
        "merchant_category": random.choice(merchant_categories),
        "card_type": random.choice(card_types),
        "transaction_channel": random.choice(channels),
        "location": random.choice(locations),
        "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 10000))).strftime('%Y-%m-%d %H:%M:%S'),
        "fraud_flag": is_fraud,
        "risk_score": risk_score,
        "device_id": f"device_{random.randint(1000,9999)}",
        "fraud_scenario": scenario
    }

# Generate all transactions
data = [generate_transaction(i+1) for i in range(NUM_RECORDS)]
df = pd.DataFrame(data)

# -------------------------
# Export to 6 file formats
# -------------------------
# CSV
df.to_csv("batch_transactions.csv", index=False)

# JSON (lines)
df.to_json("batch_transactions.json", orient="records", lines=True)

# Parquet
df.to_parquet("batch_transactions.parquet", index=False)

# XML
root = ET.Element("transactions")
for _, row in df.iterrows():
    transaction = ET.SubElement(root, "transaction")
    for col in df.columns:
        ET.SubElement(transaction, col).text = str(row[col])
tree = ET.ElementTree(root)
tree.write("batch_transactions.xml")

# TSV
df.to_csv("batch_transactions.tsv", sep="\t", index=False)

# Excel
df.to_excel("batch_transactions.xlsx", index=False)
