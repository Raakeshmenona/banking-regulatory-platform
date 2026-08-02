from faker import Faker
import pandas as pd
import random, json
from datetime import datetime, timedelta

fake = Faker()

def generate_swift_mt940(n=100):
    records = []
    for _ in range(n):
        records.append({
            "transaction_ref": fake.uuid4(),
            "value_date": fake.date_between("-30d", "today").strftime("%y%m%d"),
            "amount": round(random.uniform(10000, 5000000), 2),
            "currency": random.choice(["INR", "USD", "EUR"]),
            "counterparty": fake.company(),
            "swift_code": fake.bothify("????##??###"),
            "narrative": fake.sentence()
        })
    return pd.DataFrame(records)

def generate_gl_entries(n=200):
    records = []
    for _ in range(n):
        records.append({
            "gl_account": random.choice(["1001","1002","2001","3001","4001"]),
            "cost_center": fake.bothify("CC###"),
            "debit": round(random.uniform(0, 1000000), 2),
            "credit": round(random.uniform(0, 1000000), 2),
            "currency": random.choice(["INR", "USD"]),
            "posting_date": fake.date_between("-7d", "today").isoformat(),
            "document_no": fake.bothify("DOC#####")
        })
    return pd.DataFrame(records)

if __name__ == "__main__":
    generate_swift_mt940(500).to_csv("data/mock/swift_transactions.csv", index=False)
    generate_gl_entries(1000).to_csv("data/mock/gl_entries.csv", index=False)
    print("Mock data generated.")