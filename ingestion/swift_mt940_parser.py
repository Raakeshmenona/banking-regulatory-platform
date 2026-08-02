import pandas as pd
import re
from dataclasses import dataclass
from typing import List

@dataclass
class MT940Transaction:
    value_date: str
    entry_date: str
    debit_credit: str
    amount: float
    currency: str
    reference: str
    narrative: str

def parse_mt940_file(filepath: str) -> pd.DataFrame:
    """Parse SWIFT MT940 statement file into structured DataFrame."""
    # For mock CSV (in real life, parse raw SWIFT fixed-format messages)
    df = pd.read_csv(filepath)
    df["parsed_at"] = pd.Timestamp.now()
    df["source"] = "SWIFT_MT940"
    df["amount"] = df["amount"].astype(float)
    return df

def validate_mt940(df: pd.DataFrame) -> pd.DataFrame:
    df["is_valid"] = (
        df["amount"].notna() &
        df["currency"].isin(["INR", "USD", "EUR", "GBP"]) &
        df["transaction_ref"].notna()
    )
    invalid = df[~df["is_valid"]]
    if len(invalid) > 0:
        print(f"[WARN] {len(invalid)} invalid MT940 records found")
    return df

if __name__ == "__main__":
    df = parse_mt940_file("data/mock/swift_transactions.csv")
    df = validate_mt940(df)
    print(df.head())
    print(f"Total: {len(df)} | Valid: {df['is_valid'].sum()}")