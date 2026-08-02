import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta


CBS_BASE_URL = os.getenv("CBS_API_URL", "https://mock-cbs-api.internal/v1")
CBS_API_KEY  = os.getenv("CBS_API_KEY", "mock-key-local-dev")

HEADERS = {
    "Authorization": f"Bearer {CBS_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def fetch_accounts(branch_code: str = "ALL") -> pd.DataFrame:
    """Poll Core Banking System for account balances."""
    try:
        resp = requests.get(
            f"{CBS_BASE_URL}/accounts",
            headers=HEADERS,
            params={"branch": branch_code, "as_of": datetime.today().strftime("%Y-%m-%d")},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("accounts", [])
    except (requests.RequestException, Exception):
        print("[WARN] CBS API unavailable — loading mock data")
        data = _mock_accounts()

    df = pd.DataFrame(data)
    df["fetched_at"] = pd.Timestamp.now()
    df["source"] = "CBS"
    return df


def fetch_transactions(from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """Poll CBS for transaction feed between two dates."""
    if not from_date:
        from_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.today().strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            f"{CBS_BASE_URL}/transactions",
            headers=HEADERS,
            params={"from_date": from_date, "to_date": to_date, "limit": 10000},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json().get("transactions", [])
    except (requests.RequestException, Exception):
        print("[WARN] CBS API unavailable — loading mock transactions")
        data = _mock_transactions()

    df = pd.DataFrame(data)
    df["fetched_at"] = pd.Timestamp.now()
    df["source"] = "CBS"
    return df


def poll_with_retry(fetch_fn, retries: int = 3, delay: int = 10, **kwargs) -> pd.DataFrame:
    for attempt in range(1, retries + 1):
        try:
            df = fetch_fn(**kwargs)
            print(f"[CBS] Fetched {len(df)} records on attempt {attempt}")
            return df
        except Exception as exc:
            print(f"[CBS] Attempt {attempt} failed: {exc}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError("CBS API failed after all retries")


def _mock_accounts() -> list:
    from faker import Faker
    import random
    fake = Faker()
    return [
        {
            "account_id":   fake.bothify("ACC#######"),
            "account_type": random.choice(["SAVINGS", "CURRENT", "OD", "LOAN"]),
            "currency":     random.choice(["INR", "USD", "EUR"]),
            "balance":      round(random.uniform(1000, 50000000), 2),
            "branch_code":  fake.bothify("BR###"),
            "ifsc":         fake.bothify("????0######"),
            "status":       "ACTIVE",
        }
        for _ in range(200)
    ]


def _mock_transactions() -> list:
    from faker import Faker
    import random
    fake = Faker()
    return [
        {
            "txn_id":       fake.uuid4(),
            "account_id":   fake.bothify("ACC#######"),
            "txn_type":     random.choice(["CREDIT", "DEBIT"]),
            "amount":       round(random.uniform(100, 5000000), 2),
            "currency":     random.choice(["INR", "USD"]),
            "txn_date":     fake.date_between("-7d", "today").isoformat(),
            "description":  fake.sentence(nb_words=5),
            "channel":      random.choice(["NEFT", "RTGS", "IMPS", "UPI", "BRANCH"]),
        }
        for _ in range(500)
    ]


if __name__ == "__main__":
    accounts = fetch_accounts()
    print(f"\nAccounts fetched: {len(accounts)}")
    print(accounts.head(3).to_string())

    txns = fetch_transactions()
    print(f"\nTransactions fetched: {len(txns)}")
    print(txns.head(3).to_string())
