import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.swift_mt940_parser import parse_mt940_file, validate_mt940


@pytest.fixture
def mock_swift_csv(tmp_path):
    data = pd.DataFrame([
        {"transaction_ref": "TXN001", "amount": 10000.00, "currency": "INR",
         "value_date": "240115", "counterparty": "HDFC Bank", "swift_code": "HDFCINBB", "narrative": "Payment"},
        {"transaction_ref": "TXN002", "amount": 25000.00, "currency": "USD",
         "value_date": "240115", "counterparty": "Citi Bank", "swift_code": "CITIUSNYX", "narrative": "Wire"},
        {"transaction_ref": None,     "amount": None,     "currency": "XYZ",
         "value_date": "240115", "counterparty": "Unknown",  "swift_code": "UNKNOWN",   "narrative": "Bad"},
    ])
    path = tmp_path / "swift_transactions.csv"
    data.to_csv(path, index=False)
    return str(path)


def test_parse_returns_dataframe(mock_swift_csv):
    df = parse_mt940_file(mock_swift_csv)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3


def test_parse_adds_metadata_columns(mock_swift_csv):
    df = parse_mt940_file(mock_swift_csv)
    assert "parsed_at" in df.columns
    assert "source" in df.columns
    assert df["source"].iloc[0] == "SWIFT_MT940"


def test_validate_marks_invalid_currency(mock_swift_csv):
    df = parse_mt940_file(mock_swift_csv)
    df = validate_mt940(df)
    assert "is_valid" in df.columns
    invalid = df[~df["is_valid"]]
    assert len(invalid) >= 1


def test_validate_valid_records(mock_swift_csv):
    df = parse_mt940_file(mock_swift_csv)
    df = validate_mt940(df)
    valid = df[df["is_valid"]]
    assert len(valid) == 2


def test_amount_is_float(mock_swift_csv):
    df = parse_mt940_file(mock_swift_csv)
    assert df["amount"].dtype == float
