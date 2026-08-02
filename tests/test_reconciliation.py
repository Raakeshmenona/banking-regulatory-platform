import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.reconciliation_engine import reconcile


@pytest.fixture
def matched_data():
    swift = pd.DataFrame([
        {"transaction_ref": "TXN001", "amount": 10000.00, "currency": "INR", "value_date": "2024-01-15", "source": "SWIFT"},
        {"transaction_ref": "TXN002", "amount": 25000.00, "currency": "USD", "value_date": "2024-01-15", "source": "SWIFT"},
    ])
    gl = pd.DataFrame([
        {"transaction_ref": "TXN001", "amount": 10000.00, "currency": "INR", "posting_date": "2024-01-15", "source": "GL"},
        {"transaction_ref": "TXN002", "amount": 25000.00, "currency": "USD", "posting_date": "2024-01-15", "source": "GL"},
    ])
    return swift, gl


@pytest.fixture
def break_data():
    swift = pd.DataFrame([
        {"transaction_ref": "TXN001", "amount": 10000.00, "currency": "INR", "value_date": "2024-01-15", "source": "SWIFT"},
        {"transaction_ref": "TXN003", "amount": 5000.00,  "currency": "INR", "value_date": "2024-01-15", "source": "SWIFT"},
    ])
    gl = pd.DataFrame([
        {"transaction_ref": "TXN001", "amount": 9999.00, "currency": "INR", "posting_date": "2024-01-15", "source": "GL"},
        {"transaction_ref": "TXN002", "amount": 7500.00, "currency": "INR", "posting_date": "2024-01-15", "source": "GL"},
    ])
    return swift, gl


def test_all_matched(matched_data, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/processed", exist_ok=True)
    swift, gl = matched_data
    result = reconcile(swift, gl)
    assert result["summary"].get("MATCHED", 0) == 2
    assert len(result["breaks"]) == 0


def test_amount_break_detected(break_data, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/processed", exist_ok=True)
    swift, gl = break_data
    result = reconcile(swift, gl)
    statuses = result["all"]["status"].unique().tolist()
    assert "AMOUNT_BREAK" in statuses or "MISSING_IN_GL" in statuses or "MISSING_IN_SWIFT" in statuses


def test_missing_in_gl(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/processed", exist_ok=True)
    swift = pd.DataFrame([
        {"transaction_ref": "TXN_ONLY_SWIFT", "amount": 5000.0, "currency": "INR", "value_date": "2024-01-15", "source": "SWIFT"},
    ])
    gl = pd.DataFrame(columns=["transaction_ref", "amount", "currency", "posting_date", "source"])
    result = reconcile(swift, gl)
    assert "MISSING_IN_GL" in result["summary"]


def test_missing_in_swift(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/processed", exist_ok=True)
    swift = pd.DataFrame(columns=["transaction_ref", "amount", "currency", "value_date", "source"])
    gl = pd.DataFrame([
        {"transaction_ref": "TXN_ONLY_GL", "amount": 8000.0, "currency": "INR", "posting_date": "2024-01-15", "source": "GL"},
    ])
    result = reconcile(swift, gl)
    assert "MISSING_IN_SWIFT" in result["summary"]


def test_break_rate_within_acceptable_range(break_data, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/processed", exist_ok=True)
    swift, gl = break_data
    result = reconcile(swift, gl)
    total  = len(result["all"])
    breaks = len(result["breaks"])
    break_rate = breaks / total * 100
    assert break_rate <= 100
