import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transforms.basel_capital_calculator import calculate_rwa, calculate_car, RISK_WEIGHTS, MIN_CAR


@pytest.fixture
def sample_portfolio():
    return pd.DataFrame([
        {"asset_class": "CORPORATE_LOAN", "exposure_inr": 100_00_00_000},
        {"asset_class": "RETAIL_LOAN",    "exposure_inr": 50_00_00_000},
        {"asset_class": "MORTGAGE",       "exposure_inr": 80_00_00_000},
        {"asset_class": "SOVEREIGN",      "exposure_inr": 30_00_00_000},
    ])


def test_rwa_columns_added(sample_portfolio):
    result = calculate_rwa(sample_portfolio.copy())
    assert "risk_weight" in result.columns
    assert "rwa" in result.columns


def test_sovereign_zero_rwa(sample_portfolio):
    result = calculate_rwa(sample_portfolio.copy())
    sovereign_rwa = result[result["asset_class"] == "SOVEREIGN"]["rwa"].iloc[0]
    assert sovereign_rwa == 0.0


def test_corporate_loan_full_rwa(sample_portfolio):
    result = calculate_rwa(sample_portfolio.copy())
    corp = result[result["asset_class"] == "CORPORATE_LOAN"]
    assert corp["rwa"].iloc[0] == corp["exposure_inr"].iloc[0] * 1.00


def test_car_compliant():
    car = calculate_car(tier1_capital=50_00_00_000, tier2_capital=10_00_00_000, total_rwa=400_00_00_000)
    assert car["car_status"] == "COMPLIANT"
    assert car["car_ratio"] >= MIN_CAR * 100


def test_car_breach():
    car = calculate_car(tier1_capital=1_00_00_000, tier2_capital=50_00_000, total_rwa=500_00_00_000)
    assert car["car_status"] == "BREACH"
    assert car["car_ratio"] < MIN_CAR * 100


def test_car_minimum_boundary():
    total_rwa = 100_00_00_000
    exact_capital = total_rwa * MIN_CAR
    car = calculate_car(tier1_capital=exact_capital * 0.8, tier2_capital=exact_capital * 0.2, total_rwa=total_rwa)
    assert car["car_status"] == "COMPLIANT"


def test_unknown_asset_class_defaults_to_100_pct():
    df = pd.DataFrame([{"asset_class": "UNKNOWN_ASSET", "exposure_inr": 10_00_00_000}])
    result = calculate_rwa(df)
    assert result["risk_weight"].iloc[0] == 1.0


def test_all_risk_weights_present():
    for asset_class, weight in RISK_WEIGHTS.items():
        assert 0.0 <= weight <= 2.0, f"Unexpected risk weight for {asset_class}: {weight}"
