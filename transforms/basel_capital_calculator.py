import pandas as pd

# RBI Basel III Risk Weights (simplified)
RISK_WEIGHTS = {
    "CORPORATE_LOAN": 1.00,
    "RETAIL_LOAN": 0.75,
    "MORTGAGE": 0.35,
    "SOVEREIGN": 0.00,
    "BANK": 0.20,
    "EQUITY": 1.50
}

MIN_CAR = 0.09  # RBI minimum 9% Capital Adequacy Ratio

def calculate_rwa(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    """Risk-Weighted Assets calculation per Basel III."""
    portfolio_df["risk_weight"] = portfolio_df["asset_class"].map(RISK_WEIGHTS).fillna(1.0)
    portfolio_df["rwa"] = portfolio_df["exposure_inr"] * portfolio_df["risk_weight"]
    return portfolio_df

def calculate_car(tier1_capital: float, tier2_capital: float, total_rwa: float) -> dict:
    total_capital = tier1_capital + tier2_capital
    car = total_capital / total_rwa if total_rwa > 0 else 0
    return {
        "tier1_capital": tier1_capital,
        "tier2_capital": tier2_capital,
        "total_capital": total_capital,
        "total_rwa": total_rwa,
        "car_ratio": round(car * 100, 4),
        "car_status": "COMPLIANT" if car >= MIN_CAR else "BREACH",
        "minimum_required": MIN_CAR * 100
    }

if __name__ == "__main__":
    # Sample portfolio
    portfolio = pd.DataFrame([
        {"asset_class": "CORPORATE_LOAN", "exposure_inr": 50_00_00_000},
        {"asset_class": "RETAIL_LOAN",    "exposure_inr": 20_00_00_000},
        {"asset_class": "MORTGAGE",       "exposure_inr": 30_00_00_000},
        {"asset_class": "SOVEREIGN",      "exposure_inr": 10_00_00_000},
    ])
    portfolio = calculate_rwa(portfolio)
    
    result = calculate_car(
        tier1_capital=8_00_00_000,
        tier2_capital=2_00_00_000,
        total_rwa=portfolio["rwa"].sum()
    )
    print("\n=== Basel III CAR Report ===")
    for k, v in result.items():
        print(f"  {k}: {v}")