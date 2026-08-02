import sys
sys.path.insert(0, "/opt/airflow/project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "banking-platform",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
    "email":            ["compliance@bank.com", "rbi-reporting@bank.com"],
}


def compute_capital_adequacy(**ctx):
    import pandas as pd
    from transforms.basel_capital_calculator import calculate_rwa, calculate_car

    portfolio = pd.DataFrame([
        {"asset_class": "CORPORATE_LOAN", "exposure_inr": 50_00_00_000},
        {"asset_class": "RETAIL_LOAN",    "exposure_inr": 20_00_00_000},
        {"asset_class": "MORTGAGE",       "exposure_inr": 30_00_00_000},
        {"asset_class": "SOVEREIGN",      "exposure_inr": 10_00_00_000},
        {"asset_class": "BANK",           "exposure_inr":  5_00_00_000},
        {"asset_class": "EQUITY",         "exposure_inr":  2_00_00_000},
    ])
    portfolio = calculate_rwa(portfolio)
    car = calculate_car(
        tier1_capital=8_00_00_000,
        tier2_capital=2_00_00_000,
        total_rwa=portfolio["rwa"].sum(),
    )
    print(f"\n=== Basel III CAR Report ===")
    for k, v in car.items():
        print(f"  {k}: {v}")

    if car["car_status"] == "BREACH":
        raise ValueError(
            f"CAR BREACH: {car['car_ratio']}% is below RBI minimum {car['minimum_required']}%"
        )

    ctx["ti"].xcom_push(key="car_ratio",  value=car["car_ratio"])
    ctx["ti"].xcom_push(key="car_status", value=car["car_status"])


def generate_lcr_report(**ctx):
    """Liquidity Coverage Ratio — simplified RBI IRAC report."""
    import random
    hqla          = round(random.uniform(100_00_00_000, 200_00_00_000), 2)
    net_cash_out  = round(random.uniform(80_00_00_000,  150_00_00_000), 2)
    lcr           = round(hqla / net_cash_out * 100, 2)
    min_lcr       = 100.0

    print(f"\n=== LCR Report ===")
    print(f"  HQLA          : {hqla:,.2f}")
    print(f"  Net Cash Out  : {net_cash_out:,.2f}")
    print(f"  LCR           : {lcr}%")
    print(f"  Status        : {'COMPLIANT' if lcr >= min_lcr else 'BREACH'}")

    if lcr < min_lcr:
        raise ValueError(f"LCR BREACH: {lcr}% below minimum {min_lcr}%")

    ctx["ti"].xcom_push(key="lcr_ratio", value=lcr)


def generate_slr_report(**ctx):
    """Statutory Liquidity Ratio — RBI mandate 18% of NDTL."""
    import random
    ndtl          = round(random.uniform(500_00_00_000, 1000_00_00_000), 2)
    slr_holdings  = round(random.uniform(100_00_00_000,  200_00_00_000), 2)
    slr_pct       = round(slr_holdings / ndtl * 100, 2)
    min_slr       = 18.0

    print(f"\n=== SLR Report ===")
    print(f"  NDTL          : {ndtl:,.2f}")
    print(f"  SLR Holdings  : {slr_holdings:,.2f}")
    print(f"  SLR           : {slr_pct}%")
    print(f"  Status        : {'COMPLIANT' if slr_pct >= min_slr else 'BREACH'}")

    if slr_pct < min_slr:
        raise ValueError(f"SLR BREACH: {slr_pct}% below RBI minimum {min_slr}%")

    ctx["ti"].xcom_push(key="slr_pct", value=slr_pct)


def publish_to_delta(**ctx):
    from reporting.delta_mart_loader import get_spark, create_database
    from pyspark.sql import functions as F
    from datetime import date

    ti          = ctx["ti"]
    car_ratio   = ti.xcom_pull(task_ids="compute_car",    key="car_ratio")
    lcr_ratio   = ti.xcom_pull(task_ids="compute_lcr",    key="lcr_ratio")
    slr_pct     = ti.xcom_pull(task_ids="compute_slr",    key="slr_pct")

    spark = get_spark()
    create_database(spark)

    row = [(str(date.today()), float(car_ratio or 0), float(lcr_ratio or 0), float(slr_pct or 0), "PUBLISHED")]
    df = spark.createDataFrame(row, ["report_date", "car_ratio", "lcr_ratio", "slr_pct", "status"])
    df.write.format("delta").mode("append").saveAsTable("banking_dw.fact_regulatory_reports")
    print("Regulatory report published to Delta: banking_dw.fact_regulatory_reports")
    spark.stop()


with DAG(
    dag_id="regulatory_report",
    default_args=default_args,
    schedule_interval="0 7 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["banking", "basel3", "rbi", "regulatory"],
) as dag:

    t1 = PythonOperator(task_id="compute_car", python_callable=compute_capital_adequacy)
    t2 = PythonOperator(task_id="compute_lcr", python_callable=generate_lcr_report)
    t3 = PythonOperator(task_id="compute_slr", python_callable=generate_slr_report)
    t4 = PythonOperator(task_id="publish_to_delta", python_callable=publish_to_delta)

    [t1, t2, t3] >> t4
