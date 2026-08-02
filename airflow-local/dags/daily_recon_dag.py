import sys
sys.path.insert(0, "/opt/airflow/project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "banking-platform",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["compliance@bank.com"],
}


def ingest_swift(**ctx):
    from ingestion.swift_mt940_parser import parse_mt940_file, validate_mt940
    df = parse_mt940_file("data/mock/swift_transactions.csv")
    df = validate_mt940(df)
    invalid_count = int((~df["is_valid"]).sum())
    print(f"Ingested {len(df)} SWIFT records | Invalid: {invalid_count}")
    ctx["ti"].xcom_push(key="swift_count", value=len(df))


def ingest_cbs(**ctx):
    from ingestion.cbs_api_connector import fetch_transactions
    df = fetch_transactions()
    print(f"Ingested {len(df)} CBS transaction records")
    ctx["ti"].xcom_push(key="cbs_count", value=len(df))


def run_reconciliation(**ctx):
    from ingestion.reconciliation_engine import load_swift, load_gl, reconcile
    result = reconcile(load_swift(), load_gl())
    breaks = len(result["breaks"])
    print(f"Reconciliation complete. Breaks: {breaks}")
    if breaks > 100:
        raise ValueError(f"Break count {breaks} exceeds threshold 100 — manual review required.")
    ctx["ti"].xcom_push(key="break_count", value=breaks)


def run_quality_checks(**ctx):
    from transforms.data_quality_scorer import run_all_sources
    results = run_all_sources(
        "data/mock/swift_transactions.csv",
        "data/mock/gl_entries.csv",
    )
    for r in results:
        if r["score"] < 90:
            raise ValueError(
                f"Data quality below threshold for {r['source']}: {r['score']}%"
            )
    print("All data quality checks passed.")


def load_to_delta(**ctx):
    import pandas as pd
    from reporting.delta_mart_loader import get_spark, create_database, load_recon_breaks, load_basel_metrics, build_recon_summary_mart
    from transforms.basel_capital_calculator import calculate_rwa, calculate_car

    spark = get_spark()
    create_database(spark)

    load_recon_breaks(spark, "data/processed/recon_breaks.csv")

    portfolio = pd.DataFrame([
        {"asset_class": "CORPORATE_LOAN", "exposure_inr": 50_00_00_000},
        {"asset_class": "RETAIL_LOAN",    "exposure_inr": 20_00_00_000},
        {"asset_class": "MORTGAGE",       "exposure_inr": 30_00_00_000},
        {"asset_class": "SOVEREIGN",      "exposure_inr": 10_00_00_000},
    ])
    portfolio = calculate_rwa(portfolio)
    car = calculate_car(8_00_00_000, 2_00_00_000, portfolio["rwa"].sum())
    load_basel_metrics(spark, car)
    build_recon_summary_mart(spark)
    spark.stop()
    print("Delta mart load complete.")


with DAG(
    dag_id="daily_reconciliation",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["banking", "reconciliation", "delta"],
) as dag:

    t1_swift = PythonOperator(task_id="ingest_swift",       python_callable=ingest_swift)
    t1_cbs   = PythonOperator(task_id="ingest_cbs",         python_callable=ingest_cbs)
    t2       = PythonOperator(task_id="reconciliation",     python_callable=run_reconciliation)
    t3       = PythonOperator(task_id="data_quality",       python_callable=run_quality_checks)
    t4       = PythonOperator(task_id="load_to_delta",      python_callable=load_to_delta)

    [t1_swift, t1_cbs] >> t2 >> t3 >> t4
