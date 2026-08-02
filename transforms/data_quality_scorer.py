import pandas as pd
from datetime import datetime


QUALITY_RULES = {
    "SWIFT_MT940": {
        "no_null_amounts":      lambda df: df["amount"].notna().all(),
        "positive_amounts":     lambda df: (df["amount"] > 0).mean() > 0.95,
        "valid_currency":       lambda df: df["currency"].isin(["INR", "USD", "EUR", "GBP"]).all(),
        "no_duplicate_refs":    lambda df: ~df["transaction_ref"].duplicated().any(),
        "ref_not_empty":        lambda df: (df["transaction_ref"].str.strip() != "").all(),
    },
    "GL": {
        "no_null_amounts":      lambda df: (df["debit"].notna() & df["credit"].notna()).all(),
        "valid_gl_accounts":    lambda df: df["gl_account"].notna().all(),
        "no_duplicate_docs":    lambda df: ~df["document_no"].duplicated().any(),
        "valid_currency":       lambda df: df["currency"].isin(["INR", "USD", "EUR", "GBP"]).all(),
    },
    "CBS": {
        "no_null_balances":     lambda df: df["balance"].notna().all(),
        "positive_balances":    lambda df: (df["balance"] >= 0).all(),
        "valid_account_status": lambda df: df["status"].isin(["ACTIVE", "DORMANT", "CLOSED"]).all(),
    },
}


def run_quality_checks(df: pd.DataFrame, source_name: str) -> dict:
    rules = QUALITY_RULES.get(source_name, {})
    if not rules:
        print(f"[WARN] No quality rules defined for source: {source_name}")

    check_results = {}
    for rule_name, rule_fn in rules.items():
        try:
            check_results[rule_name] = bool(rule_fn(df))
        except Exception as exc:
            print(f"[ERROR] Rule '{rule_name}' failed to evaluate: {exc}")
            check_results[rule_name] = False

    passed = sum(1 for v in check_results.values() if v)
    failed = len(check_results) - passed
    score  = round(passed / len(check_results) * 100, 1) if check_results else 0.0

    print(f"\n=== Data Quality Report: {source_name} @ {datetime.now().strftime('%H:%M:%S')} ===")
    print(f"  Total rows : {len(df)}")
    for rule_name, passed_flag in check_results.items():
        tag = "PASS" if passed_flag else "FAIL"
        print(f"  [{tag}] {rule_name}")
    print(f"  Score: {score}%  ({passed} passed / {failed} failed)")

    return {
        "source":     source_name,
        "total_rows": len(df),
        "checks":     check_results,
        "passed":     passed,
        "failed":     failed,
        "score":      score,
        "run_at":     datetime.now().isoformat(),
    }


def run_all_sources(swift_path: str, gl_path: str) -> list:
    results = []
    swift_df = pd.read_csv(swift_path)
    gl_df    = pd.read_csv(gl_path)
    results.append(run_quality_checks(swift_df, "SWIFT_MT940"))
    results.append(run_quality_checks(gl_df,    "GL"))
    return results


if __name__ == "__main__":
    results = run_all_sources(
        "data/mock/swift_transactions.csv",
        "data/mock/gl_entries.csv",
    )
    overall = sum(r["score"] for r in results) / len(results)
    print(f"\nOverall Quality Score: {overall:.1f}%")
