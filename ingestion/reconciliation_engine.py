import pandas as pd
from datetime import datetime

def load_swift(path="data/mock/swift_transactions.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["source"] = "SWIFT"
    return df[["transaction_ref", "amount", "currency", "value_date", "source"]]

def load_gl(path="data/mock/gl_entries.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["source"] = "GL"
    df["amount"] = df["debit"] - df["credit"]
    df["transaction_ref"] = df["document_no"]
    return df[["transaction_ref", "amount", "currency", "posting_date", "source"]]

def reconcile(swift_df: pd.DataFrame, gl_df: pd.DataFrame) -> dict:
    """Match SWIFT transactions against GL entries. Flag breaks."""
    merged = swift_df.merge(
        gl_df, on=["transaction_ref", "currency"], 
        how="outer", suffixes=("_swift", "_gl")
    )
    
    # Classify each row
    merged["status"] = "MATCHED"
    merged.loc[merged["amount_gl"].isna(), "status"] = "MISSING_IN_GL"
    merged.loc[merged["amount_swift"].isna(), "status"] = "MISSING_IN_SWIFT"
    merged.loc[
        (merged["status"] == "MATCHED") & 
        (abs(merged["amount_swift"] - merged["amount_gl"]) > 0.01),
        "status"
    ] = "AMOUNT_BREAK"
    
    summary = merged["status"].value_counts().to_dict()
    breaks = merged[merged["status"] != "MATCHED"]
    
    print(f"\n=== Reconciliation Summary @ {datetime.now().strftime('%H:%M:%S')} ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"  Break Rate: {len(breaks)/len(merged)*100:.1f}%")
    
    breaks.to_csv("data/processed/recon_breaks.csv", index=False)
    return {"summary": summary, "breaks": breaks, "all": merged}

if __name__ == "__main__":
    swift = load_swift()
    gl = load_gl()
    result = reconcile(swift, gl)