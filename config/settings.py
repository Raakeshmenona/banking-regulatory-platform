import os

DATABRICKS_CONFIG = {
    "host": os.getenv("DATABRICKS_HOST"),        # e.g. adb-xxxx.azuredatabricks.net
    "token": os.getenv("DATABRICKS_TOKEN"),
    "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
    "http_path": os.getenv("DATABRICKS_HTTP_PATH")  # for SQL Warehouse
}

DELTA_PATHS = {
    "bronze_swift":    "abfss://banking-delta@staidebank.dfs.core.windows.net/bronze/swift/",
    "bronze_gl":       "abfss://banking-delta@staidebank.dfs.core.windows.net/bronze/gl/",
    "silver_recon":    "abfss://banking-delta@staidebank.dfs.core.windows.net/silver/reconciliation/",
    "gold_basel":      "abfss://banking-delta@staidebank.dfs.core.windows.net/gold/basel_metrics/",
    "gold_recon_mart": "abfss://banking-delta@staidebank.dfs.core.windows.net/gold/recon_mart/"
}

AZURE_EVENT_HUB = {
    "connection_str": os.getenv("EH_CONNECTION_STR"),
    "hub_name": "banking-feeds"
}

ADLS_CONFIG = {
    "account_name": os.getenv("ADLS_ACCOUNT"),
    "account_key": os.getenv("ADLS_KEY"),
    "container": "banking-delta"
}