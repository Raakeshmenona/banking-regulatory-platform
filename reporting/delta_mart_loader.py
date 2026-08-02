from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, StringType
from datetime import date
from delta import configure_spark_with_delta_pip


def get_spark():
    builder = (
        SparkSession.builder
        .appName("BankingDeltaMartLoader")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def create_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS banking_dw")
    spark.sql("USE banking_dw")
    print("Database ready: banking_dw")


def load_recon_breaks(spark, breaks_path: str):
    df = spark.read.csv(breaks_path, header=True, inferSchema=True)
    df = (
        df.withColumn("loaded_at", F.current_timestamp())
          .withColumn(
              "break_amount",
              F.abs(F.col("amount_swift").cast("double") - F.col("amount_gl").cast("double"))
          )
    )
    df.write.format("delta").mode("append").saveAsTable("banking_dw.fact_reconciliation")
    print(f"Loaded {df.count()} recon break records -> banking_dw.fact_reconciliation")


def load_basel_metrics(spark, car_result: dict):
    schema = StructType([
        StructField("report_date",   DateType(),   False),
        StructField("tier1_capital", DoubleType(), True),
        StructField("tier2_capital", DoubleType(), True),
        StructField("total_rwa",     DoubleType(), True),
        StructField("car_ratio",     DoubleType(), True),
        StructField("car_status",    StringType(), True),
    ])
    row = [(
        date.today(),
        float(car_result["tier1_capital"]),
        float(car_result["tier2_capital"]),
        float(car_result["total_rwa"]),
        float(car_result["car_ratio"]),
        car_result["car_status"],
    )]
    df = spark.createDataFrame(row, schema)
    df.write.format("delta").mode("append").saveAsTable("banking_dw.fact_basel_metrics")
    print(f"Basel metrics loaded -> banking_dw.fact_basel_metrics | CAR: {car_result['car_ratio']}%")


def build_recon_summary_mart(spark):
    spark.sql("""
        CREATE OR REPLACE TABLE banking_dw.mart_recon_daily AS
        SELECT
            CAST(value_date AS DATE)    AS report_date,
            currency,
            status,
            COUNT(*)                    AS txn_count,
            SUM(break_amount)           AS total_break_amount,
            AVG(break_amount)           AS avg_break_amount
        FROM banking_dw.fact_reconciliation
        GROUP BY CAST(value_date AS DATE), currency, status
    """)
    print("Gold mart built: banking_dw.mart_recon_daily")


if __name__ == "__main__":
    spark = get_spark()
    create_database(spark)
    load_recon_breaks(spark, "data/processed/recon_breaks.csv")
    build_recon_summary_mart(spark)
    spark.stop()
