from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark():
    builder = (
        SparkSession.builder
        .appName("BankingGLAggregator")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def aggregate_gl(spark: SparkSession, gl_path: str, output_path: str = "data/processed/gl_aggregated") -> None:
    df = spark.read.csv(gl_path, header=True, inferSchema=True)

    result = (
        df.groupBy("gl_account", "cost_center", "currency")
          .agg(
              F.sum("debit").alias("total_debit"),
              F.sum("credit").alias("total_credit"),
              F.count("*").alias("txn_count"),
              (F.sum("debit") - F.sum("credit")).alias("net_balance"),
              F.max("posting_date").alias("last_posting_date"),
          )
          .withColumn("processed_at", F.current_timestamp())
          .orderBy("gl_account")
    )

    result.show(20, truncate=False)

    result.write.format("delta").mode("overwrite").save(output_path)
    print(f"GL aggregation complete. {result.count()} rows written to {output_path} (Delta format)")


def reconcile_gl_to_swift(spark: SparkSession, gl_path: str, swift_path: str) -> None:
    gl_df    = spark.read.csv(gl_path,    header=True, inferSchema=True)
    swift_df = spark.read.csv(swift_path, header=True, inferSchema=True)

    gl_df    = gl_df.withColumn("net_amount", F.col("debit") - F.col("credit"))
    swift_df = swift_df.withColumnRenamed("amount", "swift_amount") \
                       .withColumnRenamed("transaction_ref", "ref")

    joined = gl_df.join(
        swift_df,
        gl_df["document_no"] == swift_df["ref"],
        how="full_outer"
    ).select(
        F.coalesce(gl_df["document_no"], swift_df["ref"]).alias("ref"),
        "net_amount",
        "swift_amount",
        (F.col("net_amount") - F.col("swift_amount")).alias("variance"),
    )

    joined.filter(F.abs(F.col("variance")) > 0.01).show(50)
    print("GL vs SWIFT reconciliation done.")


if __name__ == "__main__":
    spark = create_spark()
    aggregate_gl(spark, "data/mock/gl_entries.csv")
    spark.stop()
