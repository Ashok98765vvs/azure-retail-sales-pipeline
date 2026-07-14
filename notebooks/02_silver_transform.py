# =============================================================================
# 02_silver_transform.py
# Azure Retail Sales Pipeline - Silver Layer: Cleaning & Normalization
# Author: Ashok | Data Engineer | Auburn University at Montgomery
# =============================================================================
"""
Silver Layer: Reads Bronze Parquet, applies transformations, writes Silver Delta.
- Null handling per column business rules
- Type casting and date parsing
- String normalization (trim, upper/lower)
- Deduplication on order_id (primary key)
- Adds derived columns: order_year, order_month, order_day_of_week
- Adds silver metadata: processed_timestamp, pipeline_run_id
- Writes as Delta Lake (or Parquet in local mode)
"""

import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("silver_transform")


# ---------------------------------------------------------------------------
# Valid domain values (used for filtering/flagging bad data)
# ---------------------------------------------------------------------------
VALID_PAYMENT_TYPES = {"CARD", "CASH", "DIGITAL"}
VALID_REGIONS       = {"SE", "NE", "MW", "W"}
VALID_PRODUCT_CATS  = {"Electronics", "Apparel", "Grocery", "Beauty", "Home & Garden"}


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    if env == "local":
        return (
            SparkSession.builder
            .appName("AzureRetailSalesPipeline_Silver")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Path Helpers
# ---------------------------------------------------------------------------
def get_bronze_path(env: str, run_date: date) -> str:
    y, m, d = run_date.year, run_date.month, run_date.day
    if env == "local":
        return f"output/bronze/year={y}/month={m:02d}/day={d:02d}"
    return (
        f"abfss://bronze@adlsretailpipeline.dfs.core.windows.net/"
        f"sales/year={y}/month={m:02d}/day={d:02d}"
    )


def get_silver_path(env: str, run_date: date) -> str:
    y, m, d = run_date.year, run_date.month, run_date.day
    if env == "local":
        return f"output/silver/year={y}/month={m:02d}/day={d:02d}"
    return (
        f"abfss://silver@adlsretailpipeline.dfs.core.windows.net/"
        f"sales/year={y}/month={m:02d}/day={d:02d}"
    )


# ---------------------------------------------------------------------------
# Transformation Functions
# ---------------------------------------------------------------------------
def clean_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace and normalize string columns."""
    string_cols = ["order_id", "store_id", "product_cat", "payment_type",
                   "customer_id", "region"]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    # Uppercase categorical fields
    df = (
        df
        .withColumn("payment_type", F.upper(F.col("payment_type")))
        .withColumn("region",       F.upper(F.col("region")))
    )
    return df


def cast_types(df: DataFrame) -> DataFrame:
    """Cast columns to correct data types."""
    return (
        df
        .withColumn("order_date",   F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",   F.col("unit_price").cast(FloatType()))
        .withColumn("total_amount", F.col("total_amount").cast(FloatType()))
    )


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Handle nulls based on business rules:
    - order_id, store_id, order_date: critical - drop row if null
    - quantity: default to 1 if null
    - unit_price, total_amount: default to 0.0 if null
    - payment_type: default to 'UNKNOWN'
    - region: default to 'UNKNOWN'
    """
    # Drop rows with critical null fields
    df = df.dropna(subset=["order_id", "store_id", "order_date"])

    # Fill non-critical nulls with defaults
    df = df.fillna({
        "quantity":     1,
        "unit_price":   0.0,
        "total_amount": 0.0,
        "payment_type": "UNKNOWN",
        "region":       "UNKNOWN",
        "product_cat":  "OTHER",
        "customer_id":  "ANON",
    })
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate order_ids, keeping the first occurrence."""
    before = df.count()
    df_deduped = df.dropDuplicates(["order_id"])
    after = df_deduped.count()
    dupes = before - after
    if dupes > 0:
        logger.warning(f"Removed {dupes:,} duplicate order_ids")
    return df_deduped


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add business-useful derived columns."""
    return (
        df
        .withColumn("order_year",        F.year(F.col("order_date")))
        .withColumn("order_month",        F.month(F.col("order_date")))
        .withColumn("order_day_of_week",  F.dayofweek(F.col("order_date")))  # 1=Sun, 7=Sat
        .withColumn("order_quarter",      F.quarter(F.col("order_date")))
        .withColumn("revenue_per_unit",   F.round(F.col("total_amount") / F.col("quantity"), 2))
        .withColumn("is_high_value",      F.when(F.col("total_amount") >= 500.0, True).otherwise(False))
    )


def flag_invalid_domain(df: DataFrame) -> DataFrame:
    """Add a flag for rows that have unexpected domain values."""
    valid_payments = F.array(*[F.lit(v) for v in sorted(VALID_PAYMENT_TYPES)])
    valid_regions  = F.array(*[F.lit(v) for v in sorted(VALID_REGIONS)])
    return (
        df
        .withColumn("_invalid_payment_type",
                    ~F.col("payment_type").isin(*VALID_PAYMENT_TYPES))
        .withColumn("_invalid_region",
                    ~F.col("region").isin(*VALID_REGIONS))
    )


# ---------------------------------------------------------------------------
# Core Silver Transform
# ---------------------------------------------------------------------------
def transform_silver(spark: SparkSession, env: str, run_date: date) -> int:
    """Apply all silver transformations. Returns output record count."""
    bronze_path = get_bronze_path(env, run_date)
    silver_path = get_silver_path(env, run_date)
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    logger.info(f"Reading bronze: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    logger.info(f"Raw record count: {df.count():,}")

    # Apply transformation pipeline
    df = clean_strings(df)
    df = cast_types(df)
    df = handle_nulls(df)
    df = deduplicate(df)
    df = add_derived_columns(df)
    df = flag_invalid_domain(df)

    # Add silver metadata
    df = (
        df
        .withColumn("_silver_timestamp",  F.current_timestamp())
        .withColumn("_pipeline_run_id",   F.lit(pipeline_run_id))
        .withColumn("_layer",             F.lit("silver"))
        .withColumn("_run_date",          F.lit(str(run_date)))
    )

    record_count = df.count()
    logger.info(f"Silver record count after transforms: {record_count:,}")

    # Write silver (Delta on Azure, Parquet locally)
    logger.info(f"Writing silver to: {silver_path}")
    writer = df.write.mode("overwrite")
    if env == "azure":
        writer.format("delta").save(silver_path)
    else:
        writer.parquet(silver_path)

    logger.info(f"Silver transform complete. {record_count:,} records written.")
    return record_count


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Silver Layer Transformation")
    parser.add_argument("--env",  default="local")
    parser.add_argument("--date", default=str(date.today()))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info(f"=== Silver Transform START | env={args.env} | date={run_date} ===")
    spark = get_spark(args.env)
    try:
        count = transform_silver(spark, args.env, run_date)
        logger.info(f"=== Silver Transform COMPLETE | {count:,} records ===")
    except Exception as e:
        logger.error(f"Silver transform FAILED: {e}")
        raise
    finally:
        if args.env == "local":
            spark.stop()
