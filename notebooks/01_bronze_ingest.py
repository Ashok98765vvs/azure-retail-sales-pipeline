# =============================================================================
# 01_bronze_ingest.py
# Azure Retail Sales Pipeline - Bronze Layer: Raw Data Ingestion
# Author: Ashok | Data Engineer | Auburn University at Montgomery
# =============================================================================
"""
Bronze Layer: Ingests raw CSV files from source into ADLS Gen2 bronze/ container.
- Lands data as-is (no transformation) preserving full audit trail
- Adds ingestion metadata columns
- Partitions by year/month/day for efficient downstream reads
- Idempotent: safe to re-run for same date partition

Usage (Azure Databricks):
    %run ./01_bronze_ingest --env azure --date 2026-07-01

Usage (Local):
    python notebooks/01_bronze_ingest.py --env local --date 2026-07-01
"""

import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("bronze_ingest")


# ---------------------------------------------------------------------------
# Schema Definition (enforce on read to catch upstream issues early)
# ---------------------------------------------------------------------------
SALES_SCHEMA = StructType([
    StructField("order_id",      StringType(),  nullable=False),
    StructField("store_id",      StringType(),  nullable=True),
    StructField("order_date",    StringType(),  nullable=True),
    StructField("product_cat",   StringType(),  nullable=True),
    StructField("quantity",      IntegerType(), nullable=True),
    StructField("unit_price",    FloatType(),   nullable=True),
    StructField("total_amount",  FloatType(),   nullable=True),
    StructField("payment_type",  StringType(),  nullable=True),
    StructField("customer_id",   StringType(),  nullable=True),
    StructField("region",        StringType(),  nullable=True),
])


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    """Create or retrieve Spark session based on environment."""
    if env == "local":
        spark = (
            SparkSession.builder
            .appName("AzureRetailSalesPipeline_Bronze")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
    else:
        # Azure Databricks: SparkSession already exists
        spark = SparkSession.builder.getOrCreate()
        # Mount ADLS Gen2 via service principal (configured in cluster)
        spark.conf.set(
            "fs.azure.account.auth.type.adlsretailpipeline.dfs.core.windows.net",
            "OAuth"
        )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Path Helpers
# ---------------------------------------------------------------------------
def get_source_path(env: str, run_date: date) -> str:
    """Source path: raw CSV file for the given date."""
    date_str = run_date.strftime("%Y_%m_%d")
    if env == "local":
        return f"data/raw_sample/sales_{date_str}.csv"
    return (
        f"abfss://bronze@adlsretailpipeline.dfs.core.windows.net/"
        f"raw/sales_{date_str}.csv"
    )


def get_bronze_path(env: str, run_date: date) -> str:
    """Destination path: partitioned bronze zone."""
    y, m, d = run_date.year, run_date.month, run_date.day
    if env == "local":
        return f"output/bronze/year={y}/month={m:02d}/day={d:02d}"
    return (
        f"abfss://bronze@adlsretailpipeline.dfs.core.windows.net/"
        f"sales/year={y}/month={m:02d}/day={d:02d}"
    )


# ---------------------------------------------------------------------------
# Core Ingestion Logic
# ---------------------------------------------------------------------------
def ingest_bronze(spark: SparkSession, env: str, run_date: date) -> int:
    """
    Read raw CSV and write to bronze partition.
    Returns record count for downstream validation.
    """
    source_path = get_source_path(env, run_date)
    bronze_path = get_bronze_path(env, run_date)
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    logger.info(f"Reading source: {source_path}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")       # keep malformed rows, flag them
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(SALES_SCHEMA)
        .csv(source_path)
    )

    # Add Bronze metadata columns
    df_bronze = (
        df_raw
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_pipeline_run_id",     F.lit(pipeline_run_id))
        .withColumn("_source_file",         F.lit(source_path))
        .withColumn("_layer",               F.lit("bronze"))
        .withColumn("_run_date",            F.lit(str(run_date)))
    )

    record_count = df_bronze.count()
    logger.info(f"Records read: {record_count:,}")

    # Write as Parquet to bronze partition (overwrite for idempotency)
    logger.info(f"Writing to bronze: {bronze_path}")
    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    logger.info(f"Bronze ingestion complete. {record_count:,} records written.")
    return record_count


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--env",  default="local",      help="Environment: local | azure")
    parser.add_argument("--date", default=str(date.today()), help="Run date YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info(f"=== Bronze Ingestion START | env={args.env} | date={run_date} ===")
    spark = get_spark(args.env)

    try:
        count = ingest_bronze(spark, args.env, run_date)
        logger.info(f"=== Bronze Ingestion COMPLETE | {count:,} records ===")
    except Exception as e:
        logger.error(f"Bronze ingestion FAILED: {e}")
        raise
    finally:
        if args.env == "local":
            spark.stop()
