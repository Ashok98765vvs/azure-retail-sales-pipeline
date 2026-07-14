# =============================================================================
# 04_data_quality_checks.py
# Azure Retail Sales Pipeline - Data Quality Validation Engine
# Author: Ashok | Data Engineer | Auburn University at Montgomery
# =============================================================================
"""
Data Quality Engine: Validates data quality across Bronze/Silver layers.

Checks:
  - Null threshold rules (configurable per column)
  - Primary key uniqueness
  - Row-count drift detection (>30% drop triggers pipeline failure)
  - Schema validation (expected columns + types)
  - Domain value checks (payment_type, region)

Outputs: DQ results logged to dq_results/ with pass/fail status
"""

import argparse
import logging
import json
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("data_quality")


# ---------------------------------------------------------------------------
# DQ Config: Null Thresholds (% of rows allowed to be null)
# ---------------------------------------------------------------------------
NULL_THRESHOLDS = {
    "order_id":      0.0,   # critical: 0% nulls allowed
    "store_id":      0.0,
    "order_date":    0.0,
    "quantity":      5.0,   # 5% nulls acceptable
    "unit_price":    5.0,
    "total_amount":  5.0,
    "payment_type": 10.0,
    "region":       10.0,
    "product_cat":  15.0,
    "customer_id":  20.0,
}

MANDATORY_COLUMNS = [
    "order_id", "store_id", "order_date", "product_cat",
    "quantity", "unit_price", "total_amount", "payment_type"
]

ROW_COUNT_DRIFT_MAX_DROP_PCT = 30.0  # Fail if >30% drop vs yesterday


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    if env == "local":
        return (
            SparkSession.builder
            .appName("AzureRetailSalesPipeline_DQ")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Path Helpers
# ---------------------------------------------------------------------------
def get_silver_path(env: str, run_date: date) -> str:
    y, m, d = run_date.year, run_date.month, run_date.day
    if env == "local":
        return f"output/silver/year={y}/month={m:02d}/day={d:02d}"
    return (
        f"abfss://silver@adlsretailpipeline.dfs.core.windows.net/"
        f"sales/year={y}/month={m:02d}/day={d:02d}"
    )


def get_dq_results_path(env: str) -> str:
    if env == "local":
        return "output/dq_results"
    return "abfss://dq@adlsretailpipeline.dfs.core.windows.net/results"


# ---------------------------------------------------------------------------
# DQ Check 1: Null Threshold Validation
# ---------------------------------------------------------------------------
def check_null_thresholds(df: DataFrame, row_count: int) -> dict:
    """
    Check if null % in each column exceeds configured thresholds.
    Returns dict: {column: {null_pct, threshold, pass/fail}}
    """
    results = {}
    for col_name, threshold in NULL_THRESHOLDS.items():
        if col_name not in df.columns:
            results[col_name] = {"status": "SKIP", "reason": "column missing"}
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / row_count * 100) if row_count > 0 else 0.0
        passed = null_pct <= threshold

        results[col_name] = {
            "null_count":  null_count,
            "null_pct":    round(null_pct, 2),
            "threshold":   threshold,
            "status":      "PASS" if passed else "FAIL"
        }

        if not passed:
            logger.warning(
                f"NULL THRESHOLD FAIL: {col_name} has {null_pct:.2f}% nulls "
                f"(threshold: {threshold}%)"
            )

    return results


# ---------------------------------------------------------------------------
# DQ Check 2: Primary Key Uniqueness
# ---------------------------------------------------------------------------
def check_primary_key_uniqueness(df: DataFrame) -> dict:
    """Verify order_id is unique (no duplicates)."""
    total_count = df.count()
    unique_count = df.select("order_id").dropDuplicates().count()
    duplicate_count = total_count - unique_count
    passed = duplicate_count == 0

    if not passed:
        logger.warning(f"DUPLICATE KEY FAIL: Found {duplicate_count} duplicate order_ids")

    return {
        "total_records":    total_count,
        "unique_order_ids": unique_count,
        "duplicate_count":  duplicate_count,
        "status":           "PASS" if passed else "FAIL"
    }


# ---------------------------------------------------------------------------
# DQ Check 3: Row Count Drift Detection
# ---------------------------------------------------------------------------
def check_row_count_drift(current_count: int, previous_count: int | None) -> dict:
    """
    Detect anomalous row-count drops (>30% drop triggers FAIL).
    If no previous data, mark as PASS (first run).
    """
    if previous_count is None or previous_count == 0:
        return {
            "current_count":  current_count,
            "previous_count": previous_count,
            "drift_pct":      0.0,
            "status":         "PASS",
            "reason":         "no baseline"
        }

    drift_pct = ((current_count - previous_count) / previous_count) * 100
    passed = drift_pct >= -ROW_COUNT_DRIFT_MAX_DROP_PCT

    if not passed:
        logger.error(
            f"ROW COUNT DRIFT FAIL: Current={current_count:,}, "
            f"Previous={previous_count:,}, Drop={abs(drift_pct):.1f}%"
        )

    return {
        "current_count":  current_count,
        "previous_count": previous_count,
        "drift_pct":      round(drift_pct, 2),
        "status":         "PASS" if passed else "FAIL"
    }


# ---------------------------------------------------------------------------
# DQ Check 4: Schema Validation
# ---------------------------------------------------------------------------
def check_schema(df: DataFrame) -> dict:
    """Verify all mandatory columns are present."""
    actual_cols = set(df.columns)
    missing = [col for col in MANDATORY_COLUMNS if col not in actual_cols]
    passed = len(missing) == 0

    if not passed:
        logger.warning(f"SCHEMA FAIL: Missing columns: {missing}")

    return {
        "expected_cols": MANDATORY_COLUMNS,
        "actual_cols":   list(actual_cols),
        "missing_cols":  missing,
        "status":        "PASS" if passed else "FAIL"
    }


# ---------------------------------------------------------------------------
# DQ Check 5: Domain Value Validation
# ---------------------------------------------------------------------------
def check_domain_values(df: DataFrame) -> dict:
    """
    Check if payment_type and region contain only expected values.
    """
    valid_payments = {"CARD", "CASH", "DIGITAL", "UNKNOWN"}
    valid_regions  = {"SE", "NE", "MW", "W", "UNKNOWN"}

    invalid_payments = (
        df.filter(~F.col("payment_type").isin(*valid_payments))
          .select("payment_type").distinct().collect()
    )
    invalid_regions = (
        df.filter(~F.col("region").isin(*valid_regions))
          .select("region").distinct().collect()
    )

    passed = len(invalid_payments) == 0 and len(invalid_regions) == 0

    return {
        "invalid_payment_types": [r["payment_type"] for r in invalid_payments],
        "invalid_regions":       [r["region"] for r in invalid_regions],
        "status":                "PASS" if passed else "FAIL"
    }


# ---------------------------------------------------------------------------
# Main DQ Runner
# ---------------------------------------------------------------------------
def run_data_quality_checks(
    spark: SparkSession,
    env: str,
    run_date: date,
    previous_count: int | None = None
) -> dict:
    """
    Run all DQ checks and return comprehensive results dict.
    """
    silver_path = get_silver_path(env, run_date)
    logger.info(f"Reading silver data: {silver_path}")

    df = spark.read.parquet(silver_path)
    row_count = df.count()
    logger.info(f"Total records: {row_count:,}")

    # Execute all checks
    dq_results = {
        "run_date":          str(run_date),
        "run_timestamp":     datetime.utcnow().isoformat() + "Z",
        "layer":             "silver",
        "record_count":      row_count,
        "checks": {
            "null_thresholds":   check_null_thresholds(df, row_count),
            "primary_key":       check_primary_key_uniqueness(df),
            "row_count_drift":   check_row_count_drift(row_count, previous_count),
            "schema":            check_schema(df),
            "domain_values":     check_domain_values(df),
        }
    }

    # Aggregate: overall PASS/FAIL
    all_checks_passed = all(
        check["status"] == "PASS"
        for check_name, check_results in dq_results["checks"].items()
        for check_key, check in (
            check_results.items() if isinstance(check_results, dict) and "status" in check_results
            else [(check_key, check_val) for check_key, check_val in check_results.items()
                  if isinstance(check_val, dict) and "status" in check_val]
        )
    )

    dq_results["overall_status"] = "PASS" if all_checks_passed else "FAIL"

    return dq_results


# ---------------------------------------------------------------------------
# Save DQ Results
# ---------------------------------------------------------------------------
def save_dq_results(spark: SparkSession, env: str, results: dict):
    """Save DQ results as JSON for auditing."""
    dq_path = get_dq_results_path(env)
    run_date = results["run_date"]
    timestamp = results["run_timestamp"].replace(":", "").replace(".", "")
    output_file = f"{dq_path}/dq_{run_date}_{timestamp}.json"

    results_json = json.dumps(results, indent=2)
    logger.info(f"Saving DQ results to: {output_file}")

    if env == "local":
        import os
        os.makedirs(dq_path, exist_ok=True)
        with open(f"{dq_path}/dq_{run_date}_{timestamp}.json", "w") as f:
            f.write(results_json)
    else:
        # Write to ADLS as text file
        rdd = spark.sparkContext.parallelize([results_json])
        rdd.saveAsTextFile(output_file)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Data Quality Checks")
    parser.add_argument("--env",            default="local")
    parser.add_argument("--date",           default=str(date.today()))
    parser.add_argument("--previous-count", type=int, default=None, help="Previous day record count")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info(f"=== Data Quality Checks START | env={args.env} | date={run_date} ===")
    spark = get_spark(args.env)

    try:
        results = run_data_quality_checks(spark, args.env, run_date, args.previous_count)
        save_dq_results(spark, args.env, results)

        logger.info(f"=== DQ OVERALL STATUS: {results['overall_status']} ===")

        if results["overall_status"] == "FAIL":
            logger.error("Data quality checks FAILED. Review results for details.")
            raise RuntimeError("Data quality validation failed")
        else:
            logger.info("All data quality checks PASSED.")

    except Exception as e:
        logger.error(f"DQ checks failed with exception: {e}")
        raise
    finally:
        if args.env == "local":
            spark.stop()
