# =============================================================================
# 03_gold_aggregation.py
# Azure Retail Sales Pipeline - Gold Layer: Business Aggregations
# Author: Ashok | Data Engineer | Auburn University at Montgomery
# =============================================================================
"""
Gold Layer: Reads Silver data and produces business-ready aggregated tables.

Outputs:
  1. fact_sales_daily      - Daily revenue KPIs by store and category
  2. fact_sales_by_region  - Regional performance summary
  3. fact_payment_analysis - Payment method breakdown
  4. dim_store_summary     - Store-level running totals

Consumers: Power BI, Azure Synapse Analytics, SQL queries
"""

import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("gold_aggregation")


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    if env == "local":
        return (
            SparkSession.builder
            .appName("AzureRetailSalesPipeline_Gold")
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


def get_gold_path(env: str, table_name: str) -> str:
    if env == "local":
        return f"output/gold/{table_name}"
    return f"abfss://gold@adlsretailpipeline.dfs.core.windows.net/{table_name}"


# ---------------------------------------------------------------------------
# Gold Table 1: Daily Sales KPIs
# ---------------------------------------------------------------------------
def build_fact_sales_daily(df: DataFrame) -> DataFrame:
    """
    Daily revenue aggregation by store and product category.
    Suitable for trend analysis and Power BI time-series charts.
    """
    return (
        df
        .groupBy("order_date", "store_id", "product_cat", "region")
        .agg(
            F.count("order_id").alias("transaction_count"),
            F.sum("total_amount").alias("daily_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
            F.max("total_amount").alias("max_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("daily_revenue",   F.round(F.col("daily_revenue"),   2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .withColumn("revenue_rank",
                    F.rank().over(
                        Window.partitionBy("order_date")
                              .orderBy(F.desc("daily_revenue"))
                    ))
        .orderBy("order_date", "store_id", "product_cat")
    )


# ---------------------------------------------------------------------------
# Gold Table 2: Regional Performance
# ---------------------------------------------------------------------------
def build_fact_sales_by_region(df: DataFrame) -> DataFrame:
    """Regional summary for geographic KPIs."""
    return (
        df
        .groupBy("order_date", "region")
        .agg(
            F.count("order_id").alias("transaction_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("store_id").alias("active_stores"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_high_value") == True, 1).otherwise(0)).alias("high_value_orders"),
        )
        .withColumn("total_revenue",   F.round(F.col("total_revenue"),   2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .withColumn("revenue_pct",
                    F.round(
                        F.col("total_revenue") /
                        F.sum("total_revenue").over(Window.partitionBy("order_date")) * 100,
                        1
                    ))
        .orderBy("order_date", F.desc("total_revenue"))
    )


# ---------------------------------------------------------------------------
# Gold Table 3: Payment Analysis
# ---------------------------------------------------------------------------
def build_fact_payment_analysis(df: DataFrame) -> DataFrame:
    """Payment type breakdown for finance and fraud analytics."""
    return (
        df
        .groupBy("order_date", "payment_type")
        .agg(
            F.count("order_id").alias("transaction_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_value"),
        )
        .withColumn("total_revenue",         F.round(F.col("total_revenue"),         2))
        .withColumn("avg_transaction_value",  F.round(F.col("avg_transaction_value"), 2))
        .withColumn("pct_of_transactions",
                    F.round(
                        F.col("transaction_count") /
                        F.sum("transaction_count").over(Window.partitionBy("order_date")) * 100,
                        1
                    ))
        .orderBy("order_date", F.desc("transaction_count"))
    )


# ---------------------------------------------------------------------------
# Gold Table 4: Store Summary (Dimension-style)
# ---------------------------------------------------------------------------
def build_dim_store_summary(df: DataFrame) -> DataFrame:
    """Store-level cumulative summary for scorecards."""
    return (
        df
        .groupBy("store_id", "region")
        .agg(
            F.count("order_id").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.min("order_date").alias("first_sale_date"),
            F.max("order_date").alias("last_sale_date"),
        )
        .withColumn("total_revenue",   F.round(F.col("total_revenue"),   2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .orderBy(F.desc("total_revenue"))
    )


# ---------------------------------------------------------------------------
# Core Gold Build
# ---------------------------------------------------------------------------
def build_gold(spark: SparkSession, env: str, run_date: date) -> dict:
    """Build all Gold tables and return record counts."""
    silver_path = get_silver_path(env, run_date)
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    logger.info(f"Reading silver: {silver_path}")
    df_silver = spark.read.parquet(silver_path)
    logger.info(f"Silver records loaded: {df_silver.count():,}")

    tables = {
        "fact_sales_daily":      build_fact_sales_daily(df_silver),
        "fact_sales_by_region":  build_fact_sales_by_region(df_silver),
        "fact_payment_analysis": build_fact_payment_analysis(df_silver),
        "dim_store_summary":     build_dim_store_summary(df_silver),
    }

    counts = {}
    for table_name, df_gold in tables.items():
        gold_path = get_gold_path(env, table_name)

        # Add gold metadata
        df_gold = (
            df_gold
            .withColumn("_gold_timestamp",   F.current_timestamp())
            .withColumn("_pipeline_run_id",  F.lit(pipeline_run_id))
            .withColumn("_run_date",         F.lit(str(run_date)))
            .withColumn("_layer",            F.lit("gold"))
        )

        count = df_gold.count()
        counts[table_name] = count
        logger.info(f"Writing {table_name}: {count} rows -> {gold_path}")

        writer = df_gold.write.mode("overwrite")
        if env == "azure":
            writer.format("parquet").save(gold_path)
        else:
            writer.parquet(gold_path)

    return counts


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Gold Layer Aggregation")
    parser.add_argument("--env",  default="local")
    parser.add_argument("--date", default=str(date.today()))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    logger.info(f"=== Gold Aggregation START | env={args.env} | date={run_date} ===")
    spark = get_spark(args.env)
    try:
        counts = build_gold(spark, args.env, run_date)
        for table, count in counts.items():
            logger.info(f"  {table}: {count:,} rows")
        logger.info(f"=== Gold Aggregation COMPLETE ===")
    except Exception as e:
        logger.error(f"Gold aggregation FAILED: {e}")
        raise
    finally:
        if args.env == "local":
            spark.stop()
