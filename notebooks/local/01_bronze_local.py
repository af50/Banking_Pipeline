# notebooks/local/01_bronze_local.py
"""
Bronze Layer — Local Mode

Reads Parquet batches from local landing zone
and consolidates them into Bronze Parquet tables.

Bronze = raw storage (Parquet)
Silver = curated storage (Delta) — cleaning happens there
Gold   = analytical layer (Delta via dbt)

Run manually:
    python notebooks/local/01_bronze_local.py
    python notebooks/local/01_bronze_local.py --reset
"""
import argparse
import logging
import os
import re
import sys
import shutil

# ── Environment ───────────────────────────────────────────────────────────────
os.environ["HADOOP_HOME"]           = "E:\\hadoop"
os.environ["SPARK_LOCAL_IP"]        = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)))
from data_simulation.config import (
    BRONZE_ATM_MASTER, BRONZE_CARD_TRANSACTIONS,
    BRONZE_CARDS, BRONZE_CUSTOMERS,
    BRONZE_KAGGLE_TRANSACTIONS, BRONZE_OUT_OF_CASH,
    BRONZE_WALLET,
    LANDING_ATM_MASTER, LANDING_CARD_TRANSACTIONS,
    LANDING_CARDS, LANDING_CUSTOMERS,
    LANDING_KAGGLE_TRANSACTIONS, LANDING_OUT_OF_CASH,
    LANDING_WALLET,
    LOG_FORMAT, LOG_LEVEL,
    SPARK_APP_NAME, SPARK_LOG_LEVEL,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("bronze_local")


# ── Spark ─────────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-bronze-local")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", "C:/tmp/spark")
        .config("spark.sql.warehouse.dir", "C:/tmp/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


# ── Column cleaner ────────────────────────────────────────────────────────────
def clean_columns(df: DataFrame) -> DataFrame:
    new_cols = [
        re.sub(r'[ ,;{}()\n\t=]', '_', c).lower().strip('_')
        for c in df.columns
    ]
    return df.toDF(*new_cols)


# ── Reset ─────────────────────────────────────────────────────────────────────
def reset_bronze() -> None:
    """Deletes all bronze Parquet data."""
    from data_simulation.config import LOCAL_BRONZE
    if os.path.exists(LOCAL_BRONZE):
        shutil.rmtree(LOCAL_BRONZE)
        logger.info(f"Deleted bronze: {LOCAL_BRONZE}")
    os.makedirs(LOCAL_BRONZE, exist_ok=True)
    logger.info("Bronze reset complete")


# ── Parquet file collector ────────────────────────────────────────────────────
def collect_parquet_files(landing_path: str) -> list:
    parquet_files = []
    for root, dirs, files in os.walk(landing_path):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))
    return parquet_files


# ── Generic ingestor ──────────────────────────────────────────────────────────
def ingest_to_bronze(
    spark: SparkSession,
    landing_path: str,
    bronze_path: str,
    table_name: str,
    partition_by: str = None,
) -> int:
    """
    Reads all Parquet files from landing,
    cleans column names,
    adds bronze metadata,
    writes consolidated Parquet to bronze layer.

    Uses overwrite mode — bronze is always rebuilt from landing.
    This is correct because bronze = raw copy of landing,
    Silver handles deduplication and incremental logic.
    """
    if not os.path.exists(landing_path):
        logger.warning(f"Landing path does not exist: {landing_path}")
        return 0

    parquet_files = collect_parquet_files(landing_path)
    if not parquet_files:
        logger.warning(f"No parquet files in: {landing_path}")
        return 0

    logger.info(
        f"Ingesting {table_name} — {len(parquet_files)} parquet files"
    )

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(*parquet_files)
    )

    df = clean_columns(df)

    df = (
        df
        .withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    row_count = df.count()
    if row_count == 0:
        logger.warning(f"{table_name}: 0 rows — skipping")
        return 0

    logger.info(f"{table_name}: {row_count:,} rows")
    os.makedirs(bronze_path, exist_ok=True)

    writer = (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if partition_by and partition_by in df.columns:
        writer = writer.partitionBy(partition_by)

    writer.save(bronze_path)
    logger.info(f"✓ {table_name}: {row_count:,} rows → {bronze_path}")
    return row_count


# ── Health check ──────────────────────────────────────────────────────────────
def bronze_health_check(spark: SparkSession, results: dict) -> None:
    print()
    print("=" * 70)
    print("BRONZE HEALTH CHECK")
    print("=" * 70)

    table_paths = {
        "raw_atm_master":          BRONZE_ATM_MASTER,
        "raw_customers":           BRONZE_CUSTOMERS,
        "raw_cards":               BRONZE_CARDS,
        "raw_card_transactions":   BRONZE_CARD_TRANSACTIONS,
        "raw_wallet_transactions": BRONZE_WALLET,
        "raw_out_of_cash":         BRONZE_OUT_OF_CASH,
        "raw_kaggle_transactions": BRONZE_KAGGLE_TRANSACTIONS,
    }

    total_rows = 0
    for name, path in table_paths.items():
        try:
            if not os.path.exists(path):
                print(f"  {name:<35} NOT FOUND")
                continue
            df    = spark.read.parquet(path)
            count = df.count()
            total_rows += count
            cols   = len(df.columns)
            latest = None
            if "_bronze_loaded_at" in df.columns:
                latest = df.agg(
                    F.max("_bronze_loaded_at")
                ).collect()[0][0]
            print(
                f"  {name:<35} "
                f"{count:>10,} rows  "
                f"{cols:>3} cols  "
                f"latest: {latest}"
            )
        except Exception as e:
            print(f"  {name:<35} ERROR: {e}")

    print("-" * 70)
    print(f"  {'TOTAL':<35} {total_rows:>10,} rows")
    print("=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────
def main(reset: bool = False) -> dict:
    logger.info("=" * 60)
    logger.info("Bronze Pipeline — LOCAL MODE (Parquet)")
    logger.info(f"Reset: {reset}")
    logger.info("=" * 60)

    if reset:
        reset_bronze()

    spark = get_spark()
    results = {}

    # dimensions first
    logger.info("── Ingesting dimension sources ──")

    results["atm_master"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_ATM_MASTER,
        bronze_path=BRONZE_ATM_MASTER,
        table_name="raw_atm_master",
    )

    results["customers"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_CUSTOMERS,
        bronze_path=BRONZE_CUSTOMERS,
        table_name="raw_customers",
    )

    results["cards"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_CARDS,
        bronze_path=BRONZE_CARDS,
        table_name="raw_cards",
    )

    # facts second
    logger.info("── Ingesting fact sources ──")

    results["card_transactions"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_CARD_TRANSACTIONS,
        bronze_path=BRONZE_CARD_TRANSACTIONS,
        table_name="raw_card_transactions",
        partition_by="transaction_date",
    )

    results["wallet"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_WALLET,
        bronze_path=BRONZE_WALLET,
        table_name="raw_wallet_transactions",
    )

    results["out_of_cash"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_OUT_OF_CASH,
        bronze_path=BRONZE_OUT_OF_CASH,
        table_name="raw_out_of_cash",
    )

    results["kaggle_transactions"] = ingest_to_bronze(
        spark,
        landing_path=LANDING_KAGGLE_TRANSACTIONS,
        bronze_path=BRONZE_KAGGLE_TRANSACTIONS,
        table_name="raw_kaggle_transactions",
        partition_by="date",
    )

    bronze_health_check(spark, results)

    logger.info("Bronze pipeline complete")
    logger.info(f"Summary: {results}")

    spark.stop()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze ingestion — local mode"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete and recreate all bronze tables"
    )
    args = parser.parse_args()
    main(reset=args.reset)
