# notebooks/local/02_silver_local.py
"""
Silver Layer — Local Mode

Reads Bronze Parquet tables, applies:
- Type casting and column standardization
- Data quality validation
- Bad record quarantine
- Deduplication
- Derived columns
- PAN to customer mapping
- Writes clean Delta tables to Silver layer

Runs AFTER 01_bronze_local.py completes.
In Airflow this is Task 3 in the full pipeline DAG.

Run manually:

    python notebooks/local/02_silver_local.py
    python notebooks/local/02_silver_local.py --reset
"""
import argparse
import logging
import os
import re
import sys
import shutil
from datetime import datetime, timezone

# ── Environment ───────────────────────────────────────────────────────────────
os.environ["HADOOP_HOME"]           = "E:\\hadoop"
os.environ["SPARK_LOCAL_IP"]        = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import window as W
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType,
    IntegerType, LongType, StringType, TimestampType
)

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)))
from data_simulation.config import (
    BRONZE_ATM_MASTER, BRONZE_CARD_TRANSACTIONS,
    BRONZE_CARDS, BRONZE_CUSTOMERS,
    BRONZE_KAGGLE_TRANSACTIONS, BRONZE_OUT_OF_CASH,
    BRONZE_WALLET,
    SILVER_ATM_MASTER, SILVER_CARD_TRANSACTIONS,
    SILVER_CARDS, SILVER_CUSTOMERS,
    SILVER_KAGGLE_TRANSACTIONS, SILVER_OUT_OF_CASH,
    SILVER_WALLET,
    QUARANTINE_PATH, LOCAL_SILVER,
    LOG_FORMAT, LOG_LEVEL,
    SPARK_APP_NAME, SPARK_LOG_LEVEL,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("silver_local")


# ── Spark ─────────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-silver-local")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", "C:/tmp/spark")
        .config("spark.sql.warehouse.dir", "C:/tmp/spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


# ── Reset ─────────────────────────────────────────────────────────────────────
def reset_silver() -> None:
    """Deletes all silver Delta tables."""
    spark = get_spark()
    tables = [
        ("silver.raw_atm_master", SILVER_ATM_MASTER),
        ("silver.raw_customers", SILVER_CUSTOMERS),
        ("silver.raw_cards", SILVER_CARDS),
        ("silver.raw_card_transactions", SILVER_CARD_TRANSACTIONS),
        ("silver.raw_wallet_transactions", SILVER_WALLET),
        ("silver.raw_out_of_cash", SILVER_OUT_OF_CASH),
        ("silver.raw_kaggle_transactions", SILVER_KAGGLE_TRANSACTIONS),
    ]
    
    for table_name, path in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        if os.path.exists(path):
            shutil.rmtree(path)
            logger.info(f"Dropped {table_name}")
    
    if os.path.exists(LOCAL_SILVER):
        shutil.rmtree(LOCAL_SILVER)
        logger.info(f"Deleted silver directory: {LOCAL_SILVER}")
    
    os.makedirs(LOCAL_SILVER, exist_ok=True)
    
    if os.path.exists(QUARANTINE_PATH):
        shutil.rmtree(QUARANTINE_PATH)
        logger.info(f"Deleted quarantine: {QUARANTINE_PATH}")
    
    spark.stop()
    logger.info("Silver reset complete")


# ── Delta writer ──────────────────────────────────────────────────────────────
def write_silver_delta(
    df: DataFrame,
    silver_path: str,
    merge_key: str,
    partition_by: str = None,
) -> int:
    """
    Writes a DataFrame to a Silver Delta table.
    First run: creates table WITHOUT CDF (to avoid column mapping conflict).
    Subsequent runs: MERGE on merge_key — upserts records.
    Returns row count.
    """
    spark = df.sparkSession
    count = df.count()
    os.makedirs(silver_path, exist_ok=True)

    # FIX: Create silver schema if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

    if not DeltaTable.isDeltaTable(spark, silver_path):
        writer = (
            df.write
            .format("delta")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .mode("overwrite")
        )
        if partition_by and partition_by in df.columns:
            writer = writer.partitionBy(partition_by)
        writer.save(silver_path)
        
        # Create table in catalog
        table_name = silver_path.split("\\")[-1] if "\\" in silver_path else silver_path.split("/")[-1]
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver.raw_{table_name}
            USING DELTA
            LOCATION '{silver_path}'
        """)
        
        logger.info(f"Created Delta table: {silver_path} ({count:,} rows)")
        return count

    # MERGE — upsert on natural key
    delta_table = DeltaTable.forPath(spark, silver_path)
    (
        delta_table.alias("silver")
        .merge(
            df.alias("bronze"),
            f"silver.{merge_key} = bronze.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Merged into Delta table: {silver_path} ({count:,} rows)")
    return count


# ── Quarantine writer ─────────────────────────────────────────────────────────
def quarantine(df: DataFrame, entity: str, reason_col: str) -> None:
    """Writes bad records to quarantine zone for investigation."""
    if df.count() == 0:
        return
    path = os.path.join(QUARANTINE_PATH, entity)
    os.makedirs(path, exist_ok=True)
    (
        df
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_entity", F.lit(entity))
        .write
        .format("parquet")
        .mode("append")
        .save(path)
    )
    logger.warning(
        f"Quarantined {df.count():,} {entity} records — reason: {reason_col}"
    )


# ── 1. ATM Master ─────────────────────────────────────────────────────────────
def transform_atm_master(spark: SparkSession) -> int:
    """
    Cleans ATM master data.
    """
    logger.info("Transforming ATM master...")

    if not os.path.exists(BRONZE_ATM_MASTER):
        logger.warning("Bronze ATM master not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_ATM_MASTER)

    col_map = {
        "terminal_id":      "terminal_id",
        "governorate":      "region",
        "type":             "atm_type",
        "replenished_by":   "provider",
        "replenished_from": "location_type",
        "installation_date":"installation_date",
        "limits":           "cash_limit",
    }

    available = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.select([F.col(k).alias(v) for k, v in available.items()])

    transformed = (
        df
        .withColumn("terminal_id",
            F.upper(F.trim(F.col("terminal_id"))))
        .withColumn("region",
            F.trim(F.col("region")))
        .withColumn("atm_type",
            F.trim(F.col("atm_type")))
        .withColumn("provider",
            F.trim(F.col("provider")))
        .withColumn("location_type",
            F.trim(F.col("location_type")))
        .withColumn("installation_date",
            F.to_date(F.col("installation_date")))
        .withColumn("cash_limit",
            F.col("cash_limit").cast(DoubleType()))
        .withColumn("is_cash_deposit_enabled",
            F.col("atm_type").contains("Deposit")
            | F.col("atm_type").contains("Dépôt"))
        .withColumn("atm_age_years",
            F.floor(
                F.datediff(F.current_date(), F.col("installation_date")) / 365
            ).cast(IntegerType()))
        .withColumn("country", F.lit("Maroc"))
        .withColumn("bank_name", F.lit("Attijariwafa Bank"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(F.col("terminal_id").isNotNull())
    bad  = transformed.filter(F.col("terminal_id").isNull())
    quarantine(bad, "atm_master", "null_terminal_id")

    window = W.Window.partitionBy("terminal_id").orderBy(
        F.col("installation_date").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    count = write_silver_delta(deduped, SILVER_ATM_MASTER, "terminal_id")
    logger.info(f"✓ ATM master: {count:,} rows → silver")
    return count


# ── 2. Customers ──────────────────────────────────────────────────────────────
def transform_customers(spark: SparkSession) -> int:
    """
    Cleans customer data.
    """
    logger.info("Transforming customers...")

    if not os.path.exists(BRONZE_CUSTOMERS):
        logger.warning("Bronze customers not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CUSTOMERS)

    transformed = (
        df
        .withColumn("client_id",
            F.col("id").cast(StringType()))
        .withColumn("current_age",
            F.col("current_age").cast(IntegerType()))
        .withColumn("retirement_age",
            F.col("retirement_age").cast(IntegerType()))
        .withColumn("birth_year",
            F.col("birth_year").cast(IntegerType()))
        .withColumn("birth_month",
            F.col("birth_month").cast(IntegerType()))
        .withColumn("gender",
            F.upper(F.trim(F.col("gender"))))
        .withColumn("address",
            F.trim(F.col("address")))
        .withColumn("latitude",
            F.col("latitude").cast(DoubleType()))
        .withColumn("longitude",
            F.col("longitude").cast(DoubleType()))
        .withColumn("per_capita_income",
            F.regexp_replace(
                F.col("per_capita_income").cast(StringType()),
                "[^0-9.]", ""
            ).cast(DoubleType()))
        .withColumn("yearly_income",
            F.regexp_replace(
                F.col("yearly_income").cast(StringType()),
                "[^0-9.]", ""
            ).cast(DoubleType()))
        .withColumn("total_debt",
            F.regexp_replace(
                F.col("total_debt").cast(StringType()),
                "[^0-9.]", ""
            ).cast(DoubleType()))
        .withColumn("credit_score",
            F.col("credit_score").cast(IntegerType()))
        .withColumn("num_credit_cards",
            F.col("num_credit_cards").cast(IntegerType()))
        .withColumn("age_group",
            F.when(F.col("current_age") < 30, "18-29")
            .when(F.col("current_age") < 45, "30-44")
            .when(F.col("current_age") < 60, "45-59")
            .otherwise("60+"))
        .withColumn("income_segment",
            F.when(F.col("yearly_income") < 60000, "LOW")
            .when(F.col("yearly_income") < 150000, "MEDIUM")
            .when(F.col("yearly_income") < 300000, "HIGH")
            .otherwise("PREMIUM"))
        .withColumn("credit_tier",
            F.when(F.col("credit_score") >= 750, "EXCELLENT")
            .when(F.col("credit_score") >= 670, "GOOD")
            .when(F.col("credit_score") >= 580, "FAIR")
            .otherwise("POOR"))
        .withColumn("debt_to_income_ratio",
            F.round(
                F.col("total_debt") / F.col("yearly_income"), 2
            ))
        .withColumn("country", F.lit("Maroc"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(F.col("client_id").isNotNull())
    bad  = transformed.filter(F.col("client_id").isNull())
    quarantine(bad, "customers", "null_client_id")

    window = W.Window.partitionBy("client_id").orderBy(
        F.col("credit_score").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "id")
    )

    count = write_silver_delta(deduped, SILVER_CUSTOMERS, "client_id")
    logger.info(f"✓ Customers: {count:,} rows → silver")
    return count


# ── 3. Cards ──────────────────────────────────────────────────────────────────
def transform_cards(spark: SparkSession) -> int:
    """
    Cleans card data.
    """
    logger.info("Transforming cards...")

    if not os.path.exists(BRONZE_CARDS):
        logger.warning("Bronze cards not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARDS)

    transformed = (
        df
        .withColumn("card_id",
            F.col("id").cast(StringType()))
        .withColumn("client_id",
            F.col("client_id").cast(StringType()))
        .withColumn("card_brand",
            F.upper(F.trim(F.col("card_brand"))))
        .withColumn("card_type",
            F.upper(F.trim(F.col("card_type"))))
        .withColumn("card_number_masked",
            F.col("card_number").cast(StringType()))
        .withColumn("has_chip",
            F.upper(F.trim(F.col("has_chip"))).isin("YES", "OUI", "TRUE"))
        .withColumn("num_cards_issued",
            F.col("num_cards_issued").cast(IntegerType()))
        .withColumn("credit_limit",
            F.regexp_replace(
                F.col("credit_limit").cast(StringType()),
                "[^0-9.]", ""
            ).cast(DoubleType()))
        .withColumn("acct_open_date",
            F.to_date(F.col("acct_open_date"), "MM/yyyy"))
        .withColumn("year_pin_last_changed",
            F.col("year_pin_last_changed").cast(IntegerType()))
        .withColumn("card_on_dark_web",
            F.upper(F.trim(
                F.col("card_on_dark_web").cast(StringType())
            )).isin("YES", "OUI", "TRUE"))
        .withColumn("expires_date",
            F.to_date(F.col("expires"), "MM/yyyy"))
        .withColumn("is_expired",
            F.col("expires_date") < F.current_date())
        .withColumn("card_age_years",
            F.floor(
                F.datediff(F.current_date(), F.col("acct_open_date")) / 365
            ).cast(IntegerType()))
        .withColumn("dark_web_risk",
            F.when(F.col("card_on_dark_web"), "HIGH")
            .otherwise("LOW"))
        .withColumn("card_category",
            F.when(F.col("card_type").isin("CREDIT", "CRÉDIT"), "CREDIT")
            .when(F.col("card_type").isin("DEBIT", "DÉBIT"), "DEBIT")
            .otherwise("OTHER"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(
        F.col("card_id").isNotNull() &
        F.col("client_id").isNotNull()
    )
    bad = transformed.filter(
        F.col("card_id").isNull() |
        F.col("client_id").isNull()
    )
    quarantine(bad, "cards", "null_card_or_client_id")

    window = W.Window.partitionBy("card_id").orderBy(
        F.col("acct_open_date").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "id", "card_number", "cvv", "expires")
    )

    count = write_silver_delta(deduped, SILVER_CARDS, "card_id")
    logger.info(f"✓ Cards: {count:,} rows → silver")
    return count


# ── 4. PAN to Customer mapping ────────────────────────────────────────────────
def build_pan_customer_map(spark: SparkSession) -> DataFrame:
    """
    Builds a mapping from PAN to client_id.
    """
    logger.info("Building PAN → customer mapping...")

    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found")
        return None

    if not os.path.exists(SILVER_CUSTOMERS):
        logger.warning("Silver customers not found — run customers transform first")
        return None

    pans_df = (
        spark.read.parquet(BRONZE_CARD_TRANSACTIONS)
        .select("pan")
        .distinct()
        .withColumn("pan_num",
            F.regexp_extract(
                F.col("pan"), r"Pan\s*-\s*(\d+)", 1
            ).cast(LongType()))
        .filter(F.col("pan_num") > 0)
    )

    customers_df = (
        spark.read.format("delta").load(SILVER_CUSTOMERS)
        .select("client_id")
        .distinct()
    )
    customer_count = customers_df.count()

    pan_mapped = pans_df.withColumn(
        "customer_idx",
        (F.col("pan_num") % customer_count).cast(LongType())
    )

    customers_with_idx = customers_df.withColumn(
        "customer_idx",
        (F.monotonically_increasing_id() % customer_count).cast(LongType())
    )

    pan_customer_map = (
        pan_mapped.join(customers_with_idx, "customer_idx", "left")
        .select("pan", "pan_num", "client_id")
        .withColumn("_created_at", F.current_timestamp())
    )

    logger.info(
        f"PAN mapping built: {pan_customer_map.count():,} PANs mapped"
    )
    return pan_customer_map


# ── 5. Card Transactions (ATM) ────────────────────────────────────────────────
def transform_card_transactions(
    spark: SparkSession,
    pan_map: DataFrame
) -> int:
    """
    Cleans ATM card transactions.
    - FIX: Uses explicit column comparison for PAN join
    """
    logger.info("Transforming ATM card transactions...")

    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARD_TRANSACTIONS)

    transformed = (
        df
        .withColumn("pan_masked",
            F.trim(F.col("pan")))
        .withColumn("refnum",
            F.trim(F.col("refnum")))
        .withColumn("terminal_id",
            F.upper(F.trim(F.col("termid"))))
        .withColumn("transaction_date",
            F.to_date(F.col("date")))
        .withColumn("transaction_time",
            F.col("time").cast(StringType()))
        .withColumn("transaction_hour",
            F.col("transaction_time").substr(1, 2).cast(IntegerType()))
        .withColumn("transaction_type",
            F.trim(F.col("descr")))
        .withColumn("msg_type",
            F.col("msgtype").cast(IntegerType()))
        .withColumn("amount_mad",
            F.abs(F.col("amount").cast(DoubleType())))
        .withColumn("resp_code",
            F.col("respcode").cast(IntegerType()))
        .withColumn("is_successful",
            F.col("resp_code") == 0)
        .withColumn("is_reversal",
            F.col("msg_type") == 430)
        .withColumn("is_out_of_cash",
            F.col("resp_code") == 96)
        .withColumn("is_deposit",
            F.col("transaction_type").isin("Dépôt", "Deposit"))
        .withColumn("channel", F.lit("CARD_ATM"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    # FIX: Join using explicit column comparison
    if pan_map is not None:
        transformed = transformed.join(
            pan_map.select("pan", "client_id"),
            transformed.col("pan_masked") == F.col("pan"),
            how="left"
        ).drop("pan")
    else:
        transformed = transformed.withColumn("client_id", F.lit(None))

    good = transformed.filter(
        F.col("terminal_id").isNotNull() &
        F.col("amount_mad").isNotNull() &
        (F.col("amount_mad") >= 0)
    )
    bad = transformed.filter(
        F.col("terminal_id").isNull() |
        F.col("amount_mad").isNull() |
        (F.col("amount_mad") < 0)
    ).withColumn("_quarantine_reason",
        F.when(F.col("terminal_id").isNull(), "null_terminal_id")
        .when(F.col("amount_mad").isNull(), "null_amount")
        .otherwise("negative_amount"))

    quarantine(bad, "card_transactions", "_quarantine_reason")

    window = W.Window.partitionBy("refnum").orderBy(
        F.col("transaction_date").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    count = write_silver_delta(
        deduped, SILVER_CARD_TRANSACTIONS,
        "refnum", partition_by="transaction_date"
    )
    logger.info(f"✓ Card transactions: {count:,} rows → silver")
    return count


# ── 6. Wallet Transactions ────────────────────────────────────────────────────
def transform_wallet(spark: SparkSession) -> int:
    """
    Cleans mobile wallet transactions.
    """
    logger.info("Transforming wallet transactions...")

    if not os.path.exists(BRONZE_WALLET):
        logger.warning("Bronze wallet not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_WALLET)

    transformed = (
        df
        .withColumn("transaction_id",
            F.trim(F.col("transaction_id")))
        .withColumn("mobile_number_masked",
            F.trim(F.col("mobile_number_masked")))
        .withColumn("terminal_id",
            F.upper(F.trim(F.col("terminal_id"))))
        .withColumn("transaction_datetime",
            F.to_timestamp(F.col("transaction_datetime")))
        .withColumn("transaction_date",
            F.to_date(F.col("transaction_datetime")))
        .withColumn("transaction_hour",
            F.hour(F.col("transaction_datetime")))
        .withColumn("transaction_type",
            F.trim(F.col("transaction_type")))
        .withColumn("amount_mad",
            F.abs(F.col("amount_mad").cast(DoubleType())))
        .withColumn("transaction_status",
            F.trim(F.col("transaction_status")))
        .withColumn("is_reversal",
            F.col("transaction_type").contains("ANNULATION")
            | F.col("transaction_type").contains("REVERSAL"))
        .withColumn("is_cash_out",
            F.col("transaction_type").contains("RETRAIT")
            | F.col("transaction_type").contains("OUT"))
        .withColumn("is_successful",
            F.col("transaction_status").startswith("00000"))
        .withColumn("channel", F.lit("MOBILE_WALLET"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(
        F.col("transaction_id").isNotNull() &
        (F.col("amount_mad") >= 0)
    )
    bad = transformed.filter(
        F.col("transaction_id").isNull() |
        (F.col("amount_mad") < 0)
    )
    quarantine(bad, "wallet_transactions", "null_id_or_negative_amount")

    window = W.Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_datetime").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    count = write_silver_delta(
        deduped, SILVER_WALLET,
        "transaction_id", partition_by="transaction_date"
    )
    logger.info(f"✓ Wallet: {count:,} rows → silver")
    return count


# ── 7. Out of Cash ────────────────────────────────────────────────────────────
def transform_out_of_cash(spark: SparkSession) -> int:
    """
    Cleans out-of-cash events.
    """
    logger.info("Transforming out-of-cash events...")

    if not os.path.exists(BRONZE_OUT_OF_CASH):
        logger.warning("Bronze out-of-cash not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_OUT_OF_CASH)

    transformed = (
        df
        .withColumn("pan_masked",
            F.trim(F.col("pan_masked")))
        .withColumn("refnum",
            F.trim(F.col("refnum")))
        .withColumn("terminal_id",
            F.upper(F.trim(F.col("terminal_id"))))
        .withColumn("transaction_date",
            F.to_date(F.col("transaction_date")))
        .withColumn("transaction_time",
            F.col("transaction_time").cast(StringType()))
        .withColumn("transaction_hour",
            F.col("transaction_time").substr(1, 2).cast(IntegerType()))
        .withColumn("attempted_amount_mad",
            F.abs(F.col("attempted_amount_mad").cast(DoubleType())))
        .withColumn("resp_code",
            F.col("resp_code").cast(IntegerType()))
        .withColumn("failure_reason",
            F.trim(F.col("failure_reason")))
        .withColumn("is_confirmed_ooc",
            F.col("resp_code") == 96)
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(
        F.col("terminal_id").isNotNull() &
        F.col("attempted_amount_mad").isNotNull()
    )
    bad = transformed.filter(
        F.col("terminal_id").isNull() |
        F.col("attempted_amount_mad").isNull()
    )
    quarantine(bad, "out_of_cash", "null_terminal_or_amount")

    window = W.Window.partitionBy("refnum").orderBy(
        F.col("transaction_date").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    count = write_silver_delta(
        deduped, SILVER_OUT_OF_CASH,
        "refnum", partition_by="transaction_date"
    )
    logger.info(f"✓ Out-of-cash: {count:,} rows → silver")
    return count


# ── 8. Kaggle Transactions ────────────────────────────────────────────────────
def transform_kaggle_transactions(spark: SparkSession) -> int:
    """
    Cleans Kaggle financial transactions dataset.
    """
    logger.info("Transforming Kaggle transactions...")

    if not os.path.exists(BRONZE_KAGGLE_TRANSACTIONS):
        logger.warning("Bronze Kaggle transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_KAGGLE_TRANSACTIONS)

    fraud_labels_path = os.path.join(
        os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )),
        "data", "synthetic", "train_fraud_labels.json"
    )

    transformed = (
        df
        .withColumn("transaction_id",
            F.col("id").cast(StringType()))
        .withColumn("client_id",
            F.col("client_id").cast(StringType()))
        .withColumn("card_id",
            F.col("card_id").cast(StringType()))
        .withColumn("transaction_datetime",
            F.to_timestamp(F.col("date")))
        .withColumn("transaction_date",
            F.to_date(F.col("date")))
        .withColumn("transaction_hour",
            F.hour(F.to_timestamp(F.col("date"))))
        .withColumn("amount_mad",
            F.regexp_replace(
                F.col("amount").cast(StringType()),
                "[^0-9.\-]", ""
            ).cast(DoubleType()))
        .withColumn("amount_abs",
            F.abs(F.col("amount_mad")))
        .withColumn("is_negative_amount",
            F.col("amount_mad") < 0)
        .withColumn("use_chip",
            F.trim(F.col("use_chip")))
        .withColumn("merchant_id",
            F.col("merchant_id").cast(StringType()))
        .withColumn("merchant_city",
            F.trim(F.col("merchant_city")))
        .withColumn("merchant_region",
            F.trim(F.col("merchant_state")))
        .withColumn("zip",
            F.col("zip").cast(StringType()))
        .withColumn("mcc",
            F.col("mcc").cast(StringType()))
        .withColumn("errors",
            F.col("errors").cast(StringType()))
        .withColumn("is_online",
            F.col("use_chip").isin(
                "Online Transaction", "Transaction en ligne"
            ))
        .withColumn("is_chip_transaction",
            F.col("use_chip").isin(
                "Chip Transaction", "Transaction par puce"
            ))
        .withColumn("amount_bucket",
            F.when(F.col("amount_abs") < 100,   "MICRO")
            .when(F.col("amount_abs") < 500,   "SMALL")
            .when(F.col("amount_abs") < 2000,  "MEDIUM")
            .when(F.col("amount_abs") < 10000, "LARGE")
            .otherwise("VERY_LARGE"))
        .withColumn("channel",
            F.when(F.col("is_online"), "ONLINE")
            .when(F.col("is_chip_transaction"), "CARD_POS")
            .otherwise("CARD_SWIPE"))
        .withColumn("country", F.lit("Maroc"))
        .withColumn("currency", F.lit("MAD"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    if os.path.exists(fraud_labels_path):
        try:
            import json
            with open(fraud_labels_path) as f:
                fraud_dict = json.load(f)
            fraud_list = [
                {"transaction_id": str(k), "is_fraud": bool(v)}
                for k, v in fraud_dict.items()
            ]
            fraud_df = spark.createDataFrame(fraud_list)
            transformed = transformed.join(
                fraud_df, "transaction_id", "left"
            ).withColumn("is_fraud",
                F.coalesce(F.col("is_fraud"), F.lit(False))
            )
            logger.info("Fraud labels joined successfully")
        except Exception as e:
            logger.warning(f"Could not load fraud labels: {e}")
            transformed = transformed.withColumn("is_fraud", F.lit(False))
    else:
        transformed = transformed.withColumn("is_fraud", F.lit(False))

    good = transformed.filter(
        F.col("transaction_id").isNotNull() &
        F.col("client_id").isNotNull()
    )
    bad = transformed.filter(
        F.col("transaction_id").isNull() |
        F.col("client_id").isNull()
    )
    quarantine(bad, "kaggle_transactions", "null_transaction_or_client_id")

    window = W.Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_datetime").desc()
    )
    deduped = (
        good
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "id", "date", "amount", "merchant_state")
    )

    count = write_silver_delta(
        deduped, SILVER_KAGGLE_TRANSACTIONS,
        "transaction_id", partition_by="transaction_date"
    )
    logger.info(f"✓ Kaggle transactions: {count:,} rows → silver")
    return count


# ── Health check ──────────────────────────────────────────────────────────────
def silver_health_check(spark: SparkSession) -> None:
    print()
    print("=" * 75)
    print("SILVER HEALTH CHECK")
    print("=" * 75)

    table_paths = {
        "atm_master":           SILVER_ATM_MASTER,
        "customers":            SILVER_CUSTOMERS,
        "cards":                SILVER_CARDS,
        "card_transactions":    SILVER_CARD_TRANSACTIONS,
        "wallet_transactions":  SILVER_WALLET,
        "out_of_cash":          SILVER_OUT_OF_CASH,
        "kaggle_transactions":  SILVER_KAGGLE_TRANSACTIONS,
    }

    total_rows = 0
    for name, path in table_paths.items():
        try:
            if not os.path.exists(path):
                print(f"  {name:<30} NOT FOUND")
                continue
            df    = spark.read.format("delta").load(path)
            count = df.count()
            total_rows += count
            cols  = len(df.columns)
            latest = None
            if "_silver_loaded_at" in df.columns:
                latest = df.agg(
                    F.max("_silver_loaded_at")
                ).collect()[0][0]
            print(
                f"  {name:<30} "
                f"{count:>10,} rows  "
                f"{cols:>3} cols  "
                f"latest: {latest}"
            )
        except Exception as e:
            print(f"  {name:<30} ERROR: {e}")

    if os.path.exists(QUARANTINE_PATH):
        print()
        print("  QUARANTINE:")
        for entity in os.listdir(QUARANTINE_PATH):
            try:
                q_path = os.path.join(QUARANTINE_PATH, entity)
                q_df   = spark.read.parquet(q_path)
                q_count = q_df.count()
                print(f"    {entity:<28} {q_count:>10,} quarantined")
            except Exception:
                pass

    print("-" * 75)
    print(f"  {'TOTAL':<30} {total_rows:>10,} rows")
    print("=" * 75)


# ── Main ──────────────────────────────────────────────────────────────────────
def main(reset: bool = False) -> dict:
    logger.info("=" * 60)
    logger.info("Silver Pipeline — LOCAL MODE")
    logger.info(f"Reset: {reset}")
    logger.info("=" * 60)

    if reset:
        reset_silver()

    spark = get_spark()
    results = {}

    logger.info("── Transforming dimension sources ──")
    results["atm_master"] = transform_atm_master(spark)
    results["customers"]  = transform_customers(spark)
    results["cards"]      = transform_cards(spark)

    logger.info("── Building PAN → Customer mapping ──")
    pan_map = build_pan_customer_map(spark)

    logger.info("── Transforming fact sources ──")
    results["card_transactions"]   = transform_card_transactions(spark, pan_map)
    results["wallet"]              = transform_wallet(spark)
    results["out_of_cash"]         = transform_out_of_cash(spark)
    results["kaggle_transactions"] = transform_kaggle_transactions(spark)

    silver_health_check(spark)

    logger.info("Silver pipeline complete")
    logger.info(f"Summary: {results}")

    spark.stop()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver transformation — local mode"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate all silver tables"
    )
    args = parser.parse_args()
    main(reset=args.reset)
