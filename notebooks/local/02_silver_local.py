# notebooks/local/02_silver_local.py
"""
Silver Layer — Local Mode

Reads Bronze Parquet tables, applies:
- Type casting and column standardization
- Data quality validation and quarantine
- Deduplication
- Derived columns
- PAN to customer mapping
- Writes clean Delta tables to Silver layer

Run manually:
    python notebooks/local/02_silver_local.py
    python notebooks/local/02_silver_local.py --reset
"""
import argparse
import json
import logging
import os
import re
import shutil
import sys

os.environ["HADOOP_HOME"] = "E:\\hadoop"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import window as W
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)))
from data_simulation.config import (
    BRONZE_ATM_MASTER, BRONZE_CARD_TRANSACTIONS,
    BRONZE_CARDS, BRONZE_CUSTOMERS,
    BRONZE_KAGGLE_TRANSACTIONS, BRONZE_OUT_OF_CASH,
    BRONZE_WALLET,
    LOCAL_SILVER, LOG_FORMAT, LOG_LEVEL,
    QUARANTINE_PATH, SPARK_APP_NAME, SPARK_LOG_LEVEL,
)

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))
SILVER_BASE = os.path.join(BASE_DIR, "local_warehouse", "delta", "silver")
DATA_DIR = os.path.join(BASE_DIR, "data")

SILVER_ATM_MASTER = os.path.join(SILVER_BASE, "atm_master")
SILVER_CUSTOMERS = os.path.join(SILVER_BASE, "customers")
SILVER_CARDS = os.path.join(SILVER_BASE, "cards")
SILVER_CARD_TRANSACTIONS = os.path.join(SILVER_BASE, "card_transactions")
SILVER_WALLET = os.path.join(SILVER_BASE, "wallet_transactions")
SILVER_OUT_OF_CASH = os.path.join(SILVER_BASE, "out_of_cash")
SILVER_KAGGLE_TRANSACTIONS = os.path.join(SILVER_BASE, "kaggle_transactions")
SILVER_PAN_MAP = os.path.join(SILVER_BASE, "pan_customer_map")
QUARANTINE = os.path.join(SILVER_BASE, "_quarantine")

FRAUD_LABELS_FILE = os.path.join(DATA_DIR, "synthetic", "train_fraud_labels.json")

logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("silver_local")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-silver-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", "C:/tmp/spark")
        .config("spark.sql.warehouse.dir", "C:/tmp/spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


def reset_silver() -> None:
    if os.path.exists(SILVER_BASE):
        shutil.rmtree(SILVER_BASE)
        logger.info(f"Deleted silver: {SILVER_BASE}")
    os.makedirs(SILVER_BASE, exist_ok=True)
    logger.info("Silver reset complete")


def write_silver_delta(df: DataFrame, silver_path: str, merge_key: str, partition_by: str = None) -> int:
    assert silver_path and silver_path.strip(), f"silver_path is empty for merge_key={merge_key}"
    spark = df.sparkSession
    count = df.count()
    if count == 0:
        logger.warning(f"No rows to write to {silver_path}")
        return 0

    os.makedirs(silver_path, exist_ok=True)

    if not DeltaTable.isDeltaTable(spark, silver_path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by and partition_by in df.columns:
            writer = writer.partitionBy(partition_by)
        writer.save(silver_path)
        logger.info(f"Created {silver_path} ({count:,} rows)")
        return count

    delta_table = DeltaTable.forPath(spark, silver_path)
    (
        delta_table.alias("tgt")
        .merge(df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Merged {silver_path} ({count:,} rows)")
    return count


def write_quarantine(df: DataFrame, entity: str) -> None:
    try:
        bad_count = df.count()
        if bad_count == 0:
            return
        path = os.path.join(QUARANTINE, entity)
        os.makedirs(path, exist_ok=True)
        (
            df.withColumn("_quarantined_at", F.current_timestamp())
              .withColumn("_entity", F.lit(entity))
              .write.format("parquet")
              .mode("append")
              .save(path)
        )
        logger.warning(f"Quarantined {bad_count:,} {entity} records")
    except Exception as e:
        logger.error(f"Quarantine write failed for {entity}: {e}")


def transform_atm_master(spark: SparkSession) -> int:
    logger.info("Transforming ATM master...")
    if not os.path.exists(BRONZE_ATM_MASTER):
        logger.warning("Bronze ATM master not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_ATM_MASTER)
    logger.info(f"ATM master columns: {df.columns}")

    rename_map = {
        "terminal_id": "terminal_id",
        "governorate": "region",
        "region": "region",
        "type": "atm_type",
        "atm_type": "atm_type",
        "replenished_by": "provider",
        "provider": "provider",
        "replenished_from": "location_type",
        "location_type": "location_type",
        "installation_date": "installation_date",
        "limits": "cash_limit",
        "cash_limit_mad": "cash_limit",
        "cash_limit": "cash_limit",
    }

    select_exprs = []
    seen_targets = set()
    for src_col in df.columns:
        clean = re.sub(r"[ ,;{}()\n\t=]", "_", src_col).lower().strip("_")
        if clean in rename_map and rename_map[clean] not in seen_targets:
            select_exprs.append(F.col(src_col).alias(rename_map[clean]))
            seen_targets.add(rename_map[clean])
        elif clean not in rename_map and clean not in seen_targets and not clean.startswith("_"):
            select_exprs.append(F.col(src_col).alias(clean))
            seen_targets.add(clean)

    df = df.select(select_exprs)

    for col_name in ["terminal_id", "region", "atm_type", "provider", "location_type", "installation_date", "cash_limit"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))

    transformed = (
        df.withColumn("terminal_id", F.upper(F.trim(F.col("terminal_id"))))
          .withColumn("region", F.trim(F.col("region")))
          .withColumn("atm_type", F.trim(F.col("atm_type")))
          .withColumn("provider", F.trim(F.col("provider")))
          .withColumn("location_type", F.trim(F.col("location_type")))
          .withColumn("installation_date", F.to_date(F.col("installation_date").cast(StringType())))
          .withColumn("cash_limit", F.col("cash_limit").cast(DoubleType()))
          .withColumn("is_cash_deposit_enabled", F.col("atm_type").contains("Deposit") | F.col("atm_type").contains("Dépôt"))
          .withColumn("atm_age_years", F.floor(F.datediff(F.current_date(), F.col("installation_date")) / 365).cast(IntegerType()))
          .withColumn("country", F.lit("Maroc"))
          .withColumn("bank_name", F.lit("Attijariwafa Bank"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(F.col("terminal_id").isNotNull())
    bad = transformed.filter(F.col("terminal_id").isNull())
    write_quarantine(bad.withColumn("_reason", F.lit("null_terminal_id")), "atm_master")

    window = W.Window.partitionBy("terminal_id").orderBy(F.col("installation_date").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_ATM_MASTER, "terminal_id")
    logger.info(f"✓ ATM master: {count:,} rows")
    return count


def transform_customers(spark: SparkSession) -> int:
    logger.info("Transforming customers...")
    if not os.path.exists(BRONZE_CUSTOMERS):
        logger.warning("Bronze customers not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CUSTOMERS)
    logger.info(f"Customer columns: {df.columns}")

    if "id" in df.columns and "client_id" not in df.columns:
        df = df.withColumn("client_id", F.col("id").cast(StringType()))
    elif "client_id" not in df.columns:
        df = df.withColumn("client_id", F.lit(None).cast(StringType()))

    def clean_mad(col_name):
        if col_name in df.columns:
            return F.regexp_replace(F.col(col_name).cast(StringType()), r"[^0-9.\-]", "").cast(DoubleType())
        return F.lit(None).cast(DoubleType())

    transformed = (
        df.withColumn("client_id", F.col("client_id").cast(StringType()))
          .withColumn("current_age", F.col("current_age").cast(IntegerType()) if "current_age" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("retirement_age", F.col("retirement_age").cast(IntegerType()) if "retirement_age" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("birth_year", F.col("birth_year").cast(IntegerType()) if "birth_year" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("birth_month", F.col("birth_month").cast(IntegerType()) if "birth_month" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("gender", F.upper(F.trim(F.col("gender"))) if "gender" in df.columns else F.lit(None).cast(StringType()))
          .withColumn("address", F.trim(F.col("address")) if "address" in df.columns else F.lit(None).cast(StringType()))
          .withColumn("latitude", F.col("latitude").cast(DoubleType()) if "latitude" in df.columns else F.lit(None).cast(DoubleType()))
          .withColumn("longitude", F.col("longitude").cast(DoubleType()) if "longitude" in df.columns else F.lit(None).cast(DoubleType()))
          .withColumn("per_capita_income", clean_mad("per_capita_income"))
          .withColumn("yearly_income", clean_mad("yearly_income"))
          .withColumn("total_debt", clean_mad("total_debt"))
          .withColumn("credit_score", F.col("credit_score").cast(IntegerType()) if "credit_score" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("num_credit_cards", F.col("num_credit_cards").cast(IntegerType()) if "num_credit_cards" in df.columns else F.lit(None).cast(IntegerType()))
          .withColumn("age_group", F.when(F.col("current_age") < 30, "18-29").when(F.col("current_age") < 45, "30-44").when(F.col("current_age") < 60, "45-59").otherwise("60+"))
          .withColumn("income_segment", F.when(F.col("yearly_income") < 60000, "LOW").when(F.col("yearly_income") < 150000, "MEDIUM").when(F.col("yearly_income") < 300000, "HIGH").otherwise("PREMIUM"))
          .withColumn("credit_tier", F.when(F.col("credit_score") >= 750, "EXCELLENT").when(F.col("credit_score") >= 670, "GOOD").when(F.col("credit_score") >= 580, "FAIR").otherwise("POOR"))
          .withColumn("debt_to_income_ratio", F.when(F.col("yearly_income") > 0, F.round(F.col("total_debt") / F.col("yearly_income"), 2)).otherwise(F.lit(None).cast(DoubleType())))
          .withColumn("country", F.lit("Maroc"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(F.col("client_id").isNotNull())
    bad = transformed.filter(F.col("client_id").isNull())
    write_quarantine(bad.withColumn("_reason", F.lit("null_client_id")), "customers")

    window = W.Window.partitionBy("client_id").orderBy(F.col("credit_score").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    if "id" in deduped.columns:
        deduped = deduped.drop("id")

    count = write_silver_delta(deduped, SILVER_CUSTOMERS, "client_id")
    logger.info(f"✓ Customers: {count:,} rows")
    return count


def transform_cards(spark: SparkSession) -> int:
    logger.info("Transforming cards...")
    if not os.path.exists(BRONZE_CARDS):
        logger.warning("Bronze cards not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARDS)
    logger.info(f"Cards columns: {df.columns}")

    if "id" in df.columns and "card_id" not in df.columns:
        df = df.withColumn("card_id", F.col("id").cast(StringType()))
    elif "card_id" not in df.columns:
        df = df.withColumn("card_id", F.lit(None).cast(StringType()))

    if "client_id" not in df.columns:
        df = df.withColumn("client_id", F.lit(None).cast(StringType()))

    def clean_mad(col_name):
        if col_name in df.columns:
            return F.regexp_replace(F.col(col_name).cast(StringType()), r"[^0-9.\-]", "").cast(DoubleType())
        return F.lit(None).cast(DoubleType())

    def get_col(col_name, default=None, cast_type=StringType()):
        if col_name in df.columns:
            return F.col(col_name).cast(cast_type)
        return F.lit(default).cast(cast_type)

    transformed = (
        df.withColumn("card_id", F.col("card_id").cast(StringType()))
          .withColumn("client_id", F.col("client_id").cast(StringType()))
          .withColumn("card_brand", F.upper(F.trim(get_col("card_brand"))))
          .withColumn("card_type", F.upper(F.trim(get_col("card_type"))))
          .withColumn("card_number_masked", get_col("card_number"))
          .withColumn("has_chip", F.upper(F.trim(get_col("has_chip"))).isin("YES", "OUI", "TRUE", "1"))
          .withColumn("num_cards_issued", get_col("num_cards_issued", 1, IntegerType()))
          .withColumn("credit_limit", clean_mad("credit_limit"))
          .withColumn("acct_open_date", F.to_date(get_col("acct_open_date"), "MM/yyyy"))
          .withColumn("year_pin_last_changed", get_col("year_pin_last_changed", None, IntegerType()))
          .withColumn("card_on_dark_web", F.upper(F.trim(get_col("card_on_dark_web"))).isin("YES", "OUI", "TRUE", "1"))
          .withColumn("expires_raw", get_col("expires"))
          .withColumn("expires_date", F.to_date(F.col("expires_raw"), "MM/yyyy"))
          .withColumn("is_expired", F.col("expires_date") < F.current_date())
          .withColumn("card_age_years", F.floor(F.datediff(F.current_date(), F.col("acct_open_date")) / 365).cast(IntegerType()))
          .withColumn("dark_web_risk", F.when(F.col("card_on_dark_web"), "HIGH").otherwise("LOW"))
          .withColumn("card_category", F.when(F.col("card_type").isin("CREDIT", "CRÉDIT"), "CREDIT").when(F.col("card_type").isin("DEBIT", "DÉBIT"), "DEBIT").otherwise("OTHER"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .drop("expires_raw", "card_number", "cvv", "expires")
    )

    if "id" in transformed.columns:
        transformed = transformed.drop("id")

    good = transformed.filter(F.col("card_id").isNotNull() & F.col("client_id").isNotNull())
    bad = transformed.filter(F.col("card_id").isNull() | F.col("client_id").isNull())
    write_quarantine(bad.withColumn("_reason", F.lit("null_card_or_client_id")), "cards")

    window = W.Window.partitionBy("card_id").orderBy(F.col("acct_open_date").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_CARDS, "card_id")
    logger.info(f"✓ Cards: {count:,} rows")
    return count


def build_pan_customer_map(spark: SparkSession) -> DataFrame:
    logger.info("Building PAN → customer mapping...")
    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found")
        return None
    if not os.path.exists(SILVER_CUSTOMERS):
        logger.warning("Silver customers not found")
        return None

    pans_df = (
        spark.read.parquet(BRONZE_CARD_TRANSACTIONS)
        .select(F.col("pan").alias("pan_masked"))
        .distinct()
        .withColumn("pan_num", F.regexp_extract(F.col("pan_masked").cast(StringType()), r"(\d+)", 1).cast(LongType()))
        .filter(F.col("pan_num") > 0)
    )

    customers_df = (
        spark.read.format("delta").load(SILVER_CUSTOMERS)
        .select("client_id")
        .distinct()
        .withColumn("row_idx", F.monotonically_increasing_id())
    )

    customer_count = customers_df.count()
    if customer_count == 0:
        logger.warning("No customers in silver — PAN map empty")
        return None

    pan_mapped = pans_df.withColumn("customer_idx", F.col("pan_num") % customer_count)
    customers_with_idx = customers_df.withColumn("customer_idx", F.col("row_idx") % customer_count)

    pan_map = (
        pan_mapped
        .join(customers_with_idx.select("client_id", "customer_idx"), "customer_idx", "left")
        .select("pan_masked", "pan_num", "client_id")
        .withColumn("_created_at", F.current_timestamp())
    )

    os.makedirs(SILVER_PAN_MAP, exist_ok=True)
    pan_map.write.format("delta").mode("overwrite").save(SILVER_PAN_MAP)
    return pan_map


def transform_card_transactions(spark: SparkSession, pan_map: DataFrame) -> int:
    logger.info("Transforming ATM card transactions...")
    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARD_TRANSACTIONS)
    logger.info(f"Card transaction columns: {df.columns}")

    def get_col(col_name, default=None, cast_type=StringType()):
        if col_name in df.columns:
            return F.col(col_name).cast(cast_type)
        return F.lit(default).cast(cast_type)

    transformed = (
        df.withColumn("pan", F.trim(get_col("pan")))
          .withColumn("refnum", F.trim(get_col("refnum")))
          .withColumn("terminal_id", F.upper(F.trim(get_col("termid"))))
          .withColumn("transaction_date", F.to_date(get_col("date")))
          .withColumn("transaction_time_raw", get_col("time"))
          .withColumn("transaction_hour", F.substring(F.col("transaction_time_raw"), 1, 2).cast(IntegerType()))
          .withColumn("transaction_type", F.trim(get_col("descr")))
          .withColumn("msg_type", get_col("msgtype", 210, IntegerType()))
          .withColumn("amount_mad", F.abs(get_col("amount", 0.0, DoubleType())))
          .withColumn("resp_code", get_col("respcode", 0, IntegerType()))
          .withColumn("is_successful", F.col("resp_code") == 0)
          .withColumn("is_reversal", F.col("msg_type") == 430)
          .withColumn("is_out_of_cash", F.col("resp_code") == 96)
          .withColumn("is_deposit", F.col("transaction_type").isin("Dépôt", "Deposit"))
          .withColumn("channel", F.lit("CARD_ATM"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .drop("transaction_time_raw")
    )

    if pan_map is not None:
        transformed = (
            transformed.join(
                pan_map.select(F.col("pan_masked").alias("pan"), "client_id"),
                on="pan",
                how="left"
            )
            .drop("pan")
        )
    else:
        transformed = transformed.withColumn("client_id", F.lit(None).cast(StringType()))

    good = transformed.filter(
        F.col("terminal_id").isNotNull() &
        F.col("amount_mad").isNotNull() &
        (F.col("amount_mad") >= 0)
    )
    bad = transformed.filter(
        F.col("terminal_id").isNull() |
        F.col("amount_mad").isNull() |
        (F.col("amount_mad") < 0)
    ).withColumn(
        "_reason",
        F.when(F.col("terminal_id").isNull(), "null_terminal_id")
        .when(F.col("amount_mad").isNull(), "null_amount")
        .otherwise("negative_amount")
    )
    write_quarantine(bad, "card_transactions")

    window = W.Window.partitionBy("refnum").orderBy(F.col("transaction_date").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_CARD_TRANSACTIONS, "refnum", partition_by="transaction_date")
    logger.info(f"✓ Card transactions: {count:,} rows")
    return count


def transform_wallet(spark: SparkSession) -> int:
    logger.info("Transforming wallet transactions...")
    if not os.path.exists(BRONZE_WALLET):
        logger.warning("Bronze wallet not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_WALLET)
    logger.info(f"Wallet columns: {df.columns}")

    def get_col(col_name, default=None, cast_type=StringType()):
        if col_name in df.columns:
            return F.col(col_name).cast(cast_type)
        return F.lit(default).cast(cast_type)

    transformed = (
        df.withColumn("transaction_id", F.trim(get_col("transaction_id")))
          .withColumn("mobile_number_masked", F.trim(get_col("mobile_number")))
          .withColumn("terminal_id", F.upper(F.trim(get_col("term_id"))))
          .withColumn("transaction_datetime", F.to_timestamp(get_col("transaction_date")))
          .withColumn("transaction_date", F.to_date(F.col("transaction_datetime")))
          .withColumn("transaction_hour", F.hour(F.col("transaction_datetime")))
          .withColumn("transaction_type", F.trim(get_col("transaction_type")))
          .withColumn("amount_mad", F.abs(get_col("transaction_amount", 0.0, DoubleType())))
          .withColumn("transaction_status", F.trim(get_col("transaction_status")))
          .withColumn("is_reversal", F.col("transaction_type").contains("ANNULATION") | F.col("transaction_type").contains("REVERSAL"))
          .withColumn("is_cash_out", F.col("transaction_type").contains("RETRAIT") | F.col("transaction_type").contains("OUT"))
          .withColumn("is_successful", F.col("transaction_status").startswith("00000"))
          .withColumn("channel", F.lit("MOBILE_WALLET"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(F.col("transaction_id").isNotNull() & (F.col("amount_mad") >= 0))
    bad = transformed.filter(F.col("transaction_id").isNull() | (F.col("amount_mad") < 0)).withColumn("_reason", F.lit("null_id_or_negative_amount"))
    write_quarantine(bad, "wallet_transactions")

    window = W.Window.partitionBy("transaction_id").orderBy(F.col("transaction_datetime").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_WALLET, "transaction_id", partition_by="transaction_date")
    logger.info(f"✓ Wallet: {count:,} rows")
    return count


def transform_out_of_cash(spark: SparkSession) -> int:
    logger.info("Transforming out-of-cash events...")
    if not os.path.exists(BRONZE_OUT_OF_CASH):
        logger.warning("Bronze out-of-cash not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_OUT_OF_CASH)
    logger.info(f"OOC columns: {df.columns}")

    def get_col(col_name, default=None, cast_type=StringType()):
        if col_name in df.columns:
            return F.col(col_name).cast(cast_type)
        return F.lit(default).cast(cast_type)

    transformed = (
        df.withColumn("pan", F.trim(get_col("pan")))
          .withColumn("refnum", F.trim(get_col("refnum")))
          .withColumn("terminal_id", F.upper(F.trim(get_col("termid"))))
          .withColumn("transaction_date", F.to_date(get_col("date")))
          .withColumn("transaction_time", get_col("time"))
          .withColumn("transaction_hour", F.substring(F.col("transaction_time"), 1, 2).cast(IntegerType()))
          .withColumn("attempted_amount_mad", F.regexp_replace(get_col("amount").cast(StringType()), r"[^0-9.\-]", "").cast(DoubleType()))
          .withColumn("resp_code", get_col("respcode", 96, IntegerType()))
          .withColumn("failure_reason", F.coalesce(get_col("descr"), F.lit("GUICHET VIDE")))
          .withColumn("is_confirmed_ooc", F.col("resp_code") == 96)
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    good = transformed.filter(
        F.col("refnum").isNotNull() &
        F.col("terminal_id").isNotNull() &
        F.col("attempted_amount_mad").isNotNull()
    )
    bad = transformed.filter(
        F.col("refnum").isNull() |
        F.col("terminal_id").isNull() |
        F.col("attempted_amount_mad").isNull()
    ).withColumn("_reason", F.lit("null_required_field"))
    write_quarantine(bad, "out_of_cash")

    window = W.Window.partitionBy("refnum").orderBy(F.col("transaction_date").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_OUT_OF_CASH, "refnum", partition_by="transaction_date")
    logger.info(f"✓ Out-of-cash: {count:,} rows")
    return count


def transform_kaggle_transactions(spark: SparkSession) -> int:
    logger.info("Transforming Kaggle transactions...")
    if not os.path.exists(BRONZE_KAGGLE_TRANSACTIONS):
        logger.warning("Bronze Kaggle transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_KAGGLE_TRANSACTIONS)
    logger.info(f"Kaggle columns: {df.columns}")

    def get_col(col_name, default=None, cast_type=StringType()):
        if col_name in df.columns:
            return F.col(col_name).cast(cast_type)
        return F.lit(default).cast(cast_type)

    if "id" in df.columns and "transaction_id" not in df.columns:
        df = df.withColumn("transaction_id", F.col("id").cast(StringType()))
    elif "transaction_id" not in df.columns:
        df = df.withColumn("transaction_id", F.lit(None).cast(StringType()))

    amount_col = "amount" if "amount" in df.columns else "amount_mad"

    transformed = (
        df.withColumn("transaction_id", F.col("transaction_id").cast(StringType()))
          .withColumn("client_id", get_col("client_id"))
          .withColumn("card_id", get_col("card_id"))
          .withColumn("transaction_datetime", F.to_timestamp(get_col("date")))
          .withColumn("transaction_date", F.to_date(get_col("date")))
          .withColumn("transaction_hour", F.hour(F.to_timestamp(get_col("date"))))
          .withColumn("amount_mad", F.regexp_replace(F.col(amount_col).cast(StringType()), r"[^0-9.\-]", "").cast(DoubleType()))
          .withColumn("amount_abs", F.abs(F.col("amount_mad")))
          .withColumn("is_negative_amount", F.col("amount_mad") < 0)
          .withColumn("use_chip", F.trim(get_col("use_chip")))
          .withColumn("merchant_id", get_col("merchant_id"))
          .withColumn("merchant_city", F.trim(get_col("merchant_city")))
          .withColumn("merchant_region", F.trim(F.coalesce(get_col("merchant_state"), get_col("merchant_region"))))
          .withColumn("zip", get_col("zip"))
          .withColumn("mcc", get_col("mcc"))
          .withColumn("errors", get_col("errors"))
          .withColumn("is_online", F.col("use_chip").isin("Online Transaction", "Transaction en ligne"))
          .withColumn("is_chip_transaction", F.col("use_chip").isin("Chip Transaction", "Transaction par puce"))
          .withColumn("amount_bucket", F.when(F.col("amount_abs") < 100, "MICRO").when(F.col("amount_abs") < 500, "SMALL").when(F.col("amount_abs") < 2000, "MEDIUM").when(F.col("amount_abs") < 10000, "LARGE").otherwise("VERY_LARGE"))
          .withColumn("channel", F.when(F.col("is_online"), "ONLINE").when(F.col("is_chip_transaction"), "CARD_POS").otherwise("CARD_SWIPE"))
          .withColumn("country", F.lit("Maroc"))
          .withColumn("currency", F.lit("MAD"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    if os.path.exists(FRAUD_LABELS_FILE):
        try:
            with open(FRAUD_LABELS_FILE) as f:
                fraud_dict = json.load(f)
            fraud_rows = [{"transaction_id": str(k), "is_fraud": bool(v)} for k, v in fraud_dict.items()]
            fraud_df = spark.createDataFrame(fraud_rows)
            transformed = transformed.join(fraud_df, "transaction_id", "left").withColumn("is_fraud", F.coalesce(F.col("is_fraud"), F.lit(False)))
        except Exception as e:
            logger.warning(f"Could not load fraud labels: {e}")
            transformed = transformed.withColumn("is_fraud", F.lit(False))
    else:
        transformed = transformed.withColumn("is_fraud", F.lit(False))

    for col_to_drop in ["id", "date", "amount", "merchant_state"]:
        if col_to_drop in transformed.columns:
            transformed = transformed.drop(col_to_drop)

    good = transformed.filter(F.col("transaction_id").isNotNull() & F.col("client_id").isNotNull())
    bad = transformed.filter(F.col("transaction_id").isNull() | F.col("client_id").isNull()).withColumn("_reason", F.lit("null_transaction_or_client_id"))
    write_quarantine(bad, "kaggle_transactions")

    window = W.Window.partitionBy("transaction_id").orderBy(F.col("transaction_datetime").desc_nulls_last())
    deduped = good.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    count = write_silver_delta(deduped, SILVER_KAGGLE_TRANSACTIONS, "transaction_id", partition_by="transaction_date")
    logger.info(f"✓ Kaggle transactions: {count:,} rows")
    return count


def silver_health_check(spark: SparkSession) -> None:
    print()
    print("=" * 75)
    print("SILVER HEALTH CHECK")
    print("=" * 75)

    tables = {
        "atm_master": SILVER_ATM_MASTER,
        "customers": SILVER_CUSTOMERS,
        "cards": SILVER_CARDS,
        "card_transactions": SILVER_CARD_TRANSACTIONS,
        "wallet_transactions": SILVER_WALLET,
        "out_of_cash": SILVER_OUT_OF_CASH,
        "kaggle_transactions": SILVER_KAGGLE_TRANSACTIONS,
        "pan_customer_map": SILVER_PAN_MAP,
    }

    total_rows = 0
    for name, path in tables.items():
        try:
            if not os.path.exists(path):
                print(f"  {name:<30} NOT FOUND")
                continue
            try:
                df = spark.read.format("delta").load(path)
            except Exception:
                df = spark.read.parquet(path)
            count = df.count()
            total_rows += count
            cols = len(df.columns)
            latest = None
            ts_col = "_silver_loaded_at" if "_silver_loaded_at" in df.columns else "_created_at"
            if ts_col in df.columns:
                latest = df.agg(F.max(ts_col)).collect()[0][0]
            print(f"  {name:<30} {count:>10,} rows  {cols:>3} cols  latest: {latest}")
        except Exception as e:
            print(f"  {name:<30} ERROR: {e}")

    if os.path.exists(QUARANTINE):
        print()
        print("  QUARANTINE:")
        for entity in os.listdir(QUARANTINE):
            try:
                q_path = os.path.join(QUARANTINE, entity)
                q_df = spark.read.parquet(q_path)
                print(f"    {entity:<28} {q_df.count():>10,} records")
            except Exception:
                pass

    print("-" * 75)
    print(f"  {'TOTAL':<30} {total_rows:>10,} rows")
    print("=" * 75)


def main(reset: bool = False) -> dict:
    logger.info("=" * 60)
    logger.info("Silver Pipeline — LOCAL MODE")
    logger.info(f"Reset: {reset}")
    logger.info("=" * 60)

    if reset:
        reset_silver()

    spark = get_spark()
    results = {}

    logger.info("── Transforming dimensions ──")
    results["atm_master"] = transform_atm_master(spark)
    results["customers"] = transform_customers(spark)
    results["cards"] = transform_cards(spark)

    logger.info("── Building PAN → Customer map ──")
    pan_map = build_pan_customer_map(spark)

    logger.info("── Transforming facts ──")
    results["card_transactions"] = transform_card_transactions(spark, pan_map)
    results["wallet"] = transform_wallet(spark)
    results["out_of_cash"] = transform_out_of_cash(spark)
    results["kaggle_transactions"] = transform_kaggle_transactions(spark)

    silver_health_check(spark)

    logger.info("Silver pipeline complete")
    logger.info(f"Summary: {results}")

    spark.stop()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver transformation — local mode")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all silver tables")
    args = parser.parse_args()
    main(reset=args.reset)
