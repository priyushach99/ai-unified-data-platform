# spark_dynamic_pipeline.py

import os
import sys
import datetime
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, to_date, regexp_replace,
    when, lit, trim, current_date
)

from schema import get_schema
from db_utils import (
    get_db_config,
    get_jdbc_url,
    table_exists,
    get_db_columns,
    add_missing_columns
)

from insight_store import generate_insights_from_df, save_insights
from ai_insight_engine.insight_engine import generate_ai_insights

# =========================
# CONFIG
# =========================

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/input_file"
BAD_DATA_PATH = "data/bad_records"
TABLE_NAME = "transactions"

# =========================
# SPARK
# =========================

spark = SparkSession.builder \
    .appName("Dynamic ETL Pipeline") \
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = get_schema()

# =========================
# READ FILES
# =========================

def read_input_files(path):

    if not os.path.exists(path):
        raise Exception(f"❌ Input path does not exist: {path}")

    dfs = []

    for file in os.listdir(path):
        full_path = os.path.join(path, file)

        if file.endswith((".csv", ".txt")):
            df = spark.read \
                .option("header", True) \
                .option("inferSchema", False) \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv(full_path)

        elif file.endswith(".parquet"):
            df = spark.read.parquet(full_path)

        else:
            continue

        df = df.withColumn("source_file", lit(file))
        dfs.append(df)

    if not dfs:
        raise Exception("❌ No valid input files found")

    df_final = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs
    )

    df_final = normalize_columns(df_final)
    df_final = map_columns(df_final)

    return df_final


def normalize_columns(df):
    for c in df.columns:
        new_c = c.strip().lower()

        new_c = new_c.replace(" ", "_") \
                     .replace(".", "") \
                     .replace("__", "_")

        df = df.withColumnRenamed(c, new_c)

    return df


def map_columns(df):

    column_mapping = {
        "account_no": "account_no",
        "transaction_date": "transaction_date",
        "transaction_details": "transaction_details",
        "chqno": "chqno",
        "chqno_": "chqno",
        "chq_no": "chqno",
        "withdrawal_amt": "withdrawal_amt",
        "deposit_amt": "deposit_amt",
        "balance_amt": "balance_amt",
        "value_date": "value_date"
    }

    for src, tgt in column_mapping.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, tgt)

    return df


# =========================
# CLEAN & CAST
# =========================

from pyspark.sql.functions import expr

def clean_and_cast(df):

    for c in df.columns:
        df = df.withColumn(
            c,
            trim(
                regexp_replace(
                    regexp_replace(col(c), r"[\"']", ""),
                    r"\s+", " "
                )
            )
        )

    numeric_cols = ["withdrawal_amt", "deposit_amt", "balance_amt"]

    for c in numeric_cols:
        df = df.withColumn(
            c,
            regexp_replace(col(c), ",", "")
        )

        df = df.withColumn(
            f"{c}_clean",
            expr(f"try_cast({c} as double)")
        )

    df = df.withColumn(
        "transaction_date_clean",
        expr("try_to_date(transaction_date, 'dd-MMM-yy')")
    )

    return df


# =========================
# ERROR HANDLING
# =========================

def add_error_column(df):

    return df.withColumn(
        "error_reason",
        when(col("transaction_date_clean").isNull(), "Invalid Date")
        .when(trim(col("account_no")) == "", "Missing Account")
        .when(
            (trim(col("withdrawal_amt")) != "") &
            col("withdrawal_amt_clean").isNull(),
            "Invalid Withdrawal Amt"
        )
        .when(
            (trim(col("deposit_amt")) != "") &
            col("deposit_amt_clean").isNull(),
            "Invalid Deposit Amt"
        )
        .otherwise(None)
    )


def split_data(df):

    bad_df = df.filter(col("error_reason").isNotNull())
    good_df = df.filter(col("error_reason").isNull()).drop("error_reason")

    return good_df, bad_df


# =========================
# FINAL TRANSFORMATION
# =========================


def finalize_good_data(df, ingestion_type=None, source_file=None):

    return df \
        .withColumn("transaction_date", col("transaction_date_clean")) \
        .withColumn("withdrawal_amt", col("withdrawal_amt_clean")) \
        .withColumn("deposit_amt", col("deposit_amt_clean")) \
        .withColumn("balance_amt", col("balance_amt_clean")) \
        .withColumn("ingestion_type", lit(ingestion_type)) \
        .withColumn("source_file", lit(source_file)) \
        .withColumn("source_date", current_date()) \
        .drop(
            "transaction_date_clean",
            "withdrawal_amt_clean",
            "deposit_amt_clean",
            "balance_amt_clean"
        )


# =========================
# SCHEMA EVOLUTION
# =========================

def handle_schema_evolution(df):

    if not table_exists(TABLE_NAME):
        print(f"⚠️ Table '{TABLE_NAME}' does not exist. It will be created when first data is written.")
        return

    db_cols = get_db_columns(TABLE_NAME)
    new_cols = [c for c in df.columns if c not in db_cols]

    if new_cols:
        print(f"➕ Adding new columns to DB: {new_cols}")
        add_missing_columns(TABLE_NAME, new_cols)


# =========================
# WRITE BAD DATA
# =========================

def write_bad_data(df):

    if df.rdd.isEmpty():
        print("ℹ️ No bad records to write")
        return

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{BAD_DATA_PATH}/bad_records_{ts}.csv"

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"{BAD_DATA_PATH}/temp_bad_{ts}")

    temp_dir = f"{BAD_DATA_PATH}/temp_bad_{ts}"
    for file in os.listdir(temp_dir):
        if file.startswith("part-") and file.endswith(".csv"):
            os.rename(
                os.path.join(temp_dir, file),
                output_path
            )

    import shutil
    shutil.rmtree(temp_dir)

    print(f"❌ Bad records written to: {output_path}")


# =========================
# WRITE TO POSTGRES
# =========================

def write_to_postgres(df):

    count = df.count()
    if count == 0:
        return

    expected_columns = [
        "account_no",
        "transaction_date",
        "transaction_details",
        "chqno",
        "value_date",
        "withdrawal_amt",
        "deposit_amt",
        "balance_amt",
        "ingestion_type",
        "source_file",
        "source_date"
    ]

    available_columns = [c for c in expected_columns if c in df.columns]
    df_to_write = df.select(*available_columns)

    cfg = get_db_config()
    jdbc_url = get_jdbc_url()

    try:
        df_to_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", TABLE_NAME) \
            .option("user", cfg["user"]) \
            .option("password", cfg["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    except Exception as exc:
        print(f"❌ Postgres write failed: {exc}")
        raise


# =========================
# MAIN
# =========================

def main():

    print(f"📥 Reading from: {INPUT_PATH}")

    df_raw = read_input_files(INPUT_PATH)

    print("📊 Columns after normalization:")
    print(df_raw.columns)

    df_raw.show(5, False)

    print("🧼 Cleaning data...")
    df_clean = clean_and_cast(df_raw)

    print("🚨 Identifying bad records...")
    df_with_errors = add_error_column(df_clean)

    good_df, bad_df = split_data(df_with_errors)

    good_df = finalize_good_data(good_df,ingestion_type="Spark batch processing",source_file=INPUT_PATH)

    bad_df = bad_df.withColumn(
        "ingestion_type",
        lit("Spark batch processing")
    )

    good_count = good_df.cache().count()
    bad_count = bad_df.count()

    print(f"✅ Good records: {good_count}")
    print(f"❌ Bad records: {bad_count}")

    print("💾 Writing bad data...")
    write_bad_data(bad_df)

    if good_count == 0:
        print("⚠️ No valid records to load into PostgreSQL. Skipping DB operations.")
    else:
        print("🧠 Handling schema evolution...")
        handle_schema_evolution(good_df)

        print("🛢️ Writing to PostgreSQL...")
        write_to_postgres(good_df)

        # 🔥 FINAL CHANGE: INSIGHT GENERATION (correct place)
        print("📊 Generating base insights...")

        insight_json = generate_insights_from_df(good_df)

        print("🤖 Generating AI summary...")

        raw_transactions = []

        for r in insight_json["insights"]:
            raw_transactions.append({
                "transaction_date": str(r["transaction_date"]),
                "deposit": r.get("total_deposit", 0),
                "withdrawal": r.get("total_withdrawal", 0),
                "balance": r.get("avg_balance", 0),
                "total_transactions": r.get("total_transactions", 1),  # ← add this
                "source": "spark"
            })

        insights = generate_ai_insights(
            source_date=insight_json["source_date"],
            raw_transactions=raw_transactions   # ✅ correct
        )

        final_output = {
            **insight_json,
            "ai_summary": insights.get("ai_insights"),
            "mode": insights.get("mode")
        }

        save_insights(final_output, "spark_batch")

    print("🎉 ETL Completed Successfully")


if __name__ == "__main__":
    main()