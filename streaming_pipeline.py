import os
import shutil
import datetime
import time
from threading import Thread

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit

from schema import get_schema
from spark_dynamic_pipeline import (
    clean_and_cast,
    add_error_column,
    finalize_good_data,
    write_to_postgres
)

from insight_store import generate_insights_from_df, save_insights
from ai_insight_engine.insight_engine import generate_ai_insights

# =========================
# CONFIG
# =========================

KAFKA_BOOTSTRAP = "localhost:9092"   # use "kafka:9092" if running inside docker
TOPIC = "transactions"

CHECKPOINT_PATH = "checkpoint/main"
BAD_DATA_PATH = "data/bad_records"

TOPIC = "transactions"

# For local testing, remove old checkpoint offsets so the stream replays existing Kafka data.
# In production, set this to False once the stream has a stable checkpoint state.
RESET_CHECKPOINTS = True

ENABLE_DEBUG_CONSOLE = False     # 🔥 toggle debug output
ENABLE_IDLE_TIMEOUT = True      # 🔥 auto stop for testing
IDLE_TIMEOUT = 300               # seconds

last_data_time = time.time()

def clear_checkpoints():
    if os.path.exists(CHECKPOINT_PATH):
        print(f"🧹 Clearing existing checkpoint path: {CHECKPOINT_PATH}")
        shutil.rmtree(CHECKPOINT_PATH)


# =========================
# SPARK SESSION
# =========================

os.makedirs(BAD_DATA_PATH, exist_ok=True)

if RESET_CHECKPOINTS:
    clear_checkpoints()

spark = SparkSession.builder \
    .appName("Kafka Streaming Pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = get_schema()

# =========================
# READ FROM KAFKA
# =========================

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# =========================
# PARSE JSON
# =========================

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# =========================
# DEBUG STREAM (Kafka → Spark visibility)
# =========================

if ENABLE_DEBUG_CONSOLE:
    debug_query = df_parsed.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

# =========================
# TRANSFORMATIONS
# =========================

df_clean = clean_and_cast(df_parsed)
df_with_errors = add_error_column(df_clean)

df_with_errors = df_with_errors.withColumn("ingestion_type", lit("streaming"))

# =========================
# BATCH PROCESSING
# =========================

def process_batch(batch_df, batch_id):
    global last_data_time

    try:
        if batch_df.rdd.isEmpty():
            return

        good_df = batch_df.filter(col("error_reason").isNull()).drop("error_reason")
        bad_df = batch_df.filter(col("error_reason").isNotNull()).drop("error_reason")

        good_error = None
        bad_error = None

        if not good_df.rdd.isEmpty():
            
            last_data_time = time.time()

            final_df = finalize_good_data(good_df,ingestion_type="kafka_streaming",source_file=TOPIC)

            # =========================
            # 1. Write to Postgres
            # =========================
            try:
                write_to_postgres(final_df)
            except Exception as exc:
                good_error = exc
                print(f"❌ Failed to write good batch {batch_id} to Postgres: {exc}")

            # =========================
            # 2. Generate insights
            # =========================
            try:
                insight_json = generate_insights_from_df(final_df)

                if insight_json:

                    raw_transactions = []

                    for r in insight_json["insights"]:
                        raw_transactions.append({
                            "transaction_date": str(r["transaction_date"]),
                            "deposit": r.get("total_deposit", 0),
                            "withdrawal": r.get("total_withdrawal", 0),
                            "balance": r.get("avg_balance", 0),
                            "source": "kafka"
                        })

                    # =========================
                    # 3. AI Summary
                    # =========================
                    insights = generate_ai_insights(
                        source_date=insight_json["source_date"],
                        raw_transactions=raw_transactions   # ✅ correct
                    )

                    # =========================
                    # 4. Final JSON merge
                    # =========================
                    final_output = {
                        **insight_json,
                        "ai_summary": insights.get("ai_insights"),
                        "mode": insights.get("mode")
                    }

                    # =========================
                    # 5. Save JSON
                    # =========================
                    save_insights(final_output, "kafka_stream")

                    print(f"📊 Insights + AI generated for batch {batch_id}")

            except Exception as e:
                print(f"⚠️ Insight generation failed batch {batch_id}: {e}")

        if not bad_df.rdd.isEmpty():
            
            last_data_time = time.time()
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_path = os.path.join(BAD_DATA_PATH, f"kafka_bad_rec_{ts}_tmp")
            final_file = os.path.join(BAD_DATA_PATH, f"kafka_bad_rec_{ts}.csv")

            try:
                bad_df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", True) \
                    .csv(temp_path)

                part_files = [f for f in os.listdir(temp_path) if f.startswith("part-") and f.endswith(".csv")]
                if not part_files:
                    raise RuntimeError(f"No part file found in {temp_path}")

                os.replace(
                    os.path.join(temp_path, part_files[0]),
                    final_file
                )
                shutil.rmtree(temp_path)
            except Exception as exc:
                bad_error = exc
                print(f"❌ Failed to write bad batch {batch_id} to file: {exc}")

        if good_error or bad_error:
            print(f"⚠️ Batch {batch_id} completed with errors: good_error={good_error}, bad_error={bad_error}")
    except Exception as exc:
        print(f"❌ Batch {batch_id} failed unexpectedly: {exc}")
        print("Continuing stream despite batch failure")

main_query = df_with_errors.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

# =========================
# AUTO-STOP MONITOR (for testing)
# =========================

def monitor_and_stop():
    global last_data_time

    while True:
        time.sleep(5)

        idle_time = time.time() - last_data_time

        if idle_time > IDLE_TIMEOUT:
            print(f"⏹️ No data for {IDLE_TIMEOUT}s. Stopping all streams...")

            for q in spark.streams.active:
                q.stop()

            break

if ENABLE_IDLE_TIMEOUT:
    monitor_thread = Thread(target=monitor_and_stop, daemon=True)
    monitor_thread.start()

# =========================
# START STREAM
# =========================

print("🚀 Streaming started... Waiting for Kafka data...")

spark.streams.awaitAnyTermination()