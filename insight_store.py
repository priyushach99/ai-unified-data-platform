import os
import json
import datetime
from pyspark.sql.functions import col, sum as _sum, avg, count, current_date

INSIGHT_DIR = "insights"
ARCHIVE_DIR = "insights/archive"

os.makedirs(INSIGHT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def generate_insights_from_df(df):

    data = build_insights(df)

    if not data:
        return None

    return data

def save_insights(data, ingestion_type):

    file_name = f"insight_{ingestion_type}_{data['source_date']}.json"
    file_path = os.path.join(INSIGHT_DIR, file_name)

    # load existing file if present
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            existing = json.load(f)
    else:
        existing = {
            "source_date": data["source_date"],
            "generated_at": data["generated_at"],
            "insights": []
        }

    # index existing by transaction_date
    existing_map = {
        r["transaction_date"]: r
        for r in existing.get("insights", [])
    }

    # merge new data (upsert logic)
    for row in data.get("insights", []):
        existing_map[row["transaction_date"]] = row

    merged_list = list(existing_map.values())

    final_data = {
        "source_date": data["source_date"],
        "generated_at": data["generated_at"],
        "insights": merged_list
    }

    with open(file_path, "w") as f:
        json.dump(final_data, f, indent=2)

    print(f"📊 Insight merged & written: {file_path}")

def build_insights(df):

    if df.rdd.isEmpty():
        print("ℹ️ No data received for insight generation")
        return None

    grouped = df.groupBy("transaction_date").agg(
        count("*").alias("total_transactions"),
        _sum("deposit_amt").alias("total_deposit"),
        _sum("withdrawal_amt").alias("total_withdrawal"),
        avg("balance_amt").alias("avg_balance")
    )

    rows = grouped.collect()

    # IMPORTANT: take source_date from input batch
    source_date = str(df.select("source_date").first()[0])

    insights_list = []

    for r in rows:
        insights_list.append({
            "transaction_date": str(r["transaction_date"]),
            "total_transactions": int(r["total_transactions"] or 0),
            "total_deposit": float(r["total_deposit"] or 0),
            "total_withdrawal": float(r["total_withdrawal"] or 0),
            "avg_balance": float(r["avg_balance"] or 0)
        })

    return {
        "source_date": source_date,
        "generated_at": str(datetime.date.today()),
        "insights": insights_list
    }


def update_insights(df):

    data = build_insights(df)

    if not data:
        return

    file_name = f"insight_{data['source_date']}.json"
    file_path = os.path.join(INSIGHT_DIR, file_name)

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"📊 Insight written: {file_path}")


def archive_old_insights():

    today = str(datetime.date.today())

    for file in os.listdir(INSIGHT_DIR):
        if file.endswith(".json") and today not in file:
            src = os.path.join(INSIGHT_DIR, file)
            dst = os.path.join(ARCHIVE_DIR, file)
            os.rename(src, dst)

    print("📦 Old insights archived")