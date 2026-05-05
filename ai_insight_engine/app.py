#app.py
from ai_insight_engine.merge_insights import merge_insight_files
from ai_insight_engine.insight_engine import generate_ai_insights
import json
import sys


def build_raw_transactions(files):
    raw_transactions = []
    source_date = None

    for file_path in files:
        with open(file_path, "r") as f:
            data = json.load(f)

        source_date = data.get("source_date")
        source = "spark" if "spark" in file_path else "kafka"

        for r in data["insights"]:
            raw_transactions.append({
                "transaction_date": r["transaction_date"],
                "deposit": r.get("total_deposit", 0),
                "withdrawal": r.get("total_withdrawal", 0),
                "balance": r.get("avg_balance", 0),
                "total_transactions": r.get("total_transactions", 1),  # ← ADD THIS
                "source": source
            })

    return source_date, raw_transactions


def run(files):

    print("INPUT FILES:", files)

    # ✅ STEP 1: BUILD RAW TRUTH DATA
    source_date, raw_transactions = build_raw_transactions(files)

    print("TOTAL RAW ROWS SENT TO ENGINE:", len(raw_transactions))

    # 🔥 STEP 2: SEND RAW DATA TO INSIGHT ENGINE
    ai_output = generate_ai_insights(
        source_date=source_date,
        raw_transactions=raw_transactions
    )

    output_path = f"insights/ai_insights_combined_{source_date}.json"

    with open(output_path, "w") as f:
        json.dump(ai_output, f, indent=2)

    print("Saved:", output_path)


if __name__ == "__main__":
    run(sys.argv[1:])