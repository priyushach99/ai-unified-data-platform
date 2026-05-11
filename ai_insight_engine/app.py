from ai_insight_engine.insight_engine import generate_ai_insights
import json
import sys
from datetime import date


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
                "total_transactions": r.get("total_transactions", 1),
                "source": source if source else None,
            })

    # ✅ Fallback: if no files were read, source_date stays None — use today
    if source_date is None:
        source_date = date.today().isoformat()

    return source_date, raw_transactions


def run(files):
    print("INPUT FILES:", files)

    # ✅ STEP 1: BUILD RAW TRUTH DATA
    source_date, raw_transactions = build_raw_transactions(files)

    # ── Determine which sources are present ───────────────────────────────────
    has_spark = any("spark" in f for f in files)
    has_kafka = any("kafka" in f for f in files)

    # ── Case 1: empty raw_transactions — no files passed at all ──────────────
    if not raw_transactions:
        ai_output = {
            "source_date": source_date,
            "status": "no_insights",
            "message": "No insights for today as there are no spark and kafka processed today.",
        }
        print("⚠️  No raw transactions found. Writing no-insight marker.")

    # ── Case 2: only one source file available ────────────────────────────────
    elif not (has_spark and has_kafka):
        available_source = "Spark" if has_spark else "Kafka"
        available_file   = next((f for f in files), "unknown")

        print(f"⚠️  Only {available_source} file available: {available_file}")
        print(f"TOTAL RAW ROWS SENT TO ENGINE: {len(raw_transactions)}")

        ai_output = generate_ai_insights(
            source_date=source_date,
            raw_transactions=raw_transactions,
        )
        ai_output["note"] = (
            f"Combined insight generated from {available_source} only. "
            f"Only {available_source} pipeline ran today ({available_file}). "
            f"No {'Kafka' if has_spark else 'Spark'} file was available."
        )

    # ── Case 3: both spark and kafka available — normal combined flow ─────────
    else:
        print(f"TOTAL RAW ROWS SENT TO ENGINE: {len(raw_transactions)}")

        ai_output = generate_ai_insights(
            source_date=source_date,
            raw_transactions=raw_transactions,
        )

    # ── Write output ──────────────────────────────────────────────────────────
    output_path = f"insights/ai_insights_combined_{source_date}.json"

    with open(output_path, "w") as f:
        json.dump(ai_output, f, indent=2)

    print(f"Saved: {output_path}")


if __name__ == "__main__":
    run(sys.argv[1:])