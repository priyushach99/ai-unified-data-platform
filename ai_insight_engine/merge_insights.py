import json

def merge_insight_files(files):

    merged = []

    for file_path in files:
        source = "spark" if "spark" in file_path else "kafka"

        with open(file_path, "r") as f:
            data = json.load(f)

        for r in data["insights"]:
            merged.append({
                "transaction_date": r["transaction_date"],
                "total_deposit": r.get("total_deposit", 0),
                "total_withdrawal": r.get("total_withdrawal", 0),
                "avg_balance": r.get("avg_balance", 0),
                "transactions": r.get("total_transactions", 1),  # ← was defaulting to 0
                "source": source
            })

    return data["source_date"], merged