def rule_based_insights(data):
    if not data:
        return {"status": "no_data"}

    total_txn = sum(r.get("total_transactions", 1) for r in data)
    deposit = sum(r.get("deposit", 0) for r in data)
    withdrawal = sum(r.get("withdrawal", 0) for r in data)

    weighted_balance_sum = sum(
        r.get("balance", 0) * r.get("total_transactions", 1) for r in data
    )
    avg_balance = weighted_balance_sum / total_txn if total_txn else 0
    balances = [r.get("balance", 0) for r in data if r.get("balance") is not None]

    anomaly = "normal"
    if withdrawal > deposit:
        anomaly = "high_withdrawal"
    elif total_txn < 2:
        anomaly = "low_activity"
    elif balances and max(balances) > avg_balance * 2:
        anomaly = "balance_spike"

    # -------------------------
    # DYNAMIC CONFIDENCE SCORE
    # -------------------------
    confidence = 1.0

    # Penalize low transaction volume
    if total_txn < 5:
        confidence -= 0.2
    elif total_txn < 20:
        confidence -= 0.1

    # Penalize missing/zero balance rows
    missing_balance = sum(1 for r in data if not r.get("balance"))
    completeness_ratio = missing_balance / len(data) if data else 1
    confidence -= round(completeness_ratio * 0.2, 2)

    # Penalize if only one source is present
    sources = set(r.get("source") for r in data if r.get("source"))
    if len(sources) < 2:
        confidence -= 0.1

    # Penalize anomalies (something unusual is happening)
    if anomaly != "normal":
        confidence -= 0.15

    confidence = round(max(0.1, min(1.0, confidence)), 2)  # clamp to [0.1, 1.0]

    return {
        "total_transactions": total_txn,
        "total_deposit": deposit,
        "total_withdrawal": withdrawal,
        "avg_balance": round(avg_balance, 2),
        "anomaly": anomaly,
        "confidence": confidence  # ✅ earned, not hardcoded
    }