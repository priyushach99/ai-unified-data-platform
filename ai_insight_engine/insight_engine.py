# insight_engine.py

import json
import hashlib
from .github_client import call_github_model
from .rule_engine import rule_based_insights
from .cache_store import get_cache, set_cache


def make_cache_key(source_date, data):
    fingerprint = "_".join(
        f"{r.get('transaction_id', '')}:{r.get('transaction_date')}:{r.get('total_withdrawal', 0)}:{r.get('total_deposit', 0)}"
        for r in sorted(data, key=lambda x: (x.get('transaction_date', ''), x.get('transaction_id', '')))
    )
    raw = f"{source_date}_{fingerprint}"
    return hashlib.md5(raw.encode()).hexdigest()


def generate_ai_insights(source_date, raw_transactions):

    cache_key = make_cache_key(source_date, raw_transactions)
    cached = get_cache(cache_key)
    if cached:
        return cached

    if not raw_transactions:
        return {"status": "no_data"}

    # Rule engine always runs first — this is your ground truth
    rule_result = rule_based_insights(raw_transactions)

    # Pre-compute source splits from raw_transactions
    spark_txns = sum(r.get("total_transactions", 1) for r in raw_transactions if r.get("source") == "spark")
    kafka_txns = sum(r.get("total_transactions", 1) for r in raw_transactions if r.get("source") == "kafka")

    # Date range from raw_transactions for context
    dates = sorted([r.get("transaction_date") for r in raw_transactions if r.get("transaction_date")])
    date_range = f"{dates[0]} to {dates[-1]}" if len(dates) >= 2 else (dates[0] if dates else "unknown")

    # Top 5 highest withdrawal days — gives LLM specific signal without all 225 rows
    top_withdrawals = sorted(raw_transactions, key=lambda x: x.get("withdrawal", 0), reverse=True)[:5]
    top_withdrawal_lines = "\n".join(
        f"  - {r.get('transaction_date')}: withdrawal ${r.get('withdrawal', 0):,.2f}, deposit ${r.get('deposit', 0):,.2f}, balance ${r.get('balance', 0):,.2f}"
        for r in top_withdrawals
    )

    # Top 5 highest deposit days
    top_deposits = sorted(raw_transactions, key=lambda x: x.get("deposit", 0), reverse=True)[:5]
    top_deposit_lines = "\n".join(
        f"  - {r.get('transaction_date')}: deposit ${r.get('deposit', 0):,.2f}, withdrawal ${r.get('withdrawal', 0):,.2f}, balance ${r.get('balance', 0):,.2f}"
        for r in top_deposits
    )

    try:
        # ---------------------------------------------------------------
        # PROMPT: aggregated signals only — no raw rows
        # Stays well under 1,000 tokens regardless of transaction volume
        # Scales to 50k, 5M transactions without change
        # ---------------------------------------------------------------
        prompt = f"""You are a forensic financial analyst reviewing banking transactions.

REPORTING PERIOD: {date_range}
REPORT DATE: {source_date}

AGGREGATED METRICS (ground truth — do not deviate from these numbers):
- Total transactions: {rule_result['total_transactions']}
- Spark batch transactions: {spark_txns}
- Kafka streaming transactions: {kafka_txns}
- Total deposits: ${rule_result['total_deposit']:,.2f}
- Total withdrawals: ${rule_result['total_withdrawal']:,.2f}
- Net flow: ${rule_result['total_deposit'] - rule_result['total_withdrawal']:,.2f}
- Average balance: ${rule_result['avg_balance']:,.2f}
- Anomaly detected: {rule_result['anomaly']}
- Confidence score: {rule_result['confidence']}

TOP 5 WITHDRAWAL DAYS:
{top_withdrawal_lines}

TOP 5 DEPOSIT DAYS:
{top_deposit_lines}

YOUR TASK — respond in exactly this structure:
1. PERIOD SUMMARY: One paragraph summarizing overall financial health using the aggregated metrics above.
2. ANOMALY ANALYSIS: Explain the '{rule_result['anomaly']}' anomaly with specific dollar amounts from the data above.
3. RISK FLAGS: 2-3 specific actionable risk flags with exact dollar amounts.
4. ARCHITECTURE NOTE: State the Spark ({spark_txns} txns) vs Kafka ({kafka_txns} txns) split and what each pipeline contributes to risk detection.

Keep the total response under 300 words. Use only the numbers provided above."""

        ai_response = call_github_model(prompt)

        final_output = {
            "source_date": source_date,
            "ai_summary": ai_response,
            "rule_summary": rule_result,
            "mode": "llm"
        }

    except Exception as e:
        # -------------------------------------------------------------------
        # FALLBACK: rule_result is always computed before the try block
        # so this always has real data, never fails silently
        # -------------------------------------------------------------------
        rule = rule_result  # already computed above

        fallback_summary = (
            f"AI summary unavailable ({str(e).split(':')[0]}). "
            f"Rule-based analysis for {source_date}: "
            f"{rule['total_transactions']} transactions processed "
            f"(Spark: {spark_txns}, Kafka: {kafka_txns}). "
            f"Total deposits ${rule['total_deposit']:,.2f}, "
            f"withdrawals ${rule['total_withdrawal']:,.2f}, "
            f"net flow ${rule['total_deposit'] - rule['total_withdrawal']:,.2f}. "
            f"Avg balance ${rule['avg_balance']:,.2f}. "
            f"Anomaly: {rule['anomaly']} (confidence: {rule['confidence']})."
        )

        final_output = {
            "source_date": source_date,
            "ai_summary": fallback_summary,
            "rule_summary": rule,
            "mode": "fallback",
            "error": str(e)
        }

    set_cache(cache_key, final_output)
    return final_output