# insight_engine.py
import json
import hashlib
from .github_client import call_github_model
from .rule_engine import rule_based_insights
from .cache_store import get_cache, set_cache


# -------------------------
# CACHE KEY
# -------------------------
def make_cache_key(source_date, data):
    # Include transaction_id or the full row to guarantee uniqueness
    fingerprint = "_".join(
        f"{r.get('transaction_id', '')}:{r.get('transaction_date')}:{r.get('total_withdrawal', 0)}:{r.get('total_deposit', 0)}"
        for r in sorted(data, key=lambda x: (x.get('transaction_date', ''), x.get('transaction_id', '')))
    )
    raw = f"{source_date}_{fingerprint}"
    return hashlib.md5(raw.encode()).hexdigest()


# -------------------------
# MAIN ENGINE
# -------------------------
def generate_ai_insights(source_date, raw_transactions):

    cache_key = make_cache_key(source_date, raw_transactions)

    cached = get_cache(cache_key)
    if cached:
        return cached

    if not raw_transactions:
        return {"status": "no_data"}

    # -------------------------
    # RULE ENGINE (REAL TRUTH)
    # -------------------------
    rule_result = rule_based_insights(raw_transactions)

    confidence = rule_result.get("confidence", 0.5)

    try:

        # -------------------------
        # AI PROMPT (ONLY EXPLANATION)
        # -------------------------
        spark_txns = sum(r.get("total_transactions", 1) for r in raw_transactions if r.get("source") == "spark")
        kafka_txns = sum(r.get("total_transactions", 1) for r in raw_transactions if r.get("source") == "kafka")

        prompt = f"""
            You are a forensic financial analyst reviewing banking transactions.

            GROUND TRUTH — use these exact numbers, do not deviate:
            - Total transactions: {rule_result['total_transactions']}
            - Spark batch transactions: {spark_txns}
            - Kafka streaming transactions: {kafka_txns}
            - Total deposits: ${rule_result['total_deposit']:,.2f}
            - Total withdrawals: ${rule_result['total_withdrawal']:,.2f}
            - Average balance: ${rule_result['avg_balance']:,.2f}
            - Anomaly status: {rule_result['anomaly']}
            - Confidence: {rule_result['confidence']}

            RAW GROUPED DATA:
            {raw_transactions}

            YOUR TASK:
            1. Identify behavioral patterns in the transaction descriptions
            2. State Spark vs Kafka split explicitly using the numbers above
            3. Flag repeated transaction types or suspicious clustering by date
            4. Give 2-3 specific actionable risk flags with exact dollar amounts
            5. In the conclusion, note that the dual Spark+Kafka architecture enables 
            both historical batch analysis (Spark) AND real-time risk detection (Kafka)
            6. "DO NOT use the phrase 'continuous monitoring' — replace with a specific threshold or metric"
            7. DO NOT contradict any number in GROUND TRUTH above
            """

        ai_response = call_github_model(prompt)

        final_output = {
            "source_date": source_date,
            "ai_summary": ai_response,
            "rule_summary": rule_result,
            "mode": "llm"
        }

    except Exception as e:
        final_output = {
            "source_date": source_date,
            "ai_summary": "LLM unavailable. Using deterministic rule-based insights.",
            "rule_summary": rule_result,
            "mode": "fallback",
            "error": str(e)
        }

    set_cache(cache_key, final_output)

    return final_output
