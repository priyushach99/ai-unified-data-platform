import streamlit as st
import json
import os
from glob import glob

st.set_page_config(page_title="AI Transactions Insights Dashboard", layout="wide")


def load_latest_file():
    files = glob("insights/*.json")
    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)
    with open(latest_file, "r") as f:
        return json.load(f)


data = load_latest_file()

st.title("📊 AI Transactions Insights ")

if not data:
    st.warning("No AI insights found. Run pipeline first.")
    st.stop()

# HEADER
st.subheader(f"Source Date: {data['source_date']}")
st.success(f"Mode: {data['mode']}")

# METRICS
rule = data.get("rule_summary", {})
col1, col2, col3, col4 = st.columns(4)
col1.metric("Transactions", rule.get("total_transactions", 0))
col2.metric("Total Deposit", f"${rule.get('total_deposit', 0):,.2f}")
col3.metric("Total Withdrawal", f"${rule.get('total_withdrawal', 0):,.2f}")
col4.metric("Avg Balance", f"${rule.get('avg_balance', 0):,.2f}")

# ANOMALY — matches rule_engine.py output values
st.subheader("🚨 Anomaly Detection")
anomaly = rule.get("anomaly", "unknown")
confidence = rule.get("confidence", 0)

if anomaly == "high_withdrawal":
    st.error(f"⚠️ High withdrawal activity detected — confidence: {confidence}")
elif anomaly == "low_activity":
    st.warning(f"📉 Low transaction activity — confidence: {confidence}")
elif anomaly == "balance_spike":
    st.warning(f"📈 Balance spike detected — confidence: {confidence}")
else:
    st.success(f"✅ Normal behavior detected — confidence: {confidence}")

# CONFIDENCE BAR — visual touch that impresses on demo
st.caption("Rule Engine Confidence")
st.progress(confidence)

# AI SUMMARY — single render, $ signs escaped
st.subheader("🧠 AI Insight Summary")
ai_summary = data.get("ai_summary", "No AI summary found")
st.markdown(ai_summary.replace("$", "\\$"))

# RAW JSON
with st.expander("🔍 Raw JSON"):
    st.json(data)