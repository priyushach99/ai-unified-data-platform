🏦 AI Unified Data Platform
Real-Time & Batch Transaction Intelligence
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![GPT-4o](https://img.shields.io/badge/GPT--4o-GitHub%20Models-412991?style=flat-square&logo=openai&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/Pipeline-End--to--End%20Working-2ea44f?style=flat-square)
![Ingestion](https://img.shields.io/badge/Ingestion-Batch%20%2B%20Streaming-orange?style=flat-square)
![LLM](https://img.shields.io/badge/LLM-Token--Efficient%20Prompting-8B5CF6?style=flat-square)
A fully operational data engineering pipeline that processes synthetic banking transactions through dual ingestion paths — Apache Spark batch + Apache Kafka streaming — stores clean data in PostgreSQL, and generates AI-powered financial anomaly summaries via GPT-4o. Orchestrated end-to-end on Apache Airflow.
---
📌 What This Project Demonstrates
> This is not a tutorial pipeline. Every component reflects a real engineering decision made to solve a real constraint.
Challenge Faced	Engineering Decision Made
LLM prompt hit 8,500 tokens at 225 rows — exceeded GitHub Models 8k limit	Send only aggregated signals to LLM — prompt stays ~800 tokens at any data volume
Kafka micro-batches would overwrite running daily totals	Weighted-average merge per batch — accuracy accumulates correctly throughout the day
LLM failures would silently produce empty output	Rule engine always runs first — fallback builds a structured summary from deterministic data
New CSV columns broke PostgreSQL writes	Schema evolution layer detects new columns and issues `ALTER TABLE ADD COLUMN` automatically
Re-running pipeline would re-invoke expensive LLM calls	MD5-keyed cache from date + transaction fingerprint — duplicate calls never reach the API
---
🏗️ Architecture
```
╔══════════════════════════════════════════════════════════════╗
║                       DATA SOURCES                           ║
║      CSV / Parquet Files            Kafka Topic              ║
║          (historical)                (real-time)             ║
╚══════════╦═══════════════════════════════════╦══════════════╝
           ║                                   ║
┌──────────▼──────────┐           ┌────────────▼────────────┐
│  Spark Batch ETL    │           │ Spark Structured        │
│                     │           │ Streaming               │
│ • Multi-format read │           │ • foreachBatch()        │
│ • Column normalize  │           │ • 10s micro-batches     │
│ • Type casting      │           │ • Checkpoint recovery   │
│ • Bad record split  │           │ • Idle auto-stop        │
└──────────┬──────────┘           └────────────┬────────────┘
           ║                                   ║
╔══════════▼═══════════════════════════════════▼═════════════╗
║               Shared Transformation Layer                   ║
║  clean_and_cast → add_error_column → finalize_good_data    ║
╚══════════╦═══════════════════════════════════╦═════════════╝
           ║                                   ║
┌──────────▼──────────┐           ┌────────────▼────────────┐
│    PostgreSQL       │           │  Bad Records Store      │
│ • Schema evolution  │           │  timestamped CSV        │
│ • JDBC append       │           │  error_reason column    │
└──────────┬──────────┘           └─────────────────────────┘
           ║
╔══════════▼═════════════════════════════════════════════════╗
║                 Insight Generation Layer                    ║
║  generate_insights_from_df()                               ║
║    → insight_spark_batch_{date}.json                       ║
║    → insight_kafka_stream_{date}.json                      ║
╚══════════╦═════════════════════════════════════════════════╝
           ║
╔══════════▼═════════════════════════════════════════════════╗
║                   AI Insight Engine                         ║
║                                                             ║
║  rule_engine.py    →  deterministic ground truth           ║
║        ↓                                                    ║
║  insight_engine.py →  aggregated prompt (~800 tokens)      ║
║        ↓                                                    ║
║  github_client.py  →  GPT-4o via GitHub Models            ║
║        ↓                                                    ║
║  cache_store.py    →  MD5-keyed, no duplicate LLM calls   ║
║        ↓                                                    ║
║  ai_insights_combined_{date}.json                           ║
╚══════════╦═════════════════════════════════════════════════╝
           ║
╔══════════▼═════════════════════════════════════════════════╗
║              Apache Airflow Orchestration                   ║
║                                                             ║
║  spark_batch_processing                                     ║
║       └──→ ai_combined_insights                             ║
║                 └──→ archive_insight_files                  ║
║                                                             ║
║  Schedule: Daily 06:00 UTC                                  ║
║  Retries: 2 × 5 min delay   |   SLA: 1 hr                  ║
╚════════════════════════════════════════════════════════════╝
```
---
⚙️ Tech Stack
Layer	Technology	Purpose
![Spark](https://img.shields.io/badge/-PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)	Apache Spark 4.x	Batch ETL — multi-format ingestion, casting, schema evolution
![Kafka](https://img.shields.io/badge/-Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)	Apache Kafka	Streaming ingestion — real-time transaction feed
![Streaming](https://img.shields.io/badge/-Structured%20Streaming-E25A1C?style=flat-square&logo=apachespark&logoColor=white)	Spark Structured Streaming	Kafka consumer — foreachBatch, checkpointing, merging
![Postgres](https://img.shields.io/badge/-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)	PostgreSQL	Data sink — JDBC append with live schema evolution
![Airflow](https://img.shields.io/badge/-Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)	Apache Airflow	DAG orchestration — scheduling, retries, file archival
![OpenAI](https://img.shields.io/badge/-GPT--4o-412991?style=flat-square&logo=openai&logoColor=white)	GitHub Models / GPT-4o	AI insight generation with token-efficient prompting
![Python](https://img.shields.io/badge/-Python%203.11-3776AB?style=flat-square&logo=python&logoColor=white)	Python 3.11	Pipeline logic, rule engine, insight merging, caching
![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)	Docker Compose	All services containerized — one command startup
---
🔬 Key Engineering Decisions
1. Token-Efficient LLM Prompt Design
A naive implementation serializes all rows directly into the prompt string. At 225 grouped rows this produced a 34,286-character prompt (~8,500 tokens), hitting GPT-4o's GitHub Models limit and causing pipeline failure.
Fix: Send only aggregated signals — rule engine output + top-5 withdrawal days + top-5 deposit days. Prompt size is now constant regardless of transaction volume.
Volume	Naive Approach	This Pipeline
728 txns → 225 grouped rows	~8,500 tokens ❌	~800 tokens ✅
50,000 transactions	~750,000 tokens ❌	~800 tokens ✅
5,000,000 transactions	Impossible ❌	~800 tokens ✅
---
2. Rule Engine as Immutable Ground Truth
`rule_engine.py` always runs before any LLM call and produces deterministic aggregates. The LLM receives these numbers as fixed facts and is instructed only to narrate — never to recalculate. This prevents hallucinated figures in financial output.
```
raw_transactions
      ↓
rule_engine.py  →  { total_txns, deposits, withdrawals, anomaly, confidence }
      ↓
LLM receives these as GROUND TRUTH — narrates, never recalculates
```
---
3. Graceful Fallback — Never Silent Failure
If the LLM call fails for any reason, the pipeline falls back to a structured natural-language summary built entirely from rule engine output. The specific error is surfaced in the JSON. No crashes, no empty responses.
```json
{
  "ai_summary": "AI summary unavailable (GitHub Model Error). 728 transactions processed
  (Spark: 728, Kafka: 0). Deposits $100,559,619.00, withdrawals $101,055,209.00,
  net flow $-495,590.00. Avg balance $1,504,911.63. Anomaly: high_withdrawal (confidence: 0.75).",
  "mode": "fallback",
  "error": "tokens_limit_reached"
}
```
---
4. Schema Evolution — Zero Manual Migrations
Before every PostgreSQL write, the pipeline compares DataFrame columns against live DB columns and issues `ALTER TABLE ADD COLUMN` for any new fields. New columns in source files propagate automatically — no migration files, no downtime.
---
5. Incremental Kafka Insight Merging
Kafka micro-batches arrive every 10 seconds. A naive approach overwrites the daily insight file on each batch. Instead, batches are merged using a weighted-average balance calculation so daily totals accumulate correctly.
```python
ex["avg_balance"] = (
    (ex["avg_balance"] * n_old + r["avg_balance"] * n_new)
    / (n_old + n_new)
)
```
---
6. MD5-Keyed Insight Cache
LLM calls are rate-limited and costly. Each result is cached using an MD5 key derived from `source_date` + transaction fingerprint. Re-running the pipeline on the same data skips the LLM entirely.
---
📁 Project Structure
```
ai-unified-data-platform/
│
├── spark_dynamic_pipeline.py       # Spark batch ETL — ingest, clean, write, insights
├── kafka_streaming_pipeline.py     # Kafka streaming — micro-batch processing
├── schema.py                       # Shared Spark schema definition
├── db_utils.py                     # JDBC config, schema evolution helpers
├── insight_store.py                # DataFrame → aggregated insight JSON
│
├── ai_insight_engine/
│   ├── app.py                      # Entry point — merges spark + kafka insight files
│   ├── insight_engine.py           # Orchestrates rule engine + LLM call + cache
│   ├── rule_engine.py              # Deterministic aggregation and anomaly detection
│   ├── github_client.py            # GitHub Models API client (GPT-4o)
│   └── cache_store.py              # MD5-keyed result cache
│
├── dags/
│   └── batch_ai_pipeline_dag.py    # Airflow DAG — scheduling, retries, archival
│
├── data/
│   ├── input_file/                 # Drop CSV / Parquet files here
│   └── bad_records/                # Rejected rows — timestamped CSV per run
│
├── insights/
│   ├── insight_spark_batch_{date}.json
│   ├── insight_kafka_stream_{date}.json
│   ├── ai_insights_combined_{date}.json
│   └── archive/                    # Previous day's files auto-moved by Airflow
│
├── checkpoint/                     # Spark-managed Kafka stream checkpoint
├── .env.example
├── docker-compose.yml
└── requirements.txt
```
---
🚀 Getting Started
Prerequisites
Docker + Docker Compose
GitHub Personal Access Token with Models access
Python 3.11+
1. Clone and configure
```bash
git clone https://github.com/priyushach99/ai-unified-data-platform.git
cd ai-unified-data-platform
cp .env.example .env
# Edit .env with your credentials
```
2. Environment variables
```env
GITHUB_TOKEN=your_github_pat_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=transactions_db
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```
3. Start all services
```bash
docker-compose up -d
```
4. Run Spark batch pipeline
```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  spark_dynamic_pipeline.py \
  data/input_file/
```
5. Run Kafka streaming pipeline
```bash
python kafka_streaming_pipeline.py
```
6. Generate AI insights manually
```bash
# Both Spark and Kafka files available
python -m ai_insight_engine.app \
  insights/insight_spark_batch_2026-05-11.json \
  insights/insight_kafka_stream_2026-05-11.json

# Spark only
python -m ai_insight_engine.app \
  insights/insight_spark_batch_2026-05-11.json
```
7. Airflow DAG
Access Airflow at `http://localhost:8080`
DAG `unified_pipeline` runs daily at 06:00 UTC:
```
spark_batch_processing  →  ai_combined_insights  →  archive_insight_files
```
---
📤 Sample AI Output
```json
{
  "source_date": "2026-05-11",
  "ai_summary": "PERIOD SUMMARY: 728 transactions totalling $100,559,619.00 in deposits
  against $101,055,209.00 in withdrawals — net outflow of $495,590.00, avg balance $1,504,911.63.

  ANOMALY ANALYSIS: high_withdrawal triggered as withdrawals exceeded deposits by $495,590.00.
  Pattern suggests structural cash outflow rather than a one-off spike.

  RISK FLAGS:
  1. Sustained net outflow of $495,590.00 — review high-value withdrawal counterparties.
  2. Peak withdrawal day concentration warrants transaction-level audit.

  ARCHITECTURE NOTE: All 728 transactions via Spark batch (0 via Kafka today).",
  "rule_summary": {
    "total_transactions": 728,
    "total_deposit": 100559619.0,
    "total_withdrawal": 101055209.0,
    "avg_balance": 1504911.63,
    "anomaly": "high_withdrawal",
    "confidence": 0.75
  },
  "mode": "llm"
}
```
---
🚨 Data Quality — Bad Record Handling
Every row passes four validation gates before reaching PostgreSQL. Bad records are written to a timestamped CSV with the `error_reason` column populated. Neither path blocks the other.
Validation Rule	Error Label
`transaction_date` cannot be parsed as `dd-MMM-yy`	`Invalid Date`
`account_no` is blank or whitespace	`Missing Account`
`withdrawal_amt` is non-empty but non-numeric	`Invalid Withdrawal Amt`
`deposit_amt` is non-empty but non-numeric	`Invalid Deposit Amt`
---
📐 Confidence Score
The rule engine produces a dynamic confidence score (`0.1 – 1.0`) included in every insight JSON and passed to the LLM as context:
Condition	Penalty
Transaction volume < 5	−0.20
Transaction volume < 20	−0.10
Missing balance rows (proportional)	up to −0.20
Single source only — no Kafka	−0.10
Anomaly detected	−0.15
---
🗺️ Dataset
Synthetic banking transactions generated to simulate realistic multi-account activity:
Mixed deposit and withdrawal patterns across 12+ months
Intentional dirty rows — malformed dates, non-numeric amounts, blank account numbers — to exercise the full bad record pipeline
Volume calibrated to trigger all three anomaly types: `high_withdrawal`, `balance_spike`, `low_activity`
Synthetic data was chosen over a public dataset to allow full control over edge cases and schema variations that production pipelines encounter but public datasets rarely expose cleanly.
---
🔭 What I Would Add Next
MCP Server layer — expose PostgreSQL via Model Context Protocol so the LLM queries data interactively rather than receiving a pre-built prompt
dbt models — transform raw `transactions` into analytics-ready aggregates with full lineage
Great Expectations — data contract validation as a pre-write quality gate
Cloud deployment — Spark on EMR or Dataproc, Airflow on MWAA or Cloud Composer
Observability — Airflow XCom surfacing bad record counts to a metrics endpoint
---
👩‍💻 Author
Priyusha — Data Engineer · MS Student
![LinkedIn](https://img.shields.io/badge/LinkedIn-Priyusha-0A66C2?style=flat-square&logo=linkedin&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-priyushach99-181717?style=flat-square&logo=github&logoColor=white)
5+ years of experience designing and building data pipelines across batch and streaming systems. This project reflects full-stack data engineering ownership — from raw ingestion to AI-augmented analytics — without relying on managed services.
Skills demonstrated:
![PySpark](https://img.shields.io/badge/-PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/-Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/-Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/-Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![GPT-4o](https://img.shields.io/badge/-GPT--4o-412991?style=flat-square&logo=openai&logoColor=white)
![ETL](https://img.shields.io/badge/-ETL%20%2F%20ELT-555555?style=flat-square)
![Real-Time](https://img.shields.io/badge/-Real--Time%20Pipelines-FF6B35?style=flat-square)
![Schema Evolution](https://img.shields.io/badge/-Schema%20Evolution-2ea44f?style=flat-square)
![Prompt Engineering](https://img.shields.io/badge/-Prompt%20Engineering-8B5CF6?style=flat-square)
![Big Data](https://img.shields.io/badge/-Big%20Data-FF9900?style=flat-square)
