AI Unified Data Platform — Real-Time & Batch Transaction Intelligence
A production-grade data engineering pipeline that processes synthetic banking transactions through dual ingestion paths (Apache Spark batch + Apache Kafka streaming), loads clean data into PostgreSQL, and generates AI-powered financial insights via GitHub Models (GPT-4o) — fully orchestrated with Apache Airflow.
All components run end-to-end: Spark batch, Kafka streaming, PostgreSQL writes, Airflow DAG scheduling, and LLM-generated summaries are fully operational.
> Built to demonstrate end-to-end data engineering at scale: from raw CSV/Parquet ingestion to LLM-generated anomaly summaries, with schema evolution, bad record isolation, deterministic rule-based fallback, and token-efficient prompt design. Synthetic banking data was generated to realistically simulate multi-account transaction activity across batch and streaming workloads.
---
Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         CSV / Parquet Files          Kafka Topic                │
│              (batch)                  (streaming)               │
└──────────────────┬──────────────────────────┬───────────────────┘
                   │                          │
         ┌─────────▼──────────┐    ┌──────────▼──────────┐
         │  Apache Spark      │    │  Spark Structured   │
         │  Batch Pipeline    │    │  Streaming (Kafka)  │
         │                    │    │                     │
         │  • Multi-format    │    │  • foreachBatch     │
         │    ingestion        │    │  • 10s micro-batch  │
         │  • Column          │    │  • Checkpoint       │
         │    normalization   │    │    recovery         │
         │  • Type casting    │    │  • Idle timeout     │
         │  • Bad record      │    │    auto-stop        │
         │    isolation       │    └──────────┬──────────┘
         └─────────┬──────────┘               │
                   │                          │
         ┌─────────▼──────────────────────────▼──────────┐
         │              Shared Transformation Layer        │
         │   clean_and_cast → add_error_column →          │
         │   split_data → finalize_good_data              │
         └─────────┬──────────────────────────┬──────────┘
                   │                          │
         ┌─────────▼──────────┐    ┌──────────▼──────────┐
         │   PostgreSQL DB    │    │  Bad Records Store  │
         │                    │    │  (timestamped CSV)  │
         │  • Schema          │    └─────────────────────┘
         │    evolution       │
         │  • JDBC append     │
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────────────────────────────────┐
         │              Insight Generation Layer           │
         │                                                 │
         │  generate_insights_from_df()                    │
         │      → insight_spark_batch_{date}.json          │
         │      → insight_kafka_stream_{date}.json         │
         └─────────┬───────────────────────────────────────┘
                   │
         ┌─────────▼──────────────────────────────────────┐
         │           AI Insight Engine                     │
         │                                                 │
         │  rule_engine.py  (always runs, ground truth)   │
         │       ↓                                         │
         │  insight_engine.py  (aggregated prompt only)   │
         │       ↓                                         │
         │  github_client.py → GitHub Models / GPT-4o     │
         │       ↓                                         │
         │  cache_store.py  (MD5 keyed, avoids re-calls)  │
         │       ↓                                         │
         │  ai_insights_combined_{date}.json               │
         └─────────┬───────────────────────────────────────┘
                   │
         ┌─────────▼──────────────────────────────────────┐
         │         Apache Airflow Orchestration            │
         │                                                 │
         │  spark_batch_processing                         │
         │       → ai_combined_insights                    │
         │            → archive_insight_files              │
         │                                                 │
         │  Schedule: Daily 06:00 UTC                      │
         │  Retries: 2 × 5min delay | SLA: 1hr            │
         └─────────────────────────────────────────────────┘
```
---
Key Engineering Decisions
1. Token-Efficient LLM Prompt Design
A naive approach dumps all raw rows into the prompt. At 225 grouped rows, that produces a 34,000-character prompt (~8,500 tokens) — exceeding GPT-4o's 8,000-token limit on GitHub Models.
This pipeline sends only aggregated signals to the LLM: rule engine output + top-5 withdrawal days + top-5 deposit days. Prompt size stays under ~800 tokens regardless of transaction volume.
Volume	Naive Approach	This Pipeline
728 transactions → 225 rows	~8,500 tokens ❌	~800 tokens ✅
50,000 transactions	~750,000 tokens ❌	~800 tokens ✅
5,000,000 transactions	impossible ❌	~800 tokens ✅
2. Rule Engine as Ground Truth
`rule_engine.py` always runs first and produces deterministic aggregates. The LLM receives these as immutable ground truth and is instructed only to narrate — never to recalculate. This prevents hallucinated numbers in financial summaries.
3. Graceful Fallback — Never Silent Failure
If the LLM call fails for any reason (token limit, network, rate limit), the pipeline does not crash or return an empty result. It falls back to a structured natural-language summary built entirely from `rule_engine.py` output — with the specific error surfaced in the JSON.
```json
{
  "ai_summary": "AI summary unavailable (GitHub Model Error). Rule-based analysis for 2026-05-11: 728 transactions processed (Spark: 728, Kafka: 0). Total deposits $100,559,619.00, withdrawals $101,055,209.00, net flow $-495,590.00. Avg balance $1,504,911.63. Anomaly: high_withdrawal (confidence: 0.75).",
  "mode": "fallback"
}
```
4. Schema Evolution
Before writing to PostgreSQL, the pipeline compares DataFrame columns against existing DB columns and issues `ALTER TABLE ADD COLUMN` for any new fields — without requiring a schema migration file or manual intervention.
5. Dual-Source Insight Merging (Kafka)
Kafka micro-batches (every 10 seconds) incrementally merge into the daily insight file using a weighted average balance calculation, rather than overwriting. This preserves accuracy across batches processed throughout the day.
6. MD5-Keyed Insight Cache
LLM calls are expensive and rate-limited. Results are cached using an MD5 key derived from `source_date` + transaction fingerprint. Re-running the pipeline for the same data does not re-invoke the LLM.
---
Project Structure
```
├── spark_dynamic_pipeline.py      # Spark batch ETL — read, clean, write, insights
├── kafka_streaming_pipeline.py    # Kafka structured streaming — micro-batch processing
├── schema.py                      # Shared Spark schema definition
├── db_utils.py                    # JDBC config, table existence, schema evolution
├── insight_store.py               # Aggregation from DataFrame → insight JSON
│
├── ai_insight_engine/
│   ├── app.py                     # Entry point — merges spark+kafka insight files
│   ├── insight_engine.py          # Orchestrates rule engine + LLM call + cache
│   ├── rule_engine.py             # Deterministic aggregation and anomaly detection
│   ├── github_client.py           # GitHub Models API client (GPT-4o)
│   └── cache_store.py             # MD5-keyed result cache
│
├── dags/
│   └── batch_ai_pipeline_dag.py      # Airflow DAG — full pipeline orchestration
│
├── data/
│   ├── input_file/                # Drop CSV/Parquet files here
│   └── bad_records/               # Timestamped CSVs of rejected rows
│
├── insights/
│   ├── insight_spark_batch_{date}.json
│   ├── insight_kafka_stream_{date}.json
│   ├── ai_insights_combined_{date}.json
│   └── archive/                   # Previous day's files moved here by Airflow
│
├── checkpoint/                    # Kafka stream checkpoint (Spark managed)
├── .env                           # Secrets (not committed)
└── requirements.txt
```
---
Tech Stack
Layer	Technology
Batch Processing	Apache Spark (PySpark)
Stream Processing	Apache Kafka + Spark Structured Streaming
Database	PostgreSQL (JDBC)
Orchestration	Apache Airflow
AI / LLM	GitHub Models — GPT-4o
Language	Python 3.11
Environment	Docker Compose
---
Setup & Running
Prerequisites
Docker and Docker Compose
GitHub personal access token with Models access
Python 3.11+
1. Clone and configure
```bash
git clone https://github.com/priyushach99/mcp-ai-unified-data-platform.git
cd mcp-ai-unified-data-platform
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
3. Start services
```bash
docker-compose up -d
```
4. Run Spark batch pipeline manually
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
6. Run AI insight engine manually
```bash
# With both spark and kafka insight files
python -m ai_insight_engine.app \
  insights/insight_spark_batch_2026-05-11.json \
  insights/insight_kafka_stream_2026-05-11.json

# Spark only (kafka not available)
python -m ai_insight_engine.app \
  insights/insight_spark_batch_2026-05-11.json
```
7. Airflow DAG
The DAG `unified_pipeline` runs daily at 06:00 UTC and executes:
`spark_batch_processing` — runs spark-submit
`ai_combined_insights` — runs the AI insight engine
`archive_insight_files` — moves previous day's files to `insights/archive/`
Access Airflow UI at `http://localhost:8080`
---
Sample Output
```json
{
  "source_date": "2026-05-11",
  "ai_summary": "PERIOD SUMMARY: Between 2025-01-01 and 2026-05-11, 728 transactions were processed across the Spark batch pipeline, totalling $100,559,619.00 in deposits against $101,055,209.00 in withdrawals — a net outflow of $495,590.00 against an average balance of $1,504,911.63...\n\nANOMALY ANALYSIS: The high_withdrawal flag was triggered because total withdrawals exceeded total deposits by $495,590.00...\n\nRISK FLAGS:\n1. Net outflow of $495,590.00 sustained over the reporting period warrants review...\n2. Peak withdrawal day reached $X,XXX,XXX.00...",
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
Synthetic Data Generation
The dataset simulates realistic banking activity across multiple accounts. Transactions cover:
Mixed deposit and withdrawal patterns across 12+ months
Realistic date distribution including high-frequency days and sparse periods
Intentional dirty rows (malformed dates, non-numeric amounts, blank account numbers) to exercise the bad record pipeline
Volume sufficient to trigger all anomaly types: `high_withdrawal`, `balance_spike`, `low_activity`
This approach was chosen over a public dataset to allow full control over edge cases, schema variations, and bad data scenarios that production pipelines encounter but public datasets rarely expose cleanly.
---
Every row is evaluated against four validation rules before reaching PostgreSQL:
Validation	Error Label
`transaction_date` cannot be parsed	`Invalid Date`
`account_no` is blank	`Missing Account`
`withdrawal_amt` is non-empty but non-numeric	`Invalid Withdrawal Amt`
`deposit_amt` is non-empty but non-numeric	`Invalid Deposit Amt`
Bad records are written to `data/bad_records/bad_records_{timestamp}.csv` with the `error_reason` column populated. Good records continue to PostgreSQL. Neither path blocks the other.
---
Confidence Score
The rule engine produces a dynamic confidence score (0.1 – 1.0) penalised by:
Low transaction volume (< 5 transactions: −0.20, < 20: −0.10)
Missing balance rows (proportional penalty up to −0.20)
Single source only / no Kafka data (−0.10)
Anomaly present (−0.15)
This score is included in the AI prompt as context and in the output JSON.
---
What I Would Add Next
MCP Server layer — expose PostgreSQL tools via Model Context Protocol so the LLM can query data interactively rather than receiving a pre-built prompt
Data quality metrics dashboard — Airflow XCom passing bad record counts to a monitoring endpoint
dbt models — transform raw `transactions` table into analytics-ready aggregates
Great Expectations — data contract validation before Postgres write
---
Author
Priyusha Chandra — Data Engineer | MS Student  
GitHub · LinkedIn
5+ years of experience designing and building data pipelines across batch and streaming systems. This project demonstrates full-stack data engineering ownership — ingestion, transformation, quality, storage, orchestration, and AI-augmented analytics — without relying on managed services.
Skills demonstrated in this project:  
`PySpark` `Kafka` `Spark Structured Streaming` `PostgreSQL` `Apache Airflow` `Python` `ETL` `ELT` `Schema Evolution` `Data Quality` `LLM Integration` `Prompt Engineering` `Docker` `Big Data` `Real-Time Pipelines` `Batch Processing`
