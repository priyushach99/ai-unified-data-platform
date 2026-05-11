##🏦 AI Unified Data Platform

##Real-Time & Batch Transaction Intelligence

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

📌 What This Project Demonstrates
> Every component reflects a real engineering decision made to solve a real constraint.
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
╚══════════╦═══════════════════════════════════╦═══════════════╝
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
║               Shared Transformation Layer                  ║
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
║                 Insight Generation Layer                   ║
║  generate_insights_from_df()                               ║
║    → insight_spark_batch_{date}.json                       ║
║    → insight_kafka_stream_{date}.json                      ║
╚══════════╦═════════════════════════════════════════════════╝
           ║
╔══════════▼═════════════════════════════════════════════════╗
║                   AI Insight Engine                        ║
║                                                            ║
║  rule_engine.py    →  deterministic ground truth           ║
║        ↓                                                   ║
║  insight_engine.py →  aggregated prompt (~800 tokens)      ║
║        ↓                                                   ║
║  github_client.py  →  GPT-4o via GitHub Models             ║
║        ↓                                                   ║
║  cache_store.py    →  MD5-keyed, no duplicate LLM calls    ║
║        ↓                                                   ║
║  ai_insights_combined_{date}.json                          ║
╚══════════╦═════════════════════════════════════════════════╝
           ║
╔══════════▼═════════════════════════════════════════════════╗
║              Apache Airflow Orchestration                  ║
║                                                            ║
║  spark_batch_processing                                    ║
║       └──→ ai_combined_insights                            ║
║                 └──→ archive_insight_files                 ║
║                                                            ║
║  Schedule: Daily 06:00 UTC                                 ║
║  Retries: 2 × 5 min delay   |   SLA: 1 hr                  ║
╚════════════════════════════════════════════════════════════╝
```
