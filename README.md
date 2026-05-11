# 🏦 AI Unified Data Platform

### Real-Time & Batch Transaction Intelligence

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square\&logo=python\&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.x-E25A1C?style=flat-square\&logo=apachespark\&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=flat-square\&logo=apachekafka\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=flat-square\&logo=apacheairflow\&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=flat-square\&logo=postgresql\&logoColor=white)
![GPT-4o](https://img.shields.io/badge/GPT--4o-GitHub%20Models-412991?style=flat-square\&logo=openai\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square\&logo=docker\&logoColor=white)
![Status](https://img.shields.io/badge/Pipeline-End--to--End%20Working-2ea44f?style=flat-square)
![Ingestion](https://img.shields.io/badge/Ingestion-Batch%20%2B%20Streaming-orange?style=flat-square)
![LLM](https://img.shields.io/badge/LLM-Token--Efficient%20Prompting-8B5CF6?style=flat-square)

---

A fully operational **data engineering pipeline** that processes synthetic banking transactions through **dual ingestion paths — Apache Spark batch + Apache Kafka streaming**, stores clean data in **PostgreSQL**, and generates **AI-powered financial anomaly summaries via GPT-4o**.

Orchestrated end-to-end using **Apache Airflow**.

---

## 📌 What This Project Demonstrates

> This is not a tutorial pipeline. Every component reflects a real engineering decision made to solve a real constraint.

| Challenge Faced              | Engineering Decision Made       |
| ---------------------------- | ------------------------------- |
| LLM prompt hit ~8,500 tokens | Aggregated prompt (~800 tokens) |
| Kafka overwriting totals     | Incremental weighted merging    |
| LLM failure risk             | Rule-engine fallback            |
| Schema breaks                | Auto schema evolution           |
| Expensive re-runs            | MD5 caching                     |

---

## 🏗️ Architecture

```
DATA SOURCES
 ├── CSV / Parquet (Batch)
 └── Kafka Topic (Streaming)

        ↓

Spark Batch ETL            Spark Structured Streaming
 ├── Multi-format read     ├── foreachBatch()
 ├── Casting               ├── Micro-batches (10s)
 └── Cleaning              └── Checkpoint recovery

        ↓

Shared Transformation Layer

        ↓

PostgreSQL (Clean Data)      Bad Records (CSV)

        ↓

Insight Generation Layer

        ↓

AI Insight Engine
 ├── Rule Engine (deterministic)
 ├── Prompt Builder (~800 tokens)
 ├── GPT-4o (GitHub Models)
 └── Cache (MD5-based)

        ↓

Apache Airflow DAG
```

---

## ⚙️ Tech Stack

| Layer         | Technology              | Purpose             |
| ------------- | ----------------------- | ------------------- |
| Batch         | Apache Spark            | ETL processing      |
| Streaming     | Kafka + Spark Streaming | Real-time ingestion |
| Storage       | PostgreSQL              | Data sink           |
| Orchestration | Airflow                 | Scheduling          |
| AI            | GPT-4o                  | Insight generation  |
| Language      | Python 3.11             | Core logic          |
| Infra         | Docker Compose          | Containerization    |

---

## 🔬 Key Engineering Decisions

### 1. Token-Efficient LLM Prompting

* Naive: ~8,500 tokens ❌
* Optimized: ~800 tokens ✅
* Scales to millions of rows

---

### 2. Rule Engine as Ground Truth

LLM **never recalculates numbers**, it only narrates deterministic outputs from the rule engine.

```
Raw Data → Rule Engine → Aggregates → LLM (Narration Only)
```

---

### 3. Graceful Fallback — Never Silent Failure

If the LLM fails for any reason, a structured fallback summary is generated using rule engine output.

```json
{
  "mode": "fallback",
  "error": "tokens_limit_reached"
}
```

---

### 4. Schema Evolution

* Detects new columns automatically
* Executes `ALTER TABLE ADD COLUMN`
* No manual migrations required

---

### 5. Incremental Kafka Merge

```python
avg = ((old * n_old) + (new * n_new)) / (n_old + n_new)
```

---

### 6. MD5-Based Caching

* Prevents duplicate LLM calls
* Reduces cost and latency

---

## 📁 Project Structure

```
ai-unified-data-platform/
│
├── spark_dynamic_pipeline.py
├── kafka_streaming_pipeline.py
├── schema.py
├── db_utils.py
├── insight_store.py
│
├── ai_insight_engine/
│   ├── app.py
│   ├── insight_engine.py
│   ├── rule_engine.py
│   ├── github_client.py
│   └── cache_store.py
│
├── dags/
│   └── batch_ai_pipeline_dag.py
│
├── data/
├── insights/
├── checkpoint/
├── docker-compose.yml
└── requirements.txt
```

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone https://github.com/priyushach99/ai-unified-data-platform.git
cd ai-unified-data-platform
cp .env.example .env
```

### 2. Configure Environment

```env
GITHUB_TOKEN=your_token
DB_HOST=localhost
DB_PORT=5432
DB_NAME=transactions_db
DB_USER=user
DB_PASSWORD=password
```

### 3. Start Services

```bash
docker-compose up -d
```

### 4. Run Batch Pipeline

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  spark_dynamic_pipeline.py \
  data/input_file/
```

### 5. Run Streaming Pipeline

```bash
python kafka_streaming_pipeline.py
```

### 6. Generate AI Insights

```bash
python -m ai_insight_engine.app \
  insights/insight_spark_batch_DATE.json \
  insights/insight_kafka_stream_DATE.json
```

### 7. Airflow

* URL: [http://localhost:8080](http://localhost:8080)
* DAG: `unified_pipeline`
* Schedule: Daily 06:00 UTC

---

## 📤 Sample Output

```json
{
  "mode": "llm",
  "anomaly": "high_withdrawal",
  "confidence": 0.75
}
```

---

## 🚨 Data Quality Handling

| Rule               | Error                    |
| ------------------ | ------------------------ |
| Invalid date       | `Invalid Date`           |
| Missing account    | `Missing Account`        |
| Invalid withdrawal | `Invalid Withdrawal Amt` |
| Invalid deposit    | `Invalid Deposit Amt`    |

---

## 📐 Confidence Score

| Condition        | Impact |
| ---------------- | ------ |
| Low volume       | ↓      |
| Missing data     | ↓      |
| No Kafka data    | ↓      |
| Anomaly detected | ↓      |

---

## 🔭 Future Improvements

* MCP Server (LLM querying DB directly)
* dbt transformations
* Great Expectations validation
* Cloud deployment (EMR / MWAA)
* Observability metrics

---

## 👩‍💻 Author

**Priyusha — Data Engineer · MS Student**

* 5+ years experience in data engineering
* Focus: Batch + Streaming + AI pipelines

---

## 🧠 Skills Demonstrated

* PySpark
* Kafka
* Airflow
* PostgreSQL
* Docker
* GPT-4o
* Real-Time Pipelines
* Schema Evolution
* Prompt Engineering
