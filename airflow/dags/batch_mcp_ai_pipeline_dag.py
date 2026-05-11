from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import sys

PROJECT_DIR = "/opt/project"
INSIGHTS_DIR = f"{PROJECT_DIR}/insights"
ARCHIVE_DIR  = f"{PROJECT_DIR}/insights/archive"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="mcp_unified_pipeline",
    default_args=default_args,
    description="Spark batch → MCP AI combined insights → Archive previous day",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC — adjust to your preference
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["mcp", "spark", "ai", "data-engineering"],
) as dag:

    # ── TASK 1: Spark Batch Job ───────────────────────────────────────────────
    spark_job = BashOperator(
        task_id="spark_batch_processing",
        bash_command="""
            set -e
            cd /opt/project
            spark-submit \
                --packages org.postgresql:postgresql:42.7.3 \
                spark_dynamic_pipeline.py \
                data/input_file/
        """,
        env={
            "DB_HOST":     os.environ.get("DB_HOST", ""),
            "DB_NAME":     os.environ.get("DB_NAME", ""),
            "DB_USER":     os.environ.get("DB_USER", ""),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
            "JAVA_HOME": "/usr/lib/jvm/temurin-17-jdk-amd64",
            "SPARK_HOME":  "/home/airflow/.local/lib/python3.11/site-packages/pyspark",
            "PATH": "/home/airflow/.local/bin:/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin:/usr/lib/jvm/temurin-17-jdk-amd64/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    # ── TASK 2: MCP AI Combined Insights ─────────────────────────────────────
    def run_ai_insights(**context):
        import subprocess
        from datetime import date

        today = date.today().isoformat()
        spark_insight = f"{INSIGHTS_DIR}/insight_spark_batch_{today}.json"
        kafka_insight  = f"{INSIGHTS_DIR}/insight_kafka_stream_{today}.json"

        # ✅ REMOVED: the hard FileNotFoundError block — app.py handles all cases

        # Clean cache — mirrors your manual command
        for cache in ["__pycache__", "insight_cache"]:
            path = os.path.join(PROJECT_DIR, cache)
            if os.path.exists(path):
                shutil.rmtree(path)

        # ✅ Always use the container's own Python — host Python 3.12 has a GLIBC
        # version mismatch and cannot run inside this container
        host_python = sys.executable
        print(f"🐍 Using Python: {host_python}")

        # ✅ Pass only existing files — empty list is valid, app.py handles it
        available_files = [f for f in [spark_insight, kafka_insight] if os.path.exists(f)]
        print(f"📂 Available insight files: {available_files or 'none — will write no-insight marker'}")

        result = subprocess.run(
            [host_python, "-m", "ai_insight_engine.app"] + available_files,
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            env={**os.environ},
        )

        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"AI insight engine failed:\n{result.stderr}")

        combined = f"{INSIGHTS_DIR}/ai_insights_combined_{today}.json"
        if not os.path.exists(combined):
            raise FileNotFoundError(
                f"Engine ran successfully but output file not found: {combined}"
            )

        print(f"✅ Combined insights saved: {combined}")


    mcp_ai_insights = PythonOperator(
        task_id="mcp_ai_combined_insights",
        python_callable=run_ai_insights,
        provide_context=True,
    )

    # ── TASK 3: Archive Previous Day's Insight Files ──────────────────────────
    def archive_insights(**context):
        from datetime import date, timedelta

        os.makedirs(ARCHIVE_DIR, exist_ok=True)

        yesterday = (date.today() - timedelta(days=1)).isoformat()

        combined_yesterday = f"{INSIGHTS_DIR}/ai_insights_combined_{yesterday}.json"

        if not os.path.exists(combined_yesterday):
            print(
                f"⚠️  No combined AI insight found for {yesterday}. "
                "Nothing to archive — this is expected on first run."
            )
            return

        # Archive spark, kafka, and combined insight files from yesterday
        candidates = [
            f"{INSIGHTS_DIR}/insight_spark_batch_{yesterday}.json",
            f"{INSIGHTS_DIR}/insight_kafka_stream_{yesterday}.json",
            combined_yesterday,
        ]

        archived = []
        for filepath in candidates:
            if os.path.exists(filepath):
                dest = os.path.join(ARCHIVE_DIR, os.path.basename(filepath))
                shutil.move(filepath, dest)
                archived.append(dest)
                print(f"📦 Archived: {os.path.basename(filepath)}")
            else:
                print(f"⚠️  Not found, skipping: {os.path.basename(filepath)}")

        print(f"✅ Done. {len(archived)} file(s) archived for {yesterday}.")

    archive_job = PythonOperator(
        task_id="archive_insight_files",
        python_callable=archive_insights,
        provide_context=True,
    )

    # ── Dependency Chain ──────────────────────────────────────────────────────
    spark_job >> mcp_ai_insights >> archive_job