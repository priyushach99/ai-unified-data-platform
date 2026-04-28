from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_kafka():
    print("Kafka assumed running (external service)")

def check_db():
    print("DB check placeholder")

with DAG(
    dag_id="system_health",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    kafka = PythonOperator(
        task_id="kafka_check",
        python_callable=check_kafka
    )

    db = PythonOperator(
        task_id="db_check",
        python_callable=check_db
    )

    kafka >> db