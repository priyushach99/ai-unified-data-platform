from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="batch_spark_csv_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger for now
    catchup=False
) as dag:

    run_spark_batch = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        spark-submit \
            --master local[*] \
            --packages org.postgresql:postgresql:42.7.3 \
            /opt/airflow/project/spark_dynamic_pipeline.py \
            /opt/airflow/project/data/input_file
        """
    )

    run_spark_batch