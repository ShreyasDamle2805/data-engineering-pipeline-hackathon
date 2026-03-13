"""
Daily Spark pipeline DAG.

Runs a Spark job every day at 5:00 AM.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


# Default arguments applied to all tasks in the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="daily_spark_pipeline",
    description="Run Spark processing daily at 5:00 AM.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    tags=["spark", "daily"],
) as dag:
    # Submit the Spark job that performs the data processing.
    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="spark-submit /opt/airflow/spark_jobs/process_data.py",
    )

