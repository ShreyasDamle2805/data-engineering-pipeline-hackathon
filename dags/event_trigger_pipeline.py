"""
Event-triggered Spark pipeline DAG.

Watches the input folder and runs the Spark job when data arrives.
"""

from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor


# Default arguments applied to all tasks in the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="event_trigger_pipeline",
    description="Trigger Spark processing when files appear in the input folder.",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,  # Event-driven DAG
    catchup=False,
    tags=["spark", "sensor"],
) as dag:

    # Wait for CSV files to appear in the input directory
    wait_for_input = FileSensor(
        task_id="wait_for_input",
        filepath="/opt/data/input/*.csv",
        poke_interval=30,  # Check every 30 seconds
        timeout=60 * 60 * 24,  # Stop waiting after 24 hours
        mode="reschedule",
    )

    # Submit the Spark job once input data is detected
    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="spark-submit /opt/spark_jobs/process_data.py",
    )

    # Task dependency
    wait_for_input >> run_spark_job