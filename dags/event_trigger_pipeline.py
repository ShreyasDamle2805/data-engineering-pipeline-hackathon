"""
Manually triggered Spark pipeline DAG.
Runs Spark job only when triggered from Airflow UI/API.
"""

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


# Default arguments applied to all tasks in the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="event_trigger_pipeline",
    description="Run Spark processing only when manually triggered.",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,  # Event-driven DAG
    catchup=False,
    tags=["spark", "manual-trigger"],
) as dag:

    # Submit the Spark job when the DAG is manually triggered.
    # The Spark script itself validates input presence and writes reports.
    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="spark-submit --master local[*] /opt/airflow/spark_jobs/process_data.py",
    )

    run_spark_job