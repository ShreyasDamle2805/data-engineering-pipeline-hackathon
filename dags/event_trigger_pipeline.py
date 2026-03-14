"""Manually triggered Spark pipeline DAG for Amazon order processing."""

import glob
import json
import os
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator


# Default arguments applied to all tasks in the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="event_trigger_pipeline",
    description="Run Spark processing when new input files are detected or when triggered manually.",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "manual-trigger", "amazon-orders"],
) as dag:

    def has_new_or_changed_input_files(**context):
        """Return True only when new/updated CSV files appear in /opt/data/input."""
        dag_run = context.get("dag_run")
        manual_input_glob = None
        if dag_run and dag_run.conf:
            manual_input_glob = dag_run.conf.get("input_glob")

        # If a specific path/glob is provided manually, honor that request.
        if manual_input_glob:
            context["ti"].xcom_push(key="changed_files", value=[manual_input_glob])
            return True

        input_pattern = "/opt/data/input/*.csv"
        state_path = "/opt/data/output/reports/.input_watch_state.json"
        seen_state_path = "/opt/data/output/reports/.input_seen_files.json"

        current_state = {}
        for path in sorted(glob.glob(input_pattern)):
            try:
                stat = os.stat(path)
                current_state[os.path.basename(path)] = {
                    "mtime": stat.st_mtime,
                    "ctime": stat.st_ctime,
                    "size": stat.st_size,
                }
            except OSError:
                # Ignore files that may have been moved while scanning.
                continue

        previous_state = {}
        if os.path.exists(state_path):
            try:
                with open(state_path, "r", encoding="utf-8") as handle:
                    previous_state = json.load(handle)
            except (OSError, json.JSONDecodeError):
                previous_state = {}

        seen_files = set()
        if os.path.exists(seen_state_path):
            try:
                with open(seen_state_path, "r", encoding="utf-8") as handle:
                    loaded_seen = json.load(handle)
                if isinstance(loaded_seen, list):
                    seen_files = {str(item) for item in loaded_seen}
                elif isinstance(loaded_seen, dict):
                    seen_files = set(loaded_seen.keys())
            except (OSError, json.JSONDecodeError):
                seen_files = set()

        def _as_signature(state_entry):
            """Normalize old/new state formats for reliable comparisons."""
            if isinstance(state_entry, dict):
                return (
                    state_entry.get("mtime"),
                    state_entry.get("ctime"),
                    state_entry.get("size"),
                )
            # Backward compatibility with old mtime-only state.
            return (state_entry, None, None)

        changed_files = []

        # Always trigger for never-seen filenames so new file adds are reliable.
        newly_added = [
            f"/opt/data/input/{filename}"
            for filename in current_state.keys()
            if filename not in seen_files
        ]

        for filename, file_state in current_state.items():
            if _as_signature(previous_state.get(filename)) != _as_signature(file_state):
                changed_files.append(f"/opt/data/input/{filename}")

        changed_files = sorted(set(newly_added + changed_files))

        removed_files = sorted(
            f"/opt/data/input/{filename}"
            for filename in previous_state.keys()
            if filename not in current_state
        )

        # First run after deploying watcher: establish baseline and skip processing.
        if not previous_state:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            with open(state_path, "w", encoding="utf-8") as handle:
                json.dump(current_state, handle, indent=2)
            with open(seen_state_path, "w", encoding="utf-8") as handle:
                json.dump(sorted(current_state.keys()), handle, indent=2)
            raise AirflowSkipException("Watcher baseline initialized. No new file event yet.")

        if not changed_files and not removed_files:
            raise AirflowSkipException("No new, changed, or removed input CSV files detected.")

        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as handle:
            json.dump(current_state, handle, indent=2)
        with open(seen_state_path, "w", encoding="utf-8") as handle:
            json.dump(sorted(current_state.keys()), handle, indent=2)

        context["ti"].xcom_push(key="changed_files", value=changed_files)
        context["ti"].xcom_push(key="removed_files", value=removed_files)

        return True

    check_for_new_files = ShortCircuitOperator(
        task_id="check_for_new_files",
        python_callable=has_new_or_changed_input_files,
    )

    # Triggered from Airflow UI/API: process all CSVs and generate quality reports.
    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            "spark-submit --master spark://spark:7077 --name event_status_{{ ts_nodash }} "
            "/opt/airflow/spark_jobs/cluster_ping.py && "
            "INPUT_FILES='{{ (ti.xcom_pull(task_ids=\"check_for_new_files\", key=\"changed_files\") or []) | join(\":\") }}' "
            "INPUT_GLOB='{{ dag_run.conf.get(\"input_glob\", \"\") }}' "
            "spark-submit --master local[*] --name event_process_{{ ts_nodash }} "
            "/opt/airflow/spark_jobs/process_data.py"
        ),
    )

    check_for_new_files >> run_spark_job