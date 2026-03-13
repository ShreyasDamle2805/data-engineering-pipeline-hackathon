import glob
import json
import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import NumericType, StringType


def create_spark_session(app_name: str = "ProcessData") -> SparkSession:
    """
    Create and return a SparkSession.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_json_file(path: str, payload: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def read_input_data(spark: SparkSession, input_files: list[str]):
    """
    Read CSV files from the input path with header and schema inference enabled.
    """
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(input_files)
    )
    return df


def clean_data(df):
    """
    Apply basic data quality steps:
    - Remove duplicate rows
    - Drop rows where all columns are null
    - Apply simple generic null handling
    """
    # Remove exact duplicate records
    df = df.dropDuplicates()

    # Drop rows where every column is null
    df = df.na.drop("all")

    # Simple generic null handling:
    # - For numeric columns (all NumericType, including DecimalType): fill nulls with 0
    # - For string columns: fill nulls with "UNKNOWN"
    numeric_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]
    string_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    if numeric_cols:
        df = df.na.fill(0, subset=numeric_cols)
    if string_cols:
        df = df.na.fill("UNKNOWN", subset=string_cols)

    return df


def add_processing_timestamp(df):
    """
    Add a processing timestamp column using the current Spark timestamp.
    """
    return df.withColumn("processing_timestamp", current_timestamp())


def write_failure_report(output_base_path: str, run_id: str, reason: str, input_files: list[str]):
    report_path = (
        output_base_path.rstrip("/")
        + f"/reports/failures/failure_{run_id}.json"
    )
    write_json_file(
        report_path,
        {
            "run_id": run_id,
            "status": "FAILED",
            "reason": reason,
            "input_files": input_files,
        },
    )


def write_duplication_reports(df_raw, df_clean, output_base_path: str, run_id: str):
    reports_base = output_base_path.rstrip("/") + "/reports"
    duplicate_rows_path = reports_base + "/duplicate_rows_detected"

    row_count = df_raw.count()
    dedup_count = df_raw.dropDuplicates().count()
    duplicate_rows_detected = row_count - dedup_count

    duplicate_groups = df_raw.groupBy(df_raw.columns).count().filter("count > 1")
    (
        duplicate_groups
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(duplicate_rows_path)
    )

    write_json_file(
        reports_base + "/duplication_summary.json",
        {
            "run_id": run_id,
            "rows_in_dataset": int(row_count),
            "duplicate_rows_detected": int(duplicate_rows_detected),
            "final_rows_after_cleaning": int(df_clean.count()),
            "status": "SUCCESS",
        },
    )


def archive_processed_files(input_files: list[str], archive_base_path: str, run_id: str):
    archive_dir = archive_base_path.rstrip("/") + f"/processed_{run_id}"
    ensure_dir(archive_dir)

    for file_path in input_files:
        if os.path.isfile(file_path):
            destination = os.path.join(archive_dir, os.path.basename(file_path))
            shutil.move(file_path, destination)


def write_output_data(df, output_base_path: str):
    """
    Write processed data to both Parquet and CSV for easy validation.
    """
    parquet_path = output_base_path.rstrip("/") + "/processed_parquet/"
    csv_path = output_base_path.rstrip("/") + "/processed_csv/"

    # Analytics-friendly format
    df.write.mode("overwrite").parquet(parquet_path)

    # Human-readable output for quick inspection in the folder
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)

    print(f"Wrote Parquet output to: {parquet_path}")
    print(f"Wrote CSV output to: {csv_path}")


def main():
    # Use absolute paths that match Docker volume mounts (/opt/data is shared)
    input_path = "/opt/data/input"
    output_path = "/opt/data/output"
    archive_path = "/opt/data/archive"
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    spark = None
    input_files = sorted(glob.glob(os.path.join(input_path, "*.csv")))

    try:
        if not input_files:
            message = "Empty dataset is detected (no CSV files found in input folder)."
            write_failure_report(output_path, run_id, message, input_files)
            raise ValueError(message)

        spark = create_spark_session()

        # 1. Read raw input data
        df_raw = read_input_data(spark, input_files)
        row_count = df_raw.count()

        if row_count == 0:
            message = "Empty dataset is detected (CSV files contain no data rows)."
            write_failure_report(output_path, run_id, message, input_files)
            raise ValueError(message)

        print("=== Raw Data Schema ===")
        df_raw.printSchema()
        print("=== Sample Raw Data ===")
        df_raw.show(20, truncate=False)

        # 2. Clean and transform data
        df_clean = clean_data(df_raw)
        df_final = add_processing_timestamp(df_clean)

        print("=== Processed Data Schema ===")
        df_final.printSchema()
        print("=== Sample Processed Data ===")
        df_final.show(20, truncate=False)

        # 3. Write outputs and reports
        write_output_data(df_final, output_path)
        write_duplication_reports(df_raw, df_clean, output_path, run_id)

        # 4. Keep source files in input; do not archive automatically.
        print("Source input files retained in /opt/data/input (archiving disabled).")
    except Exception as exc:
        write_failure_report(output_path, run_id, str(exc), input_files)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

