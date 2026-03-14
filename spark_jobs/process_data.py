from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    element_at,
    input_file_name,
    lit,
    regexp_replace,
    split,
    to_date,
    trim,
)


AMAZON_COLUMNS = [
    "order_id",
    "order_date",
    "ship_date",
    "ship_mode",
    "customer_id",
    "customer_name",
    "segment",
    "country",
    "city",
    "state",
    "postal_code",
    "region",
    "product_id",
    "category",
    "sub_category",
    "product_name",
    "sales",
    "quantity",
    "discount",
    "profit",
]

DUPLICATE_KEYS = ["order_id", "product_id", "order_date", "customer_id"]


def resolve_data_root() -> Path:
    """Use container path in Docker, fallback to local repo path for direct runs."""
    container_root = Path("/opt/data")
    if container_root.exists():
        return container_root
    return Path("data")


def create_spark_session(app_name: str = "ProcessAmazonOrders") -> SparkSession:
    """Create Spark session for CSV processing."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_input_data(spark: SparkSession, input_glob: str):
    """Read all CSV files under input path and tag each row with source filename."""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(input_glob)
    )
    return df.withColumn("source_file", element_at(split(input_file_name(), "/"), -1))


def read_single_input_file(spark: SparkSession, file_path: str):
    """Read one CSV file and tag rows with deterministic source filename."""
    source_name = Path(file_path).name
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(file_path)
    )
    return df.withColumn("source_file", lit(source_name))


def ensure_columns(df):
    """Ensure every expected Amazon column exists, even if some files are missing fields."""
    result = df
    for col_name in AMAZON_COLUMNS:
        if col_name not in result.columns:
            result = result.withColumn(col_name, lit(None).cast("string"))
    if "source_file" not in result.columns:
        result = result.withColumn("source_file", lit("unknown.csv"))
    return result.select(*AMAZON_COLUMNS, "source_file")


def adapt_simple_three_column_input(df):
    """Map simple 3-column input (id,name,amount) into pipeline schema."""
    cols = {c.lower() for c in df.columns}
    if "order_id" in cols:
        return df
    if not {"id", "name", "amount"}.issubset(cols):
        return df

    adapted = df
    adapted = adapted.withColumn("order_id", trim(col("id")))
    adapted = adapted.withColumn("customer_name", trim(col("name")))
    adapted = adapted.withColumn("sales", trim(col("amount")))

    # Provide sensible defaults so simple demo files can be processed end-to-end.
    adapted = adapted.withColumn("order_date", lit("2026-03-14"))
    adapted = adapted.withColumn("ship_date", lit("2026-03-16"))
    adapted = adapted.withColumn("ship_mode", lit("Standard Class"))
    adapted = adapted.withColumn("customer_id", concat(lit("CUST-"), trim(col("id"))))
    adapted = adapted.withColumn("segment", lit("Consumer"))
    adapted = adapted.withColumn("country", lit("India"))
    adapted = adapted.withColumn("city", lit("DemoCity"))
    adapted = adapted.withColumn("state", lit("DemoState"))
    adapted = adapted.withColumn("postal_code", lit("000000"))
    adapted = adapted.withColumn("region", lit("Demo"))
    adapted = adapted.withColumn("product_id", concat(lit("PROD-"), trim(col("id"))))
    adapted = adapted.withColumn("category", lit("General"))
    adapted = adapted.withColumn("sub_category", lit("General"))
    adapted = adapted.withColumn("product_name", concat(lit("Item-"), trim(col("name"))))
    adapted = adapted.withColumn("quantity", lit("1"))
    adapted = adapted.withColumn("discount", lit("0"))
    adapted = adapted.withColumn("profit", lit("0"))

    return adapted


def normalize_and_cast(df):
    """Trim strings and cast to target data types used for quality checks."""
    normalized = df
    for col_name in AMAZON_COLUMNS:
        normalized = normalized.withColumn(col_name, trim(col(col_name)))

    normalized = (
        normalized.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd"))
        .withColumn("sales", regexp_replace(col("sales"), "[$,]", "").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("discount", col("discount").cast("double"))
        .withColumn("profit", regexp_replace(col("profit"), "[$,]", "").cast("double"))
    )
    return normalized


def split_valid_invalid(df):
    """Split cleanable rows from invalid rows based on business and type rules."""
    valid_condition = (
        col("order_id").isNotNull()
        & (col("order_id") != "")
        & col("order_date").isNotNull()
        & col("ship_date").isNotNull()
        & col("customer_id").isNotNull()
        & (col("customer_id") != "")
        & col("product_id").isNotNull()
        & (col("product_id") != "")
        & col("sales").isNotNull()
        & (col("sales") >= 0)
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("discount").isNotNull()
        & (col("discount") >= 0)
        & (col("discount") <= 1)
    )
    valid_df = df.where(valid_condition)
    invalid_df = df.where(~valid_condition)
    return valid_df, invalid_df


def separate_duplicates(df):
    """Split valid rows into duplicate and deduplicated datasets."""
    duplicate_keys_df = (
        df.groupBy("source_file", *DUPLICATE_KEYS)
        .count()
        .where(col("count") > 1)
        .drop("count")
    )
    duplicate_rows_df = duplicate_keys_df.join(
        df, on=["source_file", *DUPLICATE_KEYS], how="inner"
    )
    deduped_df = df.dropDuplicates(["source_file", *DUPLICATE_KEYS])
    return deduped_df, duplicate_rows_df


def write_csv_dataset(df, path: Path, partitions: int):
    """Write dataframe to CSV with a controlled number of part files."""
    (
        df.repartition(partitions)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(path))
    )


def write_report_summary(report_path: Path, summary: dict):
    """Write run-level summary report as JSON file."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)


def ensure_writable_output_path(path: Path):
    """Normalize directory permissions so Spark executors can write on shared mounts."""
    path.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(path):
        root_path = Path(root)
        try:
            os.chmod(root_path, 0o777)
        except OSError:
            pass
        for dirname in dirs:
            try:
                os.chmod(root_path / dirname, 0o777)
            except OSError:
                pass
        for filename in files:
            try:
                os.chmod(root_path / filename, 0o666)
            except OSError:
                pass


def safe_file_stem(filename: str) -> str:
    stem = filename.rsplit(".", 1)[0]
    return re.sub(r"[^a-zA-Z0-9_-]", "_", stem)


def build_counts_by_source(df, count_name: str):
    if df is None:
        return {}
    rows = df.groupBy("source_file").count().collect()
    return {row["source_file"]: int(row["count"]) for row in rows}


def union_dataframes(frames: list[DataFrame]):
    """Union multiple dataframes by name with missing columns allowed."""
    if not frames:
        return None
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def write_per_input_reports(
    reports_path: Path,
    run_id: str,
    source_files: list[str],
    raw_counts: dict,
    valid_counts: dict,
    clean_counts: dict,
    duplicate_counts: dict,
    invalid_counts: dict,
    duplicate_rows_df,
):
    per_input_summary_dir = reports_path / "duplication_summary_by_input" / run_id
    per_input_summary_flat_dir = reports_path / "duplication_summary" / run_id
    duplicate_rows_per_input_dir = reports_path / "duplicate_rows_by_input" / run_id
    per_input_summary_dir.mkdir(parents=True, exist_ok=True)
    per_input_summary_flat_dir.mkdir(parents=True, exist_ok=True)
    duplicate_rows_per_input_dir.mkdir(parents=True, exist_ok=True)

    for source_file in sorted(source_files):
        rows_in_dataset = raw_counts.get(source_file, 0)
        duplicates_detected = duplicate_counts.get(source_file, 0)
        final_rows_after_cleaning = clean_counts.get(source_file, 0)
        invalid_rows_detected = invalid_counts.get(source_file, 0)
        valid_rows_before_dedup = valid_counts.get(source_file, 0)

        per_input_summary = {
            "run_id": run_id,
            "input_file": source_file,
            "rows_in_dataset": rows_in_dataset,
            "valid_rows_before_dedup": valid_rows_before_dedup,
            "duplicate_rows_detected": duplicates_detected,
            "invalid_rows_detected": invalid_rows_detected,
            "final_rows_after_cleaning": final_rows_after_cleaning,
            "status": "SUCCESS",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        summary_file = per_input_summary_dir / f"{safe_file_stem(source_file)}.json"
        write_report_summary(summary_file, per_input_summary)

        # Flat per-file summaries with explicit naming for easier discovery.
        summary_file_flat = per_input_summary_flat_dir / f"duplication_summary_{safe_file_stem(source_file)}.json"
        write_report_summary(summary_file_flat, per_input_summary)

        per_input_duplicate_path = duplicate_rows_per_input_dir / safe_file_stem(source_file)
        duplicate_rows_for_file = duplicate_rows_df.where(col("source_file") == source_file)
        write_csv_dataset(duplicate_rows_for_file, per_input_duplicate_path, 1)


def main():
    data_root = resolve_data_root()
    input_files_override = os.getenv("INPUT_FILES", "").strip()
    input_glob_override = os.getenv("INPUT_GLOB", "").strip()

    if input_files_override:
        input_files = [path for path in input_files_override.split(":") if path.strip()]
        input_glob = "(from INPUT_FILES)"
    elif input_glob_override:
        input_glob = input_glob_override
        input_files = sorted(glob.glob(input_glob))
    else:
        input_glob = str(data_root / "input" / "*.csv")
        input_files = sorted(glob.glob(input_glob))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_base = data_root / "output"
    processed_csv_path = output_base / "processed_csv" / run_id
    clean_csv_path = data_root / "clean_data_csv" / run_id
    reports_path = output_base / "reports"
    duplicates_path = reports_path / "duplicate_rows_detected" / run_id
    invalid_rows_path = reports_path / "invalid_rows_detected" / run_id
    summary_path = reports_path / "summary.json"
    duplication_summary_path = reports_path / "duplication_summary.json"

    spark = create_spark_session()

    # Shared bind mounts can carry restrictive ownership from previous runs/containers.
    ensure_writable_output_path(output_base)
    ensure_writable_output_path(processed_csv_path)
    ensure_writable_output_path(clean_csv_path)
    ensure_writable_output_path(reports_path)

    if not input_files:
        summary = {
            "run_id": run_id,
            "status": "failed",
            "reason": "No input rows found in data/input/*.csv",
            "raw_rows": 0,
            "clean_rows": 0,
            "invalid_rows": 0,
            "duplicate_rows": 0,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        write_report_summary(summary_path, summary)
        raise ValueError("No CSV rows found in input folder.")

    typed_frames = []
    valid_frames = []
    invalid_frames = []
    deduped_frames = []
    duplicate_frames = []

    for file_path in input_files:
        df_file_raw = read_single_input_file(spark, file_path)
        df_file_raw = adapt_simple_three_column_input(df_file_raw)
        df_file_prepared = ensure_columns(df_file_raw)
        df_file_typed = normalize_and_cast(df_file_prepared)
        df_file_valid, df_file_invalid = split_valid_invalid(df_file_typed)
        df_file_deduped, df_file_duplicates = separate_duplicates(df_file_valid)

        typed_frames.append(df_file_typed)
        valid_frames.append(df_file_valid)
        invalid_frames.append(df_file_invalid)
        deduped_frames.append(df_file_deduped)
        duplicate_frames.append(df_file_duplicates)

    df_typed = union_dataframes(typed_frames)
    valid_df = union_dataframes(valid_frames)
    invalid_df = union_dataframes(invalid_frames)
    deduped_df = union_dataframes(deduped_frames)
    duplicate_rows_df = union_dataframes(duplicate_frames)

    if deduped_df is None:
        deduped_df = spark.createDataFrame([], ensure_columns(read_single_input_file(spark, input_files[0])).schema)
    if invalid_df is None:
        invalid_df = deduped_df.limit(0)
    if duplicate_rows_df is None:
        duplicate_rows_df = deduped_df.limit(0)
    if valid_df is None:
        valid_df = deduped_df.limit(0)
    if df_typed is None:
        df_typed = deduped_df.limit(0)

    final_df = deduped_df.withColumn("processing_timestamp", current_timestamp())

    raw_count = df_typed.count()
    clean_count = final_df.count()
    invalid_count = invalid_df.count()
    duplicate_count = duplicate_rows_df.count()

    source_files = [r["source_file"] for r in df_typed.select("source_file").distinct().collect()]

    raw_counts = build_counts_by_source(df_typed, "rows_in_dataset")
    valid_counts = build_counts_by_source(valid_df, "valid_rows_before_dedup")
    clean_counts = build_counts_by_source(final_df, "final_rows_after_cleaning")
    duplicate_counts = build_counts_by_source(duplicate_rows_df, "duplicate_rows_detected")
    invalid_counts = build_counts_by_source(invalid_df, "invalid_rows_detected")

    # Single-part writes are more stable on local Windows bind mounts.
    partitions = 1
    write_csv_dataset(final_df, processed_csv_path, partitions)
    write_csv_dataset(final_df, clean_csv_path, partitions)
    write_csv_dataset(duplicate_rows_df, duplicates_path, partitions)
    write_csv_dataset(invalid_df, invalid_rows_path, partitions)

    write_per_input_reports(
        reports_path,
        run_id,
        source_files,
        raw_counts,
        valid_counts,
        clean_counts,
        duplicate_counts,
        invalid_counts,
        duplicate_rows_df,
    )

    summary = {
        "run_id": run_id,
        "status": "success",
        "raw_rows": raw_count,
        "valid_rows_before_dedup": valid_df.count(),
        "clean_rows": clean_count,
        "invalid_rows": invalid_count,
        "duplicate_rows": duplicate_count,
        "output_processed_csv": str(processed_csv_path),
        "output_clean_csv": str(clean_csv_path),
        "report_duplicates_csv": str(duplicates_path),
        "report_invalid_rows_csv": str(invalid_rows_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_report_summary(summary_path, summary)

    legacy_duplication_summary = {
        "run_id": run_id,
        "rows_in_dataset": raw_count,
        "duplicate_rows_detected": duplicate_count,
        "final_rows_after_cleaning": clean_count,
        "status": "SUCCESS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "per_input_summary_folder": str(reports_path / "duplication_summary_by_input"),
        "per_input_summary_flat_folder": str(reports_path / "duplication_summary"),
    }
    write_report_summary(duplication_summary_path, legacy_duplication_summary)

    print(f"Run ID: {run_id}")
    print(f"Raw rows: {raw_count}")
    print(f"Clean rows: {clean_count}")
    print(f"Invalid rows: {invalid_count}")
    print(f"Duplicate rows: {duplicate_count}")
    print(f"Processed CSV path: {processed_csv_path}")
    print(f"Clean CSV path: {clean_csv_path}")
    print(f"Reports path: {reports_path}")

    spark.stop()


if __name__ == "__main__":
    main()

