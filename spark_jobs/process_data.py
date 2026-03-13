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


def read_input_data(spark: SparkSession, input_path: str):
    """
    Read CSV files from the input path with header and schema inference enabled.
    """
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(input_path)
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


def write_output_data(df, output_path: str):
    """
    Write the DataFrame in overwrite mode.
    Tries Delta first; falls back to Parquet if Delta jars are unavailable.
    """
    try:
        df.write.format("delta").mode("overwrite").save(output_path)
    except Exception as exc:
        fallback_path = output_path.rstrip("/") + "_parquet/"
        print(f"Delta write unavailable ({exc}). Writing Parquet to {fallback_path}")
        df.write.mode("overwrite").parquet(fallback_path)


def main():
    # Use absolute paths that match Docker volume mounts (/opt/data is shared)
    input_path = "/opt/data/input/"
    output_path = "/opt/data/output/processed/"

    spark = create_spark_session()

    # 1. Read raw input data
    df_raw = read_input_data(spark, input_path)

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

    # 3. Write to Delta format
    write_output_data(df_final, output_path)

    spark.stop()


if __name__ == "__main__":
    main()

