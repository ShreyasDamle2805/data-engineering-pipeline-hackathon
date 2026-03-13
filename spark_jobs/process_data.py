from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import NumericType, StringType


def create_spark_session(app_name: str = "ProcessData") -> SparkSession:
    """
    Create and return a SparkSession configured for Delta Lake.
    Assumes Delta Lake dependencies are available in the environment.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
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
    Write the DataFrame to Delta format in overwrite mode.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .save(output_path)
    )


def main():
    input_path = "data/input/"
    output_path = "data/output/"

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

