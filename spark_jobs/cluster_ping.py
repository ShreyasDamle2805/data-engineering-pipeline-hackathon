from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("EventClusterPing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Tiny distributed action so the event appears in Spark master UI.
    count = spark.range(1, 50000).count()
    print(f"Cluster ping count: {count}")
    spark.stop()


if __name__ == "__main__":
    main()
