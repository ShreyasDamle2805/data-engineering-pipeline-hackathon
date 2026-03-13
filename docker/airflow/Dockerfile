FROM apache/airflow:2.9.3

# Install Java (required by PySpark) as root
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark + Delta Lake versions compatible with Spark 3.5.x
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0

