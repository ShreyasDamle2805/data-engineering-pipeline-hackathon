FROM apache/airflow:2.9.3

# Install Java (required by PySpark) as root
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark + Delta Lake as airflow user (recommended by Airflow docs)
USER airflow
RUN pip install --no-cache-dir pyspark delta-spark

