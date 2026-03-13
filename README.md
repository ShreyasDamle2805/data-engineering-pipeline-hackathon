# End-to-End Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline that processes raw data and converts it into analytics-ready datasets. The pipeline uses Apache Spark for distributed data processing, Apache Airflow for workflow orchestration, Delta Lake for reliable storage, and Docker for environment setup.

The system automatically ingests raw datasets, performs data cleaning and transformations, and stores the processed data in Delta format.

---

## Architecture

Raw Data
↓
Input Directory
↓
Airflow DAG (Scheduler / Event Trigger)
↓
Spark Job (PySpark)
↓
Data Cleaning & Transformation
↓
Delta Lake Storage
↓
Processed Dataset for Analytics

---

## Technology Stack

* Python
* Apache Spark (PySpark)
* Apache Airflow
* Delta Lake
* Docker

---

## Project Structure

```
data-engineering-pipeline-hackathon
│
├── dags
│   ├── daily_spark_pipeline.py
│   └── event_trigger_pipeline.py
│
├── spark_jobs
│   └── process_data.py
│
├── data
│   ├── input
│   └── output
│
├── docker
│   └── docker-compose.yml
│
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1. Clone Repository

```
git clone https://github.com/ShreyasDamle2805/data-engineering-pipeline-hackathon.git
cd data-engineering-pipeline-hackathon
```

### 2. Start Docker Environment

```
docker compose up
```

### 3. Install Python Dependencies

```
pip install -r requirements.txt
```

---

## Running the Pipeline

1. Place the dataset inside:

```
data/input/
```

2. Open Airflow UI

```
http://localhost:8081
```

3. Enable the DAG:

* daily_spark_pipeline
* event_trigger_pipeline

4. The Spark job will run and process the data.

---

## Data Processing Steps

The Spark job performs the following transformations:

* Removes duplicate records
* Handles missing values
* Adds a processing timestamp column
* Writes processed data in Delta Lake format

---

## Output

The processed dataset is stored in:

```
data/output/processed/
```

The output follows the Delta Lake structure.

---

## Optional Enhancements

Additional improvements that can be implemented:

* Schema evolution
* Data partitioning
* Data quality validation
* Logging and monitoring
