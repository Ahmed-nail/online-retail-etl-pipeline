# Online Retail Data Engineering Pipeline

## Overview

This project implements an end-to-end data engineering pipeline for processing online retail transaction data.

The pipeline ingests raw batch files, performs distributed data cleaning and transformation using Apache Spark, stores intermediate and processed data in HDFS, orchestrates workflow execution with Apache Airflow, and loads curated analytical tables into Snowflake.

The final warehouse follows a star schema design to support analytics and business intelligence workloads.

---

## Objectives

* Build a reproducible ETL pipeline
* Process retail transaction data at scale
* Use distributed storage and distributed computation
* Load clean analytical data into a cloud data warehouse
* Orchestrate the workflow using Airflow

---

## Tech Stack

* **Apache Airflow** — workflow orchestration
* **Apache Spark (PySpark)** — distributed data processing
* **HDFS** — distributed storage
* **YARN** — cluster resource management
* **Snowflake** — cloud data warehouse
* **Docker Compose** — local containerized environment

---

## Project Structure

```text
etl-project/
│
├── dags/
│   └── etl-dag.py
│
├── data/
│   ├── batches/
│   ├── raw.csv
│   └── clean.csv
│
├── spark/
│   └── spark_etl.py
│
├── sql/
│   └── snowflake_schema.sql
│
├── docs/
│   ├── architecture.png
│   └── dwh_schema.png
│
├── README.md
└── docker-compose.yaml
```

---

## Pipeline Architecture

### Flow

Raw batch files are placed inside the `data/batches` directory.

Airflow orchestrates the pipeline in three stages:

### 1. Extract

* Batch CSV files are collected
* Raw records are merged into a single dataset

### 2. Transform

Spark reads the raw data from HDFS and performs:

* null value removal
* invalid quantity filtering
* invalid price filtering
* schema normalization
* star schema transformation

### 3. Load

Processed tables are loaded into Snowflake.

---

## Data Warehouse Design

The project uses a **star schema**.

### Fact Table

* `FACT_SALES`

### Dimension Tables

* `CUSTOMERS`
* `PRODUCTS`
* `TIME_DIM`

This design improves analytical query performance and keeps fact and dimension data separated.

---

## Airflow DAG

The pipeline contains the following tasks:

* `extract`
* `spark_transform`
* `load`

Execution order:

```text
extract → spark_transform → load
```

---

## Running the Project

### Start services

```bash
docker compose up -d
```

### Open Airflow

```text
http://localhost:18080
```

### Open Spark Jupyter

```text
http://localhost:8899
```

---

## HDFS Data Upload

Create HDFS directory:

```bash
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/airflow/data/batches
```

Copy local batches:

```bash
docker cp ./data/batches hadoop-namenode:/tmp/batches
```

Upload to HDFS:

```bash
docker exec hadoop-namenode hdfs dfs -put /tmp/batches /user/airflow/data/
```

---

## Output Validation

The pipeline was validated by:

* successful Airflow DAG execution
* Spark logs
* HDFS generated output files
* Snowflake table loading
* SQL validation queries

Example validation query:

```sql
SELECT COUNT(*) FROM FACT_SALES;
```

---

## Key Learning Outcomes

This project demonstrates:

* ETL orchestration with Airflow
* distributed storage with HDFS
* distributed processing with Spark
* YARN-based resource management
* dimensional modeling
* loading curated analytical data into Snowflake


