from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

SNOWFLAKE_CONFIG = {
    "user":      "AhmedNail",
    "password":  "RsBVJjci97dEkgb",
    "account":   "qhaiwqt-jd23128",
    "warehouse": "COMPUTE_WH",
    "database":  "ETL_PROJECT",
    "schema":    "STAR_SCHEMA"
}

BATCH_DIR  = "/opt/airflow/data/batches"
HDFS_PATH  = "hdfs://hadoop-namenode:9000/user/airflow/data/batches/"

def spark_extract():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("OnlineRetailETL") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .getOrCreate()

    df = spark.read.csv(
        HDFS_PATH,
        header=True,
        inferSchema=True
    )

    count = df.count()
    print(f"read spark {count:,} rows from HDFS")
    df.printSchema()
    spark.stop()

def extract():
    files = sorted(os.listdir(BATCH_DIR))[:50]
    all_data = []
    for file in files:
        path = os.path.join(BATCH_DIR, file)
        df = pd.read_csv(path, encoding="ISO-8859-1")
        all_data.append(df)
    df = pd.concat(all_data, ignore_index=True)
    df.to_csv("/opt/airflow/data/raw.csv", index=False)
    print(f"Extract: {len(df):,} row")

def transform():
    df = pd.read_csv("/opt/airflow/data/raw.csv")
    df = df.dropna(subset=["CustomerID"])
    df = df[df["Quantity"] > 0]
    df = df[df["UnitPrice"] > 0]
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["CustomerID"] = df["CustomerID"].astype(int).astype(str)
    df.to_csv("/opt/airflow/data/clean.csv", index=False)
    print(f"Transform: {len(df):,} clean row")

def load():
    df = pd.read_csv("/opt/airflow/data/clean.csv")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"]).dt.date

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    for table in ["CUSTOMERS", "PRODUCTS", "TIME_DIM", "FACT_SALES"]:
        cursor.execute(f"TRUNCATE TABLE {table}")

    customers = df[["CustomerID", "Country"]].drop_duplicates().reset_index(drop=True)
    customers.columns = ["CUSTOMER_ID", "COUNTRY"]
    write_pandas(conn, customers, "CUSTOMERS")

    products = df[["StockCode", "Description"]].drop_duplicates().reset_index(drop=True)
    products.columns = ["PRODUCT_ID", "PRODUCT_NAME"]
    write_pandas(conn, products, "PRODUCTS")

    time_dim = df[["InvoiceDate"]].drop_duplicates().reset_index(drop=True)
    time_dim.columns = ["DATE"]
    time_dim["DAY"]   = time_dim["DATE"].apply(lambda x: x.day)
    time_dim["MONTH"] = time_dim["DATE"].apply(lambda x: x.month)
    time_dim["YEAR"]  = time_dim["DATE"].apply(lambda x: x.year)
    write_pandas(conn, time_dim, "TIME_DIM")

    fact = df[["InvoiceNo", "CustomerID", "StockCode", "Quantity", "UnitPrice", "InvoiceDate"]].copy()
    fact.columns = ["ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "QUANTITY", "PRICE", "DATE"]
    fact = fact.reset_index(drop=True)
    write_pandas(conn, fact, "FACT_SALES")

    cursor.close()
    conn.close()
    print("Loaded to Snowflake successfully")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    t0 = PythonOperator(task_id="spark_extract", python_callable=spark_extract)
    t1 = PythonOperator(task_id="extract",       python_callable=extract)
    t2 = PythonOperator(task_id="transform",     python_callable=transform)
    t3 = PythonOperator(task_id="load",          python_callable=load)

    t0 >> t1 >> t2 >> t3