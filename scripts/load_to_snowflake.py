import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# CONNECT
conn = snowflake.connector.connect(
    user="AhmedNAil",
    password="",
    account="qhaiwqt-jd23128",
    warehouse="COMPUTE_WH",
    database="ETL_PROJECT",
    schema="STAR_SCHEMA"
)
cursor = conn.cursor()

# CLEAN TABLES
cursor.execute("TRUNCATE TABLE CUSTOMERS")
cursor.execute("TRUNCATE TABLE PRODUCTS")
cursor.execute("TRUNCATE TABLE TIME_DIM")
cursor.execute("TRUNCATE TABLE FACT_SALES")
print("Tables cleared")

# LOAD BATCHES
BATCH_DIR = r"C:\Users\ahmed\Desktop\etl-project\data\batches"
files = sorted(os.listdir(BATCH_DIR))[:50]

all_data = []
for file in files:
    path = os.path.join(BATCH_DIR, file)
    df = pd.read_csv(path, encoding="ISO-8859-1")
    df = df.dropna(subset=["CustomerID"])
    df = df[df["Quantity"] > 0]
    df = df[df["UnitPrice"] > 0]
    all_data.append(df)

df = pd.concat(all_data, ignore_index=True)
print(f"Total rows: {len(df):,}")

# CLEAN + TRANSFORM
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["CustomerID"] = df["CustomerID"].astype(int).astype(str)

# 1. CUSTOMERS
customers = df[["CustomerID", "Country"]].drop_duplicates().reset_index(drop=True)
customers.columns = ["CUSTOMER_ID", "COUNTRY"]
write_pandas(conn, customers, "CUSTOMERS")
print("CUSTOMERS ")

# 2. PRODUCTS
products = df[["StockCode", "Description"]].drop_duplicates().reset_index(drop=True)
products.columns = ["PRODUCT_ID", "PRODUCT_NAME"]
write_pandas(conn, products, "PRODUCTS")
print("PRODUCTS ")

# 3. TIME_DIM
time_dim = df[["InvoiceDate"]].drop_duplicates().reset_index(drop=True)
time_dim.columns = ["DATE"]
time_dim["DATE"] = time_dim["DATE"].dt.date
time_dim["DAY"]   = time_dim["DATE"].apply(lambda x: x.day)
time_dim["MONTH"] = time_dim["DATE"].apply(lambda x: x.month)
time_dim["YEAR"]  = time_dim["DATE"].apply(lambda x: x.year)
write_pandas(conn, time_dim, "TIME_DIM")
print("TIME_DIM ")

# 4. FACT_SALES
fact = df[[
    "InvoiceNo",
    "CustomerID",
    "StockCode",
    "Quantity",
    "UnitPrice",
    "InvoiceDate"
]].copy()

fact["InvoiceDate"] = fact["InvoiceDate"].dt.date

fact.columns = [
    "ORDER_ID",
    "CUSTOMER_ID",
    "PRODUCT_ID",
    "QUANTITY",
    "PRICE",
    "DATE"
]

fact = fact.astype({
    "ORDER_ID": "str",
    "CUSTOMER_ID": "str",
    "PRODUCT_ID": "str"
})

fact = fact.reset_index(drop=True)

write_pandas(conn, fact, "FACT_SALES")
print("FACT_SALES")

cursor.close()
conn.close()
print("ALL DATA LOADED SUCCESSFULLY!")
