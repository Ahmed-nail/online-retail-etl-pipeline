from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

# 1. Spark Session
spark = SparkSession.builder.appName("etl-Project").getOrCreate()

# 2. Load Data
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

df.show(5)
df.printSchema()

# 3. Data Cleaning

df = df.dropna(subset=["CustomerID"])

df = df.filter(col("Quantity") > 0)

df = df.filter(col("UnitPrice") > 0)

df.show(5)

# 4. Rename Columns
df = df.withColumnRenamed("InvoiceNo", "order_id") \
       .withColumnRenamed("CustomerID", "customer_id") \
       .withColumnRenamed("StockCode", "product_id") \
       .withColumnRenamed("Description", "product_name") \
       .withColumnRenamed("UnitPrice", "price") \
       .withColumnRenamed("InvoiceDate", "date")

df.printSchema()

# 5. Fix Date Format
df = df.withColumn("date", to_timestamp(col("date"), "M/d/yyyy H:mm"))

df.show(5)

# 6. STAR SCHEMA

# FACT TABLE
fact = df.select(
    "order_id",
    "customer_id",
    "product_id",
    "Quantity",
    "price",
    "date"
)

# DIM CUSTOMERS
dim_customers = df.select(
    "customer_id",
    "Country"
).dropDuplicates()

# DIM PRODUCTS
dim_products = df.select(
    "product_id",
    "product_name"
).dropDuplicates()

# DIM TIME
dim_time = df.select("date").dropDuplicates()

# 7. SHOW RESULTS
print("FACT TABLE")
fact.show(5)

print("CUSTOMERS DIM")
dim_customers.show(5)

print("PRODUCTS DIM")
dim_products.show(5)

print("TIME DIM")
dim_time.show(5)

# 8. SAVE OUTPUT
fact.write.mode("overwrite").parquet("output/fact")
dim_customers.write.mode("overwrite").parquet("output/dim_customers")
dim_products.write.mode("overwrite").parquet("output/dim_products")
dim_time.write.mode("overwrite").parquet("output/dim_time")

print("All tables saved successfully")