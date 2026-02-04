# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrame Basics - Solutions
# MAGIC Topics: Creating DataFrames, Schema, Basic Operations, Reading/Writing Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, when, months_between, current_date

# For local development only (Databricks has spark session pre-configured)
# spark = SparkSession.builder.appName("DataFrame Solutions").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Create DataFrame from List

# COMMAND ----------

data = [("Alice", 30, "Engineering"), ("Bob", 25, "Sales"), ("Charlie", 35, "HR")]

# Method 1: With column names
df1 = spark.createDataFrame(data, ["name", "age", "department"])
df1.show()

# COMMAND ----------

# Method 2: With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True)
])
df1 = spark.createDataFrame(data, schema)
df1.show()
df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Read CSV with Schema

# COMMAND ----------

emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Adjust path for your environment
employees = spark.read.csv(
    "datasets/csv/employees.csv",
    header=True,
    schema=emp_schema,
    nullValue=""
)
employees.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: DataFrame Info

# COMMAND ----------

# a) Print schema
employees.printSchema()

# COMMAND ----------

# b) Show first 5 rows
employees.show(5)

# COMMAND ----------

# c) Count rows
print(f"Total rows: {employees.count()}")

# COMMAND ----------

# d) Column names
print(f"Columns: {employees.columns}")

# COMMAND ----------

# e) Data types
print(f"Data types: {employees.dtypes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Select and Alias

# COMMAND ----------

# a) Select columns
df = employees.select("name", "salary")
df.show()

# COMMAND ----------

# b) Rename columns
df = employees.select(
    col("name").alias("employee_name"),
    col("salary").alias("annual_salary")
)
df.show()

# COMMAND ----------

# c) Add computed column
df = employees.select(
    col("name"),
    col("salary"),
    (col("salary") / 12).alias("monthly_salary")
)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Filter Operations

# COMMAND ----------

# a) Salary > 80000
df = employees.filter(col("salary") > 80000)
df.show()

# COMMAND ----------

# b) Engineering AND salary > 90000
df = employees.filter((col("department") == "Engineering") & (col("salary") > 90000))
df.show()

# COMMAND ----------

# c) City is New York OR San Francisco
df = employees.filter(col("city").isin("New York", "San Francisco"))
df.show()

# COMMAND ----------

# d) Name starts with 'J'
df = employees.filter(col("name").startswith("J"))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Distinct and Drop Duplicates

# COMMAND ----------

# a) Distinct departments
employees.select("department").distinct().show()

# COMMAND ----------

# b) Distinct department-city combinations
employees.select("department", "city").distinct().show()

# COMMAND ----------

# c) Drop duplicates on department
employees.dropDuplicates(["department"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Sort and OrderBy

# COMMAND ----------

# a) Sort by salary descending
employees.orderBy(col("salary").desc()).show()

# COMMAND ----------

# b) Sort by department asc, salary desc
employees.orderBy(col("department").asc(), col("salary").desc()).show()

# COMMAND ----------

# c) Top 3 highest paid
employees.orderBy(col("salary").desc()).limit(3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Add, Rename, Drop Columns

# COMMAND ----------

# a) Add tax column
df = employees.withColumn("tax", col("salary") * 0.3)
df.show()

# COMMAND ----------

# b) Add years employed (approximate)
df = employees.withColumn(
    "years_employed",
    (months_between(current_date(), col("hire_date")) / 12).cast("int")
)
df.show()

# COMMAND ----------

# c) Rename column
df = employees.withColumnRenamed("emp_id", "employee_id")
df.show()

# COMMAND ----------

# d) Drop column
df = employees.drop("manager_id")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Write DataFrame

# COMMAND ----------

# a) Write as Parquet partitioned by department
employees.write.mode("overwrite").partitionBy("department").parquet("/tmp/employees_parquet")
print("Written to Parquet with partitioning")

# COMMAND ----------

# b) Write as CSV
employees.write.mode("overwrite").option("header", True).csv("/tmp/employees_csv")
print("Written to CSV")

# COMMAND ----------

# c) Write as JSON (single file)
employees.coalesce(1).write.mode("overwrite").json("/tmp/employees_json")
print("Written to JSON")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Null Handling

# COMMAND ----------

# a) Find null manager_id
employees.filter(col("manager_id").isNull()).show()

# COMMAND ----------

# b) Fill null with 0
employees.fillna({"manager_id": 0}).show()

# COMMAND ----------

# c) Drop rows with any null
employees.dropna().show()

# COMMAND ----------

# d) Replace null in specific column
employees.fillna({"manager_id": -1, "city": "Unknown"}).show()
