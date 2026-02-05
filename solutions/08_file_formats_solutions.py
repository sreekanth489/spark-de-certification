# Databricks notebook source
# MAGIC %md
# MAGIC # File Formats - Solutions
# MAGIC Topics: CSV, JSON, Parquet, ORC, Delta, schema handling, compression

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.functions import col, input_file_name, explode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: CSV Operations

# COMMAND ----------

# a) Read CSV with custom delimiter (pipe |)
# First create sample pipe-delimited file
pipe_data = [("1", "Alice", "100"), ("2", "Bob", "200")]
pipe_df = spark.createDataFrame(pipe_data, ["id", "name", "value"])
pipe_df.write.mode("overwrite").option("delimiter", "|").csv("/tmp/pipe_delimited")

# Read it back
df = spark.read.option("delimiter", "|").option("header", False).csv("/tmp/pipe_delimited")
display(df)

# COMMAND ----------

# b) Custom null value
df = spark.read.option("nullValue", "NA").option("header", True).csv("datasets/csv/employees.csv")
display(df)

# COMMAND ----------

# e) Skip malformed records
df = spark.read \
    .option("mode", "DROPMALFORMED") \
    .option("header", True) \
    .csv("datasets/csv/employees.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV Read Modes:
# MAGIC - **PERMISSIVE** (default): Sets null for corrupt fields
# MAGIC - **DROPMALFORMED**: Drops corrupt records
# MAGIC - **FAILFAST**: Fails immediately on corrupt records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: JSON Operations

# COMMAND ----------

# a) Single-line JSON (default)
df = spark.read.json("datasets/json/events.json")
display(df)

# COMMAND ----------

# c) Nested structures
orders = spark.read.json("datasets/json/orders.json")
orders.printSchema()

# COMMAND ----------

display(orders)

# COMMAND ----------

# d) Flatten nested JSON
flattened = orders.select(
    "order_id",
    col("shipping.city").alias("ship_city"),
    col("shipping.state").alias("ship_state"),
    explode("items").alias("item")
).select(
    "order_id",
    "ship_city",
    "ship_state",
    col("item.product_id"),
    col("item.quantity")
)
display(flattened)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Parquet Operations

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)

# a) Write as Parquet
employees.write.mode("overwrite").parquet("/tmp/parquet/employees")
print("Written as Parquet!")

# COMMAND ----------

# b) Read with column pruning (automatic - just select columns)
df = spark.read.parquet("/tmp/parquet/employees").select("name", "salary")
display(df)

# COMMAND ----------

# c) Write with partitioning
employees.write.mode("overwrite").partitionBy("department").parquet("/tmp/parquet/employees_partitioned")
print("Written with partitioning!")

# COMMAND ----------

# d) Write with compression
employees.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("/tmp/parquet/employees_snappy")
print("Written with snappy compression!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compression Options:
# MAGIC - **snappy** (default): Good balance of speed and compression
# MAGIC - **gzip**: Better compression, slower
# MAGIC - **lz4**: Fast compression
# MAGIC - **zstd**: Good compression ratio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Schema Evolution

# COMMAND ----------

# a) Read files with different schemas using mergeSchema
df = spark.read.option("mergeSchema", "true").parquet("/tmp/parquet/employees_partitioned")
df.printSchema()

# COMMAND ----------

# b) Handle missing columns with coalesce
from pyspark.sql.functions import coalesce, lit

df = df.withColumn("new_column", coalesce(col("bonus"), lit(0.0)))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Partitioning

# COMMAND ----------

# a) Single column partition
employees.write.mode("overwrite").partitionBy("department").parquet("/tmp/parquet/by_dept")
print("Partitioned by department!")

# COMMAND ----------

# b) Multiple column partition
employees.write.mode("overwrite").partitionBy("department", "city").parquet("/tmp/parquet/by_dept_city")
print("Partitioned by department and city!")

# COMMAND ----------

# c) Read specific partitions (partition pruning)
df = spark.read.parquet("/tmp/parquet/by_dept") \
    .filter(col("department") == "Engineering")

# Verify partition pruning in explain
df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Complex Data Types

# COMMAND ----------

# Read orders with complex types
orders = spark.read.json("datasets/json/orders.json")

# a) Arrays
display(orders.select("order_id", "items"))

# COMMAND ----------

# b) Nested structs
display(orders.select("order_id", "shipping", "shipping.city", "shipping.state"))

# COMMAND ----------

# c) Write complex types to Parquet
orders.write.mode("overwrite").parquet("/tmp/parquet/orders_complex")

# Read back
df = spark.read.parquet("/tmp/parquet/orders_complex")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Compression Comparison

# COMMAND ----------

# a) Snappy (default, good balance)
employees.write.mode("overwrite").option("compression", "snappy").parquet("/tmp/parquet/emp_snappy")

# b) Gzip (better compression, slower)
employees.write.mode("overwrite").option("compression", "gzip").parquet("/tmp/parquet/emp_gzip")

# c) No compression
employees.write.mode("overwrite").option("compression", "none").parquet("/tmp/parquet/emp_none")

print("Written with different compressions!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: ORC Format

# COMMAND ----------

# a) Write as ORC
employees.write.mode("overwrite").orc("/tmp/orc/employees")
print("Written as ORC!")

# COMMAND ----------

# b) Read ORC
df = spark.read.orc("/tmp/orc/employees")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet vs ORC:
# MAGIC - **Parquet**: Better for nested data, wider ecosystem support
# MAGIC - **ORC**: Better Hive integration, native ACID support
# MAGIC - Both support predicate pushdown and columnar storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Text Files

# COMMAND ----------

# a) Plain text files
text_df = spark.read.text("datasets/csv/employees.csv")
display(text_df.limit(5))

# COMMAND ----------

# c) Whole text file as single row
whole_text = spark.read.option("wholetext", True).text("datasets/csv/employees.csv")
display(whole_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Delta Lake

# COMMAND ----------

# a) Write as Delta table
employees.write.format("delta").mode("overwrite").save("/tmp/delta/employees")
print("Written as Delta!")

# COMMAND ----------

# b) Read Delta table
df = spark.read.format("delta").load("/tmp/delta/employees")
display(df)

# COMMAND ----------

# c) Time travel - query by version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/employees")
display(df_v0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: File Metadata

# COMMAND ----------

# a) Get input file name for each record
df = spark.read.csv("datasets/csv/employees.csv", header=True) \
    .withColumn("source_file", input_file_name())
display(df.select("name", "source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Performance Considerations

# COMMAND ----------

# a) Check partition count
df = spark.read.parquet("/tmp/parquet/employees")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# COMMAND ----------

# c) Coalesce vs repartition before write

# Coalesce: reduces partitions, avoids shuffle
employees.coalesce(1).write.mode("overwrite").parquet("/tmp/parquet/emp_single")
print("Coalesced to 1 file!")

# Repartition: can increase/decrease, causes shuffle
employees.repartition(4).write.mode("overwrite").parquet("/tmp/parquet/emp_four")
print("Repartitioned to 4 files!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Error Handling

# COMMAND ----------

# a) Corrupt record handling modes

# PERMISSIVE (default) - nulls for corrupt fields
df_permissive = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .csv("datasets/csv/employees.csv", header=True)

display(df_permissive)

# COMMAND ----------

# DROPMALFORMED - drops corrupt records
df_drop = spark.read \
    .option("mode", "DROPMALFORMED") \
    .csv("datasets/csv/employees.csv", header=True)

display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error Handling Modes Summary:
# MAGIC | Mode | Behavior |
# MAGIC |------|----------|
# MAGIC | PERMISSIVE | Sets null for corrupt fields, optionally stores in _corrupt_record |
# MAGIC | DROPMALFORMED | Silently drops corrupt records |
# MAGIC | FAILFAST | Throws exception immediately |
