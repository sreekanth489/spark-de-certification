# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake - Solutions
# MAGIC Topics: ACID transactions, time travel, MERGE, optimization, CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, expr, when
from delta.tables import DeltaTable

# COMMAND ----------

# Load sample data
employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Creating Delta Tables

# COMMAND ----------

# a) Create from DataFrame
employees.write.format("delta").mode("overwrite").save("/tmp/delta/employees")
print("Delta table created!")

# COMMAND ----------

# b) Create using SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS delta_employees (
        emp_id INT,
        name STRING,
        department STRING,
        salary DOUBLE,
        hire_date DATE,
        manager_id INT,
        city STRING
    )
    USING DELTA
    LOCATION '/tmp/delta/employees_sql'
""")

# COMMAND ----------

# c) Convert Parquet to Delta
employees.write.format("parquet").mode("overwrite").save("/tmp/parquet/emp_to_convert")

# Convert to Delta
DeltaTable.convertToDelta(spark, "parquet.`/tmp/parquet/emp_to_convert`")
print("Converted Parquet to Delta!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Basic CRUD Operations

# COMMAND ----------

# a) INSERT using DataFrame
new_data = spark.createDataFrame(
    [(101, "New Person", "IT", 75000.0, "2024-01-01", None, "Boston")],
    ["emp_id", "name", "department", "salary", "hire_date", "manager_id", "city"]
)
new_data.write.format("delta").mode("append").save("/tmp/delta/employees")
print("Data inserted!")

# COMMAND ----------

# b) UPDATE using DeltaTable API
delta_table = DeltaTable.forPath(spark, "/tmp/delta/employees")
delta_table.update(
    condition="department = 'IT'",
    set={"salary": "salary * 1.10"}
)
print("Update complete!")

# COMMAND ----------

# c) DELETE
delta_table.delete(condition="emp_id = 101")
print("Delete complete!")

# COMMAND ----------

# d) READ Delta table
df = spark.read.format("delta").load("/tmp/delta/employees")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: MERGE (Upsert) Operations

# COMMAND ----------

# Create source data (updates)
updates = spark.createDataFrame([
    (1, "John Smith Updated", "Engineering", 90000.0, "2020-01-15", 5, "New York"),
    (200, "Brand New Employee", "Marketing", 65000.0, "2024-01-15", 11, "Boston")
], ["emp_id", "name", "department", "salary", "hire_date", "manager_id", "city"])

display(updates)

# COMMAND ----------

# a) Basic MERGE - Insert new, update existing
delta_table = DeltaTable.forPath(spark, "/tmp/delta/employees")

delta_table.alias("target").merge(
    updates.alias("source"),
    "target.emp_id = source.emp_id"
).whenMatchedUpdate(set={
    "name": "source.name",
    "department": "source.department",
    "salary": "source.salary"
}).whenNotMatchedInsertAll().execute()

print("MERGE complete!")

# COMMAND ----------

# Verify MERGE results
display(spark.read.format("delta").load("/tmp/delta/employees"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Time Travel

# COMMAND ----------

# a) Query by version number
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/employees")
display(df_v0)

# COMMAND ----------

# c) View history
history_df = delta_table.history()
display(history_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history using SQL
# MAGIC DESCRIBE HISTORY delta.`/tmp/delta/employees`

# COMMAND ----------

# d) Restore to previous version
delta_table.restoreToVersion(0)
print("Restored to version 0!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Schema Evolution

# COMMAND ----------

# a) Add new columns with mergeSchema
df_with_new_col = employees.withColumn("bonus", lit(1000.0))

df_with_new_col.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tmp/delta/employees")

print("Schema evolved - new column added!")

# COMMAND ----------

# Verify schema evolution
spark.read.format("delta").load("/tmp/delta/employees").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Table Properties and Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set table properties
# MAGIC ALTER TABLE delta.`/tmp/delta/employees`
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table details
# MAGIC DESCRIBE DETAIL delta.`/tmp/delta/employees`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE to compact small files
# MAGIC OPTIMIZE delta.`/tmp/delta/employees`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE with ZORDER
# MAGIC OPTIMIZE delta.`/tmp/delta/employees` ZORDER BY (department, city)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM to remove old files (use with caution!)
# MAGIC VACUUM delta.`/tmp/delta/employees` RETAIN 168 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Partitioning

# COMMAND ----------

# Create partitioned Delta table
employees.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("department") \
    .save("/tmp/delta/employees_partitioned")

print("Partitioned table created!")

# COMMAND ----------

# Partition pruning - filter on partition column
df = spark.read.format("delta").load("/tmp/delta/employees_partitioned") \
    .filter(col("department") == "Engineering")

# Check query plan for partition pruning
df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Change Data Feed (CDC)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Change Data Feed
# MAGIC ALTER TABLE delta.`/tmp/delta/employees` SET TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true'
# MAGIC )

# COMMAND ----------

# Make some changes to generate CDC data
delta_table = DeltaTable.forPath(spark, "/tmp/delta/employees")
delta_table.update(
    condition="department = 'Engineering'",
    set={"salary": "salary * 1.05"}
)

# COMMAND ----------

# Read changes from CDF
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("/tmp/delta/employees")

display(changes)

# COMMAND ----------

# Filter by change type
display(changes.filter(col("_change_type") == "update_postimage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Delta Lake with Streaming

# COMMAND ----------

# Read Delta table as a stream
stream_df = spark.readStream.format("delta").load("/tmp/delta/employees")

# Write to console for demo (in production, write to another Delta table)
# query = stream_df.writeStream.format("console").start()

print("Streaming setup ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 15: Complete Pipeline Example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create fact table with all best practices
# MAGIC CREATE TABLE IF NOT EXISTS sales_facts (
# MAGIC     transaction_id STRING,
# MAGIC     product_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     quantity INT,
# MAGIC     amount DOUBLE,
# MAGIC     transaction_date DATE,
# MAGIC     processed_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (transaction_date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )
# MAGIC LOCATION '/tmp/delta/sales_facts'

# COMMAND ----------

# Incremental load function using MERGE
def incremental_load(source_df):
    DeltaTable.forPath(spark, "/tmp/delta/sales_facts") \
        .alias("target") \
        .merge(
            source_df.withColumn("processed_time", current_timestamp()).alias("source"),
            "target.transaction_id = source.transaction_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

print("Incremental load function defined!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set retention policy
# MAGIC ALTER TABLE sales_facts SET TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 7 days'
# MAGIC )
