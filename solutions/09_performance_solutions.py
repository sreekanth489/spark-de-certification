# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Tuning - Solutions
# MAGIC Topics: Caching, partitioning, shuffle, broadcast, explain plans, optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, broadcast, spark_partition_id, lit, rand
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("datasets/csv/products.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Understanding Explain Plans

# COMMAND ----------

# a) Simple explain
employees.filter(col("salary") > 80000).explain()

# COMMAND ----------

# b) Extended explain (shows parsed, analyzed, optimized, physical plans)
employees.filter(col("salary") > 80000).explain(extended=True)

# COMMAND ----------

# c) Formatted explain (Spark 3.0+)
employees.filter(col("salary") > 80000).explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Caching and Persistence

# COMMAND ----------

# a) Cache DataFrame (MEMORY_AND_DISK by default)
employees.cache()
employees.count()  # Materialize cache
print("DataFrame cached!")

# COMMAND ----------

# b) Persist with different storage levels
employees.unpersist()

# MEMORY_ONLY
employees.persist(StorageLevel.MEMORY_ONLY)
print(f"Storage level: MEMORY_ONLY")

# COMMAND ----------

# d) Check if cached
print(f"Is cached: {employees.is_cached}")

# COMMAND ----------

# c) Unpersist
employees.unpersist()
print(f"After unpersist - Is cached: {employees.is_cached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Partitioning Strategies

# COMMAND ----------

# a) Check partitions
print(f"Current partitions: {employees.rdd.getNumPartitions()}")

# COMMAND ----------

# Show partition distribution
display(employees.withColumn("partition", spark_partition_id()).groupBy("partition").count())

# COMMAND ----------

# b) Repartition by number
df_repart = employees.repartition(4)
print(f"After repartition(4): {df_repart.rdd.getNumPartitions()}")

# COMMAND ----------

# c) Repartition by column (hash partitioning)
df_by_dept = employees.repartition(4, "department")
print(f"Partitioned by department: {df_by_dept.rdd.getNumPartitions()}")

# COMMAND ----------

# d) Coalesce (reduce partitions without shuffle)
df_coalesce = employees.coalesce(2)
print(f"After coalesce(2): {df_coalesce.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Broadcast Joins

# COMMAND ----------

# a) Broadcast hint
sales_with_products = sales.join(
    broadcast(products),
    "product_id"
)
display(sales_with_products.select("transaction_id", "product_name", "quantity"))

# COMMAND ----------

# c) Verify broadcast in explain plan
sales_with_products.explain()

# COMMAND ----------

# b) Configure broadcast threshold
print(f"Current threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

# Set to 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Shuffle Optimization

# COMMAND ----------

# a) Identify shuffle operations (Exchange in plan)
sales.groupBy("store_id").count().explain()

# COMMAND ----------

# c) Configure shuffle partitions
print(f"Current shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Set for small data
spark.conf.set("spark.sql.shuffle.partitions", 10)
print(f"New shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Predicate Pushdown

# COMMAND ----------

# Write Parquet for pushdown demo
employees.write.mode("overwrite").parquet("/tmp/employees_parquet")

# COMMAND ----------

# Read with filter - predicate pushed to scan
df = spark.read.parquet("/tmp/employees_parquet") \
    .filter(col("salary") > 80000)

# Verify in explain - filter should be at scan level
df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Column Pruning

# COMMAND ----------

# Select only needed columns
df = spark.read.parquet("/tmp/employees_parquet") \
    .select("name", "salary")

# Verify column pruning in explain
df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Data Skew

# COMMAND ----------

# a) Identify skew - check partition sizes
display(employees.withColumn("partition", spark_partition_id()) \
    .groupBy("partition") \
    .count() \
    .orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Adaptive Query Execution (AQE)

# COMMAND ----------

# a) Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# b) Coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")

# COMMAND ----------

# d) Skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# COMMAND ----------

# Check AQE in action
sales.join(products, "product_id").explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: Bucketing

# COMMAND ----------

# a) Create bucketed table
employees.write \
    .mode("overwrite") \
    .bucketBy(4, "department") \
    .sortBy("salary") \
    .saveAsTable("bucketed_employees")

print("Bucketed table created!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query bucketed table
# MAGIC SELECT * FROM bucketed_employees WHERE department = 'Engineering'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: File-based Optimization

# COMMAND ----------

# a) Check file sizes and partition count
df = spark.read.parquet("/tmp/employees_parquet")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# COMMAND ----------

# b) Compact small files
df.coalesce(1).write.mode("overwrite").parquet("/tmp/employees_compacted")
print("Files compacted!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Configuration Tuning

# COMMAND ----------

# Key configurations
configs = {
    "spark.sql.shuffle.partitions": spark.conf.get("spark.sql.shuffle.partitions"),
    "spark.sql.autoBroadcastJoinThreshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
    "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
}

for key, value in configs.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 14: Monitoring and Debugging

# COMMAND ----------

# Get Spark UI URL
print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key things to look for in Spark UI:
# MAGIC - **Jobs tab**: Overall job progress
# MAGIC - **Stages tab**: Task-level details, identify slow stages
# MAGIC - **Storage tab**: Cached RDDs/DataFrames
# MAGIC - **Executors tab**: Resource usage per executor
# MAGIC - **SQL tab**: Query plans and metrics
