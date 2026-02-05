# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Spark Features - Solutions
# MAGIC Topics: AQE, DPP, Pandas API, SQL hints, higher-order functions, error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, expr, array, struct, transform, filter as array_filter,
    aggregate, exists, forall, zip_with, explode, current_timestamp
)

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Adaptive Query Execution (AQE)

# COMMAND ----------

# a) Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# b) Coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

print("Partition coalescing configured!")

# COMMAND ----------

# c) Convert to broadcast join dynamically
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")

print("Auto broadcast threshold set!")

# COMMAND ----------

# d) Skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

print("Skew join optimization enabled!")

# COMMAND ----------

# e) Monitor AQE in explain plan
sales.join(employees, sales.customer_id == employees.emp_id).explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Dynamic Partition Pruning (DPP)

# COMMAND ----------

# Configure DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")

print("DPP configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DPP works when:
# MAGIC - Fact table is partitioned
# MAGIC - Join on partition column
# MAGIC - Dimension filter is selective
# MAGIC - Look for **DynamicPruningExpression** in explain plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Bloom Filter Join Optimization

# COMMAND ----------

# a) Enable bloom filter (Spark 3.3+)
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold", "10MB")

# b) Configure parameters
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.expectedNumItems", "1000000")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.numBits", "8388608")

print("Bloom filter optimization enabled!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Storage Partitioned Join (Bucketing)

# COMMAND ----------

# a) Create bucketed tables
employees.write \
    .bucketBy(8, "department") \
    .sortBy("salary") \
    .mode("overwrite") \
    .saveAsTable("emp_bucketed")

print("Bucketed table created!")

# COMMAND ----------

# b) Both tables must have same bucket count on join key
# c) Join without shuffle
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")

# result = spark.table("emp_bucketed").join(spark.table("other_bucketed"), "department")

print("Bucketing configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Pandas API on Spark

# COMMAND ----------

import pyspark.pandas as ps

# a) Create using pandas API
pdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
pdf["c"] = pdf["a"] + pdf["b"]
display(pdf)

# COMMAND ----------

# b) Convert between APIs
spark_df = employees
pandas_on_spark = spark_df.pandas_api()

# Back to Spark DataFrame
spark_df_again = pandas_on_spark.to_spark()

print("Conversion complete!")

# COMMAND ----------

# c) Pandas functions on distributed data
result = pandas_on_spark.groupby("department")["salary"].mean()
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pandas API Limitations:
# MAGIC - Some operations require shuffle
# MAGIC - Not all pandas functions supported
# MAGIC - Index handling differs from pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: ANSI SQL Mode

# COMMAND ----------

# a) Enable ANSI mode
spark.conf.set("spark.sql.ansi.enabled", "true")
print("ANSI mode enabled!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANSI Mode Behavior Changes:
# MAGIC - Arithmetic overflow raises error (not wrap around)
# MAGIC - Invalid cast raises error (not null)
# MAGIC - Array index out of bounds raises error

# COMMAND ----------

# b) Use try_* functions for safe operations
display(spark.sql("SELECT try_cast('invalid' AS INT) as safe_cast"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: SQL Hints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Broadcast hint
# MAGIC SELECT /*+ BROADCAST(s) */ *
# MAGIC FROM employees e
# MAGIC JOIN sales s ON e.emp_id = s.customer_id
# MAGIC LIMIT 10

# COMMAND ----------

# DataFrame API for hints
from pyspark.sql.functions import broadcast

result = employees.join(sales.hint("broadcast"), employees.emp_id == sales.customer_id)
result.explain()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Coalesce/repartition hints
# MAGIC SELECT /*+ COALESCE(1) */ * FROM employees LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Repartition hint
# MAGIC SELECT /*+ REPARTITION(4) */ * FROM employees LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Available Join Hints:
# MAGIC | Hint | Description |
# MAGIC |------|-------------|
# MAGIC | **BROADCAST** | Broadcast smaller table |
# MAGIC | **MERGE** | Sort-merge join |
# MAGIC | **SHUFFLE_HASH** | Hash join |
# MAGIC | **SHUFFLE_REPLICATE_NL** | Nested loop |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Higher-Order Functions

# COMMAND ----------

# Sample data with arrays
df = spark.createDataFrame([
    (1, [1, 2, 3, 4, 5]),
    (2, [10, 20, 30])
], ["id", "values"])

display(df)

# COMMAND ----------

# a) transform - apply to each element
df_transformed = df.select(
    "id",
    transform("values", lambda x: x * 2).alias("doubled")
)
display(df_transformed)

# COMMAND ----------

# b) filter - keep matching elements
df_filtered = df.select(
    "id",
    array_filter("values", lambda x: x > 2).alias("filtered")
)
display(df_filtered)

# COMMAND ----------

# c) aggregate - reduce to single value
df_agg = df.select(
    "id",
    aggregate("values", lit(0), lambda acc, x: acc + x).alias("sum")
)
display(df_agg)

# COMMAND ----------

# d) exists/forall - predicates
df_pred = df.select(
    "id",
    exists("values", lambda x: x > 10).alias("has_gt_10"),
    forall("values", lambda x: x > 0).alias("all_positive")
)
display(df_pred)

# COMMAND ----------

# e) zip_with - combine arrays
df2 = spark.createDataFrame([
    (1, [10, 20, 30, 40, 50]),
    (2, [1, 2, 3])
], ["id", "multipliers"])

df_zipped = df.join(df2, "id").select(
    "id",
    zip_with("values", "multipliers", lambda x, y: x * y).alias("products")
)
display(df_zipped)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Parameterized Queries (Spark 3.4+)

# COMMAND ----------

employees.createOrReplaceTempView("employees")

# a) Named parameters
display(spark.sql(
    "SELECT * FROM employees WHERE salary > :min_salary",
    args={"min_salary": 80000}
))

# COMMAND ----------

# b) Multiple parameters
display(spark.sql("""
    SELECT * FROM employees
    WHERE department = :dept AND salary > :min_sal
""", args={"dept": "Engineering", "min_sal": 90000}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameterized Query Benefits:
# MAGIC - Prevents SQL injection
# MAGIC - Cleaner query construction
# MAGIC - Supports named and positional parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Error Handling Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) try_* functions (Spark 3.4+)
# MAGIC SELECT
# MAGIC     try_cast('abc' AS INT) as safe_cast,
# MAGIC     try_add(2147483647, 1) as safe_add,
# MAGIC     try_divide(10, 0) as safe_divide

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) COALESCE with try functions
# MAGIC SELECT
# MAGIC     COALESCE(try_cast('abc' AS INT), 0) as parsed_with_default

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: Table-Valued Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Built-in TVFs
# MAGIC SELECT * FROM explode(array(1, 2, 3))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Range function
# MAGIC SELECT * FROM range(10)

# COMMAND ----------

# Read orders for LATERAL VIEW demo
orders = spark.read.json("datasets/json/orders.json")
orders.createOrReplaceTempView("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) LATERAL VIEW with explode
# MAGIC SELECT order_id, item.*
# MAGIC FROM orders
# MAGIC LATERAL VIEW explode(items) AS item

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Timestamp Types (Spark 3.0+)

# COMMAND ----------

# a) Configure timestamp type
# TIMESTAMP_LTZ - Local timezone, stored as UTC
# TIMESTAMP_NTZ - No timezone, stored as-is

spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
print(f"Timestamp type: {spark.conf.get('spark.sql.timestampType')}")

# COMMAND ----------

# b) Timezone-aware operations
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, to_utc_timestamp

spark.conf.set("spark.sql.session.timeZone", "America/New_York")

df = employees.withColumn("ts", to_timestamp(col("hire_date")))
df = df.withColumn("ts_utc", to_utc_timestamp(col("ts"), "America/New_York"))
display(df.select("name", "hire_date", "ts", "ts_utc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Streaming Enhancements

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Improvements (Spark 3.2+):
# MAGIC - **Multiple stateful operators**: Chain multiple aggregations
# MAGIC - **Session windows**: `session_window("timestamp", "5 minutes")`
# MAGIC - **Async progress tracking**: Non-blocking `query.recentProgress`
# MAGIC - **AvailableNow trigger**: Process all available data, then stop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 14: Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Features:
# MAGIC - **Three-level namespace**: `catalog.schema.table`
# MAGIC - **Fine-grained access control**: Row/column level security
# MAGIC - **Audit logging**: Track all data access
# MAGIC - **Data lineage**: Automatic tracking of data flow

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example Unity Catalog query
# MAGIC -- SELECT * FROM main.default.employees
# MAGIC
# MAGIC -- Grant access
# MAGIC -- GRANT SELECT ON TABLE catalog.schema.table TO user@domain.com

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 15: Performance Tuning Summary

# COMMAND ----------

# Key configurations summary
configs = {
    "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
    "spark.sql.adaptive.coalescePartitions.enabled": spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"),
    "spark.sql.adaptive.skewJoin.enabled": spark.conf.get("spark.sql.adaptive.skewJoin.enabled"),
    "spark.sql.ansi.enabled": spark.conf.get("spark.sql.ansi.enabled"),
}

for key, value in configs.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Features Summary
# MAGIC
# MAGIC | Feature | Spark Version | Purpose |
# MAGIC |---------|---------------|---------|
# MAGIC | **AQE** | 3.0+ | Runtime query optimization |
# MAGIC | **DPP** | 3.0+ | Partition pruning at runtime |
# MAGIC | **Bloom Filters** | 3.3+ | Join optimization |
# MAGIC | **Pandas API** | 3.2+ | Pandas-like operations |
# MAGIC | **Parameterized Queries** | 3.4+ | Safe SQL injection |
# MAGIC | **try_* Functions** | 3.4+ | Safe error handling |
# MAGIC | **Session Windows** | 3.2+ | Streaming sessions |
# MAGIC | **Unity Catalog** | Databricks | Unified governance |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Always enable AQE** for production workloads
# MAGIC 2. **Use broadcast joins** for small dimension tables
# MAGIC 3. **Leverage bucketing** for repeated joins on same keys
# MAGIC 4. **Use parameterized queries** to prevent SQL injection
# MAGIC 5. **Enable ANSI mode** for stricter type checking
# MAGIC 6. **Use higher-order functions** instead of UDFs for array operations
