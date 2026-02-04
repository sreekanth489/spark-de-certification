"""
Advanced Spark Features - Solutions
====================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, array, struct, transform, filter as array_filter,
    aggregate, exists, forall, zip_with, explode, current_timestamp
)

spark = SparkSession.builder \
    .appName("Advanced Spark Solutions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "true") \
    .getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Spark Connect (Spark 3.4+)
# =============================================================================
# a) Connect to remote cluster
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.remote("sc://host:port").getOrCreate()

# b) Execute queries - same API as local
# df = spark.sql("SELECT * FROM table")
# df = spark.read.parquet("path")

# c) Architecture: Client sends plans, server executes
# Benefits: Stability, resource isolation, multi-language support

# d) Connection handling
# try:
#     spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()
# except Exception as e:
#     print(f"Connection failed: {e}")
#     # Retry logic or fallback


# =============================================================================
# Problem 2: Adaptive Query Execution (AQE)
# =============================================================================
# a) Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# b) Coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# c) Convert to broadcast join
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")

# d) Skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# e) Monitor in explain plan
sales.join(employees, sales.store_id == employees.emp_id).explain(mode="formatted")


# =============================================================================
# Problem 3: Dynamic Partition Pruning (DPP)
# =============================================================================
# a) DPP pushes filter from dimension to fact table at runtime
# Example: Filter on dimension pushes to fact partition scan

# b) Configure DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")

# c) Verify in plan - look for DynamicPruningExpression
# spark.sql("""
#     SELECT * FROM fact_table f
#     JOIN dim_table d ON f.dim_id = d.id
#     WHERE d.category = 'Electronics'
# """).explain()

# d) DPP works when:
# - Fact table is partitioned
# - Join on partition column
# - Dimension filter is selective


# =============================================================================
# Problem 4: Bloom Filter Join Optimization
# =============================================================================
# a) Enable bloom filter (Spark 3.3+)
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold", "10MB")

# b) Configure parameters
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.expectedNumItems", "1000000")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.numBits", "8388608")

# c) Helps filter rows before shuffle/join

# d) Check plan for BloomFilterMightContain


# =============================================================================
# Problem 5: Storage Partitioned Join
# =============================================================================
# a) Bucketed tables can join without shuffle
employees.write \
    .bucketBy(8, "department") \
    .sortBy("salary") \
    .saveAsTable("emp_bucketed")

sales.write \
    .bucketBy(8, "store_id") \
    .sortBy("transaction_date") \
    .saveAsTable("sales_bucketed")

# b) Both tables must have same bucket count on join key

# c) Join without shuffle
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")

# result = spark.table("emp_bucketed").join(spark.table("other_bucketed"), "department")


# =============================================================================
# Problem 6: Pandas API on Spark
# =============================================================================
import pyspark.pandas as ps

# a) Use pandas API
pdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
pdf["c"] = pdf["a"] + pdf["b"]

# b) Convert between APIs
spark_df = employees
pandas_on_spark = spark_df.pandas_api()

# Back to Spark DataFrame
spark_df_again = pandas_on_spark.to_spark()

# c) Pandas functions on distributed data
result = pandas_on_spark.groupby("department")["salary"].mean()

# d) Limitations:
# - Some operations require shuffle
# - Not all pandas functions supported
# - Index handling differs


# =============================================================================
# Problem 7: Pandas-on-Spark Operations
# =============================================================================
import pyspark.pandas as ps

# a) DataFrame operations
psdf = ps.read_csv("../datasets/csv/employees.csv")
psdf = psdf[psdf["salary"] > 80000]

# b) GroupBy
grouped = psdf.groupby("department").agg({"salary": ["mean", "sum", "count"]})

# c) Window functions
psdf["salary_rank"] = psdf.groupby("department")["salary"].rank(method="dense", ascending=False)

# d) I/O
psdf.to_parquet("output.parquet")
psdf.to_csv("output.csv")


# =============================================================================
# Problem 8: Project Zen Improvements
# =============================================================================
# a) Type hints
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

def process_data(df: DataFrame) -> DataFrame:
    return df.filter(col("salary") > 80000)

# b) Better error messages (Spark 3.4+)
# Clearer stack traces, Python-native errors

# c) New APIs
# - DataFrame.transform() for chaining
# - Improved pandas UDF syntax
# - Better null handling

# d) Python-native
# - Better integration with type checkers
# - IDE support improvements


# =============================================================================
# Problem 9: ANSI SQL Mode
# =============================================================================
# a) Enable ANSI mode
spark.conf.set("spark.sql.ansi.enabled", "true")

# b) Behavior changes:
# - Arithmetic overflow raises error (not wrap around)
# - Invalid cast raises error (not null)
# - Array index out of bounds raises error

# c) Handle errors
try:
    spark.sql("SELECT CAST('invalid' AS INT)").show()
except Exception as e:
    print(f"ANSI error: {e}")

# d) Use try_* functions for safe operations
spark.sql("SELECT try_cast('invalid' AS INT)").show()  # Returns NULL


# =============================================================================
# Problem 10: Timestamp Types (Spark 3.0+)
# =============================================================================
# a) New timestamp types
# TIMESTAMP_LTZ - Local timezone, stored as UTC
# TIMESTAMP_NTZ - No timezone, stored as-is

spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")

# b) Timezone-aware
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# c) Function changes
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, to_utc_timestamp

df = employees.withColumn("ts", to_timestamp(col("hire_date")))
df = df.withColumn("ts_utc", to_utc_timestamp(col("ts"), "America/New_York"))

# d) Migration: Review timestamp handling in existing code


# =============================================================================
# Problem 11: SQL Hints
# =============================================================================
# a) Join hints
# BROADCAST - broadcast smaller table
# MERGE - sort-merge join
# SHUFFLE_HASH - hash join
# SHUFFLE_REPLICATE_NL - nested loop

spark.sql("""
    SELECT /*+ BROADCAST(s) */ *
    FROM employees e
    JOIN sales s ON e.emp_id = s.customer_id
""")

# DataFrame API
employees.join(sales.hint("broadcast"), employees.emp_id == sales.customer_id)

# b) Coalesce/repartition hints
spark.sql("SELECT /*+ COALESCE(1) */ * FROM employees")
spark.sql("SELECT /*+ REPARTITION(4) */ * FROM employees")
spark.sql("SELECT /*+ REPARTITION_BY_RANGE(department) */ * FROM employees")

# c) Skew hint
spark.sql("""
    SELECT /*+ SKEW('employees', 'department', ('Engineering')) */ *
    FROM employees
""")


# =============================================================================
# Problem 12: Table-Valued Functions
# =============================================================================
# a) Built-in TVFs
spark.sql("SELECT * FROM explode(array(1, 2, 3))")
spark.sql("SELECT * FROM range(10)")

# b) Explode as TVF
orders = spark.read.json("../datasets/json/orders.json")
orders.createOrReplaceTempView("orders")

spark.sql("""
    SELECT order_id, item.*
    FROM orders
    LATERAL VIEW explode(items) AS item
""").show()

# c) Custom TVF (using PySpark)
# Register Python function as TVF
@spark.udf.register("generate_dates", returnType="array<date>")
def generate_dates(start, end):
    from datetime import timedelta
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


# =============================================================================
# Problem 13: Higher-Order Functions
# =============================================================================
# Sample data with arrays
df = spark.createDataFrame([
    (1, [1, 2, 3, 4, 5]),
    (2, [10, 20, 30])
], ["id", "values"])

# a) transform - apply to each element
df.select(
    "id",
    transform("values", lambda x: x * 2).alias("doubled")
).show()

# b) filter - keep matching elements
df.select(
    "id",
    array_filter("values", lambda x: x > 2).alias("filtered")
).show()

# c) aggregate - reduce to single value
df.select(
    "id",
    aggregate("values", lit(0), lambda acc, x: acc + x).alias("sum")
).show()

# d) exists/forall - predicates
df.select(
    "id",
    exists("values", lambda x: x > 10).alias("has_gt_10"),
    forall("values", lambda x: x > 0).alias("all_positive")
).show()

# e) zip_with - combine arrays
df2 = spark.createDataFrame([
    (1, [10, 20, 30, 40, 50]),
    (2, [1, 2, 3])
], ["id", "multipliers"])

df.join(df2, "id").select(
    "id",
    zip_with("values", "multipliers", lambda x, y: x * y).alias("products")
).show()


# =============================================================================
# Problem 14: Parameterized Queries
# =============================================================================
# a) SQL parameters (Spark 3.4+)
spark.sql(
    "SELECT * FROM employees WHERE salary > :min_salary",
    args={"min_salary": 80000}
).show()

# b) Named parameters
spark.sql("""
    SELECT * FROM employees
    WHERE department = :dept AND salary > :min_sal
""", args={"dept": "Engineering", "min_sal": 90000}).show()

# c) Positional parameters
spark.sql(
    "SELECT * FROM employees WHERE department = ?",
    args=["Engineering"]
).show()

# d) Security - parameters prevent SQL injection


# =============================================================================
# Problem 15: Error Handling
# =============================================================================
# a) try_* functions (Spark 3.4+)
spark.sql("""
    SELECT
        try_cast('abc' AS INT) as safe_cast,
        try_add(2147483647, 1) as safe_add,
        try_divide(10, 0) as safe_divide
""").show()

# b) Raise exceptions
spark.sql("""
    SELECT
        CASE
            WHEN salary < 0 THEN raise_error('Salary cannot be negative')
            ELSE salary
        END
    FROM employees
""")

# c) ASSERT (Spark SQL)
# spark.sql("SELECT assert_true(1 > 0, 'Math is broken')")

# d) Error handling in expressions
spark.sql("""
    SELECT
        COALESCE(try_cast(value AS INT), 0) as parsed_value
    FROM source
""")


# =============================================================================
# Problem 16: Streaming Enhancements
# =============================================================================
# a) Multiple stateful operators (Spark 3.2+)
# Can chain multiple aggregations in single query

# b) Session windows (Spark 3.2+)
# from pyspark.sql.functions import session_window
# df.groupBy(session_window("timestamp", "5 minutes")).count()

# c) Async progress tracking
# query.recentProgress  # Non-blocking

# d) AvailableNow trigger (Spark 3.3+)
# .trigger(availableNow=True)  # Process all available, then stop


# =============================================================================
# Problem 17: Delta Sharing
# =============================================================================
# a) Delta Sharing - open protocol for secure data sharing
# - Share data without copying
# - Works with Delta Lake tables

# b) Create share (Delta Sharing server)
# CREATE SHARE my_share;
# ALTER SHARE my_share ADD TABLE my_schema.my_table;

# c) Access shared data
# spark.read.format("deltaSharing").load("profile.json#share.schema.table")

# d) Security: Uses bearer tokens, no direct data access


# =============================================================================
# Problem 18: Unity Catalog
# =============================================================================
# a) Three-level namespace: catalog.schema.table
# spark.sql("SELECT * FROM main.default.employees")

# b) Data governance
# - Fine-grained access control
# - Audit logging
# - Data discovery

# c) Access control
# GRANT SELECT ON TABLE catalog.schema.table TO user@domain.com

# d) Lineage tracking
# Automatic tracking of data flow through queries


# =============================================================================
# Problem 19: Photon Engine
# =============================================================================
# a) Photon: Native vectorized execution engine for Databricks
# - Written in C++
# - Columnar processing
# - SIMD optimizations

# b) Enable Photon (Databricks)
# Cluster configuration: Runtime with Photon enabled

# c) Compatible operations:
# - Scans, filters, projections
# - Aggregations
# - Joins
# - Some string operations

# d) Performance: 2-8x faster for supported operations


# =============================================================================
# Problem 20: Predictive Optimization
# =============================================================================
# a) Automatic optimization (Databricks)
# - Auto OPTIMIZE
# - Auto VACUUM
# - Auto ANALYZE

# b) Statistics collection
spark.sql("ANALYZE TABLE employees COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE employees COMPUTE STATISTICS FOR ALL COLUMNS")

# c) Optimize write
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# d) Auto-tuning
# Databricks automatically adjusts:
# - File sizes
# - Partition counts
# - Join strategies
