"""
Performance Tuning - Solutions
===============================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, spark_partition_id, lit, rand
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
    .appName("Performance Solutions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("../datasets/csv/products.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Understanding Explain Plans
# =============================================================================
# a) Simple explain
employees.filter(col("salary") > 80000).explain()

# b) Extended explain (shows parsed, analyzed, optimized, physical plans)
employees.filter(col("salary") > 80000).explain(extended=True)

# c) Formatted explain (Spark 3.0+)
employees.filter(col("salary") > 80000).explain(mode="formatted")

# d) Cost-based explain
employees.filter(col("salary") > 80000).explain(mode="cost")

# Key things to look for:
# - Scan: FileScan, InMemoryTableScan
# - Exchange: Shuffle operations
# - BroadcastExchange: Broadcast join
# - Sort, SortMergeJoin, BroadcastHashJoin


# =============================================================================
# Problem 2: Caching and Persistence
# =============================================================================
# a) Cache DataFrame (MEMORY_AND_DISK by default)
employees.cache()
employees.count()  # Materialize cache

# b) Persist with different storage levels
employees.persist(StorageLevel.MEMORY_ONLY)
employees.persist(StorageLevel.MEMORY_AND_DISK)
employees.persist(StorageLevel.DISK_ONLY)
employees.persist(StorageLevel.MEMORY_ONLY_SER)  # Serialized
employees.persist(StorageLevel.MEMORY_AND_DISK_SER)

# c) Unpersist
employees.unpersist()

# d) Check if cached
print(f"Is cached: {employees.is_cached}")

# e) When to cache:
# - DataFrame used multiple times in same job
# - After expensive transformations
# - Iterative algorithms

# When NOT to cache:
# - One-time use DataFrames
# - Very large DataFrames that don't fit in memory
# - When storage is the bottleneck


# =============================================================================
# Problem 3: Partitioning Strategies
# =============================================================================
# a) Check partitions
print(f"Current partitions: {employees.rdd.getNumPartitions()}")

# Show partition distribution
employees.withColumn("partition", spark_partition_id()).groupBy("partition").count().show()

# b) Repartition by number
df_repart = employees.repartition(4)
print(f"After repartition(4): {df_repart.rdd.getNumPartitions()}")

# c) Repartition by column (hash partitioning)
df_by_dept = employees.repartition("department")
df_by_dept = employees.repartition(4, "department")  # 4 partitions by department

# d) Coalesce (reduce partitions without shuffle)
df_coalesce = employees.coalesce(2)
print(f"After coalesce(2): {df_coalesce.rdd.getNumPartitions()}")

# e) When to use which:
# - repartition: Need to increase partitions or distribute by key
# - coalesce: Reduce partitions to avoid small files


# =============================================================================
# Problem 4: Broadcast Joins
# =============================================================================
# a) Broadcast hint
sales_with_products = sales.join(
    broadcast(products),
    "product_id"
)

# b) Configure broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB

# Disable auto broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# c) Verify broadcast in explain
sales_with_products.explain()
# Look for: BroadcastHashJoin or BroadcastExchange

# d) When NOT to use broadcast:
# - Large tables (>100MB typically)
# - When small table is actually large after filtering
# - Memory constraints on executors


# =============================================================================
# Problem 5: Shuffle Optimization
# =============================================================================
# a) Identify shuffle operations (Exchange in plan)
sales.groupBy("store_id").count().explain()

# b) Pre-partition to avoid shuffle
# If joining same key multiple times, partition once
sales_partitioned = sales.repartition("product_id")
sales_partitioned.join(products, "product_id").explain()

# c) Configure shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)  # Default
spark.conf.set("spark.sql.shuffle.partitions", 10)   # For small data

# d) Bucketing (see Problem 11)


# =============================================================================
# Problem 6: Predicate Pushdown
# =============================================================================
# Parquet/ORC support predicate pushdown
employees.write.mode("overwrite").parquet("../datasets/parquet/employees")

# Read with filter - predicate pushed to scan
df = spark.read.parquet("../datasets/parquet/employees") \
    .filter(col("salary") > 80000)

# Verify in explain - filter should be at scan level
df.explain()

# Predicates that CAN be pushed:
# - =, >, <, >=, <=, !=
# - IN, IS NULL, IS NOT NULL
# - AND, OR combinations

# Predicates that CANNOT be pushed:
# - UDFs
# - Complex expressions
# - LIKE (depends on pattern)


# =============================================================================
# Problem 7: Column Pruning
# =============================================================================
# a) Select only needed columns
df = spark.read.parquet("../datasets/parquet/employees") \
    .select("name", "salary")  # Only reads these columns

# b) Verify in explain
df.explain()
# Look for: ReadSchema with only selected columns

# c) Impact comparison
# Bad: Reading all columns
df_all = spark.read.parquet("../datasets/parquet/employees")
df_all.select("name").explain()

# Good: Column pruning
df_pruned = spark.read.parquet("../datasets/parquet/employees").select("name")
df_pruned.explain()


# =============================================================================
# Problem 8: Data Skew
# =============================================================================
# a) Identify skew - check partition sizes
employees.withColumn("partition", spark_partition_id()) \
    .groupBy("partition") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# b) Salting technique for skewed joins
from pyspark.sql.functions import concat, floor, rand

# Add salt to skewed key
num_salts = 10
salted_large = employees.withColumn(
    "salted_key",
    concat(col("department"), lit("_"), (rand() * num_salts).cast("int"))
)

# Explode small table with all salts
from pyspark.sql.functions import explode, array, lit

salts = spark.range(num_salts).withColumnRenamed("id", "salt")
small_df = products  # Example small table

# c) AQE skew join optimization (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# d) Repartition to reduce skew
employees.repartition(10, "department").explain()


# =============================================================================
# Problem 9: Adaptive Query Execution (AQE)
# =============================================================================
# a) Enable/disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Enabled by default in Spark 3.0+

# b) Coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")

# c) Convert sort-merge to broadcast
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")

# d) Skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Check AQE in action
sales.join(products, "product_id").explain(mode="formatted")


# =============================================================================
# Problem 10: Memory Management
# =============================================================================
# a) Spark memory model
# - Execution memory: Shuffles, joins, sorts, aggregations
# - Storage memory: Caching, broadcast variables
# - Unified memory management (Spark 1.6+)

# b) Configure executor memory
# spark-submit --executor-memory 4g
# Or: spark.executor.memory = 4g

# c) Handle OOM errors:
# - Increase executor memory
# - Reduce partition size (more partitions)
# - Spill to disk (default behavior)
# - Use memory-efficient operations
# - Avoid collect() on large data

# d) Memory-intensive operations:
# - Broadcast joins with large tables
# - collect_list() on large groups
# - Caching large DataFrames
# - Sorting large datasets


# =============================================================================
# Problem 11: Bucketing
# =============================================================================
# a) Create bucketed table
employees.write \
    .mode("overwrite") \
    .bucketBy(4, "department") \
    .sortBy("salary") \
    .saveAsTable("bucketed_employees")

# b) Join bucketed tables (no shuffle if same bucket columns)
# Both tables must be bucketed on join key with same number of buckets
spark.sql("""
    SELECT * FROM bucketed_employees e1
    JOIN bucketed_employees e2 ON e1.department = e2.department
""").explain()

# c) Query bucketed tables
spark.sql("SELECT * FROM bucketed_employees WHERE department = 'Engineering'").explain()

# d) Bucket pruning
# Spark can skip buckets that don't match filter condition


# =============================================================================
# Problem 12: File-based Optimization
# =============================================================================
# a) Optimal file sizes: 128MB - 1GB
# Check current file sizes and partition count

# b) Compact small files
small_files_df = spark.read.parquet("../datasets/parquet/employees_partitioned")
small_files_df.coalesce(1).write.mode("overwrite").parquet("../datasets/parquet/employees_compacted")

# c) Partition pruning
# Filter on partition column - only reads matching partitions
spark.read.parquet("../datasets/parquet/employees_partitioned") \
    .filter(col("department") == "Engineering") \
    .explain()

# d) Z-ordering (Delta Lake)
# Colocates related data for better data skipping
# deltaTable.optimize().executeZOrderBy("column")


# =============================================================================
# Problem 13: Configuration Tuning
# =============================================================================
# a) Key configurations
configs = {
    # Parallelism
    "spark.sql.shuffle.partitions": 200,  # For shuffle operations
    "spark.default.parallelism": 200,      # For RDD operations

    # Broadcast
    "spark.sql.autoBroadcastJoinThreshold": 10485760,  # 10MB

    # AQE
    "spark.sql.adaptive.enabled": True,
    "spark.sql.adaptive.coalescePartitions.enabled": True,

    # Memory
    "spark.memory.fraction": 0.6,          # Fraction of heap for execution/storage
    "spark.memory.storageFraction": 0.5,   # Fraction of memory.fraction for storage

    # Compression
    "spark.sql.parquet.compression.codec": "snappy",
}

# Apply configurations
for key, value in configs.items():
    spark.conf.set(key, value)

# b-e) Access current config values
print(f"shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"autoBroadcastJoinThreshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
print(f"adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")


# =============================================================================
# Problem 14: Monitoring and Debugging
# =============================================================================
# a) Spark UI (http://localhost:4040)
# - Jobs: Overall job progress
# - Stages: Task-level details
# - Storage: Cached RDDs/DataFrames
# - Executors: Resource usage
# - SQL: Query plans and metrics

# b) Stage timing analysis
# Look for:
# - Long-running stages
# - Uneven task distribution
# - High shuffle read/write

# c) Identify data skew in UI
# - Tasks with much longer duration than others
# - Task with much more input data than others

# d) Memory analysis
# - Storage tab shows cached data
# - Executor tab shows memory usage
# - Look for spill metrics (memory to disk)

# Programmatic metrics
print(spark.sparkContext.uiWebUrl)
