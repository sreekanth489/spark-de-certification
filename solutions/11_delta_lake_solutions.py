"""
Delta Lake - Solutions
=======================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable

# Initialize Spark with Delta
spark = SparkSession.builder \
    .appName("Delta Lake Solutions") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Sample data
employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Creating Delta Tables
# =============================================================================
# a) Create from DataFrame
employees.write.format("delta").mode("overwrite").save("../datasets/delta/employees")

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
    LOCATION '../datasets/delta/employees_sql'
""")

# c) Convert Parquet to Delta
# Write parquet first
employees.write.format("parquet").mode("overwrite").save("../datasets/parquet/emp_to_convert")

# Convert
from delta.tables import DeltaTable
DeltaTable.convertToDelta(spark, "parquet.`../datasets/parquet/emp_to_convert`")

# d) Managed vs External
# Managed (in warehouse)
spark.sql("CREATE TABLE managed_emp USING DELTA AS SELECT * FROM delta_employees")

# External (user-specified location)
spark.sql("""
    CREATE TABLE external_emp
    USING DELTA
    LOCATION '../datasets/delta/external_emp'
    AS SELECT * FROM delta_employees
""")


# =============================================================================
# Problem 2: Basic CRUD Operations
# =============================================================================
# a) INSERT
spark.sql("INSERT INTO delta_employees VALUES (100, 'New Person', 'IT', 70000, '2024-01-01', NULL, 'Boston')")

# Or using DataFrame
new_data = spark.createDataFrame([(101, "Another Person", "IT", 75000)], ["emp_id", "name", "department", "salary"])
new_data.write.format("delta").mode("append").save("../datasets/delta/employees")

# b) UPDATE
delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees")
delta_table.update(
    condition="department = 'IT'",
    set={"salary": "salary * 1.10"}
)

# SQL version
spark.sql("UPDATE delta_employees SET salary = salary * 1.10 WHERE department = 'IT'")

# c) DELETE
delta_table.delete(condition="emp_id = 100")

# SQL version
spark.sql("DELETE FROM delta_employees WHERE emp_id = 100")

# d) READ
df = spark.read.format("delta").load("../datasets/delta/employees")
df.show()


# =============================================================================
# Problem 3: MERGE (Upsert) Operations
# =============================================================================
# Create source data (updates)
updates = spark.createDataFrame([
    (1, "John Smith Updated", "Engineering", 90000, "2020-01-15", 5, "New York"),
    (200, "Brand New Employee", "Marketing", 65000, "2024-01-15", 11, "Boston")
], ["emp_id", "name", "department", "salary", "hire_date", "manager_id", "city"])

# a) Basic MERGE
delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees")

delta_table.alias("target").merge(
    updates.alias("source"),
    "target.emp_id = source.emp_id"
).whenMatchedUpdate(set={
    "name": "source.name",
    "department": "source.department",
    "salary": "source.salary"
}).whenNotMatchedInsert(values={
    "emp_id": "source.emp_id",
    "name": "source.name",
    "department": "source.department",
    "salary": "source.salary",
    "hire_date": "source.hire_date",
    "manager_id": "source.manager_id",
    "city": "source.city"
}).execute()

# b) MERGE with delete
delta_table.alias("target").merge(
    updates.alias("source"),
    "target.emp_id = source.emp_id"
).whenMatchedDelete(
    condition="source.salary = 0"
).whenMatchedUpdate(set={
    "salary": "source.salary"
}).whenNotMatchedInsert(values={
    "emp_id": "source.emp_id",
    "name": "source.name",
    "salary": "source.salary"
}).execute()

# c) SQL MERGE
spark.sql("""
    MERGE INTO delta_employees AS target
    USING updates_view AS source
    ON target.emp_id = source.emp_id
    WHEN MATCHED AND source.is_delete = true THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# d) MERGE with source filter
delta_table.alias("target").merge(
    updates.filter(col("salary") > 0).alias("source"),
    "target.emp_id = source.emp_id"
).whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

# e) SCD Type 2
# Close existing record and insert new
spark.sql("""
    MERGE INTO employees_scd2 AS target
    USING (
        SELECT *, current_timestamp() as effective_date
        FROM updates
    ) AS source
    ON target.emp_id = source.emp_id AND target.is_current = true
    WHEN MATCHED AND target.salary != source.salary THEN
        UPDATE SET is_current = false, end_date = source.effective_date
    WHEN NOT MATCHED THEN
        INSERT (emp_id, name, salary, is_current, start_date)
        VALUES (source.emp_id, source.name, source.salary, true, source.effective_date)
""")


# =============================================================================
# Problem 4: Time Travel
# =============================================================================
# a) Query by version number
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("../datasets/delta/employees")
df_v0.show()

# b) Query by timestamp
df_ts = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 10:00:00") \
    .load("../datasets/delta/employees")

# c) View history
spark.sql("DESCRIBE HISTORY delta.`../datasets/delta/employees`").show(truncate=False)

# Or using DeltaTable
delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees")
delta_table.history().show()
delta_table.history(10).show()  # Last 10 versions

# d) Restore to previous version
delta_table.restoreToVersion(0)
# Or by timestamp
# delta_table.restoreToTimestamp("2024-01-15 10:00:00")

# SQL version
spark.sql("RESTORE delta.`../datasets/delta/employees` TO VERSION AS OF 0")

# e) Compare versions
v0 = spark.read.format("delta").option("versionAsOf", 0).load("../datasets/delta/employees")
v1 = spark.read.format("delta").option("versionAsOf", 1).load("../datasets/delta/employees")

# Find new records
v1.exceptAll(v0).show()


# =============================================================================
# Problem 5: Schema Evolution
# =============================================================================
# a) Add new columns (mergeSchema)
df_with_new_col = employees.withColumn("bonus", lit(1000.0))
df_with_new_col.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("../datasets/delta/employees")

# b) Schema enforcement (default - will fail if schema differs)
# df.write.format("delta").mode("append").save(path)  # Fails if schema mismatch

# Schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# c) Change column types (using overwriteSchema)
# Only for compatible changes
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("../datasets/delta/employees")

# d) Rename columns
spark.sql("ALTER TABLE delta_employees RENAME COLUMN name TO employee_name")

# e) Drop columns (Delta 2.0+)
spark.sql("ALTER TABLE delta_employees DROP COLUMN bonus")


# =============================================================================
# Problem 6: Table Properties and Constraints
# =============================================================================
# a) Set properties
spark.sql("""
    ALTER TABLE delta_employees
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'description' = 'Employee master data'
    )
""")

# b) CHECK constraint
spark.sql("""
    ALTER TABLE delta_employees
    ADD CONSTRAINT salary_positive CHECK (salary > 0)
""")

# c) NOT NULL constraint
spark.sql("""
    ALTER TABLE delta_employees
    ALTER COLUMN emp_id SET NOT NULL
""")

# d) View details
spark.sql("DESCRIBE DETAIL delta.`../datasets/delta/employees`").show(truncate=False)
spark.sql("DESCRIBE EXTENDED delta_employees").show(truncate=False)


# =============================================================================
# Problem 7: Optimization
# =============================================================================
# a) OPTIMIZE (compact files)
spark.sql("OPTIMIZE delta.`../datasets/delta/employees`")

# b) OPTIMIZE with ZORDER
spark.sql("OPTIMIZE delta.`../datasets/delta/employees` ZORDER BY (department, city)")

# Using DeltaTable API
delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees")
delta_table.optimize().executeCompaction()
delta_table.optimize().executeZOrderBy("department")

# c) Auto-compaction settings
spark.sql("""
    ALTER TABLE delta_employees SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# d) VACUUM (remove old files)
spark.sql("VACUUM delta.`../datasets/delta/employees` RETAIN 168 HOURS")

# Or with DeltaTable
delta_table.vacuum(168)  # 168 hours = 7 days

# e) Set retention
spark.sql("""
    ALTER TABLE delta_employees SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")


# =============================================================================
# Problem 8: Partitioning
# =============================================================================
# a) Create partitioned table
employees.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("department") \
    .save("../datasets/delta/employees_partitioned")

# b) Partition pruning
df = spark.read.format("delta").load("../datasets/delta/employees_partitioned")
df.filter(col("department") == "Engineering").explain()  # Should show partition filter

# c) Dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
updates.write.format("delta") \
    .mode("overwrite") \
    .save("../datasets/delta/employees_partitioned")

# d) Generate manifests for other engines
delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees_partitioned")
delta_table.generate("symlink_format_manifest")


# =============================================================================
# Problem 9: Change Data Feed (CDC)
# =============================================================================
# a) Enable CDF on existing table
spark.sql("""
    ALTER TABLE delta_employees SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# Or when creating table
spark.sql("""
    CREATE TABLE cdf_enabled_table (id INT, value STRING)
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# b) Read changes between versions
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 5) \
    .load("../datasets/delta/employees")

changes.show()  # Includes _change_type, _commit_version, _commit_timestamp

# c) Read changes between timestamps
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-01") \
    .option("endingTimestamp", "2024-01-15") \
    .load("../datasets/delta/employees")

# d) Process CDC
changes.filter(col("_change_type") == "insert").show()  # New records
changes.filter(col("_change_type") == "update_postimage").show()  # Updated records
changes.filter(col("_change_type") == "delete").show()  # Deleted records

# e) Streaming with CDF
cdf_stream = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("../datasets/delta/employees")


# =============================================================================
# Problem 10: Delta Lake with Streaming
# =============================================================================
# a) Write streaming to Delta
# stream.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "checkpoints/delta_stream") \
#     .start("../datasets/delta/streaming_table")

# b) Read Delta as stream
stream_df = spark.readStream.format("delta").load("../datasets/delta/employees")

# c) Delta as source and sink
# spark.readStream.format("delta").load("source_path") \
#     .writeStream.format("delta") \
#     .option("checkpointLocation", "checkpoint") \
#     .start("sink_path")

# d) Handle late data with MERGE
# Use foreachBatch for complex upsert logic
def upsert_to_delta(batch_df, batch_id):
    delta_table = DeltaTable.forPath(spark, "path")
    delta_table.alias("t").merge(
        batch_df.alias("s"),
        "t.id = s.id"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# stream.writeStream.foreachBatch(upsert_to_delta).start()


# =============================================================================
# Problem 11: Liquid Clustering (Delta 3.0+)
# =============================================================================
# a) Create with liquid clustering
spark.sql("""
    CREATE TABLE liquid_clustered (
        id INT,
        date DATE,
        category STRING,
        value DOUBLE
    )
    USING DELTA
    CLUSTER BY (date, category)
""")

# b) Clustering vs others:
# - Partitioning: Physical separation, good for low-cardinality
# - Z-ordering: Within partitions, must run OPTIMIZE
# - Liquid clustering: Automatic, incremental, high-cardinality friendly

# c) Alter clustering columns
spark.sql("ALTER TABLE liquid_clustered CLUSTER BY (date)")


# =============================================================================
# Problem 12: Deletion Vectors (Delta 2.4+)
# =============================================================================
# a) Enable deletion vectors
spark.sql("""
    ALTER TABLE delta_employees SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true'
    )
""")

# b) Deletion vectors impact:
# - DELETE/UPDATE don't rewrite files immediately
# - Mark rows as deleted in separate file
# - Faster DELETE/UPDATE operations

# c) Purge deletion vectors
spark.sql("OPTIMIZE delta_employees")  # Rewrites and removes DVs


# =============================================================================
# Problem 13: UniForm
# =============================================================================
# a) Enable UniForm
spark.sql("""
    ALTER TABLE delta_employees SET TBLPROPERTIES (
        'delta.universalFormat.enabledFormats' = 'iceberg'
    )
""")

# Or at creation
spark.sql("""
    CREATE TABLE uniform_table (id INT, value STRING)
    USING DELTA
    TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
""")

# b) Query from Iceberg
# Other tools can read the table using Iceberg metadata


# =============================================================================
# Problem 14: Performance Tuning
# =============================================================================
# a) Analyze statistics
spark.sql("ANALYZE TABLE delta_employees COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE delta_employees COMPUTE STATISTICS FOR ALL COLUMNS")

# b) Configure file sizes
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 134217728)  # 128MB

# c) Handle small files
spark.sql("OPTIMIZE delta.`../datasets/delta/employees`")

# d) Join optimization
# Ensure both tables are optimized with ZORDER on join keys
spark.sql("OPTIMIZE table1 ZORDER BY (join_key)")
spark.sql("OPTIMIZE table2 ZORDER BY (join_key)")


# =============================================================================
# Problem 15: Complete Pipeline
# =============================================================================
# 1. Create table with partitioning
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_facts (
        transaction_id STRING,
        product_id STRING,
        customer_id STRING,
        quantity INT,
        amount DOUBLE,
        transaction_date DATE,
        processed_time TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (transaction_date)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    LOCATION '../datasets/delta/sales_facts'
""")

# 2. Incremental load with MERGE
def incremental_load(batch_df, batch_id):
    DeltaTable.forPath(spark, "../datasets/delta/sales_facts") \
        .alias("target") \
        .merge(
            batch_df.withColumn("processed_time", current_timestamp()).alias("source"),
            "target.transaction_id = source.transaction_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# 3. Read CDC for downstream
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("../datasets/delta/sales_facts")

# 4. Optimization schedule (run periodically)
spark.sql("OPTIMIZE sales_facts ZORDER BY (product_id, customer_id)")

# 5. Retention policy
spark.sql("""
    ALTER TABLE sales_facts SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
spark.sql("VACUUM sales_facts RETAIN 168 HOURS")
