# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Iceberg - Solutions
# MAGIC Topics: Tables, CRUD, hidden partitioning, time travel, schema evolution, maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, expr, year, month, dayofmonth

# Note: Iceberg requires specific configurations
# spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# spark.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
# spark.conf.set("spark.sql.catalog.local.type", "hadoop")
# spark.conf.set("spark.sql.catalog.local.warehouse", "datasets/iceberg-warehouse")

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Creating Iceberg Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Create using SQL
# MAGIC CREATE TABLE IF NOT EXISTS local.db.employees (
# MAGIC     emp_id INT,
# MAGIC     name STRING,
# MAGIC     department STRING,
# MAGIC     salary DOUBLE,
# MAGIC     hire_date DATE,
# MAGIC     manager_id INT,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING iceberg

# COMMAND ----------

# b) Create from DataFrame
# employees.writeTo("local.db.employees_df").using("iceberg").createOrReplace()

print("Iceberg table creation methods explained!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Specific file format (ORC)
# MAGIC CREATE TABLE local.db.employees_orc (
# MAGIC     emp_id INT,
# MAGIC     name STRING,
# MAGIC     salary DOUBLE
# MAGIC )
# MAGIC USING iceberg
# MAGIC TBLPROPERTIES (
# MAGIC     'write.format.default' = 'orc'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Basic CRUD Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) INSERT
# MAGIC INSERT INTO local.db.employees VALUES (1, 'John', 'Eng', 80000, DATE '2020-01-15', NULL, 'NYC')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) UPDATE
# MAGIC UPDATE local.db.employees
# MAGIC SET salary = salary * 1.10
# MAGIC WHERE department = 'Eng'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) DELETE
# MAGIC DELETE FROM local.db.employees
# MAGIC WHERE emp_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) MERGE (upsert)
# MAGIC MERGE INTO local.db.employees t
# MAGIC USING updates s
# MAGIC ON t.emp_id = s.emp_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Hidden Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Hidden partitioning with date transforms
# MAGIC CREATE TABLE local.db.events (
# MAGIC     event_id STRING,
# MAGIC     event_time TIMESTAMP,
# MAGIC     event_type STRING,
# MAGIC     payload STRING
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (year(event_time), month(event_time), day(event_time))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Bucket partitioning
# MAGIC CREATE TABLE local.db.users_bucketed (
# MAGIC     user_id STRING,
# MAGIC     name STRING,
# MAGIC     email STRING
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (bucket(16, user_id))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Truncate partitioning
# MAGIC CREATE TABLE local.db.logs (
# MAGIC     log_id STRING,
# MAGIC     message STRING,
# MAGIC     category STRING
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (truncate(category, 3))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Partition evolution - add new partition field
# MAGIC ALTER TABLE local.db.events
# MAGIC ADD PARTITION FIELD hour(event_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) View snapshots
# MAGIC SELECT * FROM local.db.employees.snapshots

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Query by timestamp
# MAGIC SELECT * FROM local.db.employees TIMESTAMP AS OF '2024-01-15 10:00:00'

# COMMAND ----------

# Query by snapshot ID using DataFrame API
# snapshot_id = spark.sql("SELECT snapshot_id FROM local.db.employees.snapshots LIMIT 1").first()[0]
# df = spark.read.option("snapshot-id", snapshot_id).table("local.db.employees")
# display(df)

print("Time travel query explained!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) View history
# MAGIC SELECT * FROM local.db.employees.history

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Roll back to snapshot
# MAGIC -- CALL local.system.rollback_to_snapshot('db.employees', snapshot_id)
# MAGIC -- Or by timestamp:
# MAGIC -- CALL local.system.rollback_to_timestamp('db.employees', TIMESTAMP '2024-01-15 10:00:00')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Schema Evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Add columns
# MAGIC ALTER TABLE local.db.employees
# MAGIC ADD COLUMNS (
# MAGIC     bonus DOUBLE,
# MAGIC     email STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Drop columns
# MAGIC ALTER TABLE local.db.employees
# MAGIC DROP COLUMN bonus

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Rename columns
# MAGIC ALTER TABLE local.db.employees
# MAGIC RENAME COLUMN name TO employee_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Reorder columns
# MAGIC ALTER TABLE local.db.employees
# MAGIC ALTER COLUMN email AFTER employee_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- e) Change types (widening only)
# MAGIC ALTER TABLE local.db.employees
# MAGIC ALTER COLUMN emp_id TYPE BIGINT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Partition Evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Add partition field
# MAGIC ALTER TABLE local.db.events
# MAGIC ADD PARTITION FIELD hour(event_time)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Remove partition field
# MAGIC ALTER TABLE local.db.events
# MAGIC DROP PARTITION FIELD day(event_time)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Replace partition field
# MAGIC ALTER TABLE local.db.events
# MAGIC REPLACE PARTITION FIELD day(event_time) WITH date(event_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Evolution Benefits:
# MAGIC - No need to rewrite existing data
# MAGIC - Queries work seamlessly across partition schemes
# MAGIC - Iceberg handles different partition layouts automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Table Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Expire snapshots
# MAGIC CALL local.system.expire_snapshots(
# MAGIC     table => 'db.employees',
# MAGIC     older_than => TIMESTAMP '2024-01-01 00:00:00',
# MAGIC     retain_last => 5
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Remove orphan files
# MAGIC CALL local.system.remove_orphan_files(
# MAGIC     table => 'db.employees',
# MAGIC     older_than => TIMESTAMP '2024-01-01 00:00:00'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Rewrite data files (compaction)
# MAGIC CALL local.system.rewrite_data_files(
# MAGIC     table => 'db.employees',
# MAGIC     options => map('target-file-size-bytes', '134217728')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Rewrite manifests
# MAGIC CALL local.system.rewrite_manifests('db.employees')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Branching and Tagging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Create branch
# MAGIC ALTER TABLE local.db.employees
# MAGIC CREATE BRANCH dev

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Write to branch
# MAGIC INSERT INTO local.db.employees.branch_dev
# MAGIC VALUES (999, 'Test', 'QA', 50000, DATE '2024-01-01', NULL, 'Test City')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Read from branch
# MAGIC SELECT * FROM local.db.employees VERSION AS OF 'dev'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Create tag
# MAGIC ALTER TABLE local.db.employees
# MAGIC CREATE TAG release_v1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- e) Fast-forward merge
# MAGIC CALL local.system.fast_forward('db.employees', 'main', 'dev')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Incremental Processing

# COMMAND ----------

# a) Incremental read (append-only)
# df = spark.read \
#     .option("start-snapshot-id", start_snapshot) \
#     .option("end-snapshot-id", end_snapshot) \
#     .table("local.db.employees")

# Or streaming
# stream_df = spark.readStream \
#     .format("iceberg") \
#     .option("stream-from-timestamp", "2024-01-01 00:00:00") \
#     .load("local.db.employees")

print("Incremental processing explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Metadata Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Snapshots
# MAGIC SELECT * FROM local.db.employees.snapshots

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) History
# MAGIC SELECT * FROM local.db.employees.history

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Files
# MAGIC SELECT * FROM local.db.employees.files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Manifests
# MAGIC SELECT * FROM local.db.employees.manifests

# COMMAND ----------

# MAGIC %sql
# MAGIC -- e) Partitions
# MAGIC SELECT * FROM local.db.employees.partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: COW vs MOR

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Copy-on-write (default) - better read performance
# MAGIC ALTER TABLE local.db.employees
# MAGIC SET TBLPROPERTIES (
# MAGIC     'write.delete.mode' = 'copy-on-write',
# MAGIC     'write.update.mode' = 'copy-on-write',
# MAGIC     'write.merge.mode' = 'copy-on-write'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Merge-on-read - faster writes
# MAGIC ALTER TABLE local.db.employees
# MAGIC SET TBLPROPERTIES (
# MAGIC     'write.delete.mode' = 'merge-on-read',
# MAGIC     'write.update.mode' = 'merge-on-read',
# MAGIC     'write.merge.mode' = 'merge-on-read'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### COW vs MOR:
# MAGIC | Mode | Write Speed | Read Speed | Use Case |
# MAGIC |------|------------|------------|----------|
# MAGIC | **COW** | Slower | Faster | Read-heavy workloads |
# MAGIC | **MOR** | Faster | Slower | Write-heavy workloads |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Performance Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Split configuration
# MAGIC ALTER TABLE local.db.employees SET TBLPROPERTIES (
# MAGIC     'read.split.target-size' = '134217728',
# MAGIC     'read.split.planning-lookback' = '10'
# MAGIC )

# COMMAND ----------

# b) Vectorized reads
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")
print("Vectorized reads enabled!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) File sizes
# MAGIC ALTER TABLE local.db.employees SET TBLPROPERTIES (
# MAGIC     'write.target-file-size-bytes' = '134217728',
# MAGIC     'write.parquet.row-group-size-bytes' = '8388608'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Sort order
# MAGIC ALTER TABLE local.db.employees
# MAGIC WRITE ORDERED BY (department, salary DESC)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- e) Bloom filters
# MAGIC ALTER TABLE local.db.employees SET TBLPROPERTIES (
# MAGIC     'write.parquet.bloom-filter-enabled.column.emp_id' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Complete Pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Create fact table with hidden partitioning
# MAGIC CREATE TABLE local.warehouse.sales_facts (
# MAGIC     transaction_id STRING,
# MAGIC     product_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     quantity INT,
# MAGIC     amount DOUBLE,
# MAGIC     transaction_time TIMESTAMP
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (day(transaction_time))
# MAGIC TBLPROPERTIES (
# MAGIC     'write.target-file-size-bytes' = '134217728',
# MAGIC     'format-version' = '2'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. SCD Type 2 dimension
# MAGIC CREATE TABLE local.warehouse.dim_customer (
# MAGIC     customer_key BIGINT,
# MAGIC     customer_id STRING,
# MAGIC     name STRING,
# MAGIC     email STRING,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING iceberg

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. SCD2 MERGE
# MAGIC MERGE INTO local.warehouse.dim_customer t
# MAGIC USING (
# MAGIC     SELECT *, current_timestamp() as load_time FROM staging_customers
# MAGIC ) s
# MAGIC ON t.customer_id = s.customer_id AND t.is_current = true
# MAGIC WHEN MATCHED AND t.name != s.name OR t.email != s.email THEN
# MAGIC     UPDATE SET is_current = false, effective_to = s.load_time
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_key, customer_id, name, email, effective_from, effective_to, is_current)
# MAGIC     VALUES (s.customer_key, s.customer_id, s.name, s.email, s.load_time, NULL, true)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Maintenance
# MAGIC CALL local.system.expire_snapshots('warehouse.sales_facts', retain_last => 10);
# MAGIC CALL local.system.rewrite_data_files('warehouse.sales_facts')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Iceberg vs Delta Lake
# MAGIC
# MAGIC | Feature | Iceberg | Delta Lake |
# MAGIC |---------|---------|------------|
# MAGIC | **Hidden Partitioning** | Yes (transforms) | No |
# MAGIC | **Partition Evolution** | Yes | Limited |
# MAGIC | **Schema Evolution** | Full support | Full support |
# MAGIC | **Time Travel** | Snapshot-based | Version-based |
# MAGIC | **Branching** | Native | Tags only |
# MAGIC | **File Formats** | Parquet, ORC, Avro | Parquet only |
# MAGIC | **Engine Support** | Multi-engine | Spark-native |
