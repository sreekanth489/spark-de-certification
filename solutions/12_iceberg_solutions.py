"""
Apache Iceberg - Solutions
===========================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr, year, month, dayofmonth

# Initialize Spark with Iceberg
spark = SparkSession.builder \
    .appName("Iceberg Solutions") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "../datasets/iceberg-warehouse") \
    .getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Creating Iceberg Tables
# =============================================================================
# a) Create using SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.employees (
        emp_id INT,
        name STRING,
        department STRING,
        salary DOUBLE,
        hire_date DATE,
        manager_id INT,
        city STRING
    )
    USING iceberg
""")

# b) Create from DataFrame
employees.writeTo("local.db.employees_df").using("iceberg").createOrReplace()

# c) Convert existing table (migrate procedure)
# CALL local.system.migrate('db.existing_table')
# Or snapshot existing table
# CALL local.system.snapshot('db.source', 'db.target')

# d) Specific file format
spark.sql("""
    CREATE TABLE local.db.employees_orc (
        emp_id INT,
        name STRING,
        salary DOUBLE
    )
    USING iceberg
    TBLPROPERTIES (
        'write.format.default' = 'orc'
    )
""")


# =============================================================================
# Problem 2: Basic CRUD Operations
# =============================================================================
# a) INSERT
spark.sql("INSERT INTO local.db.employees VALUES (1, 'John', 'Eng', 80000, DATE '2020-01-15', NULL, 'NYC')")

# INSERT from DataFrame
employees.writeTo("local.db.employees").append()

# b) UPDATE
spark.sql("""
    UPDATE local.db.employees
    SET salary = salary * 1.10
    WHERE department = 'Eng'
""")

# c) DELETE
spark.sql("""
    DELETE FROM local.db.employees
    WHERE emp_id = 1
""")

# d) MERGE
spark.sql("""
    MERGE INTO local.db.employees t
    USING updates s
    ON t.emp_id = s.emp_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")


# =============================================================================
# Problem 3: Hidden Partitioning
# =============================================================================
# a) Hidden partitioning with date transforms
spark.sql("""
    CREATE TABLE local.db.events (
        event_id STRING,
        event_time TIMESTAMP,
        event_type STRING,
        payload STRING
    )
    USING iceberg
    PARTITIONED BY (year(event_time), month(event_time), day(event_time))
""")

# b) Bucket partitioning
spark.sql("""
    CREATE TABLE local.db.users_bucketed (
        user_id STRING,
        name STRING,
        email STRING
    )
    USING iceberg
    PARTITIONED BY (bucket(16, user_id))
""")

# c) Truncate partitioning
spark.sql("""
    CREATE TABLE local.db.logs (
        log_id STRING,
        message STRING,
        category STRING
    )
    USING iceberg
    PARTITIONED BY (truncate(category, 3))
""")

# d) Partition evolution
spark.sql("""
    ALTER TABLE local.db.events
    ADD PARTITION FIELD hour(event_time)
""")


# =============================================================================
# Problem 4: Time Travel
# =============================================================================
# a) Query by snapshot ID
snapshot_id = spark.sql("SELECT snapshot_id FROM local.db.employees.snapshots LIMIT 1").first()[0]
spark.sql(f"SELECT * FROM local.db.employees VERSION AS OF {snapshot_id}").show()

# Or using DataFrame API
spark.read.option("snapshot-id", snapshot_id).table("local.db.employees").show()

# b) Query by timestamp
spark.sql("""
    SELECT * FROM local.db.employees TIMESTAMP AS OF '2024-01-15 10:00:00'
""").show()

# c) View history
spark.sql("SELECT * FROM local.db.employees.history").show()
spark.sql("SELECT * FROM local.db.employees.snapshots").show()

# d) Roll back
spark.sql(f"CALL local.system.rollback_to_snapshot('db.employees', {snapshot_id})")

# Or by timestamp
# CALL local.system.rollback_to_timestamp('db.employees', TIMESTAMP '2024-01-15 10:00:00')

# e) Cherry-pick (copy snapshot changes)
# CALL local.system.cherrypick_snapshot('db.employees', snapshot_id)


# =============================================================================
# Problem 5: Schema Evolution
# =============================================================================
# a) Add columns
spark.sql("""
    ALTER TABLE local.db.employees
    ADD COLUMNS (
        bonus DOUBLE,
        email STRING
    )
""")

# b) Drop columns
spark.sql("""
    ALTER TABLE local.db.employees
    DROP COLUMN bonus
""")

# c) Rename columns
spark.sql("""
    ALTER TABLE local.db.employees
    RENAME COLUMN name TO employee_name
""")

# d) Reorder columns
spark.sql("""
    ALTER TABLE local.db.employees
    ALTER COLUMN email AFTER employee_name
""")

# e) Change types (widening only)
spark.sql("""
    ALTER TABLE local.db.employees
    ALTER COLUMN emp_id TYPE BIGINT
""")

# f) Change nullability
spark.sql("""
    ALTER TABLE local.db.employees
    ALTER COLUMN emp_id DROP NOT NULL
""")


# =============================================================================
# Problem 6: Partition Evolution
# =============================================================================
# a) Add partition field
spark.sql("""
    ALTER TABLE local.db.events
    ADD PARTITION FIELD hour(event_time)
""")

# b) Remove partition field
spark.sql("""
    ALTER TABLE local.db.events
    DROP PARTITION FIELD day(event_time)
""")

# c) Replace partition field
spark.sql("""
    ALTER TABLE local.db.events
    REPLACE PARTITION FIELD day(event_time) WITH date(event_time)
""")

# d) Query seamlessly across partition schemes
# Iceberg handles different partition schemes automatically
spark.sql("SELECT * FROM local.db.events WHERE event_time >= '2024-01-01'").show()


# =============================================================================
# Problem 7: Table Maintenance
# =============================================================================
# a) Expire snapshots
spark.sql("""
    CALL local.system.expire_snapshots(
        table => 'db.employees',
        older_than => TIMESTAMP '2024-01-01 00:00:00',
        retain_last => 5
    )
""")

# b) Remove orphan files
spark.sql("""
    CALL local.system.remove_orphan_files(
        table => 'db.employees',
        older_than => TIMESTAMP '2024-01-01 00:00:00'
    )
""")

# c) Rewrite data files (compaction)
spark.sql("""
    CALL local.system.rewrite_data_files(
        table => 'db.employees',
        options => map('target-file-size-bytes', '134217728')
    )
""")

# d) Rewrite manifests
spark.sql("""
    CALL local.system.rewrite_manifests('db.employees')
""")


# =============================================================================
# Problem 8: Branching and Tagging
# =============================================================================
# a) Create branch
spark.sql("""
    ALTER TABLE local.db.employees
    CREATE BRANCH dev
""")

# b) Write to branch
spark.sql("""
    INSERT INTO local.db.employees.branch_dev
    VALUES (999, 'Test', 'QA', 50000, DATE '2024-01-01', NULL, 'Test City')
""")

# c) Read from branch
spark.sql("SELECT * FROM local.db.employees VERSION AS OF 'dev'").show()

# d) Create tag
spark.sql("""
    ALTER TABLE local.db.employees
    CREATE TAG release_v1
""")

# e) Fast-forward merge
spark.sql("""
    CALL local.system.fast_forward('db.employees', 'main', 'dev')
""")


# =============================================================================
# Problem 9: Incremental Processing
# =============================================================================
# a) Incremental read (append-only)
spark.read \
    .option("start-snapshot-id", start_snapshot) \
    .option("end-snapshot-id", end_snapshot) \
    .table("local.db.employees")

# Or streaming
stream_df = spark.readStream \
    .format("iceberg") \
    .option("stream-from-timestamp", "2024-01-01 00:00:00") \
    .load("local.db.employees")

# b) Write streaming
# stream.writeStream.format("iceberg").toTable("local.db.target")

# c) Track changes
spark.sql("SELECT * FROM local.db.employees.snapshots ORDER BY committed_at DESC").show()


# =============================================================================
# Problem 10: Metadata Tables
# =============================================================================
# a) Snapshots
spark.sql("SELECT * FROM local.db.employees.snapshots").show()

# b) History
spark.sql("SELECT * FROM local.db.employees.history").show()

# c) Files
spark.sql("SELECT * FROM local.db.employees.files").show()

# d) Manifests
spark.sql("SELECT * FROM local.db.employees.manifests").show()

# e) Partitions
spark.sql("SELECT * FROM local.db.employees.partitions").show()

# Also available:
# - entries (manifest entries)
# - all_data_files
# - all_manifests


# =============================================================================
# Problem 11: COW vs MOR
# =============================================================================
# a) Copy-on-write (default)
spark.sql("""
    ALTER TABLE local.db.employees
    SET TBLPROPERTIES (
        'write.delete.mode' = 'copy-on-write',
        'write.update.mode' = 'copy-on-write',
        'write.merge.mode' = 'copy-on-write'
    )
""")

# b) Merge-on-read
spark.sql("""
    ALTER TABLE local.db.employees
    SET TBLPROPERTIES (
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )
""")

# COW: Better read performance, slower writes
# MOR: Faster writes, read-time merge overhead


# =============================================================================
# Problem 12: Performance Optimization
# =============================================================================
# a) Split configuration
spark.sql("""
    ALTER TABLE local.db.employees SET TBLPROPERTIES (
        'read.split.target-size' = '134217728',
        'read.split.planning-lookback' = '10'
    )
""")

# b) Vectorized reads
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")

# c) File sizes
spark.sql("""
    ALTER TABLE local.db.employees SET TBLPROPERTIES (
        'write.target-file-size-bytes' = '134217728',
        'write.parquet.row-group-size-bytes' = '8388608'
    )
""")

# d) Sort order
spark.sql("""
    ALTER TABLE local.db.employees
    WRITE ORDERED BY (department, salary DESC)
""")

# e) Bloom filters
spark.sql("""
    ALTER TABLE local.db.employees SET TBLPROPERTIES (
        'write.parquet.bloom-filter-enabled.column.emp_id' = 'true'
    )
""")


# =============================================================================
# Problem 13: Multi-catalog
# =============================================================================
# Configuration example
"""
spark.sql.catalog.prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.prod.type = hive
spark.sql.catalog.prod.uri = thrift://metastore:9083

spark.sql.catalog.dev = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.dev.type = hadoop
spark.sql.catalog.dev.warehouse = s3://dev-bucket/warehouse

spark.sql.catalog.glue = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue.warehouse = s3://bucket/warehouse
"""

# Query across catalogs
# SELECT * FROM prod.db.table
# SELECT * FROM dev.db.table


# =============================================================================
# Problem 14: Row-Level Operations
# =============================================================================
# Position deletes (efficient for small deletes)
# Stored separately, merged at read time

# Equality deletes (for streaming deletes)
spark.sql("""
    ALTER TABLE local.db.employees SET TBLPROPERTIES (
        'write.delete.mode' = 'merge-on-read'
    )
""")


# =============================================================================
# Problem 15: Complete Pipeline
# =============================================================================
# 1. Create fact table with hidden partitioning
spark.sql("""
    CREATE TABLE local.warehouse.sales_facts (
        transaction_id STRING,
        product_id STRING,
        customer_id STRING,
        quantity INT,
        amount DOUBLE,
        transaction_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (day(transaction_time))
    TBLPROPERTIES (
        'write.target-file-size-bytes' = '134217728',
        'format-version' = '2'
    )
""")

# 2. SCD Type 2 dimension
spark.sql("""
    CREATE TABLE local.warehouse.dim_customer (
        customer_key BIGINT,
        customer_id STRING,
        name STRING,
        email STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    )
    USING iceberg
""")

# SCD2 MERGE
spark.sql("""
    MERGE INTO local.warehouse.dim_customer t
    USING (
        SELECT *, current_timestamp() as load_time FROM staging_customers
    ) s
    ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED AND t.name != s.name OR t.email != s.email THEN
        UPDATE SET is_current = false, effective_to = s.load_time
    WHEN NOT MATCHED THEN
        INSERT (customer_key, customer_id, name, email, effective_from, effective_to, is_current)
        VALUES (s.customer_key, s.customer_id, s.name, s.email, s.load_time, NULL, true)
""")

# 3. Incremental fact loading
spark.sql("""
    INSERT INTO local.warehouse.sales_facts
    SELECT * FROM staging_sales
    WHERE transaction_time > (SELECT MAX(transaction_time) FROM local.warehouse.sales_facts)
""")

# 4. Maintenance
spark.sql("CALL local.system.expire_snapshots('warehouse.sales_facts', retain_last => 10)")
spark.sql("CALL local.system.rewrite_data_files('warehouse.sales_facts')")

# 5. Time travel audit
spark.sql("""
    SELECT * FROM local.warehouse.sales_facts
    TIMESTAMP AS OF '2024-01-15 00:00:00'
    WHERE transaction_id = 'TXN001'
""")
