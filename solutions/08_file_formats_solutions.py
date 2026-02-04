"""
File Formats - Solutions
=========================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.functions import col, input_file_name, current_timestamp, explode

spark = SparkSession.builder \
    .appName("File Formats Solutions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# =============================================================================
# Problem 1: CSV Operations
# =============================================================================
# a) Read CSV with pipe delimiter
pipe_csv = """id|name|value
1|Alice|100
2|Bob|200"""

# Save sample file first
with open("../datasets/csv/pipe_delimited.csv", "w") as f:
    f.write(pipe_csv)

df = spark.read.option("delimiter", "|").option("header", True).csv("../datasets/csv/pipe_delimited.csv")
df.show()

# b) Custom null value
df = spark.read.option("nullValue", "NA").option("header", True).csv("../datasets/csv/employees.csv")

# c) Multiline values
df = spark.read.option("multiLine", True).option("header", True).csv("path/to/multiline.csv")

# d) Custom quote character
df = spark.read.option("quote", "'").option("header", True).csv("path/to/file.csv")

# e) Skip malformed records
df = spark.read \
    .option("mode", "DROPMALFORMED") \
    .option("header", True) \
    .csv("../datasets/csv/employees.csv")

# Other modes: PERMISSIVE (default), FAILFAST


# =============================================================================
# Problem 2: JSON Operations
# =============================================================================
# a) Single-line JSON (default)
df = spark.read.json("../datasets/json/events.json")
df.show()

# b) Multi-line JSON
df = spark.read.option("multiLine", True).json("path/to/multiline.json")

# c) Nested structures
orders = spark.read.json("../datasets/json/orders.json")
orders.printSchema()
orders.show(truncate=False)

# d) Flatten nested JSON
# Access nested fields
orders.select(
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
).show()

# e) Varying schemas - use mergeSchema
df = spark.read.option("mergeSchema", True).json("path/to/json/dir/")


# =============================================================================
# Problem 3: Parquet Operations
# =============================================================================
employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)

# a) Write as Parquet
employees.write.mode("overwrite").parquet("../datasets/parquet/employees")

# b) Read with column pruning (automatic - just select columns)
df = spark.read.parquet("../datasets/parquet/employees").select("name", "salary")
df.show()

# c) Write with partitioning
employees.write.mode("overwrite").partitionBy("department").parquet("../datasets/parquet/employees_partitioned")

# d) Write with compression
employees.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("../datasets/parquet/employees_snappy")

# Other compression: gzip, lz4, zstd

# e) Merge schema from multiple files
df = spark.read.option("mergeSchema", True).parquet("../datasets/parquet/employees_partitioned")


# =============================================================================
# Problem 4: Schema Evolution
# =============================================================================
# a) Read files with different schemas
df = spark.read.option("mergeSchema", True).parquet("path/to/files/")

# b) Handle missing columns - use coalesce with default
from pyspark.sql.functions import coalesce, lit

df = df.withColumn("new_column", coalesce(col("new_column"), lit("default")))

# c) Handle new columns
# When writing, new columns are added automatically with mergeSchema

# d) Schema enforcement vs evolution
# Enforcement: Fail if schema doesn't match
df = spark.read.schema(explicit_schema).parquet("path")

# Evolution: Allow schema changes
df = spark.read.option("mergeSchema", True).parquet("path")


# =============================================================================
# Problem 5: Partitioning
# =============================================================================
# a) Single column partition
employees.write.mode("overwrite").partitionBy("department").parquet("../datasets/parquet/by_dept")

# b) Multiple column partition
employees.write.mode("overwrite").partitionBy("department", "city").parquet("../datasets/parquet/by_dept_city")

# c) Read specific partitions (partition pruning)
df = spark.read.parquet("../datasets/parquet/by_dept") \
    .filter(col("department") == "Engineering")

# Verify partition pruning in explain
df.explain()

# d) Dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
employees.write.mode("overwrite").partitionBy("department").parquet("path")

# e) Partition pruning happens automatically when filtering on partition columns


# =============================================================================
# Problem 6: Complex Data Types
# =============================================================================
# a) Arrays
schema_with_array = StructType([
    StructField("id", IntegerType()),
    StructField("tags", ArrayType(StringType()))
])

# b) Nested structs
schema_with_struct = StructType([
    StructField("id", IntegerType()),
    StructField("address", StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("zip", StringType())
    ]))
])

# c) Maps
schema_with_map = StructType([
    StructField("id", IntegerType()),
    StructField("properties", MapType(StringType(), StringType()))
])

# d) Read/write complex types
orders = spark.read.json("../datasets/json/orders.json")
orders.write.mode("overwrite").parquet("../datasets/parquet/orders_complex")

# Read back
df = spark.read.parquet("../datasets/parquet/orders_complex")
df.printSchema()


# =============================================================================
# Problem 7: Compression
# =============================================================================
employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)

# a) Snappy (default, good balance)
employees.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("../datasets/parquet/emp_snappy")

# b) Gzip (better compression, slower)
employees.write.mode("overwrite") \
    .option("compression", "gzip") \
    .parquet("../datasets/parquet/emp_gzip")

# c) Compare file sizes (conceptual)
import os

# snappy_size = os.path.getsize("../datasets/parquet/emp_snappy")
# gzip_size = os.path.getsize("../datasets/parquet/emp_gzip")

# d) Read compressed files (automatic detection)
df = spark.read.parquet("../datasets/parquet/emp_snappy")


# =============================================================================
# Problem 8: ORC Format
# =============================================================================
# a) Write as ORC
employees.write.mode("overwrite").orc("../datasets/orc/employees")

# b) Read ORC
df = spark.read.orc("../datasets/orc/employees")
df.show()

# c) Parquet vs ORC
# - Parquet: Better for nested data, wider ecosystem
# - ORC: Better for Hive integration, ACID support
# Both support predicate pushdown and columnar storage

# d) ORC-specific options
employees.write.mode("overwrite") \
    .option("compression", "zlib") \
    .option("orc.bloom.filter.columns", "name") \
    .orc("../datasets/orc/employees_optimized")


# =============================================================================
# Problem 9: Text and Binary Files
# =============================================================================
# a) Plain text files
text_df = spark.read.text("../datasets/csv/employees.csv")
text_df.show(5, truncate=False)

# b) Line by line processing
lines = spark.read.text("../datasets/csv/employees.csv")
lines.show()

# c) Whole text file as single row
whole_text = spark.read.option("wholetext", True).text("../datasets/csv/employees.csv")
whole_text.show(truncate=False)

# d) Binary files
# binary_df = spark.read.format("binaryFile").load("path/to/binary/files")


# =============================================================================
# Problem 10: Delta Lake (if available)
# =============================================================================
# Note: Requires delta-spark package

try:
    # a) Write as Delta table
    employees.write.mode("overwrite").format("delta").save("../datasets/delta/employees")

    # b) Read Delta table
    df = spark.read.format("delta").load("../datasets/delta/employees")
    df.show()

    # c) Time travel
    # By version
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("../datasets/delta/employees")

    # By timestamp
    # df_ts = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)

    # d) MERGE/UPSERT
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, "../datasets/delta/employees")

    # delta_table.alias("target").merge(
    #     updates.alias("source"),
    #     "target.emp_id = source.emp_id"
    # ).whenMatchedUpdate(set={"salary": "source.salary"}) \
    #  .whenNotMatchedInsert(values={"emp_id": "source.emp_id", "name": "source.name"}) \
    #  .execute()

    # e) Schema evolution
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

except Exception as e:
    print(f"Delta Lake not available: {e}")


# =============================================================================
# Problem 11: File Metadata
# =============================================================================
# a) Input file name
df = spark.read.csv("../datasets/csv/employees.csv", header=True) \
    .withColumn("source_file", input_file_name())
df.select("name", "source_file").show(truncate=False)

# b) File modification time (available in Spark 3.0+)
# df = spark.read.format("binaryFile").load("path").select("modificationTime")

# c) Process multiple files with different schemas
# Use mergeSchema option or process files separately


# =============================================================================
# Problem 12: Performance Considerations
# =============================================================================
# a) Optimal file format
# - Parquet for analytics workloads (columnar, compression)
# - Delta for ACID, time travel, updates
# - Avro for row-based, schema evolution

# b) Optimal partition size: 128MB - 1GB per partition
# Check partition count
df = spark.read.parquet("../datasets/parquet/employees")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# c) Coalesce vs repartition before write
# Coalesce: reduces partitions, avoids shuffle
# Repartition: can increase/decrease, causes shuffle

employees.coalesce(1).write.mode("overwrite").parquet("../datasets/parquet/emp_single")
employees.repartition(4).write.mode("overwrite").parquet("../datasets/parquet/emp_four")

# d) File statistics and pruning
# Parquet stores min/max statistics per column per row group
# This enables data skipping


# =============================================================================
# Problem 13: Error Handling
# =============================================================================
# a) Corrupt record handling modes
# PERMISSIVE (default): Nulls for corrupt fields, adds _corrupt_record column
# DROPMALFORMED: Drops corrupt records
# FAILFAST: Fails immediately

df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .csv("../datasets/csv/employees.csv", header=True)

# b) Bad records path
df = spark.read \
    .option("badRecordsPath", "../output/bad_records") \
    .csv("../datasets/csv/employees.csv", header=True)

# c) Column name sanitization
# Spark 3.0+ automatically handles special characters
df = spark.read.option("header", True) \
    .option("enforceSchema", False) \
    .csv("path/to/file.csv")
