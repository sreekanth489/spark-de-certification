# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Evolution - Solutions
# MAGIC Topics: Schema definition, adding/removing columns, type changes, nested evolution, format-specific handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, ArrayType, MapType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, lit, coalesce, when, from_json, to_json, schema_of_json, struct

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Schema Definition and Enforcement

# COMMAND ----------

# a) Define explicit schema
employee_schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", StringType(), nullable=True)
])

print("Schema defined:")
print(employee_schema.simpleString())

# COMMAND ----------

# b) Schema enforcement (FAILFAST)
try:
    df = spark.read \
        .schema(employee_schema) \
        .option("mode", "FAILFAST") \
        .csv("datasets/csv/employees.csv", header=True)
    display(df)
except Exception as e:
    print(f"Schema mismatch: {e}")

# COMMAND ----------

# c) Permissive mode (default) - captures corrupt records
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .csv("datasets/csv/employees.csv", header=True, inferSchema=True)

display(df)

# COMMAND ----------

# d) Schema hints from JSON
json_schema = spark.read.json("datasets/json/orders.json").schema
print("Inferred JSON schema:")
json_schema.printTreeString()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Adding Columns

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)

# a) Add nullable column
df = employees.withColumn("bonus", lit(None).cast(DoubleType()))
display(df.select("name", "salary", "bonus"))

# COMMAND ----------

# b) Add column with default value
df = employees.withColumn("status", lit("active"))
df = df.withColumn("bonus_pct", lit(0.10))
display(df.select("name", "status", "bonus_pct"))

# COMMAND ----------

# c) Handle old data without new column - use coalesce
old_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType())
])

new_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("department", StringType())  # New column
])

# Reading old data with new schema - missing column becomes null
# df = spark.read.schema(new_schema).csv("old_data.csv")
# df = df.withColumn("department", coalesce(col("department"), lit("Unknown")))

print("Column addition strategies explained!")

# COMMAND ----------

# d) Merge schemas from multiple Parquet files
# df = spark.read \
#     .option("mergeSchema", "true") \
#     .parquet("datasets/parquet/employees_partitioned")

print("mergeSchema option explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Removing Columns

# COMMAND ----------

# a) Drop column
df = employees.drop("manager_id")
display(df)

# COMMAND ----------

# b) Drop multiple columns
df = employees.drop("manager_id", "hire_date")
print(f"Remaining columns: {df.columns}")

# COMMAND ----------

# c) Selective reading - only keep needed columns
required_cols = ["emp_id", "name", "salary"]
df = employees.select([c for c in required_cols if c in employees.columns])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Renaming Columns

# COMMAND ----------

# a) Single column rename
df = employees.withColumnRenamed("emp_id", "employee_id")
display(df.select("employee_id", "name"))

# COMMAND ----------

# b) Multiple columns using a map
rename_map = {
    "emp_id": "employee_id",
    "name": "full_name",
    "salary": "annual_salary"
}

df = employees
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)

print(f"Renamed columns: {df.columns}")

# COMMAND ----------

# c) Using select with alias (cleaner approach)
df = employees.select([col(c).alias(rename_map.get(c, c)) for c in employees.columns])
display(df)

# COMMAND ----------

# d) Backward compatibility - keep both columns temporarily
df = employees.withColumn("employee_id", col("emp_id"))
print(f"Has both: {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Type Changes

# COMMAND ----------

# a) Widen numeric types
df = employees.withColumn("emp_id", col("emp_id").cast(LongType()))
df = df.withColumn("salary", col("salary").cast(DoubleType()))
df.printSchema()

# COMMAND ----------

# b) String to other types
from pyspark.sql.functions import to_date, to_timestamp

df = employees.withColumn("hire_date_parsed", to_date(col("hire_date"), "yyyy-MM-dd"))
display(df.select("name", "hire_date", "hire_date_parsed"))

# COMMAND ----------

# c) Safe casting with error handling
df = employees.withColumn(
    "salary_safe",
    when(col("salary").cast(DoubleType()).isNotNull(), col("salary").cast(DoubleType()))
    .otherwise(lit(0.0))
)
display(df.select("name", "salary", "salary_safe"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Nested Schema Evolution

# COMMAND ----------

# Sample nested data
nested_schema = StructType([
    StructField("id", IntegerType()),
    StructField("info", StructType([
        StructField("name", StringType()),
        StructField("address", StructType([
            StructField("city", StringType()),
            StructField("state", StringType())
        ]))
    ]))
])

nested_data = [(1, ("John", ("NYC", "NY")))]
nested_df = spark.createDataFrame(nested_data, nested_schema)
nested_df.printSchema()

# COMMAND ----------

display(nested_df)

# COMMAND ----------

# a) Add field to nested struct
df = nested_df.withColumn(
    "info",
    struct(
        col("info.name"),
        col("info.address"),
        lit("new_field_value").alias("new_field")
    )
)
df.printSchema()

# COMMAND ----------

# b) Remove field from nested - recreate struct without field
df = nested_df.withColumn(
    "info",
    struct(
        col("info.name")
        # Omit address to remove it
    )
)
df.printSchema()

# COMMAND ----------

# c) Rename nested field
df = nested_df.withColumn(
    "info",
    struct(
        col("info.name").alias("full_name"),
        col("info.address")
    )
)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Parquet Schema Evolution

# COMMAND ----------

# a) Enable mergeSchema
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# Read with mergeSchema
# df = spark.read \
#     .option("mergeSchema", "true") \
#     .parquet("path/to/parquet/directory")

print("Parquet mergeSchema enabled!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet Compatibility Rules:
# MAGIC - ✅ Can add nullable columns
# MAGIC - ✅ Can widen numeric types (int → long, float → double)
# MAGIC - ❌ Cannot change field names
# MAGIC - ❌ Cannot change incompatible types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Delta Lake Schema Evolution

# COMMAND ----------

# a) autoMerge - automatically add new columns
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or per-write
# df.write.option("mergeSchema", "true").format("delta").mode("append").save(path)

print("Delta autoMerge enabled!")

# COMMAND ----------

# b) overwriteSchema - for breaking changes
# df.write.option("overwriteSchema", "true").format("delta").mode("overwrite").save(path)

print("overwriteSchema explained!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Column mapping modes (Delta 2.0+)
# MAGIC -- ALTER TABLE delta_table SET TBLPROPERTIES (
# MAGIC --     'delta.columnMapping.mode' = 'name'
# MAGIC -- )
# MAGIC -- Modes: none, name, id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Schema Validation

# COMMAND ----------

# a) Compare schemas
schema1 = employees.schema
schema2 = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("salary", DoubleType())
])

def schemas_equal(s1, s2):
    return s1.json() == s2.json()

print(f"Schemas equal: {schemas_equal(schema1, schema2)}")

# COMMAND ----------

# b) Schema diff
def schema_diff(old_schema, new_schema):
    old_fields = {f.name for f in old_schema.fields}
    new_fields = {f.name for f in new_schema.fields}

    added = new_fields - old_fields
    removed = old_fields - new_fields

    return {"added": added, "removed": removed}

diff = schema_diff(schema2, schema1)
print(f"Schema diff: {diff}")

# COMMAND ----------

# c) Validate compatibility
def is_compatible(old_schema, new_schema):
    """Check if new schema is backward compatible"""
    old_fields = {f.name: f for f in old_schema.fields}
    new_fields = {f.name: f for f in new_schema.fields}

    # All old fields must exist in new schema
    for name, old_field in old_fields.items():
        if name not in new_fields:
            return False, f"Missing field: {name}"
    return True, "Compatible"

compatible, msg = is_compatible(schema2, schema1)
print(f"Compatible: {compatible} - {msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Handling Schema Drift

# COMMAND ----------

# a) Detect drift
def detect_drift(current_df, reference_schema):
    current_fields = set(f.name for f in current_df.schema.fields)
    reference_fields = set(f.name for f in reference_schema.fields)

    new_fields = current_fields - reference_fields
    missing_fields = reference_fields - current_fields

    if new_fields or missing_fields:
        return True, {"new": new_fields, "missing": missing_fields}
    return False, {}

has_drift, drift_info = detect_drift(employees, schema2)
print(f"Drift detected: {has_drift}")
print(f"Drift info: {drift_info}")

# COMMAND ----------

# b) Alert on changes
def process_with_drift_detection(df, expected_schema, on_drift="warn"):
    has_drift, drift_info = detect_drift(df, expected_schema)
    if has_drift:
        if on_drift == "fail":
            raise ValueError(f"Schema drift detected: {drift_info}")
        elif on_drift == "warn":
            print(f"WARNING: Schema drift detected: {drift_info}")
    return df

df = process_with_drift_detection(employees, schema2, on_drift="warn")

# COMMAND ----------

# c) Graceful handling - add missing columns with nulls
def handle_new_columns(df, expected_schema):
    expected_cols = [f.name for f in expected_schema.fields]

    # Add missing expected columns with nulls
    for field in expected_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    # Select only expected columns
    return df.select(expected_cols)

print("Graceful handling function defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: JSON Schema Evolution

# COMMAND ----------

# a) Read JSON with schema inference
json_df = spark.read.option("multiLine", True).json("datasets/json/orders.json")
json_df.printSchema()

# COMMAND ----------

# b) Flatten with evolution support
from pyspark.sql.functions import explode_outer

flattened = json_df.select(
    "order_id",
    col("shipping.city").alias("ship_city"),
    explode_outer("items").alias("item")
).select(
    "order_id",
    "ship_city",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity")
)
display(flattened)

# COMMAND ----------

# c) Schema inference from sample
sample_json = '{"id": 1, "name": "test", "values": [1, 2, 3]}'
inferred_schema = schema_of_json(lit(sample_json))
print(f"Inferred schema: {inferred_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Streaming Schema Evolution

# COMMAND ----------

# a) Define schema explicitly for stability
stream_schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType()),
    StructField("timestamp", TimestampType())
])

# stream = spark.readStream.schema(stream_schema).json("path")

print("Streaming schema defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Schema Evolution:
# MAGIC - Schema changes in streaming require restart
# MAGIC - For breaking changes: Stop stream → Clear checkpoint → Restart with new schema
# MAGIC - New columns appear as null with explicit schema
# MAGIC - Use schema hints for incremental additions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Best Practices

# COMMAND ----------

# 1. Version schemas
schema_v1 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

schema_v2 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType())  # Added in v2
])

print("Schema versioning example!")

# COMMAND ----------

# 2. Migration procedures
def migrate_v1_to_v2(df):
    """Migrate data from schema v1 to v2"""
    return df.withColumn("email", lit(None).cast(StringType()))

# Apply migration
# migrated = migrate_v1_to_v2(old_data)

print("Migration function defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution Summary
# MAGIC
# MAGIC | Operation | Parquet | Delta | Iceberg |
# MAGIC |-----------|---------|-------|---------|
# MAGIC | **Add Column** | mergeSchema | autoMerge | ALTER TABLE |
# MAGIC | **Drop Column** | Select subset | ALTER TABLE | ALTER TABLE |
# MAGIC | **Rename Column** | Select with alias | Column mapping | ALTER TABLE |
# MAGIC | **Type Widening** | Automatic | Automatic | ALTER TABLE |
# MAGIC | **Breaking Changes** | New files | overwriteSchema | ALTER TABLE |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compatibility Rules:
# MAGIC - **Backward Compatible**: New code can read old data
# MAGIC - **Forward Compatible**: Old code can read new data
# MAGIC - **Full Compatible**: Both directions
# MAGIC
# MAGIC ### Safe Changes:
# MAGIC - ✅ Add nullable columns
# MAGIC - ✅ Widen numeric types (int → long)
# MAGIC - ❌ Remove required columns
# MAGIC - ❌ Narrow types (long → int)
# MAGIC - ❌ Change types incompatibly (string → int)
