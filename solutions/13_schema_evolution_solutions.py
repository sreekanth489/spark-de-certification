"""
Schema Evolution - Solutions
=============================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, ArrayType, MapType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, lit, coalesce, when, from_json, to_json, schema_of_json

spark = SparkSession.builder.appName("Schema Evolution Solutions").getOrCreate()

# =============================================================================
# Problem 1: Schema Definition and Enforcement
# =============================================================================
# a) Define explicit schema
employee_schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", StringType(), nullable=True)
])

# b) Schema enforcement (FAILFAST)
try:
    df = spark.read \
        .schema(employee_schema) \
        .option("mode", "FAILFAST") \
        .csv("../datasets/csv/employees.csv", header=True)
except Exception as e:
    print(f"Schema mismatch: {e}")

# c) Permissive mode (default)
df = spark.read \
    .schema(employee_schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt") \
    .csv("../datasets/csv/employees.csv", header=True)

# d) Schema hints (JSON/complex types)
json_schema = spark.read.json("../datasets/json/orders.json").schema
df = spark.read.schema(json_schema).json("../datasets/json/orders.json")


# =============================================================================
# Problem 2: Adding Columns
# =============================================================================
employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)

# a) Add nullable column
df = employees.withColumn("bonus", lit(None).cast(DoubleType()))

# b) Add column with default value
df = employees.withColumn("status", lit("active"))
df = employees.withColumn("bonus_pct", lit(0.10))

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
df = spark.read.schema(new_schema).csv("old_data.csv")
df = df.withColumn("department", coalesce(col("department"), lit("Unknown")))

# d) Merge schemas from multiple files
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("../datasets/parquet/employees_partitioned")


# =============================================================================
# Problem 3: Removing Columns
# =============================================================================
# a) Drop column
df = employees.drop("manager_id")
df = employees.drop("manager_id", "hire_date")  # Multiple columns

# b) Handle new data with fewer columns
# Just select columns that exist
required_cols = ["emp_id", "name", "salary"]
df = employees.select([c for c in required_cols if c in employees.columns])

# c) Selective reading (column pruning)
df = spark.read.parquet("../datasets/parquet/employees").select("name", "salary")

# d) Schema pruning happens automatically with columnar formats
df.explain()  # Check ReadSchema shows only selected columns


# =============================================================================
# Problem 4: Renaming Columns
# =============================================================================
# a) Single column rename
df = employees.withColumnRenamed("emp_id", "employee_id")

# b) Multiple columns
rename_map = {
    "emp_id": "employee_id",
    "name": "full_name",
    "salary": "annual_salary"
}
df = employees
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)

# Or using select with alias
df = employees.select([col(c).alias(rename_map.get(c, c)) for c in employees.columns])

# c) Backward compatibility - keep both columns temporarily
df = employees.withColumn("employee_id", col("emp_id"))

# d) Column alias in select
df = employees.select(
    col("emp_id").alias("employee_id"),
    col("name").alias("employee_name"),
    "salary"
)


# =============================================================================
# Problem 5: Type Changes
# =============================================================================
# a) Widen numeric types
df = employees.withColumn("emp_id", col("emp_id").cast(LongType()))
df = employees.withColumn("salary", col("salary").cast(DoubleType()))

# b) String to other types
df = employees.withColumn("salary_int", col("salary").cast(IntegerType()))
df = employees.withColumn("hire_date_ts", col("hire_date").cast(TimestampType()))

# c) Handle incompatible changes with safe parsing
from pyspark.sql.functions import to_date, to_timestamp

df = employees.withColumn(
    "hire_date_parsed",
    to_date(col("hire_date"), "yyyy-MM-dd")
)

# d) Safe casting with error handling
df = employees.withColumn(
    "salary_safe",
    when(col("salary").cast(DoubleType()).isNotNull(), col("salary").cast(DoubleType()))
    .otherwise(lit(0.0))
)


# =============================================================================
# Problem 6: Nested Schema Evolution
# =============================================================================
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

# a) Add field to nested struct
from pyspark.sql.functions import struct

df = nested_df.withColumn(
    "info",
    struct(
        col("info.name"),
        col("info.address"),
        lit("new_field").alias("new_field")
    )
)

# b) Remove field from nested - recreate struct without field
df = nested_df.withColumn(
    "info",
    struct(
        col("info.name")
        # Omit address to remove it
    )
)

# c) Rename nested field
df = nested_df.withColumn(
    "info",
    struct(
        col("info.name").alias("full_name"),
        col("info.address")
    )
)

# d) Array element schema changes
array_schema = StructType([
    StructField("id", IntegerType()),
    StructField("items", ArrayType(StructType([
        StructField("name", StringType()),
        StructField("value", IntegerType())
    ])))
])

# Transform array elements
from pyspark.sql.functions import transform

# df = df.withColumn("items", transform("items", lambda x: struct(x.name, x.value.cast("double"))))


# =============================================================================
# Problem 7: Parquet Schema Evolution
# =============================================================================
# a) Enable mergeSchema
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("path/to/parquet/directory")

# b) Column type promotion (compatible widening)
# int -> long, float -> double

# c) Schema evolution with partitioned data
# Each partition can have different schema
# mergeSchema combines them

# d) Parquet compatibility rules:
# - Can add nullable columns
# - Can widen numeric types
# - Cannot change field names
# - Cannot change incompatible types


# =============================================================================
# Problem 8: Delta Lake Schema Evolution
# =============================================================================
# a) autoMerge
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or per-write
# df.write.option("mergeSchema", "true").format("delta").mode("append").save(path)

# b) overwriteSchema (breaking changes)
# df.write.option("overwriteSchema", "true").format("delta").mode("overwrite").save(path)

# c) Schema enforcement (default behavior)
# Delta will fail if schema doesn't match

# d) Column mapping modes (Delta 2.0+)
spark.sql("""
    ALTER TABLE delta_table SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name'
    )
""")
# Modes: none, name, id


# =============================================================================
# Problem 9: Iceberg Schema Evolution
# =============================================================================
# a) Add/drop/rename in Iceberg
# ALTER TABLE t ADD COLUMNS (new_col STRING)
# ALTER TABLE t DROP COLUMN old_col
# ALTER TABLE t RENAME COLUMN old_name TO new_name

# b) Type promotion
# ALTER TABLE t ALTER COLUMN int_col TYPE BIGINT

# c) Nested type evolution
# ALTER TABLE t ADD COLUMNS (nested.new_field STRING)

# d) Works seamlessly across partitions


# =============================================================================
# Problem 10: Avro Schema Evolution
# =============================================================================
# a) Read Avro with evolved schema
df = spark.read \
    .format("avro") \
    .option("avroSchema", evolved_schema_json) \
    .load("path/to/avro")

# b) Schema registry integration
# Configure with spark.sql.avro.schemaRegistry.url

# c) Compatibility modes:
# - BACKWARD: New schema can read old data
# - FORWARD: Old schema can read new data
# - FULL: Both directions

# d) Default values in Avro schema
avro_schema = """
{
  "type": "record",
  "name": "Employee",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "department", "type": "string", "default": "Unknown"}
  ]
}
"""


# =============================================================================
# Problem 11: Schema Validation
# =============================================================================
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

# b) Validate compatibility
def is_compatible(old_schema, new_schema):
    """Check if new schema is backward compatible"""
    old_fields = {f.name: f for f in old_schema.fields}
    new_fields = {f.name: f for f in new_schema.fields}

    # All old fields must exist in new schema
    for name, old_field in old_fields.items():
        if name not in new_fields:
            return False, f"Missing field: {name}"
        new_field = new_fields[name]
        if old_field.dataType != new_field.dataType:
            # Allow widening
            if not is_type_compatible(old_field.dataType, new_field.dataType):
                return False, f"Incompatible type change for {name}"
    return True, "Compatible"

# c) Schema diff
def schema_diff(old_schema, new_schema):
    old_fields = {f.name for f in old_schema.fields}
    new_fields = {f.name for f in new_schema.fields}

    added = new_fields - old_fields
    removed = old_fields - new_fields

    return {"added": added, "removed": removed}

# d) Automated validation
def validate_data_schema(df, expected_schema):
    actual = df.schema
    if actual != expected_schema:
        raise ValueError(f"Schema mismatch!\nExpected: {expected_schema}\nActual: {actual}")


# =============================================================================
# Problem 12: Handling Schema Drift
# =============================================================================
# a) Detect drift
def detect_drift(current_df, reference_schema):
    current_fields = set(f.name for f in current_df.schema.fields)
    reference_fields = set(f.name for f in reference_schema.fields)

    new_fields = current_fields - reference_fields
    missing_fields = reference_fields - current_fields

    if new_fields or missing_fields:
        return True, {"new": new_fields, "missing": missing_fields}
    return False, {}

# b) Alert on changes
def process_with_drift_detection(df, expected_schema, on_drift="warn"):
    has_drift, drift_info = detect_drift(df, expected_schema)
    if has_drift:
        if on_drift == "fail":
            raise ValueError(f"Schema drift detected: {drift_info}")
        elif on_drift == "warn":
            print(f"WARNING: Schema drift detected: {drift_info}")
    return df

# c) Graceful handling
def handle_new_columns(df, expected_schema):
    expected_cols = [f.name for f in expected_schema.fields]

    # Add missing expected columns with nulls
    for field in expected_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    # Select only expected columns
    return df.select(expected_cols)

# d) Quarantine
def quarantine_schema_errors(df, expected_schema, quarantine_path):
    try:
        # Validate and process
        validated = df.select([col(f.name).cast(f.dataType) for f in expected_schema.fields])
        return validated, None
    except Exception as e:
        # Write to quarantine
        df.write.mode("append").parquet(quarantine_path)
        return None, str(e)


# =============================================================================
# Problem 13: JSON Schema Evolution
# =============================================================================
# a) Handle varying structures
json_df = spark.read.option("multiLine", True).json("../datasets/json/orders.json")

# b) Flatten with evolution
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

# c) Null vs missing
# Spark treats both as null by default
# To distinguish, use schema with specific nullable settings

# d) Schema inference
sample_json = '{"id": 1, "name": "test", "values": [1, 2, 3]}'
inferred_schema = schema_of_json(lit(sample_json))


# =============================================================================
# Problem 14: Streaming Schema Evolution
# =============================================================================
# a) Schema changes in streaming - restart required
# Define schema explicitly for stability

stream_schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType()),
    StructField("timestamp", TimestampType())
])

# stream = spark.readStream.schema(stream_schema).json("path")

# b) Handle new columns - use schema hints
# New columns appear as null

# c) Schema hints
# spark.readStream.option("schemaHints", "new_col STRING").json("path")

# d) Restart strategy
# For breaking changes:
# 1. Stop the stream
# 2. Clear checkpoint (if schema changed significantly)
# 3. Restart with new schema


# =============================================================================
# Problem 15: Best Practices
# =============================================================================
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

# 2. Compatibility rules
# - Only add nullable columns
# - Never remove required columns
# - Only widen types (int -> long)
# - Avoid renames (or use column mapping)

# 3. Migration procedures
def migrate_v1_to_v2(df):
    """Migrate data from schema v1 to v2"""
    return df.withColumn("email", lit(None).cast(StringType()))

# 4. Backward/forward compatibility
# - Backward: New code reads old data (add defaults)
# - Forward: Old code reads new data (ignore new fields)

# 5. Document changes
schema_changelog = """
## Version 2.0.0 (2024-01-15)
- Added: email (STRING, nullable)
- Deprecated: None
- Breaking: None

## Version 1.0.0 (2023-01-01)
- Initial schema: id, name
"""
