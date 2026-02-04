"""
Transformations - Solutions
============================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, concat_ws, upper, lower, trim, initcap,
    regexp_replace, substring, length, split, explode, array, struct,
    to_date, date_format, year, month, dayofmonth, expr,
    coalesce, greatest, least
)
from pyspark.sql.types import IntegerType, DoubleType, StringType, ArrayType

spark = SparkSession.builder.appName("Transformations Solutions").getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Column Expressions
# =============================================================================
# a) Concatenate name, department, city
df = employees.withColumn(
    "full_info",
    concat_ws(" - ", col("name"), col("department"), col("city"))
)
df.select("name", "full_info").show(truncate=False)

# b) Salary level based on conditions
df = employees.withColumn(
    "salary_level",
    when(col("salary") > 100000, "High")
    .when(col("salary") > 70000, "Medium")
    .otherwise("Low")
)
df.select("name", "salary", "salary_level").show()

# c) Convert name to uppercase
df = employees.withColumn("name_upper", upper(col("name")))
df.select("name", "name_upper").show()


# =============================================================================
# Problem 2: Type Casting
# =============================================================================
# a) Cast salary to double
df = employees.withColumn("salary_double", col("salary").cast(DoubleType()))
df.printSchema()

# b) Cast hire_date string to date
df = employees.withColumn("hire_date_parsed", to_date(col("hire_date"), "yyyy-MM-dd"))
df.select("hire_date", "hire_date_parsed").show()

# c) String representation with currency
df = employees.withColumn(
    "salary_formatted",
    concat(lit("$"), col("salary").cast(StringType()))
)
df.select("name", "salary_formatted").show()


# =============================================================================
# Problem 3: String Functions
# =============================================================================
# a) First 3 characters
df = employees.withColumn("name_prefix", substring(col("name"), 1, 3))

# b) Length of name
df = employees.withColumn("name_length", length(col("name")))

# c) Replace Engineering with Tech
df = employees.withColumn(
    "department_new",
    regexp_replace(col("department"), "Engineering", "Tech")
)

# d) Trim and title case
df = employees.withColumn("name_clean", initcap(trim(col("name"))))

df.select("name", "name_prefix", "name_length", "department", "department_new").show()


# =============================================================================
# Problem 4: Conditional Logic (when/otherwise)
# =============================================================================
# a) is_manager = true if manager_id is null
df = employees.withColumn(
    "is_manager",
    when(col("manager_id").isNull(), True).otherwise(False)
)

# b) Department code
df = df.withColumn(
    "department_code",
    when(col("department") == "Engineering", "ENG")
    .when(col("department") == "Sales", "SLS")
    .when(col("department") == "HR", "HR")
    .when(col("department") == "Marketing", "MKT")
    .otherwise("OTH")
)

# c) Salary band
df = df.withColumn(
    "salary_band",
    when(col("salary") < 70000, "Band1")
    .when(col("salary") < 90000, "Band2")
    .when(col("salary") < 110000, "Band3")
    .otherwise("Band4")
)

# d) Bonus calculation
df = df.withColumn(
    "bonus",
    when(col("department") == "Engineering", col("salary") * 0.20)
    .when(col("department") == "Sales", col("salary") * 0.15)
    .otherwise(col("salary") * 0.10)
)

df.select("name", "department", "salary", "is_manager", "department_code", "salary_band", "bonus").show()


# =============================================================================
# Problem 5: Working with Arrays
# =============================================================================
# a) Create array from columns
df = employees.withColumn(
    "location_dept_array",
    array(col("city"), col("department"))
)
df.select("name", "location_dept_array").show(truncate=False)

# b) Explode array into rows
df_exploded = df.select("name", explode(col("location_dept_array")).alias("value"))
df_exploded.show()

# c) Check if array contains value
from pyspark.sql.functions import array_contains

df = employees.withColumn(
    "info_array",
    array(col("city"), col("department"))
).withColumn(
    "is_in_ny",
    array_contains(col("info_array"), "New York")
)
df.select("name", "info_array", "is_in_ny").show(truncate=False)


# =============================================================================
# Problem 6: Working with Structs
# =============================================================================
# a) Create struct column
df = employees.withColumn(
    "employee_info",
    struct(
        col("name").alias("emp_name"),
        col("department").alias("dept"),
        col("salary").alias("sal")
    )
)
df.select("emp_id", "employee_info").show(truncate=False)

# b) Access nested struct fields
df.select(
    col("employee_info.emp_name"),
    col("employee_info.dept"),
    col("employee_info.sal")
).show()

# c) Flatten struct - using select with struct fields
df_flat = df.select(
    "emp_id",
    col("employee_info.emp_name").alias("name"),
    col("employee_info.dept").alias("department"),
    col("employee_info.sal").alias("salary")
)
df_flat.show()


# =============================================================================
# Problem 7: Multiple Column Operations
# =============================================================================
# a) Apply same transformation to multiple columns
from pyspark.sql.functions import col

# Convert multiple string columns to uppercase
string_cols = ["name", "department", "city"]
df = employees
for c in string_cols:
    df = df.withColumn(c + "_upper", upper(col(c)))
df.show()

# b) Rename multiple columns using mapping
rename_map = {"emp_id": "employee_id", "name": "employee_name", "salary": "annual_salary"}
df = employees
for old_name, new_name in rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)
df.show()

# c) Select columns matching pattern (e.g., all columns ending with '_id')
id_cols = [c for c in employees.columns if c.endswith("_id")]
employees.select(id_cols).show()


# =============================================================================
# Problem 8: Row-level Transformations (Sales)
# =============================================================================
# a) Total amount
df = sales.withColumn("total_amount", col("quantity") * col("unit_price"))

# b) Discount based on quantity
df = df.withColumn(
    "discount_pct",
    when(col("quantity") > 3, 0.10)
    .when(col("quantity") > 1, 0.05)
    .otherwise(0.0)
)

# c) Final amount after discount
df = df.withColumn(
    "final_amount",
    col("total_amount") * (1 - col("discount_pct"))
)

# d) Extract month from transaction_date
df = df.withColumn(
    "transaction_month",
    month(to_date(col("transaction_date")))
)

df.select("transaction_id", "quantity", "total_amount", "discount_pct", "final_amount", "transaction_month").show()


# =============================================================================
# Problem 9: Complex Transformations
# =============================================================================
# a) Pivot by payment_method
sales_with_amount = sales.withColumn("total", col("quantity") * col("unit_price"))
pivot_df = sales_with_amount.groupBy("store_id").pivot("payment_method").sum("quantity")
pivot_df.show()

# b) Unpivot/Melt - using stack
# Convert wide format to long format
unpivot_expr = "stack(3, 'Cash', Cash, 'Credit Card', `Credit Card`, 'Debit Card', `Debit Card`) as (payment_method, quantity)"
unpivot_df = pivot_df.select("store_id", expr(unpivot_expr))
unpivot_df.show()

# c) Running total using window
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

window_spec = Window.orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = sales.withColumn("total", col("quantity") * col("unit_price"))
df = df.withColumn("running_total", spark_sum("total").over(window_spec))
df.select("transaction_id", "transaction_date", "total", "running_total").show()


# =============================================================================
# Problem 10: Chaining Transformations
# =============================================================================
result = (
    employees
    .filter(col("department").isin("Engineering", "Sales"))
    .withColumn("salary_with_bonus", col("salary") * 1.10)
    .select("name", "department", "salary", "salary_with_bonus")
    .orderBy(col("salary_with_bonus").desc())
    .limit(5)
)
result.show()
