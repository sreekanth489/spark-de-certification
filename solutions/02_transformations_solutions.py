# Databricks notebook source
# MAGIC %md
# MAGIC # Transformations - Solutions
# MAGIC Topics: Column expressions, type casting, string functions, conditional logic, arrays, structs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, concat, concat_ws, upper, lower, trim, initcap,
    regexp_replace, substring, length, split, explode, array, struct,
    to_date, date_format, year, month, dayofmonth, expr,
    coalesce, greatest, least, array_contains, transform
)
from pyspark.sql.types import IntegerType, DoubleType, StringType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

# Load sample data - adjust paths for your environment
employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Column Expressions

# COMMAND ----------

# a) Concatenate name, department, city
df = employees.withColumn(
    "full_info",
    concat_ws(" - ", col("name"), col("department"), col("city"))
)
display(df.select("name", "full_info"))

# COMMAND ----------

# b) Salary level based on conditions
df = employees.withColumn(
    "salary_level",
    when(col("salary") > 100000, "High")
    .when(col("salary") > 70000, "Medium")
    .otherwise("Low")
)
display(df.select("name", "salary", "salary_level"))

# COMMAND ----------

# c) Convert name to uppercase
df = employees.withColumn("name_upper", upper(col("name")))
display(df.select("name", "name_upper"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Type Casting

# COMMAND ----------

# a) Cast salary to double
df = employees.withColumn("salary_double", col("salary").cast(DoubleType()))
df.printSchema()

# COMMAND ----------

# b) Cast hire_date string to date
df = employees.withColumn("hire_date_parsed", to_date(col("hire_date"), "yyyy-MM-dd"))
display(df.select("hire_date", "hire_date_parsed"))

# COMMAND ----------

# c) String representation with currency
df = employees.withColumn(
    "salary_formatted",
    concat(lit("$"), col("salary").cast(StringType()))
)
display(df.select("name", "salary_formatted"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: String Functions

# COMMAND ----------

# a) First 3 characters
df = employees.withColumn("name_prefix", substring(col("name"), 1, 3))

# b) Length of name
df = df.withColumn("name_length", length(col("name")))

# c) Replace Engineering with Tech
df = df.withColumn(
    "department_new",
    regexp_replace(col("department"), "Engineering", "Tech")
)

# d) Trim and title case
df = df.withColumn("name_clean", initcap(trim(col("name"))))

display(df.select("name", "name_prefix", "name_length", "department", "department_new"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Conditional Logic (when/otherwise)

# COMMAND ----------

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

display(df.select("name", "department", "is_manager", "department_code"))

# COMMAND ----------

# c) Salary band
df = employees.withColumn(
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

display(df.select("name", "department", "salary", "salary_band", "bonus"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Working with Arrays

# COMMAND ----------

# a) Create array from columns
df = employees.withColumn(
    "location_dept_array",
    array(col("city"), col("department"))
)
display(df.select("name", "location_dept_array"))

# COMMAND ----------

# b) Explode array into rows
df_exploded = df.select("name", explode(col("location_dept_array")).alias("value"))
display(df_exploded)

# COMMAND ----------

# c) Check if array contains value
df = employees.withColumn(
    "info_array",
    array(col("city"), col("department"))
).withColumn(
    "is_in_ny",
    array_contains(col("info_array"), "New York")
)
display(df.select("name", "info_array", "is_in_ny"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Working with Structs

# COMMAND ----------

# a) Create struct column
df = employees.withColumn(
    "employee_info",
    struct(
        col("name").alias("emp_name"),
        col("department").alias("dept"),
        col("salary").alias("sal")
    )
)
display(df.select("emp_id", "employee_info"))

# COMMAND ----------

# b) Access nested struct fields
display(df.select(
    col("employee_info.emp_name"),
    col("employee_info.dept"),
    col("employee_info.sal")
))

# COMMAND ----------

# c) Flatten struct - using select with struct fields
df_flat = df.select(
    "emp_id",
    col("employee_info.emp_name").alias("name"),
    col("employee_info.dept").alias("department"),
    col("employee_info.sal").alias("salary")
)
display(df_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Multiple Column Operations

# COMMAND ----------

# a) Apply same transformation to multiple columns
string_cols = ["name", "department", "city"]
df = employees
for c in string_cols:
    df = df.withColumn(c + "_upper", upper(col(c)))
display(df)

# COMMAND ----------

# b) Rename multiple columns using mapping
rename_map = {"emp_id": "employee_id", "name": "employee_name", "salary": "annual_salary"}
df = employees
for old_name, new_name in rename_map.items():
    df = df.withColumnRenamed(old_name, new_name)
display(df)

# COMMAND ----------

# c) Select columns matching pattern (e.g., all columns ending with '_id')
id_cols = [c for c in employees.columns if c.endswith("_id")]
display(employees.select(id_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Row-level Transformations (Sales)

# COMMAND ----------

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

display(df.select("transaction_id", "quantity", "total_amount", "discount_pct", "final_amount", "transaction_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Complex Transformations

# COMMAND ----------

# a) Pivot by payment_method
sales_with_amount = sales.withColumn("total", col("quantity") * col("unit_price"))
pivot_df = sales_with_amount.groupBy("store_id").pivot("payment_method").sum("quantity")
display(pivot_df)

# COMMAND ----------

# b) Unpivot/Melt - using stack
unpivot_expr = "stack(3, 'Cash', Cash, 'Credit Card', `Credit Card`, 'Debit Card', `Debit Card`) as (payment_method, quantity)"
unpivot_df = pivot_df.select("store_id", expr(unpivot_expr))
display(unpivot_df)

# COMMAND ----------

# c) Running total using window
window_spec = Window.orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = sales.withColumn("total", col("quantity") * col("unit_price"))
df = df.withColumn("running_total", spark_sum("total").over(window_spec))
display(df.select("transaction_id", "transaction_date", "total", "running_total"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Chaining Transformations

# COMMAND ----------

result = (
    employees
    .filter(col("department").isin("Engineering", "Sales"))
    .withColumn("salary_with_bonus", col("salary") * 1.10)
    .select("name", "department", "salary", "salary_with_bonus")
    .orderBy(col("salary_with_bonus").desc())
    .limit(5)
)
display(result)
