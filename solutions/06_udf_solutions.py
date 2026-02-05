# Databricks notebook source
# MAGIC %md
# MAGIC # User Defined Functions (UDFs) - Solutions
# MAGIC Topics: Python UDFs, Pandas UDFs, SQL registration, performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import udf, pandas_udf, col, lit, struct
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, ArrayType,
    StructType, StructField, MapType, BooleanType
)
import pandas as pd

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Basic Python UDF

# COMMAND ----------

# a) Tax calculation UDF
def calculate_tax(salary):
    if salary is None:
        return None
    return salary * 0.30

tax_udf = udf(calculate_tax, DoubleType())
display(employees.withColumn("tax", tax_udf(col("salary"))))

# COMMAND ----------

# b) Salary category UDF
def categorize_salary(salary):
    if salary is None:
        return "Unknown"
    elif salary < 70000:
        return "Low"
    elif salary <= 100000:
        return "Medium"
    else:
        return "High"

salary_category_udf = udf(categorize_salary, StringType())
display(employees.withColumn("salary_category", salary_category_udf(col("salary"))))

# COMMAND ----------

# c) Name formatting UDF
def format_name(name):
    if name is None:
        return None
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

format_name_udf = udf(format_name, StringType())
display(employees.withColumn("formatted_name", format_name_udf(col("name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: UDF with Multiple Arguments

# COMMAND ----------

# a) Bonus based on salary and department
def calculate_bonus(salary, department):
    if salary is None or department is None:
        return 0.0
    rates = {"Engineering": 0.20, "Sales": 0.15, "HR": 0.10, "Marketing": 0.12}
    rate = rates.get(department, 0.05)
    return salary * rate

bonus_udf = udf(calculate_bonus, DoubleType())
display(employees.withColumn("bonus", bonus_udf(col("salary"), col("department"))))

# COMMAND ----------

# b) Full address UDF
def format_address(city, state="NY", country="USA"):
    parts = [p for p in [city, state, country] if p]
    return ", ".join(parts)

address_udf = udf(format_address, StringType())
display(employees.withColumn("address", address_udf(col("city"), lit("NY"), lit("USA"))))

# COMMAND ----------

# c) Custom formatting
def custom_format(name, salary, dept):
    return f"{name} ({dept}): ${salary:,.0f}" if all([name, salary, dept]) else "N/A"

format_udf = udf(custom_format, StringType())
display(employees.withColumn("summary", format_udf(col("name"), col("salary"), col("department"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: UDF Returning Complex Types

# COMMAND ----------

# a) Return array of name parts
def split_name(name):
    if name is None:
        return []
    return name.split()

split_name_udf = udf(split_name, ArrayType(StringType()))
display(employees.withColumn("name_parts", split_name_udf(col("name"))))

# COMMAND ----------

# b) Return struct with computed fields
employee_struct_schema = StructType([
    StructField("tax", DoubleType(), True),
    StructField("net_salary", DoubleType(), True),
    StructField("category", StringType(), True)
])

def compute_employee_info(salary):
    if salary is None:
        return (None, None, None)
    tax = salary * 0.30
    net = salary - tax
    category = "High" if salary > 100000 else "Medium" if salary > 70000 else "Low"
    return (tax, net, category)

employee_info_udf = udf(compute_employee_info, employee_struct_schema)
display(employees.withColumn("emp_info", employee_info_udf(col("salary"))).select(
    "name", "salary", "emp_info.*"
))

# COMMAND ----------

# c) Return map/dictionary
def create_salary_breakdown(salary):
    if salary is None:
        return {}
    return {
        "gross": float(salary),
        "tax": float(salary * 0.30),
        "net": float(salary * 0.70),
        "monthly": float(salary / 12)
    }

breakdown_udf = udf(create_salary_breakdown, MapType(StringType(), DoubleType()))
display(employees.withColumn("breakdown", breakdown_udf(col("salary"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Registering UDFs for SQL

# COMMAND ----------

# a) Register function for SQL
def salary_grade(salary):
    if salary is None:
        return "Unknown"
    if salary >= 100000:
        return "A"
    elif salary >= 80000:
        return "B"
    elif salary >= 60000:
        return "C"
    else:
        return "D"

spark.udf.register("salary_grade", salary_grade, StringType())

# COMMAND ----------

# b) Use in SQL query
employees.createOrReplaceTempView("employees")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, salary, salary_grade(salary) as grade
# MAGIC FROM employees
# MAGIC ORDER BY salary DESC

# COMMAND ----------

# c) Register with explicit return type
spark.udf.register("double_salary", lambda x: x * 2 if x else None, DoubleType())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, salary, double_salary(salary) as doubled FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Pandas UDF - Scalar

# COMMAND ----------

# a) Scalar Pandas UDF
@pandas_udf(DoubleType())
def pandas_tax_calc(salary: pd.Series) -> pd.Series:
    return salary * 0.30

display(employees.withColumn("tax_pandas", pandas_tax_calc(col("salary"))))

# COMMAND ----------

# c) Pandas string methods
@pandas_udf(StringType())
def pandas_upper(name: pd.Series) -> pd.Series:
    return name.str.upper()

display(employees.withColumn("name_upper", pandas_upper(col("name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Pandas UDF - Grouped Map

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

# a) Normalize salaries within department
schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("department", StringType()),
    StructField("salary", DoubleType()),
    StructField("normalized_salary", DoubleType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def normalize_salary(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    mean_sal = pdf["salary"].mean()
    std_sal = pdf["salary"].std()
    if std_sal > 0:
        pdf["normalized_salary"] = (pdf["salary"] - mean_sal) / std_sal
    else:
        pdf["normalized_salary"] = 0.0
    return pdf[["emp_id", "name", "department", "salary", "normalized_salary"]]

display(employees.select("emp_id", "name", "department", "salary").groupBy("department").apply(
    normalize_salary
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Pandas UDF - Grouped Aggregate

# COMMAND ----------

# Custom aggregation - salary range
@pandas_udf(DoubleType())
def salary_range(salary: pd.Series) -> float:
    return float(salary.max() - salary.min())

display(employees.groupBy("department").agg(
    salary_range(col("salary")).alias("salary_range")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Error Handling in UDFs

# COMMAND ----------

# a) Handle null values
def safe_divide(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b

safe_divide_udf = udf(safe_divide, DoubleType())

# b) Handle exceptions gracefully
def safe_parse_int(s):
    try:
        return int(s) if s else None
    except (ValueError, TypeError):
        return None

safe_parse_udf = udf(safe_parse_int, IntegerType())

# c) Return default on error
def safe_operation(value, default=-1):
    try:
        if value is None:
            return default
        return int(value * 2)
    except Exception:
        return default

safe_op_udf = udf(safe_operation, IntegerType())

print("Error handling UDFs defined successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: UDF Performance Optimization

# COMMAND ----------

# a) Broadcast variable in UDF
dept_multipliers = {"Engineering": 1.2, "Sales": 1.15, "HR": 1.1, "Marketing": 1.12}
bc_multipliers = spark.sparkContext.broadcast(dept_multipliers)

def apply_multiplier(salary, dept):
    if salary is None or dept is None:
        return None
    multiplier = bc_multipliers.value.get(dept, 1.0)
    return salary * multiplier

multiplier_udf = udf(apply_multiplier, DoubleType())
display(employees.withColumn("adjusted_salary", multiplier_udf(col("salary"), col("department"))))

# COMMAND ----------

# b) Prefer built-in functions over UDFs
from pyspark.sql.functions import upper

# GOOD: Use built-in function (vectorized, optimized)
display(employees.withColumn("name_upper", upper(col("name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Advanced UDF Scenarios

# COMMAND ----------

# a) UDF using external library (regex)
import re

def extract_initials(name):
    if name is None:
        return None
    words = re.findall(r'\b\w', name)
    return ''.join(words).upper()

initials_udf = udf(extract_initials, StringType())
display(employees.withColumn("initials", initials_udf(col("name"))))

# COMMAND ----------

# b) UDF with closure
threshold = 80000

def above_threshold(salary, threshold=threshold):
    return salary > threshold if salary else False

above_threshold_udf = udf(above_threshold, BooleanType())
display(employees.withColumn("above_80k", above_threshold_udf(col("salary"))))

# COMMAND ----------

# c) Chaining UDFs
display(employees.withColumn(
    "formatted", format_name_udf(col("name"))
).withColumn(
    "category", salary_category_udf(col("salary"))
).withColumn(
    "tax", tax_udf(col("salary"))
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: Type Hints with Pandas UDFs

# COMMAND ----------

# Modern Pandas UDF syntax with type hints
@pandas_udf("double")
def modern_tax_calc(s: pd.Series) -> pd.Series:
    """Calculate tax using modern pandas_udf syntax."""
    return s * 0.30

display(employees.withColumn("tax", modern_tax_calc("salary")))

# COMMAND ----------

# Iterator of Series for better memory efficiency
from typing import Iterator

@pandas_udf("string")
def process_names(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in iterator:
        yield batch.str.upper()

display(employees.withColumn("name_upper", process_names("name")))
