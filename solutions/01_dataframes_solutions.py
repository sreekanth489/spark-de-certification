"""
DataFrame Basics - Solutions
=============================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, when, months_between, current_date

spark = SparkSession.builder.appName("DataFrame Solutions").getOrCreate()

# =============================================================================
# Problem 1: Create DataFrame from List
# =============================================================================
data = [("Alice", 30, "Engineering"), ("Bob", 25, "Sales"), ("Charlie", 35, "HR")]

# Method 1: With column names
df1 = spark.createDataFrame(data, ["name", "age", "department"])

# Method 2: With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True)
])
df1 = spark.createDataFrame(data, schema)
df1.show()


# =============================================================================
# Problem 2: Read CSV with Schema
# =============================================================================
emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("city", StringType(), True)
])

employees = spark.read.csv(
    "../datasets/csv/employees.csv",
    header=True,
    schema=emp_schema,
    nullValue=""
)


# =============================================================================
# Problem 3: DataFrame Info
# =============================================================================
# a) Print schema
employees.printSchema()

# b) Show first 5 rows
employees.show(5)

# c) Count rows
print(f"Total rows: {employees.count()}")

# d) Column names
print(f"Columns: {employees.columns}")

# e) Data types
print(f"Data types: {employees.dtypes}")


# =============================================================================
# Problem 4: Select and Alias
# =============================================================================
# a) Select columns
df = employees.select("name", "salary")

# b) Rename columns
df = employees.select(
    col("name").alias("employee_name"),
    col("salary").alias("annual_salary")
)

# c) Add computed column
df = employees.select(
    col("name"),
    col("salary"),
    (col("salary") / 12).alias("monthly_salary")
)
df.show()


# =============================================================================
# Problem 5: Filter Operations
# =============================================================================
# a) Salary > 80000
df = employees.filter(col("salary") > 80000)

# b) Engineering AND salary > 90000
df = employees.filter((col("department") == "Engineering") & (col("salary") > 90000))

# c) City is New York OR San Francisco
df = employees.filter(col("city").isin("New York", "San Francisco"))

# d) Name starts with 'J'
df = employees.filter(col("name").startswith("J"))
df.show()


# =============================================================================
# Problem 6: Distinct and Drop Duplicates
# =============================================================================
# a) Distinct departments
employees.select("department").distinct().show()

# b) Distinct department-city combinations
employees.select("department", "city").distinct().show()

# c) Drop duplicates on department
employees.dropDuplicates(["department"]).show()


# =============================================================================
# Problem 7: Sort and OrderBy
# =============================================================================
# a) Sort by salary descending
employees.orderBy(col("salary").desc()).show()

# b) Sort by department asc, salary desc
employees.orderBy(col("department").asc(), col("salary").desc()).show()

# c) Top 3 highest paid
employees.orderBy(col("salary").desc()).limit(3).show()


# =============================================================================
# Problem 8: Add, Rename, Drop Columns
# =============================================================================
# a) Add tax column
df = employees.withColumn("tax", col("salary") * 0.3)

# b) Add years employed (approximate)
df = employees.withColumn(
    "years_employed",
    (months_between(current_date(), col("hire_date")) / 12).cast("int")
)

# c) Rename column
df = employees.withColumnRenamed("emp_id", "employee_id")

# d) Drop column
df = employees.drop("manager_id")


# =============================================================================
# Problem 9: Write DataFrame
# =============================================================================
# a) Write as Parquet partitioned by department
employees.write.mode("overwrite").partitionBy("department").parquet("../output/employees_parquet")

# b) Write as CSV
employees.write.mode("overwrite").option("header", True).csv("../output/employees_csv")

# c) Write as JSON (single file)
employees.coalesce(1).write.mode("overwrite").json("../output/employees_json")


# =============================================================================
# Problem 10: Null Handling
# =============================================================================
# a) Find null manager_id
employees.filter(col("manager_id").isNull()).show()

# b) Fill null with 0
employees.fillna({"manager_id": 0}).show()

# c) Drop rows with any null
employees.dropna().show()

# d) Replace null in specific column
employees.fillna({"manager_id": -1, "city": "Unknown"}).show()
