"""
DataFrame Basics - Practice Problems
=====================================
Topics: Creating DataFrames, Schema, Basic Operations, Reading/Writing Data
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder.appName("DataFrame Basics").getOrCreate()

# =============================================================================
# Problem 1: Create DataFrame from List
# =============================================================================
# Create a DataFrame from the following data with explicit schema
# Data: [("Alice", 30, "Engineering"), ("Bob", 25, "Sales"), ("Charlie", 35, "HR")]
# Columns: name (string), age (integer), department (string)

# YOUR CODE HERE


# =============================================================================
# Problem 2: Read CSV with Schema
# =============================================================================
# Read the employees.csv file with:
# - Header enabled
# - Infer schema disabled (define explicit schema)
# - Handle null values represented as empty strings

# YOUR CODE HERE


# =============================================================================
# Problem 3: DataFrame Info
# =============================================================================
# For the employees DataFrame:
# a) Print the schema
# b) Show first 5 rows
# c) Count total rows
# d) Get column names
# e) Get data types

# YOUR CODE HERE


# =============================================================================
# Problem 4: Select and Alias
# =============================================================================
# From employees DataFrame:
# a) Select only name and salary columns
# b) Rename 'name' to 'employee_name' and 'salary' to 'annual_salary'
# c) Add a new column 'monthly_salary' = salary / 12

# YOUR CODE HERE


# =============================================================================
# Problem 5: Filter Operations
# =============================================================================
# Filter employees where:
# a) Salary is greater than 80000
# b) Department is 'Engineering' AND salary > 90000
# c) City is either 'New York' OR 'San Francisco'
# d) Name starts with 'J'

# YOUR CODE HERE


# =============================================================================
# Problem 6: Distinct and Drop Duplicates
# =============================================================================
# a) Get distinct departments
# b) Get distinct combinations of department and city
# c) Drop duplicates based on department only (keep first)

# YOUR CODE HERE


# =============================================================================
# Problem 7: Sort and OrderBy
# =============================================================================
# a) Sort employees by salary descending
# b) Sort by department ascending, then salary descending
# c) Get top 3 highest paid employees

# YOUR CODE HERE


# =============================================================================
# Problem 8: Add, Rename, Drop Columns
# =============================================================================
# a) Add column 'tax' = salary * 0.3
# b) Add column 'years_employed' = years since hire_date
# c) Rename 'emp_id' to 'employee_id'
# d) Drop the 'manager_id' column

# YOUR CODE HERE


# =============================================================================
# Problem 9: Write DataFrame
# =============================================================================
# Write the employees DataFrame:
# a) As Parquet (partitioned by department)
# b) As CSV (with header, overwrite mode)
# c) As JSON (one file using coalesce)

# YOUR CODE HERE


# =============================================================================
# Problem 10: Null Handling
# =============================================================================
# a) Find rows where manager_id is null
# b) Fill null manager_id with 0
# c) Drop rows where any column is null
# d) Replace null in specific column with default value

# YOUR CODE HERE
