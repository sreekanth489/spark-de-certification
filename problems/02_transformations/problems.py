"""
Transformations - Practice Problems
====================================
Topics: map, flatMap, filter, select, withColumn, when/otherwise, cast
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, upper, lower, trim, regexp_replace
from pyspark.sql.functions import substring, length, split, explode, array, struct
from pyspark.sql.types import IntegerType, DoubleType, StringType

spark = SparkSession.builder.appName("Transformations").getOrCreate()

# Load sample data
employees = spark.read.csv("../../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../../datasets/csv/sales.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Column Expressions
# =============================================================================
# a) Create a 'full_info' column concatenating name, department, and city
# b) Create 'salary_level' column: 'High' if > 100000, 'Medium' if > 70000, else 'Low'
# c) Convert name to uppercase

# YOUR CODE HERE


# =============================================================================
# Problem 2: Type Casting
# =============================================================================
# a) Cast salary from long to double
# b) Cast hire_date string to date type
# c) Create a string representation of salary with currency symbol

# YOUR CODE HERE


# =============================================================================
# Problem 3: String Functions
# =============================================================================
# Using employees DataFrame:
# a) Extract first 3 characters of name
# b) Get length of each name
# c) Replace 'Engineering' with 'Tech' in department
# d) Trim whitespace and convert to title case

# YOUR CODE HERE


# =============================================================================
# Problem 4: Conditional Logic (when/otherwise)
# =============================================================================
# Create columns:
# a) 'is_manager' = true if manager_id is null
# b) 'department_code': ENG for Engineering, SLS for Sales, HR for HR, MKT for Marketing
# c) 'salary_band': Band1 (<70k), Band2 (70-90k), Band3 (90-110k), Band4 (>110k)
# d) 'bonus': 20% for Engineering, 15% for Sales, 10% for others

# YOUR CODE HERE


# =============================================================================
# Problem 5: Working with Arrays
# =============================================================================
# a) Create an array column from city and department
# b) Explode the array into separate rows
# c) Check if array contains specific value

# YOUR CODE HERE


# =============================================================================
# Problem 6: Working with Structs
# =============================================================================
# a) Create a struct column 'employee_info' containing name, department, salary
# b) Access nested struct fields
# c) Flatten struct to separate columns

# YOUR CODE HERE


# =============================================================================
# Problem 7: Multiple Column Operations
# =============================================================================
# a) Apply same transformation to multiple columns
# b) Rename multiple columns using a mapping
# c) Select columns matching a pattern

# YOUR CODE HERE


# =============================================================================
# Problem 8: Row-level Transformations
# =============================================================================
# Using sales DataFrame:
# a) Calculate 'total_amount' = quantity * unit_price
# b) Add 'discount' column: 10% if quantity > 3, 5% if quantity > 1, else 0
# c) Calculate 'final_amount' after discount
# d) Add 'transaction_month' extracted from transaction_date

# YOUR CODE HERE


# =============================================================================
# Problem 9: Complex Transformations
# =============================================================================
# a) Pivot transaction data by payment_method showing sum of quantities
# b) Unpivot/Melt a wide DataFrame to long format
# c) Create a running total of sales

# YOUR CODE HERE


# =============================================================================
# Problem 10: Chaining Transformations
# =============================================================================
# In a single chain:
# 1. Filter employees in Engineering or Sales
# 2. Add salary_with_bonus column (10% bonus)
# 3. Select name, department, salary, salary_with_bonus
# 4. Sort by salary_with_bonus descending
# 5. Limit to top 5

# YOUR CODE HERE
