"""
Spark SQL - Practice Problems
==============================
Topics: SQL queries, temp views, catalog, databases, tables, CTEs
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

# Load sample data
employees = spark.read.csv("../../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../../datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("../../datasets/csv/products.csv", header=True, inferSchema=True)
customers = spark.read.csv("../../datasets/csv/customers.csv", header=True, inferSchema=True)

# Register as temporary views
employees.createOrReplaceTempView("employees")
sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")

# =============================================================================
# Problem 1: Basic SQL Queries
# =============================================================================
# Write SQL queries to:
# a) Select all employees with salary > 80000
# b) Count employees per department
# c) Find average salary by department
# d) Find top 5 highest paid employees

# YOUR CODE HERE


# =============================================================================
# Problem 2: SQL Joins
# =============================================================================
# a) Join sales with products to get product names
# b) Left join products with sales to find unsold products
# c) Multiple table join: sales + products + customers

# YOUR CODE HERE


# =============================================================================
# Problem 3: Subqueries
# =============================================================================
# a) Find employees earning above average salary (scalar subquery)
# b) Find departments with above-average headcount (correlated subquery)
# c) Use subquery in FROM clause (derived table)
# d) Use subquery in SELECT clause

# YOUR CODE HERE


# =============================================================================
# Problem 4: Common Table Expressions (CTEs)
# =============================================================================
# a) Use CTE to simplify complex query
# b) Multiple CTEs in one query
# c) Recursive-like pattern with CTEs

# YOUR CODE HERE


# =============================================================================
# Problem 5: Window Functions in SQL
# =============================================================================
# a) Rank employees by salary within department
# b) Calculate running total of sales
# c) Get previous and next values using LAG/LEAD
# d) Calculate percentile rank

# YOUR CODE HERE


# =============================================================================
# Problem 6: CASE Statements
# =============================================================================
# a) Categorize salaries using CASE WHEN
# b) Pivot-like transformation using CASE
# c) Nested CASE statements

# YOUR CODE HERE


# =============================================================================
# Problem 7: Set Operations
# =============================================================================
# a) UNION two result sets
# b) INTERSECT to find common values
# c) EXCEPT/MINUS to find differences
# d) UNION ALL vs UNION

# YOUR CODE HERE


# =============================================================================
# Problem 8: Date Functions in SQL
# =============================================================================
# a) Extract year, month, day from date
# b) Calculate date differences
# c) Add/subtract intervals
# d) Format dates

# YOUR CODE HERE


# =============================================================================
# Problem 9: String Functions in SQL
# =============================================================================
# a) CONCAT, SUBSTRING, LENGTH
# b) UPPER, LOWER, TRIM
# c) REGEXP functions
# d) SPLIT and array functions

# YOUR CODE HERE


# =============================================================================
# Problem 10: Database and Table Management
# =============================================================================
# a) Create database
# b) Create managed table
# c) Create external table
# d) DESCRIBE and SHOW commands
# e) ALTER and DROP operations

# YOUR CODE HERE


# =============================================================================
# Problem 11: Views
# =============================================================================
# a) Create temporary view
# b) Create global temporary view
# c) Create permanent view (if using catalog)
# d) Differences between view types

# YOUR CODE HERE


# =============================================================================
# Problem 12: Advanced SQL Features
# =============================================================================
# a) GROUPING SETS
# b) ROLLUP
# c) CUBE
# d) LATERAL VIEW with explode

# YOUR CODE HERE


# =============================================================================
# Problem 13: Query Optimization
# =============================================================================
# a) Use EXPLAIN to analyze query plan
# b) Identify and fix inefficient queries
# c) Use hints for optimization

# YOUR CODE HERE
