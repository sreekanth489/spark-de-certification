"""
Joins - Practice Problems
==========================
Topics: inner, left, right, full outer, cross, semi, anti joins, broadcast
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("Joins").getOrCreate()

# Load sample data
employees = spark.read.csv("../../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../../datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("../../datasets/csv/products.csv", header=True, inferSchema=True)
customers = spark.read.csv("../../datasets/csv/customers.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Inner Join
# =============================================================================
# a) Join sales with products on product_id
# b) Show transaction_id, product_name, quantity, unit_price
# c) Calculate total amount for each transaction

# YOUR CODE HERE


# =============================================================================
# Problem 2: Left Outer Join
# =============================================================================
# a) Left join products with sales (some products may not have sales)
# b) Find products that have never been sold
# c) Count sales per product, showing 0 for unsold products

# YOUR CODE HERE


# =============================================================================
# Problem 3: Right Outer Join
# =============================================================================
# a) Right join sales with customers
# b) Find all customer orders including those without sales
# c) Show customer name with their transaction details

# YOUR CODE HERE


# =============================================================================
# Problem 4: Full Outer Join
# =============================================================================
# Create scenario for full outer join:
# a) Join two datasets where some keys exist only in one side
# b) Identify records only in left, only in right, and in both

# YOUR CODE HERE


# =============================================================================
# Problem 5: Cross Join
# =============================================================================
# a) Cross join products with a list of discount rates [0.1, 0.15, 0.2]
# b) Calculate discounted prices for all combinations
# c) Find products where any discount makes price < cost

# YOUR CODE HERE


# =============================================================================
# Problem 6: Left Semi Join
# =============================================================================
# a) Find all products that have at least one sale (semi join)
# b) Find employees who are managers (have someone reporting to them)
# c) Find customers who have placed orders

# YOUR CODE HERE


# =============================================================================
# Problem 7: Left Anti Join
# =============================================================================
# a) Find products with no sales
# b) Find employees who are not managers
# c) Find customers who have never ordered

# YOUR CODE HERE


# =============================================================================
# Problem 8: Self Join
# =============================================================================
# a) Join employees with itself to get employee-manager pairs
# b) Show employee name with their manager's name
# c) Find employees who earn more than their manager

# YOUR CODE HERE


# =============================================================================
# Problem 9: Multiple Joins
# =============================================================================
# Create a denormalized view by joining:
# - Sales with Products (product details)
# - Sales with Customers (customer details)
# Show: transaction_date, customer_name, product_name, quantity, total_amount

# YOUR CODE HERE


# =============================================================================
# Problem 10: Broadcast Join
# =============================================================================
# a) Use broadcast hint to optimize join with small dimension table
# b) Broadcast products table when joining with sales
# c) Verify broadcast is being used in explain plan

# YOUR CODE HERE


# =============================================================================
# Problem 11: Join Conditions
# =============================================================================
# a) Join on multiple columns
# b) Join with inequality conditions (range join)
# c) Join with complex expression

# YOUR CODE HERE


# =============================================================================
# Problem 12: Handling Duplicate Column Names
# =============================================================================
# a) Join two DataFrames that have columns with same name
# b) Properly alias columns to avoid ambiguity
# c) Drop duplicate columns after join

# YOUR CODE HERE
