# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL - Solutions
# MAGIC Topics: SQL queries, temp views, catalog, databases, tables, CTEs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Load and register tables
employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("datasets/csv/products.csv", header=True, inferSchema=True)
customers = spark.read.csv("datasets/csv/customers.csv", header=True, inferSchema=True)

employees.createOrReplaceTempView("employees")
sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Basic SQL Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Employees with salary > 80000
# MAGIC SELECT * FROM employees WHERE salary > 80000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Count per department
# MAGIC SELECT department, COUNT(*) as emp_count
# MAGIC FROM employees
# MAGIC GROUP BY department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Average salary by department
# MAGIC SELECT department, ROUND(AVG(salary), 2) as avg_salary
# MAGIC FROM employees
# MAGIC GROUP BY department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Top 5 highest paid
# MAGIC SELECT name, salary
# MAGIC FROM employees
# MAGIC ORDER BY salary DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: SQL Joins

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Sales with product names
# MAGIC SELECT s.transaction_id, p.product_name, s.quantity, s.unit_price
# MAGIC FROM sales s
# MAGIC JOIN products p ON s.product_id = p.product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Left join to find unsold products
# MAGIC SELECT p.product_id, p.product_name
# MAGIC FROM products p
# MAGIC LEFT JOIN sales s ON p.product_id = s.product_id
# MAGIC WHERE s.transaction_id IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Multiple table join
# MAGIC SELECT
# MAGIC     s.transaction_date,
# MAGIC     CONCAT(c.first_name, ' ', c.last_name) as customer_name,
# MAGIC     p.product_name,
# MAGIC     s.quantity,
# MAGIC     s.quantity * s.unit_price as total
# MAGIC FROM sales s
# MAGIC JOIN products p ON s.product_id = p.product_id
# MAGIC JOIN customers c ON s.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Subqueries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Scalar subquery - above average salary
# MAGIC SELECT name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > (SELECT AVG(salary) FROM employees)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Derived table (subquery in FROM)
# MAGIC SELECT dept_stats.department, dept_stats.avg_salary
# MAGIC FROM (
# MAGIC     SELECT department, AVG(salary) as avg_salary
# MAGIC     FROM employees
# MAGIC     GROUP BY department
# MAGIC ) dept_stats
# MAGIC WHERE dept_stats.avg_salary > 80000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- d) Subquery in SELECT
# MAGIC SELECT
# MAGIC     name,
# MAGIC     salary,
# MAGIC     salary - (SELECT AVG(salary) FROM employees) as diff_from_avg
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Common Table Expressions (CTEs)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Simple CTE
# MAGIC WITH dept_stats AS (
# MAGIC     SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
# MAGIC     FROM employees
# MAGIC     GROUP BY department
# MAGIC )
# MAGIC SELECT * FROM dept_stats WHERE avg_salary > 80000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Multiple CTEs
# MAGIC WITH
# MAGIC dept_salaries AS (
# MAGIC     SELECT department, SUM(salary) as total_salary
# MAGIC     FROM employees
# MAGIC     GROUP BY department
# MAGIC ),
# MAGIC dept_counts AS (
# MAGIC     SELECT department, COUNT(*) as emp_count
# MAGIC     FROM employees
# MAGIC     GROUP BY department
# MAGIC )
# MAGIC SELECT d.department, d.total_salary, c.emp_count
# MAGIC FROM dept_salaries d
# MAGIC JOIN dept_counts c ON d.department = c.department

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Window Functions in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Rank by salary within department
# MAGIC SELECT
# MAGIC     name, department, salary,
# MAGIC     RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Running total
# MAGIC SELECT
# MAGIC     transaction_id, transaction_date,
# MAGIC     quantity * unit_price as amount,
# MAGIC     SUM(quantity * unit_price) OVER (ORDER BY transaction_date) as running_total
# MAGIC FROM sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) LAG and LEAD
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     unit_price,
# MAGIC     LAG(unit_price, 1) OVER (ORDER BY transaction_date) as prev_price,
# MAGIC     LEAD(unit_price, 1) OVER (ORDER BY transaction_date) as next_price
# MAGIC FROM sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: CASE Statements

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Categorize salaries
# MAGIC SELECT name, salary,
# MAGIC     CASE
# MAGIC         WHEN salary > 100000 THEN 'High'
# MAGIC         WHEN salary > 70000 THEN 'Medium'
# MAGIC         ELSE 'Low'
# MAGIC     END as salary_category
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Pivot-like using CASE
# MAGIC SELECT
# MAGIC     department,
# MAGIC     SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) as new_york,
# MAGIC     SUM(CASE WHEN city = 'San Francisco' THEN 1 ELSE 0 END) as san_francisco,
# MAGIC     SUM(CASE WHEN city = 'Chicago' THEN 1 ELSE 0 END) as chicago
# MAGIC FROM employees
# MAGIC GROUP BY department

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Set Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create views for set operations
# MAGIC CREATE OR REPLACE TEMP VIEW eng_emp AS
# MAGIC SELECT name, city FROM employees WHERE department = 'Engineering';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW ny_emp AS
# MAGIC SELECT name, city FROM employees WHERE city = 'New York';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) UNION
# MAGIC SELECT name, city FROM eng_emp
# MAGIC UNION
# MAGIC SELECT name, city FROM ny_emp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) INTERSECT
# MAGIC SELECT name, city FROM eng_emp
# MAGIC INTERSECT
# MAGIC SELECT name, city FROM ny_emp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) EXCEPT
# MAGIC SELECT name, city FROM eng_emp
# MAGIC EXCEPT
# MAGIC SELECT name, city FROM ny_emp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Date Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Extract year, month, day
# MAGIC SELECT
# MAGIC     hire_date,
# MAGIC     YEAR(hire_date) as year,
# MAGIC     MONTH(hire_date) as month,
# MAGIC     DAY(hire_date) as day
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Date differences
# MAGIC SELECT
# MAGIC     name, hire_date,
# MAGIC     DATEDIFF(CURRENT_DATE(), hire_date) as days_employed
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Add/subtract intervals
# MAGIC SELECT
# MAGIC     hire_date,
# MAGIC     DATE_ADD(hire_date, 30) as plus_30_days,
# MAGIC     DATE_SUB(hire_date, 30) as minus_30_days,
# MAGIC     ADD_MONTHS(hire_date, 6) as plus_6_months
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: String Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) Basic string functions
# MAGIC SELECT
# MAGIC     name,
# MAGIC     CONCAT(name, ' - ', department) as full_desc,
# MAGIC     SUBSTRING(name, 1, 3) as prefix,
# MAGIC     LENGTH(name) as name_length
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) Case functions
# MAGIC SELECT
# MAGIC     name,
# MAGIC     UPPER(name) as upper_name,
# MAGIC     LOWER(name) as lower_name
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Advanced SQL Features

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) GROUPING SETS
# MAGIC SELECT department, city, COUNT(*) as count
# MAGIC FROM employees
# MAGIC GROUP BY GROUPING SETS (
# MAGIC     (department, city),
# MAGIC     (department),
# MAGIC     (city),
# MAGIC     ()
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) ROLLUP
# MAGIC SELECT department, city, COUNT(*) as count
# MAGIC FROM employees
# MAGIC GROUP BY ROLLUP (department, city)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) CUBE
# MAGIC SELECT department, city, COUNT(*) as count
# MAGIC FROM employees
# MAGIC GROUP BY CUBE (department, city)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Query Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) EXPLAIN plan
# MAGIC EXPLAIN SELECT * FROM employees WHERE salary > 80000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) Hints for optimization - Broadcast hint
# MAGIC SELECT /*+ BROADCAST(p) */ s.*, p.product_name
# MAGIC FROM sales s
# MAGIC JOIN products p ON s.product_id = p.product_id
