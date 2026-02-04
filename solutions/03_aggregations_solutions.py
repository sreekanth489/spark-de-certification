# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregations - Solutions
# MAGIC Topics: groupBy, agg, count, sum, avg, min, max, collect_list, pivot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, min, max, first, last, round,
    collect_list, collect_set, countDistinct, stddev, variance, expr, lit
)

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("datasets/csv/products.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Basic Aggregations

# COMMAND ----------

# a) Count total employees
print(f"Total employees: {employees.count()}")

# COMMAND ----------

# b) Min, max, avg salary
employees.select(
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    round(avg("salary"), 2).alias("avg_salary")
).show()

# COMMAND ----------

# c) Total salary per department
display(employees.groupBy("department").agg(
    sum("salary").alias("total_salary")
))

# COMMAND ----------

# d) Count employees per city
display(employees.groupBy("city").agg(
    count("*").alias("employee_count")
).orderBy(col("employee_count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: groupBy with Multiple Aggregations

# COMMAND ----------

display(employees.groupBy("department").agg(
    count("*").alias("employee_count"),
    round(avg("salary"), 2).alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("hire_date").alias("earliest_hire")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Multiple Grouping Columns

# COMMAND ----------

# a) Group by department AND city
display(employees.groupBy("department", "city").agg(
    count("*").alias("count")
))

# COMMAND ----------

# b) Average salary by department and city
dept_city_avg = employees.groupBy("department", "city").agg(
    round(avg("salary"), 2).alias("avg_salary")
)
display(dept_city_avg)

# COMMAND ----------

# c) Highest average salary combination
display(dept_city_avg.orderBy(col("avg_salary").desc()).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Collect Functions

# COMMAND ----------

# a) Collect names per department
display(employees.groupBy("department").agg(
    collect_list("name").alias("employee_names")
))

# COMMAND ----------

# b) Unique cities per department
display(employees.groupBy("department").agg(
    collect_set("city").alias("unique_cities")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Sales Aggregations

# COMMAND ----------

sales_with_revenue = sales.withColumn("revenue", col("quantity") * col("unit_price"))

# a) Total revenue per product
display(sales_with_revenue.groupBy("product_id").agg(
    round(sum("revenue"), 2).alias("total_revenue")
).orderBy(col("total_revenue").desc()))

# COMMAND ----------

# b) Best selling product by quantity
display(sales.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity")
).orderBy(col("total_quantity").desc()).limit(1))

# COMMAND ----------

# c) Average transaction value per payment method
display(sales_with_revenue.groupBy("payment_method").agg(
    round(avg("revenue"), 2).alias("avg_transaction_value")
))

# COMMAND ----------

# d) Transactions per store per day
display(sales.groupBy("store_id", "transaction_date").agg(
    count("*").alias("transaction_count")
).orderBy("store_id", "transaction_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Statistical Functions

# COMMAND ----------

# a) Standard deviation of salaries
employees.select(
    round(stddev("salary"), 2).alias("salary_stddev")
).show()

# COMMAND ----------

# b) Variance per department
display(employees.groupBy("department").agg(
    round(variance("salary"), 2).alias("salary_variance")
))

# COMMAND ----------

# d) Percentiles
employees.select(
    expr("percentile_approx(salary, 0.25)").alias("p25"),
    expr("percentile_approx(salary, 0.50)").alias("p50_median"),
    expr("percentile_approx(salary, 0.75)").alias("p75")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Pivot Tables

# COMMAND ----------

# a) Pivot: rows=department, columns=city, values=count
pivot_dept_city = employees.groupBy("department").pivot("city").count()
display(pivot_dept_city)

# COMMAND ----------

# b) Pivot sales: rows=store_id, columns=payment_method, values=sum of revenue
sales_with_revenue = sales.withColumn("revenue", col("quantity") * col("unit_price"))
pivot_sales = sales_with_revenue.groupBy("store_id").pivot("payment_method").agg(
    round(sum("revenue"), 2)
)
display(pivot_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Rollup and Cube

# COMMAND ----------

# a) Rollup - hierarchical subtotals
display(employees.rollup("department", "city").agg(
    count("*").alias("count"),
    sum("salary").alias("total_salary")
).orderBy("department", "city"))

# COMMAND ----------

# b) Cube - all combinations
display(employees.cube("department", "city").agg(
    count("*").alias("count")
).orderBy("department", "city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Having Clause Equivalent

# COMMAND ----------

# a) Departments with more than 2 employees
display(employees.groupBy("department").agg(
    count("*").alias("emp_count")
).filter(col("emp_count") > 2))

# COMMAND ----------

# b) Departments with avg salary > 80000
display(employees.groupBy("department").agg(
    avg("salary").alias("avg_salary")
).filter(col("avg_salary") > 80000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Complex Aggregation Scenario

# COMMAND ----------

dept_summary = employees.groupBy("department").agg(
    count("*").alias("total_employees"),
    sum("salary").alias("total_salary_expense"),
    round(avg("salary"), 2).alias("avg_salary"),
    (max("salary") - min("salary")).alias("salary_range"),
    collect_list("name").alias("employee_names"),
    countDistinct("city").alias("unique_cities")
).filter(
    col("avg_salary") > 75000
).orderBy(
    col("total_salary_expense").desc()
)

display(dept_summary)
