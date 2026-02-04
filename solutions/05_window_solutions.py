# Databricks notebook source
# MAGIC %md
# MAGIC # Window Functions - Solutions
# MAGIC Topics: row_number, rank, dense_rank, lead, lag, running totals, partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, ntile,
    lead, lag, first, last, sum, avg, min, max, count,
    percent_rank, cume_dist
)

# COMMAND ----------

# Load sample data
employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Row Number

# COMMAND ----------

# a) Row number by salary descending
window_salary = Window.orderBy(col("salary").desc())
df = employees.withColumn("row_num", row_number().over(window_salary))
display(df)

# COMMAND ----------

# b) Row number within department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
df = employees.withColumn("dept_row_num", row_number().over(window_dept))
display(df)

# COMMAND ----------

# c) Highest paid per department using row_number
df = employees.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .drop("rn")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Rank and Dense Rank

# COMMAND ----------

# Rank vs Dense Rank comparison
window_spec = Window.orderBy(col("salary").desc())

df = employees.select(
    "name", "department", "salary",
    rank().over(window_spec).alias("rank"),
    dense_rank().over(window_spec).alias("dense_rank")
)
display(df)

# COMMAND ----------

# Rank within department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
df = employees.withColumn("dept_rank", rank().over(window_dept))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Lead and Lag

# COMMAND ----------

window_date = Window.orderBy("transaction_date")

sales_with_lag = sales.select(
    "transaction_id",
    "transaction_date",
    "unit_price",
    lag("unit_price", 1).over(window_date).alias("prev_price"),
    lead("unit_price", 1).over(window_date).alias("next_price"),
    (col("unit_price") - lag("unit_price", 1).over(window_date)).alias("price_diff"),
    lag("unit_price", 2).over(window_date).alias("prev_2_price")
)
display(sales_with_lag)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Running Aggregations

# COMMAND ----------

# Running total
window_running = Window.orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = sales.withColumn("running_total", sum(col("quantity") * col("unit_price")).over(window_running))
display(df.select("transaction_id", "transaction_date", "quantity", "unit_price", "running_total"))

# COMMAND ----------

# Cumulative sum within partition (department)
window_dept_running = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = employees.withColumn("cumulative_salary", sum("salary").over(window_dept_running))
display(df.select("name", "department", "hire_date", "salary", "cumulative_salary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Moving Averages

# COMMAND ----------

# 3-row moving average
window_moving = Window.orderBy("transaction_date").rowsBetween(-2, 0)

df = sales.withColumn("moving_avg_3", avg(col("unit_price")).over(window_moving))
display(df.select("transaction_id", "transaction_date", "unit_price", "moving_avg_3"))

# COMMAND ----------

# Moving sum over last 5 rows
window_5 = Window.orderBy("transaction_date").rowsBetween(-4, 0)
df = sales.withColumn("moving_sum_5", sum(col("quantity")).over(window_5))
display(df.select("transaction_id", "transaction_date", "quantity", "moving_sum_5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: First and Last Values

# COMMAND ----------

window_dept_full = Window.partitionBy("department").orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df = employees.select(
    "name", "department", "hire_date",
    first("hire_date").over(window_dept_full).alias("first_hire"),
    last("hire_date").over(window_dept_full).alias("last_hire")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Percent Rank and Cumulative Distribution

# COMMAND ----------

window_spec = Window.orderBy("salary")

df = employees.select(
    "name", "salary",
    percent_rank().over(window_spec).alias("percent_rank"),
    cume_dist().over(window_spec).alias("cume_dist")
)
display(df)

# COMMAND ----------

# Top 10% earners
df.filter(col("percent_rank") >= 0.9).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Ntile (Bucketing)

# COMMAND ----------

# 4 salary quartiles
window_spec = Window.orderBy("salary")
df = employees.withColumn("quartile", ntile(4).over(window_spec))
display(df.select("name", "salary", "quartile"))

# COMMAND ----------

# 10 buckets by hire date
window_date = Window.orderBy("hire_date")
df = employees.withColumn("decile", ntile(10).over(window_date))
display(df.select("name", "hire_date", "decile"))

# COMMAND ----------

# Top quartile per department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
df = employees.withColumn("dept_quartile", ntile(4).over(window_dept)) \
    .filter(col("dept_quartile") == 1)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Practical Scenarios

# COMMAND ----------

# Scenario 1: Employees earning more than department average
window_dept = Window.partitionBy("department")
df = employees.withColumn("dept_avg", avg("salary").over(window_dept)) \
    .filter(col("salary") > col("dept_avg"))
display(df.select("name", "department", "salary", "dept_avg"))

# COMMAND ----------

# Scenario 2: Second highest salary per department
window_dept_rank = Window.partitionBy("department").orderBy(col("salary").desc())
df = employees.withColumn("rank", dense_rank().over(window_dept_rank)) \
    .filter(col("rank") == 2)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Combining Window Functions

# COMMAND ----------

window_dept_salary = Window.partitionBy("department").orderBy(col("salary").desc())
window_dept_running = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_dept_full = Window.partitionBy("department")

result = employees.select(
    "name", "department", "salary", "hire_date",
    rank().over(window_dept_salary).alias("salary_rank"),
    sum("salary").over(window_dept_running).alias("running_salary"),
    lag("salary").over(window_dept_salary).alias("prev_higher_salary"),
    avg("salary").over(window_dept_full).alias("dept_avg"),
    (col("salary") - avg("salary").over(window_dept_full)).alias("diff_from_avg")
)
display(result)
