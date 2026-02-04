"""
Window Functions - Solutions
=============================
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, ntile
from pyspark.sql.functions import lead, lag, first, last
from pyspark.sql.functions import sum, avg, min, max, count
from pyspark.sql.functions import percent_rank, cume_dist

spark = SparkSession.builder.appName("Window Solutions").getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Row Number
# =============================================================================
# a) Row number by salary descending
window_salary = Window.orderBy(col("salary").desc())
employees.withColumn("row_num", row_number().over(window_salary)).show()

# b) Row number within department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
employees.withColumn("dept_row_num", row_number().over(window_dept)).show()

# c) Highest paid per department
employees.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .show()


# =============================================================================
# Problem 2: Rank and Dense Rank
# =============================================================================
window_spec = Window.orderBy(col("salary").desc())

df = employees.select(
    "name", "department", "salary",
    rank().over(window_spec).alias("rank"),
    dense_rank().over(window_spec).alias("dense_rank")
)
df.show()

# Rank within department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
employees.withColumn("dept_rank", rank().over(window_dept)).show()


# =============================================================================
# Problem 3: Lead and Lag
# =============================================================================
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
sales_with_lag.show()


# =============================================================================
# Problem 4: Running Aggregations
# =============================================================================
# Running total
window_running = Window.orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

sales.withColumn("running_total", sum(col("quantity") * col("unit_price")).over(window_running)).show()

# Cumulative sum within partition
window_dept_running = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

employees.withColumn("cumulative_salary", sum("salary").over(window_dept_running)).show()


# =============================================================================
# Problem 5: Moving Averages
# =============================================================================
# 3-row moving average
window_moving = Window.orderBy("transaction_date").rowsBetween(-2, 0)

sales.withColumn("moving_avg_3", avg(col("unit_price")).over(window_moving)).show()

# Moving sum over last 5
window_5 = Window.orderBy("transaction_date").rowsBetween(-4, 0)
sales.withColumn("moving_sum_5", sum(col("quantity")).over(window_5)).show()


# =============================================================================
# Problem 6: First and Last Values
# =============================================================================
window_dept = Window.partitionBy("department").orderBy("hire_date")
window_dept_full = Window.partitionBy("department").orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

employees.select(
    "name", "department", "hire_date",
    first("hire_date").over(window_dept_full).alias("first_hire"),
    last("hire_date").over(window_dept_full).alias("last_hire")
).show()


# =============================================================================
# Problem 7: Percent Rank and Cumulative Distribution
# =============================================================================
window_spec = Window.orderBy("salary")

df = employees.select(
    "name", "salary",
    percent_rank().over(window_spec).alias("percent_rank"),
    cume_dist().over(window_spec).alias("cume_dist")
)
df.show()

# Top 10%
df.filter(col("percent_rank") >= 0.9).show()


# =============================================================================
# Problem 8: Ntile
# =============================================================================
window_spec = Window.orderBy("salary")

# 4 quartiles
employees.withColumn("quartile", ntile(4).over(window_spec)).show()

# 10 buckets by hire date
window_date = Window.orderBy("hire_date")
employees.withColumn("decile", ntile(10).over(window_date)).show()

# Top quartile per department
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
employees.withColumn("dept_quartile", ntile(4).over(window_dept)) \
    .filter(col("dept_quartile") == 1) \
    .show()


# =============================================================================
# Problem 10: Practical Scenarios
# =============================================================================
# Scenario 1: Employees earning more than department average
window_dept = Window.partitionBy("department")
employees.withColumn("dept_avg", avg("salary").over(window_dept)) \
    .filter(col("salary") > col("dept_avg")) \
    .show()

# Scenario 2: Second highest salary per department
window_dept_rank = Window.partitionBy("department").orderBy(col("salary").desc())
employees.withColumn("rank", dense_rank().over(window_dept_rank)) \
    .filter(col("rank") == 2) \
    .show()


# =============================================================================
# Problem 12: Combining Window Functions
# =============================================================================
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
result.show()
