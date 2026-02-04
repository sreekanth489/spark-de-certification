"""
Aggregations - Solutions
=========================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, first, last,
    collect_list, collect_set, countDistinct, approx_count_distinct,
    stddev, variance, expr, round, lit
)

spark = SparkSession.builder.appName("Aggregations Solutions").getOrCreate()

employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("../datasets/csv/products.csv", header=True, inferSchema=True)

# =============================================================================
# Problem 1: Basic Aggregations
# =============================================================================
# a) Count total employees
print(f"Total employees: {employees.count()}")

# b) Min, max, avg salary
employees.select(
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    round(avg("salary"), 2).alias("avg_salary")
).show()

# c) Total salary per department
employees.groupBy("department").agg(
    sum("salary").alias("total_salary")
).show()

# d) Count employees per city
employees.groupBy("city").agg(
    count("*").alias("employee_count")
).orderBy(col("employee_count").desc()).show()


# =============================================================================
# Problem 2: groupBy with Multiple Aggregations
# =============================================================================
employees.groupBy("department").agg(
    count("*").alias("employee_count"),
    round(avg("salary"), 2).alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("hire_date").alias("earliest_hire")
).show()


# =============================================================================
# Problem 3: Multiple Grouping Columns
# =============================================================================
# a) Group by department AND city
employees.groupBy("department", "city").agg(
    count("*").alias("count")
).show()

# b) Average salary by department and city
dept_city_avg = employees.groupBy("department", "city").agg(
    round(avg("salary"), 2).alias("avg_salary")
)
dept_city_avg.show()

# c) Highest average salary combination
dept_city_avg.orderBy(col("avg_salary").desc()).limit(1).show()


# =============================================================================
# Problem 4: Collect Functions
# =============================================================================
# a) Collect names per department
employees.groupBy("department").agg(
    collect_list("name").alias("employee_names")
).show(truncate=False)

# b) Unique cities per department
employees.groupBy("department").agg(
    collect_set("city").alias("unique_cities")
).show(truncate=False)

# c) First and last employee names (alphabetically)
from pyspark.sql.functions import sort_array

employees.groupBy("department").agg(
    first("name").alias("first_name"),
    last("name").alias("last_name"),
    sort_array(collect_list("name")).alias("sorted_names")
).show(truncate=False)


# =============================================================================
# Problem 5: Sales Aggregations
# =============================================================================
sales_with_revenue = sales.withColumn("revenue", col("quantity") * col("unit_price"))

# a) Total revenue per product
sales_with_revenue.groupBy("product_id").agg(
    round(sum("revenue"), 2).alias("total_revenue")
).orderBy(col("total_revenue").desc()).show()

# b) Best selling product by quantity
sales.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity")
).orderBy(col("total_quantity").desc()).limit(1).show()

# c) Average transaction value per payment method
sales_with_revenue.groupBy("payment_method").agg(
    round(avg("revenue"), 2).alias("avg_transaction_value")
).show()

# d) Transactions per store per day
sales.groupBy("store_id", "transaction_date").agg(
    count("*").alias("transaction_count")
).orderBy("store_id", "transaction_date").show()


# =============================================================================
# Problem 6: Statistical Functions
# =============================================================================
# a) Standard deviation of salaries
employees.select(
    round(stddev("salary"), 2).alias("salary_stddev")
).show()

# b) Variance per department
employees.groupBy("department").agg(
    round(variance("salary"), 2).alias("salary_variance")
).show()

# c) Correlation (simulated - between salary and emp_id as proxy for tenure)
employees.select(
    expr("corr(salary, emp_id)").alias("salary_tenure_corr")
).show()

# d) Percentiles using percentile_approx
employees.select(
    expr("percentile_approx(salary, 0.25)").alias("p25"),
    expr("percentile_approx(salary, 0.50)").alias("p50_median"),
    expr("percentile_approx(salary, 0.75)").alias("p75")
).show()


# =============================================================================
# Problem 7: Pivot Tables
# =============================================================================
# a) Pivot: rows=department, columns=city, values=count
pivot_dept_city = employees.groupBy("department").pivot("city").count()
pivot_dept_city.show()

# b) Pivot sales: rows=store_id, columns=payment_method, values=sum of revenue
sales_with_revenue = sales.withColumn("revenue", col("quantity") * col("unit_price"))
pivot_sales = sales_with_revenue.groupBy("store_id").pivot("payment_method").agg(
    round(sum("revenue"), 2)
)
pivot_sales.show()

# c) Pivot with multiple aggregations
employees.groupBy("department").pivot("city").agg(
    count("*").alias("count"),
    round(avg("salary"), 0).alias("avg_sal")
).show()


# =============================================================================
# Problem 8: Rollup and Cube
# =============================================================================
# a) Rollup - hierarchical subtotals
employees.rollup("department", "city").agg(
    count("*").alias("count"),
    sum("salary").alias("total_salary")
).orderBy("department", "city").show()

# b) Cube - all combinations
employees.cube("department", "city").agg(
    count("*").alias("count")
).orderBy("department", "city").show()

# c) Identify subtotals using grouping()
from pyspark.sql.functions import grouping, grouping_id

employees.cube("department", "city").agg(
    count("*").alias("count"),
    grouping("department").alias("dept_subtotal"),
    grouping("city").alias("city_subtotal"),
    grouping_id("department", "city").alias("grouping_id")
).orderBy("department", "city").show()


# =============================================================================
# Problem 9: Having Clause Equivalent
# =============================================================================
# a) Departments with more than 2 employees
employees.groupBy("department").agg(
    count("*").alias("emp_count")
).filter(col("emp_count") > 2).show()

# b) Departments with avg salary > 80000
employees.groupBy("department").agg(
    avg("salary").alias("avg_salary")
).filter(col("avg_salary") > 80000).show()

# c) Products sold more than 5 times
sales.groupBy("product_id").agg(
    count("*").alias("sale_count")
).filter(col("sale_count") > 5).show()

# d) Cities with total salary expense > 200000
employees.groupBy("city").agg(
    sum("salary").alias("total_salary")
).filter(col("total_salary") > 200000).show()


# =============================================================================
# Problem 10: Complex Aggregation Scenario
# =============================================================================
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

dept_summary.show(truncate=False)
