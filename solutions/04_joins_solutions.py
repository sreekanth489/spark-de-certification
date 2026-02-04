# Databricks notebook source
# MAGIC %md
# MAGIC # Joins - Solutions
# MAGIC Topics: inner, left, right, full outer, cross, semi, anti joins, broadcast

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, broadcast, coalesce, lit, when, sum, count, concat

# COMMAND ----------

employees = spark.read.csv("datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("datasets/csv/products.csv", header=True, inferSchema=True)
customers = spark.read.csv("datasets/csv/customers.csv", header=True, inferSchema=True)

display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Inner Join

# COMMAND ----------

# a) Join sales with products
sales_products = sales.join(products, sales.product_id == products.product_id, "inner")

# b) Show specific columns
display(sales_products.select(
    sales.transaction_id,
    products.product_name,
    sales.quantity,
    sales.unit_price
))

# COMMAND ----------

# c) Calculate total amount
display(sales_products.select(
    sales.transaction_id,
    products.product_name,
    (sales.quantity * sales.unit_price).alias("total_amount")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Left Outer Join

# COMMAND ----------

# a) Left join products with sales
products_sales = products.join(sales, "product_id", "left")

# b) Products never sold (sales columns are null)
display(products.join(sales, "product_id", "left").filter(
    sales.transaction_id.isNull()
).select(products.product_id, products.product_name))

# COMMAND ----------

# c) Count sales per product, 0 for unsold
display(products.join(sales, "product_id", "left").groupBy(
    products.product_id, products.product_name
).agg(
    count(sales.transaction_id).alias("sale_count")
).orderBy("product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Right Outer Join

# COMMAND ----------

# a) Right join sales with customers
sales_customers = sales.join(customers, sales.customer_id == customers.customer_id, "right")

# b) All customers including those without sales
display(sales_customers.select(
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    sales.transaction_id
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Full Outer Join

# COMMAND ----------

# Create sample data for demonstration
left_data = [(1, "A"), (2, "B"), (3, "C")]
right_data = [(2, "X"), (3, "Y"), (4, "Z")]

left_df = spark.createDataFrame(left_data, ["id", "left_val"])
right_df = spark.createDataFrame(right_data, ["id", "right_val"])

# a) Full outer join
full_join = left_df.join(right_df, "id", "full")
display(full_join)

# COMMAND ----------

# b) Identify source of records
display(full_join.withColumn(
    "source",
    when(col("left_val").isNull(), "right_only")
    .when(col("right_val").isNull(), "left_only")
    .otherwise("both")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Cross Join

# COMMAND ----------

# a) Cross join with discount rates
discount_rates = spark.createDataFrame([(0.1,), (0.15,), (0.2,)], ["discount_rate"])
products_discounts = products.crossJoin(discount_rates)

# b) Calculate discounted prices
products_discounts = products_discounts.withColumn(
    "discounted_price",
    col("list_price") * (1 - col("discount_rate"))
)
display(products_discounts.select("product_name", "list_price", "discount_rate", "discounted_price"))

# COMMAND ----------

# c) Products where discount makes price < cost
display(products_discounts.filter(
    col("discounted_price") < col("cost_price")
).select("product_name", "cost_price", "discounted_price", "discount_rate"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Left Semi Join

# COMMAND ----------

# a) Products with at least one sale
products_with_sales = products.join(sales, "product_id", "leftsemi")
display(products_with_sales)

# COMMAND ----------

# b) Employees who are managers
managers = employees.alias("mgr").select(col("manager_id").alias("emp_id")).distinct().filter(col("emp_id").isNotNull())
employees_who_manage = employees.join(managers, "emp_id", "leftsemi")
display(employees_who_manage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Left Anti Join

# COMMAND ----------

# a) Products with no sales
products_no_sales = products.join(sales, "product_id", "leftanti")
display(products_no_sales.select("product_id", "product_name"))

# COMMAND ----------

# b) Customers who never ordered
customers_no_orders = customers.join(sales, "customer_id", "leftanti")
display(customers_no_orders.select("customer_id", "first_name", "last_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Self Join

# COMMAND ----------

# a) Employee-manager pairs
emp = employees.alias("emp")
mgr = employees.alias("mgr")

emp_mgr = emp.join(
    mgr,
    col("emp.manager_id") == col("mgr.emp_id"),
    "left"
)

# b) Employee with manager name
display(emp_mgr.select(
    col("emp.name").alias("employee_name"),
    col("mgr.name").alias("manager_name")
))

# COMMAND ----------

# c) Employees earning more than their manager
display(emp_mgr.filter(
    col("emp.salary") > col("mgr.salary")
).select(
    col("emp.name").alias("employee"),
    col("emp.salary").alias("emp_salary"),
    col("mgr.name").alias("manager"),
    col("mgr.salary").alias("mgr_salary")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Multiple Joins

# COMMAND ----------

# Denormalized view
denormalized = sales \
    .join(products, "product_id") \
    .join(customers, "customer_id") \
    .select(
        sales.transaction_date,
        concat(customers.first_name, lit(" "), customers.last_name).alias("customer_name"),
        products.product_name,
        sales.quantity,
        (sales.quantity * sales.unit_price).alias("total_amount")
    )

display(denormalized)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Broadcast Join

# COMMAND ----------

# Broadcast small dimension table
sales_with_products = sales.join(
    broadcast(products),
    "product_id"
)
display(sales_with_products.select("transaction_id", "product_name", "quantity"))

# COMMAND ----------

# Verify broadcast in explain plan
sales_with_products.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: Join Conditions

# COMMAND ----------

# b) Range join (inequality)
price_ranges = spark.createDataFrame([
    ("Budget", 0, 50),
    ("Mid", 50, 150),
    ("Premium", 150, 1000)
], ["category", "min_price", "max_price"])

display(products.join(
    price_ranges,
    (products.list_price >= price_ranges.min_price) &
    (products.list_price < price_ranges.max_price)
).select("product_name", "list_price", "category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Handling Duplicate Column Names

# COMMAND ----------

# a) Join with same column names - use aliases
df1 = employees.select("emp_id", "name", "department").alias("df1")
df2 = employees.select("emp_id", "name", "salary").alias("df2")

# b) Properly alias to avoid ambiguity
joined = df1.join(df2, df1.emp_id == df2.emp_id)
result = joined.select(
    col("df1.emp_id"),
    col("df1.name").alias("name_1"),
    col("df1.department"),
    col("df2.salary")
)
display(result)

# COMMAND ----------

# c) Using join with list of columns keeps single column
clean_join = employees.select("emp_id", "name").join(
    employees.select("emp_id", "salary"),
    "emp_id"  # String creates single column
)
display(clean_join)
