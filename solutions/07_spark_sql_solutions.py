"""
Spark SQL - Solutions
======================
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL Solutions").getOrCreate()

# Load and register tables
employees = spark.read.csv("../datasets/csv/employees.csv", header=True, inferSchema=True)
sales = spark.read.csv("../datasets/csv/sales.csv", header=True, inferSchema=True)
products = spark.read.csv("../datasets/csv/products.csv", header=True, inferSchema=True)
customers = spark.read.csv("../datasets/csv/customers.csv", header=True, inferSchema=True)

employees.createOrReplaceTempView("employees")
sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")

# =============================================================================
# Problem 1: Basic SQL Queries
# =============================================================================
# a) Employees with salary > 80000
spark.sql("""
    SELECT * FROM employees WHERE salary > 80000
""").show()

# b) Count per department
spark.sql("""
    SELECT department, COUNT(*) as emp_count
    FROM employees
    GROUP BY department
""").show()

# c) Average salary by department
spark.sql("""
    SELECT department, ROUND(AVG(salary), 2) as avg_salary
    FROM employees
    GROUP BY department
""").show()

# d) Top 5 highest paid
spark.sql("""
    SELECT name, salary
    FROM employees
    ORDER BY salary DESC
    LIMIT 5
""").show()


# =============================================================================
# Problem 2: SQL Joins
# =============================================================================
# a) Sales with product names
spark.sql("""
    SELECT s.transaction_id, p.product_name, s.quantity, s.unit_price
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
""").show()

# b) Left join to find unsold products
spark.sql("""
    SELECT p.product_id, p.product_name
    FROM products p
    LEFT JOIN sales s ON p.product_id = s.product_id
    WHERE s.transaction_id IS NULL
""").show()

# c) Multiple table join
spark.sql("""
    SELECT
        s.transaction_date,
        CONCAT(c.first_name, ' ', c.last_name) as customer_name,
        p.product_name,
        s.quantity,
        s.quantity * s.unit_price as total
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    JOIN customers c ON s.customer_id = c.customer_id
""").show()


# =============================================================================
# Problem 3: Subqueries
# =============================================================================
# a) Scalar subquery - above average salary
spark.sql("""
    SELECT name, salary
    FROM employees
    WHERE salary > (SELECT AVG(salary) FROM employees)
""").show()

# b) Correlated subquery - departments with above avg headcount
spark.sql("""
    SELECT department, COUNT(*) as dept_count
    FROM employees
    GROUP BY department
    HAVING COUNT(*) > (SELECT COUNT(*) / COUNT(DISTINCT department) FROM employees)
""").show()

# c) Derived table (subquery in FROM)
spark.sql("""
    SELECT dept_stats.department, dept_stats.avg_salary
    FROM (
        SELECT department, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    ) dept_stats
    WHERE dept_stats.avg_salary > 80000
""").show()

# d) Subquery in SELECT
spark.sql("""
    SELECT
        name,
        salary,
        salary - (SELECT AVG(salary) FROM employees) as diff_from_avg
    FROM employees
""").show()


# =============================================================================
# Problem 4: Common Table Expressions (CTEs)
# =============================================================================
# a) Simple CTE
spark.sql("""
    WITH dept_stats AS (
        SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
        FROM employees
        GROUP BY department
    )
    SELECT * FROM dept_stats WHERE avg_salary > 80000
""").show()

# b) Multiple CTEs
spark.sql("""
    WITH
    dept_salaries AS (
        SELECT department, SUM(salary) as total_salary
        FROM employees
        GROUP BY department
    ),
    dept_counts AS (
        SELECT department, COUNT(*) as emp_count
        FROM employees
        GROUP BY department
    )
    SELECT d.department, d.total_salary, c.emp_count
    FROM dept_salaries d
    JOIN dept_counts c ON d.department = c.department
""").show()

# c) Hierarchical CTE (simulated - Spark doesn't support true recursive CTEs)
spark.sql("""
    WITH managers AS (
        SELECT emp_id, name, manager_id
        FROM employees
        WHERE manager_id IS NULL
    )
    SELECT e.name as employee, m.name as manager
    FROM employees e
    LEFT JOIN managers m ON e.manager_id = m.emp_id
""").show()


# =============================================================================
# Problem 5: Window Functions in SQL
# =============================================================================
# a) Rank by salary within department
spark.sql("""
    SELECT
        name, department, salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
""").show()

# b) Running total
spark.sql("""
    SELECT
        transaction_id, transaction_date,
        quantity * unit_price as amount,
        SUM(quantity * unit_price) OVER (ORDER BY transaction_date) as running_total
    FROM sales
""").show()

# c) LAG and LEAD
spark.sql("""
    SELECT
        transaction_id,
        unit_price,
        LAG(unit_price, 1) OVER (ORDER BY transaction_date) as prev_price,
        LEAD(unit_price, 1) OVER (ORDER BY transaction_date) as next_price
    FROM sales
""").show()

# d) Percentile rank
spark.sql("""
    SELECT
        name, salary,
        PERCENT_RANK() OVER (ORDER BY salary) as percentile
    FROM employees
""").show()


# =============================================================================
# Problem 6: CASE Statements
# =============================================================================
# a) Categorize salaries
spark.sql("""
    SELECT name, salary,
        CASE
            WHEN salary > 100000 THEN 'High'
            WHEN salary > 70000 THEN 'Medium'
            ELSE 'Low'
        END as salary_category
    FROM employees
""").show()

# b) Pivot-like using CASE
spark.sql("""
    SELECT
        department,
        SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) as new_york,
        SUM(CASE WHEN city = 'San Francisco' THEN 1 ELSE 0 END) as san_francisco,
        SUM(CASE WHEN city = 'Chicago' THEN 1 ELSE 0 END) as chicago
    FROM employees
    GROUP BY department
""").show()

# c) Nested CASE
spark.sql("""
    SELECT name, department, salary,
        CASE department
            WHEN 'Engineering' THEN
                CASE WHEN salary > 100000 THEN 'Senior' ELSE 'Junior' END
            WHEN 'Sales' THEN
                CASE WHEN salary > 90000 THEN 'Senior' ELSE 'Junior' END
            ELSE 'Staff'
        END as level
    FROM employees
""").show()


# =============================================================================
# Problem 7: Set Operations
# =============================================================================
# Create sample views for set operations
spark.sql("SELECT name, city FROM employees WHERE department = 'Engineering'").createOrReplaceTempView("eng_emp")
spark.sql("SELECT name, city FROM employees WHERE city = 'New York'").createOrReplaceTempView("ny_emp")

# a) UNION
spark.sql("""
    SELECT name, city FROM eng_emp
    UNION
    SELECT name, city FROM ny_emp
""").show()

# b) INTERSECT
spark.sql("""
    SELECT name, city FROM eng_emp
    INTERSECT
    SELECT name, city FROM ny_emp
""").show()

# c) EXCEPT
spark.sql("""
    SELECT name, city FROM eng_emp
    EXCEPT
    SELECT name, city FROM ny_emp
""").show()

# d) UNION ALL (keeps duplicates)
spark.sql("""
    SELECT name, city FROM eng_emp
    UNION ALL
    SELECT name, city FROM ny_emp
""").show()


# =============================================================================
# Problem 8: Date Functions
# =============================================================================
# a) Extract year, month, day
spark.sql("""
    SELECT
        hire_date,
        YEAR(hire_date) as year,
        MONTH(hire_date) as month,
        DAY(hire_date) as day
    FROM employees
""").show()

# b) Date differences
spark.sql("""
    SELECT
        name, hire_date,
        DATEDIFF(CURRENT_DATE(), hire_date) as days_employed
    FROM employees
""").show()

# c) Add/subtract intervals
spark.sql("""
    SELECT
        hire_date,
        DATE_ADD(hire_date, 30) as plus_30_days,
        DATE_SUB(hire_date, 30) as minus_30_days,
        ADD_MONTHS(hire_date, 6) as plus_6_months
    FROM employees
""").show()

# d) Format dates
spark.sql("""
    SELECT
        hire_date,
        DATE_FORMAT(hire_date, 'MMMM dd, yyyy') as formatted
    FROM employees
""").show()


# =============================================================================
# Problem 9: String Functions
# =============================================================================
# a) Basic string functions
spark.sql("""
    SELECT
        name,
        CONCAT(name, ' - ', department) as full_desc,
        SUBSTRING(name, 1, 3) as prefix,
        LENGTH(name) as name_length
    FROM employees
""").show()

# b) Case functions
spark.sql("""
    SELECT
        name,
        UPPER(name) as upper_name,
        LOWER(name) as lower_name,
        TRIM(name) as trimmed
    FROM employees
""").show()

# c) REGEXP functions
spark.sql("""
    SELECT
        name,
        REGEXP_REPLACE(name, ' ', '_') as underscored,
        REGEXP_EXTRACT(name, '^(\\w+)', 1) as first_word
    FROM employees
""").show()

# d) SPLIT and array
spark.sql("""
    SELECT
        name,
        SPLIT(name, ' ') as name_parts,
        SPLIT(name, ' ')[0] as first_name
    FROM employees
""").show()


# =============================================================================
# Problem 10: Database and Table Management
# =============================================================================
# a) Create database
spark.sql("CREATE DATABASE IF NOT EXISTS certification_db")

# b) Create managed table
spark.sql("""
    CREATE TABLE IF NOT EXISTS certification_db.employees_managed
    USING PARQUET
    AS SELECT * FROM employees
""")

# c) Create external table (location would be specified)
# spark.sql("""
#     CREATE EXTERNAL TABLE IF NOT EXISTS certification_db.employees_external
#     USING PARQUET
#     LOCATION '/path/to/data'
# """)

# d) DESCRIBE and SHOW commands
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES IN certification_db").show()
spark.sql("DESCRIBE certification_db.employees_managed").show()

# e) ALTER and DROP
spark.sql("DROP TABLE IF EXISTS certification_db.employees_managed")
spark.sql("DROP DATABASE IF EXISTS certification_db CASCADE")


# =============================================================================
# Problem 11: Views
# =============================================================================
# a) Temporary view (session-scoped)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW high_earners AS
    SELECT * FROM employees WHERE salary > 90000
""")
spark.sql("SELECT * FROM high_earners").show()

# b) Global temporary view (application-scoped)
spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW all_high_earners AS
    SELECT * FROM employees WHERE salary > 90000
""")
spark.sql("SELECT * FROM global_temp.all_high_earners").show()

# c) Permanent view (requires catalog support like Hive/Delta)
# spark.sql("CREATE OR REPLACE VIEW db.permanent_view AS SELECT ...")

# d) View differences
# - Temp view: session only
# - Global temp view: all sessions in same app
# - Permanent view: persisted in metastore


# =============================================================================
# Problem 12: Advanced SQL Features
# =============================================================================
# a) GROUPING SETS
spark.sql("""
    SELECT department, city, COUNT(*) as count
    FROM employees
    GROUP BY GROUPING SETS (
        (department, city),
        (department),
        (city),
        ()
    )
""").show()

# b) ROLLUP (hierarchical totals)
spark.sql("""
    SELECT department, city, COUNT(*) as count
    FROM employees
    GROUP BY ROLLUP (department, city)
""").show()

# c) CUBE (all combinations)
spark.sql("""
    SELECT department, city, COUNT(*) as count
    FROM employees
    GROUP BY CUBE (department, city)
""").show()

# d) LATERAL VIEW with explode
orders = spark.read.json("../datasets/json/orders.json")
orders.createOrReplaceTempView("orders")

spark.sql("""
    SELECT order_id, item.product_id, item.quantity
    FROM orders
    LATERAL VIEW explode(items) AS item
""").show()


# =============================================================================
# Problem 13: Query Optimization
# =============================================================================
# a) EXPLAIN plan
spark.sql("EXPLAIN SELECT * FROM employees WHERE salary > 80000").show(truncate=False)

# Extended explain
spark.sql("EXPLAIN EXTENDED SELECT * FROM employees WHERE salary > 80000").show(truncate=False)

# b) Identify inefficient queries
# - Full table scans when filter could use partition pruning
# - Shuffle operations that could be avoided with broadcast
# - Missing predicate pushdown

# c) Hints for optimization
spark.sql("""
    SELECT /*+ BROADCAST(p) */ s.*, p.product_name
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
""").show()

# Coalesce hint
spark.sql("""
    SELECT /*+ COALESCE(1) */ * FROM employees
""").show()

# Repartition hint
spark.sql("""
    SELECT /*+ REPARTITION(4) */ * FROM employees
""").show()
