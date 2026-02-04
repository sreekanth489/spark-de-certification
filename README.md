# Spark Data Engineering Certification Practice

Practice repository for Databricks Certified Data Engineer Professional certification preparation.

## Repository Structure

```
spark-de-certification/
├── datasets/
│   ├── csv/              # CSV sample datasets
│   ├── json/             # JSON sample datasets
│   ├── parquet/          # Parquet files (generated)
│   └── delta/            # Delta tables (generated)
├── problems/
│   ├── 01_dataframes/    # DataFrame basics
│   ├── 02_transformations/  # Data transformations
│   ├── 03_aggregations/  # GroupBy, aggregations
│   ├── 04_joins/         # Join operations
│   ├── 05_window_functions/  # Window functions
│   ├── 06_udf/           # User-defined functions
│   ├── 07_spark_sql/     # SQL operations
│   ├── 08_file_formats/  # File format handling
│   ├── 09_performance/   # Performance tuning
│   └── 10_streaming/     # Structured streaming
├── solutions/            # Solution files
├── notebooks/            # Jupyter/Databricks notebooks
└── requirements.txt
```

## Sample Datasets

| Dataset | Description | Columns |
|---------|-------------|---------|
| employees.csv | Employee data | emp_id, name, department, salary, hire_date, manager_id, city |
| sales.csv | Transaction data | transaction_id, product_id, customer_id, quantity, unit_price, transaction_date, store_id, payment_method |
| products.csv | Product catalog | product_id, product_name, category, brand, cost_price, list_price, stock_quantity |
| customers.csv | Customer info | customer_id, first_name, last_name, email, phone, city, state, country, registration_date |
| orders.json | Nested order data | order_id, customer_id, items (array), shipping (struct), status |
| events.json | Event tracking | event_id, event_type, user_id, timestamp, properties (struct) |
| logs.csv | Service logs | timestamp, level, service, message, trace_id, duration_ms |

## Topics Covered

### 1. DataFrame Basics
- Creating DataFrames (from lists, files)
- Schema definition and inference
- Basic operations (select, filter, sort)
- Column operations (add, rename, drop)
- Null handling

### 2. Transformations
- Column expressions and functions
- Type casting
- String functions
- Conditional logic (when/otherwise)
- Arrays and structs

### 3. Aggregations
- GroupBy operations
- Aggregate functions (count, sum, avg, min, max)
- Collect functions (collect_list, collect_set)
- Pivot tables
- Rollup and cube

### 4. Joins
- Inner, left, right, full outer joins
- Cross joins
- Semi and anti joins
- Self joins
- Broadcast joins

### 5. Window Functions
- Ranking (row_number, rank, dense_rank)
- Lead and lag
- Running aggregations
- Moving averages
- Ntile and percentiles

### 6. UDFs
- Python UDFs
- Pandas UDFs (vectorized)
- Registering UDFs for SQL
- Performance considerations

### 7. Spark SQL
- SQL queries with temporary views
- Subqueries and CTEs
- Database and table management
- Views and catalog

### 8. File Formats
- CSV, JSON, Parquet, ORC
- Schema evolution
- Partitioning
- Compression
- Delta Lake operations

### 9. Performance Tuning
- Explain plans
- Caching and persistence
- Partitioning strategies
- Broadcast joins
- Adaptive Query Execution (AQE)

### 10. Structured Streaming
- Streaming sources and sinks
- Output modes and triggers
- Windowed aggregations
- Watermarks and late data
- Stream-stream joins

## Setup

### Local Setup (PySpark)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run practice problems
cd problems/01_dataframes
python problems.py
```

### Databricks Setup

1. Import notebooks from `notebooks/` directory
2. Datasets are in `datasets/` - upload to DBFS or use as local files
3. Each problem file can be converted to a Databricks notebook

## How to Practice

1. **Read the problem** - Each problem has clear requirements
2. **Write your solution** - Fill in the `# YOUR CODE HERE` sections
3. **Check solutions** - Compare with files in `solutions/` directory
4. **Run and verify** - Execute code to see results

## Certification Exam Topics

This repo covers key topics from the Databricks Data Engineer Professional exam:

- [ ] DataFrame API and Spark SQL
- [ ] Data transformations and aggregations
- [ ] Joins and window functions
- [ ] Reading/writing various file formats
- [ ] Performance optimization
- [ ] Structured Streaming
- [ ] Delta Lake operations
- [ ] Data quality and testing

## Tips for Certification

1. **Hands-on practice** - Run every example, don't just read
2. **Understand explain plans** - Know how to interpret query plans
3. **Know the differences** - rank vs dense_rank, repartition vs coalesce
4. **Performance matters** - Understand broadcast joins, caching, partitioning
5. **Delta Lake** - If the exam covers Delta, know MERGE, time travel, optimization
