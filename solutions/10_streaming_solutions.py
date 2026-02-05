# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming - Solutions
# MAGIC Topics: Sources, sinks, triggers, windows, watermarks, state management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, window, from_json, to_json, current_timestamp,
    expr, sum, count, avg, max, min, explode, split,
    timestamp_seconds, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType, ArrayType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 1: Reading from Sources

# COMMAND ----------

# a) Rate source (for testing) - generates rows per second
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

# Rate source schema: timestamp, value
print("Rate source schema:")
rate_df.printSchema()

# COMMAND ----------

# b) CSV directory schema (for file source)
csv_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", TimestampType())
])

# csv_stream = spark.readStream \
#     .schema(csv_schema) \
#     .option("header", True) \
#     .csv("path/to/csv/dir")

print("CSV schema defined!")

# COMMAND ----------

# c) JSON directory schema
json_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType())
])

# json_stream = spark.readStream \
#     .schema(json_schema) \
#     .json("path/to/json/dir")

print("JSON schema defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 2: Basic Streaming Operations

# COMMAND ----------

# Using rate source for demonstration
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

# a) Filter
filtered = rate_df.filter(col("value") % 2 == 0)

# b) Select and transform
transformed = rate_df.select(
    col("timestamp"),
    col("value"),
    (col("value") * 2).alias("doubled")
)

# c) Add computed columns
with_computed = rate_df.withColumn(
    "processing_time",
    current_timestamp()
).withColumn(
    "value_category",
    expr("CASE WHEN value > 50 THEN 'high' ELSE 'low' END")
)

print("Streaming transformations defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 3: Output Modes
# MAGIC
# MAGIC | Mode | Description | Use Cases |
# MAGIC |------|-------------|-----------|
# MAGIC | **Append** | New rows only (default) | Non-aggregation queries, aggregations with watermark |
# MAGIC | **Complete** | All rows | Aggregations |
# MAGIC | **Update** | Only updated rows | Aggregations (outputs only changed aggregates) |

# COMMAND ----------

# Example aggregation with complete mode
# query = rate_df.groupBy(window("timestamp", "10 seconds")) \
#     .count() \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

print("Output modes explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 4: Writing to Sinks

# COMMAND ----------

# a) Console sink (debugging)
# query = rate_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start()

# b) Memory sink (testing)
# query = rate_df.writeStream \
#     .format("memory") \
#     .queryName("rate_table") \
#     .outputMode("append") \
#     .start()
# spark.sql("SELECT * FROM rate_table").show()

# c) File sink (Parquet)
# query = rate_df.writeStream \
#     .format("parquet") \
#     .option("path", "output/parquet") \
#     .option("checkpointLocation", "checkpoints/parquet") \
#     .outputMode("append") \
#     .start()

print("Sink examples defined!")

# COMMAND ----------

# d) Foreach sink (custom processing)
def process_row(row):
    print(f"Processing: {row}")

# ForeachBatch for batch processing
def process_batch(df, batch_id):
    print(f"Batch {batch_id}: {df.count()} rows")
    df.write.mode("append").parquet(f"output/batch_{batch_id}")

# query = rate_df.writeStream \
#     .foreachBatch(process_batch) \
#     .start()

print("ForeachBatch defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 5: Triggers

# COMMAND ----------

from pyspark.sql.streaming import Trigger

# a) Default - process as fast as possible
# .trigger(Trigger.ProcessingTime(0))

# b) Fixed interval
# .trigger(Trigger.ProcessingTime("10 seconds"))
# .trigger(processingTime="10 seconds")

# c) Once - process all available and stop
# .trigger(Trigger.Once())
# .trigger(once=True)

# d) Available-now (Spark 3.3+) - process all available, then stop
# .trigger(Trigger.AvailableNow())
# .trigger(availableNow=True)

print("Trigger options explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 6: Windowed Aggregations

# COMMAND ----------

# a) Tumbling window (non-overlapping)
tumbling = rate_df.groupBy(
    window(col("timestamp"), "10 seconds")
).agg(
    count("*").alias("count"),
    sum("value").alias("total")
)

print("Tumbling window defined!")
tumbling.printSchema()

# COMMAND ----------

# b) Sliding window (overlapping)
# Window of 10 seconds, sliding every 5 seconds
sliding = rate_df.groupBy(
    window(col("timestamp"), "10 seconds", "5 seconds")
).count()

print("Sliding window defined!")
sliding.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 7: Watermarks

# COMMAND ----------

# a) Define watermark for late data
windowed_with_watermark = rate_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes")
    ).count()

print("Watermark defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermark Behavior:
# MAGIC - **Watermark** = max event time seen - threshold
# MAGIC - Events arriving more than threshold late are dropped
# MAGIC - State for windows older than watermark is cleaned up
# MAGIC - Data older than watermark may be dropped (in append mode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 8: Stateful Operations

# COMMAND ----------

# a) Streaming aggregations (automatic state management)
# State is maintained for each group key
agg = rate_df.groupBy(
    window(col("timestamp"), "1 minute")
).agg(
    count("*").alias("count"),
    sum("value").alias("sum")
)

print("Stateful aggregation defined!")

# COMMAND ----------

# b) Streaming deduplication
# dedupe = stream_df \
#     .withWatermark("timestamp", "10 seconds") \
#     .dropDuplicates(["id", "timestamp"])

# State management:
# - State stored in checkpoints
# - Automatically cleaned up based on watermarks
# - Use flatMapGroupsWithState for custom state logic

print("Deduplication explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 9: Stream-Static Joins

# COMMAND ----------

# Load static dimension table
products = spark.read.csv("datasets/csv/products.csv", header=True, inferSchema=True)

# a) Join streaming with static
# enriched = streaming_sales.join(
#     products,
#     "product_id"
# )

# b) Use case: Enrich events with dimension data
# events.join(broadcast(dim_table), "key")

print("Stream-static join explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream-Static Join Considerations:
# MAGIC - Static table is read once at start (use broadcast for small tables)
# MAGIC - To refresh static data, restart query or use foreachBatch
# MAGIC - Inner, left outer (stream left), right outer (stream right) supported

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10: Stream-Stream Joins

# COMMAND ----------

# a) Inner join with watermarks
# joined = left_stream \
#     .withWatermark("timestamp", "10 minutes") \
#     .join(
#         right_stream.withWatermark("timestamp", "10 minutes"),
#         expr("""
#             left_stream.value = right_stream.value AND
#             left_stream.timestamp >= right_stream.timestamp AND
#             left_stream.timestamp <= right_stream.timestamp + interval 5 minutes
#         """),
#         "inner"
#     )

# Time-based constraints required for:
# - Outer joins
# - State cleanup

print("Stream-stream join explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11: Checkpointing

# COMMAND ----------

# a) Configure checkpoint location
# query = stream_df.writeStream \
#     .format("parquet") \
#     .option("path", "output/data") \
#     .option("checkpointLocation", "checkpoints/query1") \
#     .start()

print("Checkpoint configuration explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint Contents:
# MAGIC - **offsets/**: Input offsets processed
# MAGIC - **commits/**: Completed batch metadata
# MAGIC - **state/**: Stateful operation state
# MAGIC
# MAGIC ### Recovery:
# MAGIC - Query automatically resumes from last checkpoint
# MAGIC - Same checkpoint location must be used
# MAGIC - Query logic changes may require new checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12: Monitoring

# COMMAND ----------

# a) Query progress
# query.status  # Current status
# query.lastProgress  # Last micro-batch progress
# query.recentProgress  # Recent progress

# Example progress info:
progress_info = {
    "id": "query-uuid",
    "runId": "run-uuid",
    "batchId": 10,
    "numInputRows": 100,
    "inputRowsPerSecond": 50.0,
    "processedRowsPerSecond": 100.0,
    "durationMs": {},
    "stateOperators": [],
    "sources": [],
    "sink": {}
}

print("Progress monitoring explained!")

# COMMAND ----------

# b) StreamingQueryListener
from pyspark.sql.streaming import StreamingQueryListener

class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(f"Progress: {event.progress.numInputRows} rows")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

# spark.streams.addListener(MyListener())

print("StreamingQueryListener defined!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13: Kafka Integration

# COMMAND ----------

# a) Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "topic_name") \
#     .option("startingOffsets", "earliest") \
#     .load()

# Kafka DataFrame schema:
# key (binary), value (binary), topic, partition, offset, timestamp, timestampType

print("Kafka read explained!")

# COMMAND ----------

# b) Write to Kafka
# query = df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "output_topic") \
#     .option("checkpointLocation", "checkpoints/kafka") \
#     .start()

# c) Deserialize JSON from Kafka
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("action", StringType())
])

# parsed = kafka_df.select(
#     from_json(col("value").cast("string"), event_schema).alias("data")
# ).select("data.*")

print("Kafka write and parse explained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 14: Practical Streaming Pipeline

# COMMAND ----------

# Complete streaming pipeline example

# 1. Read from source
stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 100) \
    .load()

# 2. Transform
transformed = stream \
    .withColumn("event_time", col("timestamp")) \
    .withColumn("category", expr("CASE WHEN value % 2 = 0 THEN 'even' ELSE 'odd' END"))

# 3. Windowed aggregation with watermark
aggregated = transformed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("category")
    ).agg(
        count("*").alias("count"),
        sum("value").alias("total"),
        avg("value").alias("average")
    )

print("Complete pipeline defined!")
aggregated.printSchema()

# COMMAND ----------

# 4. Write to sink with checkpointing (console for demo)
# query = aggregated.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# 5. Monitor
# query.awaitTermination(30)  # Run for 30 seconds
# query.stop()

print("Pipeline write configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Summary
# MAGIC
# MAGIC | Component | Options |
# MAGIC |-----------|---------|
# MAGIC | **Sources** | Rate, File (CSV/JSON/Parquet), Kafka, Delta |
# MAGIC | **Sinks** | Console, Memory, File, Kafka, Delta, ForeachBatch |
# MAGIC | **Triggers** | ProcessingTime, Once, AvailableNow |
# MAGIC | **Output Modes** | Append, Complete, Update |
# MAGIC | **Windows** | Tumbling, Sliding, Session |
