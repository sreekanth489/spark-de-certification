"""
Structured Streaming - Solutions
=================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, from_json, to_json, current_timestamp,
    expr, sum, count, avg, max, min, explode, split,
    timestamp_seconds, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType, ArrayType
)

spark = SparkSession.builder \
    .appName("Streaming Solutions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# =============================================================================
# Problem 1: Reading from Sources
# =============================================================================
# a) Rate source (for testing) - generates rows per second
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

# Rate source schema: timestamp, value

# b) CSV directory (file source)
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

# c) JSON directory
json_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType())
])

# json_stream = spark.readStream \
#     .schema(json_schema) \
#     .json("path/to/json/dir")

# d) Parquet directory
# parquet_stream = spark.readStream \
#     .schema(parquet_schema) \
#     .parquet("path/to/parquet/dir")


# =============================================================================
# Problem 2: Basic Streaming Operations
# =============================================================================
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

# d) Aggregate (requires output mode complete or update)
# counted = rate_df.groupBy(
#     window(col("timestamp"), "10 seconds")
# ).count()


# =============================================================================
# Problem 3: Output Modes
# =============================================================================
# a) Append mode - new rows only (default)
# - Use with: non-aggregation queries, aggregations with watermark
# - Cannot use with: aggregations without watermark

# b) Complete mode - all rows
# - Use with: aggregations
# - Cannot use with: non-aggregation queries, queries with limits

# c) Update mode - only updated rows
# - Use with: aggregations (outputs only changed aggregates)
# - Cannot use with: sorting, queries needing complete results

# Example aggregation with complete mode
# query = rate_df.groupBy(window("timestamp", "10 seconds")) \
#     .count() \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()


# =============================================================================
# Problem 4: Writing to Sinks
# =============================================================================
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
#
# # Query the in-memory table
# spark.sql("SELECT * FROM rate_table").show()

# c) File sink (Parquet)
# query = rate_df.writeStream \
#     .format("parquet") \
#     .option("path", "output/parquet") \
#     .option("checkpointLocation", "checkpoints/parquet") \
#     .outputMode("append") \
#     .start()

# d) Foreach sink (custom processing)
def process_row(row):
    print(f"Processing: {row}")

# query = rate_df.writeStream \
#     .foreach(process_row) \
#     .start()

# ForeachBatch for batch processing
def process_batch(df, batch_id):
    print(f"Batch {batch_id}: {df.count()} rows")
    df.write.mode("append").parquet(f"output/batch_{batch_id}")

# query = rate_df.writeStream \
#     .foreachBatch(process_batch) \
#     .start()


# =============================================================================
# Problem 5: Triggers
# =============================================================================
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

# Example with trigger
# query = rate_df.writeStream \
#     .format("console") \
#     .trigger(processingTime="5 seconds") \
#     .start()


# =============================================================================
# Problem 6: Windowed Aggregations
# =============================================================================
# a) Tumbling window (non-overlapping)
tumbling = rate_df.groupBy(
    window(col("timestamp"), "10 seconds")
).agg(
    count("*").alias("count"),
    sum("value").alias("total")
)

# b) Sliding window (overlapping)
# Window of 10 seconds, sliding every 5 seconds
sliding = rate_df.groupBy(
    window(col("timestamp"), "10 seconds", "5 seconds")
).count()

# c) Session window (Spark 3.2+)
# session = rate_df.groupBy(
#     session_window(col("timestamp"), "5 minutes")
# ).count()

# d) Window with watermark (see Problem 7)


# =============================================================================
# Problem 7: Watermarks
# =============================================================================
# a) Define watermark for late data
windowed_with_watermark = rate_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes")
    ).count()

# b) Watermark threshold meaning:
# - Events arriving more than 10 minutes late are dropped
# - State for windows older than watermark is cleaned up

# c) Watermark behavior:
# - Watermark = max event time seen - threshold
# - Data older than watermark may be dropped (in append mode)
# - State older than watermark is eventually cleaned

# d) Handling late data:
# - Increase watermark threshold for more tolerance
# - Use update mode to update aggregates as late data arrives
# - Output late data to separate location for analysis


# =============================================================================
# Problem 8: Stateful Operations
# =============================================================================
# a) Streaming aggregations (automatic state management)
# State is maintained for each group key
agg = rate_df.groupBy(
    window(col("timestamp"), "1 minute")
).agg(
    count("*").alias("count"),
    sum("value").alias("sum")
)

# b) Streaming deduplication
# dedupe = stream_df \
#     .withWatermark("timestamp", "10 seconds") \
#     .dropDuplicates(["id", "timestamp"])

# c) Stream-stream joins (see Problem 10)

# d) State management
# - State stored in checkpoints
# - Automatically cleaned up based on watermarks
# - Use flatMapGroupsWithState for custom state logic


# =============================================================================
# Problem 9: Stream-Static Joins
# =============================================================================
# Load static dimension table
products = spark.read.csv("../datasets/csv/products.csv", header=True, inferSchema=True)

# a) Join streaming with static
# enriched = streaming_sales.join(
#     products,
#     "product_id"
# )

# b) Use case: Enrich events with dimension data
# events.join(broadcast(dim_table), "key")

# c) Considerations:
# - Static table is read once at start (use broadcast for small tables)
# - To refresh static data, restart query or use foreachBatch
# - Inner, left outer (stream left), right outer (stream right) supported


# =============================================================================
# Problem 10: Stream-Stream Joins
# =============================================================================
# Create two streams for demonstration
# left_stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
# right_stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

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

# b) Left outer join (requires watermark on right)
# left_outer = left_stream \
#     .withWatermark("timestamp", "10 minutes") \
#     .join(
#         right_stream.withWatermark("timestamp", "10 minutes"),
#         "value",
#         "leftOuter"
#     )

# c) Time-based constraints required for:
# - Outer joins
# - State cleanup


# =============================================================================
# Problem 11: Checkpointing
# =============================================================================
# a) Configure checkpoint location
# query = stream_df.writeStream \
#     .format("parquet") \
#     .option("path", "output/data") \
#     .option("checkpointLocation", "checkpoints/query1") \
#     .start()

# b) Checkpoint contents:
# - offsets/: Input offsets processed
# - commits/: Completed batch metadata
# - state/: Stateful operation state

# c) Recovery from checkpoint:
# - Query automatically resumes from last checkpoint
# - Same checkpoint location must be used
# - Query logic changes may require new checkpoint

# d) Checkpoint cleanup:
# - Automatic based on watermarks
# - Manual cleanup not recommended during query execution


# =============================================================================
# Problem 12: Schema Evolution
# =============================================================================
# a) New columns in source
# - Use schema with nullable new columns
# - Or use schema evolution options

# b) Schema enforcement
# schema = StructType([...])
# stream = spark.readStream.schema(schema).json("path")

# c) Options for changes:
# - Add new optional columns to schema
# - Use mergeSchema for Parquet/Delta
# - Version your schemas


# =============================================================================
# Problem 13: Monitoring
# =============================================================================
# a) Query progress
# query.status  # Current status
# query.lastProgress  # Last micro-batch progress
# query.recentProgress  # Recent progress

# Example progress info:
# {
#   "id": "query-uuid",
#   "runId": "run-uuid",
#   "batchId": 10,
#   "numInputRows": 100,
#   "inputRowsPerSecond": 50.0,
#   "processedRowsPerSecond": 100.0,
#   "durationMs": {...},
#   "stateOperators": [...],
#   "sources": [...],
#   "sink": {...}
# }

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

# c) Metrics
# Available in Spark UI under Streaming tab
# Or via REST API: /api/v1/applications/{app-id}/streaming/statistics


# =============================================================================
# Problem 14: Kafka Integration
# =============================================================================
# a) Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "topic_name") \
#     .option("startingOffsets", "earliest") \
#     .load()

# Kafka DataFrame schema:
# key (binary), value (binary), topic, partition, offset, timestamp, timestampType

# b) Write to Kafka
# query = df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "output_topic") \
#     .option("checkpointLocation", "checkpoints/kafka") \
#     .start()

# c) Handle key-value pairs
# parsed = kafka_df.select(
#     col("key").cast("string"),
#     col("value").cast("string"),
#     col("timestamp")
# )

# d) Deserialize JSON from Kafka
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("action", StringType())
])

# parsed = kafka_df.select(
#     from_json(col("value").cast("string"), event_schema).alias("data")
# ).select("data.*")


# =============================================================================
# Problem 15: Practical Streaming Pipeline
# =============================================================================
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

# 4. Write to sink with checkpointing
# query = aggregated.writeStream \
#     .format("parquet") \
#     .option("path", "output/streaming_results") \
#     .option("checkpointLocation", "checkpoints/pipeline") \
#     .outputMode("append") \
#     .trigger(processingTime="30 seconds") \
#     .start()

# 5. Monitor
# query.awaitTermination()

# For console testing:
query = aggregated.writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .start()

# query.awaitTermination(30)  # Run for 30 seconds
# query.stop()
