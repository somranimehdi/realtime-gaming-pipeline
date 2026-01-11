from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, count, concat, lit, window
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("GameEventsTimeAggToES") \
    .getOrCreate()

# 2. Define Kafka source
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "game-events"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as message")

# 3. Define schema & Parse
schema = StructType() \
    .add("event_time", StringType()) \
    .add("player_id", StringType()) \
    .add("match_id", StringType()) \
    .add("event_type", StringType()) \
    .add("map", StringType()) \
    .add("player_level", IntegerType()) \
    .add("region", StringType())

df_parsed = df.withColumn("data", from_json(col("message"), schema)).select("data.*")
# Convert to timestamp for windowing
df_parsed = df_parsed.withColumn("event_time", to_timestamp("event_time"))

# -----------------------------
# 4. Time-Based Aggregation
# -----------------------------
# We add a Watermark of 10 minutes (allows for late data)
# We group by a 1-minute window, region, and event_type
agg_df = df_parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"), 
        col("region"), 
        col("event_type")
    ).agg(count("*").alias("event_count"))

# Flatten the window and create a unique ID including the timestamp
# This ensures each minute has its own record in ES
agg_df_final = agg_df.select(
    col("window.start").alias("event_window"),
    col("region"),
    col("event_type"),
    col("event_count")
).withColumn("doc_id", concat(col("region"), lit("_"), col("event_type"), lit("_"), col("event_window").cast("string")))

# -----------------------------
# 5. Write to Elasticsearch
# -----------------------------
def write_to_elasticsearch(batch_df, batch_id):
    batch_df.write \
        .format("es") \
        .mode("append") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", "game-events-time-agg") \
        .option("es.mapping.id", "doc_id") \
        .option("es.write.operation", "upsert") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.ssl", "false") \
        .save()

query = agg_df_final.writeStream \
    .foreachBatch(write_to_elasticsearch) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoints-time") \
    .start()

query.awaitTermination()