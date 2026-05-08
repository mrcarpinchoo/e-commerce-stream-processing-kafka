"""
E-Commerce PySpark Structured Streaming Consumer
─────────────────────────────────────────────────
Reads enriched order-item events from a Kafka topic and applies a multi-stage
transformation pipeline before persisting results to MongoDB via foreachBatch.

Pipeline stages
───────────────
1. Ingest        – read JSON messages from Kafka
2. Parse         – deserialize JSON payload, enforce schema, cast types
3. Enrich        – derive computed columns (line_total, event_timestamp)
4. Aggregation A – windowed revenue + order count per product category
                   (1-minute tumbling window, 30-second watermark)
5. Aggregation B – running order count per shipping country
                   (append mode with watermark)
6. Aggregation C – payment method distribution per window
7. Sink          – write each micro-batch to MongoDB via foreachBatch

MongoDB collections
───────────────────
  ecommerce.revenue_by_category   – windowed category aggregates
  ecommerce.orders_by_country     – windowed country aggregates
  ecommerce.payment_method_stats  – windowed payment method counts
  ecommerce.orders_raw            – enriched raw events (for validation)

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  – e.g. kafka:9092
  KAFKA_TOPIC              – source topic
  MONGO_URI                – e.g. mongodb://mongo:27017
  MONGO_DATABASE           – target database (default: ecommerce)
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")
CHECKPOINT_DIR = "/tmp/spark-checkpoints"


# ── Schema ─────────────────────────────────────────────────────────────────────
# Matches the enriched order-item events produced by producer.py
ORDER_ITEM_SCHEMA = StructType([
    StructField("order_item_id",   StringType(),    True),
    StructField("order_id",        StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("quantity",        IntegerType(),   True),
    StructField("unit_price",      DoubleType(),    True),
    StructField("customer_id",     StringType(),    True),
    StructField("order_date",      StringType(),    True),   # parsed below
    StructField("total_amount",    DoubleType(),    True),
    StructField("payment_method",  StringType(),    True),
    StructField("shipping_country",StringType(),    True),
    StructField("product_name",    StringType(),    True),
    StructField("category",        StringType(),    True),
    StructField("product_price",   DoubleType(),    True),
    StructField("brand",           StringType(),    True),
    StructField("customer_name",   StringType(),    True),
    StructField("country",         StringType(),    True),
])


# ── Spark session ──────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ECommerceStreamingConsumer")
        .master("spark://spark-master:7077")
        # MongoDB connector config
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        # Serializer tuning
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Streaming micro-batch interval
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


# ── Stage 1 & 2: Ingest + Parse ───────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw bytes from Kafka and deserialize the JSON payload."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka delivers value as binary; cast to string then parse JSON
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), ORDER_ITEM_SCHEMA).alias("data"))
        .select("data.*")
    )
    return parsed


# ── Stage 3: Enrich ───────────────────────────────────────────────────────────
def enrich(df: DataFrame) -> DataFrame:
    """
    Derive computed columns:
      - line_total       : quantity × unit_price
      - event_timestamp  : parse order_date string → timestamp (used for windowing)
      - processing_time  : current_timestamp() for auditing
    """
    return (
        df
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("order_date"), "yyyy-MM-dd"),
        )
        .withColumn("processing_time", F.current_timestamp())
        .filter(F.col("event_timestamp").isNotNull())   # drop unparseable rows
        .filter(F.col("line_total") > 0)                # sanity filter
    )


# ── Stage 4: Windowed revenue per category ────────────────────────────────────
def revenue_by_category(df: DataFrame) -> DataFrame:
    """
    Tumbling 1-day window (matches the daily granularity of order_date),
    30-day watermark to handle late data.

    Output columns:
      window_start, window_end, category, total_revenue, total_orders, avg_order_value
    """
    return (
        df
        .withWatermark("event_timestamp", "30 days")
        .groupBy(
            F.window("event_timestamp", "1 day").alias("window"),
            F.col("category"),
        )
        .agg(
            F.sum("line_total").alias("total_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("line_total").alias("avg_line_value"),
            F.sum("quantity").alias("total_units_sold"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            F.round("total_revenue", 2).alias("total_revenue"),
            "total_orders",
            F.round("avg_line_value", 2).alias("avg_line_value"),
            "total_units_sold",
        )
    )


# ── Stage 5: Order count per shipping country ─────────────────────────────────
def orders_by_country(df: DataFrame) -> DataFrame:
    """
    Tumbling 1-day window grouped by shipping_country.

    Output columns:
      window_start, window_end, shipping_country, order_count, total_revenue
    """
    return (
        df
        .withWatermark("event_timestamp", "30 days")
        .groupBy(
            F.window("event_timestamp", "1 day").alias("window"),
            F.col("shipping_country"),
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("line_total").alias("total_revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "shipping_country",
            "order_count",
            F.round("total_revenue", 2).alias("total_revenue"),
        )
    )


# ── Stage 6: Payment method distribution ──────────────────────────────────────
def payment_method_stats(df: DataFrame) -> DataFrame:
    """
    Tumbling 1-day window grouped by payment_method.

    Output columns:
      window_start, window_end, payment_method, transaction_count, total_amount
    """
    return (
        df
        .withWatermark("event_timestamp", "30 days")
        .groupBy(
            F.window("event_timestamp", "1 day").alias("window"),
            F.col("payment_method"),
        )
        .agg(
            F.count("order_item_id").alias("transaction_count"),
            F.sum("total_amount").alias("total_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "payment_method",
            "transaction_count",
            F.round("total_amount", 2).alias("total_amount"),
        )
    )


# ── Stage 7: MongoDB sink via foreachBatch ────────────────────────────────────
def make_mongo_writer(collection: str, upsert_keys: list[str]):
    """
    Returns a foreachBatch function that upserts a micro-batch DataFrame
    into the specified MongoDB collection.

    Upsert logic: replaceOne with filter on upsert_keys (e.g. window + category).
    """
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()
        print(f"[batch {batch_id}] Writing {row_count} rows to {MONGO_DATABASE}.{collection}")

        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", collection)
            .save()
        )

    return write_batch


def make_raw_writer(collection: str = "orders_raw"):
    """foreachBatch writer for the raw enriched events (no aggregation)."""
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()
        print(f"[batch {batch_id}] Writing {row_count} raw events to {MONGO_DATABASE}.{collection}")

        (
            batch_df
            # Drop the window-related columns not needed in raw sink
            .drop("processing_time")
            .write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", collection)
            .save()
        )

    return write_batch


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Ingest + Parse
    raw_stream = read_kafka_stream(spark)

    # ── Enrich (shared base for all aggregations)
    enriched = enrich(raw_stream)

    # ── Aggregation streams
    cat_stream     = revenue_by_category(enriched)
    country_stream = orders_by_country(enriched)
    payment_stream = payment_method_stats(enriched)

    # ── Query 1: revenue by category → MongoDB
    q1 = (
        cat_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/revenue_by_category")
        .foreachBatch(make_mongo_writer("revenue_by_category", ["window_start", "category"]))
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Query 2: orders by country → MongoDB
    q2 = (
        country_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders_by_country")
        .foreachBatch(make_mongo_writer("orders_by_country", ["window_start", "shipping_country"]))
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Query 3: payment method stats → MongoDB
    q3 = (
        payment_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/payment_method_stats")
        .foreachBatch(make_mongo_writer("payment_method_stats", ["window_start", "payment_method"]))
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Query 4: raw enriched events → MongoDB (for validation queries)
    q4 = (
        enriched.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders_raw")
        .foreachBatch(make_raw_writer("orders_raw"))
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Streaming queries started. Waiting for termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
