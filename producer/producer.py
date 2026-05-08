"""
E-Commerce Kafka Producer
─────────────────────────
Reads orders + order_items CSV files from S3, joins them in memory,
and streams each enriched order event as a JSON message into a Kafka topic.

Environment variables (set via .env / docker-compose):
  AWS_ACCESS_KEY_ID       – AWS credentials
  AWS_SECRET_ACCESS_KEY
  AWS_REGION              – default: us-east-1
  S3_BUCKET               – bucket name
  S3_PREFIX               – key prefix (e.g. "data")
  KAFKA_BOOTSTRAP_SERVERS – e.g. kafka:9092
  KAFKA_TOPIC             – target topic name
  BATCH_SIZE              – rows to send per run  (default: 5000)
  DELAY_MS                – ms between messages   (default: 100)
"""

import json
import logging
import os
import time
from io import StringIO

import boto3
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "pdm-project-e-commerce-dataset")
S3_PREFIX             = os.getenv("S3_PREFIX", "data")
KAFKA_SERVERS         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC           = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
BATCH_SIZE            = int(os.getenv("BATCH_SIZE", "5000"))
DELAY_MS              = int(os.getenv("DELAY_MS", "100"))


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def list_csv_keys(client, prefix: str) -> list[str]:
    """Return all object keys under a given S3 prefix."""
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def read_s3_csv(client, key: str) -> pd.DataFrame:
    """Download a single CSV from S3 and return it as a DataFrame."""
    log.info("Reading s3://%s/%s", S3_BUCKET, key)
    response = client.get_object(Bucket=S3_BUCKET, Key=key)
    body = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(body))


def load_table(client, table_name: str) -> pd.DataFrame:
    """Load all CSV part-files for a given table into a single DataFrame."""
    prefix = f"{S3_PREFIX}/{table_name}/"
    keys = list_csv_keys(client, prefix)
    if not keys:
        raise FileNotFoundError(f"No CSV files found at s3://{S3_BUCKET}/{prefix}")
    frames = [read_s3_csv(client, k) for k in sorted(keys)]
    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded table '%s': %d rows", table_name, len(df))
    return df


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def build_producer(retries: int = 10, wait: int = 5) -> KafkaProducer:
    """Create a KafkaProducer, retrying until the broker is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d). Retrying in %ds…", attempt, retries, wait)
            time.sleep(wait)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    client = s3_client()

    # Load the four tables
    orders      = load_table(client, "orders")
    order_items = load_table(client, "order_items")
    customers   = load_table(client, "customers")
    products    = load_table(client, "products")

    # ── Enrich order_items with order and product metadata ─────────────────────
    # Each Kafka message represents one order-item event enriched with:
    #   - order context  (customer_id, order_date, total_amount, payment_method, shipping_country)
    #   - product context (product_name, category, price, brand)
    enriched = (
        order_items
        .merge(
            orders[["order_id", "customer_id", "order_date",
                    "total_amount", "payment_method", "shipping_country"]],
            on="order_id",
            how="left",
        )
        .merge(
            products[["product_id", "product_name", "category", "price", "brand"]],
            on="product_id",
            how="left",
        )
        .merge(
            customers[["customer_id", "name", "country"]],
            on="customer_id",
            how="left",
        )
    )

    # Rename to avoid column name collisions after merge
    enriched.rename(columns={"price": "product_price", "name": "customer_name"}, inplace=True)

    # Sample to BATCH_SIZE rows (reproducible)
    sample = enriched.sample(n=min(BATCH_SIZE, len(enriched)), random_state=42).reset_index(drop=True)
    log.info("Sending %d enriched order-item events to topic '%s'", len(sample), KAFKA_TOPIC)

    producer = build_producer()
    delay_s  = DELAY_MS / 1000.0

    for idx, row in sample.iterrows():
        message = row.to_dict()
        producer.send(KAFKA_TOPIC, value=message)

        if (idx + 1) % 500 == 0:
            log.info("Sent %d / %d messages…", idx + 1, len(sample))

        time.sleep(delay_s)

    producer.flush()
    log.info("Done. All %d messages sent to Kafka topic '%s'.", len(sample), KAFKA_TOPIC)


if __name__ == "__main__":
    main()
