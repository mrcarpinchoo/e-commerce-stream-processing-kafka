"""
E-Commerce Kafka Producer
=========================
Procesamiento de Datos Masivos | ITESO

Reads the e-commerce dataset from S3 (orders + order_items + products + customers),
joins them into enriched order-item events, and sends each event as a JSON message
to a Kafka topic with a configurable delay between records.

Usage (from inside the spark-notebook container):
  python3 /opt/spark/work-dir/src/producers/producer.py \\
      --broker kafka:9093 \\
      --topic ecommerce-orders \\
      --records 5000 \\
      --delay 0.5

Dependencies:
  pip install kafka-python boto3 pandas
"""

import argparse
import json
import time
from io import StringIO

import boto3
import pandas as pd
from kafka import KafkaProducer

# ── S3 config (read from environment or hardcoded defaults) ───────────────────
import os

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.environ.get("AWS_SESSION_TOKEN")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET             = os.environ.get("S3_BUCKET", "pdm-project-e-commerce-dataset")
S3_PREFIX             = os.environ.get("S3_PREFIX", "data")

DELAY_MIN = 0.1   # seconds (default lower bound)
DELAY_MAX = 0.5   # seconds (default upper bound)


# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )


def load_table(client, table_name: str) -> pd.DataFrame:
    """Load all CSV part-files for a table from S3 into a single DataFrame."""
    prefix = f"{S3_PREFIX}/{table_name}/"
    paginator = client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    if not keys:
        raise FileNotFoundError(f"No CSV files found at s3://{S3_BUCKET}/{prefix}")

    frames = []
    for key in sorted(keys):
        print(f"  Reading s3://{S3_BUCKET}/{key}")
        body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
        frames.append(pd.read_csv(StringIO(body)))

    df = pd.concat(frames, ignore_index=True)
    print(f"  Loaded '{table_name}': {len(df):,} rows")
    return df


# ── Producer ──────────────────────────────────────────────────────────────────

def build_enriched_dataset(client) -> pd.DataFrame:
    """Join the four tables into enriched order-item events."""
    print("\nLoading tables from S3...")
    orders      = load_table(client, "orders")
    order_items = load_table(client, "order_items")
    customers   = load_table(client, "customers")
    products    = load_table(client, "products")

    print("\nJoining tables...")
    enriched = (
        order_items
        .merge(
            orders[["order_id", "customer_id", "order_date",
                    "total_amount", "payment_method", "shipping_country"]],
            on="order_id", how="left",
        )
        .merge(
            products[["product_id", "product_name", "category", "price", "brand"]],
            on="product_id", how="left",
        )
        .merge(
            customers[["customer_id", "name", "country"]],
            on="customer_id", how="left",
        )
    )
    enriched.rename(columns={"price": "product_price", "name": "customer_name"}, inplace=True)
    print(f"  Enriched dataset: {len(enriched):,} rows\n")
    return enriched


def run_producer(args):
    client   = s3_client()
    enriched = build_enriched_dataset(client)

    # Sample the requested number of records (reproducible)
    n_records = args.records if args.records > 0 else len(enriched)
    sample = enriched.sample(n=min(n_records, len(enriched)), random_state=42).reset_index(drop=True)

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda msg: json.dumps(msg, default=str).encode("utf-8"),
        acks="all",
    )

    print(f"Connected to broker : {args.broker}")
    print(f"Topic               : {args.topic}")
    print(f"Records to send     : {len(sample):,}")
    print(f"Delay between records: {args.delay}s")
    print("-" * 55)

    try:
        for idx, row in sample.iterrows():
            producer.send(args.topic, value=row.to_dict())
            producer.flush()

            print(f"[{idx + 1}] Sent order_item_id={row.get('order_item_id', '?')}  "
                  f"product={row.get('product_name', '?')}  "
                  f"category={row.get('category', '?')}")

            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()
        print(f"\nDone. Total records sent: {idx + 1}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream enriched e-commerce order-item events to a Kafka topic."
    )
    parser.add_argument(
        "--broker",
        default="kafka:9093",
        help="Kafka broker address (default: kafka:9093).",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce-orders",
        help="Kafka topic name (default: ecommerce-orders).",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=5000,
        help="Number of records to send. 0 means send all (default: 5000).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to wait between records (default: 0.2).",
    )
    return parser


def main():
    parser = build_parser()
    args   = parser.parse_args()
    run_producer(args)


if __name__ == "__main__":
    main()
