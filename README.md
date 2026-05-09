# E-Commerce Structured Streaming — Part II

Kafka → PySpark Structured Streaming → MongoDB pipeline for the Big Data Engineering final project.

## Architecture

```
S3 (CSV files)
     │
     ▼
┌─────────────┐     JSON messages      ┌───────────────────────────────────────┐
│  Producer   │ ──────────────────────▶│  Kafka Topic: ecommerce-orders        │
│  (Python)   │                        └───────────────────────────────────────┘
└─────────────┘                                          │
                                                         ▼
                                        ┌────────────────────────────────────────┐
                                        │   PySpark Structured Streaming         │
                                        │                                        │
                                        │  1. Ingest (Kafka source)              │
                                        │  2. Parse JSON + enforce schema        │
                                        │  3. Enrich (line_total, timestamp)     │
                                        │  4. Window agg: revenue by category    │
                                        │  5. Window agg: orders by country      │
                                        │  6. Window agg: payment method stats   │
                                        │  7. foreachBatch → MongoDB             │
                                        └────────────────────────────────────────┘
                                                         │
                                                         ▼
                                        ┌────────────────────────────────────────┐
                                        │   MongoDB (ecommerce database)         │
                                        │                                        │
                                        │  • revenue_by_category                 │
                                        │  • orders_by_country                   │
                                        │  • payment_method_stats                │
                                        │  • orders_raw                          │
                                        └────────────────────────────────────────┘
```

## Stack

| Component         | Technology                          |
| ----------------- | ----------------------------------- |
| Message broker    | Apache Kafka (wurstmeister)         |
| Stream processor  | Apache Spark 4.0.1 (PySpark)        |
| Scala version     | 2.13                                |
| Java version      | 17                                  |
| Notebook          | Jupyter (via spark-base image)      |
| Database          | MongoDB 7.0                         |
| Kafka connector   | `spark-sql-kafka-0-10_2.13:4.0.0`   |
| MongoDB connector | `mongo-spark-connector_2.13:10.3.0` |
| Orchestration     | Docker Compose                      |

## Prerequisites

- Docker
- Docker Compose v2
- AWS credentials with read access to the S3 dataset bucket

## Setup

1. Copy the environment file and fill in your credentials:

```bash
cp .env.example .env
# edit .env with your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
```

2. Build the Spark base image first (required by master and worker):

```bash
docker build -t spark-base:4.0 ./docker/spark-base
```

3. Start the full stack:

```bash
docker compose up --build -d zookeeper kafka mongo spark-master spark-worker spark-notebook
```

4. Open the notebook at http://localhost:8888 — `consumer/consumer.ipynb` will be available there.

5. Run all cells in `consumer.ipynb` to start the streaming queries.

6. In a separate terminal, shell into the notebook container and run the producer:

```bash
docker exec -it spark-notebook /bin/bash

python3 /opt/spark/work-dir/src/producers/producer.py \
    --broker kafka:9093 \
    --topic ecommerce-orders \
    --records 100 \
    --delay 0.2
```

## Monitoring

| UI               | URL                                                                       |
| ---------------- | ------------------------------------------------------------------------- |
| Spark Master     | http://localhost:9090                                                     |
| Jupyter Notebook | http://localhost:8888                                                     |
| Spark App UI     | http://localhost:4040 (only while a job is running)                       |
| Kafka (via CLI)  | `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| MongoDB          | `docker exec -it mongo mongosh`                                           |

## MongoDB Validation Queries

Connect to MongoDB:

```bash
docker exec -it mongo mongosh ecommerce
```

### 1. Top 10 categories by total revenue

```javascript
db.revenue_by_category.aggregate([
  {
    $group: {
      _id: "$category",
      total_revenue: { $sum: "$total_revenue" },
      total_orders: { $sum: "$total_orders" },
    },
  },
  { $sort: { total_revenue: -1 } },
  { $limit: 10 },
]);
```

### 2. Top 5 shipping countries by order count

```javascript
db.orders_by_country.aggregate([
  {
    $group: {
      _id: "$shipping_country",
      order_count: { $sum: "$order_count" },
      total_revenue: { $sum: "$total_revenue" },
    },
  },
  { $sort: { order_count: -1 } },
  { $limit: 5 },
]);
```

### 3. Payment method distribution

```javascript
db.payment_method_stats.aggregate([
  {
    $group: {
      _id: "$payment_method",
      total_transactions: { $sum: "$transaction_count" },
      total_amount: { $sum: "$total_amount" },
    },
  },
  { $sort: { total_transactions: -1 } },
]);
```

### 4. Validate raw events were persisted

```javascript
db.orders_raw.findOne();
db.orders_raw.countDocuments();
```

## Project Structure

```
.
├── docker-compose.yml
├── .env.example
├── src/
│   ├── spark_utils.py              # SparkUtils helper (session creation)
│   └── producers/
│       └── producer.py             # CLI script: S3 → Kafka
├── consumer/
│   ├── consumer.py                 # Kafka → PySpark → MongoDB (script version)
│   └── consumer.ipynb              # Notebook version (primary)
└── docker/
    ├── spark-base/Dockerfile       # spark:4.0.1-scala2.13-java17-ubuntu + Jupyter
    ├── spark-master/
    │   ├── Dockerfile
    │   └── entrypoint.sh           # starts Master process in foreground
    └── spark-worker/
        ├── Dockerfile
        └── entrypoint.sh           # starts Worker process in foreground
```
