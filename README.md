# spark-streaming-kafka

> PySpark Structured Streaming pipeline over Kafka — windowed aggregations with watermarking, a Kafka dead-letter route for malformed records, and JDBC sink into PostgreSQL.

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-E25A1C?logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-source%2Fsink-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-JDBC%20sink-4169E1?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Status](https://img.shields.io/badge/status-working%20core-success)

---

## Overview / Aim

A **stateful stream-processing** pipeline that consumes payment events from Kafka,
computes **windowed per-user aggregates** with event-time semantics, and persists
results to PostgreSQL — while routing malformed records to a Kafka dead-letter
topic. It demonstrates the core competencies of streaming data engineering:
**event-time windowing, watermarking for late data, checkpointed fault tolerance,
and an idempotent sink pattern**.

## Architecture / How It Works

```
Kafka (payments)
       │  readStream (startingOffsets=latest)
       ▼
from_json → typed schema (event_id, user_id, amount, ts)
       │
       ├─ valid ──▶ withWatermark("ts","10 min")
       │            groupBy window(1h, slide 30m), user_id
       │            Σ amount · count · avg
       │            foreachBatch → JDBC append ──▶ PostgreSQL (hourly_totals)
       │
       └─ malformed ──▶ to_json(struct(*)) ──▶ Kafka (payments_dlq)

Checkpointing: spark.sql.streaming.checkpointLocation
```

- **Event-time windowing** — sliding 1-hour windows advancing every 30 minutes,
  keyed by `user_id`.
- **Watermarking** — a 10-minute watermark bounds state and tolerates late-arriving
  events without unbounded memory growth.
- **Idempotent sink** — `foreachBatch` writes each micro-batch to Postgres over
  JDBC, the standard pattern for exactly-once-style external sinks.
- **Dead-letter routing** — records carrying a parse error are serialised and
  pushed to `payments_dlq` rather than dropped.

> The repository's `docs/architecture.md` records design targets for this pipeline
> (e.g. a ~800 ms p50 end-to-end target, 10-minute watermark, 30 s checkpoint
> cadence). These are documented design intentions, not independently benchmarked
> results.

## Tech Stack & Tools

| Tool | Role |
|------|------|
| **PySpark 3.4 (Structured Streaming)** | Stream processing engine |
| **spark-sql-kafka-0-10** | Kafka source/sink connector |
| **Apache Kafka** | Event source + dead-letter sink |
| **PostgreSQL (JDBC)** | Aggregate sink (`hourly_totals`) |
| **Docker Compose** | Local Kafka/Zookeeper (via Makefile) |
| **pytest** | Local `SparkSession` schema/transform tests |

## Project Structure

```
spark-streaming-kafka/
├── src/
│   ├── consumer.py        # readStream from Kafka + from_json typed parse
│   ├── aggregations.py    # watermarked windowed aggs + foreachBatch JDBC sink
│   └── dlq_handler.py     # route malformed records to payments_dlq
├── tests/
│   └── test_aggregations.py   # local SparkSession schema/transform checks
├── conf/
│   └── spark-defaults.conf    # executor mem/cores, shuffle partitions, checkpoint
├── docs/
│   └── architecture.md        # data-flow diagram + design targets
└── Makefile                   # start-kafka / submit-job / test
```

## Key Features / Highlights

- **Typed JSON ingestion** — an explicit `StructType` schema parses Kafka values,
  rejecting shape drift early.
- **Event-time sliding windows** — 1-hour windows with 30-minute slide per user,
  emitting sum/count/avg of `amount`.
- **Watermark-bounded state** — 10-minute watermark caps state size and handles
  late data deterministically.
- **`foreachBatch` JDBC sink** — micro-batch writes to PostgreSQL, the idiomatic
  way to land streaming aggregates in a relational warehouse.
- **Dead-letter handling** — malformed records are quarantined to a Kafka DLQ topic
  with a `dlq_ts`, never silently lost.
- **Tuned runtime config** — `conf/spark-defaults.conf` sets executor resources,
  shuffle partitions, per-partition max rate, and the checkpoint location.

## Challenges

- **Late & out-of-order events** — reconciling correctness with bounded state via
  watermarking.
- **Streaming-to-relational sink** — `foreachBatch` is used to bridge a continuous
  stream into JDBC batch writes with checkpointed recovery.
- **Backpressure** — `maxRatePerPartition` caps intake to protect downstream
  Postgres.

## Future Work

- Move sink credentials out of code into secrets / Spark conf.
- Avro + Schema Registry deserialisation.
- Exactly-once sink via idempotent upserts / transactional staging.
- Structured Streaming metrics → Prometheus/Grafana.
- Kubernetes / Spark-on-K8s deployment manifests.

## Getting Started / Usage

```bash
# Start Kafka + Zookeeper
make start-kafka

# Submit the streaming job (pulls the Kafka connector package)
make submit-job
# == spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 src/consumer.py

# Run tests (local SparkSession)
make test
```

## Conclusion

Demonstrates **real-time data engineering** with Spark Structured Streaming:
event-time windowing, watermarking, checkpointed fault tolerance, a dead-letter
strategy, and a relational sink — the end-to-end shape of a production streaming
aggregation pipeline.
