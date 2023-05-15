# Architecture 2023-05-15

```
Kafka (payments) → PySpark Structured Streaming → PostgreSQL (hourly_totals)
                                ↓ malformed
                         Kafka (payments_dlq)
```

## Latency
- End-to-end p50: ~800ms
- Watermark: 10 min (tolerate late data)
- Checkpoint: every 30s
