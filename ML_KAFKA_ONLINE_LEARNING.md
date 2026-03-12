# Kafka-Based River Online Learning

## Overview

This implementation consumes directly from the `funnel` Kafka topic for true real-time online learning, bypassing the need to poll RisingWave.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     RisingWave Stream                        │
│              (aggregates events to funnel topic)             │
└──────────────────────────┬──────────────────────────────────┘
                           │ Kafka topic 'funnel'
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 ml/online/kafka_streamer.py                  │
│            (KafkaConsumer consuming funnel topic)            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 ml/online/kafka_learner.py                   │
│  - Maintains rolling history window                          │
│  - Computes lag features (like RisingWave)                   │
│  - Calls learn_one() for each new record                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              ml/serving/kafka_river_predictor.py             │
│              FastAPI with real-time predictions              │
└─────────────────────────────────────────────────────────────┘
```

## Why Kafka vs RisingWave Polling?

| Aspect | RisingWave Polling | Kafka Direct |
|--------|-------------------|--------------|
| **Latency** | Poll interval (5s) | Near real-time (ms) |
| **Coupling** | Depends on RisingWave availability | Depends only on Kafka |
| **Complexity** | Simple SQL queries | Requires lag feature computation |
| **Deduplication** | Handled by RisingWave | Implemented in streamer |
| **Backpressure** | Limited by poll interval | Automatic via consumer groups |

## Quick Start

### 1. Start with Kafka Source

```bash
# Using the convenience script
./bin/4_run_ml_kafka.sh

# Or manually
export USE_ONLINE_LEARNING=true
export USE_KAFKA_SOURCE=true
./bin/4_run_ml_serving.sh
```

### 2. Verify

```bash
# Health check - should show source: kafka
curl http://localhost:8001/health

# Response:
# {
#   "healthy": true,
#   "mode": "online",
#   "source": "kafka",
#   "kafka_connected": true
# }

# Get predictions
curl http://localhost:8001/predict

# Check detailed status
curl http://localhost:8001/online/status
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_ONLINE_LEARNING` | `false` | Enable River online learning |
| `USE_KAFKA_SOURCE` | `false` | Use Kafka instead of RisingWave polling |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka broker addresses |
| `CHECKPOINT_INTERVAL` | `60` | Seconds between MinIO checkpoints |

## How Lag Features Work

Since we're consuming directly from Kafka, we compute lag features locally:

```python
# History window maintains last 20 records (20-second windows)
history = deque(maxlen=20)

# For each new record:
# - lag_1 = history[-1] (20 seconds ago)
# - lag_2 = history[-4] (~1 minute ago)
# - lag_3 = history[-7] (~2 minutes ago)
# - TPS computed from consecutive differences
```

This mirrors the `funnel_training` view in RisingWave but computed in-memory.

## Deduplication

RisingWave emits updates to Kafka (retractions + new values). We deduplicate by:

```python
# Track seen window_start values
seen_windows = set()

# Skip if we've seen this window before
if window_start in seen_windows:
    return
```

This ensures we don't double-count when RisingWave emits corrections.

## Files

```
ml/online/
├── kafka_streamer.py      # Kafka consumer for funnel topic
└── kafka_learner.py       # Online learner with lag feature computation

ml/serving/
├── kafka_river_predictor.py  # Predictor using Kafka-based learner
└── main.py                   # Updated to support both sources

bin/
└── 4_run_ml_kafka.sh      # Launcher for Kafka-based mode
```

## Comparison: All Three Modes

### 1. Batch Mode (Original)
```bash
./bin/4_run_ml_serving.sh
```
- Dagster trains models every 5 minutes
- Models loaded from MinIO
- Good for stability/auditability

### 2. Online Learning (RisingWave)
```bash
./bin/4_run_ml_online.sh
# or: USE_ONLINE_LEARNING=true ./bin/4_run_ml_serving.sh
```
- Polls RisingWave every 5 seconds
- Models learn incrementally
- Good for demos with minimal setup

### 3. Online Learning (Kafka) ⭐ Recommended
```bash
./bin/4_run_ml_kafka.sh
# or: USE_ONLINE_LEARNING=true USE_KAFKA_SOURCE=true ./bin/4_run_ml_serving.sh
```
- Consumes Kafka directly
- Lowest latency, highest throughput
- Best for production streaming

## Monitoring

```bash
# Watch Kafka consumer lag
curl -s http://localhost:8001/online/status | jq '.history_size, .records_learned, .kafka_connected'

# Check model readiness
curl -s http://localhost:8001/models | jq '.models[].ready'

# Get predictions with source info
curl -s http://localhost:8001/predict | jq '.source, .viewers.model_type'
```

## Troubleshooting

### Kafka not connected
```bash
# Check Kafka is running
docker ps | grep kafka

# Verify topic exists
docker exec -it <kafka-container> kafka-topics --list --bootstrap-server localhost:9092
```

### No predictions
- Models need 5+ samples before predicting
- Check `records_learned` in `/online/status`
- Verify funnel topic has data: check modern-dashboard backend logs

### High memory usage
- History window is limited to 20 records per metric
- Checkpoints every 60 seconds by default
- Reduce `CHECKPOINT_INTERVAL` if needed