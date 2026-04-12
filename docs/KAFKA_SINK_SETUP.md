# RisingWave Kafka Sink for Funnel Dashboard

This document describes the new architecture that streams funnel analytics data from RisingWave to Kafka, and how the modern dashboard consumes from it.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Sources │────▶│    RisingWave   │     │                 │
│  (page_views,   │     │                 │     │                 │
│   cart_events,  │     │  funnel MV      │────▶│  Kafka Sink     │
│   purchases)    │     │  (1-min window) │     │  (funnel topic) │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                              ┌──────────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Dashboard      │
                    │  Backend API    │
                    │  (Kafka Consumer)│
                    └────────┬────────┘
                             │
                              ▼
                    ┌─────────────────┐
                    │  React Frontend │
                    │  (HTTP polling) │
                    └─────────────────┘
```

## Components

### 1. Kafka Sink Model (`dbt/models/sink_funnel_to_kafka.sql`)

Creates a RisingWave sink that publishes funnel data to the `funnel` Kafka topic.

```sql
CREATE SINK IF NOT EXISTS funnel_kafka_sink
FROM funnel
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'funnel'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
```

### 2. Dashboard Backend (`modern-dashboard/backend/api.py`)

The backend now consumes from the Kafka `funnel` topic instead of querying RisingWave directly.

**Key Changes:**
- Background Kafka consumer thread started on FastAPI startup
- In-memory cache of latest funnel data
- Maintains history of last 100 records for charts
- All endpoints (`/api/funnel`, `/api/stats`, `/api/last-event-time`) serve from cache

**Configuration:**
```python
KAFKA_BOOTSTRAP_SERVERS = ['redpanda:9092']
FUNNEL_TOPIC = 'funnel'
```

## Setup Instructions

### 1. Deploy the Kafka Sink

```bash
# Run dbt to create the funnel materialized view and Kafka sink
cd dbt && dbt run --models funnel sink_funnel_to_kafka
```

Or from the project root:
```bash
./bin/3_run_dbt.sh
```

### 2. Start the Dashboard

```bash
# Start the modern dashboard (backend + frontend)
./bin/4_run_modern.sh
```

The backend will automatically connect to Kafka and start consuming from the `funnel` topic.

### 3. Verify Data Flow

**Check Kafka topic exists:**
```bash
docker exec -it redpanda rpk topic list --brokers localhost:9092
```

**Consume messages manually:**
```bash
python scripts/consume_funnel_from_kafka.py
```

Or using rpk:
```bash
docker exec -it redpanda rpk topic consume funnel --brokers localhost:9092
```

## API Endpoints

All endpoints return data from the in-memory cache maintained by the Kafka consumer.

### GET `/api/funnel`
Returns the last 100 funnel records (oldest first for chart compatibility).

```json
[
  {
    "window_start": "2026-03-03T04:25:00",
    "window_end": "2026-03-03T04:26:00",
    "viewers": 150,
    "carters": 45,
    "purchasers": 12,
    "view_to_cart_rate": 0.30,
    "cart_to_buy_rate": 0.27
  }
]
```

### GET `/api/stats`
Returns the latest stats and percentage changes from previous window.

```json
{
  "latest": {
    "viewers": 150,
    "carters": 45,
    "purchasers": 12,
    "view_to_cart_rate": 0.30,
    "cart_to_buy_rate": 0.27
  },
  "changes": {
    "viewers": 5.2,
    "carters": -2.1,
    "purchasers": 0.0,
    "view_to_cart_rate": 1.5,
    "cart_to_buy_rate": -0.5
  }
}
```

### GET `/api/last-event-time`
Returns the timestamp of the most recent funnel data.

```json
{"last_event_time": "2026-03-03T04:26:00"}
```

## Troubleshooting

### No data in dashboard

1. **Check if funnel MV has data:**
   ```bash
   psql -h localhost -p 4566 -d dev -c "SELECT * FROM funnel LIMIT 5;"
   ```

2. **Verify Kafka sink is running:**
   ```bash
   psql -h localhost -p 4566 -d dev -c "SHOW SINKS;"
   ```

3. **Check Kafka topic has messages:**
   ```bash
   docker exec -it redpanda rpk topic consume funnel --brokers localhost:9092
   ```

4. **Check dashboard backend logs for Kafka connection errors**

### Reset the sink

If you need to recreate the sink:

```bash
psql -h localhost -p 4566 -d dev -c "DROP SINK funnel_kafka_sink;"
cd dbt && dbt run --models sink_funnel_to_kafka
```

## Configuration Options

### Changing Kafka Bootstrap Servers

Edit `modern-dashboard/backend/api.py`:
```python
KAFKA_BOOTSTRAP_SERVERS = ['redpanda:9092']  # For container-to-container
# Or for external access:
# KAFKA_BOOTSTRAP_SERVERS = ['localhost:19092']
```

### Adjusting Consumer Group

Multiple dashboard instances can share the same consumer group:
```python
group_id='dashboard-backend'
```

Each instance will receive a partition of the data. For all instances to receive all messages, use different group IDs.
