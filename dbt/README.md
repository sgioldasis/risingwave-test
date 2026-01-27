# Real-Time E-Commerce Conversion Funnel

This project demonstrates a real-time e-commerce conversion funnel using RisingWave, dbt, and Apache Kafka (Redpanda). It tracks user behavior through page views, cart events, and purchases to calculate conversion rates in real-time.

## Project Structure

```
dbt/                              # dbt project folder
├── models/                        # dbt models
│   ├── src_cart.sql            # Cart events source
│   ├── src_page.sql            # Page views source
│   ├── src_purchase.sql        # Purchase events source
│   └── funnel.sql              # Conversion funnel materialized view
├── profiles.yml                  # dbt profile configuration
├── dbt_project.yml               # dbt project configuration
├── producer.py                   # Data generation script
├── main.py                       # Main script
├── pyproject.toml               # Python dependencies
├── uv.lock                     # Dependency lock file
├── .python-version              # Python version specification
└── README.md                   # This file
```

## Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```

## Setup Instructions

### 1. Start Infrastructure

From the main project folder (`risingwave-test`):
```bash
docker compose up -d
```

This will start:
- RisingWave database (port 4566)
- Redpanda Kafka (port 9092)
- Redpanda Console (port 8080)
- Supporting services (PostgreSQL, MinIO, LakeKeeper)

### 2. Install Dependencies

From inside the `dbt` folder:
```bash
cd dbt
uv sync
```

### 3. Generate Sample Data

Start the data producer to simulate real-time e-commerce events:
```bash
python producer.py
```

This script generates:
- Page view events
- Cart events  
- Purchase events

### 4. Create dbt Sources

Open a second window. Run the dbt models to create Kafka sources and the funnel view:
```bash
dbt run --profiles-dir .
```

### 5. View Real-Time Results

Monitor the conversion funnel in real-time:
```bash
watch "psql -h localhost -p 4566 -d dev -U root -c 'SELECT * FROM funnel ORDER BY window_start DESC LIMIT 5;'"
```

## Understanding the Funnel

The `funnel` materialized view provides real-time metrics:

- **window_start**: Time window (1-minute intervals)
- **viewers**: Number of unique users viewing pages
- **carters**: Number of unique users adding items to cart
- **purchasers**: Number of unique users making purchases
- **view_to_cart_rate**: Conversion rate from viewing to cart
- **cart_to_buy_rate**: Conversion rate from cart to purchase

## Kafka Topics

The system creates three Kafka topics:
- `page_views`: User page view events
- `cart_events`: Cart add/remove events
- `purchases`: Purchase completion events

## Architecture

```
Producer Python Script
    ↓ (Events)
Kafka (Redpanda)
    ↓ (Streaming)
RisingWave
    ↓ (SQL)
dbt Models
    ↓
Real-time Conversion Funnel
```

## Troubleshooting

### Connection Issues

If dbt fails with Kafka connection errors, ensure:
1. All containers are running: `docker ps`
2. The source models use `redpanda:9092` instead of `localhost:9092`

### Redpanda Console

Access the Kafka management UI at: http://localhost:8080

### RisingWave Management

Access the RisingWave UI at: http://localhost:5691 (if available)

## Stopping the Project

To stop all services:
```bash
docker compose down
```