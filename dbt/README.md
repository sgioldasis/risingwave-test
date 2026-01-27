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
├── .python-version              # Python version specification
├── profiles.yml                  # dbt profile configuration
├── dbt_project.yml               # dbt project configuration
├── pyproject.toml               # Python dependencies
├── uv.lock                     # Dependency lock file
├── producer.py                   # Data generation script
├── dashboard.py                  # Real-time dashboard
├── 1_up.sh                      # Start infrastructure services
├── 5_down.sh                    # Stop services and cleanup
├── 2_create_topics.sh           # Create Kafka topics
├── 3_run_dbt.sh                 # Run dbt models
├── 4_run_dashboard.sh           # Start dashboard
└── README.md                    # This file
```

*Note: `logs/` and `target/` directories are generated during runtime and are automatically cleaned up by `5_down.sh`*

## Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```

## Quick Start

### Complete Setup Sequence

From the `dbt` folder, run the following commands in order:

```bash
# 1. Start all infrastructure services
./1_up.sh

# 2. Create required Kafka topics
./2_create_topics.sh

# 3. Run dbt models to create sources and materialized views
./3_run_dbt.sh

# 4. Start the dashboard for real-time monitoring
./4_run_dashboard.sh

# 5. When finished, stop all services and clean up volumes
./5_down.sh
```

### Individual Steps

#### 1. Start Infrastructure

```bash
./1_up.sh
```

This will start:
- RisingWave database (port 4566)
- Redpanda Kafka (port 9092)
- Redpanda Console (port 8080)
- Supporting services (PostgreSQL, MinIO, LakeKeeper)

#### 2. Install Dependencies

```bash
uv sync
```

#### 3. Create Kafka Topics

```bash
./2_create_topics.sh
```

Creates the required Kafka topics:
- `page_views`: User page view events
- `cart_events`: Cart add/remove events  
- `purchases`: Purchase completion events

#### 4. Create dbt Sources

```bash
./3_run_dbt.sh
```

This runs the dbt models to create Kafka sources and the funnel view.

#### 5. Start Dashboard

```bash
./4_run_dashboard.sh
```

Launches a web-based dashboard at http://localhost:8050 to monitor real-time conversion metrics. 

**Note**: The dashboard includes producer controls in the right panel. Use the "Start Producer" button to begin data generation and "Stop Producer" to pause it. The producer no longer auto-starts to prevent unexpected data generation.

#### 6. Monitor Real-Time Results

You can also monitor the conversion funnel directly:
```bash
watch "psql -h localhost -p 4566 -d dev -U root -c 'SELECT * FROM funnel ORDER BY window_start DESC LIMIT 5;'"
```

#### 7. Generate Test Data (Optional)

To generate test data, either:
- Use the dashboard's producer controls (recommended)
- Run standalone producer: `python producer.py`

**Note**: The producer will generate events every second and send them to Kafka topics. Stop it when you want to see static conversion metrics.

## Understanding the Funnel

The `funnel` materialized view provides real-time metrics:

- **window_start**: Time window (1-minute intervals)
- **viewers**: Number of unique users viewing pages
- **carters**: Number of unique users adding items to cart
- **purchasers**: Number of unique users making purchases
- **view_to_cart_rate**: Conversion rate from viewing to cart
- **cart_to_buy_rate**: Conversion rate from cart to purchase

## Available Scripts

| Script | Purpose |
|--------|---------|
| `./1_up.sh` | Start all Docker Compose services |
| `./2_create_topics.sh` | Create required Kafka topics |
| `./3_run_dbt.sh` | Run dbt models (equivalent to `dbt run --profiles-dir .`) |
| `./4_run_dashboard.sh` | Start the real-time dashboard |
| `./5_down.sh` | Stop all services and clean up volumes |

## Kafka Topics

The system uses three Kafka topics:
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

### Producer Auto-Start Issues

**Fixed**: Previously, counts would keep increasing even after stopping the producer because the dashboard would auto-start a new producer process. This has been resolved:
- Producer now requires manual start via dashboard controls
- Sources use `scan.startup.mode = 'latest'` to prevent reprocessing historical data
- Counts will only increase when producer is manually started

### Data Still Increasing After Stopping Producer?

If you notice counts continue rising after stopping the producer:
1. Check dashboard: Verify producer indicator shows "Stopped"
2. Restart dashboard: `./4_run_dashboard.sh` to apply the fixed configuration
3. Verify no other producer processes: `ps aux | grep producer`

### Redpanda Console

Access the Kafka management UI at: http://localhost:8080

### RisingWave Management

Access the RisingWave UI at: http://localhost:5691 (if available)

## Stopping the Project

To stop all services and clean up volumes:
```bash
./5_down.sh
```

## Individual Components

### Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```