# Real-Time E-Commerce Conversion Funnel

This project demonstrates a real-time e-commerce conversion funnel using RisingWave, dbt, and Apache Kafka (Redpanda). It tracks user behavior through page views, cart events, and purchases to calculate conversion rates in real-time.

## Project Structure

```
dbt/                              # dbt project folder
├── models/                        # dbt models
│   ├── src_cart.sql            # Cart events source
│   ├── src_page.sql            # Page views source
│   ├── src_purchase.sql        # Purchase events source
│   ├── iceberg_page_views.sql  # Iceberg table for page views
│   ├── iceberg_cart_events.sql # Iceberg table for cart events
│   ├── iceberg_purchases.sql   # Iceberg table for purchases
│   └── funnel.sql              # Conversion funnel materialized view
├── macros/                       # dbt macros
│   ├── create_iceberg_connection.sql
│   └── materializations/
│       ├── iceberg_table.sql
│       └── sink.sql
├── modern-dashboard/            # Modern React dashboard
│   ├── backend/
│   │   └── api.py              # FastAPI backend
│   └── frontend/
│       ├── src/
│       │   ├── components/
│       │   │   ├── PureCSSFunnel.jsx
│       │   │   └── ThreeDFunnel.jsx
│       │   └── App.jsx
│       └── package.json
├── .python-version              # Python version specification
├── profiles.yml                  # dbt profile configuration
├── dbt_project.yml               # dbt project configuration
├── pyproject.toml               # Python dependencies
├── uv.lock                     # Dependency lock file
├── producer.py                   # Data generation script
├── dashboard.py                  # Real-time dashboard (legacy)
├── query_raw_iceberg.py        # Query Iceberg tables via DuckDB
├── 1_up.sh                      # Start infrastructure services
├── 2_create_topics.sh           # Create Kafka topics
├── 3_run_dbt.sh                 # Run dbt models
├── 4_run_dashboard.sh           # Start legacy dashboard
├── 4_run_modern.sh             # Start modern React dashboard
├── 5_query_iceberg.sh           # Query Iceberg tables via DuckDB
├── 6_down.sh                    # Stop services and cleanup
└── README.md                    # This file
```

*Note: `logs/` and `target/` directories are generated during runtime and are automatically cleaned up by `6_down.sh`*

## Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```

## Quick Start

### Complete Setup Sequence

From `dbt` folder, run the following commands in order:

```bash
# 1. Start all infrastructure services (includes uv sync for dependencies)
./1_up.sh

# 2. Create required Kafka topics
./2_create_topics.sh

# 3. Run dbt models to create sources, materialized views, and Iceberg tables
./3_run_dbt.sh

# 4. Start dashboard for real-time monitoring (choose one)
./4_run_dashboard.sh      # Legacy dashboard (port 8050)
# OR
./4_run_modern.sh         # Modern React dashboard (port 4000)

# 5. Query Iceberg tables via DuckDB (optional)
./5_query_iceberg.sh

# 6. When finished, stop all services and clean up volumes
./6_down.sh
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

Dependencies are now automatically installed when running `./1_up.sh` (which includes `uv sync`). 

If you need to install/update dependencies manually:
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
| `./3_run_dbt.sh` | Run dbt models (sources, views, Iceberg tables, sinks) |
| `./4_run_dashboard.sh` | Start the legacy real-time dashboard (port 8050) |
| `./4_run_modern.sh` | Start the modern React dashboard (port 4000) |
| `./5_query_iceberg.sh` | Query Iceberg tables via DuckDB |
| `./6_down.sh` | Stop all services and clean up volumes |

## Modern Dashboard (React)

The modern dashboard is a React-based frontend with a FastAPI backend that provides an enhanced visualization of the conversion funnel.

### Prerequisites

Frontend dependencies are automatically installed when you run `./1_up.sh` (if `node_modules` doesn't exist). If you need to install them manually:

```bash
cd modern-dashboard/frontend
npm install
cd ../..
```

### Running the Modern Dashboard

From the `dbt` folder, run:

```bash
./4_run_modern.sh
```

This will start:
- **Backend**: FastAPI server at http://localhost:8000
- **Frontend**: React dev server at http://localhost:4000

### Dashboard Features

- **Real-time Funnel Visualization**: Animated funnel showing viewers → carters → purchasers
- **Conversion Metrics**: Live view-to-cart and cart-to-purchase rates
- **3D Funnel View**: Interactive 3D funnel visualization
- **Dark Theme**: Modern dark UI with RisingWave branding

**Note**: The modern dashboard requires the infrastructure to be running (`./1_up.sh` and `./3_run_dbt.sh` should be executed first).

## Iceberg Integration

The project persists raw events to Apache Iceberg tables for persistent storage and analysis:

### Iceberg Tables
- `iceberg_page_views`: Raw page view events
- `iceberg_cart_events`: Raw cart add/remove events
- `iceberg_purchases`: Raw purchase events

### Querying Iceberg Data
Use the DuckDB query script to analyze data from Iceberg:

```bash
./5_query_iceberg.sh              # Show funnel analytics
./5_query_iceberg.sh --debug      # Show debug info with raw counts
./5_query_iceberg.sh --live       # Live monitoring mode (refreshes every 5 sec)
```

The query computes the same funnel metrics (viewers, carters, purchasers) from raw Iceberg events using DuckDB.

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
    ├──→ (SQL) dbt Models → Real-time Conversion Funnel (Dashboard)
    └──→ (Sink) Iceberg Tables (Persistent Storage)
                                              ↓
                                       DuckDB Queries
```

Data flows from the producer through Kafka to RisingWave, where it's processed in two ways:
1. **Real-time funnel** via dbt models displayed on the dashboard
2. **Persistent storage** via Iceberg sinks for analysis with DuckDB

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
./6_down.sh
```

## Individual Components

### Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```