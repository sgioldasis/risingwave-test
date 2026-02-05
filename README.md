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
└── ...

sql/                              # Compiled SQL files
└── all_models.sql              # Pre-compiled SQL (ready for psql)

```

Modern Dashboard Structure (in root folder):

```
modern-dashboard/                # Modern React dashboard
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
└── README.md                    # This file

**Note:** Python dependencies are now managed in the root `pyproject.toml`
```

**Python scripts are located in the `scripts/` folder:**
- `scripts/producer.py` - Data generation script
- `scripts/dashboard.py` - Real-time dashboard (legacy)
- `scripts/query_raw_iceberg.py` - Query Iceberg tables via DuckDB
- `scripts/user_activity_flow.py` - Marimo notebook for Iceberg analysis

**Scripts are now located in the `bin/` folder:**
- `bin/1_up.sh` - Start infrastructure services (includes automatic topic and namespace creation)
- `bin/3_run_dbt.sh` - Run dbt models
- `bin/3_run_psql.sh` - Run SQL file directly via psql
- `bin/4_run_dashboard.sh` - Start legacy dashboard
- `bin/4_run_modern.sh` - Start modern React dashboard
- `bin/5_duckdb_iceberg.sh` - Query Iceberg tables via DuckDB
- `bin/5_spark_iceberg.sh` - Run Spark notebook for Iceberg
- `bin/6_down.sh` - Stop services and cleanup
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

From project **root** folder, run the following commands in order:

```bash
# 1. Start all infrastructure services (includes uv sync for dependencies)
#    This also creates required Kafka topics and Lakekeeper namespace automatically
./bin/1_up.sh

# 2. Run dbt models to create sources, materialized views, and Iceberg tables
./bin/3_run_dbt.sh
# OR run the compiled SQL directly via psql (no Jinja compilation needed)
./bin/3_run_psql.sh

# 4. Start dashboard for real-time monitoring (choose one)
./bin/4_run_dashboard.sh      # Legacy dashboard (port 8050)
# OR
./bin/4_run_modern.sh         # Modern React dashboard (port 4000)

# 5. Query Iceberg tables (optional)
./bin/5_duckdb_iceberg.sh     # Query via DuckDB CLI
./bin/5_spark_iceberg.sh      # Interactive Spark notebook

# 6. When finished, stop all services and clean up volumes
./bin/6_down.sh
```

### Prerequisites

1. **Development Environment**: Run `devbox shell` from main project folder (`risingwave-test`)

2. **DBT Fusion**: If you have dbt-fusion installed, first uninstall it:
   ```bash
   dbtf system uninstall
   ```

### Individual Steps

#### 1. Start Infrastructure

```bash
./bin/1_up.sh
```

This will start:
- RisingWave database (port 4566)
- Redpanda Kafka (port 9092)
- Redpanda Console (port 9090)
- Supporting services (PostgreSQL, MinIO, LakeKeeper)

And automatically creates:
- **Kafka topics**: `page_views`, `cart_events`, `purchases`
- **Lakekeeper namespace**: `analytics`

**Web Consoles:**
- [RisingWave Console](http://localhost:5691) - RisingWave dashboard
- [Redpanda Console](http://localhost:9090) - Kafka topic management and monitoring
- [Lakekeeper UI](http://localhost:8181) - Iceberg catalog management
- [MinIO Console](http://localhost:9301) - S3-compatible storage (login: hummockadmin / hummockadmin)

#### 2. Install Dependencies

Dependencies are now automatically installed when running `./bin/1_up.sh` (which includes `uv sync`).

If you need to install/update dependencies manually:
```bash
uv sync
```

#### 3. Create dbt Sources

```bash
./bin/3_run_dbt.sh
```

This runs the dbt models to create Kafka sources and the funnel view.

#### 4. Start Dashboard

```bash
./bin/4_run_dashboard.sh
```

Launches a web-based dashboard at http://localhost:8050 to monitor real-time conversion metrics.

**Note**: The dashboard includes producer controls in the right panel. Use the "Start Producer" button to begin data generation and "Stop Producer" to pause it. The producer no longer auto-starts to prevent unexpected data generation.

#### 5. Monitor Real-Time Results

You can also monitor the conversion funnel directly:
```bash
watch "psql -h localhost -p 4566 -d dev -U root -c 'SELECT * FROM funnel ORDER BY window_start DESC LIMIT 5;'"
```

#### 6. Generate Test Data (Optional)

To generate test data, either:
- Use the dashboard's producer controls (recommended)
- Run standalone producer: `python scripts/producer.py`

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
| `./bin/1_up.sh` | Start all Docker Compose services (includes topic & namespace creation) |
| `./bin/3_run_dbt.sh` | Run dbt models (sources, views, Iceberg tables, sinks) |
| `./bin/3_run_psql.sh` | Run SQL file directly via psql (alternative to dbt) |
| `./bin/4_run_dashboard.sh` | Start the legacy real-time dashboard (port 8050) |
| `./bin/4_run_modern.sh` | Start the modern React dashboard (port 4000) |
| `./bin/5_duckdb_iceberg.sh` | Query Iceberg tables via DuckDB |
| `./bin/5_spark_iceberg.sh` | Run interactive Spark notebook for Iceberg analysis |
| `./bin/6_down.sh` | Stop all services and clean up volumes |

## Modern Dashboard (React)

The modern dashboard is a React-based frontend with a FastAPI backend that provides an enhanced visualization of the conversion funnel.

### Prerequisites

Frontend dependencies are automatically installed when you run `./bin/1_up.sh` (if `node_modules` doesn't exist). If you need to install them manually:

```bash
cd modern-dashboard/frontend
npm install
cd ../..
```

### Running the Modern Dashboard

From the project **root** folder, run:

```bash
./bin/4_run_modern.sh
```

This will start:
- **Backend**: FastAPI server at http://localhost:8000
- **Frontend**: React dev server at http://localhost:4000

### Dashboard Features

- **Real-time Funnel Visualization**: Animated funnel showing viewers → carters → purchasers
- **Conversion Metrics**: Live view-to-cart and cart-to-purchase rates
- **3D Funnel View**: Interactive 3D funnel visualization
- **Dark Theme**: Modern dark UI with RisingWave branding

**Note**: The modern dashboard requires the infrastructure to be running (`./bin/1_up.sh` and `./bin/3_run_dbt.sh` should be executed first).

## Running SQL Directly via psql

As an alternative to dbt, you can run the pre-compiled SQL file directly using psql. This is useful when you don't need Jinja2 templating or want faster execution.

### Using the Pre-compiled SQL File

The [`sql/all_models.sql`](sql/all_models.sql) file contains all the compiled SQL statements ready to run:

```bash
# Run the pre-compiled SQL file
./bin/3_run_psql.sh

# Or run with custom psql settings
PSQL_HOST=localhost PSQL_PORT=4566 ./bin/3_run_psql.sh

# Or run psql directly
psql -h localhost -p 4566 -d dev -U root -f sql/all_models.sql
```

The SQL file creates:
1. Iceberg connection to Lakekeeper
2. Kafka sources (page views, cart events, purchases)
3. Materialized view for funnel analysis
4. Iceberg tables for persistent storage
5. Sinks to write data to Iceberg

## Spark Notebook

An interactive Spark notebook is available for analyzing Iceberg table data with rich visualizations.

### Running the Notebook

```bash
./bin/5_spark_iceberg.sh
```

This starts the Spark notebook server in edit mode, allowing you to interactively explore:
- User activity flow through the conversion funnel
- Sankey diagrams showing user progression
- Time-series analysis of events
- Detailed breakdown tables with conversion metrics

### Notebook Features

- **PyIceberg Integration**: Direct queries to Iceberg tables via Lakekeeper catalog
- **Interactive Visualizations**: Plotly charts for funnel analysis
- **Live Data**: Real-time connection to RisingWave-persisted Iceberg data
- **Reproducible**: Self-contained notebook with all dependencies

## Iceberg Integration

The project persists raw events to Apache Iceberg tables for persistent storage and analysis:

### Iceberg Tables
- `iceberg_page_views`: Raw page view events
- `iceberg_cart_events`: Raw cart add/remove events
- `iceberg_purchases`: Raw purchase events

### Querying Iceberg Data
Use the DuckDB query script to analyze data from Iceberg:

```bash
./5_duckdb_iceberg.sh             # Show funnel analytics
./5_duckdb_iceberg.sh --debug     # Show debug info with raw counts
./5_duckdb_iceberg.sh --live      # Live monitoring mode (refreshes every 5 sec)
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
    │                      (via dbt or Dagster orchestration)
    └──→ (Sink) Iceberg Tables (Persistent Storage)
                                              ↓
                                        DuckDB Queries
                                        Marimo Notebook
```

Data flows from the producer through Kafka to RisingWave, where it's processed in multiple ways:
1. **Real-time funnel** via dbt models displayed on the dashboard (using direct dbt or Dagster orchestration)
2. **Persistent storage** via Iceberg sinks for analysis with DuckDB
3. **Interactive analysis** via the Marimo notebook with PyIceberg

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

Access the Kafka management UI at: http://localhost:9090

### RisingWave Console

Access the RisingWave dashboard at: http://localhost:5691

### Lakekeeper UI

Access the Iceberg catalog management UI at: http://localhost:8181

### MinIO Console

Access the S3-compatible storage browser at: http://localhost:9301
- Username: `hummockadmin`
- Password: `hummockadmin`

## Stopping the Project

To stop all services and clean up volumes:
```bash
./bin/6_down.sh
```
