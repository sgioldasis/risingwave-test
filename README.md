# Real-Time E-Commerce Conversion Funnel

This project demonstrates a real-time e-commerce conversion funnel using RisingWave, dbt, Apache Kafka (Redpanda), Apache Iceberg, and ML predictions. It tracks user behavior through page views, cart events, and purchases to calculate conversion rates in real-time with predictive analytics.

## Technologies Used

| Technology | Purpose |
|------------|---------|
| **RisingWave** | Stream processing database for real-time analytics |
| **dbt** | Data transformation and modeling |
| **Apache Kafka (Redpanda)** | Event streaming platform |
| **Apache Iceberg** | Open table format for data lake storage |
| **Lakekeeper** | Iceberg REST catalog |
| **Trino** | Distributed SQL query engine for Iceberg and RisingWave |
| **Dagster** | Data orchestration for dbt models and ML training |
| **MinIO** | S3-compatible object storage for models and data |
| **FastAPI** | Backend API for dashboard and ML serving |
| **React** | Modern frontend dashboard |
| **scikit-learn** | ML model training (RandomForest, LinearRegression) |
| **DuckDB** | Local analytics on Iceberg data |
| **Spark** | Interactive notebook for data analysis |

## Project Structure

```
dbt/                              # dbt project folder
├── models/                       # dbt models
│   ├── src_cart.sql              # Cart events source
│   ├── src_page.sql              # Page views source
│   ├── src_purchase.sql          # Purchase events source
│   ├── src_iceberg_countries.sql # Iceberg countries source
│   ├── iceberg_page_views.sql    # Iceberg table for page views
│   ├── iceberg_cart_events.sql   # Iceberg table for cart events
│   ├── iceberg_purchases.sql     # Iceberg table for purchases
│   ├── funnel.sql                # Conversion funnel materialized view
│   ├── funnel_training.sql       # ML training data view
│   ├── rw_countries.sql          # Countries materialized view
│   ├── sink_funnel_to_kafka.sql  # Kafka sink for dashboard
│   └── sink_funnel_to_iceberg.sql # Iceberg sink for persistence
├── macros/                       # dbt macros
│   ├── create_iceberg_connection.sql
│   ├── sync_from_iceberg_via_trino.sql
│   └── materializations/
│       ├── iceberg_table.sql
│       ├── risingwave_source.sql
│       └── sink.sql
└ ...

ml/                               # Machine Learning modules
├── training/                     # ML training module
│   ├── trainer.py                # Core training logic
│   ├── model_registry.py         # MinIO-based model storage
│   └── data_fetcher.py           # RisingWave data fetching
└ serving/                        # ML serving module
    ├── main.py                   # FastAPI serving application
    ├── model_loader.py           # Model loading with hot-reload
    └── predictor.py              # Prediction logic with caching

modern-dashboard/                 # Modern React dashboard
├── backend/
│   └── api.py                    # FastAPI backend (Kafka consumer)
└ frontend/
    ├── src/
    │   ├── components/
    │   │   ├── PureCSSFunnel.jsx
    │   │   ├── ThreeDFunnel.jsx
    │   │   └── PredictionsTab.jsx
    │   └── App.jsx
    └── package.json

orchestration/                    # Dagster orchestration
├── definitions.py                # Dagster assets and jobs
├── assets/
│   ├── iceberg_countries.py      # Trino-based Iceberg asset
│   └── risingwave_countries_table.py

scripts/                          # Python utility scripts
├── producer.py                   # Event generation
├── dashboard.py                  # Legacy dashboard
├── consume_funnel_from_kafka.py  # Kafka consumer
└ ...

sql/                              # Compiled SQL files
└ all_models.sql                  # Pre-compiled SQL (ready for psql)

trino/                            # Trino configuration
└ catalog/
    ├── iceberg.properties        # Iceberg catalog config
    └── risingwave.properties     # RisingWave catalog config
```

## Key Use Cases

### Use Case 1: Push Model - Modern Dashboard via Kafka Sink

The modern dashboard uses a **push architecture** where data flows from RisingWave to the dashboard entirely through Kafka, without the backend ever querying RisingWave directly.

#### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Sources │────▶│    RisingWave   │     │                 │
│  (page_views,   │     │                 │     │                 │
│   cart_events,  │     │  funnel_summary │────▶│  Kafka Sink     │
│   purchases)    │     │  (1-min window) │     │  (funnel topic) │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
         ↑                                               │
         │                                               │ PUSH
         │                                               │ (no pull)
         │                               ┌───────────────┘
         │                               ▼
         │                     ┌─────────────────┐
         │                     │  Dashboard      │
         │                     │  Backend API    │
         │                     │  (port 8000)    │
         │                     │                 │
         │                     │  Kafka Consumer │
         │                     │  + SSE Emitter  │
         │                     └────────┬────────┘
         │                              │
         │                               ▼
         │                     ┌─────────────────┐
         │                     │  React Frontend │
         └─────────────────────│  (port 4000)    │
                               │  (EventSource)  │
                               └─────────────────┘
```

#### Components

**1. Kafka Sink Model** ([`dbt/models/sink_funnel_to_kafka.sql`](dbt/models/sink_funnel_to_kafka.sql))
```sql
CREATE SINK IF NOT EXISTS funnel_kafka_sink
FROM funnel_summary
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'funnel'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
```
This sink publishes every update from the `funnel_summary` materialized view to the Kafka `funnel` topic in real-time.

**2. Dashboard Backend** ([`modern-dashboard/backend/api.py`](modern-dashboard/backend/api.py))
- **No RisingWave Connection**: The backend NEVER connects to RisingWave
- **Kafka Consumer Thread**: Background thread consumes from `funnel` topic on startup
- **In-Memory Cache**: Stores latest funnel data and history (last 1000 records)
- **Deduplication Logic**: Handles materialized view retractions by keeping only latest per window
- **SSE Endpoint**: `GET /api/funnel/stream` streams updates to frontend via Server-Sent Events

**3. Frontend** ([`modern-dashboard/frontend/src/App.jsx`](modern-dashboard/frontend/src/App.jsx))
- Uses **Server-Sent Events (SSE)** via `EventSource` API for real-time updates
- Receives push updates from backend only when data changes
- Automatic reconnection on connection loss
- Displays 3D funnel visualization and real-time metrics
- No direct connection to Kafka or RisingWave

#### Why Push Model?
- **Decoupling**: Dashboard backend doesn't need RisingWave credentials
- **Scalability**: Multiple dashboard instances can consume from Kafka independently
- **Resilience**: Dashboard can survive temporary RisingWave outages (Kafka buffers data)
- **Performance**: No query load on RisingWave from dashboard users

---

### Use Case 2: ML Training & Serving

ML models are trained on funnel data using Dagster orchestration, stored in MinIO, and served via a dedicated FastAPI service. The modern dashboard integrates predictions through its backend.

#### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TRAINING PIPELINE                               │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   Dagster    │────▶│   ML Training│────▶│    MinIO     │                 │
│  │   Sensor     │     │   (trainer)  │     │  (ml-models  │                 │
│  │  (20s/demo)  │     │              │     │   bucket)    │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
│         │                    ▲                                              │
│         │                    │                                              │
│         └────────────────────┘                                              │
│              Reads funnel_training MV                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       │ Models stored with version
                                       │ format: vYYYYMMDD_HHMMSS
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SERVING PIPELINE                                │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │    ML        │◀────│  ModelLoader │◀────│    MinIO     │                 │
│  │   Serving    │     │ (hot-reload) │     │  (ml-models  │                 │
│  │  (port 8001) │     │              │     │   bucket)    │                 │
│  └──────┬───────┘     └──────────────┘     └──────────────┘                 │
│         │                                                                   │
│         │ GET /predict                                                      │
│         │ GET /predict/{metric}                                             │
│         │ GET /models                                                       │
│         ▼                                                                   │
│  ┌──────────────┐                                                           │
│  │   Dashboard  │     ┌──────────────┐     ┌──────────────┐                 │
│  │   Backend    │────▶│   Frontend   │────▶│  Predictions │                 │
│  │  (port 8000) │     │  (port 4000) │     │     Tab      │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
│       Proxies                                                               │
│       /api/predictions/*                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Components

**1. Dagster ML Training** ([`orchestration/definitions.py`](orchestration/definitions.py))
```python
@asset(
    group_name="ml",
    deps=[AssetKey(["public", "funnel_training"])],
)
def ml_trained_models(context: AssetExecutionContext):
    """Asset that trains ML models and saves them to MinIO."""
    trainer = ModelTrainer()
    results = trainer.train_all_metrics(minutes_back=1)
    ...

# Realtime demo sensor (20 seconds)
@sensor(
    job=ml_training_job,
    minimum_interval_seconds=20,
    default_status=DefaultSensorStatus.STOPPED,
)
def ml_training_sensor_realtime(context):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return RunRequest(run_key=f"ml_training_{timestamp}")
```
- **Training Data**: Fetches last 1 minute from `funnel_training` materialized view
- **Models**: Trains 5 models (viewers, carters, purchasers, view_to_cart_rate, cart_to_buy_rate)
- **Algorithms**: RandomForestRegressor (for 10+ samples) or LinearRegression (fallback)
- **Schedule**: 
  - Production: 5-minute cron schedule
  - Demo: 20-second sensor (set `ML_TRAINING_MODE=realtime`)
- **Storage**: Models saved to MinIO `ml-models` bucket with versioning

**2. ML Serving Service** ([`ml/serving/main.py`](ml/serving/main.py))
- **Port**: 8001
- **ModelLoader**: Loads models from MinIO with ETag-based change detection (hot-reload)
- **ModelPredictor**: Caches predictions for 10 seconds, calculates confidence scores
- **Endpoints**:
  - `GET /predict` - Get predictions for all metrics
  - `GET /predict/{metric}` - Get prediction for specific metric
  - `GET /models` - Get model status with version timestamps
  - `POST /reload` - Force model reload
  - `GET /health` - Health check

**3. Dashboard Integration** ([`modern-dashboard/backend/api.py`](modern-dashboard/backend/api.py))
```python
# ML Serving Service configuration
ML_SERVING_URL = os.environ.get("ML_SERVING_URL", "http://localhost:8001")

async def call_ml_serving(endpoint: str, method: str = "GET", json_data: dict = None) -> dict:
    """Call the ML serving service."""
    url = f"{ML_SERVING_URL}{endpoint}"
    ...

@app.get("/api/predictions/next")
async def get_next_predictions():
    """Proxy to ML serving for next-minute predictions."""
    return await call_ml_serving("/predict")
```
- The dashboard backend acts as a **proxy** to the ML serving service
- Frontend calls `/api/predictions/next` → Dashboard backend calls `http://localhost:8001/predict`
- This design allows the frontend to use a single backend connection for both funnel data and predictions

**4. Frontend Predictions Tab** ([`modern-dashboard/frontend/src/components/PredictionsTab.jsx`](modern-dashboard/frontend/src/components/PredictionsTab.jsx))
- Real-time prediction cards for all 5 metrics
- Predictions vs Actuals comparison chart
- Model status monitoring
- Auto-trains models on first prediction request if not already trained

#### How Predictions Flow
1. **Training**: Dagster sensor triggers every 20s → Fetches data from RisingWave → Trains models → Saves to MinIO
2. **Serving**: ML serving service polls MinIO for model changes → Loads new models automatically
3. **Consumption**: Frontend requests predictions → Dashboard backend proxies to ML serving → Returns predictions with confidence scores

---

### Use Case 3: Historical Iceberg Data in RisingWave (iceberg_countries)

This use case demonstrates how historical/reference data stored in Iceberg can be made available in RisingWave with automatic synchronization. Updates made via Trino to the Iceberg table are immediately visible in RisingWave.

#### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            WRITE PATH (Trino)                                │
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   Dagster    │────▶│    Trino     │────▶│   Iceberg    │                 │
│  │   Asset      │     │   (JDBC)     │     │  (Lakekeeper)│                 │
│  │              │     │              │     │              │                 │
│  │ iceberg_count│     │  INSERT/     │     │ iceberg_count│                 │
│  │ ries.py      │     │  UPDATE/     │     │ ries table   │                 │
│  └──────────────┘     │  DELETE      │     └──────┬───────┘                 │
└───────────────────────┴──────────────┴─────────────┼─────────────────────────┘
                                                     │
                              ┌─────────────────────┘
                              │ REST Catalog API
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            READ PATH (RisingWave)                            │
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   RisingWave │◀────│  Native      │◀────│  Lakekeeper  │                 │
│  │   SOURCE     │     │  Iceberg     │     │  REST API    │                 │
│  │              │     │  Connector   │     │              │                 │
│  │ src_iceberg_ │     │              │     │              │                 │
│  │ countries    │     │ Auto-refresh │     │              │                 │
│  └──────┬───────┘     └──────────────┘     └──────────────┘                 │
│         │                                                                    │
│         │ (changelog stream)                                                 │
│         ▼                                                                    │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │ Materialized │     │funnel_summary│     │ funnel_summary│                │
│  │ View         │────▶│   _with_     │────▶│   (joined)   │                 │
│  │ rw_countries │     │  countries   │     │              │                 │
│  │              │     │              │     │              │                 │
│  │ Deduplicates │     │ Joins funnel │     │ View in      │                 │
│  │ changelog to │     │ with country │     │ RisingWave   │                 │
│  │ latest only  │     │ names        │     │              │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Components

**1. Iceberg Table Creation** ([`orchestration/assets/iceberg_countries.py`](orchestration/assets/iceberg_countries.py))
The Dagster asset uses Trino to create and populate the `iceberg_countries` table:
```python
# Via Trino JDBC connection
cur.execute("""
    CREATE TABLE IF NOT EXISTS iceberg.analytics.iceberg_countries (
        country VARCHAR,
        country_name VARCHAR,
        region VARCHAR
    ) WITH (format = 'PARQUET')
""")
# Then INSERT data from data/countries.csv
```

**2. RisingWave Native Iceberg Source** ([`dbt/models/src_iceberg_countries.sql`](dbt/models/src_iceberg_countries.sql))
```sql
CREATE SOURCE IF NOT EXISTS src_iceberg_countries
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog',
    warehouse = 'risingwave-warehouse',
    database.name = 'analytics',
    table.name = 'iceberg_countries'
)
```
- **Type**: Native RisingWave SOURCE (not a table)
- **Behavior**: Returns **changelog data** (all changes, including retractions)
- **Auto-refresh**: ✅ **Immediate automatic refresh** when Iceberg data changes
- **No polling**: RisingWave uses the Lakekeeper REST API to detect changes

**3. Deduplication Layer** ([`dbt/models/rw_countries.sql`](dbt/models/rw_countries.sql))
```sql
{{ config(materialized='materialized_view') }}

WITH ranked_countries AS (
    SELECT
        country,
        country_name,
        region,
        _row_id,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY _row_id DESC) as rn
    FROM {{ source('public', 'src_iceberg_countries') }}
)
SELECT country, country_name, region
FROM ranked_countries
WHERE rn = 1
```
- **Problem**: `src_iceberg_countries` returns changelog (duplicates on update)
- **Solution**: Materialized view uses `ROW_NUMBER()` to keep only latest row per country
- **Result**: Current snapshot only, no duplicates, auto-refreshes when source changes

**4. Join with Funnel Data** ([`dbt/models/funnel_summary_with_country.sql`](dbt/models/funnel_summary_with_country.sql))
```sql
{{ config(materialized='view') }}

SELECT
    f.window_start,
    f.window_end,
    f.country,
    c.country_name,  -- From Iceberg!
    f.viewers,
    f.carters,
    f.purchasers,
    f.view_to_cart_rate,
    f.cart_to_buy_rate
FROM {{ ref('funnel_summary') }} f
LEFT JOIN {{ ref('rw_countries') }} c
    ON f.country = c.country
```
- Joins real-time funnel data with Iceberg countries reference data
- Country names from Iceberg enrich the funnel analytics

#### The Magic: Immediate Propagation

When you update data in Iceberg via Trino:

```bash
# Update via Trino
docker compose exec trino trino --catalog iceberg --schema analytics \
  --execute "UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'"
# Output: UPDATE: 1 row

# Query in RisingWave (IMMEDIATELY shows 'Hellas')
psql -h localhost -p 4566 -d dev -U root \
  -c "SELECT * FROM rw_countries WHERE country = 'GR'"
# Output: GR | Hellas | Europe

# Query the joined view (also shows 'Hellas')
psql -h localhost -p 4566 -d dev -U root \
  -c "SELECT country, country_name FROM funnel_summary_with_country WHERE country = 'GR' LIMIT 1"
# Output: GR | Hellas
```

**How is this achieved?**
1. **Trino writes** to Iceberg table via Lakekeeper REST catalog
2. **Lakekeeper** updates the table metadata
3. **RisingWave SOURCE** detects metadata change via REST API polling
4. **RisingWave** fetches the changelog (old value retracted, new value inserted)
5. **Materialized view** recomputes, keeping only latest per country
6. **Result visible immediately** - no batch jobs, no manual refresh

#### Key Advantages

1. **✅ Full SQL Support**: Trino supports UPDATE/DELETE (DuckDB doesn't)
2. **✅ Automatic Sync**: RisingWave source immediately reflects Iceberg changes
3. **✅ No Batch Jobs**: No need for scheduled sync jobs or sensors
4. **✅ Fast Queries**: Materialized view provides local RisingWave performance
5. **✅ Native Integration**: Uses RisingWave's built-in Iceberg connector
6. **✅ Change Tracking**: Changelog captures full history of changes

#### Query Paths Comparison

| Path | Use Case | Latency |
|------|----------|---------|
| Trino → Iceberg | Direct queries on Iceberg data | Immediate |
| Trino → Iceberg → RisingWave SOURCE | Real-time analytics with reference data | < 1 second |
| RisingWave MV → funnel_summary_with_country | Enriched funnel analytics | Real-time |

---

### Implementation: Server-Sent Events (SSE)

The modern dashboard uses **Server-Sent Events (SSE)** for real-time updates between the React frontend and FastAPI backend. SSE provides true push-based updates, eliminating the inefficiencies of HTTP polling.

#### Previous Implementation (HTTP Polling - Deprecated)
The original implementation used polling with these problems:
- **Wasted resources**: 60 requests/minute even when data hasn't changed
- **Latency**: Updates delayed until next poll cycle (avg 500ms lag)
- **Connection overhead**: Repeated HTTP handshakes

#### Current Implementation: Server-Sent Events (SSE)

SSE provides push-based updates from backend to frontend:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Topic   │────▶│  Dashboard      │────▶│  React Frontend │
│    (funnel)     │     │  Backend API    │     │  (EventSource)  │
└─────────────────┘     │                 │     │                 │
                        │  Kafka Consumer │     │  Single HTTP    │
                        │  + SSE Emitter  │     │  connection     │
                        └─────────────────┘     │  (persistent)   │
                                                └─────────────────┘
```

**Benefits of SSE:**

| Aspect | HTTP Polling | SSE |
|--------|--------------|-----|
| **Latency** | 0-1000ms (avg 500ms) | < 100ms |
| **Efficiency** | 60 requests/minute | 1 connection, events only when data changes |
| **Server Load** | High (repeated headers, connections) | Low (persistent connection) |
| **Implementation** | Simple | Slightly more complex |
| **Scalability** | Poor | Better |

**Implementation Details:**

1. **Backend Endpoint** ([`modern-dashboard/backend/api.py`](modern-dashboard/backend/api.py)):
   - `GET /api/funnel/stream` - SSE endpoint that streams updates
   - Checks for data changes every 100ms
   - Only sends data when values change (hash-based comparison)
   - Sends heartbeat comments to keep connection alive
   - Auto-reconnect handled by browser's EventSource API

2. **Frontend** ([`modern-dashboard/frontend/src/App.jsx`](modern-dashboard/frontend/src/App.jsx)):
   - Uses `EventSource` API for persistent connection
   - Automatic reconnection on connection loss (3-second delay)
   - Falls back gracefully if SSE fails

**Why SSE over WebSockets?**
- WebSockets are bidirectional, but we only need **server → client** push
- SSE is simpler (standard HTTP, auto-reconnect, built-in event IDs)
- SSE works over HTTP/1.1 and HTTP/2 without protocol upgrade
- WebSockets require more complex infrastructure (sticky sessions, etc.)

---

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

# 3. (Optional) Start ML serving service for predictions
./bin/4_run_ml_serving.sh

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

### Alternative: Web-Based Script Runner

You can also use the web-based script runner to manage all scripts from a browser interface:

```bash
./bin/0_script_runner.sh
```

This starts a web application at [http://localhost:8080](http://localhost:8080) where you can:
- Run any script with a single click
- View output in real-time with tabs for each script
- Run multiple scripts simultaneously
- Restart running scripts
- Auto-detects already-running dashboard processes

## Web Consoles

| Service | URL | Description |
|---------|-----|-------------|
| RisingWave Console | http://localhost:5691 | Stream processing dashboard |
| Redpanda Console | http://localhost:9090 | Kafka topic management |
| Lakekeeper UI | http://localhost:8181 | Iceberg catalog management |
| MinIO Console | http://localhost:9301 | S3 storage (login: hummockadmin/hummockadmin) |
| Dagster UI | http://localhost:3000 | Pipeline orchestration |
| ML Serving API | http://localhost:8001/docs | ML predictions API |
| Modern Dashboard | http://localhost:4000 | React frontend |

## ML Predictions Usage

### Start ML Serving
```bash
./bin/4_run_ml_serving.sh
```

### Get Predictions
```bash
# Get all predictions
curl http://localhost:8001/predict

# Get specific metric prediction
curl http://localhost:8001/predict/viewers

# Check model status
curl http://localhost:8001/models

# Force model reload
curl -X POST http://localhost:8001/reload
```

### Train Models via Dagster
Training runs automatically via Dagster schedule (5-minute cron or 20-second sensor in realtime mode). Or trigger manually via Dagster UI.

## Trino Usage Examples

### Load Countries to Iceberg
```bash
uv run python scripts/load_countries_to_iceberg_trino.py
```

### Query via Trino CLI
```bash
# Interactive shell
trino --server http://localhost:8080 --catalog iceberg --schema analytics

# One-liner query
trino --server http://localhost:8080 --catalog iceberg --schema analytics \
  --execute "SELECT * FROM iceberg_countries"
```

### Update Data via Trino
```bash
docker compose exec trino trino --catalog iceberg --schema analytics \
  --execute "UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'"
```

### Query in RisingWave (Auto-Refresh)
```bash
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM rw_countries WHERE country = 'GR'"
```

## Understanding the Funnel

The `funnel` materialized view provides real-time metrics:

| Metric | Description |
|--------|-------------|
| **window_start** | Time window (1-minute intervals) |
| **viewers** | Number of unique users viewing pages |
| **carters** | Number of unique users adding items to cart |
| **purchasers** | Number of unique users making purchases |
| **view_to_cart_rate** | Conversion rate from viewing to cart |
| **cart_to_buy_rate** | Conversion rate from cart to purchase |

## Available Scripts

| Script | Purpose |
|--------|---------|
| `./bin/0_script_runner.sh` | Web-based script runner (recommended) |
| `./bin/1_up.sh` | Start all Docker Compose services |
| `./bin/3_run_dbt.sh` | Run dbt models |
| `./bin/3_run_psql.sh` | Run SQL file directly via psql |
| `./bin/3_run_producer.sh` | Generate test events |
| `./bin/4_run_dashboard.sh` | Legacy dashboard (port 8050) |
| `./bin/4_run_modern.sh` | Modern React dashboard (port 4000) |
| `./bin/4_run_ml_serving.sh` | ML serving API (port 8001) |
| `./bin/5_duckdb_iceberg.sh` | Query Iceberg tables via DuckDB |
| `./bin/5_spark_iceberg.sh` | Interactive Spark notebook |
| `./bin/6_down.sh` | Stop all services and cleanup |

## Modern Dashboard (React)

The modern dashboard is a React-based frontend with a FastAPI backend that provides an enhanced visualization of the conversion funnel.

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
- **Predictions Tab**: ML predictions with comparison charts
- **Dark Theme**: Modern dark UI with RisingWave branding
- **Kafka Consumer**: Backend consumes from Kafka for real-time updates

## Architecture Overview

```
Producer Python Script
    ↓ (Events)
Kafka (Redpanda)
    ↓ (Streaming)
RisingWave
    ├──→ (SQL) dbt Models → Real-time Conversion Funnel (Dashboard)
    │                      (via dbt or Dagster orchestration)
    ├──→ (Sink) Kafka Topic → Modern Dashboard (Real-time)
    ├──→ (Sink) Iceberg Tables (Persistent Storage)
    │                                          ↓
    │                                     DuckDB Queries
    │                                     Spark Notebook
    └──→ (View) funnel_training → ML Training → MinIO Models
                                                 ↓
                                          ML Serving API
                                                 ↓
                                          Dashboard Predictions
```

Data flows from the producer through Kafka to RisingWave, where it's processed in multiple ways:
1. **Real-time funnel** via dbt models displayed on the dashboard
2. **Kafka sink** for the modern dashboard backend
3. **Persistent storage** via Iceberg sinks for analysis with DuckDB/Spark
4. **ML training** via the `funnel_training` view, with models stored in MinIO
5. **Predictions** served via FastAPI and displayed in the dashboard

## Troubleshooting

### Connection Issues

If dbt fails with Kafka connection errors, ensure:
1. All containers are running: `docker ps`
2. The source models use `redpanda:9092` instead of `localhost:9092`

### Producer Auto-Start Issues

The producer now requires manual start via dashboard controls. Sources use `scan.startup.mode = 'latest'` to prevent reprocessing historical data.

### ML Serving Connection

If predictions fail, ensure the ML serving service is running:
```bash
curl http://localhost:8001/health
```

### Dagster Schedule Not Running

Check Dagster UI at http://localhost:3000 for job status. Set `ML_TRAINING_MODE=realtime` for 20-second sensor instead of 5-minute cron.

## Stopping the Project

To stop all services and clean up volumes:
```bash
./bin/6_down.sh
```
