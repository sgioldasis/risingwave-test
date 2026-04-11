# RisingWave Docker Compose Setup

Standard Docker Compose configuration for RisingWave with supporting services.

## Basic Service Definition

```yaml
services:
  risingwave:
    image: risingwavelabs/risingwave:latest
    ports:
      - "4566:4566"   # SQL port
      - "5691:5691"   # Dashboard
    environment:
      - RW_BACKEND=mem
    command: >
      standalone
      --meta-addr=0.0.0.0:5690
      --advertise-meta-addr=127.0.0.1:5690
      --prometheus-listener-addr=0.0.0.0:1222
      --config-path=/risingwave.toml
    volumes:
      - ./risingwave.toml:/risingwave.toml
    healthcheck:
      test: ["CMD", "psql", "-h", "localhost", "-p", "4566", "-U", "root", "-c", "SELECT 1"]
      interval: 5s
      timeout: 5s
      retries: 5
```

## Full Stack Example

```yaml
version: '3.8'

services:
  # RisingWave
  risingwave:
    image: risingwavelabs/risingwave:latest
    ports:
      - "4566:4566"
      - "5691:5691"
    environment:
      - RW_BACKEND=mem
    command: standalone
    volumes:
      - ./risingwave.toml:/risingwave.toml

  # Kafka (Redpanda)
  redpanda:
    image: redpandadata/redpanda:latest
    ports:
      - "9092:9092"
      - "9090:9090"  # Console
    command: >
      redpanda start
      --smp 1
      --memory 1G
      --kafka-addr 0.0.0.0:9092
      --advertise-kafka-addr redpanda:9092

  # Iceberg Catalog (Lakekeeper)
  lakekeeper:
    image: lakekeeper/lakekeeper:latest
    ports:
      - "8181:8181"
    environment:
      - LAKEKEEPER_WAREHOUSE=risingwave-warehouse
      - LAKEKEEPER_S3_ENDPOINT=http://minio:9301
      - LAKEKEEPER_S3_ACCESS_KEY=hummockadmin
      - LAKEKEEPER_S3_SECRET_KEY=hummockadmin

  # S3 Storage (MinIO)
  minio:
    image: minio/minio:latest
    ports:
      - "9301:9000"
      - "9302:9001"
    environment:
      - MINIO_ROOT_USER=hummockadmin
      - MINIO_ROOT_PASSWORD=hummockadmin
    command: server /data --console-address ":9001"
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RW_BACKEND` | Metadata backend | `mem` |
| `RW_DATA_DIR` | Data directory | - |
| `RW_CONFIG_PATH` | Config file path | - |

## Port Reference

| Port | Service | Description |
|------|---------|-------------|
| 4566 | RisingWave | SQL/Postgres protocol |
| 5691 | RisingWave | Web Dashboard |
| 9092 | Redpanda | Kafka protocol |
| 9090 | Redpanda | Web Console |
| 8181 | Lakekeeper | Iceberg REST API |
| 9301 | MinIO | S3 API |
| 9302 | MinIO | Web Console |

## Health Checks

```yaml
healthcheck:
  test: ["CMD", "psql", "-h", "localhost", "-p", "4566", "-U", "root", "-c", "SELECT 1"]
  interval: 5s
  timeout: 5s
  retries: 5
  start_period: 10s
```

## Volumes

```yaml
volumes:
  risingwave-data:
  minio-data:
```

## Network Configuration

```yaml
networks:
  default:
    driver: bridge
```

## Related Skills

- `risingwave/kafka-source` - Connect to Kafka
- `risingwave/iceberg-source` - Connect to Iceberg
- `risingwave/iceberg-sink` - Write to Iceberg
