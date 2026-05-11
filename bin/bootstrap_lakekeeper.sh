#!/bin/sh
set -e

apk add --no-cache curl jq >/dev/null

# Bypass any HTTP proxy injected into the container environment (e.g. OrbStack
# sets http_proxy=http://proxyproxy.orb.internal:8305, which doesn't know how
# to route docker-network hostnames like `lakekeeper` and returns HTTP 502).
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
export NO_PROXY='*'
export no_proxy='*'

# Retry configuration: 20 attempts with capped exponential backoff
# Total max wait ~= sum(min(2^n, 15)) for n=1..20 ≈ 240s (4 minutes)
MAX_ATTEMPTS=20
BACKOFF_CAP=15

# Transient HTTP codes that warrant a retry (gateway/upstream issues during
# Lakekeeper cold start where the management API isn't yet routable).
# 000 = curl couldn't connect; 408 = request timeout; 5xx = server/gateway.
is_transient() {
  case "$1" in
    000|408|425|429|500|502|503|504) return 0 ;;
    *) return 1 ;;
  esac
}

backoff() {
  # Capped exponential backoff: min(2^attempt, BACKOFF_CAP)
  delay=$((2 ** $1))
  [ "$delay" -gt "$BACKOFF_CAP" ] && delay=$BACKOFF_CAP
  sleep "$delay"
}

echo "Waiting for Lakekeeper /health to respond..."
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  http=$(curl -s -o /dev/null -w "%{http_code}" http://lakekeeper:8181/health || echo "000")
  if [ "$http" = "200" ]; then
    echo "✓ Lakekeeper is healthy."
    break
  fi
  echo "Health check attempt $attempt: HTTP $http; retrying..."
  if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
    echo "Lakekeeper never became healthy after $MAX_ATTEMPTS attempts." >&2
    exit 1
  fi
  backoff "$attempt"
done

echo "Bootstrapping Lakekeeper..."
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  http=$(curl -s -o /tmp/bootstrap.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{"accept-terms-of-use": true}' \
    http://lakekeeper:8181/management/v1/bootstrap || echo "000")

  case "$http" in
    200|201|204) echo "✓ Successfully bootstrapped Lakekeeper!"; break ;;
    400|409) echo "✓ Already bootstrapped or conflict; continuing..."; break ;;
    *)
      if is_transient "$http"; then
        echo "Bootstrap attempt $attempt: transient HTTP $http; retrying..."
        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
          echo "Bootstrap failed after $MAX_ATTEMPTS attempts (last HTTP $http)." >&2
          cat /tmp/bootstrap.json 2>/dev/null || true
          exit 1
        fi
        backoff "$attempt"
      else
        echo "Bootstrap failed with non-retryable HTTP $http." >&2
        cat /tmp/bootstrap.json 2>/dev/null || true
        exit 1
      fi
      ;;
  esac
done

echo "Creating warehouse risingwave-warehouse..."
sleep 2
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  http=$(curl -s -o /tmp/warehouse.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{
      "warehouse-name": "risingwave-warehouse",
      "delete-profile": { "type": "hard" },
      "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": "hummockadmin",
        "aws-secret-access-key": "hummockadmin"
      },
      "storage-profile": {
        "type": "s3",
        "bucket": "hummock001",
        "region": "us-east-1",
        "flavor": "s3-compat",
        "endpoint": "http://minio-0:9301",
        "path-style-access": true,
        "sts-enabled": false,
        "key-prefix": "risingwave-lakekeeper"
      }
    }' \
    http://lakekeeper:8181/management/v1/warehouse || echo "000")

  case "$http" in
    200|201) echo "✓ Successfully created warehouse!"; break ;;
    400|409) echo "✓ Warehouse already exists/overlaps; continuing..."; break ;;
    *)
      if is_transient "$http"; then
        echo "Warehouse creation attempt $attempt: transient HTTP $http; retrying..."
        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
          echo "Warehouse creation failed after $MAX_ATTEMPTS attempts (last HTTP $http)." >&2
          cat /tmp/warehouse.json 2>/dev/null || true
          exit 1
        fi
        backoff "$attempt"
      else
        echo "Warehouse creation failed with non-retryable HTTP $http." >&2
        cat /tmp/warehouse.json 2>/dev/null || true
        exit 1
      fi
      ;;
  esac
done

# Extract warehouse ID from response or fetch it if warehouse already existed
WAREHOUSE_ID=$(cat /tmp/warehouse.json 2>/dev/null | jq -r '."warehouse-id" // .id // empty' || echo "")
if [ -z "$WAREHOUSE_ID" ]; then
  echo "Fetching existing warehouse ID..."
  WAREHOUSE_ID=$(curl -s http://lakekeeper:8181/management/v1/warehouse | jq -r '.warehouses[0].id')
fi
echo "Using warehouse ID: $WAREHOUSE_ID"

echo "Creating public namespace..."
sleep 2
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  http=$(curl -s -o /tmp/namespace.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{"namespace": ["public"]}' \
    "http://lakekeeper:8181/catalog/v1/${WAREHOUSE_ID}/namespaces" || echo "000")

  case "$http" in
    200|201) echo "✓ Successfully created public namespace!"; break ;;
    400|409) echo "✓ Public namespace already exists; continuing..."; break ;;
    *)
      if is_transient "$http"; then
        echo "Namespace creation attempt $attempt: transient HTTP $http; retrying..."
        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
          echo "Namespace creation failed after $MAX_ATTEMPTS attempts (last HTTP $http)." >&2
          cat /tmp/namespace.json 2>/dev/null || true
          exit 1
        fi
        backoff "$attempt"
      else
        echo "Namespace creation failed with non-retryable HTTP $http." >&2
        cat /tmp/namespace.json 2>/dev/null || true
        exit 1
      fi
      ;;
  esac
done

echo "🎉 Lakekeeper setup completed successfully!"
echo "Lakekeeper is ready with warehouse: risingwave-warehouse"
