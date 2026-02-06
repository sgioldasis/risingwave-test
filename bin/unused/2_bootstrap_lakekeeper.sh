#!/bin/sh
set -e

apk add --no-cache curl jq >/dev/null

echo "Waiting for Lakekeeper to be ready..."
sleep 5

echo "Bootstrapping Lakekeeper..."
for attempt in 1 2 3 4 5; do
  http=$(curl -s -o /tmp/bootstrap.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{"accept-terms-of-use": true}' \
    http://lakekeeper:8181/management/v1/bootstrap)

  case "$http" in
    200|201) echo "✓ Successfully bootstrapped Lakekeeper!"; break ;;
    400|409) echo "✓ Already bootstrapped or conflict; continuing..."; break ;;
    *) echo "Bootstrap attempt $attempt failed (HTTP $http)"; \
       [ $attempt -eq 5 ] && { cat /tmp/bootstrap.json 2>/dev/null || true; exit 1; }; \
       sleep $((2 ** attempt)) ;;
  esac
done

echo "Creating warehouse risingwave-warehouse..."
sleep 2
for attempt in 1 2 3 4 5; do
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
    http://lakekeeper:8181/management/v1/warehouse)

  case "$http" in
    200|201) echo "✓ Successfully created warehouse!"; break ;;
    400|409) echo "✓ Warehouse already exists/overlaps; continuing..."; break ;;
    *) echo "Warehouse creation attempt $attempt failed (HTTP $http)"; \
       [ $attempt -eq 5 ] && { cat /tmp/warehouse.json 2>/dev/null || true; exit 1; }; \
       sleep $((2 ** attempt)) ;;
  esac
done

# Extract warehouse ID from response or fetch it if warehouse already existed
WAREHOUSE_ID=$(cat /tmp/warehouse.json 2>/dev/null | jq -r '."warehouse-id" // .id // empty' || echo "")
if [ -z "$WAREHOUSE_ID" ]; then
  echo "Fetching existing warehouse ID..."
  WAREHOUSE_ID=$(curl -s http://lakekeeper:8181/management/v1/warehouse | jq -r '.warehouses[0].id')
fi
echo "Using warehouse ID: $WAREHOUSE_ID"

echo "Creating analytics namespace..."
sleep 2
for attempt in 1 2 3 4 5; do
  http=$(curl -s -o /tmp/namespace.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{"namespace": ["analytics"]}' \
    "http://lakekeeper:8181/catalog/v1/${WAREHOUSE_ID}/namespaces")

  case "$http" in
    200|201) echo "✓ Successfully created analytics namespace!"; break ;;
    400|409) echo "✓ Analytics namespace already exists; continuing..."; break ;;
    *) echo "Namespace creation attempt $attempt failed (HTTP $http)"; \
       [ $attempt -eq 5 ] && { cat /tmp/namespace.json 2>/dev/null || true; exit 1; }; \
       sleep $((2 ** attempt)) ;;
  esac
done

echo "🎉 Lakekeeper setup completed successfully!"
echo "Lakekeeper is ready with warehouse: risingwave-warehouse"
