#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

required_vars=(
  DBT_DATABRICKS_HOST
  DATABRICKS_AZURE_CLIENT_ID
  DATABRICKS_AZURE_CLIENT_SECRET
  DATABRICKS_AZURE_TENANT_ID
  ADLS_ACCOUNT_NAME
  ADLS_CLIENT_ID
  ADLS_CLIENT_SECRET
  ADLS_TENANT_ID
)

resolved_env="$(docker compose config --environment 2>/dev/null)"
for variable_name in "${required_vars[@]}"; do
  if ! grep -Eq "^${variable_name}=" <<<"$resolved_env"; then
    echo "Missing Compose environment variable: ${variable_name}" >&2
    exit 1
  fi
done

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available. Start Docker and retry." >&2
  exit 1
fi

echo "Starting StarRocks and catalog initialization..."
docker compose up -d starrocks starrocks-init

echo "Waiting for StarRocks catalog initialization..."
while docker inspect starrocks-init >/dev/null 2>&1; do
  init_state="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' starrocks-init)"
  case "$init_state" in
    exited\ 0)
      break
      ;;
    exited\ *)
      echo "starrocks-init failed: ${init_state}" >&2
      docker logs --tail 80 starrocks-init >&2 || true
      exit 1
      ;;
  esac
  sleep 2
done

run_sql() {
  docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw -e "$1"
}

echo "Checking StarRocks catalogs..."
run_sql "SHOW CATALOGS LIKE 'databricks_uc'; SHOW CATALOGS LIKE 'lakekeeper_local'; SHOW CATALOGS LIKE 'risingwave';"

echo "Checking Databricks Unity Catalog namespace..."
run_sql "SHOW DATABASES FROM databricks_uc; SHOW TABLES FROM databricks_uc.rw_poc;"

echo "Reading Databricks table metadata..."
run_sql "DESCRIBE databricks_uc.rw_poc.rw_casino_transactions;"

echo "Reading Databricks table data..."
run_sql "SELECT * FROM databricks_uc.rw_poc.rw_casino_transactions LIMIT 5;"

echo "StarRocks Databricks smoke test passed."
echo "MySQL: mysql -h 127.0.0.1 -P 9030 -u root"
echo "UI/API: http://localhost:8030"
