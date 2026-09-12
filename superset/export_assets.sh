#!/bin/bash
# Re-export the current Superset dashboards/charts/datasets/databases into
# superset/assets/export/ (git-tracked YAML), so they can be reproduced on
# any machine via superset-init's auto-import on startup.
#
# Run this after making changes to dashboards/charts in the live Superset
# UI/API that you want to persist into the repo. Requires the stack to be
# up and Superset reachable at localhost:3002.

set -e

cd "$(dirname "${BASH_SOURCE[0]}")/.."

SUPERSET_URL="http://localhost:3002"
ADMIN_USER="${SUPERSET_ADMIN_USERNAME:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-sr_poc_admin_2026}"
COOKIE_JAR="$(mktemp)"

echo "=== Logging into Superset ==="
CSRF=$(curl -s -m 10 -c "$COOKIE_JAR" "$SUPERSET_URL/login/" \
  | grep -oE 'csrf_token[^>]*value="[^"]*"' | grep -oE 'value="[^"]*"' | sed 's/value="//;s/"//')
curl -s -m 10 -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
  -X POST "$SUPERSET_URL/login/" \
  --data-urlencode "csrf_token=$CSRF" \
  --data-urlencode "username=$ADMIN_USER" \
  --data-urlencode "password=$ADMIN_PASS" \
  -o /dev/null -w "login HTTP_STATUS:%{http_code}\n"

echo "=== Fetching all dashboard IDs ==="
DASH_IDS=$(curl -s -m 10 "$SUPERSET_URL/api/v1/dashboard/?q=(page_size:100)" \
  -b "$COOKIE_JAR" -H "Referer: $SUPERSET_URL/" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(','.join(str(r['id']) for r in d['result']))")

if [ -z "$DASH_IDS" ]; then
  echo "No dashboards found -- nothing to export."
  exit 0
fi
echo "Exporting dashboards: $DASH_IDS"

echo "=== Exporting via /api/v1/dashboard/export/ ==="
curl -s -m 30 "$SUPERSET_URL/api/v1/dashboard/export/?q=!($DASH_IDS)" \
  -b "$COOKIE_JAR" -H "Referer: $SUPERSET_URL/" \
  -o /tmp/superset_export.zip -w "export HTTP_STATUS:%{http_code}\n"

file /tmp/superset_export.zip | grep -q "Zip archive" || {
  echo "❌ Export did not produce a valid ZIP (see /tmp/superset_export.zip for the error body):"
  cat /tmp/superset_export.zip
  exit 1
}

echo "=== Replacing superset/assets/export/ ==="
rm -rf superset/assets/export
mkdir -p superset/assets/export
(cd superset/assets/export && unzip -q /tmp/superset_export.zip)

# The export ZIP wraps everything in a single top-level
# dashboard_export_<timestamp>/ directory -- flatten it.
EXPORT_ROOT=$(find superset/assets/export -maxdepth 1 -type d -name "dashboard_export_*")
if [ -n "$EXPORT_ROOT" ]; then
  mv "$EXPORT_ROOT"/* superset/assets/export/
  rmdir "$EXPORT_ROOT"
fi

rm -f "$COOKIE_JAR"
echo "✅ Exported to superset/assets/export/ -- review with 'git status' / 'git diff' and commit."
