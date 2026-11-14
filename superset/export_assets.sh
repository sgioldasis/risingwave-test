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

echo "=== Logging into Superset (JWT) ==="
# /api/v1/* rejects plain session cookies here ("Missing Authorization
# Header"), so the API calls below need a bearer token, not the old
# form-login + cookie-jar approach this script used before.
TOKEN=$(curl -s -m 10 -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASS\", \"provider\": \"db\", \"refresh\": true}" \
  | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token', ''))")
if [ -z "$TOKEN" ]; then
  echo "❌ JWT login failed -- is the stack up at $SUPERSET_URL?"
  exit 1
fi
AUTH="Authorization: Bearer $TOKEN"

echo "=== Fetching all dashboard IDs ==="
DASH_IDS=$(curl -s -m 10 "$SUPERSET_URL/api/v1/dashboard/?q=(page_size:100)" \
  -H "$AUTH" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(','.join(str(r['id']) for r in d['result']))")

if [ -z "$DASH_IDS" ]; then
  echo "No dashboards found -- nothing to export."
  exit 0
fi
echo "Exporting dashboards: $DASH_IDS"

echo "=== Exporting via /api/v1/dashboard/export/ ==="
curl -s -m 30 "$SUPERSET_URL/api/v1/dashboard/export/?q=!($DASH_IDS)" \
  -H "$AUTH" \
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

echo "✅ Exported to superset/assets/export/ -- review with 'git status' / 'git diff' and commit."
