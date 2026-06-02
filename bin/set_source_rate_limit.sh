#!/bin/bash
# Set source_rate_limit on BOTH Casino sources (src_casino_prd, src_bets_br) on the
# running RisingWave cluster — live, without bringing anything down. MVs/sinks stay intact.
#
# source_rate_limit is PER SOURCE ACTOR (not per source). Aggregate msg/s ≈ value × #actors.
# src_casino_prd runs 4 actors (parallelism=4), so:
#   N > 0 : N rows/s per actor  → ≈ N×4 msg/s on src_casino_prd  (e.g. 150 ≈ 600/s, 200 ≈ 800/s)
#   0     : PAUSE — 0 msg/s (stop consuming)
#   -1    : UNLIMITED (no throttle)
# It's a CEILING, not a target: actual = min(N×actors, msgs available, downstream capacity).
# The casino pipeline tops out ~800/s aggregate (double-UNNEST compute), so ~150–200 is the
# sweet spot; src_bets_br is low-volume, so its cap rarely binds.
#
# Usage: set_source_rate_limit.sh [N]   (default 1)
# Wired into the script runner as the "🎚️ Set Source Rate Limit" button.

set -euo pipefail

RATE="${1:-1}"
PSQL_URL="${PSQL_URL:-postgresql://root@localhost:4566/dev}"
SOURCES=(src_casino_prd src_bets_br)

# Validate: integer, optionally negative (to allow -1).
if ! printf '%s' "$RATE" | grep -qE '^-?[0-9]+$'; then
    echo "❌ Invalid rate limit: '$RATE' — must be an integer (0 = pause, -1 = unlimited)." >&2
    exit 1
fi

echo "=== Set source_rate_limit = $RATE on both Casino sources ==="
case "$RATE" in
    0)  echo "  → 0 = PAUSE — 0 msg/s (stop consuming)" ;;
    -1) echo "  → -1 = UNLIMITED (no throttle; settles at the ~800/s compute ceiling)" ;;
    *)  echo "  → $RATE rows/s per source actor  ≈ $((RATE * 4)) msg/s aggregate on src_casino_prd (4 actors)" ;;
esac
echo "  (rate is per-actor; aggregate msg/s ≈ value × number of source actors)"
echo ""

# Friendly check: is RisingWave reachable / has the job created the sources yet?
if ! psql "$PSQL_URL" -tAc 'SELECT 1' >/dev/null 2>&1; then
    echo "❌ Cannot reach RisingWave at $PSQL_URL." >&2
    echo "   Start the stack and run the Dagster casino job first, then retry." >&2
    exit 1
fi

rc=0
for tbl in "${SOURCES[@]}"; do
    if psql "$PSQL_URL" -v ON_ERROR_STOP=1 -c "ALTER TABLE ${tbl} SET source_rate_limit = ${RATE};" >/dev/null 2>&1; then
        echo "  ✓ ${tbl}  → source_rate_limit = ${RATE}"
    else
        echo "  ⚠ ${tbl}  not found — has the casino job created it yet? (skipped)" >&2
        rc=1
    fi
done

echo ""
echo "✓ Applied. Takes effect on the next barrier (~1–2s)."
exit $rc
