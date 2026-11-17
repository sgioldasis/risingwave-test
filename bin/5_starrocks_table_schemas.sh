#!/bin/bash

# StarRocks SQL walkthrough — where every StarRocks DDL in this project lives,
# for showing an audience. The SQL used to be invisible: string constants
# inside Python assets. Since 2026-11-17 the asset-managed DDL is extracted
# into sql/starrocks/ (one file per object, each with a WHAT / FEATURE / WHY /
# WHERE narrative header carrying its verified gotchas), while dbt-managed
# relations stay as dbt_starrocks/models/*.sql. This script prints both homes
# and then proves the cluster actually matches the files with live
# SHOW CREATE TABLE output.

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_DIR="$REPO_ROOT/sql/starrocks"
DBT_MODELS="$REPO_ROOT/dbt_starrocks/models"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"

# SQL syntax coloring for the script-runner UI (its web renderer understands
# ANSI codes 30-37/90 + bold, line by line). Comment/narrative lines dim,
# keywords green, quoted strings yellow. Escapes are closed at line end so
# every line carries balanced codes; in a real terminal the same codes just
# render natively.
highlight_sql() {
  perl -pe '
    chomp $_;                                                # keep escapes inside their own line
    if (/^\s*--/) {
      $_ = "\e[90m$_\e[0m";                                  # comments dim
    } else {
      s/("[^"]*")/\e[33m$1\e[0m/g;                           # double-quoted strings yellow
      s/(\x27[^\x27]*\x27)/\e[33m$1\e[0m/g;                  # single-quoted strings yellow
      s/\b(CREATE|TABLE|IF|NOT|EXISTS|PRIMARY|UNIQUE|AGGREGATE|KEY|DUPLICATE|DISTRIBUTED|BY|HASH|MATERIALIZED|VIEW|AS|SELECT|FROM|WHERE|GROUP|INSERT|INTO|ROUTINE|LOAD|COLUMNS|PROPERTIES|FORMAT|KAFKA|TOPIC|SCHEDULE|EVERY|INTERVAL|TASK|SUBMIT|USER|GRANT|RESOURCE|ALTER|ON|TYPE|SUM|REPLACE|MAX|DATETIME|DATE|VARCHAR|BIGINT|INT|DECIMAL|DOUBLE|AND|IN)\b/\e[32m$1\e[0m/g;  # keywords green
    }
    $_ .= "\n";
  '
}

# File separator + name in cyan so each of the 22 files stands out in the wall of SQL.
file_header() {
  printf '\n\e[36m---------------------------------------------------------------\e[0m\n'
  printf '\e[36m\e[1m  sql/starrocks/%s\e[0m\n' "$1"
  printf '\e[36m---------------------------------------------------------------\e[0m\n'
}

cat <<'INTRO'

"Where is the StarRocks SQL?" — it lives in exactly two homes, split by who
manages it:

  sql/starrocks/            asset-managed DDL — the mechanical layer dbt
                            can't express: Routine Load jobs, AGGREGATE KEY
                            tables, the synchronous rollup MV, and the
                            serving-feature demo objects. One file per
                            object; every file opens with a narrative
                            header: WHAT it is, which StarRocks FEATURE it
                            demonstrates, WHY the demo needs it, WHERE it's
                            used (dashboard + owning Dagster asset), then
                            the verified gotchas, then the SQL.

  dbt_starrocks/models/     dbt-managed relations — the table shells the
                            Routine Loads feed (on_table_exists='ignore' so
                            dbt never drops a live table), the funnel views
                            over them, dim_users, and the async MVs. Read
                            with ref() chains, built by the dbt build step.

The reverse-ETL target (orchestration/assets/reverse_etl_starrocks_target.py)
is the one deliberate exception: its DDL is GENERATED from each sync's row
fields (columns, jsonpaths and temp-name derivations are Python logic), so
there is no static SQL file to show.

The three steps:

  Step 1  the census        -> how many files in each home
  Step 2  asset-managed SQL -> every sql/starrocks/ file: header + DDL
  Step 3  dbt-managed SQL   -> the model list with each one's materialization
  Step 4  live proof        -> SHOW CREATE TABLE straight from the running
                               cluster: what the files say is what runs

INTRO

echo "=== 1. The census ==="
echo "  asset-managed  sql/starrocks/:        $(ls "$SQL_DIR"/*.sql | wc -l | tr -d ' ') files"
echo "  dbt-managed    dbt_starrocks/models/: $(ls "$DBT_MODELS"/*.sql | wc -l | tr -d ' ') files"

echo ""
echo "=== 2. Asset-managed SQL: sql/starrocks/ (narrative header + DDL per file) ==="
for f in "$SQL_DIR"/*.sql; do
    file_header "$(basename "$f")"
    highlight_sql < "$f"
done

echo ""
echo "=== 3. dbt-managed SQL: dbt_starrocks/models/ (name + materialization) ==="
for f in "$DBT_MODELS"/*.sql; do
    m=$(grep -m1 "materialized" "$f" | sed 's/^[[:space:]]*//' || true)
    printf '  \e[36m%s\e[0m  ' "$(basename "$f")"
    case "$m" in
        *materialized_view*) printf '\e[35m%s\e[0m\n' "$m" ;;  # async MV: pink
        *table*)             printf '\e[32m%s\e[0m\n' "$m" ;;  # table: green
        *view*)              printf '\e[33m%s\e[0m\n' "$m" ;;  # view: yellow
        *)                   printf '%s\n' "${m:-(plain SQL / shared snippet)}" ;;
    esac
done
printf '  \e[90m(full model SQL: dbt_starrocks/models/*.sql — compiled versions in dbt_starrocks/target/)\e[0m\n'

echo ""
echo "=== 4. Live proof: SHOW CREATE TABLE from the running cluster ==="
for t in wallet_transactions_direct_kafka wallet_type_totals_unique \
         wallet_type_totals_agg funnel_daily_totals_agg funnel_window_stats; do
    echo ""
    printf '\e[36m--- SHOW CREATE TABLE sr_local_db.%s ---\e[0m\n' "$t"
    if out=$($MYSQL -D sr_local_db -e "SHOW CREATE TABLE sr_local_db.$t\G" 2>/dev/null); then
        # Drop the "Create Table:" label; an empty line separates the title row
        # from the DDL; unindent mysql's "       Table: ..." metadata line
        printf '%s\n' "$out" | perl -pe 's/^Create Table:\s*/\n/; s/^\s+(Table: )/$1/' | highlight_sql
    else
        echo "  (not built yet — run the StarRocks Demo button first)"
    fi
done
echo ""
echo "--- The Routine Load jobs actually running ---"
$MYSQL -D sr_local_db -e "SHOW ROUTINE LOAD" 2>/dev/null | awk -F'\t' '
    NR==1 { for (i = 1; i <= NF; i++) h[i] = $i; next }
    {
        for (i = 1; i <= NF; i++) {
            if (h[i] == "Name") printf "%s=\033[36m%s\033[0m ", h[i], $i;
            else if (h[i] == "State") {
                c = ($i == "RUNNING") ? "\033[32m" : "\033[33m";   # RUNNING green, anything else yellow
                printf "%s=%s%s\033[0m ", h[i], c, $i;
            }
        }
        print ""
    }' \
    || echo "  (none — run the StarRocks Demo button first)"
