#!/usr/bin/env bash
set -euo pipefail

start=$(date +%s)
end=$((start + 300))
last_slot=-1
out="/tmp/compaction_live_5m_$(date +%s).csv"

echo "ts,active_files,added_70s,deleted_70s,replace_70s,c0_running,c1_running" > "$out"

while [ "$(date +%s)" -lt "$end" ]; do
  now=$(date +%s)
  slot=$((now / 10))
  if [ "$slot" -ne "$last_slot" ]; then
    last_slot="$slot"
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    active=$(docker exec trino trino --server http://localhost:8080 --catalog datalake --schema public --execute "SELECT count(*) FROM \"rw_managed_funnel\$files\"" 2>/dev/null | tr -d '"\r' | tail -n1)

    snap=$(docker exec trino trino --server http://localhost:8080 --catalog datalake --schema public --execute "SELECT coalesce(sum(CAST(coalesce(element_at(summary,'added-data-files'),'0') AS integer)),0), coalesce(sum(CAST(coalesce(element_at(summary,'deleted-data-files'),'0') AS integer)),0), sum(CASE WHEN operation='replace' THEN 1 ELSE 0 END) FROM \"rw_managed_funnel\$snapshots\" WHERE committed_at > current_timestamp - INTERVAL '70' SECOND" 2>/dev/null | tr -d '"\r' | tail -n1)

    added=$(echo "$snap" | cut -d',' -f1)
    deleted=$(echo "$snap" | cut -d',' -f2)
    repl=$(echo "$snap" | cut -d',' -f3)

    c0=$(docker logs --since 70s compactor-0 2>&1 | rg -o "running_parallelism_count=[0-9]+" | tail -n1 | cut -d= -f2)
    c1=$(docker logs --since 70s compactor-1 2>&1 | rg -o "running_parallelism_count=[0-9]+" | tail -n1 | cut -d= -f2)
    c0=${c0:-0}
    c1=${c1:-0}

    echo "$ts,$active,$added,$deleted,$repl,$c0,$c1" | tee -a "$out"
  fi
done

echo "--- SUMMARY ---"
awk -F, 'NR>1{
  n++
  if (n==1) first=$2
  last=$2
  add+=$3
  del+=$4
  rep+=$5
  if ($6>0) c0pos++
  if ($7>0) c1pos++
}
END {
  print "samples=" n
  print "active_files_first=" first
  print "active_files_last=" last
  print "active_files_delta=" (last-first)
  print "added_sum=" add
  print "deleted_sum=" del
  print "net_add_minus_delete=" (add-del)
  print "replace_events_seen=" rep
  print "samples_c0_running_gt0=" c0pos+0
  print "samples_c1_running_gt0=" c1pos+0
}' "$out"

echo "csv_path=$out"
