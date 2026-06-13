#!/usr/bin/env bash
# End-to-end driver. Walks the full matrix:
#
#   1. enable_secondary_index_write=false  -> stream load    -> measure_load_off
#   2. drop+recreate (clear table)
#   3. enable_secondary_index_write=true   -> stream load    -> measure_load_on
#   4. measure index file count + size on storage
#   5. enable_secondary_index_read=false   -> query batch    -> measure_query_off
#   6. enable_secondary_index_read=true    -> query batch    -> measure_query_on
#
# Required env:
#   FE_HOST FE_PORT BE_HTTP   (BE HTTP host:port for /api/update_config)
#   TABLET_IDS                (comma-separated tablet ids for the table)
#   USER PASSWORD             (mysql / stream load credentials)
# Required arg: <csv_path>

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <csv_path>" >&2
    exit 2
fi

: "${FE_HOST:?missing FE_HOST}"
: "${FE_PORT:=9030}"
: "${SL_PORT:=8030}"          # stream load FE HTTP
: "${BE_HTTP:?missing BE_HTTP, e.g. be-1:8040}"
: "${TABLET_IDS:?missing TABLET_IDS, comma-separated}"
: "${USER:=root}"
: "${PASSWORD:=}"
: "${DB:=bench_sidx}"
: "${TABLE:=orders}"
: "${INDEX_NAME:=idx_town}"
: "${INDEX_COLS:=town}"
: "${RUNS:=20}"
: "${WARMUP:=3}"

csv_path="$1"
out_dir="./out/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$out_dir"

# Build the secondary_index_defs string for all tablets, e.g.
# "100:idx_town:town;101:idx_town:town;102:idx_town:town"
defs=""
IFS=',' read -ra TID_ARR <<< "$TABLET_IDS"
for tid in "${TID_ARR[@]}"; do
    [[ -n "$defs" ]] && defs+=";"
    defs+="${tid}:${INDEX_NAME}:${INDEX_COLS}"
done

apply_config() {
    local key="$1"
    local val="$2"
    # URL-encode ';' for safety
    local enc=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$val")
    curl -fsS "http://${BE_HTTP}/api/update_config?${key}=${enc}" -X POST > /dev/null \
        || { echo "update_config failed: $key=$val" >&2; exit 1; }
    echo "  config $key=$val"
}

mysql_exec() {
    mysql -h "$FE_HOST" -P "$FE_PORT" -u "$USER" --password="$PASSWORD" \
          -e "$1" "${DB}" 2>/dev/null
}

clear_table() {
    mysql_exec "TRUNCATE TABLE ${TABLE};"
    sleep 2
}

echo "=== Phase 1: load with index OFF ==="
apply_config enable_secondary_index_write false
apply_config enable_secondary_index_read false
clear_table
bash run_load.sh load_off "$csv_path" > "${out_dir}/load_off.csv"
cat "${out_dir}/load_off.csv"

echo ""
echo "=== Phase 2: clear + reload with index ON ==="
apply_config enable_secondary_index_write true
apply_config secondary_index_defs "$defs"
clear_table
bash run_load.sh load_on "$csv_path" > "${out_dir}/load_on.csv"
cat "${out_dir}/load_on.csv"

echo ""
echo "=== Phase 3: query with index OFF ==="
apply_config enable_secondary_index_read false
python3 run_query.py --label index_off --runs "$RUNS" --warmup "$WARMUP" \
    --out "${out_dir}/query_off.csv"

echo ""
echo "=== Phase 4: query with index ON ==="
apply_config enable_secondary_index_read true
python3 run_query.py --label index_on --runs "$RUNS" --warmup "$WARMUP" \
    --out "${out_dir}/query_on.csv"

echo ""
echo "=== Results ==="
echo "Load:"
cat "${out_dir}/load_off.csv" "${out_dir}/load_on.csv"
echo ""
echo "Query:"
join -t, -j 2 \
    <(tail -n +2 "${out_dir}/query_off.csv" | sort -t, -k2,2) \
    <(tail -n +2 "${out_dir}/query_on.csv"  | sort -t, -k2,2) \
    | awk -F, '{ printf "%-26s OFF p50=%7.1f p95=%7.1f p99=%7.1f  | ON p50=%7.1f p95=%7.1f p99=%7.1f  | speedup p50 %.1fx p99 %.1fx\n", \
                $1, $4, $5, $6, $11, $12, $13, $4/$11, $6/$13 }'
echo ""
echo "All results under: ${out_dir}"
