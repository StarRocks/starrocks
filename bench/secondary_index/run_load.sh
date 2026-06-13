#!/usr/bin/env bash
# Run a single Stream Load and emit one CSV line:
#   <label>,<rows>,<bytes>,<elapsed_sec>,<rows_per_sec>,<MB_per_sec>
#
# Required env: FE_HOST, FE_PORT (for HTTP-redirected stream load), USER, PASSWORD
# Required args: <label> <csv_path>

set -euo pipefail

label="$1"
csv_path="$2"

: "${FE_HOST:?missing FE_HOST}"
: "${FE_PORT:=8030}"
: "${USER:=root}"
: "${PASSWORD:=}"
: "${DB:=bench_sidx}"
: "${TABLE:=orders}"

label_id="bench_sidx_${label}_$(date +%s)"
size_bytes=$(stat -c%s "$csv_path")

start=$(date +%s.%N)

response=$(curl -s -w "\n%{http_code}" -u "${USER}:${PASSWORD}" \
    -H "Expect: 100-continue" \
    -H "label: ${label_id}" \
    -H "format: CSV" \
    -H "column_separator: ," \
    -H "columns: order_id,create_ts,user_id,town,type,amount,desc_blob,extra1,extra2,extra3" \
    -T "$csv_path" \
    "http://${FE_HOST}:${FE_PORT}/api/${DB}/${TABLE}/_stream_load")

http=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n -1)
end=$(date +%s.%N)

if [[ "$http" != "200" ]]; then
    echo "stream load failed (http=$http): $body" >&2
    exit 1
fi

rows=$(echo "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("NumberLoadedRows",0))')
elapsed=$(python3 -c "print(f'{${end} - ${start}:.3f}')")
rows_per_sec=$(python3 -c "print(f'{${rows} / max(${end} - ${start}, 0.001):.1f}')")
mb_per_sec=$(python3 -c "print(f'{${size_bytes} / 1048576 / max(${end} - ${start}, 0.001):.1f}')")

echo "${label},${rows},${size_bytes},${elapsed},${rows_per_sec},${mb_per_sec}"
