#!/usr/bin/env bash
# Faithful 4-table rebuild + load for the covering-index validation.
#   1. create db + 4 tables
#   2. look up the single tablet id of orders / orders_bi
#   3. register secondary index (user_id,town) on those two tablets + enable write
#   4. stream-load the same CSV into all 4 tables
#
# Env: FE_HOST, FE_PORT(9030), SL_PORT(8030), CN_HTTP(host:8040), CSV, USER(root), PASSWORD
set -euo pipefail
: "${FE_HOST:?}"; : "${FE_PORT:=9030}"; : "${SL_PORT:=8030}"; : "${CN_HTTP:?}"
: "${CSV:?}"; : "${USER:=root}"; : "${PASSWORD:=}"; : "${DB:=bench_sidx}"
COLS="order_id,create_ts,user_id,town,type,amount,desc_blob,extra1,extra2,extra3"

myq() { mysql -h "$FE_HOST" -P "$FE_PORT" -u "$USER" --password="$PASSWORD" -N -e "$1" 2>/dev/null; }
cfg() { local k="$1" v="$2"; local e=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$v")
        curl -fsS -X POST "http://${CN_HTTP}/api/update_config?${k}=${e}" >/dev/null && echo "  cfg $k=$v"; }
tablet_of() { myq "SHOW TABLET FROM ${DB}.$1" | awk '{print $1}' | head -1; }
load() { local tbl="$1"; local lbl="rb_${tbl}_$(date +%s)"
    echo "  -> stream load $tbl ..."
    curl -s -w '\n%{http_code}' -u "${USER}:${PASSWORD}" -H "Expect: 100-continue" \
        -H "label: ${lbl}" -H "format: CSV" -H "column_separator: ," -H "columns: ${COLS}" \
        -H "timeout: 7200" -T "$CSV" \
        "http://${FE_HOST}:${SL_PORT}/api/${DB}/${tbl}/_stream_load" | tail -8; echo; }

echo "=== create schema ==="
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$USER" --password="$PASSWORD" < setup_4tables.sql
echo "tables:"; myq "SHOW TABLES FROM ${DB}"

TID_ORD=$(tablet_of orders); TID_BI=$(tablet_of orders_bi)
echo "=== tablet ids: orders=$TID_ORD orders_bi=$TID_BI ==="

echo "=== register index + enable write ==="
cfg enable_secondary_index_write true
cfg secondary_index_defs "${TID_ORD}:idx_user_town:user_id,town;${TID_BI}:idx_user_town:user_id,town"

echo "=== load all 4 tables (idx tables first, while write enabled) ==="
load orders
load orders_bi
load orders_ob
load orders_bi_ob

echo "=== row counts ==="
for t in orders orders_ob orders_bi orders_bi_ob; do
    printf "  %-14s %s\n" "$t" "$(myq "SELECT COUNT(*) FROM ${DB}.$t")"
done
echo "TID_ORD=$TID_ORD TID_BI=$TID_BI"
