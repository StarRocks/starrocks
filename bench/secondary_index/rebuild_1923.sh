#!/usr/bin/env bash
# Rebuild orders(+idx_user_town,+idx_ts) and orders_ob on a fresh build-1923
# cluster, then measure covering vs lookup vs ORDER BY (sort-key user_id and
# non-sort-key create_ts). Env: FE_HOST, CN_HTTP, CSV.
set -euo pipefail
: "${FE_HOST:?}"; : "${CN_HTTP:?}"; : "${CSV:=/home/disk3/luoyixin/orders_100m.csv}"
FP=9030; SL=8030
myq() { mysql -h "$FE_HOST" -P $FP -u root -N -e "$1" bench_sidx 2>/dev/null; }
cfg() { local e; e=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$2"); curl -fsS -X POST "http://${CN_HTTP}/api/update_config?$1=${e}" >/dev/null && echo "  cfg $1=$2"; }
COLS="order_id,create_ts,user_id,town,type,amount,desc_blob,extra1,extra2,extra3"
load() { local t="$1"; local t0; t0=$(date +%s); curl -s --location-trusted -u root: -H "Expect: 100-continue" -H "label: r23_${t}_$(date +%s)" -H "format: CSV" -H "column_separator: ," -H "columns: ${COLS}" -H "timeout: 7200" -T "$CSV" "http://${FE_HOST}:${SL}/api/bench_sidx/${t}/_stream_load" | python3 -c "import json,sys;d=json.load(sys.stdin);print('  '+\"$t\",d.get('Status'),'rows',d.get('NumberLoadedRows'),'ms',d.get('LoadTimeMs'))"; echo "  wall=$(($(date +%s)-t0))s"; }

echo "=== schema ==="
mysql -h "$FE_HOST" -P $FP -u root 2>/dev/null <<'SQL'
DROP DATABASE IF EXISTS bench_sidx FORCE;
CREATE DATABASE bench_sidx; USE bench_sidx;
CREATE TABLE orders (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64))
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");
CREATE TABLE orders_ob (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64))
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
ORDER BY (user_id, town)
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");
SQL

echo "=== disable lake compaction ==="
mysql -h "$FE_HOST" -P $FP -u root -e "ADMIN SET FRONTEND CONFIG ('lake_compaction_max_tasks'='0');" 2>/dev/null
TID=$(myq "SHOW TABLET FROM bench_sidx.orders" | awk '{print $1}' | head -1)
echo "=== orders tablet=$TID; register idx_user_town + idx_ts ==="
cfg enable_load_spill false
cfg secondary_index_build_mem_limit_mb 4096
cfg enable_secondary_index_write true
cfg secondary_index_defs "${TID}:idx_user_town:user_id,town;${TID}:idx_ts:create_ts"
echo "=== load (orders no-spill builds .idx inline) ==="
load orders
load orders_ob
cfg enable_secondary_index_read true
cfg enable_secondary_index_covering true
echo "=== row counts ==="
for t in orders orders_ob; do printf "  %-10s %s\n" "$t" "$(myq "SELECT COUNT(*) FROM $t")"; done
echo "TID=$TID DONE"
