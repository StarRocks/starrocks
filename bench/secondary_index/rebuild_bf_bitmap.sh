#!/usr/bin/env bash
# Base table with native indexes: orders_plain (no index), orders_bf (bloom
# filter on user_id), orders_bm (bitmap index on user_id). Same VARCHAR schema
# + same CSV. For measuring how BF/bitmap change the base-table selectivity sweep.
set -euo pipefail
: "${FE_HOST:?}"; : "${CSV:=/home/disk3/luoyixin/orders_100m.csv}"
FP=9030; SL=8030
myq() { mysql -h "$FE_HOST" -P $FP -u root -N -e "$1" bench_sidx 2>/dev/null; }
COLS="order_id,create_ts,user_id,town,type,amount,desc_blob,extra1,extra2,extra3"
load() { local t="$1"; local t0; t0=$(date +%s); curl -s --location-trusted -u root: -H "Expect: 100-continue" -H "label: bf_${t}_$(date +%s)" -H "format: CSV" -H "column_separator: ," -H "columns: ${COLS}" -H "timeout: 7200" -T "$CSV" "http://${FE_HOST}:${SL}/api/bench_sidx/${t}/_stream_load" | python3 -c "import json,sys;d=json.load(sys.stdin);print('  '+\"$t\",d.get('Status'),'rows',d.get('NumberLoadedRows'),'ms',d.get('LoadTimeMs'),'msg',d.get('Message'))"; echo "  wall=$(($(date +%s)-t0))s"; }

echo "=== schema (plain / bloom-filter / bitmap) ==="
mysql -h "$FE_HOST" -P $FP -u root 2>&1 <<'SQL' | grep -iE "error|ERROR" || echo "  DDL ok"
DROP DATABASE IF EXISTS bench_sidx FORCE;
CREATE DATABASE bench_sidx; USE bench_sidx;

CREATE TABLE orders_plain (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64))
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");

CREATE TABLE orders_bf (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64))
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1",
            "bloom_filter_columns"="user_id");

CREATE TABLE orders_bm (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64),
  INDEX idx_uid (user_id) USING BITMAP)
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");
SQL
echo "tables:"; myq "SHOW TABLES FROM bench_sidx"
echo "=== load ==="
load orders_plain
load orders_bf
load orders_bm
echo "=== row counts ==="
for t in orders_plain orders_bf orders_bm; do printf "  %-14s %s\n" "$t" "$(myq "SELECT COUNT(*) FROM $t")"; done
echo DONE
