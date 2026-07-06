#!/usr/bin/env bash
# Validate the buffered multi-run secondary-index write design on one binary by
# loading `orders` twice with different buffer sizes:
#   BUFFER_MB=100000  -> ~1 run/index  (≈ old accumulate-all behavior) = baseline
#   BUFFER_MB=100     -> multi-run                                      = new design
# Captures: load LoadTimeMs, peak CN RSS during load, run count, then the
# lookup selectivity sweep (MAX(amount) WHERE user_id BETWEEN ...).
#
# Env: FE_HOST, CN_HOST (ip), CN_HTTP(ip:8040), CSV
set -euo pipefail
: "${FE_HOST:?}"; : "${CN_HOST:?}"; : "${CN_HTTP:?}"; : "${CSV:=/home/disk3/luoyixin/orders_100m.csv}"
SSHP='sshpass -p sr@test ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
myq(){ mysql -h "$FE_HOST" -P 9030 -u root -N -e "$1" bench_sidx 2>/dev/null; }
cfg(){ local e; e=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$2"); curl -fsS -X POST "http://${CN_HTTP}/api/update_config?$1=${e}" >/dev/null && echo "  cfg $1=$2"; }
COLS="order_id,create_ts,user_id,town,type,amount,desc_blob,extra1,extra2,extra3"

ddl(){
mysql -h "$FE_HOST" -P 9030 -u root 2>/dev/null <<'SQL'
DROP DATABASE IF EXISTS bench_sidx FORCE;
CREATE DATABASE bench_sidx; USE bench_sidx;
CREATE TABLE orders (
  order_id VARCHAR(20) NOT NULL, create_ts VARCHAR(20) NOT NULL, user_id VARCHAR(10) NOT NULL,
  town VARCHAR(64) NOT NULL, type VARCHAR(16) NOT NULL, amount VARCHAR(20),
  desc_blob VARCHAR(256), extra1 VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64))
PRIMARY KEY(order_id, create_ts) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");
SQL
mysql -h "$FE_HOST" -P 9030 -u root -e "ADMIN SET FRONTEND CONFIG ('lake_compaction_max_tasks'='0');" 2>/dev/null
}

run_load(){  # $1=label  $2=buffer_mb
  local label="$1" bmb="$2"
  myq "TRUNCATE TABLE orders;"; sleep 2
  local tid; tid=$(myq "SHOW TABLET FROM bench_sidx.orders" | awk '{print $1}' | head -1)
  cfg enable_load_spill false
  cfg enable_secondary_index_write true
  cfg secondary_index_build_mem_limit_mb 512
  cfg secondary_index_buffer_mb "$bmb"
  cfg secondary_index_defs "${tid}:idx_user_town:user_id,town"
  # peak RSS sampler on CN during load
  ( for i in $(seq 1 2000); do $SSHP sr@"$CN_HOST" "ps -o rss= -C starrocks_be 2>/dev/null | head -1" 2>/dev/null; sleep 2; done ) > /tmp/rss_$label.txt 2>/dev/null &
  local sampler=$!
  local t0; t0=$(date +%s)
  curl -s --location-trusted -u root: -H "Expect: 100-continue" -H "label: vb_${label}_$(date +%s)" \
     -H "format: CSV" -H "column_separator: ," -H "columns: ${COLS}" -H "timeout: 7200" \
     -T "$CSV" "http://${FE_HOST}:8030/api/bench_sidx/orders/_stream_load" \
   | python3 -c "import json,sys;d=json.load(sys.stdin);print('  '+\"$label\",'Status',d.get('Status'),'rows',d.get('NumberLoadedRows'),'LoadMs',d.get('LoadTimeMs'))"
  echo "  wall=$(($(date +%s)-t0))s"
  kill $sampler 2>/dev/null || true
  local peak; peak=$(sort -n /tmp/rss_$label.txt 2>/dev/null | tail -1)
  echo "  peak CN RSS = $(python3 -c "print(f'{${peak:-0}/1024/1024:.2f} GB')")"
  local runs; runs=$($SSHP sr@"$CN_HOST" "grep -h 'collector: built' /home/disk1/sr/be/log/cn.INFO 2>/dev/null | grep -c '$tid'" 2>/dev/null | tr -d '[:space:]')
  echo "  run files built for tablet $tid = $runs"
  echo "$tid"
}

sweep(){  # $1=label
  cfg enable_secondary_index_read true
  cfg enable_secondary_index_covering false
  timeit(){ local q="$1" best=99999 t0 d; for i in 1 2 3 4 5 6; do t0=$(date +%s.%N); myq "$q">/dev/null; d=$(python3 -c "print(($(date +%s.%N)-$t0)*1000)"); best=$(python3 -c "print(min($best,$d))"); done; printf "%.0f" "$best"; }
  for spec in "0.0005%|user_id='12345'" "0.55%|user_id BETWEEN '12345' AND '12444'" "5.5%|user_id BETWEEN '14000' AND '14999'" "27.8%|user_id BETWEEN '10000' AND '14999'"; do
    local sel="${spec%%|*}" p="${spec##*|}"
    echo "  [$1] $sel : MAX(amount) = $(timeit "SELECT MAX(amount) FROM orders WHERE $p") ms"
  done
}

echo "===== DDL ====="; ddl
echo "===== A) baseline: buffer=100000 MB (~1 run) ====="; run_load baseline 100000
echo "  --- query sweep (baseline) ---"; sweep baseline
echo "===== B) new: buffer=100 MB (multi-run) ====="; run_load multirun 100
echo "  --- query sweep (multi-run) ---"; sweep multirun
echo "DONE"
