-- Lake PK table for the secondary-index benchmark.
-- Run from a mysql client connected to an FE that knows about the Lake
-- storage volume.
--
-- Index registration is done OUT-OF-BAND via BE config because the FE
-- DDL for `INDEX ... USING SORTED` is not wired in this PoC. After this
-- DDL runs, look up the tablet ids via `SHOW PROC '/dbs/<db>/<table>'`
-- and push them into each BE with:
--
--   curl -s "http://${BE_HTTP}/api/update_config?\
--     secondary_index_defs=<TID1>:idx_town:town;<TID2>:idx_town:town"

DROP DATABASE IF EXISTS bench_sidx FORCE;
CREATE DATABASE bench_sidx;
USE bench_sidx;

CREATE TABLE orders (
    order_id   BIGINT NOT NULL,
    create_ts  DATETIME NOT NULL,
    user_id    BIGINT NOT NULL,
    town       VARCHAR(64) NOT NULL,
    type       VARCHAR(16) NOT NULL,
    amount     DECIMAL(18,2),
    desc_blob  VARCHAR(256),
    extra1     VARCHAR(64),
    extra2     VARCHAR(64),
    extra3     VARCHAR(64)
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES (
    "datacache.enable"   = "true",
    "enable_persistent_index" = "true",
    "replication_num"    = "1"
);
