-- 4-table secondary-index benchmark setup (faithful rebuild).
--   orders        : all-VARCHAR schema, served by sorted SECONDARY INDEX on (user_id, town)
--   orders_ob     : same VARCHAR schema, physical ORDER BY (user_id, town)  [control]
--   orders_bi     : BIGINT/DATETIME/DECIMAL schema, secondary index on (user_id, town)
--   orders_bi_ob  : same BIGINT schema, physical ORDER BY (user_id, town)   [control]
--
-- The index tables (orders, orders_bi) need their tablet ids registered into
-- BE config secondary_index_defs as `<tid>:idx_user_town:user_id,town` and
-- enable_secondary_index_write=true BEFORE loading, so .idx is built inline.

DROP DATABASE IF EXISTS bench_sidx FORCE;
CREATE DATABASE bench_sidx;
USE bench_sidx;

-- (1) VARCHAR + secondary index
CREATE TABLE orders (
    order_id   VARCHAR(20)  NOT NULL,
    create_ts  VARCHAR(20)  NOT NULL,
    user_id    VARCHAR(10)  NOT NULL,
    town       VARCHAR(64)  NOT NULL,
    type       VARCHAR(16)  NOT NULL,
    amount     VARCHAR(20),
    desc_blob  VARCHAR(256),
    extra1     VARCHAR(64),
    extra2     VARCHAR(64),
    extra3     VARCHAR(64)
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");

-- (2) VARCHAR + ORDER BY control
CREATE TABLE orders_ob (
    order_id   VARCHAR(20)  NOT NULL,
    create_ts  VARCHAR(20)  NOT NULL,
    user_id    VARCHAR(10)  NOT NULL,
    town       VARCHAR(64)  NOT NULL,
    type       VARCHAR(16)  NOT NULL,
    amount     VARCHAR(20),
    desc_blob  VARCHAR(256),
    extra1     VARCHAR(64),
    extra2     VARCHAR(64),
    extra3     VARCHAR(64)
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
ORDER BY (user_id, town)
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");

-- (3) BIGINT + secondary index
CREATE TABLE orders_bi (
    order_id   BIGINT NOT NULL,
    create_ts  DATETIME NOT NULL,
    user_id    BIGINT NOT NULL,
    town       VARCHAR(64) NOT NULL,
    type       VARCHAR(16) NOT NULL,
    amount     DECIMAL(18,2),
    desc_blob  VARCHAR(256),
    extra1     VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64)
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");

-- (4) BIGINT + ORDER BY control
CREATE TABLE orders_bi_ob (
    order_id   BIGINT NOT NULL,
    create_ts  DATETIME NOT NULL,
    user_id    BIGINT NOT NULL,
    town       VARCHAR(64) NOT NULL,
    type       VARCHAR(16) NOT NULL,
    amount     DECIMAL(18,2),
    desc_blob  VARCHAR(256),
    extra1     VARCHAR(64), extra2 VARCHAR(64), extra3 VARCHAR(64)
)
PRIMARY KEY(order_id, create_ts)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
ORDER BY (user_id, town)
PROPERTIES ("datacache.enable"="true","enable_persistent_index"="true","replication_num"="1");
