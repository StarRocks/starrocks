-- name: test_mv_iceberg_dropped_partition
-- Test Point: an mv coarser than its Iceberg base keeps the partition covering one whose rows are
--             all deleted, so the deleted rows survive there unless that partition is recomputed.
-- Method: build an Iceberg base partitioned by identity(dt) with a sealed month; create a month
--         grained MV; refresh; delete every row of one sealed day so the partition leaves the
--         connector's live list; refresh; assert the sealed month matches the base table.
-- Scope: dropped base partition x roll-up mv x Iceberg (the DefaultTraits half of the fix).
function: create_iceberg_catalog("mv_icedrop_${uuid0}", "${iceberg_sql_test_catalog_type}")

set catalog mv_icedrop_${uuid0};
create database ice_drop_db_${uuid0};
use ice_drop_db_${uuid0};
CREATE TABLE fact_i (id int, dt date, val int)
PARTITION BY (dt);
-- 2026-08 is sealed: the later write goes to 2026-09, so the august mv partition is never
-- recomputed by accident and a passing result cannot be a coincidence.
INSERT INTO fact_i VALUES (1, '2026-08-10', 20), (2, '2026-08-11', 10), (3, '2026-09-09', 50);

set catalog default_catalog;
create database db_${uuid0};
use db_${uuid0};

CREATE MATERIALIZED VIEW mv_ice_month
PARTITION BY (date_trunc('month', dt))
REFRESH DEFERRED MANUAL
PROPERTIES ("replication_num" = "1")
AS SELECT id, dt, val FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i;
REFRESH MATERIALIZED VIEW mv_ice_month WITH SYNC MODE;

-- The sealed month must match before the delete, or the result after it means nothing.
set enable_materialized_view_rewrite = false;
SELECT sum(val) FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt < '2026-09-01';
SELECT sum(val) FROM mv_ice_month WHERE dt < '2026-09-01';

-- Emptying a partition removes it from the connector's live partition list, which is the Iceberg
-- equivalent of dropping it; 2026-08-11 survives, so the august mv partition survives with it.
DELETE FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt = '2026-08-10';
REFRESH EXTERNAL TABLE mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i;
REFRESH MATERIALIZED VIEW mv_ice_month WITH SYNC MODE;

-- Base truth is 10. An mv still reporting 30 kept the deleted partition's row.
SELECT sum(val) FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt < '2026-09-01';
SELECT sum(val) FROM mv_ice_month WHERE dt < '2026-09-01';
SELECT id, dt, val FROM mv_ice_month ORDER BY dt, id;

DROP MATERIALIZED VIEW mv_ice_month;
