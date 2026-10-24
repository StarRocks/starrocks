-- name: test_mv_iceberg_dropped_partition
function: create_iceberg_catalog("mv_icedrop_${uuid0}", "${iceberg_sql_test_catalog_type}")
-- result:
None
-- !result
set catalog mv_icedrop_${uuid0};
-- result:
-- !result
create database ice_drop_db_${uuid0};
-- result:
-- !result
use ice_drop_db_${uuid0};
-- result:
-- !result
CREATE TABLE fact_i (id int, dt date, val int)
PARTITION BY (dt);
-- result:
-- !result
INSERT INTO fact_i VALUES (1, '2026-08-10', 20), (2, '2026-08-11', 10), (3, '2026-09-09', 50);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW mv_ice_month
PARTITION BY (date_trunc('month', dt))
REFRESH DEFERRED MANUAL
PROPERTIES ("replication_num" = "1")
AS SELECT id, dt, val FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv_ice_month WITH SYNC MODE;
set enable_materialized_view_rewrite = false;
-- result:
-- !result
SELECT sum(val) FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt < '2026-09-01';
-- result:
30
-- !result
SELECT sum(val) FROM mv_ice_month WHERE dt < '2026-09-01';
-- result:
30
-- !result
DELETE FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt = '2026-08-10';
-- result:
-- !result
REFRESH EXTERNAL TABLE mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv_ice_month WITH SYNC MODE;
SELECT sum(val) FROM mv_icedrop_${uuid0}.ice_drop_db_${uuid0}.fact_i WHERE dt < '2026-09-01';
-- result:
10
-- !result
SELECT sum(val) FROM mv_ice_month WHERE dt < '2026-09-01';
-- result:
10
-- !result
SELECT id, dt, val FROM mv_ice_month ORDER BY dt, id;
-- result:
2	2026-08-11	10
3	2026-09-09	50
-- !result
DROP MATERIALIZED VIEW mv_ice_month;
-- result:
-- !result