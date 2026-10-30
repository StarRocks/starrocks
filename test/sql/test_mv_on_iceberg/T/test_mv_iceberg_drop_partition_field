-- name: test_mv_iceberg_drop_partition_field
-- Test Point: ALTER TABLE ... DROP PARTITION COLUMN on an Iceberg base
--             with a composite partition spec — verify the drop itself
--             succeeds, SHOW CREATE reflects the new spec, and the
--             downstream MV's IS_ACTIVE + refresh correctness after the
--             partition-spec evolution.
-- Method: build Iceberg base with month(dt) + bucket(id, 4); create MV
--         aligned on month(dt); refresh; DROP partition column
--         bucket(id, 4); insert new rows; refresh; assert correctness.
-- Scope: Iceberg partition evolution (DROP) × MV (M3-2).
function: create_iceberg_catalog("mv_iceberg_${uuid0}", "${iceberg_sql_test_catalog_type}")

set catalog mv_iceberg_${uuid0};
create database mv_ice_db_${uuid0};
use mv_ice_db_${uuid0};
CREATE TABLE t1 (id int, dt date, val int)
PARTITION BY month(dt), bucket(id, 4);
INSERT INTO t1 VALUES
  (1, '2024-01-03', 10),
  (2, '2024-01-15', 20),
  (3, '2024-02-05', 30);

set catalog default_catalog;
create database db_${uuid0};
use db_${uuid0};
set new_planner_optimize_timeout=10000;

CREATE MATERIALIZED VIEW test_mv1
PARTITION BY (date_trunc('month', dt))
REFRESH DEFERRED MANUAL
PROPERTIES ("replication_num" = "1")
AS SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1;
[UC]REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

SELECT IS_ACTIVE FROM information_schema.materialized_views
  WHERE table_name = 'test_mv1' AND table_schema = 'db_${uuid0}';
SELECT count(*) FROM test_mv1;
SELECT id, dt, val FROM test_mv1 ORDER BY id;
function: print_hit_materialized_view("SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 ORDER BY id", "test_mv1")

-- DROP the bucket partition field; month stays
alter table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 drop partition column bucket(id, 4);

-- Verify SHOW CREATE TABLE reflects the drop
show create table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1;

-- Insert new data after drop
INSERT INTO mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 VALUES
  (4, '2024-03-10', 40),
  (5, '2024-03-25', 50);

-- Lock IS_ACTIVE + refresh behavior
SELECT IS_ACTIVE FROM information_schema.materialized_views
  WHERE table_name = 'test_mv1' AND table_schema = 'db_${uuid0}';
[UC]REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;
SELECT count(*) FROM test_mv1;
SELECT id, dt, val FROM test_mv1 ORDER BY id;
function: print_hit_materialized_view("SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 ORDER BY id", "test_mv1")

drop materialized view test_mv1;
drop table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 force;
drop database default_catalog.db_${uuid0} force;
drop database mv_iceberg_${uuid0}.mv_ice_db_${uuid0} force;
drop catalog mv_iceberg_${uuid0};
