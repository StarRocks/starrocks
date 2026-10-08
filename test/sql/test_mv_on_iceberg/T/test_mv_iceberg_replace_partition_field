-- name: test_mv_iceberg_replace_partition_field
-- Test Point: ALTER TABLE ... REPLACE PARTITION COLUMN on an Iceberg base
--             under an MV — lock FE behavior on MV IS_ACTIVE, refresh, and
--             rewrite after the partition-spec replacement.
-- Method: build Iceberg base partitioned by identity(dt); create MV aligned
--         on dt; refresh; REPLACE the identity dt with month(dt); insert new
--         rows; refresh MV; assert.
-- Scope: Iceberg partition evolution (REPLACE) × MV (M3-3).
function: create_iceberg_catalog("mv_iceberg_${uuid0}", "${iceberg_sql_test_catalog_type}")

set catalog mv_iceberg_${uuid0};
create database mv_ice_db_${uuid0};
use mv_ice_db_${uuid0};
CREATE TABLE t1 (id int, dt date, val int)
PARTITION BY (dt);
INSERT INTO t1 VALUES
  (1, '2024-01-03', 10),
  (2, '2024-01-15', 20),
  (3, '2024-02-05', 30);

set catalog default_catalog;
create database db_${uuid0};
use db_${uuid0};
set new_planner_optimize_timeout=10000;

CREATE MATERIALIZED VIEW test_mv1
PARTITION BY dt
REFRESH DEFERRED MANUAL
PROPERTIES ("replication_num" = "1")
AS SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1;
[UC]REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- Baseline
SELECT IS_ACTIVE FROM information_schema.materialized_views
  WHERE table_name = 'test_mv1' AND table_schema = 'db_${uuid0}';
SELECT count(*) FROM test_mv1;
SELECT id, dt, val FROM test_mv1 ORDER BY id;
function: print_hit_materialized_view("SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 ORDER BY id", "test_mv1")

-- REPLACE: identity(dt) -> month(dt)
alter table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 replace partition column dt with month(dt);

-- Verify SHOW CREATE TABLE reflects new spec
show create table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1;

-- Post-evolution inserts
INSERT INTO mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 VALUES
  (4, '2024-03-10', 40),
  (5, '2024-03-25', 50);

SELECT IS_ACTIVE FROM information_schema.materialized_views
  WHERE table_name = 'test_mv1' AND table_schema = 'db_${uuid0}';

-- [UC] on refresh because FE may reject with "undergone partition evolution" error
[UC]REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;
[UC]SELECT count(*) FROM test_mv1;
[UC]SELECT id, dt, val FROM test_mv1 ORDER BY id;
function: print_hit_materialized_view("SELECT id, dt, val FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 ORDER BY id", "test_mv1")

drop materialized view test_mv1;
drop table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.t1 force;
drop database default_catalog.db_${uuid0} force;
drop database mv_iceberg_${uuid0}.mv_ice_db_${uuid0} force;
drop catalog mv_iceberg_${uuid0};
