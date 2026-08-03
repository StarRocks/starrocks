-- name: test_iceberg_mv_pct_schema_drift_detect
create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "${iceberg_catalog_hive_metastore_uris}"
);
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
set enable_materialized_view_rewrite = false;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database c1_db_${uuid0};
-- result:
-- !result
use c1_db_${uuid0};
-- result:
-- !result
create table c1_base (k int, v int) properties('format-version'='2');
-- result:
-- !result
insert into c1_base values (1, 10), (2, 20);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW c1_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c1_db_${uuid0}.c1_base;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c1_mv WITH SYNC MODE;
[UC]C1_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c1_mv';
SELECT k, v FROM c1_mv ORDER BY k;
-- result:
1	10
2	20
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
use c1_db_${uuid0};
-- result:
-- !result
ALTER TABLE c1_base MODIFY COLUMN v BIGINT;
-- result:
-- !result
INSERT INTO c1_base VALUES (3, 5000000000), (4, 99);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c1_mv WITH SYNC MODE;
SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C1_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
FAILED
-- !result
SELECT ERROR_MESSAGE LIKE '%column schema not compatible%' FROM information_schema.task_runs WHERE TASK_NAME = '${C1_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
1
-- !result
SELECT IS_ACTIVE, INACTIVE_REASON LIKE '%column schema not compatible%' FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c1_mv';
-- result:
false	1
-- !result
SELECT k, v FROM c1_mv ORDER BY k;
-- result:
1	10
2	20
-- !result
DROP MATERIALIZED VIEW c1_mv;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database c2_db_${uuid0};
-- result:
-- !result
use c2_db_${uuid0};
-- result:
-- !result
create table c2_base (k int, v int) properties('format-version'='2');
-- result:
-- !result
insert into c2_base values (1, 100), (1, 200), (2, 50);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW c2_mv
REFRESH DEFERRED MANUAL
AS SELECT k, SUM(v) s, MAX(v) mx, COUNT(*) c
   FROM mv_iceberg_${uuid0}.c2_db_${uuid0}.c2_base GROUP BY k;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c2_mv WITH SYNC MODE;
[UC]C2_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c2_mv';
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
use c2_db_${uuid0};
-- result:
-- !result
ALTER TABLE c2_base MODIFY COLUMN v BIGINT;
-- result:
-- !result
INSERT INTO c2_base VALUES (1, 5000000000);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c2_mv WITH SYNC MODE;
SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C2_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
FAILED
-- !result
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c2_mv';
-- result:
false
-- !result
DROP MATERIALIZED VIEW c2_mv;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database c3_db_${uuid0};
-- result:
-- !result
use c3_db_${uuid0};
-- result:
-- !result
create table c3_base (k int, v bigint) properties('format-version'='2');
-- result:
-- !result
insert into c3_base values (1, 10), (2, 20);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW c3_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c3_db_${uuid0}.c3_base;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c3_mv WITH SYNC MODE;
[UC]C3_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c3_mv';
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
use c3_db_${uuid0};
-- result:
-- !result
ALTER TABLE c3_base DROP COLUMN v;
-- result:
-- !result
INSERT INTO c3_base VALUES (3);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c3_mv WITH SYNC MODE;
SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C3_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
FAILED
-- !result
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c3_mv';
-- result:
false
-- !result
DROP MATERIALIZED VIEW c3_mv;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database c4_db_${uuid0};
-- result:
-- !result
use c4_db_${uuid0};
-- result:
-- !result
create table c4_base (k int, v bigint) properties('format-version'='2');
-- result:
-- !result
insert into c4_base values (1, 10), (2, 20);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW c4_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c4_db_${uuid0}.c4_base;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c4_mv WITH SYNC MODE;
[UC]C4_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c4_mv';
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
use c4_db_${uuid0};
-- result:
-- !result
ALTER TABLE c4_base RENAME COLUMN v TO v_new;
-- result:
-- !result
INSERT INTO c4_base VALUES (3, 30);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c4_mv WITH SYNC MODE;
SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C4_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
FAILED
-- !result
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c4_mv';
-- result:
false
-- !result
DROP MATERIALIZED VIEW c4_mv;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database c5_db_${uuid0};
-- result:
-- !result
use c5_db_${uuid0};
-- result:
-- !result
create table c5_base (k int, v bigint) properties('format-version'='2');
-- result:
-- !result
insert into c5_base values (1, 10), (2, 20);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE MATERIALIZED VIEW c5_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c5_db_${uuid0}.c5_base;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c5_mv WITH SYNC MODE;
[UC]C5_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c5_mv';
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
use c5_db_${uuid0};
-- result:
-- !result
DROP TABLE c5_base;
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW c5_mv WITH SYNC MODE;
SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C5_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
-- result:
FAILED
-- !result
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c5_mv';
-- result:
false
-- !result
DROP MATERIALIZED VIEW c5_mv;
-- result:
-- !result
drop database db_${uuid0} force;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
drop table c1_db_${uuid0}.c1_base force;
-- result:
-- !result
drop table c2_db_${uuid0}.c2_base force;
-- result:
-- !result
drop table c3_db_${uuid0}.c3_base force;
-- result:
-- !result
drop table c4_db_${uuid0}.c4_base force;
-- result:
-- !result
drop database c1_db_${uuid0} force;
-- result:
-- !result
drop database c2_db_${uuid0} force;
-- result:
-- !result
drop database c3_db_${uuid0} force;
-- result:
-- !result
drop database c4_db_${uuid0} force;
-- result:
-- !result
drop database c5_db_${uuid0} force;
-- result:
-- !result
drop catalog mv_iceberg_${uuid0};
-- result:
-- !result