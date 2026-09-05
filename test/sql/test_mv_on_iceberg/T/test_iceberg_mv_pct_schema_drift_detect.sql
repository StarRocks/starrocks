-- name: test_iceberg_mv_pct_schema_drift_detect

create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "${iceberg_catalog_hive_metastore_uris}"
);

set catalog default_catalog;
create database db_${uuid0};
use db_${uuid0};

set enable_materialized_view_rewrite = false;

-- ==============================================================
-- Case 1: MODIFY COLUMN widening (INT -> BIGINT) + projection MV
-- pre-fix: silent NULL on 5e9; expected: REFRESH FAILED + MV inactive
-- ==============================================================
set catalog mv_iceberg_${uuid0};
create database c1_db_${uuid0};
use c1_db_${uuid0};
create table c1_base (k int, v int) properties('format-version'='2');
insert into c1_base values (1, 10), (2, 20);

set catalog default_catalog;
use db_${uuid0};
CREATE MATERIALIZED VIEW c1_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c1_db_${uuid0}.c1_base;

[UC]REFRESH MATERIALIZED VIEW c1_mv WITH SYNC MODE;
[UC]C1_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c1_mv';

SELECT k, v FROM c1_mv ORDER BY k;

set catalog mv_iceberg_${uuid0};
use c1_db_${uuid0};
ALTER TABLE c1_base MODIFY COLUMN v BIGINT;
INSERT INTO c1_base VALUES (3, 5000000000), (4, 99);

set catalog default_catalog;
use db_${uuid0};
[UC]REFRESH MATERIALIZED VIEW c1_mv WITH SYNC MODE;

SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C1_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT ERROR_MESSAGE LIKE '%column schema not compatible%' FROM information_schema.task_runs WHERE TASK_NAME = '${C1_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT IS_ACTIVE, INACTIVE_REASON LIKE '%column schema not compatible%' FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c1_mv';
SELECT k, v FROM c1_mv ORDER BY k;

DROP MATERIALIZED VIEW c1_mv;

-- ==============================================================
-- Case 2: MODIFY COLUMN widening + aggregation MV (covers state column)
-- ==============================================================
set catalog mv_iceberg_${uuid0};
create database c2_db_${uuid0};
use c2_db_${uuid0};
create table c2_base (k int, v int) properties('format-version'='2');
insert into c2_base values (1, 100), (1, 200), (2, 50);

set catalog default_catalog;
use db_${uuid0};
CREATE MATERIALIZED VIEW c2_mv
REFRESH DEFERRED MANUAL
AS SELECT k, SUM(v) s, MAX(v) mx, COUNT(*) c
   FROM mv_iceberg_${uuid0}.c2_db_${uuid0}.c2_base GROUP BY k;

[UC]REFRESH MATERIALIZED VIEW c2_mv WITH SYNC MODE;
[UC]C2_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c2_mv';

set catalog mv_iceberg_${uuid0};
use c2_db_${uuid0};
ALTER TABLE c2_base MODIFY COLUMN v BIGINT;
INSERT INTO c2_base VALUES (1, 5000000000);

set catalog default_catalog;
use db_${uuid0};
[UC]REFRESH MATERIALIZED VIEW c2_mv WITH SYNC MODE;

SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C2_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c2_mv';

DROP MATERIALIZED VIEW c2_mv;

-- ==============================================================
-- Case 3: DROP COLUMN referenced by MV
-- ==============================================================
set catalog mv_iceberg_${uuid0};
create database c3_db_${uuid0};
use c3_db_${uuid0};
create table c3_base (k int, v bigint) properties('format-version'='2');
insert into c3_base values (1, 10), (2, 20);

set catalog default_catalog;
use db_${uuid0};
CREATE MATERIALIZED VIEW c3_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c3_db_${uuid0}.c3_base;

[UC]REFRESH MATERIALIZED VIEW c3_mv WITH SYNC MODE;
[UC]C3_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c3_mv';

set catalog mv_iceberg_${uuid0};
use c3_db_${uuid0};
ALTER TABLE c3_base DROP COLUMN v;
INSERT INTO c3_base VALUES (3);

set catalog default_catalog;
use db_${uuid0};
[UC]REFRESH MATERIALIZED VIEW c3_mv WITH SYNC MODE;

SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C3_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c3_mv';

DROP MATERIALIZED VIEW c3_mv;

-- ==============================================================
-- Case 4: RENAME COLUMN referenced by MV
-- ==============================================================
set catalog mv_iceberg_${uuid0};
create database c4_db_${uuid0};
use c4_db_${uuid0};
create table c4_base (k int, v bigint) properties('format-version'='2');
insert into c4_base values (1, 10), (2, 20);

set catalog default_catalog;
use db_${uuid0};
CREATE MATERIALIZED VIEW c4_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c4_db_${uuid0}.c4_base;

[UC]REFRESH MATERIALIZED VIEW c4_mv WITH SYNC MODE;
[UC]C4_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c4_mv';

set catalog mv_iceberg_${uuid0};
use c4_db_${uuid0};
ALTER TABLE c4_base RENAME COLUMN v TO v_new;
INSERT INTO c4_base VALUES (3, 30);

set catalog default_catalog;
use db_${uuid0};
[UC]REFRESH MATERIALIZED VIEW c4_mv WITH SYNC MODE;

SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C4_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c4_mv';

DROP MATERIALIZED VIEW c4_mv;

-- ==============================================================
-- Case 5: base TABLE dropped entirely (covers Optional.empty branch)
-- ==============================================================
set catalog mv_iceberg_${uuid0};
create database c5_db_${uuid0};
use c5_db_${uuid0};
create table c5_base (k int, v bigint) properties('format-version'='2');
insert into c5_base values (1, 10), (2, 20);

set catalog default_catalog;
use db_${uuid0};
CREATE MATERIALIZED VIEW c5_mv
REFRESH DEFERRED MANUAL
AS SELECT k, v FROM mv_iceberg_${uuid0}.c5_db_${uuid0}.c5_base;

[UC]REFRESH MATERIALIZED VIEW c5_mv WITH SYNC MODE;
[UC]C5_TASK=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME='c5_mv';

set catalog mv_iceberg_${uuid0};
use c5_db_${uuid0};
DROP TABLE c5_base;

set catalog default_catalog;
use db_${uuid0};
[UC]REFRESH MATERIALIZED VIEW c5_mv WITH SYNC MODE;

SELECT STATE FROM information_schema.task_runs WHERE TASK_NAME = '${C5_TASK}' ORDER BY CREATE_TIME DESC LIMIT 1;
SELECT IS_ACTIVE FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'c5_mv';

DROP MATERIALIZED VIEW c5_mv;

-- ==============================================================
-- Cleanup (iceberg DROP DATABASE FORCE does not cascade tables —
-- drop base tables individually first; c5_base was already dropped)
-- ==============================================================
drop database db_${uuid0} force;

set catalog mv_iceberg_${uuid0};
drop table c1_db_${uuid0}.c1_base force;
drop table c2_db_${uuid0}.c2_base force;
drop table c3_db_${uuid0}.c3_base force;
drop table c4_db_${uuid0}.c4_base force;
drop database c1_db_${uuid0} force;
drop database c2_db_${uuid0} force;
drop database c3_db_${uuid0} force;
drop database c4_db_${uuid0} force;
drop database c5_db_${uuid0} force;
drop catalog mv_iceberg_${uuid0};
