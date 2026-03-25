-- name: test_mv_bucket_only_optimize @slow @native
-- Test Point: verify a bucket-only optimize on the base table does not set the async materialized view inactive.
-- 1. The MV should stay active after `ALTER TABLE ... PARTITION(...) DISTRIBUTED BY HASH(...) BUCKETS ...`.
-- 2. After the optimize, new rows written into the base table should be refreshed into the MV correctly.
-- 3. The MV should still support sync refresh, transparent rewrite, and direct MV reads after the optimize finishes.

create database db_${uuid0};
use db_${uuid0};

CREATE TABLE t1 (
    k1 int,
    k2 date,
    k3 string
)
DUPLICATE KEY(k1)
PARTITION BY RANGE(k2) (
    PARTITION p1 VALUES [('2020-06-01'), ('2020-07-01')),
    PARTITION p2 VALUES [('2020-07-01'), ('2020-08-01'))
)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');

CREATE MATERIALIZED VIEW mv1
REFRESH MANUAL
AS select sum(k1), k2, k3 from t1 group by k2, k3;
[UC]REFRESH MATERIALIZED VIEW mv1 with sync mode;

SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views
WHERE table_name = 'mv1' and TABLE_SCHEMA = 'db_${uuid0}';
function: print_hit_materialized_view("select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;", "mv1")

ALTER TABLE t1 PARTITION(p1) DISTRIBUTED BY HASH(k1) BUCKETS 1;
function: wait_optimize_table_finish()

SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views
WHERE table_name = 'mv1' and TABLE_SCHEMA = 'db_${uuid0}';
INSERT INTO t1 VALUES (4,'2020-06-02','BJ');
[UC]REFRESH MATERIALIZED VIEW mv1 with sync mode;
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views
WHERE table_name = 'mv1' and TABLE_SCHEMA = 'db_${uuid0}';
function: print_hit_materialized_view("select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;", "mv1")
select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;
select * from mv1 order by 2, 3;

drop database db_${uuid0} force;
