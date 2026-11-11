-- name: test_colocate_range_parallel_scan @cloud
-- Test Point: Exact range-scan aggregates preserve duplicates, NULLs and cross-date DISTINCT.
-- Method: Compare hand-derived rows in colocate/parallel modes at DOP 1/4; assert plan and rejection boundaries.
-- Scope: Range scan execution mode, global aggregate merge, schema evolution v2 and file bundling.

CLEANUP {
set range_scan_execution_mode = 'colocate';
rollback;
drop database if exists db_${uuid0} force;
} END CLEANUP

set range_scan_execution_mode = 'colocate';

set enable_range_distribution = true;

set enable_pipeline_engine = true;

set single_node_exec_plan = false;

set parallel_fragment_exec_instance_num = 1;

set enable_query_cache = false;

set enable_phased_scheduler = false;

set enable_lake_prepared_physical_split_scan = false;

set enable_local_shuffle_agg = true;

create database db_${uuid0};

use db_${uuid0};

create table t (
    exp_id int,
    grp_id int,
    bucket_id int,
    dt date,
    ugid bigint
)
duplicate key(exp_id, grp_id, bucket_id, dt)
partition by range(dt) (
    partition p1 values [('2026-01-01'), ('2026-01-02')),
    partition p2 values [('2026-01-02'), ('2026-01-03')),
    partition p3 values [('2026-01-03'), ('2026-01-04'))
)
order by(exp_id, grp_id, bucket_id, dt)
properties (
    'replication_num' = '1',
    'colocate_with' = 'grp_parallel_${uuid0}:exp_id,grp_id,bucket_id',
    'cloud_native_fast_schema_evolution_v2' = 'true',
    'file_bundling' = 'true'
);

insert into t values
    (10, 20, 30, '2026-01-01', 1),
    (10, 20, 31, '2026-01-01', 2),
    (10, 20, 30, '2026-01-02', 2),
    (10, 20, 31, '2026-01-02', 3),
    (10, 20, 30, '2026-01-03', NULL),
    (10, 20, 31, '2026-01-03', 3),
    (10, 20, 30, '2026-01-03', 4);

alter table t add column extra int default '5';

function: wait_alter_table_finish()

insert into t (exp_id, grp_id, bucket_id, dt, ugid) values (10, 20, 30, '2026-01-01', 1);

set range_scan_execution_mode = 'CoLoCaTe';

set pipeline_dop = 1;

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;

set pipeline_dop = 4;

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;

set range_scan_execution_mode = 'PaRaLlEl';

set pipeline_dop = 1;

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;

set pipeline_dop = 4;

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(*) from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: PARALLEL_SCAN", "EFFECTIVE DISTRIBUTION: ANY, STORAGE DISTRIBUTION: RANGE", "HASH_PARTITIONED")

function: assert_explain_contains("select count(distinct ugid) from t", "SCAN EXECUTION MODE: PARALLEL_SCAN", "EXCHANGE", "UNPARTITIONED")

set role all;

set enable_sql_transaction = true;

begin;

commit;

begin;

rollback;

set enable_sql_transaction = false;

function: assert_query_error_contains("select count(distinct ugid) from t where dt = '2026-01-01'", "range_scan_execution_mode=parallel", "at least two selected logical partitions are required")

function: assert_query_error_contains("select ugid from t", "range_scan_execution_mode=parallel", "an aggregate is required")

function: assert_query_error_contains("set range_scan_execution_mode = 'auto'", "Variable 'range_scan_execution_mode' can't be set to the value of 'auto'")

function: assert_query_error_contains("set range_scan_execution_mode = 'unknown'", "Variable 'range_scan_execution_mode' can't be set to the value of 'unknown'")

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;

set range_scan_execution_mode = 'colocate';

drop database db_${uuid0};
