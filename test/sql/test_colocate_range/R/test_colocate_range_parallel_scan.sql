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
-- result:
-- !result

set enable_range_distribution = true;
-- result:
-- !result

set enable_pipeline_engine = true;
-- result:
-- !result

set single_node_exec_plan = false;
-- result:
-- !result

set parallel_fragment_exec_instance_num = 1;
-- result:
-- !result

set enable_query_cache = false;
-- result:
-- !result

set enable_phased_scheduler = false;
-- result:
-- !result

set enable_lake_prepared_physical_split_scan = false;
-- result:
-- !result

set enable_local_shuffle_agg = true;
-- result:
-- !result

create database db_${uuid0};
-- result:
-- !result

use db_${uuid0};
-- result:
-- !result

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
-- result:
-- !result

insert into t values
    (10, 20, 30, '2026-01-01', 1),
    (10, 20, 31, '2026-01-01', 2),
    (10, 20, 30, '2026-01-02', 2),
    (10, 20, 31, '2026-01-02', 3),
    (10, 20, 30, '2026-01-03', NULL),
    (10, 20, 31, '2026-01-03', 3),
    (10, 20, 30, '2026-01-03', 4);
-- result:
-- !result

alter table t add column extra int default '5';
-- result:
-- !result

function: wait_alter_table_finish()
-- result:
None
-- !result

insert into t (exp_id, grp_id, bucket_id, dt, ugid) values (10, 20, 30, '2026-01-01', 1);
-- result:
-- !result

set range_scan_execution_mode = 'CoLoCaTe';
-- result:
-- !result

set pipeline_dop = 1;
-- result:
-- !result

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;
-- result:
8	7	1	16	4	40
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	30	5	4	1	8	3	25
10	20	31	3	3	0	8	2	15
-- !result

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;
-- result:
2026-01-01	3	3	0	4	2	15
2026-01-02	2	2	0	5	2	10
2026-01-03	3	2	1	7	2	15
-- !result

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;
-- result:
10	20	30	2026-01-01	2	2	0	2	1
10	20	30	2026-01-02	1	1	0	2	1
10	20	30	2026-01-03	2	1	1	4	1
10	20	31	2026-01-01	1	1	0	2	1
10	20	31	2026-01-02	1	1	0	3	1
10	20	31	2026-01-03	1	1	0	3	1
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;
-- result:
0	0	None	0	None
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;
-- result:
0	0	None	0	None
-- !result

set pipeline_dop = 4;
-- result:
-- !result

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;
-- result:
8	7	1	16	4	40
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	30	5	4	1	8	3	25
10	20	31	3	3	0	8	2	15
-- !result

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;
-- result:
2026-01-01	3	3	0	4	2	15
2026-01-02	2	2	0	5	2	10
2026-01-03	3	2	1	7	2	15
-- !result

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;
-- result:
10	20	30	2026-01-01	2	2	0	2	1
10	20	30	2026-01-02	1	1	0	2	1
10	20	30	2026-01-03	2	1	1	4	1
10	20	31	2026-01-01	1	1	0	2	1
10	20	31	2026-01-02	1	1	0	3	1
10	20	31	2026-01-03	1	1	0	3	1
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;
-- result:
0	0	None	0	None
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;
-- result:
0	0	None	0	None
-- !result

set range_scan_execution_mode = 'PaRaLlEl';
-- result:
-- !result

set pipeline_dop = 1;
-- result:
-- !result

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;
-- result:
8	7	1	16	4	40
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	30	5	4	1	8	3	25
10	20	31	3	3	0	8	2	15
-- !result

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;
-- result:
2026-01-01	3	3	0	4	2	15
2026-01-02	2	2	0	5	2	10
2026-01-03	3	2	1	7	2	15
-- !result

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;
-- result:
10	20	30	2026-01-01	2	2	0	2	1
10	20	30	2026-01-02	1	1	0	2	1
10	20	30	2026-01-03	2	1	1	4	1
10	20	31	2026-01-01	1	1	0	2	1
10	20	31	2026-01-02	1	1	0	3	1
10	20	31	2026-01-03	1	1	0	3	1
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;
-- result:
0	0	None	0	None
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;
-- result:
0	0	None	0	None
-- !result

set pipeline_dop = 4;
-- result:
-- !result

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;
-- result:
8	7	1	16	4	40
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	30	5	4	1	8	3	25
10	20	31	3	3	0	8	2	15
-- !result

[ORDER]select dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra)
from t group by dt order by dt;
-- result:
2026-01-01	3	3	0	4	2	15
2026-01-02	2	2	0	5	2	10
2026-01-03	3	2	1	7	2	15
-- !result

[ORDER]select exp_id, grp_id, bucket_id, dt, count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid)
from t group by exp_id, grp_id, bucket_id, dt order by exp_id, grp_id, bucket_id, dt;
-- result:
10	20	30	2026-01-01	2	2	0	2	1
10	20	30	2026-01-02	1	1	0	2	1
10	20	30	2026-01-03	2	1	1	4	1
10	20	31	2026-01-01	1	1	0	2	1
10	20	31	2026-01-02	1	1	0	3	1
10	20	31	2026-01-03	1	1	0	3	1
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where 1 = 0;
-- result:
0	0	None	0	None
-- !result

select count(*), count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t where ugid = 99;
-- result:
0	0	None	0	None
-- !result

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(*) from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: PARALLEL_SCAN", "EFFECTIVE DISTRIBUTION: ANY, STORAGE DISTRIBUTION: RANGE", "HASH_PARTITIONED")
-- result:
None
-- !result

function: assert_explain_contains("select count(distinct ugid) from t", "SCAN EXECUTION MODE: PARALLEL_SCAN", "EXCHANGE", "UNPARTITIONED")
-- result:
None
-- !result

set role all;
-- result:
-- !result

set enable_sql_transaction = true;
-- result:
-- !result

begin;
-- result:
-- !result

commit;
-- result:
-- !result

begin;
-- result:
-- !result

rollback;
-- result:
-- !result

set enable_sql_transaction = false;
-- result:
-- !result

function: assert_query_error_contains("select count(distinct ugid) from t where dt = '2026-01-01'", "range_scan_execution_mode=parallel", "at least two selected logical partitions are required")
-- result:
None
-- !result

function: assert_query_error_contains("select ugid from t", "range_scan_execution_mode=parallel", "an aggregate is required")
-- result:
None
-- !result

function: assert_query_error_contains("set range_scan_execution_mode = 'auto'", "Variable 'range_scan_execution_mode' can't be set to the value of 'auto'")
-- result:
None
-- !result

function: assert_query_error_contains("set range_scan_execution_mode = 'unknown'", "Variable 'range_scan_execution_mode' can't be set to the value of 'unknown'")
-- result:
None
-- !result

select count(*), count(ugid), count(*) - count(ugid), sum(ugid), count(distinct ugid), sum(extra) from t;
-- result:
8	7	1	16	4	40
-- !result

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

drop database db_${uuid0};
-- result:
-- !result
