-- name: test_colocate_range_grouped_scan @cloud
-- Test Point:
--   1. Complete-C UV survives duplicate/NULL/extreme values, source EOS and canonical refinement.
--   2. Grouped rejects unsupported shapes/layouts while colocate/parallel transitions remain valid.
-- Method: Exact hand-derived C/UV rows at DOP 1/4, EXPLAIN assertions and scoped split convergence.
-- Scope: Native shared-data DUP, three BIGINT NOT NULL C keys, one direct BIGINT COUNT DISTINCT.

-- Save dynamic session defaults; only these non-oracle values bypass result comparison.

[UC]saved_range_scan_execution_mode=select @@range_scan_execution_mode;

[UC]saved_enable_range_distribution=select @@enable_range_distribution;

[UC]saved_enable_tablet_pre_split=select @@enable_tablet_pre_split;

[UC]saved_enable_pipeline_engine=select @@enable_pipeline_engine;

[UC]saved_enable_group_execution=select @@enable_group_execution;

[UC]saved_enable_runtime_adaptive_dop=select @@enable_runtime_adaptive_dop;

[UC]saved_single_node_exec_plan=select @@single_node_exec_plan;

[UC]saved_parallel_fragment_exec_instance_num=select @@parallel_fragment_exec_instance_num;

[UC]saved_enable_query_cache=select @@enable_query_cache;

[UC]saved_enable_phased_scheduler=select @@enable_phased_scheduler;


[UC]saved_enable_local_shuffle_agg=select @@enable_local_shuffle_agg;

[UC]saved_enable_sort_aggregate=select @@enable_sort_aggregate;

[UC]saved_new_planner_agg_stage=select @@new_planner_agg_stage;

[UC]saved_group_execution_group_scale=select @@group_execution_group_scale;

[UC]saved_group_execution_max_groups=select @@group_execution_max_groups;

[UC]saved_pipeline_dop=select @@pipeline_dop;

CLEANUP {
set range_scan_execution_mode = 'colocate';
drop database if exists db_${uuid0} force;
set enable_range_distribution = ${saved_enable_range_distribution};
set enable_tablet_pre_split = ${saved_enable_tablet_pre_split};
set enable_pipeline_engine = ${saved_enable_pipeline_engine};
set enable_group_execution = ${saved_enable_group_execution};
set enable_runtime_adaptive_dop = ${saved_enable_runtime_adaptive_dop};
set single_node_exec_plan = ${saved_single_node_exec_plan};
set parallel_fragment_exec_instance_num = ${saved_parallel_fragment_exec_instance_num};
set enable_query_cache = ${saved_enable_query_cache};
set enable_phased_scheduler = ${saved_enable_phased_scheduler};
set enable_local_shuffle_agg = ${saved_enable_local_shuffle_agg};
set enable_sort_aggregate = ${saved_enable_sort_aggregate};
set new_planner_agg_stage = ${saved_new_planner_agg_stage};
set group_execution_group_scale = ${saved_group_execution_group_scale};
set group_execution_max_groups = ${saved_group_execution_max_groups};
set pipeline_dop = ${saved_pipeline_dop};
set range_scan_execution_mode = '${saved_range_scan_execution_mode}';
} END CLEANUP

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

set enable_range_distribution = true;
-- result:
-- !result

set enable_tablet_pre_split = false;
-- result:
-- !result

set enable_pipeline_engine = true;
-- result:
-- !result

set enable_group_execution = true;
-- result:
-- !result

set enable_runtime_adaptive_dop = false;
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


set enable_local_shuffle_agg = true;
-- result:
-- !result

set enable_sort_aggregate = true;
-- result:
-- !result

set new_planner_agg_stage = 0;
-- result:
-- !result

set group_execution_group_scale = 64;
-- result:
-- !result

set group_execution_max_groups = 128;
-- result:
-- !result

set pipeline_dop = 4;
-- result:
-- !result

create database db_${uuid0};
-- result:
-- !result

use db_${uuid0};
-- result:
-- !result

-- No internal cut: two dates alone must not license grouped execution.

create table no_cut (
    exp_id bigint not null,
    grp_id bigint not null,
    bucket_id bigint not null,
    dt date not null,
    u bigint null,
    marker varchar(8)
)
duplicate key(exp_id, grp_id, bucket_id)
partition by range(dt) (
    partition p1 values [('2026-01-01'), ('2026-01-02')),
    partition p2 values [('2026-01-02'), ('2026-01-03'))
)
order by(exp_id, grp_id, bucket_id)
properties('replication_num' = '1', 'colocate_with' = 'grp_no_cut_${uuid0}:exp_id,grp_id,bucket_id');
-- result:
-- !result

insert into no_cut values (10,20,1,'2026-01-01',1,'first'), (10,20,1,'2026-01-02',1,'last');
-- result:
-- !result

set range_scan_execution_mode = 'grouped';
-- result:
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) from no_cut group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "fewer than two geometrically nonempty C groups")
-- result:
None
-- !result

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

select count(*) from no_cut;
-- result:
2
-- !result

-- Seed enough C values for a real scoped four-way split before the date table is created.

create table seed (exp_id bigint not null, grp_id bigint not null, bucket_id bigint not null, u bigint)
duplicate key(exp_id, grp_id, bucket_id) order by(exp_id, grp_id, bucket_id)
properties('replication_num' = '1', 'colocate_with' = 'grp_grouped_${uuid0}:exp_id,grp_id,bucket_id');
-- result:
-- !result

insert into seed select 10, 20, x, x from table(generate_series(1, 40000)) as g(x);
-- result:
-- !result

function: set_first_tablet_id("seed")
-- result:
None
-- !result

alter table seed split tablet (${tablet_id}) properties ('tablet_reshard_target_size' = '-4');
-- result:
-- !result

function: wait_reshard_job_finish("seed", "SPLIT_TABLET", 1)
-- result:
None
-- !result

shell: s=false; for i in $(seq 1 60); do s=$(${mysql_cmd} -Ne "show proc '/colocation_group'" 2>/dev/null | grep "grp_grouped_${uuid0}" | awk '{print $NF}'); [ "$s" = "true" ] && break; sleep 1; done; [ "$s" = "true" ] && echo "stable=true"
-- result:
0
stable=true
-- !result

create table t (
    exp_id bigint not null,
    grp_id bigint not null,
    bucket_id bigint not null,
    dt date not null,
    u bigint null,
    marker varchar(8)
)
duplicate key(exp_id, grp_id, bucket_id)
partition by range(dt) (
    partition p1 values [('2026-01-01'), ('2026-01-02')),
    partition p2 values [('2026-01-02'), ('2026-01-03'))
)
order by(exp_id, grp_id, bucket_id)
properties('replication_num' = '1', 'colocate_with' = 'grp_grouped_${uuid0}:exp_id,grp_id,bucket_id',
    'cloud_native_fast_schema_evolution_v2' = 'true', 'file_bundling' = 'true');
-- result:
-- !result

insert into t values
    (-9223372036854775808, -9223372036854775808, -9223372036854775808, '2026-01-01', -9223372036854775808, 'first'),
    (-9223372036854775808, -9223372036854775808, -9223372036854775808, '2026-01-01', NULL, 'first'),
    (-9223372036854775808, -9223372036854775808, -9223372036854775808, '2026-01-02', -9223372036854775808, 'last'),
    (-9223372036854775808, -9223372036854775808, -9223372036854775808, '2026-01-02', 0, 'last'),
    (10, 20, 1, '2026-01-01', 1, 'first'),
    (10, 20, 1, '2026-01-01', 1, 'first'),
    (10, 20, 1, '2026-01-01', 2, 'first'),
    (10, 20, 1, '2026-01-02', 2, 'last'),
    (10, 20, 1, '2026-01-02', 3, 'last'),
    (10, 20, 1, '2026-01-02', NULL, 'last'),
    (10, 20, 15000, '2026-01-01', 7, 'first'),
    (10, 20, 15000, '2026-01-01', NULL, 'first'),
    (10, 20, 15000, '2026-01-02', 7, 'last'),
    (10, 20, 15000, '2026-01-02', 8, 'last'),
    (10, 20, 25000, '2026-01-01', NULL, 'first'),
    (10, 20, 25000, '2026-01-02', NULL, 'last'),
    (10, 20, 35000, '2026-01-01', 9, 'first'),
    (10, 20, 35000, '2026-01-02', 9, 'last'),
    (10, 20, 35000, '2026-01-02', 10, 'last'),
    (10, 20, 35000, '2026-01-02', 9223372036854775807, 'last'),
    (9223372036854775807, 9223372036854775807, 9223372036854775807, '2026-01-01', 9223372036854775807, 'first'),
    (9223372036854775807, 9223372036854775807, 9223372036854775807, '2026-01-02', -9223372036854775808, 'last'),
    (9223372036854775807, 9223372036854775807, 9223372036854775807, '2026-01-02', 9223372036854775807, 'last');
-- result:
-- !result

-- Independent UV sets: extremes {MIN,0}/{MIN,MAX}; buckets 1={1,2,3}, 15000={7,8},
-- 25000={} (NULL only), 35000={9,10,MAX}. There are 23 physical rows, 18 non-NULL u.

select count(*), count(u) from t;
-- result:
23	18
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

set range_scan_execution_mode = 'GrOuPeD';
-- result:
-- !result

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: GROUPED_SCAN", "update finalize", "multi_distinct_count")
-- result:
None
-- !result

function: assert_explain_not_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "HASH_PARTITIONED", "merge finalize", "STREAMING")
-- result:
None
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

set pipeline_dop = 1;
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

set pipeline_dop = 4;
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = '10' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
-- !result

-- Residual on a non-partition column empties the first date, some C groups, or every task.
-- Selected source snapshots and geometric groups remain present even when no row survives.

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where marker = 'last' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	2
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where u > 0 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	1	3
10	20	15000	2
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	1
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where u is null group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	0
10	20	1	0
10	20	15000	0
10	20	25000	0
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where marker = 'missing' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where 1 = 0 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where dt >= '2026-01-03' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id having uv >= 2 order by uv desc, bucket_id limit 2;
-- result:
10	20	1	3
10	20	35000	3
-- !result

-- Every rejection is checked for the public mode plus its specific unsupported boundary.

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where dt = '2026-01-01' group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "fewer than two selected physical partitions")
-- result:
None
-- !result

function: assert_query_error_contains("select u from t", "range_scan_execution_mode=grouped", "an aggregate is required")
-- result:
None
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(*) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "one COUNT DISTINCT over a direct BIGINT column is required")
-- result:
None
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u + 1) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "one COUNT DISTINCT over a direct BIGINT column is required")
-- result:
None
-- !result

function: assert_query_error_contains("select exp_id, count(distinct u) from t group by exp_id", "range_scan_execution_mode=grouped", "GROUP BY must contain exactly the three direct C columns")
-- result:
None
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, sum(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "only COUNT DISTINCT is supported for distinct aggregates")
-- result:
None
-- !result

set enable_group_execution = false;
-- result:
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "Group Execution is required")
-- result:
None
-- !result

set enable_group_execution = true;
-- result:
-- !result

set enable_query_cache = true;
-- result:
-- !result

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "Query Cache is unsupported")
-- result:
None
-- !result

set enable_query_cache = false;
-- result:
-- !result

-- Replan through the established modes; the small-query colocate path retains exact results.

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
-- !result

function: assert_explain_not_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "GROUPED_SCAN", "PARALLEL_SCAN")
-- result:
None
-- !result

set range_scan_execution_mode = 'parallel';
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: PARALLEL_SCAN", "HASH_PARTITIONED")
-- result:
None
-- !result

set range_scan_execution_mode = 'grouped';
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

-- Refining the seed promotes a canonical K=C cut across the group; wait for alignment.
-- This does not manufacture independent historical cuts (covered by the reader unit fixtures).

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

function: set_max_tablet_id("seed")
-- result:
None
-- !result

alter table seed split tablet (${tablet_id}) properties ('tablet_reshard_target_size' = '-2');
-- result:
-- !result

function: wait_reshard_job_finish("seed", "SPLIT_TABLET", 2)
-- result:
None
-- !result

shell: s=false; for i in $(seq 1 60); do s=$(${mysql_cmd} -Ne "show proc '/colocation_group'" 2>/dev/null | grep "grp_grouped_${uuid0}" | awk '{print $NF}'); [ "$s" = "true" ] && break; sleep 1; done; [ "$s" = "true" ] && echo "stable=true"
-- result:
0
stable=true
-- !result

select count(*), count(u) from t;
-- result:
23	18
-- !result

set range_scan_execution_mode = 'grouped';
-- result:
-- !result

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;
-- result:
-9223372036854775808	-9223372036854775808	-9223372036854775808	2
10	20	1	3
10	20	15000	2
10	20	25000	0
10	20	35000	3
9223372036854775807	9223372036854775807	9223372036854775807	2
-- !result

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: GROUPED_SCAN", "update finalize")
-- result:
None
-- !result

set range_scan_execution_mode = 'colocate';
-- result:
-- !result

drop database db_${uuid0} force;
-- result:
-- !result

set enable_range_distribution = ${saved_enable_range_distribution};
-- result:
-- !result

set enable_tablet_pre_split = ${saved_enable_tablet_pre_split};
-- result:
-- !result

set enable_pipeline_engine = ${saved_enable_pipeline_engine};
-- result:
-- !result

set enable_group_execution = ${saved_enable_group_execution};
-- result:
-- !result

set enable_runtime_adaptive_dop = ${saved_enable_runtime_adaptive_dop};
-- result:
-- !result

set single_node_exec_plan = ${saved_single_node_exec_plan};
-- result:
-- !result

set parallel_fragment_exec_instance_num = ${saved_parallel_fragment_exec_instance_num};
-- result:
-- !result

set enable_query_cache = ${saved_enable_query_cache};
-- result:
-- !result

set enable_phased_scheduler = ${saved_enable_phased_scheduler};
-- result:
-- !result

-- result:
-- !result

set enable_local_shuffle_agg = ${saved_enable_local_shuffle_agg};
-- result:
-- !result

set enable_sort_aggregate = ${saved_enable_sort_aggregate};
-- result:
-- !result

set new_planner_agg_stage = ${saved_new_planner_agg_stage};
-- result:
-- !result

set group_execution_group_scale = ${saved_group_execution_group_scale};
-- result:
-- !result

set group_execution_max_groups = ${saved_group_execution_max_groups};
-- result:
-- !result

set pipeline_dop = ${saved_pipeline_dop};
-- result:
-- !result

set range_scan_execution_mode = '${saved_range_scan_execution_mode}';
-- result:
-- !result
