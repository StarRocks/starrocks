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

set enable_range_distribution = true;

set enable_tablet_pre_split = false;

set enable_pipeline_engine = true;

set enable_group_execution = true;

set enable_runtime_adaptive_dop = false;

set single_node_exec_plan = false;

set parallel_fragment_exec_instance_num = 1;

set enable_query_cache = false;

set enable_phased_scheduler = false;


set enable_local_shuffle_agg = true;

set enable_sort_aggregate = true;

set new_planner_agg_stage = 0;

set group_execution_group_scale = 64;

set group_execution_max_groups = 128;

set pipeline_dop = 4;

create database db_${uuid0};

use db_${uuid0};

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

insert into no_cut values (10,20,1,'2026-01-01',1,'first'), (10,20,1,'2026-01-02',1,'last');

set range_scan_execution_mode = 'grouped';

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) from no_cut group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "fewer than two geometrically nonempty C groups")

set range_scan_execution_mode = 'colocate';

select count(*) from no_cut;

-- Seed enough C values for a real scoped four-way split before the date table is created.

create table seed (exp_id bigint not null, grp_id bigint not null, bucket_id bigint not null, u bigint)
duplicate key(exp_id, grp_id, bucket_id) order by(exp_id, grp_id, bucket_id)
properties('replication_num' = '1', 'colocate_with' = 'grp_grouped_${uuid0}:exp_id,grp_id,bucket_id');

insert into seed select 10, 20, x, x from table(generate_series(1, 40000)) as g(x);

function: set_first_tablet_id("seed")

alter table seed split tablet (${tablet_id}) properties ('tablet_reshard_target_size' = '-4');

function: wait_reshard_job_finish("seed", "SPLIT_TABLET", 1)

shell: s=false; for i in $(seq 1 60); do s=$(${mysql_cmd} -Ne "show proc '/colocation_group'" 2>/dev/null | grep "grp_grouped_${uuid0}" | awk '{print $NF}'); [ "$s" = "true" ] && break; sleep 1; done; [ "$s" = "true" ] && echo "stable=true"

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

-- Independent UV sets: extremes {MIN,0}/{MIN,MAX}; buckets 1={1,2,3}, 15000={7,8},
-- 25000={} (NULL only), 35000={9,10,MAX}. There are 23 physical rows, 18 non-NULL u.

select count(*), count(u) from t;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

set range_scan_execution_mode = 'GrOuPeD';

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: GROUPED_SCAN", "update finalize", "multi_distinct_count")

function: assert_explain_not_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "HASH_PARTITIONED", "merge finalize", "STREAMING")

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

set pipeline_dop = 1;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

set pipeline_dop = 4;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = '10' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

-- Residual on a non-partition column empties the first date, some C groups, or every task.
-- Selected source snapshots and geometric groups remain present even when no row survives.

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where marker = 'last' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where u > 0 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where u is null group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where marker = 'missing' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where 1 = 0 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where dt >= '2026-01-03' group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id having uv >= 2 order by uv desc, bucket_id limit 2;

-- Every rejection is checked for the public mode plus its specific unsupported boundary.

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where dt = '2026-01-01' group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "fewer than two selected physical partitions")

function: assert_query_error_contains("select u from t", "range_scan_execution_mode=grouped", "an aggregate is required")

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(*) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "one COUNT DISTINCT over a direct BIGINT column is required")

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u + 1) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "one COUNT DISTINCT over a direct BIGINT column is required")

function: assert_query_error_contains("select exp_id, count(distinct u) from t group by exp_id", "range_scan_execution_mode=grouped", "GROUP BY must contain exactly the three direct C columns")

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, sum(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "only COUNT DISTINCT is supported for distinct aggregates")

set enable_group_execution = false;

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "Group Execution is required")

set enable_group_execution = true;

set enable_query_cache = true;

function: assert_query_error_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "range_scan_execution_mode=grouped", "Query Cache is unsupported")

set enable_query_cache = false;

-- Replan through the established modes; the small-query colocate path retains exact results.

set range_scan_execution_mode = 'colocate';

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t where exp_id = 10 group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

function: assert_explain_not_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "GROUPED_SCAN", "PARALLEL_SCAN")

set range_scan_execution_mode = 'parallel';

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: PARALLEL_SCAN", "HASH_PARTITIONED")

set range_scan_execution_mode = 'grouped';

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

-- Refining the seed promotes a canonical K=C cut across the group; wait for alignment.
-- This does not manufacture independent historical cuts (covered by the reader unit fixtures).

set range_scan_execution_mode = 'colocate';

function: set_max_tablet_id("seed")

alter table seed split tablet (${tablet_id}) properties ('tablet_reshard_target_size' = '-2');

function: wait_reshard_job_finish("seed", "SPLIT_TABLET", 2)

shell: s=false; for i in $(seq 1 60); do s=$(${mysql_cmd} -Ne "show proc '/colocation_group'" 2>/dev/null | grep "grp_grouped_${uuid0}" | awk '{print $NF}'); [ "$s" = "true" ] && break; sleep 1; done; [ "$s" = "true" ] && echo "stable=true"

select count(*), count(u) from t;

set range_scan_execution_mode = 'grouped';

[ORDER]select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id order by exp_id, grp_id, bucket_id;

function: assert_explain_contains("select exp_id, grp_id, bucket_id, count(distinct u) as uv from t group by exp_id, grp_id, bucket_id", "SCAN EXECUTION MODE: GROUPED_SCAN", "update finalize")

set range_scan_execution_mode = 'colocate';

drop database db_${uuid0} force;

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
