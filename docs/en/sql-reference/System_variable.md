---
displayed_sidebar: docs
keywords: ['session variable']
description: "StarRocks provides many system variables that can be set and modified to suit your requirements."
---

# System variables

import VariableWarehouse from '../_assets/commonMarkdown/variable_warehouse.mdx'

StarRocks provides many system variables that can be set and modified to suit your requirements. This section describes the variables supported by StarRocks. You can view the settings of these variables by running the [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) command on your MySQL client. You can also use the [SET](sql-statements/cluster-management/config_vars/SET.md) command to dynamically set or modify variables. You can make these variables take effect globally on the entire system, only in the current session, or only in a single query statement.

The variables in StarRocks refer to the variable sets in MySQL, but **some variables are only compatible with the MySQL client protocol and do not function on the MySQL database**.

> **NOTE**
>
> Any user has the privilege to run SHOW VARIABLES and make a variable take effect at session level. However, only users with the SYSTEM-level OPERATE privilege can make a variable take effect globally. Globally effective variables take effect on all the future sessions (excluding the current session).
>
> If you want to make a setting change for the current session and also make that setting change apply to all future sessions, you can make the change twice, once without the `GLOBAL` modifier and once with it. For example:
>
> ```SQL
> SET query_mem_limit = 137438953472; -- Apply to the current session.
> SET GLOBAL query_mem_limit = 137438953472; -- Apply to all future sessions.
> ```

## Variable hierarchy and types

StarRocks supports three types (levels) of variables: global variables, session variables, and `SET_VAR` hints. Their hierarchical relationship is as follows:

* Global variables take effect on global level, and can be overridden by session variables and `SET_VAR` hints.
* Session variables take effect only on the current session, and can be overridden by `SET_VAR` hints.
* `SET_VAR` hints take effect only on the current query statement.

## View variables

You can view all or some variables by using `SHOW VARIABLES [LIKE 'xxx']`. Example:

```SQL
-- Show all variables in the system.
SHOW VARIABLES;

-- Show variables that match a certain pattern.
SHOW VARIABLES LIKE '%time_zone%';
```

## Set variables

### Set variables globally or for a single session

You can set variables to take effect **globally** or **only on the current session**. When set to global, the new value will be used for all the future sessions, while the current session still uses the original value. When set to "current session only", the variable will only take effect on the current session.

A variable set by `SET <var_name> = xxx;` only takes effect for the current session. Example:

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

A variable set by `SET GLOBAL <var_name> = xxx;` takes effect globally. Example:

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

The following variables only take effect globally. They cannot take effect for a single session, which means you must use `SET GLOBAL <var_name> = xxx;` for these variables. If you try to set such a variable for a single session (`SET <var_name> = xxx;`), an error is returned.

* activate_all_roles_on_login
* character_set_database
* cngroup_low_watermark_cpu_used_permille
* cngroup_low_watermark_running_query_count
* cngroup_resource_usage_fresh_ratio
* cngroup_schedule_mode
* default_rowset_type
* enable_reduce_cast_varchar_expr_sync_type
* enable_reduce_cast_varchar_length_inheritance
* enable_group_level_query_queue
* enable_query_history
* enable_query_queue_load
* enable_query_queue_select
* enable_query_queue_statistic
* enable_table_name_case_insensitive
* enable_tde
* init_connect
* language
* license
* lower_case_table_names
* performance_schema
* query_cache_size
* query_history_keep_seconds
* query_history_load_interval_seconds
* query_queue_concurrency_limit
* query_queue_cpu_used_permille_limit
* query_queue_driver_high_water
* query_queue_driver_low_water
* query_queue_fresh_resource_usage_interval_ms
* query_queue_max_queued_queries
* query_queue_mem_used_pct_limit
* query_queue_pending_timeout_second
* system_time_zone
* version
* version_comment


In addition, variable settings also support constant expressions, such as:

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

 ```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### Set variables in a single query statement

In some scenarios, you may need to set variables specifically for certain queries. By using the `SET_VAR` hint, you can set session variables that will take effect only within a single statement.

StarRocks supports using `SET_VAR` in the following statements;

- SELECT
- INSERT (from v3.1.12 and v3.2.0 onwards)
- UPDATE (from v3.1.12 and v3.2.0 onwards)
- DELETE (from v3.1.12 and v3.2.0 onwards)

`SET_VAR` can only be placed after the above keywords and enclosed in `/*+...*/`.

Example:

```sql
SELECT /*+ SET_VAR(query_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);

UPDATE /*+ SET_VAR(insert_timeout=100) */ tbl SET c1 = 2 WHERE c1 = 1;

DELETE /*+ SET_VAR(query_mem_limit = 8589934592) */
FROM my_table PARTITION p1
WHERE k1 = 3;

INSERT /*+ SET_VAR(insert_timeout = 10000000) */
INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

You can also set multiple variables in a single statement. Example:

```sql
SELECT /*+ SET_VAR
  (
  exec_mem_limit = 515396075520,
  query_timeout=10000000,
  batch_size=4096,
  parallel_fragment_exec_instance_num=32
  )
  */ * FROM TABLE;
```

### Set variables as user properties

You can set session variables as user properties using the [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md). This feature is supported from v3.3.3.

Example:

```SQL
-- Set the session variable `query_timeout` to `600` for the user jack.
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## Variable categories

System variables are grouped into the following categories:

- [Query Optimizer and Planning](System_variables/optimizer_planning.md): Session variables that control the cost-based optimizer, statistics-driven planning, pruning, rewrite, and CTE reuse.
- [Joins, Aggregation, and Materialized Views](System_variables/joins_agg_mv.md): Session variables for join execution, aggregation, top-N, and materialized view query rewrite.
- [Execution Engine and Parallelism](System_variables/execution_parallelism.md): Session variables for the pipeline engine, parallelism, runtime filters, and query cache.
- [Memory and Spill](System_variables/memory_spill.md): Session variables that control query memory limits and spilling to disk or remote storage.
- [Scan, I/O, and Data Cache](System_variables/scan_io_datacache.md): Session variables for scan concurrency, I/O tasks, data cache, and page cache.
- [External Catalogs and Data Lake](System_variables/external_catalog_lake.md): Session variables for external catalog connectors and data lake formats such as Hive, Iceberg, and Hudi.
- [Loading, Insert, and Transactions](System_variables/loading_insert_txn.md): Session variables for data loading, INSERT behavior, and transaction handling.
- [Session, Protocol, and Governance](System_variables/session_protocol_governance.md): Session variables for MySQL protocol compatibility, query queue, timeouts, resource groups, and profiling.

## Variable index

The following table lists every system variable and the category page that documents it. Variables labeled `global` take effect only globally; others can take effect either globally or for a single session.

| Variable | Category |
| --- | --- |
| [`activate_all_roles_on_login`](System_variables/session_protocol_governance.md#activate_all_roles_on_login-global) | Session, Protocol, and Governance |
| [`allow_lake_without_partition_filter`](System_variables/external_catalog_lake.md#allow_lake_without_partition_filter) | External Catalogs and Data Lake |
| [`array_low_cardinality_optimize`](System_variables/optimizer_planning.md#array_low_cardinality_optimize) | Query Optimizer and Planning |
| [`authentication_policy`](System_variables/session_protocol_governance.md#authentication_policy) | Session, Protocol, and Governance |
| [`auto_increment_increment`](System_variables/session_protocol_governance.md#auto_increment_increment) | Session, Protocol, and Governance |
| [`avro_use_jni_reader`](System_variables/external_catalog_lake.md#avro_use_jni_reader) | External Catalogs and Data Lake |
| [`big_query_profile_threshold`](System_variables/session_protocol_governance.md#big_query_profile_threshold) | Session, Protocol, and Governance |
| [`binary_encoding_format`](System_variables/session_protocol_governance.md#binary_encoding_format) | Session, Protocol, and Governance |
| [`binary_encoding_level`](System_variables/session_protocol_governance.md#binary_encoding_level) | Session, Protocol, and Governance |
| [`blacklist_backup_routing`](System_variables/execution_parallelism.md#blacklist_backup_routing) | Execution Engine and Parallelism |
| [`broadcast_row_limit`](System_variables/joins_agg_mv.md#broadcast_row_limit) | Joins, Aggregation, and Materialized Views |
| [`catalog`](System_variables/session_protocol_governance.md#catalog) | Session, Protocol, and Governance |
| [`cbo_cte_force_reuse_limit_without_order_by`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_limit_without_order_by) | Query Optimizer and Planning |
| [`cbo_cte_force_reuse_node_count`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_node_count) | Query Optimizer and Planning |
| [`cbo_cte_max_limit`](System_variables/optimizer_planning.md#cbo_cte_max_limit) | Query Optimizer and Planning |
| [`cbo_cte_reuse`](System_variables/optimizer_planning.md#cbo_cte_reuse) | Query Optimizer and Planning |
| [`cbo_cte_reuse_rate`](System_variables/optimizer_planning.md#cbo_cte_reuse_rate) | Query Optimizer and Planning |
| [`cbo_disabled_rules`](System_variables/optimizer_planning.md#cbo_disabled_rules) | Query Optimizer and Planning |
| [`cbo_enable_low_cardinality_optimize`](System_variables/optimizer_planning.md#cbo_enable_low_cardinality_optimize) | Query Optimizer and Planning |
| [`cbo_enable_low_cardinality_optimize_for_join`](System_variables/optimizer_planning.md#cbo_enable_low_cardinality_optimize_for_join) | Query Optimizer and Planning |
| [`cbo_eq_base_type`](System_variables/optimizer_planning.md#cbo_eq_base_type) | Query Optimizer and Planning |
| [`cbo_json_v2_dict_opt`](System_variables/optimizer_planning.md#cbo_json_v2_dict_opt) | Query Optimizer and Planning |
| [`cbo_json_v2_rewrite`](System_variables/optimizer_planning.md#cbo_json_v2_rewrite) | Query Optimizer and Planning |
| [`cbo_max_reorder_node_use_dp`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_dp) | Query Optimizer and Planning |
| [`cbo_max_reorder_node_use_exhaustive`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_exhaustive) | Query Optimizer and Planning |
| [`cbo_max_reorder_node_use_greedy`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_greedy) | Query Optimizer and Planning |
| [`cbo_prune_json_subfield`](System_variables/optimizer_planning.md#cbo_prune_json_subfield) | Query Optimizer and Planning |
| [`cbo_use_correlated_predicate_estimate`](System_variables/optimizer_planning.md#cbo_use_correlated_predicate_estimate) | Query Optimizer and Planning |
| [`character_set_database`](System_variables/session_protocol_governance.md#character_set_database-global) | Session, Protocol, and Governance |
| [`collation_connection`](System_variables/session_protocol_governance.md#collation_connection) | Session, Protocol, and Governance |
| [`collation_database`](System_variables/session_protocol_governance.md#collation_database) | Session, Protocol, and Governance |
| [`collation_server`](System_variables/session_protocol_governance.md#collation_server) | Session, Protocol, and Governance |
| [`computation_fragment_scheduling_policy`](System_variables/execution_parallelism.md#computation_fragment_scheduling_policy) | Execution Engine and Parallelism |
| [`connector_io_tasks_per_scan_operator`](System_variables/external_catalog_lake.md#connector_io_tasks_per_scan_operator) | External Catalogs and Data Lake |
| [`connector_sink_compression_codec`](System_variables/external_catalog_lake.md#connector_sink_compression_codec) | External Catalogs and Data Lake |
| [`connector_sink_target_max_file_size`](System_variables/external_catalog_lake.md#connector_sink_target_max_file_size) | External Catalogs and Data Lake |
| [`count_distinct_column_buckets`](System_variables/joins_agg_mv.md#count_distinct_column_buckets) | Joins, Aggregation, and Materialized Views |
| [`custom_query_id`](System_variables/session_protocol_governance.md#custom_query_id-session) | Session, Protocol, and Governance |
| [`custom_session_name`](System_variables/session_protocol_governance.md#custom_session_name-session) | Session, Protocol, and Governance |
| [`datacache_sharing_work_period`](System_variables/scan_io_datacache.md#datacache_sharing_work_period) | Scan, I/O, and Data Cache |
| [`decimal_overflow_to_double`](System_variables/session_protocol_governance.md#decimal_overflow_to_double) | Session, Protocol, and Governance |
| [`default_authentication_plugin`](System_variables/session_protocol_governance.md#default_authentication_plugin) | Session, Protocol, and Governance |
| [`default_rowset_type`](System_variables/session_protocol_governance.md#default_rowset_type-global) | Session, Protocol, and Governance |
| [`default_storage_engine`](System_variables/session_protocol_governance.md#default_storage_engine) | Session, Protocol, and Governance |
| [`default_table_compression`](System_variables/session_protocol_governance.md#default_table_compression) | Session, Protocol, and Governance |
| [`default_tmp_storage_engine`](System_variables/session_protocol_governance.md#default_tmp_storage_engine) | Session, Protocol, and Governance |
| [`default_view_sql_security`](System_variables/session_protocol_governance.md#default_view_sql_security) | Session, Protocol, and Governance |
| [`disable_colocate_join`](System_variables/joins_agg_mv.md#disable_colocate_join) | Joins, Aggregation, and Materialized Views |
| [`disable_colocate_set`](System_variables/joins_agg_mv.md#disable_colocate_set) | Joins, Aggregation, and Materialized Views |
| [`disable_join_reorder`](System_variables/joins_agg_mv.md#disable_join_reorder) | Joins, Aggregation, and Materialized Views |
| [`disable_spill_to_local_disk`](System_variables/memory_spill.md#disable_spill_to_local_disk) | Memory and Spill |
| [`div_precision_increment`](System_variables/session_protocol_governance.md#div_precision_increment) | Session, Protocol, and Governance |
| [`dynamic_overwrite`](System_variables/loading_insert_txn.md#dynamic_overwrite) | Loading, Insert, and Transactions |
| [`enable_adaptive_sink_dop`](System_variables/execution_parallelism.md#enable_adaptive_sink_dop) | Execution Engine and Parallelism |
| [`enable_bucket_aware_execution_on_lake`](System_variables/execution_parallelism.md#enable_bucket_aware_execution_on_lake) | Execution Engine and Parallelism |
| [`enable_cache_udaf`](System_variables/execution_parallelism.md#enable_cache_udaf) | Execution Engine and Parallelism |
| [`enable_cbo_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_cbo_based_mv_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_cbo_table_prune`](System_variables/optimizer_planning.md#enable_cbo_table_prune) | Query Optimizer and Planning |
| [`enable_color_explain_output`](System_variables/session_protocol_governance.md#enable_color_explain_output) | Session, Protocol, and Governance |
| [`enable_connector_adaptive_io_tasks`](System_variables/external_catalog_lake.md#enable_connector_adaptive_io_tasks) | External Catalogs and Data Lake |
| [`enable_cost_based_multi_stage_agg`](System_variables/joins_agg_mv.md#enable_cost_based_multi_stage_agg) | Joins, Aggregation, and Materialized Views |
| [`enable_datacache_async_populate_mode`](System_variables/scan_io_datacache.md#enable_datacache_async_populate_mode) | Scan, I/O, and Data Cache |
| [`enable_datacache_io_adaptor`](System_variables/scan_io_datacache.md#enable_datacache_io_adaptor) | Scan, I/O, and Data Cache |
| [`enable_datacache_sharing`](System_variables/scan_io_datacache.md#enable_datacache_sharing) | Scan, I/O, and Data Cache |
| [`enable_distinct_agg_over_window`](System_variables/joins_agg_mv.md#enable_distinct_agg_over_window) | Joins, Aggregation, and Materialized Views |
| [`enable_distinct_column_bucketization`](System_variables/joins_agg_mv.md#enable_distinct_column_bucketization) | Joins, Aggregation, and Materialized Views |
| [`enable_eliminate_agg`](System_variables/joins_agg_mv.md#enable_eliminate_agg) | Joins, Aggregation, and Materialized Views |
| [`enable_explain_in_profile`](System_variables/session_protocol_governance.md#enable_explain_in_profile) | Session, Protocol, and Governance |
| [`enable_filter_unused_columns_in_scan_stage`](System_variables/optimizer_planning.md#enable_filter_unused_columns_in_scan_stage) | Query Optimizer and Planning |
| [`enable_force_rule_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_force_rule_based_mv_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_gin_filter`](System_variables/scan_io_datacache.md#enable_gin_filter) | Scan, I/O, and Data Cache |
| [`enable_global_runtime_filter`](System_variables/execution_parallelism.md#enable_global_runtime_filter) | Execution Engine and Parallelism |
| [`enable_group_by_compressed_key`](System_variables/joins_agg_mv.md#enable_group_by_compressed_key) | Joins, Aggregation, and Materialized Views |
| [`enable_group_execution`](System_variables/execution_parallelism.md#enable_group_execution) | Execution Engine and Parallelism |
| [`enable_group_level_query_queue`](System_variables/session_protocol_governance.md#enable_group_level_query_queue-global) | Session, Protocol, and Governance |
| [`enable_incremental_mv`](System_variables/joins_agg_mv.md#enable_incremental_mv) | Joins, Aggregation, and Materialized Views |
| [`enable_insert_partial_update`](System_variables/loading_insert_txn.md#enable_insert_partial_update) | Loading, Insert, and Transactions |
| [`enable_insert_strict`](System_variables/loading_insert_txn.md#enable_insert_strict) | Loading, Insert, and Transactions |
| [`enable_lake_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_lake_tablet_internal_parallel) | Execution Engine and Parallelism |
| [`enable_lambda_pushdown`](System_variables/optimizer_planning.md#enable_lambda_pushdown) | Query Optimizer and Planning |
| [`enable_large_in_predicate`](System_variables/optimizer_planning.md#enable_large_in_predicate) | Query Optimizer and Planning |
| [`enable_load_profile`](System_variables/loading_insert_txn.md#enable_load_profile) | Loading, Insert, and Transactions |
| [`enable_local_shuffle_agg`](System_variables/execution_parallelism.md#enable_local_shuffle_agg) | Execution Engine and Parallelism |
| [`enable_materialized_view_agg_pushdown_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_agg_pushdown_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_materialized_view_for_insert`](System_variables/joins_agg_mv.md#enable_materialized_view_for_insert) | Joins, Aggregation, and Materialized Views |
| [`enable_materialized_view_text_match_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_text_match_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_materialized_view_union_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_union_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_metadata_profile`](System_variables/session_protocol_governance.md#enable_metadata_profile) | Session, Protocol, and Governance |
| [`enable_multicolumn_global_runtime_filter`](System_variables/execution_parallelism.md#enable_multicolumn_global_runtime_filter) | Execution Engine and Parallelism |
| [`enable_mv_planner`](System_variables/joins_agg_mv.md#enable_mv_planner) | Joins, Aggregation, and Materialized Views |
| [`enable_optimize_skew_join_v1`](System_variables/joins_agg_mv.md#enable_optimize_skew_join_v1) | Joins, Aggregation, and Materialized Views |
| [`enable_optimize_skew_join_v2`](System_variables/joins_agg_mv.md#enable_optimize_skew_join_v2) | Joins, Aggregation, and Materialized Views |
| [`enable_parallel_merge`](System_variables/execution_parallelism.md#enable_parallel_merge) | Execution Engine and Parallelism |
| [`enable_parquet_reader_bloom_filter`](System_variables/scan_io_datacache.md#enable_parquet_reader_bloom_filter) | Scan, I/O, and Data Cache |
| [`enable_parquet_reader_page_index`](System_variables/scan_io_datacache.md#enable_parquet_reader_page_index) | Scan, I/O, and Data Cache |
| [`enable_partition_hash_join`](System_variables/joins_agg_mv.md#enable_partition_hash_join) | Joins, Aggregation, and Materialized Views |
| [`enable_per_bucket_optimize`](System_variables/execution_parallelism.md#enable_per_bucket_optimize) | Execution Engine and Parallelism |
| [`enable_phased_scheduler`](System_variables/execution_parallelism.md#enable_phased_scheduler) | Execution Engine and Parallelism |
| [`enable_pipeline_engine`](System_variables/execution_parallelism.md#enable_pipeline_engine) | Execution Engine and Parallelism |
| [`enable_plan_advisor`](System_variables/optimizer_planning.md#enable_plan_advisor) | Query Optimizer and Planning |
| [`enable_predicate_reorder`](System_variables/optimizer_planning.md#enable_predicate_reorder) | Query Optimizer and Planning |
| [`enable_profile`](System_variables/session_protocol_governance.md#enable_profile) | Session, Protocol, and Governance |
| [`enable_query_cache`](System_variables/execution_parallelism.md#enable_query_cache) | Execution Engine and Parallelism |
| [`enable_query_dump`](System_variables/session_protocol_governance.md#enable_query_dump) | Session, Protocol, and Governance |
| [`enable_query_queue_load`](System_variables/session_protocol_governance.md#enable_query_queue_load-global) | Session, Protocol, and Governance |
| [`enable_query_queue_select`](System_variables/session_protocol_governance.md#enable_query_queue_select-global) | Session, Protocol, and Governance |
| [`enable_query_queue_statistic`](System_variables/session_protocol_governance.md#enable_query_queue_statistic-global) | Session, Protocol, and Governance |
| [`enable_query_tablet_affinity`](System_variables/execution_parallelism.md#enable_query_tablet_affinity) | Execution Engine and Parallelism |
| [`enable_query_trigger_analyze`](System_variables/optimizer_planning.md#enable_query_trigger_analyze) | Query Optimizer and Planning |
| [`enable_rbo_table_prune`](System_variables/optimizer_planning.md#enable_rbo_table_prune) | Query Optimizer and Planning |
| [`enable_reduce_cast_varchar_expr_sync_type`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_expr_sync_type-global) | Query Optimizer and Planning |
| [`enable_reduce_cast_varchar_length_inheritance`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_length_inheritance-global) | Query Optimizer and Planning |
| [`enable_rewrite_groupingsets_to_union_all`](System_variables/joins_agg_mv.md#enable_rewrite_groupingsets_to_union_all) | Joins, Aggregation, and Materialized Views |
| [`enable_runtime_adaptive_dop`](System_variables/execution_parallelism.md#enable_runtime_adaptive_dop) | Execution Engine and Parallelism |
| [`enable_scan_datacache`](System_variables/scan_io_datacache.md#enable_scan_datacache) | Scan, I/O, and Data Cache |
| [`enable_shared_scan`](System_variables/scan_io_datacache.md#enable_shared_scan) | Scan, I/O, and Data Cache |
| [`enable_short_circuit`](System_variables/execution_parallelism.md#enable_short_circuit) | Execution Engine and Parallelism |
| [`enable_sort_aggregate`](System_variables/joins_agg_mv.md#enable_sort_aggregate) | Joins, Aggregation, and Materialized Views |
| [`enable_spill`](System_variables/memory_spill.md#enable_spill) | Memory and Spill |
| [`enable_spill_to_remote_storage`](System_variables/memory_spill.md#enable_spill_to_remote_storage) | Memory and Spill |
| [`enable_split_topn_agg`](System_variables/joins_agg_mv.md#enable_split_topn_agg) | Joins, Aggregation, and Materialized Views |
| [`enable_split_topn_agg_limit`](System_variables/joins_agg_mv.md#enable_split_topn_agg_limit) | Joins, Aggregation, and Materialized Views |
| [`enable_spm_rewrite`](System_variables/optimizer_planning.md#enable_spm_rewrite) | Query Optimizer and Planning |
| [`enable_strict_order_by`](System_variables/session_protocol_governance.md#enable_strict_order_by) | Session, Protocol, and Governance |
| [`enable_sync_materialized_view_rewrite`](System_variables/joins_agg_mv.md#enable_sync_materialized_view_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_table_prune_on_update`](System_variables/optimizer_planning.md#enable_table_prune_on_update) | Query Optimizer and Planning |
| [`enable_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_tablet_internal_parallel) | Execution Engine and Parallelism |
| [`enable_tablet_pre_split`](System_variables/execution_parallelism.md#enable_tablet_pre_split) | Execution Engine and Parallelism |
| [`enable_topn_filter_back_pressure`](System_variables/execution_parallelism.md#enable_topn_filter_back_pressure) | Execution Engine and Parallelism |
| [`enable_topn_runtime_filter`](System_variables/execution_parallelism.md#enable_topn_runtime_filter) | Execution Engine and Parallelism |
| [`enable_ukfk_join_reorder`](System_variables/joins_agg_mv.md#enable_ukfk_join_reorder) | Joins, Aggregation, and Materialized Views |
| [`enable_ukfk_opt`](System_variables/optimizer_planning.md#enable_ukfk_opt) | Query Optimizer and Planning |
| [`enable_view_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_view_based_mv_rewrite) | Joins, Aggregation, and Materialized Views |
| [`enable_wait_dependent_event`](System_variables/execution_parallelism.md#enable_wait_dependent_event) | Execution Engine and Parallelism |
| [`enable_write_hive_external_table`](System_variables/external_catalog_lake.md#enable_write_hive_external_table) | External Catalogs and Data Lake |
| [`event_scheduler`](System_variables/session_protocol_governance.md#event_scheduler) | Session, Protocol, and Governance |
| [`force_schedule_local`](System_variables/execution_parallelism.md#force_schedule_local-session) | Execution Engine and Parallelism |
| [`forward_to_leader`](System_variables/session_protocol_governance.md#forward_to_leader) | Session, Protocol, and Governance |
| [`group_concat_max_len`](System_variables/session_protocol_governance.md#group_concat_max_len) | Session, Protocol, and Governance |
| [`group_execution_max_groups`](System_variables/execution_parallelism.md#group_execution_max_groups) | Execution Engine and Parallelism |
| [`group_execution_min_scan_rows`](System_variables/execution_parallelism.md#group_execution_min_scan_rows) | Execution Engine and Parallelism |
| [`hash_join_push_down_right_table`](System_variables/joins_agg_mv.md#hash_join_push_down_right_table) | Joins, Aggregation, and Materialized Views |
| [`historical_nodes_min_update_interval`](System_variables/session_protocol_governance.md#historical_nodes_min_update_interval) | Session, Protocol, and Governance |
| [`init_connect`](System_variables/session_protocol_governance.md#init_connect-global) | Session, Protocol, and Governance |
| [`innodb_read_only`](System_variables/session_protocol_governance.md#innodb_read_only) | Session, Protocol, and Governance |
| [`insert_max_filter_ratio`](System_variables/loading_insert_txn.md#insert_max_filter_ratio) | Loading, Insert, and Transactions |
| [`insert_timeout`](System_variables/loading_insert_txn.md#insert_timeout) | Loading, Insert, and Transactions |
| [`interactive_timeout`](System_variables/session_protocol_governance.md#interactive_timeout) | Session, Protocol, and Governance |
| [`io_tasks_per_scan_operator`](System_variables/scan_io_datacache.md#io_tasks_per_scan_operator) | Scan, I/O, and Data Cache |
| [`jit_level`](System_variables/execution_parallelism.md#jit_level) | Execution Engine and Parallelism |
| [`join_late_materialization`](System_variables/joins_agg_mv.md#join_late_materialization) | Joins, Aggregation, and Materialized Views |
| [`lake_bucket_assign_mode`](System_variables/external_catalog_lake.md#lake_bucket_assign_mode) | External Catalogs and Data Lake |
| [`language`](System_variables/session_protocol_governance.md#language-global) | Session, Protocol, and Governance |
| [`large_in_predicate_threshold`](System_variables/optimizer_planning.md#large_in_predicate_threshold) | Query Optimizer and Planning |
| [`license`](System_variables/session_protocol_governance.md#license-global) | Session, Protocol, and Governance |
| [`load_mem_limit`](System_variables/loading_insert_txn.md#load_mem_limit) | Loading, Insert, and Transactions |
| [`log_rejected_record_num`](System_variables/loading_insert_txn.md#log_rejected_record_num-v31-and-later) | Loading, Insert, and Transactions |
| [`low_cardinality_optimize_on_lake`](System_variables/optimizer_planning.md#low_cardinality_optimize_on_lake) | Query Optimizer and Planning |
| [`low_cardinality_optimize_v2`](System_variables/optimizer_planning.md#low_cardinality_optimize_v2) | Query Optimizer and Planning |
| [`lower_case_table_names`](System_variables/session_protocol_governance.md#lower_case_table_names-global) | Session, Protocol, and Governance |
| [`materialized_view_rewrite_mode`](System_variables/joins_agg_mv.md#materialized_view_rewrite_mode-v32-and-later) | Joins, Aggregation, and Materialized Views |
| [`materialized_view_subquery_text_match_max_count`](System_variables/joins_agg_mv.md#materialized_view_subquery_text_match_max_count) | Joins, Aggregation, and Materialized Views |
| [`max_allowed_packet`](System_variables/session_protocol_governance.md#max_allowed_packet) | Session, Protocol, and Governance |
| [`max_parallel_scan_instance_num`](System_variables/execution_parallelism.md#max_parallel_scan_instance_num) | Execution Engine and Parallelism |
| [`max_pipeline_dop`](System_variables/execution_parallelism.md#max_pipeline_dop) | Execution Engine and Parallelism |
| [`max_pushdown_conditions_per_column`](System_variables/scan_io_datacache.md#max_pushdown_conditions_per_column) | Scan, I/O, and Data Cache |
| [`max_scan_key_num`](System_variables/scan_io_datacache.md#max_scan_key_num) | Scan, I/O, and Data Cache |
| [`max_unknown_string_meta_length`](System_variables/session_protocol_governance.md#max_unknown_string_meta_length-global) | Session, Protocol, and Governance |
| [`metadata_collect_query_timeout`](System_variables/external_catalog_lake.md#metadata_collect_query_timeout) | External Catalogs and Data Lake |
| [`nested_mv_rewrite_max_level`](System_variables/joins_agg_mv.md#nested_mv_rewrite_max_level) | Joins, Aggregation, and Materialized Views |
| [`net_buffer_length`](System_variables/session_protocol_governance.md#net_buffer_length) | Session, Protocol, and Governance |
| [`net_read_timeout`](System_variables/session_protocol_governance.md#net_read_timeout) | Session, Protocol, and Governance |
| [`net_write_timeout`](System_variables/session_protocol_governance.md#net_write_timeout) | Session, Protocol, and Governance |
| [`new_planner_agg_stage`](System_variables/joins_agg_mv.md#new_planner_agg_stage) | Joins, Aggregation, and Materialized Views |
| [`new_planner_optimize_timeout`](System_variables/optimizer_planning.md#new_planner_optimize_timeout) | Query Optimizer and Planning |
| [`optimizer_materialized_view_timelimit`](System_variables/joins_agg_mv.md#optimizer_materialized_view_timelimit) | Joins, Aggregation, and Materialized Views |
| [`orc_use_column_names`](System_variables/external_catalog_lake.md#orc_use_column_names) | External Catalogs and Data Lake |
| [`parallel_exchange_instance_num`](System_variables/execution_parallelism.md#parallel_exchange_instance_num) | Execution Engine and Parallelism |
| [`parallel_fragment_exec_instance_num`](System_variables/execution_parallelism.md#parallel_fragment_exec_instance_num) | Execution Engine and Parallelism |
| [`parallel_merge_late_materialization_mode`](System_variables/execution_parallelism.md#parallel_merge_late_materialization_mode) | Execution Engine and Parallelism |
| [`partial_update_mode`](System_variables/loading_insert_txn.md#partial_update_mode) | Loading, Insert, and Transactions |
| [`performance_schema`](System_variables/session_protocol_governance.md#performance_schema-global) | Session, Protocol, and Governance |
| [`phased_scheduler_max_concurrency`](System_variables/execution_parallelism.md#phased_scheduler_max_concurrency) | Execution Engine and Parallelism |
| [`pipeline_dop`](System_variables/execution_parallelism.md#pipeline_dop) | Execution Engine and Parallelism |
| [`pipeline_profile_level`](System_variables/session_protocol_governance.md#pipeline_profile_level) | Session, Protocol, and Governance |
| [`pipeline_sink_dop`](System_variables/execution_parallelism.md#pipeline_sink_dop) | Execution Engine and Parallelism |
| [`plan_mode`](System_variables/optimizer_planning.md#plan_mode) | Query Optimizer and Planning |
| [`populate_datacache_mode`](System_variables/scan_io_datacache.md#populate_datacache_mode) | Scan, I/O, and Data Cache |
| [`prefer_cte_rewrite`](System_variables/optimizer_planning.md#prefer_cte_rewrite) | Query Optimizer and Planning |
| [`profile_log_latency_threshold_ms`](System_variables/session_protocol_governance.md#profile_log_latency_threshold_ms) | Session, Protocol, and Governance |
| [`query_cache_agg_cardinality_limit`](System_variables/execution_parallelism.md#query_cache_agg_cardinality_limit) | Execution Engine and Parallelism |
| [`query_cache_entry_max_bytes`](System_variables/execution_parallelism.md#query_cache_entry_max_bytes) | Execution Engine and Parallelism |
| [`query_cache_entry_max_rows`](System_variables/execution_parallelism.md#query_cache_entry_max_rows) | Execution Engine and Parallelism |
| [`query_cache_size`](System_variables/execution_parallelism.md#query_cache_size-global) | Execution Engine and Parallelism |
| [`query_cache_type`](System_variables/execution_parallelism.md#query_cache_type) | Execution Engine and Parallelism |
| [`query_delivery_timeout`](System_variables/session_protocol_governance.md#query_delivery_timeout) | Session, Protocol, and Governance |
| [`query_mem_limit`](System_variables/memory_spill.md#query_mem_limit) | Memory and Spill |
| [`query_queue_concurrency_limit`](System_variables/session_protocol_governance.md#query_queue_concurrency_limit-global) | Session, Protocol, and Governance |
| [`query_queue_cpu_used_permille_limit`](System_variables/session_protocol_governance.md#query_queue_cpu_used_permille_limit-global) | Session, Protocol, and Governance |
| [`query_queue_max_queued_queries`](System_variables/session_protocol_governance.md#query_queue_max_queued_queries-global) | Session, Protocol, and Governance |
| [`query_queue_mem_used_pct_limit`](System_variables/session_protocol_governance.md#query_queue_mem_used_pct_limit-global) | Session, Protocol, and Governance |
| [`query_queue_pending_timeout_second`](System_variables/session_protocol_governance.md#query_queue_pending_timeout_second-global) | Session, Protocol, and Governance |
| [`query_timeout`](System_variables/session_protocol_governance.md#query_timeout) | Session, Protocol, and Governance |
| [`range_pruner_max_predicate`](System_variables/optimizer_planning.md#range_pruner_max_predicate) | Query Optimizer and Planning |
| [`resource_group`](System_variables/session_protocol_governance.md#resource_group) | Session, Protocol, and Governance |
| [`runtime_filter_on_exchange_node`](System_variables/execution_parallelism.md#runtime_filter_on_exchange_node) | Execution Engine and Parallelism |
| [`runtime_join_filter_push_down_limit`](System_variables/execution_parallelism.md#runtime_join_filter_push_down_limit) | Execution Engine and Parallelism |
| [`runtime_profile_report_interval`](System_variables/session_protocol_governance.md#runtime_profile_report_interval) | Session, Protocol, and Governance |
| [`scan_lake_partition_num_limit`](System_variables/external_catalog_lake.md#scan_lake_partition_num_limit) | External Catalogs and Data Lake |
| [`scan_olap_partition_num_limit`](System_variables/scan_io_datacache.md#scan_olap_partition_num_limit) | Scan, I/O, and Data Cache |
| [`skip_local_disk_cache`](System_variables/scan_io_datacache.md#skip_local_disk_cache) | Scan, I/O, and Data Cache |
| [`skip_page_cache`](System_variables/scan_io_datacache.md#skip_page_cache) | Scan, I/O, and Data Cache |
| [`spill_encode_level`](System_variables/memory_spill.md#spill_encode_level) | Memory and Spill |
| [`spill_mode`](System_variables/memory_spill.md#spill_mode-30-and-later) | Memory and Spill |
| [`spill_partitionwise_agg`](System_variables/memory_spill.md#spill_partitionwise_agg) | Memory and Spill |
| [`spill_revocable_max_bytes`](System_variables/memory_spill.md#spill_revocable_max_bytes) | Memory and Spill |
| [`spill_storage_volume`](System_variables/memory_spill.md#spill_storage_volume) | Memory and Spill |
| [`sql_dialect`](System_variables/session_protocol_governance.md#sql_dialect) | Session, Protocol, and Governance |
| [`sql_mode`](System_variables/session_protocol_governance.md#sql_mode) | Session, Protocol, and Governance |
| [`sql_safe_updates`](System_variables/session_protocol_governance.md#sql_safe_updates) | Session, Protocol, and Governance |
| [`sql_select_limit`](System_variables/session_protocol_governance.md#sql_select_limit) | Session, Protocol, and Governance |
| [`storage_engine`](System_variables/session_protocol_governance.md#storage_engine) | Session, Protocol, and Governance |
| [`streaming_preaggregation_mode`](System_variables/joins_agg_mv.md#streaming_preaggregation_mode) | Joins, Aggregation, and Materialized Views |
| [`system_time_zone`](System_variables/session_protocol_governance.md#system_time_zone) | Session, Protocol, and Governance |
| [`time_zone`](System_variables/session_protocol_governance.md#time_zone) | Session, Protocol, and Governance |
| [`transaction_isolation`](System_variables/loading_insert_txn.md#transaction_isolation) | Loading, Insert, and Transactions |
| [`transaction_read_only`](System_variables/loading_insert_txn.md#transaction_read_only) | Loading, Insert, and Transactions |
| [`transmission_compression_type`](System_variables/execution_parallelism.md#transmission_compression_type) | Execution Engine and Parallelism |
| [`tx_isolation`](System_variables/loading_insert_txn.md#tx_isolation) | Loading, Insert, and Transactions |
| [`tx_visible_wait_timeout`](System_variables/loading_insert_txn.md#tx_visible_wait_timeout) | Loading, Insert, and Transactions |
| [`use_page_cache`](System_variables/scan_io_datacache.md#use_page_cache) | Scan, I/O, and Data Cache |
| [`version`](System_variables/session_protocol_governance.md#version-global) | Session, Protocol, and Governance |
| [`version_comment`](System_variables/session_protocol_governance.md#version_comment-global) | Session, Protocol, and Governance |
| [`wait_timeout`](System_variables/session_protocol_governance.md#wait_timeout) | Session, Protocol, and Governance |

<VariableWarehouse />
