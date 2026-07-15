---
displayed_sidebar: docs
description: "StarRocks 系统变量可通过 SET 命令动态设置，支持全局和会话范围的配置。"
keywords: ['session','variable']
---

# 系统变量

import VariableWarehouse from '../_assets/commonMarkdown/variable_warehouse.mdx'

StarRocks 提供多个系统变量（system variables），方便您根据业务情况进行调整。本文介绍 StarRocks 支持的变量。您可以在 MySQL 客户端通过命令 [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) 查看当前变量。也可以通过 [SET](sql-statements/cluster-management/config_vars/SET.md) 命令动态设置或者修改变量。您可以设置变量在系统全局 (global) 范围内生效、仅在当前会话 (session) 中生效、或者仅在单个查询语句中生效。

StarRocks 中的变量参考 MySQL 中的变量设置，但**部分变量仅用于兼容 MySQL 客户端协议，并不产生其在 MySQL 数据库中的实际意义**。

> **说明**
>
> 任何用户都有权限通过 SHOW VARIABLES 查看变量。任何用户都有权限设置变量在 Session 级别生效。只有拥有 System 级 OPERATE 权限的用户才可以设置变量为全局生效。设置全局生效后，后续所有新的会话都会使用新配置，当前会话仍然使用老的配置。

## 查看变量

可以通过 `SHOW VARIABLES [LIKE 'xxx'];` 查看所有或指定的变量。例如：

```SQL

-- 查看系统中所有变量。
SHOW VARIABLES;

-- 查看符合匹配规则的变量。
SHOW VARIABLES LIKE '%time_zone%';
```

## 变量层级和类型

StarRocks 支持三种类型（层级）的变量：全局变量、Session 变量和 `SET_VAR` Hint。它们的层级关系如下：

* 全局变量在全局级别生效，可以被 Session 变量和 `SET_VAR` Hint 覆盖。
* Session 变量仅在当前会话中生效，可以被 `SET_VAR` Hint 覆盖。
* `SET_VAR` Hint 仅在当前查询语句中生效。

## 设置变量

### 设置变量全局生效或在会话中生效

变量一般可以设置为**全局**生效或**仅当前会话**生效。设置为全局生效后，**后续所有新的会话**连接中会使用新设置的值，当前会话还会继续使用之前设置的值；设置为仅当前会话生效时，变量仅对当前会话产生作用。

通过 `SET <var_name> = xxx;` 语句设置的变量仅在当前会话生效。如：

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

通过 `SET GLOBAL <var_name> = xxx;` 语句设置的变量全局生效。如：

 ```SQL
SET GLOBAL query_mem_limit = 137438953472;
 ```

以下变量仅支持全局生效，不支持设置为会话级别生效。您必须使用 `SET GLOBAL <var_name> = xxx;`，不能使用 `SET <var_name> = xxx;`，否则返回错误。

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


Session 级变量既可以设置全局生效也可以设置 session 级生效。

此外，变量设置也支持常量表达式，如：

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### 设置变量在单个查询语句中生效

在一些场景中，可能需要对某些查询专门设置变量。可以使用 SET_VAR 提示 (Hint) 在查询中设置仅在单个语句内生效的会话变量。

当前，StarRocks 支持在以下语句中使用 `SET_VAR` Hint：

- SELECT
- INSERT（自 v3.1.12 和 v3.2.0 起支持）
- UPDATE（自 v3.1.12 和 v3.2.0 起支持）
- DELETE（自 v3.1.12 和 v3.2.0 起支持）

`SET_VAR` 只能跟在以上关键字之后，必须以 `/*+` 开头，以 `*/` 结束。

举例：

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

StarRocks 同时支持在单个语句中设置多个变量，参考如下示例：

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

### 设置变量为用户属性

您可以通过 [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md) 将 Session 变量设置为用户属性该功能自 v3.3.3 起支持。

示例：

```SQL
-- 设置用户 jack 的 Session 变量 `query_timeout` 为 `600`。
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## 变量分类

系统变量分为以下几类：

- [查询优化器与规划](System_variables/optimizer_planning.md): 控制基于成本的优化器、统计信息驱动的规划、剪枝、重写以及 CTE 重用的会话变量。
- [连接、聚合与物化视图](System_variables/joins_agg_mv.md): 用于连接执行、聚合、Top-N 以及物化视图查询改写的会话变量。
- [执行引擎与并行度](System_variables/execution_parallelism.md): 用于 Pipeline 引擎、并行度、运行时过滤器以及查询缓存的会话变量。
- [内存与落盘](System_variables/memory_spill.md): 控制查询内存限制以及溢出到磁盘或远程存储的会话变量。
- [扫描、I/O 与 Data Cache](System_variables/scan_io_datacache.md): 用于扫描并发、I/O 任务、Data Cache 以及页缓存的会话变量。
- [外部 Catalog 与数据湖](System_variables/external_catalog_lake.md): 用于外部 Catalog 连接器以及 Hive、Iceberg、Hudi 等数据湖格式的会话变量。
- [导入、Insert 与事务](System_variables/loading_insert_txn.md): 用于数据导入、INSERT 行为以及事务处理的会话变量。
- [会话、协议与治理](System_variables/session_protocol_governance.md): 用于 MySQL 协议兼容、查询队列、超时、资源组以及性能分析的会话变量。

## 变量索引

下表列出了所有系统变量及其对应的分类页面。带有 `global` 标签的变量仅在全局级别生效，其他变量可在全局级别或单个会话中生效。

| 变量 | 分类 |
| --- | --- |
| [`activate_all_roles_on_login`](System_variables/session_protocol_governance.md#activate_all_roles_on_login-global) | 会话、协议与治理 |
| [`allow_lake_without_partition_filter`](System_variables/external_catalog_lake.md#allow_lake_without_partition_filter) | 外部 Catalog 与数据湖 |
| [`array_low_cardinality_optimize`](System_variables/optimizer_planning.md#array_low_cardinality_optimize) | 查询优化器与规划 |
| [`auto_increment_increment`](System_variables/session_protocol_governance.md#auto_increment_increment) | 会话、协议与治理 |
| [`avro_use_jni_reader`](System_variables/external_catalog_lake.md#avro_use_jni_reader) | 外部 Catalog 与数据湖 |
| [`big_query_profile_threshold`](System_variables/session_protocol_governance.md#big_query_profile_threshold) | 会话、协议与治理 |
| [`binary_encoding_format`](System_variables/session_protocol_governance.md#binary_encoding_format) | 会话、协议与治理 |
| [`binary_encoding_level`](System_variables/session_protocol_governance.md#binary_encoding_level) | 会话、协议与治理 |
| [`blacklist_backup_routing`](System_variables/execution_parallelism.md#blacklist_backup_routing) | 执行引擎与并行度 |
| [`catalog`](System_variables/session_protocol_governance.md#catalog-324) | 会话、协议与治理 |
| [`cbo_cte_force_reuse_limit_without_order_by`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_limit_without_order_by) | 查询优化器与规划 |
| [`cbo_cte_force_reuse_node_count`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_node_count) | 查询优化器与规划 |
| [`cbo_cte_max_limit`](System_variables/optimizer_planning.md#cbo_cte_max_limit) | 查询优化器与规划 |
| [`cbo_cte_reuse`](System_variables/optimizer_planning.md#cbo_cte_reuse) | 查询优化器与规划 |
| [`cbo_cte_reuse_rate`](System_variables/optimizer_planning.md#cbo_cte_reuse_rate) | 查询优化器与规划 |
| [`cbo_disabled_rules`](System_variables/optimizer_planning.md#cbo_disabled_rules) | 查询优化器与规划 |
| [`cbo_enable_low_cardinality_optimize`](System_variables/optimizer_planning.md#cbo_enable_low_cardinality_optimize) | 查询优化器与规划 |
| [`cbo_eq_base_type`](System_variables/optimizer_planning.md#cbo_eq_base_type) | 查询优化器与规划 |
| [`cbo_json_v2_dict_opt`](System_variables/optimizer_planning.md#cbo_json_v2_dict_opt) | 查询优化器与规划 |
| [`cbo_json_v2_rewrite`](System_variables/optimizer_planning.md#cbo_json_v2_rewrite) | 查询优化器与规划 |
| [`cbo_max_reorder_node_use_dp`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_dp) | 查询优化器与规划 |
| [`cbo_max_reorder_node_use_exhaustive`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_exhaustive) | 查询优化器与规划 |
| [`cbo_max_reorder_node_use_greedy`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_greedy) | 查询优化器与规划 |
| [`cbo_use_correlated_predicate_estimate`](System_variables/optimizer_planning.md#cbo_use_correlated_predicate_estimate) | 查询优化器与规划 |
| [`character_set_database`](System_variables/session_protocol_governance.md#character_set_database-global) | 会话、协议与治理 |
| [`collation_server`](System_variables/session_protocol_governance.md#collation_server) | 会话、协议与治理 |
| [`computation_fragment_scheduling_policy`](System_variables/execution_parallelism.md#computation_fragment_scheduling_policy) | 执行引擎与并行度 |
| [`connector_io_tasks_per_scan_operator`](System_variables/external_catalog_lake.md#connector_io_tasks_per_scan_operator) | 外部 Catalog 与数据湖 |
| [`connector_sink_compression_codec`](System_variables/external_catalog_lake.md#connector_sink_compression_codec) | 外部 Catalog 与数据湖 |
| [`connector_sink_target_max_file_size`](System_variables/external_catalog_lake.md#connector_sink_target_max_file_size) | 外部 Catalog 与数据湖 |
| [`count_distinct_column_buckets`](System_variables/joins_agg_mv.md#count_distinct_column_buckets) | 连接、聚合与物化视图 |
| [`custom_query_id`](System_variables/session_protocol_governance.md#custom_query_id-session) | 会话、协议与治理 |
| [`datacache_sharing_work_period`](System_variables/scan_io_datacache.md#datacache_sharing_work_period) | 扫描、I/O 与 Data Cache |
| [`default_authentication_plugin`](System_variables/session_protocol_governance.md#default_authentication_plugin) | 会话、协议与治理 |
| [`default_rowset_type`](System_variables/session_protocol_governance.md#default_rowset_type-global) | 会话、协议与治理 |
| [`default_table_compression`](System_variables/session_protocol_governance.md#default_table_compression) | 会话、协议与治理 |
| [`default_tmp_storage_engine`](System_variables/session_protocol_governance.md#default_tmp_storage_engine) | 会话、协议与治理 |
| [`default_view_sql_security`](System_variables/session_protocol_governance.md#default_view_sql_security) | 会话、协议与治理 |
| [`disable_colocate_join`](System_variables/joins_agg_mv.md#disable_colocate_join) | 连接、聚合与物化视图 |
| [`disable_join_reorder`](System_variables/joins_agg_mv.md#disable_join_reorder) | 连接、聚合与物化视图 |
| [`disable_spill_to_local_disk`](System_variables/memory_spill.md#disable_spill_to_local_disk) | 内存与落盘 |
| [`div_precision_increment`](System_variables/session_protocol_governance.md#div_precision_increment) | 会话、协议与治理 |
| [`dynamic_overwrite`](System_variables/loading_insert_txn.md#dynamic_overwrite) | 导入、Insert 与事务 |
| [`enable_adaptive_sink_dop`](System_variables/execution_parallelism.md#enable_adaptive_sink_dop) | 执行引擎与并行度 |
| [`enable_bucket_aware_execution_on_lake`](System_variables/execution_parallelism.md#enable_bucket_aware_execution_on_lake) | 执行引擎与并行度 |
| [`enable_cache_udaf`](System_variables/execution_parallelism.md#enable_cache_udaf) | 执行引擎与并行度 |
| [`enable_cbo_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_cbo_based_mv_rewrite) | 连接、聚合与物化视图 |
| [`enable_cbo_table_prune`](System_variables/optimizer_planning.md#enable_cbo_table_prune) | 查询优化器与规划 |
| [`enable_color_explain_output`](System_variables/session_protocol_governance.md#enable_color_explain_output) | 会话、协议与治理 |
| [`enable_connector_adaptive_io_tasks`](System_variables/external_catalog_lake.md#enable_connector_adaptive_io_tasks) | 外部 Catalog 与数据湖 |
| [`enable_datacache_async_populate_mode`](System_variables/scan_io_datacache.md#enable_datacache_async_populate_mode) | 扫描、I/O 与 Data Cache |
| [`enable_datacache_io_adaptor`](System_variables/scan_io_datacache.md#enable_datacache_io_adaptor) | 扫描、I/O 与 Data Cache |
| [`enable_datacache_sharing`](System_variables/scan_io_datacache.md#enable_datacache_sharing) | 扫描、I/O 与 Data Cache |
| [`enable_distinct_agg_over_window`](System_variables/joins_agg_mv.md#enable_distinct_agg_over_window) | 连接、聚合与物化视图 |
| [`enable_distinct_column_bucketization`](System_variables/joins_agg_mv.md#enable_distinct_column_bucketization) | 连接、聚合与物化视图 |
| [`enable_explain_in_profile`](System_variables/session_protocol_governance.md#enable_explain_in_profile) | 会话、协议与治理 |
| [`enable_force_rule_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_force_rule_based_mv_rewrite) | 连接、聚合与物化视图 |
| [`enable_gin_filter`](System_variables/scan_io_datacache.md#enable_gin_filter) | 扫描、I/O 与 Data Cache |
| [`enable_global_runtime_filter`](System_variables/execution_parallelism.md#enable_global_runtime_filter) | 执行引擎与并行度 |
| [`enable_group_by_compressed_key`](System_variables/joins_agg_mv.md#enable_group_by_compressed_key) | 连接、聚合与物化视图 |
| [`enable_group_execution`](System_variables/execution_parallelism.md#enable_group_execution) | 执行引擎与并行度 |
| [`enable_group_level_query_queue`](System_variables/session_protocol_governance.md#enable_group_level_query_queue-global) | 会话、协议与治理 |
| [`enable_incremental_mv`](System_variables/joins_agg_mv.md#enable_incremental_mv) | 连接、聚合与物化视图 |
| [`enable_insert_partial_update`](System_variables/loading_insert_txn.md#enable_insert_partial_update) | 导入、Insert 与事务 |
| [`enable_insert_strict`](System_variables/loading_insert_txn.md#enable_insert_strict) | 导入、Insert 与事务 |
| [`enable_lake_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_lake_tablet_internal_parallel) | 执行引擎与并行度 |
| [`enable_load_profile`](System_variables/loading_insert_txn.md#enable_load_profile) | 导入、Insert 与事务 |
| [`enable_local_shuffle_agg`](System_variables/execution_parallelism.md#enable_local_shuffle_agg) | 执行引擎与并行度 |
| [`enable_materialized_view_agg_pushdown_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_agg_pushdown_rewrite) | 连接、聚合与物化视图 |
| [`enable_materialized_view_for_insert`](System_variables/joins_agg_mv.md#enable_materialized_view_for_insert) | 连接、聚合与物化视图 |
| [`enable_materialized_view_text_match_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_text_match_rewrite) | 连接、聚合与物化视图 |
| [`enable_materialized_view_union_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_union_rewrite) | 连接、聚合与物化视图 |
| [`enable_metadata_profile`](System_variables/session_protocol_governance.md#enable_metadata_profile) | 会话、协议与治理 |
| [`enable_multicolumn_global_runtime_filter`](System_variables/execution_parallelism.md#enable_multicolumn_global_runtime_filter) | 执行引擎与并行度 |
| [`enable_mv_planner`](System_variables/joins_agg_mv.md#enable_mv_planner) | 连接、聚合与物化视图 |
| [`enable_parallel_merge`](System_variables/execution_parallelism.md#enable_parallel_merge) | 执行引擎与并行度 |
| [`enable_parquet_reader_bloom_filter`](System_variables/scan_io_datacache.md#enable_parquet_reader_bloom_filter) | 扫描、I/O 与 Data Cache |
| [`enable_parquet_reader_page_index`](System_variables/scan_io_datacache.md#enable_parquet_reader_page_index) | 扫描、I/O 与 Data Cache |
| [`enable_partition_hash_join`](System_variables/joins_agg_mv.md#enable_partition_hash_join) | 连接、聚合与物化视图 |
| [`enable_per_bucket_optimize`](System_variables/execution_parallelism.md#enable_per_bucket_optimize) | 执行引擎与并行度 |
| [`enable_phased_scheduler`](System_variables/execution_parallelism.md#enable_phased_scheduler) | 执行引擎与并行度 |
| [`enable_pipeline_engine`](System_variables/execution_parallelism.md#enable_pipeline_engine) | 执行引擎与并行度 |
| [`enable_plan_advisor`](System_variables/optimizer_planning.md#enable_plan_advisor) | 查询优化器与规划 |
| [`enable_predicate_reorder`](System_variables/optimizer_planning.md#enable_predicate_reorder) | 查询优化器与规划 |
| [`enable_profile`](System_variables/session_protocol_governance.md#enable_profile) | 会话、协议与治理 |
| [`enable_query_cache`](System_variables/execution_parallelism.md#enable_query_cache) | 执行引擎与并行度 |
| [`enable_query_queue_load`](System_variables/session_protocol_governance.md#enable_query_queue_load-global) | 会话、协议与治理 |
| [`enable_query_queue_select`](System_variables/session_protocol_governance.md#enable_query_queue_select-global) | 会话、协议与治理 |
| [`enable_query_queue_statistic`](System_variables/session_protocol_governance.md#enable_query_queue_statistic-global) | 会话、协议与治理 |
| [`enable_query_tablet_affinity`](System_variables/execution_parallelism.md#enable_query_tablet_affinity) | 执行引擎与并行度 |
| [`enable_query_trigger_analyze`](System_variables/optimizer_planning.md#enable_query_trigger_analyze) | 查询优化器与规划 |
| [`enable_rbo_table_prune`](System_variables/optimizer_planning.md#enable_rbo_table_prune) | 查询优化器与规划 |
| [`enable_reduce_cast_varchar_expr_sync_type`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_expr_sync_type-global) | 查询优化器与规划 |
| [`enable_reduce_cast_varchar_length_inheritance`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_length_inheritance-global) | 查询优化器与规划 |
| [`enable_runtime_adaptive_dop`](System_variables/execution_parallelism.md#enable_runtime_adaptive_dop) | 执行引擎与并行度 |
| [`enable_scan_datacache`](System_variables/scan_io_datacache.md#enable_scan_datacache) | 扫描、I/O 与 Data Cache |
| [`enable_short_circuit`](System_variables/execution_parallelism.md#enable_short_circuit) | 执行引擎与并行度 |
| [`enable_sort_aggregate`](System_variables/joins_agg_mv.md#enable_sort_aggregate) | 连接、聚合与物化视图 |
| [`enable_spill`](System_variables/memory_spill.md#enable_spill) | 内存与落盘 |
| [`enable_spill_to_remote_storage`](System_variables/memory_spill.md#enable_spill_to_remote_storage) | 内存与落盘 |
| [`enable_spm_rewrite`](System_variables/optimizer_planning.md#enable_spm_rewrite) | 查询优化器与规划 |
| [`enable_strict_order_by`](System_variables/session_protocol_governance.md#enable_strict_order_by) | 会话、协议与治理 |
| [`enable_sync_materialized_view_rewrite`](System_variables/joins_agg_mv.md#enable_sync_materialized_view_rewrite) | 连接、聚合与物化视图 |
| [`enable_table_prune_on_update`](System_variables/optimizer_planning.md#enable_table_prune_on_update) | 查询优化器与规划 |
| [`enable_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_tablet_internal_parallel) | 执行引擎与并行度 |
| [`enable_tablet_pre_split`](System_variables/execution_parallelism.md#enable_tablet_pre_split) | 执行引擎与并行度 |
| [`enable_topn_filter_back_pressure`](System_variables/execution_parallelism.md#enable_topn_filter_back_pressure) | 执行引擎与并行度 |
| [`enable_topn_runtime_filter`](System_variables/execution_parallelism.md#enable_topn_runtime_filter) | 执行引擎与并行度 |
| [`enable_ukfk_opt`](System_variables/optimizer_planning.md#enable_ukfk_opt) | 查询优化器与规划 |
| [`enable_view_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_view_based_mv_rewrite) | 连接、聚合与物化视图 |
| [`enable_wait_dependent_event`](System_variables/execution_parallelism.md#enable_wait_dependent_event) | 执行引擎与并行度 |
| [`enable_write_hive_external_table`](System_variables/external_catalog_lake.md#enable_write_hive_external_table) | 外部 Catalog 与数据湖 |
| [`event_scheduler`](System_variables/session_protocol_governance.md#event_scheduler) | 会话、协议与治理 |
| [`forward_to_leader`](System_variables/session_protocol_governance.md#forward_to_leader) | 会话、协议与治理 |
| [`group_concat_max_len`](System_variables/session_protocol_governance.md#group_concat_max_len) | 会话、协议与治理 |
| [`group_execution_max_groups`](System_variables/execution_parallelism.md#group_execution_max_groups) | 执行引擎与并行度 |
| [`group_execution_min_scan_rows`](System_variables/execution_parallelism.md#group_execution_min_scan_rows) | 执行引擎与并行度 |
| [`hash_join_push_down_right_table`](System_variables/joins_agg_mv.md#hash_join_push_down_right_table) | 连接、聚合与物化视图 |
| [`historical_nodes_min_update_interval`](System_variables/session_protocol_governance.md#historical_nodes_min_update_interval) | 会话、协议与治理 |
| [`init_connect`](System_variables/session_protocol_governance.md#init_connect-global) | 会话、协议与治理 |
| [`innodb_read_only`](System_variables/session_protocol_governance.md#innodb_read_only) | 会话、协议与治理 |
| [`insert_max_filter_ratio`](System_variables/loading_insert_txn.md#insert_max_filter_ratio) | 导入、Insert 与事务 |
| [`insert_timeout`](System_variables/loading_insert_txn.md#insert_timeout) | 导入、Insert 与事务 |
| [`interactive_timeout`](System_variables/session_protocol_governance.md#interactive_timeout) | 会话、协议与治理 |
| [`io_tasks_per_scan_operator`](System_variables/scan_io_datacache.md#io_tasks_per_scan_operator) | 扫描、I/O 与 Data Cache |
| [`jit_level`](System_variables/execution_parallelism.md#jit_level) | 执行引擎与并行度 |
| [`lake_bucket_assign_mode`](System_variables/external_catalog_lake.md#lake_bucket_assign_mode) | 外部 Catalog 与数据湖 |
| [`language`](System_variables/session_protocol_governance.md#language-global) | 会话、协议与治理 |
| [`license`](System_variables/session_protocol_governance.md#license-global) | 会话、协议与治理 |
| [`load_mem_limit`](System_variables/loading_insert_txn.md#load_mem_limit) | 导入、Insert 与事务 |
| [`log_rejected_record_num`](System_variables/loading_insert_txn.md#log_rejected_record_num-31) | 导入、Insert 与事务 |
| [`low_cardinality_optimize_on_lake`](System_variables/optimizer_planning.md#low_cardinality_optimize_on_lake) | 查询优化器与规划 |
| [`low_cardinality_optimize_v2`](System_variables/optimizer_planning.md#low_cardinality_optimize_v2) | 查询优化器与规划 |
| [`lower_case_table_names`](System_variables/session_protocol_governance.md#lower_case_table_names-global) | 会话、协议与治理 |
| [`materialized_view_rewrite_mode`](System_variables/joins_agg_mv.md#materialized_view_rewrite_mode-32) | 连接、聚合与物化视图 |
| [`materialized_view_subquery_text_match_max_count`](System_variables/joins_agg_mv.md#materialized_view_subquery_text_match_max_count) | 连接、聚合与物化视图 |
| [`max_allowed_packet`](System_variables/session_protocol_governance.md#max_allowed_packet) | 会话、协议与治理 |
| [`max_pipeline_dop`](System_variables/execution_parallelism.md#max_pipeline_dop) | 执行引擎与并行度 |
| [`max_pushdown_conditions_per_column`](System_variables/scan_io_datacache.md#max_pushdown_conditions_per_column) | 扫描、I/O 与 Data Cache |
| [`max_scan_key_num`](System_variables/scan_io_datacache.md#max_scan_key_num) | 扫描、I/O 与 Data Cache |
| [`max_unknown_string_meta_length`](System_variables/session_protocol_governance.md#max_unknown_string_meta_length-global) | 会话、协议与治理 |
| [`metadata_collect_query_timeout`](System_variables/external_catalog_lake.md#metadata_collect_query_timeout) | 外部 Catalog 与数据湖 |
| [`nested_mv_rewrite_max_level`](System_variables/joins_agg_mv.md#nested_mv_rewrite_max_level) | 连接、聚合与物化视图 |
| [`net_buffer_length`](System_variables/session_protocol_governance.md#net_buffer_length) | 会话、协议与治理 |
| [`net_read_timeout`](System_variables/session_protocol_governance.md#net_read_timeout) | 会话、协议与治理 |
| [`net_write_timeout`](System_variables/session_protocol_governance.md#net_write_timeout) | 会话、协议与治理 |
| [`new_planner_optimize_timeout`](System_variables/optimizer_planning.md#new_planner_optimize_timeout) | 查询优化器与规划 |
| [`optimizer_materialized_view_timelimit`](System_variables/joins_agg_mv.md#optimizer_materialized_view_timelimit) | 连接、聚合与物化视图 |
| [`orc_use_column_names`](System_variables/external_catalog_lake.md#orc_use_column_names) | 外部 Catalog 与数据湖 |
| [`parallel_exchange_instance_num`](System_variables/execution_parallelism.md#parallel_exchange_instance_num) | 执行引擎与并行度 |
| [`parallel_fragment_exec_instance_num`](System_variables/execution_parallelism.md#parallel_fragment_exec_instance_num) | 执行引擎与并行度 |
| [`parallel_merge_late_materialization_mode`](System_variables/execution_parallelism.md#parallel_merge_late_materialization_mode) | 执行引擎与并行度 |
| [`partial_update_mode`](System_variables/loading_insert_txn.md#partial_update_mode) | 导入、Insert 与事务 |
| [`performance_schema`](System_variables/session_protocol_governance.md#performance_schema-global) | 会话、协议与治理 |
| [`phased_scheduler_max_concurrency`](System_variables/execution_parallelism.md#phased_scheduler_max_concurrency) | 执行引擎与并行度 |
| [`pipeline_dop`](System_variables/execution_parallelism.md#pipeline_dop) | 执行引擎与并行度 |
| [`pipeline_profile_level`](System_variables/session_protocol_governance.md#pipeline_profile_level) | 会话、协议与治理 |
| [`pipeline_sink_dop`](System_variables/execution_parallelism.md#pipeline_sink_dop) | 执行引擎与并行度 |
| [`plan_mode`](System_variables/optimizer_planning.md#plan_mode) | 查询优化器与规划 |
| [`populate_datacache_mode`](System_variables/scan_io_datacache.md#populate_datacache_mode) | 扫描、I/O 与 Data Cache |
| [`prefer_cte_rewrite`](System_variables/optimizer_planning.md#prefer_cte_rewrite) | 查询优化器与规划 |
| [`profile_log_latency_threshold_ms`](System_variables/session_protocol_governance.md#profile_log_latency_threshold_ms) | 会话、协议与治理 |
| [`query_cache_agg_cardinality_limit`](System_variables/execution_parallelism.md#query_cache_agg_cardinality_limit) | 执行引擎与并行度 |
| [`query_cache_entry_max_bytes`](System_variables/execution_parallelism.md#query_cache_entry_max_bytes) | 执行引擎与并行度 |
| [`query_cache_entry_max_rows`](System_variables/execution_parallelism.md#query_cache_entry_max_rows) | 执行引擎与并行度 |
| [`query_cache_size`](System_variables/execution_parallelism.md#query_cache_size-global) | 执行引擎与并行度 |
| [`query_cache_type`](System_variables/execution_parallelism.md#query_cache_type) | 执行引擎与并行度 |
| [`query_mem_limit`](System_variables/memory_spill.md#query_mem_limit) | 内存与落盘 |
| [`query_queue_concurrency_limit`](System_variables/session_protocol_governance.md#query_queue_concurrency_limit-global) | 会话、协议与治理 |
| [`query_queue_cpu_used_permille_limit`](System_variables/session_protocol_governance.md#query_queue_cpu_used_permille_limit-global) | 会话、协议与治理 |
| [`query_queue_max_queued_queries`](System_variables/session_protocol_governance.md#query_queue_max_queued_queries-global) | 会话、协议与治理 |
| [`query_queue_mem_used_pct_limit`](System_variables/session_protocol_governance.md#query_queue_mem_used_pct_limit-global) | 会话、协议与治理 |
| [`query_queue_pending_timeout_second`](System_variables/session_protocol_governance.md#query_queue_pending_timeout_second-global) | 会话、协议与治理 |
| [`query_timeout`](System_variables/session_protocol_governance.md#query_timeout) | 会话、协议与治理 |
| [`range_pruner_max_predicate`](System_variables/optimizer_planning.md#range_pruner_max_predicate) | 查询优化器与规划 |
| [`resource_group`](System_variables/session_protocol_governance.md#resource_group) | 会话、协议与治理 |
| [`runtime_filter_on_exchange_node`](System_variables/execution_parallelism.md#runtime_filter_on_exchange_node) | 执行引擎与并行度 |
| [`runtime_join_filter_push_down_limit`](System_variables/execution_parallelism.md#runtime_join_filter_push_down_limit) | 执行引擎与并行度 |
| [`runtime_profile_report_interval`](System_variables/session_protocol_governance.md#runtime_profile_report_interval) | 会话、协议与治理 |
| [`scan_lake_partition_num_limit`](System_variables/external_catalog_lake.md#scan_lake_partition_num_limit) | 外部 Catalog 与数据湖 |
| [`scan_olap_partition_num_limit`](System_variables/scan_io_datacache.md#scan_olap_partition_num_limit) | 扫描、I/O 与 Data Cache |
| [`skip_local_disk_cache`](System_variables/scan_io_datacache.md#skip_local_disk_cache) | 扫描、I/O 与 Data Cache |
| [`spill_encode_level`](System_variables/memory_spill.md#spill_encode_level) | 内存与落盘 |
| [`spill_mode`](System_variables/memory_spill.md#spill_mode-30) | 内存与落盘 |
| [`spill_storage_volume`](System_variables/memory_spill.md#spill_storage_volume) | 内存与落盘 |
| [`sql_dialect`](System_variables/session_protocol_governance.md#sql_dialect) | 会话、协议与治理 |
| [`sql_mode`](System_variables/session_protocol_governance.md#sql_mode) | 会话、协议与治理 |
| [`sql_safe_updates`](System_variables/session_protocol_governance.md#sql_safe_updates) | 会话、协议与治理 |
| [`sql_select_limit`](System_variables/session_protocol_governance.md#sql_select_limit) | 会话、协议与治理 |
| [`storage_engine`](System_variables/session_protocol_governance.md#storage_engine) | 会话、协议与治理 |
| [`streaming_preaggregation_mode`](System_variables/joins_agg_mv.md#streaming_preaggregation_mode) | 连接、聚合与物化视图 |
| [`system_time_zone`](System_variables/session_protocol_governance.md#system_time_zone-global) | 会话、协议与治理 |
| [`time_zone`](System_variables/session_protocol_governance.md#time_zone) | 会话、协议与治理 |
| [`transaction_read_only`](System_variables/loading_insert_txn.md#transaction_read_only) | 导入、Insert 与事务 |
| [`tx_isolation`](System_variables/loading_insert_txn.md#tx_isolation) | 导入、Insert 与事务 |
| [`tx_visible_wait_timeout`](System_variables/loading_insert_txn.md#tx_visible_wait_timeout) | 导入、Insert 与事务 |
| [`version`](System_variables/session_protocol_governance.md#version-global) | 会话、协议与治理 |
| [`version_comment`](System_variables/session_protocol_governance.md#version_comment-global) | 会话、协议与治理 |
| [`wait_timeout`](System_variables/session_protocol_governance.md#wait_timeout) | 会话、协议与治理 |

<VariableWarehouse />
