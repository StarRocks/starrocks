---
displayed_sidebar: docs
description: "StarRocks が提供する多くのシステム変数の設定と変更について説明。"
---

# システム変数

import VariableWarehouse from '../_assets/commonMarkdown/variable_warehouse.mdx'

StarRocks は、多くのシステム変数を提供しており、要件に応じて設定や変更が可能です。このセクションでは、StarRocks がサポートする変数について説明します。これらの変数の設定を確認するには、MySQL クライアントで [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) コマンドを実行します。また、[SET](sql-statements/cluster-management/config_vars/SET.md) コマンドを使用して、変数を動的に設定または変更することもできます。これらの変数は、システム全体でグローバルに、現在のセッションのみで、または単一のクエリ文でのみ有効にすることができます。

StarRocks の変数は、MySQL の変数セットを参照していますが、**一部の変数は MySQL クライアントプロトコルとの互換性のみを持ち、MySQL データベースでは機能しません**。

> **注意**
>
> どのユーザーでも SHOW VARIABLES を実行し、セッションレベルで変数を有効にする権限があります。ただし、SYSTEM レベルの OPERATE 権限を持つユーザーのみが、変数をグローバルに有効にすることができます。グローバルに有効な変数は、すべての将来のセッション（現在のセッションを除く）で有効になります。
>
> 現在のセッションの設定変更を行い、さらにその設定変更をすべての将来のセッションに適用したい場合は、`GLOBAL` 修飾子を使用せずに一度、使用してもう一度変更を行うことができます。例：
>
> ```SQL
> SET query_mem_limit = 137438953472; -- 現在のセッションに適用。
> SET GLOBAL query_mem_limit = 137438953472; -- すべての将来のセッションに適用。
> ```

## 変数の階層と種類

StarRocks は、グローバル変数、セッション変数、`SET_VAR` ヒントの3種類（レベル）の変数をサポートしています。それらの階層関係は次のとおりです：

* グローバル変数はグローバルレベルで有効であり、セッション変数や `SET_VAR` ヒントによって上書きされることがあります。
* セッション変数は現在のセッションでのみ有効であり、`SET_VAR` ヒントによって上書きされることがあります。
* `SET_VAR` ヒントは、現在のクエリ文でのみ有効です。

## 変数の表示

`SHOW VARIABLES [LIKE 'xxx']` を使用して、すべてまたは一部の変数を表示できます。例：

```SQL
-- システム内のすべての変数を表示。
SHOW VARIABLES;

-- 特定のパターンに一致する変数を表示。
SHOW VARIABLES LIKE '%time_zone%';
```

## 変数の設定

### 変数をグローバルまたは単一のセッションで設定

変数を **グローバルに** または **現在のセッションのみで** 有効に設定できます。グローバルに設定すると、新しい値はすべての将来のセッションで使用されますが、現在のセッションは元の値を使用します。「現在のセッションのみ」に設定すると、変数は現在のセッションでのみ有効になります。

`SET <var_name> = xxx;` で設定された変数は、現在のセッションでのみ有効です。例：

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

`SET GLOBAL <var_name> = xxx;` で設定された変数はグローバルに有効です。例：

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

以下の変数はグローバルにのみ有効です。単一のセッションで有効にすることはできません。これらの変数には `SET GLOBAL <var_name> = xxx;` を使用する必要があります。単一のセッションでそのような変数を設定しようとすると（`SET <var_name> = xxx;`）、エラーが返されます。

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


さらに、変数設定は定数式もサポートしています。例：

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### 単一のクエリ文で変数を設定

特定のクエリに対して変数を設定する必要がある場合があります。`SET_VAR` ヒントを使用することで、単一の文内でのみ有効なセッション変数を設定できます。

StarRocks は、以下の文で `SET_VAR` の使用をサポートしています：

- SELECT
- INSERT (v3.1.12 および v3.2.0 以降)
- UPDATE (v3.1.12 および v3.2.0 以降)
- DELETE (v3.1.12 および v3.2.0 以降)

`SET_VAR` は、上記のキーワードの後にのみ配置され、`/*+...*/` で囲まれます。

例：

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

単一の文で複数の変数を設定することもできます。例：

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

### ユーザーのプロパティとして変数を設定

[ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md) を使用して、セッション変数をユーザーのプロパティとして設定できます。この機能は v3.3.3 からサポートされています。

例：

```SQL
-- ユーザー jack に対してセッション変数 `query_timeout` を `600` に設定。
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## 変数のカテゴリ

システム変数は以下のカテゴリに分類されます。

- [クエリオプティマイザとプランニング](System_variables/optimizer_planning.md): コストベースオプティマイザ、統計情報に基づくプランニング、プルーニング、リライト、CTE 再利用を制御するセッション変数。
- [結合・集計・マテリアライズドビュー](System_variables/joins_agg_mv.md): 結合の実行、集計、Top-N、マテリアライズドビューのクエリリライトに関するセッション変数。
- [実行エンジンと並列度](System_variables/execution_parallelism.md): パイプラインエンジン、並列度、ランタイムフィルタ、クエリキャッシュに関するセッション変数。
- [メモリとスピル](System_variables/memory_spill.md): クエリのメモリ制限と、ディスクまたはリモートストレージへのスピルを制御するセッション変数。
- [スキャン・I/O・データキャッシュ](System_variables/scan_io_datacache.md): スキャンの並列度、I/O タスク、データキャッシュ、ページキャッシュに関するセッション変数。
- [外部カタログとデータレイク](System_variables/external_catalog_lake.md): 外部カタログコネクタと、Hive・Iceberg・Hudi などのデータレイク形式に関するセッション変数。
- [ロード・INSERT・トランザクション](System_variables/loading_insert_txn.md): データロード、INSERT の挙動、トランザクション処理に関するセッション変数。
- [セッション・プロトコル・ガバナンス](System_variables/session_protocol_governance.md): MySQL プロトコル互換、クエリキュー、タイムアウト、リソースグループ、プロファイリングに関するセッション変数。

## 変数インデックス

次の表に、すべてのシステム変数とそれを説明するカテゴリページを示します。`global` ラベルが付いた変数はグローバルにのみ有効で、その他の変数はグローバルまたは単一のセッションで有効になります。

| 変数 | カテゴリ |
| --- | --- |
| [`activate_all_roles_on_login`](System_variables/session_protocol_governance.md#activate_all_roles_on_login-global) | セッション・プロトコル・ガバナンス |
| [`allow_lake_without_partition_filter`](System_variables/external_catalog_lake.md#allow_lake_without_partition_filter) | 外部カタログとデータレイク |
| [`array_low_cardinality_optimize`](System_variables/optimizer_planning.md#array_low_cardinality_optimize) | クエリオプティマイザとプランニング |
| [`auto_increment_increment`](System_variables/session_protocol_governance.md#auto_increment_increment) | セッション・プロトコル・ガバナンス |
| [`avro_use_jni_reader`](System_variables/external_catalog_lake.md#avro_use_jni_reader) | 外部カタログとデータレイク |
| [`big_query_profile_threshold`](System_variables/session_protocol_governance.md#big_query_profile_threshold) | セッション・プロトコル・ガバナンス |
| [`binary_encoding_format`](System_variables/session_protocol_governance.md#binary_encoding_format) | セッション・プロトコル・ガバナンス |
| [`binary_encoding_level`](System_variables/session_protocol_governance.md#binary_encoding_level) | セッション・プロトコル・ガバナンス |
| [`blacklist_backup_routing`](System_variables/execution_parallelism.md#blacklist_backup_routing) | 実行エンジンと並列度 |
| [`catalog`](System_variables/session_protocol_governance.md#catalog) | セッション・プロトコル・ガバナンス |
| [`cbo_cte_force_reuse_limit_without_order_by`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_limit_without_order_by) | クエリオプティマイザとプランニング |
| [`cbo_cte_force_reuse_node_count`](System_variables/optimizer_planning.md#cbo_cte_force_reuse_node_count) | クエリオプティマイザとプランニング |
| [`cbo_cte_max_limit`](System_variables/optimizer_planning.md#cbo_cte_max_limit) | クエリオプティマイザとプランニング |
| [`cbo_cte_reuse`](System_variables/optimizer_planning.md#cbo_cte_reuse) | クエリオプティマイザとプランニング |
| [`cbo_cte_reuse_rate`](System_variables/optimizer_planning.md#cbo_cte_reuse_rate) | クエリオプティマイザとプランニング |
| [`cbo_disabled_rules`](System_variables/optimizer_planning.md#cbo_disabled_rules) | クエリオプティマイザとプランニング |
| [`cbo_enable_low_cardinality_optimize`](System_variables/optimizer_planning.md#cbo_enable_low_cardinality_optimize) | クエリオプティマイザとプランニング |
| [`cbo_eq_base_type`](System_variables/optimizer_planning.md#cbo_eq_base_type) | クエリオプティマイザとプランニング |
| [`cbo_json_v2_dict_opt`](System_variables/optimizer_planning.md#cbo_json_v2_dict_opt) | クエリオプティマイザとプランニング |
| [`cbo_json_v2_rewrite`](System_variables/optimizer_planning.md#cbo_json_v2_rewrite) | クエリオプティマイザとプランニング |
| [`cbo_max_reorder_node_use_dp`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_dp) | クエリオプティマイザとプランニング |
| [`cbo_max_reorder_node_use_exhaustive`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_exhaustive) | クエリオプティマイザとプランニング |
| [`cbo_max_reorder_node_use_greedy`](System_variables/optimizer_planning.md#cbo_max_reorder_node_use_greedy) | クエリオプティマイザとプランニング |
| [`cbo_use_correlated_predicate_estimate`](System_variables/optimizer_planning.md#cbo_use_correlated_predicate_estimate) | クエリオプティマイザとプランニング |
| [`character_set_database`](System_variables/session_protocol_governance.md#character_set_database-global) | セッション・プロトコル・ガバナンス |
| [`collation_server`](System_variables/session_protocol_governance.md#collation_server) | セッション・プロトコル・ガバナンス |
| [`computation_fragment_scheduling_policy`](System_variables/execution_parallelism.md#computation_fragment_scheduling_policy) | 実行エンジンと並列度 |
| [`connector_io_tasks_per_scan_operator`](System_variables/external_catalog_lake.md#connector_io_tasks_per_scan_operator) | 外部カタログとデータレイク |
| [`connector_sink_compression_codec`](System_variables/external_catalog_lake.md#connector_sink_compression_codec) | 外部カタログとデータレイク |
| [`connector_sink_target_max_file_size`](System_variables/external_catalog_lake.md#connector_sink_target_max_file_size) | 外部カタログとデータレイク |
| [`count_distinct_column_buckets`](System_variables/joins_agg_mv.md#count_distinct_column_buckets) | 結合・集計・マテリアライズドビュー |
| [`custom_query_id`](System_variables/session_protocol_governance.md#custom_query_id-session) | セッション・プロトコル・ガバナンス |
| [`datacache_sharing_work_period`](System_variables/scan_io_datacache.md#datacache_sharing_work_period) | スキャン・I/O・データキャッシュ |
| [`default_authentication_plugin`](System_variables/session_protocol_governance.md#default_authentication_plugin) | セッション・プロトコル・ガバナンス |
| [`default_rowset_type`](System_variables/session_protocol_governance.md#default_rowset_type-global) | セッション・プロトコル・ガバナンス |
| [`default_table_compression`](System_variables/session_protocol_governance.md#default_table_compression) | セッション・プロトコル・ガバナンス |
| [`default_tmp_storage_engine`](System_variables/session_protocol_governance.md#default_tmp_storage_engine) | セッション・プロトコル・ガバナンス |
| [`default_view_sql_security`](System_variables/session_protocol_governance.md#default_view_sql_security) | セッション・プロトコル・ガバナンス |
| [`disable_colocate_join`](System_variables/joins_agg_mv.md#disable_colocate_join) | 結合・集計・マテリアライズドビュー |
| [`disable_join_reorder`](System_variables/joins_agg_mv.md#disable_join_reorder) | 結合・集計・マテリアライズドビュー |
| [`disable_spill_to_local_disk`](System_variables/memory_spill.md#disable_spill_to_local_disk) | メモリとスピル |
| [`div_precision_increment`](System_variables/session_protocol_governance.md#div_precision_increment) | セッション・プロトコル・ガバナンス |
| [`dynamic_overwrite`](System_variables/loading_insert_txn.md#dynamic_overwrite) | ロード・INSERT・トランザクション |
| [`enable_adaptive_sink_dop`](System_variables/execution_parallelism.md#enable_adaptive_sink_dop) | 実行エンジンと並列度 |
| [`enable_bucket_aware_execution_on_lake`](System_variables/execution_parallelism.md#enable_bucket_aware_execution_on_lake) | 実行エンジンと並列度 |
| [`enable_cbo_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_cbo_based_mv_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_cbo_table_prune`](System_variables/optimizer_planning.md#enable_cbo_table_prune) | クエリオプティマイザとプランニング |
| [`enable_color_explain_output`](System_variables/session_protocol_governance.md#enable_color_explain_output) | セッション・プロトコル・ガバナンス |
| [`enable_connector_adaptive_io_tasks`](System_variables/external_catalog_lake.md#enable_connector_adaptive_io_tasks) | 外部カタログとデータレイク |
| [`enable_datacache_async_populate_mode`](System_variables/scan_io_datacache.md#enable_datacache_async_populate_mode) | スキャン・I/O・データキャッシュ |
| [`enable_datacache_io_adaptor`](System_variables/scan_io_datacache.md#enable_datacache_io_adaptor) | スキャン・I/O・データキャッシュ |
| [`enable_datacache_sharing`](System_variables/scan_io_datacache.md#enable_datacache_sharing) | スキャン・I/O・データキャッシュ |
| [`enable_distinct_agg_over_window`](System_variables/joins_agg_mv.md#enable_distinct_agg_over_window) | 結合・集計・マテリアライズドビュー |
| [`enable_distinct_column_bucketization`](System_variables/joins_agg_mv.md#enable_distinct_column_bucketization) | 結合・集計・マテリアライズドビュー |
| [`enable_explain_in_profile`](System_variables/session_protocol_governance.md#enable_explain_in_profile) | セッション・プロトコル・ガバナンス |
| [`enable_force_rule_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_force_rule_based_mv_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_gin_filter`](System_variables/scan_io_datacache.md#enable_gin_filter) | スキャン・I/O・データキャッシュ |
| [`enable_global_runtime_filter`](System_variables/execution_parallelism.md#enable_global_runtime_filter) | 実行エンジンと並列度 |
| [`enable_group_by_compressed_key`](System_variables/joins_agg_mv.md#enable_group_by_compressed_key) | 結合・集計・マテリアライズドビュー |
| [`enable_group_execution`](System_variables/execution_parallelism.md#enable_group_execution) | 実行エンジンと並列度 |
| [`enable_group_level_query_queue`](System_variables/session_protocol_governance.md#enable_group_level_query_queue-global) | セッション・プロトコル・ガバナンス |
| [`enable_incremental_mv`](System_variables/joins_agg_mv.md#enable_incremental_mv) | 結合・集計・マテリアライズドビュー |
| [`enable_insert_partial_update`](System_variables/loading_insert_txn.md#enable_insert_partial_update) | ロード・INSERT・トランザクション |
| [`enable_insert_strict`](System_variables/loading_insert_txn.md#enable_insert_strict) | ロード・INSERT・トランザクション |
| [`enable_lake_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_lake_tablet_internal_parallel) | 実行エンジンと並列度 |
| [`enable_load_profile`](System_variables/loading_insert_txn.md#enable_load_profile) | ロード・INSERT・トランザクション |
| [`enable_local_shuffle_agg`](System_variables/execution_parallelism.md#enable_local_shuffle_agg) | 実行エンジンと並列度 |
| [`enable_materialized_view_agg_pushdown_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_agg_pushdown_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_materialized_view_for_insert`](System_variables/joins_agg_mv.md#enable_materialized_view_for_insert) | 結合・集計・マテリアライズドビュー |
| [`enable_materialized_view_text_match_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_text_match_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_materialized_view_union_rewrite`](System_variables/joins_agg_mv.md#enable_materialized_view_union_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_metadata_profile`](System_variables/session_protocol_governance.md#enable_metadata_profile) | セッション・プロトコル・ガバナンス |
| [`enable_multicolumn_global_runtime_filter`](System_variables/execution_parallelism.md#enable_multicolumn_global_runtime_filter) | 実行エンジンと並列度 |
| [`enable_mv_planner`](System_variables/joins_agg_mv.md#enable_mv_planner) | 結合・集計・マテリアライズドビュー |
| [`enable_parallel_merge`](System_variables/execution_parallelism.md#enable_parallel_merge) | 実行エンジンと並列度 |
| [`enable_parquet_reader_bloom_filter`](System_variables/scan_io_datacache.md#enable_parquet_reader_bloom_filter) | スキャン・I/O・データキャッシュ |
| [`enable_parquet_reader_page_index`](System_variables/scan_io_datacache.md#enable_parquet_reader_page_index) | スキャン・I/O・データキャッシュ |
| [`enable_partition_hash_join`](System_variables/joins_agg_mv.md#enable_partition_hash_join) | 結合・集計・マテリアライズドビュー |
| [`enable_per_bucket_optimize`](System_variables/execution_parallelism.md#enable_per_bucket_optimize) | 実行エンジンと並列度 |
| [`enable_phased_scheduler`](System_variables/execution_parallelism.md#enable_phased_scheduler) | 実行エンジンと並列度 |
| [`enable_pipeline_engine`](System_variables/execution_parallelism.md#enable_pipeline_engine) | 実行エンジンと並列度 |
| [`enable_plan_advisor`](System_variables/optimizer_planning.md#enable_plan_advisor) | クエリオプティマイザとプランニング |
| [`enable_predicate_reorder`](System_variables/optimizer_planning.md#enable_predicate_reorder) | クエリオプティマイザとプランニング |
| [`enable_profile`](System_variables/session_protocol_governance.md#enable_profile) | セッション・プロトコル・ガバナンス |
| [`enable_query_cache`](System_variables/execution_parallelism.md#enable_query_cache) | 実行エンジンと並列度 |
| [`enable_query_queue_load`](System_variables/session_protocol_governance.md#enable_query_queue_load-global) | セッション・プロトコル・ガバナンス |
| [`enable_query_queue_select`](System_variables/session_protocol_governance.md#enable_query_queue_select-global) | セッション・プロトコル・ガバナンス |
| [`enable_query_queue_statistic`](System_variables/session_protocol_governance.md#enable_query_queue_statistic-global) | セッション・プロトコル・ガバナンス |
| [`enable_query_tablet_affinity`](System_variables/execution_parallelism.md#enable_query_tablet_affinity) | 実行エンジンと並列度 |
| [`enable_query_trigger_analyze`](System_variables/optimizer_planning.md#enable_query_trigger_analyze) | クエリオプティマイザとプランニング |
| [`enable_rbo_table_prune`](System_variables/optimizer_planning.md#enable_rbo_table_prune) | クエリオプティマイザとプランニング |
| [`enable_reduce_cast_varchar_expr_sync_type`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_expr_sync_type-global) | クエリオプティマイザとプランニング |
| [`enable_reduce_cast_varchar_length_inheritance`](System_variables/optimizer_planning.md#enable_reduce_cast_varchar_length_inheritance-global) | クエリオプティマイザとプランニング |
| [`enable_runtime_adaptive_dop`](System_variables/execution_parallelism.md#enable_runtime_adaptive_dop) | 実行エンジンと並列度 |
| [`enable_scan_datacache`](System_variables/scan_io_datacache.md#enable_scan_datacache) | スキャン・I/O・データキャッシュ |
| [`enable_short_circuit`](System_variables/execution_parallelism.md#enable_short_circuit) | 実行エンジンと並列度 |
| [`enable_sort_aggregate`](System_variables/joins_agg_mv.md#enable_sort_aggregate) | 結合・集計・マテリアライズドビュー |
| [`enable_spill`](System_variables/memory_spill.md#enable_spill) | メモリとスピル |
| [`enable_spill_to_remote_storage`](System_variables/memory_spill.md#enable_spill_to_remote_storage) | メモリとスピル |
| [`enable_spm_rewrite`](System_variables/optimizer_planning.md#enable_spm_rewrite) | クエリオプティマイザとプランニング |
| [`enable_strict_order_by`](System_variables/session_protocol_governance.md#enable_strict_order_by) | セッション・プロトコル・ガバナンス |
| [`enable_sync_materialized_view_rewrite`](System_variables/joins_agg_mv.md#enable_sync_materialized_view_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_table_prune_on_update`](System_variables/optimizer_planning.md#enable_table_prune_on_update) | クエリオプティマイザとプランニング |
| [`enable_tablet_internal_parallel`](System_variables/execution_parallelism.md#enable_tablet_internal_parallel) | 実行エンジンと並列度 |
| [`enable_tablet_pre_split`](System_variables/execution_parallelism.md#enable_tablet_pre_split) | 実行エンジンと並列度 |
| [`enable_topn_runtime_filter`](System_variables/execution_parallelism.md#enable_topn_runtime_filter) | 実行エンジンと並列度 |
| [`enable_ukfk_opt`](System_variables/optimizer_planning.md#enable_ukfk_opt) | クエリオプティマイザとプランニング |
| [`enable_view_based_mv_rewrite`](System_variables/joins_agg_mv.md#enable_view_based_mv_rewrite) | 結合・集計・マテリアライズドビュー |
| [`enable_wait_dependent_event`](System_variables/execution_parallelism.md#enable_wait_dependent_event) | 実行エンジンと並列度 |
| [`enable_write_hive_external_table`](System_variables/external_catalog_lake.md#enable_write_hive_external_table) | 外部カタログとデータレイク |
| [`event_scheduler`](System_variables/session_protocol_governance.md#event_scheduler) | セッション・プロトコル・ガバナンス |
| [`forward_to_leader`](System_variables/session_protocol_governance.md#forward_to_leader) | セッション・プロトコル・ガバナンス |
| [`group_concat_max_len`](System_variables/session_protocol_governance.md#group_concat_max_len) | セッション・プロトコル・ガバナンス |
| [`group_execution_max_groups`](System_variables/execution_parallelism.md#group_execution_max_groups) | 実行エンジンと並列度 |
| [`group_execution_min_scan_rows`](System_variables/execution_parallelism.md#group_execution_min_scan_rows) | 実行エンジンと並列度 |
| [`hash_join_push_down_right_table`](System_variables/joins_agg_mv.md#hash_join_push_down_right_table) | 結合・集計・マテリアライズドビュー |
| [`historical_nodes_min_update_interval`](System_variables/session_protocol_governance.md#historical_nodes_min_update_interval) | セッション・プロトコル・ガバナンス |
| [`init_connect`](System_variables/session_protocol_governance.md#init_connect-global) | セッション・プロトコル・ガバナンス |
| [`innodb_read_only`](System_variables/session_protocol_governance.md#innodb_read_only) | セッション・プロトコル・ガバナンス |
| [`insert_max_filter_ratio`](System_variables/loading_insert_txn.md#insert_max_filter_ratio) | ロード・INSERT・トランザクション |
| [`insert_timeout`](System_variables/loading_insert_txn.md#insert_timeout) | ロード・INSERT・トランザクション |
| [`interactive_timeout`](System_variables/session_protocol_governance.md#interactive_timeout) | セッション・プロトコル・ガバナンス |
| [`io_tasks_per_scan_operator`](System_variables/scan_io_datacache.md#io_tasks_per_scan_operator) | スキャン・I/O・データキャッシュ |
| [`jit_level`](System_variables/execution_parallelism.md#jit_level) | 実行エンジンと並列度 |
| [`lake_bucket_assign_mode`](System_variables/external_catalog_lake.md#lake_bucket_assign_mode) | 外部カタログとデータレイク |
| [`language`](System_variables/session_protocol_governance.md#language-global) | セッション・プロトコル・ガバナンス |
| [`license`](System_variables/session_protocol_governance.md#license-global) | セッション・プロトコル・ガバナンス |
| [`load_mem_limit`](System_variables/loading_insert_txn.md#load_mem_limit) | ロード・INSERT・トランザクション |
| [`log_rejected_record_num`](System_variables/loading_insert_txn.md#log_rejected_record_num-v31) | ロード・INSERT・トランザクション |
| [`low_cardinality_optimize_on_lake`](System_variables/optimizer_planning.md#low_cardinality_optimize_on_lake) | クエリオプティマイザとプランニング |
| [`low_cardinality_optimize_v2`](System_variables/optimizer_planning.md#low_cardinality_optimize_v2) | クエリオプティマイザとプランニング |
| [`lower_case_table_names`](System_variables/session_protocol_governance.md#lower_case_table_names-global) | セッション・プロトコル・ガバナンス |
| [`materialized_view_rewrite_mode`](System_variables/joins_agg_mv.md#materialized_view_rewrite_mode-v32) | 結合・集計・マテリアライズドビュー |
| [`materialized_view_subquery_text_match_max_count`](System_variables/joins_agg_mv.md#materialized_view_subquery_text_match_max_count) | 結合・集計・マテリアライズドビュー |
| [`max_allowed_packet`](System_variables/session_protocol_governance.md#max_allowed_packet) | セッション・プロトコル・ガバナンス |
| [`max_pipeline_dop`](System_variables/execution_parallelism.md#max_pipeline_dop) | 実行エンジンと並列度 |
| [`max_pushdown_conditions_per_column`](System_variables/scan_io_datacache.md#max_pushdown_conditions_per_column) | スキャン・I/O・データキャッシュ |
| [`max_scan_key_num`](System_variables/scan_io_datacache.md#max_scan_key_num) | スキャン・I/O・データキャッシュ |
| [`max_unknown_string_meta_length`](System_variables/session_protocol_governance.md#max_unknown_string_meta_length-global) | セッション・プロトコル・ガバナンス |
| [`metadata_collect_query_timeout`](System_variables/external_catalog_lake.md#metadata_collect_query_timeout) | 外部カタログとデータレイク |
| [`nested_mv_rewrite_max_level`](System_variables/joins_agg_mv.md#nested_mv_rewrite_max_level) | 結合・集計・マテリアライズドビュー |
| [`net_buffer_length`](System_variables/session_protocol_governance.md#net_buffer_length) | セッション・プロトコル・ガバナンス |
| [`net_read_timeout`](System_variables/session_protocol_governance.md#net_read_timeout) | セッション・プロトコル・ガバナンス |
| [`net_write_timeout`](System_variables/session_protocol_governance.md#net_write_timeout) | セッション・プロトコル・ガバナンス |
| [`new_planner_optimize_timeout`](System_variables/optimizer_planning.md#new_planner_optimize_timeout) | クエリオプティマイザとプランニング |
| [`optimizer_materialized_view_timelimit`](System_variables/joins_agg_mv.md#optimizer_materialized_view_timelimit) | 結合・集計・マテリアライズドビュー |
| [`orc_use_column_names`](System_variables/external_catalog_lake.md#orc_use_column_names) | 外部カタログとデータレイク |
| [`parallel_exchange_instance_num`](System_variables/execution_parallelism.md#parallel_exchange_instance_num) | 実行エンジンと並列度 |
| [`parallel_fragment_exec_instance_num`](System_variables/execution_parallelism.md#parallel_fragment_exec_instance_num) | 実行エンジンと並列度 |
| [`parallel_merge_late_materialization_mode`](System_variables/execution_parallelism.md#parallel_merge_late_materialization_mode) | 実行エンジンと並列度 |
| [`partial_update_mode`](System_variables/loading_insert_txn.md#partial_update_mode) | ロード・INSERT・トランザクション |
| [`performance_schema`](System_variables/session_protocol_governance.md#performance_schema-global) | セッション・プロトコル・ガバナンス |
| [`phased_scheduler_max_concurrency`](System_variables/execution_parallelism.md#phased_scheduler_max_concurrency) | 実行エンジンと並列度 |
| [`pipeline_dop`](System_variables/execution_parallelism.md#pipeline_dop) | 実行エンジンと並列度 |
| [`pipeline_profile_level`](System_variables/session_protocol_governance.md#pipeline_profile_level) | セッション・プロトコル・ガバナンス |
| [`pipeline_sink_dop`](System_variables/execution_parallelism.md#pipeline_sink_dop) | 実行エンジンと並列度 |
| [`plan_mode`](System_variables/optimizer_planning.md#plan_mode) | クエリオプティマイザとプランニング |
| [`populate_datacache_mode`](System_variables/scan_io_datacache.md#populate_datacache_mode) | スキャン・I/O・データキャッシュ |
| [`prefer_cte_rewrite`](System_variables/optimizer_planning.md#prefer_cte_rewrite) | クエリオプティマイザとプランニング |
| [`profile_log_latency_threshold_ms`](System_variables/session_protocol_governance.md#profile_log_latency_threshold_ms) | セッション・プロトコル・ガバナンス |
| [`query_cache_agg_cardinality_limit`](System_variables/execution_parallelism.md#query_cache_agg_cardinality_limit) | 実行エンジンと並列度 |
| [`query_cache_entry_max_bytes`](System_variables/execution_parallelism.md#query_cache_entry_max_bytes) | 実行エンジンと並列度 |
| [`query_cache_entry_max_rows`](System_variables/execution_parallelism.md#query_cache_entry_max_rows) | 実行エンジンと並列度 |
| [`query_cache_size`](System_variables/execution_parallelism.md#query_cache_size-global) | 実行エンジンと並列度 |
| [`query_cache_type`](System_variables/execution_parallelism.md#query_cache_type) | 実行エンジンと並列度 |
| [`query_mem_limit`](System_variables/memory_spill.md#query_mem_limit) | メモリとスピル |
| [`query_queue_concurrency_limit`](System_variables/session_protocol_governance.md#query_queue_concurrency_limit-global) | セッション・プロトコル・ガバナンス |
| [`query_queue_cpu_used_permille_limit`](System_variables/session_protocol_governance.md#query_queue_cpu_used_permille_limit-global) | セッション・プロトコル・ガバナンス |
| [`query_queue_max_queued_queries`](System_variables/session_protocol_governance.md#query_queue_max_queued_queries-global) | セッション・プロトコル・ガバナンス |
| [`query_queue_mem_used_pct_limit`](System_variables/session_protocol_governance.md#query_queue_mem_used_pct_limit-global) | セッション・プロトコル・ガバナンス |
| [`query_queue_pending_timeout_second`](System_variables/session_protocol_governance.md#query_queue_pending_timeout_second-global) | セッション・プロトコル・ガバナンス |
| [`query_timeout`](System_variables/session_protocol_governance.md#query_timeout) | セッション・プロトコル・ガバナンス |
| [`range_pruner_max_predicate`](System_variables/optimizer_planning.md#range_pruner_max_predicate) | クエリオプティマイザとプランニング |
| [`resource_group`](System_variables/session_protocol_governance.md#resource_group) | セッション・プロトコル・ガバナンス |
| [`runtime_filter_on_exchange_node`](System_variables/execution_parallelism.md#runtime_filter_on_exchange_node) | 実行エンジンと並列度 |
| [`runtime_join_filter_push_down_limit`](System_variables/execution_parallelism.md#runtime_join_filter_push_down_limit) | 実行エンジンと並列度 |
| [`runtime_profile_report_interval`](System_variables/session_protocol_governance.md#runtime_profile_report_interval) | セッション・プロトコル・ガバナンス |
| [`scan_lake_partition_num_limit`](System_variables/external_catalog_lake.md#scan_lake_partition_num_limit) | 外部カタログとデータレイク |
| [`scan_olap_partition_num_limit`](System_variables/scan_io_datacache.md#scan_olap_partition_num_limit) | スキャン・I/O・データキャッシュ |
| [`skip_local_disk_cache`](System_variables/scan_io_datacache.md#skip_local_disk_cache) | スキャン・I/O・データキャッシュ |
| [`spill_encode_level`](System_variables/memory_spill.md#spill_encode_level) | メモリとスピル |
| [`spill_mode`](System_variables/memory_spill.md#spill_mode-30) | メモリとスピル |
| [`spill_storage_volume`](System_variables/memory_spill.md#spill_storage_volume) | メモリとスピル |
| [`sql_dialect`](System_variables/session_protocol_governance.md#sql_dialect) | セッション・プロトコル・ガバナンス |
| [`sql_mode`](System_variables/session_protocol_governance.md#sql_mode) | セッション・プロトコル・ガバナンス |
| [`sql_safe_updates`](System_variables/session_protocol_governance.md#sql_safe_updates) | セッション・プロトコル・ガバナンス |
| [`sql_select_limit`](System_variables/session_protocol_governance.md#sql_select_limit) | セッション・プロトコル・ガバナンス |
| [`storage_engine`](System_variables/session_protocol_governance.md#storage_engine) | セッション・プロトコル・ガバナンス |
| [`streaming_preaggregation_mode`](System_variables/joins_agg_mv.md#streaming_preaggregation_mode) | 結合・集計・マテリアライズドビュー |
| [`system_time_zone`](System_variables/session_protocol_governance.md#system_time_zone) | セッション・プロトコル・ガバナンス |
| [`time_zone`](System_variables/session_protocol_governance.md#time_zone) | セッション・プロトコル・ガバナンス |
| [`transaction_read_only`](System_variables/loading_insert_txn.md#transaction_read_only) | ロード・INSERT・トランザクション |
| [`tx_isolation`](System_variables/loading_insert_txn.md#tx_isolation) | ロード・INSERT・トランザクション |
| [`tx_visible_wait_timeout`](System_variables/loading_insert_txn.md#tx_visible_wait_timeout) | ロード・INSERT・トランザクション |
| [`version`](System_variables/session_protocol_governance.md#version-global) | セッション・プロトコル・ガバナンス |
| [`version_comment`](System_variables/session_protocol_governance.md#version_comment-global) | セッション・プロトコル・ガバナンス |
| [`wait_timeout`](System_variables/session_protocol_governance.md#wait_timeout) | セッション・プロトコル・ガバナンス |

<VariableWarehouse />
