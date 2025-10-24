---
displayed_sidebar: docs
keywords: ['session variable']
---

# System variables

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

## Descriptions of variables

The variables are described **in alphabetical order**. Variables with the `global` label can only take effect globally. Other variables can take effect either globally or for a single session.

### big_query_profile_threshold

* **Description**: Used to set the threshold for big queries. When the session variable `enable_profile` is set to `false` and the amount of time taken by a query exceeds the threshold specified by the variable `big_query_profile_threshold`, a profile is generated for that query.

  Note: In versions v3.1.5 to v3.1.7, as well as v3.2.0 to v3.2.2, we introduced the `big_query_profile_second_threshold` for setting the threshold for big queries. In versions v3.1.8, v3.2.3, and subsequent releases, this parameter has been replaced by `big_query_profile_threshold` to offer more flexible configuration options.
* **Default**: 0
* **Unit**: Second
* **Data type**: String
* **Introduced in**: v3.1

### catalog

* **Description**: Used to specify the catalog to which the session belongs.
* **Default**: default_catalog
* **Data type**: String
* **Introduced in**: v3.2.4

### cbo_enable_low_cardinality_optimize

* **Description**: Whether to enable low cardinality optimization. After this feature is enabled, the performance of querying STRING columns improves by about three times.
* **Default**: true

### cbo_eq_base_type

* **Description**: Specifies the data type used for data comparison between DECIMAL data and STRING data. The default value is `DECIMAL`, and VARCHAR is also a valid value. **This variable takes effect only for `=` and `!=` comparison.**
* **Data type**: String
* **Introduced in**: v2.5.14

### connector_io_tasks_per_scan_operator

* **Description**: The maximum number of concurrent I/O tasks that can be issued by a scan operator during external table queries. The value is an integer. Currently, StarRocks can adaptively adjust the number of concurrent I/O tasks when querying external tables. This feature is controlled by the variable `enable_connector_adaptive_io_tasks`, which is enabled by default.
* **Default**: 16
* **Data type**: Int
* **Introduced in**: v2.5

### connector_sink_compression_codec

* **Description**: Specifies the compression algorithm used for writing data into Hive tables or Iceberg tables, or exporting data with Files().
* **Valid values**: `uncompressed`, `snappy`, `lz4`, `zstd`, and `gzip`.
* **Default**: uncompressed
* **Data type**: String
* **Introduced in**: v3.2.3

### connector_sink_target_max_file_size

* **Description**: Specifies the maximum size of target file for writing data into Hive tables or Iceberg tables, or exporting data with Files(). The limit is not exact and is applied on a best-effort basis.
* **Unit**: Bytes
* **Default**: 1073741824
* **Data type**: Long
* **Introduced in**: v3.3.0

### count_distinct_column_buckets

* **Description**: The number of buckets for the COUNT DISTINCT column in a group-by-count-distinct query. This variable takes effect only when `enable_distinct_column_bucketization` is set to `true`.
* **Default**: 1024
* **Introduced in**: v2.5

### custom_query_id (session)

* **Description**: Used to bind some external identifier to a current query. Can be set using `SET SESSION custom_query_id = 'my-query-id';` before executing a query. The value is reset after query is finished. This value can be passed to `KILL QUERY 'my-query-id'`. Value can be found in audit logs as a `customQueryId` field.
* **Default**: ""
* **Data type**: String
* **Introduced in**: v3.4.0

### disable_colocate_join

* **Description**: Used to control whether the Colocation Join is enabled. The default value is `false`, meaning the feature is enabled. When this feature is disabled, query planning will not attempt to execute Colocation Join.
* **Default**: false

### dynamic_overwrite

* **Description**: Whether to enable the [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) semantic for INSERT OVERWRITE with partitioned tables. Valid values:
  * `true`: Enables Dynamic Overwrite.
  * `false`: Disables Dynamic Overwrite and uses the default semantic.
* **Default**: false
* **Introduced in**: v3.4.0

### enable_adaptive_sink_dop

* **Description**: Specifies whether to enable adaptive parallelism for data loading. After this feature is enabled, the system automatically sets load parallelism for INSERT INTO and Broker Load jobs, which is equivalent to the mechanism of `pipeline_dop`. For a newly deployed v2.5 StarRocks cluster, the value is `true` by default. For a v2.5 cluster upgraded from v2.4, the value is `false`.
* **Default**: false
* **Introduced in**: v2.5

### enable_bucket_aware_execution_on_lake

* **Description**: Whether to enable bucket-aware execution for queries against data lakes (such as Iceberg tables). When this feature is enabled, the system optimizes query execution by leveraging bucketing information to reduce data shuffling and improve performance. This optimization is particularly effective for join operations and aggregations on bucketed tables.
* **Default**: true
* **Data type**: Boolean
* **Introduced in**: v4.0

### enable_cbo_based_mv_rewrite

* **Description**: Whether to enable materialized view rewrite in CBO phase which can maximize the likelihood of successful query rewriting (e.g., when the join order differs between materialized views and queries), but it will increase the execution time of the optimizer phase.
* **Default**: true
* **Introduced in**: v3.5.5, v4.0.1

### enable_connector_adaptive_io_tasks

* **Description**: Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value is `true`. If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.
* **Default**: true
* **Introduced in**: v2.5

### enable_datacache_async_populate_mode

* **Description**: Whether to populate the data cache in asynchronous mode. By default, the system uses the synchronous mode to populate data cache, that is, populating the cache while querying data.
* **Default**: false
* **Introduced in**: v3.2.7

### enable_datacache_io_adaptor

* **Description**: Whether to enable the Data Cache I/O Adaptor. Setting this to `true` enables the feature. When this feature is enabled, the system automatically routes some cache requests to remote storage when the disk I/O load is high, reducing disk pressure.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_datacache_sharing

* **Description**: Whether to enable Cache Sharing. Setting this to `true` enables the feature. Cache Sharing is used to support accessing cache data from other nodes through the network, which can help to reduce performance jitter caused by cache invalidation during cluster scaling. This variable takes effect only when the FE parameter `enable_trace_historical_node` is set to `true`.
* **Default**: true
* **Introduced in**: v3.5.1

### enable_distinct_column_bucketization

* **Description**: Whether to enable bucketization for the COUNT DISTINCT colum in a group-by-count-distinct query. Use the `select a, count(distinct b) from t group by a;` query as an example. If the GROUP BY colum `a` is a low-cardinality column and the COUNT DISTINCT column `b` is a high-cardinality column which has severe data skew, performance bottleneck will occur. In this situation, you can split data in the COUNT DISTINCT column into multiple buckets to balance data and prevent data skew. You must use this variable with the variable `count_distinct_column_buckets`.

  You can also enable bucketization for the COUNT DISTINCT column by adding the `skew` hint to your query, for example, `select a,count(distinct [skew] b) from t group by a;`.

* **Default**: false, which means this feature is disabled.
* **Introduced in**: v2.5

### enable_force_rule_based_mv_rewrite

* **Description**: Whether to enable query rewrite for queries against multiple tables in the optimizer's rule-based optimization phase. Enabling this feature will improve the robustness of the query rewrite. However, it will also increase the time consumption if the query misses the materialized view.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_gin_filter

* **Description**: Whether to utilize the [fulltext inverted index](../table_design/indexes/inverted_index.md) during queries.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_global_runtime_filter

Whether to enable global runtime filter (RF for short). RF filters data at runtime. Data filtering often occurs in the Join stage. During multi-table joins, optimizations such as predicate pushdown are used to filter data, in order to reduce the number of scanned rows for Join and the I/O in the Shuffle stage, thereby speeding up the query.

StarRocks offers two types of RF: Local RF and Global RF. Local RF is suitable for Broadcast Hash Join and Global RF is suitable for Shuffle Join.

Default value: `true`, which means global RF is enabled. If this feature is disabled, global RF does not take effect. Local RF can still work.

### enable_group_by_compressed_key

* **Description**: Whether to use accurate statistical information to compress the GROUP BY Key column. Valid values: `true` and `false`.
* **Default**: true
* **Introduced in**: v4.0

### enable_insert_strict

* **Description**: Whether to enable strict mode while loading data using INSERT from files(). Valid values: `true` and `false` (Default). When strict mode is enabled, the system loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows. For more information, see [Strict mode](../loading/load_concept/strict_mode.md). In versions earlier than v3.4.0, when `enable_insert_strict` is set to `true`, the INSERT jobs fails when there is an unqualified rows.
* **Default**: true

### enable_materialized_view_agg_pushdown_rewrite

* **Description**: Whether to enable aggregation pushdown for materialized view query rewrite. If it is set to `true`, aggregate functions will be pushed down to Scan Operator during query execution and rewritten by the materialized view before the Join Operator is executed. This will relieve the data expansion caused by Join and thereby improve the query performance. For detailed information about the scenarios and limitations of this feature, see [Aggregation pushdown](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#aggregation-pushdown).
* **Default**: false
* **Introduced in**: v3.3.0

### enable_materialized_view_for_insert

* **Description**: Whether to allow StarRocks to rewrite queries in INSERT INTO SELECT statements.
* **Default**: false, which means Query Rewrite in such scenarios is disabled by default.
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_text_match_rewrite

* **Description**: Whether to enable text-based materialized view rewrite. When this item is set to true, the optimizer will compare the query with the existing materialized views. A query will be rewritten if the abstract syntax tree of the materialized view's definition matches that of the query or its sub-query.
* **Default**: true
* **Introduced in**: v3.2.5, v3.3.0

### enable_materialized_view_union_rewrite

* **Description**: Whether to enable materialized view union rewrite. If this item is set to `true`, the system seeks to compensate the predicates using UNION ALL when the predicates in the materialized view cannot satisfy the query's predicates.
* **Default**: true
* **Introduced in**: v2.5.20, v3.1.9, v3.2.7, v3.3.0

### enable_metadata_profile

* **Description**: Whether to enabled Profile for Iceberg Catalog metadata.
* **Default**: true
* **Introduced in**: v3.3.3

### enable_parquet_reader_bloom_filter

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable Bloom Filter optimization when reading Parquet files.
  * `true` (Default): Enable Bloom Filter optimization when reading Parquet files.
  * `false`: Disable Bloom Filter optimization when reading Parquet files.
* **Introduced in**: v3.5.0

### enable_parquet_reader_page_index

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable Page Index optimization when reading Parquet files.
  * `true` (Default): Enable Page Index optimization when reading Parquet files.
  * `false`: Disable Page Index optimization when reading Parquet files.
* **Introduced in**: v3.5.0

### enable_partition_hash_join

* **Description**: Whether to enable adaptive Partition Hash Join.
* **Default**: true
* **Introduced in**: v3.4

### enable_phased_scheduler

* **Description**: Whether to enable multi-phased scheduling. When multi-phased scheduling is enabled, it will schedule fragments according to their dependencies. For example, the system will first schedule the fragment on the build side of a Shuffle Join, and then the fragment on the probe side (Note that, unlike stage-by-stage scheduling, phased scheduling is still under the MPP execution mode). Enabling multi-phased scheduling can significantly reduce memory usage for a large number of UNION ALL queries.
* **Default**: false
* **Introduced in**: v3.3

### enable_pipeline_engine

* **Description**: Specifies whether to enable the pipeline execution engine. `true` indicates enabled and `false` indicates the opposite. Default value: `true`.
* **Default**: true

### enable_plan_advisor

* **Description**: Whether to enable Query Feedback feature for slow queries and manually marked queries.
* **Default**: true
* **Introduced in**: v3.4.0

### enable_query_cache

* **Description**: Specifies whether to enable the Query Cache feature. Valid values: true and false. `true` specifies to enable this feature, and `false` specifies to disable this feature. When this feature is enabled, it works only for queries that meet the conditions specified in the application scenarios of [Query Cache](../using_starrocks/caching/query_cache.md#application-scenarios).
* **Default**: false
* **Introduced in**: v2.5

### enable_query_tablet_affinity

* **Description**: Boolean value to control whether to direct multiple queries against the same tablet to a fixed replica.

  In scenarios where the table to query has a large number of tablets, this feature significantly improves query performance because the meta information and data of the tablet can be cached in memory more quickly.

  However, if there are some hotspot tablets, this feature may degrade the query performance because it directs the queries to the same BE, making it unable to fully use the resources of multiple BEs in high-concurrency scenarios.

* **Default**: false, which means the system selects a replica for each query.
* **Introduced in**: v2.5.6, v3.0.8, v3.1.4, and v3.2.0.

### enable_query_trigger_analyze

* **Default**: true
* **Type**: Boolean
* **Description**: Whether to enable query-trigger ANALYZE tasks.
* **Introduced in**: v3.4.0

### enable_short_circuit

* **Description**: Whether to enable short circuiting for queries. Default: `false`. If it is set to `true`, when the query meets the criteria (to evaluate whether the query is a point query): the conditional columns in the WHERE clause include all primary key columns, and the operators in the WHERE clause are `=` or `IN`, the query takes the short circuit.
* **Default**: false
* **Introduced in**: v3.2.3

### enable_sort_aggregate

* **Description**: Specifies whether to enable sorted streaming. `true` indicates sorted streaming is enabled to sort data in data streams.
* **Default**: false
* **Introduced in**: v2.5

### enable_spill

* **Description**: Whether to enable intermediate result spilling. Default: `false`. If it is set to `true`, StarRocks spills the intermediate results to disk to reduce the memory usage when processing aggregate, sort, or join operators in queries.
* **Default**: false
* **Introduced in**: v3.0

### enable_spill_to_remote_storage

* **Description**: Whether to enable intermediate result spilling to object storage. If it is set to `true`, StarRocks spills the intermediate results to the storage volume specified in `spill_storage_volume` after the capacity limit of the local disk is reached. For more information, see [Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage).
* **Default**: false
* **Introduced in**: v3.3.0

### enable_spm_rewrite

* **Description**: Whether to enable SQL Plan Manager (SPM) query rewrite. When enabled, StarRocks automatically rewrites queries to use bound query plans, improving query performance and stability.
* **Default**: false

### enable_strict_order_by

* **Description**: Used to check whether the column name referenced in ORDER BY is ambiguous. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries.
* **Default**: true
* **Introduced in**: v2.5.18 and v3.1.7

### enable_sync_materialized_view_rewrite

* **Description**: Whether to enable query rewrite based on synchronous materialized views.
* **Default**: true
* **Introduced in**: v3.1.11, v3.2.5

### enable_tablet_internal_parallel

* **Description**: Whether to enable adaptive parallel scanning of tablets. After this feature is enabled, multiple threads can be used to scan one tablet by segment, increasing the scan concurrency.
* **Default**: true
* **Introduced in**: v2.3

### enable_topn_runtime_filter

* **Description**: Whether to enable TopN Runtime Filter. If this feature is enabled, a runtime filter will be dynamically constructed for ORDER BY LIMIT queries and pushed down to the scan for filtering.
* **Default**: true
* **Introduced in**: v3.3

### enable_view_based_mv_rewrite

* **Description**: Whether to enable query rewrite for logical view-based materialized views. If this item is set to `true`, the logical view is used as a unified node to rewrite the queries against itself for better performance. If this item is set to `false`, the system transcribes the queries against logical views into queries against physical tables or materialized views and then rewrites them.
* **Default**: false
* **Introduced in**: v3.1.9, v3.2.5, v3.3.0

### enable_wait_dependent_event

* **Description**: Whether Pipeline waits for a dependent operator to finish execution before continuing within the same fragment. For example, in a left join query, when this feature is enabled, the probe-side operator waits for the build-side operator to finish before it starts executing. Enabling this feature can reduce memory usage, but may increase the query latency. However, for queries reused in CTE, enabling this feature may increase memory usage.
* **Default**: false
* **Introduced in**: v3.3

### enable_write_hive_external_table

* **Description**: Whether to allow for sinking data to external tables of Hive.
* **Default**: false
* **Introduced in**: v3.2

### event_scheduler

Used for MySQL client compatibility. No practical usage.

### group_concat_max_len

* **Description**: The maximum length of string returned by the [group_concat](sql-functions/string-functions/group_concat.md) function.
* **Default**: 1024
* **Min value**: 4
* **Unit**: Characters
* **Data type**: Long

### hash_join_push_down_right_table

* **Description**: Used to control whether the data of the left table can be filtered by using the filter condition against the right table in the Join query. If so, it can reduce the amount of data that needs to be processed during the query.
**Default**: `true` indicates the operation is allowed and the system decides whether the left table can be filtered. `false` indicates the operation is disabled. The default value is `true`.

### historical_nodes_min_update_interval

* **Description**: The minimum interval between two updates of historical node records. If the nodes of a cluster change frequently in a short period of time (that is, less than the value set in this variable), some intermediate states will not be recorded as valid historical node snapshots. The historical nodes are the main basis for the Cache Sharing feature to choose the right cache nodes during cluster scaling.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### insert_max_filter_ratio

* **Description**: The maximum error tolerance of INSERT from files(). It's the maximum ratio of data records that can be filtered out due to inadequate data quality. When the ratio of unqualified data records reaches this threshold, the job fails. Range: [0, 1].
* **Default**: 0
* **Introduced in**: v3.4.0

### insert_timeout

* **Description**: The timeout duration of the INSERT job. Unit: Seconds. From v3.4.0 onwards, `insert_timeout` applies to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE), replacing `query_timeout`.
* **Default**: 14400
* **Introduced in**: v3.4.0

### io_tasks_per_scan_operator

* **Description**: The number of concurrent I/O tasks that can be issued by a scan operator. Increase this value if you want to access remote storage systems such as HDFS or S3 but the latency is high. However, a larger value causes more memory consumption.
* **Default**: 4
* **Data type**: Int
* **Introduced in**: v2.5

### jit_level

* **Description**: The level at which JIT compilation for expressions is enabled. Valid values:
  * `1`: The system adaptively enables JIT compilation for compilable expressions.
  * `-1`: JIT compilation is enabled for all compilable, non-constant expressions.
  * `0`: JIT compilation is disabled. You can disable it manually if any error is returned for this feature.
* **Default**: 1
* **Data type**: Int
* **Introduced in**: -

### lake_bucket_assign_mode

* **Description**: The bucket assignment mode for queries against tables in data lakes. This variable controls how buckets are distributed among worker nodes when bucket-aware execution takes effect during query execution. Valid values:
  * `balance`: Distributes buckets evenly across worker nodes to achieve balanced workload and better performance.
  * `elastic`: Uses consistent hashing to assign buckets to worker nodes, which can provide better load distribution in elastic environments.
* **Default**: balance
* **Data type**: String
* **Introduced in**: v4.0

### load_mem_limit

Specifies the memory limit for the import operation. The default value is 0, meaning that this variable is not used and `query_mem_limit` is used instead.

This variable is only used for the `INSERT` operation which involves both query and import. If the user does not set this variable, the memory limit for both query and import will be set as `exec_mem_limit`. Otherwise, the memory limit for query will be set as `exec_mem_limit` and the memory limit for import will be as `load_mem_limit`.

Other import methods such as `BROKER LOAD`, `STREAM LOAD` still use `exec_mem_limit` for memory limit.

### log_rejected_record_num (v3.1 and later)

Specifies the maximum number of unqualified data rows that can be logged. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.

* The value `0` specifies that data rows that are filtered out will not be logged.
* The value `-1` specifies that all data rows that are filtered out will be logged.
* A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.

### low_cardinality_optimize_on_lake

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable low cardinality optimization on data lake queries. Valid values:
  * `true` (Default): Enable low cardinality optimization on data lake queries.
  * `false`: Disable low cardinality optimization on data lake queries.
* **Introduced in**: v3.5.0

### materialized_view_rewrite_mode (v3.2 and later)

Specifies the query rewrite mode of asynchronous materialized views. Valid values:

* `disable`: Disable automatic query rewrite of asynchronous materialized views.
* `default` (Default value): Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, it directly scans the data in the base table.
* `default_or_error`: Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, an error is returned.
* `force`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, it directly scans the data in the base table.
* `force_or_error`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, an error is returned.

### materialized_view_subuqery_text_match_max_count

* **Description**: Specifies the maximum number of times that the system checks whether a query's sub-query matches the materialized views' definition.
* **Default**: 4
* **Introduced in**: v3.2.5, v3.3.0

### max_allowed_packet

* **Description**: Used for compatibility with the JDBC connection pool C3P0. This variable specifies the maximum size of packets that can be transmitted between the client and server.
* **Default**: 33554432 (32 MB). You can raise this value if the client reports "PacketTooBigException".
* **Unit**: Byte
* **Data type**: Int

### max_pushdown_conditions_per_column

* **Description**: The maximum number of predicates that can be pushed down for a column.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.
* **Data type**: Int

### max_scan_key_num

* **Description**: The maximum number of scan key segmented by each query.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.

### metadata_collect_query_timeout

* **Description**: The timeout duration for Iceberg Catalog metadata collection queries.
* **Unit**: Second
* **Default**: 60
* **Introduced in**: v3.3.3

### nested_mv_rewrite_max_level

* **Description**: The maximum levels of nested materialized views that can be used for query rewrite.
* **Value range**: [1, +∞). The value of `1` indicates that only materialized views created on base tables can be used for query rewrite.
* **Default**: 3
* **Data type**: Int

### new_planner_optimize_timeout

* **Description**: The timeout duration of the query optimizer. When the optimizer times out, an error is returned and the query is stopped, which affects the query performance. You can set this variable to a larger value based on your query or contact StarRocks technical support for troubleshooting. A timeout often occurs when a query has too many joins.
* **Default**: 3000
* **Unit**: ms

### optimizer_materialized_view_timelimit

* **Description**: Specifies the maximum time that one materialized view rewrite rule can consume. When the threshold is reached, this rule will not be used for query rewrite.
* **Default**: 1000
* **Unit**: ms
* **Introduced in**: v3.1.9, v3.2.5

### orc_use_column_names

* **Description**: Used to specify how columns are matched when StarRocks reads ORC files from Hive. The default value is `false`, which means columns in ORC files are read based on their ordinal positions in the Hive table definition. If this variable is set to `true`, columns are read based on their names.
* **Default**: false
* **Introduced in**: v3.1.10

### parallel_exchange_instance_num

Used to set the number of exchange nodes that an upper-level node uses to receive data from a lower-level node in the execution plan. The default value is -1, meaning the number of exchange nodes is equal to the number of execution instances of the lower-level node. When  this variable is set to be greater than 0 but smaller than the number of execution instances of the lower-level node, the number of exchange nodes equals the set value.

In a distributed query execution plan, the upper-level node usually has one or more exchange nodes to receive data from the execution instances of the lower-level node on different BEs. Usually the number of exchange nodes is equal to the number of execution instances of the lower-level node.

In some aggregation query scenarios where the amount of data decreases drastically after aggregation, you can try to modify this variable to a smaller value to reduce the resource overhead. An example would be running aggregation queries using the Duplicate Key table.

### parallel_fragment_exec_instance_num

Used to set the number of instances used to scan nodes on each BE. The default value is 1.

A query plan typically produces a set of scan ranges. This data is distributed across multiple BE nodes. A BE node will have one or more scan ranges, and by default, each BE node's set of scan ranges is processed by only one execution instance. When machine resources suffice, you can increase this variable to allow more execution instances to process a scan range simultaneously for efficiency purposes.

The number of scan instances determines the number of other execution nodes in the upper level, such as aggregation nodes and join nodes. Therefore, it increases the concurrency of the entire query plan execution. Modifying this variable will help  improve efficiency, but larger values will consume more machine resources, such as CPU, memory, and disk IO.

### partial_update_mode

* **Description**: Used to control the mode of partial updates. Valid values:

  * `auto` (default): The system automatically determines the mode of partial updates by analyzing the UPDATE statement and the columns involved.
  * `column`: The column mode is used for the partial updates, which is particularly suitable for the partial updates which involve a small number of columns and a large number of rows.

  For more information, see [UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#partial-updates-in-column-mode-since-v31).
* **Default**: auto
* **Introduced in**: v3.1

### phased_scheduler_max_concurrency

* **Description**: The concurrency for phased scheduler scheduling leaf node fragments. For example, the default value means that, in a large number of UNION ALL Scan queries, at most two scan fragments are allowed to be scheduled at the same time.
* **Default**: 2
* **Introduced in**: v3.3

### pipeline_dop

* **Description**: The parallelism of a pipeline instance, which is used to adjust the query concurrency. Default value: 0, indicating the system automatically adjusts the parallelism of each pipeline instance. This variable also controls the parallelism of loading jobs on OLAP tables. You can also set this variable to a value greater than 0. Generally, set the value to half the number of physical CPU cores. From v3.0 onwards, StarRocks adaptively adjusts this variable based on query parallelism.

* **Default**: 0
* **Data type**: Int

### pipeline_profile_level

* **Description**: Controls the level of the query profile. A query profile often has five layers: Fragment, FragmentInstance, Pipeline, PipelineDriver, and Operator. Different levels provide different details of the profile:

  * 0: StarRocks combines metrics of the profile and shows only a few core metrics.
  * 1: default value. StarRocks simplifies the profile and combines metrics of the profile to reduce profile layers.
  * 2: StarRocks retains all the layers of the profile. The profile size is large in this scenario, especially when the SQL query is complex. This value is not recommended.

* **Default**: 1
* **Data type**: Int

### pipeline_sink_dop

* **Description**: The parallelism of sink for loading data into Iceberg tables and Hive tables, and unloading data using INSERT INTO FILES(). It is used to adjust the concurrency of these loading jobs. Default value: 0, indicating the system automatically adjusts the parallelism. You can also set this variable to a value greater than 0.
* **Default**: 0
* **Data type**: Int

### plan_mode

* **Description**: The metadata retrieval strategy of Iceberg Catalog. For more information, see [Iceberg Catalog metadata retrieval strategy](../data_source/catalog/iceberg/iceberg_catalog.md#appendix-periodic-metadata-refresh-strategy). Valid values:
  * `auto`: The system will automatically select the retrieval plan.
  * `local`: Use the local cache plan.
  * `distributed`: Use the distributed plan.
* **Default**: auto
* **Introduced in**: v3.3.3

#### enable_iceberg_column_statistics

* **Description**: Whether to obtain column statistics, such as `min`, `max`, `null count`, `row size`, and `ndv` (if a puffin file exists). When this item is set to `false`, only the row count information will be collected.
* **Default**: false
* **Introduced in**: v3.4

### populate_datacache_mode

* **Description**: Specifies the population behavior of Data Cache when reading data blocks from external storage systems. Valid values:
  * `auto` (default): the system automatically caches data selectively based on the population rule.
  * `always`: Always cache the data.
  * `never`: Never cache the data.
* **Default**: auto
* **Introduced in**: v3.3.2

### prefer_compute_node

* **Description**: Specifies whether the FEs distribute query execution plans to CN nodes. Valid values:
  * `true`: indicates that the FEs distribute query execution plans to CN nodes.
  * `false`: indicates that the FEs do not distribute query execution plans to CN nodes.
* **Default**: false
* **Introduced in**: v2.4

### query_cache_agg_cardinality_limit

* **Description**: The upper limit of cardinality for GROUP BY in Query Cache. Query Cache is not enabled if the rows generated by GROUP BY exceeds this value. Default value: 5000000. If `query_cache_entry_max_bytes` or `query_cache_entry_max_rows` is set to 0, the Passthrough mode is used even when no computation results are generated from the involved tablets.
* **Default**: 5000000
* **Data type**: Long
* **Introduced in**: v2.5

### query_cache_entry_max_bytes

* **Description**: The threshold for triggering the Passthrough mode. When the number of bytes or rows from the computation results of a specific tablet accessed by a query exceeds the threshold specified by `query_cache_entry_max_bytes` or `query_cache_entry_max_rows`, the query is switched to Passthrough mode.
* **Valid values**: 0 to 9223372036854775807
* **Default**: 4194304
* **Unit**: Byte
* **Introduced in**: v2.5

### query_cache_entry_max_rows

* **Description**: The upper limit of rows that can be cached. See the description in `query_cache_entry_max_bytes`. Default value: .
* **Default**: 409600
* **Introduced in**: v2.5

### query_mem_limit

* **Description**: Used to set the memory limit of a query on each BE node. The default value is 0, which means no limit for it. This item takes effect only after Pipeline Engine is enabled. When the `Memory Exceed Limit` error happens, you could try to increase this variable. Setting it to `0` indicates no limit is imposed.
* **Default**: 0
* **Unit**: Byte

### query_timeout

* **Description**: Used to set the query timeout in "seconds". This variable will act on all query statements in the current connection. The default value is 300 seconds. From v3.4.0 onwards, `query_timeout` does not apply to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE).
* **Value range**: [1, 259200]
* **Default**: 300
* **Data type**: Int
* **Unit**: Second

### range_pruner_max_predicate

* **Description**: The maximum number of IN predicates that can be used for Range partition pruning. Default value: 100. A value larger than 100 may cause the system to scan all tablets, which compromises the query performance.
* **Default**: 100
* **Introduced in**: v3.0

### runtime_filter_on_exchange_node

* **Description**: Whether to place GRF on Exchange Node after GRF is pushed down across the Exchange operator to a lower-level operator. The default value is `false`, which means GRF will not be placed on Exchange Node after it is pushed down across the Exchange operator to a lower-level operator. This prevents repetitive use of GRF and reduces the computation time.

  However, GRF delivery is a "try-best" process. If the lower-level operator fails to receive the GRF but the GRF is not placed on Exchange Node, data cannot be filtered, which compromises filter performance. `true` means GRF will still be placed on Exchange Node even after it is pushed down across the Exchange operator to a lower-level operator.

* **Default**: false

### runtime_profile_report_interval

* **Description**: The time interval at which runtime profiles are reported.
* **Default**: 10
* **Unit**: Second
* **Data type**: Int
* **Introduced in**: v3.1.0

### scan_olap_partition_num_limit

* **Description**: The number of partitions allowed to be scanned for a single table in the execution plan.
* **Default**: 0 (No limit)
* **Introduced in**: v3.3.9

### spill_mode (3.0 and later)

The execution mode of intermediate result spilling. Valid values:

* `auto`: Spilling is automatically triggered when the memory usage threshold is reached.
* `force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.

This variable takes effect only when the variable `enable_spill` is set to `true`.

### sql_dialect

* **Description**: The SQL dialect that is used. For example, you can run the `set sql_dialect = 'trino';` command to set the SQL dialect to Trino, so you can use Trino-specific SQL syntax and functions in your queries.

  > **NOTICE**
  >
  > After you configure StarRocks to use the Trino dialect, identifiers in queries are not case-sensitive by default. Therefore, you must specify names in lowercase for your databases and tables at database and table creation. If you specify database and table names in uppercase, queries against these databases and tables will fail.

* **Data type**: StarRocks
* **Introduced in**: v3.0

### sql_mode

Used to specify the SQL mode to accommodate certain SQL dialects. Valid values include:

* `PIPES_AS_CONCAT`: The pipe symbol `|` is used to concatenate strings, for example, `select 'hello ' || 'world'`.
* `ONLY_FULL_GROUP_BY` (Default): The SELECT LIST can only contain GROUP BY columns or aggregate functions.
* `ALLOW_THROW_EXCEPTION`: returns an error instead of NULL when type conversion fails.
* `FORBID_INVALID_DATE`: prohibits invalid dates.
* `MODE_DOUBLE_LITERAL`: interprets floating-point types as DOUBLE rather than DECIMAL.
* `SORT_NULLS_LAST`: places NULL values at the end after sorting.
* `ERROR_IF_OVERFLOW`: returns an error instead of NULL in the case of arithmetic overflow. Currently, only the DECIMAL data type supports this option.
* `GROUP_CONCAT_LEGACY`: uses the `group_concat` syntax of v2.5 and earlier. This option is supported from v3.0.9 and v3.1.6.

You can set only one SQL mode, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

Or, you can set multiple modes at a time, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_select_limit

* **Description**: Used to limit the maximum number of rows returned by a query, which can prevent issues such as insufficient memory or network congestion caused by the query returning too much data.
* **Default**: Unlimited
* **Data type**: Long

### storage_engine

The types of engines supported by StarRocks:

* olap (default): StarRocks system-owned engine.
* mysql: MySQL external tables.
* broker: Access external tables through a broker program.
* elasticsearch or es: Elasticsearch external tables.
* hive: Hive external tables.
* iceberg: Iceberg external tables, supported from v2.1.
* hudi: Hudi external tables, supported from v2.2.
* jdbc: external table for JDBC-compatible databases, supported from v2.3.

### streaming_preaggregation_mode

Used to specify the preaggregation mode for the first phase of GROUP BY. If the preaggregation effect in the first phase is not satisfactory, you can use the streaming mode, which performs simple data serialization before streaming data to the destination. Valid values:

* `auto`: The system first tries local preaggregation. If the effect is not satisfactory, it switches to the streaming mode. This is the default value.
* `force_preaggregation`: The system directly performs local preaggregation.
* `force_streaming`: The system directly performs streaming.

### time_zone

Used to set the time zone of the current session. The time zone can affect the results of certain time functions.

### use_compute_nodes

* **Description**: The maximum number of CN nodes that can be used. This variable is valid when `prefer_compute_node=true`. Valid values:

  * `-1`: indicates that all CN nodes are used.
  * `0`: indicates that no CN nodes are used.
* **Default**: -1
* **Data type**: Int
* **Introduced in**: v2.4

### wait_timeout

* **Description**: The number of seconds the server waits for activity on a non-interactive connection before closing it. If a client does not interact with StarRocks for this length of time, StarRocks will actively close the connection.
* **Default**: 28800 (8 hours).
* **Unit**: Second
* **Data type**: Int


