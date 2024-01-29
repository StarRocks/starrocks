---
displayed_sidebar: "English"
---

# System variables

StarRocks provides many system variables that can be set and modified to suit your requirements. This section describes the variables supported by StarRocks. You can view the settings of these variables by running the [SHOW VARIABLES](../sql-reference/sql-statements/Administration/SHOW_VARIABLES.md) command on your MySQL client. You can also use the [SET](../sql-reference/sql-statements/Administration/SET.md) command to dynamically set or modify variables. You can make these variables take effect globally on the entire system, only in the current session, or only in a single query statement.

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
* default_rowset_type
* enable_query_queue_select
* enable_query_queue_statistic
* enable_query_queue_load
* init_connect
* lower_case_table_names
* license
* language
* query_cache_size
* query_queue_fresh_resource_usage_interval_ms
* query_queue_concurrency_limit
* query_queue_mem_used_pct_limit
* query_queue_cpu_used_permille_limit
* query_queue_pending_timeout_second
* query_queue_max_queued_queries
* system_time_zone
* version_comment
* version

In addition, variable settings also support constant expressions, such as:

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

 ```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### Set variables in a single query statement

In some scenarios, you may need to set variables specifically for certain queries. By using the `SET_VAR` hint, you can set session variables that will take effect only within a single statement. Example:

```sql
SELECT /*+ SET_VAR(query_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);
```

> **NOTE**
>
> `SET_VAR` can only be placed after the `SELECT` keyword and enclosed in `/*+...*/`.

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

## Descriptions of variables

The variables are described **in alphabetical order**. Variables with the `global` label can only take effect globally. Other variables can take effect either globally or for a single session.

### activate_all_roles_on_login (global）

Whether to enable all roles (including default roles and granted roles) for a StarRocks user when the user connects to the StarRocks cluster. This variable is supported since v3.0.

* If enabled (true), all roles of the user are activated at user login. This takes precedence over the roles set by [SET DEFAULT ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md).
* If disabled (false), the roles set by SET DEFAULT ROLE are activated.

Default value: false.

If you want to activate the roles assigned to you in a session, use the [SET ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) command.

### auto_increment_increment

Used for MySQL client compatibility. No practical usage.

### autocommit

Used for MySQL client compatibility. No practical usage.

### batch_size

Used to specify the number of rows of a single packet transmitted by each node during query execution. The default is 1024, i.e., every 1024 rows of data generated by the source node is packaged and sent to the destination node. A larger number of rows will improve the query throughput in large data volume scenarios, but may increase the query latency in small data volume scenarios. Also, it may increase the memory overhead of the query. We recommend to set `batch_size` between 1024 to 4096.

### big_query_profile_threshold (3.1 and later)

When the session variable `enable_profile` is set to `false` and the amount of time taken by a query exceeds the threshold specified by the variable `big_query_profile_threshold`, a profile is generated for that query.

Note: In versions v3.1.5 to v3.1.7, as well as v3.2.0 to v3.2.2, we introduced the `big_query_profile_second_threshold` for setting the threshold for big queries. In versions v3.1.8, v3.2.3, and subsequent releases, this parameter has been replaced by `big_query_profile_threshold` to offer more flexible configuration options.

### cbo_decimal_cast_string_strict (2.5.14 and later)

Controls how the CBO converts data from the DECIMAL type to the STRING type. If this variable is set to `true`, the logic built in v2.5.x and later versions prevails and the system implements strict conversion (namely, the system truncates the generated string and fills 0s based on the scale length). If this variable is set to `false`, the logic built in versions earlier than v2.5.x prevails and the system processes all valid digits to generate a string. The default value is `true`.

### cbo_enable_low_cardinality_optimize

Whether to enable low cardinality optimization. After this feature is enabled, the performance of querying STRING columns improves by about three times. Default value: true.

### cbo_eq_base_type (2.5.14 and later)

Specifies the data type used for data comparison between DECIMAL-type data and STRING-type data. The default value is `VARCHAR`, and DECIMAL is also a valid value.

### character_set_database (global）

The character set supported by StarRocks. Only UTF8 (`utf8`) is supported.

### connector_io_tasks_per_scan_operator (2.5 and later)

The maximum number of concurrent I/O tasks that can be issued by a scan operator during external table queries. The value is an integer. Default value: 16.

Currently, StarRocks can adaptively adjust the number of concurrent I/O tasks when querying external tables. This feature is controlled by the variable `enable_connector_adaptive_io_tasks`, which is enabled by default.

### count_distinct_column_buckets (2.5 and later)

The number of buckets for the COUNT DISTINCT column in a group-by-count-distinct query. This variable takes effect only when `enable_distinct_column_bucketization` is set to `true`. Default value: 1024.

### default_rowset_type (global)

Global variable. Used to set the default storage format used by the storage engine of the computing node. The currently supported storage formats are `alpha` and `beta`.

### default_table_compression (3.0 and later)

The default compression algorithm for table storage. Supported compression algorithms are `snappy, lz4, zlib, zstd`. Default value: lz4_frame.

Note that if you specified the `compression` property in a CREATE TABLE statement, the compression algorithm specified by `compression` takes effect.

### disable_colocate_join

Used to control whether the Colocation Join is enabled. The default value is `false`, meaning the feature is enabled. When this feature is disabled, query planning will not attempt to execute Colocation Join.

### disable_streaming_preaggregations

Used to enable the streaming pre-aggregations. The default value is `false`, meaning  it is enabled.

### div_precision_increment

Used for MySQL client compatibility. No practical usage.

### enable_connector_adaptive_io_tasks (2.5 and later)

Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value: true.

If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.

### enable_distinct_column_bucketization (2.5 and later)

Whether to enable bucketization for the COUNT DISTINCT colum in a group-by-count-distinct query. Use the `select a, count(distinct b) from t group by a;` query as an example. If the GROUP BY colum `a` is a low-cardinality column and the COUNT DISTINCT column `b` is a high-cardinality column which has severe data skew, performance bottleneck will occur. In this situation, you can split data in the COUNT DISTINCT column into multiple buckets to balance data and prevent data skew.

Default value: false, which means this feature is disabled. You must use this variable with the variable `count_distinct_column_buckets`.

You can also enable bucketization for the COUNT DISTINCT column by adding the `skew` hint to your query, for example, `select a,count(distinct [skew] b) from t group by a;`.

### enable_group_level_query_queue (3.1.4 and later)

Whether to enable resource group-level [query queue](../administration/query_queues.md).

Default value: false, which means this feature is disabled.

### enable_insert_strict

Used to enable the strict mode when loading data using the INSERT statement. The default value is `true`, indicating the strict mode is enabled by default. For more information, see [Strict mode](../loading/load_concept/strict_mode.md).

### enable_materialized_view_for_insert

* Description: Whether to allow StarRocks to rewrite queries in INSERT INTO SELECT statements.
* Default: false, which means Query Rewrite in such scenarios is disabled by default.
* Introduced in: v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_union_rewrite (2.5 and later)

Boolean value to control whether to enable materialized view Union query rewrite. Default: `true`.

### enable_rule_based_materialized_view_rewrite (2.5 and later)

Boolean value to control whether to enable rule-based materialized view query rewrite. This variable is mainly used in single-table query rewrite. Default: `true`.

### enable_short_circuit (3.2.3 and later)

Whether to enable short circuiting for queries. Default: `false`. If it is set to true, when the table uses hybrid row-column storage and the query meets the criteria (to evaluate whether the query is a point query): the conditional columns in the WHERE clause include all primary key columns, and the operators in the WHERE clause are `=` or `IN`, the query takes the short circuit to directly query the data stored in the row-by-row fashion.

### enable_spill (3.0 and later)

Whether to enable intermediate result spilling. Default: `false`. If it is set to `true`, StarRocks spills the intermediate results to disk to reduce the memory usage when processing aggregate, sort, or join operators in queries.

### enable_strict_order_by

Used to check whether the column name referenced in ORDER BY is ambiguous. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries.

This variable is supported from v2.5.18 and v3.1.7.

### enable_profile

Specifies whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required.

By default, a profile is sent to the FE only when a query error occurs in the BE. Profile sending causes network overhead and therefore affects high concurrency.

If you need to analyze the profile of a query, you can set this variable to `true`. After the query is completed, the profile can be viewed on the web page of the currently connected FE (address: `fe_host:fe_http_port/query`). This page displays the profiles of the latest 100 queries with `enable_profile` turned on.

### enable_query_queue_load (global)

Boolean value to enable query queues for loading tasks. Default: `false`.

### enable_query_queue_select (global)

Boolean value to enable query queues for SELECT queries. Default: `false`.

### enable_query_queue_statistic (global)

Boolean value to enable query queues for statistics queries.

### enable_query_tablet_affinity（2.5 and later）

Boolean value to control whether to direct multiple queries against the same tablet to a fixed replica.

In scenarios where the table to query has a large number of tablets, this feature significantly improves query performance because the meta information and data of the tablet can be cached in memory more quickly.

However, if there are some hotspot tablets, this feature may degrade the query performance because it directs the queries to the same BE, making it unable to fully use the resources of multiple BEs in high-concurrency scenarios.

Default value: `false`, which means the system selects a replica for each query. This feature is supported since 2.5.6, 3.0.8, 3.1.4, and 3.2.0.

### enable_scan_datacache (2.5 and later)

Specifies whether to enable the Data Cache feature. After this feature is enabled, StarRocks caches hot data read from external storage systems into blocks, which accelerates queries and analysis. For more information, see [Data Cache](../data_source/data_cache.md). In versions prior to 3.2, this variable was named as `enable_scan_block_cache`.

### enable_populate_datacache (2.5 and later)

Specifies whether to cache data blocks read from external storage systems in StarRocks. If you do not want to cache data blocks read from external storage systems, set this variable to `false`. Default value: true. This variable is supported from 2.5. In versions prior to 3.2, this variable was named as `enable_scan_block_cache`.

### enable_tablet_internal_parallel (2.3 and later)

Whether to enable adaptive parallel scanning of tablets. After this feature is enabled, multiple threads can be used to scan one tablet by segment, increasing the scan concurrency. Default value: true.

### enable_query_cache (2.5 and later)

Specifies whether to enable the Query Cache feature. Valid values: true and false. `true` specifies to enable this feature, and `false` specifies to disable this feature. When this feature is enabled, it works only for queries that meet the conditions specified in the application scenarios of [Query Cache](../using_starrocks/query_cache.md#application-scenarios).

### enable_adaptive_sink_dop (2.5 and later)

Specifies whether to enable adaptive parallelism for data loading. After this feature is enabled, the system automatically sets load parallelism for INSERT INTO and Broker Load jobs, which is equivalent to the mechanism of `pipeline_dop`. For a newly deployed v2.5 StarRocks cluster, the value is `true` by default. For a v2.5 cluster upgraded from v2.4, the value is `false`.

### enable_pipeline_engine

Specifies whether to enable the pipeline execution engine. `true` indicates enabled and `false` indicates the opposite. Default value: `true`.

### enable_sort_aggregate (2.5 and later)

Specifies whether to enable sorted streaming. `true` indicates sorted streaming is enabled to sort data in data streams.

### enable_global_runtime_filter

Whether to enable global runtime filter (RF for short). RF filters data at runtime. Data filtering often occurs in the Join stage. During multi-table joins, optimizations such as predicate pushdown are used to filter data, in order to reduce the number of scanned rows for Join and the I/O in the Shuffle stage, thereby speeding up the query.

StarRocks offers two types of RF: Local RF and Global RF. Local RF is suitable for Broadcast Hash Join and Global RF is suitable for Shuffle Join.

Default value: `true`, which means global RF is enabled. If this feature is disabled, global RF does not take effect. Local RF can still work.

### enable_multicolumn_global_runtime_filter

Whether to enable multi-column global runtime filter. Default value: `false`, which means multi-column global RF is disabled.

If a Join (other than Broadcast Join and Replicated Join) has multiple equi-join conditions:

* If this feature is disabled, only Local RF works.
* If this feature is enabled, multi-column Global RF takes effect and carries `multi-column` in the partition by clause.

### ENABLE_WRITE_HIVE_EXTERNAL_TABLE (v3.2 and later)

Whether to allow for sinking data to external tables of Hive. Default value: `false`.

### event_scheduler

Used for MySQL client compatibility. No practical usage.

### enable_strict_type (v3.1 and later)

Whether to allow implicit conversions for all compound predicates and for all expressions in the WHERE clause. Default value: `false`.

### force_streaming_aggregate

Used to control whether the aggregation node enables streaming aggregation for computing. The default value is false, meaning the feature is not enabled.

### forward_to_master

Used to specify whether some commands will be forwarded to the leader FE for execution. The default value is `false`, meaning not forwarding to the leader FE. There are multiple FEs in a StarRocks cluster, one of which is the leader FE. Normally, users can connect to any FE for full-featured operations. However, some information is only available on the leader FE.

For example, if the SHOW BACKENDS command is not forwarded to the leader FE, only basic information (for example, whether the node is alive) can be viewed. Forwarding to the leader FE can get more detailed information including the node start time and last heartbeat time.

The commands affected by this variable are as follows:

* SHOW FRONTENDS: Forwarding to the leader FE allows users to view the last heartbeat message.

* SHOW BACKENDS: Forwarding to the leader FE allows users to view the boot time, last heartbeat information, and disk capacity information.

* SHOW BROKER: Forwarding to the leader FE allows users to view the boot time and last heartbeat information.

* SHOW TABLET

* ADMIN SHOW REPLICA DISTRIBUTION

* ADMIN SHOW REPLICA STATUS: Forwarding to the leader FE allows users to view the tablet information stored in the metadata of the leader FE. Normally, the tablet information should be the same in the metadata of different FEs. If an error occurs, you can use this method to compare the metadata of the current FE and the leader FE.

* Show PROC: Forwarding to the leader FE allows users to view the PROC information stored in the metadata. This is mainly used for metadata comparison.

### group_concat_max_len

The maximum length of string returned by the [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) function. Default value: 1024. Min value: 4. Unit: characters.

### hash_join_push_down_right_table

Used to control whether the data of the left table can be filtered by using the filter condition against the right table in the Join query. If so, it can reduce the amount of data that needs to be processed during the query.

`true` indicates the operation is allowed and the system decides whether the left table can be filtered. `false` indicates the operation is disabled. The default value is `true`.

### init_connect (global)

Used for MySQL client compatibility. No practical usage.

### interactive_timeout

Used for MySQL client compatibility. No practical usage.

### io_tasks_per_scan_operator (2.5 and later)

The number of concurrent I/O tasks that can be issued by a scan operator. Increase this value if you want to access remote storage systems such as HDFS or S3 but the latency is high. However, a larger value causes more memory consumption.

The value is an integer. Default value: 4.

### language (global)

Used for MySQL client compatibility. No practical usage.

### license (global)

Displays the license of StarRocks.

### load_mem_limit

Specifies the memory limit for the import operation. The default value is 0, meaning that this variable is not used and `query_mem_limit` is used instead.

This variable is only used for the `INSERT` operation which involves both query and import. If the user does not set this variable, the memory limit for both query and import will be set as `exec_mem_limit`. Otherwise, the memory limit for query will be set as `exec_mem_limit` and the memory limit for import will be as `load_mem_limit`.

Other import methods such as `BROKER LOAD`, `STREAM LOAD` still use `exec_mem_limit` for memory limit.

### log_rejected_record_num (v3.1 and later)

Specifies the maximum number of unqualified data rows that can be logged. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.

* The value `0` specifies that data rows that are filtered out will not be logged.
* The value `-1` specifies that all data rows that are filtered out will be logged.
* A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.

### lower_case_table_names (global)

Used for MySQL client compatibility. No practical usage. Table names in StarRocks are case-sensitive.

### materialized_view_rewrite_mode (v3.2 and later)

Specifies the query rewrite mode of asynchronous materialized views. Valid values:

* `disable`: Disable automatic query rewrite of asynchronous materialized views.
* `default` (Default value): Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, it directly scans the data in the base table.
* `default_or_error`: Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, an error is returned.
* `force`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, it directly scans the data in the base table.
* `force_or_error`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, an error is returned.

### max_allowed_packet

Used for compatibility with the JDBC connection pool C3P0. This variable specifies the maximum size of packets that can be transmitted between the client and server. Default value: 32 MB. Unit: Byte. You can raise this value if the client reports "PacketTooBigException".

### max_scan_key_num

The maximum number of scan key segmented by each query. Default value: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.

### max_pushdown_conditions_per_column

The maximum number of predicates that can be pushed down for a column. Default value: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.

### nested_mv_rewrite_max_level

The maximum levels of nested materialized views that can be used for query rewrite. Type: INT. Range: [1, +∞). The value of `1` indicates that only materialized views created on base tables can be used for query rewrite. Default: `3`.

### net_buffer_length

Used for MySQL client compatibility. No practical usage.

### net_read_timeout

Used for MySQL client compatibility. No practical usage.

### net_write_timeout

Used for MySQL client compatibility. No practical usage.

### new_planner_optimize_timeout

The timeout duration of the query optimizer. When the optimizer times out, an error is returned and the query is stopped, which affects the query performance. You can set this variable to a larger value based on your query or contact StarRocks technical support for troubleshooting. A timeout often occurs when a query has too many joins.

Default value: 3000. Unit: ms.

### parallel_exchange_instance_num

Used to set the number of exchange nodes that an upper-level node uses to receive data from a lower-level node in the execution plan. The default value is -1, meaning the number of exchange nodes is equal to the number of execution instances of the lower-level node. When  this variable is set to be greater than 0 but smaller than the number of execution instances of the lower-level node, the number of exchange nodes equals the set value.

In a distributed query execution plan, the upper-level node usually has one or more exchange nodes to receive data from the execution instances of the lower-level node on different BEs. Usually the number of exchange nodes is equal to the number of execution instances of the lower-level node.

In some aggregation query scenarios where the amount of data decreases drastically after aggregation, you can try to modify this variable to a smaller value to reduce the resource overhead. An example would be running aggregation queries using the Duplicate Key table.

### parallel_fragment_exec_instance_num

Used to set the number of instances used to scan nodes on each BE. The default value is 1.

A query plan typically produces a set of scan ranges. This data is distributed across multiple BE nodes. A BE node will have one or more scan ranges, and by default, each BE node's set of scan ranges is processed by only one execution instance. When machine resources suffice, you can increase this variable to allow more execution instances to process a scan range simultaneously for efficiency purposes.

The number of scan instances determines the number of other execution nodes in the upper level, such as aggregation nodes and join nodes. Therefore, it increases the concurrency of the entire query plan execution. Modifying this variable will help  improve efficiency, but larger values will consume more machine resources, such as CPU, memory, and disk IO.

### partial_update_mode (3.1 and later)

Used to control the mode of partial updates. Valid values:

* `auto` (default): The system automatically determines the mode of partial updates by analyzing the UPDATE statement and the columns involved.
* `column`: The column mode is used for the partial updates, which is particularly suitable for the partial updates which involve a small number of columns and a large number of rows.

For more information, see [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md#partial-updates-in-column-mode-since-v31).

### performance_schema

Used for compatibility with MySQL JDBC versions 8.0.16 and above. No practical usage.

### prefer_compute_node

Specifies whether the FEs distribute query execution plans to CN nodes. Valid values:

* true: indicates that the FEs distribute query execution plans to CN nodes.
* false: indicates that the FEs do not distribute query execution plans to CN nodes.

### pipeline_dop

The parallelism of a pipeline instance, which is used to adjust the query concurrency. Default value: 0, indicating the system automatically adjusts the parallelism of each pipeline instance. You can also set this variable to a value greater than 0. Generally, set the value to half the number of physical CPU cores.

From v3.0 onwards, StarRocks adaptively adjusts this variable based on query parallelism.

### pipeline_profile_level

Controls the level of the query profile. A query profile often has five layers: Fragment, FragmentInstance, Pipeline, PipelineDriver, and Operator. Different levels provide different details of the profile:

* 0: StarRocks combines metrics of the profile and shows only a few core metrics.
* 1: default value. StarRocks simplifies the profile and combines metrics of the profile to reduce profile layers.
* 2: StarRocks retains all the layers of the profile. The profile size is large in this scenario, especially when the SQL query is complex. This value is not recommended.

### query_cache_entry_max_bytes (2.5 and later)

The threshold for triggering the Passthrough mode. Valid values: 0 to 9223372036854775807. When the number of bytes or rows from the computation results of a specific tablet accessed by a query exceeds the threshold specified by `query_cache_entry_max_bytes` or `query_cache_entry_max_rows`, the query is switched to Passthrough mode.

### query_cache_entry_max_rows (2.5 and later)

The upper limit of rows that can be cached. See the description in `query_cache_entry_max_bytes`. Default value: 409600.

### query_cache_agg_cardinality_limit (2.5 and later)

The upper limit of cardinality for GROUP BY in Query Cache. Query Cache is not enabled if the rows generated by GROUP BY exceeds this value. Default value: 5000000. If `query_cache_entry_max_bytes` or `query_cache_entry_max_rows` is set to 0, the Passthrough mode is used even when no computation results are generated from the involved tablets.

### query_cache_size (global)

Used for MySQL client compatibility. No practical use.

### query_cache_type

Used for compatibility with JDBC connection pool C3P0. No practical use.

### query_mem_limit

Used to set the memory limit of a query on each BE node. Unit: Byte. The default value is 0, which means no limit for it. This item takes effect only after Pipeline Engine is enbaled.

When the `Memory Exceed Limit` error happens, you could try to increase this variable.

### query_queue_concurrency_limit (global)

The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. Default: `0`.

### query_queue_cpu_used_permille_limit (global)

The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Default: `0`. Range: [0, 1000]

### query_queue_max_queued_queries (global)

The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. Default: `1024`.

### query_queue_mem_used_pct_limit (global)

The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Default: `0`. Range: [0, 1]

### query_queue_pending_timeout_second (global)

The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected. Unit: second. Default: `300`.

### query_timeout

Used to set the query timeout in "seconds". This variable will act on all query statements in the current connection, as well as INSERT statements. The default value is 300 seconds. Value range: [1, 259200].

### range_pruner_max_predicate (v3.0 and later)

The maximum number of IN predicates that can be used for Range partition pruning. Default value: 100. A value larger than 100 may cause the system to scan all tablets, which compromises the query performance.

### rewrite_count_distinct_to_bitmap_hll

Used to decide whether to rewrite count distinct queries to bitmap_union_count and hll_union_agg.

### runtime_filter_on_exchange_node

Whether to place GRF on Exchange Node after GRF is pushed down across the Exchange operator to a lower-level operator. The default value is `false`, which means GRF will not be placed on Exchange Node after it is pushed down across the Exchange operator to a lower-level operator. This prevents repetitive use of GRF and reduces the computation time.

However, GRF delivery is a "try-best" process. If the lower-level operator fails to receive the GRF but the GRF is not placed on Exchange Node, data cannot be filtered, which compromises filter performance. `true` means GRF will still be placed on Exchange Node even after it is pushed down across the Exchange operator to a lower-level operator.

### runtime_join_filter_push_down_limit

The maximum number of rows allowed for the Hash table based on which Bloom filter Local RF is generated. Local RF will not be generated if this value is exceeded. This variable prevents the generation of an excessively long Local RF.

The value is an integer. Default value: 1024000.

### runtime_profile_report_interval

The time interval at which runtime profiles are reported. This variable is supported from v3.1.0 onwards.

Unit: second, Default: `10`.

### spill_mode (3.0 and later)

The execution mode of intermediate result spilling. Valid values:

* `auto`: Spilling is automatically triggered when the memory usage threshold is reached.
* `force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.

This variable takes effect only when the variable `enable_spill` is set to `true`.

### SQL_AUTO_IS_NULL

Used for compatibility with the JDBC connection pool C3P0. No practical usage.

### sql_dialect  (v3.0 and later)

The SQL dialect that is used. For example, you can run the `set sql_dialect = 'trino';` command to set the SQL dialect to Trino, so you can use Trino-specific SQL syntax and functions in your queries.

> **NOTICE**
>
> After you configure StarRocks to use the Trino dialect, identifiers in queries are not case-sensitive by default. Therefore, you must specify names in lowercase for your databases and tables at database and table creation. If you specify database and table names in uppercase, queries against these databases and tables will fail.

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

### sql_safe_updates

Used for MySQL client compatibility. No practical usage.

### sql_select_limit

Used for MySQL client compatibility. No practical usage.

### statistic_collect_parallel

Used to adjust the parallelism of statistics collection tasks that can run on BEs. Default value: 1. You can increase this value to speed up collection tasks.

### storage_engine

The types of engines supported by StarRocks:

* olap: StarRocks system-owned engine.
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

### system_time_zone

Used to display the time zone of the current system. Cannot be changed.

### time_zone

Used to set the time zone of the current session. The time zone can affect the results of certain time functions.

### transaction_read_only

* Description: Used for MySQL 5.8 compatibility. The alias is `tx_read_only`. This variable specifies the transaction access mode. `ON` indicates read only and `OFF` indicates readable and writable.
* Default: OFF
* Introduced in: v2.5.18, v3.0.9, v3.1.7

### tx_isolation

Used for MySQL client compatibility. No practical usage. The alias is `transaction_isolation`.

### use_compute_nodes

The maximum number of CN nodes that can be used. This variable is valid when `prefer_compute_node=true`. Valid values:

* `-1`: indicates that all CN nodes are used.
* `0`: indicates that no CN nodes are used.

### use_v2_rollup

Used to control the query to fetch data using the rollup index of the segment v2 storage format. This variable is used for validation when going online with segment v2. It is not recommended for other cases.

### vectorized_engine_enable (deprecated from v2.4 onwards)

Used to control whether the vectorized engine is used to execute queries. A value of `true` indicates that the vectorized engine is used, otherwise the non-vectorized engine is used. The default is `true`. This feature is enabled by default from v2.4 onwards and therefore, is deprecated.

### version (global)

The MySQL server version returned to the client.

### version_comment (global)

The StarRocks version. Cannot be changed.

### wait_timeout

Used to set the connection timeout for idle connections. When an idle connection does not interact with StarRocks for that length of time, StarRocks will actively disconnect the link. The default value is 8 hours, in seconds.
