# StarRocks version 3.1

## 3.1.0

Release date: July 7, 2023

### New Features

#### Shared-data cluster

- Added support for Primary Key tables, on which persistent indexes cannot be enabled.
- Supports the [AUTO_INCREMENT](../sql-reference/sql-statements/auto_increment.md) column attribute, which enables a globally unique ID for each data row and thus simplifies data management.
- Supports [automatically creating partitions during loading and using partitioning expressions to define partitioning rules](../table_design/automatic_partitioning.md), thereby making partition creation easier to use and more flexible.
- Supports [abstraction of storage volumes](../deployment/deploy_shared_data.md#create-default-storage-volume), in which users can configure storage location and authentication information, in StarRocks shared-data clusters. Users can directly reference an existing storage volume when creating a database or table, making authentication configuration easier.

#### Data Lake analytics

- Supports accessing views created on tables within [Hive catalogs](../data_source/catalog/hive_catalog.md).
- Supports accessing Parquet-formatted Iceberg v2 tables.
- [Preview] Supports sinking data to Parquet-formatted Iceberg tables.
- [Preview] Supports accessing data stored in Elasticsearch by using [Elasticsearch catalogs](../data_source/catalog/elasticsearch_catalog.md). This simplifies the creation of Elasticsearch external tables.
- [Preview] Supports performing analytics on streaming data stored in Apache Paimon by using [Paimon catalogs](../data_source/catalog/paimon_catalog.md).

#### Storage engine, data ingestion, and query

- Upgraded automatic partitioning to [expression partitioning](../table_design/expression_partitioning.md). Users only need to use a simple partition expression (either a time function expression or a column expression) to specify a partitioning method at table creation, and StarRocks will automatically create partitions based on the data characteristics and the rule defined in the partition expression during data loading. This method of partition creation is suitable for most scenarios and is more flexible and user-friendly.
- Supports [list partitioning](../table_design/list_partitioning.md). Data is partitioned based on a list of values predefined for a particular column, which can accelerate queries and manage clearly categorized data more efficiently.
- Added a new table named `loads` to the `Information_schema` database. Users can query the results of [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) and [Insert](../sql-reference/sql-statements/data-manipulation/insert.md) jobs from the `loads` table.
- Supports logging the unqualified data rows that are filtered out by [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md), [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md), and [Spark Load](../sql-reference/sql-statements/data-manipulation/SPARK%20LOAD.md) jobs. Users can use the `log_rejected_record_num` parameter in their load job to specify the maximum number of data rows that can be logged.
- Supports [random bucketing](../table_design/Data_distribution.md#choose-bucketing-columns). With this feature, users do not need to configure bucketing columns at table creation, and StarRocks will randomly distribute the data loaded into it to buckets. Using this feature together with the capability of automatically setting the number of buckets (`BUCKETS`) that StarRocks has provided since v2.5.7, users no longer need to consider bucket configurations, and table creation statements are greatly simplified. In big data and high performance-demanding scenarios, however, we recommend that users continue using hash bucketing, because this way they can use bucket pruning to accelerate queries.
- Supports using the table function FILES() in [INSERT INTO](../loading/InsertInto.md) to directly load the data of Parquet- or ORC-formatted data files stored in AWS S3. Additionally, the FILES() function can automatically infer the table schema, which relieves the need to create external catalogs or file external tables before data loading and therefore greatly simplifies the data loading process.
- Supports [generated columns](../sql-reference/sql-statements/generated_columns.md). With the generated column feature, StarRocks can automatically generate and store the values of column expressions and automatically rewrite queries to improve query performance.
- Supports loading data from Spark to StarRocks by using [Spark connector](../loading/Spark-connector-starrocks.md). Compared to [Spark Load](../loading/SparkLoad.md), the Spark connector provides more comprehensive capabilities. Users can define a Spark job to perform ETL operations on the data, and the Spark connector serves as the sink in the Spark job.
- Supports loading data into columns of the [MAP](../sql-reference/sql-statements/data-types/Map.md) and [STRUCT](../sql-reference/sql-statements/data-types/STRUCT.md) data types, and supports nesting Fast Decimal values in ARRAY, MAP, and STRUCT.

#### SQL reference

- Added the following storage volume-related statements: [CREATE STORAGE VOLUME](../sql-reference/sql-statements/Administration/CREATE%20STORAGE%20VOLUME.md), [ALTER STORAGE VOLUME](../sql-reference/sql-statements/Administration/ALTER%20STORAGE%20VOLUME.md), [DROP STORAGE VOLUME](../sql-reference/sql-statements/Administration/DROP%20STORAGE%20VOLUME.md), [SET DEFAULT STORAGE VOLUME](../sql-reference/sql-statements/Administration/SET%20DEFAULT%20STORAGE%20VOLUME.md), [DESC STORAGE VOLUME](../sql-reference/sql-statements/Administration/DESC%20STORAGE%20VOLUME.md), [SHOW STORAGE VOLUMES](../sql-reference/sql-statements/Administration/SHOW%20STORAGE%20VOLUMES.md).

- Supports altering table comments using [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER%20TABLE.md). [#21035](https://github.com/StarRocks/starrocks/pull/21035)

- Added the following functions:

  - Struct functions: [struct (row)](../sql-reference/sql-functions/struct-functions/row.md), [named_struct](../sql-reference/sql-functions/struct-functions/named_struct.md)
  - Map functions: [str_to_map](../sql-reference/sql-functions/string-functions/str_to_map.md), [map_concat](../sql-reference/sql-functions/map-functions/map_concat.md), [map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md), [element_at](../sql-reference/sql-functions/map-functions/element_at.md), [distinct_map_keys](../sql-reference/sql-functions/map-functions/distinct_map_keys.md), [cardinality](../sql-reference/sql-functions/map-functions/cardinality.md)
  - Higher-order Map functions: [map_filter](../sql-reference/sql-functions/map-functions/map_filter.md), [map_apply](../sql-reference/sql-functions/map-functions/map_apply.md), [transform_keys](../sql-reference/sql-functions/map-functions/transform_keys.md), [transform_values](../sql-reference/sql-functions/map-functions/transform_values.md)
  - Array functions: [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) supports `ORDER BY`, [array_generate](../sql-reference/sql-functions/array-functions/array_generate.md), [element_at](../sql-reference/sql-functions/array-functions/element_at.md), [cardinality](../sql-reference/sql-functions/array-functions/cardinality.md)
  - Higher-order Array functions: [all_match](../sql-reference/sql-functions/array-functions/all_match.md), [any_match](../sql-reference/sql-functions/array-functions/any_match.md)
  - Aggregate functions: [min_by](../sql-reference/sql-functions/aggregate-functions/min_by.md), [percentile_disc](../sql-reference/sql-functions/aggregate-functions/percentile_disc.md)
  - Table functions: [generate_series](../sql-reference/sql-functions/table-functions/generate_series.md), [FILES](../sql-reference/sql-functions/table-functions/files.md)
  - Date functions: [next_day](../sql-reference/sql-functions/date-time-functions/next_day.md), [previous_day](../sql-reference/sql-functions/date-time-functions/previous_day.md), [last_day](../sql-reference/sql-functions/date-time-functions/last_day.md), [makedate](../sql-reference/sql-functions/date-time-functions/makedate.md), [date_diff](../sql-reference/sql-functions/date-time-functions/date_diff.md)
  - Bitmap functions：[bitmap_subset_limit](../sql-reference/sql-functions/bitmap-functions/bitmap_subset_limit.md), [bitmap_subset_in_range](../sql-reference/sql-functions/bitmap-functions/bitmap_subset_in_range.md)

#### Privileges and security

Added [privilege items](../administration/privilege_item.md#storage-volume) related to storage volumes and [privilege items](../administration/privilege_item.md#catalog) related to external catalogs, and supports using [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) to grant and revoke these privileges.

### Improvements

#### Shared-data cluster

Optimized the data cache in StarRocks shared-data clusters. The optimized data cache allows for specifying the range of hot data. It can also prevent queries against cold data from occupying the local disk cache, thereby ensuring the performance of queries against hot data.

#### Materialized view

- Optimized the creation of an asynchronous materialized view:
  - Supports random bucketing. If users do not specify bucketing columns, StarRocks adopts random bucketing by default.
  - Supports using `ORDER BY` to specify a sort key.
  - Supports specifying attributes such as `colocate_group`, `storage_medium`, and `storage_cooldown_time`.
  - Supports using session variables. Users can configure these variables by using the `properties("session.<variable_name>" = "<value>")` syntax to flexibly adjust view refreshing strategies.
  - Enables the spill feature for all asynchronous materialized views and implements a query timeout duration of 1 hour by default.
  - Supports creating materialized views based on views. This makes materialized views easier to use in data modeling scenarios, because users can flexibly use views and materialized views based on their varying needs to implement layered modeling.
- Optimized query rewrite with asynchronous materialized views:
  - Supports Stale Rewrite, which allows materialized views that are not refreshed within a specified time interval to be used for query rewrite regardless of whether the base tables of the materialized views are updated. Users can specify the time interval by using the `mv_rewrite_staleness_second` property at materialized view creation.
  - Supports rewriting View Delta Join queries against materialized views that are created on Hive catalog tables (a primary key and a foreign key must be defined).
  - Optimized the mechanism for rewriting queries that contain union operations, and supports rewriting queries that contain joins or functions such as COUNT DISTINCT and time_slice.
- Optimized the refreshing of asynchronous materialized views:
  - Optimized the mechanism for refreshing materialized views that are created on Hive catalog tables. StarRocks now can perceive partition-level data changes, and refreshes only the partitions with data changes during each automatic refresh.
  - Supports using the `REFRESH MATERIALIZED VIEW WITH SYNC MODE` syntax to synchronously invoke materialized view refresh tasks.
- Enhanced the use of asynchronous materialized views:
  - Supports using `ALTER MATERIALIZED VIEW {ACTIVE | INACTIVE}` to enable or disable a materialized view. Materialized views that are disabled (in the `INACTIVE` state) cannot be refreshed or used for query rewrite, but can be directly queried.
  - Supports using `ALTER MATERIALIZED VIEW SWAP WITH` to swap two materialized views. Users can create a new materialized view and then perform an atomic swap with an existing materialized view to implement schema changes on the existing materialized view.
- Optimized synchronous materialized views:
  - Supports direct queries against synchronous materialized views using SQL hints `[_SYNC_MV_]`, allowing for walking around issues that some queries cannot be properly rewritten in rare circumstances.
  - Supports more expressions, such as `CASE-WHEN`, `CAST`, and mathematical operations, which make materialized views suitable for more business scenarios.

#### Data Lake analytics

- Optimized metadata caching and access for Iceberg to improve Iceberg data query performance.
- Optimized the data cache to further improve data lake analytics performance.

#### Storage engine, data ingestion, and query

- Announced the general availability of the [spill](../administration/spill_to_disk.md) feature, which supports spilling the intermediate computation results of some blocking operators to disk. With the spill feature enabled, when a query contains aggregate, sort, or join operators, StarRocks can cache the intermediate computation results of the operators to disk to reduce memory consumption, thereby minimizing query failures caused by memory limits.
- Supports pruning on cardinality-preserving joins. If users maintain a large number of tables which are organized in the star schema (for example, SSB) or the snowflake schema (for example, TCP-H) but they query only a small number of these tables, this feature helps prune unnecessary tables to improve the performance of joins.
- Supports partial updates in column mode. Users can enable the column mode when they perform partial updates on Primary Key tables by using the [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) statement. The column mode is suitable for updating a small number of columns but a large number of rows, and can improve the updating performance by up to 10 times.
- Optimized the collection of statistics for the CBO. This reduces the impact of statistics collection on data ingestion and increases statistics collection performance.
- Optimized the merge algorithm to increase the overall performance by up to 2 times in permutation scenarios.
- Optimized the query logic to reduce dependency on database locks.

#### SQL reference

- Conditional functions case, coalesce, if, ifnull, and nullif support the ARRAY, MAP, STRUCT, and JSON data types.
- The following Array functions support nested types MAP, STRUCT, and ARRAY:
  - array_agg
  - array_contains, array_contains_all, array_contains_any
  - array_slice, array_concat
  - array_length, array_append, array_remove, array_position
  - reverse, array_distinct, array_intersect, arrays_overlap
  - array_sortby
- The following Array functions support the Fast Decimal data type:
  - array_agg
  - array_append, array_remove, array_position, array_contains
  - array_length
  - array_max, array_min, array_sum, array_avg
  - arrays_overlap, array_difference
  - array_slice, array_distinct, array_sort, reverse, array_intersect, array_concat
  - array_sortby, array_contains_all, array_contains_any

### Bug Fixes

Fixed the following issues:

- Requests to reconnect to Kafka for Routine Load jobs cannot be properly processed. [#23477](https://github.com/StarRocks/starrocks/issues/23477)
- For SQL queries that involve multiple tables and contain a `WHERE` clause, if these SQL queries have the same semantics but the order of the tables in each SQL query is different, some of these SQL queries may fail to be rewritten to benefit from the related materialized views. [#22875](https://github.com/StarRocks/starrocks/issues/22875)
- Duplicate records are returned for queries that contain a `GROUP BY` clause. [#19640](https://github.com/StarRocks/starrocks/issues/19640)
- Invoking the lead() or lag() function may cause BE crashes. [#22945](https://github.com/StarRocks/starrocks/issues/22945)
- Rewriting partial partition queries based on materialized views that are created on external catalog tables fail. [#19011](https://github.com/StarRocks/starrocks/issues/19011)
- SQL statements that contain both a backward slash (`\`) and a semicolon (`;`) cannot be properly parsed. [#16552](https://github.com/StarRocks/starrocks/issues/16552)
- A table cannot be truncated if a materialized view created on the table is removed. [#19802](https://github.com/StarRocks/starrocks/issues/19802)

### Behavior Change

- The `storage_cache_ttl` parameter is deleted from the table creation syntax used for StarRocks shared-data clusters. Now the data in the local cache is evicted based on the LRU algorithm.
- The BE configuration items `disable_storage_page_cache` and `alter_tablet_worker_count` and the FE configuration item `lake_compaction_max_tasks` are changed from immutable parameters to mutable parameters.
- The default values of the BE configuration items `block_cache_checksum_enable` and `enable_new_load_on_memory` are changed from `true` to `false`.
- The default value of the FE configuration item `max_running_txn_num_per_db` is changed from `100` to `1000`.
- The default value of the FE configuration item `http_max_header_size` is changed from `8192` to `32768`.
- The default value of the FE configuration item `tablet_create_timeout_second` is changed from `1` to `10`.
- The default value of the FE configuration item `max_routine_load_task_num_per_be` is changed from `5` to `16`, and error information will be returned if a large number of Routine Load tasks are created.
- The FE configuration item `quorom_publish_wait_time_ms` is renamed as `quorum_publish_wait_time_ms`, and the FE configuration item `async_load_task_pool_size` is renamed as `max_broker_load_job_concurrency`.
- The BE configuration item `routine_load_thread_pool_size` is deprecated. Now the routine load thread pool size per BE node is controlled only by the FE configuration item `max_routine_load_task_num_per_be`.
- The BE configuration item `txn_commit_rpc_timeout_ms` and the system variable `tx_visible_wait_timeout` are deprecated. Now the `time_out` parameter is used to specify the transaction timeout duration.
- The FE configuration items `max_broker_concurrency` and `load_parallel_instance_num` are deprecated.
- The FE configuration item `max_routine_load_job_num` is deprecated. Now StarRocks dynamically infers the maximum number of Routine Load tasks supported by each individual BE node based on the `max_routine_load_task_num_per_be` parameter and provides suggestions on task failures.
- Two new Routine Load job properties, `task_consume_second` and `task_timeout_second`, are added to control the maximum amount of time to consume data and the timeout duration for individual load tasks within a Routine Load job, making job adjustment more flexible. If users do not specify these two properties in their Routine Load job, the FE configuration items `routine_load_task_consume_second` and `routine_load_task_timeout_second` prevail.
- TWo new reserved keywords, COMPACTION and TEXT, are added.
