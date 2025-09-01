---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.7

Release Date: September 1, 2025

### Bug Fixes

The following issues have been fixed:

- Routine Load jobs did not serialize `max_filter_ratio`. [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- In Stream Load, the `now(precision)` function lost the precision parameter. [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- In Audit Log, the Scan Rows result for `INSERT INTO SELECT` statements was inaccurate. [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- After upgrading the cluster to v3.4.5, the `fslib read iops` metric increased compared to before the upgrade. [#61724](https://github.com/StarRocks/starrocks/pull/61724)
- Queries against SQLServer using JDBC Catalog often got stuck. [#61719](https://github.com/StarRocks/starrocks/pull/61719)

## 3.4.6

Release Date: August 7, 2025

### Improvements

- When exporting data to Parquet files using `INSERT INTO FILES`, you can now specify the Parquet version via the [`parquet.version`](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files.md#parquetversion) property to improve compatibility with other tools when reading the exported files. [#60843](https://github.com/StarRocks/starrocks/pull/60843)

### Bug Fixes

The following issues have been fixed:

- Loading jobs failed due to overly coarse lock granularity in `TableMetricsManager`. [#58911](https://github.com/StarRocks/starrocks/pull/58911)
- Case sensitivity issue in column names when loading Parquet data via `FILES()`. [#61059](https://github.com/StarRocks/starrocks/pull/61059)
- Cache did not take effect after upgrading a shared-data cluster from v3.3 to v3.4 or later. [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- A division-by-zero error occurred when the partition ID was null, causing a BE crash. [#60842](https://github.com/StarRocks/starrocks/pull/60842)
- Broker Load jobs failed during BE scaling. [#60224](https://github.com/StarRocks/starrocks/pull/60224)

### Behavior Changes

- The `keyword` column in the `information_schema.keywords` view has been renamed to `word` to align with the MySQL definition. [#60863](https://github.com/StarRocks/starrocks/pull/60863)

## 3.4.5

Release Date: July 10, 2025

### Improvements

- Enhanced observability of loading job execution: Unified the runtime information of loading tasks into the `information_schema.loads` view. Users can view the execution details of all INSERT, Broker Load, Stream Load, and Routine Load subtasks in this view. Additional fields have been added to help users better understand the status of loading tasks and the association with parent jobs (PIPES, Routine Load Jobs).
- Support modifying `kafka_broker_list` via the `ALTER ROUTINE LOAD` statement.

### Bug Fixes

The following issues have been fixed:

- Under high-frequency loading scenarios, Compaction could be delayed. [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- Querying Iceberg external tables via Unified Catalog would throw an error: `not support getting unified metadata table factory`. [#59412](https://github.com/StarRocks/starrocks/pull/59412)
- When using `DESC FILES()` to view CSV files in remote storage, incorrect results were returned because the system mistakenly inferred `xinf` as the FLOAT type. [#59574](https://github.com/StarRocks/starrocks/pull/59574)
- `INSERT INTO` could cause BE to crash when encountering empty partitions. [#59553](https://github.com/StarRocks/starrocks/pull/59553)
- When StarRocks reads Equality Delete files in Iceberg, it could still access deleted data if the data had already been removed from the Iceberg table. [#59709](https://github.com/StarRocks/starrocks/pull/59709)
- Query failures caused by renaming columns. [#59178](https://github.com/StarRocks/starrocks/pull/59178)

### Behavior Changes

- The default value of the BE configuration item `skip_pk_preload` has been changed from `false` to `true`. As a result, the system will skip preloading Primary Key Indexes for Primary Key tables to reduce the likelihood of `Reached Timeout` errors. This change may increase query latency for operations that require loading Primary Key Indexes.

## 3.4.4

Release Date: June 10, 2025

### Improvements

- Storage Volume now supports ADLS2 using Managed Identity as the credential. [#58454](https://github.com/StarRocks/starrocks/pull/58454)
- For [partitions based on complex time function expressions](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/#partitioning-based-on-a-complex-time-function-expression-since-v34), partition pruning works well for partitions based on most DATETIME-related functions
- Supports loading Avro data files from Azure using the `FILES` function. [#58131](https://github.com/StarRocks/starrocks/pull/58131)
- When Routine Load encounters invalid JSON data, the consumed partition and offset information is logged in the error log to facilitate troubleshooting. [#55772](https://github.com/StarRocks/starrocks/pull/55772)

### Bug Fixes

The following issues have been fixed:

- Concurrent queries accessing the same partition in a partitioned table caused Hive Metastore to hang. [#58089](https://github.com/StarRocks/starrocks/pull/58089)
- Abnormal termination of `INSERT` tasks caused the job to remain in the `QUEUEING` state. [#58603](https://github.com/StarRocks/starrocks/pull/58603)
- After upgrading the cluster from v3.4.0 to v3.4.2, a large number of tablet replicas encounter exceptions. [#58518](https://github.com/StarRocks/starrocks/pull/58518)
- FE OOM caused by incorrect `UNION` execution plans. [#59040](https://github.com/StarRocks/starrocks/pull/59040)
- Invalid database IDs during partition recycling could cause FE startup to fail. [#59666](https://github.com/StarRocks/starrocks/pull/59666)
- After a failed FE CheckPoint operation, the process could not exit properly, resulting in blocking. [#58602](https://github.com/StarRocks/starrocks/pull/58602)

## 3.4.3

Release Date: April 30, 2025

### Improvements

- Routine Load and Stream Load support the use of Lambda expressions in the `columns` parameter for complex column data extraction. `array_filter`/`map_filter` can be used to filter and extract ARRAY/MAP data. Complex filtering and extraction of JSON data can be achieved by combining the `cast` function to convert JSON array/JSON object to ARRAY and MAP types. For example, `COLUMNS (js, col=array_filter(i -> json_query(i, '$.type')=='t1', cast(js as Array<JSON>))[1])` can extract the first JSON object from the JSON array `js` where `type` is `t1`. [#58149](https://github.com/StarRocks/starrocks/pull/58149)
- Supports converting JSON objects to MAP type using the `cast` function, combined with `map_filter` to extract items from the JSON object that meet specific conditions. For example, `map_filter((k, v) -> json_query(v, '$.type') == 't1', cast(js AS MAP<String, JSON>))` can extract the JSON object from `js` where `type` is `t1`. [#58045](https://github.com/StarRocks/starrocks/pull/58045)
- LIMIT is now supported when querying the `information_schema.task_runs` view. [#57404](https://github.com/StarRocks/starrocks/pull/57404)

### Bug Fixes

The following issues have been fixed:

- Queries against ORC format Hive tables are returned with an error `OrcChunkReader::lazy_seek_to failed. reason = bad read in RleDecoderV2: :readByte`. [#57454](https://github.com/StarRocks/starrocks/pull/57454)
- RuntimeFilter from the upper layer could not be pushed down when querying Iceberg tables that contain Equality Delete files. [#57651](https://github.com/StarRocks/starrocks/pull/57651)
- Enabling the spill-to-disk pre-aggregation strategy causes queries to fail. [#58022](https://github.com/StarRocks/starrocks/pull/58022)
- Queries are returned with an error `ConstantRef-cmp-ConstantRef not supported here, null != 111 should be eliminated earlier`. [#57735](https://github.com/StarRocks/starrocks/pull/57735)
- Query timeout with the `query_queue_pending_timeout_second` parameter while the Query Queue feature is not enabled. [#57719](https://github.com/StarRocks/starrocks/pull/57719)

## 3.4.2

Release Date: April 10, 2025

### Improvements

- FE supports graceful shutdown to improve system availability. When exiting FE via `./stop_fe.sh -g`, FE will first return a 500 status code to the front-end Load Balancer via the `/api/health` API to indicate that it is preparing to shut down, allowing the Load Balancer to switch to other available FE nodes. Meanwhile, FE will continue to run ongoing queries until they finish or timeout (default timeout: 60 seconds). [#56823](https://github.com/StarRocks/starrocks/pull/56823)

### Bug Fixes

The following issues have been fixed:

- Partition pruning might not work if the partition column is a generated column. [#54543](https://github.com/StarRocks/starrocks/pull/54543)
- Incorrect parameter handling in the `concat` function could cause a BE crash during query execution. [#57522](https://github.com/StarRocks/starrocks/pull/57522)
- The `ssl_enable` property did not take effect when using Broker Load to load data. [#57229](https://github.com/StarRocks/starrocks/pull/57229)
- When NULL values exist, querying subfields of STRUCT-type columns could cause a BE crash. [#56496](https://github.com/StarRocks/starrocks/pull/56496)
- When modifying the bucket distribution of a table with the statement `ALTER TABLE {table} PARTITIONS (p1, p1) DISTRIBUTED BY ...`, specifying duplicate partition names could result in failure to delete internally generated temporary partitions. [#57005](https://github.com/StarRocks/starrocks/pull/57005)
- In a shared-data cluster, running `SHOW PROC '/current_queries'` resulted in the error "Error 1064 (HY000): Sending collect query statistics request fails". [#56597](https://github.com/StarRocks/starrocks/pull/56597)
- Running `INSERT OVERWRITE` loading tasks in parallel caused the error "ConcurrentModificationException: null", resulting in loading failure. [#56557](https://github.com/StarRocks/starrocks/pull/56557)
- After upgrading from v2.5.21 to v3.1.17, running multiple Broker Load tasks concurrently could cause exceptions. [#56512](https://github.com/StarRocks/starrocks/pull/56512)

### Behavior Changes

- The default value of the BE configuration item `avro_ignore_union_type_tag` has been changed to `true`, enabling the direct parsing of `["NULL", "STRING"]` as STRING type data, which better aligns with typical user requirements. [#57553](https://github.com/StarRocks/starrocks/pull/57553)
- The default value of the session variable `big_query_profile_threshold` has been changed from 0 to 30 (seconds). [#57177](https://github.com/StarRocks/starrocks/pull/57177)
- A new FE configuration item `enable_mv_refresh_collect_profile` has been added to control whether to collect Profile information during materialized view refresh. The default value is `false` (previously, the system collected Profile by default). [#56971](https://github.com/StarRocks/starrocks/pull/56971)

## 3.4.1 (Yanked)

Release Date: March 12, 2025

:::tip

This version has been taken offline due to metadata loss issues in **shared-data clusters**.

- **Problem**: When there are committed compaction transactions that are not yet been published during a shift of Leader FE node in a shared-data cluster, metadata loss may occur after the shift.

- **Impact scope**: This problem only affects shared-data clusters. Shared-nothing clusters are unaffected.

- **Temporary workaround**: When the Publish task is returned with an error, you can execute `SHOW PROC 'compactions'` to check if there are any partitions that have two compaction transactions with empty `FinishTime`. You can execute `ALTER TABLE DROP PARTITION FORCE` to drop the partitions to avoid Publish tasks getting hang.

:::

### New Features and Enhancements

- Data lake analytics supports Deletion Vector in Delta Lake.
- Supports secure views. By creating a secure view, you can prevent users without the SELECT privilege on the referenced base tables from querying the view (even if they have the SELECT privilege on the view).
- Supports for Sketch HLL ([`ds_hll_count_distinct`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_hll_count_distinct/)). Compared to `approx_count_distinct`, this function provides higher-precision approximate deduplication.
- Shared-data clusters support automatic snapshot creation for cluster recovery.
- Storage Volume in the shared-data clusters supports Azure Data Lake Storage Gen2.
- Supports SSL authentication for connections to StarRocks via the MySQL protocol, ensuring that data transmitted between the client and the StarRocks cluster cannot be read by unauthorized users.

### Bug Fixes

The following issues have been fixed:

- An issue where OLAP views affected the materialized view processing logic. [#52989](https://github.com/StarRocks/starrocks/pull/52989)
- Write transactions would fail if one replica was not found, regardless of how many replicas had successfully committed. (After the fix, the transaction succeeds as long as the majority replicas succeed. [#55212](https://github.com/StarRocks/starrocks/pull/55212)
- Stream Load fails when a node with an Alive status of false was scheduled. [#55371](https://github.com/StarRocks/starrocks/pull/55371)
- Files in cluster snapshots were mistakenly deleted. [#56338](https://github.com/StarRocks/starrocks/pull/56338)

### Behavior Changes

- Graceful shutdown is now enabled by default (previously it was disabled). The default value of the related BE/CN parameter `loop_count_wait_fragments_finish` has been changed to `2`, meaning that the system will wait up to 20 seconds for running queries to complete. [#56002](https://github.com/StarRocks/starrocks/pull/56002)

## 3.4.0

Release date: January 24, 2025

### Data Lake Analytics

- Optimized Iceberg V2 query performance and lowered memory usage by reducing repeated reads of delete-files.
- Supports column mapping for Delta Lake tables, allowing queries against data after Delta Schema Evolution. For more information, see [Delta Lake catalog - Feature support](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/#feature-support).
- Data Cache related improvements:
  - Introduces a Segmented LRU (SLRU) Cache eviction strategy, which significantly defends against cache pollution from occasional large queries, improves cache hit rate, and reduces fluctuations in query performance. In simulated test cases with large queries, SLRU-based query performance can be improved by 70% or even higher. For more information, see [Data Cache - Cache replacement policies](https://docs.starrocks.io/docs/data_source/data_cache/#cache-replacement-policies).
  - Unified the Data Cache instance used in both shared-data architecture and data lake query scenarios to simplify the configuration and improve resource utilization. For more information, see [Data Cache](https://docs.starrocks.io/docs/using_starrocks/caching/block_cache/).
  - Provides an adaptive I/O strategy optimization for Data Cache, which flexibly routes some query requests to remote storage based on the cache disk's load and performance, thereby enhancing overall access throughput.
  - Supports persistence of data in Data Cache in Data Lake query scenarios. The previously cached data can be reused after BE restarts to reduce query performance fluctuations.
- Supports automatic collection of external table statistics through automatic ANALYZE tasks triggered by queries. It can provide more accurate NDV information compared to metadata files, thereby optimizing the query plan and improving query performance. For more information, see [Query-triggered collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#query-triggered-collection).
- Provides Time Travel query capability for Iceberg, allowing data to be read from a specified BRANCH or TAG by specifying TIMESTAMP or VERSION.
- Supports asynchronous delivery of query fragments for data lake queries. It avoids the restriction that FE must obtain all files to be queried before BE can execute a query, thus allowing FE to fetch query files and BE to execute queries in parallel, and reducing the overall latency of data lake queries involving a large number of files that are not in the cache. Meanwhile, it reduces the memory load on FE due to caching the file list and improves query stability. (Currently, the optimization for Hudi and Delta Lake is implemented, while the optimization for Iceberg is still under development.)

### Performance Improvement and Query Optimization

- [Experimental] Offers a preliminary Query Feedback feature for automatic optimization of slow queries. The system will collect the execution details of slow queries, automatically analyze its query plan for potential opportunities for optimization, and generate a tailored optimization guide for the query. If CBO generates the same bad plan for subsequent identical queries, the system will locally optimize this query plan based on the guide. For more information, see [Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/).
- [Experimental] Supports Python UDFs, offering more convenient function customization compared to Java UDFs. For more information, see [Python UDF](https://docs.starrocks.io/docs/sql-reference/sql-functions/Python_UDF/).
- Enables the pushdown of multi-column OR predicates, allowing queries with multi-column OR conditions (for example, `a = xxx OR b = yyy`) to utilize certain column indexes, thus reducing data read volume and improving query performance.
- Optimized TPC-DS query performance by roughly 20% under the TPC-DS 1TB Iceberg dataset. Optimization methods include table pruning and aggregated column pruning using primary and foreign keys, and aggregation pushdown.

### Shared-data Enhancements

- Supports Query Cache, aligning the shared-nothing architecture.
- Supports synchronous materialized views, aligning the shared-nothing architecture.

### Storage Engine

- Unified all partitioning methods into the expression partitioning and supported multi-level partitioning, where each level can be any expression. For more information, see [Expression Partitioning](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/).
- [Preview] Supports all native aggregate functions in Aggregate tables. By introducing a generic aggregate function state storage framework, all native aggregate functions supported by StarRocks can be used to define an Aggregate table.
- Supports vector indexes, enabling fast approximate nearest neighbor searches (ANNS) of large-scale, high-dimensional vectors, which are commonly required in deep learning and machine learning scenarios. Currently, StarRocks supports two types of vector indexes: IVFPQ and HNSW.

### Loading

- INSERT OVERWRITE now supports a new semantic - Dynamic Overwrite. When this semantic is enabled, the ingested data will either create new partitions or overwrite existing partitions that correspond to the new data records. Partitions not involved will not be truncated or deleted. This semantic is especially useful when users want to recover data in specific partitions without specifying the partition names. For more information, see [Dynamic Overwrite](https://docs.starrocks.io/docs/loading/InsertInto/#dynamic-overwrite).
- Optimized the data ingestion with INSERT from FILES to replace Broker Load as the preferred loading method:
  - FILES now supports listing files in remote storage, and providing basic statistics of the files. For more information, see [FILES - list_files_only](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#list_files_only).
  - INSERT now supports matching columns by name, which is especially useful when users load data from numerous columns with identical names. (The default behavior matches columns by their position.) For more information, see [Match column by name](https://docs.starrocks.io/docs/loading/InsertInto/#match-column-by-name).
  - INSERT supports specifying PROPERTIES, aligning with other loading methods. Users can specify `strict_mode`, `max_filter_ratio`, and `timeout` for INSERT operations to control and behavior and quality of the data ingestion. For more information, see [INSERT - PROPERTIES](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#properties).
  - INSERT from FILES supports pushing down the target table schema check to the Scan stage of FILES to infer a more accurate source data schema. For more information, see see [Push down target table schema check](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#push-down-target-table-schema-check).
  - FILES supports unionizing files with different schema. The schema of Parquet and ORC files are unionized based on the column names, and that of CSV files are unionized based on the position (order) of the columns. When there are mismatched columns, users can choose to fill the columns with NULL or return an error by specifying the property `fill_mismatch_column_with`. For more information, see [Union files with different schema](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#union-files-with-different-schema).
  - FILES supports inferring the STRUCT type data from Parquet files. (In earlier versions, STRUCT data is inferred as STRING type.) For more information, see [Infer STRUCT type from Parquet](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#infer-struct-type-from-parquet).
- Supports merging multiple concurrent Stream Load requests into a single transaction and committing data in a batch, thus improving the throughput of real-time data ingestion. It is designed for high-concurrency, small-batch (from KB to tens of MB) real-time loading scenarios. It can reduce the excessive data versions caused by frequent loading operations, resource consumption during Compaction, and IOPS and I/O latency brought by excessive small files.

### Others

- Optimized the graceful exit process of BE and CN by accurately displaying the status of BE or CN nodes during a graceful exit as `SHUTDOWN`.
- Optimized log printing to avoid excessive disk space being occupied.
- Shared-nothing clusters now support backing up and restoring more objects: logical view, external catalog metadata, and partitions created with expression partitioning and list partitioning strategies.
- [Preview] Supports CheckPoint on Follower FE to avoid excessive memory on Leader FE during CheckPoint, thereby improving the stability of Leader FE.

### Behavior Changes

Because the Data Cache instance used in both shared-data architecture and data lake query scenarios is now unified, there will be the following behavior changes after the upgrade to v3.4.0:

- BE configuration item `datacache_disk_path` is now deprecated. The data will be cached under the directory `${storage_root_path}/datacache`. If you want to allocate a dedicated disk for data cache, you can manually point the directory to the directory mentioned above using a symlink.
- Cached data in the shared-data cluster will be automatically migrated to `${storage_root_path}/datacache` and can be re-used after the upgrade.
- The behavior changes of `datacache_disk_size`:

  - When `datacache_disk_size` is `0` (Default), the automatic adjustment of cache capacity is enabled (consistent with the behavior before the upgrade).
  - When `datacache_disk_size` is set to a value greater than `0`, the system will pick a larger value between `datacache_disk_size` and  `starlet_star_cache_disk_size_percent` as the cache capacity.

### Downgrade Notes

- Clusters can be downgraded from v3.4.0 only to v3.3.9 and later.
