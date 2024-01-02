---
displayed_sidebar: "English"
---

# StarRocks version 3.2

## 3.2.2

Release date: December 30, 2023

### Bug Fixes

Fixed the following issue:

- When StarRocks is upgraded from v3.1.2 or earlier to v3.2, FEs may fail to restart. [#38172](https://github.com/StarRocks/starrocks/pull/38172)

## 3.2.1

Release date: December 21, 2023

### New Features

#### Data Lake Analytics

- Supports reading [Hive Catalog](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) tables and file external tables in Avro, SequenceFile, and RCFile formats through Java Native Interface (JNI).

#### Materialized View

- Added a view `object_dependencies` to the database `sys`. It contains the lineage information of asynchronous materialized views. [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- Supports creating synchronous materialized views with the WHERE clause.
- Supports partition-level incremental refresh for asynchronous materialized views created upon Iceberg catalogs.
- [Preview] Supports creating asynchronous materialized views based on tables in a Paimon catalog with partition-level refresh.

#### Query and SQL functions

- Supports the prepared statement. It allows better performance for processing high-concurrency point lookup queries. It also prevents SQL injection effectively.
- Supports the following Bitmap functions: [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/), [bitmap_from_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/), and [bitmap_to_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/).
- Supports the Array function [array_unique_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_unique_agg/).

#### Monitoring and alerts

- Added a new metric `max_tablet_rowset_num` for setting the maximum allowed number of rowsets. This metric helps detect possible compaction issues and thus reduces the occurrences of the error "too many versions". [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### Parameter changes

- A new BE configuration item `enable_stream_load_verbose_log` is added. The default value is `false`. With this parameter set to `true`, StarRocks can record the HTTP requests and responses for Stream Load jobs, making troubleshooting easier. [#36113](https://github.com/StarRocks/starrocks/pull/36113)

### Improvements

- Upgraded the default GC algorithm in JDK8 to G1. [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- A new value option `GROUP_CONCAT_LEGACY` is added to the session variable [sql_mode](https://docs.starrocks.io/docs/reference/System_variable/#sql_mode) to provide compatibility with the implementation logic of the [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) function in versions earlier than v2.5. [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- The authentication information `aws.s3.access_key` and `aws.s3.access_secret` for [AWS S3 in Broker Load jobs](https://docs.starrocks.io/docs/loading/s3/) are hidden in audit logs. [#36571](https://github.com/StarRocks/starrocks/pull/36571)
- The `be_tablets` view in the `information_schema` database provides a new field `INDEX_DISK`, which records the disk usage (measured in bytes) of persistent indexes. [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement provides a new field `OtherMsg`, which shows information about the last failed task. [#35806](https://github.com/StarRocks/starrocks/pull/35806)

### Bug Fixes

Fixed the following issues:

- The BEs crash if users create persistent indexes in the event of data corruption.[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- The [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) function occasionally causes the BEs to crash. [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- After the DISTINCT window operator pushdown feature is enabled, errors are reported if SELECT DISTINCT operations are performed on the complex expressions of the columns computed by window functions.  [#36357](https://github.com/StarRocks/starrocks/pull/36357)
- Some S3-compatible object storage returns duplicate files, causing the BEs to crash. [#36103](https://github.com/StarRocks/starrocks/pull/36103)

## 3.2.0 

Release date: December 1, 2023

### New Features

#### Shared-data cluster

- Supports persisting indexes of [Primary Key tables](../table_design/table_types/primary_key_table.md) to local disks.
- Supports even distribution of Data Cache among multiple local disks.

#### Materialized View

**Asynchronous materialized view**

- The Query Dump file can include information of asynchronous materialized views.
- The Spill to Disk feature is enabled by default for the refresh tasks of asynchronous materialized views, reducing memory consumption.

#### Data Lake Analytics

- Supports creating and dropping databases and managed tables in [Hive catalogs](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/), and supports exporting data to Hive's managed tables using INSERT or INSERT OVERWRITE.
- Supports [Unified Catalog](https://docs.starrocks.io/docs/data_source/catalog/unified_catalog/), with which users can access different table formats (Hive, Iceberg, Hudi, and Delta Lake) that share a common metastore like Hive metastore or AWS Glue.
- Supports collecting statistics of Hive and Iceberg tables using ANALYZE TABLE, and storing the statistics in StarRocks, thus facilitating optimization of query plans and accelerating subsequent queries.
- Supports Information Schema for external tables, providing additional convenience for interactions between external systems (such as BI tools) and StarRocks.

#### Storage engine, data ingestion, and export

- Added the following features of loading with the table function [FILES()](../sql-reference/sql-functions/table-functions/files.md):
  - Loading Parquet and ORC format data from Azure or GCP.
  - Extracting the value of a key/value pair from the file path as the value of a column using the parameter `columns_from_path`.
  - Loading complex data types including ARRAY, JSON, MAP, and STRUCT.
- Supports unloading data from StarRocks to Parquet-formatted files stored in AWS S3 or HDFS by using INSERT INTO FILES. For detailed instructions, see [Unload data using INSERT INTO FILES](../unloading/unload_using_insert_into_files.md).
- Supports [manual optimization of table structure and data distribution strategy](../table_design/Data_distribution.md#optimize-data-distribution-after-table-creation-since-32) used in an existing table to optimize the query and loading performance. You can set a new bucket key, bucket number, or sort key for a table. You can also set a different bucket number for specific partitions.
- Supports continuous data loading from [AWS S3](https://docs.starrocks.io/docs/loading/s3/#use-pipe) or [HDFS](https://docs.starrocks.io/docs/loading/hdfs_load/#use-pipe) using the PIPE method.
  - When PIPE detects new  or modifications in a remote storage directory, it can automatically load the new or modified data into the destination table in StarRocks. While loading data, PIPE automatically splits a large loading task into smaller, serialized tasks, enhancing stability in large-scale data ingestion scenarios and reducing the cost of error retries.

#### Query

- Supports [HTTP SQL API](../reference/HTTP_API/SQL.md), enabling users to access StarRocks data via HTTP and execute SELECT, SHOW, EXPLAIN, or KILL operations.
- Supports Runtime Profile and text-based Profile analysis commands (SHOW PROFILELIST, ANALYZE PROFILE, EXPLAIN ANALYZE) to allow users to directly analyze profiles via MySQL clients, facilitating bottleneck identification and discovery of optimization opportunities.

#### SQL reference

Added the following functions:

- String functions: substring_index, url_extract_parameter, url_encode, url_decode, and translate
- Date functions: dayofweek_iso, week_iso, quarters_add, quarters_sub, milliseconds_add, milliseconds_sub, date_diff, jodatime_format, str_to_jodatime, to_iso8601, to_tera_date, and to_tera_timestamp
- Pattern matching function: regexp_extract_all
- hash function: xx_hash3_64
- Aggregate functions: approx_top_k
- Window functions: cume_dist, percent_rank and session_number
- Utility functions: dict_mapping and get_query_profile

#### Privileges and security

StarRocks supports access control through [Apache Ranger](../administration/ranger_plugin.md), providing a higher level of data security and allowing the reuse of existing services of external data sources. After integrating with Apache Ranger, StarRocks enables the following access control methods:

- When accessing internal tables, external tables, or other objects in StarRocks, access control can be enforced based on the access policies configured for the StarRocks Service in Ranger.
- When accessing an external catalog, access control can also leverage the corresponding Ranger service of the original data source (such as Hive Service) to control access (currently, access control for exporting data to Hive is not yet supported).

For more information, see [Manage permissions with Apache Ranger](../administration/ranger_plugin.md).

### Improvements

#### Data Lake Analytics

- Optimized ORC Reader:
  - Optimized the ORC Column Reader, resulting in nearly a two-fold performance improvement for VARCHAR and CHAR data reading.
  - Optimized the decompression performance of ORC files in Zlib compression format.
- Optimized Parquet Reader:
  - Supports adaptive I/O merging, allowing adaptive merging of columns with and without predicates based on filtering effects, thus reducing I/O.
  - Optimized Dict Filter for faster predicate rewriting. Supports STRUCT sub-columns, and on-demand dictionary column decoding.
  - Optimized Dict Decode performance.
  - Optimized late materialization performance.
  - Supports caching file footers to avoid repeated computation overhead.
  - Supports decompression of Parquet files in lzo compression format.
- Optimized CSV Reader:
  - Optimized the Reader performance.
  - Supports decompression of CSV files in Snappy and lzo compression formats.
- Optimized the performance of the count calculation.
- Optimized Iceberg Catalog capabilities:
  - Supports collecting column statistics from Manifest files to accelerate queries.
  - Supports collecting NDV (number of distinct values) from Puffin files to accelerate queries.
  - Supports partition pruning.
  - Reduced Iceberg metadata memory consumption to enhance stability in scenarios with large metadata volume or high query concurrency.

#### Materialized View

**Asynchronous materialized view**

- Supports automatic refresh for an asynchronous materialized view created upon views or materialized views when schema changes occur on the views, materialized views, or their base tables.
- Data consistency:
  - Added the property `query_rewrite_consistency` for asynchronous materialized view creation. This property defines the query rewrite rules based on the consistency check.
  - Add the property `force_external_table_query_rewrite` for external catalog-based asynchronous materialized view creation. This property defines whether to allow force query rewrite for asynchronous materialized views created upon external catalogs.
  -  For detailed information, see [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md).
- Added a consistency check for materialized views' partitioning key.
  -  When users create an asynchronous materialized view with window functions that include a PARTITION BY expression, the partitioning column of the window function must match that of the materialized view.

#### Storage engine, data ingestion, and export

- Optimized the persistent index for Primary Key tables by improving memory usage logic while reducing I/O read and write amplification. [#24875](https://github.com/StarRocks/starrocks/pull/24875)  [#27577](https://github.com/StarRocks/starrocks/pull/27577)  [#28769](https://github.com/StarRocks/starrocks/pull/28769)
- Supports data re-distribution across local disks for Primary Key tables.
- Partitioned tables support automatic cooldown based on the partition time range and cooldown time. Compared to the original cooldown logic, it is more convenient to perform hot and cold data management on the partition level. For more information, see [Specify initial storage medium, automatic storage cooldown time, replica number](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#specify-initial-storage-medium-automatic-storage-cooldown-time-replica-number).
- The Publish phase of a load job that writes data into a Primary Key table is changed from asynchronous mode to synchronous mode. As such, the data loaded can be queried immediately after the load job finishes. For more information, see [enable_sync_publish](../administration/FE_configuration.md#enable_sync_publish)。
- Supports Fast Schema Evolution, which is controlled by the table property [`fast_schema_evolution`](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#set-fast-schema-evolution). After this feature is enabled, the execution efficiency of adding or dropping columns is significantly improved. This mode is disabled by default (Default value is `false`). You cannot modify this property for existing tables using ALTER TABLE.
- [Supports dynamically adjusting the number of tablets to create](../table_design/Data_distribution.md#set-the-number-of-buckets) according to cluster information and the size of the data for **Duplicate Key** tables created with the Radom Bucketing strategy.

#### Query

- Optimized StarRocks' compatibility with Metabase and Superset. Supports integrating them with external catalogs.

#### SQL Reference

- [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) supports the keyword DISTINCT.
- INSERT, UPDATE, and DELETE operations now support `SET_VAR`. [#35283](https://github.com/StarRocks/starrocks/pull/35283)

#### Others

- Added the session variable `large_decimal_underlying_type = "panic"|"double"|"decimal"` to set the rules to deal with DECIMAL type overflow. `panic` indicates returning an error immediately, `double` indicates converting the data to DOUBLE type, and `decimal` indicates converting the data to DECIMAL(38,s).

### Developer tools

- Supports Trace Query Profile for asynchronous materialized views, which can be used to analyze its transparent rewrite.

### Compatibility Changes

#### Behavior Changes

To be updated.

#### Parameters

##### FE Configuration

- Added the following FE configuration items:
  - `catalog_metadata_cache_size`
  - `enable_backup_materialized_view`
  - `enable_colocate_mv_index` 
  - `enable_fast_schema_evolution`
  - `json_file_size_limit`
  - `lake_enable_ingest_slowdown` 
  - `lake_ingest_slowdown_threshold` 
  - `lake_ingest_slowdown_ratio`
  - `lake_compaction_score_upper_bound`
  - `mv_auto_analyze_async`
  - `primary_key_disk_schedule_time`
  - `statistic_auto_collect_small_table_rows`
  - `stream_load_task_keep_max_num`
  - `stream_load_task_keep_max_second`
- Removed FE configuration item `enable_pipeline_load`.
- Default value modifications:
  - The default value of `enable_sync_publish` is changed from `false` to `true`.
  - The default value of `enable_persistent_index_by_default` is changed from `false` to `true`.

##### BE Configuration

- Data Cache-related configuration changes.
  - Added `datacache_enable` to replace `block_cache_enable`.
  - Added `datacache_mem_size` to replace `block_cache_mem_size`.
  - Added `datacache_disk_size` to replace `block_cache_disk_size`.
  - Added `datacache_disk_path` to replace `block_cache_disk_path`.
  - Added `datacache_meta_path` to replace `block_cache_meta_path`.
  - Added `datacache_block_size` to replace `block_cache_block_size`.
  - Added `datacache_checksum_enable` to replace `block_cache_checksum_enable`.
  - Added `datacache_direct_io_enable` to replace `block_cache_direct_io_enable`.
  - Added `datacache_max_concurrent_inserts` to replace `block_cache_max_concurrent_inserts`.
  - Added `datacache_max_flying_memory_mb`.
  - Added `datacache_engine` to replace `block_cache_engine`.
  - Removed `block_cache_max_parcel_memory_mb`.
  - Removed `block_cache_report_stats`.
  - Removed `block_cache_lru_insertion_point`.

  After renaming Block Cache to Data Cache, StarRocks has introduced a new set of BE parameters prefixed with `datacache` to replace the original parameters prefixed with `block_cache`. After upgrade to v3.2, the original parameters will still be effective. Once enabled, the new parameters will override the original ones. The mixed usage of new and original parameters is not supported, as it may result in some configurations not taking effect. In the future, StarRocks plans to deprecate the original parameters with the `block_cache` prefix, so we recommend you use the new parameters with the `datacache` prefix.

- Added the following BE configuration items:
  - `spill_max_dir_bytes_ratio`
  - `streaming_agg_limited_memory_size`
  - `streaming_agg_chunk_buffer_size`
- Removed the following BE configuration items:
  - Dynamic parameter `tc_use_memory_min`
  - Dynamic parameter `tc_free_memory_rate`
  - Dynamic parameter `tc_gc_period`
  - Static parameter `tc_max_total_thread_cache_byte`
- Default value modifications:
  - The default value of `disable_column_pool` is changed from `false` to `true`.
  - The default value of `txn_commit_rpc_timeout_ms` is changed from `20000` to `60000`.
  - The default value of `thrift_port` is changed from `9060` to `0`.
  - The default value of `enable_load_colocate_mv` is changed from `false` to `true`.
  - The default value of `enable_pindex_minor_compaction` is changed from `false` to `true`.

#### System Variables

- Added the following session variables:
  - `enable_per_bucket_optmize`
  - `enable_write_hive_external_table`
  - `hive_temp_staging_dir`
  - `spill_revocable_max_bytes`
  - `thrift_plan_protocol`
- Removed the following session variables:
  - `enable_pipeline_query_statistic`
  - `enable_deliver_batch_fragments`
- Renamed the following session variables:
  - `enable_scan_block_cache` is renamed as `enable_scan_datacache`.
  - `enable_populate_block_cache` is renamed as `enable_populate_datacache`.

#### Reserved Keywords

Added reserved keywords `OPTIMIZE` and `PREPARE`.

### Bug Fixes

Fixed the following issues:

- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Schema Change may fail if it takes an excessively long period of time, because the specified tablet version is handled by garbage collection. [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- Failed to access the Parquet files in MinIO via file external tables. [#29873] (https://github.com/StarRocks/starrocks/pull/29873)
- The ARRAY, MAP, and STRUCT type columns are not correctly displayed in  `information_schema.columns`. [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- An error is reported if specific path formats are used during data loading via Broker Load: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`. [#32720](https://github.com/StarRocks/starrocks/pull/32720)
- `DATA_TYPE` and `COLUMN_TYPE` for BINARY or VARBINARY data types are displayed as `unknown` in the `information_schema.columns` view. [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- Complex queries that involve many unions, expressions, and SELECT columns can result in a sudden surge in the bandwidth or CPU usage within an FE node.
- The refresh of asynchronous materialized view may occasionally encounter deadlock. [#35736](https://github.com/StarRocks/starrocks/pull/35736)

### Upgrade Notes

- Optimization on **Random Bucketing** is disabled by default. To enable it, you need to add the property `bucket_size` when creating tables. This allows the system to dynamically adjust the number of tablets based on cluster information and the size of loaded data. Please note that once this optimization is enabled, if you need to roll back your cluster to v3.1 or earlier, you must delete tables with this optimization enabled and manually execute a metadata checkpoint (by executing `ALTER SYSTEM CREATE IMAGE`). Otherwise, the rollback will fail.
- Starting from v3.2.0, StarRocks has disabled non-Pipeline queries. Therefore, before upgrading your cluster to v3.2, you need to globally enable the Pipeline engine (by adding the configuration `enable_pipeline_engine=true` in the FE configuration file **fe.conf**). Failure to do so will result in errors for non-Pipeline queries.
