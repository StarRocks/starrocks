---
displayed_sidebar: docs
sidebar_label: "External Catalogs and Data Lake"
sidebar_position: 6
description: "Session variables for external catalog connectors and data lake formats such as Hive, Iceberg, and Hudi."
---

# System Variables - External Catalogs and Data Lake

For how to view and set variables, see the [System variables overview](../System_variable.md).

import Restricted from '../../_assets/commonMarkdown/_restricted.mdx'

### allow_lake_without_partition_filter

* **Description**: Whether to allow queries on lake tables (Hive, Iceberg, Delta Lake, Paimon, etc.) without a partition filter predicate. When set to `false`, queries that do not contain a valid partition predicate on these tables will be rejected to prevent accidental full-table scans.
* **Scope**: Session
* **Default**: `true`
* **Data type**: Boolean
* **Alias**: `allow_hive_without_partition_filter`

### always_collect_low_card_dict_on_lake

<Restricted />

* **Description**: Enables automatic collection of low-cardinality dictionary encoding for Lake format tables.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### avro_use_jni_reader

* **Scope**: Session
* **Description**: Controls whether StarRocks uses the JNI-based Avro reader when scanning Avro data from external catalogs such as Hive. When enabled (`true`), StarRocks uses the JNI reader. When disabled (`false`), StarRocks uses the native Avro reader. This option is mainly used as a compatibility fallback. Because the default value is `false`, StarRocks uses the native Avro reader by default.

  Current notes:
  - The native Avro reader and the JNI reader are now aligned for `CHAR(n)` semantics. See [#73579](https://github.com/StarRocks/starrocks/pull/73579) for the alignment change, so the native and JNI behaviors are currently consistent on this point.
  - The native Avro reader currently supports only `null`, `deflate`, and `snappy`, and does not support other codecs such as `bzip2`. If you need to process a codec that is unsupported by the native reader, manually enable the JNI reader.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v4.1.1

### connector_huge_file_size

* **Description**: Specifies the file size threshold for identifying large files in external connector operations.
* **Scope**: Session
* **Default**: `512L * 1024L * 1024L`
* **Data type**: `long`
* **Mutable**: Yes

### connector_incremental_scan_ranges_size

* **Description**: Specifies the maximum number of scan ranges to process in each incremental scan batch for external connectors.
* **Scope**: Session
* **Default**: `500`
* **Data type**: `int`
* **Mutable**: Yes

### connector_io_tasks_per_scan_operator

* **Description**: The maximum number of concurrent I/O tasks that can be issued by a scan operator during external table queries. The value is an integer. Currently, StarRocks can adaptively adjust the number of concurrent I/O tasks when querying external tables. This feature is controlled by the variable `enable_connector_adaptive_io_tasks`, which is enabled by default.
* **Default**: 16
* **Data type**: Int
* **Introduced in**: v2.5

### connector_io_tasks_slow_io_latency_ms

<Restricted />

* **Description**: Specifies the latency threshold in milliseconds for identifying slow I/O operations in connector adaptive task scheduling.
* **Scope**: Session
* **Default**: `50`
* **Data type**: `int`
* **Mutable**: Yes

### connector_max_split_size

* **Description**: Limits the maximum size of data splits when reading from external connectors.
* **Scope**: Session
* **Default**: `64L * 1024L * 1024L`
* **Data type**: `long`
* **Mutable**: Yes

### connector_remote_file_async_queue_size

<Restricted />

* **Description**: Specifies the maximum number of pending asynchronous tasks in the queue for remote file access through connectors.
* **Scope**: Session
* **Default**: `1000`
* **Data type**: `int`
* **Mutable**: Yes

### connector_remote_file_async_task_size

<Restricted />

* **Description**: Specifies the number of concurrent asynchronous tasks for reading remote files through connectors.
* **Scope**: Session
* **Default**: `4`
* **Data type**: `int`
* **Mutable**: Yes

### connector_scan_use_query_mem_ratio

* **Description**: Controls the ratio of query memory that connector scans can use during execution.
* **Scope**: Session
* **Default**: `0.3`
* **Data type**: `double`
* **Mutable**: Yes

### connector_sink_compression_codec

* **Description**: Specifies the compression algorithm used for writing data into Hive tables or Iceberg tables, or exporting data with Files(). This parameter only takes effect in the following situations:
  * The `compression_codec` property does not exist in the Hive tables.
  * The `write.parquet.compression-codec` properties do not exist in the Iceberg tables.
  * The `compression` property is not set for `INSERT INTO FILES`.
* **Valid values**: `uncompressed`, `snappy`, `lz4`, `zstd`, and `gzip`.
* **Default**: uncompressed
* **Data type**: String
* **Introduced in**: v3.2.3

### connector_sink_shuffle_mode

* **Description**: Specifies the shuffle strategy for connector sink operations, with adaptive optimization as the default mode.
* **Scope**: Session
* **Default**: `ConnectorSinkShuffleMode.AUTO.modeName()`
* **Data type**: `String`
* **Mutable**: Yes

### connector_sink_shuffle_partition_node_ratio

<Restricted />

* **Description**: Determines the threshold ratio of partitions to backend nodes for enabling shuffle during connector sink operations.
* **Scope**: Session
* **Default**: `2.0`
* **Data type**: `double`
* **Mutable**: Yes

### connector_sink_shuffle_partition_threshold

<Restricted />

* **Description**: Specifies the partition count threshold that triggers adaptive global shuffle for connector sink operations.
* **Scope**: Session
* **Default**: `500`
* **Data type**: `long`
* **Mutable**: Yes

### connector_sink_sort_scope

* **Description**: Specifies the scope level for sorting data in connector sink operations.
* **Scope**: Session
* **Default**: `ConnectorSinkSortScope.FILE.scopeName()`
* **Data type**: `String`
* **Mutable**: Yes

### connector_sink_spill_mem_limit_threshold

<Restricted />

* **Description**: Controls the memory threshold ratio for triggering spill-to-disk during connector sink operations.
* **Scope**: Session
* **Default**: `0.5`
* **Data type**: `double`
* **Mutable**: Yes

### connector_sink_target_max_file_size

* **Description**: Specifies the maximum size of target file for writing data into Hive tables or Iceberg tables, or exporting data with Files(). The limit is not exact and is applied on a best-effort basis.
* **Unit**: Bytes
* **Default**: 1073741824
* **Data type**: Long
* **Introduced in**: v3.3.0

### enable_connector_adaptive_io_tasks

* **Description**: Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value is `true`. If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.
* **Default**: true
* **Introduced in**: v2.5

### enable_delta_lake_column_statistics

* **Description**: Enables the use of column statistics from Delta Lake table metadata for query optimization.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_hive_column_stats

* **Description**: Enables the use of column statistics from Hive tables for query optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_hive_metadata_cache_with_insert

* **Description**: Enables caching of Hive table metadata during INSERT operations.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_iceberg_column_statistics

* **Description**: Enables the use of column statistics from Iceberg table metadata for query optimization.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_iceberg_compaction_with_row_lineage

<Restricted />

* **Description**: Enables tracking of row lineage information during Iceberg table compaction operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_iceberg_identity_column_optimize

* **Description**: Enables optimization of identity column transformations when querying Iceberg tables.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_iceberg_sink_global_shuffle

<Restricted />

* **Description**: Deprecated: Use connector_sink_shuffle_mode instead
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_paimon_column_statistics

* **Description**: Enables the use of column statistics from Paimon tables for query optimization.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_prune_iceberg_manifest

* **Description**: Enables pruning of Iceberg manifest files to reduce the amount of metadata scanned during query planning.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_read_iceberg_equality_delete_with_partition_evolution

* **Description**: Enables reading Iceberg tables with equality delete files when partition evolution has occurred.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_read_iceberg_puffin_ndv

* **Description**: Enables reading NDV (number of distinct values) statistics from Iceberg Puffin files for query optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_rewrite_simple_agg_to_hdfs_scan

* **Description**: Enables rewriting simple aggregation queries to scan HDFS metadata instead of table data for optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_write_hive_external_table

* **Description**: Whether to allow for sinking data to external tables of Hive.
* **Default**: false
* **Introduced in**: v3.2

### hdfs_backend_selector_cache_replica_num

* **Description**: Specifies the number of replicas to cache when selecting HDFS backends for query execution.
* **Scope**: Session
* **Default**: `1`
* **Data type**: `int`
* **Mutable**: Yes

### hdfs_backend_selector_force_rebalance

<Restricted />

* **Description**: Enables forced rebalancing of backend selection for HDFS data access.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### hdfs_backend_selector_hash_algorithm

<Restricted />

* **Description**: Specifies the hash algorithm used to select HDFS backends for query execution.
* **Scope**: Session
* **Default**: `"consistent"`
* **Data type**: `String`
* **Mutable**: Yes

### hdfs_backend_selector_scan_range_shuffle

<Restricted />

* **Description**: Enables random shuffling of scan range assignments when selecting HDFS backend nodes for query execution.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### hive_partition_stats_sample_size

* **Description**: Specifies the sample size for collecting partition statistics from Hive tables.
* **Scope**: Session
* **Default**: `3000`
* **Data type**: `int`
* **Mutable**: Yes

### hive_temp_staging_dir

* **Description**: Specifies the temporary staging directory path used for Hive data operations.
* **Scope**: Session
* **Default**: `"/tmp/starrocks"`
* **Data type**: `String`
* **Mutable**: Yes

### hudi_mor_force_jni_reader

* **Description**: Enables the JNI reader for Hudi Merge-On-Read table scans.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### lake_bucket_assign_mode

* **Description**: The bucket assignment mode for queries against tables in data lakes. This variable controls how buckets are distributed among worker nodes when bucket-aware execution takes effect during query execution. Valid values:
  * `balance`: Distributes buckets evenly across worker nodes to achieve balanced workload and better performance.
  * `elastic`: Uses consistent hashing to assign buckets to worker nodes, which can provide better load distribution in elastic environments.
* **Default**: balance
* **Data type**: String
* **Introduced in**: v4.0

### metadata_collect_query_timeout

* **Description**: The timeout duration for Iceberg Catalog metadata collection queries.
* **Unit**: Second
* **Default**: 60
* **Introduced in**: v3.3.3

### orc_use_column_names

* **Description**: Used to specify how columns are matched when StarRocks reads ORC files from Hive. The default value is `false`, which means columns in ORC files are read based on their ordinal positions in the Hive table definition. If this variable is set to `true`, columns are read based on their names.
* **Default**: false
* **Introduced in**: v3.1.10

### paimon_force_jni_reader

* **Description**: Enables the use of JNI reader for reading Paimon table data.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### scan_lake_partition_num_limit

* **Description**: The maximum number of partitions allowed to be scanned for a single lake table (Hive, Iceberg, Delta Lake, Paimon, etc.). When set to `0`, no limit is applied. When exceeded, the query will return an error. Note that for catalog types that enumerate splits incrementally (Iceberg, Delta Lake), the limit check is performed during scan-range dispatch and the query may fail mid-execution rather than being rejected upfront.
* **Scope**: Session
* **Default**: `0` (No limit)
* **Data type**: Int
* **Alias**: `scan_hive_partition_num_limit`

