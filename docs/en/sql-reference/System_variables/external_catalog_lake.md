---
displayed_sidebar: docs
sidebar_label: "External Catalogs and Data Lake"
sidebar_position: 6
description: "Session variables for external catalog connectors and data lake formats such as Hive, Iceberg, and Hudi."
---

# System Variables - External Catalogs and Data Lake

For how to view and set variables, see the [System variables overview](../System_variable.md).

### avro_use_jni_reader

* **Scope**: Session
* **Description**: Controls whether StarRocks uses the JNI-based Avro reader when scanning Avro data from external catalogs such as Hive. When enabled (`true`), StarRocks uses the JNI reader. When disabled (`false`), StarRocks uses the native Avro reader. This option is mainly used as a compatibility fallback. Because the default value is `false`, StarRocks uses the native Avro reader by default.

  Current notes:
  - The native Avro reader and the JNI reader are now aligned for `CHAR(n)` semantics. See [#73579](https://github.com/StarRocks/starrocks/pull/73579) for the alignment change, so the native and JNI behaviors are currently consistent on this point.
  - The native Avro reader currently supports only `null`, `deflate`, and `snappy`, and does not support other codecs such as `bzip2`. If you need to process a codec that is unsupported by the native reader, manually enable the JNI reader.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v4.1.1

### connector_io_tasks_per_scan_operator

* **Description**: The maximum number of concurrent I/O tasks that can be issued by a scan operator during external table queries. The value is an integer. Currently, StarRocks can adaptively adjust the number of concurrent I/O tasks when querying external tables. This feature is controlled by the variable `enable_connector_adaptive_io_tasks`, which is enabled by default.
* **Default**: 16
* **Data type**: Int
* **Introduced in**: v2.5

### connector_sink_compression_codec

* **Description**: Specifies the compression algorithm used for writing data into Hive tables or Iceberg tables, or exporting data with Files(). This parameter only takes effect in the following situations:
  * The `compression_codec` property does not exist in the Hive tables.
  * The `write.parquet.compression-codec` properties do not exist in the Iceberg tables.
  * The `compression` property is not set for `INSERT INTO FILES`.
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

### enable_connector_adaptive_io_tasks

* **Description**: Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value is `true`. If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.
* **Default**: true
* **Introduced in**: v2.5

### enable_write_hive_external_table

* **Description**: Whether to allow for sinking data to external tables of Hive.
* **Default**: false
* **Introduced in**: v3.2

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

### allow_lake_without_partition_filter

* **Description**: Whether to allow queries on lake tables (Hive, Iceberg, Delta Lake, Paimon, etc.) without a partition filter predicate. When set to `false`, queries that do not contain a valid partition predicate on these tables will be rejected to prevent accidental full-table scans.
* **Scope**: Session
* **Default**: `true`
* **Data type**: Boolean
* **Alias**: `allow_hive_without_partition_filter`

### scan_lake_partition_num_limit

* **Description**: The maximum number of partitions allowed to be scanned for a single lake table (Hive, Iceberg, Delta Lake, Paimon, etc.). When set to `0`, no limit is applied. When exceeded, the query will return an error. Note that for catalog types that enumerate splits incrementally (Iceberg, Delta Lake), the limit check is performed during scan-range dispatch and the query may fail mid-execution rather than being rejected upfront.
* **Scope**: Session
* **Default**: `0` (No limit)
* **Data type**: Int
* **Alias**: `scan_hive_partition_num_limit`

