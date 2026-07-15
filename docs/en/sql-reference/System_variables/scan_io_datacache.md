---
displayed_sidebar: docs
sidebar_label: "Scan, I/O, and Data Cache"
sidebar_position: 5
description: "Session variables for scan concurrency, I/O tasks, data cache, and page cache."
---

# System Variables - Scan, I/O, and Data Cache

For how to view and set variables, see the [System variables overview](../System_variable.md).

import Restricted from '../../_assets/commonMarkdown/_restricted.mdx'

### big_query_log_scan_bytes_threshold

* **Description**: the value is set for testing, if a query needs to scan more than 10GB of data, we treat it as a big query. Users need to set up according to their own scenario.
* **Scope**: Session
* **Default**: `1024L * 1024 * 1024 * 10`
* **Data type**: `long`
* **Mutable**: Yes

### big_query_log_scan_rows_threshold

* **Description**: the value is set for testing, if a query need to scan more than 1 billion rows of data, we treat it as a big query. Users need to set up according to their own scenario.
* **Scope**: Session
* **Default**: `1000000000L`
* **Data type**: `long`
* **Mutable**: Yes

### chunk_size

<Restricted />

* **Description**: _Description pending._
* **Scope**: Session
* **Default**: `4096`
* **Data type**: `int`
* **Mutable**: Yes

### datacache_evict_probability

<Restricted />

* **Description**: Specifies the probability percentage for evicting entries from the data cache.
* **Scope**: Session
* **Default**: `100`
* **Data type**: `int`
* **Mutable**: Yes

### datacache_sharing_work_period

* **Description**: The period of time that Cache Sharing takes effect. After each cluster scaling operation, only the requests within this period of time will try to access the cache data from other nodes if the Cache Sharing feature is enabled.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### enable_collect_table_level_scan_stats

* **Description**: This variable is introduced to solve compatibility issues/ see more details: https://github.com/StarRocks/starrocks/pull/29678
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_connector_deploy_scan_ranges_background

* **Description**: Enables background deployment of scan ranges for connector queries.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_connector_incremental_scan_ranges

* **Description**: Enables incremental scanning of file ranges from external connectors to reduce memory overhead during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_connector_split_io_tasks

* **Description**: Enables splitting of connector I/O tasks to improve query performance and parallelism.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_datacache_async_populate_mode

* **Description**: Whether to populate the data cache in asynchronous mode. By default, the system uses the synchronous mode to populate data cache, that is, populating the cache while querying data.
* **Default**: `true`
* **Introduced in**: v3.2.7

### enable_datacache_io_adaptor

* **Description**: Whether to enable the Data Cache I/O Adaptor. Setting this to `true` enables the feature. When this feature is enabled, the system automatically routes some cache requests to remote storage when the disk I/O load is high, reducing disk pressure.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_datacache_sharing

* **Description**: Whether to enable Cache Sharing. Setting this to `true` enables the feature. Cache Sharing is used to support accessing cache data from other nodes through the network, which can help to reduce performance jitter caused by cache invalidation during cluster scaling. This variable takes effect only when the FE parameter `enable_trace_historical_node` is set to `true`.
* **Default**: true
* **Introduced in**: v3.5.1

### enable_dynamic_prune_scan_range

* **Description**: Enables dynamic pruning of scan ranges during query execution to reduce data scanned.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_evaluate_schema_scan_rule

* **Description**: Enables optimization of schema scan operations through rule-based evaluation during query planning.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_gin_filter

* **Description**: Whether to utilize the [fulltext inverted index](../table_design/indexes/inverted_index.md) during queries.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_hyperscan_vec

* **Description**: Enables vectorized pattern matching using the Hyperscan library for improved query performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

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

### enable_populate_datacache

<Restricted />

* **Description**: Enables automatic population of the data cache during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_scan_datacache

* **Description**: Specifies whether to enable the Data Cache feature. After this feature is enabled, StarRocks caches hot data read from external storage systems into blocks, which accelerates queries and analysis. For more information, see [Data Cache](../data_source/data_cache.md). In versions prior to 3.2, this variable was named as `enable_scan_block_cache`.
* **Default**: true
* **Introduced in**: v2.5

### enable_scan_predicate_expr_reuse

<Restricted />

* **Description**: Enables reuse of predicate expressions during table scan operations to optimize query performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_shared_scan

* **Scope**: Session
* **Description**: Session-level boolean flag intended to request shared scan execution for pipeline queries. When pipeline execution is enabled, the FE will propagate this setting into the fragment execution parameters (`TExecPlanFragmentParams.enable_shared_scan`) so the BE can perform shared scanning (see `.../TFragmentInstanceFactory.java:153-159`). However, the FE currently does not honor user changes: `SessionVariable.isEnableSharedScan()` always returns `false` (see `.../SessionVariable.java:4176-4180`) and the feature has been disabled in FE since later versions due to incompatibility with event-based scheduling. As a result, setting this variable in a session has no effect in current releases.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### io_tasks_per_scan_operator

* **Description**: The number of concurrent I/O tasks that can be issued by a scan operator. Increase this value if you want to access remote storage systems such as HDFS or S3 but the latency is high. However, a larger value causes more memory consumption.
* **Default**: 4
* **Data type**: Int
* **Introduced in**: v2.5

### max_pushdown_conditions_per_column

* **Description**: The maximum number of predicates that can be pushed down for a column.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.
* **Data type**: Int

### max_scan_key_num

* **Description**: The maximum number of scan key segmented by each query.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.

### populate_datacache_mode

* **Description**: Specifies the population behavior of Data Cache when reading data blocks from external storage systems. Valid values:
  * `auto` (default): the system automatically caches data selectively based on the population rule.
  * `always`: Always cache the data.
  * `never`: Never cache the data.
* **Default**: auto
* **Introduced in**: v3.3.2

### runtime_filter_scan_wait_time

<Restricted />

* **Description**: Specifies the maximum time in milliseconds a scan operator waits to receive runtime filters before proceeding.
* **Scope**: Session
* **Default**: `20L`
* **Data type**: `long`
* **Mutable**: Yes

### scan_olap_partition_num_limit

* **Description**: The number of partitions allowed to be scanned for a single table in the execution plan.
* **Default**: 0 (No limit)
* **Introduced in**: v3.3.9

### scan_or_to_union_limit

<Restricted />

* **Description**: Limits the maximum number of OR conditions in a scan that will be converted to a UNION operation.
* **Scope**: Session
* **Default**: `4`
* **Data type**: `int`
* **Mutable**: Yes

### scan_or_to_union_threshold

<Restricted />

* **Description**: Specifies the threshold size for converting OR predicates in scan operations to UNION queries.
* **Scope**: Session
* **Default**: `50000000`
* **Data type**: `long`
* **Mutable**: Yes

### scan_use_query_mem_ratio

* **Description**: Specifies the ratio of total query memory that can be used for table scanning operations.
* **Scope**: Session
* **Default**: `0.3`
* **Data type**: `double`
* **Mutable**: Yes

### skip_local_disk_cache

* **Description**: Session flag that instructs the FE, when building scan ranges, to mark each tablet's internal scan range with `skip_disk_cache`. When set to `true`, `OlapScanNode.addScanRangeLocations()` sets `internalRange.setSkip_disk_cache(true)` on the created `TInternalScanRange` objects so downstream BE scan nodes are told to bypass the local disk cache for that scan. It is applied per-session and is evaluated at plan/scan-range construction time. Use this together with `skip_page_cache` (to control page cache skipping) and data-cache related variables (`enable_scan_datacache` / `enable_populate_datacache`) as appropriate.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### skip_page_cache

* **Scope**: Session
* **Description**: Session-level boolean flag that instructs the planner/frontend to mark scan ranges so backends should bypass the page cache when reading data. When enabled, `OlapScanNode.addScanRangeLocations` sets `TInternalScanRange.skip_page_cache` for each scan range sent to the backend, causing storage reads to skip the OS/page caching layer. Typical use cases: large one-time scans where avoiding page-cache pollution is desired or when a user prefers direct I/O semantics. Do not confuse with `skip_local_disk_cache`, which controls the storage-layer data cache; `fill_data_cache` can also influence caching behavior.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### use_page_cache

* **Description**: Session-scoped boolean that controls whether a query should use the backend page cache. If not explicitly set by FE, the query follows the BE's page-cache policy; if set in the session, FE enforces the session value. The session variable is propagated to the execution layer (e.g., `tResult.setUse_page_cache`) so BE execution honors the decision. Commonly disabled (`false`) for internal/background jobs (statistics collection, hyper queries, online optimize) to avoid polluting the shared page cache with non-user data — see usages in `StatisticsCollectJob`, `HyperQueryJob`, and `OnlineOptimizeJobV2`.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

