---
displayed_sidebar: docs
sidebar_label: "Shared-data, Data Lake, and Others"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration - Shared-data, Data Lake, and Others

<BEConfigMethod />

<CNConfigMethod />

## View BE configuration items

You can view the BE configuration items using the following command:

```SQL
SELECT * FROM information_schema.be_configs [WHERE NAME LIKE "%<name_pattern>%"]
```

## Configure BE parameters

<PostBEConfig />

<StaticBEConfigNote />

---

This topic introduces the following types of BE configurations:
- [Shared-data](#shared-data)
- [Data Lake](#data-lake)
- [Other](#other)

## Shared-data

### cloud_native_pk_index_rebuild_files_threshold

- Default: 50
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segment files that need to be rebuilt in cloud-native Primary Key index. If the number of files that need to be rebuilt during index recovery exceeds this threshold, StarRocks will flush the in-memory MemTable immediately to reduce the number of segments that must be replayed. Set to `0` to disable this early-flush strategy.
- Introduced in: -

### cloud_native_pk_index_rebuild_rows_threshold

- Default: 10000000
- Type: Long
- Unit: Rows
- Is mutable: Yes
- Description: The maximum number of rows that need to be rebuilt in cloud-native Primary Key index. If the number of rows that need to be rebuilt during index recovery exceeds this threshold, StarRocks will flush the in-memory MemTable immediately to reduce the rebuild overhead. Set to `0` to disable this early-flush strategy. Works in conjunction with `cloud_native_pk_index_rebuild_files_threshold`; a flush is triggered if either threshold is exceeded.
- Introduced in: -

### download_buffer_size

- Default: 4194304
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Size (in bytes) of the in-memory copy buffer used when downloading snapshot files. SnapshotLoader::download passes this value to fs::copy as the per-transfer chunk size when reading from the remote sequential file into the local writable file. Larger values can improve throughput on high-bandwidth links by reducing syscall/IO overhead; smaller values reduce peak memory use per active transfer. Note: this parameter controls buffer size per stream, not the number of download threads—total memory consumption = download_buffer_size * number_of_concurrent_downloads.
- Introduced in: v3.2.13

### graceful_exit_wait_for_frontend_heartbeat

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Determines whether to await at least one frontend heartbeat response indicating SHUTDOWN status before completing graceful exit. When enabled, the graceful shutdown process remains active until a SHUTDOWN confirmation is responded via heartbeat RPC, ensuring the frontend has sufficient time to detect the termination state between two regular heartbeat intervals.
- Introduced in: v3.4.5

### lake_compaction_stream_buffer_size_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The reader's remote I/O buffer size for cloud-native table compaction in a shared-data cluster. The default value is 1MB. You can increase this value to accelerate compaction process.
- Introduced in: v3.2.3

### lake_pk_compaction_max_input_rowsets

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of input rowsets allowed in a Primary Key table compaction task in a shared-data cluster. The default value of this parameter is changed from `5` to `1000` since v3.2.4 and v3.1.10, and to `500` since v3.3.1 and v3.2.9. After the Sized-tiered Compaction policy is enabled for Primary Key tables (by setting `enable_pk_size_tiered_compaction_strategy` to `true`), StarRocks does not need to limit the number of rowsets for each compaction to reduce write amplification. Therefore, the default value of this parameter is increased.
- Introduced in: v3.1.8, v3.2.3

### loop_count_wait_fragments_finish

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of loops to be waited when the BE/CN process exits. Each loop is a fixed interval of 10 seconds. You can set it to `0` to disable the loop wait. From v3.4 onwards, this item is changed to mutable and its default value is changed from `0` to `2`.
- Introduced in: v2.5

### max_client_cache_size_per_host

- Default: 10
- Type: Int
- Unit: entries (cached client instances) per host
- Is mutable: No
- Description: The maximum number of cached client instances retained for each remote host by BE-wide client caches. This single setting is used when creating BackendServiceClientCache, FrontendServiceClientCache, and BrokerServiceClientCache during ExecEnv initialization, so it limits the number of client stubs/connections kept per host across those caches. Raising this value reduces reconnects and stub creation overhead at the cost of increased memory and file-descriptor usage; lowering it saves resources but may increase connection churn. The value is read at startup and cannot be changed at runtime. Currently one shared setting controls all client cache types; separate per-cache configuration may be introduced later.
- Introduced in: v3.2.0

### starlet_filesystem_instance_cache_capacity

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The cache capacity of starlet filesystem instances.
- Introduced in: v3.2.16, v3.3.11, v3.4.1

### starlet_filesystem_instance_cache_ttl_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The cache expiration time of starlet filesystem instances.
- Introduced in: v3.3.15, 3.4.5

### starlet_port

- Default: 9070
- Type: Int
- Unit: -
- Is mutable: No
- Description: An extra agent service port for BE and CN.
- Introduced in: -

### starlet_star_cache_disk_size_percent

- Default: 80
- Type: Int
- Unit: -
- Is mutable: No
- Description: The percentage of disk capacity that Data Cache can use at most in a shared-data cluster. Only takes effect when `datacache_unified_instance_enable` is `false`.
- Introduced in: v3.1

### starlet_use_star_cache

- Default: false in v3.1 and true from v3.2.3
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Cache in a shared-data cluster. `true` indicates enabling this feature and `false` indicates disabling it. The default value is set from `false` to `true` from v3.2.3 onwards.
- Introduced in: v3.1

### starlet_write_file_with_tag

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: In a shared-data cluster, whether to tag files written to object storage with object storage tags for convenient custom file management.
- Introduced in: v3.5.3

### table_schema_service_max_retries

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of retries for Table Schema Service requests.
- Introduced in: v4.1

## Data Lake

### datacache_block_buffer_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Block Buffer to optimize Data Cache efficiency. When Block Buffer is enabled, the system reads the Block data from the Data Cache and caches it in a temporary buffer, thus reducing the extra overhead caused by frequent cache reads.
- Introduced in: v3.2.0

### datacache_disk_adjust_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval of Data Cache automatic capacity scaling. At regular intervals, the system checks the cache disk usage, and triggers Automatic Scaling when necessary.
- Introduced in: v3.3.0

### datacache_disk_idle_seconds_for_expansion

- Default: 7200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum wait time for Data Cache automatic expansion. Automatic scaling up is triggered only if the disk usage remains below `datacache_disk_low_level` for longer than this duration.
- Introduced in: v3.3.0

### datacache_disk_size

- Default: 0
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The maximum amount of data that can be cached on a single disk. You can set it as a percentage (for example, `80%`) or a physical limit (for example, `2T`, `500G`). For example, if you use two disks and set the value of the `datacache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached on these two disks. The default value is `0`, which indicates that only memory is used to cache data.
- Introduced in: -

### datacache_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Data Cache. `true` indicates Data Cache is enabled, and `false` indicates Data Cache is disabled. The default value is changed to `true` from v3.3.
- Introduced in: -

### datacache_eviction_policy

- Default: slru
- Type: String
- Unit: -
- Is mutable: No
- Description: The eviction policy of Data Cache. Valid values: `lru` (least recently used) and `slru` (Segmented LRU).
- Introduced in: v3.4.0

### datacache_inline_item_count_limit

- Default: 130172
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of inline cache items in Data Cache. For some particularly small cache blocks, Data Cache stores them in `inline` mode, which caches the block data and metadata together in memory.
- Introduced in: v3.4.0

### datacache_mem_size

- Default: 0
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The maximum amount of data that can be cached in memory. You can set it as a percentage (for example, `10%`) or a physical limit (for example, `10G`, `21474836480`).
- Introduced in: -

### datacache_min_disk_quota_for_adjustment

- Default: 10737418240
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum effective capacity for Data Cache Automatic Scaling. If the system tries to adjust the cache capacity to less than this value, the cache capacity will be directly set to `0` to prevent suboptimal performance caused by frequent cache fills and evictions due to insufficient cache capacity.
- Introduced in: v3.3.0

### disk_high_level

- Default: 90
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The upper limit of disk usage (in percentage) that triggers the automatic scaling up of the cache capacity. When the disk usage exceeds this value, the system automatically evicts cache data from the Data Cache. From v3.4.0 onwards, the default value is changed from `80` to `90`. This item is renamed from `datacache_disk_high_level` to `disk_high_level` from v4.0 onwards.
- Introduced in: v3.3.0

### disk_low_level

- Default: 60
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The lower limit of disk usage (in percentage) that triggers the automatic scaling down of the cache capacity. When the disk usage remains below this value for the period specified in `datacache_disk_idle_seconds_for_expansion`, and the space allocated for Data Cache is fully utilized, the system will automatically expand the cache capacity by increasing the upper limit. This item is renamed from `datacache_disk_low_level` to `disk_low_level` from v4.0 onwards.
- Introduced in: v3.3.0

### disk_safe_level

- Default: 80
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The safe level of disk usage (in percentage) for Data Cache. When Data Cache performs automatic scaling, the system adjusts the cache capacity with the goal of maintaining disk usage as close to this value as possible. From v3.4.0 onwards, the default value is changed from `70` to `80`. This item is renamed from `datacache_disk_safe_level` to `disk_safe_level` from v4.0 onwards.
- Introduced in: v3.3.0

### enable_connector_sink_spill

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Spilling for writes to external tables. Enabling this feature prevents the generation of a large number of small files as a result of writing to an external table when memory is insufficient. Currently, this feature only supports writing to Iceberg tables.
- Introduced in: v4.0.0

### enable_datacache_disk_auto_adjust

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Automatic Scaling for Data Cache disk capacity. When it is enabled, the system dynamically adjusts the cache capacity based on the current disk usage rate. This item is renamed from `datacache_auto_adjust_enable` to `enable_datacache_disk_auto_adjust` from v4.0 onwards.
- Introduced in: v3.3.0

### datacache_unified_instance_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use a unified Data Cache instance to manage data caching for both internal catalog and external catalog in a shared-data cluster.
- Introduced in: v3.4.0

### jdbc_connection_idle_timeout_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which an idle connection in the JDBC connection pool expires. If the connection idle time in the JDBC connection pool exceeds this value, the connection pool closes idle connections beyond the number specified in the configuration item `jdbc_minimum_idle_connections`.
- Introduced in: -

### jdbc_connection_pool_size

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The JDBC connection pool size. On each BE node, queries that access the external table with the same `jdbc_url` share the same connection pool.
- Introduced in: -

### jdbc_minimum_idle_connections

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of idle connections in the JDBC connection pool.
- Introduced in: -

### jdbc_connection_max_lifetime_ms

- Default: 300000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: Maximum lifetime of a connection in the JDBC connection pool. Connections are recycled before this timeout to prevent stale connections. Minimum allowed value is 30000 (30 seconds).
- Introduced in: -

### jdbc_connection_keepalive_time_ms

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: Keepalive interval for idle JDBC connections. Idle connections are tested at this interval to detect stale connections proactively. Set to 0 to disable keepalive probing. When enabled, must be >= 30000 and less than `jdbc_connection_max_lifetime_ms`. Invalid enabled values are silently disabled (reset to 0).
- Introduced in: -

### lake_clear_corrupted_cache_data

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to clear the corrupted data cache in a shared-data cluster.
- Introduced in: v3.4

### lake_clear_corrupted_cache_meta

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to clear the corrupted metadata cache in a shared-data cluster.
- Introduced in: v3.3

### lake_enable_vertical_compaction_fill_data_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow vertical compaction tasks to cache data on local disks in a shared-data cluster.
- Introduced in: v3.1.7, v3.2.3

### lake_replication_read_buffer_size

- Default: 16777216
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The read buffer size used when downloading lake segment files during lake replication. This value determines the per-read allocation for reading remote files; the implementation uses the larger of this setting and a 1 MB minimum. A larger value reduces the number of read calls and can improve throughput but increases memory used per concurrent download; a smaller value lowers memory usage at the cost of more I/O calls. Tune according to network bandwidth, storage I/O characteristics, and the number of parallel replication threads.
- Introduced in: -

### lake_replication_max_file_copy_retry

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of retry attempts for non-segment file copy (`.sst`, `.delvec`, `.del`, `.cols`) during lake-to-lake (shared-data) cross-cluster replication. Each attempt verifies the copied file size matches the source to detect truncated copies caused by transient object storage issues. Increase this value if experiencing intermittent file corruption during replication over unreliable storage.
- Introduced in: -

### lake_service_max_concurrency

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum concurrency of RPC requests in a shared-data cluster. Incoming requests will be rejected when this threshold is reached. When this item is set to `0`, no limit is imposed on the concurrency.
- Introduced in: -

### max_hdfs_scanner_num

- Default: 50
- Type: Int
- Unit: -
- Is mutable: No
- Description: Limits the maximum number of concurrently running connector (HDFS/remote) scanners that a ConnectorScanNode can have. During scan startup the node computes an estimated concurrency (based on memory, chunk size and scanner_row_num) and then caps it with this value to determine how many scanners and chunks to reserve and how many scanner threads to start. It is also consulted when scheduling pending scanners at runtime (to avoid oversubscription) and when deciding how many pending scanners can be re-submitted considering file-handle limits. Lowering this reduces threads, memory and open-file pressure at the cost of potential throughput; increasing it raises concurrency and resource usage.
- Introduced in: v3.2.0

### query_max_memory_limit_percent

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum memory that the Query Pool can use. It is expressed as a percentage of the Process memory limit.
- Introduced in: v3.1.0

### rocksdb_max_write_buffer_memory_bytes

- Default: 1073741824
- Type: Int64
- Unit: -
- Is mutable: No
- Description: It is the max size of the write buffer for meta in rocksdb. Default is 1GB.
- Introduced in: v3.5.0

### rocksdb_write_buffer_memory_percent

- Default: 5
- Type: Int64
- Unit: -
- Is mutable: No
- Description: It is the memory percent of write buffer for meta in rocksdb. default is 5% of system memory. However, aside from this, the final calculated size of the write buffer memory will not be less than 64MB nor exceed 1G (rocksdb_max_write_buffer_memory_bytes)
- Introduced in: v3.5.0

## Other

### default_mv_resource_group_concurrency_limit

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency (per BE node) of the materialized view refresh tasks in the resource group `default_mv_wg`. The default value `0` indicates no limits.
- Introduced in: v3.1

### default_mv_resource_group_cpu_limit

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of CPU cores (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`.
- Introduced in: v3.1

### default_mv_resource_group_memory_limit

- Default: 0.8
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The maximum memory proportion (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`. The default value indicates 80% of the memory.
- Introduced in: v3.1

### default_mv_resource_group_spill_mem_limit_threshold

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The memory usage threshold before a materialized view refresh task in the resource group `default_mv_wg` triggers intermediate result spilling. The default value indicates 80% of the memory.
- Introduced in: v3.1

### enable_resolve_hostname_to_ip_in_load_error_url

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: For `error_urls` debugging, whether to allow operators to choose between using original hostnames from FE heartbeat or forcing resolution to IP addresses based on their environment needs.
  - `true`: Resolve hostnames to IPs.
  - `false` (Default): Keeps the original hostname in the error URL.
- Introduced in: v4.0.1

### enable_retry_apply

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, Tablet apply failures that are classified as retryable (for example transient memory-limit errors) are rescheduled for retry instead of immediately marking the tablet in error. The retry path in TabletUpdates schedules the next attempt using `retry_apply_interval_second` multiplied by the current failure count and clamped to a 600s maximum, so backoff grows with successive failures. Explicitly non-retryable errors (for example corruption) bypass retries and cause the apply process to enter the error state immediately. Retries continue until an overall timeout/terminal condition is reached, after which the apply will enter the error state. Turning this off disables automatic rescheduling of failed apply tasks and causes failed applies to transition to error state without retries.
- Introduced in: v3.2.9

### enable_token_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the token check. `true` indicates enabling the token check, and `false` indicates disabling it.
- Introduced in: -

### es_scroll_keepalive

- Default: 5m
- Type: String
- Unit: Minutes (string with suffix, e.g. "5m")
- Is mutable: No
- Description: The keep-alive duration sent to Elasticsearch for scroll search contexts. The value is used verbatim (for example "5m") when building the initial scroll URL (`?scroll=<value>`) and when sending subsequent scroll requests (via ESScrollQueryBuilder). This controls how long the ES search context is retained before garbage collection on the ES side; setting it longer keeps scroll contexts alive for more time but prolongs resource usage on the ES cluster. The value is read at startup by the ES scan reader and is not changeable at runtime.
- Introduced in: v3.2.0

### load_replica_status_check_interval_ms_on_failure

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval that the secondary replica checks it's status on the primary replica if the last check rpc fails.
- Introduced in: v3.5.1

### load_replica_status_check_interval_ms_on_success

- Default: 15000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval that the secondary replica checks it's status on the primary replica if the last check rpc successes.
- Introduced in: v3.5.1

### max_length_for_bitmap_function

- Default: 1000000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for bitmap functions.
- Introduced in: -

### max_length_for_to_base64

- Default: 200000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for the to_base64() function.
- Introduced in: -

### memory_high_level

- Default: 75
- Type: Long
- Unit: Percent
- Is mutable: Yes
- Description: High water memory threshold expressed as a percentage of the process memory limit. When total memory consumption rises above this percentage, BE begins to free memory gradually (currently by evicting data cache and update cache) to relieve pressure. The monitor uses this value to compute memory_high = mem_limit * memory_high_level / 100 and, if consumption `>` memory_high, performs controlled eviction guided by the GC advisor; if consumption exceeds memory_urgent_level (a separate config), more aggressive immediate reductions occur. This value is also consulted to disable certain memory‑intensive operations (for example, primary-key preload) when the threshold is exceeded. Must satisfy validation with memory_urgent_level (memory_urgent_level `>` memory_high_level, memory_high_level `>=` 1, memory_urgent_level `<=` 100).
- Introduced in: v3.2.0

### report_exec_rpc_request_retry_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The retry times of rpc request to report exec rpc request to FE. The default value is 10, which means that the rpc request will be retried 10 times if it fails only if it's fragment instatnce finish rpc. Report exec rpc request is important for load job, if one fragment instance finish report failed, the load job will be hang until timeout.
- Introduced in: -

### sleep_one_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: A small, global sleep interval (in seconds) used by BE agent worker threads as a one-second pause when the master address/heartbeat is not yet available or when a short retry/backoff is needed. In the codebase it is referenced by several report worker pools (e.g., ReportDiskStateTaskWorkerPool, ReportOlapTableTaskWorkerPool, ReportWorkgroupTaskWorkerPool) to avoid busy-waiting and reduce CPU consumption while retrying. Increasing this value slows the retry frequency and responsiveness to master availability; reducing it increases polling rate and CPU usage. Adjust only with awareness of the trade-off between responsiveness and resource use.
- Introduced in: v3.2.0

### small_file_dir

- Default: `${STARROCKS_HOME}/lib/small_file/`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store the files downloaded by the file manager.
- Introduced in: -

### upload_buffer_size

- Default: 4194304
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Buffer size (in bytes) used by file copy operations when uploading snapshot files to remote storage (broker or direct FileSystem). In the upload path (snapshot_loader.cpp) this value is passed to fs::copy as the read/write chunk size for each upload stream. The default is 4 MiB. Increasing this value can improve throughput on high-latency or high-bandwidth links but increases memory usage per concurrent upload; decreasing it reduces per-stream memory but may reduce transfer efficiency. Tune together with upload_worker_count and overall available memory.
- Introduced in: v3.2.13

### user_function_dir

- Default: `${STARROCKS_HOME}/lib/udf`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store User-defined Functions (UDFs).
- Introduced in: -

### web_log_bytes

- Default: 1048576 (1 MB)
- Type: long
- Unit: Bytes
- Is mutable: No
- Description: Maximum number of bytes to read from the INFO logfile and show on the BE debug webserver's log page. The handler uses this value to compute a seek offset (showing the last N bytes) to avoid reading or serving very large log files. If the logfile is smaller than this value the whole file is shown. Note: in the current implementation the code that reads and serves the INFO log is commented out and the handler reports that the INFO log file couldn't be opened, so this parameter may have no effect unless the log-serving code is enabled.
- Introduced in: v3.2.0
