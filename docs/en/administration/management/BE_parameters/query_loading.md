---
displayed_sidebar: docs
sidebar_label: "Query and Loading"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration - Query and Loading

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
- [Query](#query)
- [Loading and unloading](#loading-and-unloading)

## Query

### clear_udf_cache_when_start

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, the BE's UserFunctionCache will clear all locally cached user function libraries on startup. During UserFunctionCache::init, the code calls _reset_cache_dir(), which removes UDF files from the configured UDF library directory (organized into kLibShardNum subdirectories) and deletes files with Java/Python UDF suffixes (.jar/.py). When disabled (default), the BE loads existing cached UDF files instead of deleting them. Enabling this forces UDF binaries to be re-downloaded on first use after restart (increasing network traffic and first-use latency).
- Introduced in: v4.0.0

### dictionary_speculate_min_chunk_size

- Default: 10000
- Type: Int
- Unit: Rows
- Is mutable: No
- Description: Minimum number of rows (chunk size) used by StringColumnWriter and DictColumnWriter to trigger dictionary-encoding speculation. If an incoming column (or the accumulated buffer plus incoming rows) has size larger than or equal `dictionary_speculate_min_chunk_size` the writer will run speculation immediately and set an encoding (DICT, PLAIN or BIT_SHUFFLE) rather than buffering more rows. Speculation uses `dictionary_encoding_ratio` for string columns and `dictionary_encoding_ratio_for_non_string_column` for numeric/non-string columns to decide whether dictionary encoding is beneficial. Also, a large column byte_size (larger than or equal to UINT32_MAX) forces immediate speculation to avoid `BinaryColumn<uint32_t>` overflow.
- Introduced in: v3.2.0

### disable_storage_page_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to disable PageCache.
  - When PageCache is enabled, StarRocks caches the recently scanned data.
  - PageCache can significantly improve the query performance when similar queries are repeated frequently.
  - `true` indicates disabling PageCache.
  - The default value of this item has been changed from `true` to `false` since StarRocks v2.4.
- Introduced in: -

### enable_bitmap_index_memory_page_cache

- Default: true 
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for Bitmap index. Memory cache is recommended if you want to use Bitmap indexes to accelerate point queries.
- Introduced in: v3.1

### enable_compaction_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable compaction for Flat JSON data.
- Introduced in: v3.3.3

### enable_json_flat

- Default: false
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable the Flat JSON feature. After this feature is enabled, newly loaded JSON data will be automatically flattened, improving JSON query performance.
- Introduced in: v3.3.0

### enable_lazy_dynamic_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable Lazy Dyamic Flat JSON when a query misses Flat JSON schema in read process. When this item is set to `true`, StarRocks will postpone the Flat JSON operation to calculation process instead of read process.
- Introduced in: v3.3.3

### enable_ordinal_index_memory_page_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for ordinal index. Ordinal index is a mapping from row IDs to data page positions, and it can be used to accelerate scans.
- Introduced in: -

### enable_string_prefix_zonemap

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable ZoneMap for string (CHAR/VARCHAR) columns using prefix-based min/max. For non-key string columns, the min/max values are truncated to a fixed prefix length configured by `string_prefix_zonemap_prefix_len`.
- Introduced in: -

### enable_zonemap_index_memory_page_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for zonemap index. Memory cache is recommended if you want to use zonemap indexes to accelerate scan.
- Introduced in: -

### exchg_node_buffer_size_bytes

- Default: 10485760
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum buffer size on the receiver end of an exchange node for each query. This configuration item is a soft limit. A backpressure is triggered when data is sent to the receiver end with an excessive speed.
- Introduced in: -

### exec_state_report_max_threads

- Default: 2
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Maximum number of threads for the exec-state-report thread pool. This pool is used by `ExecStateReporter` to asynchronously send non-priority execution status reports (such as fragment completion and error status) from BE to FE via RPC. The actual pool size at startup is `max(1, exec_state_report_max_threads)`. Changing this config at runtime triggers `update_max_threads` on the pool in every executor set (shared and exclusive). The pool has a fixed task queue size of 1000; report submissions are silently dropped when all threads are busy and the queue is full. Paired with `priority_exec_state_report_max_threads` for the high-priority pool. Increase this value when delayed or dropped exec-state reports are observed under high query concurrency.
- Introduced in: v4.1.0, v4.0.8, v3.5.15

### file_descriptor_cache_capacity

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of file descriptors that can be cached.
- Introduced in: -

### flamegraph_tool_dir

- Default: `${STARROCKS_HOME}/bin/flamegraph`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory of the flamegraph tool, which should contain pprof, stackcollapse-go.pl, and flamegraph.pl scripts for generating flame graphs from profile data.
- Introduced in: -

### fragment_pool_queue_size

- Default: 2048
- Type: Int
- Unit: -
- Is mutable: No
- Description: The upper limit of the query number that can be processed on each BE node.
- Introduced in: -

### fragment_pool_thread_num_max

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads used for query.
- Introduced in: -

### fragment_pool_thread_num_min

- Default: 64
- Type: Int
- Unit: Minutes -
- Is mutable: No
- Description: The minimum number of threads used for query.
- Introduced in: -

### hdfs_client_enable_hedged_read

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the hedged read feature.
- Introduced in: v3.0

### hdfs_client_hedged_read_threadpool_size

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Specifies the size of the Hedged Read thread pool on your HDFS client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your HDFS client. It is equivalent to the `dfs.client.hedged.read.threadpool.size` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

### hdfs_client_hedged_read_threshold_millis

- Default: 2500
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to `30`. In this situation, if a read from a block has not returned within 30 milliseconds, your HDFS client immediately starts up a new read against a different block replica. It is equivalent to the `dfs.client.hedged.read.threshold.millis` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

### io_coalesce_adaptive_lazy_active

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Based on the selectivity of predicates, adaptively determines whether to combine the I/O of predicate columns and non-predicate columns.
- Introduced in: v3.2

### jit_lru_cache_size

- Default: 0
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The LRU cache size for JIT compilation. It represents the actual size of the cache if it is set to greater than 0. If it is set to less than or equal to 0, the system will adaptively set the cache using the formula `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` (while `mem_limit` of the node must be greater or equal to 16 GB).
- Introduced in: -

### json_flat_column_max

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of sub-fields that can be extracted by Flat JSON. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

### json_flat_create_zonemap

- Default: true
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to create ZoneMaps for flattened JSON sub-columns during write. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: -

### json_flat_null_factor

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of NULL values in the column to extract for Flat JSON. A column will not be extracted if its proportion of NULL value is higher than this threshold. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

### json_flat_sparsity_factor

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of columns with the same name for Flat JSON. Extraction is not performed if the proportion of columns with the same name is lower than this value. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

### lake_tablet_ignore_invalid_delete_predicate

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether ignore invalid delete predicates in tablet rowset metadata which may be introduced by logic deletion to a duplicate key table after the column name renamed.
- Introduced in: v4.0

### late_materialization_ratio

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: Integer ratio in range [0-1000] that controls the use of late materialization in the SegmentIterator (vector query engine). A value of `0` (or &le; 0) disables late materialization; `1000` (or &ge; 1000) forces late materialization for all reads. Values &gt; 0 and &lt; 1000 enable a conditional strategy where both late and early materialization contexts are prepared and the iterator selects behavior based on predicate filter ratios (higher values favor late materialization). When a segment contains complex metric types, StarRocks uses `metric_late_materialization_ratio` instead. If `lake_io_opts.cache_file_only` is set, late materialization is disabled.
- Introduced in: v3.2.0

### max_hdfs_file_handle

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of HDFS file descriptors that can be opened.
- Introduced in: -

### max_hdfs_scanner_num

- Default: 50
- Type: Int
- Unit: -
- Is mutable: No
- Description: Maximum number of concurrent remote scanners (HDFS, object storage, etc.) that ConnectorScanNode can run simultaneously. This value caps estimated concurrency at startup and also limits pending-scanner scheduling at runtime, controlling thread, memory, and file-handle pressure.
- Introduced in: v3.2.0

### max_memory_sink_batch_count

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of Scan Cache batches.
- Introduced in: -

### max_pushdown_conditions_per_column

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of conditions that allow pushdown in each column. If the number of conditions exceeds this limit, the predicates are not pushed down to the storage layer.
- Introduced in: -

### max_scan_key_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of scan keys segmented by each query.
- Introduced in: -

### metric_late_materialization_ratio

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: No
- Description: Controls when the late-materialization row access strategy is used for reads that include complex metric columns. Valid range: [0-1000]. `0` disables late materialization; `1000` forces late materialization for all applicable reads. Values 1–999 enable a conditional strategy where both late and early materialization contexts are prepared and chosen at runtime based on predicate/selectivity. When complex metric types exist, `metric_late_materialization_ratio` overrides the general `late_materialization_ratio`. Note: `cache_file_only` I/O mode will cause late materialization to be disabled regardless of this setting.
- Introduced in: v3.2.0

### min_file_descriptor_number

- Default: 60000
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of file descriptors in the BE process.
- Introduced in: -

### object_storage_connect_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish socket connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

### object_storage_request_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish HTTP connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

### parquet_late_materialization_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the late materialization of Parquet reader to improve performance. `true` indicates enabling late materialization, and `false` indicates disabling it.
- Introduced in: -

### parquet_page_index_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the pageindex of Parquet file to improve performance. `true` indicates enabling pageindex, and `false` indicates disabling it.
- Introduced in: v3.3

### parquet_reader_bloom_filter_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the bloom filter of Parquet file to improve performance. `true` indicates enabling the bloom filter, and `false` indicates disabling it. You can also control this behavior on session level using the system variable `enable_parquet_reader_bloom_filter`. Bloom filters in Parquet are maintained **at the column level within each row group**. If a Parquet file contains bloom filters for certain columns, queries can use predicates on those columns to efficiently skip row groups.
- Introduced in: v3.5

### path_gc_check_step

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of files that can be scanned continuously each time.
- Introduced in: -

### path_gc_check_step_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval between file scans.
- Introduced in: -

### path_scan_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which GC cleans expired data.
- Introduced in: -

### pipeline_connector_scan_thread_num_per_cpu

- Default: 8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The number of scan threads assigned to Pipeline Connector per CPU core in the BE node. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

### pipeline_enable_large_column_checker

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable large column detection in the pipeline execution framework. When enabled, queries fail with a capacity limit error if an intermediate column reaches the chunk capacity limit in pipeline execution or spill serialization.
- Introduced in: v4.0.0

### pipeline_poller_timeout_guard_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When this item is set to greater than `0`, if a driver takes longer than `pipeline_poller_timeout_guard_ms` for a single dispatch in the poller, then the information of the driver and operator is printed.
- Introduced in: -

### pipeline_prepare_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum queue lenggth of PREPARE fragment thread pool for Pipeline execution engine.
- Introduced in: -

### pipeline_prepare_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of threads in the pipeline execution engine PREPARE fragment thread pool. `0` indicates the value is equal to the number of system VCPU core number.
- Introduced in: -

### pipeline_prepare_timeout_guard_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When this item is set to greater than `0`, if a plan fragment exceeds `pipeline_prepare_timeout_guard_ms` during the PREPARE process, a stack trace of the plan fragment is printed.
- Introduced in: -

### pipeline_scan_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum task queue length of SCAN thread pool for Pipeline execution engine.
- Introduced in: -

### enable_lock_free_scan_task_queue

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether `ScanExecutor` uses `LockFreeWorkGroupScanTaskQueue` for OLAP scan and connector scan task scheduling. When enabled, scan tasks are scheduled through a lock-free per-workgroup queue plus a workgroup-level coordinator, which reduces contention compared with the legacy mutex-based `WorkGroupScanTaskQueue`. Set this item to `false` to fall back to the legacy scan task queue implementation during troubleshooting or rollback.
- Introduced in: v4.1.0

### pk_index_parallel_get_threadpool_size

- Default: 1048576
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the maximum queue size (number of pending tasks) for the "cloud_native_pk_index_get" thread pool used by PK index parallel get operations in shared-data (cloud-native/lake) mode. The actual thread count for that pool is controlled by `pk_index_parallel_get_threadpool_max_threads`; this setting only limits how many tasks may be queued awaiting execution. The very large default (2^20) effectively makes the queue unbounded; lowering it prevents excessive memory growth from queued tasks but may cause task submissions to block or fail when the queue is full. Tune together with `pk_index_parallel_get_threadpool_max_threads` based on workload concurrency and memory constraints.
- Introduced in: -

### priority_exec_state_report_max_threads

- Default: 2
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Maximum number of threads for the high-priority exec-state-report thread pool. This pool is used by `ExecStateReporter` to asynchronously send high-priority execution status reports (such as urgent fragment failures) from BE to FE via RPC. Unlike the normal exec-state-report pool, this pool has an unbounded task queue. The actual pool size at startup is `max(1, priority_exec_state_report_max_threads)`. Changing this config at runtime triggers `update_max_threads` on the priority pool in every executor set (shared and exclusive). Paired with `exec_state_report_max_threads` for the normal pool. Increase this value when high-priority reports are delayed under heavy concurrent query loads.
- Introduced in: v4.1.0, v4.0.8, v3.5.15

### priority_queue_remaining_tasks_increased_frequency

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how often the BlockingPriorityQueue increases ("ages") the priority of all remaining tasks to avoid starvation. Each successful get/pop increments an internal `_upgrade_counter`; when `_upgrade_counter` exceeds `priority_queue_remaining_tasks_increased_frequency`, the queue increments every element's priority, rebuilds the heap, and resets the counter. Lower values cause more frequent priority aging (reducing starvation but increasing CPU cost due to iterating and re-heapifying); higher values reduce that overhead but delay priority adjustments. The value is a simple operation count threshold, not a time duration.
- Introduced in: v3.2.0

### query_cache_capacity

- Default: 536870912
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The size of the query cache in the BE. The default size is 512 MB. The size cannot be less than 4 MB. If the memory capacity of the BE is insufficient to provision your expected query cache size, you can increase the memory capacity of the BE.
- Introduced in: -

### query_pool_spill_mem_limit_threshold

- Default: 1.0
- Type: Double
- Unit: -
- Is mutable: No
- Description: If automatic spilling is enabled, when the memory usage of all queries exceeds `query_pool memory limit * query_pool_spill_mem_limit_threshold`, intermediate result spilling will be triggered.
- Introduced in: v3.2.7

### query_scratch_dirs

- Default: `${STARROCKS_HOME}`
- Type: string
- Unit: -
- Is mutable: No
- Description: Comma-separated list of writable scratch directories used by query execution to spill intermediate data (for example, external sorts, hash joins, and other operators). Specify one or more paths separated by `;` (e.g. `/mnt/ssd1/tmp;/mnt/ssd2/tmp`). Directories should be accessible and writable by the BE process and have sufficient free space; StarRocks will pick among them to distribute spill I/O. Changes require a restart to take effect. If a directory is missing, not writable, or full, spilling may fail or degrade query performance.
- Introduced in: v3.2.0

### result_buffer_cancelled_interval_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The wait time before BufferControlBlock releases data.
- Introduced in: -

### scan_context_gc_interval_min

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The time interval at which to clean the Scan Context.
- Introduced in: -

### scanner_row_num

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum row count returned by each scan thread in a scan.
- Introduced in: -

### scanner_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of scan tasks supported by the storage engine.
- Introduced in: -

### scanner_thread_pool_thread_num

- Default: 48
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads which the storage engine used for concurrent storage volume scanning. All threads are managed in the thread pool.
- Introduced in: -

### string_prefix_zonemap_prefix_len

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Prefix length used for string ZoneMap min/max when `enable_string_prefix_zonemap` is enabled.
- Introduced in: -

### udf_thread_pool_size

- Default: 1
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the size of the UDF call PriorityThreadPool created in ExecEnv (used for executing user-defined functions / UDF-related tasks). The value is used as the pool thread count and also as the pool queue capacity when constructing the thread pool (PriorityThreadPool("udf", thread_num, queue_size)). Increase to allow more concurrent UDF executions; keep small to avoid excessive CPU and memory contention.
- Introduced in: v3.2.0

### update_memory_limit_percent

- Default: 60
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Fraction of the BE process memory reserved for update-related memory and caches. During startup `GlobalEnv` computes the `MemTracker` for updates as process_mem_limit * clamp(update_memory_limit_percent, 0, 100) / 100. `UpdateManager` also uses this percentage to size its primary-index/index-cache capacity (index cache capacity = GlobalEnv::process_mem_limit * update_memory_limit_percent / 100). The HTTP config update logic registers a callback that calls `update_primary_index_memory_limit` on the update managers, so changes would be applied to the update subsystem if the config were changed. Increasing this value gives more memory to update/primary-index paths (reducing memory available for other pools); decreasing it reduces update memory and cache capacity. Values are clamped to the range 0–100.
- Introduced in: v3.2.0

### vector_chunk_size

- Default: 4096
- Type: Int
- Unit: Rows
- Is mutable: No
- Description: The number of rows per vectorized chunk (batch) used throughout the execution and storage code paths. This value controls Chunk and RuntimeState batch_size creation, affects operator throughput, memory footprint per operator, spill and sort buffer sizing, and I/O heuristics (for example, ORC writer natural write size). Increasing it can improve CPU and I/O efficiency for wide/CPU-bound workloads but raises peak memory usage and can increase latency for small-result queries. Tune only when profiling shows batch-size is a bottleneck; otherwise keep the default for balanced memory and performance.
- Introduced in: v3.2.0

## Loading and unloading

### clear_transaction_task_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for clearing transaction.
- Introduced in: -

### column_mode_partial_update_insert_batch_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Batch size for column mode partial update when processing inserted rows. If this item is set to `0` or negative, it will be clamped to `1` to avoid infinite loop. This item controls the number of newly inserted rows processed in each batch. Larger values can improve write performance but will consume more memory.
- Introduced in: v3.5.10, v4.0.2

### partial_update_memory_limit_per_worker

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Maximum memory per worker thread for partial update operations. Controls the memory footprint of individual worker threads when processing partial updates.
- Introduced in: -

### enable_load_spill_parallel_merge

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Specifies whether to enable parallel spill merge within a single tablet. Enabling this can improve the performance of spill merge during data loading.
- Introduced in: -

### enable_parallel_memtable_finalize

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Specifies whether to enable parallel memtable finalize when loading data to lake tables (shared-data mode). When enabled, the memtable finalize operation (sort/aggregate) is moved from the write thread to the flush thread, allowing the write thread to continue inserting data into a new memtable while the previous one is being finalized and flushed in parallel. This can significantly improve load throughput by overlapping CPU-intensive finalize operations with I/O-bound flush operations. Note that this optimization is automatically disabled when auto-increment columns need to be filled, as auto-increment ID assignment must happen before the memtable is submitted for flush.
- Introduced in: -

### allow_list_object_for_random_bucketing_on_cache_miss

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether to allow object-storage LIST fallback when lake metadata cache misses during random bucketing size checks. `true` means fallback to LIST metadata files to compute base size (historical behavior, more accurate size estimation). `false` means skip LIST and use `base_size = 0`, which reduces LIST object requests but may delay immutable marking due to less accurate size estimation.
- Introduced in: 4.1.0, 4.0.7, 3.5.15

### enable_stream_load_verbose_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Specifies whether to log the HTTP requests and responses for Stream Load jobs.
- Introduced in: v2.5.17, v3.0.9, v3.1.6, v3.2.1

### flush_thread_num_per_store

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store.
- Introduced in: -

### lake_flush_thread_num_per_store

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store in a shared-data cluster. 
When this value is set to `0`, the system uses twice of the CPU core count as the value.
When this value is set to less than `0`, the system uses the product of its absolute value and the CPU core count as the value.
- Introduced in: v3.1.12, 3.2.7

### load_data_reserve_hours

- Default: 4
- Type: Int
- Unit: Hours
- Is mutable: No
- Description: The reservation time for the files produced by small-scale loadings.
- Introduced in: -

### load_error_log_reserve_hours

- Default: 48
- Type: Int
- Unit: Hours
- Is mutable: Yes
- Description: The time for which data loading logs are reserved.
- Introduced in: -

### load_process_max_memory_limit_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum size limit of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

### load_spill_memory_usage_per_merge

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum memory usage per merge operation during spill merge. Default is 1 GB (1073741824 bytes). This parameter controls the memory consumption of individual merge tasks during data loading spill merge to prevent excessive memory usage.
- Introduced in: -

### max_consumer_num_per_group

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of consumers in a consumer group of Routine Load.
- Introduced in: -

### max_runnings_transactions_per_txn_map

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of transactions that can run concurrently in each partition.
- Introduced in: -

### number_tablet_writer_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of tablet writer threads used in ingestion, such as Stream Load, Broker Load and Insert. When the parameter is set to less than or equal to 0, the system uses half of the number of CPU cores, with a minimum of 16. When the parameter is set to greater than 0, the system uses that value. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

### push_worker_count_high_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with HIGH priority.
- Introduced in: -

### push_worker_count_normal_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with NORMAL priority.
- Introduced in: -

### streaming_load_max_batch_size_mb

- Default: 100
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a JSON file that can be streamed into StarRocks.
- Introduced in: -

### streaming_load_max_mb

- Default: 102400
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a file that can be streamed into StarRocks. From v3.0, the default value has been changed from `10240` to `102400`.
- Introduced in: -

### streaming_load_rpc_max_alive_time_sec

- Default: 1200
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The RPC timeout for Stream Load.
- Introduced in: -

### transaction_publish_version_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Minimum number of threads in the Publish Version thread pool. The pool can shrink to this value when idle. `0` means no fixed lower bound.
- Introduced in: -

### transaction_publish_version_thread_pool_idle_time_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The idle time before a thread is reclaimed by the Publish Version thread pool.
- Introduced in: -

### transaction_publish_version_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used to publish a version. When this value is set to less than or equal to `0`, the system uses the CPU core count as the value, so as to avoid insufficient thread resources when import concurrency is high but only a fixed number of threads are used. From v2.5, the default value has been changed from `8` to `0`.
- Introduced in: -

### use_mmap_allocate_chunk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use anonymous mmap (`MAP_ANONYMOUS | MAP_PRIVATE`) for chunk allocation. When enabled, many VM mappings are created; you must raise `vm.max_map_count` (e.g., `echo 262144 > /proc/sys/vm/max_map_count`) and set a large `chunk_reserved_bytes_limit`, otherwise frequent map/unmap operations will cause severe performance degradation.
- Introduced in: v3.2.0

### write_buffer_size

- Default: 104857600
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The buffer size of MemTable in the memory. This configuration item is the threshold to trigger a flush.
- Introduced in: -

### broker_write_timeout_seconds

- Default: 30
- Type: int
- Unit: Seconds
- Is mutable: No
- Description: Timeout (in seconds) used by backend broker operations for write/IO RPCs. The value is multiplied by 1000 to produce millisecond timeouts and is passed as the default timeout_ms to BrokerFileSystem and BrokerServiceConnection instances (e.g., file export and snapshot upload/download). Increase this when brokers or network are slow or when transferring large files to avoid premature timeouts; decreasing it may cause broker RPCs to fail earlier. This value is defined in common/config and is applied at process start (not dynamically reloadable).
- Introduced in: v3.2.0

### enable_load_channel_rpc_async

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, handling of load-channel open RPCs (for example, `PTabletWriterOpen`) is offloaded from the BRPC worker to a dedicated thread pool: the request handler creates a `ChannelOpenTask` and submits it to the internal `_async_rpc_pool` instead of running `LoadChannelMgr::_open` inline. This reduces work and blocking inside BRPC threads and allows tuning concurrency via `load_channel_rpc_thread_pool_num` and `load_channel_rpc_thread_pool_queue_size`. If the thread pool submission fails (when pool is full or shut down), the request is canceled and an error status is returned. The pool is shut down on `LoadChannelMgr::close()`, so consider capacity and lifecycle when you want to enable this feature so as to avoid request rejections or delayed processing.
- Introduced in: v3.5.0

### enable_load_diagnose

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, StarRocks will attempt an automated load diagnosis from BE OlapTableSink/NodeChannel after a brpc timeout matching "[E1008]Reached timeout". The code creates a `PLoadDiagnoseRequest` and sends an RPC to the remote LoadChannel to collect a profile and/or stack trace (controlled by `load_diagnose_rpc_timeout_profile_threshold_ms` and `load_diagnose_rpc_timeout_stack_trace_threshold_ms`). The diagnose RPC uses `load_diagnose_send_rpc_timeout_ms` as its timeout. Diagnosis is skipped if a diagnose request is already in progress. Enabling this produces additional RPCs and profiling work on target nodes; disable on sensitive production workloads to avoid extra overhead.
- Introduced in: v3.5.0

### enable_load_segment_parallel

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, rowset segment loading and rowset-level reads are performed concurrently using StarRocks background thread pools (ExecEnv::load_segment_thread_pool and ExecEnv::load_rowset_thread_pool). Rowset::load_segments and TabletReader::get_segment_iterators submit per-segment or per-rowset tasks to these pools, falling back to serial loading and logging a warning if submission fails. Enable this to reduce read/load latency for large rowsets at the cost of increased CPU/IO concurrency and memory pressure. Note: parallel loading can change the load completion order of segments and therefore prevents partial compaction (code checks `_parallel_load` and disables partial compaction when enabled); consider implications for operations that rely on segment order.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### enable_streaming_load_thread_pool

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether streaming load scanners are submitted to the dedicated streaming load thread pool. When enabled and a query is a LOAD with `TLoadJobType::STREAM_LOAD`, ConnectorScanNode submits scanner tasks to the `streaming_load_thread_pool` (which is configured with INT32_MAX threads and queue sizes, i.e. effectively unbounded). When disabled, scanners use the general `thread_pool` and its `PriorityThreadPool` submission logic (priority computation, try_offer/offer behavior). Enabling isolates streaming-load work from regular query execution to reduce interference; however, because the dedicated pool is effectively unbounded, enabling may increase concurrent threads and resource usage under heavy streaming-load traffic. This option is on by default and typically does not require modification.
- Introduced in: v3.2.0

### es_http_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: HTTP connection timeout (in milliseconds) used by the ES network client in ESScanReader for Elasticsearch scroll requests. This value is applied via network_client.set_timeout_ms() before sending subsequent scroll POSTs and controls how long the client waits for an ES response during scrolling. Increase this value for slow networks or large queries to avoid premature timeouts; decrease to fail faster on unresponsive ES nodes. This setting complements `es_scroll_keepalive`, which controls the scroll context keep-alive duration.
- Introduced in: v3.2.0

### es_index_max_result_window

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: No
- Description: Limits the maximum number of documents StarRocks will request from Elasticsearch in a single batch. StarRocks sets the ES request batch size to min(`es_index_max_result_window`, `chunk_size`) when building `KEY_BATCH_SIZE` for the ES reader. If an ES request exceeds the Elasticsearch index setting `index.max_result_window`, Elasticsearch returns HTTP 400 (Bad Request). Adjust this value when scanning large indexes or increase the ES `index.max_result_window` on the Elasticsearch side to permit larger single requests.
- Introduced in: v3.2.0

### ignore_load_tablet_failure

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `false`, the system will treat any tablet header load failures (non-NotFound and non-AlreadyExist errors) as fatal: the code logs the error and calls LOG(FATAL) to stop the BE process. When it is set to `true`, the BE continues startup despite such per-tablet load errors — failed tablet IDs are recorded and skipped while successful tablets are still loaded. Note that this parameter does NOT suppress fatal errors from the RocksDB meta scan itself, which always cause the process to quit.
- Introduced in: v3.2.0

### load_channel_abort_clean_up_delay_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls how long (in seconds) the system keeps the load IDs of aborted load channels before removing them from `_aborted_load_channels`. When a load job is cancelled or fails, the load ID stays recorded so any late-arriving load RPCs can be rejected immediately; once the delay expires, the entry is cleaned during the periodic background sweep (minimum sweep interval is 60 seconds). Setting the delay too low risks accepting stray RPCs after an abort, while setting it too high may retain state and consume resources longer than necessary. Tune this to balance correctness of late-request rejection and resource retention for aborted loads.
- Introduced in: v3.5.11, v4.0.4

### load_channel_rpc_thread_pool_num

- Default: -1
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Maximum number of threads for the load-channel async RPC thread pool. When set to less than or equal to 0 (default `-1`) the pool size is auto-set to the number of CPU cores (`CpuInfo::num_cores()`). The configured value is used as ThreadPoolBuilder's max threads and the pool's min threads is set to min(5, max_threads). The pool queue size is controlled separately by `load_channel_rpc_thread_pool_queue_size`. This setting was introduced to align the async RPC pool size with brpc workers' default (`brpc_num_threads`) so behavior remains compatible after switching load RPC handling from synchronous to asynchronous. Changing this config at runtime triggers `ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(...)`.
- Introduced in: v3.5.0

### load_channel_rpc_thread_pool_queue_size

- Default: 1024000
- Type: int
- Unit: Count
- Is mutable: No
- Description: Sets the maximum pending-task queue size for the Load channel RPC thread pool created by LoadChannelMgr. This thread pool executes asynchronous `open` requests when `enable_load_channel_rpc_async` is enabled; the pool size is paired with `load_channel_rpc_thread_pool_num`. The large default (1024000) aligns with brpc workers' defaults to preserve behavior after switching from synchronous to asynchronous handling. If the queue is full, ThreadPool::submit() will fail and the incoming open RPC is cancelled with an error, causing the caller to receive a rejection. Increase this value to buffer larger bursts of concurrent `open` requests; reducing it tightens backpressure but may cause more rejections under load.
- Introduced in: v3.5.0

### load_diagnose_rpc_timeout_profile_threshold_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When a load RPC times out (error contains "[E1008]Reached timeout") and `enable_load_diagnose` is true, this threshold controls whether a full profiling diagnose is requested. If the request-level RPC timeout `_rpc_timeout_ms` is greater than `load_diagnose_rpc_timeout_profile_threshold_ms`, profiling is enabled for that diagnose. For smaller `_rpc_timeout_ms` values, profiling is sampled once every 20 timeouts to avoid frequent heavy diagnostics for real-time/short-timeout loads. This value affects the `profile` flag in the `PLoadDiagnoseRequest` sent; stack-trace behavior is controlled separately by `load_diagnose_rpc_timeout_stack_trace_threshold_ms` and send timeout by `load_diagnose_send_rpc_timeout_ms`.
- Introduced in: v3.5.0

### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used to decide when to request remote stack traces for long-running load RPCs. When a load RPC times out with a timeout error and the effective RPC timeout (_rpc_timeout_ms) exceeds this value, `OlapTableSink`/`NodeChannel` will include `stack_trace=true` in a `load_diagnose` RPC to the target BE so the BE can return stack traces for debugging. `LocalTabletsChannel::SecondaryReplicasWaiter` also triggers a best-effort stack-trace diagnose from the primary if waiting for secondary replicas exceeds this interval. This behavior requires `enable_load_diagnose` and uses `load_diagnose_send_rpc_timeout_ms` for the diagnose RPC timeout; profiling is gated separately by `load_diagnose_rpc_timeout_profile_threshold_ms`. Lowering this value increases how aggressively stack traces are requested.
- Introduced in: v3.5.0

### load_diagnose_send_rpc_timeout_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout (in milliseconds) applied to diagnosis-related brpc calls initiated by BE load paths. It is used to set the controller timeout for `load_diagnose` RPCs (sent by NodeChannel/OlapTableSink when a LoadChannel brpc call times out) and for replica-status queries (used by SecondaryReplicasWaiter / LocalTabletsChannel when checking primary replica state). Choose a value high enough to allow the remote side to respond with profile or stack-trace data, but not so high that failure handling is delayed. This parameter works together with `enable_load_diagnose`, `load_diagnose_rpc_timeout_profile_threshold_ms`, and `load_diagnose_rpc_timeout_stack_trace_threshold_ms` which control when and what diagnostic information is requested.
- Introduced in: v3.5.0

### load_fp_brpc_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Overrides the per-channel brpc RPC timeout used by OlapTableSink when the `node_channel_set_brpc_timeout` fail point is triggered. If set to a positive value, NodeChannel will set its internal `_rpc_timeout_ms` to this value (in milliseconds) causing open/add-chunk/cancel RPCs to use the shorter timeout and enabling simulation of brpc timeouts that produce the "[E1008]Reached timeout" error. Default (`-1`) disables the override. Changing this value is intended for testing and fault injection; small values may produce false timeouts and trigger load diagnostics (see `enable_load_diagnose`, `load_diagnose_rpc_timeout_profile_threshold_ms`, `load_diagnose_rpc_timeout_stack_trace_threshold_ms`, and `load_diagnose_send_rpc_timeout_ms`).
- Introduced in: v3.5.0

### load_fp_tablets_channel_add_chunk_block_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When enabled (set to a positive milliseconds value) this fail-point configuration makes TabletsChannel::add_chunk sleep for the specified time during load processing. It is used to simulate BRPC timeout errors (e.g., "[E1008]Reached timeout") and to emulate an expensive add_chunk operation that increases load latency. A value less than or equal to 0 (default `-1`) disables the injection. Intended for testing fault handling, timeouts, and replica synchronization behavior — do not enable in normal production workloads as it delays write completion and can trigger upstream timeouts or replica aborts.
- Introduced in: v3.5.0

### load_segment_thread_pool_num_max

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Sets the maximum number of worker threads for BE load-related thread pools. This value is used by ThreadPoolBuilder to limit threads for both `load_rowset_pool` and `load_segment_pool` in exec_env.cpp, controlling concurrency for processing loaded rowsets and segments (e.g., decoding, indexing, writing) during streaming and batch loads. Increasing this value raises parallelism and can improve load throughput but also increases CPU, memory usage, and potential contention; decreasing it limits concurrent load processing and may reduce throughput. Tune together with `load_segment_thread_pool_queue_size` and `streaming_load_thread_pool_idle_time_ms`. Change requires BE restart.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### load_segment_thread_pool_queue_size

- Default: 10240
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: Sets the maximum queue length (number of pending tasks) for the load-related thread pools created as "load_rowset_pool" and "load_segment_pool". These pools use `load_segment_thread_pool_num_max` for their max thread count and this configuration controls how many load segment/rowset tasks can be buffered before the ThreadPool's overflow policy takes effect (further submissions may be rejected or blocked depending on the ThreadPool implementation). Increase to allow more pending load work (uses more memory and can raise latency); decrease to limit buffered load concurrency and reduce memory usage.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### max_pulsar_consumer_num_per_group

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the maximum number of Pulsar consumers that may be created in a single data consumer group for routine load on a BE. Because cumulative acknowledge is not supported for multi-topic subscriptions, each consumer subscribes exactly one topic/partition; if the number of partitions in `pulsar_info->partitions` exceeds this value, group creation fails with an error advising to increase `max_pulsar_consumer_num_per_group` on the BE or add more BEs. This limit is enforced when constructing a PulsarDataConsumerGroup and prevents a BE from hosting more than this many consumers for one routine load group. For Kafka routine load, `max_consumer_num_per_group` is used instead.
- Introduced in: v3.2.0

### pull_load_task_dir

- Default: `${STARROCKS_HOME}/var/pull_load`
- Type: string
- Unit: -
- Is mutable: No
- Description: Filesystem path where the BE stores data and working files for "pull load" tasks (downloaded source files, task state, temporary output, etc.). The directory must be writable by the BE process and have sufficient disk space for incoming loads. The default is relative to STARROCKS_HOME; tests create and expect this directory to exist (see test configuration).
- Introduced in: v3.2.0

### routine_load_kafka_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Timeout in seconds used for Kafka-related routine load operations. When a client request does not specify a timeout, `routine_load_kafka_timeout_second` is used as the default RPC timeout (converted to milliseconds) for `get_info`. It is also used as the per-call consume poll timeout for the librdkafka consumer (converted to milliseconds and capped by remaining runtime). Note: the internal `get_info` path reduces this value to 80% before passing it to librdkafka to avoid FE-side timeout races. Set this to a value that balances timely failure reporting and sufficient time for network/broker responses; changes require a restart because the setting is not mutable.
- Introduced in: v3.2.0

### routine_load_pulsar_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Default timeout (in seconds) that the BE uses for Pulsar-related routine load operations when the request does not supply an explicit timeout. Specifically, `PInternalServiceImplBase::get_pulsar_info` multiplies this value by 1000 to form the millisecond timeout passed to the routine load task executor methods that fetch Pulsar partition metadata and backlog. Increase to allow slower Pulsar responses at the cost of longer failure detection; decrease to fail faster on slow brokers. Analogous to `routine_load_kafka_timeout_second` used for Kafka.
- Introduced in: v3.2.0

### streaming_load_thread_pool_idle_time_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the thread idle timeout (in milliseconds) for streaming-load related thread pools. The value is used as the idle timeout passed to ThreadPoolBuilder for the `stream_load_io` pool and also for `load_rowset_pool` and `load_segment_pool`. Threads in these pools are reclaimed when idle for this duration; lower values reduce idle resource usage but increase thread creation overhead, while higher values keep threads alive longer. The `stream_load_io` pool is used when `enable_streaming_load_thread_pool` is enabled.
- Introduced in: v3.2.0

### streaming_load_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Minimum number of threads for the streaming load IO thread pool ("stream_load_io") created during ExecEnv initialization. The pool is built with `set_max_threads(INT32_MAX)` and `set_max_queue_size(INT32_MAX)` so it is effectively unbounded to avoid deadlocks for concurrent streaming loads. A value of 0 lets the pool start with no threads and grow on demand; setting a positive value reserves that many threads at startup. This pool is used when `enable_streaming_load_thread_pool` is true and its idle timeout is controlled by `streaming_load_thread_pool_idle_time_ms`. Overall concurrency is still constrained by `fragment_pool_thread_num_max` and `webserver_num_workers`; changing this value is rarely necessary and may increase resource usage if set too high.
- Introduced in: v3.2.0
