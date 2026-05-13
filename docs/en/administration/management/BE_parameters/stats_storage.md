---
displayed_sidebar: docs
sidebar_label: "Statistics and Storage"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE Configuration - Statistics and Storage

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
- [Statistic report](#statistic-report)
- [Storage](#storage)

## Statistic report

### enable_metric_calculator

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, the BE process launches a background "metrics_daemon" thread (started in Daemon::init on non-Apple platforms) that runs every ~15 seconds to invoke `MetricRegistry::trigger_hook()` on the process metrics registry and compute derived/system metrics (e.g., push/query bytes/sec, max disk I/O util, max network send/receive rates), log memory breakdowns and run table metrics cleanup. When false, those hooks are executed synchronously inside `MetricRegistry::collect` at metric collection time, which can increase metric-scrape latency. Requires process restart to take effect.
- Introduced in: v3.2.0

### enable_system_metrics

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When true, StarRocks initializes system-level monitoring during startup: it discovers disk devices from the configured store paths and enumerates network interfaces, then passes this information into the metrics subsystem to enable collection of disk I/O, network traffic and memory-related system metrics. If device or interface discovery fails, initialization logs a warning and aborts system metrics setup. This flag only controls whether system metrics are initialized; periodic metric aggregation threads are controlled separately by `enable_metric_calculator`, and JVM metrics initialization is controlled by `enable_jvm_metrics`. Changing this value requires a restart.
- Introduced in: v3.2.0

### profile_report_interval

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval in seconds that the ProfileReportWorker uses to (1) decide when to report per-fragment profile information for LOAD queries and (2) sleep between reporting cycles. The worker compares current time against each task's last_report_time using (profile_report_interval * 1000) ms to determine if a profile should be re-reported for both non-pipeline and pipeline load tasks. At each loop the worker reads the current value (mutable at runtime); if the configured value is less than or euqual to 0 the worker forces it to 1 and emits a warning. Changing this value affects the next reporting decision and sleep duration.
- Introduced in: v3.2.0

### report_disk_state_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the storage volume state, which includes the size of data within the volume.
- Introduced in: -

### report_resource_usage_interval_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Interval, in milliseconds, between periodic resource-usage reports sent by the BE agent to the FE (master). The agent worker thread collects TResourceUsage (number of running queries, memory used/limit, CPU used permille, and resource-group usages) and calls report_task, then sleeps for this configured interval (see task_worker_pool). Lower values increase reporting timeliness but raise CPU, network, and master load; higher values reduce overhead but make resource information less current. The reporting updates related metrics (report_resource_usage_requests_total, report_resource_usage_requests_failed). Tune according to cluster scale and FE load.
- Introduced in: v3.2.0

### report_tablet_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all tablets.
- Introduced in: -

### report_task_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the state of a task. A task can be creating a table, dropping a table, loading data, or changing a table schema.
- Introduced in: -

### report_workgroup_interval_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all workgroups.
- Introduced in: -

## Storage

<EditionSpecificBEItem />

### alter_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used for Schema Change.
- Introduced in: -

### automatic_partition_thread_pool_thread_num

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads in the automatic partition thread pool used for automatic partition creation during loading. The queue size of the pool is automatically set to 10 times the thread count.
- Introduced in: -

### avro_ignore_union_type_tag

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to strip the type tag from the JSON string serialized from the Avro Union data type.
- Introduced in: v3.3.7, v3.4

### base_compaction_check_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Base Compaction.
- Introduced in: -

### base_compaction_interval_seconds_since_last_operation

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval since the last Base Compaction. This configuration item is one of the conditions that trigger a Base Compaction.
- Introduced in: -

### base_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for Base Compaction on each storage volume.
- Introduced in: -

### base_cumulative_delta_ratio

- Default: 0.3
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction.
- Introduced in: -

### chaos_test_enable_random_compaction_strategy

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, TabletUpdates::compaction() uses the random compaction strategy (compaction_random) intended for chaos engineering tests. This flag forces compaction to follow a nondeterministic/random policy instead of normal strategies (e.g., size-tiered compaction), and takes precedence during compaction selection for the tablet. It is intended only for controlled testing: enabling it can produce unpredictable compaction order, increased I/O/CPU, and test flakiness. Do not enable in production; use only for fault-injection or chaos-test scenarios.
- Introduced in: v3.3.12, 3.4.2, 3.5.0, 4.0.0

### check_consistency_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for checking the consistency of tablets.
- Introduced in: -

### clear_expired_replication_snapshots_interval_seconds

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which the system clears the expired snapshots left by abnormal replications.
- Introduced in: v3.3.5

### compact_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for concurrent compaction tasks. This configuration is changed to dynamic from v3.1.7 and v3.2.2 onwards.
- Introduced in: v3.0.0

### compaction_max_memory_limit

- Default: -1
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Global upper bound (in bytes) for memory available to compaction tasks on this BE. During BE initialization the final compaction memory limit is computed as min(`compaction_max_memory_limit`, process_mem_limit * `compaction_max_memory_limit_percent` / 100). If `compaction_max_memory_limit` is negative (default `-1`) it falls back to the BE process memory limit derived from `mem_limit`. The percent value is clamped to [0,100]. If the process memory limit is not set (negative) compaction memory remains unlimited (`-1`). This computed value is used to initialize the `_compaction_mem_tracker`. See also `compaction_max_memory_limit_percent` and `compaction_memory_limit_per_worker`.
- Introduced in: v3.2.0

### compaction_max_memory_limit_percent

- Default: 100
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Percentage of the BE process memory that may be used for compaction. The BE computes the compaction memory cap as the minimum of `compaction_max_memory_limit` and (process memory limit × this percent / 100). If this value is < 0 or > 100 it is treated as 100. If `compaction_max_memory_limit` < 0 the process memory limit is used instead. The calculation also considers the BE process memory derived from `mem_limit`. Combined with `compaction_memory_limit_per_worker` (per-worker cap), this setting controls total compaction memory available and therefore affects compaction concurrency and OOM risk.
- Introduced in: v3.2.0

### compaction_memory_limit_per_worker

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size allowed for each Compaction thread.
- Introduced in: -

### compaction_trace_threshold

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace.
- Introduced in: -

### create_tablet_worker_count

- Default: 3
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the maximum number of worker threads in the AgentServer thread pool that process TTaskType::CREATE (create-tablet) tasks submitted by FE. At BE startup this value is used as the thread-pool max (the pool is created with min threads = 1 and max queue size = unlimited), and changing it at runtime triggers `ExecEnv::agent_server()->get_thread_pool(TTaskType::CREATE)->update_max_threads(...)`. Increase this to raise concurrent tablet creation throughput (useful during bulk load or partition creation); decreasing it throttles concurrent create operations. Raising the value increases CPU, memory and I/O concurrency and may cause contention; the thread pool enforces at least one thread, so values less than 1 have no practical effect.
- Introduced in: v3.2.0

### cumulative_compaction_check_interval_seconds

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Cumulative Compaction.
- Introduced in: -

### cumulative_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of Cumulative Compaction threads per disk.
- Introduced in: -

### data_page_size

- Default: 65536
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Target uncompressed page size (in bytes) used when building column data and index pages. This value is copied into ColumnWriterOptions.data_page_size and IndexedColumnWriterOptions.index_page_size and is consulted by page builders (e.g., BinaryPlainPageBuilder::is_page_full and buffer reservation logic) to decide when to finish a page and how much memory to reserve. A value of 0 disables the page-size limit in builders. Changing this value affects page count, metadata overhead, memory reservation and I/O/compression trade-offs (smaller pages → more pages and metadata; larger pages → fewer pages, potentially better compression but larger memory spikes).
- Introduced in: v3.2.4

### default_num_rows_per_column_file_block

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows that can be stored in each row block.
- Introduced in: -

### delete_worker_count_high_priority

- Default: 1
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of worker threads in the DeleteTaskWorkerPool that are allocated as HIGH-priority delete threads. On startup AgentServer creates the delete pool with total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority; the first delete_worker_count_high_priority threads are marked to exclusively try to pop TPriority::HIGH tasks (they poll for high-priority delete tasks and sleep/loop if none are available). Increasing this value increases concurrency for high-priority delete requests; decreasing it reduces dedicated capacity and may increase latency for high-priority deletes.
- Introduced in: v3.2.0

### dictionary_encoding_ratio

- Default: 0.7
- Type: Double
- Unit: -
- Is mutable: No
- Description: Fraction (0.0–1.0) used by StringColumnWriter during the encode-speculation phase to decide between dictionary (DICT_ENCODING) and plain (PLAIN_ENCODING) encoding for a chunk. The code computes max_card = row_count * `dictionary_encoding_ratio` and scans the chunk’s distinct key count; if the distinct count exceeds max_card the writer chooses PLAIN_ENCODING. The check is performed only when the chunk size passes `dictionary_speculate_min_chunk_size` (and when row_count > dictionary_min_rowcount). Setting the value higher favors dictionary encoding (tolerates more distinct keys); setting it lower causes earlier fallback to plain encoding. A value of 1.0 effectively forces dictionary encoding (distinct count can never exceed row_count).
- Introduced in: v3.2.0

### dictionary_encoding_ratio_for_non_string_column

- Default: 0
- Type: double
- Unit: -
- Is mutable: No
- Description: Ratio threshold used to decide whether to use dictionary encoding for non-string columns (numeric, date/time, decimal types). When enabled (value &gt; 0.0001) the writer computes max_card = row_count * dictionary_encoding_ratio_for_non_string_column and, for samples with row_count &gt; `dictionary_min_rowcount`, chooses DICT_ENCODING only if distinct_count ≤ max_card; otherwise it falls back to BIT_SHUFFLE. A value of `0` (default) disables non-string dictionary encoding. This parameter is analogous to `dictionary_encoding_ratio` but applies to non-string columns. Use values in (0,1] — smaller values restrict dictionary encoding to lower-cardinality columns and reduce dictionary memory/IO overhead.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### dictionary_page_size

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Size in bytes of dictionary pages used when building rowset segments. This value is read into `PageBuilderOptions::dict_page_size` in the BE rowset code and controls how many dictionary entries can be stored in a single dictionary page. Increasing this value can improve compression ratio for dictionary-encoded columns by allowing larger dictionaries, but larger pages consume more memory during write/encode and can increase I/O and latency when reading or materializing pages. Set conservatively for large-memory, write-heavy workloads and avoid excessively large values to prevent runtime performance degradation.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### disk_stat_monitor_interval

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to monitor health status of disks.
- Introduced in: -

### download_low_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The download speed lower limit of each HTTP request. An HTTP request aborts when it constantly runs with a lower speed than this value within the time span specified in the configuration item `download_low_speed_time`.
- Introduced in: -

### download_low_speed_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time that an HTTP request can run with a download speed lower than the limit. An HTTP request aborts when it constantly runs with a lower speed than the value of `download_low_speed_limit_kbps` within the time span specified in this configuration item.
- Introduced in: -

### download_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the download tasks of restore jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

### drop_tablet_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used to drop a tablet. `0` indicates half of the CPU cores in the node.
- Introduced in: -

### enable_check_string_lengths

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to check the data length during loading to solve compaction failures caused by out-of-bound VARCHAR data.
- Introduced in: -

### enable_event_based_compaction_framework

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Event-based Compaction Framework. `true` indicates Event-based Compaction Framework is enabled, and `false` indicates it is disabled. Enabling Event-based Compaction Framework can greatly reduce the overhead of compaction in scenarios where there are many tablets or a single tablet has a large amount of data.
- Introduced in: -

### enable_lazy_delta_column_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, compaction will prefer a "lazy" strategy for delta columns produced by partial column updates: StarRocks will avoid eagerly merging delta-column files back into their main segment files to save compaction I/O. In practice the compaction selection code checks for partial column-update rowsets and multiple candidates; if found and this flag is true, the engine will either stop adding further inputs to the compaction or only merge empty rowsets (level -1), leaving delta columns separate. This reduces immediate I/O and CPU during compaction at the cost of delayed consolidation (potentially more segments and temporary storage overhead). Correctness and query semantics are unchanged.
- Introduced in: v3.2.3

### enable_new_load_on_memory_limit_exceeded

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow new loading processes when the hard memory resource limit is reached. `true` indicates new loading processes will be allowed, and `false` indicates they will be rejected.
- Introduced in: v3.3.2

### enable_pk_index_parallel_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable parallel Compaction for Primary Key index in a shared-data cluster.
- Introduced in: -

### enable_pk_index_parallel_execution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable parallel execution for Primary Key index operations in a shared-data cluster. When enabled, the system uses a thread pool to process segments concurrently during publish operations, significantly improving performance for large tablets.
- Introduced in: -

### enable_pk_index_eager_build

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to eagerly build Primary Key index files during data import and compaction phases. When enabled, the system generates persistent PK index files immediately during data writes, improving subsequent query performance.
- Introduced in: -

### enable_pk_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy for Primary Key tables. `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: This item takes effect for shared-data clusters from v3.2.4 and v3.1.10 onwards, and for shared-nothing clusters from v3.2.5 and v3.1.10 onwards.

### enable_rowset_verify

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to verify the correctness of generated rowsets. When enabled, the correctness of the generated rowsets will be checked after Compaction and Schema Change.
- Introduced in: -

### enable_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy (excluding Primary Key tables). `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: -

### enable_strict_delvec_crc_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enable_strict_delvec_crc_check is set to true, we will perform a strict CRC32 check on the delete vector, and if a mismatch is detected, a failure will be returned.
- Introduced in: -

### enable_transparent_data_encryption

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, StarRocks will create encrypted on‑disk artifacts for newly written storage objects (segment files, delete/update files, rowset segments, lake SSTs, persistent index files, etc.). Writers (RowsetWriter/SegmentWriter, lake UpdateManager/LakePersistentIndex and related code paths) will request encryption info from the KeyCache, attach encryption_info to writable files and persist encryption_meta into rowset / segment / sstable metadata (segment_encryption_metas, delete/update encryption metadata). The Frontend and Backend/CN encryption flags must match — a mismatch causes the BE to abort on heartbeat (LOG(FATAL)). This flag is not runtime‑mutable; enable it before deployment and ensure key management (KEK) and KeyCache are properly configured and synchronized across the cluster.
- Introduced in: v3.3.1, 3.4.0, 3.5.0, 4.0.0

### enable_zero_copy_from_page_cache

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, FixedLengthColumnBase may avoid copying bytes when appending data that originates from a page-cache-backed buffer. In append_numbers the code will acquire the incoming ContainerResource and set the column's internal resource pointer (zero-copy) if all conditions are met: the config is true, the incoming resource is owned, the resource memory is aligned for the column element type, the column is empty, and the resource length is a multiple of the element size. Enabling this reduces CPU and memory-copy overhead and can improve ingestion/scan throughput. Drawbacks: it couples the column lifetime to the acquired buffer and relies on correct ownership/alignment; disable to force safe copying.
- Introduced in: -

### file_descriptor_cache_clean_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean file descriptors that have not been used for a certain period of time.
- Introduced in: -

### ignore_broken_disk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls startup behavior when configured storage paths fail read/write checks or fail to parse. When `false` (default), BE treats any broken entry in `storage_root_path` or `spill_local_storage_dir` as fatal and will abort startup. When `true`, StarRocks will skip (log a warning and remove) any storage path that fails `check_datapath_rw` or fails parsing so the BE can continue starting with the remaining healthy paths. Note: if all configured paths are removed, BE will still exit. Enabling this can mask misconfigured or failed disks and cause data on ignored paths to be unavailable; monitor logs and disk health accordingly.
- Introduced in: v3.2.0

### inc_rowset_expired_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the incoming data. This configuration item is used in incremental clone.
- Introduced in: -

### load_process_max_memory_hard_limit_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The hard limit (ratio) of memory resources that can be taken up by all load processes on a BE node. When `enable_new_load_on_memory_limit_exceeded` is set to `false`, and the memory consumption of all loading processes exceeds `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio`, new loading processes will be rejected.
- Introduced in: v3.3.2

### load_process_max_memory_limit_percent

- Default: 30
- Type: Int
- Unit: -
- Is mutable: No
- Description: The soft limit (in percentage) of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

### lz4_acceleration

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the LZ4 "acceleration" parameter used by the built-in LZ4 compressor (passed to LZ4_compress_fast_continue). Higher values prioritize compression speed at the cost of compression ratio; lower values (1) produce better compression but are slower. Valid range: MIN=1, MAX=65537. This setting affects all LZ4-based codecs in BlockCompression (e.g., LZ4 and Hadoop-LZ4) and only changes how compression is performed — it does not change the LZ4 format or decompression compatibility. Tune upward (e.g., 4, 8, ...) for CPU-bound or low-latency workloads where larger output is acceptable; keep at 1 for storage- or IO-sensitive workloads. Test with representative data before changing, since throughput vs. size trade-offs are highly data-dependent.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

### lz4_expected_compression_ratio

- Default: 2.1
- Type: double
- Unit: Dimensionless (compression ratio)
- Is mutable: Yes
- Description: Threshold used by the serialization compression strategy to judge whether observed LZ4 compression is "good". In compress_strategy.cpp this value divides the observed compress_ratio when computing a reward metric together with lz4_expected_compression_speed_mbps; if the combined reward `>` 1.0 the strategy records positive feedback. Increasing this value raises the expected compression ratio (making the condition harder to satisfy), while lowering it makes it easier for observed compression to be considered satisfactory. Tune to match typical data compressibility. Valid range: MIN=1, MAX=65537.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

### lz4_expected_compression_speed_mbps

- Default: 600
- Type: double
- Unit: MB/s
- Is mutable: Yes
- Description: Expected LZ4 compression throughput in megabytes per second used by the adaptive compression policy (CompressStrategy). The feedback routine computes a reward_ratio = (observed_compression_ratio / lz4_expected_compression_ratio) * (observed_speed / lz4_expected_compression_speed_mbps). A reward_ratio `>` 1.0 increments the positive counter (alpha), otherwise the negative counter (beta); this influences whether future data will be compressed. Tune this value to reflect typical LZ4 throughput on your hardware — raising it makes the policy harder to classify a run as "good" (requires higher observed speed), lowering it makes classification easier. Must be a positive finite number.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

### make_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the make snapshot tasks on a BE node.
- Introduced in: -

### manual_compaction_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of threads for Manual Compaction.
- Introduced in: -

### max_base_compaction_num_singleton_deltas

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be compacted in each Base Compaction.
- Introduced in: -

### max_compaction_candidate_num

- Default: 40960
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of candidate tablets for compaction. If the value is too large, it will cause high memory usage and high CPU load.
- Introduced in: -

### max_compaction_concurrency

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compactions (including both Base Compaction and Cumulative Compaction). The value `-1` indicates that no limit is imposed on the concurrency. `0` indicates disabling compaction. This parameter is mutable when the Event-based Compaction Framework is enabled.
- Introduced in: -

### max_cumulative_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be merged in a single Cumulative Compaction. You can reduce this value if OOM occurs during compaction.
- Introduced in: -

### max_download_speed_kbps

- Default: 50000
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The maximum download speed of each HTTP request. This value affects the performance of data replica synchronization across BE nodes.
- Introduced in: -

### max_garbage_sweep_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

### max_percentage_of_error_disk

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum percentage of error that is tolerable in a storage volume before the corresponding BE node quits.
- Introduced in: -

### max_queueing_memtable_per_tablet

- Default: 2
- Type: Long
- Unit: Count
- Is mutable: Yes
- Description: Controls per-tablet backpressure for write paths: when a tablet's number of queueing (not-yet-flushing) memtables reaches or exceeds `max_queueing_memtable_per_tablet`, writers in `LocalTabletsChannel` and `LakeTabletsChannel` will block (sleep/retry) before submitting more write work. This reduces simultaneous memtable flush concurrency and peak memory use at the cost of increased latency or RPC timeouts for heavy load. Set higher to allow more concurrent memtables (more memory and I/O burst); set lower to limit memory pressure and increase write throttling.
- Introduced in: v3.2.0

### max_row_source_mask_memory_bytes

- Default: 209715200
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size of the row source mask buffer. When the buffer is larger than this value, data will be persisted to a temporary file on the disk. This value should be set lower than the value of `compaction_memory_limit_per_worker`.
- Introduced in: -

### max_tablet_write_chunk_bytes

- Default: 536870912
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Maximum allowed memory (in bytes) for the current in-memory tablet write chunk before it is treated as full and enqueued for sending. Increase this value to reduce the frequency of RPCs when loading wide tables (many columns), which can improve throughput at the cost of higher memory usage and larger RPC payloads. Tune to balance fewer RPCs against memory and serialization/BRPC limits.
- Introduced in: v3.2.12

### max_update_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rowsets that can be merged in a single Compaction for Primary Key tables.
- Introduced in: -

### memory_limitation_per_thread_for_schema_change

- Default: 2
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: The maximum memory size allowed for each schema change task.
- Introduced in: -

### memory_ratio_for_sorting_schema_change

- Default: 0.8
- Type: Double
- Unit: - (unitless ratio)
- Is mutable: Yes
- Description: Fraction of the per-thread schema-change memory limit used as the memtable maximum buffer size during sorting schema-change operations. The ratio is multiplied by memory_limitation_per_thread_for_schema_change (configured in GB and converted to bytes) to compute max_buffer_size, and that result is capped at 4GB. Used by SchemaChangeWithSorting and SortedSchemaChange when creating MemTable/DeltaWriter. Increasing this ratio allows larger in-memory buffers (fewer flushes/merges) but raises risk of memory pressure; reducing it causes more frequent flushes and higher I/O/merge overhead.
- Introduced in: v3.2.0

### min_base_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments that trigger a Base Compaction.
- Introduced in: -

### min_compaction_failure_interval_sec

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which a tablet compaction can be scheduled since the previous compaction failure.
- Introduced in: -

### min_cumulative_compaction_failure_interval_sec

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which Cumulative Compaction retries upon failures.
- Introduced in: -

### min_cumulative_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments to trigger Cumulative Compaction.
- Introduced in: -

### min_garbage_sweep_interval

- Default: 180
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

### parallel_clone_task_per_path

- Default: 8
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Number of parallel clone worker threads allocated per storage path on a BE. At BE startup the clone thread-pool max threads is computed as max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL). For example, with 4 storage paths and default=8 the clone pool max = 32. This setting directly controls concurrency of CLONE tasks (tablet replica copies) handled by the BE: increasing it raises parallel clone throughput but also increases CPU, disk and network contention; decreasing it limits simultaneous clone tasks and can throttle FE-scheduled clone operations. The value is applied to the dynamic clone thread pool and can be changed at runtime via the update-config path (causes agent_server to update the clone pool max threads).
- Introduced in: v3.2.0

### partial_update_memory_limit_per_worker

- Default: 2147483648
- Type: long
- Unit: Bytes
- Is mutable: Yes
- Description: Maximum memory (in bytes) a single worker may use for assembling a source chunk when performing partial column updates (used in compaction / rowset update processing). The reader estimates per-row update memory (total_update_row_size / num_rows_upt) and multiplies it by the number of rows read; when that product exceeds this limit the current chunk is flushed and processed to avoid additional memory growth. Set this to match the available memory per update worker—too low increases I/O/processing overhead (many small chunks); too high risks memory pressure or OOM. If the per-row estimate is zero (legacy rowsets), this config does not impose a byte-based limit (only the INT32_MAX row count limit applies).
- Introduced in: v3.2.10

### path_gc_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, StorageEngine starts per-data-dir background threads that perform periodic path scanning and garbage collection. On startup `start_bg_threads()` spawns `_path_scan_thread_callback` (calls `DataDir::perform_path_scan` and `perform_tmp_path_scan`) and `_path_gc_thread_callback` (calls `DataDir::perform_path_gc_by_tablet`, `DataDir::perform_path_gc_by_rowsetid`, `DataDir::perform_delta_column_files_gc`, and `DataDir::perform_crm_gc`). The scan and GC intervals are controlled by `path_scan_interval_second` and `path_gc_check_interval_second`; CRM file cleanup uses `unused_crm_file_threshold_second`. Disable this to prevent automatic path-level cleanup (you must then manage orphaned/temp files manually). Changing this flag requires restarting the process.
- Introduced in: v3.2.0

### path_gc_check_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Interval in seconds between runs of the storage engine's path garbage-collection background thread. Each wake triggers DataDir to perform path GC by tablet, by rowset id, delta column file GC and CRM GC (the CRM GC call uses `unused_crm_file_threshold_second`). If set to a non-positive value the code forces the interval to 1800 seconds (half hour) and emits a warning. Tune this to control how frequently on-disk temporary or downloaded files are scanned and removed.
- Introduced in: v3.2.0

### pending_data_expire_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the pending data in the storage engine.
- Introduced in: -

### pindex_major_compaction_limit_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks.
- Introduced in: v3.0.9

### pk_index_compaction_score_ratio

- Default: 1.5
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Compaction score ratio for Primary Key index in a shared-data cluster. For example, if there are N filesets, the Compaction score will be `N * pk_index_compaction_score_ratio`.
- Introduced in: -

### pk_index_early_sst_compaction_threshold

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: early sst compaction threshold for primary key index in a shared-data cluster.
- Introduced in: -

### pk_index_map_shard_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of shards used by the Primary Key index shard map in the lake UpdateManager. UpdateManager allocates a vector of `PkIndexShard` of this size and maps a tablet ID to a shard via a bitmask. Increasing this value reduces lock contention among tablets that would otherwise share the same shard, at the cost of more mutex objects and slightly higher memory usage. The value must be a power of two because the code relies on bitmask indexing. For sizing guidance see `tablet_map_shard_size` heuristic: `total_num_of_tablets_in_BE / 512`.
- Introduced in: v3.2.0

### pk_index_memtable_flush_threadpool_max_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for Primary Key index MemTable flush in a shared-data cluster. `0` means automatically set to half of the number of CPU cores.
- Introduced in: -

### pk_index_memtable_flush_threadpool_size

- Default: 1048576
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the maximum queue size (number of pending tasks) for the Primary Key index memtable flush thread pool used in shared-data (cloud-native / lake) mode. The thread pool is created as "cloud_native_pk_index_flush" in ExecEnv; its max thread count is governed by `pk_index_memtable_flush_threadpool_max_threads`. Increasing this value permits more memtable flush tasks to be buffered before execution, which can reduce immediate backpressure but increases memory consumed by queued task objects. Decreasing it limits buffered tasks and can cause earlier backpressure or task rejections depending on thread-pool behavior. Tune according to available memory and expected concurrent flush workload.
- Introduced in: -

### pk_index_memtable_max_count

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of MemTables for Primary Key index in a shared-data cluster.
- Introduced in: -

### pk_index_memtable_max_wait_flush_timeout_ms

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The maximum timeout for waiting for Primary Key index MemTable flush completion in a shared-data cluster. When synchronously flushing all MemTables (for example, before an ingest SST operation), the system waits up to this timeout. The default is 30 seconds.
- Introduced in: -

### pk_index_parallel_compaction_task_split_threshold_bytes

- Default: 33554432
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The splitting threshold for Primary Key index Compaction tasks. When the total size of the files involved in a task is smaller than this threshold, the task will not be split.
- Introduced in: -

### pk_index_parallel_compaction_threadpool_max_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for cloud native Primary Key index parallel Compaction in a shared-data cluster. `0` means automatically set to half of the number of CPU cores.
- Introduced in: -

### pk_index_parallel_compaction_threadpool_size

- Default: 1048576
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum queue size (number of pending tasks) for the thread pool used by cloud-native Primary Key index parallel compaction in shared-data mode. This setting controls how many Compaction tasks can be enqueued before the thread pool rejects new submissions. The effective parallelism is bounded by `pk_index_parallel_compaction_threadpool_max_threads`; increase this value to avoid task rejections when you expect many concurrent Compaction tasks, but be aware larger queues can increase memory and latency for queued work.
- Introduced in: -

### pk_index_parallel_execution_min_rows

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum rows threshold to enable parallel execution for Primary Key index operations in a shared-data cluster.
- Introduced in: -

### pk_index_parallel_execution_threadpool_max_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for Primary Key index parallel execution in a shared-data cluster. `0` means automatically set to half of the number of CPU cores.
- Introduced in: -

### pk_index_parallel_load_dels_mem_ratio

- Default: 50
- Type: Int
- Unit: percent (0-100)
- Is mutable: Yes
- Description: In a shared-data cluster, skip the parallel two-phase delete-file prefetch in `LakePersistentIndex::load_dels` when the update mem tracker is already past this percent of its limit. In that regime the function falls back to a single-pass loop that holds only one decoded del-file column at a time, trading the cold-start latency win for bounded peak memory. Set to a higher value to allow the optimization under more memory pressure; set to `100` to disable the memory gate (always run the parallel path when `enable_pk_index_parallel_execution=true` and there are multiple del files).
- Introduced in: -

### lake_partial_update_thread_pool_max_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for lake partial update segment-level parallelism in a shared-data cluster. This thread pool is used by both row-mode and column-mode partial updates to parallelize I/O-heavy segment operations (load_segment + rewrite_segment for row-mode, DCG generation for column-mode). `0` means automatically set to half of the number of CPU cores. Runtime on/off is controlled by `enable_pk_index_parallel_execution`.
- Introduced in: v4.1

### lake_partial_update_thread_pool_queue_size

- Default: 2048
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The task queue size for the lake partial update thread pool.
- Introduced in: v4.1

### pk_index_size_tiered_level_multiplier

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The level multiplier parameter for Primary Key index size-tiered Compaction strategy.
- Introduced in: -

### pk_index_size_tiered_max_level

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum level for Primary Key index size-tiered Compaction strategy.
- Introduced in: -

### pk_index_size_tiered_min_level_size

- Default: 131072
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum level for Primary Key index size-tiered Compaction strategy.
- Introduced in: -

### pk_index_sstable_sample_interval_bytes

- Default: 16777216
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The sampling interval size for SSTable files in a shared-data cluster. When the size of an SSTable file exceeds this threshold, the system samples keys from the SSTable at this interval to optimize the boundary partitioning of Compaction tasks. For SSTables smaller than this threshold, only the start key is used as the boundary key. The default is 16 MB.
- Introduced in: -

### pk_index_target_file_size

- Default: 67108864
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The target file size for Primary Key index in a shared-data cluster.
- Introduced in: -

### pk_index_eager_build_threshold_bytes

- Default: 104857600
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: When `enable_pk_index_eager_build` is set to true, the system will eagerly build PK index files only if the data generated during import or compaction exceeds this threshold. Default is 100MB.
- Introduced in: -

### primary_key_limit_size

- Default: 128
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a key column in Primary Key tables.
- Introduced in: v2.5

### release_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the release snapshot tasks on a BE node.
- Introduced in: -

### repair_compaction_interval_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval to poll Repair Compaction threads.
- Introduced in: -

### replication_max_speed_limit_kbps

- Default: 50000
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The maximum speed of each replication thread.
- Introduced in: v3.3.5

### replication_min_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The minimum speed of each replication thread.
- Introduced in: v3.3.5

### replication_min_speed_time_seconds

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration allowed for a replication thread to be under the minimum speed. Replication will fail if the time when the actual speed is lower than `replication_min_speed_limit_kbps` exceeds this value.
- Introduced in: v3.3.5

### replication_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for replication. `0` indicates setting the thread number to four times the BE CPU core count.
- Introduced in: v3.3.5

### size_tiered_level_multiple

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The multiple of data size between two contiguous levels in the Size-tiered Compaction policy.
- Introduced in: -

### size_tiered_level_multiple_dupkey

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In the Size-tiered Compaction policy, the multiple of the data amount difference between two adjacent levels for Duplicate Key tables.
- Introduced in: -

### size_tiered_level_num

- Default: 7
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of levels for the Size-tiered Compaction policy. At most one rowset is reserved for each level. Therefore, under a stable condition, there are, at most, as many rowsets as the level number specified in this configuration item.
- Introduced in: -

### size_tiered_max_compaction_level

- Default: 3
- Type: Int
- Unit: Levels
- Is mutable: Yes
- Description: Limits how many size-tiered levels may be merged into a single primary-key real-time compaction task. During the PK size-tiered compaction selection, StarRocks builds ordered "levels" of rowsets by size and will add successive levels into the chosen compaction input until this limit is reached (the code uses compaction_level `<=` size_tiered_max_compaction_level). The value is inclusive and counts the number of distinct size tiers merged (the top level is counted as 1). Effective only when the PK size-tiered compaction strategy is enabled; raising it lets a compaction task include more levels (larger, more I/O- and CPU-intensive merges, potential higher write amplification), while lowering it restricts merges and reduces task size and resource usage.
- Introduced in: v4.0.0

### size_tiered_min_level_size

- Default: 131072
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The data size of the minimum level in the Size-tiered Compaction policy. Rowsets smaller than this value immediately trigger the data compaction.
- Introduced in: -

### small_dictionary_page_size

- Default: 4096
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Threshold (in bytes) used by BinaryPlainPageDecoder to decide whether to eagerly parse a dictionary (binary/plain) page. If a page's encoded size is &lt; `small_dictionary_page_size`, the decoder pre-parses all string entries into an in-memory vector (`_parsed_datas`) to accelerate random access and batch reads. Raising this value causes more pages to be pre-parsed (which can reduce per-access decoding overhead and may increase effective compression for larger dictionaries) but increases memory usage and CPU spent parsing; excessively large values can degrade overall performance. Tune only after measuring memory and access-latency trade-offs.
- Introduced in: v3.4.1, v3.5.0

### snapshot_expire_time_sec

- Default: 172800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of snapshot files.
- Introduced in: -

### stale_memtable_flush_time_sec

- Default: 0
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: When a sender job's memory usage is high, memtables that have not been updated for longer than `stale_memtable_flush_time_sec` seconds will be flushed to reduce memory pressure. This behavior is only considered when memory limits are approaching (`limit_exceeded_by_ratio(70)` or higher). In `LocalTabletsChannel`, an additional path at very high memory usage (`limit_exceeded_by_ratio(95)`) may flush memtables whose size exceeds `write_buffer_size / 4`. A value of `0` disables this age-based stale-memtable flushing (immutable-partition memtables still flush immediately when idle or on high memory).
- Introduced in: v3.2.0

### storage_flood_stage_left_capacity_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in all BE directories. If the remaining storage space of the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_flood_stage_usage_percent`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_reserve_bytes` to allow the configurations to take effect.
- Introduced in: -

### storage_flood_stage_usage_percent

- Default: 95
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in all BE directories. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_flood_stage_left_capacity_bytes`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_percent` to allow the configurations to take effect.
- Introduced in: -

### storage_high_usage_disk_protect_ratio

- Default: 0.1
- Type: double
- Unit: -
- Is mutable: Yes
- Description: When selecting a storage root for tablet creation, StorageEngine sorts candidate disks by `disk_usage(0)` and computes the average usage. Any disk whose usage is greater than (average usage + `storage_high_usage_disk_protect_ratio`) is excluded from the preferential selection pool (it will not participate in the randomized, preferrential shuffle and thus is deferred from being chosen initially). Set to 0 to disable this protection. Values are fractional (typical range 0.0–1.0); larger values make the scheduler more tolerant of higher-than-average disks.
- Introduced in: v3.2.0

### storage_medium_migrate_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for storage medium migration (from SATA to SSD).
- Introduced in: -

### storage_root_path

- Default: `${STARROCKS_HOME}/storage`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory and medium of the storage volume. Example: `/data1,medium:hdd;/data2,medium:ssd`.
  - Multiple volumes are separated by semicolons (`;`).
  - If the storage medium is SSD, add `,medium:ssd` at the end of the directory.
  - If the storage medium is HDD, add `,medium:hdd` at the end of the directory.
- Introduced in: -

### sync_tablet_meta

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the synchronization of the tablet metadata. `true` indicates enabling synchronization, and `false` indicates disabling it.
- Introduced in: -

### tablet_map_shard_size

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The tablet map shard size. The value must be a power of two.
- Introduced in: -

### tablet_max_pending_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending versions that are tolerable on a Primary Key tablet. Pending versions refer to versions that are committed but not applied yet.
- Introduced in: -

### tablet_max_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of versions allowed on a tablet. If the number of versions exceeds this value, new write requests will fail.
- Introduced in: -

### tablet_meta_checkpoint_min_interval_secs

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a TabletMeta Checkpoint.
- Introduced in: -

### tablet_meta_checkpoint_min_new_rowsets_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of rowsets to create since the last TabletMeta Checkpoint.
- Introduced in: -

### tablet_rowset_stale_sweep_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to sweep the stale rowsets in tablets.
- Introduced in: -

### tablet_stat_cache_update_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: 是
- Description: The time interval at which Tablet Stat Cache updates.
- Introduced in: -

### lake_enable_accurate_pk_row_count

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to use accurate row counts for lake primary-key tablets. When enabled, StarRocks reads each rowset's delete vector from object storage and subtracts deleted rows, producing more accurate stats but potentially increasing `get_tablet_stats` RPC overhead. When disabled, StarRocks uses the approximate `num_dels` value in rowset metadata to avoid remote I/O, which may slightly overcount rows that were deleted but not yet compacted.
- Introduced in: -

### lake_tablet_stat_slow_log_ms

- Default: 300000
- Type: Int64
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in milliseconds) for logging slow tablet-stat collection tasks. If a single tablet stat task exceeds this value, StarRocks emits a warning log with diagnostics such as `tablet_id`, version, rowset count, accurate mode, and elapsed time.
- Introduced in: -

### lake_metadata_fetch_thread_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The count of threads for shared-data table tablet metadata fetch operations (e.g., `get_tablet_stats`, `get_tablet_metadatas`).
- Introduced in: v3.5.16, v4.0.9

### tablet_writer_open_rpc_timeout_sec

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Timeout (in seconds) for the RPC that opens a tablet writer on a remote BE. The value is converted to milliseconds and applied to both the request timeout and the brpc control timeout when issuing the open call. The runtime uses the effective timeout as the minimum of `tablet_writer_open_rpc_timeout_sec` and half of the overall load timeout (i.e., min(`tablet_writer_open_rpc_timeout_sec`, `load_timeout_sec` / 2)). Set this to balance timely failure detection (too small may cause premature open failures) and giving BEs enough time to initialize writers (too large delays error handling).
- Introduced in: v3.2.0

### transaction_apply_worker_count

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Controls the maximum number of worker threads used by the UpdateManager's "update_apply" thread pool — the pool that applies rowsets for transactions (notably for primary-key tables). A value `>0` sets a fixed maximum thread count; 0 (the default) makes the pool size equal to the number of CPU cores. The configured value is applied at startup (UpdateManager::init) and can be changed at runtime via the update-config HTTP action, which updates the pool's max threads. Tune this to increase apply concurrency (throughput) or limit CPU/memory contention; min threads and idle timeout are governed by transaction_apply_thread_pool_num_min and transaction_apply_worker_idle_time_ms respectively.
- Introduced in: v3.2.0

### transaction_apply_worker_idle_time_ms

- Default: 500
- Type: int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the idle timeout (in milliseconds) for the UpdateManager's "update_apply" thread pool used to apply transactions/updates. The value is passed to ThreadPoolBuilder::set_idle_timeout via MonoDelta::FromMilliseconds, so worker threads that remain idle longer than this timeout may be terminated (subject to the pool's configured minimum thread count and max threads). Lower values free resources faster but increase thread creation/teardown overhead under bursty load; higher values keep workers warm for short bursts at the cost of higher baseline resource usage.
- Introduced in: v3.2.11

### trash_file_expire_time_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean trash files. The default value has been changed from 259,200 to 86,400 since v2.5.17, v3.0.9, and v3.1.6.
- Introduced in: -

### unused_rowset_monitor_interval

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean the expired rowsets.
- Introduced in: -

### update_cache_expire_sec

- Default: 360
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of Update Cache.
- Introduced in: -

### update_compaction_check_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to check compaction for Primary Key tables.
- Introduced in: -

### update_compaction_delvec_file_io_amp_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Used to control the priority of compaction for rowsets that contain Delvec files in Primary Key tables. The larger the value, the higher the priority.
- Introduced in: -

### update_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of Compaction threads per disk for Primary Key tables.
- Introduced in: -

### update_compaction_per_tablet_min_interval_seconds

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which compaction is triggered for each tablet in a Primary Key table.
- Introduced in: -

### update_compaction_ratio_threshold

- Default: 0.5
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The maximum proportion of data that a compaction can merge for a Primary Key table in a shared-data cluster. It is recommended to shrink this value if a single tablet becomes excessively large.
- Introduced in: v3.1.5

### update_compaction_result_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum result size of a single compaction for Primary Key tables.
- Introduced in: -

### update_compaction_size_threshold

- Default: 268435456
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score of Primary Key tables is calculated based on the file size, which is different from other table types. This parameter can be used to make the Compaction Score of Primary Key tables similar to that of other table types, making it easier for users to understand.
- Introduced in: -

### upload_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the upload tasks of backup jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

### vertical_compaction_max_columns_per_group

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of columns per group of Vertical Compactions.
- Introduced in: -
