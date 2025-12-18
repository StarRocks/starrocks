---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration

<BEConfigMethod />

<CNConfigMethod />


## View BE configuration items

You can view the BE configuration items using the following command:

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## Configure BE parameters

<PostBEConfig />

<StaticBEConfigNote />

## Understand BE Parameters

### Logging

##### diagnose_stack_trace_interval_ms

- Default: 1800000 (30 minutes)
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Controls the minimum time gap between successive stack-trace diagnostics performed by DiagnoseDaemon for `STACK_TRACE` requests. When a diagnose request arrives, the daemon skips collecting and logging stack traces if the last collection happened less than `diagnose_stack_trace_interval_ms` milliseconds ago. Increase this value to reduce CPU overhead and log volume from frequent stack dumps; decrease it to capture more frequent traces to debug transient issues (for example, in load fail-point simulations of long `TabletsChannel::add_chunk` blocking).
- Introduced in: v3.5.0

##### load_rpc_slow_log_frequency_threshold_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls how frequently the system prints slow-log entries for load RPCs that exceed their configured RPC timeout. The slow-log also includes the load channel runtime profile. Setting this value to 0 causes per-timeout logging in practice.
- Introduced in: v3.4.3, v3.5.0

##### log_buffer_level

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The strategy for flushing logs. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffered in memory.
- Introduced in: -

##### pprof_profile_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory path where StarRocks writes pprof artifacts (Jemalloc heap snapshots and gperftools CPU profiles).
- Introduced in: v3.2.0

##### sys_log_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL).
- Introduced in: -

##### sys_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: Yes (from v3.3.0, v3.2.7, and v3.1.12)
- Description: The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL. This item was changed to a dynamic configuration from v3.3.0, v3.2.7, and v3.1.12 onwards.
- Introduced in: -

##### sys_log_roll_mode

- Default: SIZE-MB-1024
- Type: String
- Unit: -
- Is mutable: No
- Description: The mode in which system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-`size. The default value indicates that logs are segmented into rolls, each of which is 1 GB.
- Introduced in: -

##### sys_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of log rolls to reserve.
- Introduced in: -

##### sys_log_timezone

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to show timezone information in the log prefix. `true` indicates to show timezone information, `false` indicates not to show.
- Introduced in: -

##### sys_log_verbose_level

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The level of the logs to be printed. This configuration item is used to control the output of logs initiated with VLOG in codes.
- Introduced in: -

##### sys_log_verbose_modules

- Default: 
- Type: Strings
- Unit: -
- Is mutable: No
- Description: The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including `starrocks`, `starrocks::debug`, `starrocks::fs`, `starrocks::io`, `starrocks::lake`, `starrocks::pipeline`, `starrocks::query_cache`, `starrocks::stream`, and `starrocks::workgroup`.
- Introduced in: -

### Server

##### abort_on_large_memory_allocation

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When a single allocation request exceeds the configured large-allocation threshold (g_large_memory_alloc_failure_threshold `>` 0 and requested size `>` threshold), this flag controls how the process responds. If true, StarRocks calls std::abort() immediately (hard crash) when such a large allocation is detected. If false, the allocation is blocked and the allocator returns failure (nullptr or ENOMEM) so callers can handle the error. This check only takes effect for allocations that are not wrapped with the TRY_CATCH_BAD_ALLOC path (the mem hook uses a different flow when bad-alloc is being caught). Enable for fail-fast debugging of unexpected huge allocations; keep disabled in production unless you want an immediate process abort on over-large allocation attempts.
- Introduced in: v3.4.3, 3.5.0, 4.0.0

##### arrow_flight_port

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: TCP port for the BE Arrow Flight SQL server. `-1` indicaes to disable the Arrow Flight service. On non-macOS builds, BE invokes Arrow Flight SQL Server with this port during startup; if the port is unavailable, the server startup fails and the BE process exits. The configured port is reported to the FE in the heartbeat payload.
- Introduced in: v3.4.0, v3.5.0

##### be_exit_after_disk_write_hang_second

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The length of time that the BE waits to exit after the disk hangs.
- Introduced in: -

##### be_http_num_workers

- Default: 48
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used by the HTTP server.
- Introduced in: -

##### be_http_port

- Default: 8040
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE HTTP server port.
- Introduced in: -

##### be_port

- Default: 9060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE thrift server port, which is used to receive requests from FEs.
- Introduced in: -

##### be_service_threads

- Default: 64
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of worker threads the BE Thrift server uses to serve backend RPC/execution requests. This value is passed to ThriftServer when creating the BackendService and controls how many concurrent request handlers are available; requests are queued when all worker threads are busy. Tune based on expected concurrent RPC load and available CPU/memory: increasing it raises concurrency but also per-thread memory and context-switch cost, decreasing it limits parallel handling and may increase request latency.
- Introduced in: v3.2.0

##### brpc_connection_type

- Default: `"single"`
- Type: string
- Unit: -
- Is mutable: No
- Description: The bRPC channel connection mode. Valid values:
  - `"single"` (Default): One persistent TCP connection for each channel.
  - `"pooled"`: A pool of persistent connections for higher concurrency at the cost of more sockets/file descriptors.
  - `"short"`: Short‑lived connections created per RPC to reduce persistent resource usage but with higher latency.
  The choice affects per-socket buffering behavior and can influence `Socket.Write` failures (EOVERCROWDED) when unwritten bytes exceed socket limits.
- Introduced in: v3.2.5

##### brpc_max_body_size

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum body size of a bRPC.
- Introduced in: -

##### brpc_max_connections_per_server

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of persistent bRPC connections the client keeps for each remote server endpoint. For each endpoint `BrpcStubCache` creates a `StubPool` whose `_stubs` vector is reserved to this size. On first accesses, new stubs are created until the limit is reached. After that, existing stubs are returned in a round‑robin fashion. Increasing this value raises per‑endpoint concurrency (reduces contention on a single channel) at the cost of more file descriptors, memory, and channels.
- Introduced in: v3.2.0

##### brpc_num_threads

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of bthreads of a bRPC. The value `-1` indicates the same number with the CPU threads.
- Introduced in: -

##### brpc_port

- Default: 8060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE bRPC port, which is used to view the network statistics of bRPCs.
- Introduced in: -

##### brpc_socket_max_unwritten_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the per-socket limit for unwritten outbound bytes in the bRPC server. When the amount of buffered, not-yet-written data on a socket reaches this limit, subsequent `Socket.Write` calls fail with EOVERCROWDED. This prevents unbounded per-connection memory growth but can cause RPC send failures for very large messages or slow peers. Align this value with `brpc_max_body_size` to ensure single-message bodies are not larger than the allowed unwritten buffer. Increasing the value raises memory usage per connection.
- Introduced in: v3.2.0

##### brpc_stub_expire_s

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expire time of bRPC stub cache. The default value is 60 minutes.
- Introduced in: -

##### compress_rowbatches

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to compress the row batches in RPCs between BEs. `true` indicates compressing the row batches, and `false` indicates not compressing them.
- Introduced in: -

##### consistency_max_memory_limit_percent

- Default: 20
- Type: Int
- Unit: -
- Is mutable: No
- Description: Percentage cap used to compute the memory budget for consistency-related tasks. During BE startup, the final consistency limit is computed as the minimum of the value parsed from `consistency_max_memory_limit` (bytes) and (`process_mem_limit * consistency_max_memory_limit_percent / 100`). If `process_mem_limit` is unset (-1), consistency memory is considered unlimited. For `consistency_max_memory_limit_percent`, values less than 0 or greater than 100 are treated as 100. Adjusting this value increases or decreases memory reserved for consistency operations and therefore affects memory available for queries and other services.
- Introduced in: v3.2.0

##### delete_worker_count_normal_priority

- Default: 2
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of normal-priority worker threads dedicated to handling delete (REALTIME_PUSH with DELETE) tasks on the BE agent. At startup this value is added to delete_worker_count_high_priority to size the DeleteTaskWorkerPool (see agent_server.cpp). The pool assigns the first delete_worker_count_high_priority threads as HIGH priority and the rest as NORMAL; normal-priority threads process standard delete tasks and contribute to overall delete throughput. Increase to raise concurrent delete capacity (higher CPU/IO usage); decrease to reduce resource contention.
- Introduced in: v3.2.0

##### disable_mem_pools

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to disable MemPool. When this item is set to `true`, the MemPool chunk pooling is disabled so each allocation gets its own sized chunk instead of reusing or increasing pooled chunks. Disabling pooling reduces long-lived retained buffer memory at the cost of more frequent allocations, increased number of chunks, and skipped integrity checks (which are avoided because of the large chunk count). Keep `disable_mem_pools` as `false` (default) to benefit from allocation reuse and fewer system calls. Set it to `true` only when you must avoid large pooled memory retention (for example, low-memory environments or diagnostic runs).
- Introduced in: v3.2.0

##### enable_https

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the BE's bRPC server is configured to use TLS: `ServerOptions.ssl_options` will be populated with the certificate and private key specified by `ssl_certificate_path` and `ssl_private_key_path` at BE startup. This enables HTTPS/TLS for incoming bRPC connections; clients must connect using TLS. Ensure the certificate and key files exist, are accessible to the BE process, and match bRPC/SSL expectations.
- Introduced in: v4.0.0

##### enable_jemalloc_memory_tracker

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the BE starts a background thread (jemalloc_tracker_daemon) that polls jemalloc statistics (once per second) and updates the GlobalEnv jemalloc metadata MemTracker with the jemalloc "stats.metadata" value. This ensures jemalloc metadata consumption is included in StarRocks process memory accounting and prevents under‑reporting of memory used by jemalloc internals. The tracker is only compiled/started on non‑macOS builds (#ifndef __APPLE__) and runs as a daemon thread named "jemalloc_tracker_daemon". Because this setting affects startup behaviour and threads that maintain MemTracker state, changing it requires a restart. Disable only if jemalloc is not used or when jemalloc tracking is intentionally managed differently; otherwise keep enabled to maintain accurate memory accounting and allocation safeguards.
- Introduced in: v3.2.12

##### enable_jvm_metrics

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the system initializes and registers JVM-specific metrics at startup. When enabled the metrics subsystem will create JVM-related collectors (for example, heap, GC and thread metrics) for export, and when disabled, those collectors are not initialized. This parameter is intended for forward compatibility and may be removed in a future release. Use `enable_system_metrics` to control system-level metric collection.
- Introduced in: v4.0.0

##### get_pindex_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the number of worker threads for the "get_pindex" thread pool in UpdateManager, which is used to load / fetch persistent index data (used when applying rowsets for primary-key tables). At runtime, a config update will adjust the pool's maximum threads: if `>0` that value is applied; if 0 the runtime callback uses the number of CPU cores (CpuInfo::num_cores()). On initialization the pool's max threads is computed as max(get_pindex_worker_count, max_apply_thread_cnt * 2) where max_apply_thread_cnt is the apply-thread pool maximum. Increase to raise parallelism for pindex loading; lowering reduces concurrency and memory/CPU usage.
- Introduced in: v3.2.0

##### heartbeat_service_port

- Default: 9050
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE heartbeat service port, which is used to receive heartbeats from FEs.
- Introduced in: -

##### heartbeat_service_thread_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The thread count of the BE heartbeat service.
- Introduced in: -

##### local_library_dir

- Default: `${UDF_RUNTIME_DIR}`
- Type: string
- Unit: -
- Is mutable: No
- Description: Local directory on the BE where UDF (user-defined function) libraries are staged and where Python UDF worker processes operate. StarRocks copies UDF libraries from HDFS into this path, creates per-worker Unix domain sockets at `<local_library_dir>/pyworker_<pid>`, and chdirs Python worker processes into this directory before exec. The directory must exist, be writable by the BE process, and reside on a filesystem that supports Unix domain sockets (i.e., a local filesystem). Because this config is immutable at runtime, set it before startup and ensure adequate permissions and disk space on each BE.
- Introduced in: v3.2.0

##### mem_limit

- Default: 90%
- Type: String
- Unit: -
- Is mutable: No
- Description: BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100G"). The default hard limit is 90% of the server's memory size, and the soft limit is 80%. You need to configure this parameter if you want to deploy StarRocks with other memory-intensive services on a same server.
- Introduced in: -

##### memory_max_alignment

- Default: 16
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum byte alignment that MemPool will accept for aligned allocations. Increase this value only when callers require larger alignment (for SIMD, device buffers, or ABI constraints). Larger values increase per-allocation padding and reserved memory waste and must remain within what the system allocator and platform support.
- Introduced in: v3.2.0

##### memory_urgent_level

- Default: 85
- Type: long
- Unit: Percentage (0-100)
- Is mutable: Yes
- Description: The emergency memory water‑level expressed as a percentage of the process memory limit. When process memory consumption exceeds `(limit * memory_urgent_level / 100)`, BE triggers immediate memory reclamation, which forces data cache shrinkage, evicts update caches, and causes persistent/lake MemTables to be treated as "full" so they will be flushed/compacted soon. The code validates that this setting must be greater than `memory_high_level`, and `memory_high_level` must be greater or equal to `1`, and less thant or equal to `100`). A lower value causes more aggressive, earlier reclamation, that is, more frequent cache evictions and flushes. A higher value delays reclamation and risks OOM if too close to 100. Tune this item together with `memory_high_level` and Data Cache-related auto‑adjust settings.
- Introduced in: v3.2.0

##### net_use_ipv6_when_priority_networks_empty

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

##### num_cores

- Default: 0
- Type: Int
- Unit: Cores
- Is mutable: No
- Description: Controls the number of CPU cores the system will use for CPU-aware decisions (for example, thread-pool sizing and runtime scheduling). A value of 0 enables auto-detection: the system reads `/proc/cpuinfo` and uses all available cores. If set to a positive integer, that value overrides the detected core count and becomes the effective core count. When running inside containers, cgroup cpuset or cpu quota settings can further restrict usable cores; `CpuInfo` also respects those cgroup limits.
- Introduced in: v3.2.0

##### priority_networks

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as `10.10.10.0/24`. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

##### rpc_compress_ratio_threshold

- Default: 1.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Threshold (uncompressed_size / compressed_size) used when deciding whether to send serialized row-batches over the network in compressed form. When compression is attempted (e.g., in DataStreamSender, exchange sink, tablet sink index channel, dictionary cache writer), StarRocks computes compress_ratio = uncompressed_size / compressed_size; it uses the compressed payload only if compress_ratio `>` rpc_compress_ratio_threshold. With the default 1.1, compressed data must be at least ~9.1% smaller than uncompressed to be used. Lower the value to prefer compression (more CPU for smaller bandwidth savings); raise it to avoid compression overhead unless it yields larger size reductions. Note: this applies to RPC/shuffle serialization and is effective only when row-batch compression is enabled (compress_rowbatches).
- Introduced in: v3.2.0

##### ssl_private_key_path

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: File system path to the TLS/SSL private key (PEM) that the BE's brpc server uses as the private key for the default certificate. When `enable_https` is set to `true`, the system sets `brpc::ServerOptions::ssl_options().default_cert.private_key` to this path at process start. The file must be accessible by the BE process and must match the certificate provided by `ssl_certificate_path`. If this value is not set or the file is missing or unaccessible, HTTPS will not be configured and the bRPC server may fail to start. Protect this file with restrictive filesystem permissions (for example, 600).
- Introduced in: v4.0.0

##### thrift_client_retry_interval_ms

- Default: 100
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval at which a thrift client retries.
- Introduced in: -

##### thrift_connect_timeout_seconds

- Default: 3
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Connection timeout (in seconds) used when creating Thrift clients. ClientCacheHelper::_create_client multiplies this value by 1000 and passes it to ThriftClientImpl::set_conn_timeout(), so it controls the TCP/connect handshake timeout for new Thrift connections opened by the BE client cache. This setting affects only connection establishment; send/receive timeouts are configured separately. Very small values can cause spurious connection failures on high-latency networks, while large values delay detection of unreachable peers.
- Introduced in: v3.2.0

##### thrift_port

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Port used to export the internal Thrift-based BackendService. When the process runs as a Compute Node and this item is set to a non-zero value, it overrides `be_port` and the Thrift server binds to this value; otherwise `be_port` is used. This configuration is deprecated — setting a non-zero `thrift_port` logs a warning advising to use `be_port` instead.
- Introduced in: v3.2.0

##### thrift_rpc_connection_max_valid_time_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Maximum valid time for a thrift RPC connection. A connection will be closed if it has existed in the connection pool for longer than this value. It must be set consistent with FE configuration `thrift_client_timeout_ms`.
- Introduced in: -

##### thrift_rpc_max_body_size

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description: The maximum string body size of RPC. `0` indicates the size is unlimited.
- Introduced in: -

##### thrift_rpc_strict_mode

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether thrift's strict execution mode is enabled. For more information on thrift strict mode, see [Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md).
- Introduced in: -

##### thrift_rpc_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout for a thrift RPC.
- Introduced in: -

##### transaction_apply_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the minimum number of threads for the "update_apply" thread pool in BE's UpdateManager — the pool that applies rowsets for primary-key tables. A value of 0 disables a fixed minimum (no enforced lower bound); when transaction_apply_worker_count is also 0 the pool's max threads defaults to the number of CPU cores, so effective worker capacity equals CPU cores. You can raise this to guarantee a baseline concurrency for applying transactions; setting it too high may increase CPU contention. Changes are applied at runtime via the update_config HTTP handler (it calls update_min_threads on the apply thread pool).
- Introduced in: v3.2.11

##### transaction_publish_version_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the minimum number of threads reserved in the AgentServer "publish_version" dynamic thread pool (used to publish transaction versions / handle TTaskType::PUBLISH_VERSION tasks). At startup the pool is created with min = max(config value, MIN_TRANSACTION_PUBLISH_WORKER_COUNT) (MIN_TRANSACTION_PUBLISH_WORKER_COUNT = 1), so the default 0 results in a minimum of 1 thread. Changing this value at runtime invokes the update callback to call ThreadPool::update_min_threads, raising or lowering the pool's guaranteed minimum (but not below the enforced minimum of 1). Coordinate with transaction_publish_version_worker_count (max threads) and transaction_publish_version_thread_pool_idle_time_ms (idle timeout).
- Introduced in: v3.2.11

##### use_mmap_allocate_chunk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system allocates chunks using anonymous private mmap mappings (MAP_ANONYMOUS | MAP_PRIVATE) and frees them with munmap. Enabling this may create many virtual memory mappings, thus you must raise the kernel limit (as root user, running `sysctl -w vm.max_map_count=262144` or `echo 262144 > /proc/sys/vm/max_map_count`), and set `chunk_reserved_bytes_limit` to a relatively large value. Otherwise, enabling mmap can cause very poor performance due to frequent mapping/unmapping.
- Introduced in: v3.2.0

### Metadata and cluster management

##### cluster_id

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: Global cluster identifier for this StarRocks backend. At startup StorageEngine reads config::cluster_id into its effective cluster id and verifies that all data root paths contain the same cluster id (see StorageEngine::_check_all_root_path_cluster_id). A value of -1 means "unset" — the engine may derive the effective id from existing data directories or from master heartbeats. If a non‑negative id is configured, any mismatch between configured id and ids stored in data directories will cause startup verification to fail (Status::Corruption). When some roots lack an id and the engine is allowed to write ids (options.need_write_cluster_id), it will persist the effective id into those roots.
- Introduced in: v3.2.0

##### consistency_max_memory_limit

- Default: 10G
- Type: String
- Unit: -
- Is mutable: No
- Description: Memory size specification for the CONSISTENCY memory tracker.
- Introduced in: v3.2.0

##### make_snapshot_rpc_timeout_ms

- Default: 20000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the Thrift RPC timeout in milliseconds used when making a snapshot on a remote BE. Increase this value when remote snapshot creation regularly exceeds the default timeout; reduce it to fail faster on unresponsive BEs. Note other timeouts may affect end-to-end operations (for example the effective tablet-writer open timeout can relate to `tablet_writer_open_rpc_timeout_sec` and `load_timeout_sec`).
- Introduced in: v3.2.0

##### metadata_cache_memory_limit_percent

- Default: 30
- Type: Int
- Unit: Percent
- Is mutable: Yes
- Description: Sets the metadata LRU cache size as a percentage of the process memory limit. At startup StarRocks computes cache bytes as (process_mem_limit * metadata_cache_memory_limit_percent / 100) and passes that to the metadata cache allocator. The cache is only used for non-PRIMARY_KEYS rowsets (PK tables are not supported) and is enabled only when metadata_cache_memory_limit_percent &gt; 0; set it &lt;= 0 to disable the metadata cache. Increasing this value raises metadata cache capacity but reduces memory available to other components; tune based on workload and system memory. Not active in BE_TEST builds.
- Introduced in: v3.2.10

##### retry_apply_interval_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Base interval (in seconds) used when scheduling retries of failed tablet apply operations. It is used directly to schedule a retry after a submission failure and as the base multiplier for backoff: the next retry delay is calculated as min(600, `retry_apply_interval_second` * failed_attempts). The code also uses `retry_apply_interval_second` to compute the cumulative retry duration (arithmetic-series sum) which is compared against `retry_apply_timeout_second` to decide whether to keep retrying. Effective only when `enable_retry_apply` is true. Increasing this value lengthens both individual retry delays and the cumulative time spent retrying; decreasing it makes retries more frequent and may increase the number of attempts before reaching `retry_apply_timeout_second`.
- Introduced in: v3.2.9

##### retry_apply_timeout_second

- Default: 7200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum cumulative retry time (in seconds) allowed for applying a pending version before the apply process gives up and the tablet enters an error state. The apply logic accumulates exponential/backoff intervals based on `retry_apply_interval_second` and compares the total duration against `retry_apply_timeout_second`. If `enable_retry_apply` is true and the error is considered retryable, apply attempts will be rescheduled until the accumulated backoff exceeds `retry_apply_timeout_second`; then apply stops and the tablet transitions to error. Explicitly non-retryable errors (e.g., Corruption) are not retried regardless of this setting. Tune this value to control how long StarRocks will keep retrying apply operations (default 7200s = 2 hours).
- Introduced in: v3.3.13, v3.4.3, v3.5.0

##### txn_commit_rpc_timeout_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Maximum allowed lifetime (in milliseconds) for Thrift RPC connections used by BE stream-load and transaction commit calls. StarRocks sets this value as the `thrift_rpc_timeout_ms` on requests sent to FE (used in stream_load planning, loadTxnBegin/loadTxnPrepare/loadTxnCommit, and getLoadTxnStatus). If a connection has been pooled longer than this value it will be closed. When a per-request timeout (`ctx->timeout_second`) is provided, the BE computes the RPC timeout as rpc_timeout_ms = max(ctx*1000/4, min(ctx*1000/2, txn_commit_rpc_timeout_ms)), so the effective RPC timeout is bounded by the context and this configuration. Keep this consistent with FE's `thrift_client_timeout_ms` to avoid mismatched timeouts.
- Introduced in: v3.2.0

##### update_schema_worker_count

- Default: 3
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the maximum number of worker threads in the backend's "update_schema" dynamic ThreadPool that processes TTaskType::UPDATE_SCHEMA tasks. The ThreadPool is created in agent_server during startup with a minimum of 0 threads (it can scale down to zero when idle) and a max equal to this setting; the pool uses the default idle timeout and an effectively unlimited queue. Increase this value to allow more concurrent schema-update tasks (higher CPU and memory usage), or lower it to limit parallel schema operations.
- Introduced in: v3.2.3

### User, role, and privilege

##### ssl_certificate_path

- Default: 
- Type: String
- Unit: -
- Is mutable: No
- Description: Absolute path to the TLS/SSL certificate file (PEM) that the BE's brpc server will use when enable_https is true. At BE startup this value is copied into `brpc::ServerOptions::ssl_options().default_cert.certificate`; you must also set `ssl_private_key_path` to the matching private key. Provide the server certificate and any intermediate certificates in PEM format (certificate chain) if required by your CA. The file must be readable by the StarRocks BE process and is applied only at startup. If unset or invalid while enable_https is enabled, brpc TLS setup may fail and prevent the server from starting correctly.
- Introduced in: v4.0.0

### Query engine

##### clear_udf_cache_when_start

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, the BE's UserFunctionCache will clear all locally cached user function libraries on startup. During UserFunctionCache::init, the code calls _reset_cache_dir(), which removes UDF files from the configured UDF library directory (organized into kLibShardNum subdirectories) and deletes files with Java/Python UDF suffixes (.jar/.py). When disabled (default), the BE loads existing cached UDF files instead of deleting them. Enabling this forces UDF binaries to be re-downloaded on first use after restart (increasing network traffic and first-use latency).
- Introduced in: v4.0.0

##### dictionary_speculate_min_chunk_size

- Default: 10000
- Type: Int
- Unit: Rows
- Is mutable: No
- Description: Minimum number of rows (chunk size) used by StringColumnWriter and DictColumnWriter to trigger dictionary-encoding speculation. If an incoming column (or the accumulated buffer plus incoming rows) has size larger than or equal `dictionary_speculate_min_chunk_size` the writer will run speculation immediately and set an encoding (DICT, PLAIN or BIT_SHUFFLE) rather than buffering more rows. Speculation uses `dictionary_encoding_ratio` for string columns and `dictionary_encoding_ratio_for_non_string_column` for numeric/non-string columns to decide whether dictionary encoding is beneficial. Also, a large column byte_size (larger than or equal to UINT32_MAX) forces immediate speculation to avoid `BinaryColumn<uint32_t>` overflow.
- Introduced in: v3.2.0

##### disable_storage_page_cache

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

##### enable_bitmap_index_memory_page_cache

- Default: true 
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for Bitmap index. Memory cache is recommended if you want to use Bitmap indexes to accelerate point queries.
- Introduced in: v3.1

##### enable_compaction_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable compaction for Flat JSON data.
- Introduced in: v3.3.3

##### enable_json_flat

- Default: false
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable the Flat JSON feature. After this feature is enabled, newly loaded JSON data will be automatically flattened, improving JSON query performance.
- Introduced in: v3.3.0

##### enable_lazy_dynamic_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable Lazy Dyamic Flat JSON when a query misses Flat JSON schema in read process. When this item is set to `true`, StarRocks will postpone the Flat JSON operation to calculation process instead of read process.
- Introduced in: v3.3.3

##### enable_ordinal_index_memory_page_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for ordinal index. Ordinal index is a mapping from row IDs to data page positions, and it can be used to accelerate scans.
- Introduced in: -

##### enable_string_prefix_zonemap

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable ZoneMap for string (CHAR/VARCHAR) columns using prefix-based min/max. For non-key string columns, the min/max values are truncated to a fixed prefix length configured by `string_prefix_zonemap_prefix_len`.
- Introduced in: -

##### enable_zonemap_index_memory_page_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for zonemap index. Memory cache is recommended if you want to use zonemap indexes to accelerate scan.
- Introduced in: -

##### exchg_node_buffer_size_bytes

- Default: 10485760
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum buffer size on the receiver end of an exchange node for each query. This configuration item is a soft limit. A backpressure is triggered when data is sent to the receiver end with an excessive speed.
- Introduced in: -

##### file_descriptor_cache_capacity

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of file descriptors that can be cached.
- Introduced in: -

##### flamegraph_tool_dir

- Default: `${STARROCKS_HOME}/bin/flamegraph`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory of the flamegraph tool, which should contain pprof, stackcollapse-go.pl, and flamegraph.pl scripts for generating flame graphs from profile data.
- Introduced in: -

##### fragment_pool_queue_size

- Default: 2048
- Type: Int
- Unit: -
- Is mutable: No
- Description: The upper limit of the query number that can be processed on each BE node.
- Introduced in: -

##### fragment_pool_thread_num_max

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads used for query.
- Introduced in: -

##### fragment_pool_thread_num_min

- Default: 64
- Type: Int
- Unit: Minutes -
- Is mutable: No
- Description: The minimum number of threads used for query.
- Introduced in: -

##### hdfs_client_enable_hedged_read

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the hedged read feature.
- Introduced in: v3.0

##### hdfs_client_hedged_read_threadpool_size

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Specifies the size of the Hedged Read thread pool on your HDFS client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your HDFS client. It is equivalent to the `dfs.client.hedged.read.threadpool.size` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

##### hdfs_client_hedged_read_threshold_millis

- Default: 2500
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to `30`. In this situation, if a read from a block has not returned within 30 milliseconds, your HDFS client immediately starts up a new read against a different block replica. It is equivalent to the `dfs.client.hedged.read.threshold.millis` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

##### io_coalesce_adaptive_lazy_active

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Based on the selectivity of predicates, adaptively determines whether to combine the I/O of predicate columns and non-predicate columns.
- Introduced in: v3.2

##### jit_lru_cache_size

- Default: 0
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The LRU cache size for JIT compilation. It represents the actual size of the cache if it is set to greater than 0. If it is set to less than or equal to 0, the system will adaptively set the cache using the formula `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` (while `mem_limit` of the node must be greater or equal to 16 GB).
- Introduced in: -

##### json_flat_column_max

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of sub-fields that can be extracted by Flat JSON. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### json_flat_create_zonemap

- Default: true
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to create ZoneMaps for flattened JSON sub-columns during write. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: -

##### json_flat_null_factor

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of NULL values in the column to extract for Flat JSON. A column will not be extracted if its proportion of NULL value is higher than this threshold. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### json_flat_sparsity_factor

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of columns with the same name for Flat JSON. Extraction is not performed if the proportion of columns with the same name is lower than this value. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### lake_tablet_ignore_invalid_delete_predicate

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether ignore invalid delete predicates in tablet rowset metadata which may be introduced by logic deletion to a duplicate key table after the column name renamed.
- Introduced in: v4.0

##### max_hdfs_file_handle

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of HDFS file descriptors that can be opened.
- Introduced in: -

##### max_memory_sink_batch_count

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of Scan Cache batches.
- Introduced in: -

##### max_pushdown_conditions_per_column

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of conditions that allow pushdown in each column. If the number of conditions exceeds this limit, the predicates are not pushed down to the storage layer.
- Introduced in: -

##### max_scan_key_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of scan keys segmented by each query.
- Introduced in: -

##### min_file_descriptor_number

- Default: 60000
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of file descriptors in the BE process.
- Introduced in: -

##### object_storage_connect_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish socket connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

##### object_storage_request_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish HTTP connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

##### parquet_late_materialization_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the late materialization of Parquet reader to improve performance. `true` indicates enabling late materialization, and `false` indicates disabling it.
- Introduced in: -

##### parquet_page_index_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the pageindex of Parquet file to improve performance. `true` indicates enabling pageindex, and `false` indicates disabling it.
- Introduced in: v3.3

##### parquet_reader_bloom_filter_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the bloom filter of Parquet file to improve performance. `true` indicates enabling the bloom filter, and `false` indicates disabling it. You can also control this behavior on session level using the system variable `enable_parquet_reader_bloom_filter`. Bloom filters in Parquet are maintained **at the column level within each row group**. If a Parquet file contains bloom filters for certain columns, queries can use predicates on those columns to efficiently skip row groups.
- Introduced in: v3.5

##### path_gc_check_step

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of files that can be scanned continuously each time.
- Introduced in: -

##### path_gc_check_step_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval between file scans.
- Introduced in: -

##### path_scan_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which GC cleans expired data.
- Introduced in: -

##### pipeline_connector_scan_thread_num_per_cpu

- Default: 8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The number of scan threads assigned to Pipeline Connector per CPU core in the BE node. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

##### pipeline_poller_timeout_guard_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When this item is set to greater than `0`, if a driver takes longer than `pipeline_poller_timeout_guard_ms` for a single dispatch in the poller, then the information of the driver and operator is printed.
- Introduced in: -

##### pipeline_prepare_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum queue lenggth of PREPARE fragment thread pool for Pipeline execution engine.
- Introduced in: -

##### pipeline_prepare_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of threads in the pipeline execution engine PREPARE fragment thread pool. `0` indicates the value is equal to the number of system VCPU core number.
- Introduced in: -

##### pipeline_prepare_timeout_guard_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When this item is set to greater than `0`, if a plan fragment exceeds `pipeline_prepare_timeout_guard_ms` during the PREPARE process, a stack trace of the plan fragment is printed.
- Introduced in: -

##### pipeline_scan_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum task queue length of SCAN thread pool for Pipeline execution engine.
- Introduced in: -

##### priority_queue_remaining_tasks_increased_frequency

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how often the BlockingPriorityQueue increases ("ages") the priority of all remaining tasks to avoid starvation. Each successful get/pop increments an internal `_upgrade_counter`; when `_upgrade_counter` exceeds `priority_queue_remaining_tasks_increased_frequency`, the queue increments every element's priority, rebuilds the heap, and resets the counter. Lower values cause more frequent priority aging (reducing starvation but increasing CPU cost due to iterating and re-heapifying); higher values reduce that overhead but delay priority adjustments. The value is a simple operation count threshold, not a time duration.
- Introduced in: v3.2.0

##### query_cache_capacity

- Default: 536870912
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The size of the query cache in the BE. The default size is 512 MB. The size cannot be less than 4 MB. If the memory capacity of the BE is insufficient to provision your expected query cache size, you can increase the memory capacity of the BE.
- Introduced in: -

##### query_pool_spill_mem_limit_threshold

- Default: 1.0
- Type: Double
- Unit: -
- Is mutable: No
- Description: If automatic spilling is enabled, when the memory usage of all queries exceeds `query_pool memory limit * query_pool_spill_mem_limit_threshold`, intermediate result spilling will be triggered.
- Introduced in: v3.2.7

##### query_scratch_dirs

- Default: `${STARROCKS_HOME}`
- Type: string
- Unit: -
- Is mutable: No
- Description: Comma-separated list of writable scratch directories used by query execution to spill intermediate data (for example, external sorts, hash joins, and other operators). Specify one or more paths separated by `;` (e.g. `/mnt/ssd1/tmp;/mnt/ssd2/tmp`). Directories should be accessible and writable by the BE process and have sufficient free space; StarRocks will pick among them to distribute spill I/O. Changes require a restart to take effect. If a directory is missing, not writable, or full, spilling may fail or degrade query performance.
- Introduced in: v3.2.0

##### result_buffer_cancelled_interval_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The wait time before BufferControlBlock releases data.
- Introduced in: -

##### scan_context_gc_interval_min

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The time interval at which to clean the Scan Context.
- Introduced in: -

##### scanner_row_num

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum row count returned by each scan thread in a scan.
- Introduced in: -

##### scanner_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of scan tasks supported by the storage engine.
- Introduced in: -

##### scanner_thread_pool_thread_num

- Default: 48
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads which the storage engine used for concurrent storage volume scanning. All threads are managed in the thread pool.
- Introduced in: -

##### string_prefix_zonemap_prefix_len

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Prefix length used for string ZoneMap min/max when `enable_string_prefix_zonemap` is enabled.
- Introduced in: -

##### udf_thread_pool_size

- Default: 1
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the size of the UDF call PriorityThreadPool created in ExecEnv (used for executing user-defined functions / UDF-related tasks). The value is used as the pool thread count and also as the pool queue capacity when constructing the thread pool (PriorityThreadPool("udf", thread_num, queue_size)). Increase to allow more concurrent UDF executions; keep small to avoid excessive CPU and memory contention.
- Introduced in: v3.2.0

##### update_memory_limit_percent

- Default: 60
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Fraction of the BE process memory reserved for update-related memory and caches. During startup `GlobalEnv` computes the `MemTracker` for updates as process_mem_limit * clamp(update_memory_limit_percent, 0, 100) / 100. `UpdateManager` also uses this percentage to size its primary-index/index-cache capacity (index cache capacity = GlobalEnv::process_mem_limit * update_memory_limit_percent / 100). The HTTP config update logic registers a callback that calls `update_primary_index_memory_limit` on the update managers, so changes would be applied to the update subsystem if the config were changed. Increasing this value gives more memory to update/primary-index paths (reducing memory available for other pools); decreasing it reduces update memory and cache capacity. Values are clamped to the range 0–100.
- Introduced in: v3.2.0

### Loading

##### clear_transaction_task_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for clearing transaction.
- Introduced in: -

##### column_mode_partial_update_insert_batch_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Batch size for column mode partial update when processing inserted rows. If this item is set to `0` or negative, it will be clamped to `1` to avoid infinite loop. This item controls the number of newly inserted rows processed in each batch. Larger values can improve write performance but will consume more memory.
- Introduced in: v3.5.10, v4.0.2

##### enable_stream_load_verbose_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Specifies whether to log the HTTP requests and responses for Stream Load jobs.
- Introduced in: v2.5.17, v3.0.9, v3.1.6, v3.2.1

##### flush_thread_num_per_store

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store.
- Introduced in: -

##### lake_flush_thread_num_per_store

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store in shared-data mode. 
When this value is set to `0`, the system uses twice of the CPU core count as the value.
When this value is set to less than `0`, the system uses the product of its absolute value and the CPU core count as the value.
- Introduced in: v3.1.12, 3.2.7

##### load_data_reserve_hours

- Default: 4
- Type: Int
- Unit: Hours
- Is mutable: No
- Description: The reservation time for the files produced by small-scale loadings.
- Introduced in: -

##### load_error_log_reserve_hours

- Default: 48
- Type: Int
- Unit: Hours
- Is mutable: Yes
- Description: The time for which data loading logs are reserved.
- Introduced in: -

##### load_process_max_memory_limit_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum size limit of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

##### max_consumer_num_per_group

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of consumers in a consumer group of Routine Load.
- Introduced in: -

##### max_runnings_transactions_per_txn_map

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of transactions that can run concurrently in each partition.
- Introduced in: -

##### number_tablet_writer_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of tablet writer threads used in ingestion, such as Stream Load, Broker Load and Insert. When the parameter is set to less than or equal to 0, the system uses half of the number of CPU cores, with a minimum of 16. When the parameter is set to greater than 0, the system uses that value. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

##### push_worker_count_high_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with HIGH priority.
- Introduced in: -

##### push_worker_count_normal_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with NORMAL priority.
- Introduced in: -

##### streaming_load_max_batch_size_mb

- Default: 100
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a JSON file that can be streamed into StarRocks.
- Introduced in: -

##### streaming_load_max_mb

- Default: 102400
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a file that can be streamed into StarRocks. From v3.0, the default value has been changed from `10240` to `102400`.
- Introduced in: -

##### streaming_load_rpc_max_alive_time_sec

- Default: 1200
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The RPC timeout for Stream Load.
- Introduced in: -

##### transaction_publish_version_thread_pool_idle_time_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The idle time before a thread is reclaimed by the Publish Version thread pool.
- Introduced in: -

##### transaction_publish_version_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used to publish a version. When this value is set to less than or equal to `0`, the system uses the CPU core count as the value, so as to avoid insufficient thread resources when import concurrency is high but only a fixed number of threads are used. From v2.5, the default value has been changed from `8` to `0`.
- Introduced in: -

##### write_buffer_size

- Default: 104857600
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The buffer size of MemTable in the memory. This configuration item is the threshold to trigger a flush.
- Introduced in: -

### Loading and unloading

##### broker_write_timeout_seconds

- Default: 30
- Type: int
- Unit: Seconds
- Is mutable: No
- Description: Timeout (in seconds) used by backend broker operations for write/IO RPCs. The value is multiplied by 1000 to produce millisecond timeouts and is passed as the default timeout_ms to BrokerFileSystem and BrokerServiceConnection instances (e.g., file export and snapshot upload/download). Increase this when brokers or network are slow or when transferring large files to avoid premature timeouts; decreasing it may cause broker RPCs to fail earlier. This value is defined in common/config and is applied at process start (not dynamically reloadable).
- Introduced in: v3.2.0

##### enable_load_channel_rpc_async

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, handling of load-channel open RPCs (for example, `PTabletWriterOpen`) is offloaded from the BRPC worker to a dedicated thread pool: the request handler creates a `ChannelOpenTask` and submits it to the internal `_async_rpc_pool` instead of running `LoadChannelMgr::_open` inline. This reduces work and blocking inside BRPC threads and allows tuning concurrency via `load_channel_rpc_thread_pool_num` and `load_channel_rpc_thread_pool_queue_size`. If the thread pool submission fails (when pool is full or shut down), the request is canceled and an error status is returned. The pool is shut down on `LoadChannelMgr::close()`, so consider capacity and lifecycle when you want to enable this feature so as to avoid request rejections or delayed processing.
- Introduced in: v3.5.0

##### enable_load_diagnose

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, StarRocks will attempt an automated load diagnosis from BE OlapTableSink/NodeChannel after a brpc timeout matching "[E1008]Reached timeout". The code creates a `PLoadDiagnoseRequest` and sends an RPC to the remote LoadChannel to collect a profile and/or stack trace (controlled by `load_diagnose_rpc_timeout_profile_threshold_ms` and `load_diagnose_rpc_timeout_stack_trace_threshold_ms`). The diagnose RPC uses `load_diagnose_send_rpc_timeout_ms` as its timeout. Diagnosis is skipped if a diagnose request is already in progress. Enabling this produces additional RPCs and profiling work on target nodes; disable on sensitive production workloads to avoid extra overhead.
- Introduced in: v3.5.0

##### enable_load_segment_parallel

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, rowset segment loading and rowset-level reads are performed concurrently using StarRocks background thread pools (ExecEnv::load_segment_thread_pool and ExecEnv::load_rowset_thread_pool). Rowset::load_segments and TabletReader::get_segment_iterators submit per-segment or per-rowset tasks to these pools, falling back to serial loading and logging a warning if submission fails. Enable this to reduce read/load latency for large rowsets at the cost of increased CPU/IO concurrency and memory pressure. Note: parallel loading can change the load completion order of segments and therefore prevents partial compaction (code checks `_parallel_load` and disables partial compaction when enabled); consider implications for operations that rely on segment order.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### enable_streaming_load_thread_pool

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether streaming load scanners are submitted to the dedicated streaming load thread pool. When enabled and a query is a LOAD with `TLoadJobType::STREAM_LOAD`, ConnectorScanNode submits scanner tasks to the `streaming_load_thread_pool` (which is configured with INT32_MAX threads and queue sizes, i.e. effectively unbounded). When disabled, scanners use the general `thread_pool` and its `PriorityThreadPool` submission logic (priority computation, try_offer/offer behavior). Enabling isolates streaming-load work from regular query execution to reduce interference; however, because the dedicated pool is effectively unbounded, enabling may increase concurrent threads and resource usage under heavy streaming-load traffic. This option is on by default and typically does not require modification.
- Introduced in: v3.2.0

##### es_http_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: HTTP connection timeout (in milliseconds) used by the ES network client in ESScanReader for Elasticsearch scroll requests. This value is applied via network_client.set_timeout_ms() before sending subsequent scroll POSTs and controls how long the client waits for an ES response during scrolling. Increase this value for slow networks or large queries to avoid premature timeouts; decrease to fail faster on unresponsive ES nodes. This setting complements `es_scroll_keepalive`, which controls the scroll context keep-alive duration.
- Introduced in: v3.2.0

##### es_index_max_result_window

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: No
- Description: Limits the maximum number of documents StarRocks will request from Elasticsearch in a single batch. StarRocks sets the ES request batch size to min(`es_index_max_result_window`, `chunk_size`) when building `KEY_BATCH_SIZE` for the ES reader. If an ES request exceeds the Elasticsearch index setting `index.max_result_window`, Elasticsearch returns HTTP 400 (Bad Request). Adjust this value when scanning large indexes or increase the ES `index.max_result_window` on the Elasticsearch side to permit larger single requests.
- Introduced in: v3.2.0

##### load_channel_rpc_thread_pool_num

- Default: -1
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Maximum number of threads for the load-channel async RPC thread pool. When set to less than or equal to 0 (default `-1`) the pool size is auto-set to the number of CPU cores (`CpuInfo::num_cores()`). The configured value is used as ThreadPoolBuilder's max threads and the pool's min threads is set to min(5, max_threads). The pool queue size is controlled separately by `load_channel_rpc_thread_pool_queue_size`. This setting was introduced to align the async RPC pool size with brpc workers' default (`brpc_num_threads`) so behavior remains compatible after switching load RPC handling from synchronous to asynchronous. Changing this config at runtime triggers `ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(...)`.
- Introduced in: v3.5.0

##### load_channel_rpc_thread_pool_queue_size

- Default: 1024000
- Type: int
- Unit: Count
- Is mutable: No
- Description: Sets the maximum pending-task queue size for the Load channel RPC thread pool created by LoadChannelMgr. This thread pool executes asynchronous `open` requests when `enable_load_channel_rpc_async` is enabled; the pool size is paired with `load_channel_rpc_thread_pool_num`. The large default (1024000) aligns with brpc workers' defaults to preserve behavior after switching from synchronous to asynchronous handling. If the queue is full, ThreadPool::submit() will fail and the incoming open RPC is cancelled with an error, causing the caller to receive a rejection. Increase this value to buffer larger bursts of concurrent `open` requests; reducing it tightens backpressure but may cause more rejections under load.
- Introduced in: v3.5.0

##### load_channel_abort_clean_up_delay_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls how long (in seconds) LoadChannelMgr keeps the load IDs of aborted load channels before removing them from `_aborted_load_channels`. When a load job is cancelled or fails, the load ID stays recorded so any late-arriving load RPCs can be rejected immediately; once the delay expires, the entry is cleaned during the periodic background sweep (minimum sweep interval is 60 seconds). Setting the delay too low risks accepting stray RPCs after an abort, while setting it too high may retain state and consume resources longer than necessary. Tune this to balance correctness of late-request rejection and resource retention for aborted loads.
- Introduced in: v3.5.11, v4.0.4

##### load_diagnose_rpc_timeout_profile_threshold_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When a load RPC times out (error contains "[E1008]Reached timeout") and `enable_load_diagnose` is true, this threshold controls whether a full profiling diagnose is requested. If the request-level RPC timeout `_rpc_timeout_ms` is greater than `load_diagnose_rpc_timeout_profile_threshold_ms`, profiling is enabled for that diagnose. For smaller `_rpc_timeout_ms` values, profiling is sampled once every 20 timeouts to avoid frequent heavy diagnostics for real-time/short-timeout loads. This value affects the `profile` flag in the `PLoadDiagnoseRequest` sent; stack-trace behavior is controlled separately by `load_diagnose_rpc_timeout_stack_trace_threshold_ms` and send timeout by `load_diagnose_send_rpc_timeout_ms`.
- Introduced in: v3.5.0

##### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used to decide when to request remote stack traces for long-running load RPCs. When a load RPC times out with a timeout error and the effective RPC timeout (_rpc_timeout_ms) exceeds this value, `OlapTableSink`/`NodeChannel` will include `stack_trace=true` in a `load_diagnose` RPC to the target BE so the BE can return stack traces for debugging. `LocalTabletsChannel::SecondaryReplicasWaiter` also triggers a best-effort stack-trace diagnose from the primary if waiting for secondary replicas exceeds this interval. This behavior requires `enable_load_diagnose` and uses `load_diagnose_send_rpc_timeout_ms` for the diagnose RPC timeout; profiling is gated separately by `load_diagnose_rpc_timeout_profile_threshold_ms`. Lowering this value increases how aggressively stack traces are requested.
- Introduced in: v3.5.0

##### load_diagnose_send_rpc_timeout_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout (in milliseconds) applied to diagnosis-related brpc calls initiated by BE load paths. It is used to set the controller timeout for `load_diagnose` RPCs (sent by NodeChannel/OlapTableSink when a LoadChannel brpc call times out) and for replica-status queries (used by SecondaryReplicasWaiter / LocalTabletsChannel when checking primary replica state). Choose a value high enough to allow the remote side to respond with profile or stack-trace data, but not so high that failure handling is delayed. This parameter works together with `enable_load_diagnose`, `load_diagnose_rpc_timeout_profile_threshold_ms`, and `load_diagnose_rpc_timeout_stack_trace_threshold_ms` which control when and what diagnostic information is requested.
- Introduced in: v3.5.0

##### load_fp_brpc_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Overrides the per-channel brpc RPC timeout used by OlapTableSink when the `node_channel_set_brpc_timeout` fail point is triggered. If set to a positive value, NodeChannel will set its internal `_rpc_timeout_ms` to this value (in milliseconds) causing open/add-chunk/cancel RPCs to use the shorter timeout and enabling simulation of brpc timeouts that produce the "[E1008]Reached timeout" error. Default (`-1`) disables the override. Changing this value is intended for testing and fault injection; small values may produce false timeouts and trigger load diagnostics (see `enable_load_diagnose`, `load_diagnose_rpc_timeout_profile_threshold_ms`, `load_diagnose_rpc_timeout_stack_trace_threshold_ms`, and `load_diagnose_send_rpc_timeout_ms`).
- Introduced in: v3.5.0

##### load_fp_tablets_channel_add_chunk_block_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: When enabled (set to a positive milliseconds value) this fail-point configuration makes TabletsChannel::add_chunk sleep for the specified time during load processing. It is used to simulate BRPC timeout errors (e.g., "[E1008]Reached timeout") and to emulate an expensive add_chunk operation that increases load latency. A value less than or equal to 0 (default `-1`) disables the injection. Intended for testing fault handling, timeouts, and replica synchronization behavior — do not enable in normal production workloads as it delays write completion and can trigger upstream timeouts or replica aborts.
- Introduced in: v3.5.0

##### load_segment_thread_pool_num_max

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Sets the maximum number of worker threads for BE load-related thread pools. This value is used by ThreadPoolBuilder to limit threads for both `load_rowset_pool` and `load_segment_pool` in exec_env.cpp, controlling concurrency for processing loaded rowsets and segments (e.g., decoding, indexing, writing) during streaming and batch loads. Increasing this value raises parallelism and can improve load throughput but also increases CPU, memory usage, and potential contention; decreasing it limits concurrent load processing and may reduce throughput. Tune together with `load_segment_thread_pool_queue_size` and `streaming_load_thread_pool_idle_time_ms`. Change requires BE restart.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### load_segment_thread_pool_queue_size

- Default: 10240
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: Sets the maximum queue length (number of pending tasks) for the load-related thread pools created as "load_rowset_pool" and "load_segment_pool". These pools use `load_segment_thread_pool_num_max` for their max thread count and this configuration controls how many load segment/rowset tasks can be buffered before the ThreadPool's overflow policy takes effect (further submissions may be rejected or blocked depending on the ThreadPool implementation). Increase to allow more pending load work (uses more memory and can raise latency); decrease to limit buffered load concurrency and reduce memory usage.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### max_pulsar_consumer_num_per_group

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the maximum number of Pulsar consumers that may be created in a single data consumer group for routine load on a BE. Because cumulative acknowledge is not supported for multi-topic subscriptions, each consumer subscribes exactly one topic/partition; if the number of partitions in `pulsar_info->partitions` exceeds this value, group creation fails with an error advising to increase `max_pulsar_consumer_num_per_group` on the BE or add more BEs. This limit is enforced when constructing a PulsarDataConsumerGroup and prevents a BE from hosting more than this many consumers for one routine load group. For Kafka routine load, `max_consumer_num_per_group` is used instead.
- Introduced in: v3.2.0

##### pull_load_task_dir

- Default: `${STARROCKS_HOME}/var/pull_load`
- Type: string
- Unit: -
- Is mutable: No
- Description: Filesystem path where the BE stores data and working files for "pull load" tasks (downloaded source files, task state, temporary output, etc.). The directory must be writable by the BE process and have sufficient disk space for incoming loads. The default is relative to STARROCKS_HOME; tests create and expect this directory to exist (see test configuration).
- Introduced in: v3.2.0

##### routine_load_kafka_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Timeout in seconds used for Kafka-related routine load operations. When a client request does not specify a timeout, `routine_load_kafka_timeout_second` is used as the default RPC timeout (converted to milliseconds) for `get_info`. It is also used as the per-call consume poll timeout for the librdkafka consumer (converted to milliseconds and capped by remaining runtime). Note: the internal `get_info` path reduces this value to 80% before passing it to librdkafka to avoid FE-side timeout races. Set this to a value that balances timely failure reporting and sufficient time for network/broker responses; changes require a restart because the setting is not mutable.
- Introduced in: v3.2.0

##### routine_load_pulsar_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Default timeout (in seconds) that the BE uses for Pulsar-related routine load operations when the request does not supply an explicit timeout. Specifically, `PInternalServiceImplBase::get_pulsar_info` multiplies this value by 1000 to form the millisecond timeout passed to the routine load task executor methods that fetch Pulsar partition metadata and backlog. Increase to allow slower Pulsar responses at the cost of longer failure detection; decrease to fail faster on slow brokers. Analogous to `routine_load_kafka_timeout_second` used for Kafka.
- Introduced in: v3.2.0

##### streaming_load_thread_pool_idle_time_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the thread idle timeout (in milliseconds) for streaming-load related thread pools. The value is used as the idle timeout passed to ThreadPoolBuilder for the `stream_load_io` pool and also for `load_rowset_pool` and `load_segment_pool`. Threads in these pools are reclaimed when idle for this duration; lower values reduce idle resource usage but increase thread creation overhead, while higher values keep threads alive longer. The `stream_load_io` pool is used when `enable_streaming_load_thread_pool` is enabled.
- Introduced in: v3.2.0

##### streaming_load_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Minimum number of threads for the streaming load IO thread pool ("stream_load_io") created during ExecEnv initialization. The pool is built with `set_max_threads(INT32_MAX)` and `set_max_queue_size(INT32_MAX)` so it is effectively unbounded to avoid deadlocks for concurrent streaming loads. A value of 0 lets the pool start with no threads and grow on demand; setting a positive value reserves that many threads at startup. This pool is used when `enable_streaming_load_thread_pool` is true and its idle timeout is controlled by `streaming_load_thread_pool_idle_time_ms`. Overall concurrency is still constrained by `fragment_pool_thread_num_max` and `webserver_num_workers`; changing this value is rarely necessary and may increase resource usage if set too high.
- Introduced in: v3.2.0

### Statistic report

##### enable_metric_calculator

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, the BE process launches a background "metrics_daemon" thread (started in Daemon::init on non-Apple platforms) that runs every ~15 seconds to invoke `StarRocksMetrics::instance()->metrics()->trigger_hook()` and compute derived/system metrics (e.g., push/query bytes/sec, max disk I/O util, max network send/receive rates), log memory breakdowns and run table metrics cleanup. When false, those hooks are executed synchronously inside `MetricRegistry::collect` at metric collection time, which can increase metric-scrape latency. Requires process restart to take effect.
- Introduced in: v3.2.0

##### enable_system_metrics

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When true, StarRocks initializes system-level monitoring during startup: it discovers disk devices from the configured store paths and enumerates network interfaces, then passes this information into the metrics subsystem to enable collection of disk I/O, network traffic and memory-related system metrics. If device or interface discovery fails, initialization logs a warning and aborts system metrics setup. This flag only controls whether system metrics are initialized; periodic metric aggregation threads are controlled separately by `enable_metric_calculator`, and JVM metrics initialization is controlled by `enable_jvm_metrics`. Changing this value requires a restart.
- Introduced in: v3.2.0

##### profile_report_interval

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval in seconds that the ProfileReportWorker uses to (1) decide when to report per-fragment profile information for LOAD queries and (2) sleep between reporting cycles. The worker compares current time against each task's last_report_time using (profile_report_interval * 1000) ms to determine if a profile should be re-reported for both non-pipeline and pipeline load tasks. At each loop the worker reads the current value (mutable at runtime); if the configured value is less than or euqual to 0 the worker forces it to 1 and emits a warning. Changing this value affects the next reporting decision and sleep duration.
- Introduced in: v3.2.0

##### report_disk_state_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the storage volume state, which includes the size of data within the volume.
- Introduced in: -

##### report_resource_usage_interval_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Interval, in milliseconds, between periodic resource-usage reports sent by the BE agent to the FE (master). The agent worker thread collects TResourceUsage (number of running queries, memory used/limit, CPU used permille, and resource-group usages) and calls report_task, then sleeps for this configured interval (see task_worker_pool). Lower values increase reporting timeliness but raise CPU, network, and master load; higher values reduce overhead but make resource information less current. The reporting updates related metrics (report_resource_usage_requests_total, report_resource_usage_requests_failed). Tune according to cluster scale and FE load.
- Introduced in: v3.2.0

##### report_tablet_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all tablets.
- Introduced in: -

##### report_task_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the state of a task. A task can be creating a table, dropping a table, loading data, or changing a table schema.
- Introduced in: -

##### report_workgroup_interval_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all workgroups.
- Introduced in: -

### Storage

##### alter_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used for Schema Change.
- Introduced in: -

##### avro_ignore_union_type_tag

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to strip the type tag from the JSON string serialized from the Avro Union data type.
- Introduced in: v3.3.7, v3.4

##### base_compaction_check_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Base Compaction.
- Introduced in: -

##### base_compaction_interval_seconds_since_last_operation

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval since the last Base Compaction. This configuration item is one of the conditions that trigger a Base Compaction.
- Introduced in: -

##### base_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for Base Compaction on each storage volume.
- Introduced in: -

##### base_cumulative_delta_ratio

- Default: 0.3
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction.
- Introduced in: -

##### chaos_test_enable_random_compaction_strategy

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, TabletUpdates::compaction() uses the random compaction strategy (compaction_random) intended for chaos engineering tests. This flag forces compaction to follow a nondeterministic/random policy instead of normal strategies (e.g., size-tiered compaction), and takes precedence during compaction selection for the tablet. It is intended only for controlled testing: enabling it can produce unpredictable compaction order, increased I/O/CPU, and test flakiness. Do not enable in production; use only for fault-injection or chaos-test scenarios.
- Introduced in: v3.3.12, 3.4.2, 3.5.0, 4.0.0

##### check_consistency_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for checking the consistency of tablets.
- Introduced in: -

##### clear_expired_replication_snapshots_interval_seconds

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which the system clears the expired snapshots left by abnormal replications.
- Introduced in: v3.3.5

##### compact_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for concurrent compaction tasks. This configuration is changed to dynamic from v3.1.7 and v3.2.2 onwards.
- Introduced in: v3.0.0

##### compaction_max_memory_limit

- Default: -1
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Global upper bound (in bytes) for memory available to compaction tasks on this BE. During BE initialization the final compaction memory limit is computed as min(`compaction_max_memory_limit`, process_mem_limit * `compaction_max_memory_limit_percent` / 100). If `compaction_max_memory_limit` is negative (default `-1`) it falls back to the BE process memory limit derived from `mem_limit`. The percent value is clamped to [0,100]. If the process memory limit is not set (negative) compaction memory remains unlimited (`-1`). This computed value is used to initialize the `_compaction_mem_tracker`. See also `compaction_max_memory_limit_percent` and `compaction_memory_limit_per_worker`.
- Introduced in: v3.2.0

##### compaction_max_memory_limit_percent

- Default: 100
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Percentage of the BE process memory that may be used for compaction. The BE computes the compaction memory cap as the minimum of `compaction_max_memory_limit` and (process memory limit × this percent / 100). If this value is < 0 or > 100 it is treated as 100. If `compaction_max_memory_limit` < 0 the process memory limit is used instead. The calculation also considers the BE process memory derived from `mem_limit`. Combined with `compaction_memory_limit_per_worker` (per-worker cap), this setting controls total compaction memory available and therefore affects compaction concurrency and OOM risk.
- Introduced in: v3.2.0

##### compaction_memory_limit_per_worker

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size allowed for each Compaction thread.
- Introduced in: -

##### compaction_trace_threshold

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace.
- Introduced in: -

##### create_tablet_worker_count

- Default: 3
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the maximum number of worker threads in the AgentServer thread pool that process TTaskType::CREATE (create-tablet) tasks submitted by FE. At BE startup this value is used as the thread-pool max (the pool is created with min threads = 1 and max queue size = unlimited), and changing it at runtime triggers `ExecEnv::agent_server()->get_thread_pool(TTaskType::CREATE)->update_max_threads(...)`. Increase this to raise concurrent tablet creation throughput (useful during bulk load or partition creation); decreasing it throttles concurrent create operations. Raising the value increases CPU, memory and I/O concurrency and may cause contention; the thread pool enforces at least one thread, so values less than 1 have no practical effect.
- Introduced in: v3.2.0

##### cumulative_compaction_check_interval_seconds

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Cumulative Compaction.
- Introduced in: -

##### cumulative_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of Cumulative Compaction threads per disk.
- Introduced in: -

##### data_page_size

- Default: 65536
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Target uncompressed page size (in bytes) used when building column data and index pages. This value is copied into ColumnWriterOptions.data_page_size and IndexedColumnWriterOptions.index_page_size and is consulted by page builders (e.g., BinaryPlainPageBuilder::is_page_full and buffer reservation logic) to decide when to finish a page and how much memory to reserve. A value of 0 disables the page-size limit in builders. Changing this value affects page count, metadata overhead, memory reservation and I/O/compression trade-offs (smaller pages → more pages and metadata; larger pages → fewer pages, potentially better compression but larger memory spikes).
- Introduced in: v3.2.4

##### default_num_rows_per_column_file_block

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows that can be stored in each row block.
- Introduced in: -

##### delete_worker_count_high_priority

- Default: 1
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of worker threads in the DeleteTaskWorkerPool that are allocated as HIGH-priority delete threads. On startup AgentServer creates the delete pool with total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority; the first delete_worker_count_high_priority threads are marked to exclusively try to pop TPriority::HIGH tasks (they poll for high-priority delete tasks and sleep/loop if none are available). Increasing this value increases concurrency for high-priority delete requests; decreasing it reduces dedicated capacity and may increase latency for high-priority deletes.
- Introduced in: v3.2.0

##### dictionary_encoding_ratio

- Default: 0.7
- Type: Double
- Unit: -
- Is mutable: No
- Description: Fraction (0.0–1.0) used by StringColumnWriter during the encode-speculation phase to decide between dictionary (DICT_ENCODING) and plain (PLAIN_ENCODING) encoding for a chunk. The code computes max_card = row_count * `dictionary_encoding_ratio` and scans the chunk’s distinct key count; if the distinct count exceeds max_card the writer chooses PLAIN_ENCODING. The check is performed only when the chunk size passes `dictionary_speculate_min_chunk_size` (and when row_count > dictionary_min_rowcount). Setting the value higher favors dictionary encoding (tolerates more distinct keys); setting it lower causes earlier fallback to plain encoding. A value of 1.0 effectively forces dictionary encoding (distinct count can never exceed row_count).
- Introduced in: v3.2.0

##### dictionary_encoding_ratio_for_non_string_column

- Default: 0
- Type: double
- Unit: -
- Is mutable: No
- Description: Ratio threshold used to decide whether to use dictionary encoding for non-string columns (numeric, date/time, decimal types). When enabled (value &gt; 0.0001) the writer computes max_card = row_count * dictionary_encoding_ratio_for_non_string_column and, for samples with row_count &gt; `dictionary_min_rowcount`, chooses DICT_ENCODING only if distinct_count ≤ max_card; otherwise it falls back to BIT_SHUFFLE. A value of `0` (default) disables non-string dictionary encoding. This parameter is analogous to `dictionary_encoding_ratio` but applies to non-string columns. Use values in (0,1] — smaller values restrict dictionary encoding to lower-cardinality columns and reduce dictionary memory/IO overhead.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### dictionary_page_size

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Size in bytes of dictionary pages used when building rowset segments. This value is read into `PageBuilderOptions::dict_page_size` in the BE rowset code and controls how many dictionary entries can be stored in a single dictionary page. Increasing this value can improve compression ratio for dictionary-encoded columns by allowing larger dictionaries, but larger pages consume more memory during write/encode and can increase I/O and latency when reading or materializing pages. Set conservatively for large-memory, write-heavy workloads and avoid excessively large values to prevent runtime performance degradation.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### disk_stat_monitor_interval

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to monitor health status of disks.
- Introduced in: -

##### download_low_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The download speed lower limit of each HTTP request. An HTTP request aborts when it constantly runs with a lower speed than this value within the time span specified in the configuration item `download_low_speed_time`.
- Introduced in: -

##### download_low_speed_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time that an HTTP request can run with a download speed lower than the limit. An HTTP request aborts when it constantly runs with a lower speed than the value of `download_low_speed_limit_kbps` within the time span specified in this configuration item.
- Introduced in: -

##### download_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the download tasks of restore jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

##### drop_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used to drop a tablet.
- Introduced in: -

##### enable_check_string_lengths

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to check the data length during loading to solve compaction failures caused by out-of-bound VARCHAR data.
- Introduced in: -

##### enable_event_based_compaction_framework

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Event-based Compaction Framework. `true` indicates Event-based Compaction Framework is enabled, and `false` indicates it is disabled. Enabling Event-based Compaction Framework can greatly reduce the overhead of compaction in scenarios where there are many tablets or a single tablet has a large amount of data.
- Introduced in: -

##### enable_lazy_delta_column_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, compaction will prefer a "lazy" strategy for delta columns produced by partial column updates: StarRocks will avoid eagerly merging delta-column files back into their main segment files to save compaction I/O. In practice the compaction selection code checks for partial column-update rowsets and multiple candidates; if found and this flag is true, the engine will either stop adding further inputs to the compaction or only merge empty rowsets (level -1), leaving delta columns separate. This reduces immediate I/O and CPU during compaction at the cost of delayed consolidation (potentially more segments and temporary storage overhead). Correctness and query semantics are unchanged.
- Introduced in: v3.2.3

##### enable_new_load_on_memory_limit_exceeded

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow new loading processes when the hard memory resource limit is reached. `true` indicates new loading processes will be allowed, and `false` indicates they will be rejected.
- Introduced in: v3.3.2

##### enable_pk_parallel_execution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Determines whether the Primary Key table parallel execution strategy is enabled. When enabled, PK index files will be generated during the import and compaction phases.
- Introduced in: -

##### enable_pk_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy for Primary Key tables. `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: This item takes effect for shared-data clusters from v3.2.4 and v3.1.10 onwards, and for shared-nothing clusters from v3.2.5 and v3.1.10 onwards.

##### enable_rowset_verify

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to verify the correctness of generated rowsets. When enabled, the correctness of the generated rowsets will be checked after Compaction and Schema Change.
- Introduced in: -

##### enable_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy (excluding Primary Key tables). `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: -

##### enable_strict_delvec_crc_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enable_strict_delvec_crc_check is set to true, we will perform a strict CRC32 check on the delete vector, and if a mismatch is detected, a failure will be returned.
- Introduced in: -

##### enable_transparent_data_encryption

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, StarRocks will create encrypted on‑disk artifacts for newly written storage objects (segment files, delete/update files, rowset segments, lake SSTs, persistent index files, etc.). Writers (RowsetWriter/SegmentWriter, lake UpdateManager/LakePersistentIndex and related code paths) will request encryption info from the KeyCache, attach encryption_info to writable files and persist encryption_meta into rowset / segment / sstable metadata (segment_encryption_metas, delete/update encryption metadata). The Frontend and Backend/CN encryption flags must match — a mismatch causes the BE to abort on heartbeat (LOG(FATAL)). This flag is not runtime‑mutable; enable it before deployment and ensure key management (KEK) and KeyCache are properly configured and synchronized across the cluster.
- Introduced in: v3.3.1, 3.4.0, 3.5.0, 4.0.0

##### enable_zero_copy_from_page_cache

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, FixedLengthColumnBase may avoid copying bytes when appending data that originates from a page-cache-backed buffer. In append_numbers the code will acquire the incoming ContainerResource and set the column's internal resource pointer (zero-copy) if all conditions are met: the config is true, the incoming resource is owned, the resource memory is aligned for the column element type, the column is empty, and the resource length is a multiple of the element size. Enabling this reduces CPU and memory-copy overhead and can improve ingestion/scan throughput. Drawbacks: it couples the column lifetime to the acquired buffer and relies on correct ownership/alignment; disable to force safe copying.
- Introduced in: -

##### file_descriptor_cache_clean_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean file descriptors that have not been used for a certain period of time.
- Introduced in: -

##### ignore_broken_disk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls startup behavior when configured storage paths fail read/write checks or fail to parse. When `false` (default), BE treats any broken entry in `storage_root_path` or `spill_local_storage_dir` as fatal and will abort startup. When `true`, StarRocks will skip (log a warning and remove) any storage path that fails `check_datapath_rw` or fails parsing so the BE can continue starting with the remaining healthy paths. Note: if all configured paths are removed, BE will still exit. Enabling this can mask misconfigured or failed disks and cause data on ignored paths to be unavailable; monitor logs and disk health accordingly.
- Introduced in: v3.2.0

##### inc_rowset_expired_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the incoming data. This configuration item is used in incremental clone.
- Introduced in: -

##### load_process_max_memory_hard_limit_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The hard limit (ratio) of memory resources that can be taken up by all load processes on a BE node. When `enable_new_load_on_memory_limit_exceeded` is set to `false`, and the memory consumption of all loading processes exceeds `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio`, new loading processes will be rejected.
- Introduced in: v3.3.2

##### load_process_max_memory_limit_percent

- Default: 30
- Type: Int
- Unit: -
- Is mutable: No
- Description: The soft limit (in percentage) of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

##### lz4_acceleration

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the LZ4 "acceleration" parameter used by the built-in LZ4 compressor (passed to LZ4_compress_fast_continue). Higher values prioritize compression speed at the cost of compression ratio; lower values (1) produce better compression but are slower. Valid range: MIN=1, MAX=65537. This setting affects all LZ4-based codecs in BlockCompression (e.g., LZ4 and Hadoop-LZ4) and only changes how compression is performed — it does not change the LZ4 format or decompression compatibility. Tune upward (e.g., 4, 8, ...) for CPU-bound or low-latency workloads where larger output is acceptable; keep at 1 for storage- or IO-sensitive workloads. Test with representative data before changing, since throughput vs. size trade-offs are highly data-dependent.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

##### lz4_expected_compression_ratio

- Default: 2.1
- Type: double
- Unit: Dimensionless (compression ratio)
- Is mutable: Yes
- Description: Threshold used by the serialization compression strategy to judge whether observed LZ4 compression is "good". In compress_strategy.cpp this value divides the observed compress_ratio when computing a reward metric together with lz4_expected_compression_speed_mbps; if the combined reward `>` 1.0 the strategy records positive feedback. Increasing this value raises the expected compression ratio (making the condition harder to satisfy), while lowering it makes it easier for observed compression to be considered satisfactory. Tune to match typical data compressibility. Valid range: MIN=1, MAX=65537.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

##### lz4_expected_compression_speed_mbps

- Default: 600
- Type: double
- Unit: MB/s
- Is mutable: Yes
- Description: Expected LZ4 compression throughput in megabytes per second used by the adaptive compression policy (CompressStrategy). The feedback routine computes a reward_ratio = (observed_compression_ratio / lz4_expected_compression_ratio) * (observed_speed / lz4_expected_compression_speed_mbps). A reward_ratio `>` 1.0 increments the positive counter (alpha), otherwise the negative counter (beta); this influences whether future data will be compressed. Tune this value to reflect typical LZ4 throughput on your hardware — raising it makes the policy harder to classify a run as "good" (requires higher observed speed), lowering it makes classification easier. Must be a positive finite number.
- Introduced in: v3.4.1, 3.5.0, 4.0.0

##### make_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the make snapshot tasks on a BE node.
- Introduced in: -

##### manual_compaction_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of threads for Manual Compaction.
- Introduced in: -

##### max_base_compaction_num_singleton_deltas

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be compacted in each Base Compaction.
- Introduced in: -

##### max_compaction_candidate_num

- Default: 40960
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of candidate tablets for compaction. If the value is too large, it will cause high memory usage and high CPU load.
- Introduced in: -

##### max_compaction_concurrency

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compactions (including both Base Compaction and Cumulative Compaction). The value `-1` indicates that no limit is imposed on the concurrency. `0` indicates disabling compaction. This parameter is mutable when the Event-based Compaction Framework is enabled.
- Introduced in: -

##### max_cumulative_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be merged in a single Cumulative Compaction. You can reduce this value if OOM occurs during compaction.
- Introduced in: -

##### max_download_speed_kbps

- Default: 50000
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The maximum download speed of each HTTP request. This value affects the performance of data replica synchronization across BE nodes.
- Introduced in: -

##### max_garbage_sweep_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

##### max_percentage_of_error_disk

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum percentage of error that is tolerable in a storage volume before the corresponding BE node quits.
- Introduced in: -

##### max_queueing_memtable_per_tablet

- Default: 2
- Type: Long
- Unit: Count
- Is mutable: Yes
- Description: Controls per-tablet backpressure for write paths: when a tablet's number of queueing (not-yet-flushing) memtables reaches or exceeds `max_queueing_memtable_per_tablet`, writers in `LocalTabletsChannel` and `LakeTabletsChannel` will block (sleep/retry) before submitting more write work. This reduces simultaneous memtable flush concurrency and peak memory use at the cost of increased latency or RPC timeouts for heavy load. Set higher to allow more concurrent memtables (more memory and I/O burst); set lower to limit memory pressure and increase write throttling.
- Introduced in: v3.2.0

##### max_row_source_mask_memory_bytes

- Default: 209715200
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size of the row source mask buffer. When the buffer is larger than this value, data will be persisted to a temporary file on the disk. This value should be set lower than the value of `compaction_memory_limit_per_worker`.
- Introduced in: -

##### max_update_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rowsets that can be merged in a single Compaction for Primary Key tables.
- Introduced in: -

##### memory_limitation_per_thread_for_schema_change

- Default: 2
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: The maximum memory size allowed for each schema change task.
- Introduced in: -

##### memory_ratio_for_sorting_schema_change

- Default: 0.8
- Type: Double
- Unit: - (unitless ratio)
- Is mutable: Yes
- Description: Fraction of the per-thread schema-change memory limit used as the memtable maximum buffer size during sorting schema-change operations. The ratio is multiplied by memory_limitation_per_thread_for_schema_change (configured in GB and converted to bytes) to compute max_buffer_size, and that result is capped at 4GB. Used by SchemaChangeWithSorting and SortedSchemaChange when creating MemTable/DeltaWriter. Increasing this ratio allows larger in-memory buffers (fewer flushes/merges) but raises risk of memory pressure; reducing it causes more frequent flushes and higher I/O/merge overhead.
- Introduced in: v3.2.0

##### min_base_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments that trigger a Base Compaction.
- Introduced in: -

##### min_compaction_failure_interval_sec

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which a tablet compaction can be scheduled since the previous compaction failure.
- Introduced in: -

##### min_cumulative_compaction_failure_interval_sec

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which Cumulative Compaction retries upon failures.
- Introduced in: -

##### min_cumulative_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments to trigger Cumulative Compaction.
- Introduced in: -

##### min_garbage_sweep_interval

- Default: 180
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

##### parallel_clone_task_per_path

- Default: 8
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Number of parallel clone worker threads allocated per storage path on a BE. At BE startup the clone thread-pool max threads is computed as max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL). For example, with 4 storage paths and default=8 the clone pool max = 32. This setting directly controls concurrency of CLONE tasks (tablet replica copies) handled by the BE: increasing it raises parallel clone throughput but also increases CPU, disk and network contention; decreasing it limits simultaneous clone tasks and can throttle FE-scheduled clone operations. The value is applied to the dynamic clone thread pool and can be changed at runtime via the update-config path (causes agent_server to update the clone pool max threads).
- Introduced in: v3.2.0

##### partial_update_memory_limit_per_worker

- Default: 2147483648
- Type: long
- Unit: Bytes
- Is mutable: Yes
- Description: Maximum memory (in bytes) a single worker may use for assembling a source chunk when performing partial column updates (used in compaction / rowset update processing). The reader estimates per-row update memory (total_update_row_size / num_rows_upt) and multiplies it by the number of rows read; when that product exceeds this limit the current chunk is flushed and processed to avoid additional memory growth. Set this to match the available memory per update worker—too low increases I/O/processing overhead (many small chunks); too high risks memory pressure or OOM. If the per-row estimate is zero (legacy rowsets), this config does not impose a byte-based limit (only the INT32_MAX row count limit applies).
- Introduced in: v3.2.10

##### path_gc_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When enabled, StorageEngine starts per-data-dir background threads that perform periodic path scanning and garbage collection. On startup `start_bg_threads()` spawns `_path_scan_thread_callback` (calls `DataDir::perform_path_scan` and `perform_tmp_path_scan`) and `_path_gc_thread_callback` (calls `DataDir::perform_path_gc_by_tablet`, `DataDir::perform_path_gc_by_rowsetid`, `DataDir::perform_delta_column_files_gc`, and `DataDir::perform_crm_gc`). The scan and GC intervals are controlled by `path_scan_interval_second` and `path_gc_check_interval_second`; CRM file cleanup uses `unused_crm_file_threshold_second`. Disable this to prevent automatic path-level cleanup (you must then manage orphaned/temp files manually). Changing this flag requires restarting the process.
- Introduced in: v3.2.0

##### path_gc_check_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Interval in seconds between runs of the storage engine's path garbage-collection background thread. Each wake triggers DataDir to perform path GC by tablet, by rowset id, delta column file GC and CRM GC (the CRM GC call uses `unused_crm_file_threshold_second`). If set to a non-positive value the code forces the interval to 1800 seconds (half hour) and emits a warning. Tune this to control how frequently on-disk temporary or downloaded files are scanned and removed.
- Introduced in: v3.2.0

##### pending_data_expire_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the pending data in the storage engine.
- Introduced in: -

##### pindex_major_compaction_limit_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks.
- Introduced in: v3.0.9

##### pk_parallel_execution_threshold_bytes

- Default: 104857600
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: When enable_pk_parallel_execution is set to true, the Primary Key table parallel execution strategy will be enabled if the data generated during import or compaction exceeds this threshold. Default is 100MB.
- Introduced in: -

##### pk_index_parallel_compaction_threadpool_max_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for cloud native primary key index parallel compaction in shared-data mode.
- Introduced in: -

##### pk_index_parallel_compaction_task_split_threshold_bytes

- Default: 104857600
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The splitting threshold for primary key index compaction tasks. When the total size of the files involved in a task is smaller than this threshold, the task will not be split. Default is 100MB.
- Introduced in: -

##### pk_index_target_file_size

- Default: 67108864
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Target file size for primary key index in shared-data mode. Default is 64MB.
- Introduced in: -

##### pk_index_compaction_score_ratio

- Default: 1.5
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Compaction score ratio for primary key index in shared-data mode. For example, if there are N filesets, the compaction score will be N * pk_index_compaction_score_ratio.
- Introduced in: -

##### pk_index_ingest_sst_compaction_threshold

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Ingest SST compaction threshold for primary key index in shared-data mode.
- Introduced in: -

##### enable_pk_index_parallel_compaction

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable parallel compaction for primary key index in shared-data mode.
- Introduced in: -

##### enable_pk_index_parallel_get

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable parallel get for primary key index in shared-data mode.
- Introduced in: -

##### pk_index_parallel_get_min_rows

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum rows threshold to enable parallel get for primary key index in shared-data mode.
- Introduced in: -

##### pk_index_parallel_get_threadpool_max_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for primary key index parallel get in shared-data mode. 0 means auto configuration.
- Introduced in: -

##### pk_index_memtable_flush_threadpool_max_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads in the thread pool for primary key index memtable flush in shared-data mode.
- Introduced in: -

##### pk_index_memtable_max_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of memtables for primary key index in shared-data mode.
- Introduced in: -

##### pk_index_size_tiered_min_level_size

- Default: 131072
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum level size parameter for primary key index size-tiered compaction strategy.
- Introduced in: -

##### pk_index_size_tiered_level_multiplier 

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The level multiple parameter for primary key index size-tiered compaction strategy.
- Introduced in: -

##### pk_index_size_tiered_max_level 

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The level number parameter for primary key index size-tiered compaction strategy.
- Introduced in: -

##### primary_key_limit_size

- Default: 128
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a key column in Primary Key tables.
- Introduced in: v2.5

##### release_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the release snapshot tasks on a BE node.
- Introduced in: -

##### repair_compaction_interval_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval to poll Repair Compaction threads.
- Introduced in: -

##### replication_max_speed_limit_kbps

- Default: 50000
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The maximum speed of each replication thread.
- Introduced in: v3.3.5

##### replication_min_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The minimum speed of each replication thread.
- Introduced in: v3.3.5

##### replication_min_speed_time_seconds

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration allowed for a replication thread to be under the minimum speed. Replication will fail if the time when the actual speed is lower than `replication_min_speed_limit_kbps` exceeds this value.
- Introduced in: v3.3.5

##### replication_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for replication. `0` indicates setting the thread number to four times the BE CPU core count.
- Introduced in: v3.3.5

##### size_tiered_level_multiple

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The multiple of data size between two contiguous levels in the Size-tiered Compaction policy.
- Introduced in: -

##### size_tiered_level_multiple_dupkey

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In the Size-tiered Compaction policy, the multiple of the data amount difference between two adjacent levels for Duplicate Key tables.
- Introduced in: -

##### size_tiered_level_num

- Default: 7
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of levels for the Size-tiered Compaction policy. At most one rowset is reserved for each level. Therefore, under a stable condition, there are, at most, as many rowsets as the level number specified in this configuration item.
- Introduced in: -

##### size_tiered_max_compaction_level

- Default: 3
- Type: Int
- Unit: Levels
- Is mutable: Yes
- Description: Limits how many size-tiered levels may be merged into a single primary-key real-time compaction task. During the PK size-tiered compaction selection, StarRocks builds ordered "levels" of rowsets by size and will add successive levels into the chosen compaction input until this limit is reached (the code uses compaction_level `<=` size_tiered_max_compaction_level). The value is inclusive and counts the number of distinct size tiers merged (the top level is counted as 1). Effective only when the PK size-tiered compaction strategy is enabled; raising it lets a compaction task include more levels (larger, more I/O- and CPU-intensive merges, potential higher write amplification), while lowering it restricts merges and reduces task size and resource usage.
- Introduced in: v4.0.0

##### size_tiered_min_level_size

- Default: 131072
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The data size of the minimum level in the Size-tiered Compaction policy. Rowsets smaller than this value immediately trigger the data compaction.
- Introduced in: -

##### small_dictionary_page_size

- Default: 4096
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Threshold (in bytes) used by BinaryPlainPageDecoder to decide whether to eagerly parse a dictionary (binary/plain) page. If a page's encoded size is &lt; `small_dictionary_page_size`, the decoder pre-parses all string entries into an in-memory vector (`_parsed_datas`) to accelerate random access and batch reads. Raising this value causes more pages to be pre-parsed (which can reduce per-access decoding overhead and may increase effective compression for larger dictionaries) but increases memory usage and CPU spent parsing; excessively large values can degrade overall performance. Tune only after measuring memory and access-latency trade-offs.
- Introduced in: v3.4.1, v3.5.0

##### snapshot_expire_time_sec

- Default: 172800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of snapshot files.
- Introduced in: -

##### stale_memtable_flush_time_sec

- Default: 0
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: When a sender job's memory usage is high, memtables that have not been updated for longer than `stale_memtable_flush_time_sec` seconds will be flushed to reduce memory pressure. This behavior is only considered when memory limits are approaching (`limit_exceeded_by_ratio(70)` or higher). In `LocalTabletsChannel`, an additional path at very high memory usage (`limit_exceeded_by_ratio(95)`) may flush memtables whose size exceeds `write_buffer_size / 4`. A value of `0` disables this age-based stale-memtable flushing (immutable-partition memtables still flush immediately when idle or on high memory).
- Introduced in: v3.2.0

##### storage_flood_stage_left_capacity_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in all BE directories. If the remaining storage space of the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_flood_stage_usage_percent`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_reserve_bytes` to allow the configurations to take effect.
- Introduced in: -

##### storage_flood_stage_usage_percent

- Default: 95
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in all BE directories. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_flood_stage_left_capacity_bytes`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_percent` to allow the configurations to take effect.
- Introduced in: -

##### storage_high_usage_disk_protect_ratio

- Default: 0.1
- Type: double
- Unit: -
- Is mutable: Yes
- Description: When selecting a storage root for tablet creation, StorageEngine sorts candidate disks by `disk_usage(0)` and computes the average usage. Any disk whose usage is greater than (average usage + `storage_high_usage_disk_protect_ratio`) is excluded from the preferential selection pool (it will not participate in the randomized, preferrential shuffle and thus is deferred from being chosen initially). Set to 0 to disable this protection. Values are fractional (typical range 0.0–1.0); larger values make the scheduler more tolerant of higher-than-average disks.
- Introduced in: v3.2.0

##### storage_medium_migrate_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for storage medium migration (from SATA to SSD).
- Introduced in: -

##### storage_root_path

- Default: `${STARROCKS_HOME}/storage`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory and medium of the storage volume. Example: `/data1,medium:hdd;/data2,medium:ssd`.
  - Multiple volumes are separated by semicolons (`;`).
  - If the storage medium is SSD, add `,medium:ssd` at the end of the directory.
  - If the storage medium is HDD, add `,medium:hdd` at the end of the directory.
- Introduced in: -

##### sync_tablet_meta

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the synchronization of the tablet metadata. `true` indicates enabling synchronization, and `false` indicates disabling it.
- Introduced in: -

##### tablet_map_shard_size

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The tablet map shard size. The value must be a power of two.
- Introduced in: -

##### tablet_max_pending_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending versions that are tolerable on a Primary Key tablet. Pending versions refer to versions that are committed but not applied yet.
- Introduced in: -

##### tablet_max_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of versions allowed on a tablet. If the number of versions exceeds this value, new write requests will fail.
- Introduced in: -

##### tablet_meta_checkpoint_min_interval_secs

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a TabletMeta Checkpoint.
- Introduced in: -

##### tablet_meta_checkpoint_min_new_rowsets_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of rowsets to create since the last TabletMeta Checkpoint.
- Introduced in: -

##### tablet_rowset_stale_sweep_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to sweep the stale rowsets in tablets.
- Introduced in: -

##### tablet_stat_cache_update_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: 是
- Description: The time interval at which Tablet Stat Cache updates.
- Introduced in: -

##### tablet_writer_open_rpc_timeout_sec

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Timeout (in seconds) for the RPC that opens a tablet writer on a remote BE. The value is converted to milliseconds and applied to both the request timeout and the brpc control timeout when issuing the open call. The runtime uses the effective timeout as the minimum of `tablet_writer_open_rpc_timeout_sec` and half of the overall load timeout (i.e., min(`tablet_writer_open_rpc_timeout_sec`, `load_timeout_sec` / 2)). Set this to balance timely failure detection (too small may cause premature open failures) and giving BEs enough time to initialize writers (too large delays error handling).
- Introduced in: v3.2.0

##### transaction_apply_worker_count

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Controls the maximum number of worker threads used by the UpdateManager's "update_apply" thread pool — the pool that applies rowsets for transactions (notably for primary-key tables). A value `>0` sets a fixed maximum thread count; 0 (the default) makes the pool size equal to the number of CPU cores. The configured value is applied at startup (UpdateManager::init) and can be changed at runtime via the update-config HTTP action, which updates the pool's max threads. Tune this to increase apply concurrency (throughput) or limit CPU/memory contention; min threads and idle timeout are governed by transaction_apply_thread_pool_num_min and transaction_apply_worker_idle_time_ms respectively.
- Introduced in: v3.2.0

##### transaction_apply_worker_idle_time_ms

- Default: 500
- Type: int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the idle timeout (in milliseconds) for the UpdateManager's "update_apply" thread pool used to apply transactions/updates. The value is passed to ThreadPoolBuilder::set_idle_timeout via MonoDelta::FromMilliseconds, so worker threads that remain idle longer than this timeout may be terminated (subject to the pool's configured minimum thread count and max threads). Lower values free resources faster but increase thread creation/teardown overhead under bursty load; higher values keep workers warm for short bursts at the cost of higher baseline resource usage.
- Introduced in: v3.2.11

##### trash_file_expire_time_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean trash files. The default value has been changed from 259,200 to 86,400 since v2.5.17, v3.0.9, and v3.1.6.
- Introduced in: -

##### unused_rowset_monitor_interval

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean the expired rowsets.
- Introduced in: -

##### update_cache_expire_sec

- Default: 360
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of Update Cache.
- Introduced in: -

##### update_compaction_check_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to check compaction for Primary Key tables.
- Introduced in: -

##### update_compaction_chunk_size_for_row_store

- Default: 0
- Type: Int
- Unit: Rows
- Is mutable: Yes
- Description: Overrides the chunk size (number of rows per chunk) used during update compaction for tablets that use column-with-row-store representation. By default (0) StarRocks computes per-column-group chunk size with CompactionUtils::get_read_chunk_size based on compaction memory limit, vector chunk size, total input rows and memory footprint. When this config is set to a positive integer and tablet.is_column_with_row_store() is true, RowsetMerger forces _chunk_size to this value in both horizontal and vertical update compaction paths (rowset_merger.cpp). Use a non-zero value only when the automatic calculation is producing suboptimal results; increasing the value can reduce writer/IO overhead but raises peak memory use, while decreasing it reduces memory pressure at the cost of more chunks and potential performance impact.
- Introduced in: v3.2.3

##### update_compaction_delvec_file_io_amp_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Used to control the priority of compaction for rowsets that contain Delvec files in Primary Key tables. The larger the value, the higher the priority.
- Introduced in: -

##### update_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of Compaction threads per disk for Primary Key tables.
- Introduced in: -

##### update_compaction_per_tablet_min_interval_seconds

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which compaction is triggered for each tablet in a Primary Key table.
- Introduced in: -

##### update_compaction_ratio_threshold

- Default: 0.5
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The maximum proportion of data that a compaction can merge for a Primary Key table in a shared-data cluster. It is recommended to shrink this value if a single tablet becomes excessively large.
- Introduced in: v3.1.5

##### update_compaction_result_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum result size of a single compaction for Primary Key tables.
- Introduced in: -

##### update_compaction_size_threshold

- Default: 268435456
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score of Primary Key tables is calculated based on the file size, which is different from other table types. This parameter can be used to make the Compaction Score of Primary Key tables similar to that of other table types, making it easier for users to understand.
- Introduced in: -

##### upload_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the upload tasks of backup jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

##### vertical_compaction_max_columns_per_group

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of columns per group of Vertical Compactions.
- Introduced in: -

### Shared-data

##### download_buffer_size

- Default: 4194304
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Size (in bytes) of the in-memory copy buffer used when downloading snapshot files. SnapshotLoader::download passes this value to fs::copy as the per-transfer chunk size when reading from the remote sequential file into the local writable file. Larger values can improve throughput on high-bandwidth links by reducing syscall/IO overhead; smaller values reduce peak memory use per active transfer. Note: this parameter controls buffer size per stream, not the number of download threads—total memory consumption = download_buffer_size * number_of_concurrent_downloads.
- Introduced in: v3.2.13

##### graceful_exit_wait_for_frontend_heartbeat

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Determines whether to await at least one frontend heartbeat response indicating SHUTDOWN status before completing graceful exit. When enabled, the graceful shutdown process remains active until a SHUTDOWN confirmation is responded via heartbeat RPC, ensuring the frontend has sufficient time to detect the termination state between two regular heartbeat intervals.
- Introduced in: v3.4.5

##### lake_compaction_stream_buffer_size_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The reader's remote I/O buffer size for cloud-native table compaction in a shared-data cluster. The default value is 1MB. You can increase this value to accelerate compaction process.
- Introduced in: v3.2.3

##### lake_pk_compaction_max_input_rowsets

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of input rowsets allowed in a Primary Key table compaction task in a shared-data cluster. The default value of this parameter is changed from `5` to `1000` since v3.2.4 and v3.1.10, and to `500` since v3.3.1 and v3.2.9. After the Sized-tiered Compaction policy is enabled for Primary Key tables (by setting `enable_pk_size_tiered_compaction_strategy` to `true`), StarRocks does not need to limit the number of rowsets for each compaction to reduce write amplification. Therefore, the default value of this parameter is increased.
- Introduced in: v3.1.8, v3.2.3

##### loop_count_wait_fragments_finish

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of loops to be waited when the BE/CN process exits. Each loop is a fixed interval of 10 seconds. You can set it to `0` to disable the loop wait. From v3.4 onwards, this item is changed to mutable and its default value is changed from `0` to `2`.
- Introduced in: v2.5

##### max_client_cache_size_per_host

- Default: 10
- Type: Int
- Unit: entries (cached client instances) per host
- Is mutable: No
- Description: The maximum number of cached client instances retained for each remote host by BE-wide client caches. This single setting is used when creating BackendServiceClientCache, FrontendServiceClientCache, and BrokerServiceClientCache during ExecEnv initialization, so it limits the number of client stubs/connections kept per host across those caches. Raising this value reduces reconnects and stub creation overhead at the cost of increased memory and file-descriptor usage; lowering it saves resources but may increase connection churn. The value is read at startup and cannot be changed at runtime. Currently one shared setting controls all client cache types; separate per-cache configuration may be introduced later.
- Introduced in: v3.2.0

##### starlet_filesystem_instance_cache_capacity

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The cache capacity of starlet filesystem instances.
- Introduced in: v3.2.16, v3.3.11, v3.4.1

##### starlet_filesystem_instance_cache_ttl_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The cache expiration time of starlet filesystem instances.
- Introduced in: v3.3.15, 3.4.5

##### starlet_port

- Default: 9070
- Type: Int
- Unit: -
- Is mutable: No
- Description: An extra agent service port for BE and CN.
- Introduced in: -

##### starlet_star_cache_disk_size_percent

- Default: 80
- Type: Int
- Unit: -
- Is mutable: No
- Description: The percentage of disk capacity that Data Cache can use at most in a shared-data cluster.
- Introduced in: v3.1

##### starlet_use_star_cache

- Default: false in v3.1 and true from v3.2.3
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Cache in a shared-data cluster. `true` indicates enabling this feature and `false` indicates disabling it. The default value is set from `false` to `true` from v3.2.3 onwards.
- Introduced in: v3.1

##### starlet_write_file_with_tag

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: In a shared-data cluster, whether to tag files written to object storage with object storage tags for convenient custom file management.
- Introduced in: v3.5.3

### Data Lake

##### datacache_block_buffer_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Block Buffer to optimize Data Cache efficiency. When Block Buffer is enabled, the system reads the Block data from the Data Cache and caches it in a temporary buffer, thus reducing the extra overhead caused by frequent cache reads.
- Introduced in: v3.2.0

##### datacache_disk_adjust_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval of Data Cache automatic capacity scaling. At regular intervals, the system checks the cache disk usage, and triggers Automatic Scaling when necessary.
- Introduced in: v3.3.0

##### datacache_disk_idle_seconds_for_expansion

- Default: 7200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum wait time for Data Cache automatic expansion. Automatic scaling up is triggered only if the disk usage remains below `datacache_disk_low_level` for longer than this duration.
- Introduced in: v3.3.0

##### datacache_disk_size

- Default: 0
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The maximum amount of data that can be cached on a single disk. You can set it as a percentage (for example, `80%`) or a physical limit (for example, `2T`, `500G`). For example, if you use two disks and set the value of the `datacache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached on these two disks. The default value is `0`, which indicates that only memory is used to cache data.
- Introduced in: -

##### datacache_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Data Cache. `true` indicates Data Cache is enabled, and `false` indicates Data Cache is disabled. The default value is changed to `true` from v3.3.
- Introduced in: -

##### datacache_eviction_policy

- Default: slru
- Type: String
- Unit: -
- Is mutable: No
- Description: The eviction policy of Data Cache. Valid values: `lru` (least recently used) and `slru` (Segmented LRU).
- Introduced in: v3.4.0

##### datacache_inline_item_count_limit

- Default: 130172
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of inline cache items in Data Cache. For some particularly small cache blocks, Data Cache stores them in `inline` mode, which caches the block data and metadata together in memory.
- Introduced in: v3.4.0

##### datacache_mem_size

- Default: 0
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The maximum amount of data that can be cached in memory. You can set it as a percentage (for example, `10%`) or a physical limit (for example, `10G`, `21474836480`).
- Introduced in: -

##### datacache_min_disk_quota_for_adjustment

- Default: 10737418240
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum effective capacity for Data Cache Automatic Scaling. If the system tries to adjust the cache capacity to less than this value, the cache capacity will be directly set to `0` to prevent suboptimal performance caused by frequent cache fills and evictions due to insufficient cache capacity.
- Introduced in: v3.3.0

##### disk_high_level

- Default: 90
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The upper limit of disk usage (in percentage) that triggers the automatic scaling up of the cache capacity. When the disk usage exceeds this value, the system automatically evicts cache data from the Data Cache. From v3.4.0 onwards, the default value is changed from `80` to `90`. This item is renamed from `datacache_disk_high_level` to `disk_high_level` from v4.0 onwards.
- Introduced in: v3.3.0

##### disk_low_level

- Default: 60
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The lower limit of disk usage (in percentage) that triggers the automatic scaling down of the cache capacity. When the disk usage remains below this value for the period specified in `datacache_disk_idle_seconds_for_expansion`, and the space allocated for Data Cache is fully utilized, the system will automatically expand the cache capacity by increasing the upper limit. This item is renamed from `datacache_disk_low_level` to `disk_low_level` from v4.0 onwards.
- Introduced in: v3.3.0

##### disk_safe_level

- Default: 80
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The safe level of disk usage (in percentage) for Data Cache. When Data Cache performs automatic scaling, the system adjusts the cache capacity with the goal of maintaining disk usage as close to this value as possible. From v3.4.0 onwards, the default value is changed from `70` to `80`. This item is renamed from `datacache_disk_safe_level` to `disk_safe_level` from v4.0 onwards.
- Introduced in: v3.3.0

##### enable_connector_sink_spill

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Spilling for writes to external tables. Enabling this feature prevents the generation of a large number of small files as a result of writing to an external table when memory is insufficient. Currently, this feature only supports writing to Iceberg tables.
- Introduced in: v4.0.0

##### enable_datacache_disk_auto_adjust

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Automatic Scaling for Data Cache disk capacity. When it is enabled, the system dynamically adjusts the cache capacity based on the current disk usage rate. This item is renamed from `datacache_auto_adjust_enable` to `enable_datacache_disk_auto_adjust` from v4.0 onwards.
- Introduced in: v3.3.0

##### jdbc_connection_idle_timeout_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which an idle connection in the JDBC connection pool expires. If the connection idle time in the JDBC connection pool exceeds this value, the connection pool closes idle connections beyond the number specified in the configuration item `jdbc_minimum_idle_connections`.
- Introduced in: -

##### jdbc_connection_pool_size

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The JDBC connection pool size. On each BE node, queries that access the external table with the same `jdbc_url` share the same connection pool.
- Introduced in: -

##### jdbc_minimum_idle_connections

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of idle connections in the JDBC connection pool.
- Introduced in: -

##### lake_clear_corrupted_cache_data

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to clear the corrupted data cache in a shared-data cluster.
- Introduced in: v3.4

##### lake_clear_corrupted_cache_meta

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to clear the corrupted metadata cache in a shared-data cluster.
- Introduced in: v3.3

##### lake_enable_vertical_compaction_fill_data_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow vertical compaction tasks to cache data on local disks in a shared-data cluster.
- Introduced in: v3.1.7, v3.2.3

##### lake_service_max_concurrency

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum concurrency of RPC requests in a shared-data cluster. Incoming requests will be rejected when this threshold is reached. When this item is set to `0`, no limit is imposed on the concurrency.
- Introduced in: -

##### max_hdfs_scanner_num

- Default: 50
- Type: Int
- Unit: -
- Is mutable: No
- Description: Limits the maximum number of concurrently running connector (HDFS/remote) scanners that a ConnectorScanNode can have. During scan startup the node computes an estimated concurrency (based on memory, chunk size and scanner_row_num) and then caps it with this value to determine how many scanners and chunks to reserve and how many scanner threads to start. It is also consulted when scheduling pending scanners at runtime (to avoid oversubscription) and when deciding how many pending scanners can be re-submitted considering file-handle limits. Lowering this reduces threads, memory and open-file pressure at the cost of potential throughput; increasing it raises concurrency and resource usage.
- Introduced in: v3.2.0

##### query_max_memory_limit_percent

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum memory that the Query Pool can use. It is expressed as a percentage of the Process memory limit.
- Introduced in: v3.1.0

##### rocksdb_max_write_buffer_memory_bytes

- Default: 1073741824
- Type: Int64
- Unit: -
- Is mutable: No
- Description: It is the max size of the write buffer for meta in rocksdb. Default is 1GB.
- Introduced in: v3.5.0

##### rocksdb_write_buffer_memory_percent

- Default: 5
- Type: Int64
- Unit: -
- Is mutable: No
- Description: It is the memory percent of write buffer for meta in rocksdb. default is 5% of system memory. However, aside from this, the final calculated size of the write buffer memory will not be less than 64MB nor exceed 1G (rocksdb_max_write_buffer_memory_bytes)
- Introduced in: v3.5.0

### Other

##### default_mv_resource_group_concurrency_limit

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency (per BE node) of the materialized view refresh tasks in the resource group `default_mv_wg`. The default value `0` indicates no limits.
- Introduced in: v3.1

##### default_mv_resource_group_cpu_limit

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of CPU cores (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`.
- Introduced in: v3.1

##### default_mv_resource_group_memory_limit

- Default: 0.8
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The maximum memory proportion (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`. The default value indicates 80% of the memory.
- Introduced in: v3.1

##### default_mv_resource_group_spill_mem_limit_threshold

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The memory usage threshold before a materialized view refresh task in the resource group `default_mv_wg` triggers intermediate result spilling. The default value indicates 80% of the memory.
- Introduced in: v3.1

##### enable_resolve_hostname_to_ip_in_load_error_url

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: For `error_urls` debugging, whether to allow operators to choose between using original hostnames from FE heartbeat or forcing resolution to IP addresses based on their environment needs.
  - `true`: Resolve hostnames to IPs.
  - `false` (Default): Keeps the original hostname in the error URL.
- Introduced in: v4.0.1

##### enable_retry_apply

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, Tablet apply failures that are classified as retryable (for example transient memory-limit errors) are rescheduled for retry instead of immediately marking the tablet in error. The retry path in TabletUpdates schedules the next attempt using `retry_apply_interval_second` multiplied by the current failure count and clamped to a 600s maximum, so backoff grows with successive failures. Explicitly non-retryable errors (for example corruption) bypass retries and cause the apply process to enter the error state immediately. Retries continue until an overall timeout/terminal condition is reached, after which the apply will enter the error state. Turning this off disables automatic rescheduling of failed apply tasks and causes failed applies to transition to error state without retries.
- Introduced in: v3.2.9

##### enable_token_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the token check. `true` indicates enabling the token check, and `false` indicates disabling it.
- Introduced in: -

##### es_scroll_keepalive

- Default: 5m
- Type: String
- Unit: Minutes (string with suffix, e.g. "5m")
- Is mutable: No
- Description: The keep-alive duration sent to Elasticsearch for scroll search contexts. The value is used verbatim (for example "5m") when building the initial scroll URL (`?scroll=<value>`) and when sending subsequent scroll requests (via ESScrollQueryBuilder). This controls how long the ES search context is retained before garbage collection on the ES side; setting it longer keeps scroll contexts alive for more time but prolongs resource usage on the ES cluster. The value is read at startup by the ES scan reader and is not changeable at runtime.
- Introduced in: v3.2.0

##### load_replica_status_check_interval_ms_on_failure

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval that the secondary replica checks it's status on the primary replica if the last check rpc fails.
- Introduced in: v3.5.1

##### load_replica_status_check_interval_ms_on_success

- Default: 15000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval that the secondary replica checks it's status on the primary replica if the last check rpc successes.
- Introduced in: v3.5.1

##### max_length_for_bitmap_function

- Default: 1000000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for bitmap functions.
- Introduced in: -

##### max_length_for_to_base64

- Default: 200000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for the to_base64() function.
- Introduced in: -

##### memory_high_level

- Default: 75
- Type: Long
- Unit: Percent
- Is mutable: Yes
- Description: High water memory threshold expressed as a percentage of the process memory limit. When total memory consumption rises above this percentage, BE begins to free memory gradually (currently by evicting data cache and update cache) to relieve pressure. The monitor uses this value to compute memory_high = mem_limit * memory_high_level / 100 and, if consumption `>` memory_high, performs controlled eviction guided by the GC advisor; if consumption exceeds memory_urgent_level (a separate config), more aggressive immediate reductions occur. This value is also consulted to disable certain memory‑intensive operations (for example, primary-key preload) when the threshold is exceeded. Must satisfy validation with memory_urgent_level (memory_urgent_level `>` memory_high_level, memory_high_level `>=` 1, memory_urgent_level `<=` 100).
- Introduced in: v3.2.0

##### report_exec_rpc_request_retry_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The retry times of rpc request to report exec rpc request to FE. The default value is 10, which means that the rpc request will be retried 10 times if it fails only if it's fragment instatnce finish rpc. Report exec rpc request is important for load job, if one fragment instance finish report failed, the load job will be hang until timeout.
- Introduced in: -

##### sleep_one_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: A small, global sleep interval (in seconds) used by BE agent worker threads as a one-second pause when the master address/heartbeat is not yet available or when a short retry/backoff is needed. In the codebase it is referenced by several report worker pools (e.g., ReportDiskStateTaskWorkerPool, ReportOlapTableTaskWorkerPool, ReportWorkgroupTaskWorkerPool) to avoid busy-waiting and reduce CPU consumption while retrying. Increasing this value slows the retry frequency and responsiveness to master availability; reducing it increases polling rate and CPU usage. Adjust only with awareness of the trade-off between responsiveness and resource use.
- Introduced in: v3.2.0

##### small_file_dir

- Default: `${STARROCKS_HOME}/lib/small_file/`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store the files downloaded by the file manager.
- Introduced in: -

##### upload_buffer_size

- Default: 4194304
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Buffer size (in bytes) used by file copy operations when uploading snapshot files to remote storage (broker or direct FileSystem). In the upload path (snapshot_loader.cpp) this value is passed to fs::copy as the read/write chunk size for each upload stream. The default is 4 MiB. Increasing this value can improve throughput on high-latency or high-bandwidth links but increases memory usage per concurrent upload; decreasing it reduces per-stream memory but may reduce transfer efficiency. Tune together with upload_worker_count and overall available memory.
- Introduced in: v3.2.13

##### user_function_dir

- Default: `${STARROCKS_HOME}/lib/udf`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store User-defined Functions (UDFs).
- Introduced in: -

##### web_log_bytes

- Default: 1048576 (1 MB)
- Type: long
- Unit: Bytes
- Is mutable: No
- Description: Maximum number of bytes to read from the INFO logfile and show on the BE debug webserver's log page. The handler uses this value to compute a seek offset (showing the last N bytes) to avoid reading or serving very large log files. If the logfile is smaller than this value the whole file is shown. Note: in the current implementation the code that reads and serves the INFO log is commented out and the handler reports that the INFO log file couldn't be opened, so this parameter may have no effect unless the log-serving code is enabled.
- Introduced in: v3.2.0
