---
displayed_sidebar: docs
sidebar_label: "Logging, Server, and Metadata"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration - Logging, Server, and Metadata

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
- [Logging](#logging)
- [Server](#server)
- [Metadata and Cluster Management](#metadata-and-cluster-management)

## Logging

### diagnose_stack_trace_interval_ms

- Default: 1800000 (30 minutes)
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Controls the minimum time gap between successive stack-trace diagnostics performed by DiagnoseDaemon for `STACK_TRACE` requests. When a diagnose request arrives, the daemon skips collecting and logging stack traces if the last collection happened less than `diagnose_stack_trace_interval_ms` milliseconds ago. Increase this value to reduce CPU overhead and log volume from frequent stack dumps; decrease it to capture more frequent traces to debug transient issues (for example, in load fail-point simulations of long `TabletsChannel::add_chunk` blocking).
- Introduced in: v3.5.0

### lake_replication_slow_log_ms

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold for emitting slow-log entries during lake replication. After each file copy the code measures elapsed time in microseconds and marks the operation as slow when elapsed time is greater than or equal to `lake_replication_slow_log_ms * 1000`. When triggered, StarRocks writes an INFO log with file size, cost and trace metrics for that replicated file. Increase the value to reduce noisy slow logs for large/slow transfers; decrease it to detect and surface smaller slow-copy events sooner.
- Introduced in: -

### load_rpc_slow_log_frequency_threshold_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls how frequently the system prints slow-log entries for load RPCs that exceed their configured RPC timeout. The slow-log also includes the load channel runtime profile. Setting this value to 0 causes per-timeout logging in practice.
- Introduced in: v3.4.3, v3.5.0

### log_buffer_level

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The strategy for flushing logs. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffered in memory.
- Introduced in: -

### pprof_profile_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory path where StarRocks writes pprof artifacts (Jemalloc heap snapshots and gperftools CPU profiles).
- Introduced in: v3.2.0

### sys_log_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL).
- Introduced in: -

### sys_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: Yes (from v3.3.0, v3.2.7, and v3.1.12)
- Description: The severity levels into which system log entries are classified. Valid values: INFO, WARNING, ERROR, and FATAL. This item was changed to a dynamic configuration from v3.3.0, v3.2.7, and v3.1.12 onwards.
- Introduced in: -

### sys_log_roll_mode

- Default: SIZE-MB-1024
- Type: String
- Unit: -
- Is mutable: No
- Description: The mode in which system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-`size. The default value indicates that logs are segmented into rolls, each of which is 1 GB.
- Introduced in: -

### sys_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of log rolls to reserve.
- Introduced in: -

### sys_log_timezone

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to show timezone information in the log prefix. `true` indicates to show timezone information, `false` indicates not to show.
- Introduced in: -

### sys_log_verbose_level

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The level of the logs to be printed. This configuration item is used to control the output of logs initiated with VLOG in codes.
- Introduced in: -

### sys_log_verbose_modules

- Default: 
- Type: Strings
- Unit: -
- Is mutable: No
- Description: Specifies the file names (without extensions) or file name wildcards for which VLOG logs should be printed. Multiple file names can be separated by commas. For example, if you set this configuration item to `storage_engine,tablet_manager`, StarRocks prints VLOG logs from the storage_engine.cpp and tablet_manager.cpp files. You can also use wildcards, e.g., set to `*` to print VLOG logs from all files. The VLOG log printing level is controlled by the `sys_log_verbose_level` parameter.
- Introduced in: -

## Server

### abort_on_large_memory_allocation

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When a single allocation request exceeds the configured large-allocation threshold (g_large_memory_alloc_failure_threshold `>` 0 and requested size `>` threshold), this flag controls how the process responds. If true, StarRocks calls std::abort() immediately (hard crash) when such a large allocation is detected. If false, the allocation is blocked and the allocator returns failure (nullptr or ENOMEM) so callers can handle the error. This check only takes effect for allocations that are not wrapped with the TRY_CATCH_BAD_ALLOC path (the mem hook uses a different flow when bad-alloc is being caught). Enable for fail-fast debugging of unexpected huge allocations; keep disabled in production unless you want an immediate process abort on over-large allocation attempts.
- Introduced in: v3.4.3, 3.5.0, 4.0.0

### arrow_flight_port

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: TCP port for the BE Arrow Flight SQL server. `-1` indicaes to disable the Arrow Flight service. On non-macOS builds, BE invokes Arrow Flight SQL Server with this port during startup; if the port is unavailable, the server startup fails and the BE process exits. The configured port is reported to the FE in the heartbeat payload.
- Introduced in: v3.4.0, v3.5.0

### be_exit_after_disk_write_hang_second

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The length of time that the BE waits to exit after the disk hangs.
- Introduced in: -

### be_http_num_workers

- Default: 48
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used by the HTTP server.
- Introduced in: -

### be_http_port

- Default: 8040
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE HTTP server port.
- Introduced in: -

### be_port

- Default: 9060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE thrift server port, which is used to receive requests from FEs.
- Introduced in: -

### be_service_threads

- Default: 64
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of worker threads the BE Thrift server uses to serve backend RPC/execution requests. This value is passed to ThriftServer when creating the BackendService and controls how many concurrent request handlers are available; requests are queued when all worker threads are busy. Tune based on expected concurrent RPC load and available CPU/memory: increasing it raises concurrency but also per-thread memory and context-switch cost, decreasing it limits parallel handling and may increase request latency.
- Introduced in: v3.2.0

### brpc_connection_type

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

### brpc_max_body_size

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum body size of a bRPC.
- Introduced in: -

### brpc_max_connections_per_server

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of persistent bRPC connections the client keeps for each remote server endpoint. For each endpoint `BrpcStubCache` creates a `StubPool` whose `_stubs` vector is reserved to this size. On first accesses, new stubs are created until the limit is reached. After that, existing stubs are returned in a round‑robin fashion. Increasing this value raises per‑endpoint concurrency (reduces contention on a single channel) at the cost of more file descriptors, memory, and channels.
- Introduced in: v3.2.0

### brpc_num_threads

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of bthreads of a bRPC. The value `-1` indicates the same number with the CPU threads.
- Introduced in: -

### brpc_port

- Default: 8060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE bRPC port, which is used to view the network statistics of bRPCs.
- Introduced in: -

### brpc_socket_max_unwritten_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the per-socket limit for unwritten outbound bytes in the bRPC server. When the amount of buffered, not-yet-written data on a socket reaches this limit, subsequent `Socket.Write` calls fail with EOVERCROWDED. This prevents unbounded per-connection memory growth but can cause RPC send failures for very large messages or slow peers. Align this value with `brpc_max_body_size` to ensure single-message bodies are not larger than the allowed unwritten buffer. Increasing the value raises memory usage per connection.
- Introduced in: v3.2.0

### brpc_stub_expire_s

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expire time of bRPC stub cache. The default value is 60 minutes.
- Introduced in: -

### compress_rowbatches

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to compress the row batches in RPCs between BEs. `true` indicates compressing the row batches, and `false` indicates not compressing them.
- Introduced in: -

### consistency_max_memory_limit_percent

- Default: 20
- Type: Int
- Unit: -
- Is mutable: No
- Description: Percentage cap used to compute the memory budget for consistency-related tasks. During BE startup, the final consistency limit is computed as the minimum of the value parsed from `consistency_max_memory_limit` (bytes) and (`process_mem_limit * consistency_max_memory_limit_percent / 100`). If `process_mem_limit` is unset (-1), consistency memory is considered unlimited. For `consistency_max_memory_limit_percent`, values less than 0 or greater than 100 are treated as 100. Adjusting this value increases or decreases memory reserved for consistency operations and therefore affects memory available for queries and other services.
- Introduced in: v3.2.0

### delete_worker_count_normal_priority

- Default: 2
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Number of normal-priority worker threads dedicated to handling delete (REALTIME_PUSH with DELETE) tasks on the BE agent. At startup this value is added to delete_worker_count_high_priority to size the DeleteTaskWorkerPool (see agent_server.cpp). The pool assigns the first delete_worker_count_high_priority threads as HIGH priority and the rest as NORMAL; normal-priority threads process standard delete tasks and contribute to overall delete throughput. Increase to raise concurrent delete capacity (higher CPU/IO usage); decrease to reduce resource contention.
- Introduced in: v3.2.0

### disable_mem_pools

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to disable MemPool. When this item is set to `true`, the MemPool chunk pooling is disabled so each allocation gets its own sized chunk instead of reusing or increasing pooled chunks. Disabling pooling reduces long-lived retained buffer memory at the cost of more frequent allocations, increased number of chunks, and skipped integrity checks (which are avoided because of the large chunk count). Keep `disable_mem_pools` as `false` (default) to benefit from allocation reuse and fewer system calls. Set it to `true` only when you must avoid large pooled memory retention (for example, low-memory environments or diagnostic runs).
- Introduced in: v3.2.0

### enable_https

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the BE's bRPC server is configured to use TLS: `ServerOptions.ssl_options` will be populated with the certificate and private key specified by `ssl_certificate_path` and `ssl_private_key_path` at BE startup. This enables HTTPS/TLS for incoming bRPC connections; clients must connect using TLS. Ensure the certificate and key files exist, are accessible to the BE process, and match bRPC/SSL expectations.
- Introduced in: v4.0.0

### enable_jemalloc_memory_tracker

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the BE starts a background thread (jemalloc_tracker_daemon) that polls jemalloc statistics (once per second) and updates the GlobalEnv jemalloc metadata MemTracker with the jemalloc "stats.metadata" value. This ensures jemalloc metadata consumption is included in StarRocks process memory accounting and prevents under‑reporting of memory used by jemalloc internals. The tracker is only compiled/started on non‑macOS builds (#ifndef __APPLE__) and runs as a daemon thread named "jemalloc_tracker_daemon". Because this setting affects startup behaviour and threads that maintain MemTracker state, changing it requires a restart. Disable only if jemalloc is not used or when jemalloc tracking is intentionally managed differently; otherwise keep enabled to maintain accurate memory accounting and allocation safeguards.
- Introduced in: v3.2.12

### enable_jvm_metrics

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the system initializes and registers JVM-specific metrics at startup. When enabled the metrics subsystem will create JVM-related collectors (for example, heap, GC and thread metrics) for export, and when disabled, those collectors are not initialized. This parameter is intended for forward compatibility and may be removed in a future release. Use `enable_system_metrics` to control system-level metric collection.
- Introduced in: v4.0.0

### get_pindex_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the number of worker threads for the "get_pindex" thread pool in UpdateManager, which is used to load / fetch persistent index data (used when applying rowsets for primary-key tables). At runtime, a config update will adjust the pool's maximum threads: if `>0` that value is applied; if 0 the runtime callback uses the number of CPU cores (CpuInfo::num_cores()). On initialization the pool's max threads is computed as max(get_pindex_worker_count, max_apply_thread_cnt * 2) where max_apply_thread_cnt is the apply-thread pool maximum. Increase to raise parallelism for pindex loading; lowering reduces concurrency and memory/CPU usage.
- Introduced in: v3.2.0

### heartbeat_service_port

- Default: 9050
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE heartbeat service port, which is used to receive heartbeats from FEs.
- Introduced in: -

### heartbeat_service_thread_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The thread count of the BE heartbeat service.
- Introduced in: -

### local_library_dir

- Default: `${UDF_RUNTIME_DIR}`
- Type: string
- Unit: -
- Is mutable: No
- Description: Local directory on the BE where UDF (user-defined function) libraries are staged and where Python UDF worker processes operate. StarRocks copies UDF libraries from HDFS into this path, creates per-worker Unix domain sockets at `<local_library_dir>/pyworker_<pid>`, and chdirs Python worker processes into this directory before exec. The directory must exist, be writable by the BE process, and reside on a filesystem that supports Unix domain sockets (i.e., a local filesystem). Because this config is immutable at runtime, set it before startup and ensure adequate permissions and disk space on each BE.
- Introduced in: v3.2.0

### max_transmit_batched_bytes

- Default: 262144
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Maximum number of serialized bytes to accumulate in a single transmit request before it is flushed to the network. Sender implementations add serialized ChunkPB payloads into a PTransmitChunkParams request and send the request once the accumulated bytes exceed `max_transmit_batched_bytes` or when EOS is reached. Increase this value to reduce RPC frequency and improve throughput at the cost of higher per-request latency and memory use; reduce it to lower latency and memory but increase RPC rate.
- Introduced in: v3.2.0

### mem_limit

- Default: 90%
- Type: String
- Unit: -
- Is mutable: No
- Description: BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100G"). The default hard limit is 90% of the server's memory size, and the soft limit is 80%. You need to configure this parameter if you want to deploy StarRocks with other memory-intensive services on a same server.
- Introduced in: -

### memory_max_alignment

- Default: 16
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum byte alignment that MemPool will accept for aligned allocations. Increase this value only when callers require larger alignment (for SIMD, device buffers, or ABI constraints). Larger values increase per-allocation padding and reserved memory waste and must remain within what the system allocator and platform support.
- Introduced in: v3.2.0

### memory_urgent_level

- Default: 85
- Type: long
- Unit: Percentage (0-100)
- Is mutable: Yes
- Description: The emergency memory water‑level expressed as a percentage of the process memory limit. When process memory consumption exceeds `(limit * memory_urgent_level / 100)`, BE triggers immediate memory reclamation, which forces data cache shrinkage, evicts update caches, and causes persistent/lake MemTables to be treated as "full" so they will be flushed/compacted soon. The code validates that this setting must be greater than `memory_high_level`, and `memory_high_level` must be greater or equal to `1`, and less thant or equal to `100`). A lower value causes more aggressive, earlier reclamation, that is, more frequent cache evictions and flushes. A higher value delays reclamation and risks OOM if too close to 100. Tune this item together with `memory_high_level` and Data Cache-related auto‑adjust settings.
- Introduced in: v3.2.0

### net_use_ipv6_when_priority_networks_empty

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

### num_cores

- Default: 0
- Type: Int
- Unit: Cores
- Is mutable: No
- Description: Controls the number of CPU cores the system will use for CPU-aware decisions (for example, thread-pool sizing and runtime scheduling). A value of 0 enables auto-detection: the system reads `/proc/cpuinfo` and uses all available cores. If set to a positive integer, that value overrides the detected core count and becomes the effective core count. When running inside containers, cgroup cpuset or cpu quota settings can further restrict usable cores; `CpuInfo` also respects those cgroup limits.
- Introduced in: v3.2.0

### plugin_path

- Default: `${STARROCKS_HOME}/plugin`
- Type: String
- Unit: -
- Is mutable: No
- Description: Filesystem directory where StarRocks loads external plugins (dynamic libraries, connector artifacts, UDF binaries, etc.). `plugin_path` should point to a directory accessible by the BE process (read and execute permissions) and must exist before plugins are loaded. Ensure correct ownership and that plugin files use the platform's native binary extension (for example, .so on Linux).
- Introduced in: v3.2.0

### priority_networks

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as `10.10.10.0/24`. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

### rpc_compress_ratio_threshold

- Default: 1.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Threshold (uncompressed_size / compressed_size) used when deciding whether to send serialized row-batches over the network in compressed form. When compression is attempted (e.g., in DataStreamSender, exchange sink, tablet sink index channel, dictionary cache writer), StarRocks computes compress_ratio = uncompressed_size / compressed_size; it uses the compressed payload only if compress_ratio `>` rpc_compress_ratio_threshold. With the default 1.1, compressed data must be at least ~9.1% smaller than uncompressed to be used. Lower the value to prefer compression (more CPU for smaller bandwidth savings); raise it to avoid compression overhead unless it yields larger size reductions. Note: this applies to RPC/shuffle serialization and is effective only when row-batch compression is enabled (compress_rowbatches).
- Introduced in: v3.2.0

### enable_rpc_compress_overflow_skip

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls the behavior when the serialized chunk size exceeds the compression codec's maximum allowed input size. When set to `true` (default), StarRocks logs a warning and skips compression, sending the data uncompressed instead. When set to `false`, StarRocks returns an `InternalError` and aborts the RPC.
- Introduced in: -

### ssl_private_key_path

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: File system path to the TLS/SSL private key (PEM) that the BE's brpc server uses as the private key for the default certificate. When `enable_https` is set to `true`, the system sets `brpc::ServerOptions::ssl_options().default_cert.private_key` to this path at process start. The file must be accessible by the BE process and must match the certificate provided by `ssl_certificate_path`. If this value is not set or the file is missing or unaccessible, HTTPS will not be configured and the bRPC server may fail to start. Protect this file with restrictive filesystem permissions (for example, 600).
- Introduced in: v4.0.0

### thrift_client_retry_interval_ms

- Default: 100
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval at which a thrift client retries.
- Introduced in: -

### thrift_connect_timeout_seconds

- Default: 3
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: Connection timeout (in seconds) used when creating Thrift clients. ClientCacheHelper::_create_client multiplies this value by 1000 and passes it to ThriftClientImpl::set_conn_timeout(), so it controls the TCP/connect handshake timeout for new Thrift connections opened by the BE client cache. This setting affects only connection establishment; send/receive timeouts are configured separately. Very small values can cause spurious connection failures on high-latency networks, while large values delay detection of unreachable peers.
- Introduced in: v3.2.0

### thrift_port

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Port used to export the internal Thrift-based BackendService. When the process runs as a Compute Node and this item is set to a non-zero value, it overrides `be_port` and the Thrift server binds to this value; otherwise `be_port` is used. This configuration is deprecated — setting a non-zero `thrift_port` logs a warning advising to use `be_port` instead.
- Introduced in: v3.2.0

### thrift_rpc_connection_max_valid_time_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Maximum valid time for a thrift RPC connection. A connection will be closed if it has existed in the connection pool for longer than this value. It must be set consistent with FE configuration `thrift_client_timeout_ms`.
- Introduced in: -

### thrift_rpc_max_body_size

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description: The maximum string body size of RPC. `0` indicates the size is unlimited.
- Introduced in: -

### thrift_rpc_strict_mode

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether thrift's strict execution mode is enabled. For more information on thrift strict mode, see [Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md).
- Introduced in: -

### thrift_rpc_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout for a thrift RPC.
- Introduced in: -

### transaction_apply_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the minimum number of threads for the "update_apply" thread pool in BE's UpdateManager — the pool that applies rowsets for primary-key tables. A value of 0 disables a fixed minimum (no enforced lower bound); when transaction_apply_worker_count is also 0 the pool's max threads defaults to the number of CPU cores, so effective worker capacity equals CPU cores. You can raise this to guarantee a baseline concurrency for applying transactions; setting it too high may increase CPU contention. Changes are applied at runtime via the update_config HTTP handler (it calls update_min_threads on the apply thread pool).
- Introduced in: v3.2.11

### transaction_publish_version_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Threads
- Is mutable: Yes
- Description: Sets the minimum number of threads reserved in the AgentServer "publish_version" dynamic thread pool (used to publish transaction versions / handle TTaskType::PUBLISH_VERSION tasks). At startup the pool is created with min = max(config value, MIN_TRANSACTION_PUBLISH_WORKER_COUNT) (MIN_TRANSACTION_PUBLISH_WORKER_COUNT = 1), so the default 0 results in a minimum of 1 thread. Changing this value at runtime invokes the update callback to call ThreadPool::update_min_threads, raising or lowering the pool's guaranteed minimum (but not below the enforced minimum of 1). Coordinate with transaction_publish_version_worker_count (max threads) and transaction_publish_version_thread_pool_idle_time_ms (idle timeout).
- Introduced in: v3.2.11

### use_mmap_allocate_chunk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system allocates chunks using anonymous private mmap mappings (MAP_ANONYMOUS | MAP_PRIVATE) and frees them with munmap. Enabling this may create many virtual memory mappings, thus you must raise the kernel limit (as root user, running `sysctl -w vm.max_map_count=262144` or `echo 262144 > /proc/sys/vm/max_map_count`), and set `chunk_reserved_bytes_limit` to a relatively large value. Otherwise, enabling mmap can cause very poor performance due to frequent mapping/unmapping.
- Introduced in: v3.2.0

### ssl_certificate_path

- Default: 
- Type: String
- Unit: -
- Is mutable: No
- Description: Absolute path to the TLS/SSL certificate file (PEM) that the BE's brpc server will use when enable_https is true. At BE startup this value is copied into `brpc::ServerOptions::ssl_options().default_cert.certificate`; you must also set `ssl_private_key_path` to the matching private key. Provide the server certificate and any intermediate certificates in PEM format (certificate chain) if required by your CA. The file must be readable by the StarRocks BE process and is applied only at startup. If unset or invalid while enable_https is enabled, brpc TLS setup may fail and prevent the server from starting correctly.
- Introduced in: v4.0.0

## Metadata and cluster management

### cluster_id

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: Global cluster identifier for this StarRocks backend. At startup StorageEngine reads config::cluster_id into its effective cluster id and verifies that all data root paths contain the same cluster id (see StorageEngine::_check_all_root_path_cluster_id). A value of -1 means "unset" — the engine may derive the effective id from existing data directories or from master heartbeats. If a non‑negative id is configured, any mismatch between configured id and ids stored in data directories will cause startup verification to fail (Status::Corruption). When some roots lack an id and the engine is allowed to write ids (options.need_write_cluster_id), it will persist the effective id into those roots.
- Introduced in: v3.2.0

### consistency_max_memory_limit

- Default: 10G
- Type: String
- Unit: -
- Is mutable: No
- Description: Memory size specification for the CONSISTENCY memory tracker.
- Introduced in: v3.2.0

### make_snapshot_rpc_timeout_ms

- Default: 20000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Sets the Thrift RPC timeout in milliseconds used when making a snapshot on a remote BE. Increase this value when remote snapshot creation regularly exceeds the default timeout; reduce it to fail faster on unresponsive BEs. Note other timeouts may affect end-to-end operations (for example the effective tablet-writer open timeout can relate to `tablet_writer_open_rpc_timeout_sec` and `load_timeout_sec`).
- Introduced in: v3.2.0

### metadata_cache_memory_limit_percent

- Default: 30
- Type: Int
- Unit: Percent
- Is mutable: Yes
- Description: Sets the metadata LRU cache size as a percentage of the process memory limit. At startup StarRocks computes cache bytes as (process_mem_limit * metadata_cache_memory_limit_percent / 100) and passes that to the metadata cache allocator. The cache is only used for non-PRIMARY_KEYS rowsets (PK tables are not supported) and is enabled only when metadata_cache_memory_limit_percent &gt; 0; set it &lt;= 0 to disable the metadata cache. Increasing this value raises metadata cache capacity but reduces memory available to other components; tune based on workload and system memory. Not active in BE_TEST builds.
- Introduced in: v3.2.10

### retry_apply_interval_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Base interval (in seconds) used when scheduling retries of failed tablet apply operations. It is used directly to schedule a retry after a submission failure and as the base multiplier for backoff: the next retry delay is calculated as min(600, `retry_apply_interval_second` * failed_attempts). The code also uses `retry_apply_interval_second` to compute the cumulative retry duration (arithmetic-series sum) which is compared against `retry_apply_timeout_second` to decide whether to keep retrying. Effective only when `enable_retry_apply` is true. Increasing this value lengthens both individual retry delays and the cumulative time spent retrying; decreasing it makes retries more frequent and may increase the number of attempts before reaching `retry_apply_timeout_second`.
- Introduced in: v3.2.9

### retry_apply_timeout_second

- Default: 7200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum cumulative retry time (in seconds) allowed for applying a pending version before the apply process gives up and the tablet enters an error state. The apply logic accumulates exponential/backoff intervals based on `retry_apply_interval_second` and compares the total duration against `retry_apply_timeout_second`. If `enable_retry_apply` is true and the error is considered retryable, apply attempts will be rescheduled until the accumulated backoff exceeds `retry_apply_timeout_second`; then apply stops and the tablet transitions to error. Explicitly non-retryable errors (e.g., Corruption) are not retried regardless of this setting. Tune this value to control how long StarRocks will keep retrying apply operations (default 7200s = 2 hours).
- Introduced in: v3.3.13, v3.4.3, v3.5.0

### txn_commit_rpc_timeout_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Maximum allowed lifetime (in milliseconds) for Thrift RPC connections used by BE stream-load and transaction commit calls. StarRocks sets this value as the `thrift_rpc_timeout_ms` on requests sent to FE (used in stream_load planning, loadTxnBegin/loadTxnPrepare/loadTxnCommit, and getLoadTxnStatus). If a connection has been pooled longer than this value it will be closed. When a per-request timeout (`ctx->timeout_second`) is provided, the BE computes the RPC timeout as rpc_timeout_ms = max(ctx*1000/4, min(ctx*1000/2, txn_commit_rpc_timeout_ms)), so the effective RPC timeout is bounded by the context and this configuration. Keep this consistent with FE's `thrift_client_timeout_ms` to avoid mismatched timeouts.
- Introduced in: v3.2.0

### txn_map_shard_size

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of lock-map shards used by the transaction manager to partition transaction locks and reduce contention. Its value should be a power of two (2^n); increasing it augments concurrency and reduces lock contention at the cost of additional memory and marginal bookkeeping overhead. Choose a shard count sized for expected concurrent transactions and available memory.
- Introduced in: v3.2.0

### txn_shard_size

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: Controls the number of lock shards used by the transaction manager. This value determines the shard size for txn locks. It must be a power of two; Setting it to a larger value reduces lock contention and improves concurrent COMMIT/PUBLISH throughput at the expense of additional memory and finer-grained internal bookkeeping.
- Introduced in: v3.2.0

### update_schema_worker_count

- Default: 3
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the maximum number of worker threads in the backend's "update_schema" dynamic ThreadPool that processes TTaskType::UPDATE_SCHEMA tasks. The ThreadPool is created in agent_server during startup with a minimum of 0 threads (it can scale down to zero when idle) and a max equal to this setting; the pool uses the default idle timeout and an effectively unlimited queue. Increase this value to allow more concurrent schema-update tasks (higher CPU and memory usage), or lower it to limit parallel schema operations.
- Introduced in: v3.2.3

### update_tablet_meta_info_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the maximum number of worker threads in the backend thread pool that handles tablet metadata update tasks. The thread pool is created during backend startup with a minimum of 0 threads (it can scale down to zero when idle) and a max equal to this setting (clamped to at least 1). Updating this value at runtime adjusts the pool's max threads. Increase it to allow more concurrent metadata-update tasks, or lower it to limit concurrency.
- Introduced in: v4.1.0, v4.0.6, v3.5.13
