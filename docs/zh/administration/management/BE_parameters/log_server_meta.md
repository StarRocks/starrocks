---
displayed_sidebar: docs
sidebar_label: "日志、服务器和元数据"
keywords: ['Canshu']
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 配置项 - 日志、服务器和元数据

<BEConfigMethod />

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```SQL
SELECT * FROM information_schema.be_configs [WHERE NAME LIKE "%<name_pattern>%"]
```

## 配置 BE 参数

<PostBEConfig />

<StaticBEConfigNote />

---

当前主题包含以下类型的 BE 配置：
- [日志](#日志)
- [服务器](#服务器)
- [元数据和集群管理](#元数据和集群管理)

## 日志

### diagnose_stack_trace_interval_ms

- 默认值：1800000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：DiagnoseDaemon 处理 `STACK_TRACE` 请求时，两次堆栈诊断的最小时间间隔。若距离上次采集不足该间隔则跳过，以减少频繁堆栈抓取的 CPU 和日志开销；排查瞬时问题可适当调小。
- 引入版本：v3.5.0

### load_rpc_slow_log_frequency_threshold_seconds

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：控制导入类 RPC 超时慢日志的打印频率。超过超时的导入 RPC 会按该间隔输出慢日志（包含 load channel 运行时 profile）；设为 0 则几乎每次超时都会记录。
- 引入版本：v3.4.3, v3.5.0

### log_buffer_level

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：日志落盘的策略。默认值表示日志缓存在内存中。有效值为 `-1` 和 `0`。`-1` 表示日志不在内存中缓存。
- 引入版本：-

### pprof_profile_dir

- 默认值：`${STARROCKS_HOME}/log`
- 类型：string
- 单位：-
- 是否动态：否
- 描述：StarRocks 写入 pprof 工件（jemalloc 堆快照和 gperftools CPU 配置文件）的目录路径。代码会在此目录下构造文件名（例如 `heap_profile.<pid>.<rand>` 和 `starrocks_profile.<pid>.<rand>`），并且 /pprof/ 下的 HTTP 处理器会提供这些配置文件。在启动时，如果 `pprof_profile_dir` 非空，StarRocks 会尝试创建该目录。请确保该路径存在或对 BE 进程可写，并且有足够的磁盘空间；性能分析可能产生较大的文件，并在运行时影响性能。
- 引入版本：v3.2.0

### sys_log_dir

- 默认值：`${STARROCKS_HOME}/log`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：存放日志的地方，包括 INFO、WARNING、ERROR、FATAL 日志。
- 引入版本：-

### sys_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否动态：是（自 v3.3.0、v3.2.7 及 v3.1.12 起）
- 描述：日志级别。有效值：INFO、WARNING、ERROR、FATAL。自 v3.3.0、v3.2.7 及 v3.1.12 起，该参数变为动态参数。
- 引入版本：-

### sys_log_roll_mode

- 默认值：SIZE-MB-1024
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志分卷的模式。有效值包括 `TIME-DAY`、`TIME-HOUR` 和 `SIZE-MB-` 大小。默认值表示日志被分割成大小为 1GB 的日志卷。
- 引入版本：-

### sys_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：日志卷保留的数目。
- 引入版本：-

### sys_log_timezone

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在日志前缀中显示时区信息。`true` 表示显示时区信息，`false` 表示不显示。
- 引入版本：-

### sys_log_verbose_level

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：日志显示的级别，用于控制代码中 VLOG 开头的日志输出。
- 引入版本：-

### sys_log_verbose_modules

- 默认值：
- 类型：Strings
- 单位：-
- 是否动态：否
- 描述：设置需要输出 VLOG 日志的文件名（去掉文件扩展名）或文件名通配符。可以指定多个文件名，用逗号分隔。例如，如果将此配置项设置为 `storage_engine,tablet_manager`，StarRocks 将打印 storage_engine.cpp、tablet_manager.cpp 文件的 VLOG 日志。您也可以使用通配符，如设置为 `*` 表示打印所有文件的 VLOG 日志。VLOG 日志打印级别通过 `sys_log_verbose_level` 参数控制。
- 引入版本：-

## 服务器

### abort_on_large_memory_allocation

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当单次分配超过 `g_large_memory_alloc_failure_threshold` 时的处理策略。设为 `true` 会直接 `std::abort()`（硬退出）；设为 `false` 则阻塞并让分配返回失败（nullptr/ENOMEM）。仅对未走 TRY_CATCH_BAD_ALLOC 的分配生效，生产环境通常保持关闭，排查异常超大分配时可暂时开启。
- 引入版本：v3.4.3, v3.5.0, v4.0.0

### arrow_flight_port

- 默认值：-1
- 类型：Int
- 单位：Port
- 是否动态：否
- 描述：TCP 端口，用于 BE 的 Arrow Flight SQL 服务器。将其设置为 `-1` 以禁用 Arrow Flight 服务。在非 macOS 构建中，BE 在启动期间会调用通过该端口启动 Arrow Flight SQL Server；如果端口不可用，服务器启动会失败并且 BE 进程退出。配置的端口会在心跳负载中上报给 FE。
- 引入版本：v3.4.0, v3.5.0

### be_exit_after_disk_write_hang_second

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：磁盘挂起后触发 BE 进程退出的等待时间。
- 引入版本：-

### be_http_num_workers

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HTTP Server 线程数。
- 引入版本：-

### be_http_port

- 默认值：8040
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE HTTP Server 端口。
- 引入版本：-

### be_port

- 默认值：9060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 上 Thrift Server 的端口，用于接收来自 FE 的请求。
- 引入版本：-

### be_service_threads

- 默认值：64
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：BE Thrift 服务用于处理后端 RPC/执行请求的工作线程数。该值在创建 BackendService 时传递给 ThriftServer，用于控制可用的并发请求处理器数量；当所有工作线程都忙碌时，请求会排队。根据预期的并发 RPC 负载和可用的 CPU/内存调整：增大该值可以提高并发度，但也会增加每线程内存和上下文切换开销；减小则限制并行处理能力，可能增加请求延迟。
- 引入版本：v3.2.0

### brpc_connection_type

- 默认值：`"single"`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：bRPC 渠道的连接模式。`single`（默认）每个渠道复用一条长连接；`pooled` 为每端点维护连接池以提升并发（增加 FD 占用）；`short` 为短连接，每次 RPC 建连以减少持久连接但会提高延迟和建连开销。
- 引入版本：v3.2.5

### brpc_max_body_size

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bRPC 最大的包容量。
- 引入版本：-

### brpc_max_connections_per_server

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：客户端为每个远程服务器端点保持的最大持久 bRPC 连接数。对于每个端点，`BrpcStubCache` 会创建一个 `StubPool`，其 `_stubs` 向量会预留为此大小。首次访问时会创建新的 stub，直到达到限制。之后，现有的 stub 将以轮询方式返回。增加此值可以提高每个端点的并发度（降低单通道的争用），代价是更多的文件描述符、内存和通道数。
- 引入版本：v3.2.0

### brpc_num_threads

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的 bthread 线程数量，`-1` 表示和 CPU 核数一样。
- 引入版本：-

### brpc_port

- 默认值：8060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的端口，可以查看 bRPC 的一些网络统计信息。
- 引入版本：-

### brpc_socket_max_unwritten_bytes

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：单个 bRPC 套接字允许的未写出数据上限。达到上限后 `Socket.Write` 会返回 EOVERCROWDED，以限制单连接内存占用；值过小可能导致大消息/慢端发送失败，过大则提高内存占用。
- 引入版本：v3.2.0

### brpc_stub_expire_s

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：BRPC stub 缓存的过期时间，默认 60 minutes。
- 引入版本：-

### compress_rowbatches

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否压缩 RowBatch。`true` 表示压缩（减少带宽、增加 CPU），`false` 表示不压缩。
- 引入版本：-

### consistency_max_memory_limit_percent

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：一致性相关任务的内存百分比上限。启动时会取 `consistency_max_memory_limit`（字节）与 `process_mem_limit * percent / 100` 的较小值作为最终上限；`process_mem_limit` 为 -1 时视为不限制。非法取值(&lt;0 或 &gt;100)按 100 处理。
- 引入版本：v3.2.0

### consistency_max_memory_limit

- 默认值：1099511627776
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：一致性相关任务的内存字节上限。最终上限取该值与 `process_mem_limit * consistency_max_memory_limit_percent / 100` 的最小值；`process_mem_limit` 为 -1 时视为不限制。
- 引入版本：v3.2.0

### disable_mem_pools

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否禁用 MemPool 的 chunk 复用。禁用后每次分配使用独立 chunk，降低长时间保留的池化内存但增加分配次数和 chunk 数量。一般保持默认 false，仅在诊断或低内存场景下考虑开启。
- 引入版本：v3.2.0

### compress_rowbatches

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。`true` 表示压缩，`false` 表示不压缩。
- 引入版本：-

### consistency_max_memory_limit_percent

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：用于计算一致性相关任务内存预算的百分比上限。在 BE 启动期间，最终的一致性限制计算为从 `consistency_max_memory_limit`（字节）解析出的值与（`process_mem_limit * consistency_max_memory_limit_percent / 100`）两者的最小值。如果 `process_mem_limit` 未设置（-1），则一致性内存视为无限。对于 `consistency_max_memory_limit_percent`，小于 0 或大于 100 的值将被视为 100。调整此值会增加或减少为一致性操作保留的内存，从而影响查询和其他服务可用的内存。
- 引入版本：v3.2.0

### delete_worker_count_normal_priority

- 默认值：2
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：在 BE agent 上专门用于处理删除（REALTIME_PUSH with DELETE）任务的普通优先级工作线程数量。启动时，此值会与 delete_worker_count_high_priority 相加以确定 DeleteTaskWorkerPool 的大小（参见 agent_server.cpp）。线程池会将前 delete_worker_count_high_priority 个线程分配为 HIGH 优先级，其余为 NORMAL；普通优先级线程处理标准删除任务并有助于整体删除吞吐量。增加此值可提高并发删除能力（会增加 CPU/IO 使用）；减少则可降低资源争用。
- 引入版本：v3.2.0

### disable_mem_pools

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否禁用 MemPool。将此项设置为 `true` 时，会禁用 MemPool 的 chunk 池化机制，使每次分配得到其自身大小的 chunk，而不是重用或扩展池化的 chunk。禁用池化会减少长期保留的缓冲区内存，但代价是更频繁的分配、更多的 chunk 数量以及跳过完整性检查（因大量 chunk 而避免）。保持 `disable_mem_pools` 为 `false`（默认）可以受益于分配重用和更少的系统调用。仅在必须避免大规模池化内存保留（例如内存受限的环境或诊断运行）时，将其设为 `true`。
- 引入版本：v3.2.0

### enable_https

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：当此项设置为 `true` 时，BE 的 bRPC 服务配置为使用 TLS，并使用 `ssl_certificate_path` 和 `ssl_private_key_path` 指定的证书和私钥。这样会为传入的 bRPC 连接启用 HTTPS/TLS。客户端必须使用 TLS 连接。请确保证书和密钥文件存在、且 BE 进程可访问，并符合 bRPC/SSL 的要求。
- 引入版本：v4.0.0

### enable_jemalloc_memory_tracker

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：该项设置为 `true` 时，BE 会启动一个后台线程（`jemalloc_tracker_daemon`），该线程以每秒一次的频率轮询 Jemalloc 统计信息，并将 Jemalloc 的 "stats.metadata" 值更新到 GlobalEnv 的 Jemalloc 元数据 MemTracker 中。这可确保 Jemalloc 元数据的消耗被计入 StarRocks 进程的内存统计，防止低报 Jemalloc 内部使用的内存。该 Tracker 仅在非 macOS 构建上编译/启动（#ifndef __APPLE__），并以名为 "jemalloc_tracker_daemon" 的守护线程运行。仅在未使用 Jemalloc 或者 Jemalloc 跟踪被有意以不同方式管理时才禁用，否则请保持启用以维护准确的内存计量和分配保护。
- 引入版本：v3.2.12

### enable_jvm_metrics

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否初始化并注册 JVM 相关指标（heap、GC、线程等）。这是预留的兼容开关，未来可能移除；系统级指标由 `enable_system_metrics` 控制。
- 引入版本：v4.0.0

### get_pindex_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：为 UpdateManager 中的 "get_pindex" 线程池设置工作线程数，该线程池用于加载/获取持久化索引数据（在为主键表应用 rowset 时使用）。如果设置为大于 0 则系统应用该值；如果设置为 0 则运行时回调使用 CPU 核心数。在初始化时，改线程池的最大线程数计算为 `max(get_pindex_worker_count, max_apply_thread_cnt * 2)`，其中 `max_apply_thread_cnt` 是 apply-thread 池的最大值。增大此值可提高 pindex 加载的并行度；降低则减少并发和内存/CPU 使用。
- 引入版本：v3.2.0

### heartbeat_service_port

- 默认值：9050
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 心跳服务端口，用于接收来自 FE 的心跳。
- 引入版本：-

### heartbeat_service_thread_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：心跳线程数。
- 引入版本：-

### local_library_dir

- 默认值：`${UDF_RUNTIME_DIR}`
- 类型：string
- 单位：-
- 是否动态：否
- 描述：BE 上的本地目录，用于存放 UDF（用户自定义函数）库并作为 Python UDF worker 进程的工作目录。StarRocks 会将 UDF 库从 HDFS 复制到此路径，在 `<local_library_dir>/pyworker_<pid>` 创建每个 worker 的 Unix 域套接字，并在 exec 前将 Python worker 进程的工作目录切换到此目录。该目录必须存在、对 BE 进程可写，并且位于支持 Unix 域套接字的文件系统上（即本地文件系统）。由于该配置在运行时不可变，请在启动前设置并确保每个 BE 上具有足够的权限和磁盘空间。
- 引入版本：v3.2.0

### max_transmit_batched_bytes

- 默认值：262144
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：在单个传输请求中可累积的序列化字节数上限，超过该值后会将请求刷新到网络。发送端实现会将序列化的 ChunkPB 负载加入到 PTransmitChunkParams 请求中，并在累计字节数超过 `max_transmit_batched_bytes` 或达到 EOS 时发送该请求。增大此值可降低 RPC 频率并提高吞吐量，但会以更高的每次请求延迟和内存使用为代价；减小此值可降低延迟和内存使用，但会增加 RPC 频率。
- 引入版本：v3.2.0

### mem_limit

- 默认值：90%
- 类型：String
- 单位：-
- 是否动态：否
- 描述：BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100G"）。默认的硬限制为服务器内存大小的 90%，软限制为 80%。如果您希望在同一台服务器上同时部署 StarRocks 和其他内存密集型服务，则需要配置此参数。
- 引入版本：-

### memory_max_alignment

- 默认值：16
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：内存分配的最大对齐字节数，用于约束分配对齐的上限。
- 引入版本：-

### num_cores

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：逻辑 CPU 核心数上限。-1 表示使用机器实际核心数；可用于在多租场景下限制 BE 可用核心数。
- 引入版本：-
### memory_urgent_level

- 默认值：85
- 类型：long
- 单位：百分比 (0-100)
- 是否动态：是
- 描述：紧急内存水位，以进程内存限制的百分比表示。当进程内存消耗超过 `(limit * memory_urgent_level / 100)` 时，BE 会触发立即的内存回收，强制缩减 data cache、驱逐 update cache，并将 persistent/lake 的 MemTable 视为“已满”，以便尽快刷写/压缩。代码校验此设置必须大于 `memory_high_level`，且 `memory_high_level` 必须大于或等于 `1` 且小于或等于 `100`。较低的值会导致更激进、更早的回收（即更频繁的缓存驱逐和刷写）；较高的值会延迟回收，如果接近 100 则有 OOM 风险。请与 `memory_high_level` 及 Data Cache 相关的自动调整设置一起调优。
- 引入版本：v3.2.0

### net_use_ipv6_when_priority_networks_empty

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址的布尔值。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本：-

### priority_networks

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：为有多个 IP 地址的服务器声明 IP 选择策略。请注意，最多应该有一个 IP 地址与此列表匹配。此参数的值是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。如果没有 IP 地址匹配此列表中的条目，系统将随机选择服务器的一个可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，系统将默认使用 IPv4 地址。您可以通过将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本：-

### rpc_compress_ratio_threshold

- 默认值：1.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：在决定是否以压缩形式通过网络发送序列化的 row-batches 时使用的阈值（uncompressed_size / compressed_size）。当尝试压缩时（例如在 DataStreamSender、exchange sink、tablet sink 的索引通道、dictionary cache writer 中），StarRocks 会计算 compress_ratio = uncompressed_size / compressed_size；仅当 compress_ratio `>` rpc_compress_ratio_threshold 时才使用压缩后的负载。默认值 1.1 意味着压缩数据必须至少比未压缩小约 9.1% 才会被使用。将该值调低以偏好压缩（以更多 CPU 换取更小的带宽）；将其调高以避免压缩开销，除非压缩能带来更大的尺寸缩减。注意：此项适用于 RPC/shuffle 序列化，仅在启用 row-batch 压缩（compress_rowbatches）时生效。
- 引入版本：v3.2.0

### enable_rpc_compress_overflow_skip

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制序列化的 chunk 大小超过压缩 codec 允许的最大输入大小时的行为。设置为 `true`（默认）时，StarRocks 记录一条警告日志并跳过压缩，以未压缩形式发送数据。设置为 `false` 时，StarRocks 返回 `InternalError` 并中止 RPC。
- 引入版本：-

### ssl_private_key_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：文件系统中用于 BE 的 bRPC 服务作为默认证书私钥的 TLS/SSL 私钥（PEM）文件路径。当 `enable_https` 设置为 `true` 时生效。该文件必须对 BE 进程可访问，并且必须与由 `ssl_certificate_path` 提供的证书匹配。如果未设置此值或文件丢失或不可访问，则不会配置 HTTPS 并且 bRPC 服务器可能无法启动。请使用严格的文件系统权限（例如 600）保护此文件。
- 引入版本：v4.0.0

### thrift_client_retry_interval_ms

- 默认值：100
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：Thrift Client 默认的重试时间间隔。
- 引入版本：-

### thrift_connect_timeout_seconds

- 默认值：3
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：Thrift 连接建立超时时长（秒）。连接超过该时间未建立会失败重试。
- 引入版本：-

### thrift_port

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：用于导出基于 Thrift 的内部 BackendService 的端口。当进程以 Compute Node 模式运行且该项设置为非零值时，它会覆盖 `be_port` 并使 Thrift 服务器绑定到此值；否则使用 `be_port`。此配置已弃用 — 将非零 `thrift_port` 设置会记录警告，建议改用 `be_port`。
- 引入版本：v3.2.0

### thrift_rpc_connection_max_valid_time_ms

- 默认值：5000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：Thrift RPC 连接的最长有效时间。如果连接池中存在的时间超过此值，连接将被关闭。需要与 FE 配置项 `thrift_client_timeout_ms` 保持一致。
- 引入版本：-

### thrift_rpc_max_body_size

- 默认值：0
- 类型：Int
- 单位：单位：Milliseconds
- 是否动态：否
- 描述：RPC 最大字符串体大小。`0` 表示无限制。
- 引入版本：-

### thrift_rpc_strict_mode

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用了 Thrift 的严格执行模式。 Thrift 严格模式，参见 [Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md)。
- 引入版本：-

### thrift_rpc_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：Thrift RPC 超时的时长。
- 引入版本：-

### transaction_apply_thread_pool_num_min

- 默认值：0
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：为 BE 的 UpdateManager 中的 "update_apply" 线程池设置最小线程数。该线程池用于为主键表应用 rowset。值为 0 表示禁用固定最小值（不强制下限）；当 `transaction_apply_worker_count` 也为 0 时，该线程池的最大线程数默认为 CPU 核心数，因此有效的工作线程容量等于 CPU 核心数。可以增大此值以保证应用事务的基线并发；设置过高可能增加 CPU 争用。
- 引入版本：v3.2.11

### transaction_apply_worker_count

- 默认值：0
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：update_apply 线程池的最大线程数（0 表示按 CPU 核数）。与 `transaction_apply_thread_pool_num_min` 配合控制并发应用主键表 rowset 的能力。
- 引入版本：-

### transaction_apply_worker_idle_time_ms

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：update_apply 线程池中空闲线程的超时时间。超过该时间的空闲线程会被回收。
- 引入版本：-

### retry_apply_interval_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：事务 apply 重试的时间间隔。
- 引入版本：-

### retry_apply_timeout_second

- 默认值：20
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：事务 apply 重试的超时时间。
- 引入版本：-

### txn_commit_rpc_timeout_ms

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：BE 发往 FE 的事务提交/stream load 相关 Thrift RPC 的超时时长上限，也用于连接池中超时连接的关闭。实际超时会结合请求上下文计算，但不会超过该值；需与 FE `thrift_client_timeout_ms` 保持一致以避免不匹配。
- 引入版本：v3.2.0

### enable_retry_apply

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用事务 apply 的重试机制。关闭后发生 apply 失败将不再重试。
- 引入版本：-

### ssl_certificate_path

- 默认值：
- 类型：String
- 单位：-
- 是否动态：否
- 描述：在 enable_https 为 true 时，BE 的 brpc server 将使用的 TLS/SSL 证书文件（PEM 格式）的绝对路径。在 BE 启动时，此值会复制到 `brpc::ServerOptions::ssl_options().default_cert.certificate`；你还必须将 `ssl_private_key_path` 设置为对应的私钥。若 CA 要求，提供服务器证书及任何中间证书的 PEM 格式（证书链）。该文件必须对 StarRocks BE 进程可读，并且仅在启动时应用。如果在 enable_https 启用时该值未设置或无效，brpc 的 TLS 配置可能失败并导致服务器无法正确启动。
- 引入版本：v4.0.0

## 元数据和集群管理

### cluster_id

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 节点在集群的全局集群标识符。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。默认值 -1 表示在 Leader FE 首次启动时随机生成一个。
- 引入版本：v3.2.0

### make_snapshot_rpc_timeout_ms

- 默认值：20000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：设置在远程 BE 上制作快照时使用的 Thrift RPC 超时时间（毫秒）。当远程快照创建经常超过默认超时时，增加此值；若希望在 BE 无响应时更快失败则减小该值。注意其他超时也可能影响端到端操作（例如有效的 tablet-writer 打开超时可能与 `tablet_writer_open_rpc_timeout_sec` 和 `load_timeout_sec` 有关）。
- 引入版本：v3.2.0

### metadata_cache_memory_limit_percent

- 默认值：30
- 类型：Int
- 单位：Percent
- 是否动态：是
- 描述：将元数据 LRU 缓存大小设置为进程内存限制的百分比。启动时 StarRocks 将缓存字节数计算为 (`process_mem_limit * metadata_cache_memory_limit_percent / 100`)，并将其传递给元数据缓存分配器。该缓存仅用于非 PRIMARY_KEYS 行集（不支持 PK 表），并且仅在 `metadata_cache_memory_limit_percent` 大于 0 时启用；将其设置为小于等于 0 可禁用元数据缓存。增加此值会提高元数据缓存容量，但会减少分配给其他组件的内存；请根据工作负载和系统内存进行调优。在 BE_TEST 构建中不生效。
- 引入版本：v3.2.10

### update_schema_worker_count

- 默认值：3
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：配置后端用于处理 TTaskType::UPDATE_SCHEMA 任务的动态 ThreadPool（名称为 "update_schema"）中最大的工作线程数。该 ThreadPool 在 agent_server 启动时创建，最小线程数为 0（空闲时可缩减为零），最大线程数等于此设置；线程池使用默认的空闲超时并具有近乎无限的队列。增加此值可允许更多并发的 schema 更新任务（会增加 CPU 和内存使用），降低则可限制并行的 schema 操作。
- 引入版本：v3.2.3

### update_tablet_meta_info_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：配置后端用于处理 tablet 元数据更新任务的动态线程池中的最大工作线程数。该线程池在后端启动时创建，最小线程数为 0（空闲时可缩减为零），最大线程数等于此设置（最小限制为 1）。运行时修改该值会动态调整线程池的最大线程数。增加此值可提高元数据更新任务的并发度，降低则限制并行度。
- 引入版本：v4.1.0、v4.0.6、v3.5.13
