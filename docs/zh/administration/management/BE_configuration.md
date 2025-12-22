---
displayed_sidebar: docs
keywords: ['Canshu']
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 配置项

<BEConfigMethod />

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## 配置 BE 参数

<PostBEConfig />

<StaticBEConfigNote />

## BE 参数描述

### 日志

##### log_buffer_level

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：日志落盘的策略。默认值表示日志缓存在内存中。有效值为 `-1` 和 `0`。`-1` 表示日志不在内存中缓存。
- 引入版本：-

##### pprof_profile_dir

- 默认值: `${STARROCKS_HOME}/log`
- 类型: string
- 单位：-
- 是否动态：否
- 描述: StarRocks 写入 pprof 工件（jemalloc 堆快照和 gperftools CPU 配置文件）的目录路径。代码会在此目录下构造文件名（例如 `heap_profile.<pid>.<rand>` 和 `starrocks_profile.<pid>.<rand>`），并且 /pprof/ 下的 HTTP 处理器会提供这些配置文件。在启动时，如果 `pprof_profile_dir` 非空，StarRocks 会尝试创建该目录。请确保该路径存在或对 BE 进程可写，并且有足够的磁盘空间；性能分析可能产生较大的文件，并在运行时影响性能。
- 引入版本: v3.2.0

##### sys_log_dir

- 默认值：`${STARROCKS_HOME}/log`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：存放日志的地方，包括 INFO、WARNING、ERROR、FATAL 日志。
- 引入版本：-

##### sys_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否动态：是（自 v3.3.0、v3.2.7 及 v3.1.12 起）
- 描述：日志级别。有效值：INFO、WARNING、ERROR、FATAL。自 v3.3.0、v3.2.7 及 v3.1.12 起，该参数变为动态参数。
- 引入版本：-

##### sys_log_roll_mode

- 默认值：SIZE-MB-1024
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志分卷的模式。有效值包括 `TIME-DAY`、`TIME-HOUR` 和 `SIZE-MB-` 大小。默认值表示日志被分割成大小为 1GB 的日志卷。
- 引入版本：-

##### sys_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：日志卷保留的数目。
- 引入版本：-

##### sys_log_timezone

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在日志前缀中显示时区信息。`true` 表示显示时区信息，`false` 表示不显示。
- 引入版本：-

##### sys_log_verbose_level

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：日志显示的级别，用于控制代码中 VLOG 开头的日志输出。
- 引入版本：-

##### sys_log_verbose_modules

- 默认值：
- 类型：Strings
- 单位：-
- 是否动态：否
- 描述：日志打印的模块。有效值为 BE 的 namespace，包括 `starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream` 以及 `starrocks::workgroup`。
- 引入版本：-

### 服务器

##### arrow_flight_port

- 默认值: -1
- 类型: Int
- 单位: Port
- 是否动态：否
- 描述: TCP 端口，用于 BE 的 Arrow Flight SQL 服务器。将其设置为 `-1` 以禁用 Arrow Flight 服务。在非 macOS 构建中，BE 在启动期间会调用通过该端口启动 Arrow Flight SQL Server；如果端口不可用，服务器启动会失败并且 BE 进程退出。配置的端口会在心跳负载中上报给 FE。
- 引入版本: v3.4.0, v3.5.0

##### be_exit_after_disk_write_hang_second

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：磁盘挂起后触发 BE 进程退出的等待时间。
- 引入版本：-

##### be_http_num_workers

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HTTP Server 线程数。
- 引入版本：-

##### be_http_port

- 默认值：8040
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE HTTP Server 端口。
- 引入版本：-

##### be_port

- 默认值：9060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 上 Thrift Server 的端口，用于接收来自 FE 的请求。
- 引入版本：-

##### be_service_threads

- 默认值: 64
- 类型: Int
- 单位: Threads
- 是否动态：否
- 描述: BE Thrift 服务用于处理后端 RPC/执行请求的工作线程数。该值在创建 BackendService 时传递给 ThriftServer，用于控制可用的并发请求处理器数量；当所有工作线程都忙碌时，请求会排队。根据预期的并发 RPC 负载和可用的 CPU/内存调整：增大该值可以提高并发度，但也会增加每线程内存和上下文切换开销；减小则限制并行处理能力，可能增加请求延迟。
- 引入版本: v3.2.0

##### brpc_max_body_size

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bRPC 最大的包容量。
- 引入版本：-

##### brpc_max_connections_per_server

- 默认值: 1
- 类型: Int
- 单位: -
- 是否动态：否
- 描述: 客户端为每个远程服务器端点保持的最大持久 bRPC 连接数。对于每个端点，`BrpcStubCache` 会创建一个 `StubPool`，其 `_stubs` 向量会预留为此大小。首次访问时会创建新的 stub，直到达到限制。之后，现有的 stub 将以轮询方式返回。增加此值可以提高每个端点的并发度（降低单通道的争用），代价是更多的文件描述符、内存和通道数。
- 引入版本: v3.2.0

##### brpc_num_threads

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的 bthread 线程数量，`-1` 表示和 CPU 核数一样。
- 引入版本：-

##### brpc_port

- 默认值：8060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的端口，可以查看 bRPC 的一些网络统计信息。
- 引入版本：-

##### brpc_stub_expire_s

- 默认值: 3600
- 类型: Int
- 单位: Seconds
- 是否动态：是
- 描述: BRPC stub 缓存的过期时间，默认 60 minutes。
- 引入版本: -

##### compress_rowbatches

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。`true` 表示压缩，`false` 表示不压缩。
- 引入版本：-

##### delete_worker_count_normal_priority

- 默认值: 2
- 类型: Int
- 单位：Threads
- 是否动态：否
- 描述: 在 BE agent 上专门用于处理删除（REALTIME_PUSH with DELETE）任务的普通优先级工作线程数量。启动时，此值会与 delete_worker_count_high_priority 相加以确定 DeleteTaskWorkerPool 的大小（参见 agent_server.cpp）。线程池会将前 delete_worker_count_high_priority 个线程分配为 HIGH 优先级，其余为 NORMAL；普通优先级线程处理标准删除任务并有助于整体删除吞吐量。增加此值可提高并发删除能力（会增加 CPU/IO 使用）；减少则可降低资源争用。
- 引入版本: v3.2.0

##### enable_https

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 当此项设置为 `true` 时，BE 的 bRPC 服务配置为使用 TLS，并使用 `ssl_certificate_path` 和 `ssl_private_key_path` 指定的证书和私钥。这样会为传入的 bRPC 连接启用 HTTPS/TLS。客户端必须使用 TLS 连接。请确保证书和密钥文件存在、且 BE 进程可访问，并符合 bRPC/SSL 的要求。
- 引入版本: v4.0.0

##### enable_jemalloc_memory_tracker

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 该项设置为 `true` 时，BE 会启动一个后台线程（`jemalloc_tracker_daemon`），该线程以每秒一次的频率轮询 Jemalloc 统计信息，并将 Jemalloc 的 "stats.metadata" 值更新到 GlobalEnv 的 Jemalloc 元数据 MemTracker 中。这可确保 Jemalloc 元数据的消耗被计入 StarRocks 进程的内存统计，防止低报 Jemalloc 内部使用的内存。该 Tracker 仅在非 macOS 构建上编译/启动（#ifndef __APPLE__），并以名为 "jemalloc_tracker_daemon" 的守护线程运行。仅在未使用 Jemalloc 或者 Jemalloc 跟踪被有意以不同方式管理时才禁用，否则请保持启用以维护准确的内存计量和分配保护。
- 引入版本: v3.2.12

##### get_pindex_worker_count

- 默认值: 0
- 类型: Int
- 单位：-
- 是否动态：是
- 描述: 为 UpdateManager 中的 "get_pindex" 线程池设置工作线程数，该线程池用于加载/获取持久化索引数据（在为主键表应用 rowset 时使用）。如果设置为大于 0 则系统应用该值；如果设置为 0 则运行时回调使用 CPU 核心数。在初始化时，改线程池的最大线程数计算为 `max(get_pindex_worker_count, max_apply_thread_cnt * 2)`，其中 `max_apply_thread_cnt` 是 apply-thread 池的最大值。增大此值可提高 pindex 加载的并行度；降低则减少并发和内存/CPU 使用。
- 引入版本: v3.2.0

##### heartbeat_service_port

- 默认值：9050
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 心跳服务端口，用于接收来自 FE 的心跳。
- 引入版本：-

##### heartbeat_service_thread_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：心跳线程数。
- 引入版本：-

##### local_library_dir

- 默认值: `${UDF_RUNTIME_DIR}`
- 类型: string
- 单位：-
- 是否动态：否
- 描述: BE 上的本地目录，用于存放 UDF（用户自定义函数）库并作为 Python UDF worker 进程的工作目录。StarRocks 会将 UDF 库从 HDFS 复制到此路径，在 `<local_library_dir>/pyworker_<pid>` 创建每个 worker 的 Unix 域套接字，并在 exec 前将 Python worker 进程的工作目录切换到此目录。该目录必须存在、对 BE 进程可写，并且位于支持 Unix 域套接字的文件系统上（即本地文件系统）。由于该配置在运行时不可变，请在启动前设置并确保每个 BE 上具有足够的权限和磁盘空间。
- 引入版本: v3.2.0

##### mem_limit

- 默认值：90%
- 类型：String
- 单位：-
- 是否动态：否
- 描述：BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100G"）。默认的硬限制为服务器内存大小的 90%，软限制为 80%。如果您希望在同一台服务器上同时部署 StarRocks 和其他内存密集型服务，则需要配置此参数。
- 引入版本：-

##### net_use_ipv6_when_priority_networks_empty

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址的布尔值。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本：-

##### priority_networks

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：为有多个 IP 地址的服务器声明 IP 选择策略。请注意，最多应该有一个 IP 地址与此列表匹配。此参数的值是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。如果没有 IP 地址匹配此列表中的条目，系统将随机选择服务器的一个可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，系统将默认使用 IPv4 地址。您可以通过将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本：-

##### rpc_compress_ratio_threshold

- 默认值: 1.1
- 类型: Double
- 单位: -
- 是否动态：是
- 描述: 在决定是否以压缩形式通过网络发送序列化的 row-batches 时使用的阈值（uncompressed_size / compressed_size）。当尝试压缩时（例如在 DataStreamSender、exchange sink、tablet sink 的索引通道、dictionary cache writer 中），StarRocks 会计算 compress_ratio = uncompressed_size / compressed_size；仅当 compress_ratio `>` rpc_compress_ratio_threshold 时才使用压缩后的负载。默认值 1.1 意味着压缩数据必须至少比未压缩小约 9.1% 才会被使用。将该值调低以偏好压缩（以更多 CPU 换取更小的带宽）；将其调高以避免压缩开销，除非压缩能带来更大的尺寸缩减。注意：此项适用于 RPC/shuffle 序列化，仅在启用 row-batch 压缩（compress_rowbatches）时生效。
- 引入版本: v3.2.0

##### ssl_private_key_path

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否动态：否
- 描述: 文件系统中用于 BE 的 bRPC 服务作为默认证书私钥的 TLS/SSL 私钥（PEM）文件路径。当 `enable_https` 设置为 `true` 时生效。该文件必须对 BE 进程可访问，并且必须与由 `ssl_certificate_path` 提供的证书匹配。如果未设置此值或文件丢失或不可访问，则不会配置 HTTPS 并且 bRPC 服务器可能无法启动。请使用严格的文件系统权限（例如 600）保护此文件。
- 引入版本: v4.0.0

##### thrift_client_retry_interval_ms

- 默认值：100
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：Thrift Client 默认的重试时间间隔。
- 引入版本：-

##### thrift_port

- 默认值: 0
- 类型: Int
- 单位：-
- 是否动态：否
- 描述: 用于导出基于 Thrift 的内部 BackendService 的端口。当进程以 Compute Node 模式运行且该项设置为非零值时，它会覆盖 `be_port` 并使 Thrift 服务器绑定到此值；否则使用 `be_port`。此配置已弃用 — 将非零 `thrift_port` 设置会记录警告，建议改用 `be_port`。
- 引入版本: v3.2.0

##### thrift_rpc_connection_max_valid_time_ms

- 默认值：5000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：Thrift RPC 连接的最长有效时间。如果连接池中存在的时间超过此值，连接将被关闭。需要与 FE 配置项 `thrift_client_timeout_ms` 保持一致。
- 引入版本：-

##### thrift_rpc_max_body_size

- 默认值：0
- 类型：Int
- 单位：单位：Milliseconds
- 是否动态：否
- 描述：RPC 最大字符串体大小。`0` 表示无限制。
- 引入版本：-

##### thrift_rpc_strict_mode

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用了 Thrift 的严格执行模式。 Thrift 严格模式，参见 [Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md)。
- 引入版本：-

##### thrift_rpc_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：Thrift RPC 超时的时长。
- 引入版本：-

##### transaction_apply_thread_pool_num_min

- 默认值: 0
- 类型: Int
- 单位：Threads
- 是否动态：是
- 描述: 为 BE 的 UpdateManager 中的 "update_apply" 线程池设置最小线程数。该线程池用于为主键表应用 rowset。值为 0 表示禁用固定最小值（不强制下限）；当 `transaction_apply_worker_count` 也为 0 时，该线程池的最大线程数默认为 CPU 核心数，因此有效的工作线程容量等于 CPU 核心数。可以增大此值以保证应用事务的基线并发；设置过高可能增加 CPU 争用。
- 引入版本: v3.2.11

### 元数据与集群管理

##### cluster_id

- 默认值: -1
- 类型: Int
- 单位: -
- 是否动态：否
- 描述: BE 节点在集群的全局集群标识符。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。默认值 -1 表示在 Leader FE 首次启动时随机生成一个。
- 引入版本: v3.2.0

##### metadata_cache_memory_limit_percent

- 默认值: 30
- 类型: Int
- 单位：Percent
- 是否动态：是
- 描述: 将元数据 LRU 缓存大小设置为进程内存限制的百分比。启动时 StarRocks 将缓存字节数计算为 (`process_mem_limit * metadata_cache_memory_limit_percent / 100`)，并将其传递给元数据缓存分配器。该缓存仅用于非 PRIMARY_KEYS 行集（不支持 PK 表），并且仅在 `metadata_cache_memory_limit_percent` 大于 0 时启用；将其设置为小于等于 0 可禁用元数据缓存。增加此值会提高元数据缓存容量，但会减少分配给其他组件的内存；请根据工作负载和系统内存进行调优。在 BE_TEST 构建中不生效。
- 引入版本: v3.2.10

##### update_schema_worker_count

- 默认值: 3
- 类型: Int
- 单位：Threads
- 是否动态：否
- 描述: 配置后端用于处理 TTaskType::UPDATE_SCHEMA 任务的动态 ThreadPool（名称为 "update_schema"）中最大的工作线程数。该 ThreadPool 在 agent_server 启动时创建，最小线程数为 0（空闲时可缩减为零），最大线程数等于此设置；线程池使用默认的空闲超时并具有近乎无限的队列。增加此值可允许更多并发的 schema 更新任务（会增加 CPU 和内存使用），降低则可限制并行的 schema 操作。
- 引入版本: v3.2.3

### 用户，角色及权限

##### ssl_certificate_path

- 默认值: 
- 类型: String
- 单位：-
- 是否动态：否
- 描述: 在 enable_https 为 true 时，BE 的 brpc server 将使用的 TLS/SSL 证书文件（PEM 格式）的绝对路径。在 BE 启动时，此值会复制到 `brpc::ServerOptions::ssl_options().default_cert.certificate`；你还必须将 `ssl_private_key_path` 设置为对应的私钥。若 CA 要求，提供服务器证书及任何中间证书的 PEM 格式（证书链）。该文件必须对 StarRocks BE 进程可读，并且仅在启动时应用。如果在 enable_https 启用时该值未设置或无效，brpc 的 TLS 配置可能失败并导致服务器无法正确启动。
- 引入版本: v4.0.0

### 查询引擎

##### dictionary_speculate_min_chunk_size

- 默认值: 10000
- 类型: Int
- 单位：Rows
- 是否动态：否
- 描述: StringColumnWriter 和 DictColumnWriter 用于触发字典编码推测的最小行数（chunk 大小）。如果传入列（或累积缓冲区加上传入行）大小大于等于 `dictionary_speculate_min_chunk_size`，写入器将立即运行推测并设置一种编码（DICT、PLAIN 或 BIT_SHUFFLE），而不是继续缓冲更多行。对于字符串列，推测使用 `dictionary_encoding_ratio` 来决定字典编码是否有利；对于数值/非字符串列，使用 `dictionary_encoding_ratio_for_non_string_column`。此外，如果列的 byte_size 很大（大于等于 UINT32_MAX），会强制立即进行推测以避免 `BinaryColumn<uint32_t>` 溢出。
- 引入版本: v3.2.0

##### disable_storage_page_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 PageCache。
  - 开启 PageCache 后，StarRocks 会缓存最近扫描过的数据，
  - 对于查询重复性高的场景，会大幅提升查询效率。
  - `true` 表示不开启。
  - 自 2.4 版本起，该参数默认值由 `true` 变更为 `false`。自 3.1 版本起，该参数由静态变为动态。
- 引入版本：-

##### enable_bitmap_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Bitmap index 开启 Memory Cache。使用 Bitmap index 加速点查时，可以考虑开启。
- 引入版本：v3.1

##### enable_compaction_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：控制是否为 Flat Json 数据进行 Compaction。
- 引入版本：v3.3.3

##### enable_json_flat

- 默认值：false
- 类型：Boolean
- 单位：
- 是否动态：是
- 描述：是否开启 Flat JSON 特性。开启后新导入的 JSON 数据会自动打平，提升 JSON 数据查询性能。
- 引入版本：v3.3.0

##### enable_lazy_dynamic_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：当查询在读过程中未命中 Flat JSON Schema 时，是否启用 Lazy Dynamic Flat JSON。当此项设置为 `true` 时，StarRocks 将把 Flat JSON 操作推迟到计算流程，而不是读取流程。
- 引入版本：v3.3.3

##### enable_ordinal_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Ordinal index 开启 Memory Cache。Ordinal index 是行号到数据 page position 的映射，可以加速 Scan。
- 引入版本：-

##### enable_string_prefix_zonemap

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为字符串（CHAR/VARCHAR）列启用基于前缀的 Zonemap 索引。对于非键列，最小值/最大值会截断到由 `string_prefix_zonemap_prefix_len` 配置的前缀长度。
- 引入版本：-

##### enable_zonemap_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Zonemap index 开启 Memory Cache。使用 Zonemap index 加速 Scan 时，可以考虑开启。
- 引入版本：-

##### exchg_node_buffer_size_bytes

- 默认值：10485760
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Exchange 算子中，单个查询在接收端的 Buffer 容量。这是一个软限制，如果数据的发送速度过快，接收端会触发反压来限制发送速度。
- 引入版本：-

##### file_descriptor_cache_capacity

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：文件描述符缓存的容量。
- 引入版本：-

##### flamegraph_tool_dir

- 默认值：`${STARROCKS_HOME}/bin/flamegraph`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：火焰图工具的目录，该目录应包含 pprof、stackcollapse-go.pl 和 flamegraph.pl 脚本，用于从性能分析数据生成火焰图。
- 引入版本：-

##### fragment_pool_queue_size

- 默认值：2048
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单 BE 节点上能够处理的查询请求上限。
- 引入版本：-

##### fragment_pool_thread_num_max

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最大查询线程数。
- 引入版本：-

##### fragment_pool_thread_num_min

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最小查询线程数。
- 引入版本：-

##### hdfs_client_enable_hedged_read

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Hedged Read 功能。`true` 表示开启，`false` 表示不开启。
- 引入版本：v3.0

##### hdfs_client_hedged_read_threadpool_size

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HDFS 客户端侧 Hedged Read 线程池的大小，即 HDFS 客户端侧允许有多少个线程用于服务 Hedged Read。该参数对应 HDFS 集群配置文件 **hdfs-site.xml** 中的 `dfs.client.hedged.read.threadpool.size` 参数。
- 引入版本：v3.0

##### hdfs_client_hedged_read_threshold_millis

- 默认值：2500
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：发起 Hedged Read 请求前需要等待多少毫秒。例如，假设该参数设置为 `30`，那么如果一个 Read 任务未能在 30 毫秒内返回结果，则 HDFS 客户端会立即发起一个 Hedged Read，从目标数据块的副本上读取数据。该参数对应 HDFS 集群配置文件 **hdfs-site.xml** 中的 `dfs.client.hedged.read.threshold.millis` 参数。
- 引入版本：v3.0

##### io_coalesce_adaptive_lazy_active

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：根据谓词选择度，自适应决定是否将谓词列 IO 和非谓词列 IO 进行合并。
- 引入版本：v3.2

##### jit_lru_cache_size

- 默认值：0
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：JIT 编译的 LRU 缓存大小。如果设置为大于 0，则表示实际的缓存大小。如果设置为小于或等于 0，系统将自适应设置缓存大小，使用的公式为 `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` （节点的 `mem_limit` 必须大于或等于 16 GB）。
- 引入版本：-

##### json_flat_column_max

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，最多提取的子列数量。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

##### json_flat_create_zonemap

- 默认值：true
- 类型：Boolean
- 单位：
- 是否动态：是
- 描述：是否为打平后的 JSON 子列创建 Zonemap。仅当 `enable_json_flat` 为 `true` 时生效。
- 引入版本：-

##### json_flat_null_factor

- 默认值：0.3
- 类型：Double
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，提取列的 NULL 值占比阈值，高于该比例不对该列进行提取，默认为 0.3。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

##### json_flat_sparsity_factor

- 默认值：0.9
- 类型：Double
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，同名列的占比阈值，当同名列占比低于该值时不进行提取，默认为 0.9。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

##### lake_tablet_ignore_invalid_delete_predicate

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 控制是否忽略 tablet rowset 元数据中可能由逻辑删除在列名重命名后引入到重复键（duplicate key）表中的无效 delete predicates 的布尔值。
- 引入版本: v4.0

##### max_hdfs_file_handle

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多可以打开的 HDFS 文件描述符数量。
- 引入版本：-

##### max_memory_sink_batch_count

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Scan Cache 的最大缓存批次数量。
- 引入版本：-

##### max_pushdown_conditions_per_column

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单列上允许下推的最大谓词数量，如果超出数量限制，谓词不会下推到存储层。
- 引入版本：-

##### max_scan_key_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询最多拆分的 Scan Key 数目。
- 引入版本：-

##### min_file_descriptor_number

- 默认值：60000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 进程中文件描述符的最小数量。
- 引入版本：-

##### object_storage_connect_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：对象存储 Socket 连接的超时时间。`-1` 表示使用 SDK 中的默认时间。
- 引入版本：v3.0.9

##### object_storage_request_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：对象存储 HTTP 连接的超时时间。`-1` 表示使用 SDK 中的默认时间。
- 引入版本：v3.0.9

##### parquet_late_materialization_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用延迟物化优化 Parquet 读性能。
- 引入版本：-

##### parquet_page_index_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Parquet 文件的 Bloom Filter 以提高性能。`true` 表示启用 Bloom Filter，`false` 表示禁用。还可以使用系统变量 `enable_parquet_reader_bloom_filter` 在 Session 级别上控制这一行为。Parquet 中的 Bloom Filter 是在**每个行组的列级维护的**。如果 Parquet 文件包含某些列的 Bloom Filter，查询就可以使用这些列上的谓词来有效地跳过行组。
- 引入版本：v3.5

##### parquet_reader_bloom_filter_enable

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 控制是否启用 Parquet 文件的布隆过滤器以提升性能的布尔值。`true` 表示启用布隆过滤器，`false` 表示禁用。也可以通过会话级别的系统变量 `enable_parquet_reader_bloom_filter` 控制此行为。Parquet 中的布隆过滤器是按“每个 row group 的列级别”维护的。如果 Parquet 文件为某些列维护了布隆过滤器，则对这些列的谓词可以高效地跳过不相关的 row group。
- 引入版本: v3.5

##### path_gc_check_step

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：：单次连续 Scan 最大的文件数量。
- 引入版本：-

##### path_gc_check_step_interval_ms

- 默认值：10
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：多次连续 Scan 文件间隔时间。
- 引入版本：-

##### path_scan_interval_second

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：GC 线程清理过期数据的间隔时间。
- 引入版本：-

##### pipeline_connector_scan_thread_num_per_cpu

- 默认值：8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：BE 节点中每个 CPU 核心分配给 Pipeline Connector 的扫描线程数量。自 v3.1.7 起变为动态参数。
- 引入版本：-

##### pipeline_poller_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：当该值大于 `0` 时，则在轮询器中，如果某个 Driver 的单次调度时间超过了 `pipeline_poller_timeout_guard_ms` 的时间，则会打印该 Driver 以及 Operator 信息。
- 引入版本：-

##### pipeline_prepare_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎在线程池中执行 PREPARE Fragment 的队列长度。
- 引入版本：-

##### pipeline_prepare_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎准备片段线程池中的线程数。`0` 表示等于系统 VCPU 数量。
- 引入版本：-

##### pipeline_prepare_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：当该值大于 `0` 时，如果 PREPARE 过程中 Plan Fragment 超过 `pipeline_prepare_timeout_guard_ms` 的时间，则会打印 Plan Fragment 的堆栈跟踪。
- 引入版本：-

##### pipeline_scan_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎扫描线程池任务队列的最大队列长度。
- 引入版本：-

##### query_cache_capacity

- 默认值：536870912
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：指定 Query Cache 的大小。默认为 512 MB。最小不低于 4 MB。如果当前的 BE 内存容量无法满足您期望的 Query Cache 大小，可以增加 BE 的内存容量，然后再设置合理的 Query Cache 大小。每个 BE 都有自己私有的 Query Cache 存储空间，BE 只 Populate 或 Probe 自己本地的 Query Cache 存储空间。
- 引入版本：-

##### query_pool_spill_mem_limit_threshold

- 默认值：1.0
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：如果开启自动落盘功能, 当所有查询使用的内存超过 `query_pool memory limit * query_pool_spill_mem_limit_threshold` 时，系统触发中间结果落盘。
- 引入版本：3.2.7

##### result_buffer_cancelled_interval_time

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：BufferControlBlock 释放数据的等待时间。
- 引入版本：-

##### scan_context_gc_interval_min

- 默认值：5
- 类型：Int
- 单位：Minutes
- 是否动态：是
- 描述：Scan Context 的清理间隔。
- 引入版本：-

##### scanner_row_num

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个扫描线程单次执行最多返回的数据行数。
- 引入版本：-

##### scanner_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存储引擎支持的扫描任务数。
- 引入版本：-

##### scanner_thread_pool_thread_num

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存储引擎并发扫描磁盘的线程数，统一管理在线程池中。
- 引入版本：-

##### string_prefix_zonemap_prefix_len

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：启用 `enable_string_prefix_zonemap` 时用于字符串 Zonemap 最小值/最大值的前缀长度。
- 引入版本：-

##### udf_thread_pool_size

- 默认值: 1
- 类型: Int
- 单位：Threads
- 是否动态：否
- 描述: 设置在 ExecEnv 中创建的 UDF 调用 PriorityThreadPool 的大小（用于执行用户自定义函数/UDF 相关任务）。该值既作为线程池的线程数，也在构造线程池时作为队列容量（PriorityThreadPool("udf", thread_num, queue_size)）。增大该值可以允许更多并发的 UDF 执行；保持较小可避免过度的 CPU 和内存争用。
- 引入版本: v3.2.0

##### update_memory_limit_percent

- 默认值: 60
- 类型: Int
- 单位：Percent
- 是否动态：否
- 描述: BE 进程内存中为更新相关内存和缓存保留的比例。在启动期间，`GlobalEnv` 将更新的 `MemTracker` 计算为 process_mem_limit * clamp(update_memory_limit_percent, 0, 100) / 100。`UpdateManager` 也使用该百分比来确定其 primary-index/index-cache 的容量（index cache capacity = GlobalEnv::process_mem_limit * update_memory_limit_percent / 100）。HTTP 配置更新逻辑会注册一个回调，在配置更改时调用 update managers 的 `update_primary_index_memory_limit`，因此配置更改会应用到更新子系统。增加此值会为更新/primary-index 路径分配更多内存（减少其他内存池可用内存）；减少它会降低更新内存和缓存容量。值会被限定在 0–100 范围内。
- 引入版本: v3.2.0

### 导入

##### clear_transaction_task_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：清理事务的线程数。
- 引入版本：-

##### column_mode_partial_update_insert_batch_size

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：列模式部分更新中处理插入行时的批次大小。如果设置为 `0` 或负数，将会被限制为 `1` 以避免无限循环。该参数控制每次批量处理新插入行的数量，较大的值可以提高写入性能但会占用更多内存。
- 引入版本：v3.5.10, v4.0.2

##### enable_stream_load_verbose_log

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在日志中记录 Stream Load 的 HTTP 请求和响应信息。`true` 表示启用，`false` 表示不启用。
- 引入版本：v2.5.17, v3.0.9, v3.1.6, v3.2.1

##### flush_thread_num_per_store

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Store 用以 Flush MemTable 的线程数。
- 引入版本：-

##### lake_flush_thread_num_per_store

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：在存算分离集群中，每个 Store 用以 Flush MemTable 的线程数。当该参数被设置为 `0` 时，系统使用 CPU 核数的两倍。
当该参数被设置为小于 `0` 时，系统使用该参数的绝对值与 CPU 核数的乘积。
- 引入版本：3.1.12, 3.2.7

##### load_data_reserve_hours

- 默认值：4
- 类型：Int
- 单位：Hours
- 是否动态：否
- 描述：小批量导入生成的文件保留的时长。
- 引入版本：-

##### load_error_log_reserve_hours

- 默认值：48
- 类型：Int
- 单位：Hours
- 是否动态：是
- 描述：导入数据信息保留的时长。
- 引入版本：-

##### load_process_max_memory_limit_bytes

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：单节点上所有的导入线程占据的内存上限。
- 引入版本：-

##### max_consumer_num_per_group

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Routine load 中，每个 Consumer Group 内最大的 Consumer 数量。
- 引入版本：-

##### max_runnings_transactions_per_txn_map

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个分区内部同时运行的最大事务数量。
- 引入版本：-

##### number_tablet_writer_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：导入用的 tablet writer 线程数, 用于 Stream Load、Broker Load、Insert 等。当参数设置为小于等于 0 时，系统使用 CPU 核数的二分之一，最小为 16。当参数设置为大于 0 时，系统使用该值。自 v3.1.7 起变为动态参数。
- 引入版本：-

##### push_worker_count_high_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 HIGH 优先级任务。
- 引入版本：-

##### push_worker_count_normal_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 NORMAL 优先级任务。
- 引入版本：-

##### streaming_load_max_batch_size_mb

- 默认值：100
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个 JSON 文件大小的上限。
- 引入版本：-

##### streaming_load_max_mb

- 默认值：102400
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个文件大小的上限。自 3.0 版本起，默认值由 10240 变为 102400。
- 引入版本：-

##### streaming_load_rpc_max_alive_time_sec

- 默认值：1200
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：Stream Load 的 RPC 超时时长。
- 引入版本：-

##### transaction_publish_version_thread_pool_idle_time_ms

- 默认值：60000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：线程被 Publish Version 线程池回收前的 Idle 时间。
- 引入版本：-

##### transaction_publish_version_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：生效版本的最大线程数。当该参数被设置为小于或等于 `0` 时，系统默认使用当前节点的 CPU 核数，以避免因使用固定值而导致在导入并行较高时线程资源不足。自 2.5 版本起，默认值由 `8` 变更为 `0`。
- 引入版本：-

##### write_buffer_size

- 默认值：104857600
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：MemTable 在内存中的 Buffer 大小，超过这个限制会触发 Flush。
- 引入版本：-

### 导入导出

##### enable_load_channel_rpc_async

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 启用后，load-channel Open 类型的 RPC（例如 PTabletWriterOpen）的处理会从 BRPC worker 转移到一个专用的线程池：请求处理器会创建一个 ChannelOpenTask 并将其提交到内部 `_async_rpc_pool`，而不是内联执行 `LoadChannelMgr::_open`。这样可以减少 BRPC 线程内的工作量和阻塞，并允许通过 `load_channel_rpc_thread_pool_num` 和 `load_channel_rpc_thread_pool_queue_size` 调整并发。如果线程池提交失败（池已满或已关闭），该请求会被取消并返回错误状态。该线程池会在 `LoadChannelMgr::close()` 时关闭，因此在启用该功能时需要考虑容量和生命周期，以避免请求被拒绝或处理延迟。
- 引入版本: v3.5.0

##### enable_streaming_load_thread_pool

- 默认值: true
- 类型: Boolean
- 单位：-
- 是否动态：是
- 描述: 控制是否将 streaming load 的 scanner 提交到专用的 streaming load 线程池。当启用且查询为带有 `TLoadJobType::STREAM_LOAD` 的 LOAD 时，ConnectorScanNode 会将 scanner 任务提交到 `streaming_load_thread_pool`（该池配置为 INT32_MAX 的线程数和队列大小，即实际上是无界的）。当禁用时，scanner 使用通用的 `thread_pool` 及其 `PriorityThreadPool` 提交逻辑（优先级计算、try_offer/offer 行为）。启用可以将 streaming-load 的工作与常规查询执行隔离以减少干扰；但由于专用池实际上是无界的，在重度 streaming-load 流量下启用可能会增加并发线程数和资源使用。此选项默认开启，通常无需修改。
- 引入版本: v3.2.0

##### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- 默认值: 600000
- 类型: Int
- 单位：毫秒
- 是否动态：是
- 描述: 用于决定何时为长时间运行的 load RPC 请求远程堆栈跟踪的阈值（毫秒）。当 load RPC 超时并返回超时错误且实际的 RPC 超时时间（_rpc_timeout_ms）超过此值时，`OlapTableSink`/`NodeChannel` 将在发往目标 BE 的 `load_diagnose` RPC 中包含 `stack_trace=true`，以便 BE 返回用于调试的堆栈跟踪。`LocalTabletsChannel::SecondaryReplicasWaiter` 也会在等待 secondary replicas 超过该间隔时触发从 primary 的尽力堆栈跟踪诊断。此行为依赖于 `enable_load_diagnose` 并使用 `load_diagnose_send_rpc_timeout_ms` 作为诊断 RPC 的超时；性能分析由 `load_diagnose_rpc_timeout_profile_threshold_ms` 单独控制。降低此值会更积极地请求堆栈跟踪。
- 引入版本: v3.5.0

##### load_fp_brpc_timeout_ms

- 默认值: -1
- 类型: Int
- 单位：毫秒
- 是否动态：是
- 描述: 在触发 `node_channel_set_brpc_timeout` fail point 时，覆盖 OlapTableSink 所使用的每通道 brpc RPC 超时。如果设置为正值，NodeChannel 会将其内部 `_rpc_timeout_ms` 设置为该值（毫秒），使 open/add-chunk/cancel RPC 使用更短的超时，从而模拟产生 “[E1008]Reached timeout” 错误的 brpc 超时。默认值（`-1`）禁用覆盖。更改此值用于测试和故障注入；较小的值可能导致虚假超时并触发 load 诊断（参见 `enable_load_diagnose`、`load_diagnose_rpc_timeout_profile_threshold_ms`、`load_diagnose_rpc_timeout_stack_trace_threshold_ms` 和 `load_diagnose_send_rpc_timeout_ms`）。
- 引入版本: v3.5.0

##### pull_load_task_dir

- 默认值: `${STARROCKS_HOME}/var/pull_load`
- 类型: string
- 单位：-
- 是否动态：否
- 描述: BE 存储 “pull load” 任务的数据和工作文件（下载的源文件、任务状态、临时输出等）的文件系统路径。该目录必须对 BE 进程可写，并且具有足够的磁盘空间以容纳下载的内容。
- 引入版本: v3.2.0

##### routine_load_pulsar_timeout_second

- 默认值: 10
- 类型: Int
- 单位：秒
- 是否动态：否
- 描述: BE 在请求未提供显式超时时使用的 Pulsar 相关 routine load 操作的默认超时（秒）。具体地，`PInternalServiceImplBase::get_pulsar_info` 将该值乘以 1000，形成以毫秒为单位的超时值，传递给用于获取 Pulsar 分区元数据和 backlog 的 routine load 任务执行器方法。增大该值可在 Pulsar 响应较慢时减少超时失败，但会延长故障检测时间；减小该值可在 broker 响应慢时更快失败。与用于 Kafka 的 `routine_load_kafka_timeout_second` 类似。
- 引入版本: v3.2.0

### 统计信息

##### enable_system_metrics

- 默认值: true
- 类型: Boolean
- 单位：-
- 是否动态：否
- 描述: 为 true 时，StarRocks 在启动期间初始化系统级监控：它会根据配置的存储路径发现磁盘设备并枚举网络接口，然后将这些信息传入 metrics 子系统以启用磁盘 I/O、网络流量和内存相关的系统指标采集。如果设备或接口发现失败，初始化会记录警告并中止系统指标的设置。该标志仅控制是否初始化系统指标；周期性指标聚合线程由 `enable_metric_calculator` 单独控制，JVM 指标初始化由 `enable_jvm_metrics` 控制。更改此值需要重启。
- 引入版本: v3.2.0

##### profile_report_interval

- 默认值: 30
- 类型: Int
- 单位: Seconds
- 是否动态：是
- 描述: ProfileReportWorker 用于（1）决定何时上报 LOAD 查询的每个 fragment 的 profile 信息以及（2）在上报周期之间休眠的间隔（秒）。该 worker 使用 (profile_report_interval * 1000) ms 将当前时间与每个任务的 last_report_time 进行比较，以确定是否需要对非 pipeline 和 pipeline 的 load 任务重新上报 profile。在每次循环中，worker 会读取当前值（运行时可变）；如果配置值小于等于 0，worker 会强制将其设为 1 并发出警告。修改此值会影响下一次的上报判断和休眠时长。
- 引入版本: v3.2.0

##### report_disk_state_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报磁盘状态的间隔。汇报各个磁盘的状态，以及其中数据量等。
- 引入版本：-

##### report_resource_usage_interval_ms

- 默认值: 1000
- 类型: Int
- 单位：毫秒
- 是否动态：是
- 描述: 由 BE Agent 定期向 FE 发送资源使用报告的间隔（毫秒）。较低的值能提高报告的及时性，但会增加 CPU、网络和 FE 的负载；较高的值可降低开销但会使资源信息不够实时。上报会更新相关指标（`report_resource_usage_requests_total`、`report_resource_usage_requests_failed`）。请根据集群规模和 FE 负载调整。
- 引入版本: v3.2.0

##### report_tablet_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报 Tablet 的间隔。汇报所有的 Tablet 的最新版本。
- 引入版本：-

##### report_task_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报单个任务的间隔。建表，删除表，导入，Schema Change 都可以被认定是任务。
- 引入版本：-

##### report_workgroup_interval_seconds

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报 Workgroup 的间隔。汇报所有 Workgroup 的最新版本。
- 引入版本：-

### 存储

##### alter_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：进行 Schema Change 的线程数。自 2.5 版本起，该参数由静态变为动态。
- 引入版本：-

##### avro_ignore_union_type_tag

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在 Avro Union 数据结构序列化成 JSON 格式时，去除 Union 结构的类型标签信息。
- 引入版本：v3.3.7, v3.4

##### base_compaction_check_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Base Compaction 线程轮询的间隔。
- 引入版本：-

##### base_compaction_interval_seconds_since_last_operation

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：上一轮 Base Compaction 距今的间隔。此项为 Base Compaction 触发条件之一。
- 引入版本：-

##### base_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个磁盘 Base Compaction 线程的数目。
- 引入版本：-

##### base_cumulative_delta_ratio

- 默认值：0.3
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：Cumulative 文件大小达到 Base 文件的比例。此项为 Base Compaction 触发条件之一。
- 引入版本：-

##### chaos_test_enable_random_compaction_strategy

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 当此项设置为 `true` 时，TabletUpdates::compaction() 将使用为混沌测试准备的随机压缩策略（compaction_random）。此标志强制在平板的压缩选择中采用非确定性/随机策略，而不是正常策略（例如 size-tiered compaction），并在压缩选择时具有优先权。仅用于可控的测试场景：启用后可能导致不可预测的压缩顺序、增加的 I/O/CPU 和测试不稳定性。请勿在生产环境中启用；仅用于故障注入或混沌测试场景。
- 引入版本: v3.3.12, 3.4.2, 3.5.0, 4.0.0

##### check_consistency_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：计算 Tablet 的校验和（checksum）的线程数。
- 引入版本：-

##### clear_expired_replication_snapshots_interval_seconds

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：系统清除异常同步遗留的过期快照的时间间隔。
- 引入版本：v3.3.5

##### compact_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 引入版本：v3.0.0

##### compaction_memory_limit_per_worker

- 默认值: 2147483648
- 类型: Int
- 单位: Bytes
- 是否动态：否
- 描述: 每个 Compaction 线程允许的最大内存大小。
- 引入版本: -

##### compaction_trace_threshold

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：单次 Compaction 打印 Trace 的时间阈值，如果单次 Compaction 时间超过该阈值就打印 Trace。
- 引入版本：-

##### cumulative_compaction_check_interval_seconds

- 默认值：1
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Cumulative Compaction 线程轮询的间隔。
- 引入版本：-

##### cumulative_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个磁盘 Cumulative Compaction 线程的数目。
- 引入版本：-

##### data_page_size

- 默认值: 65536
- 类型: Int
- 单位: Bytes
- 是否动态：否
- 描述: 构建列数据与索引页时使用的目标未压缩 Page 大小（以字节为单位）。该值会被 Page Builder 用来决定何时完成一个 Page 以及预留多少内存。值为 0 会在构建器中禁用 Page 大小限制。更改此值会影响 Page 数量、元数据开销、内存预留以及 I/O/压缩的权衡（Page 越小 → Page 数和元数据越多；Page 越大 → Page 更少，压缩比更大，但内存峰值可能更大）。
- 引入版本: v3.2.4

##### default_num_rows_per_column_file_block

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Row Block 最多存放的行数。
- 引入版本：-

##### delete_worker_count_high_priority

- 默认值: 1
- 类型: Int
- 单位: Threads
- 是否动态：否
- 描述: 在 DeleteTaskWorkerPool 中被分配为高优先级删除线程的工作线程数。启动时 AgentServer 使用 total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority 创建删除线程池；前 delete_worker_count_high_priority 个线程被标记为专门尝试弹出 TPriority::HIGH 任务（它们轮询高优先级删除任务，若无可用任务则睡眠/循环）。增加此值可以提高高优先级删除请求的并发性；减少它会降低专用容量并可能增加高优先级删除的延迟。更改需要重启进程才能生效。
- 引入版本: v3.2.0

##### disk_stat_monitor_interval

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘健康状态检测的间隔。
- 引入版本：-

##### download_low_speed_limit_kbps

- 默认值：50
- 类型：Int
- 单位：KB/Second
- 是否动态：是
- 描述：单个 HTTP 请求的下载速率下限。如果在 `download_low_speed_time` 秒内下载速度一直低于 `download_low_speed_limit_kbps`，那么请求会被终止。
- 引入版本：-

##### download_low_speed_time

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：单个 HTTP 请求持续以低于 `download_low_speed_limit_kbps` 值的速度运行时，允许运行的最长时间。在配置项中指定的时间跨度内，当一个 HTTP 请求持续以低于该值的速度运行时，该请求将被中止。
- 引入版本：-

##### download_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点下载任务的最大线程数，用于恢复作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

##### drop_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：删除 Tablet 的线程数。
- 引入版本：-

##### enable_check_string_lengths

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在导入时进行数据长度检查，以解决 VARCHAR 类型数据越界导致的 Compaction 失败问题。
- 引入版本：-

##### enable_event_based_compaction_framework

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Event-based Compaction Framework。`true` 代表开启。`false` 代表关闭。开启则能够在 Tablet 数比较多或者单个 Tablet 数据量比较大的场景下大幅降低 Compaction 的开销。
- 引入版本：-

##### enable_lazy_delta_column_compaction

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 启用后，Compaction 将对由部分列更新产生的 Delta 列采用“懒惰”策略：StarRocks 会避免将 Delta-column 文件立即合并回其主段文件以节省 Compaction 的 I/O。实际上，Compaction 选择代码会检查是否存在部分列更新的 Rowset 和多个候选项；如果发现以上情况且此配置项为 `true`，引擎要么停止向 Compaction 添加更多输入，要么仅合并空的 Rowset（level -1），将 Delta 列保持分离。这会减少 Compaction 期间的即时 I/O 和 CPU 开销，但以延迟合并为代价（可能产生更多段和临时存储开销）。正确性和查询语义不受影响。
- 引入版本: v3.2.3

##### enable_new_load_on_memory_limit_exceeded

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在导入线程内存占用达到硬上限后，是否允许新的导入线程。`true` 表示允许新导入线程，`false` 表示拒绝新导入线程。
- 引入版本：v3.3.2

##### enable_pk_index_parallel_compaction

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用存算分离集群中主键索引的并行 Compaction。
- 引入版本：-

##### enable_pk_index_parallel_get

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用存算分离集群中主键索引的并行读取。
- 引入版本：-

##### enable_pk_parallel_execution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Primary Key 表并行执行策略。当并行执行策略开启时，Primary Key 索引文件会在导入和compaction阶段生成。
- 引入版本：-

##### enable_pk_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否为 Primary Key 表开启 Size-tiered Compaction 策略。`true` 代表开启。`false` 代表关闭。
- 引入版本：
  - 存算分离集群自 v3.2.4, v3.1.10 起生效
  - 存算一体集群自 v3.2.5, v3.1.10 起生效

##### enable_rowset_verify

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否检查 Rowset 的正确性。开启后，会在 Compaction、Schema Change 后检查生成的 Rowset 的正确性。
- 引入版本：-

##### enable_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Size-tiered Compaction 策略 (Primary Key 表除外)。`true` 代表开启。`false` 代表关闭。
- 引入版本：-

##### enable_strict_delvec_crc_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当此项设置为 `true` 后，系统会对 Delete Vector 的 crc32 进行严格检查，如果不一致，将返回失败。
- 引入版本：-

##### enable_transparent_data_encryption

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 启用后，StarRocks 将为新写入的存储对象（segment 文件、delete/update 文件、rowset segments、lake SSTs、persistent index 文件等）进行磁盘加密。写入路径（RowsetWriter/SegmentWriter、lake UpdateManager/LakePersistentIndex 及相关代码路径）会从 KeyCache 请求加密信息，将 encryption_info 附加到可写文件，并将 encryption_meta 持久化到 rowset / segment / sstable 元数据中（如 segment_encryption_metas、delete/update encryption metadata）。FE 与 FE/CN 的加密标志必须匹配。如果不匹配会导致 BE 在心跳时中止（LOG(FATAL)）。此参数不可在运行时修改，必须在第一次部署集群前启用，并确保密钥管理（KEK）与 KeyCache 已在集群中正确配置并同步。
- 引入版本: v3.3.1

##### file_descriptor_cache_clean_interval

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：文件描述符缓存清理的间隔，用于清理长期不用的文件描述符。
- 引入版本：-

##### inc_rowset_expired_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：导入生效的数据，存储引擎保留的时间，用于增量克隆。
- 引入版本：-

##### load_process_max_memory_hard_limit_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单节点上所有的导入线程占据内存的硬上限（比例）。当 `enable_new_load_on_memory_limit_exceeded` 设置为 `false`，并且所有导入线程的内存占用超过 `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio` 时，系统将会拒绝新的导入线程。
- 引入版本：v3.3.2

##### load_process_max_memory_limit_percent

- 默认值：30
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单节点上所有的导入线程占据内存的软上限（百分比）。
- 引入版本：-

##### make_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点快照任务的最大线程数。
- 引入版本：-

##### manual_compaction_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Number of threads for Manual Compaction.
- 引入版本：-

##### max_base_compaction_num_singleton_deltas

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Base Compaction 合并的最大 Segment 数。
- 引入版本：-

##### max_compaction_candidate_num

- 默认值：40960
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 候选 Tablet 的最大数量。太大会导致内存占用和 CPU 负载高。
- 引入版本：-

##### max_compaction_concurrency

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 `-1` 代表没有限制。`0` 表示禁用 Compaction。开启 Event-based Compaction Framework 时，该参数才支持动态设置。
- 引入版本：-

##### max_cumulative_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Cumulative Compaction 能合并的最大 Segment 数。如果 Compaction 时出现内存不足的情况，可以调小该值。
- 引入版本：-

##### max_download_speed_kbps

- 默认值：50000
- 类型：Int
- 单位：KB/Second
- 是否动态：是
- 描述：单个 HTTP 请求的最大下载速率。这个值会影响 BE 之间同步数据副本的速度。
- 引入版本：-

##### max_garbage_sweep_interval

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘进行垃圾清理的最大间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

##### max_percentage_of_error_disk

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：错误磁盘达到该比例上限，BE 退出。
- 引入版本：-

##### max_row_source_mask_memory_bytes

- 默认值：209715200
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：Row source mask buffer 的最大内存占用大小。当 buffer 大于该值时将会持久化到磁盘临时文件中。该值应该小于 `compaction_memory_limit_per_worker` 参数的值。
- 引入版本：-

##### max_update_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大 Rowset 数。
- 引入版本：-

##### memory_limitation_per_thread_for_schema_change

- 默认值：2
- 类型：Int
- 单位：GB
- 是否动态：是
- 描述：单个 Schema Change 任务允许占用的最大内存。
- 引入版本：-

##### memory_ratio_for_sorting_schema_change

- 默认值: 0.8
- 类型: Double
- 单位: - (unitless ratio)
- 是否动态：是
- 描述: 在排序型 schema-change 操作期间，用作 memtable 最大缓冲区大小的每线程 schema-change 内存限制的比例。该比例会与 memory_limitation_per_thread_for_schema_change（以 GB 配置并转换为字节）相乘以计算 max_buffer_size，且结果上限为 4GB。SchemaChangeWithSorting 和 SortedSchemaChange 在创建 MemTable/DeltaWriter 时使用此值。增大该比例允许更大的内存缓冲区（减少 flush/merge 次数），但会增加内存压力风险；减小该比例会导致更频繁的 flush，从而增加 I/O/merge 开销。
- 引入版本: v3.2.0

##### min_base_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Base Compaction 的最小 Segment 数。
- 引入版本：-

##### min_compaction_failure_interval_sec

- 默认值：120
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Tablet Compaction 失败之后，再次被调度的间隔。
- 引入版本：-

##### min_cumulative_compaction_failure_interval_sec

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Cumulative Compaction 失败后的最小重试间隔。
- 引入版本：-

##### min_cumulative_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Cumulative Compaction 的最小 Segment 数。
- 引入版本：-

##### min_garbage_sweep_interval

- 默认值：180
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘进行垃圾清理的最小间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

##### parallel_clone_task_per_path

- 默认值: 8
- 类型: Int
- 单位: Threads
- 是否动态：是
- 描述: 在 BE 的每个存储路径上分配的并行 clone 工作线程数。BE 启动时，clone 线程池的最大线程数计算为 max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL)。例如，若有 4 个存储路径且默认=8，则 clone 池最大 = 32。此设置直接控制 BE 处理的 CLONE 任务（tablet 副本拷贝）的并发度：增加它会提高并行 clone 吞吐量，但也会增加 CPU、磁盘和网络争用；减少它会限制同时进行的 clone 任务并可能限制 FE 调度的 clone 操作。该值应用于动态 clone 线程池，可通过 update-config 路径在运行时更改（会导致 agent_server 更新 clone 池的最大线程数）。
- 引入版本: v3.2.0

##### pending_data_expire_time_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：存储引擎保留的未生效数据的最大时长。
- 引入版本：-

##### pindex_major_compaction_limit_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题。
- 引入版本：v3.0.9

##### pk_index_compaction_score_ratio

- 默认值：1.5
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引的 Compaction 分数比率。例如，如果有 N 个文件集，Compaction 分数将为 `N * pk_index_compaction_score_ratio`。
- 引入版本：-

##### pk_index_ingest_sst_compaction_threshold

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引 Ingest SST Compaction 的阈值。
- 引入版本：-

##### pk_index_memtable_flush_threadpool_max_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引 MemTable 刷盘的线程池最大线程数。
- 引入版本：-

##### pk_index_memtable_max_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引的最大 MemTable 数量。
- 引入版本：-

##### pk_index_parallel_compaction_task_split_threshold_bytes

- 默认值：104857600
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Compaction 任务拆分的阈值。当任务涉及的文件总大小小于此阈值时，任务将不会被拆分。默认为 100MB。
- 引入版本：-

##### pk_index_parallel_compaction_threadpool_max_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引并行 Compaction 的线程池最大线程数。
- 引入版本：-

##### pk_index_parallel_get_min_rows

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，启用主键索引并行读取的最小行数阈值。
- 引入版本：-

##### pk_index_parallel_get_threadpool_max_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引并行读取的线程池最大线程数。0 表示自动配置。
- 引入版本：-

##### pk_index_size_tiered_level_multiplier

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的层级倍数参数。
- 引入版本：-

##### pk_index_size_tiered_max_level

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的层级数量参数。
- 引入版本：-

##### pk_index_size_tiered_min_level_size

- 默认值：131072
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的最小层级大小参数。
- 引入版本：-

##### pk_index_target_file_size

- 默认值：67108864
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群中，主键索引的目标文件大小。
- 引入版本：-

##### pk_parallel_execution_threshold_bytes

- 默认值：104857600
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：当 `enable_pk_parallel_execution` 设置为 `true` 后，导入或者 Compaction 生成的数据大于该阈值时，主键表并行执行策略将被启用。
- 引入版本：-

##### primary_key_limit_size

- 默认值：128
- 类型：Int
- 单位：Byte
- 是否动态：是
- 描述：主键表中单条主键值最大长度。
- 引入版本：v2.5

##### release_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点释放快照任务的最大线程数。
- 引入版本：-

##### repair_compaction_interval_seconds

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Repair Compaction 线程轮询的间隔。
- 引入版本：-

##### replication_max_speed_limit_kbps

- 默认值：50000
- 类型：Int
- 单位：KB/s
- 是否动态：是
- 描述：每个同步线程的最大速度。
- 引入版本：v3.3.5

##### replication_min_speed_limit_kbps

- 默认值：50
- 类型：Int
- 单位：KB/s
- 是否动态：是
- 描述：每个同步线程的最小速度。
- 引入版本：v3.3.5

##### replication_min_speed_time_seconds

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：同步线程低于最低速度所允许的持续时间。如果实际速度低于 `replication_min_speed_limit_kbps` 的时间超过此值，同步将失败。
- 引入版本：v3.3.5

##### replication_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于同步的最大线程数。0 表示将线程数设置为 BE CPU 内核数的四倍。
- 引入版本：v3.3.5

##### size_tiered_level_multiple

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，相邻两个 Level 之间相差的数据量的倍数。
- 引入版本：-

##### size_tiered_level_multiple_dupkey

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，Duplicate Key 表相邻两个 Level 之间相差的数据量的倍数。
- 引入版本：-

##### size_tiered_level_num

- 默认值：7
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略的 Level 数量。每个 Level 最多保留一个 Rowset，因此稳定状态下最多会有和 Level 数相同的 Rowset。
- 引入版本：-

##### size_tiered_min_level_size

- 默认值：131072
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，最小 Level 的大小，小于此数值的 Rowset 会直接触发 Compaction。
- 引入版本：-

##### snapshot_expire_time_sec

- 默认值：172800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：快照文件清理的间隔。
- 引入版本：-

##### storage_flood_stage_left_capacity_bytes

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：BE 存储目录整体磁盘剩余空间的硬限制。如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，StarRocks 会拒绝 Load 和 Restore 作业，默认 100GB。需要同步修改 FE 配置 `storage_usage_hard_limit_reserve_bytes` 以使其生效。
- 引入版本：-

##### storage_flood_stage_usage_percent

- 默认值：95
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 存储目录整体磁盘空间使用率的硬上限。如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，StarRocks 会拒绝 Load 和 Restore 作业。需要同步修改 FE 配置 `storage_usage_hard_limit_percent` 以使其生效。
- 引入版本：-

##### storage_medium_migrate_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：介质迁移的线程数，用于从 SATA 迁移到 SSD。
- 引入版本：-

##### storage_root_path

- 默认值：`${STARROCKS_HOME}/storage`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：存储数据的目录以及存储介质类型。示例：`/data1,medium:hdd;/data2,medium:ssd`。
  - 多块盘配置使用分号 `;` 隔开。
  - 如果为 SSD 磁盘，需在路径后添加 `,medium:ssd`。
  - 如果为 HDD 磁盘，需在路径后添加 `,medium:hdd`。
- 引入版本：-

##### sync_tablet_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Tablet 元数据同步。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

##### tablet_map_shard_size

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Tablet Map Shard 大小。该值必须是二的倍数。
- 引入版本：-

##### tablet_max_pending_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表每个 Tablet 上允许已提交 (Commit) 但是未 Apply 的最大版本数。
- 引入版本：-

##### tablet_max_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Tablet 上允许的最大版本数。如果超过该值，新的写入请求会失败。
- 引入版本：-

##### tablet_meta_checkpoint_min_interval_secs

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：TabletMeta Checkpoint 线程轮询的时间间隔。
- 引入版本：-

##### tablet_meta_checkpoint_min_new_rowsets_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：自上次 TabletMeta Checkpoint 至今新创建的 Rowset 数量。
- 引入版本：-

##### tablet_rowset_stale_sweep_time_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：失效 Rowset 的清理间隔。
- 引入版本：-

##### tablet_stat_cache_update_interval_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Tablet Stat Cache 的更新间隔。
- 引入版本：-

##### trash_file_expire_time_sec

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：回收站清理的间隔。自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259200 变为 86400。
- 引入版本：-

##### unused_rowset_monitor_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：清理过期 Rowset 的时间间隔。
- 引入版本：-

##### update_cache_expire_sec

- 默认值：360
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Update Cache 的过期时间。
- 引入版本：-

##### update_compaction_check_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：主键表 Compaction 的检查间隔。
- 引入版本：-

##### update_compaction_delvec_file_io_amp_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于控制主键表包含 Delvec 文件的 Rowset 做 Compaction 的优先级。该值越大优先级越高。
- 引入版本：-

##### update_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表每个磁盘 Compaction 线程的数目。
- 引入版本：-

##### update_compaction_per_tablet_min_interval_seconds

- 默认值：120
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：主键表每个 Tablet 做 Compaction 的最小时间间隔。
- 引入版本：-

##### update_compaction_ratio_threshold

- 默认值：0.5
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群下主键表单次 Compaction 可以合并的最大数据比例。如果单个 Tablet 过大，建议适当调小该配置项取值。
- 引入版本：v3.1.5

##### update_compaction_result_bytes

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大结果的大小。
- 引入版本：-

##### update_compaction_size_threshold

- 默认值：268435456
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表的 Compaction Score 是基于文件大小计算的，与其他表类型的文件数量不同。通过该参数可以使主键表的 Compaction Score 与其他类型表的相近，便于用户理解。
- 引入版本：-

##### upload_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点上传任务的最大线程数，用于备份作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

##### vertical_compaction_max_columns_per_group

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每组 Vertical Compaction 的最大列数。
- 引入版本：-

### 存算分离

##### graceful_exit_wait_for_frontend_heartbeat

- 默认值：false
- 类型： Boolean
- 单位：-
- 是否动态：是
- 描述： 确定是否在完成优雅退出前等待至少一个指示SHUTDOWN状态的FE心跳响应。启用后，优雅关闭进程将持续运行直至通过心跳RPC返回给FE SHUTDOWN状态变化，确保FE在两次常规心跳探测间隔期间有足够时间感知终止状态。
- 引入版本：v3.4.5

##### lake_compaction_stream_buffer_size_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群 Compaction 任务在远程 FS 读 I/O 阶段的 Buffer 大小。默认值为 1MB。您可以适当增大该配置项取值以加速 Compaction 任务。
- 引入版本：v3.2.3

##### lake_pk_compaction_max_input_rowsets

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。该参数默认值自 v3.2.4 和 v3.1.10 版本开始从 `5` 变更为 `1000`，并自 v3.3.1 和 v3.2.9 版本开始变更为 `500`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 引入版本：v3.1.8, v3.2.3

##### loop_count_wait_fragments_finish

- 默认值: 2
- 类型：Int
- 单位：-
- 是否动态： 是
- 描述：BE/CN 退出时需要等待正在执行的查询完成的轮次，一轮次固定 10 秒。设置为 `0` 表示禁用轮询等待，立即退出。自 v3.4 起，该参数变为动态参数，且默认值由 `0` 变为 `2`。
- 引入版本：v2.5

##### starlet_filesystem_instance_cache_capacity

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述: starlet filesystem 实例的缓存容量。
- 引入版本: v3.2.16, v3.3.11, v3.4.1

##### starlet_filesystem_instance_cache_ttl_sec

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述: starlet filesystem 实例缓存的过期时间。
- 引入版本: v3.3.15, 3.4.5

##### starlet_port

- 默认值：9070
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 和 CN 的额外 Agent 服务端口。
- 引入版本：-

##### starlet_star_cache_disk_size_percent

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离集群中，Data Cache 最多可使用的磁盘容量百分比。
- 引入版本：v3.1

##### starlet_use_star_cache

- 默认值：false（v3.1）true（v3.2.3 起）
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群中是否使用 Data Cache。`true` 表示启用该功能，`false` 表示禁用。自 v3.2.3 起，默认值由 `false` 调整为 `true`。
- 引入版本：v3.1

##### starlet_write_file_with_tag

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述: 存算分离集群下，是否将写入到对象存储的文件打上对象存储 Tag，方便自定义管理文件。
- 引入版本: v3.5.3

##### table_schema_service_max_retries

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Table Schema Service 请求的最大重试次数。
- 引入版本：v4.1

### 数据湖

##### datacache_block_buffer_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Block Buffer 优化 Data Cache 效率。当启用 Block Buffer 时，系统会从 Data Cache 中读取完整的 Block 数据并缓存在临时 Buffer 中，从而减少频繁读取缓存带来的额外开销。
- 引入版本：v3.2.0

##### datacache_disk_adjust_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Data Cache 容量自动调整周期。每隔这段时间系统会进行一次缓存磁盘使用率检测，必要时触发相应扩缩容操作。
- 引入版本：v3.3.0

##### datacache_disk_idle_seconds_for_expansion

- 默认值：7200
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Data Cache 自动扩容最小等待时间。只有当磁盘使用率在 `datacache_disk_low_level` 以下持续时间超过该时长，才会触发自动扩容。
- 引入版本：v3.3.0

##### datacache_disk_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：单个磁盘缓存数据量的上限，可设为比例上限（如 `80%`）或物理上限（如 `2T`, `500G` 等）。假设系统使用了两块磁盘进行缓存，并设置 `datacache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 引入版本：-

##### datacache_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Data Cache。`true` 表示启用，`false` 表示不启用。自 v3.3 起，默认值变为 `true`。
- 引入版本：-

##### datacache_eviction_policy

- 默认值：slru
- 类型：String
- 单位：-
- 是否动态：否
- 描述：缓存淘汰策略。有效值：`lru` (least recently used) 和 `slru` (Segmented LRU)。
- 引入版本：v3.4.0

##### datacache_inline_item_count_limit

- 默认值：130172
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Data Cache 内联对象数量上限。当缓存的 Block 对象特别小时，Data Cache 会选择使用内联方式将 Block 数据和元数据一起缓存在内存中。
- 引入版本：v3.4.0

##### datacache_mem_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：内存缓存数据量的上限，可设为比例上限（如 `10%`）或物理上限（如 `10G`, `21474836480` 等）。
- 引入版本：-

##### datacache_min_disk_quota_for_adjustment

- 默认值：10737418240
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Data Cache 自动扩缩容时的最小有效容量。当需要调整的目标容量小于该值时，系统会直接将缓存空间调整为 `0`，以避免缓存空间过小导致频繁填充和淘汰带来负优化。
- 引入版本：v3.3.0

##### disk_high_level

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘高水位（百分比）。当磁盘使用率高于该值时，系统自动淘汰 Data Cache 中的缓存数据。自 v3.4.0 起，该参数默认值由 `80` 变更为 `90`。该参数自 v4.0 起由 `datacache_disk_high_level` 更名为 `disk_high_level`。
- 引入版本：v3.3.0

##### disk_low_level

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘低水位（百分比）。当磁盘使用率在 `datacache_disk_idle_seconds_for_expansion` 指定的时间内持续低于该值，且用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。该参数自 v4.0 起由 `datacache_disk_low_level` 更名为 `disk_low_level`。
- 引入版本：v3.3.0

##### disk_safe_level

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘安全水位（百分比）。当 Data Cache 进行缓存自动扩缩容时，系统将尽可能以该阈值为磁盘使用率目标调整缓存容量。自 v3.4.0 起，该参数默认值由 `70` 变更为 `80`。该参数自 v4.0 起由 `datacache_disk_safe_level` 更名为 `disk_safe_level`。
- 引入版本：v3.3.0

##### enable_connector_sink_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否支持在外表写入时启用数据 Spill。启用该功能能够避免当内存不足时写入外表导致生成大量小文件问题。当前仅支持向 Iceberg 表写入数据时启用 Spill 功能。
- 引入版本：v4.0.0

##### enable_datacache_disk_auto_adjust

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘容量自动调整开关，启用后会根据当前磁盘使用率动态调整缓存容量。该参数自 v4.0 起由 `datacache_auto_adjust_enable` 更名为 `enable_datacache_disk_auto_adjust`。
- 引入版本：v3.3.0

##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：JDBC 空闲连接超时时间。如果 JDBC 连接池内的连接空闲时间超过此值，连接池会关闭超过 `jdbc_minimum_idle_connections` 配置项中指定数量的空闲连接。
- 引入版本：-

##### jdbc_connection_pool_size

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：JDBC 连接池大小。每个 BE 节点上访问 `jdbc_url` 相同的外表时会共用同一个连接池。
- 引入版本：-

##### jdbc_minimum_idle_connections

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：JDBC 连接池中最少的空闲连接数量。
- 引入版本：-

##### lake_clear_corrupted_cache_data

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的数据缓存。
- 引入版本：v3.4

##### lake_clear_corrupted_cache_meta

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的元数据缓存。
- 引入版本：v3.3

##### lake_enable_vertical_compaction_fill_data_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许 Vertical Compaction 任务在执行时缓存数据到本地磁盘上。`true` 表示启用，`false` 表示不启用。
- 引入版本：v3.1.7, v3.2.3

##### lake_service_max_concurrency

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：在存算分离集群中，RPC 请求的最大并发数。当达到此阈值时，新请求会被拒绝。将此项设置为 `0` 表示对并发不做限制。
- 引入版本：-

##### query_max_memory_limit_percent

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Query Pool 能够使用的最大内存上限。以 Process 内存上限的百分比来表示。
- 引入版本：v3.1.0

##### rocksdb_max_write_buffer_memory_bytes

- 默认值：1073741824
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer内存的最大上限。
- 引入版本：v3.5.0

##### rocksdb_write_buffer_memory_percent

- 默认值：5
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer可以使用的内存占比。默认值是百分之5，最终取值不会小于64MB，也不会大于1GB。
- 引入版本：v3.5.0

### 其他

##### default_mv_resource_group_concurrency_limit

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务在单个 BE 上的并发上限。默认为 `0`，即不做并发数限制。
- 引入版本：-

##### default_mv_resource_group_cpu_limit

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 的 CPU 核数上限。
- 引入版本：-

##### default_mv_resource_group_memory_limit

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 内存上限，默认 80%。
- 引入版本：v3.1

##### default_mv_resource_group_spill_mem_limit_threshold

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务触发落盘的内存占用阈值，默认80%。
- 引入版本：v3.1

##### enable_resolve_hostname_to_ip_in_load_error_url

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态: 是
- 描述: `error_urls` Debug 过程中，是否允许 Operator 根据环境需求选择使用 FE 心跳的原始主机名，或强制解析为 IP 地址。
  - `true`：将主机名解析为 IP 地址。
  - `false`（默认）：在错误 URL 中保留原始主机名。
- 引入版本：v4.0.1

##### enable_token_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Token 检验。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

##### es_scroll_keepalive

- 默认值: 5m
- 类型: String
- 单位：Minutes (string with suffix, e.g. "5m")
- 是否动态：否
- 描述: 发送给 Elasticsearch 的 scroll 搜索上下文的 keep-alive 时长。该值在构建初始 scroll URL (`?scroll=<value>`) 以及发送后续 scroll 请求（通过 ESScrollQueryBuilder）时按字面使用（例如 "5m"）。此设置控制 ES 端在垃圾回收前保留搜索上下文的时间；设置更长会让 scroll 上下文存活更久，但会延长 ES 集群的资源占用。该值在启动时由 ES scan reader 读取，运行时不可更改。
- 引入版本: v3.2.0

##### load_replica_status_check_interval_ms_on_failure

- 默认值: 2000
- 类型: Int
- 单位: Milliseconds
- 是否动态：是
- 描述: 当上一次检查的 RPC 失败时，从副本向主副本检查其状态的时间间隔。
- 引入版本: v3.5.1

##### load_replica_status_check_interval_ms_on_success

- 默认值: 15000
- 类型: Int
- 单位: Milliseconds
- 是否动态：是
- 描述: 当上一次检查的 RPC 成功时，从副本向主副本检查其状态的时间间隔。
- 引入版本: v3.5.1

##### max_length_for_bitmap_function

- 默认值：1000000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bitmap 函数输入值的最大长度。
- 引入版本：-

##### max_length_for_to_base64

- 默认值：200000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：to_base64() 函数输入值的最大长度。
- 引入版本：-

##### memory_high_level

- 默认值: 75
- 类型: Long
- 单位: Percent
- 是否动态：是
- 描述: 以进程内存上限的百分比表示的高水位内存阈值。当总内存消耗上升超过该百分比时，BE 开始逐步释放内存（目前通过驱逐 data cache 和 update cache）以缓解压力。监控器使用此值来计算 `memory_high = mem_limit * memory_high_level / 100`，并且如果消耗大于 memory_high，则在 GC advisor 的指导下执行受控驱逐；如果消耗超过 memory_urgent_level（一个单独的配置），则会进行更激进的即时回收。此值还用于在超过阈值时禁用某些高内存消耗的操作（例如 primary-key preload）。必须满足与 memory_urgent_level 的校验关系（memory_urgent_level `>` memory_high_level，memory_high_level `>=` 1，memory_urgent_level `<=` 100）。
- 引入版本: v3.2.0

##### report_exec_rpc_request_retry_num

- 默认值：10
- 类型: Int
- 单位：-
- 是否动态：是
- 描述：用于向 FE 汇报执行状态的 RPC 请求的重试次数。默认值为 10，意味着如果该 RPC 请求失败（仅限于 fragment instance 的 finish RPC），将最多重试 10 次。该请求对于导入任务（load job）非常重要，如果某个 fragment instance 的完成状态报告失败，整个导入任务将会一直挂起，直到超时。
-引入版本：-

##### sleep_one_second

- 默认值: 1
- 类型: Int
- 单位：秒
- 是否动态：否
- 描述: BE Agent Worker 线程的全局短睡眠间隔，当 Master 地址/心跳尚不可用或需要短时间重试/退避时作为一秒的暂停。多个 Report Worker Pool（例如 ReportDiskStateTaskWorkerPool、ReportOlapTableTaskWorkerPool、ReportWorkgroupTaskWorkerPool）引用此值，以避免忙等（busy-waiting）并在重试时降低 CPU 消耗。增大此值会降低重试频率并减慢对 Master 可用性的响应；减小会提高轮询频率并增加 CPU 使用。请在权衡响应性与资源使用后调整该项。
- 引入版本: v3.2.0

##### small_file_dir

- 默认值：`${STARROCKS_HOME}/lib/small_file/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：保存文件管理器下载的文件的目录。
- 引入版本：-

##### user_function_dir

- 默认值：`${STARROCKS_HOME}/lib/udf`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：UDF 存放的路径。
- 引入版本：-

##### web_log_bytes

- 默认值: 1048576 (1 MB)
- 类型: long
- 单位：Bytes
- 是否动态：否
- 描述: 从 INFO 日志文件读取并在 BE 调试 Web Server 的日志页面上显示的最大字节数。该处理器使用此值计算一个 seek 偏移量（显示最后 N 字节），以避免读取或提供非常大的日志文件。如果日志文件小于该值则显示整个文件。注意：在当前实现中，用于读取并服务 INFO 日志的代码被注释掉了，处理器会报告无法打开 INFO 日志文件，因此除非启用日志服务代码，否则此参数可能无效。
- 引入版本: v3.2.0

