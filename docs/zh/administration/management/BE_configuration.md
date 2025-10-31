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

##### sys_log_verbose_modules

- 默认值：
- 类型：Strings
- 单位：-
- 是否动态：否
- 描述：日志打印的模块。有效值为 BE 的 namespace，包括 `starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream` 以及 `starrocks::workgroup`。
- 引入版本：-

##### sys_log_verbose_level

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：日志显示的级别，用于控制代码中 VLOG 开头的日志输出。
- 引入版本：-

##### log_buffer_level

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：日志落盘的策略。默认值表示日志缓存在内存中。有效值为 `-1` 和 `0`。`-1` 表示日志不在内存中缓存。
- 引入版本：-

### 服务器

##### be_port

- 默认值：9060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 上 Thrift Server 的端口，用于接收来自 FE 的请求。
- 引入版本：-

##### brpc_port

- 默认值：8060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的端口，可以查看 bRPC 的一些网络统计信息。
- 引入版本：-

##### brpc_num_threads

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：bRPC 的 bthread 线程数量，`-1` 表示和 CPU 核数一样。
- 引入版本：-

##### priority_networks

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：为有多个 IP 地址的服务器声明 IP 选择策略。请注意，最多应该有一个 IP 地址与此列表匹配。此参数的值是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。如果没有 IP 地址匹配此列表中的条目，系统将随机选择服务器的一个可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，系统将默认使用 IPv4 地址。您可以通过将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本：-

##### net_use_ipv6_when_priority_networks_empty

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址的布尔值。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本：-

##### mem_limit

- 默认值：90%
- 类型：String
- 单位：-
- 是否动态：否
- 描述：BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100G"）。默认的硬限制为服务器内存大小的 90%，软限制为 80%。如果您希望在同一台服务器上同时部署 StarRocks 和其他内存密集型服务，则需要配置此参数。
- 引入版本：-

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

##### compress_rowbatches

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。`true` 表示压缩，`false` 表示不压缩。
- 引入版本：-

##### thrift_client_retry_interval_ms

- 默认值：100
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Thrift Client 默认的重试时间间隔。
- 引入版本：-

##### be_http_port

- 默认值：8040
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE HTTP Server 端口。
- 引入版本：-

##### be_http_num_workers

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HTTP Server 线程数。
- 引入版本：-

##### be_exit_after_disk_write_hang_second

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：磁盘挂起后触发 BE 进程退出的等待时间。
- 引入版本：-

##### thrift_rpc_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Thrift RPC 超时的时长。
- 引入版本：-

##### thrift_rpc_strict_mode

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用了 Thrift 的严格执行模式。 Thrift 严格模式，参见 [Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md)。
- 引入版本：-

##### thrift_rpc_max_body_size

- 默认值：0
- 类型：Int
- 单位：单位：Milliseconds
- 是否动态：否
- 描述：RPC 最大字符串体大小。`0` 表示无限制。
- 引入版本：-

##### thrift_rpc_connection_max_valid_time_ms

- 默认值：5000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：Thrift RPC 连接的最长有效时间。如果连接池中存在的时间超过此值，连接将被关闭。需要与 FE 配置项 `thrift_client_timeout_ms` 保持一致。
- 引入版本：-

##### brpc_max_body_size

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bRPC 最大的包容量。
- 引入版本：-

### 查询引擎

##### scanner_thread_pool_thread_num

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存储引擎并发扫描磁盘的线程数，统一管理在线程池中。
- 引入版本：-

##### scanner_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存储引擎支持的扫描任务数。
- 引入版本：-

##### scanner_row_num

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个扫描线程单次执行最多返回的数据行数。
- 引入版本：-

##### max_scan_key_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询最多拆分的 Scan Key 数目。
- 引入版本：-

##### max_pushdown_conditions_per_column

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单列上允许下推的最大谓词数量，如果超出数量限制，谓词不会下推到存储层。
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

##### min_file_descriptor_number

- 默认值：60000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 进程中文件描述符的最小数量。
- 引入版本：-

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

##### enable_zonemap_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Zonemap index 开启 Memory Cache。使用 Zonemap index 加速 Scan 时，可以考虑开启。
- 引入版本：-

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

##### string_prefix_zonemap_prefix_len

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：启用 `enable_string_prefix_zonemap` 时用于字符串 Zonemap 最小值/最大值的前缀长度。
- 引入版本：-

##### fragment_pool_thread_num_min

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最小查询线程数。
- 引入版本：-

##### fragment_pool_thread_num_max

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最大查询线程数。
- 引入版本：-

##### fragment_pool_queue_size

- 默认值：2048
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单 BE 节点上能够处理的查询请求上限。
- 引入版本：-

##### flamegraph_tool_dir

- 默认值：`${STARROCKS_HOME}/bin/flamegraph`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：火焰图工具的目录，该目录应包含 pprof、stackcollapse-go.pl 和 flamegraph.pl 脚本，用于从性能分析数据生成火焰图。
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
- 单位：Seconds
- 是否动态：是
- 描述：BufferControlBlock 释放数据的等待时间。
- 引入版本：-

##### max_memory_sink_batch_count

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Scan Cache 的最大缓存批次数量。
- 引入版本：-

##### scan_context_gc_interval_min

- 默认值：5
- 类型：Int
- 单位：Minutes
- 是否动态：是
- 描述：Scan Context 的清理间隔。
- 引入版本：-

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
- 单位：Milliseconds
- 是否动态：是
- 描述：多次连续 Scan 文件间隔时间。
- 引入版本：-

##### path_scan_interval_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：GC 线程清理过期数据的间隔时间。
- 引入版本：-

##### pipeline_poller_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：当该值大于 `0` 时，则在轮询器中，如果某个 Driver 的单次调度时间超过了 `pipeline_poller_timeout_guard_ms` 的时间，则会打印该 Driver 以及 Operator 信息。
- 引入版本：-

##### pipeline_prepare_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：当该值大于 `0` 时，如果 PREPARE 过程中 Plan Fragment 超过 `pipeline_prepare_timeout_guard_ms` 的时间，则会打印 Plan Fragment 的堆栈跟踪。
- 引入版本：-

##### pipeline_connector_scan_thread_num_per_cpu

- 默认值：8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：BE 节点中每个 CPU 核心分配给 Pipeline Connector 的扫描线程数量。自 v3.1.7 起变为动态参数。
- 引入版本：-

##### pipeline_scan_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎扫描线程池任务队列的最大队列长度。
- 引入版本：-

##### pipeline_prepare_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎准备片段线程池中的线程数。`0` 表示等于系统 VCPU 数量。
- 引入版本：-

##### pipeline_prepare_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎在线程池中执行 PREPARE Fragment 的队列长度。
- 引入版本：-

##### max_hdfs_file_handle

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多可以打开的 HDFS 文件描述符数量。
- 引入版本：-

##### object_storage_connect_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：对象存储 Socket 连接的超时时间。`-1` 表示使用 SDK 中的默认时间。
- 引入版本：v3.0.9

##### object_storage_request_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：Milliseconds
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

##### io_coalesce_adaptive_lazy_active

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：根据谓词选择度，自适应决定是否将谓词列 IO 和非谓词列 IO 进行合并。
- 引入版本：v3.2

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
- 单位：Milliseconds
- 是否动态：否
- 描述：发起 Hedged Read 请求前需要等待多少毫秒。例如，假设该参数设置为 `30`，那么如果一个 Read 任务未能在 30 毫秒内返回结果，则 HDFS 客户端会立即发起一个 Hedged Read，从目标数据块的副本上读取数据。该参数对应 HDFS 集群配置文件 **hdfs-site.xml** 中的 `dfs.client.hedged.read.threshold.millis` 参数。
- 引入版本：v3.0

##### query_cache_capacity

- 默认值：536870912
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：指定 Query Cache 的大小。默认为 512 MB。最小不低于 4 MB。如果当前的 BE 内存容量无法满足您期望的 Query Cache 大小，可以增加 BE 的内存容量，然后再设置合理的 Query Cache 大小。每个 BE 都有自己私有的 Query Cache 存储空间，BE 只 Populate 或 Probe 自己本地的 Query Cache 存储空间。
- 引入版本：-

##### enable_json_flat

- 默认值：false
- 类型：Boolean
- 单位：
- 是否动态：是
- 描述：是否开启 Flat JSON 特性。开启后新导入的 JSON 数据会自动打平，提升 JSON 数据查询性能。
- 引入版本：v3.3.0

##### enable_compaction_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：控制是否为 Flat Json 数据进行 Compaction。
- 引入版本：v3.3.3

##### enable_lazy_dynamic_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：当查询在读过程中未命中 Flat JSON Schema 时，是否启用 Lazy Dynamic Flat JSON。当此项设置为 `true` 时，StarRocks 将把 Flat JSON 操作推迟到计算流程，而不是读取流程。
- 引入版本：v3.3.3

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

##### json_flat_column_max

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，最多提取的子列数量。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

##### jit_lru_cache_size

- 默认值：0
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：JIT 编译的 LRU 缓存大小。如果设置为大于 0，则表示实际的缓存大小。如果设置为小于或等于 0，系统将自适应设置缓存大小，使用的公式为 `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` （节点的 `mem_limit` 必须大于或等于 16 GB）。
- 引入版本：-

### 导入

##### push_worker_count_normal_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 NORMAL 优先级任务。
- 引入版本：-

##### push_worker_count_high_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 HIGH 优先级任务。
- 引入版本：-

##### transaction_publish_version_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：生效版本的最大线程数。当该参数被设置为小于或等于 `0` 时，系统默认使用当前节点的 CPU 核数，以避免因使用固定值而导致在导入并行较高时线程资源不足。自 2.5 版本起，默认值由 `8` 变更为 `0`。
- 引入版本：-

##### transaction_publish_version_thread_pool_idle_time_ms

- 默认值：60000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：线程被 Publish Version 线程池回收前的 Idle 时间。
- 引入版本：-

##### clear_transaction_task_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：清理事务的线程数。
- 引入版本：-

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

##### number_tablet_writer_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：导入用的 tablet writer 线程数, 用于 Stream Load、Broker Load、Insert 等。当参数设置为小于等于 0 时，系统使用 CPU 核数的二分之一，最小为 16。当参数设置为大于 0 时，系统使用该值。自 v3.1.7 起变为动态参数。
- 引入版本：-

##### streaming_load_max_mb

- 默认值：102400
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个文件大小的上限。自 3.0 版本起，默认值由 10240 变为 102400。
- 引入版本：-

##### streaming_load_max_batch_size_mb

- 默认值：100
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个 JSON 文件大小的上限。
- 引入版本：-

##### streaming_load_rpc_max_alive_time_sec

- 默认值：1200
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：Stream Load 的 RPC 超时时长。
- 引入版本：-

##### write_buffer_size

- 默认值：104857600
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：MemTable 在内存中的 Buffer 大小，超过这个限制会触发 Flush。
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
- 描述：在存算分离模式下，每个 Store 用以 Flush MemTable 的线程数。当该参数被设置为 `0` 时，系统使用 CPU 核数的两倍。
当该参数被设置为小于 `0` 时，系统使用该参数的绝对值与 CPU 核数的乘积。
- 引入版本：3.1.12, 3.2.7

##### max_runnings_transactions_per_txn_map

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个分区内部同时运行的最大事务数量。
- 引入版本：-

##### enable_stream_load_verbose_log

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在日志中记录 Stream Load 的 HTTP 请求和响应信息。`true` 表示启用，`false` 表示不启用。
- 引入版本：v2.5.17, v3.0.9, v3.1.6, v3.2.1

### 统计信息

##### report_task_interval_seconds

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：汇报单个任务的间隔。建表，删除表，导入，Schema Change 都可以被认定是任务。
- 引入版本：-

##### report_disk_state_interval_seconds

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：汇报磁盘状态的间隔。汇报各个磁盘的状态，以及其中数据量等。
- 引入版本：-

##### report_tablet_interval_seconds

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：汇报 Tablet 的间隔。汇报所有的 Tablet 的最新版本。
- 引入版本：-

##### report_workgroup_interval_seconds

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：汇报 Workgroup 的间隔。汇报所有 Workgroup 的最新版本。
- 引入版本：-

### 存储

##### drop_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：删除 Tablet 的线程数。
- 引入版本：-

##### alter_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：进行 Schema Change 的线程数。自 2.5 版本起，该参数由静态变为动态。
- 引入版本：-

##### storage_medium_migrate_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：介质迁移的线程数，用于从 SATA 迁移到 SSD。
- 引入版本：-

##### check_consistency_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：计算 Tablet 的校验和（checksum）的线程数。
- 引入版本：-

##### upload_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点上传任务的最大线程数，用于备份作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

##### download_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点下载任务的最大线程数，用于恢复作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

##### make_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点快照任务的最大线程数。
- 引入版本：-

##### release_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点释放快照任务的最大线程数。
- 引入版本：-

##### max_download_speed_kbps

- 默认值：50000
- 类型：Int
- 单位：KB/Second
- 是否动态：是
- 描述：单个 HTTP 请求的最大下载速率。这个值会影响 BE 之间同步数据副本的速度。
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
- 单位：Seconds
- 是否动态：是
- 描述：单个 HTTP 请求持续以低于 `download_low_speed_limit_kbps` 值的速度运行时，允许运行的最长时间。在配置项中指定的时间跨度内，当一个 HTTP 请求持续以低于该值的速度运行时，该请求将被中止。
- 引入版本：-

##### compact_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 引入版本：v3.0.0

##### replication_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于同步的最大线程数。0 表示将线程数设置为 BE CPU 内核数的四倍。
- 引入版本：v3.3.5

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
- 单位：Seconds
- 是否动态：是
- 描述：同步线程低于最低速度所允许的持续时间。如果实际速度低于 `replication_min_speed_limit_kbps` 的时间超过此值，同步将失败。
- 引入版本：v3.3.5

##### clear_expired_replication_snapshots_interval_seconds

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：系统清除异常同步遗留的过期快照的时间间隔。
- 引入版本：v3.3.5

##### memory_limitation_per_thread_for_schema_change

- 默认值：2
- 类型：Int
- 单位：GB
- 是否动态：是
- 描述：单个 Schema Change 任务允许占用的最大内存。
- 引入版本：-

##### update_cache_expire_sec

- 默认值：360
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Update Cache 的过期时间。
- 引入版本：-

##### file_descriptor_cache_clean_interval

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：文件描述符缓存清理的间隔，用于清理长期不用的文件描述符。
- 引入版本：-

##### disk_stat_monitor_interval

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：磁盘健康状态检测的间隔。
- 引入版本：-

##### unused_rowset_monitor_interval

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：清理过期 Rowset 的时间间隔。
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

##### max_percentage_of_error_disk

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：错误磁盘达到该比例上限，BE 退出。
- 引入版本：-

##### default_num_rows_per_column_file_block

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Row Block 最多存放的行数。
- 引入版本：-

##### pending_data_expire_time_sec

- 默认值：1800
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：存储引擎保留的未生效数据的最大时长。
- 引入版本：-

##### inc_rowset_expired_sec

- 默认值：1800
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导入生效的数据，存储引擎保留的时间，用于增量克隆。
- 引入版本：-

##### tablet_rowset_stale_sweep_time_sec

- 默认值：1800
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：失效 Rowset 的清理间隔。
- 引入版本：-

##### max_garbage_sweep_interval

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：磁盘进行垃圾清理的最大间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

##### min_garbage_sweep_interval

- 默认值：180
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：磁盘进行垃圾清理的最小间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

##### snapshot_expire_time_sec

- 默认值：172800
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：快照文件清理的间隔。
- 引入版本：-

##### trash_file_expire_time_sec

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：回收站清理的间隔。自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259200 变为 86400。
- 引入版本：-

##### base_compaction_check_interval_seconds

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Base Compaction 线程轮询的间隔。
- 引入版本：-

##### min_base_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Base Compaction 的最小 Segment 数。
- 引入版本：-

##### max_base_compaction_num_singleton_deltas

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Base Compaction 合并的最大 Segment 数。
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

##### base_compaction_interval_seconds_since_last_operation

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：上一轮 Base Compaction 距今的间隔。此项为 Base Compaction 触发条件之一。
- 引入版本：-

##### cumulative_compaction_check_interval_seconds

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Cumulative Compaction 线程轮询的间隔。
- 引入版本：-

##### min_cumulative_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Cumulative Compaction 的最小 Segment 数。
- 引入版本：-

##### max_cumulative_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Cumulative Compaction 能合并的最大 Segment 数。如果 Compaction 时出现内存不足的情况，可以调小该值。
- 引入版本：-

##### cumulative_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个磁盘 Cumulative Compaction 线程的数目。
- 引入版本：-

##### max_compaction_candidate_num

- 默认值：40960
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 候选 Tablet 的最大数量。太大会导致内存占用和 CPU 负载高。
- 引入版本：-

##### update_compaction_check_interval_seconds

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：主键表 Compaction 的检查间隔。
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
- 单位：Seconds
- 是否动态：是
- 描述：主键表每个 Tablet 做 Compaction 的最小时间间隔。
- 引入版本：-

##### max_update_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大 Rowset 数。
- 引入版本：-

##### update_compaction_size_threshold

- 默认值：268435456
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表的 Compaction Score 是基于文件大小计算的，与其他表类型的文件数量不同。通过该参数可以使主键表的 Compaction Score 与其他类型表的相近，便于用户理解。
- 引入版本：-

##### update_compaction_result_bytes

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大结果的大小。
- 引入版本：-

##### update_compaction_delvec_file_io_amp_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于控制主键表包含 Delvec 文件的 Rowset 做 Compaction 的优先级。该值越大优先级越高。
- 引入版本：-

##### update_compaction_ratio_threshold

- 默认值：0.5
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群下主键表单次 Compaction 可以合并的最大数据比例。如果单个 Tablet 过大，建议适当调小该配置项取值。
- 引入版本：v3.1.5

##### repair_compaction_interval_seconds

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Repair Compaction 线程轮询的间隔。
- 引入版本：-

##### manual_compaction_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Number of threads for Manual Compaction.
- 引入版本：-

##### min_compaction_failure_interval_sec

- 默认值：120
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Tablet Compaction 失败之后，再次被调度的间隔。
- 引入版本：-

##### min_cumulative_compaction_failure_interval_sec

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Cumulative Compaction 失败后的最小重试间隔。
- 引入版本：-

##### max_compaction_concurrency

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 `-1` 代表没有限制。`0` 表示禁用 Compaction。开启 Event-based Compaction Framework 时，该参数才支持动态设置。
- 引入版本：-

##### compaction_trace_threshold

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：单次 Compaction 打印 Trace 的时间阈值，如果单次 Compaction 时间超过该阈值就打印 Trace。
- 引入版本：-

##### enable_rowset_verify

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否检查 Rowset 的正确性。开启后，会在 Compaction、Schema Change 后检查生成的 Rowset 的正确性。
- 引入版本：-

##### vertical_compaction_max_columns_per_group

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每组 Vertical Compaction 的最大列数。
- 引入版本：-

##### enable_event_based_compaction_framework

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Event-based Compaction Framework。`true` 代表开启。`false` 代表关闭。开启则能够在 Tablet 数比较多或者单个 Tablet 数据量比较大的场景下大幅降低 Compaction 的开销。
- 引入版本：-

##### enable_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Size-tiered Compaction 策略 (Primary Key 表除外)。`true` 代表开启。`false` 代表关闭。
- 引入版本：-

##### enable_pk_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否为 Primary Key 表开启 Size-tiered Compaction 策略。`true` 代表开启。`false` 代表关闭。
- 引入版本：存算分离集群自 v3.2.4, v3.1.10 起生效，存算一体集群自 v3.2.5, v3.1.10 起生效

##### enable_pk_parallel_execution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Primary Key 表并行执行策略。当并行执行策略开启时，pk索引文件会在导入和compaction阶段生成。
- 引入版本：-

##### pk_parallel_execution_threshold_bytes

- 默认值：314572800
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：当enable_pk_parallel_execution设置为true后，导入或者compaction生成的数据大于该阈值时，Primary Key 表并行执行策略将被启用。
- 引入版本：-

##### size_tiered_min_level_size

- 默认值：131072
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，最小 Level 的大小，小于此数值的 Rowset 会直接触发 Compaction。
- 引入版本：-

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

##### enable_check_string_lengths

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在导入时进行数据长度检查，以解决 VARCHAR 类型数据越界导致的 Compaction 失败问题。
- 引入版本：-

##### max_row_source_mask_memory_bytes

- 默认值：209715200
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：Row source mask buffer 的最大内存占用大小。当 buffer 大于该值时将会持久化到磁盘临时文件中。该值应该小于 `compaction_memory_limit_per_worker` 参数的值。
- 引入版本：-

##### load_process_max_memory_limit_percent

- 默认值：30
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单节点上所有的导入线程占据内存的软上限（百分比）。
- 引入版本：-

##### load_process_max_memory_hard_limit_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单节点上所有的导入线程占据内存的硬上限（比例）。当 `enable_new_load_on_memory_limit_exceeded` 设置为 `false`，并且所有导入线程的内存占用超过 `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio` 时，系统将会拒绝新的导入线程。
- 引入版本：v3.3.2

##### enable_new_load_on_memory_limit_exceeded

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在导入线程内存占用达到硬上限后，是否允许新的导入线程。`true` 表示允许新导入线程，`false` 表示拒绝新导入线程。
- 引入版本：v3.3.2

##### tablet_stat_cache_update_interval_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Tablet Stat Cache 的更新间隔。
- 引入版本：-

##### sync_tablet_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Tablet 元数据同步。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

##### storage_flood_stage_usage_percent

- 默认值：95
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 存储目录整体磁盘空间使用率的硬上限。如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，StarRocks 会拒绝 Load 和 Restore 作业。需要同步修改 FE 配置 `storage_usage_hard_limit_percent` 以使其生效。
- 引入版本：-

##### storage_flood_stage_left_capacity_bytes

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：BE 存储目录整体磁盘剩余空间的硬限制。如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，StarRocks 会拒绝 Load 和 Restore 作业，默认 100GB。需要同步修改 FE 配置 `storage_usage_hard_limit_reserve_bytes` 以使其生效。
- 引入版本：-

##### tablet_meta_checkpoint_min_new_rowsets_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：自上次 TabletMeta Checkpoint 至今新创建的 Rowset 数量。
- 引入版本：-

##### tablet_meta_checkpoint_min_interval_secs

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：TabletMeta Checkpoint 线程轮询的时间间隔。
- 引入版本：-

##### tablet_map_shard_size

- 默认值：32
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Tablet Map Shard 大小。该值必须是二的倍数。
- 引入版本：-

##### tablet_max_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Tablet 上允许的最大版本数。如果超过该值，新的写入请求会失败。
- 引入版本：-

##### tablet_max_pending_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表每个 Tablet 上允许已提交 (Commit) 但是未 Apply 的最大版本数。
- 引入版本：-

##### pindex_major_compaction_limit_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题。
- 引入版本：v3.0.9

##### primary_key_limit_size

- 默认值：128
- 类型：Int
- 单位：Byte
- 是否动态：是
- 描述：主键表中单条主键值最大长度。
- 引入版本：v2.5

##### avro_ignore_union_type_tag

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在 Avro Union 数据结构序列化成 JSON 格式时，去除 Union 结构的类型标签信息。
- 引入版本：v3.3.7, v3.4

### 存算分离

##### starlet_port

- 默认值：9070
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 和 CN 的额外 Agent 服务端口。
- 引入版本：-

##### starlet_use_star_cache

- 默认值：false（v3.1）true（v3.2.3 起）
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离模式下是否使用 Data Cache。`true` 表示启用该功能，`false` 表示禁用。自 v3.2.3 起，默认值由 `false` 调整为 `true`。
- 引入版本：v3.1

##### starlet_star_cache_disk_size_percent

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离模式下，Data Cache 最多可使用的磁盘容量百分比。
- 引入版本：v3.1

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

##### starlet_write_file_with_tag

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述: 存算分离集群下，是否将写入到对象存储的文件打上对象存储 Tag，方便自定义管理文件。
- 引入版本: v3.5.3

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

##### graceful_exit_wait_for_frontend_heartbeat

- 默认值：false
- 类型： Boolean
- 单位：-
- 是否动态：是
- 描述： 确定是否在完成优雅退出前等待至少一个指示SHUTDOWN状态的FE心跳响应。启用后，优雅关闭进程将持续运行直至通过心跳RPC返回给FE SHUTDOWN状态变化，确保FE在两次常规心跳探测间隔期间有足够时间感知终止状态。
- 引入版本：v3.4.5

### 数据湖

##### disk_high_level

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘高水位（百分比）。当磁盘使用率高于该值时，系统自动淘汰 Data Cache 中的缓存数据。自 v3.4.0 起，该参数默认值由 `80` 变更为 `90`。该参数自 v4.0 起由 `datacache_disk_high_level` 更名为 `disk_high_level`。
- 引入版本：v3.3.0

##### disk_safe_level

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘安全水位（百分比）。当 Data Cache 进行缓存自动扩缩容时，系统将尽可能以该阈值为磁盘使用率目标调整缓存容量。自 v3.4.0 起，该参数默认值由 `70` 变更为 `80`。该参数自 v4.0 起由 `datacache_disk_safe_level` 更名为 `disk_safe_level`。
- 引入版本：v3.3.0

##### disk_low_level

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘低水位（百分比）。当磁盘使用率在 `datacache_disk_idle_seconds_for_expansion` 指定的时间内持续低于该值，且用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。该参数自 v4.0 起由 `datacache_disk_low_level` 更名为 `disk_low_level`。
- 引入版本：v3.3.0

##### query_max_memory_limit_percent

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Query Pool 能够使用的最大内存上限。以 Process 内存上限的百分比来表示。
- 引入版本：v3.1.0

##### lake_clear_corrupted_cache_meta

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的元数据缓存。
- 引入版本：v3.3

##### lake_clear_corrupted_cache_data

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的数据缓存。
- 引入版本：v3.4

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

##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：JDBC 空闲连接超时时间。如果 JDBC 连接池内的连接空闲时间超过此值，连接池会关闭超过 `jdbc_minimum_idle_connections` 配置项中指定数量的空闲连接。
- 引入版本：-

##### datacache_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Data Cache。`true` 表示启用，`false` 表示不启用。自 v3.3 起，默认值变为 `true`。
- 引入版本：-

##### datacache_mem_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：内存缓存数据量的上限，可设为比例上限（如 `10%`）或物理上限（如 `10G`, `21474836480` 等）。
- 引入版本：-

##### datacache_disk_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：单个磁盘缓存数据量的上限，可设为比例上限（如 `80%`）或物理上限（如 `2T`, `500G` 等）。假设系统使用了两块磁盘进行缓存，并设置 `datacache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 引入版本：-

##### datacache_block_buffer_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Block Buffer 优化 Data Cache 效率。当启用 Block Buffer 时，系统会从 Data Cache 中读取完整的 Block 数据并缓存在临时 Buffer 中，从而减少频繁读取缓存带来的额外开销。
- 引入版本：v3.2.0

##### enable_datacache_disk_auto_adjust

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘容量自动调整开关，启用后会根据当前磁盘使用率动态调整缓存容量。该参数自 v4.0 起由 `datacache_auto_adjust_enable` 更名为 `enable_datacache_disk_auto_adjust`。
- 引入版本：v3.3.0

##### datacache_disk_adjust_interval_seconds

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Data Cache 容量自动调整周期。每隔这段时间系统会进行一次缓存磁盘使用率检测，必要时触发相应扩缩容操作。
- 引入版本：v3.3.0

##### datacache_disk_idle_seconds_for_expansion

- 默认值：7200
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Data Cache 自动扩容最小等待时间。只有当磁盘使用率在 `datacache_disk_low_level` 以下持续时间超过该时长，才会触发自动扩容。
- 引入版本：v3.3.0

##### datacache_min_disk_quota_for_adjustment

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Data Cache 自动扩缩容时的最小有效容量。当需要调整的目标容量小于该值时，系统会直接将缓存空间调整为 `0`，以避免缓存空间过小导致频繁填充和淘汰带来负优化。
- 引入版本：v3.3.0

##### datacache_inline_item_count_limit

- 默认值：130172
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Data Cache 内联对象数量上限。当缓存的 Block 对象特别小时，Data Cache 会选择使用内联方式将 Block 数据和元数据一起缓存在内存中。
- 引入版本：v3.4.0

##### datacache_eviction_policy

- 默认值：slru
- 类型：String
- 单位：-
- 是否动态：否
- 描述：缓存淘汰策略。有效值：`lru` (least recently used) 和 `slru` (Segmented LRU)。
- 引入版本：v3.4.0

##### rocksdb_write_buffer_memory_percent

- 默认值：5
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer可以使用的内存占比。默认值是百分之5，最终取值不会小于64MB，也不会大于1GB。
- 引入版本：v3.5.0

##### rocksdb_max_write_buffer_memory_bytes

- 默认值：1073741824
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer内存的最大上限。
- 引入版本：v3.5.0

##### lake_service_max_concurrency

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：在存算分离集群中，RPC 请求的最大并发数。当达到此阈值时，新请求会被拒绝。将此项设置为 `0` 表示对并发不做限制。
- 引入版本：-

##### lake_enable_vertical_compaction_fill_data_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许 Vertical Compaction 任务在执行时缓存数据到本地磁盘上。`true` 表示启用，`false` 表示不启用。
- 引入版本：v3.1.7, v3.2.3

##### enable_connector_sink_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否支持在外表写入时启用数据 Spill。启用该功能能够避免当内存不足时写入外表导致生成大量小文件问题。当前仅支持向 Iceberg 表写入数据时启用 Spill 功能。
- 引入版本：v4.0.0

### 其他

##### enable_resolve_hostname_to_ip_in_load_error_url

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否动态: 是
- 描述: `error_urls` Debug 过程中，是否允许 Operator 根据环境需求选择使用 FE 心跳的原始主机名，或强制解析为 IP 地址。
  - `true`：将主机名解析为 IP 地址。
  - `false`（默认）：在错误 URL 中保留原始主机名。
- 引入版本：v4.0.1

##### user_function_dir

- 默认值：`${STARROCKS_HOME}/lib/udf`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：UDF 存放的路径。
- 引入版本：-

##### enable_token_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Token 检验。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

##### small_file_dir

- 默认值：`${STARROCKS_HOME}/lib/small_file/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：保存文件管理器下载的文件的目录。
- 引入版本：-

##### report_exec_rpc_request_retry_num

- 默认值：10
- 类型: Int
- 单位：-
- 是否动态：是
- 描述：用于向 FE 汇报执行状态的 RPC 请求的重试次数。默认值为 10，意味着如果该 RPC 请求失败（仅限于 fragment instance 的 finish RPC），将最多重试 10 次。该请求对于导入任务（load job）非常重要，如果某个 fragment instance 的完成状态报告失败，整个导入任务将会一直挂起，直到超时。
-引入版本：-

##### max_length_for_to_base64

- 默认值：200000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：to_base64() 函数输入值的最大长度。
- 引入版本：-

##### max_length_for_bitmap_function

- 默认值：1000000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bitmap 函数输入值的最大长度。
- 引入版本：-

##### default_mv_resource_group_memory_limit

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 内存上限，默认 80%。
- 引入版本：v3.1

##### default_mv_resource_group_cpu_limit

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 的 CPU 核数上限。
- 引入版本：-

##### default_mv_resource_group_concurrency_limit

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务在单个 BE 上的并发上限。默认为 `0`，即不做并发数限制。
- 引入版本：-

##### default_mv_resource_group_spill_mem_limit_threshold

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务触发落盘的内存占用阈值，默认80%。
- 引入版本：v3.1

