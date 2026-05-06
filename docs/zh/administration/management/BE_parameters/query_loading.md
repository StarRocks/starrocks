---
displayed_sidebar: docs
sidebar_label: "查询引擎和导入导出"
keywords: ['Canshu']
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 配置项 - 查询引擎和导入导出

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

当前主题包含以下类型的 FE 配置：
- [查询引擎](#查询引擎)
- [导入导出](#导入导出)

## 查询引擎

### clear_udf_cache_when_start

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在 BE 启动时清除所有本地缓存的用户函数库。启用后，BE 将在 UserFunctionCache 初始化时删除 UDF 目录下的 `.jar`/`.py` 文件，并在首次使用时重新下载，会增加网络流量和首次使用延迟。禁用（默认）时，BE 直接加载已有缓存文件。
- 引入版本：v4.0.0

### dictionary_speculate_min_chunk_size

- 默认值：10000
- 类型：Int
- 单位：Rows
- 是否动态：否
- 描述：StringColumnWriter 和 DictColumnWriter 用于触发字典编码推测的最小行数（chunk 大小）。如果传入列（或累积缓冲区加上传入行）大小大于等于 `dictionary_speculate_min_chunk_size`，写入器将立即运行推测并设置一种编码（DICT、PLAIN 或 BIT_SHUFFLE），而不是继续缓冲更多行。对于字符串列，推测使用 `dictionary_encoding_ratio` 来决定字典编码是否有利；对于数值/非字符串列，使用 `dictionary_encoding_ratio_for_non_string_column`。此外，如果列的 byte_size 很大（大于等于 UINT32_MAX），会强制立即进行推测以避免 `BinaryColumn<uint32_t>` 溢出。
- 引入版本：v3.2.0

### disable_storage_page_cache

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

### enable_bitmap_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Bitmap index 开启 Memory Cache。使用 Bitmap index 加速点查时，可以考虑开启。
- 引入版本：v3.1

### enable_compaction_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：控制是否为 Flat Json 数据进行 Compaction。
- 引入版本：v3.3.3

### enable_json_flat

- 默认值：false
- 类型：Boolean
- 单位：
- 是否动态：是
- 描述：是否开启 Flat JSON 特性。开启后新导入的 JSON 数据会自动打平，提升 JSON 数据查询性能。
- 引入版本：v3.3.0

### enable_lazy_dynamic_flat_json

- 默认值：True
- 类型：Bool
- 单位：
- 是否动态：是
- 描述：当查询在读过程中未命中 Flat JSON Schema 时，是否启用 Lazy Dynamic Flat JSON。当此项设置为 `true` 时，StarRocks 将把 Flat JSON 操作推迟到计算流程，而不是读取流程。
- 引入版本：v3.3.3

### enable_ordinal_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Ordinal index 开启 Memory Cache。Ordinal index 是行号到数据 page position 的映射，可以加速 Scan。
- 引入版本：-

### enable_string_prefix_zonemap

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为字符串（CHAR/VARCHAR）列启用基于前缀的 Zonemap 索引。对于非键列，最小值/最大值会截断到由 `string_prefix_zonemap_prefix_len` 配置的前缀长度。
- 引入版本：-

### enable_zonemap_index_memory_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Zonemap index 开启 Memory Cache。使用 Zonemap index 加速 Scan 时，可以考虑开启。
- 引入版本：-

### exchg_node_buffer_size_bytes

- 默认值：10485760
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Exchange 算子中，单个查询在接收端的 Buffer 容量。这是一个软限制，如果数据的发送速度过快，接收端会触发反压来限制发送速度。
- 引入版本：-

### exec_state_report_max_threads

- 默认值：2
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：exec-state-report 线程池的最大线程数。该线程池由 `ExecStateReporter` 用于将普通优先级的执行状态报告（如 Fragment 完成状态、错误状态等）从 BE 异步地通过 RPC 上报给 FE。启动时实际使用的线程数为 `max(1, exec_state_report_max_threads)`。运行时修改此配置会触发对所有 Executor Set（共享和独占）中线程池调用 `update_max_threads`。该线程池的任务队列大小固定为 1000，当所有线程繁忙且队列已满时，新的上报任务将被静默丢弃。高优先级线程池由 `priority_exec_state_report_max_threads` 控制。若在高并发查询场景下观察到执行状态上报延迟或丢失，可适当增大此值。
- 引入版本：v4.1.0, v4.0.8, v3.5.15

### file_descriptor_cache_capacity

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：文件描述符缓存的容量。
- 引入版本：-

### flamegraph_tool_dir

- 默认值：`${STARROCKS_HOME}/bin/flamegraph`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：火焰图工具的目录，该目录应包含 pprof、stackcollapse-go.pl 和 flamegraph.pl 脚本，用于从性能分析数据生成火焰图。
- 引入版本：-

### fragment_pool_queue_size

- 默认值：2048
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单 BE 节点上能够处理的查询请求上限。
- 引入版本：-

### fragment_pool_thread_num_max

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最大查询线程数。
- 引入版本：-

### fragment_pool_thread_num_min

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：最小查询线程数。
- 引入版本：-

### hdfs_client_enable_hedged_read

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Hedged Read 功能。`true` 表示开启，`false` 表示不开启。
- 引入版本：v3.0

### hdfs_client_hedged_read_threadpool_size

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HDFS 客户端侧 Hedged Read 线程池的大小，即 HDFS 客户端侧允许有多少个线程用于服务 Hedged Read。该参数对应 HDFS 集群配置文件 **hdfs-site.xml** 中的 `dfs.client.hedged.read.threadpool.size` 参数。
- 引入版本：v3.0

### hdfs_client_hedged_read_threshold_millis

- 默认值：2500
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：发起 Hedged Read 请求前需要等待多少毫秒。例如，假设该参数设置为 `30`，那么如果一个 Read 任务未能在 30 毫秒内返回结果，则 HDFS 客户端会立即发起一个 Hedged Read，从目标数据块的副本上读取数据。该参数对应 HDFS 集群配置文件 **hdfs-site.xml** 中的 `dfs.client.hedged.read.threshold.millis` 参数。
- 引入版本：v3.0

### io_coalesce_adaptive_lazy_active

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：根据谓词选择度，自适应决定是否将谓词列 IO 和非谓词列 IO 进行合并。
- 引入版本：v3.2

### jit_lru_cache_size

- 默认值：0
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：JIT 编译的 LRU 缓存大小。如果设置为大于 0，则表示实际的缓存大小。如果设置为小于或等于 0，系统将自适应设置缓存大小，使用的公式为 `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` （节点的 `mem_limit` 必须大于或等于 16 GB）。
- 引入版本：-

### json_flat_column_max

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，最多提取的子列数量。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

### json_flat_create_zonemap

- 默认值：true
- 类型：Boolean
- 单位：
- 是否动态：是
- 描述：是否为打平后的 JSON 子列创建 Zonemap。仅当 `enable_json_flat` 为 `true` 时生效。
- 引入版本：-

### json_flat_null_factor

- 默认值：0.3
- 类型：Double
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，提取列的 NULL 值占比阈值，高于该比例不对该列进行提取，默认为 0.3。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

### json_flat_sparsity_factor

- 默认值：0.3
- 类型：Double
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，同名列的占比阈值，当同名列占比低于该值时不进行提取，默认为 0.9。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

### lake_tablet_ignore_invalid_delete_predicate

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制是否忽略 tablet rowset 元数据中可能由逻辑删除在列名重命名后引入到重复键（duplicate key）表中的无效 delete predicates 的布尔值。
- 引入版本：v4.0

### late_materialization_ratio

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：控制 SegmentIterator 延迟物化策略的整数比率，取值范围 [0-1000]。`0` 禁用延迟物化；`1000` 强制所有读取使用延迟物化；1–999 时根据谓词过滤率动态决策，值越大越倾向使用延迟物化。当 Segment 包含复杂 Metric 类型时，改用 `metric_late_materialization_ratio`。启用 `lake_io_opts.cache_file_only` 时延迟物化自动禁用。
- 引入版本：v3.2.0

### max_hdfs_file_handle

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多可以打开的 HDFS 文件描述符数量。
- 引入版本：-

### max_memory_sink_batch_count

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Scan Cache 的最大缓存批次数量。
- 引入版本：-

### max_pushdown_conditions_per_column

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单列上允许下推的最大谓词数量，如果超出数量限制，谓词不会下推到存储层。
- 引入版本：-

### max_scan_key_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询最多拆分的 Scan Key 数目。
- 引入版本：-

### metric_late_materialization_ratio

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：当读取包含复杂 Metric 列时，控制延迟物化行访问策略的比率，取值范围 [0-1000]。`0` 禁用延迟物化；`1000` 强制所有适用读取使用延迟物化；1–999 时根据选择率动态决策。存在复杂 Metric 类型时，此参数优先于 `late_materialization_ratio`。`cache_file_only` I/O 模式下延迟物化始终禁用。
- 引入版本：v3.2.0

### min_file_descriptor_number

- 默认值：60000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 进程中文件描述符的最小数量。
- 引入版本：-

### object_storage_connect_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：对象存储 Socket 连接的超时时间。`-1` 表示使用 SDK 中的默认时间。
- 引入版本：v3.0.9

### object_storage_request_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：对象存储 HTTP 连接的超时时间。`-1` 表示使用 SDK 中的默认时间。
- 引入版本：v3.0.9

### parquet_late_materialization_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用延迟物化优化 Parquet 读性能。
- 引入版本：-

### parquet_page_index_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Parquet 文件的 Bloom Filter 以提高性能。`true` 表示启用 Bloom Filter，`false` 表示禁用。还可以使用系统变量 `enable_parquet_reader_bloom_filter` 在 Session 级别上控制这一行为。Parquet 中的 Bloom Filter 是在**每个行组的列级维护的**。如果 Parquet 文件包含某些列的 Bloom Filter，查询就可以使用这些列上的谓词来有效地跳过行组。
- 引入版本：v3.5

### parquet_reader_bloom_filter_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制是否启用 Parquet 文件的布隆过滤器以提升性能的布尔值。`true` 表示启用布隆过滤器，`false` 表示禁用。也可以通过会话级别的系统变量 `enable_parquet_reader_bloom_filter` 控制此行为。Parquet 中的布隆过滤器是按“每个 row group 的列级别”维护的。如果 Parquet 文件为某些列维护了布隆过滤器，则对这些列的谓词可以高效地跳过不相关的 row group。
- 引入版本：v3.5

### path_gc_check_step

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：：单次连续 Scan 最大的文件数量。
- 引入版本：-

### path_gc_check_step_interval_ms

- 默认值：10
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：多次连续 Scan 文件间隔时间。
- 引入版本：-

### path_scan_interval_second

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：GC 线程清理过期数据的间隔时间。
- 引入版本：-

### pipeline_connector_scan_thread_num_per_cpu

- 默认值：8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：BE 节点中每个 CPU 核心分配给 Pipeline Connector 的扫描线程数量。自 v3.1.7 起变为动态参数。
- 引入版本：-

### pipeline_poller_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：当该值大于 `0` 时，则在轮询器中，如果某个 Driver 的单次调度时间超过了 `pipeline_poller_timeout_guard_ms` 的时间，则会打印该 Driver 以及 Operator 信息。
- 引入版本：-

### pipeline_prepare_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎在线程池中执行 PREPARE Fragment 的队列长度。
- 引入版本：-

### pipeline_prepare_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎准备片段线程池中的线程数。`0` 表示等于系统 VCPU 数量。
- 引入版本：-

### priority_queue_remaining_tasks_increased_frequency

- 默认值：512
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BlockingPriorityQueue 为避免任务饥饿提升剩余任务优先级的频率阈值（按获取次数计数）。累计弹出次数超过该值就会整体提升队列元素优先级并重建堆；值越小提升越频繁，减少饥饿但增加 CPU 开销。
- 引入版本：v3.2.0

### pipeline_prepare_timeout_guard_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：当该值大于 `0` 时，如果 PREPARE 过程中 Plan Fragment 超过 `pipeline_prepare_timeout_guard_ms` 的时间，则会打印 Plan Fragment 的堆栈跟踪。
- 引入版本：-

### pipeline_scan_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pipeline 执行引擎扫描线程池任务队列的最大队列长度。
- 引入版本：-

### enable_lock_free_scan_task_queue

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制 `ScanExecutor` 是否使用 `LockFreeWorkGroupScanTaskQueue` 来调度 OLAP scan 和 connector scan 任务。开启后，扫描任务会通过“每个 workgroup 的无锁队列 + workgroup 级协调器”进行调度，相比原先基于互斥锁的 `WorkGroupScanTaskQueue` 可以降低竞争开销。排障或回滚时，可将该参数设置为 `false` 以切回旧的 scan task queue 实现。
- 引入版本：v4.1.0

### pk_index_parallel_get_threadpool_size

- 默认值：1048576
- 类型：Int
- 单位：Tasks
- 是否动态：是
- 描述：设置用于 shared-data（cloud-native / lake）模式下 PK 索引并行获取操作的 "cloud_native_pk_index_get" 线程池的最大队列大小（待处理任务数量）。该池的实际线程数由 `pk_index_parallel_get_threadpool_max_threads` 控制；此设置仅限制可排队等待执行的任务数量。非常大的默认值（2^20）实际上使队列近似无界；降低此值可以防止排队任务导致的内存过度增长，但在队列已满时可能导致任务提交阻塞或失败。应根据工作负载并发性和内存约束与 `pk_index_parallel_get_threadpool_max_threads` 一起调优。
- 引入版本：-

### priority_exec_state_report_max_threads

- 默认值：2
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：高优先级 exec-state-report 线程池的最大线程数。该线程池由 `ExecStateReporter` 用于将高优先级执行状态报告（如紧急 Fragment 失败）从 BE 异步地通过 RPC 上报给 FE。与普通线程池不同，该线程池的任务队列无上限。启动时实际使用的线程数为 `max(1, priority_exec_state_report_max_threads)`。运行时修改此配置会触发对所有 Executor Set（共享和独占）中优先级线程池调用 `update_max_threads`。普通线程池由 `exec_state_report_max_threads` 控制。
- 引入版本：v4.1.0, v4.0.8, v3.5.15

### query_cache_capacity

- 默认值：536870912
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：指定 Query Cache 的大小。默认为 512 MB。最小不低于 4 MB。如果当前的 BE 内存容量无法满足您期望的 Query Cache 大小，可以增加 BE 的内存容量，然后再设置合理的 Query Cache 大小。每个 BE 都有自己私有的 Query Cache 存储空间，BE 只 Populate 或 Probe 自己本地的 Query Cache 存储空间。
- 引入版本：-

### query_pool_spill_mem_limit_threshold

- 默认值：1.0
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：如果开启自动落盘功能, 当所有查询使用的内存超过 `query_pool memory limit * query_pool_spill_mem_limit_threshold` 时，系统触发中间结果落盘。
- 引入版本：3.2.7

### query_scratch_dirs

- 默认值：`${STARROCKS_HOME}`
- 类型：string
- 单位：-
- 是否动态：否
- 描述：用于查询执行在发生中间数据溢写（例如外部排序、哈希连接和其他算子）时的可写 scratch 目录的逗号分隔列表。指定一个或多个以 `;` 分隔的路径（例如 `/mnt/ssd1/tmp;/mnt/ssd2/tmp`）。这些目录应当对 BE 进程可访问且可写，并具有足够的可用空间；StarRocks 会在它们之间选择以分散溢写 I/O。更改此项需要重启才能生效。如果目录丢失、不可写或已满，溢写可能失败或导致查询性能下降。
- 引入版本：v3.2.0

### result_buffer_cancelled_interval_time

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：BufferControlBlock 释放数据的等待时间。
- 引入版本：-

### scan_context_gc_interval_min

- 默认值：5
- 类型：Int
- 单位：Minutes
- 是否动态：是
- 描述：Scan Context 的清理间隔。
- 引入版本：-

### scanner_row_num

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个扫描线程单次执行最多返回的数据行数。
- 引入版本：-

### scanner_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存储引擎支持的扫描任务数。
- 引入版本：-

### scanner_thread_pool_thread_num

- 默认值：48
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存储引擎并发扫描磁盘的线程数，统一管理在线程池中。
- 引入版本：-

### max_hdfs_scanner_num

- 默认值：50
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：限制 ConnectorScanNode 可同时运行的远端扫描器（HDFS/对象存储等）数量上限。启动时会用此值裁剪估算并发，运行时调度 pending scanner 也会受其限制，用于控制线程、内存和文件句柄压力。
- 引入版本：v3.2.0

### string_prefix_zonemap_prefix_len

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：启用 `enable_string_prefix_zonemap` 时用于字符串 Zonemap 最小值/最大值的前缀长度。
- 引入版本：-

### udf_thread_pool_size

- 默认值：1
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：设置在 ExecEnv 中创建的 UDF 调用 PriorityThreadPool 的大小（用于执行用户自定义函数/UDF 相关任务）。该值既作为线程池的线程数，也在构造线程池时作为队列容量（PriorityThreadPool("udf", thread_num, queue_size)）。增大该值可以允许更多并发的 UDF 执行；保持较小可避免过度的 CPU 和内存争用。
- 引入版本：v3.2.0

### update_memory_limit_percent

- 默认值：60
- 类型：Int
- 单位：Percent
- 是否动态：否
- 描述：BE 进程内存中为更新相关内存和缓存保留的比例。在启动期间，`GlobalEnv` 将更新的 `MemTracker` 计算为 process_mem_limit * clamp(update_memory_limit_percent, 0, 100) / 100。`UpdateManager` 也使用该百分比来确定其 primary-index/index-cache 的容量（index cache capacity = GlobalEnv::process_mem_limit * update_memory_limit_percent / 100）。HTTP 配置更新逻辑会注册一个回调，在配置更改时调用 update managers 的 `update_primary_index_memory_limit`，因此配置更改会应用到更新子系统。增加此值会为更新/primary-index 路径分配更多内存（减少其他内存池可用内存）；减少它会降低更新内存和缓存容量。值会被限定在 0–100 范围内。
- 引入版本：v3.2.0

### vector_chunk_size

- 默认值：4096
- 类型：Int
- 单位：Rows
- 是否动态：否
- 描述：向量化执行中每个 Chunk（批次）的行数，贯穿执行引擎和存储层。影响算子吞吐量、内存占用、Spill/Sort 缓冲区大小及 I/O 启发式策略。增大可提升宽表/CPU 密集型场景的效率，但会提高峰值内存并增加小结果集查询的延迟。除非分析显示批大小是瓶颈，否则建议保持默认值。
- 引入版本：v3.2.0

## 导入导出

### clear_transaction_task_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：清理事务的线程数。
- 引入版本：-

### column_mode_partial_update_insert_batch_size

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：列模式部分更新中处理插入行时的批次大小。如果设置为 `0` 或负数，将会被限制为 `1` 以避免无限循环。该参数控制每次批量处理新插入行的数量，较大的值可以提高写入性能但会占用更多内存。
- 引入版本：v3.5.10, v4.0.2

### partial_update_memory_limit_per_worker

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：部分更新（partial update）每个工作线程的内存上限，用于限制单个 worker 在处理 partial update 时的内存占用。
### enable_load_spill_parallel_merge

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用单个 Tablet 内部的并行 Spill Merge。启用后可以提高导入过程中 Spill Merge 的性能。
- 引入版本：-

### enable_parallel_memtable_finalize

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在存算分离（Lake）表导入数据时启用并行 MemTable Finalize。启用后，MemTable 的 Finalize 操作（排序/聚合）将从写入线程移至 Flush 线程执行，使写入线程可以继续向新的 MemTable 插入数据，同时前一个 MemTable 正在并行进行 Finalize 和 Flush。这可以通过重叠 CPU 密集型的 Finalize 操作与 I/O 密集型的 Flush 操作来显著提高导入吞吐量。注意：当需要填充自增列时，此优化会自动禁用，因为自增 ID 的分配必须在 MemTable 提交 Flush 之前完成。
- 引入版本：-

### allow_list_object_for_random_bucketing_on_cache_miss

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制 random bucketing 大小检查在 Lake metadata 缓存未命中时是否允许回退到对象存储 LIST。`true` 表示回退到 LIST 元数据文件计算 base size（历史行为、估算更准确）；`false` 表示跳过 LIST，直接使用 `base_size = 0`，可减少 LIST object 请求，但因大小估算精度下降，可能使 immutable 标记稍晚触发。
- 引入版本：4.1.0, 4.0.7, 3.5.15

### enable_stream_load_verbose_log

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在日志中记录 Stream Load 的 HTTP 请求和响应信息。`true` 表示启用，`false` 表示不启用。
- 引入版本：v2.5.17, v3.0.9, v3.1.6, v3.2.1

### flush_thread_num_per_store

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Store 用以 Flush MemTable 的线程数。
- 引入版本：-

### lake_flush_thread_num_per_store

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：在存算分离集群中，每个 Store 用以 Flush MemTable 的线程数。当该参数被设置为 `0` 时，系统使用 CPU 核数的两倍。
当该参数被设置为小于 `0` 时，系统使用该参数的绝对值与 CPU 核数的乘积。
- 引入版本：3.1.12, 3.2.7

### load_data_reserve_hours

- 默认值：4
- 类型：Int
- 单位：Hours
- 是否动态：否
- 描述：小批量导入生成的文件保留的时长。
- 引入版本：-

### load_error_log_reserve_hours

- 默认值：48
- 类型：Int
- 单位：Hours
- 是否动态：是
- 描述：导入数据信息保留的时长。
- 引入版本：-

### load_process_max_memory_limit_bytes

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：单节点上所有的导入线程占据的内存上限。
- 引入版本：-

### load_spill_memory_usage_per_merge

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Spill Merge 期间每次 Merge 操作的最大内存使用量。默认为 1GB (1073741824 字节)。该参数用于控制导入过程中 Spill Merge 时单个 Merge 任务的内存占用，避免内存使用过高。
- 引入版本：-

### max_consumer_num_per_group

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Routine load 中，每个 Consumer Group 内最大的 Consumer 数量。
- 引入版本：-

### max_pulsar_consumer_num_per_group

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Pulsar Routine Load 每个 Consumer Group 允许的最大 Consumer 数量上限。
- 引入版本：-

### max_runnings_transactions_per_txn_map

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个分区内部同时运行的最大事务数量。
- 引入版本：-

### number_tablet_writer_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：导入用的 tablet writer 线程数, 用于 Stream Load、Broker Load、Insert 等。当参数设置为小于等于 0 时，系统使用 CPU 核数的二分之一，最小为 16。当参数设置为大于 0 时，系统使用该值。自 v3.1.7 起变为动态参数。
- 引入版本：-

### push_worker_count_high_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 HIGH 优先级任务。
- 引入版本：-

### push_worker_count_normal_priority

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入线程数，用于处理 NORMAL 优先级任务。
- 引入版本：-

### streaming_load_max_batch_size_mb

- 默认值：100
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个 JSON 文件大小的上限。
- 引入版本：-

### streaming_load_max_mb

- 默认值：102400
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：流式导入单个文件大小的上限。自 3.0 版本起，默认值由 10240 变为 102400。
- 引入版本：-

### streaming_load_rpc_max_alive_time_sec

- 默认值：1200
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：Stream Load 的 RPC 超时时长。
- 引入版本：-

### transaction_publish_version_thread_pool_num_min

- 默认值：0
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：Publish Version 线程池的最小线程数，空闲时可收缩到该值。0 表示不设固定下限。
- 引入版本：-

### transaction_publish_version_thread_pool_idle_time_ms

- 默认值：60000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：线程被 Publish Version 线程池回收前的 Idle 时间。
- 引入版本：-

### transaction_publish_version_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：生效版本的最大线程数。当该参数被设置为小于或等于 `0` 时，系统默认使用当前节点的 CPU 核数，以避免因使用固定值而导致在导入并行较高时线程资源不足。自 2.5 版本起，默认值由 `8` 变更为 `0`。
- 引入版本：-

### use_mmap_allocate_chunk

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用匿名 mmap 方式分配 chunk（MAP_ANONYMOUS | MAP_PRIVATE）。开启会产生大量 VM 映射，需提高 `vm.max_map_count` 并设置较大的 `chunk_reserved_bytes_limit`，否则可能因频繁映射/解除映射造成性能问题。
- 引入版本：v3.2.0

### write_buffer_size

- 默认值：104857600
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：MemTable 在内存中的 Buffer 大小，超过这个限制会触发 Flush。
- 引入版本：-

### broker_write_timeout_seconds

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：后端与 Broker 交互的写/IO RPC 超时时长（秒），内部会换算为毫秒并作为默认超时传给 BrokerFileSystem、BrokerServiceConnection 等。网络或 Broker 较慢、大文件传输时可适当调大。
- 引入版本：v3.2.0

### enable_load_diagnose

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：导入 RPC 超时时是否自动触发诊断（收集 profile/堆栈），诊断 RPC 行为由 `load_diagnose_rpc_timeout_profile_threshold_ms`、`load_diagnose_rpc_timeout_stack_trace_threshold_ms` 和 `load_diagnose_send_rpc_timeout_ms` 控制。可能带来额外 RPC 与开销，敏感场景可关闭。
- 引入版本：v3.5.0

### enable_load_segment_parallel

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启导入写入的分段并行处理。开启可提升吞吐，但会增加资源占用。
- 引入版本：-

### enable_load_channel_rpc_async

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：启用后，load-channel Open 类型的 RPC（例如 PTabletWriterOpen）的处理会从 BRPC worker 转移到一个专用的线程池：请求处理器会创建一个 ChannelOpenTask 并将其提交到内部 `_async_rpc_pool`，而不是内联执行 `LoadChannelMgr::_open`。这样可以减少 BRPC 线程内的工作量和阻塞，并允许通过 `load_channel_rpc_thread_pool_num` 和 `load_channel_rpc_thread_pool_queue_size` 调整并发。如果线程池提交失败（池已满或已关闭），该请求会被取消并返回错误状态。该线程池会在 `LoadChannelMgr::close()` 时关闭，因此在启用该功能时需要考虑容量和生命周期，以避免请求被拒绝或处理延迟。
- 引入版本：v3.5.0

### load_channel_rpc_thread_pool_num

- 默认值：-1
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：异步处理 load channel Open RPC 的线程池大小。过小可能导致提交失败，过大增加线程开销。
- 引入版本：v3.5.0

### load_channel_rpc_thread_pool_queue_size

- 默认值：1024000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：异步 load channel Open RPC 线程池的队列长度，队列满则请求会被拒绝。
- 引入版本：v3.5.0

### enable_streaming_load_thread_pool

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制是否将 streaming load 的 scanner 提交到专用的 streaming load 线程池。当启用且查询为带有 `TLoadJobType::STREAM_LOAD` 的 LOAD 时，ConnectorScanNode 会将 scanner 任务提交到 `streaming_load_thread_pool`（该池配置为 INT32_MAX 的线程数和队列大小，即实际上是无界的）。当禁用时，scanner 使用通用的 `thread_pool` 及其 `PriorityThreadPool` 提交逻辑（优先级计算、try_offer/offer 行为）。启用可以将 streaming-load 的工作与常规查询执行隔离以减少干扰；但由于专用池实际上是无界的，在重度 streaming-load 流量下启用可能会增加并发线程数和资源使用。此选项默认开启，通常无需修改。
- 引入版本：v3.2.0

### streaming_load_thread_pool_idle_time_ms

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：streaming load 线程池中空闲线程的存活时间，超过则回收。
- 引入版本：-

### streaming_load_thread_pool_num_min

- 默认值：0
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：streaming load 线程池的最小线程数。空闲时可收缩到该值，0 表示不设固定下限。
- 引入版本：-
### es_http_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：ESScanReader 在执行 Elasticsearch Scroll 请求时，ES 网络客户端的 HTTP 连接超时时间。在网络较慢或查询量较大时可适当调大，以避免过早超时；调小则可以更快感知 ES 节点无响应。与控制 Scroll 上下文存活时间的 `es_scroll_keepalive` 配合使用。
- 引入版本：v3.2.0

### es_index_max_result_window

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：限制 StarRocks 在单个批次中从 Elasticsearch 请求的最大文档数。StarRocks 在为 ES reader 构建 `KEY_BATCH_SIZE` 时将 ES 请求批大小设置为 min(`es_index_max_result_window`, `chunk_size`)。如果 ES 请求超过 Elasticsearch 索引设置 `index.max_result_window`，Elasticsearch 会返回 HTTP 400 (Bad Request)。在扫描大型索引时调整此值，或在 Elasticsearch 端增加 ES `index.max_result_window` 以允许更大的单次请求。
- 引入版本：v3.2.0

### ignore_load_tablet_failure

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：设置为 `false` 时，任何 Tablet Header 加载失败（非 NotFound 和非 AlreadyExist 的错误）均视为致命错误，BE 进程将调用 LOG(FATAL) 停止。设置为 `true` 时，BE 在启动时跳过加载失败的 Tablet，并继续加载其他 Tablet。注意：此参数不抑制 RocksDB 元数据扫描本身的致命错误，这类错误始终会导致进程退出。
- 引入版本：v3.2.0

### load_channel_abort_clean_up_delay_seconds

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：控制 LoadChannelMgr 在将已中止的 load channel 的 load ID 从 `_aborted_load_channels` 中移除之前保留这些 load ID 的时长（以秒为单位）。当 load 作业被取消或失败时，load ID 会被记录下来以便立即拒绝任何迟到到达的 load RPC；一旦延迟到期，该条目将在周期性后台清理（最小清理间隔为 60 秒）时被清除。将延迟设置得过低会有在中止后仍接受零散 RPC 的风险，而设置得过高则可能比必要时间更长地保留状态并消耗资源。根据对迟到请求拒绝的正确性与中止 load 的资源保留之间的权衡进行调优。
- 引入版本：v3.5.11, v4.0.4

### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- 默认值：600000
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：用于决定何时为长时间运行的 load RPC 请求远程堆栈跟踪的阈值（毫秒）。当 load RPC 超时并返回超时错误且实际的 RPC 超时时间（_rpc_timeout_ms）超过此值时，`OlapTableSink`/`NodeChannel` 将在发往目标 BE 的 `load_diagnose` RPC 中包含 `stack_trace=true`，以便 BE 返回用于调试的堆栈跟踪。`LocalTabletsChannel::SecondaryReplicasWaiter` 也会在等待 secondary replicas 超过该间隔时触发从 primary 的尽力堆栈跟踪诊断。此行为依赖于 `enable_load_diagnose` 并使用 `load_diagnose_send_rpc_timeout_ms` 作为诊断 RPC 的超时；性能分析由 `load_diagnose_rpc_timeout_profile_threshold_ms` 单独控制。降低此值会更积极地请求堆栈跟踪。
- 引入版本：v3.5.0

### load_diagnose_rpc_timeout_profile_threshold_ms

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：当 load RPC 超时且耗时超过该阈值时，在诊断 RPC 中收集 profile（performance profile）。依赖 `enable_load_diagnose`，诊断 RPC 超时由 `load_diagnose_send_rpc_timeout_ms` 控制。
- 引入版本：v3.5.0

### load_diagnose_send_rpc_timeout_ms

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：诊断 RPC（收集 profile/堆栈）自身的超时时长。
- 引入版本：v3.5.0

### load_fp_brpc_timeout_ms

- 默认值：-1
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：在触发 `node_channel_set_brpc_timeout` fail point 时，覆盖 OlapTableSink 所使用的每通道 brpc RPC 超时。如果设置为正值，NodeChannel 会将其内部 `_rpc_timeout_ms` 设置为该值（毫秒），使 open/add-chunk/cancel RPC 使用更短的超时，从而模拟产生 “[E1008]Reached timeout” 错误的 brpc 超时。默认值（`-1`）禁用覆盖。更改此值用于测试和故障注入；较小的值可能导致虚假超时并触发 load 诊断（参见 `enable_load_diagnose`、`load_diagnose_rpc_timeout_profile_threshold_ms`、`load_diagnose_rpc_timeout_stack_trace_threshold_ms` 和 `load_diagnose_send_rpc_timeout_ms`）。
- 引入版本：v3.5.0

### load_fp_tablets_channel_add_chunk_block_ms

- 默认值：-1
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：导入 failpoint，“TabletsChannel::add_chunk” 人为阻塞的毫秒数，用于调试。
- 引入版本：v3.4.3, v3.5.0

### load_segment_thread_pool_num_max

- 默认值：128
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：导入 segment 处理线程池的最大线程数。调大提高并行度也会增加资源消耗。
- 引入版本：-

### load_segment_thread_pool_queue_size

- 默认值：10240
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导入 segment 线程池的队列长度，队列满时新的 segment 任务会被拒绝。
- 引入版本：-

### pull_load_task_dir

- 默认值：`${STARROCKS_HOME}/var/pull_load`
- 类型：string
- 单位：-
- 是否动态：否
- 描述：BE 存储 “pull load” 任务的数据和工作文件（下载的源文件、任务状态、临时输出等）的文件系统路径。该目录必须对 BE 进程可写，并且具有足够的磁盘空间以容纳下载的内容。
- 引入版本：v3.2.0

### routine_load_pulsar_timeout_second

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：BE 在请求未提供显式超时时使用的 Pulsar 相关 routine load 操作的默认超时（秒）。具体地，`PInternalServiceImplBase::get_pulsar_info` 将该值乘以 1000，形成以毫秒为单位的超时值，传递给用于获取 Pulsar 分区元数据和 backlog 的 routine load 任务执行器方法。增大该值可在 Pulsar 响应较慢时减少超时失败，但会延长故障检测时间；减小该值可在 broker 响应慢时更快失败。与用于 Kafka 的 `routine_load_kafka_timeout_second` 类似。
- 引入版本：v3.2.0

### routine_load_kafka_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：Routine Load Kafka 操作的默认超时时长（未显式指定时使用）。
- 引入版本：-
