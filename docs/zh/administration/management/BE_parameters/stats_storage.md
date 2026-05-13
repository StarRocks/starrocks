---
displayed_sidebar: docs
sidebar_label: "统计报告和存储"
keywords: ['Canshu']
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE 配置项 - 统计报告和存储

<BEConfigMethod />

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## 配置 BE 参数

<PostBEConfig />

<StaticBEConfigNote />

---

当前主题包含以下类型的 BE 配置：
- [统计报告](#统计报告)
- [存储](#存储)

## 统计报告

### enable_metric_calculator

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用周期性指标聚合线程（metrics calculator）。控制指标计算周期任务的启停；系统级指标初始化由 `enable_system_metrics` 控制。
- 引入版本：-

### enable_system_metrics

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：为 true 时，StarRocks 在启动期间初始化系统级监控：它会根据配置的存储路径发现磁盘设备并枚举网络接口，然后将这些信息传入 metrics 子系统以启用磁盘 I/O、网络流量和内存相关的系统指标采集。如果设备或接口发现失败，初始化会记录警告并中止系统指标的设置。该标志仅控制是否初始化系统指标；周期性指标聚合线程由 `enable_metric_calculator` 单独控制，JVM 指标初始化由 `enable_jvm_metrics` 控制。更改此值需要重启。
- 引入版本：v3.2.0

### profile_report_interval

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：ProfileReportWorker 用于（1）决定何时上报 LOAD 查询的每个 fragment 的 profile 信息以及（2）在上报周期之间休眠的间隔（秒）。该 worker 使用 (profile_report_interval * 1000) ms 将当前时间与每个任务的 last_report_time 进行比较，以确定是否需要对非 pipeline 和 pipeline 的 load 任务重新上报 profile。在每次循环中，worker 会读取当前值（运行时可变）；如果配置值小于等于 0，worker 会强制将其设为 1 并发出警告。修改此值会影响下一次的上报判断和休眠时长。
- 引入版本：v3.2.0

### report_disk_state_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报磁盘状态的间隔。汇报各个磁盘的状态，以及其中数据量等。
- 引入版本：-

### report_resource_usage_interval_ms

- 默认值：1000
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：由 BE Agent 定期向 FE 发送资源使用报告的间隔（毫秒）。较低的值能提高报告的及时性，但会增加 CPU、网络和 FE 的负载；较高的值可降低开销但会使资源信息不够实时。上报会更新相关指标（`report_resource_usage_requests_total`、`report_resource_usage_requests_failed`）。请根据集群规模和 FE 负载调整。
- 引入版本：v3.2.0

### report_tablet_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报 Tablet 的间隔。汇报所有的 Tablet 的最新版本。
- 引入版本：-

### report_task_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报单个任务的间隔。建表，删除表，导入，Schema Change 都可以被认定是任务。
- 引入版本：-

### report_workgroup_interval_seconds

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：汇报 Workgroup 的间隔。汇报所有 Workgroup 的最新版本。
- 引入版本：-

## 存储

<EditionSpecificBEItem />

### alter_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：进行 Schema Change 的线程数。自 2.5 版本起，该参数由静态变为动态。
- 引入版本：-

### automatic_partition_thread_pool_thread_num

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：自动分区线程池的线程数。队列长度自动设置为线程数的 10 倍。
- 引入版本：-

### create_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：处理创建 Tablet 任务（TTaskType::CREATE）的线程池最大线程数。启动时用于初始化 AgentServer 的 CREATE 线程池，运行时修改会更新线程池上限。调大可提升批量建表/分区时的并发，调小可降低资源占用。
- 引入版本：v3.2.0

### avro_ignore_union_type_tag

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在 Avro Union 数据结构序列化成 JSON 格式时，去除 Union 结构的类型标签信息。
- 引入版本：v3.3.7, v3.4

### base_compaction_check_interval_seconds

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Base Compaction 线程轮询的间隔。
- 引入版本：-

### base_compaction_interval_seconds_since_last_operation

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：上一轮 Base Compaction 距今的间隔。此项为 Base Compaction 触发条件之一。
- 引入版本：-

### base_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个磁盘 Base Compaction 线程的数目。
- 引入版本：-

### base_cumulative_delta_ratio

- 默认值：0.3
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：Cumulative 文件大小达到 Base 文件的比例。此项为 Base Compaction 触发条件之一。
- 引入版本：-

### chaos_test_enable_random_compaction_strategy

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当此项设置为 `true` 时，TabletUpdates::compaction() 将使用为混沌测试准备的随机压缩策略（compaction_random）。此标志强制在平板的压缩选择中采用非确定性/随机策略，而不是正常策略（例如 size-tiered compaction），并在压缩选择时具有优先权。仅用于可控的测试场景：启用后可能导致不可预测的压缩顺序、增加的 I/O/CPU 和测试不稳定性。请勿在生产环境中启用；仅用于故障注入或混沌测试场景。
- 引入版本：v3.3.12, 3.4.2, 3.5.0, 4.0.0

### check_consistency_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：计算 Tablet 的校验和（checksum）的线程数。
- 引入版本：-

### clear_expired_replication_snapshots_interval_seconds

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：系统清除异常同步遗留的过期快照的时间间隔。
- 引入版本：v3.3.5

### compact_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 引入版本：v3.0.0

### compaction_memory_limit_per_worker

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：每个 Compaction 线程允许的最大内存大小。
- 引入版本：-

### compaction_max_memory_limit_percent

- 默认值：20
- 类型：Int
- 单位：Percent
- 是否动态：否
- 描述：Compaction 内存上限占进程内存的百分比，最终上限取 `min(compaction_max_memory_limit, process_mem_limit * percent / 100)`；`process_mem_limit` 为 -1 时视为不限制。
- 引入版本：-

### compaction_max_memory_limit

- 默认值：-1
- 类型：Long
- 单位：Bytes
- 是否动态：否
- 描述：Compaction 内存的字节上限。与 `compaction_max_memory_limit_percent` 共同决定 Compaction 可用的内存预算，取二者计算结果的较小值；-1 表示不单独限制（仍受百分比限制约束）。
- 引入版本：-

### compaction_trace_threshold

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：单次 Compaction 打印 Trace 的时间阈值，如果单次 Compaction 时间超过该阈值就打印 Trace。
- 引入版本：-

### cumulative_compaction_check_interval_seconds

- 默认值：1
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Cumulative Compaction 线程轮询的间隔。
- 引入版本：-

### cumulative_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个磁盘 Cumulative Compaction 线程的数目。
- 引入版本：-

### data_page_size

- 默认值：65536
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：构建列数据与索引页时使用的目标未压缩 Page 大小（以字节为单位）。该值会被 Page Builder 用来决定何时完成一个 Page 以及预留多少内存。值为 0 会在构建器中禁用 Page 大小限制。更改此值会影响 Page 数量、元数据开销、内存预留以及 I/O/压缩的权衡（Page 越小 → Page 数和元数据越多；Page 越大 → Page 更少，压缩比更大，但内存峰值可能更大）。
- 引入版本：v3.2.4

### default_num_rows_per_column_file_block

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Row Block 最多存放的行数。
- 引入版本：-

### delete_worker_count_high_priority

- 默认值：1
- 类型：Int
- 单位：Threads
- 是否动态：否
- 描述：在 DeleteTaskWorkerPool 中被分配为高优先级删除线程的工作线程数。启动时 AgentServer 使用 total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority 创建删除线程池；前 delete_worker_count_high_priority 个线程被标记为专门尝试弹出 TPriority::HIGH 任务（它们轮询高优先级删除任务，若无可用任务则睡眠/循环）。增加此值可以提高高优先级删除请求的并发性；减少它会降低专用容量并可能增加高优先级删除的延迟。更改需要重启进程才能生效。
- 引入版本：v3.2.0

### dictionary_encoding_ratio

- 默认值：0.7
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：字符串列在推测编码阶段决定是否使用字典编码的阈值。计算 `max_card = row_count * dictionary_encoding_ratio`，当去重基数超过该值时改用 PLAIN_ENCODING，否则使用 DICT_ENCODING。仅在行数超过 `dictionary_speculate_min_chunk_size` 且大于字典最小行数时生效。值越高越偏向字典编码，1.0 基本强制字典。
- 引入版本：v3.2.0

### dictionary_encoding_ratio_for_non_string_column

- 默认值：0
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：非字符串列是否采用字典编码的比率阈值。大于 0 时，计算 `max_card = row_count * 该值`，只有 distinct ≤ max_card 才用 DICT_ENCODING，否则回退 BIT_SHUFFLE；0 表示禁用非字符串字典编码。与 `dictionary_encoding_ratio` 类似但作用于非字符串列。
- 引入版本：v3.3.0, v3.4.0, v3.5.0

### dictionary_page_size

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：字典页大小。控制单个字典页可容纳的字典条目数，影响写入时的内存占用与压缩效果。过大提升压缩比但占用更多内存/IO，过小则可能降低压缩收益。
- 引入版本：v3.3.0, v3.4.0, v3.5.0

### lz4_acceleration

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：LZ4 压缩加速参数，对应 LZ4_fastCompress 的 acceleration。值越大压缩越快但压缩率下降。
- 引入版本：-

### lz4_expected_compression_ratio

- 默认值：5.0
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：预期的 LZ4 压缩比，用于评估压缩收益。
- 引入版本：-

### lz4_expected_compression_speed_mbps

- 默认值：500
- 类型：Double
- 单位：MB/s
- 是否动态：否
- 描述：预期的 LZ4 压缩速度，用于估算压缩开销。
- 引入版本：-

### small_dictionary_page_size

- 默认值：262144
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：小字典页大小，用于小字典场景的页大小控制，影响内存占用与压缩效率。
- 引入版本：-

### disk_stat_monitor_interval

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘健康状态检测的间隔。
- 引入版本：-

### download_low_speed_limit_kbps

- 默认值：50
- 类型：Int
- 单位：KB/Second
- 是否动态：是
- 描述：单个 HTTP 请求的下载速率下限。如果在 `download_low_speed_time` 秒内下载速度一直低于 `download_low_speed_limit_kbps`，那么请求会被终止。
- 引入版本：-

### download_low_speed_time

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：单个 HTTP 请求持续以低于 `download_low_speed_limit_kbps` 值的速度运行时，允许运行的最长时间。在配置项中指定的时间跨度内，当一个 HTTP 请求持续以低于该值的速度运行时，该请求将被中止。
- 引入版本：-

### download_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点下载任务的最大线程数，用于恢复作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

### drop_tablet_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：删除 Tablet 的线程数。`0` 表示当前节点的 CPU 核数的一半。
- 引入版本：-

### enable_check_string_lengths

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否在导入时进行数据长度检查，以解决 VARCHAR 类型数据越界导致的 Compaction 失败问题。
- 引入版本：-

### enable_event_based_compaction_framework

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Event-based Compaction Framework。`true` 代表开启。`false` 代表关闭。开启则能够在 Tablet 数比较多或者单个 Tablet 数据量比较大的场景下大幅降低 Compaction 的开销。
- 引入版本：-

### enable_lazy_delta_column_compaction

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：启用后，Compaction 将对由部分列更新产生的 Delta 列采用“懒惰”策略：StarRocks 会避免将 Delta-column 文件立即合并回其主段文件以节省 Compaction 的 I/O。实际上，Compaction 选择代码会检查是否存在部分列更新的 Rowset 和多个候选项；如果发现以上情况且此配置项为 `true`，引擎要么停止向 Compaction 添加更多输入，要么仅合并空的 Rowset（level -1），将 Delta 列保持分离。这会减少 Compaction 期间的即时 I/O 和 CPU 开销，但以延迟合并为代价（可能产生更多段和临时存储开销）。正确性和查询语义不受影响。
- 引入版本：v3.2.3

### enable_new_load_on_memory_limit_exceeded

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在导入线程内存占用达到硬上限后，是否允许新的导入线程。`true` 表示允许新导入线程，`false` 表示拒绝新导入线程。
- 引入版本：v3.3.2

### enable_pk_index_parallel_compaction

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用存算分离集群中主键索引的并行 Compaction。
- 引入版本：-

### enable_pk_index_parallel_execution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用存算分离集群中主键索引操作的并行执行。开启后，系统会在发布操作期间使用线程池并发处理分段，显著提升大表的性能。
- 引入版本：-

### enable_pk_index_eager_build

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在导入和 Compaction 阶段即时构建 Primary Key 索引文件。开启后，系统会在数据写入时直接生成持久化的主键索引文件，提升后续查询性能。
- 引入版本：-

### enable_pk_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否为 Primary Key 表开启 Size-tiered Compaction 策略。`true` 代表开启。`false` 代表关闭。
- 引入版本：
  - 存算分离集群自 v3.2.4, v3.1.10 起生效
  - 存算一体集群自 v3.2.5, v3.1.10 起生效

### enable_rowset_verify

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否检查 Rowset 的正确性。开启后，会在 Compaction、Schema Change 后检查生成的 Rowset 的正确性。
- 引入版本：-

### enable_size_tiered_compaction_strategy

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 Size-tiered Compaction 策略 (Primary Key 表除外)。`true` 代表开启。`false` 代表关闭。
- 引入版本：-

### enable_strict_delvec_crc_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当此项设置为 `true` 后，系统会对 Delete Vector 的 crc32 进行严格检查，如果不一致，将返回失败。
- 引入版本：-

### enable_transparent_data_encryption

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：启用后，StarRocks 将为新写入的存储对象（segment 文件、delete/update 文件、rowset segments、lake SSTs、persistent index 文件等）进行磁盘加密。写入路径（RowsetWriter/SegmentWriter、lake UpdateManager/LakePersistentIndex 及相关代码路径）会从 KeyCache 请求加密信息，将 encryption_info 附加到可写文件，并将 encryption_meta 持久化到 rowset / segment / sstable 元数据中（如 segment_encryption_metas、delete/update encryption metadata）。FE 与 FE/CN 的加密标志必须匹配。如果不匹配会导致 BE 在心跳时中止（LOG(FATAL)）。此参数不可在运行时修改，必须在第一次部署集群前启用，并确保密钥管理（KEK）与 KeyCache 已在集群中正确配置并同步。
- 引入版本：v3.3.1

### enable_zero_copy_from_page_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：允许 FixedLengthColumnBase 在追加来自页缓存的对齐缓冲区时零拷贝复用底层内存（需列为空、长度按元素大小对齐且资源可拥有）。开启可减少拷贝开销、提升导入/扫描吞吐；关闭则强制拷贝以降低耦合风险。
- 引入版本：-

### file_descriptor_cache_clean_interval

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：文件描述符缓存清理的间隔，用于清理长期不用的文件描述符。
- 引入版本：-

### ignore_broken_disk

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：启动时若配置的存储路径（`storage_root_path`/`spill_local_storage_dir`）读写检查失败或解析失败，是否跳过该路径继续启动。`false`（默认）遇到坏路径会终止启动；`true` 则忽略并移除坏路径继续启动（若全部路径都坏仍会退出）。开启可能掩盖磁盘故障，需关注日志和磁盘健康。
- 引入版本：v3.2.0

### inc_rowset_expired_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：导入生效的数据，存储引擎保留的时间，用于增量克隆。
- 引入版本：-

### load_process_max_memory_hard_limit_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单节点上所有的导入线程占据内存的硬上限（比例）。当 `enable_new_load_on_memory_limit_exceeded` 设置为 `false`，并且所有导入线程的内存占用超过 `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio` 时，系统将会拒绝新的导入线程。
- 引入版本：v3.3.2

### load_process_max_memory_limit_percent

- 默认值：30
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：单节点上所有的导入线程占据内存的软上限（百分比）。
- 引入版本：-

### lz4_expected_compression_ratio

- 默认值：2.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：序列化压缩策略用于判断观察到的 LZ4 压缩是否“良好”的阈值。增大此值会提高期望的压缩比（使条件更难满足），降低则更容易使观察到的压缩被视为令人满意。根据典型数据的可压缩性进行调优。有效范围：MIN=1, MAX=65537。
- 引入版本：v3.4.1, 3.5.0, 4.0.0

### lz4_expected_compression_speed_mbps

- 默认值：600
- 类型：Double
- 单位：MB/s
- 是否动态：是
- 描述：自适应压缩策略中用于表示期望 LZ4 压缩吞吐量的值，单位为 MB/s。反馈例程会计算 `reward_ratio = (observed_compression_ratio / lz4_expected_compression_ratio) * (observed_speed / lz4_expected_compression_speed_mbps)`。当 reward_ratio 大于 1.0 时增加正计数器（alpha），否则增加负计数器（beta）；这会影响未来数据是否被压缩。请根据你的硬件上典型的 LZ4 吞吐量调整此值——提高它会使策略更难将一次运行判定为“良好”（需要更高的观测速度），降低则更容易被判定为良好。必须为正的有限数。
- 引入版本：v3.4.1, 3.5.0, 4.0.0

### make_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点快照任务的最大线程数。
- 引入版本：-

### make_snapshot_rpc_timeout_ms

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：make snapshot 相关 RPC 的超时时长（毫秒），用于快照任务的远端调用。
- 引入版本：-

### manual_compaction_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Number of threads for Manual Compaction.
- 引入版本：-

### max_base_compaction_num_singleton_deltas

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Base Compaction 合并的最大 Segment 数。
- 引入版本：-

### max_compaction_candidate_num

- 默认值：40960
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 候选 Tablet 的最大数量。太大会导致内存占用和 CPU 负载高。
- 引入版本：-

### max_compaction_concurrency

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 `-1` 代表没有限制。`0` 表示禁用 Compaction。开启 Event-based Compaction Framework 时，该参数才支持动态设置。
- 引入版本：-

### max_cumulative_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 Cumulative Compaction 能合并的最大 Segment 数。如果 Compaction 时出现内存不足的情况，可以调小该值。
- 引入版本：-

### max_download_speed_kbps

- 默认值：50000
- 类型：Int
- 单位：KB/Second
- 是否动态：是
- 描述：单个 HTTP 请求的最大下载速率。这个值会影响 BE 之间同步数据副本的速度。
- 引入版本：-

### max_garbage_sweep_interval

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘进行垃圾清理的最大间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

### max_percentage_of_error_disk

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：错误磁盘达到该比例上限，BE 退出。
- 引入版本：-

### max_queueing_memtable_per_tablet

- 默认值：2
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：控制写路径的每个 Tablet 的反压：当某个 Tablet 的排队（尚未刷写）memtable 数量达到或超过 `max_queueing_memtable_per_tablet` 时，`LocalTabletsChannel` 和 `LakeTabletsChannel` 中的写入者在提交更多写入工作之前会阻塞（sleep/retry）。这可以降低同时进行的 memtable 刷写并减少峰值内存使用，但代价是增加延迟或在高负载下发生 RPC 超时。将此值设高以允许更多并发 memtable（更多内存和 I/O 突发）；设低以限制内存压力并增加写入节流。
- 引入版本：v3.2.0

### max_row_source_mask_memory_bytes

- 默认值：209715200
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：Row source mask buffer 的最大内存占用大小。当 buffer 大于该值时将会持久化到磁盘临时文件中。该值应该小于 `compaction_memory_limit_per_worker` 参数的值。
- 引入版本：-

### max_tablet_write_chunk_bytes

- 默认值：536870912
- 类型：long
- 单位：Bytes
- 是否动态：是
- 描述：当前内存 tablet 写入 chunk 在被视为已满并入队发送之前允许的最大内存（以字节为单位）。增大此值可以在加载宽表（列数多）时减少 RPC 频率，从而提高吞吐量，但代价是更高的内存使用和更大的 RPC 负载。需要调整此值以在减少 RPC 次数与内存及序列化/BRPC 限制之间取得平衡。
- 引入版本：v3.2.12

### max_update_compaction_num_singleton_deltas

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大 Rowset 数。
- 引入版本：-

### memory_limitation_per_thread_for_schema_change

- 默认值：2
- 类型：Int
- 单位：GB
- 是否动态：是
- 描述：单个 Schema Change 任务允许占用的最大内存。
- 引入版本：-

### memory_ratio_for_sorting_schema_change

- 默认值：0.8
- 类型：Double
- 单位：- (unitless ratio)
- 是否动态：是
- 描述：在排序型 schema-change 操作期间，用作 memtable 最大缓冲区大小的每线程 schema-change 内存限制的比例。该比例会与 memory_limitation_per_thread_for_schema_change（以 GB 配置并转换为字节）相乘以计算 max_buffer_size，且结果上限为 4GB。SchemaChangeWithSorting 和 SortedSchemaChange 在创建 MemTable/DeltaWriter 时使用此值。增大该比例允许更大的内存缓冲区（减少 flush/merge 次数），但会增加内存压力风险；减小该比例会导致更频繁的 flush，从而增加 I/O/merge 开销。
- 引入版本：v3.2.0

### min_base_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Base Compaction 的最小 Segment 数。
- 引入版本：-

### min_compaction_failure_interval_sec

- 默认值：120
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Tablet Compaction 失败之后，再次被调度的间隔。
- 引入版本：-

### min_cumulative_compaction_failure_interval_sec

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Cumulative Compaction 失败后的最小重试间隔。
- 引入版本：-

### min_cumulative_compaction_num_singleton_deltas

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发 Cumulative Compaction 的最小 Segment 数。
- 引入版本：-

### min_garbage_sweep_interval

- 默认值：180
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：磁盘进行垃圾清理的最小间隔。自 3.0 版本起，该参数由静态变为动态。
- 引入版本：-

### parallel_clone_task_per_path

- 默认值：8
- 类型：Int
- 单位：Threads
- 是否动态：是
- 描述：在 BE 的每个存储路径上分配的并行 clone 工作线程数。BE 启动时，clone 线程池的最大线程数计算为 max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL)。例如，若有 4 个存储路径且默认=8，则 clone 池最大 = 32。此设置直接控制 BE 处理的 CLONE 任务（tablet 副本拷贝）的并发度：增加它会提高并行 clone 吞吐量，但也会增加 CPU、磁盘和网络争用；减少它会限制同时进行的 clone 任务并可能限制 FE 调度的 clone 操作。该值应用于动态 clone 线程池，可通过 update-config 路径在运行时更改（会导致 agent_server 更新 clone 池的最大线程数）。
- 引入版本：v3.2.0

### pending_data_expire_time_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：存储引擎保留的未生效数据的最大时长。
- 引入版本：-

### pindex_major_compaction_limit_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题。
- 引入版本：v3.0.9

### pk_index_compaction_score_ratio

- 默认值：1.5
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引的 Compaction 分数比率。例如，如果有 N 个文件集，Compaction 分数将为 `N * pk_index_compaction_score_ratio`。
- 引入版本：-

### pk_index_early_sst_compaction_threshold

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引 early SST Compaction 的阈值。
- 引入版本：-

### pk_index_memtable_flush_threadpool_max_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引 MemTable 刷盘的线程池最大线程数。0 表示自动设置为 CPU 核数的一半。
- 引入版本：-

### pk_index_memtable_flush_threadpool_size

- 默认值：1048576
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：控制用于存算分离（云原生 / 数据湖）模式下 PK 索引 memtable 刷写的线程池（在 ExecEnv 中创建为 "cloud_native_pk_index_flush"）的最大队列大小（待处理任务数量）。该线程池的最大线程数由 `pk_index_memtable_flush_threadpool_max_threads` 管理。增大此值允许在执行前缓冲更多的 memtable flush 任务，这可以减少即时背压，但会增加由排队任务对象消耗的内存。减小它可以限制被缓冲的任务数量，并可能根据线程池行为更早引发背压或任务被拒绝。请根据可用内存和预期并发 flush 工作量进行调优。
- 引入版本：-

### pk_index_memtable_max_count

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引的最大 MemTable 数量。
- 引入版本：-

### pk_index_memtable_max_wait_flush_timeout_ms

- 默认值：30000
- 类型：Int
- 单位：毫秒
- 是否动态：是
- 描述：存算分离集群中，等待主键索引 MemTable 刷盘完成的最大超时时间。当需要同步刷盘所有 MemTable 时（例如在 ingest SST 操作之前），系统最多等待该时间。默认为 30 秒。
- 引入版本：-

### pk_index_parallel_compaction_task_split_threshold_bytes

- 默认值：33554432
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Compaction 任务拆分的阈值。当任务涉及的文件总大小小于此阈值时，任务将不会被拆分。默认为 32MB。
- 引入版本：-

### pk_index_parallel_compaction_threadpool_max_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引并行 Compaction 的线程池最大线程数。0 表示自动设置为 CPU 核数的一半。
- 引入版本：-

### pk_index_parallel_execution_min_rows

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，启用主键索引并行执行的最小行数阈值。
- 引入版本：-

### pk_index_parallel_execution_threadpool_max_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，主键索引并行执行的线程池最大线程数。0 表示自动设置为 CPU 核数的一半。
- 引入版本：-

### pk_index_parallel_load_dels_mem_ratio

- 默认值：50
- 类型：Int
- 单位：百分比（0-100）
- 是否动态：是
- 描述：存算分离集群中，当 update mem tracker 已超过其上限的此百分比时，`LakePersistentIndex::load_dels` 将跳过并行两阶段 del 文件预取，退回到单遍循环路径，一次只持有一个解码后的 del 文件列。在该内存压力下放弃冷启延迟收益以换取受控的峰值内存。值越大表示在更大的内存压力下仍允许该优化；设为 `100` 时禁用内存门控（只要 `enable_pk_index_parallel_execution=true` 且存在多个 del 文件即并行）。
- 引入版本：-

### lake_partial_update_thread_pool_max_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，部分列更新 segment 级并行执行的线程池最大线程数。该线程池同时用于行模式（并行 load_segment + rewrite_segment）和列模式（并行 DCG 生成）的部分列更新。0 表示自动设置为 CPU 核数的一半。运行时开关由 `enable_pk_index_parallel_execution` 控制。
- 引入版本：v4.1

### lake_partial_update_thread_pool_queue_size

- 默认值：2048
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：部分列更新线程池的任务队列大小。
- 引入版本：v4.1

### pk_index_size_tiered_level_multiplier

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的层级倍数参数。
- 引入版本：-

### pk_index_size_tiered_max_level

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的层级数量参数。
- 引入版本：-

### pk_index_size_tiered_min_level_size

- 默认值：131072
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键索引 Size-Tiered Compaction 策略的最小层级大小参数。
- 引入版本：-

### pk_index_sstable_sample_interval_bytes

- 默认值：16777216
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群中,主键索引 SSTable 文件的采样间隔大小。当 SSTable 文件大小超过该阈值时,系统会按照此间隔对 SSTable 中的键进行采样,用于优化 Compaction 任务的边界划分;对于小于该阈值的 SSTable,仅使用起始键作为边界键。默认为 16 MB。
- 引入版本：-

### pk_index_target_file_size

- 默认值：67108864
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群中，主键索引的目标文件大小。
- 引入版本：-

### pk_index_eager_build_threshold_bytes

- 默认值：104857600
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：当 `enable_pk_index_eager_build` 设置为 `true` 后，导入或 Compaction 生成的数据大于该阈值时，系统才会即时构建主键索引文件。默认为 100MB。
- 引入版本：-

### primary_key_limit_size

- 默认值：128
- 类型：Int
- 单位：Byte
- 是否动态：是
- 描述：主键表中单条主键值最大长度。
- 引入版本：v2.5

### release_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点释放快照任务的最大线程数。
- 引入版本：-

### repair_compaction_interval_seconds

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Repair Compaction 线程轮询的间隔。
- 引入版本：-

### replication_max_speed_limit_kbps

- 默认值：50000
- 类型：Int
- 单位：KB/s
- 是否动态：是
- 描述：每个同步线程的最大速度。
- 引入版本：v3.3.5

### replication_min_speed_limit_kbps

- 默认值：50
- 类型：Int
- 单位：KB/s
- 是否动态：是
- 描述：每个同步线程的最小速度。
- 引入版本：v3.3.5

### replication_min_speed_time_seconds

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：同步线程低于最低速度所允许的持续时间。如果实际速度低于 `replication_min_speed_limit_kbps` 的时间超过此值，同步将失败。
- 引入版本：v3.3.5

### replication_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于同步的最大线程数。0 表示将线程数设置为 BE CPU 内核数的四倍。
- 引入版本：v3.3.5

### size_tiered_level_multiple

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，相邻两个 Level 之间相差的数据量的倍数。
- 引入版本：-

### size_tiered_level_multiple_dupkey

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，Duplicate Key 表相邻两个 Level 之间相差的数据量的倍数。
- 引入版本：-

### size_tiered_level_num

- 默认值：7
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 策略的 Level 数量。每个 Level 最多保留一个 Rowset，因此稳定状态下最多会有和 Level 数相同的 Rowset。
- 引入版本：-

### size_tiered_max_compaction_level

- 默认值：7
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Size-tiered Compaction 允许的最大合并层级，超过该层级的输入不再向上合并。
- 引入版本：-

### size_tiered_min_level_size

- 默认值：131072
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Size-tiered Compaction 策略中，最小 Level 的大小，小于此数值的 Rowset 会直接触发 Compaction。
- 引入版本：-

### snapshot_expire_time_sec

- 默认值：172800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：快照文件清理的间隔。
- 引入版本：-

### stale_memtable_flush_time_sec

- 默认值：0
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：当内存接近限制时，超过该时间未被更新的 MemTable 会被提前 Flush 以缓解内存压力；0 表示禁用按“陈旧时间”触发的刷盘（仍可能因高内存或不可变分区触发）。
- 引入版本：v3.2.0

### storage_flood_stage_left_capacity_bytes

- 默认值：107374182400
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：BE 存储目录整体磁盘剩余空间的硬限制。如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，StarRocks 会拒绝 Load 和 Restore 作业，默认 100GB。需要同步修改 FE 配置 `storage_usage_hard_limit_reserve_bytes` 以使其生效。
- 引入版本：-

### storage_flood_stage_usage_percent

- 默认值：95
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 存储目录整体磁盘空间使用率的硬上限。如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，StarRocks 会拒绝 Load 和 Restore 作业。需要同步修改 FE 配置 `storage_usage_hard_limit_percent` 以使其生效。
- 引入版本：-

### storage_high_usage_disk_protect_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：创建 Tablet 时，为避免高使用率磁盘被优先选择，系统会计算所有磁盘的平均使用率，超过“平均 + 本阈值”的磁盘将被暂时排除优先选择。设为 0 可关闭此保护。
- 引入版本：v3.2.0

### storage_medium_migrate_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：介质迁移的线程数，用于从 SATA 迁移到 SSD。
- 引入版本：-

### storage_root_path

- 默认值：`${STARROCKS_HOME}/storage`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：存储数据的目录以及存储介质类型。示例：`/data1,medium:hdd;/data2,medium:ssd`。
  - 多块盘配置使用分号 `;` 隔开。
  - 如果为 SSD 磁盘，需在路径后添加 `,medium:ssd`。
  - 如果为 HDD 磁盘，需在路径后添加 `,medium:hdd`。
- 引入版本：-

### sync_tablet_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Tablet 元数据同步。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

### tablet_map_shard_size

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Tablet Map Shard 大小。该值必须是二的倍数。
- 引入版本：-

### tablet_max_pending_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表每个 Tablet 上允许已提交 (Commit) 但是未 Apply 的最大版本数。
- 引入版本：-

### tablet_max_versions

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Tablet 上允许的最大版本数。如果超过该值，新的写入请求会失败。
- 引入版本：-

### tablet_meta_checkpoint_min_interval_secs

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：TabletMeta Checkpoint 线程轮询的时间间隔。
- 引入版本：-

### tablet_meta_checkpoint_min_new_rowsets_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：自上次 TabletMeta Checkpoint 至今新创建的 Rowset 数量。
- 引入版本：-

### tablet_rowset_stale_sweep_time_sec

- 默认值：1800
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：失效 Rowset 的清理间隔。
- 引入版本：-

### tablet_stat_cache_update_interval_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Tablet Stat Cache 的更新间隔。
- 引入版本：-

### lake_enable_accurate_pk_row_count

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为存算分离（Lake）主键表 tablet 使用精确行数统计。开启后会读取每个 rowset 在对象存储中的 delete vector 来扣减删除行，统计更准确，但会显著增加 `get_tablet_stats` RPC 的开销；关闭后使用 rowset 元数据中的近似 `num_dels`，可避免远端 I/O，但对“已删除但尚未 compaction”的行可能略有高估。
- 引入版本：-

### lake_tablet_stat_slow_log_ms

- 默认值：300000
- 类型：Int64
- 单位：Milliseconds
- 是否动态：是
- 描述：Tablet 统计采集慢日志阈值（毫秒）。单次 tablet 统计任务耗时超过该阈值时，会输出告警日志，附带 `tablet_id`、版本、rowset 数、是否精确模式和耗时等诊断信息。
- 引入版本：-

### lake_metadata_fetch_thread_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于存算分离表 tablet 元数据获取操作（例如 `get_tablet_stats`、`get_tablet_metadatas`）的线程数。
- 引入版本：v3.5.16, v4.0.9

### tablet_writer_open_rpc_timeout_sec

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：远端打开 Tablet Writer 的 RPC 超时时长，转换为毫秒应用于请求和 brpc control。实际生效为 `min(tablet_writer_open_rpc_timeout_sec, load_timeout_sec/2)`，过小易早退，过大则延迟错误处理。
- 引入版本：v3.2.0

### trash_file_expire_time_sec

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：回收站清理的间隔。自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259200 变为 86400。
- 引入版本：-

### unused_rowset_monitor_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：清理过期 Rowset 的时间间隔。
- 引入版本：-

### path_gc_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启存储路径的垃圾回收检查。关闭后不会对存储路径执行 GC，可能导致无用文件堆积。
- 引入版本：-

### path_gc_check_interval_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：存储路径垃圾回收检查的执行间隔。
- 引入版本：-

### update_cache_expire_sec

- 默认值：360
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Update Cache 的过期时间。
- 引入版本：-

### update_compaction_check_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：主键表 Compaction 的检查间隔。
- 引入版本：-

### update_compaction_chunk_size_for_row_store

- 默认值：0
- 类型：Int
- 单位：Rows
- 是否动态：是
- 描述：row-store 形态的 Tablet 在更新 Compaction 时强制使用的 chunk 行数。0 表示按默认公式自适应计算；大于 0 时直接使用该值，可能提升吞吐但增加内存占用，或调小以降低内存压力。
- 引入版本：v3.2.3

### update_compaction_delvec_file_io_amp_ratio

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于控制主键表包含 Delvec 文件的 Rowset 做 Compaction 的优先级。该值越大优先级越高。
- 引入版本：-

### update_compaction_num_threads_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表每个磁盘 Compaction 线程的数目。
- 引入版本：-

### update_compaction_per_tablet_min_interval_seconds

- 默认值：120
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：主键表每个 Tablet 做 Compaction 的最小时间间隔。
- 引入版本：-

### update_compaction_ratio_threshold

- 默认值：0.5
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群下主键表单次 Compaction 可以合并的最大数据比例。如果单个 Tablet 过大，建议适当调小该配置项取值。
- 引入版本：v3.1.5

### update_compaction_result_bytes

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：主键表单次 Compaction 合并的最大结果的大小。
- 引入版本：-

### update_compaction_size_threshold

- 默认值：268435456
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：主键表的 Compaction Score 是基于文件大小计算的，与其他表类型的文件数量不同。通过该参数可以使主键表的 Compaction Score 与其他类型表的相近，便于用户理解。
- 引入版本：-

### upload_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE 节点上传任务的最大线程数，用于备份作业。`0` 表示设置线程数为 BE 所在机器的 CPU 核数。
- 引入版本：-

### vertical_compaction_max_columns_per_group

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每组 Vertical Compaction 的最大列数。
- 引入版本：-
