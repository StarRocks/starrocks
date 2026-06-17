---
displayed_sidebar: docs
description: "BE configuration parameters for shared-data clusters, data lake integration, and miscellaneous settings."
sidebar_label: "存算分离、数据湖和其他"
keywords: ['Canshu']
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 配置项 - 存算分离、数据湖和其他

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
- [存算分离](#存算分离)
- [数据湖](#数据湖)
- [其他](#其他)

## 存算分离

### cloud_native_pk_index_rebuild_files_threshold

- 默认值：50
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：云原生主键索引在恢复（Rebuild）时允许重建的最大 Segment 文件数。若需要重建的文件数超过该阈值，StarRocks 会立即将内存中的 MemTable 刷盘，以减少需要重放的 Segment 数量。设置为 `0` 则禁用此提前刷盘策略。
- 引入版本：-

### cloud_native_pk_index_rebuild_rows_threshold

- 默认值：10000000
- 类型：Long
- 单位：行
- 是否动态：是
- 描述：云原生主键索引在恢复（Rebuild）时允许重建的最大行数。若需要重建的行数超过该阈值，StarRocks 会立即将内存中的 MemTable 刷盘，以降低索引重建开销。设置为 `0` 则禁用此提前刷盘策略。与 `cloud_native_pk_index_rebuild_files_threshold` 配合使用，任一阈值超出均会触发刷盘。
- 引入版本：-

### download_buffer_size

- 默认值：4194304
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：下载快照文件时每条传输流使用的内存缓冲区大小（fs::copy 的单次读写块大小）。值越大吞吐越高但每个并发下载占用更多内存。
- 引入版本：v3.2.13

### graceful_exit_wait_for_frontend_heartbeat

- 默认值：false
- 类型： Boolean
- 单位：-
- 是否动态：是
- 描述： 确定是否在完成优雅退出前等待至少一个指示SHUTDOWN状态的FE心跳响应。启用后，优雅关闭进程将持续运行直至通过心跳RPC返回给FE SHUTDOWN状态变化，确保FE在两次常规心跳探测间隔期间有足够时间感知终止状态。
- 引入版本：v3.4.5

### lake_compaction_stream_buffer_size_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群 Compaction 任务在远程 FS 读 I/O 阶段的 Buffer 大小。默认值为 1MB。您可以适当增大该配置项取值以加速 Compaction 任务。
- 引入版本：v3.2.3

### enable_lake_autonomous_compaction

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：BE 侧自治 compaction 调度器的总开关。`false` 时 BE 行为不变，所有 compaction 仍由 FE 通过已有的 `compact()` RPC 发起；`true` 时 BE 的 `LakeCompactionManager` 启动事件驱动的 dispatch 循环，自行挑选高分 tablet 执行 compaction 并把结果落本地为 `CompactionResultPB` 文件，等待 FE 后续以 `COLLECT_AND_PUBLISH` 汇总为一个 OpParallelCompaction 事务发表。需与 FE 侧 `enable_lake_autonomous_compaction = true` 同时开启。
- 引入版本：v3.6.0

### lake_autonomous_compaction_max_concurrent_tasks

- 默认值：32
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单 BE 同时进行的自治 compaction 任务总数上限。dispatch 循环在 `running_tasks` 达到该值时停止从队列出队。仅在 `enable_lake_autonomous_compaction` 为 `true` 时生效。
- 引入版本：v3.6.0

### lake_autonomous_compaction_max_tasks_per_tablet

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个 tablet 同时进行的自治 compaction 任务上限。命中后该 tablet 暂从队列移除，待已有任务完成后再重新评估入队。仅在 `enable_lake_autonomous_compaction` 为 `true` 时生效。
- 引入版本：v3.6.0

### lake_autonomous_compaction_score_threshold

- 默认值：10.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：tablet 进入自治调度队列所需的最低 compaction score。较低值会让更多低分 tablet 进入队列；较高值则减少调度频率但允许部分 tablet 累积更多 rowset。仅在 `enable_lake_autonomous_compaction` 为 `true` 时生效。
- 引入版本：v3.6.0

### lake_autonomous_compaction_local_result_dir_max_bytes

- 默认值：1073741824
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：BE 上所有 store path 合计的 `CompactionResultPB` 文件磁盘占用硬上限，达到后 `append_result` 直接返回 `ResourceBusy`，暂停新的自治 compaction 直到下一次 `COLLECT_AND_PUBLISH` 把本地结果排空。默认 1 GiB。仅在 `enable_lake_autonomous_compaction` 为 `true` 时生效。
- 引入版本：v3.6.0

### lake_pk_compaction_max_input_rowsets

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。该参数默认值自 v3.2.4 和 v3.1.10 版本开始从 `5` 变更为 `1000`，并自 v3.3.1 和 v3.2.9 版本开始变更为 `500`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 引入版本：v3.1.8, v3.2.3

### enable_lake_pk_compaction_score_gate

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下主键表 Size-tiered Compaction「评分门控」的总开关。默认开启时，会跳过低价值的稀疏中间层 Compaction：当被选中的层的 Compaction 评分低于 `lake_pk_compaction_min_level_score`，且未触发下述任一豁免条件时，该层不会被 Compaction，从而避免为极小的文件数收益而重写大量基线数据。设置为 `false` 可一键关闭整个门控；此时每个被选中的层都会无条件执行 Compaction（即门控引入前的行为），下述阈值参数也将不再生效。
- 引入版本：v4.2

### lake_pk_compaction_min_level_score

- 默认值：2.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：主键表 Compaction 评分门控的评分阈值（参见 `enable_lake_pk_compaction_score_gate`）。当 Size-tiered 选层器选中的层的总评分低于该值，且未触发任何豁免条件时，跳过本次 Compaction。每个 Rowset 的评分为 `io_count * 1MB / read_bytes`，并按层求和：包含大量小 Rowset 或重叠 Rowset 的层评分高（值得 Compaction），而少量大的非重叠 Rowset 评分低（近乎纯粹的基线重写）。设置为 `0` 可使门控失效，因为层的评分恒大于等于 `0`。
- 引入版本：v4.2

### lake_pk_compaction_min_benefit_cost_ratio

- 默认值：0.005
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：主键表 Compaction 评分门控的一个豁免条件。指低于阈值的层仍被 Compaction 所需的最小收益/成本比（每重写 1 MB 节省的 Segment 数）。该收益还会计入删除向量（delvec）的清理压力（参见 `lake_pk_compaction_delvec_benefit_weight`）。设置为 `0` 可禁用该豁免，此时仅 `lake_pk_compaction_min_level_score` 生效。
- 引入版本：v4.2

### lake_pk_compaction_emergency_score

- 默认值：50.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：主键表 Compaction 评分门控的一个豁免条件。当 Tablet 整体的读取压力评分（所有 Rowset 评分之和）达到该值时，即使被选中的层低于 `lake_pk_compaction_min_level_score`，也会执行 Compaction，从而避免读放大严重的热点 Tablet 长期得不到 Compaction。设置为 `0` 可禁用该豁免。
- 引入版本：v4.2

### lake_pk_compaction_delvec_benefit_weight

- 默认值：12.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：在 `lake_pk_compaction_min_benefit_cost_ratio` 使用的收益/成本比中，将层的删除压力换算为等效 Segment 收益单位的权重，计算公式为 `benefit = real_benefit_segments + delete_ratio * input_segments * weight`。值越大，Compaction 越积极地清理删除向量；值越小，越倾向于降低写放大。设置为 `0` 可去除删除项的贡献，此时该比值仅反映 Segment 数收益。
- 引入版本：v4.2

### lake_pk_compaction_size_overflow_ratio

- 默认值：2.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：主键表 Compaction 评分门控的一个豁免条件。当低于阈值的层的总字节数超过 `ratio * largest_rowset_bytes * size_tiered_level_multiple`（即自然的下一层晋升目标的 `ratio` 倍）时，强制执行 Compaction，以约束长尾的中间层堆积。默认值 `2.0` 表示在强制合并前容忍达到自然晋升阈值的两倍。设置为 `0` 可禁用该豁免，即不设置大小上限。
- 引入版本：v4.2

### lake_put_txn_log_timeout_guard_ms

- 默认值：-1
- 类型：Int64
- 单位：毫秒
- 是否动态：是
- 描述：存算分离集群下，向对象存储写入事务日志（`put_txn_log` 和 `put_combined_txn_log` 路径）的超时守护阈值。如果写入一条事务日志的耗时超过该值，StarRocks 会将慢线程的堆栈打印到 BE 日志，以便诊断对象存储写入慢的问题。默认关闭（小于或等于 `0` 表示关闭该守护）；如需开启，可将其设置为正值，例如 `4000`（4 秒）。
- 引入版本：-

### lake_rows_mapper_read_parallelism

- 默认值：32
- 类型：Int
- 单位：sub-chunk 个数
- 是否动态：是
- 描述：存算分离集群下，主键表轻量 Compaction 发布阶段，`RowsMapperIterator` 读取 `.lcrm`（lake compaction rows-mapper）文件时，允许同时 in-flight 的 sub-chunk 读取数量上限。每个 sub-chunk 大小由 `lake_rows_mapper_sub_chunk_bytes` 控制，且不跨 segment 边界。迭代器最多向 PK index 执行线程池提交该数量的并发读取请求，让远端读取与调用方的 per-segment 处理流水化进行。内存上限为 `lake_rows_mapper_read_parallelism * lake_rows_mapper_sub_chunk_bytes`。设为 `1` 可关闭流水化，回退到顺序读取。

### lake_rows_mapper_sub_chunk_bytes

- 默认值：4194304
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群下，主键表轻量 Compaction 发布阶段，`RowsMapperIterator` 流水化读取 `.lcrm` 文件时使用的 sub-chunk 粒度。每个输出 segment 被切分为 `ceil(segment_bytes / lake_rows_mapper_sub_chunk_bytes)` 个 sub-chunk，独立流水化。值越小，少而大的输出 segment 能获得越高的并发度，但代价是更多的范围读取和消费时一次额外的 memcpy。默认 4 MiB，与 starcache 磁盘层 block 大小对齐。

### lake_vacuum_min_batch_delete_size

- 默认值：200
- 类型：Int64
- 单位：文件数
- 是否动态：是
- 描述：存算分离集群下，Vacuum 单次 `DeleteObjects` 请求批量合并的过期文件数量。批量越大，单次调用摊销的 HTTP/认证/签名等固定开销越多，且对对象存储 prefix 级别的请求频次压力越小；代价是单次调用 latency 升高、出现瞬时错误需要 retry 时回放成本增大。AWS S3 用户可以进一步将该值调到协议上限 `1000`，因为 AWS S3 单 `DeleteObjects` 服务端耗时几乎与 batch size 无关。

### loop_count_wait_fragments_finish

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：BE/CN 退出时需要等待正在执行的查询完成的轮次，一轮次固定 10 秒。设置为 `0` 表示禁用轮询等待，立即退出。自 v3.4 起，该参数变为动态参数，且默认值由 `0` 变为 `2`。
- 引入版本：v2.5

### max_client_cache_size_per_host

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 范围内的客户端缓存为每个远程主机保留的最大缓存 client 实例数。提高此值可以减少重连和 stub 创建开销，但会增加内存和文件描述符使用；降低它则节省资源但可能增加连接 churn。该值在启动时读取，运行时无法更改。目前一个共享设置控制所有客户端缓存类型；将来可能会引入每种缓存的独立配置。
- 引入版本：v3.2.0

### starlet_filesystem_instance_cache_capacity

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：starlet filesystem 实例的缓存容量。
- 引入版本：v3.2.16, v3.3.11, v3.4.1

### starlet_filesystem_instance_cache_ttl_sec

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：starlet filesystem 实例缓存的过期时间。
- 引入版本：v3.3.15, 3.4.5

### starlet_port

- 默认值：9070
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 和 CN 的额外 Agent 服务端口。
- 引入版本：-

### starlet_star_cache_disk_size_percent

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离集群中，Data Cache 最多可使用的磁盘容量百分比。仅在 `datacache_unified_instance_enable` 为 `false` 时生效。
- 引入版本：v3.1

### starlet_use_star_cache

- 默认值：false（v3.1）true（v3.2.3 起）
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群中是否使用 Data Cache。`true` 表示启用该功能，`false` 表示禁用。自 v3.2.3 起，默认值由 `false` 调整为 `true`。
- 引入版本：v3.1

### starlet_write_file_with_tag

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否将写入到对象存储的文件打上对象存储 Tag，方便自定义管理文件。
- 引入版本：v3.5.3

### table_schema_service_max_retries

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Table Schema Service 请求的最大重试次数。
- 引入版本：v4.1

## 数据湖

### datacache_block_buffer_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Block Buffer 优化 Data Cache 效率。当启用 Block Buffer 时，系统会从 Data Cache 中读取完整的 Block 数据并缓存在临时 Buffer 中，从而减少频繁读取缓存带来的额外开销。
- 引入版本：v3.2.0

### datacache_disk_adjust_interval_seconds

- 默认值：10
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Data Cache 容量自动调整周期。每隔这段时间系统会进行一次缓存磁盘使用率检测，必要时触发相应扩缩容操作。
- 引入版本：v3.3.0

### datacache_disk_idle_seconds_for_expansion

- 默认值：7200
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Data Cache 自动扩容最小等待时间。只有当磁盘使用率在 `datacache_disk_low_level` 以下持续时间超过该时长，才会触发自动扩容。
- 引入版本：v3.3.0

### datacache_disk_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：单个磁盘缓存数据量的上限，可设为比例上限（如 `80%`）或物理上限（如 `2T`, `500G` 等）。假设系统使用了两块磁盘进行缓存，并设置 `datacache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 引入版本：-

### datacache_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Data Cache。`true` 表示启用，`false` 表示不启用。自 v3.3 起，默认值变为 `true`。
- 引入版本：-

### datacache_eviction_policy

- 默认值：slru
- 类型：String
- 单位：-
- 是否动态：否
- 描述：缓存淘汰策略。有效值：`lru` (least recently used) 和 `slru` (Segmented LRU)。
- 引入版本：v3.4.0

### datacache_inline_item_count_limit

- 默认值：130172
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Data Cache 内联对象数量上限。当缓存的 Block 对象特别小时，Data Cache 会选择使用内联方式将 Block 数据和元数据一起缓存在内存中。
- 引入版本：v3.4.0

### datacache_mem_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：内存缓存数据量的上限，可设为比例上限（如 `10%`）或物理上限（如 `10G`, `21474836480` 等）。
- 引入版本：-

### datacache_min_disk_quota_for_adjustment

- 默认值：10737418240
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：Data Cache 自动扩缩容时的最小有效容量。当需要调整的目标容量小于该值时，系统会直接将缓存空间调整为 `0`，以避免缓存空间过小导致频繁填充和淘汰带来负优化。
- 引入版本：v3.3.0

### datacache_unified_instance_enable

- 默认值：true
- 类型：Bool
- 单位：-
- 是否动态：否
- 描述：存算分离集群中，是否使用统一的 data cache 实例管理 internal catalog 和 external catalog 的数据缓存。
- 引入版本：v3.4.0

### disk_high_level

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘高水位（百分比）。当磁盘使用率高于该值时，系统自动淘汰 Data Cache 中的缓存数据。自 v3.4.0 起，该参数默认值由 `80` 变更为 `90`。该参数自 v4.0 起由 `datacache_disk_high_level` 更名为 `disk_high_level`。
- 引入版本：v3.3.0

### disk_low_level

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘低水位（百分比）。当磁盘使用率在 `datacache_disk_idle_seconds_for_expansion` 指定的时间内持续低于该值，且用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。该参数自 v4.0 起由 `datacache_disk_low_level` 更名为 `disk_low_level`。
- 引入版本：v3.3.0

### disk_safe_level

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：磁盘安全水位（百分比）。当 Data Cache 进行缓存自动扩缩容时，系统将尽可能以该阈值为磁盘使用率目标调整缓存容量。自 v3.4.0 起，该参数默认值由 `70` 变更为 `80`。该参数自 v4.0 起由 `datacache_disk_safe_level` 更名为 `disk_safe_level`。
- 引入版本：v3.3.0

### enable_connector_sink_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否支持在外表写入时启用数据 Spill。启用该功能能够避免当内存不足时写入外表导致生成大量小文件问题。当前仅支持向 Iceberg 表写入数据时启用 Spill 功能。
- 引入版本：v4.0.0

### enable_datacache_disk_auto_adjust

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘容量自动调整开关，启用后会根据当前磁盘使用率动态调整缓存容量。该参数自 v4.0 起由 `datacache_auto_adjust_enable` 更名为 `enable_datacache_disk_auto_adjust`。
- 引入版本：v3.3.0

### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：毫秒
- 是否动态：否
- 描述：JDBC 空闲连接超时时间。如果 JDBC 连接池内的连接空闲时间超过此值，连接池会关闭超过 `jdbc_minimum_idle_connections` 配置项中指定数量的空闲连接。
- 引入版本：-

### jdbc_connection_pool_size

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：JDBC 连接池大小。每个 BE 节点上访问 `jdbc_url` 相同的外表时会共用同一个连接池。
- 引入版本：-

### jdbc_minimum_idle_connections

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：JDBC 连接池中最少的空闲连接数量。
- 引入版本：-

### jdbc_connection_max_lifetime_ms

- 默认值：300000
- 类型：Long
- 单位：毫秒
- 是否动态：否
- 描述：JDBC 连接池中连接的最大生命周期。连接在此超时前会被回收，以防止出现陈旧连接。允许的最小值为 30000 (30 秒)。
- 引入版本：-

### jdbc_connection_keepalive_time_ms

- 默认值：30000
- 类型：Long
- 单位：毫秒
- 是否动态：否
- 描述：空闲 JDBC 连接的保活间隔。空闲连接会在此间隔进行测试，以主动检测陈旧连接。设置为 0 可禁用保活探测。启用时，必须 >= 30000 且小于 `jdbc_connection_max_lifetime_ms`。无效的启用值将被静默禁用（重置为 0）。
- 引入版本：-

### lake_clear_corrupted_cache_data

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的数据缓存。
- 引入版本：v3.4

### lake_clear_corrupted_cache_meta

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许自动清理损坏的元数据缓存。
- 引入版本：v3.3

### lake_enable_vertical_compaction_fill_data_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许 Vertical Compaction 任务在执行时缓存数据到本地磁盘上。`true` 表示启用，`false` 表示不启用。
- 引入版本：v3.1.7, v3.2.3

### lake_replication_read_buffer_size

- 默认值：16777216
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：存算分离跨集群复制（lake replication）下载 segment 文件时使用的读缓冲区大小。该值决定每次读取远程文件的分配大小，实际使用时取此设置与 1 MB 的较大值。较大的值减少读取调用次数，可提高吞吐，但增加每个并发下载的内存占用；较小的值降低内存使用但增加 I/O 调用次数。请根据网络带宽、存储 I/O 特性和并行复制线程数量进行调优。
- 引入版本：v4.1.2

### lake_replication_max_file_copy_retry

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离跨集群复制（lake-to-lake replication）中非 segment 文件（`.sst`、`.delvec`、`.del`、`.cols`）拷贝的最大重试次数。每次尝试会校验已拷贝文件大小是否与源文件一致，以检测因对象存储瞬时问题导致的截断拷贝。如果在不稳定的存储上复制时频繁出现文件损坏，可增大此值。
- 引入版本：v4.1.2

### lake_replication_file_copy_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离跨集群复制（lake-to-lake replication）逐文件拷贝所使用的独立线程池大小。`0` 表示 `cpu_cores * 4`（与 `replication_threads` 默认语义一致）；负值表示 `-value * cpu_cores`。该线程池与 agent 任务的 `replicate_snapshot` 线程池刻意区分，目的是让外层任务可以安全地通过 `ThreadPoolToken::wait()` 等待逐文件拷贝子任务，而不触发线程池自死锁保护。该线程池在启动时一次性创建，无运行时 resize 入口，调整大小需重启 CN。
- 引入版本：v4.1.2

### lake_service_max_concurrency

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：在存算分离集群中，RPC 请求的最大并发数。当达到此阈值时，新请求会被拒绝。将此项设置为 `0` 表示对并发不做限制。
- 引入版本：-

### query_max_memory_limit_percent

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Query Pool 能够使用的最大内存上限。以 Process 内存上限的百分比来表示。
- 引入版本：v3.1.0

### rocksdb_max_write_buffer_memory_bytes

- 默认值：1073741824
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer内存的最大上限。
- 引入版本：v3.5.0

### rocksdb_write_buffer_memory_percent

- 默认值：5
- 类型：Int64
- 单位：-
- 是否动态：否
- 描述：rocksdb中write buffer可以使用的内存占比。默认值是百分之5，最终取值不会小于64MB，也不会大于1GB。
- 引入版本：v3.5.0

## 其他

### default_mv_resource_group_concurrency_limit

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务在单个 BE 上的并发上限。默认为 `0`，即不做并发数限制。
- 引入版本：-

### default_mv_resource_group_cpu_limit

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 的 CPU 核数上限。
- 引入版本：-

### default_mv_resource_group_memory_limit

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务占用单个 BE 内存上限，默认 80%。
- 引入版本：v3.1

### default_mv_resource_group_spill_mem_limit_threshold

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：物化视图刷新任务触发落盘的内存占用阈值，默认80%。
- 引入版本：v3.1

### enable_resolve_hostname_to_ip_in_load_error_url

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：`error_urls` Debug 过程中，是否允许 Operator 根据环境需求选择使用 FE 心跳的原始主机名，或强制解析为 IP 地址。
  - `true`：将主机名解析为 IP 地址。
  - `false`（默认）：在错误 URL 中保留原始主机名。
- 引入版本：v4.0.1

### enable_retry_apply

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：启用后，被分类为可重试的 Tablet apply 失败（例如暂时的内存限制错误）会被重新调度以重试，而不是立即将 tablet 标记为错误。TabletUpdates 中的重试路径使用 `retry_apply_interval_second` 乘以当前失败计数并限制到 600s 的最大值来安排下一次尝试，因此随着连续失败，退避会增长。明确不可重试的错误（例如 corruption）会绕过重试并导致 apply 过程立即进入错误状态。重试会持续直到达到总体超时/终止条件，之后 apply 将进入错误状态。关闭此项会禁用对失败 apply 任务的自动重新调度，使失败的 apply 在没有重试的情况下直接转为错误状态。
- 引入版本：v3.2.9

### enable_token_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Token 检验。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

### es_http_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：访问 Elasticsearch 的 HTTP 超时时长（毫秒）。在建立连接和发送请求时使用，超时会导致请求失败。
- 引入版本：-

### es_index_max_result_window

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Elasticsearch 索引的 `max_result_window` 限制，用于控制 Scroll/分页的最大返回窗口；超出将被 ES 拒绝。需要与 ES 端设置匹配。
- 引入版本：-

### es_scroll_keepalive

- 默认值：5m
- 类型：String
- 单位：Minutes (string with suffix, e.g. "5m")
- 是否动态：否
- 描述：发送给 Elasticsearch 的 scroll 搜索上下文的 keep-alive 时长。该值在构建初始 scroll URL (`?scroll=<value>`) 以及发送后续 scroll 请求（通过 ESScrollQueryBuilder）时按字面使用（例如 "5m"）。此设置控制 ES 端在垃圾回收前保留搜索上下文的时间；设置更长会让 scroll 上下文存活更久，但会延长 ES 集群的资源占用。该值在启动时由 ES scan reader 读取，运行时不可更改。
- 引入版本：v3.2.0

### load_replica_status_check_interval_ms_on_failure

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：当上一次检查的 RPC 失败时，从副本向主副本检查其状态的时间间隔。
- 引入版本：v3.5.1

### load_replica_status_check_interval_ms_on_success

- 默认值：15000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：当上一次检查的 RPC 成功时，从副本向主副本检查其状态的时间间隔。
- 引入版本：v3.5.1

### max_length_for_bitmap_function

- 默认值：1000000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bitmap 函数输入值的最大长度。
- 引入版本：-

### max_length_for_to_base64

- 默认值：200000
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：to_base64() 函数输入值的最大长度。
- 引入版本：-

### memory_high_level

- 默认值：75
- 类型：Long
- 单位：Percent
- 是否动态：是
- 描述：以进程内存上限的百分比表示的高水位内存阈值。当总内存消耗上升超过该百分比时，BE 开始逐步释放内存（目前通过驱逐 data cache 和 update cache）以缓解压力。监控器使用此值来计算 `memory_high = mem_limit * memory_high_level / 100`，并且如果消耗大于 memory_high，则在 GC advisor 的指导下执行受控驱逐；如果消耗超过 memory_urgent_level（一个单独的配置），则会进行更激进的即时回收。此值还用于在超过阈值时禁用某些高内存消耗的操作（例如 primary-key preload）。必须满足与 memory_urgent_level 的校验关系（memory_urgent_level `>` memory_high_level，memory_high_level `>=` 1，memory_urgent_level `<=` 100）。
- 引入版本：v3.2.0

### memory_urgent_level

- 默认值：90
- 类型：Int
- 单位：Percent
- 是否动态：否
- 描述：内存紧急阈值（占进程内存上限的百分比）。当消耗超过该值时会触发更激进的即时回收；需满足 `memory_urgent_level > memory_high_level` 且不超过 100。
- 引入版本：-

### report_exec_rpc_request_retry_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于向 FE 汇报执行状态的 RPC 请求的重试次数。默认值为 10，意味着如果该 RPC 请求失败（仅限于 fragment instance 的 finish RPC），将最多重试 10 次。该请求对于导入任务（load job）非常重要，如果某个 fragment instance 的完成状态报告失败，整个导入任务将会一直挂起，直到超时。
- 引入版本：-

### sleep_one_second

- 默认值：1
- 类型：Int
- 单位：秒
- 是否动态：否
- 描述：BE Agent Worker 线程的全局短睡眠间隔，当 Master 地址/心跳尚不可用或需要短时间重试/退避时作为一秒的暂停。多个 Report Worker Pool（例如 ReportDiskStateTaskWorkerPool、ReportOlapTableTaskWorkerPool、ReportWorkgroupTaskWorkerPool）引用此值，以避免忙等（busy-waiting）并在重试时降低 CPU 消耗。增大此值会降低重试频率并减慢对 Master 可用性的响应；减小会提高轮询频率并增加 CPU 使用。请在权衡响应性与资源使用后调整该项。
- 引入版本：v3.2.0

### small_file_dir

- 默认值：`${STARROCKS_HOME}/lib/small_file/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：保存文件管理器下载的文件的目录。
- 引入版本：-

### upload_buffer_size

- 默认值：4194304
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：上传快照文件到远端存储时的缓冲区大小（fs::copy 读写块大小）。值越大传输吞吐越高但每个并发上传占用更多内存；可与 upload_worker_count 一起调优。
- 引入版本：v3.2.13

### user_function_dir

- 默认值：`${STARROCKS_HOME}/lib/udf`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：UDF 存放的路径。
- 引入版本：-

### web_log_bytes

- 默认值：1048576 (1 MB)
- 类型：long
- 单位：Bytes
- 是否动态：否
- 描述：从 INFO 日志文件读取并在 BE 调试 Web Server 的日志页面上显示的最大字节数。该处理器使用此值计算一个 seek 偏移量（显示最后 N 字节），以避免读取或提供非常大的日志文件。如果日志文件小于该值则显示整个文件。注意：在当前实现中，用于读取并服务 INFO 日志的代码被注释掉了，处理器会报告无法打开 INFO 日志文件，因此除非启用日志服务代码，否则此参数可能无效。
- 引入版本：v3.2.0
