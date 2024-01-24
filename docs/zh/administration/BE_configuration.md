---
displayed_sidebar: "Chinese"
---

# BE 配置项

部分 BE 节点配置项为动态参数，您可以通过命令在线修改。其他配置项为静态参数，需要通过修改 **be.conf** 文件后重启 BE 服务使相关修改生效。

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## 配置 BE 动态参数

您可以通过 `curl` 命令在线修改 BE 节点动态参数。

```shell
curl -XPOST http://be_host:http_port/api/update_config?configuration_item=value
```

以下是 BE 动态参数列表。

#### enable_stream_load_verbose_log

- 含义：是否在日志中记录 Stream Load 的 HTTP 请求和响应信息。
- 默认值：false
- **引入版本：**2.5.17、3.0.9、3.1.6、3.2.1

#### report_task_interval_seconds

- 含义：汇报单个任务的间隔。建表，删除表，导入，schema change 都可以被认定是任务。
- 单位：秒
- 默认值：10

#### report_disk_state_interval_seconds

- 含义：汇报磁盘状态的间隔。汇报各个磁盘的状态，以及其中数据量等。
- 单位：秒
- 默认值：60

#### report_tablet_interval_seconds

- 含义：汇报 tablet 的间隔。汇报所有的 tablet 的最新版本。
- 单位：秒
- 默认值：60

#### report_workgroup_interval_seconds

- 含义：汇报 workgroup 的间隔。汇报所有 workgroup 的最新版本。
- 单位：秒
- 默认值：5

#### max_download_speed_kbps

- 含义：单个 HTTP 请求的最大下载速率。这个值会影响 BE 之间同步数据副本的速度。
- 单位：KB/s
- 默认值：50000

#### download_low_speed_limit_kbps

- 含义：单个 HTTP 请求的下载速率下限。如果在 `download_low_speed_time` 秒内下载速度一直低于`download_low_speed_limit_kbps`，那么请求会被终止。
- 单位：KB/s
- 默认值：50

#### download_low_speed_time

- 含义：见 `download_low_speed_limit_kbps`。
- 单位：秒
- 默认值：300

#### status_report_interval

- 含义：查询汇报 profile 的间隔，用于 FE 收集查询统计信息。
- 单位：秒
- 默认值：5

#### scanner_thread_pool_thread_num

- 含义：存储引擎并发扫描磁盘的线程数，统一管理在线程池中。
- 默认值：48

#### thrift_client_retry_interval_ms

- 含义：Thrift client 默认的重试时间间隔。
- 单位：毫秒
- 默认值：100

#### scanner_thread_pool_queue_size

- 含义：存储引擎支持的扫描任务数。
- 默认值：102400

#### scanner_row_num

- 含义：每个扫描线程单次执行最多返回的数据行数。
- 默认值：16384

#### max_scan_key_num

- 含义：查询最多拆分的 scan key 数目。
- 默认值：1024

#### max_pushdown_conditions_per_column

- 含义：单列上允许下推的最大谓词数量，如果超出数量限制，谓词不会下推到存储层。
- 默认值：1024

#### exchg_node_buffer_size_bytes  

- 含义：Exchange 算子中，单个查询在接收端的 buffer 容量。<br />这是一个软限制，如果数据的发送速度过快，接收端会触发反压来限制发送速度。
- 单位：字节
- 默认值：10485760

#### column_dictionary_key_ratio_threshold

- 含义：字符串类型的取值比例，小于这个比例采用字典压缩算法。
- 单位：%
- 默认值：0

#### memory_limitation_per_thread_for_schema_change

- 含义：单个 schema change 任务允许占用的最大内存。
- 单位：GB
- 默认值：2

#### update_cache_expire_sec

- 含义：Update Cache 的过期时间。
- 单位：秒
- 默认值：360

#### file_descriptor_cache_clean_interval

- 含义：文件句柄缓存清理的间隔，用于清理长期不用的文件句柄。
- 单位：秒
- 默认值：3600

#### disk_stat_monitor_interval

- 含义：磁盘健康状态检测的间隔。
- 单位：秒
- 默认值：5

#### unused_rowset_monitor_interval

- 含义：清理过期 Rowset 的时间间隔。
- 单位：秒
- 默认值：30

#### max_percentage_of_error_disk  

- 含义：错误磁盘达到一定比例，BE 退出。
- 单位：%
- 默认值：0

#### default_num_rows_per_column_file_block

- 含义：每个 row block 最多存放的行数。
- 默认值：1024

#### pending_data_expire_time_sec  

- 含义：存储引擎保留的未生效数据的最大时长。
- 单位：秒
- 默认值：1800

#### inc_rowset_expired_sec

- 含义：导入生效的数据，存储引擎保留的时间，用于增量克隆。
- 单位：秒
- 默认值：1800

#### tablet_rowset_stale_sweep_time_sec

- 含义：失效 rowset 的清理间隔。
- 单位：秒
- 默认值：1800

#### snapshot_expire_time_sec

- 含义：快照文件清理的间隔，默认 48 个小时。
- 单位：秒
- 默认值：172800

#### trash_file_expire_time_sec

- 含义：回收站清理的间隔，默认 24 个小时。自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259,200 变为 86,400。
- 单位：秒
- 默认值：86,400

#### base_compaction_check_interval_seconds

- 含义：Base Compaction 线程轮询的间隔。
- 单位：秒
- 默认值：60

#### min_base_compaction_num_singleton_deltas

- 含义：触发 Base Compaction 的最小 segment 数。
- 默认值：5

#### max_base_compaction_num_singleton_deltas

- 含义：单次 Base Compaction 合并的最大 segment 数。
- 默认值：100

#### base_compaction_interval_seconds_since_last_operation

- 含义：上一轮 Base Compaction 距今的间隔，是触发 Base Compaction 条件之一。
- 单位：秒
- 默认值：86400

#### cumulative_compaction_check_interval_seconds

- 含义：Cumulative Compaction 线程轮询的间隔。
- 单位：秒
- 默认值：1

#### min_cumulative_compaction_num_singleton_deltas

- 含义：触发 Cumulative Compaction 的最小 segment 数。
- 默认值：5

#### max_cumulative_compaction_num_singleton_deltas

- 含义：单次 Cumulative Compaction 能合并的最大 segment 数。如果 Compaction 时出现内存不足的情况，可以调小该值。
- 默认值：1000

#### max_compaction_candidate_num

- 含义：Compaction 候选 tablet 的最大数量。太大会导致内存占用和 CPU 负载高。
- 默认值：40960

#### update_compaction_check_interval_seconds

- 含义：主键表 Compaction 的检查间隔。
- 单位：秒
- 默认值：60

#### update_compaction_num_threads_per_disk

- 含义：主键表每个磁盘 Compaction 线程的数目。
- 默认值：1

#### update_compaction_per_tablet_min_interval_seconds

- 含义：主键表每个 tablet 做 Compaction 的最小时间间隔。
- 默认值：120
- 单位：秒

#### max_update_compaction_num_singleton_deltas

- 含义：主键表单次 Compaction 合并的最大 Rowset 数。
- 默认值：1000

#### update_compaction_size_threshold

- 含义：主键表的 Compaction Score 是基于文件大小计算的，与其他表类型的文件数量不同。通过该参数可以使 主键表的 Compaction Score 与其他类型表的相近，便于用户理解。
- 单位：字节
- 默认值：268435456

#### update_compaction_result_bytes

- 含义：主键表单次 Compaction 合并的最大结果的大小。
- 单位：字节
- 默认值：1073741824

#### update_compaction_delvec_file_io_amp_ratio

- 含义：用于控制主键表包含 Delvec 文件的 Rowset 做 Compaction 的优先级。该值越大优先级越高。
- 默认值：2

#### repair_compaction_interval_seconds

- 含义：Repair Compaction 线程轮询的间隔。
- 默认值：600
- 单位：秒

#### manual_compaction_threads

- 含义：Manual Compaction 线程数量。
- 默认值：4

#### enable_rowset_verify

- 含义：是否检查 Rowset 的正确性。开启后，会在 Compaction、Schema Change 后检查生成的 Rowset 的正确性。
- 默认值：false

#### enable_size_tiered_compaction_strategy

- 含义：是否开启 Size-tiered Compaction 策略。
- 默认值：true

#### min_compaction_failure_interval_sec

- 含义：Tablet Compaction 失败之后，再次被调度的间隔。
- 单位：秒
- 默认值：120

#### periodic_counter_update_period_ms

- 含义：Counter 统计信息的间隔。
- 单位：毫秒
- 默认值：500

#### load_error_log_reserve_hours  

- 含义：导入数据信息保留的时长。
- 单位：小时
- 默认值：48

#### streaming_load_max_mb

- 含义：流式导入单个文件大小的上限。自 3.0 版本起，默认值由 10240 变为 102400。
- 单位：MB
- 默认值：102400

#### streaming_load_max_batch_size_mb  

- 含义：流式导入单个 JSON 文件大小的上限。
- 单位：MB
- 默认值：100

#### memory_maintenance_sleep_time_s

- 含义：触发 ColumnPool GC 任务的时间间隔。StarRocks 会周期运行 GC 任务，尝试将空闲内存返还给操作系统。
- 单位：秒
- 默认值：10

#### write_buffer_size

- 含义：MemTable 在内存中的 buffer 大小，超过这个限制会触发 flush。
- 单位：字节
- 默认值：104857600

#### tablet_stat_cache_update_interval_second

- 含义：Tablet Stat Cache 的更新间隔。
- 单位：秒
- 默认值：300

#### result_buffer_cancelled_interval_time

- 含义：BufferControlBlock 释放数据的等待时间。
- 单位：秒
- 默认值：300

#### thrift_rpc_timeout_ms

- 含义：Thrift 超时的时长。
- 单位：毫秒
- 默认值：5000

#### max_consumer_num_per_group

- 含义：Routine load 中，每个consumer group 内最大的 consumer 数量。
- 默认值：3

#### max_memory_sink_batch_count

- 含义：Scan cache 的最大缓存批次数量。
- 默认值：20

#### scan_context_gc_interval_min

- 含义：Scan context 的清理间隔。
- 单位：分钟
- 默认值：5

#### path_gc_check_step

- 含义：单次连续 scan 最大的文件数量。
- 默认值：1000

#### path_gc_check_step_interval_ms

- 含义：多次连续 scan 文件间隔时间。
- 单位：毫秒
- 默认值：10

#### path_scan_interval_second

- 含义：gc 线程清理过期数据的间隔时间。
- 单位：秒
- 默认值：86400

#### storage_flood_stage_usage_percent

- 含义：如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，会拒绝 Load 和 Restore 作业。
- 单位：%
- 默认值：95

#### storage_flood_stage_left_capacity_bytes

- 含义：如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，会拒绝 Load 和 Restore 作业，默认 100GB。
- 单位：字节
- 默认值：107374182400

#### tablet_meta_checkpoint_min_new_rowsets_num  

- 含义：自上次 TabletMeta Checkpoint 至今新创建的 rowset 数量。
- 默认值：10

#### tablet_meta_checkpoint_min_interval_secs

- 含义：TabletMeta Checkpoint 线程轮询的时间间隔。
- 单位：秒
- 默认值：600

#### max_runnings_transactions_per_txn_map

- 含义：每个分区内部同时运行的最大事务数量。
- 默认值：100

#### tablet_max_pending_versions

- 含义：主键表每个 tablet 上允许已提交 (committed) 但是未 apply 的最大版本数。
- 默认值：1000

#### tablet_max_versions

- 含义：每个 tablet 上允许的最大版本数。如果超过该值，新的写入请求会失败。
- 默认值：1000

#### alter_tablet_worker_count

- 含义：进行 schema change 的线程数。自 2.5 版本起，该参数由静态变为动态。
- 默认值：3

#### max_hdfs_file_handle

- 含义：最多可以打开的 HDFS 文件句柄数量。
- 默认值：1000

#### be_exit_after_disk_write_hang_second

- 含义：磁盘挂起后触发 BE 进程退出的等待时间。
- 单位：秒
- 默认值：60

#### min_cumulative_compaction_failure_interval_sec

- 含义：Cumulative Compaction 失败后的最小重试间隔。
- 单位：秒
- 默认值：30

#### size_tiered_level_num

- 含义：Size-tiered Compaction 策略的 level 数量。每个 level 最多保留一个 rowset，因此稳定状态下最多会有和 level 数相同的 rowset。 |
- 默认值：7

#### size_tiered_level_multiple

- 含义：Size-tiered Compaction 策略中，相邻两个 level 之间相差的数据量的倍数。
- 默认值：5

#### size_tiered_level_multiple_dupkey

- 含义：Size-tiered Compaction 策略中，Duplicate Key 表相邻两个 level 之间相差的数据量的倍数。
- 默认值：10

#### size_tiered_min_level_size

- 含义：Size-tiered Compaction 策略中，最小 level 的大小，小于此数值的 rowset 会直接触发 compaction。
- 单位：字节
- 默认值：131072

#### storage_page_cache_limit

- 含义：PageCache 的容量，STRING，可写为容量大小，例如： `20G`、`20480M`、`20971520K` 或 `21474836480B`。也可以写为 PageCache 占系统内存的比例，例如，`20%`。该参数仅在 `disable_storage_page_cache` 为 `false` 时生效。
- 默认值：20%

#### disable_storage_page_cache

- 含义：是否开启 PageCache。开启 PageCache 后，StarRocks 会缓存最近扫描过的数据，对于查询重复性高的场景，会大幅提升查询效率。`true` 表示不开启。自 2.4 版本起，该参数默认值由 `true` 变更为 `false`。自 3.1 版本起，该参数由静态变为动态。
- 默认值：FALSE

#### max_compaction_concurrency

- 含义：Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 -1 代表没有限制。0 表示不允许 compaction。
- 默认值：-1

#### internal_service_async_thread_num

- 含义：单个 BE 上与 Kafka 交互的线程池大小。当前 Routine Load FE 与 Kafka 的交互需经由 BE 完成，而每个 BE 上实际执行操作的是一个单独的线程池。当 Routine Load 任务较多时，可能会出现线程池线程繁忙的情况，可以调整该配置。
- 默认值：10

#### lake_enable_vertical_compaction_fill_data_cache

- 含义：存算分离集群下，是否允许 Compaction 任务在执行时缓存数据到本地磁盘上。
- 默认值：false
- 引入版本：v3.1.7、v3.2.3

#### compact_threads

- 含义：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 默认值：4
- 引入版本：v3.0.0

#### update_compaction_ratio_threshold

- 含义：存算分离集群下主键表单次 Compaction 可以合并的最大数据比例。如果单个 Tablet 过大，建议适当调小该配置项取值。自 v3.1.5 起支持。
- 默认值：0.5

#### create_tablet_worker_count

- 含义：创建 Tablet 的线程数。自 v3.1.7 起变为动态参数。
- 默认值：3

#### number_tablet_writer_threads

- 含义：用于 Stream Load 的线程数。自 v3.1.7 起变为动态参数。
- 默认值：16

#### pipeline_connector_scan_thread_num_per_cpu

- 含义：BE 节点中每个 CPU 核心分配给 Pipeline Connector 的扫描线程数量。自 v3.1.7 起变为动态参数。
- 默认值：8

#### max_garbage_sweep_interval

- 含义：磁盘进行垃圾清理的最大间隔。自 3.0 版本起，该参数由静态变为动态。
- 单位：秒
- 默认值：3600

#### min_garbage_sweep_interval

- 含义：磁盘进行垃圾清理的最小间隔。自 3.0 版本起，该参数由静态变为动态。
- 单位：秒
- 默认值：180

## 配置 BE 静态参数

以下 BE 配置项为静态参数，不支持在线修改，您需要在 **be.conf** 中修改并重启 BE 服务。

#### hdfs_client_enable_hedged_read

- 含义：指定是否开启 Hedged Read 功能。
- 默认值：false
- 引入版本：3.0

#### hdfs_client_hedged_read_threadpool_size  

- 含义：指定 HDFS 客户端侧 Hedged Read 线程池的大小，即 HDFS 客户端侧允许有多少个线程用于服务 Hedged Read。该参数从 3.0 版本起支持，对应 HDFS 集群配置文件 `hdfs-site.xml` 中的 `dfs.client.hedged.read.threadpool.size` 参数。
- 默认值：128
- 引入版本：3.0

#### hdfs_client_hedged_read_threshold_millis

- 含义：指定发起 Hedged Read 请求前需要等待多少毫秒。例如，假设该参数设置为 `30`，那么如果一个 Read 任务未能在 30 毫秒内返回结果，则 HDFS 客户端会立即发起一个 Hedged Read，从目标数据块的副本上读取数据。该参数从 3.0 版本起支持，对应 HDFS 集群配置文件 `hdfs-site.xml` 中的 `dfs.client.hedged.read.threshold.millis` 参数。
- 单位：毫秒
- 默认值：2500
- 引入版本：3.0

#### be_port

- 含义：BE 上 thrift server 的端口，用于接收来自 FE 的请求。
- 默认值：9060

#### brpc_port

- 含义：bRPC 的端口，可以查看 bRPC 的一些网络统计信息。
- 默认值：8060

#### brpc_num_threads

- 含义：bRPC 的 bthreads 线程数量，-1 表示和 CPU 核数一样。
- 默认值：-1

#### compaction_memory_limit_per_worker

- 含义：单个 Compaction 线程的最大内存使用量。
- 单位：字节
- 默认值：2147483648 (2 GB)

#### priority_networks

- 含义：以 CIDR 形式 10.10.10.0/24 指定 BE IP 地址，适用于机器有多个 IP，需要指定优先使用的网络。
- 默认值：空字符串

#### starlet_port

- 含义：存算分离集群中 CN（v3.0 中的 BE）的额外 Agent 服务端口。
- 默认值：9070

#### heartbeat_service_port

- 含义：心跳服务端口（thrift），用户接收来自 FE 的心跳。
- 默认值：9050

#### heartbeat_service_thread_count

- 含义：心跳线程数。
- 默认值：1

#### drop_tablet_worker_count

- 含义：删除 tablet 的线程数。
- 默认值：3

#### push_worker_count_normal_priority

- 含义：导入线程数，处理 NORMAL 优先级任务。
- 默认值：3

#### push_worker_count_high_priority

- 含义：导入线程数，处理 HIGH 优先级任务。
- 默认值：3

#### transaction_publish_version_worker_count

- 含义：生效版本的最大线程数。当该参数被设置为小于或等于 `0` 时，系统默认使用 CPU 核数的一半，以避免因使用固定值而导致在导入并行较高时线程资源不足。自 2.5 版本起，默认值由 `8` 变更为 `0`。
- 默认值：0

#### clear_transaction_task_worker_count

- 含义：清理事务的线程数。
- 默认值：1

#### clone_worker_count

- 含义：克隆的线程数。
- 默认值：3

#### storage_medium_migrate_count

- 含义：介质迁移的线程数，SATA 迁移到 SSD。
- 默认值：1

#### check_consistency_worker_count

- 含义：计算 tablet 的校验和 (checksum)。
- 默认值：1

#### object_storage_connect_timeout_ms

- 含义：对象存储 Socket 连接的超时时间。
- 默认值：`-1`，表示使用 SDK 中的默认时间。
- 引入版本：3.0.9

#### object_storage_request_timeout_ms

- 含义：对象存储 HTTP 连接的超时时间。
- 默认值：`-1`，表示使用 SDK 中的默认时间。
- 引入版本：3.0.9

#### sys_log_dir

- 含义：存放日志的地方，包括 INFO，WARNING，ERROR，FATAL 等日志。
- 默认值：`${STARROCKS_HOME}/log`

#### user_function_dir

- 含义：UDF 程序存放的路径。
- 默认值：`${STARROCKS_HOME}/lib/udf`

#### small_file_dir

- 含义：保存文件管理器下载的文件的目录。
- 默认值：`${STARROCKS_HOME}/lib/small_file`

#### sys_log_level

- 含义：日志级别，INFO < WARNING < ERROR < FATAL。
- 默认值：INFO

#### sys_log_roll_mode

- 含义：日志拆分的大小，每 1G 拆分一个日志。
- 默认值：SIZE-MB-1024

#### sys_log_roll_num

- 含义：日志保留的数目。
- 默认值：10

#### sys_log_verbose_modules

- 含义：日志打印的模块。有效值为 BE 的 namespace，包括 `starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream` 以及 `starrocks::workgroup`。
- 默认值：空字符串

#### sys_log_verbose_level

- 含义：日志显示的级别，用于控制代码中 VLOG 开头的日志输出。
- 默认值：10

#### log_buffer_level

- 含义：日志刷盘的策略，默认保持在内存中。
- 默认值：空字符串

#### num_threads_per_core

- 含义：每个 CPU core 启动的线程数。
- 默认值：3

#### compress_rowbatches

- 含义：BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。
- 默认值：TRUE

#### serialize_batch

- 含义：BE 之间 RPC 通信是否序列化 RowBatch，用于查询层之间的数据传输。
- 默认值：FALSE

#### storage_root_path

- 含义：存储数据的目录以及存储介质类型，多块盘配置使用分号 `;` 隔开。<br />如果为 SSD 磁盘，需在路径后添加 `,medium:ssd`，如果为 HDD 磁盘，需在路径后添加 `,medium:hdd`。例如：`/data1,medium:hdd;/data2,medium:ssd`。
- 默认值：`${STARROCKS_HOME}/storage`

#### max_length_for_bitmap_function

- 含义：bitmap 函数输入值的最大长度。
- 单位：字节。
- 默认值：1000000

#### max_length_for_to_base64

- 含义：to_base64() 函数输入值的最大长度。
- 单位：字节
- 默认值：200000

#### max_tablet_num_per_shard

- 含义：每个 shard 的 tablet 数目，用于划分 tablet，防止单个目录下 tablet 子目录过多。
- 默认值：1024

#### file_descriptor_cache_capacity

- 含义：文件句柄缓存的容量。
- 默认值：16384
  
#### min_file_descriptor_number

- 含义：BE 进程的文件句柄 limit 要求的下线。
- 默认值：60000

#### index_stream_cache_capacity

- 含义：BloomFilter/Min/Max 等统计信息缓存的容量。
- 默认值：10737418240

#### base_compaction_num_threads_per_disk

- 含义：每个磁盘 Base Compaction 线程的数目。
- 默认值：1

#### base_cumulative_delta_ratio

- 含义：Base Compaction 触发条件之一：Cumulative 文件大小达到 Base 文件的比例。
- 默认值：0.3

#### compaction_trace_threshold

- 含义：单次 Compaction 打印 trace 的时间阈值，如果单次 compaction 时间超过该阈值就打印 trace。
- 单位：秒
- 默认值：60

#### cumulative_compaction_num_threads_per_disk

- 含义：每个磁盘 Cumulative Compaction 线程的数目。
- 默认值：1

#### vertical_compaction_max_columns_per_group

- 含义：每组 Vertical Compaction 的最大列数。
- 默认值：5

#### enable_check_string_lengths

- 含义：是否在导入时进行数据长度检查，以解决 VARCHAR 类型数据越界导致的 Compaction 失败问题。
- 默认值：true

#### max_row_source_mask_memory_bytes

- 含义：Row source mask buffer 的最大内存占用大小。当 buffer 大于该值时将会持久化到磁盘临时文件中。该值应该小于 `compaction_mem_limit` 参数。
- 单位：字节
- 默认值：209715200

#### be_http_port

- 含义：HTTP Server 端口。
- 默认值：8040

#### webserver_num_workers

- 含义：HTTP Server 线程数。
- 默认值：48

#### load_data_reserve_hours

- 含义：小批量导入生成的文件保留的时。
- 默认值：4

#### streaming_load_rpc_max_alive_time_sec

- 含义：流式导入 RPC 的超时时间。
- 默认值：1200

#### fragment_pool_thread_num_min

- 含义：最小查询线程数，默认启动 64 个线程。
- 默认值：64

#### fragment_pool_thread_num_max

- 含义：最大查询线程数。
- 默认值：4096

#### fragment_pool_queue_size

- 含义：单节点上能够处理的查询请求上限。
- 默认值：2048

#### enable_token_check

- 含义：Token 开启检验。
- 默认值：TRUE

#### enable_prefetch

- 含义：查询提前预取。
- 默认值：TRUE

#### load_process_max_memory_limit_bytes

- 含义：单节点上所有的导入线程占据的内存上限，100GB。
- 单位：字节
- 默认值：107374182400

#### load_process_max_memory_limit_percent

- 含义：单节点上所有的导入线程占据的内存上限比例。
- 默认值：30

#### sync_tablet_meta

- 含义：存储引擎是否开 sync 保留到磁盘上。
- 默认值：FALSE

#### routine_load_thread_pool_size

- 含义：单节点上 Routine Load 线程池大小。从 3.1.0 版本起，该参数已经废弃。单节点上 Routine Load 线程池大小完全由 FE 动态参数`max_routine_load_task_num_per_be` 控制。
- 默认值：10

#### brpc_max_body_size

- 含义：bRPC 最大的包容量。
- 单位：字节
- 默认值：2147483648

#### tablet_map_shard_size

- 含义：Tablet 分组数。
- 默认值：32

#### enable_bitmap_union_disk_format_with_set

- 含义：Bitmap 新存储格式，可以优化 bitmap_union 性能。
- 默认值：FALSE

#### mem_limit

- 含义：BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100G"）。
- 默认值：90%

#### flush_thread_num_per_store

- 含义：每个 Store 用以 Flush MemTable 的线程数。
- 默认值：2

#### datacache_enable

- 含义：是否启用 Data Cache。<ul><li>`true`：启用。</li><li>`false`：不启用，为默认值。</li></ul> 如要启用，设置该参数值为 `true`。
- 默认值：false

#### datacache_disk_path  

- 含义：磁盘路径。支持添加多个路径，多个路径之间使用分号(;) 隔开。建议 BE 机器有几个磁盘即添加几个路径。
- 默认值：N/A

#### datacache_meta_path  

- 含义：Block 的元数据存储目录，可自定义。推荐创建在 **`$STARROCKS_HOME`** 路径下。
- 默认值：N/A

#### datacache_block_size

- 含义：单个 block 大小，单位：字节。默认值为 `1048576`，即 1 MB。
- 默认值：1048576

#### datacache_mem_size

- 含义：内存缓存数据量的上限，可设为比例上限（如 `10%`）或物理上限（如 `10G`, `21474836480` 等）。默认值为 `10%`。推荐将该参数值最低设置成 10 GB。
- 单位：N/A
- 默认值：10%

#### datacache_disk_size

- 含义：单个磁盘缓存数据量的上限，可设为比例上限（如 `80%`）或物理上限（如 `2T`, `500G` 等）。举例：在 `datacache_disk_path` 中配置了 2 个磁盘，并设置 `datacache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 单位：N/A
- 默认值：0

#### jdbc_connection_pool_size

- 含义：JDBC 连接池大小。每个 BE 节点上访问 `jdbc_url` 相同的外表时会共用同一个连接池。
- 默认值：8

#### jdbc_minimum_idle_connections  

- 含义：JDBC 连接池中最少的空闲连接数量。
- 默认值：1

#### jdbc_connection_idle_timeout_ms  

- 含义：JDBC 空闲连接超时时间。如果 JDBC 连接池内的连接空闲时间超过此值，连接池会关闭超过 `jdbc_minimum_idle_connections` 配置项中指定数量的空闲连接。
- 单位：毫秒
- 默认值：600000

#### query_cache_capacity  

- 含义：指定 Query Cache 的大小。单位：字节。默认为 512 MB。最小不低于 4 MB。如果当前的 BE 内存容量无法满足您期望的 Query Cache 大小，可以增加 BE 的内存容量，然后再设置合理的 Query Cache 大小。<br />每个 BE 都有自己私有的 Query Cache 存储空间，BE 只 Populate 或 Probe 自己本地的 Query Cache 存储空间。
- 单位：字节
- 默认值：536870912

#### enable_event_based_compaction_framework  

- 含义：是否开启 Event-based Compaction Framework。`true` 代表开启。`false` 代表关闭。开启则能够在 tablet 数比较多或者单个 tablet 数据量比较大的场景下大幅降低 compaction 的开销。
- 默认值：TRUE

#### lake_service_max_concurrency

- 含义：在存算分离集群中，RPC 请求的最大并发数。当达到此阈值时，新请求会被拒绝。将此项设置为 0 表示对并发不做限制。
- 单位：N/A
- 默认值：0
