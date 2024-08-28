---
displayed_sidebar: docs
keywords: ['Canshu']
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.md'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.md'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.md'

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

### Server

<!--
##### cluster_id

- 默认值：-1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

##### num_threads_per_core

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 CPU Core 启动的线程数。
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

##### compress_rowbatches

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。`true` 表示压缩，`false` 表示不压缩。
- 引入版本：-

<!--
##### rpc_compress_ratio_threshold

- 默认值：1.1
- 类型：Double
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### serialize_batch

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：BE 之间 RPC 通信是否序列化 RowBatch，用于查询层之间的数据传输。`true` 表示序列化，`false` 表示不进行序列化。
- 引入版本：-

#### Thrift

##### be_port

- 默认值：9060
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 上 Thrift Server 的端口，用于接收来自 FE 的请求。
- 引入版本：-

<!--
##### thrift_port

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### thrift_connect_timeout_seconds

- 默认值：3
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### thrift_client_retry_interval_ms

- 默认值：100
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Thrift Client 默认的重试时间间隔。
- 引入版本：-

##### thrift_rpc_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Thrift RPC 超时的时长。
- 引入版本：-

<!--
##### thrift_rpc_strict_mode

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### thrift_rpc_max_body_size

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

#### bRPC

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

<!--
##### brpc_max_connections_per_server

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### brpc_max_body_size

- 默认值：2147483648
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：bRPC 最大的包容量。
- 引入版本：-

<!--
##### brpc_socket_max_unwritten_bytes

- 默认值：1073741824
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_auto_adjust_pagecache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### memory_urgent_level

- 默认值：85
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### memory_high_level

- 默认值：75
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pagecache_adjust_period

- 默认值：20
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### auto_adjust_pagecache_interval_seconds

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

#### Heartbeat

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

<!--
##### report_resource_usage_interval_ms

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### status_report_interval

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：查询汇报 Profile 的间隔，用于 FE 收集查询统计信息。
- 引入版本：-

<!--
##### sleep_one_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### sleep_five_seconds

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### periodic_counter_update_period_ms

- 默认值：500
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Counter 统计信息的间隔。
- 引入版本：-

### 存储

<!--
##### create_tablet_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：The number of threads used to create a tablet. This configuration is changed to dynamic from v3.1.7 onwards.
-->

##### primary_key_limit_size

- 默认值：128
- 类型：Int
- 单位：Byte
- 是否动态：是
- 描述：主键表中单条主键值最大长度。
- 引入版本：v2.5

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

<!--
##### delete_worker_count_normal_priority

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### delete_worker_count_high_priority

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### parallel_clone_task_per_path

- 默认值：8
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### clone_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：克隆的线程数。
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

<!--
##### update_schema_worker_count

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### upload_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### download_worker_count

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### make_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### release_snapshot_worker_count

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

##### memory_limitation_per_thread_for_schema_change

- 默认值：2
- 类型：Int
- 单位：GB
- 是否动态：是
- 描述：单个 Schema Change 任务允许占用的最大内存。
- 引入版本：-

<!--
##### memory_ratio_for_sorting_schema_change

- 默认值：0.8
- 类型：Double
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

<!--
##### replication_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### clear_expired_replcation_snapshots_interval_seconds

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

##### max_tablet_num_per_shard

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 Shard 的 Tablet 数目，用于划分 Tablet，防止单个目录下 Tablet 子目录过多。
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

##### compact_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 引入版本：v3.0.0

<!--
##### compact_thread_pool_queue_size

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### enable_lazy_delta_column_compaction

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

<!--
##### update_compaction_chunk_size_for_row_store

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

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
- 描述：Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 `-1` 代表没有限制。`0` 表示禁用 Compaction。
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
- 描述：Row source mask buffer 的最大内存占用大小。当 buffer 大于该值时将会持久化到磁盘临时文件中。该值应该小于 `compaction_mem_limit` 参数的值。
- 引入版本：-

##### memory_maintenance_sleep_time_s

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：触发 ColumnPool GC 任务的时间间隔。StarRocks 会周期运行 GC 任务，尝试将空闲内存返还给操作系统。
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

<!--
##### storage_high_usage_disk_protect_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

##### tablet_stat_cache_update_interval_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Tablet Stat Cache 的更新间隔。
- 引入版本：-

##### enable_bitmap_union_disk_format_with_set

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Bitmap 新存储格式，可以优化 bitmap_union 性能。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

<!--
##### l0_l1_merge_ratio

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### l0_max_file_size

- 默认值：209715200
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### l0_min_mem_usage

- 默认值：2097152
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### l0_max_mem_usage

- 默认值：104857600
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### l0_snapshot_size

- 默认值：16777216
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_tmp_l1_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_parallel_get_and_bf

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pindex_minor_compaction

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_allow_pindex_l2_num

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pindex_major_compaction_num_threads

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### pindex_major_compaction_limit_per_disk

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题。
- 引入版本：v3.0.9

<!--
##### pindex_major_compaction_schedule_interval_seconds

- 默认值：15
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pindex_shared_data_gc_evict_interval_seconds

- 默认值：18000
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pindex_filter

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pindex_compression

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_bf_read_bytes_percent

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pindex_rebuild_in_compaction

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

### 导入导出

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

<!--
##### transaction_apply_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### get_pindex_worker_count

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于 Stream Load 的线程数。自 v3.1.7 起变为动态参数。
- 引入版本：-

<!--
##### max_queueing_memtable_per_tablet

- 默认值：2
- 类型：Int
- 单位：
- 是否动态：是
- 描述：每个 tablet 上排队的 memtable 个数。用于控制导入内存使用量。
- 引入版本：v3.1
-->

<!--
##### stale_memtable_flush_time_sec

- 默认值：0
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：0表示禁止，其他上次更新时间大于stale_memtable_flush_time_sec的memtable会在内存不足时持久化
- 引入版本：-
-->

<!--
##### dictionary_encoding_ratio

- 默认值：0.7
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### dictionary_page_size

- 默认值：1048576
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### dictionary_encoding_ratio_for_non_string_column

- 默认值：0
- 类型：Double
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### dictionary_speculate_min_chunk_size

- 默认值：10000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_streaming_load_thread_pool

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### streaming_load_thread_pool_num_min

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### streaming_load_thread_pool_idle_time_ms

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

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
- 描述：The maximum size of a JSON file that can be streamed into StarRocks.
- 引入版本：-

##### streaming_load_rpc_max_alive_time_sec

- 默认值：1200
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：流式导入单个 JSON 文件大小的上限。
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

##### txn_commit_rpc_timeout_ms (Deprecated)

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Transaction Commit RPC 超时的时长。该参数自 v3.1.0 起弃用。
- 引入版本：-

##### max_consumer_num_per_group

- 默认值：3
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Routine load 中，每个 Consumer Group 内最大的 Consumer 数量。
- 引入版本：-

<!--
##### max_pulsar_consumer_num_per_group

- 默认值：10
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### routine_load_kafka_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### routine_load_pulsar_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### udf_thread_pool_size

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### port

- 默认值：20001
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### broker_write_timeout_seconds

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### scanner_row_num

- 默认值：16384
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个扫描线程单次执行最多返回的数据行数。
- 引入版本：-

<!--
##### max_hdfs_scanner_num

- 默认值：50
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### sorter_block_size

- 默认值：8388608
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### column_dictionary_key_ratio_threshold

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### column_dictionary_key_size_threshold

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->


<!--
##### profile_report_interval

- 默认值：30
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

##### index_stream_cache_capacity

- 默认值：10737418240
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：BloomFilter/Min/Max 等统计信息缓存的容量。
- 引入版本：-

##### storage_page_cache_limit

- 默认值：20%
- 类型：String
- 单位：-
- 是否动态：是
- 描述：PageCache 的容量，可写为容量大小，例如： `20G`、`20480M`、`20971520K` 或 `21474836480B`。也可以写为 PageCache 占系统内存的比例，例如，`20%`。该参数仅在 `disable_storage_page_cache` 为 `false` 时生效。
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

<!--
##### enable_bitmap_index_memory_page_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Bitmap index 开启 Memory Cache。使用 Bitmap index 加速点查时，可以考虑开启。
- 引入版本：v3.1
-->

<!--
##### enable_zonemap_index_memory_page_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_ordinal_index_memory_page_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### disable_column_pool

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### query_scratch_dirs

- 默认值：`${STARROCKS_HOME}`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_free_io_buffers

- 默认值：128
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### disable_mem_pools

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### use_mmap_allocate_chunk

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### chunk_reserved_bytes_limit

- 默认值：2147483648
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pprof_profile_dir

- 默认值：`${STARROCKS_HOME}/log`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### enable_prefetch

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启查询提前预取。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

<!--
##### query_max_memory_limit_percent

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### pipeline_scan_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### pipeline_connector_scan_thread_num_per_cpu

- 默认值：8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：BE 节点中每个 CPU 核心分配给 Pipeline Connector 的扫描线程数量。自 v3.1.7 起变为动态参数。
- 引入版本：-

<!--
##### pipeline_scan_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_exec_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_prepare_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_prepare_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_sink_io_thread_pool_thread_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_sink_io_thread_pool_queue_size

- 默认值：102400
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_sink_buffer_size

- 默认值：64
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_sink_brpc_dop

- 默认值：64
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_max_num_drivers_per_exec_thread

- 默认值：10240
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_print_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_driver_queue_level_time_slice_base_ns

- 默认值：200000000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：pipeline 执行线程多级反馈队列的调度参数，最低优先级队列的时间片。
- 引入版本：v3.1
-->

<!--
##### pipeline_driver_queue_ratio_of_adjacent_queue

- 默认值：1.2
- 类型：Double
- 单位：
- 是否动态：否
- 描述：pipeline 执行线程多级反馈队列的调度参数。相邻两级队列时间片的倍数关系。
- 引入版本：v3.1
-->

<!--
##### pipeline_scan_queue_mode

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：控制 Scan Task 的调度策略。`0` 表示 PriorityScanTaskQueue，`1` 表示 MultiLevelFeedScanTaskQueue。
  - PriorityScanTaskQueue 以提交的任务次数为优先级。
  - MultiLevelFeedScanTaskQueue 以最短执行时间为优先级。
- 引入版本：v3.1
-->

<!--
##### pipeline_scan_queue_level_time_slice_base_ns

- 默认值：100000000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：控制 MultiLevelFeedScanTaskQueue 的调度参数，在 `pipeline_scan_queue_mode` 为 1 时有效。
最低优先级队列的时间片。
- 引入版本：v3.1
-->

<!--
##### pipeline_scan_queue_ratio_of_adjacent_queue

- 默认值：1.5
- 类型：Double
- 单位：
- 是否动态：否
- 描述：控制 MultiLevelFeedScanTaskQueue 的调度参数，在 `pipeline_scan_queue_mode` 为 1 时有效。
相邻两级队列时间片的倍数关系。
- 引入版本：v3.1
-->

<!--
##### pipeline_analytic_max_buffer_size

- 默认值：128
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_analytic_removable_chunk_num

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_analytic_enable_streaming_process

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipeline_analytic_enable_removable_cumulative_process

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### tablet_internal_parallel_min_splitted_scan_rows

- 默认值：16384
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_internal_parallel_max_splitted_scan_rows

- 默认值：1048576
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_internal_parallel_max_splitted_scan_bytes

- 默认值：536870912
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_internal_parallel_min_scan_dop

- 默认值：4
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### bitmap_serialize_version

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### max_hdfs_file_handle

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多可以打开的 HDFS 文件描述符数量。
- 引入版本：-

<!--
##### max_segment_file_size

- 默认值：1073741824
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### rewrite_partial_segment

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_access_key_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_secret_access_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_bucket

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_region

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_max_connection

- 默认值：102400
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_endpoint_use_https

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### object_storage_endpoint_path_style_access

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### text_io_range_size

- 默认值：16777216
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_orc_late_materialization

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_orc_libdeflate_decompression

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### orc_natural_read_size

- 默认值：8388608
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### orc_coalesce_read_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### orc_tiny_stripe_threshold_size

- 默认值：8388608
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### parquet_coalesce_read_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### parquet_late_materialization_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用延迟物化优化 Parquet 读性能。
- 引入版本：-


##### parquet_late_materialization_v2_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 v2 版延迟物化优化 Parquet 读性能。v3.2 版本支持两个版本的 Parquet Reader 延迟物化，v3.3 版本仅保留 `parquet_late_materialization_enable` 延迟物化，并删除该变量。
- 引入版本：v3.2

##### parquet_page_index_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 Parquet Pageindex 信息优化读性能。
- 引入版本：v3.3

<!--
##### io_coalesce_read_max_buffer_size

- 默认值：8388608
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### io_coalesce_read_max_distance_size

- 默认值：1048576
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### io_coalesce_adaptive_lazy_active

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：根据谓词选择度，自适应决定是否将谓词列 IO 和非谓词列 IO 进行合并。
- 引入版本：v3.2

<!--
##### io_tasks_per_scan_operator

- 默认值：4
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_per_scan_operator

- 默认值：16
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_min_size

- 默认值：2
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_adjust_interval_ms

- 默认值：50
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_adjust_step

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_adjust_smooth

- 默认值：4
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### connector_io_tasks_slow_io_latency_ms

- 默认值：50
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### scan_use_query_mem_ratio

- 默认值：0.25
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### connector_scan_use_query_mem_ratio

- 默认值：0.3
- 类型：Double
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

<!--
##### hdfs_client_max_cache_size

- 默认值：64
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### hdfs_client_io_read_retry

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### aws_sdk_logging_trace_enabled

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### aws_sdk_logging_trace_level

- 默认值：trace
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### aws_sdk_enable_compliant_rfc3986_encoding

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 RFC-3986 编码。从 Google GCS 查询数据时，如果 Object.key 包含特殊字符（例如 `=`，`$`），由于 result URL 未解析这些字符，会导致认证失败。开启 RFC-3986 编码能确保字符正确编码。该特性对于 Hive 分区表非常重要。如果使用 OBS 或 KS3 对象存储，需要在 `be.conf` 开启该参数，不然访问不通。
- 引入版本：v3.1

<!--
##### experimental_s3_max_single_part_size

- 默认值：16777216
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_s3_min_upload_part_size

- 默认值：16777216
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_load_dop

- 默认值：16
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_load_colocate_mv

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### meta_threshold_to_manual_compact

- 默认值：10737418240
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### manual_compact_before_data_dir_load

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### deliver_broadcast_rf_passthrough_bytes_limit

- 默认值：131072
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### deliver_broadcast_rf_passthrough_inflight_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### send_rpc_runtime_filter_timeout_ms

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### send_runtime_filter_via_http_rpc_min_size

- 默认值：67108864
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### rpc_connect_timeout_ms

- 默认值：30000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_batch_publish_latency_ms

- 默认值：100
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### jaeger_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### query_debug_trace_dir

- 默认值：`${STARROCKS_HOME}/query_debug_trace`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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

##### json_flat_internal_column_min_limit

- 默认值：5
- 类型：Int
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，JSON 内部字段数量限制，低于该数量的 JSON 不执行 Flat JSON 优化，默认为 5。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

##### json_flat_column_max

- 默认值：20
- 类型：Int
- 单位：
- 是否动态：是
- 描述：控制 Flat JSON 时，最多提取的子列数量，默认为 20。该参数仅在 `enable_json_flat` 为 `true` 时生效。
- 引入版本：v3.3.0

### 存算分离

##### starlet_port

- 默认值：9070
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：BE 和 CN 的额外 Agent 服务端口。
- 引入版本：-

<!--
##### starlet_cache_thread_num

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_cache_dir

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_cache_check_interval

- 默认值：900
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_cache_evict_interval

- 默认值：60
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：在存算分离模式下启用 file data cache，系统进行缓存淘汰（Cache Eviction）的间隔。
- 引入版本：v3.0
-->

<!--
##### starlet_cache_evict_low_water

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：在存算分离模式下启用 file data cache，如果当前剩余磁盘空间（百分比）低于此配置项中指定的值，将会触发缓存淘汰。
- 引入版本：v3.0
-->
  
<!--  
##### starlet_cache_evict_high_water

- 默认值：0.2
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：在存算分离模式下启用 file data cache，如果当前剩余磁盘空间（百分比）高于此配置项中指定的值，将会停止缓存淘汰。
- 引入版本：v3.0
-->
  
<!--
##### starlet_cache_dir_allocate_policy

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_cache_evict_percent

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_cache_evict_throughput_mb

- 默认值：200
- 类型：Int
- 单位：MB
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fs_stream_buffer_size_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### starlet_use_star_cache

- 默认值：false（v3.1）true（v3.2.3 起）
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离模式下是否使用 Data Cache。`true` 表示启用该功能，`false` 表示禁用。自 v3.2.3 起，默认值由 `false` 调整为 `true`。
- 引入版本：v3.1

<!--
##### starlet_star_cache_mem_size_percent

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### starlet_star_cache_disk_size_percent

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离模式下，Data Cache 最多可使用的磁盘容量百分比。
- 引入版本：v3.1

<!--
##### starlet_star_cache_disk_size_bytes

- 默认值：0
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_star_cache_block_size_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_s3_virtual_address_domainlist

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_s3_client_max_cache_capacity

- 默认值：8
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_s3_client_num_instances_per_cache

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fs_read_prefetch_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fs_read_prefetch_threadpool_size

- 默认值：128
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fslib_s3client_nonread_max_retries

- 默认值：5
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fslib_s3client_nonread_retry_scale_factor

- 默认值：200
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### starlet_fslib_s3client_connect_timeout_ms

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_metadata_cache_limit

- 默认值：2147483648
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_print_delete_log

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_compaction_stream_buffer_size_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存算分离集群 Compaction 任务在远程 FS 读 I/O 阶段的 Buffer 大小。默认值为 1MB。您可以适当增大该配置项取值以加速 Compaction 任务。
- 引入版本：v3.2.3

<!--
##### experimental_lake_ignore_lost_segment

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_lake_wait_per_put_ms

- 默认值：0
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_lake_wait_per_get_ms

- 默认值：0
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_lake_wait_per_delete_ms

- 默认值：0
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_lake_ignore_pk_consistency_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_publish_version_slow_log_ms

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_enable_publish_version_trace_log

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_vacuum_retry_pattern

- 默认值：*request rate*
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_vacuum_retry_max_attempts

- 默认值：5
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_vacuum_retry_min_delay_ms

- 默认值：100
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_max_garbage_version_distance

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_primary_key_recover

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_enable_compaction_async_write

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_pk_compaction_max_input_rowsets

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。从 v3.2.4 和 v3.1.10 版本开始，该参数默认值从 `5` 变更为 `1000`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 引入版本：v3.1.8, v3.2.3

<!--
##### lake_pk_preload_memory_limit_percent

- 默认值：30
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### dependency_librdkafka_debug_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### dependency_librdkafka_debug

- 默认值：all
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### loop_count_wait_fragments_finish

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

### 数据湖

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

<!--
##### spill_local_storage_dir

- 默认值：`${STARROCKS_HOME}/spill`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### experimental_spill_skip_sync

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### spill_init_partition

- 默认值：16
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### spill_max_partition_level

- 默认值：7
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### spill_max_partition_size

- 默认值：1024
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### spill_max_log_block_container_bytes

- 默认值：10737418240
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### spill_max_dir_bytes_ratio

- 默认值：0.8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### internal_service_query_rpc_thread_num

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### cardinality_of_inject

- 默认值：10
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### directory_of_inject

- 默认值：/src/exec/pipeline/hashjoin,/src/exec/pipeline/scan,/src/exec/pipeline/aggregate,/src/exec/pipeline/crossjoin,/src/exec/pipeline/sort,/src/exec/pipeline/exchange,/src/exec/pipeline/analysis
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### datacache_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用 Data Cache。`true` 表示启用，`false` 表示不启用。
- 引入版本：-

##### datacache_mem_size

- 默认值：10%
- 类型：String
- 单位：-
- 是否动态：否
- 描述：内存缓存数据量的上限，可设为比例上限（如 `10%`）或物理上限（如 `10G`, `21474836480` 等）。默认值为 `10%`。推荐将该参数值最低设置成 10 GB。
- 引入版本：-

##### datacache_disk_size

- 默认值：0
- 类型：String
- 单位：-
- 是否动态：否
- 描述：单个磁盘缓存数据量的上限，可设为比例上限（如 `80%`）或物理上限（如 `2T`, `500G` 等）。举例：在 `datacache_disk_path` 中配置了 2 个磁盘，并设置 `datacache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 引入版本：-

##### datacache_disk_path

- 默认值：`${STARROCKS_HOME}/datacache/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：磁盘路径。支持添加多个路径，多个路径之间使用分号(;) 隔开。建议 BE 机器有几个磁盘即添加几个路径。
- 引入版本：-

##### datacache_meta_path

- 默认值：`${STARROCKS_HOME}/datacache/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Block 的元数据存储目录，可自定义。推荐创建在 `$STARROCKS_HOME` 路径下。
- 引入版本：-

##### datacache_auto_adjust_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘容量自动调整开关，启用后会根据当前磁盘使用率动态调整缓存容量。
- 引入版本：v3.3.0

##### datacache_disk_high_level

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘高水位（百分比）。当磁盘使用率高于该值时，系统自动淘汰 Data Cache 中的缓存数据。
- 引入版本：v3.3.0

##### datacache_disk_safe_level

- 默认值：70
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘安全水位（百分比）。当 Data Cache 进行缓存自动扩缩容时，系统将尽可能以该阈值为磁盘使用率目标调整缓存容量。
- 引入版本：v3.3.0

##### datacache_disk_low_level

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Data Cache 磁盘低水位（百分比）。当磁盘使用率在 `datacache_disk_idle_seconds_for_expansion` 指定的时间内持续低于该值，且用于缓存数据的空间已经写满时，系统将自动进行缓存扩容，增加缓存上限。
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

<!--
##### datacache_block_size

- 默认值：262144
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_checksum_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_direct_io_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_max_concurrent_inserts

- 默认值：1500000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_max_flying_memory_mb

- 默认值：256
- 类型：Int
- 单位：MB
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_adaptor_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_skip_read_factor

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_block_buffer_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### datacache_engine

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### report_datacache_metrics_interval_ms

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_disk_size

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_disk_path

- 默认值：`${STARROCKS_HOME}/block_cache/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_meta_path

- 默认值：`${STARROCKS_HOME}/block_cache/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_block_size

- 默认值：262144
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_mem_size

- 默认值：2147483648
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_max_concurrent_inserts

- 默认值：1500000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_checksum_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_direct_io_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### block_cache_engine

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### query_cache_num_lanes_per_driver

- 默认值：4
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### send_channel_buffer_limit

- 默认值：67108864
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### exception_stack_level

- 默认值：1
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### exception_stack_white_list

- 默认值：std::
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### exception_stack_black_list

- 默认值：apache::thrift::,ue2::,arangodb::
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### rocksdb_cf_options_string

- 默认值：block_based_table_factory={block_cache={capacity=256M;num_shard_bits=0}}
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### local_exchange_buffer_mem_limit_per_driver

- 默认值：134217728
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### streaming_agg_limited_memory_size

- 默认值：134217728
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### streaming_agg_chunk_buffer_size

- 默认值：1024
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### wait_apply_time

- 默认值：6000
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### binlog_file_max_size

- 默认值：536870912
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### binlog_page_max_size

- 默认值：1048576
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### txn_info_history_size

- 默认值：20000
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### file_write_history_size

- 默认值：10000
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### update_cache_evict_internal_sec

- 默认值：11
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_auto_evict_update_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### load_tablet_timeout_seconds

- 默认值：60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pk_value_column_zonemap

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### primary_key_batch_get_index_memory_limit

- 默认值：104857600
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_short_key_for_one_column_filter

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：如果 sort key 是高基数，可以考虑开启该特性加速点查。
- 引入版本：v3.1
-->

<!--
##### enable_http_stream_load_limit

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### finish_publish_version_internal

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### get_txn_status_internal_sec

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### dump_metrics_with_bvar

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_drop_tablet_if_unfinished_txn

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_service_max_concurrency

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：在存算分离集群中，RPC 请求的最大并发数。当达到此阈值时，新请求会被拒绝。将此项设置为 `0` 表示对并发不做限制。
- 引入版本：-

<!--
##### lake_vacuum_min_batch_delete_size

- 默认值：100
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### desc_hint_split_range

- 默认值：10
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_local_pk_index_unused_threshold_seconds

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_enable_vertical_compaction_fill_data_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，是否允许 Compaction 任务在执行时缓存数据到本地磁盘上。`true` 表示启用，`false` 表示不启用。
- 引入版本：v3.1.7, v3.2.3

<!--
##### dictionary_cache_refresh_timeout_ms

- 默认值：60000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### dictionary_cache_refresh_threadpool_size

- 默认值：8
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_json_flat

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### json_flat_null_factor

- 默认值：0.3
- 类型：Double
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### json_flat_sparsity_factor

- 默认值：0.9
- 类型：Double
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### json_flat_internal_column_min_limit

- 默认值：5
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### json_flat_column_max

- 默认值：20
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pk_dump_interval_seconds

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_profile_for_external_plan

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### olap_string_max_length

- 默认值：1048576
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### jit_lru_cache_size

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

### 其他

##### user_function_dir

- 默认值：`${STARROCKS_HOME}/lib/udf`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：UDF 存放的路径。
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

<!--
##### pull_load_task_dir

- 默认值：`${STARROCKS_HOME}/var/pull_load`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### web_log_bytes

- 默认值：1048576
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### be_service_threads

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### default_query_options

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### local_library_dir

- 默认值：`${UDF_RUNTIME_DIR}`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### tablet_writer_open_rpc_timeout_sec

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### make_snapshot_rpc_timeout_ms

- 默认值：20000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### olap_table_sink_send_interval_ms

- 默认值：10
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### enable_token_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Token 检验。`true` 表示开启，`false` 表示不开启。
- 引入版本：-

<!--
##### enable_system_metrics

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### num_cores

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ignore_broken_disk

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### scratch_dirs

- 默认值：/tmp
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### madvise_huge_pages

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### mmap_buffers

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### memory_max_alignment

- 默认值：16
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### consistency_max_memory_limit

- 默认值：10G
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### consistency_max_memory_limit_percent

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### update_memory_limit_percent

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### priority_queue_remaining_tasks_increased_frequency

- 默认值：512
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_metric_calculator

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### es_scroll_keepalive

- 默认值：5m
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### es_http_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### es_index_max_result_window

- 默认值：10000
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_client_cache_size_per_host

- 默认值：10
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### small_file_dir

- 默认值：`${STARROCKS_HOME}/lib/small_file/`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：保存文件管理器下载的文件的目录。
- 引入版本：-

<!--
##### path_gc_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### path_gc_check_interval_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pk_index_map_shard_size

- 默认值：4096
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### plugin_path

- 默认值：`${STARROCKS_HOME}/plugin`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### txn_map_shard_size

- 默认值：128
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### txn_shard_size

- 默认值：1024
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ignore_load_tablet_failure

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ignore_rowset_stale_unconsistent_delete

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### vector_chunk_size

- 默认值：4096
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### late_materialization_ratio

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### metric_late_materialization_ratio

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_transmit_batched_bytes

- 默认值：262144
- 类型：Int
- 单位：Bytes
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bitmap_max_filter_items

- 默认值：30
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bitmap_max_filter_ratio

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bitmap_filter_enable_not_equal

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### storage_format_version

- 默认值：2
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### null_encoding

- 默认值：0
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pre_aggregate_factor

- 默认值：80
- 类型：Int
- 单位：
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### sys_minidump_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### sys_minidump_dir

- 默认值：`${STARROCKS_HOME}`
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### sys_minidump_max_files

- 默认值：16
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### sys_minidump_limit

- 默认值：20480
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### sys_minidump_interval

- 默认值：600
- 类型：Int
- 单位：
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### dump_trace_info

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

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
