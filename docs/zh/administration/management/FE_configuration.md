---
displayed_sidebar: docs
keywords: ['Canshu']
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.md'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.md'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.md'

# FE 配置项

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端执行 ADMIN SHOW FRONTEND CONFIG 命令来查看参数配置。如果您想查看具体参数的配置，执行如下命令：

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

详细的命令返回字段解释，参见 [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以通过 [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令在线修改 FE 动态参数。

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

## FE 参数描述

### 日志

##### log_roll_size_mb

- 默认值：1024
- 类型：Int
- 单位：MB
- 是否动态：否
- 描述：单个系统日志或审计日志文件的大小上限。
- 引入版本：-

##### sys_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志文件的保存目录。
- 引入版本：-

##### sys_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志的级别，从低到高依次为 `INFO`、`WARN`、`ERROR`、`FATAL`。
- 引入版本：-

##### sys_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `sys_log_roll_interval` 时间段内，允许保留的系统日志文件的最大数目。
- 引入版本：-

##### sys_log_verbose_modules

- 默认值：空字符串
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：打印系统日志的模块。如果设置参数取值为 `org.apache.starrocks.catalog`，则表示只打印 Catalog 模块下的日志。
- 引入版本：-

##### sys_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。
  - 取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。
  - 取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 引入版本：-

##### sys_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志文件的保留时长。默认值 `7d` 表示系统日志文件可以保留 7 天，保留时长超过 7 天的系统日志文件会被删除。
- 引入版本：-

<!--
##### sys_log_roll_mode (Deprecated)

- 默认值：SIZE-MB-1024
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### sys_log_to_console

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### audit_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：审计日志文件的保存目录。
- 引入版本：-

##### audit_log_roll_num

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `audit_log_roll_interval` 时间段内，允许保留的审计日志文件的最大数目。
- 引入版本：-

##### audit_log_modules

- 默认值：slow_query, query
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：打印审计日志的模块。默认打印 `slow_query` 和 `query` 模块的日志。自 v3.0 起 支持 `connection` 模块，即连接日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 引入版本：-

##### qe_slow_log_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：Slow query 的认定时长。如果查询的响应时间超过此阈值，则会在审计日志 `fe.audit.log` 中记录为 slow query。
- 引入版本：-

##### audit_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：审计日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。
  - 取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。
  - 取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 引入版本：-

##### audit_log_delete_age

- 默认值：30d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：审计日志文件的保留时长。默认值 `30d` 表示审计日志文件可以保留 30 天，保留时长超过 30 天的审计日志文件会被删除。
- 引入版本：-

<!--
##### slow_lock_threshold_ms

- 默认值：3000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### slow_lock_log_every_ms

- 默认值：3000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### custom_config_dir

- 默认值：/conf
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### internal_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### internal_log_roll_num

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### internal_log_modules

- 默认值：{"base", "statistic"}
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### internal_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### internal_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### dump_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Dump 日志文件的保存目录。
- 引入版本：-

##### dump_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `dump_log_roll_interval` 时间内，允许保留的 Dump 日志文件的最大数目。
- 引入版本：-

##### dump_log_modules

- 默认值：query
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：打印 Dump 日志的模块。默认打印 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 引入版本：-

##### dump_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Dump 日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。
  - 取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。
  - 取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 引入版本：-

##### dump_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Dump 日志文件的保留时长。默认值 `7d` 表示 Dump 日志文件可以保留 7 天，保留时长超过 7 天的 Dump 日志文件会被删除。
- 引入版本：-

<!--
##### big_query_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### big_query_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### big_query_log_modules

- 默认值：query
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### big_query_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### big_query_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### log_plan_cancelled_by_crash_be

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在查询异常结束时，打印 COSTS 计划到日志中。只有在 `enable_collect_query_detail_info` 为 `false` 时有效，因为 `enable_collect_query_detail_info` 为 `true` 时，plan 会被记录到 query detail 中。
- 引入版本：v3.1
-->

<!--
##### log_register_and_unregister_query_id

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

### Server

##### frontend_address

- 默认值：0.0.0.0
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 节点的 IP 地址。
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

##### http_port

- 默认值：8030
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 节点上 HTTP 服务器的端口。
- 引入版本：-

##### http_worker_threads_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Http Server 用于处理 HTTP 请求的线程数。如果配置为负数或 0 ，线程数将设置为 CPU 核数的 2 倍。
- 引入版本：v2.5.18，v3.0.10，v3.1.7，v3.2.2

##### http_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HTTP 服务器支持的 Backlog 队列长度。
- 引入版本：-

<!--
##### http_max_initial_line_length

- 默认值：4096
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### http_max_header_size

- 默认值：32768
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### http_max_chunk_size

- 默认值：8192
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### http_web_page_display_hardware

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_http_detail_metrics

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### cluster_name

- 默认值：StarRocks Cluster
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群的名称，显示为网页标题。
- 引入版本：-

##### rpc_port

- 默认值：9020
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 节点上 Thrift 服务器的端口。
- 引入版本：-

##### thrift_server_max_worker_threads

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：Thrift 服务器支持的最大工作线程数。
- 引入版本：-

##### thrift_server_queue_size

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Thrift 服务器 pending 队列长度。如果当前处理线程数量超过了配置项 `thrift_server_max_worker_threads` 的值，则将超出的线程加入 Pending 队列。
- 引入版本：-

##### thrift_client_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：Thrift 客户端链接的空闲超时时间，即链接超过该时间无新请求后则将链接断开。
- 引入版本：-

##### thrift_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Thrift 服务器支持的 Backlog 队列长度。
- 引入版本：-

<!--
##### thrift_rpc_timeout_ms

- 默认值：10000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### thrift_rpc_retry_times

- 默认值：3
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

- 默认值：-1
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### brpc_connection_pool_size

- 默认值：16
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### brpc_idle_wait_max_time

- 默认值：10000
- 类型：Int
- 单位：ms
- 是否动态：否
- 描述：bRPC 的空闲等待时间。
- 引入版本：-

##### query_port

- 默认值：9030
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 节点上 MySQL 服务器的端口。
- 引入版本：-

##### mysql_nio_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器支持的 Backlog 队列长度。
- 引入版本：-

##### mysql_service_io_threads_num

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器中用于处理 I/O 事件的最大线程数。
- 引入版本：-

##### max_mysql_service_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器中用于处理任务的最大线程数。
- 引入版本：-

<!--
##### max_http_sql_service_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### mysql_server_version

- 默认值：5.1.0
- 类型：String
- 单位：-
- 是否动态：是
- 描述：MySQL 服务器的版本。修改该参数配置会影响以下场景中返回的版本号：
  1. `select version();`
  2. Handshake packet 版本
  3. 全局变量 `version` 的取值 (`show variables like 'version';`)
- 引入版本：-

##### qe_max_connection

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 支持的最大连接数，包括所有用户发起的连接。默认值由 v3.1.12、v3.2.7 起由 `1024` 变为 `4096`。
- 引入版本：-

### 元数据与集群管理

##### cluster_id

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群的 ID。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。默认值 `-1` 表示在 Leader FE 首次启动时随机生成一个。
- 引入版本：-

##### meta_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：元数据的保存目录。
- 引入版本：-

##### edit_log_type

- 默认值：BDB
- 类型：String
- 单位：-
- 是否动态：否
- 描述：编辑日志的类型。取值只能为 `BDB`。
- 引入版本：-

##### edit_log_port

- 默认值：9010
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中各 Leader FE、Follower FE、Observer FE 之间通信用的端口。
- 引入版本：-

##### edit_log_roll_num

- 默认值：50000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：该参数用于控制日志文件的大小，指定了每写多少条元数据日志，执行一次日志滚动操作来为这些日志生成新的日志文件。新日志文件会写入到 BDBJE Database。
- 引入版本：-

<!--
##### edit_log_write_slow_log_threshold_ms

- 默认值：2000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### ignore_unknown_log_id

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否忽略未知的 logID。当 FE 回滚到低版本时，可能存在低版本 FE 无法识别的 logID。如果设置为 `TRUE`，则 FE 会忽略这些 logID；否则 FE 会退出。
- 引入版本：-

<!--
##### hdfs_read_buffer_size_kb

- 默认值：8192
- 类型：Int
- 单位：KB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### hdfs_write_buffer_size_kb

- 默认值：1024
- 类型：Int
- 单位：KB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### hdfs_file_system_expire_seconds

- 默认值：300
- 别名：hdfs_file_sytem_expire_seconds
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### meta_delay_toleration_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：FE 所在 StarRocks 集群中，非 Leader FE 能够容忍的元数据落后的最大时间。如果非 Leader FE 上的元数据与 Leader FE 上的元数据之间的延迟时间超过该参数取值，则该非 Leader FE 将停止服务。
- 引入版本：-

##### master_sync_policy

- 默认值：SYNC
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中，Leader FE 上的日志刷盘方式。该参数仅在当前 FE 为 Leader 时有效。取值范围：
  - `SYNC`：事务提交时同步写日志并刷盘。
  - `NO_SYNC`：事务提交时不同步写日志。
  - `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。
  
  如果您只部署了一个 Follower FE，建议将其设置为 `SYNC`。 如果您部署了 3 个及以上 Follower FE，建议将其与下面的 `replica_sync_policy` 均设置为 `WRITE_NO_SYNC`。

- 引入版本：-

##### replica_sync_policy

- 默认值：SYNC
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中，Follower FE 上的日志刷盘方式。取值范围：
  - `SYNC`：事务提交时同步写日志并刷盘。
  - `NO_SYNC`：事务提交时不同步写日志。
  - `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。
- 引入版本：-

##### replica_ack_policy

- 默认值：SIMPLE_MAJORITY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：判定日志是否有效的策略，默认值表示多数 Follower FE 返回确认消息，就认为生效。
- 引入版本：-

##### bdbje_heartbeat_timeout_second

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中 Leader FE 和 Follower FE 之间的 BDB JE 心跳超时时间。
- 引入版本：-

##### bdbje_replica_ack_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中，元数据从 Leader FE 写入到多个 Follower FE 时，Leader FE 等待足够多的 Follower FE 发送 ACK 消息的超时时间。当写入的元数据较多时，可能返回 ACK 的时间较长，进而导致等待超时。如果超时，会导致写元数据失败，FE 进程退出，此时可以适当地调大该参数取值。
- 引入版本：-

##### bdbje_lock_timeout_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：BDB JE 操作的锁超时时间。
- 引入版本：-

##### bdbje_reset_election_group

- 默认值：false
- 类型：String
- 单位：-
- 是否动态：否
- 描述：是否重置 BDBJE 复制组。如果设置为 `TRUE`，FE 将重置 BDBJE 复制组（即删除所有 FE 节点的信息）并以 Leader 身份启动。重置后，该 FE 将成为集群中唯一的成员，其他 FE 节点通过 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入该集群。仅当无法成功选举出 leader FE 时（因为大部分 follower FE 数据已损坏）才使用此配置。该参数用来替代 `metadata_failure_recovery`。
- 引入版本：-

##### max_bdbje_clock_delta_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中 Leader FE 与非 Leader FE 之间能够容忍的最大时钟偏移。
- 引入版本：-

<!--
##### bdbje_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bdbje_cleaner_threads

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bdbje_replay_cost_percent

- 默认值：150
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### bdbje_reserved_disk_size

- 默认值：512 * 1024 * 1024
- 类型：Long
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### txn_rollback_limit

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：允许回滚的最大事务数。
- 引入版本：-

##### heartbeat_mgr_threads_num

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中用于发送心跳任务的最大线程数。
- 引入版本：-

##### heartbeat_mgr_blocking_queue_size

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中存储心跳任务的阻塞队列大小。
- 引入版本：-

##### catalog_try_lock_timeout_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：全局锁（Global Lock）获取的超时时长。
- 引入版本：-

##### ignore_materialized_view_error

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否忽略因物化视图错误导致的元数据异常。如果 FE 因为物化视图错误导致的元数据异常而无法启动，您可以通过将该参数设置为 `true` 以忽略错误。
- 引入版本：v2.5.10

##### ignore_meta_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否忽略元数据落后的情形。如果为 true，非主 FE 将忽略主 FE 与其自身之间的元数据延迟间隙，即使元数据延迟间隙超过 `meta_delay_toleration_second`，非主 FE 仍将提供读取服务。当您尝试停止 Leader FE 较长时间，但仍希望非 Leader FE 可以提供读取服务时，该参数会很有帮助。
- 引入版本：-

##### drop_backend_after_decommission

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：BE 被下线后，是否删除该 BE。true 代表 BE 被下线后会立即删除该 BE。False 代表下线完成后不删除 BE。
- 引入版本：-

##### enable_collect_query_detail_info

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否收集查询的 Profile 信息。设置为 `true` 时，系统会收集查询的 Profile。设置为 `false` 时，系统不会收集查询的 profile。
- 引入版本：-

##### enable_background_refresh_connector_metadata

- 默认值：true in v3.0 and later and false in v2.5
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。
- 引入版本：v2.5.5

<!--
##### enable_background_refresh_resource_table_metadata

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### background_refresh_file_metadata_concurrency

- 默认值：4
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### background_refresh_metadata_interval_millis

- 默认值：600000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：接连两次 Hive 元数据缓存刷新之间的间隔。
- 引入版本：v2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- 默认值：3600 * 24
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。
- 引入版本：v2.5.5

##### enable_statistics_collect_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：统计信息查询时是否生成 Profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计查询生成 Profile。
- 引入版本：v3.1.5

#### metadata_enable_recovery_mode

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启元数据恢复模式。开启此模式后，在部分元数据丢失的情况下，系统会根据 BE 上的信息恢复元数据。当前仅支持恢复分区的版本信息。
- 引入版本：v3.3.0

#### lock_manager_enabled

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启锁管理。lock manager 可以对锁实现集中管理，例如控制是否将元数据锁的粒度从库级别细化为表级别。
- 引入版本：v3.3.0

##### lock_manager_enable_using_fine_granularity_lock

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否将元数据锁的粒度从库级别细化为表级别。元数据锁细化为表级别后，可以减小锁冲突和竞争，提高导入和查询的并发性能。该参数只在 `lock_manager_enabled` 开启的前提下生效。
- 引入版本：v3.3.0

##### black_host_history_sec

- 默认值：2 * 60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：黑名单中 BE 节点连接失败记录的保留时长。如果一个 BE 节点被自动添加到 BE 黑名单中，StarRocks 将评估其连接状态，并判断是否可以将其从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，StarRocks 才会将其从 BE 黑名单中移除。
- 引入版本：v3.3.0

##### black_host_connect_failures_within_time

- 默认值：5
- 类型：Int
- Unit:
- 是否动态：是
- 描述：黑名单中的 BE 节点允许连接失败的上限。如果一个 BE 节点被自动添加到 BE 黑名单中，StarRocks 将评估其连接状态，并判断是否可以将其从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，StarRocks 才会将其从 BE 黑名单中移除。
- 引入版本：v3.3.0

##### enable_legacy_compatibility_for_replication

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为跨集群数据迁移开启旧版本兼容。新旧版本的集群间可能存在行为差异，从而导致跨集群数据迁移时出现问题。因此在数据迁移前，您需要为目标集群开启旧版本兼容，并在数据迁移完成后关闭。`true` 表示开启兼容。
- 引入版本：v3.1.10, v3.2.6

##### automated_cluster_snapshot_interval_seconds

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：自动化集群快照任务的触发间隔。
- 引入版本：v3.4.2

### 用户，角色及权限

##### privilege_max_total_roles_per_user

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个用户最多可以拥有的角色数量。
- 引入版本：-

##### privilege_max_role_depth

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个角色最多的嵌套层数。
- 引入版本：-

### 查询引擎

##### publish_version_interval_ms

- 默认值：10
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：两个版本发布操作之间的时间间隔。
- 引入版本：-

##### statistic_cache_columns

- 默认值：100000
- 类型：Long
- 单位：-
- 是否动态：否
- 描述：缓存统计信息表的最大行数。
- 引入版本：-

##### statistic_cache_thread_pool_size

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：用于刷新统计缓存的线程池大小。
- 引入版本：-

##### max_allowed_in_element_num_of_delete

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：DELETE 语句中 IN 谓词最多允许的元素数量。
- 引入版本：-

##### enable_materialized_view

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许创建物化视图。
- 引入版本：-

##### enable_materialized_view_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为物化视图的刷新任务开启中间结果落盘功能。
- 引入版本：v3.1.1

##### enable_backup_materialized_view

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在数据库的备份操作中，是否对数据库中的异步物化视图进行备份。如果设置为 `false`，将跳过对异步物化视图的备份。
- 引入版本：v3.2.0

<!--
##### enable_show_materialized_views_include_all_task_runs

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### materialized_view_min_refresh_interval

- 默认值：60
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### skip_whole_phase_lock_mv_limit

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### enable_experimental_mv

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启异步物化视图功能。`TRUE` 表示开启。从 2.5.2 版本开始，该功能默认开启。2.5.2 版本之前默认值为 `FALSE`。
- 引入版本：v2.4

##### enable_colocate_mv_index

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在创建同步物化视图时，是否将同步物化视图的索引与基表加入到相同的 Colocate Group。如果设置为 `true`，TabletSink 将加速同步物化视图的写入性能。
- 引入版本：v3.2.0

##### default_mv_refresh_immediate

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：创建异步物化视图后，是否立即刷新该物化视图。当设置为 `true` 时，异步物化视图创建后会立即刷新。
- 引入版本：v3.2.3

##### enable_materialized_view_metrics_collect

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否默认收集异步物化视图的监控指标。
- 引入版本：v3.1.11，v3.2.5

##### enable_materialized_view_text_based_rewrite

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否默认启用基于文本的查询改写。如果此项设置为 `true`，则系统在创建异步物化视图时构建抽象语法树。
- 引入版本：v3.2.5

##### enable_mv_automatic_active_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许系统自动检查和重新激活异步物化视图。启用此功能后，系统将会自动激活因基表（或视图）Schema Change 或重建而失效（Inactive）的物化视图。请注意，此功能不会激活由用户手动设置为 Inactive 的物化视图。
- 引入版本：v3.1.6

##### enable_active_materialized_view_schema_strict_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在激活失效物化视图时是否严格检查数据类型长度一致性。当设置为 `false` 时，如基表的数据类型长度有变化，也不影响物化视图的激活。
- 引入版本：v3.3.4

<!--
##### mv_active_checker_interval_seconds

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### default_mv_partition_refresh_number

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### mv_auto_analyze_async

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### enable_udf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 UDF。
- 引入版本：-

##### enable_decimal_v3

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Decimal V3。
- 引入版本：-

##### enable_sql_blacklist

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 SQL Query 黑名单校验。如果开启，在黑名单中的 Query 不能被执行。
- 引入版本：-

##### dynamic_partition_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启动态分区功能。打开后，您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。
- 引入版本：-

##### dynamic_partition_check_interval_seconds

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：动态分区检查的时间周期。如果有新数据生成，会自动生成分区。
- 引入版本：-

<!--
##### max_dynamic_partition_num

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### memory_tracker_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### memory_tracker_interval_seconds

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_create_partial_partition_in_batch

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### max_query_retry_time

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 上查询重试的最大次数。
- 引入版本：-

##### max_create_table_timeout_second

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：建表的最大超时时间。
- 引入版本：-

##### create_table_max_serial_replicas

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：串行创建副本的最大数量。如果实际副本数量超过此值，副本将并发创建。如果建表需要长时间才能完成，请尝试减少此值。
- 引入版本：-

##### http_slow_request_threshold_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：如果一条 HTTP 请求的时间超过了该参数指定的时长，会生成日志来跟踪该请求。
- 引入版本：v2.5.15，v3.1.5

##### max_partitions_in_one_batch

- 默认值：4096
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：批量创建分区时，分区数目的最大值。
- 引入版本：-

##### max_running_rollup_job_num_per_table

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Table 执行 Rollup 任务的最大并发度。
- 引入版本：-

##### expr_children_limit

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个表达式中允许的最大子表达式数量。
- 引入版本：-

##### max_planner_scalar_rewrite_num

- 默认值：100000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：优化器重写 ScalarOperator 允许的最大次数。
- 引入版本：-

##### enable_statistic_collect

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否采集统计信息，该开关默认打开。
- 引入版本：-

##### enable_statistic_collect_on_first_load

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当空表第一次导入数据时，是否自动触发统计信息采集。如果一张表包含多个分区，只要是某个空的分区第一次导入数据，都会触发该分区的统计信息采集。如果系统频繁创建新表并且导入数据，会存在一定内存和 CPU 开销。
- 引入版本：v3.1

<!--
##### semi_sync_collect_statistic_await_seconds

- 默认值：30
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### statistic_auto_analyze_start_time

- 默认值：00:00:00
- 类型：String
- 单位：-
- 是否动态：是
- 描述：用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。
- 引入版本：-

##### statistic_auto_analyze_end_time

- 默认值：23:59:59
- 类型：String
- 单位：-
- 是否动态：是
- 描述：用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。
- 引入版本：-

<!--
##### statistic_manager_sleep_time_sec

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### statistic_analyze_status_keep_second

- 默认值：3 * 24 * 3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：统计信息采集任务的记录保留时间，默认为 3 天。
- 引入版本：-

<!--
##### statistic_check_expire_partition

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### statistic_collect_interval_sec

- 默认值：5 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：自动定期采集任务中，检测数据更新的间隔时间。
- 引入版本：-

<!--
##### statistic_analyze_task_pool_size

- 默认值：3
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### statistic_collect_query_timeout

- 默认值：3600
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### slot_manager_response_thread_pool_size

- 默认值：16
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### statistic_dict_columns

- 默认值：100000
- 类型：Long
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### statistic_update_interval_sec

- 默认值：24 * 60 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：统计信息内存 Cache 失效时间。
- 引入版本：-

<!--
##### statistic_collect_too_many_version_sleep

- 默认值：600000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### enable_collect_full_statistic

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启自动全量统计信息采集，该开关默认打开。
- 引入版本：-

##### statistic_auto_collect_ratio

- 默认值：0.8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。
- 引入版本：-

<!--
##### statistic_full_collect_buffer

- 默认值：1024 * 1024 * 20
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### statistic_auto_collect_sample_threshold

- 默认值：0.3
- 类型：Double
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### statistic_auto_collect_small_table_size

- 默认值：5 * 1024 * 1024 * 1024
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### statistic_auto_collect_small_table_rows

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：自动收集中，用于判断外部数据源下的表 (Hive, Iceberg, Hudi) 是否为小表的行数门限。
- 引入版本：v3.2

<!--
##### statistic_auto_collect_small_table_interval

- 默认值：0
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### statistic_auto_collect_large_table_interval

- 默认值：3600 * 12
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### statistic_max_full_collect_data_size

- 默认值：100 * 1024 * 1024 * 1024
- 类型：Long
- 单位：bytes
- 是否动态：是
- 描述：自动统计信息采集的单次任务最大数据量。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。
- 引入版本：-

##### statistic_collect_max_row_count_per_query

- 默认值：5000000000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：单个 ANALYZE 任务查询的最大行数。如果超过此值， ANALYZE 任务将被拆分为多个查询。
- 引入版本：-

##### statistic_sample_collect_rows

- 默认值：200000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：最小采样行数。如果指定了采集类型为抽样采集（SAMPLE），需要设置该参数。如果参数取值超过了实际的表行数，默认进行全量采集。
- 引入版本：-

##### histogram_buckets_size

- 默认值：64
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：直方图默认分桶数。
- 引入版本：-

##### histogram_mcv_size

- 默认值：100
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：直方图默认 Most Common Value 的数量。
- 引入版本：-

##### histogram_sample_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：直方图默认采样比例。
- 引入版本：-

##### histogram_max_sample_row_count

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：直方图最大采样行数。
- 引入版本：-

##### connector_table_query_trigger_task_schedule_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Schedule 线程调度查询触发的后台任务的周期。该项用于取代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。此处后台任务是指 v3.4 中的 `ANALYZE` 任务，以及 v3.4 之后版本中引入的低基数列字典的收集任务。
- 引入版本：v3.4.2

##### connector_table_query_trigger_analyze_small_table_rows

- 默认值：10000000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表阈值。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表采集间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_large_table_interval

- 默认值：12 * 3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的大表采集间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 中处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 中处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

##### enable_local_replica_selection

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否选择本地副本进行查询。本地副本可以减少数据传输的网络时延。如果设置为 `true`，优化器优先选择与当前 FE 相同 IP 的 BE 节点上的 Tablet 副本。设置为 `false` 表示选择可选择本地或非本地副本进行查询。
- 引入版本：-

##### max_distribution_pruner_recursion_depth

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：: 分区裁剪允许的最大递归深度。增加递归深度可以裁剪更多元素但同时增加 CPU 资源消耗。
- 引入版本：-

##### slow_query_analyze_threshold

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 Query Feedback 分析的执行时间阈值。
- 引入版本：v3.4.0

### 导入导出

##### load_straggler_wait_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：控制 BE 副本最大容忍的导入落后时长，超过这个时长就进行克隆。
- 引入版本：-

##### load_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：导入作业的轮询间隔。
- 引入版本：-

<!--
##### lock_checker_interval_second

- 默认值：30
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lock_checker_enable_deadlock_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### broker_load_default_timeout_second

- 默认值：14400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Broker Load 的超时时间。
- 引入版本：-

<!--
##### spark_load_submit_timeout_second

- 默认值：300
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### min_bytes_per_broker_scanner

- 默认值：67108864
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个 Broker Load 实例处理的最小数据量。
- 引入版本：-

##### insert_load_default_timeout_second

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Insert Into 语句的超时时间。
- 引入版本：-

##### stream_load_default_timeout_second

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Stream Load 的默认超时时间。
- 引入版本：-

##### max_stream_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Stream Load 的最大超时时间。
- 引入版本：-

<!--
##### max_stream_load_batch_size_mb

- 默认值：100
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### stream_load_max_txn_num_per_be

- 默认值：-1
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### prepared_transaction_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### max_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导入作业的最大超时时间，适用于所有导入。
- 引入版本：-

##### min_load_timeout_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导入作业的最小超时时间，适用于所有导入。
- 引入版本：-

##### spark_dpp_version

- 默认值：1.0.0
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark DPP 特性的版本。
- 引入版本：-

##### spark_load_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Spark 导入的超时时间。
- 引入版本：-

##### spark_home_default_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 客户端根目录。
- 引入版本：-

##### spark_resource_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 依赖包的根目录。
- 引入版本：-

##### spark_launcher_log_dir

- 默认值：sys_log_dir + "/spark_launcher_log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 日志的保存目录。
- 引入版本：-

##### yarn_client_path

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Yarn 客户端的根目录。
- 引入版本：-

##### yarn_config_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Yarn 配置文件的保存目录。
- 引入版本：-

##### desired_max_waiting_jobs

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多等待的任务数，适用于所有的任务，建表、导入、Schema Change。如果 FE 中处于 PENDING 状态的作业数目达到该值，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。从 2.5 版本开始，该参数默认值从 100 变为 1024。
- 引入版本：-

##### max_running_txn_num_per_db

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：StarRocks 集群每个数据库中正在运行的导入相关事务的最大个数，默认值为 `1000`。自 3.1 版本起，默认值由 100 变为 1000。当数据库中正在运行的导入相关事务超过最大个数限制时，后续的导入不会执行。如果是同步的导入作业请求，作业会被拒绝；如果是异步的导入作业请求，作业会在队列中等待。不建议调大该值，会增加系统负载。
- 引入版本：-

##### max_broker_load_job_concurrency

- 默认值：5
- 别名：async_load_task_pool_size
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：StarRocks 集群中可以并行执行的 Broker Load 作业的最大数量。本参数仅适用于 Broker Load。取值必须小于 `max_running_txn_num_per_db`。从 2.5 版本开始，该参数默认值从 `10` 变为 `5`。
- 引入版本：-

##### load_parallel_instance_num (Deprecated)

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个 BE 上每个作业允许的最大并发实例数。自 3.1 版本起弃用。
- 引入版本：-

##### disable_load_job

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当集群遇到错误时是否禁用导入。这可以防止因集群错误而造成的任何损失。默认值为 `FALSE`，表示导入未禁用。`TRUE` 表示导入已禁用，集群处于只读状态。
- 引入版本：-

##### history_job_keep_max_second

- 默认值：7 * 24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：历史任务最大的保留时长，例如 Schema Change 任务。
- 引入版本：-

##### label_keep_max_second

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：已经完成、且处于 FINISHED 或 CANCELLED 状态的导入作业记录在 StarRocks 系统 label 的保留时长，默认值为 3 天。该参数配置适用于所有模式的导入作业。设定过大将会消耗大量内存。
- 引入版本：-

##### label_keep_max_num

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：一定时间内所保留导入任务的最大数量。超过之后历史导入作业的信息会被删除。
- 引入版本：-

##### max_routine_load_task_concurrent_num

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Routine Load 作业最大并发执行的 Task 数。
- 引入版本：-

##### max_routine_load_task_num_per_be

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 BE 并发执行的 Routine Load 导入任务数量上限。从 3.1.0 版本开始，参数默认值从 5 变为 16，并且不再需要小于等于 BE 的配置项 `routine_load_thread_pool_size`（已废弃）。
- 引入版本：-

##### max_routine_load_batch_size

- 默认值：4294967296
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：每个 Routine Load Task 导入的最大数据量。
- 引入版本：-

##### routine_load_task_consume_second

- 默认值：15
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：集群内每个 Routine Load 导入任务消费数据的最大时间。自 v3.1.0 起，Routine Load 导入作业 [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_consume_second`，作用于单个 Routine Load 导入作业内的导入任务，更加灵活。
- 引入版本：-

##### routine_load_task_timeout_second

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：集群内每个 Routine Load 导入任务超时时间，自 v3.1.0 起，Routine Load 导入作业 [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_timeout_second`，作用于单个 Routine Load 导入作业内的任务，更加灵活。
- 引入版本：-

<!--
##### routine_load_kafka_timeout_second

- 默认值：12
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### routine_load_pulsar_timeout_second

- 默认值：12
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### routine_load_unstable_threshold_second

- 默认值：3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：Routine Load 导入作业的任一导入任务消费延迟，即正在消费的消息时间戳与当前时间的差值超过该阈值，且数据源中存在未被消费的消息，则导入作业置为 UNSTABLE 状态。
- 引入版本：-

##### max_tolerable_backend_down_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许的最大故障 BE 数。如果故障的 BE 节点数超过该阈值，则不能自动恢复 Routine Load 作业。
- 引入版本：-

##### period_of_auto_resume_min

- 默认值：5
- 类型：Int
- 单位：Minutes
- 是否动态：是
- 描述：自动恢复 Routine Load 的时间间隔。
- 引入版本：-

##### export_task_default_timeout_second

- 默认值：2 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导出作业的超时时长。
- 引入版本：-

##### export_max_bytes_per_be_per_task

- 默认值：268435456
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个导出任务在单个 BE 上导出的最大数据量。
- 引入版本：-

##### export_task_pool_size

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导出任务线程池的大小。
- 引入版本：-

##### export_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：导出作业调度器的调度间隔。
- 引入版本：-

##### export_running_job_num_limit

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：导出作业最大的运行数目。
- 引入版本：-

##### empty_load_as_error

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：导入数据为空时，是否返回报错提示 `all partitions have no load data`。有效值：
  - `true`：当导入数据为空时，显示导入失败，并返回报错提示 `all partitions have no load data`。
  - `false`：当导入数据为空时，显示导入成功，并返回 `OK`，不返回报错提示。
- 引入版本：-

##### external_table_commit_timeout_ms

- 默认值：10000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：发布写事务到 StarRocks 外表的超时时长，单位为毫秒。默认值 `10000` 表示超时时长为 10 秒。
- 引入版本：-

##### enable_sync_publish

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在导入事务 publish 阶段同步执行 apply 任务，仅适用于主键表。有效值：
  - `TRUE`：导入事务 publish 阶段同步执行 apply 任务，即 apply 任务完成后才会返回导入事务 publish 成功，此时所导入数据真正可查。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，开启该参数可以提升查询性能和稳定性，但是会增加导入耗时。
  - `FALSE`：在导入事务 publish 阶段异步执行 apply 任务，即在导入事务 publish 阶段 apply 任务提交之后立即返回导入事务 publish 成功，然而此时导入数据并不真正可查。这时并发的查询需要等到 apply 任务完成或者超时，才能继续执行。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，关闭该参数会影响查询性能和稳定性。
- 引入版本：v3.2.0

<!--
##### stream_load_task_keep_max_num

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### stream_load_task_keep_max_second

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### label_clean_interval_second

- 默认值：4 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：作业标签的清理间隔。建议清理间隔尽量短，从而确保历史作业的标签能够及时清理掉。
- 引入版本：-

<!--
##### task_check_interval_second

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### task_ttl_second

- 默认值：24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### task_runs_ttl_second

- 默认值：24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### task_runs_max_history_number

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### transaction_clean_interval_second

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：已结束事务的清理间隔。建议清理间隔尽量短，从而确保已完成的事务能够及时清理掉。
- 引入版本：-

### 存储

##### default_replication_num

- 默认值：3
- 类型：Short
- 单位：-
- 是否动态：是
- 描述：用于配置分区默认的副本数。如果建表时指定了 `replication_num` 属性，则该属性优先生效；如果建表时未指定 `replication_num`，则配置的 `default_replication_num` 生效。建议该参数的取值不要超过集群内 BE 节点数。
- 引入版本：-

##### enable_strict_storage_medium_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：建表时，是否严格校验存储介质类型。该值为 `true` 时表示在建表时，会严格校验 BE 上的存储介质。比如建表时指定 `storage_medium = HDD`，而 BE 上只配置了 SSD，那么建表失败。该值为 `false` 时则忽略介质匹配，建表成功。
- 引入版本：-

##### catalog_trash_expire_second

- 默认值：86400
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：删除表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以通过[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 语句恢复。
- 引入版本：-

##### enable_auto_tablet_distribution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启自动设置分桶功能。
  - 设置为 `true` 表示开启，您在建表或新增分区时无需指定分桶数目，StarRocks 自动决定分桶数量。自动设置分桶数目的策略，请参见[设置分桶数量](../../table_design/data_distribution/Data_distribution.md#设置分桶数量)。
  - 设置为 `false` 表示关闭，您在建表时需要手动指定分桶数量。
  - 新增分区时，如果您不指定分桶数量，则新分区的分桶数量继承建表时候的分桶数量。当然您也可以手动指定新增分区的分桶数量。
- 引入版本：v2.5.7

##### enable_experimental_rowstore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启[行列混存表](../../table_design/hybrid_table.md)功能。
- 引入版本：v3.2.3

##### enable_experimental_gin

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启[全文倒排索引](../../table_design/indexes/inverted_index.md)功能。
- 引入版本：v3.3.0

##### storage_usage_soft_limit_percent

- 默认值：90
- 别名：storage_high_watermark_usage_percent
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个 BE 存储目录空间使用率软上限。如果 BE 存储目录空间使用率超过该值且剩余空间小于 `storage_usage_soft_limit_reserve_bytes`，则不能继续向该路径克隆 Tablet。
- 引入版本：-

##### storage_usage_soft_limit_reserve_bytes

- 默认值：200 * 1024 * 1024 * 1024
- 别名：storage_min_left_capacity_bytes
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个 BE 存储目录剩余空间软限制。如果 BE 存储目录下剩余空间小于该值且空间使用率超过 `storage_usage_soft_limit_percent`，则不能继续向该路径克隆 Tablet。
- 引入版本：-

##### storage_usage_hard_limit_percent

- 默认值：95
- 别名：storage_flood_stage_usage_percent
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个 BE 存储目录空间使用率硬上限。如果 BE 存储目录空间使用率超过该值且剩余空间小于 `storage_usage_hard_limit_reserve_bytes`，StarRocks 会拒绝 Load 和 Restore 作业。需要同步修改 BE 配置 `storage_flood_stage_usage_percent` 以使其生效。
- 引入版本：-

##### storage_usage_hard_limit_reserve_bytes

- 默认值：100 * 1024 * 1024 * 1024
- 别名：storage_flood_stage_left_capacity_bytes
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个 BE 存储目录剩余空间硬限制。如果 BE 存储目录下剩余空间小于该值且空间使用率超过 `storage_usage_hard_limit_percent`，StarRocks 会拒绝 Load 和 Restore 作业。需要同步修改 BE 配置 `storage_flood_stage_left_capacity_bytes` 以使其生效。
- 引入版本：-

##### alter_table_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Schema Change 超时时间。
- 引入版本：-

##### enable_fast_schema_evolution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启集群内所有表的 fast schema evolution，取值：`TRUE` 或 `FALSE`。开启后增删列时可以提高 Schema Change 速度并降低资源使用。
- 引入版本：v3.2.0

> **说明**
>
> - StarRocks 存算分离集群自 v3.3.0 起支持该参数。
> - 如果您需要为某张表设置该配置，例如关闭该表的 fast schema evolution，则可以在建表时设置表属性 [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#设置-fast-schema-evolution)。

##### recover_with_empty_tablet

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在 Tablet 副本丢失/损坏时，是否使用空的 Tablet 代替。这样可以保证在有 Tablet 副本丢失/损坏时，查询依然能被执行（但是由于缺失了数据，结果可能是错误的）。默认为 `false`，表示不进行替代，查询会失败。
- 引入版本：-

##### tablet_create_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：创建 tablet 的超时时长。自 v3.1 版本起，默认值由 1 改为 10。
- 引入版本：-

##### tablet_delete_timeout_second

- 默认值：2
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：删除 tablet 的超时时长。
- 引入版本：-

##### check_consistency_default_timeout_second

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：副本一致性检测的超时时间。
- 引入版本：-

##### tablet_sched_slot_num_per_path

- 默认值：8
- 别名：schedule_slot_num_per_path
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：一个 BE 存储目录能够同时执行 tablet 相关任务的数目。参数别名 `schedule_slot_num_per_path`。从 2.5 版本开始，该参数默认值从 2.4 版本的 `4` 变为 `8`。
- 引入版本：-

##### tablet_sched_max_scheduling_tablets

- 默认值：10000
- 别名：max_scheduling_tablets
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：可同时调度的 Tablet 的数量。如果正在调度的 Tablet 数量超过该值，跳过 Tablet 均衡和修复检查。
- 引入版本：-

##### tablet_sched_disable_balance

- 默认值：false
- 别名：disable_balance
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否禁用 Tablet 均衡调度。
- 引入版本：-

##### tablet_sched_disable_colocate_balance

- 默认值：false
- 别名：disable_colocate_balance
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否禁用 Colocate Table 的副本均衡。
- 引入版本：-

<!--
##### tablet_sched_disable_colocate_overall_balance

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_sched_colocate_balance_high_prio_backends

- 默认值：{}
- 类型：Long[]
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_sched_always_force_decommission_replica

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### tablet_sched_be_down_tolerate_time_s

- 默认值：900
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：调度器容忍 BE 节点保持不存活状态的最长时间。超时之后该节点上的 Tablet 会被迁移到其他存活的 BE 节点上。
- 引入版本：2.5.7

<!--
##### tablet_sched_colocate_be_down_tolerate_time_s

- 默认值：12 * 3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### tablet_sched_max_balancing_tablets

- 默认值：500
- 别名：max_balancing_tablets
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：正在均衡的 Tablet 数量的最大值。如果正在均衡的 Tablet 数量超过该值，跳过 Tablet 重新均衡。
- 引入版本：-

##### tablet_sched_storage_cooldown_second

- 默认值：-1
- 别名：storage_cooldown_second
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br />参数别名为 `storage_cooldown_second`。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 `-1`。
- 引入版本：-

##### tablet_sched_max_not_being_scheduled_interval_ms

- 默认值：15 * 60 * 1000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：克隆 Tablet 调度时，如果超过该时间一直未被调度，则将该 Tablet 的调度优先级升高，以尽可能优先调度。
- 引入版本：-

##### tablet_sched_balance_load_score_threshold

- 默认值：0.1
- 别名：balance_load_score_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：用于判断 BE 负载是否均衡的百分比阈值。如果一个 BE 的负载低于所有 BE 的平均负载，且差值大于该阈值，则认为该 BE 处于低负载状态。相反，如果一个 BE 的负载比平均负载高且差值大于该阈值，则认为该 BE 处于高负载状态。
- 引入版本：-

##### tablet_sched_num_based_balance_threshold_ratio

- 默认值：0.5
- 别名：-
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：做分布均衡时可能会打破磁盘大小均衡，但磁盘间的最大差距不能超过tablet_sched_num_based_balance_threshold_ratio * table_sched_balance_load_score_threshold。 如果集群中存在不断从 A 到 B、从 B 到 A 的克隆，请减小该值。 如果希望tablet分布更加均衡，请调大该值。
- 引入版本：3.1

##### tablet_sched_balance_load_disk_safe_threshold

- 默认值：0.5
- 别名：balance_load_disk_safe_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：判断 BE 磁盘使用率是否均衡的百分比阈值。如果所有 BE 的磁盘使用率低于该值，认为磁盘使用均衡。当有 BE 磁盘使用率超过该阈值时，如果最大和最小 BE 磁盘使用率之差高于 10%，则认为磁盘使用不均衡，会触发 Tablet 重新均衡。
- 引入版本：-

##### tablet_sched_repair_delay_factor_second

- 默认值：60
- 别名：tablet_repair_delay_factor_second
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：FE 进行副本修复的间隔。
- 引入版本：-

##### tablet_sched_min_clone_task_timeout_sec

- 默认值：3 * 60
- 别名：min_clone_task_timeout_sec
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：克隆 Tablet 的最小超时时间。
- 引入版本：-

##### tablet_sched_max_clone_task_timeout_sec

- 默认值：2 * 60 * 60
- 别名：max_clone_task_timeout_sec
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：克隆 Tablet 的最大超时时间。
- 引入版本：-

<!--
##### tablet_sched_checker_interval_seconds

- 默认值：20
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### tablet_sched_max_migration_task_sent_once

- 默认值：1000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_sched_consecutive_full_clone_delay_sec

- 默认值：180
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_report_drop_tablet_delay_sec

- 默认值：120
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_checker_partition_batch_num

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### tablet_stat_update_interval_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 向每个 BE 请求收集 Tablet 统计信息的时间间隔。
- 引入版本：-

##### max_automatic_partition_number

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：系统自动创建分区数量上限。
- 引入版本：v3.1

##### auto_partition_max_creation_number_per_load

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个导入任务在表达式分区表中最多可以创建的分区数量。
- 引入版本：v3.3.2

##### max_partition_number_per_table

- 默认值：100000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个表中最多可以创建的分区数量。
- 引入版本：v3.3.2

##### max_bucket_number_per_partition

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个分区中最多可以创建的分桶数量。
- 引入版本：v3.3.2

##### max_column_number_per_table

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个表中最多可以创建的列数量。
- 引入版本：v3.3.2

### 存算分离

##### run_mode

- 默认值：shared_nothing
- 类型：String
- 单位：-
- 是否动态：否
- 描述：StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing` (默认)。
  - `shared_data` 表示在存算分离模式下运行 StarRocks。
  - `shared_nothing` 表示在存算一体模式下运行 StarRocks。

  > **注意**
  >
  > - StarRocks 集群不支持存算分离和存算一体模式混合部署。
  > - 请勿在集群部署完成后更改 `run_mode`，否则将导致集群无法再次启动。不支持从存算一体集群转换为存算分离集群，反之亦然。

- 引入版本：-

<!--
##### shard_group_clean_threshold_sec

- 默认值：3600
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### star_mgr_meta_sync_interval_sec

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### cloud_native_meta_port

- 默认值：6090
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：云原生元数据服务监听端口。
- 引入版本：-


##### enable_load_volume_from_conf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否允许 StarRocks 使用 FE 配置文件中指定的存储相关属性创建默认存储卷。自 v3.4.1 起，默认值由 `true` 变为 `false`。
- 引入版本：v3.1.0


##### cloud_native_storage_type

- 默认值：S3
- 类型：String
- 单位：-
- 是否动态：否
- 描述：您使用的存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS 、Azure Blob（自 v3.1.1 起支持）、Azure Data Lake Storage Gen2（自 v3.4.1 起支持）、以及兼容 S3 协议的对象存储中（例如 AWS S3、Google GCP、阿里云 OSS 以及 MinIO）。有效值：`S3`（默认）、`AZBLOB`、`ADLS2` 和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果您将此项指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的配置项。如果您将此项指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本：-

##### cloud_native_hdfs_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本：-

##### aws_s3_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- 引入版本：v3.0

##### aws_s3_region

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：需访问的 S3 存储空间的地区，如 `us-west-2`。
- 引入版本：v3.0

##### aws_s3_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本：v3.0

##### aws_s3_use_aws_sdk_default_behavior

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false`。
- 引入版本：v3.0

##### aws_s3_use_instance_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false`。
  - 如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。
  - 如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。
  - 如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws_s3_iam_role_arn`。
  - 如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要额外指定 `aws_s3_external_id`。
- 引入版本：v3.0

##### aws_s3_access_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的 Access Key。
- 引入版本：v3.0

##### aws_s3_secret_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的 Secret Key。
- 引入版本：v3.0

##### aws_s3_iam_role_arn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：有访问 S3 存储空间权限 IAM Role 的 ARN。
- 引入版本：v3.0

##### aws_s3_external_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于跨 AWS 账户访问 S3 存储空间的外部 ID。
- 引入版本：v3.0

##### azure_blob_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Azure Blob Storage 的链接地址，如 `https://test.blob.core.windows.net`。
- 引入版本：v3.1

##### azure_blob_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于存储数据的 Azure Blob Storage 路径，由存 Storage Account 中的容器名称和容器下的子路径（如有）组成，如 `testcontainer/subpath`。
- 引入版本：v3.1

##### azure_blob_shared_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 Azure Blob Storage 的 Shared Key。
- 引入版本：v3.1

##### azure_blob_sas_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 Azure Blob Storage 的共享访问签名（SAS）。
- 引入版本：v3.1

<!--
##### starmgr_grpc_timeout_seconds

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_compaction_score_selector_min_score

- 默认值：10.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，触发 Compaction 操作的 Compaction Score 阈值。当一个表分区的 Compaction Score 大于或等于该值时，系统会对该分区执行 Compaction 操作。
- 引入版本：v3.1.0

Compaction Score 代表了一个表分区是否值得进行 Compaction 的评分，您可以通过 [SHOW PARTITIONS](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_PARTITIONS.md) 语句返回中的 `MaxCS` 一列的值来查看某个分区的 Compaction Score。Compaction Score 和分区中的文件数量有关系。文件数量过多将影响查询性能，因此系统后台会定期执行 Compaction 操作来合并小文件，减少文件数量。

##### lake_compaction_max_tasks

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下允许同时执行的 Compaction 任务数。系统依据分区中 Tablet 数量来计算 Compaction 任务数。如果一个分区有 10 个 Tablet，那么对该分区作一次 Compaction 就会创建 10 个 Compaction 任务。如果正在执行中的 Compaction 任务数超过该阈值，系统将不会创建新的 Compaction 任务。将该值设置为 `0` 表示禁止 Compaction，设置为 `-1` 表示系统依据自适应策略自动计算该值。
- 引入版本：v3.1.0

##### lake_compaction_history_size

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下在 Leader FE 节点内存中保留多少条最近成功的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 引入版本：v3.1.0

##### lake_publish_version_max_threads

- 默认值：512
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下发送生效版本（Publish Version）任务的最大线程数。
- 引入版本：v3.2.0

<!--
##### lake_publish_delete_txnlog_max_threads

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_autovacuum_max_previous_versions

- 默认值：0
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### lake_autovacuum_parallel_partitions

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：存算分离集群下最多可以同时对多少个表分区进行垃圾数据清理（AutoVacuum，即在 Compaction 后进行的垃圾文件回收）。
- 引入版本：v3.1.0

##### lake_autovacuum_partition_naptime_seconds

- 默认值：180
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：存算分离集群下对同一个表分区进行垃圾数据清理的最小间隔时间。
- 引入版本：v3.1.0

##### lake_autovacuum_grace_period_minutes

- 默认值：30
- 类型：Long
- 单位：Minutes
- 是否动态：是
- 描述：存算分离集群下保留历史数据版本的时间范围。此时间范围内的历史数据版本不会被自动清理。您需要将该值设置为大于最大查询时间，以避免正在访问中的数据被删除导致查询失败。自 v3.3.0，v3.2.5 及 v3.1.10 起，默认值由 `5` 变更为 `30`。
- 引入版本：v3.1.0

##### lake_autovacuum_stale_partition_threshold

- 默认值：12
- 类型：Long
- 单位：Hours
- 是否动态：是
- 描述：存算分离集群下，如果某个表分区在该阈值范围内没有任何更新操作(导入、删除或 Compaction)，将不再触发该分区的自动垃圾数据清理操作。
- 引入版本：v3.1.0

##### lake_enable_ingest_slowdown

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为存算分离集群开启导入限速功能。开启导入限速功能后，当某个表分区的 Compaction Score 超过了 `lake_ingest_slowdown_threshold`，该表分区上的导入任务将会被限速。只有当 `run_mode` 设置为 `shared_data` 后，该配置项才会生效。自 v3.3.6 起，默认值由 `false` 变为 `true`。
- 引入版本：v3.2.0

##### lake_ingest_slowdown_threshold

- 默认值：100
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：触发导入限速的 Compaction Score 阈值。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。
- 引入版本：v3.2.0

> **说明**
>
> 当 `lake_ingest_slowdown_threshold` 比配置项 `lake_compaction_score_selector_min_score` 小时，实际生效的阈值会是 `lake_compaction_score_selector_min_score`。

##### lake_ingest_slowdown_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：导入限速比例。

  数据导入任务可以分为数据写入和数据提交（COMMIT）两个阶段，导入限速是通过延迟数据提交来达到限速的目的的，延迟比例计算公式为：`(compaction_score - lake_ingest_slowdown_threshold) * lake_ingest_slowdown_ratio`。例如，数据写入阶段耗时为 5 分钟，`lake_ingest_slowdown_ratio` 为 0.1，Compaction Score 比 `lake_ingest_slowdown_threshold` 多 10，那么延迟提交的时间为 `5 * 10 * 0.1 = 5` 分钟，相当于写入阶段的耗时由 5 分钟增加到了 10 分钟，平均导入速度下降了一倍。

- 引入版本：v3.2.0

> **说明**
>
> - 如果一个导入任务同时向多个分区写入，那么会取所有分区的 Compaction Score 的最大值来计算延迟提交时间。
> - 延迟提交的时间是在第一次尝试提交时计算的，一旦确定便不会更改，延迟时间一到，只要 Compaction Score 不超过 `lake_compaction_score_upper_bound`，系统都会执行数据提交（COMMIT）操作。
> - 如果延迟之后的提交时间超过了导入任务的超时时间，那么导入任务会直接失败。

##### lake_compaction_score_upper_bound

- 默认值：2000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：表分区的 Compaction Score 的上限, `0` 表示没有上限。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。当表分区 Compaction Score 达到或超过该上限后，新的导入会被拒绝。自 v3.3.6 起，默认值由 `0` 变为 `2000`。
- 引入版本：v3.2.0

##### lake_compaction_disable_tables

- 默认值：""
- 类型：String
- 单位：-
- 是否动态：是
- 描述：禁止存算分离内表 compaction 的 table id 名单。格式为 `tableId1;tableId2`，table id 之间用分号隔开，例如 `12345;98765`。
- 引入版本：v3.1.11

##### lake_enable_balance_tablets_between_workers

- 默认值：false
- 类型：Boolean
- Unit: -
- 是否动态：是
- 描述：是否在存算分离集群内表的 Tablet 调度过程中平衡 CN 节点之间的 Tablet 数量。`true` 表示启用平衡 Tablet 数量，`false` 表示禁用此功能。
- 引入版本：v3.3.4

##### lake_balance_tablets_threshold

- 默认值：0.15
- 类型：Double
- Unit: -
- 是否动态：是
- 描述：系统用于判断存算分离集群中 Worker 之间 Tablet 分布平衡的阈值，不平衡因子的计算公式为 `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`。如果该因子大于 `lake_balance_tablets_threshold`，则会触发节点间 Tablet 调度。此配置项仅在 `lake_enable_balance_tablets_between_workers` 设为 `true`时生效。
- 引入版本：v3.3.4

### 其他

##### tmp_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br />这些过程完成以后，所产生的临时文件会被清除掉。
- 引入版本：-

##### plugin_dir

- 默认值：System.getenv("STARROCKS_HOME") + "/plugins"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：插件的安装目录。
- 引入版本：-

##### plugin_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启了插件功能。只能在 Leader FE 安装/卸载插件。
- 引入版本：-

<!--
##### profile_process_threads_num

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### profile_process_blocking_queue_size

- 默认值：profile_process_threads_num * 128
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### max_agent_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：代理任务线程池中用于处理代理任务的最大线程数。
- 引入版本：-

##### agent_task_resend_wait_time_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：Agent task 重新发送前的等待时间。当代理任务的创建时间已设置，并且距离现在超过该值，才能重新发送代理任务。该参数用于防止过于频繁的代理任务发送。
- 引入版本：-

<!--
##### start_with_incomplete_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_backend_down_time_second

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_enable_batch_publish_version 

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_batch_publish_max_version_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_batch_publish_min_version_num

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### capacity_used_percent_high_water

- 默认值：0.75
- 类型：Double
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### alter_max_worker_threads

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### alter_max_worker_queue_size

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### task_runs_queue_length

- 默认值：500
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### task_runs_concurrency

- 默认值：4
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_task_runs_threads_num

- 默认值：512
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### consistency_check_start_time

- 默认值：23
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### consistency_check_end_time

- 默认值：4
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### consistency_tablet_meta_check_interval_ms

- 默认值：2 * 3600 * 1000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### backup_plugin_path (Deprecated)

- 默认值：/tools/trans_file_tool/trans_files.sh
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### backup_job_default_timeout_ms

- 默认值：86400 * 1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Backup 作业的超时时间。
- 引入版本：-

##### locale

- 默认值：zh_CN.UTF-8
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所使用的字符集。
- 引入版本：-

<!--
##### db_used_data_quota_update_interval_secs

- 默认值：300
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### disable_hadoop_load

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### report_queue_size (Deprecated)

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-

##### enable_metric_calculator

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启定期收集指标 (Metrics) 的功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 引入版本：-

<!--
##### enable_replicated_storage_as_default_engine

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_schedule_insert_query_by_row_count

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### max_small_file_number

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许存储小文件数目的最大值。
- 引入版本：-

##### max_small_file_size_bytes

- 默认值：1024 * 1024
- 类型：Int
- 单位：Bytes
- 是否动态：是
- 描述：存储文件的大小上限。
- 引入版本：-

##### small_file_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：小文件的根目录。
- 引入版本：-

##### enable_auth_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启鉴权检查功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开启该功能。`FALSE`表示关闭该功能。
- 引入版本：-

<!--
##### enable_starrocks_external_table_auth_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authorization_enable_column_level_privilege

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authentication_chain

- 默认值：{AUTHENTICATION_CHAIN_MECHANISM_NATIVE}
- 类型：String[]
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### authentication_ldap_simple_server_host

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：LDAP 服务器所在主机的主机名。
- 引入版本：-

##### authentication_ldap_simple_server_port

- 默认值：389
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：LDAP 服务器的端口。
- 引入版本：-

##### authentication_ldap_simple_bind_base_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：检索用户时，使用的 Base DN，用于指定 LDAP 服务器检索用户鉴权信息的起始点。
- 引入版本：-

##### authentication_ldap_simple_user_search_attr

- 默认值：uid
- 类型：String
- 单位：-
- 是否动态：是
- 描述：LDAP 对象中标识用户的属性名称。
- 引入版本：-

##### authentication_ldap_simple_bind_root_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：检索用户时，使用的管理员账号的 DN。
- 引入版本：-

##### authentication_ldap_simple_bind_root_pwd

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：检索用户时，使用的管理员账号的密码。
- 引入版本：-

<!--
##### enable_token_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### auth_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于内部身份验证的集群令牌。为空则在 Leader FE 第一次启动时随机生成一个。
- 引入版本：-

<!--
##### enable_authentication_kerberos

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authentication_kerberos_service_principal

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authentication_kerberos_service_key_tab

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authorization_enable_admin_user_protection

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### authorization_enable_priv_collection_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_partition_number_per_table

- 默认值：100000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_automatic_bucket

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### default_automatic_bucket_size

- 默认值：4 * 1024 * 1024 * 1024
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_agent_tasks_send_per_be

- 默认值：10000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### hive_meta_cache_refresh_min_threads

- 默认值：50
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### hive_meta_load_concurrency

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Hive 元数据支持的最大并发线程数。
- 引入版本：-

##### hive_meta_cache_refresh_interval_s

- 默认值：3600 * 2
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：刷新 Hive 外表元数据缓存的时间间隔。
- 引入版本：-

##### hive_meta_cache_ttl_s

- 默认值：3600 * 24
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：Hive 外表元数据缓存的失效时间。
- 引入版本：-

<!--
##### remote_file_cache_ttl_s

- 默认值：3600 * 36
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### max_hive_partitions_per_rpc

- 默认值：5000
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### remote_file_cache_refresh_interval_s

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### remote_file_metadata_load_concurrency

- 默认值：32
- 类型：Int
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### hive_meta_store_timeout_s

- 默认值：10
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：连接 Hive Metastore 的超时时间。
- 引入版本：-

<!--
##### enable_hms_events_incremental_sync

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### hms_events_polling_interval_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### hms_events_batch_size_per_rpc

- 默认值：500
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_hms_parallel_process_evens

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### hms_process_events_parallel_num

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### hive_max_split_size

- 默认值：64 * 1024 * 1024
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_refresh_hive_partitions_statistics

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_iceberg_custom_worker_thread

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_worker_num_threads

- 默认值：Runtime.getRuntime().availableProcessors()
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_table_refresh_threads

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_table_refresh_expire_sec

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_cache_disk_path

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_memory_cache_capacity

- 默认值：536870912
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_memory_cache_expiration_seconds

- 默认值：86500
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_iceberg_metadata_disk_cache

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_disk_cache_capacity

- 默认值：2147483648
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_disk_cache_expiration_seconds

- 默认值：7 * 24 * 60 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### iceberg_metadata_cache_max_entry_size

- 默认值：8388608
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### es_state_sync_interval_second

- 默认值：10
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：FE 获取 Elasticsearch Index 和同步 StarRocks 外部表元数据的时间间隔。
- 引入版本：-

<!--
##### broker_client_timeout_ms

- 默认值：120000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### vectorized_load_enable (Deprecated)

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_pipeline_load (Deprecated)

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_shuffle_load

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### eliminate_shuffle_load_by_replicated_storage

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_vectorized_file_load (Deprecated)

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_routine_load_lag_metrics

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### query_detail_cache_time_nanosecond

- 默认值：30000000000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### min_routine_load_lag_for_metrics

- 默认值：10000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### heartbeat_timeout_second

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### heartbeat_retry_times

- 默认值：3
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_display_shadow_partitions

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_dict_optimize_routine_load

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_dict_optimize_stream_load

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_validate_password

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_password_reuse

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### quorum_publish_wait_time_ms

- 默认值：5000
- 别名：quorom_publish_wait_time_ms
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：等待该参数指定的时间后，才能执行 Quorum publish。可以增加该参数值来避免不必要的 Clone。
- 引入版本：v3.1
-->

<!--
##### metadata_journal_queue_size

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### metadata_journal_max_batch_size_mb

- 默认值：10
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### metadata_journal_max_batch_cnt

- 默认值：100
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### jaeger_grpc_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_selector

- 默认值：ScoreSelector
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_sorter

- 默认值：ScoreSorter
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_simple_selector_min_versions

- 默认值：3
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_simple_selector_threshold_versions

- 默认值：10
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lake_compaction_simple_selector_threshold_seconds

- 默认值：300
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_new_publish_mechanism

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### metadata_journal_skip_bad_journal_ids

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### profile_info_reserved_num

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### load_profile_info_reserved_num

- 默认值：500
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### profile_info_format

- 默认值：default
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### ignore_invalid_privilege_authentications

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### ssl_keystore_location

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ssl_keystore_password

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ssl_key_password

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ssl_truststore_location

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### ssl_truststore_password

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_check_db_state

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### binlog_ttl_second

- 默认值：60 * 30
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### binlog_max_size

- 默认值：Long.MAX_VALUE
- 类型：Long
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_safe_mode

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### safe_mode_checker_interval_sec

- 默认值：5
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### auto_increment_cache_size

- 默认值：100000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_experimental_temporary_table

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### max_per_node_grep_log_limit

- 默认值：500000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### enable_execute_script_on_frontend

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### alter_scheduler_interval_millisecond

- 默认值：10000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### routine_load_scheduler_interval_millisecond

- 默认值：10000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### stream_load_profile_collect_threshold_second

- 默认值：0
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### max_upload_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 BACKUP 操作下，系统向单个 BE 节点下发的最大上传任务数。设置为小于或等于 0 时表示不限制任务数。
- 引入版本：v3.1.0

##### max_download_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 RESTORE 操作下，系统向单个 BE 节点下发的最大下载任务数。设置为小于或等于 0 时表示不限制任务数。
- 引入版本：v3.1.0

##### enable_colocate_restore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Colocate 表启用备份恢复。`true` 表示启用 Colocate 表备份恢复，`false` 表示禁用。
- 引入版本：v3.2.10、v3.3.3

<!--
##### enable_persistent_index_by_default

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### experimental_enable_fast_schema_evolution_in_shared_data

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### pipe_listener_interval_millis

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### pipe_scheduler_interval_millis

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### enable_show_external_catalog_privilege

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### primary_key_disk_schedule_time

- 默认值：3600
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### access_control

- 默认值：native
- 类型：String
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### ranger_user_ugi

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### catalog_metadata_cache_size

- 默认值：500
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### mv_plan_cache_expire_interval_sec

- 默认值：24 * 60 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### mv_plan_cache_max_size

- 默认值：1000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### mv_query_context_cache_max_size

- 默认值：1000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### port_connectivity_check_interval_sec

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### port_connectivity_check_retry_times

- 默认值：3
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### port_connectivity_check_timeout_ms

- 默认值：10000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### json_file_size_limit

- 默认值：4294967296
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### allow_system_reserved_names

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许用户创建以 `__op` 或 `__row` 开头命名的列。TRUE 表示启用此功能。请注意，在 StarRocks 中，这样的列名被保留用于特殊目的，创建这样的列可能导致未知行为，因此系统默认禁止使用这类名字。
- 引入版本：v3.2.0

<!--
##### use_lock_manager

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### lock_table_num

- 默认值：32
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<!--
##### lock_manager_enable_resolve_deadlock

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### lock_manager_dead_lock_detection_delay_time_ms

- 默认值：3000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### refresh_dictionary_cache_thread_num

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### replication_interval_ms

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：调度执行同步任务的最小时间间隔。
- 引入版本：v3.3.5

##### replication_max_parallel_table_count

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许并发执行的数据同步任务数。StarRocks 为一张表创建一个同步任务。
- 引入版本：v3.3.5

##### replication_max_parallel_replica_count

- 默认值：10240
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许并发同步的 Tablet 副本数。
- 引入版本：v3.3.5

##### replication_max_parallel_data_size_mb

- 默认值：1048576
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：允许并发同步的数据量。
- 引入版本：v3.3.5

##### replication_transaction_timeout_sec

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：同步任务的超时时间。
- 引入版本：v3.3.5

##### jdbc_meta_default_cache_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：JDBC Catalog 元数据缓存是否开启的默认值。当设置为 `TRUE` 时，新创建的 JDBC Catalog 会默认开启元数据缓存。
- 引入版本：-

##### jdbc_meta_default_cache_expire_sec

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：JDBC Catalog 元数据缓存的默认过期时间。当 `jdbc_meta_default_cache_enable` 设置为 `TRUE` 时，新创建的 JDBC Catalog 会默认设置元数据缓存的过期时间。
- 引入版本：-

##### jdbc_connection_pool_size

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 的容量上限。
- 引入版本：-

##### jdbc_minimum_idle_connections

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 中处于 idle 状态的连接最低数量。
- 引入版本：-

##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：访问 JDBC Catalog 时，连接建立的超时时长。超过参数取值时间的连接被认为是 idle 状态。
- 引入版本：-

##### query_detail_explain_level

- 默认值：COSTS
- 类型：String
- 单位：-
- 是否动态：是
- 描述：EXPLAIN 语句返回的查询计划的解释级别。有效值：COSTS、NORMAL、VERBOSE。
- 引入版本：v3.2.12，v3.3.5

<!--
##### max_varchar_length

- 默认值：1048576
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->
