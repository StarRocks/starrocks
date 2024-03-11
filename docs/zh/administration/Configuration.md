---
displayed_sidebar: "Chinese"
keywords: ['Canshu']
---

# 配置参数

本文介绍如何配置 StarRocks FE 节点、BE 节点、Broker 以及系统参数，并介绍相关参数。

## FE 配置项

FE 参数分为动态参数和静态参数。动态参数可通过 SQL 命令进行在线配置和调整，方便快捷。**需要注意通过 SQL 命令所做的动态设置在重启 FE 后会失效。如果想让设置长期生效，建议同时修改 fe.conf 文件。**

静态参数必须在 FE 配置文件 **fe.conf** 中进行配置和调整。**调整完成后，需要重启 FE 使变更生效。**

参数是否为动态参数可通过 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md) 返回结果中的 `IsMutable` 列查看。`TRUE` 表示动态参数。

静态和动态参数均可通过 **fe.conf** 文件进行修改。

### 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端执行 ADMIN SHOW FRONTEND CONFIG 命令来查看参数配置。如果您想查看具体参数的配置，执行如下命令：

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

详细的命令返回字段解释，参见 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md)。

> **注意**
>
> 只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关命令。

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
## 配置 FE 参数

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
### 配置 FE 动态参数

您可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) 命令在线修改 FE 动态参数。

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **注意**
>
> 动态设置的配置项，在 FE 重启之后会恢复成 **fe.conf** 文件中的配置或者默认值。如果需要让配置长期生效，建议设置完之后同时修改 **fe.conf** 文件，防止重启后修改失效。

### 配置 FE 静态参数

FE 静态参数不支持在线修改，您需要在 `fe.conf` 中修改并重启 FE。

<<<<<<< HEAD:docs/zh/administration/Configuration.md
#### Log

=======
## FE 参数描述

### 日志

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
- 描述：打印审计日志的模块。默认打印 slow_query 和 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 引入版本：-

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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
- 描述：为那些有多个 IP 地址的服务器声明一个选择策略。 请注意，最多应该有一个 IP 地址与此列表匹配。这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。 如果没有匹配这条规则的ip，会随机选择一个。
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

##### mysql_service_nio_enabled

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 MySQL 服务器的异步 I/O 选项。
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

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 支持的最大连接数，包括所有用户发起的连接。
- 引入版本：-

##### max_connection_scheduler_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：连接调度器支持的最大线程数。
- 引入版本：-

#### 元数据与集群管理

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### catalog_try_lock_timeout_ms
=======
##### cluster_id
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群的 ID。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。默认值 `-1` 表示在 Leader FE 首次启动时随机生成一个。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
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

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
##### edit_log_roll_num

- 默认值：50000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：该参数用于控制日志文件的大小，指定了每写多少条元数据日志，执行一次日志滚动操作来为这些日志生成新的日志文件。新日志文件会写入到 BDBJE Database。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### ignore_unknown_log_id

- 含义：是否忽略未知的 logID。当 FE 回滚到低版本时，可能存在低版本 BE 无法识别的 logID。<br />如果为 TRUE，则 FE 会忽略这些 logID；否则 FE 会退出。
- 默认值：FALSE

##### ignore_materialized_view_error

- 含义：是否忽略因物化视图错误导致的元数据异常。如果 FE 因为物化视图错误导致的元数据异常而无法启动，您可以通过将该参数设置为 `true` 以忽略错误。
- 默认值：FALSE
- 引入版本：2.5.10

##### ignore_meta_check

- 含义：是否忽略元数据落后的情形。如果为 true，非主 FE 将忽略主 FE 与其自身之间的元数据延迟间隙，即使元数据延迟间隙超过 meta_delay_toleration_second，非主 FE 仍将提供读取服务。<br />当您尝试停止 Master FE 较长时间，但仍希望非 Master FE 可以提供读取服务时，该参数会很有帮助。
- 默认值：FALSE

##### meta_delay_toleration_second

- 含义：FE 所在 StarRocks 集群中，非 Leader FE 能够容忍的元数据落后的最大时间。<br />如果非 Leader FE 上的元数据与 Leader FE 上的元数据之间的延迟时间超过该参数取值，则该非 Leader FE 将停止服务。
- 单位：秒
- 默认值：300

##### drop_backend_after_decommission

- 含义：BE 被下线后，是否删除该 BE。true 代表 BE 被下线后会立即删除该 BE。False 代表下线完成后不删除 BE。
- 默认值：TRUE

##### enable_collect_query_detail_info

- 含义：是否收集查询的 profile 信息。设置为 true 时，系统会收集查询的 profile。设置为 false 时，系统不会收集查询的 profile。
- 默认值：FALSE

##### enable_background_refresh_connector_metadata

- 含义：是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。
- 默认值：v3.0 为 TRUE；v2.5 为 FALSE

##### background_refresh_metadata_interval_millis

- 含义：接连两次 Hive 元数据缓存刷新之间的间隔。
- 单位：毫秒
- 默认值：600000
- 引入版本：2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- 含义：Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。
- 默认值：86400
- 单位：秒
- 引入版本：2.5.5

##### enable_statistics_collect_profile

- 含义：统计信息查询时是否生成 Profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计查询生成 Profile。
=======
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

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
#### 用户，角色，权限

##### privilege_max_total_roles_per_user

- 含义：每个用户最多可以拥有的角色数量。
- 默认值：64
- 引入版本：v3.0.0

##### privilege_max_role_depth

- 含义：每个角色最多的嵌套层数。
- 默认值：16
- 引入版本：v3.0.0

#### Query engine

##### max_allowed_in_element_num_of_delete

- 含义：DELETE 语句中 IN 谓词最多允许的元素数量。
- 默认值：10000

##### enable_materialized_view

- 含义：是否允许创建物化视图。
- 默认值：TRUE

##### enable_decimal_v3

- 含义：是否开启 Decimal V3。
- 默认值：TRUE

##### expr_children_limit

- 含义：一个表达式中子表达式的最大数量。
- 默认值：10000

##### enable_sql_blacklist

- 含义：是否开启 SQL Query 黑名单校验。如果开启，在黑名单中的 Query 不能被执行。
- 默认值：FALSE

##### dynamic_partition_check_interval_seconds

- 含义：动态分区检查的时间周期。如果有新数据生成，会自动生成分区。
- 单位：秒
- 默认值：600

##### dynamic_partition_enable

- 含义：是否开启动态分区功能。打开后，您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。
- 默认值：TRUE

##### http_slow_request_threshold_ms

- 含义：如果一条 HTTP 请求的时间超过了该参数指定的时长，会生成日志来跟踪该请求。
- 单位：毫秒
- 默认值：5000
- 引入版本：2.5.15，3.1.5

##### max_partitions_in_one_batch

- 含义：批量创建分区时，分区数目的最大值。
- 默认值：4096

##### max_query_retry_time

- 含义：FE 上查询重试的最大次数。
- 默认值：2

##### max_create_table_timeout_second

- 含义：建表的最大超时时间。
- 单位：秒
- 默认值：600

##### max_running_rollup_job_num_per_table

- 含义：每个 Table 执行 Rollup 任务的最大并发度。
- 默认值：1

##### max_planner_scalar_rewrite_num

- 含义：优化器重写 ScalarOperator 允许的最大次数。
- 默认值：100000

##### enable_statistic_collect

- 含义：是否采集统计信息，该开关默认打开。
- 默认值：TRUE

##### enable_collect_full_statistic

- 含义：是否开启自动全量统计信息采集，该开关默认打开。
- 默认值：TRUE

##### statistic_auto_collect_ratio

- 含义：自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。
- 默认值：0.8

##### statistic_max_full_collect_data_size

- 含义：自动统计信息采集的最大分区大小。<br />如果超过该值，则放弃全量采集，转为对该表进行抽样采集。
- 单位：GB
- 默认值：100

##### statistic_collect_interval_sec

- 含义：自动定期采集任务中，检测数据更新的间隔时间，默认为 5 分钟。
- 单位：秒
- 默认值：300

##### statistic_auto_analyze_start_time

- 含义：用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。
- 类型：STRING
- 默认值：00:00:00
- 引入版本：2.5.0

##### statistic_auto_analyze_end_time

- 含义：用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。
- 类型：STRING
- 默认值：23:59:59
- 引入版本：2.5.0

##### statistic_sample_collect_rows

- 含义：最小采样行数。如果指定了采集类型为抽样采集（SAMPLE），需要设置该参数。<br />如果参数取值超过了实际的表行数，默认进行全量采集。
- 默认值：200000

##### histogram_buckets_size

- 含义：直方图默认分桶数。
- 默认值：64

##### histogram_mcv_size

- 含义：直方图默认 most common value 的数量。
- 默认值：100

##### histogram_sample_ratio

- 含义：直方图默认采样比例。
- 默认值：0.1

##### histogram_max_sample_row_count

- 含义：直方图最大采样行数。
- 默认值：10000000

##### statistics_manager_sleep_time_sec

- 含义：统计信息相关元数据调度间隔周期。系统根据这个间隔周期，来执行如下操作：<ul><li>创建统计信息表；</li><li>删除已经被删除的表的统计信息；</li><li>删除过期的统计信息历史记录。</li></ul>
- 单位：秒
- 默认值：60

##### statistic_update_interval_sec

- 含义：统计信息内存 Cache 失效时间。
- 单位：秒
- 默认值：24 \* 60 \* 60

##### statistic_analyze_status_keep_second

- 含义：统计信息采集任务的记录保留时间，默认为 3 天。
- 单位：秒
- 默认值：259200

##### statistic_collect_concurrency

- 含义：手动采集任务的最大并发数，默认为 3，即最多可以有 3 个手动采集任务同时运行。超出的任务处于 PENDING 状态，等待调度。
- 默认值：3

#### enable_statistic_collect_on_first_load

- 含义：当空表第一次导入数据时，是否自动触发统计信息采集。如果一张表包含多个分区，只要是某个空的分区第一次导入数据，都会触发该分区的统计信息采集。如果系统频繁创建新表并且导入数据，会存在一定内存和 CPU 开销。
- 默认值：true
- 引入版本：v3.1

##### enable_local_replica_selection

- 含义：是否选择本地副本进行查询。本地副本可以减少数据传输的网络时延。<br />如果设置为 true，优化器优先选择与当前 FE 相同 IP 的 BE 节点上的 tablet 副本。设置为 false 表示选择可选择本地或非本地副本进行查询。
- 默认值：FALSE

##### max_distribution_pruner_recursion_depth

- 含义：分区裁剪允许的最大递归深度。增加递归深度可以裁剪更多元素但同时增加 CPU 资源消耗。
- 默认值：100

##### enable_udf

- 含义：是否开启 UDF。
- 默认值：FALSE

#### 导入和导出

##### max_broker_load_job_concurrency

- 含义：StarRocks 集群中可以并行执行的 Broker Load 作业的最大数量。本参数仅适用于 Broker Load。取值必须小于 `max_running_txn_num_per_db`。从 2.5 版本开始，该参数默认值从 `10` 变为 `5`。参数别名 `async_load_task_pool_size`。
- 默认值：5

##### load_straggler_wait_second

- 含义：控制 BE 副本最大容忍的导入落后时长，超过这个时长就进行克隆。
- 单位：秒
- 默认值：300

##### desired_max_waiting_jobs

- 含义：最多等待的任务数，适用于所有的任务，建表、导入、schema change。<br />如果 FE 中处于 PENDING 状态的作业数目达到该值，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。从 2.5 版本开始，该参数默认值从 100 变为 1024。
=======
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：1024
- 类型：Int
- 单位：KB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### max_load_timeout_second
=======
<!--
##### hdfs_file_system_expire_seconds
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：300
- 别名：hdfs_file_sytem_expire_seconds
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### min_load_timeout_second
=======
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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：BDB JE 操作的锁超时时间。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### max_running_txn_num_per_db
=======
##### bdbje_reset_election_group
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：false
- 类型：String
- 单位：-
- 是否动态：否
- 描述：是否重置 BDBJE 复制组。如果设置为 `TRUE`，FE 将重置 BDBJE 复制组（即删除所有 FE 节点的信息）并以 Leader 身份启动。重置后，该 FE 将成为集群中唯一的成员，其他 FE 节点通过 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入该集群。仅当无法成功选举出 leader FE 时（因为大部分 follower FE 数据已损坏）才使用此配置。该参数用来替代 `metadata_failure_recovery`。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### load_parallel_instance_num
=======
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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### disable_load_job

- 含义：是否禁用任何导入任务，集群出问题时的止损措施。设置为 TRUE 时，无法进行导入任务，集群仅处于可读状态。设置为 `TRUE` 时，无法进行导入任务，集群仅处于可读状态。
- 默认值：FALSE

##### history_job_keep_max_second
=======
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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：512 * 1024 * 1024
- 类型：Long
- Unit:
- 是否动态：否
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### label_keep_max_num

- 含义：一定时间内所保留导入任务的最大数量。超过之后历史导入作业的信息会被删除。
- 默认值：1000

##### label_keep_max_second

- 含义：已经完成、且处于 FINISHED 或 CANCELLED 状态的导入作业记录在 StarRocks 系统 label 的保留时长，默认值为 3 天。<br />该参数配置适用于所有模式的导入作业。- 单位：秒。设定过大将会消耗大量内存。
- 默认值：259200

##### max_routine_load_task_concurrent_num

- 含义：每个 Routine Load 作业最大并发执行的 task 数。
- 默认值：5

##### max_routine_load_task_num_per_be

- 含义：每个 BE 并发执行的 Routine Load 导入任务数量上限。从 3.1.0 版本开始，参数默认值从 5 变为 16，并且不再需要小于等于 BE 的配置项 `routine_load_thread_pool_size`（已废弃）。
- 默认值：16

##### max_routine_load_batch_size

- 含义：每个 Routine Load task 导入的最大数据量。
- 单位：字节
- 默认值：4294967296

##### routine_load_task_consume_second

- 含义：集群内每个 Routine Load 导入任务消费数据的最大时间。<br />自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_consume_second`，作用于单个 Routine Load 导入作业内的导入任务，更加灵活。
- 单位：秒
- 默认值：15

##### routine_load_task_timeout_second

- 含义：集群内每个 Routine Load 导入任务超时时间，- 单位：秒。<br />自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_timeout_second`，作用于单个 Routine Load 导入作业内的任务，更加灵活。
- 单位：秒
- 默认值：60

#### routine_load_unstable_threshold_second

- 含义：Routine Load 导入作业的任一导入任务消费延迟，即正在消费的消息时间戳与当前时间的差值超过该阈值，且数据源中存在未被消费的消息，则导入作业置为 UNSTABLE 状态。
- 单位：秒
- 默认值：3600

##### max_tolerable_backend_down_num

- 含义：允许的最大故障 BE 数。如果故障的 BE 节点数超过该阈值，则不能自动恢复 Routine Load 作业。
- 默认值：0

##### period_of_auto_resume_min

- 含义：自动恢复 Routine Load 的时间间隔。
- 单位：分钟
- 默认值：5

##### spark_load_default_timeout_second

- 含义：Spark 导入的超时时间。
- 单位：秒
- 默认值：86400

##### spark_home_default_dir

- 含义：Spark 客户端根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"`

##### stream_load_default_timeout_second

- 含义：Stream Load 的默认超时时间。
- 单位：秒
- 默认值：600

##### max_stream_load_timeout_second

- 含义：Stream Load 的最大超时时间。
- 单位：秒
- 默认值：259200

##### insert_load_default_timeout_second

- 含义：Insert Into 语句的超时时间。
- 单位：秒
- 默认值：3600

##### broker_load_default_timeout_second

- 含义：Broker Load 的超时时间。
- 单位：秒
- 默认值：14400

##### min_bytes_per_broker_scanner

- 含义：单个 Broker Load 任务最大并发实例数。
- 单位：字节
- 默认值：67108864

##### max_broker_concurrency

- 含义：单个 Broker Load 任务最大并发实例数。从 3.1 版本起，StarRocks 不再支持该参数。
=======
##### txn_rollback_limit

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：允许回滚的最大事务数。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### export_max_bytes_per_be_per_task
=======
##### heartbeat_mgr_threads_num
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中用于发送心跳任务的最大线程数。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### export_running_job_num_limit
=======
##### heartbeat_mgr_blocking_queue_size
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中存储心跳任务的阻塞队列大小。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### export_task_default_timeout_second
=======
##### catalog_try_lock_timeout_ms
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：全局锁（Global Lock）获取的超时时长。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### empty_load_as_error
=======
##### ignore_materialized_view_error
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否忽略因物化视图错误导致的元数据异常。如果 FE 因为物化视图错误导致的元数据异常而无法启动，您可以通过将该参数设置为 `true` 以忽略错误。
- 引入版本：v2.5.10

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### enable_sync_publish

- 含义：是否在导入事务 publish 阶段同步执行 apply 任务，仅适用于主键模型表。取值：
  - `TRUE`（默认）：导入事务 publish 阶段同步执行 apply 任务，即 apply 任务完成后才会返回导入事务 publish 成功，此时所导入数据真正可查。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，开启该参数可以提升查询性能和稳定性，但是会增加导入耗时。
  - `FALSE`：在导入事务 publish 阶段异步执行 apply 任务，即在导入事务 publish 阶段 apply 任务提交之后立即返回导入事务 publish 成功，然而此时导入数据并不真正可查。这时并发的查询需要等到 apply 任务完成或者超时，才能继续执行。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，关闭该参数会影响查询性能和稳定性。
- 默认值：TRUE
- 引入版本：v3.1.4

##### external_table_commit_timeout_ms
=======
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

<!--
##### enable_materialized_view_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：
- 引入版本：-
-->

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

##### enable_mv_automatic_active_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许系统自动检查和重新激活异步物化视图。启用此功能后，系统将会自动激活因基表（或视图）Schema Change 或重建而失效（Inactive）的物化视图。请注意，此功能不会激活由用户手动设置为 Inactive 的物化视图。
- 引入版本：v3.1.6

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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

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
- 描述：自动统计信息采集的最大分区大小。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。
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
- 描述：集群内每个 Routine Load 导入任务消费数据的最大时间。自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_consume_second`，作用于单个 Routine Load 导入作业内的导入任务，更加灵活。
- 引入版本：-

##### routine_load_task_timeout_second

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：集群内每个 Routine Load 导入任务超时时间，自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_timeout_second`，作用于单个 Routine Load 导入作业内的任务，更加灵活。
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

- 默认值：4 * 3600
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

#### 存储

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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### enable_auto_tablet_distribution

- 含义：是否开启自动设置分桶功能。<ul><li>设置为 `true` 表示开启，您在建表或新增分区时无需指定分桶数目，StarRocks 自动决定分桶数量。自动设置分桶数目的策略，请参见[确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。</li><li>设置为 `false` 表示关闭，您在建表时需要手动指定分桶数量。<br />新增分区时，如果您不指定分桶数量，则新分区的分桶数量继承建表时候的分桶数量。当然您也可以手动指定新增分区的分桶数量。</li></ul>
- 默认值：TRUE
- 引入版本：2.5.6

=======
##### catalog_trash_expire_second

- 默认值：86400
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：删除表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以通过[RECOVER](../sql-reference/sql-statements/data-definition/RECOVER.md) 语句恢复。
- 引入版本：-

##### enable_auto_tablet_distribution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启自动设置分桶功能。
  - 设置为 `true` 表示开启，您在建表或新增分区时无需指定分桶数目，StarRocks 自动决定分桶数量。自动设置分桶数目的策略，请参见[设置分桶数量](../table_design/Data_distribution.md#设置分桶数量)。
  - 设置为 `false` 表示关闭，您在建表时需要手动指定分桶数量。
  - 新增分区时，如果您不指定分桶数量，则新分区的分桶数量继承建表时候的分桶数量。当然您也可以手动指定新增分区的分桶数量。
- 引入版本：v2.5.7

##### enable_experimental_rowstore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启[行列混存表](../table_design/hybrid_table.md)功能。
- 引入版本：-

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### catalog_trash_expire_second
=======
##### alter_table_timeout_second
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Schema Change 超时时间。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### alter_table_timeout_second
=======
##### enable_fast_schema_evolution
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启集群内所有表的 fast schema evolution，取值：`TRUE` 或 `FALSE`。开启后增删列时可以提高 Schema Change 速度并降低资源使用。
- 引入版本：v3.2.0

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### recover_with_empty_tablet
=======
> **说明**
>
> - StarRocks 存算分离集群不支持该参数。
> - 如果您需要为某张表设置该配置，例如关闭该表的 fast schema evolution，则可以在建表时设置表属性 [`fast_schema_evolution`](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#设置-fast-schema-evolution)。

##### recover_with_empty_tablet

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在 Tablet 副本丢失/损坏时，是否使用空的 Tablet 代替。这样可以保证在有 Tablet 副本丢失/损坏时，查询依然能被执行（但是由于缺失了数据，结果可能是错误的）。默认为 `false`，表示不进行替代，查询会失败。
- 引入版本：-
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

##### tablet_create_timeout_second

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### tablet_create_timeout_second

- 含义：创建 tablet 的超时时长。自 3.1 版本起，默认值由 1 改为 10。
- 单位：秒
=======
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
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

<!--
##### tablet_sched_be_down_tolerate_time_s

- 默认值：900
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### tablet_sched_colocate_be_down_tolerate_time_s

- 默认值：12 * 3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
##### tablet_sched_max_balancing_tablets

- 默认值：500
- 别名：max_balancing_tablets
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：正在均衡的 Tablet 数量的最大值。如果正在均衡的 Tablet 数量超过该值，跳过 Tablet 重新均衡。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### tablet_sched_balance_load_disk_safe_threshold
=======
##### tablet_sched_storage_cooldown_second
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：-1
- 别名：storage_cooldown_second
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br />参数别名为 `storage_cooldown_second`。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 `-1`。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
##### tablet_sched_max_not_being_scheduled_interval_ms

- 默认值：15 * 60 * 1000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：克隆 Tablet 调度时，如果超过该时间一直未被调度，则将该 Tablet 的调度优先级升高，以尽可能优先调度。
- 引入版本：-

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
##### tablet_sched_balance_load_score_threshold

- 默认值：0.1
- 别名：balance_load_score_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：用于判断 BE 负载是否均衡的百分比阈值。如果一个 BE 的负载低于所有 BE 的平均负载，且差值大于该阈值，则认为该 BE 处于低负载状态。相反，如果一个 BE 的负载比平均负载高且差值大于该阈值，则认为该 BE 处于高负载状态。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
##### tablet_sched_balance_load_disk_safe_threshold

- 默认值：0.5
- 别名：balance_load_disk_safe_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：判断 BE 磁盘使用率是否均衡的百分比阈值。如果所有 BE 的磁盘使用率低于该值，认为磁盘使用均衡。当有 BE 磁盘使用率超过该阈值时，如果最大和最小 BE 磁盘使用率之差高于 10%，则认为磁盘使用不均衡，会触发 Tablet 重新均衡。
- 引入版本：-

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### tablet_sched_max_not_being_scheduled_interval_ms
=======
<!--
##### tablet_sched_checker_interval_seconds
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：20
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
#### 存算分离相关动态参数

##### lake_compaction_score_selector_min_score
=======
<!--
##### tablet_sched_max_migration_task_sent_once

- 默认值：1000
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

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

<!--
##### enable_load_volume_from_conf

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

##### cloud_native_storage_type

- 默认值：S3
- 类型：String
- 单位：-
- 是否动态：否
- 描述：您使用的存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS 、Azure Blob（自 v3.1.1 起支持）、以及兼容 S3 协议的对象存储中（例如 AWS S3、Google GCP、阿里云 OSS 以及 MinIO）。有效值：`S3`（默认）、`AZBLOB` 和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果您将此项指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
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

Compaction Score 代表了一个表分区是否值得进行 Compaction 的评分，您可以通过 [SHOW PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW_PARTITIONS.md) 语句返回中的 `MaxCS` 一列的值来查看某个分区的 Compaction Score。Compaction Score 和分区中的文件数量有关系。文件数量过多将影响查询性能，因此系统后台会定期执行 Compaction 操作来合并小文件，减少文件数量。

##### lake_compaction_max_tasks

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下允许同时执行的 Compaction 任务数。系统依据分区中 Tablet 数量来计算 Compaction 任务数。如果一个分区有 10 个 Tablet，那么对该分区作一次 Compaction 就会创建 10 个 Compaction 任务。如果正在执行中的 Compaction 任务数超过该阈值，系统将不会创建新的 Compaction 任务。将该值设置为 `0` 表示禁止 Compaction，设置为 `-1` 表示系统依据自适应策略自动计算该值。
- 引入版本：v3.1.0

##### lake_compaction_history_size

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### lake_compaction_history_size

- 含义：在 Leader FE 节点内存中保留多少条最近成功的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
=======
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：12
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下在 Leader FE 节点内存中保留多少条最近成功的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 引入版本：v3.1.0

##### lake_compaction_fail_history_size

- 默认值：12
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下在 Leader FE 节点内存中保留多少条最近失败的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近失败的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 引入版本：v3.1.0

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
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

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

- 默认值：5
- 类型：Long
- 单位：Minutes
- 是否动态：是
- 描述：存算分离集群下保留历史数据版本的时间范围。此时间范围内的历史数据版本不会被自动清理。您需要将该值设置为大于最大查询时间，以避免正在访问中的数据被删除导致查询失败。
- 引入版本：v3.1.0

##### lake_autovacuum_stale_partition_threshold

- 默认值：12
- 类型：Long
- 单位：Hours
- 是否动态：是
- 描述：存算分离集群下，如果某个表分区在该阈值范围内没有任何更新操作(导入、删除或 Compaction)，将不再触发该分区的自动垃圾数据清理操作。
- 引入版本：v3.1.0

<<<<<<< HEAD:docs/zh/administration/Configuration.md
#### 其他动态参数

##### plugin_enable
=======
##### lake_enable_ingest_slowdown

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为存算分离集群开启导入限速功能。开启导入限速功能后，当某个表分区的 Compaction Score 超过了 `lake_ingest_slowdown_threshold`，该表分区上的导入任务将会被限速。只有当 `run_mode` 设置为 `shared_data` 后，该配置项才会生效。
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

- 默认值：0
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：表分区的 Compaction Score 的上限, `0` 表示没有上限。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。当表分区 Compaction Score 达到或超过该上限后，所有涉及到该分区的导入任务将会被无限延迟提交，直到 Compaction Score 降到该值以下或者任务超时。
- 引入版本：v3.2.0

### 其他

##### tmp_dir
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br />这些过程完成以后，所产生的临时文件会被清除掉。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### max_small_file_number
=======
##### plugin_dir
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：System.getenv("STARROCKS_HOME") + "/plugins"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：插件的安装目录。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### max_small_file_size_bytes
=======
##### plugin_enable
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启了插件功能。只能在 Leader FE 安装/卸载插件。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
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

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
##### agent_task_resend_wait_time_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：Agent task 重新发送前的等待时间。当代理任务的创建时间已设置，并且距离现在超过该值，才能重新发送代理任务。该参数用于防止过于频繁的代理任务发送。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### backup_job_default_timeout_ms
=======
<!--
##### start_with_incomplete_meta
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### enable_experimental_mv
=======
<!--
##### max_backend_down_time_second
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### authentication_ldap_simple_bind_base_dn
=======
<!--
##### lake_enable_batch_publish_version 

- 默认值：false
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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：LDAP 服务器所在主机的主机名。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### authentication_ldap_simple_bind_root_dn

- 含义：检索用户时，使用的管理员账号的 DN。
- 默认值：空字符串

##### authentication_ldap_simple_bind_root_pwd

- 含义：检索用户时，使用的管理员账号的密码。
- 默认值：空字符串

##### authentication_ldap_simple_server_host

- 含义：LDAP 服务器所在主机的主机名。
- 默认值：空字符串

##### authentication_ldap_simple_server_port

- 含义：LDAP 服务器的端口。
=======
##### authentication_ldap_simple_server_port

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：389
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：LDAP 服务器的端口。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
##### authentication_ldap_simple_bind_base_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：检索用户时，使用的 Base DN，用于指定 LDAP 服务器检索用户鉴权信息的起始点。
- 引入版本：-

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
##### authentication_ldap_simple_user_search_attr

- 默认值：uid
- 类型：String
- 单位：-
- 是否动态：是
- 描述：LDAP 对象中标识用户的属性名称。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
=======
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
##### max_automatic_partition_number

- 默认值：4096
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
##### stream_load_profile_collect_second

- 默认值：10
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
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

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### allow_system_reserved_names

- **默认值**: FALSE
- 是否允许用户创建以 `__op` 或 `__row` 开头命名的列。TRUE 表示启用此功能。请注意，在 StarRocks 中，这样的列名被保留用于特殊目的，创建这样的列可能导致未知行为，因此系统默认禁止使用这类名字。该参数自 v3.1.5 起新增。

##### enable_mv_automatic_active_check
=======
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
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md

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
##### lock_manager_enable_loading_using_fine_granularity_lock

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

<!--
##### replication_max_parallel_table_count

- 默认值：100
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### replication_max_parallel_data_size_mb

- 默认值：10240
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### replication_transaction_timeout_sec

- 默认值：1 * 60 * 60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### default_mv_refresh_immediate

- 含义：创建异步物化视图后，是否立即刷新该物化视图。当设置为 `true` 时，异步物化视图创建后会立即刷新。
- 默认值：TRUE
- 引入版本：v3.1.7

### 配置 FE 静态参数

以下 FE 配置项为静态参数，不支持在线修改，您需要在 `fe.conf` 中修改并重启 FE。

本节对 FE 静态参数做了如下分类：

- [Log](#log-fe-静态)
- [Server](#server-fe-静态)
- [元数据与集群管理](#元数据与集群管理fe-静态)
- [Query engine](#query-enginefe-静态)
- [导入和导出](#导入和导出fe-静态)
- [存储](#存储fe-静态)
- [其他动态参数](#其他动态参数)

#### Log (FE 静态)

##### log_roll_size_mb

- 含义：日志文件的大小。
- 单位：MB
- 默认值：1024，表示每个日志文件的大小为 1 GB。

##### sys_log_dir

- 含义：系统日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

##### sys_log_level

- 含义：系统日志的级别，从低到高依次为 `INFO`、`WARN`、`ERROR`、`FATAL`。
- 默认值：INFO

##### sys_log_verbose_modules

- 含义：打印系统日志的模块。如果设置参数取值为 `org.apache.starrocks.catalog`，则表示只打印 Catalog 模块下的日志。
- 默认值：空字符串

##### sys_log_roll_interval

- 含义：系统日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

##### sys_log_delete_age

- 含义：系统日志文件的保留时长。默认值 `7d` 表示系统日志文件可以保留 7 天，保留时长超过 7 天的系统日志文件会被删除。
- 默认值：`7d`

##### sys_log_roll_num

- 含义：每个 `sys_log_roll_interval` 时间段内，允许保留的系统日志文件的最大数目。
- 默认值：10

##### audit_log_dir

- 含义：审计日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

##### audit_log_roll_num

- 含义：每个 `audit_log_roll_interval` 时间段内，允许保留的审计日志文件的最大数目。
- 默认值：90

##### audit_log_modules

- 含义：打印审计日志的模块。默认打印 slow_query 和 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 默认值：slow_query, query

##### audit_log_roll_interval

- 含义：审计日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

##### audit_log_delete_age

- 含义：审计日志文件的保留时长。- 默认值 `30d` 表示审计日志文件可以保留 30 天，保留时长超过 30 天的审计日志文件会被删除。
- 默认值：`30d`

##### dump_log_dir

- 含义：Dump 日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

##### dump_log_modules

- 含义：打印 Dump 日志的模块。默认打印 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 默认值：query

##### dump_log_roll_interval

- 含义：Dump 日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

##### dump_log_roll_num

- 含义：每个 `dump_log_roll_interval` 时间内，允许保留的 Dump 日志文件的最大数目。
- 默认值：10

##### dump_log_delete_age

- 含义：Dump 日志文件的保留时长。- 默认值 `7d` 表示 Dump 日志文件可以保留 7 天，保留时长超过 7 天的 Dump 日志文件会被删除。
- 默认值：`7d`

#### Server (FE 静态)

##### frontend_address

- 含义：FE 节点的 IP 地址。
- 默认值：0.0.0.0

##### priority_networks

- 含义：为那些有多个 IP 地址的服务器声明一个选择策略。 <br />请注意，最多应该有一个 IP 地址与此列表匹配。这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。 如果没有匹配这条规则的ip，会随机选择一个。
- 默认值：空字符串

##### http_port

- 含义：FE 节点上 HTTP 服务器的端口。
- 默认值：8030

##### http_worker_threads_num

- 含义：Http Server 用于处理 HTTP 请求的线程数。如果配置为负数或 0 ，线程数将设置为 CPU 核数的 2 倍。
- 默认值：0
- 引入版本：2.5.18，3.0.10，3.1.7，3.2.2

##### http_backlog_num

- 含义：HTTP 服务器支持的 Backlog 队列长度。
- 默认值：1024

##### cluster_name

- 含义：FE 所在 StarRocks 集群的名称，显示为网页标题。
- 默认值：StarRocks Cluster

##### rpc_port

- 含义：FE 节点上 Thrift 服务器的端口。
- 默认值：9020

##### thrift_backlog_num

- 含义：Thrift 服务器支持的 Backlog 队列长度。
- 默认值：1024

##### thrift_server_max_worker_threads

- 含义：Thrift 服务器支持的最大工作线程数。
- 默认值：4096

##### thrift_client_timeout_ms

- 含义：Thrift 客户端链接的空闲超时时间，即链接超过该时间无新请求后则将链接断开。
- 单位：毫秒。
- 默认值：5000

##### thrift_server_queue_size

- 含义：Thrift 服务器 pending 队列长度。如果当前处理线程数量超过了配置项 `thrift_server_max_worker_threads` 的值，则将超出的线程加入 pending 队列。
- 默认值：4096

##### brpc_idle_wait_max_time

- 含义：bRPC 的空闲等待时间。单位：毫秒。
- 默认值：10000

##### query_port

- 含义：FE 节点上 MySQL 服务器的端口。
- 默认值：9030

#### mysql_service_nio_enabled

- 含义：是否开启 MySQL 服务器的异步 I/O 选项。
- 默认值：TRUE

##### mysql_service_io_threads_num

- 含义：MySQL 服务器中用于处理 I/O 事件的最大线程数。
- 默认值：4

##### mysql_nio_backlog_num

- 含义：MySQL 服务器支持的 Backlog 队列长度。
- 默认值：1024

##### max_mysql_service_task_threads_num

- 含义：MySQL 服务器中用于处理任务的最大线程数。
- 默认值：4096

##### mysql_server_version

- 含义：MySQL 服务器的版本。修改该参数配置会影响以下场景中返回的版本号：1. `select version();` 2. Handshake packet 版本 3. 全局变量 `version` 的取值 (`show variables like 'version';`)
- 默认值：5.1.0

##### max_connection_scheduler_threads_num

- 含义：连接调度器支持的最大线程数。
- 默认值：4096

##### qe_max_connection

- 含义：FE 支持的最大连接数，包括所有用户发起的连接。
- 默认值：1024

##### check_java_version

- 含义：检查已编译的 Java 版本与运行的 Java 版本是否兼容。<br />如果不兼容，则上报 Java 版本不匹配的异常信息，并终止启动。
- 默认值：TRUE

#### 元数据与集群管理（FE 静态）

##### meta_dir

- 含义：元数据的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/meta"`

##### heartbeat_mgr_threads_num

- 含义：Heartbeat Manager 中用于发送心跳任务的最大线程数。
=======
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

<!--
##### black_host_history_sec

- 默认值：2 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：
- 引入版本：-
-->

<!--
##### black_host_connect_failures_within_time

- 默认值：5
- 类型：Long
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->

##### jdbc_connection_pool_size

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 的容量上限。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### heartbeat_mgr_blocking_queue_size

- 含义：Heartbeat Manager 中存储心跳任务的阻塞队列大小。
- 默认值：1024

##### metadata_failure_recovery

- 含义：是否强制重置 FE 的元数据。请谨慎使用该配置项。
- 默认值：FALSE

##### edit_log_port

- 含义：FE 所在 StarRocks 集群中各 Leader FE、Follower FE、Observer FE 之间通信用的端口。
- 默认值：9010

##### edit_log_type

- 含义：编辑日志的类型。取值只能为 `BDB`。
- 默认值：BDB

##### bdbje_heartbeat_timeout_second

- 含义：FE 所在 StarRocks 集群中 Leader FE 和 Follower FE 之间的 BDB JE 心跳超时时间。
- 单位：秒。
- 默认值：30

##### bdbje_lock_timeout_second

- 含义：BDB JE 操作的锁超时时间。
- 单位：秒。
=======
##### jdbc_minimum_idle_connections

>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 中处于 idle 状态的连接最低数量。
- 引入版本：-

<<<<<<< HEAD:docs/zh/administration/Configuration.md
##### max_bdbje_clock_delta_ms

- 含义：FE 所在 StarRocks 集群中 Leader FE 与非 Leader FE 之间能够容忍的最大时钟偏移。
- 单位：毫秒。
- 默认值：5000

##### txn_rollback_limit

- 含义：允许回滚的最大事务数。
- 默认值：100

#### bdbje_replica_ack_timeout_second

- 含义：FE 所在 StarRocks 集群中，元数据从 Leader FE 写入到多个 Follower FE 时，Leader FE 等待足够多的 Follower FE 发送 ACK 消息的超时时间。当写入的元数据较多时，可能返回 ACK 的时间较长，进而导致等待超时。如果超时，会导致写元数据失败，FE 进程退出，此时可以适当地调大该参数取值。
- 单位：秒
- 默认值：10

##### master_sync_policy

- 含义：FE 所在 StarRocks 集群中，Leader FE 上的日志刷盘方式。该参数仅在当前 FE 为 Leader 时有效。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。 </li></ul>如果您只部署了一个 Follower FE，建议将其设置为 `SYNC`。 如果您部署了 3 个及以上 Follower FE，建议将其与下面的 `replica_sync_policy` 均设置为 `WRITE_NO_SYNC`。
- 默认值：SYNC

##### replica_sync_policy

- 含义：FE 所在 StarRocks 集群中，Follower FE 上的日志刷盘方式。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。</li></ul>
- 默认值：SYNC

##### replica_ack_policy

- 含义：判定日志是否有效的策略，默认是多数 Follower FE 返回确认消息，就认为生效。
- 默认值：SIMPLE_MAJORITY

##### cluster_id

- 含义：FE 所在 StarRocks 集群的 ID。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。- 默认值 `-1` 表示在 Leader FE 首次启动时随机生成一个。
- 默认值：-1

#### Query engine（FE 静态）

##### publish_version_interval_ms

- 含义：两个版本发布操作之间的时间间隔。
- 单位：毫秒
- 默认值：10

##### statistic_cache_columns

- 含义：缓存统计信息表的最大行数。
- 默认值：100000

#### 导入和导出（FE 静态）

##### load_checker_interval_second

- 含义：导入作业的轮询间隔。
- 单位：秒。
- 默认值：5

##### transaction_clean_interval_second

- 含义：已结束事务的清理间隔。建议清理间隔尽量短，从而确保已完成的事务能够及时清理掉。
- 单位：秒。
- 默认值：30

##### label_clean_interval_second

- 含义：作业标签的清理间隔。建议清理间隔尽量短，从而确保历史作业的标签能够及时清理掉。
- 单位：秒。
- 默认值：14400

##### spark_dpp_version

- 含义：Spark DPP 特性的版本。
- 默认值：1.0.0

##### spark_resource_path

- 含义：Spark 依赖包的根目录。
- 默认值：空字符串

##### spark_launcher_log_dir

- 含义：Spark 日志的保存目录。
- 默认值：`sys_log_dir + "/spark_launcher_log"`

##### yarn_client_path

- 含义：Yarn 客户端的根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"`

##### yarn_config_dir

- 含义：Yarn 配置文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"`

##### export_checker_interval_second

- 含义：导出作业调度器的调度间隔。
- 默认值：5

##### export_task_pool_size

- 含义：导出任务线程池的大小。
- 默认值：5

##### broker_client_timeout_ms

- 含义：Broker RPC 的默认超时时间。
- 单位：毫秒
- 默认值 10s

#### 存储（FE 静态）

##### tablet_sched_storage_cooldown_second

- 含义：从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br />参数别名为 `storage_cooldown_second`。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 0。
- 单位：秒。
- 默认值：-1

##### tablet_stat_update_interval_second

- 含义：FE 向每个 BE 请求收集 Tablet 统计信息的时间间隔。
- 单位：秒。
- 默认值：300

#### StarRocks 存算分离集群

##### run_mode

- 含义：StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing` (默认)。`shared_data` 表示在存算分离模式下运行 StarRocks。`shared_nothing` 表示在存算一体模式下运行 StarRocks。<br />**注意**<br />StarRocks 集群不支持存算分离和存算一体模式混合部署。<br />请勿在集群部署完成后更改 `run_mode`，否则将导致集群无法再次启动。不支持从存算一体集群转换为存算分离集群，反之亦然。
- 默认值：shared_nothing

##### cloud_native_meta_port

- 含义：云原生元数据服务监听端口。
- 默认值：6090

##### cloud_native_storage_type

- 含义：您使用的存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS 、Azure Blob（自 v3.1.1 起支持）、以及兼容 S3 协议的对象存储中（例如 AWS S3、Google GCP、阿里云 OSS 以及 MinIO）。有效值：`S3`（默认）、`AZBLOB` 和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果您将此项指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 默认值：S3

##### cloud_native_hdfs_url

- 含义：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。

##### aws_s3_path

- 含义：用于存储数据的 S3 存储空间路径，由 S3 存储桶的名称及其下的子路径（如有）组成，如 `testbucket/subpath`。

##### aws_s3_region

- 含义：需访问的 S3 存储空间的地区，如 `us-west-2`。

##### aws_s3_endpoint

- 含义：访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。

##### aws_s3_use_aws_sdk_default_behavior

- 含义：是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。
- 默认值：false

##### aws_s3_use_instance_profile

- 含义：是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws_s3_iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要额外指定 `aws_s3_external_id`。</li></ul>
- 默认值：false

##### aws_s3_access_key

- 含义：访问 S3 存储空间的 Access Key。

##### aws_s3_secret_key

- 含义：访问 S3 存储空间的 Secret Key。

##### aws_s3_iam_role_arn

- 含义：有访问 S3 存储空间权限 IAM Role 的 ARN。

##### aws_s3_external_id

- 含义：用于跨 AWS 账户访问 S3 存储空间的外部 ID。

##### azure_blob_path

- 含义：用于存储数据的 Azure Blob Storage 路径，由存 Storage Account 中的容器名称和容器下的子路径（如有）组成，如 `testcontainer/subpath`。

##### azure_blob_endpoint

- 含义：Azure Blob Storage 的链接地址，如 `https://test.blob.core.windows.net`。

##### azure_blob_shared_key

- 含义：访问 Azure Blob Storage 的 Shared Key。

##### azure_blob_sas_token

- 含义：访问 Azure Blob Storage 的共享访问签名（SAS）。
- 默认值：N/A

#### 其他静态参数

##### plugin_dir

- 含义：插件的安装目录。
- 默认值：`STARROCKS_HOME_DIR/plugins`

##### small_file_dir

- 含义：小文件的根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/small_files"`

##### max_agent_task_threads_num

- 含义：代理任务线程池中用于处理代理任务的最大线程数。
- 默认值：4096

##### auth_token

- 含义：用于内部身份验证的集群令牌。为空则在 Leader FE 第一次启动时随机生成一个。
- 默认值：空字符串

##### tmp_dir

- 含义：临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br />这些过程完成以后，所产生的临时文件会被清除掉。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"`

##### locale

- 含义：FE 所使用的字符集。
- 默认值：zh_CN.UTF-8

##### hive_meta_load_concurrency

- 含义：Hive 元数据支持的最大并发线程数。
- 默认值：4

##### hive_meta_cache_refresh_interval_s

- 含义：刷新 Hive 外表元数据缓存的时间间隔。
- 单位：秒。
- 默认值：7200

##### hive_meta_cache_ttl_s

- 含义：Hive 外表元数据缓存的失效时间。
- 单位：秒。
- 默认值：86400

##### hive_meta_store_timeout_s

- 含义：连接 Hive Metastore 的超时时间。
- 单位：秒。
- 默认值：10

##### es_state_sync_interval_second

- 含义：FE 获取 Elasticsearch Index 和同步 StarRocks 外部表元数据的时间间隔。
- 单位：秒
- 默认值：10

##### enable_auth_check

- 含义：是否开启鉴权检查功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 默认值：TRUE

##### enable_metric_calculator

- 含义：是否开启定期收集指标 (Metrics) 的功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 默认值：TRUE

## BE 配置项

部分 BE 节点配置项为动态参数，您可以通过命令在线修改。其他配置项为静态参数，需要通过修改 **be.conf** 文件后重启 BE 服务使相关修改生效。

### 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

### 配置 BE 动态参数

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

- 含义：失效 rowset 的清理间隔。缩短该间隔可以降低导入时元数据的占用。
- 单位：秒
- 默认值：1800

#### snapshot_expire_time_sec

- 含义：快照文件的保留时长，默认 48 个小时。后台会按照配置的清理间隔来定期清理保留时长超过 `snapshot_expire_time_sec` 的快照文件。清理间隔受磁盘空间使用率影响：当磁盘空间使用率小于 60% 时，会按照最大清理间隔 `max_garbage_sweep_interval` 来清理；当磁盘空间大于 80% 时，会按照最小清理间隔 `min_garbage_sweep_interval` 来清理。
- 单位：秒
- 默认值：172800

#### trash_file_expire_time_sec

- 含义：Trash 目录下文件的保留时长，默认 1 天。后台会按照配置的清理间隔来定期清理保留时长超过 `trash_file_expire_time_sec` 的文件内容。清理间隔受磁盘空间使用率影响：当磁盘空间使用率小于 60% 时，会按照最大清理间隔 `max_garbage_sweep_interval` 来清理；当磁盘空间大于 80% 时，会按照最小清理间隔 `min_garbage_sweep_interval` 来清理。自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259200 变为 86400。
- 单位：秒
- 默认值：86400

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

- 含义：主键表 compaction 的检查间隔。
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

- 含义：是否开启 Size-tiered Compaction 策略 (Primary Key 表除外)。
- 默认值：true

#### enable_pk_size_tiered_compaction_strategy

- 含义：是否为 Primary Key 表开启 Size-tiered Compaction 策略。
- 默认值：true
- 引入版本：v3.2.4，v3.1.10

#### min_compaction_failure_interval_sec

- 含义：Tablet Compaction 失败之后，再次被调度的间隔。
- 单位：秒
- 默认值：120

#### periodic_counter_update_period_ms

- 含义：Counter 统计信息的间隔。
- 单位：毫秒
- 默认值：500

#### pindex_major_compaction_limit_per_disk

- 含义：每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题。
- 默认值：1
- 引入版本：3.0.9

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

#### txn_commit_rpc_timeout_ms

- 含义：Transaction Commit RPC 超时的时长。该参数自 v3.1.0 起弃用。
- 单位：毫秒
- 默认值：60000

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

- 含义：BE 存储目录整体磁盘空间使用率的硬上限。如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，会拒绝 Load 和 Restore 作业。配置该参数时，需要同步修改 FE 配置 `storage_usage_hard_limit_percent` 才能使配置生效。
- 单位：%
- 默认值：95

#### storage_flood_stage_left_capacity_bytes

- 含义：BE 存储目录整体磁盘剩余空间的硬限制。如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，会拒绝 Load 和 Restore 作业，默认 100GB。配置该参数时，需要同步修改 FE 配置 `storage_usage_hard_limit_reserve_bytes` 才能使配置生效。
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

- 含义：Compaction 线程数上限（即 Base Compaction + Cumulative Compaction 的最大并发）。该参数防止 Compaction 占用过多内存。 -1 代表没有限制。0 表示不允许 compaction。
- 默认值：-1

#### lake_enable_vertical_compaction_fill_data_cache

- 含义：存算分离集群下，是否允许 Compaction 任务在执行时缓存数据到本地磁盘上。
- 默认值：false
- 引入版本：v3.1.7、v3.2.3

#### lake_pk_compaction_max_input_rowsets

- 含义：存算分离集群下，主键表 Compaction 任务中允许的最大输入 Rowset 数量。从 v3.2.4 和 v3.1.10 版本开始，该参数默认值从 `5` 变更为 `1000`。存算分离集群中的主键表在开启 Sized-tiered Compaction 策略后 (即设置 `enable_pk_size_tiered_compaction_strategy` 为 `true`)，无需通过限制每次 Compaction 的 Rowset 个数来降低写放大，因此调大该值。
- 默认值：1000
- 引入版本：v3.1.8、v3.2.3

#### compact_threads

- 含义：并发 Compaction 任务的最大线程数。自 v3.1.7，v3.2.2 起变为动态参数。
- 默认值：4
- 引入版本：v3.0.0

#### update_compaction_ratio_threshold

- 含义：存算分离集群下主键模型表单次 Compaction 可以合并的最大数据比例。如果单个 Tablet 过大，建议适当调小该配置项取值。自 v3.1.5 起支持。
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

#### starlet_use_star_cache

- 含义：存算分离模式下是否使用 block data cache。`true` 表示启用该功能，`false` 表示禁用。
- 默认值：true
- 引入版本：3.1

#### starlet_cache_evict_high_water

- 含义：在存算分离模式下启用 file data cache，如果当前剩余磁盘容量百分比低于此配置项中指定的值，StarRocks 将触发缓存淘汰。默认值表示 file data cache 默认最多使用 80% 磁盘容量。
- 默认值：0.2
- 引入版本：3.0

### 配置 BE 静态参数

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

- 含义：单节点上 Routine Load 线程池大小。从 3.1.0 版本起，该参数已经废弃。单节点上 Routine Load 线程池大小完全由 FE 动态参数 `max_routine_load_task_num_per_be` 控制。
- 默认值：10

#### internal_service_async_thread_num

- 含义：单个 BE 上与 Kafka 交互的线程池大小。当前 Routine Load FE 与 Kafka 的交互需经由 BE 完成，而每个 BE 上实际执行操作的是一个单独的线程池。当 Routine Load 任务较多时，可能会出现线程池线程繁忙的情况，可以调整该配置。从 3.1.0 版本起，该参数已经废弃。单节点上 Routine Load 线程池大小完全由 FE 动态参数 `max_routine_load_task_num_per_be` 控制。

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

- 含义：BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100G"）。默认硬上限为 BE 所在机器内存的 90%，软上限为 BE 所在机器内存的 80%。如果 BE 为独立部署，则无需配置，如果 BE 与其它占用内存较多的服务混合部署，则需要合理配置。
- 默认值：90%

#### flush_thread_num_per_store

- 含义：每个 Store 用以 Flush MemTable 的线程数。
- 默认值：2

#### block_cache_enable

- 含义：是否启用 Data Cache。<ul><li>`true`：启用。</li><li>`false`：不启用，为默认值。</li></ul> 如要启用，设置该参数值为 `true`。
- 默认值：false

#### block_cache_disk_path

- 含义：磁盘路径。支持添加多个路径，多个路径之间使用分号(;) 隔开。建议 BE 机器有几个磁盘即添加几个路径。配置路径后，StarRocks 会自动创建名为 **cachelib_data** 的文件用于缓存 block。
- 默认值：N/A

#### block_cache_meta_path

- 含义：Block 的元数据存储目录，可自定义。推荐创建在 **`$STARROCKS_HOME`** 路径下。
- 默认值：N/A

#### block_cache_block_size

- 含义：单个 block 大小，单位：字节。默认值为 `1048576`，即 1 MB。
- 默认值：1048576

#### block_cache_mem_size

- 含义：内存缓存数据量的上限，单位：字节。默认值为 `2147483648`，即 2 GB。推荐将该参数值最低设置成 20 GB。如在开启 Data Cache 期间，存在大量从磁盘读取数据的情况，可考虑调大该参数。
- 单位：字节
- 默认值：2147483648

#### block_cache_disk_size

- 含义：单个磁盘缓存数据量的上限。举例：在 `block_cache_disk_path` 中配置了 2 个磁盘，并设置 `block_cache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。
- 单位：字节
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

#### starlet_star_cache_disk_size_percent

- 含义：存算分离模式下，block data cache 最多可使用的磁盘容量百分比。
- 默认值：80
- 引入版本：3.1
=======
##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：访问 JDBC Catalog 时，连接建立的超时时长。超过参数取值时间的连接被认为是 idle 状态。
- 引入版本：-

<!--
##### max_varchar_length

- 默认值：1048576
- 类型：Int
- Unit:
- 是否动态：是
- 描述：
- 引入版本：-
-->
>>>>>>> 9720a78081 ([Doc] Categorize Parameters (#41964)):docs/zh/administration/FE_configuration.md
