---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.md'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.md'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.md'

# FE 配置

<FEConfigMethod />

## 查看 FE 配置项

在 FE 启动后，您可以在 MySQL 客户端上运行 ADMIN SHOW FRONTEND CONFIG 命令来检查参数配置。如果您想查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细描述，请参见 [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行与集群管理相关的命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 配置或修改 FE 动态参数的设置。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

## 理解 FE 参数

### 日志

##### log_roll_size_mb

- 默认值：1024
- 类型：Int
- 单位：MB
- 是否可变：否
- 描述：系统日志文件或审计日志文件的最大大小。
- 引入版本：-

##### sys_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储系统日志文件的目录。
- 引入版本：-

##### sys_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否可变：否
- 描述：系统日志条目的严重级别。有效值：`INFO`、`WARN`、`ERROR` 和 `FATAL`。
- 引入版本：-

##### sys_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：在 `sys_log_roll_interval` 参数指定的每个保留期内可以保留的系统日志文件的最大数量。
- 引入版本：-

##### sys_log_verbose_modules

- 默认值：空字符串
- 类型：String[]
- 单位：-
- 是否可变：否
- 描述：StarRocks 生成系统日志的模块。如果此参数设置为 `org.apache.starrocks.catalog`，则 StarRocks 仅为 catalog 模块生成系统日志。用逗号（,）和空格分隔模块名称。
- 引入版本：-

##### sys_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 轮换系统日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在系统日志文件名中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在系统日志文件名中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本：-

##### sys_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否可变：否
- 描述：系统日志文件的保留期。默认值 `7d` 指定每个系统日志文件可以保留 7 天。StarRocks 检查每个系统日志文件并删除 7 天前生成的文件。
- 引入版本：-

##### audit_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储审计日志文件的目录。
- 引入版本：-

##### audit_log_roll_num

- 默认值：90
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：在 `audit_log_roll_interval` 参数指定的每个保留期内可以保留的审计日志文件的最大数量。
- 引入版本：-

##### audit_log_modules

- 默认值：slow_query, query
- 类型：String[]
- 单位：-
- 是否可变：否
- 描述：StarRocks 生成审计日志条目的模块。默认情况下，StarRocks 为 `slow_query` 模块和 `query` 模块生成审计日志。从 v3.0 开始支持 `connection` 模块。用逗号（,）和空格分隔模块名称。
- 引入版本：-

##### qe_slow_log_ms

- 默认值：5000
- 类型：Long
- 单位：毫秒
- 是否可变：是
- 描述：用于确定查询是否为慢查询的阈值。如果查询的响应时间超过此阈值，则将其记录为 **fe.audit.log** 中的慢查询。
- 引入版本：-

##### audit_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 轮换审计日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在审计日志文件名中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在审计日志文件名中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本：-

##### audit_log_delete_age

- 默认值：30d
- 类型：String
- 单位：-
- 是否可变：否
- 描述：审计日志文件的保留期。默认值 `30d` 指定每个审计日志文件可以保留 30 天。StarRocks 检查每个审计日志文件并删除 30 天前生成的文件。
- 引入版本：-

##### dump_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储转储日志文件的目录。
- 引入版本：-

##### dump_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：在 `dump_log_roll_interval` 参数指定的每个保留期内可以保留的转储日志文件的最大数量。
- 引入版本：-

##### dump_log_modules

- 默认值：query
- 类型：String[]
- 单位：-
- 是否可变：否
- 描述：StarRocks 生成转储日志条目的模块。默认情况下，StarRocks 为 query 模块生成转储日志。用逗号（,）和空格分隔模块名称。
- 引入版本：-

##### dump_log_roll_interval

- 默认值：DAY
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 轮换转储日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在转储日志文件名中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在转储日志文件名中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本：-

##### dump_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否可变：否
- 描述：转储日志文件的保留期。默认值 `7d` 指定每个转储日志文件可以保留 7 天。StarRocks 检查每个转储日志文件并删除 7 天前生成的文件。
- 引入版本：-

### 服务器

##### frontend_address

- 默认值：0.0.0.0
- 类型：String
- 单位：-
- 是否可变：否
- 描述：FE 节点的 IP 地址。
- 引入版本：-

##### priority_networks

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：声明具有多个 IP 地址的服务器的选择策略。注意，最多只能有一个 IP 地址与此参数指定的列表匹配。此参数的值是一个由条目组成的列表，条目用 CIDR 表示法分隔，例如 10.10.10.0/24。如果没有 IP 地址与此列表中的条目匹配，则将随机选择服务器的可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，系统默认使用 IPv4 地址。您可以通过将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本：-

##### net_use_ipv6_when_priority_networks_empty

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：一个布尔值，用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本：v3.3.0

##### http_port

- 默认值：8030
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 HTTP 服务器监听的端口。
- 引入版本：-

##### http_worker_threads_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：HTTP 服务器处理 HTTP 请求的工作线程数。对于负值或 0 值，线程数将是 CPU 核心数的两倍。
- 引入版本：v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### http_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 HTTP 服务器持有的积压队列长度。
- 引入版本：-

##### enable_http_async_handler

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否允许系统异步处理 HTTP 请求。如果启用此功能，Netty 工作线程接收到的 HTTP 请求将提交到单独的线程池进行服务逻辑处理，以避免阻塞 HTTP 服务器。如果禁用，Netty 工作线程将处理服务逻辑。
- 引入版本：4.0.0

##### http_async_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：异步 HTTP 请求处理的线程池大小。别名为 `max_http_sql_service_task_threads_num`。
- 引入版本：4.0.0

#### enable_https
- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否在 FE 节点中启用 HTTPS 服务器。
- 引入版本：-

#### https_port
- 默认值：8443
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 HTTPS 服务器监听的端口。
- 引入版本：-

##### cluster_name

- 默认值：StarRocks Cluster
- 类型：String
- 单位：-
- 是否可变：否
- 描述：FE 所属的 StarRocks 集群的名称。集群名称显示为网页上的 `Title`。
- 引入版本：-

##### rpc_port

- 默认值：9020
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 Thrift 服务器监听的端口。
- 引入版本：-

##### thrift_server_max_worker_threads

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 节点中 Thrift 服务器支持的最大工作线程数。
- 引入版本：-

##### thrift_server_queue_size

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：请求挂起的队列长度。如果 Thrift 服务器中正在处理的线程数超过 `thrift_server_max_worker_threads` 指定的值，新请求将添加到挂起队列中。
- 引入版本：-

##### thrift_client_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否可变：否
- 描述：空闲客户端连接超时的时间长度。
- 引入版本：-

##### thrift_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 Thrift 服务器持有的积压队列长度。
- 引入版本：-

##### brpc_idle_wait_max_time

- 默认值：10000
- 类型：Int
- 单位：毫秒
- 是否可变：否
- 描述：bRPC 客户端在空闲状态下等待的最长时间。
- 引入版本：-

##### query_port

- 默认值：9030
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 MySQL 服务器监听的端口。
- 引入版本：-

##### mysql_nio_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 MySQL 服务器持有的积压队列长度。
- 引入版本：-

##### mysql_service_nio_enable_keep_alive

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：为 MySQL 连接启用 TCP Keep-Alive。适用于负载均衡器后面的长时间空闲连接。
- 引入版本：-

##### mysql_service_io_threads_num

- 默认值：4
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 MySQL 服务器可以运行的最大线程数以处理 I/O 事件。
- 引入版本：-

##### max_mysql_service_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 节点中 MySQL 服务器可以运行的最大线程数以处理任务。
- 引入版本：-

##### mysql_server_version

- 默认值：8.0.33
- 类型：String
- 单位：-
- 是否可变：是
- 描述：返回给客户端的 MySQL 服务器版本。修改此参数将影响以下情况中的版本信息：
  1. `select version();`
  2. 握手包版本
  3. 全局变量 `version` 的值（`show variables like 'version';`）
- 引入版本：-

##### qe_max_connection

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：所有用户可以与 FE 节点建立的最大连接数。从 v3.1.12 和 v3.2.7 开始，默认值已从 `1024` 更改为 `4096`。
- 引入版本：-

### 元数据和集群管理

##### cluster_id

- 默认值：-1
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 所属的 StarRocks 集群的 ID。具有相同集群 ID 的 FEs 或 BEs 属于同一个 StarRocks 集群。有效值：任何正整数。默认值 `-1` 指定 StarRocks 在集群的 Leader FE 首次启动时为 StarRocks 集群生成一个随机集群 ID。
- 引入版本：-

##### meta_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储元数据的目录。
- 引入版本：-

##### edit_log_type

- 默认值：BDB
- 类型：String
- 单位：-
- 是否可变：否
- 描述：可以生成的编辑日志类型。将值设置为 `BDB`。
- 引入版本：-

##### edit_log_port

- 默认值：9010
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：集群中 Leader、Follower 和 Observer FEs 之间通信使用的端口。
- 引入版本：-

##### edit_log_roll_num

- 默认值：50000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在为这些日志条目创建日志文件之前可以写入的最大元数据日志条目数。此参数用于控制日志文件的大小。新日志文件写入 BDBJE 数据库。
- 引入版本：-

##### metadata_ignore_unknown_operation_type

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否忽略未知日志 ID。当 FE 回滚时，早期版本的 FEs 可能无法识别某些日志 ID。如果值为 `TRUE`，FE 忽略未知日志 ID。如果值为 `FALSE`，FE 退出。
- 引入版本：-

##### meta_delay_toleration_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Follower 和 Observer FEs 上的元数据可以滞后于 Leader FE 上的元数据的最大持续时间。单位：秒。如果超过此持续时间，非 Leader FEs 将停止提供服务。
- 引入版本：-

##### master_sync_policy

- 默认值：SYNC
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Leader FE 刷新日志到磁盘的策略。此参数仅在当前 FE 为 Leader FE 时有效。有效值：
  - `SYNC`：当事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`：当事务提交时，日志条目的生成和刷新不会同时发生。
  - `WRITE_NO_SYNC`：当事务提交时，日志条目同时生成但不会刷新到磁盘。

  如果您只部署了一个 Follower FE，我们建议将此参数设置为 `SYNC`。如果您部署了三个或更多 Follower FEs，我们建议将此参数和 `replica_sync_policy` 都设置为 `WRITE_NO_SYNC`。

- 引入版本：-

##### replica_sync_policy

- 默认值：SYNC
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Follower FE 刷新日志到磁盘的策略。此参数仅在当前 FE 为 Follower FE 时有效。有效值：
  - `SYNC`：当事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`：当事务提交时，日志条目的生成和刷新不会同时发生。
  - `WRITE_NO_SYNC`：当事务提交时，日志条目同时生成但不会刷新到磁盘。
- 引入版本：-

##### replica_ack_policy

- 默认值：SIMPLE_MAJORITY
- 类型：String
- 单位：-
- 是否可变：否
- 描述：日志条目被视为有效的策略。默认值 `SIMPLE_MAJORITY` 指定如果大多数 Follower FEs 返回 ACK 消息，则日志条目被视为有效。
- 引入版本：-

##### bdbje_heartbeat_timeout_second

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：StarRocks 集群中 Leader、Follower 和 Observer FEs 之间的心跳超时的时间。
- 引入版本：-

##### bdbje_replica_ack_timeout_second

- 默认值：10
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：Leader FE 在将元数据从 Leader FE 写入 Follower FEs 时，可以等待指定数量的 Follower FEs 的 ACK 消息的最长时间。单位：秒。如果正在写入大量元数据，Follower FEs 需要很长时间才能返回 ACK 消息给 Leader FE，导致 ACK 超时。在这种情况下，元数据写入失败，FE 进程退出。我们建议您增加此参数的值以防止这种情况。
- 引入版本：-

##### bdbje_lock_timeout_second

- 默认值：1
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：基于 BDB JE 的 FE 中锁超时的时间。
- 引入版本：-

##### bdbje_reset_election_group

- 默认值：false
- 类型：String
- 单位：-
- 是否可变：否
- 描述：是否重置 BDBJE 复制组。如果此参数设置为 `TRUE`，FE 将重置 BDBJE 复制组（即，删除所有可选 FE 节点的信息）并作为 Leader FE 启动。重置后，此 FE 将是集群中的唯一成员，其他 FEs 可以通过使用 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入此集群。仅在由于大多数 Follower FEs 的数据已损坏而无法选举出 Leader FE 时使用此设置。`reset_election_group` 用于替代 `metadata_failure_recovery`。
- 引入版本：-

##### max_bdbje_clock_delta_ms

- 默认值：5000
- 类型：Long
- 单位：毫秒
- 是否可变：否
- 描述：StarRocks 集群中 Leader FE 与 Follower 或 Observer FEs 之间允许的最大时钟偏移。
- 引入版本：-

##### txn_rollback_limit

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：可以回滚的最大事务数。
- 引入版本：-

##### heartbeat_mgr_threads_num

- 默认值：8
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：Heartbeat Manager 可以运行的线程数以执行心跳任务。
- 引入版本：-

##### heartbeat_mgr_blocking_queue_size

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：Heartbeat Manager 运行的心跳任务存储的阻塞队列大小。
- 引入版本：-

##### catalog_try_lock_timeout_ms

- 默认值：5000
- 类型：Long
- 单位：毫秒
- 是否可变：是
- 描述：获取全局锁的超时时间。
- 引入版本：-

##### ignore_materialized_view_error

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：FE 是否忽略由物化视图错误引起的元数据异常。如果 FE 因物化视图错误引起的元数据异常而无法启动，您可以将此参数设置为 `true` 以允许 FE 忽略异常。
- 引入版本：v2.5.10

##### ignore_meta_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：非 Leader FEs 是否忽略来自 Leader FE 的元数据差距。如果值为 TRUE，非 Leader FEs 忽略来自 Leader FE 的元数据差距并继续提供数据读取服务。此参数确保即使您长时间停止 Leader FE 时也能持续提供数据读取服务。如果值为 FALSE，非 Leader FEs 不忽略来自 Leader FE 的元数据差距并停止提供数据读取服务。
- 引入版本：-

##### drop_backend_after_decommission

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：BE 退役后是否删除 BE。`TRUE` 表示 BE 在退役后立即删除。`FALSE` 表示 BE 在退役后不删除。
- 引入版本：-

##### enable_collect_query_detail_info

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否收集查询的 Profile。如果此参数设置为 `TRUE`，系统收集查询的 Profile。如果此参数设置为 `FALSE`，系统不收集查询的 Profile。
- 引入版本：-

##### profile_info_format

- 默认值：default
- 类型：String
- 单位：-
- 是否可变：是
- 描述：系统输出的 Profile 格式。有效值：`default` 和 `json`。设置为 `default` 时，Profile 为默认格式。设置为 `json` 时，系统以 JSON 格式输出 Profile。
- 引入版本：v2.5

##### enable_background_refresh_connector_metadata

- 默认值：v3.0 及更高版本为 true，v2.5 为 false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用周期性的 Hive 元数据缓存刷新。启用后，StarRocks 轮询 Hive 集群的 metastore（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Hive catalogs 的缓存元数据以感知数据变化。`true` 表示启用 Hive 元数据缓存刷新，`false` 表示禁用。
- 引入版本：v2.5.5

##### background_refresh_metadata_interval_millis

- 默认值：600000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：两次连续 Hive 元数据缓存刷新之间的间隔。
- 引入版本：v2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- 默认值：3600 * 24
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：Hive 元数据缓存刷新任务的过期时间。对于已访问的 Hive catalog，如果超过指定时间未访问，StarRocks 将停止刷新其缓存元数据。对于未访问的 Hive catalog，StarRocks 将不刷新其缓存元数据。
- 引入版本：v2.5.5

##### enable_statistics_collect_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为统计查询生成 Profile。您可以将此项设置为 `true` 以允许 StarRocks 为系统统计查询生成查询 Profile。
- 引入版本：v3.1.5

#### metadata_enable_recovery_mode

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否启用元数据恢复模式。启用此模式后，如果部分集群元数据丢失，可以根据 BE 的信息进行恢复。目前，仅可以恢复分区的版本信息。
- 引入版本：v3.3.0

##### black_host_history_sec

- 默认值：2 * 60
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：BE 黑名单中 BE 节点历史连接失败的保留时间。如果 BE 节点自动添加到 BE 黑名单中，StarRocks 将评估其连接性并判断是否可以从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才可以从 BE 黑名单中移除。
- 引入版本：v3.3.0

##### black_host_connect_failures_within_time

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：黑名单中 BE 节点允许的连接失败阈值。如果 BE 节点自动添加到 BE 黑名单中，StarRocks 将评估其连接性并判断是否可以从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才可以从 BE 黑名单中移除。
- 引入版本：v3.3.0

#### lock_manager_enabled

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否启用锁管理器。锁管理器对锁进行集中管理。例如，它可以控制是否将元数据锁的粒度从数据库级别细化到表级别。
- 引入版本：v3.3.0

##### lock_manager_enable_using_fine_granularity_lock

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否将元数据锁的粒度从数据库级别细化到表级别。元数据锁细化到表级别后，可以减少锁冲突和争用，从而提高导入和查询并发性。此参数仅在 `lock_manager_enabled` 启用时生效。
- 引入版本：v3.3.0

##### enable_legacy_compatibility_for_replication

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用复制的旧版兼容性。StarRocks 在旧版本和新版本之间可能会有不同的行为，导致跨集群数据迁移时出现问题。因此，您必须在数据迁移之前为目标集群启用旧版兼容性，并在数据迁移完成后禁用它。`true` 表示启用此模式。
- 引入版本：v3.1.10, v3.2.6

##### automated_cluster_snapshot_interval_seconds

- 默认值：600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：自动集群快照任务触发的间隔。
- 引入版本：v3.4.2

### 用户、角色和权限

##### privilege_max_total_roles_per_user

- 默认值：64
- 类型：Int
- 单位：
- 是否可变：是
- 描述：用户可以拥有的最大角色数。
- 引入版本：v3.0.0

##### privilege_max_role_depth

- 默认值：16
- 类型：Int
- 单位：
- 是否可变：是
- 描述：角色的最大角色深度（继承级别）。
- 引入版本：v3.0.0

### 查询引擎

##### publish_version_interval_ms

- 默认值：10
- 类型：Int
- 单位：毫秒
- 是否可变：否
- 描述：发布验证任务发出的时间间隔。
- 引入版本：-

##### statistic_cache_columns

- 默认值：100000
- 类型：Long
- 单位：-
- 是否可变：否
- 描述：可以缓存到统计表中的行数。
- 引入版本：-

##### statistic_cache_thread_pool_size

- 默认值：10
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：用于刷新统计缓存的线程池大小。
- 引入版本：-

##### max_allowed_in_element_num_of_delete

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：DELETE 语句中 IN 谓词允许的最大元素数。
- 引入版本：-

##### enable_materialized_view

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用创建物化视图。
- 引入版本：-

##### enable_materialized_view_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用物化视图刷新任务的中间结果溢出。
- 引入版本：v3.1.1

##### enable_backup_materialized_view

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在备份或恢复特定数据库时是否启用异步物化视图的备份和恢复。如果此项设置为 `false`，StarRocks 将跳过备份异步物化视图。
- 引入版本：v3.2.0

##### enable_experimental_mv

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用异步物化视图功能。TRUE 表示启用此功能。从 v2.5.2 开始，默认启用此功能。对于 v2.5.2 之前的版本，默认禁用此功能。
- 引入版本：v2.4

##### enable_colocate_mv_index

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在创建同步物化视图时是否支持将同步物化视图索引与基表进行共置。如果此项设置为 `true`，tablet sink 将加速同步物化视图的写入性能。
- 引入版本：v3.2.0

##### default_mv_refresh_immediate

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在创建异步物化视图后是否立即刷新。当此项设置为 `true` 时，新创建的物化视图将立即刷新。
- 引入版本：v3.2.3

##### enable_materialized_view_metrics_collect

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否默认收集异步物化视图的监控指标。
- 引入版本：v3.1.11, v3.2.5

##### enable_materialized_view_text_based_rewrite

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否默认启用基于文本的查询改写。如果此项设置为 `true`，系统在创建异步物化视图时构建抽象语法树。
- 引入版本：v3.2.5

##### enable_mv_automatic_active_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用系统自动检查并重新激活由于基表（视图）进行 Schema Change 或被删除并重新创建而设置为非活动状态的异步物化视图。请注意，此功能不会重新激活用户手动设置为非活动状态的物化视图。
- 引入版本：v3.1.6

##### enable_active_materialized_view_schema_strict_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在激活非活动物化视图时是否严格检查数据类型的长度一致性。当此项设置为 `false` 时，如果基表中的数据类型长度发生变化，物化视图的激活不受影响。
- 引入版本：v3.3.4

##### mv_active_checker_interval_seconds

- 默认值：60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：当启用后台 active_checker 线程时，系统将定期检测并自动重新激活由于基表（或视图）的架构更改或重建而变为非活动状态的物化视图。此参数控制检查器线程的调度间隔，以秒为单位。默认值为系统定义。
- 引入版本：v3.1.6

##### default_mv_partition_refresh_number

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：当物化视图刷新涉及多个分区时，此参数控制默认情况下在单个批次中刷新的分区数量。从版本 3.3.0 开始，系统默认一次刷新一个分区，以避免潜在的内存不足（OOM）问题。在早期版本中，默认情况下所有分区同时刷新，这可能导致内存耗尽和任务失败。然而，请注意，当物化视图刷新涉及大量分区时，一次仅刷新一个分区可能导致过多的调度开销、较长的整体刷新时间和大量的刷新记录。在这种情况下，建议适当调整此参数以提高刷新效率并减少调度成本。
- 引入版本：v3.3.0

##### enable_udf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否启用 UDF。
- 引入版本：-

##### enable_decimal_v3

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否支持 DECIMAL V3 数据类型。
- 引入版本：-

##### enable_sql_blacklist

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用 SQL 查询的黑名单检查。启用此功能后，黑名单中的查询无法执行。
- 引入版本：-

##### dynamic_partition_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用动态分区功能。启用此功能后，StarRocks 动态为新数据创建分区，并自动删除过期分区以确保数据的新鲜度。
- 引入版本：-

##### dynamic_partition_check_interval_seconds

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：检查新数据的间隔。如果检测到新数据，StarRocks 会自动为数据创建分区。
- 引入版本：-

##### max_query_retry_time

- 默认值：2
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在 FE 上查询的最大重试次数。
- 引入版本：-

##### max_create_table_timeout_second

- 默认值：600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：创建表的最大超时时间。
- 引入版本：-

##### create_table_max_serial_replicas

- 默认值：128
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：串行创建的最大副本数。如果实际副本数超过此值，副本将并发创建。如果表创建需要很长时间才能完成，请尝试减少此值。
- 引入版本：-

##### http_slow_request_threshold_ms

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：如果 HTTP 请求的响应时间超过此参数指定的值，则会生成日志以跟踪此请求。
- 引入版本：v2.5.15, v3.1.5

##### max_partitions_in_one_batch

- 默认值：4096
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：批量创建分区时可以创建的最大分区数。
- 引入版本：-

##### max_running_rollup_job_num_per_table

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：表中可以并行运行的最大 rollup 任务数。
- 引入版本：-

##### expr_children_limit

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：表达式中允许的最大子表达式数。
- 引入版本：-

##### max_planner_scalar_rewrite_num

- 默认值：100000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：优化器可以重写标量运算符的最大次数。
- 引入版本：-

##### max_scalar_operator_optimize_depth

- 默认值：256
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：标量运算符优化可以应用的最大深度。
- 引入版本：-

##### max_scalar_operator_flat_children

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：标量运算符的最大平面子节点数。您可以设置此限制以防止优化器使用过多的内存。
- 引入版本：-

##### enable_statistic_collect

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 CBO 收集统计信息。此功能默认启用。
- 引入版本：-

##### enable_statistic_collect_on_first_load

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在首次将数据加载到表中时自动收集统计信息。如果表有多个分区，任何数据加载到该表的空分区都会触发对该分区的自动统计信息收集。如果频繁创建新表并频繁加载数据，内存和 CPU 开销将增加。
- 引入版本：v3.1

##### statistic_auto_analyze_start_time

- 默认值：00:00:00
- 类型：String
- 单位：-
- 是否可变：是
- 描述：自动收集的开始时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本：-

##### statistic_auto_analyze_end_time

- 默认值：23:59:59
- 类型：String
- 单位：-
- 是否可变：是
- 描述：自动收集的结束时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本：-

##### statistic_analyze_status_keep_second

- 默认值：3 * 24 * 3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：保留收集任务历史的时长。默认值为 3 天。
- 引入版本：-

##### statistic_collect_interval_sec

- 默认值：5 * 60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：自动收集期间检查数据更新的间隔。
- 引入版本：-

##### statistic_update_interval_sec

- 默认值：24 * 60 * 60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：统计信息缓存更新的间隔。
- 引入版本：-

##### enable_collect_full_statistic

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用自动全量统计信息收集。此功能默认启用。
- 引入版本：-

##### statistic_auto_collect_ratio

- 默认值：0.8
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：用于确定自动收集的统计信息是否健康的阈值。如果统计信息健康度低于此阈值，则触发自动收集。
- 引入版本：-

##### statistic_auto_collect_small_table_rows

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：在自动收集中确定外部数据源（Hive、Iceberg、Hudi）中的表是否为小表的阈值。如果表的行数小于此值，则该表被视为小表。
- 引入版本：v3.2

##### statistic_max_full_collect_data_size

- 默认值：100 * 1024 * 1024 * 1024
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：自动收集统计信息的数据大小阈值。如果总大小超过此值，则执行采样收集而不是全量收集。
- 引入版本：-

##### statistic_collect_max_row_count_per_query

- 默认值：5000000000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：单个分析任务查询的最大行数。如果超过此值，分析任务将被拆分为多个查询。
- 引入版本：-

##### statistic_sample_collect_rows

- 默认值：200000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：采样收集的最小行数。如果参数值超过表中的实际行数，则执行全量收集。
- 引入版本：-

##### histogram_buckets_size

- 默认值：64
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：直方图的默认桶数。
- 引入版本：-

##### histogram_mcv_size

- 默认值：100
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：直方图的最常见值（MCV）数量。
- 引入版本：-

##### histogram_sample_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：直方图的采样比例。
- 引入版本：-

##### histogram_max_sample_row_count

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：直方图收集的最大行数。
- 引入版本：-

##### connector_table_query_trigger_task_schedule_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：调度程序线程调度查询触发的后台任务的间隔。此项用于替代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。这里，后台任务指的是 v3.4 中的 `ANALYZE` 任务，以及 v3.4 之后版本中低基数列字典的收集任务。
- 引入版本：v3.4.2

##### connector_table_query_trigger_analyze_small_table_rows

- 默认值：10000000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：用于确定表是否为查询触发的 ANALYZE 任务的小表的阈值。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：小表的查询触发 ANALYZE 任务的间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_large_table_interval

- 默认值：12 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：大表的查询触发 ANALYZE 任务的间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- 默认值：2
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

##### enable_local_replica_selection

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为查询选择本地副本。本地副本减少网络传输成本。如果此参数设置为 TRUE，CBO 优先选择与当前 FE 具有相同 IP 地址的 BEs 上的 tablet 副本。如果此参数设置为 `FALSE`，则可以选择本地副本和非本地副本。
- 引入版本：-

##### max_distribution_pruner_recursion_depth

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：分区裁剪器允许的最大递归深度。增加递归深度可以裁剪更多元素，但也会增加 CPU 消耗。
- 引入版本：-

##### slow_query_analyze_threshold

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：触发查询反馈分析的查询执行时间阈值。
- 引入版本：v3.4.0

##### low_cardinality_threshold

- 默认值：255
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：低基数字典的阈值。
- 引入版本：v3.5.0

### 数据导入和卸载

##### load_straggler_wait_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：BE 副本可以容忍的最大导入滞后。如果超过此值，将执行克隆以从其他副本克隆数据。
- 引入版本：-

##### load_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：导入作业按滚动方式处理的时间间隔。
- 引入版本：-

##### broker_load_default_timeout_second

- 默认值：14400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Broker Load 作业的超时时间。
- 引入版本：-

##### min_bytes_per_broker_scanner

- 默认值：67108864
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：Broker Load 实例可以处理的最小数据量。
- 引入版本：-

##### insert_load_default_timeout_second

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：用于加载数据的 INSERT INTO 语句的超时时间。
- 引入版本：-

##### stream_load_default_timeout_second

- 默认值：600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：每个 Stream Load 作业的默认超时时间。
- 引入版本：-

##### max_stream_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Stream Load 作业允许的最大超时时间。
- 引入版本：-

##### max_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：导入作业允许的最大超时时间。如果超过此限制，导入作业将失败。此限制适用于所有类型的导入作业。
- 引入版本：-

##### min_load_timeout_second

- 默认值：1
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：导入作业允许的最小超时时间。此限制适用于所有类型的导入作业。
- 引入版本：-

##### spark_dpp_version

- 默认值：1.0.0
- 类型：String
- 单位：-
- 是否可变：否
- 描述：使用的 Spark 动态分区裁剪（DPP）版本。
- 引入版本：-

##### spark_load_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：每个 Spark Load 作业的超时时间。
- 引入版本：-

##### spark_home_default_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Spark 客户端的根目录。
- 引入版本：-

##### spark_resource_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Spark 依赖包的根目录。
- 引入版本：-

##### spark_launcher_log_dir

- 默认值：sys_log_dir + "/spark_launcher_log"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储 Spark 日志文件的目录。
- 引入版本：-

##### yarn_client_path

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Yarn 客户端包的根目录。
- 引入版本：-

##### yarn_config_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储 Yarn 配置文件的目录。
- 引入版本：-

##### desired_max_waiting_jobs

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 中待处理作业的最大数量。此数量指所有作业，例如表创建、导入和 schema change 作业。如果 FE 中待处理作业的数量达到此值，FE 将拒绝新的导入请求。此参数仅对异步导入生效。从 v2.5 开始，默认值从 100 更改为 1024。
- 引入版本：-

##### max_running_txn_num_per_db

- 默认值：1000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：StarRocks 集群中每个数据库允许运行的最大导入事务数。默认值为 `1000`。从 v3.1 开始，默认值从 `100` 更改为 `1000`。当数据库的实际运行导入事务数超过此参数的值时，将不处理新的导入请求。新的同步导入作业请求将被拒绝，新的异步导入作业请求将排队。我们不建议增加此参数的值，因为这会增加系统负载。
- 引入版本：-

##### max_broker_load_job_concurrency

- 默认值：5
- 别名：async_load_task_pool_size
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：StarRocks 集群中允许的最大并发 Broker Load 作业数。此参数仅对 Broker Load 有效。此参数的值必须小于 `max_running_txn_num_per_db` 的值。从 v2.5 开始，默认值从 `10` 更改为 `5`。
- 引入版本：-

##### load_parallel_instance_num (已弃用)

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：每个导入作业在 BE 上的最大并发导入实例数。从 v3.1 开始，此项已弃用。
- 引入版本：-

##### disable_load_job

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：当集群遇到错误时是否禁用导入。这可以防止集群错误导致的任何损失。默认值为 `FALSE`，表示不禁用导入。`TRUE` 表示禁用导入，集群处于只读状态。
- 引入版本：-

##### history_job_keep_max_second

- 默认值：7 * 24 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：历史作业（如 schema change 作业）可以保留的最长时间。
- 引入版本：-

##### label_keep_max_second

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：已完成并处于 FINISHED 或 CANCELLED 状态的导入作业标签的最大保留时间（以秒为单位）。默认值为 3 天。此持续时间到期后，标签将被删除。此参数适用于所有类型的导入作业。值过大会消耗大量内存。
- 引入版本：-

##### label_keep_max_num

- 默认值：1000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在一段时间内可以保留的导入作业的最大数量。如果超过此数量，将删除历史作业的信息。
- 引入版本：-

##### max_routine_load_task_concurrent_num

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：每个 Routine Load 作业的最大并发任务数。
- 引入版本：-

##### max_routine_load_task_num_per_be

- 默认值：16
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：每个 BE 上的最大并发 Routine Load 任务数。从 v3.1.0 开始，此参数的默认值从 5 增加到 16，并且不再需要小于或等于 BE 静态参数 `routine_load_thread_pool_size`（已弃用）的值。
- 引入版本：-

##### max_routine_load_batch_size

- 默认值：4294967296
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：Routine Load 任务可以加载的最大数据量。
- 引入版本：-

##### routine_load_task_consume_second

- 默认值：15
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：集群中每个 Routine Load 任务消耗数据的最大时间。从 v3.1.0 开始，Routine Load 作业支持在 [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中使用新参数 `task_consume_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本：-

##### routine_load_task_timeout_second

- 默认值：60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：集群中每个 Routine Load 任务的超时时间。从 v3.1.0 开始，Routine Load 作业支持在 [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中使用新参数 `task_timeout_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本：-

##### routine_load_unstable_threshold_second

- 默认值：3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：如果 Routine Load 作业中的任何任务滞后，Routine Load 作业将设置为 UNSTABLE 状态。具体而言，正在消费的消息的时间戳与当前时间的差异超过此阈值，并且数据源中存在未消费的消息。
- 引入版本：-

##### enable_routine_load_lag_metrics

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否收集 Routine Load Kafka 分区偏移滞后指标。请注意，将此项设置为 `true` 将调用 Kafka API 以获取分区的最新偏移。
- 引入版本：-

##### min_routine_load_lag_for_metrics

- 默认值：10000
- 类型：INT
- 单位：-
- 是否可变：是
- 描述：在监控指标中显示的 Routine Load 作业的最小偏移滞后。偏移滞后大于此值的 Routine Load 作业将在指标中显示。
- 引入版本：-

##### max_tolerable_backend_down_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：允许的故障 BE 节点的最大数量。如果超过此数量，Routine Load 作业无法自动恢复。
- 引入版本：-

##### period_of_auto_resume_min

- 默认值：5
- 类型：Int
- 单位：分钟
- 是否可变：是
- 描述：Routine Load 作业自动恢复的间隔。
- 引入版本：-

##### export_task_default_timeout_second

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：数据导出任务的超时时间。
- 引入版本：-

##### export_max_bytes_per_be_per_task

- 默认值：268435456
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：单个数据卸载任务从单个 BE 导出的最大数据量。
- 引入版本：-

##### export_task_pool_size

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：卸载任务线程池的大小。
- 引入版本：-

##### export_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：导入作业调度的时间间隔。
- 引入版本：-

##### export_running_job_num_limit

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：可以并行运行的数据导出任务的最大数量。
- 引入版本：-

##### empty_load_as_error

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：如果没有数据加载，是否返回错误消息 "all partitions have no load data"。有效值：
  - `true`：如果没有数据加载，系统显示失败消息并返回错误 "all partitions have no load data"。
  - `false`：如果没有数据加载，系统显示成功消息并返回 OK，而不是错误。
- 引入版本：-

##### external_table_commit_timeout_ms

- 默认值：10000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：提交（发布）写事务到 StarRocks 外部表的超时时间。默认值 `10000` 表示 10 秒超时时间。
- 引入版本：-

##### enable_sync_publish

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在导入事务的发布阶段同步执行应用任务。此参数仅适用于 Primary Key 表。有效值：
  - `TRUE`（默认）：在导入事务的发布阶段同步执行应用任务。这意味着只有在应用任务完成后，导入事务才被报告为成功，并且加载的数据才能真正被查询。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `true` 可以提高查询性能和稳定性，但可能会增加导入延迟。
  - `FALSE`：在导入事务的发布阶段异步执行应用任务。这意味着在应用任务提交后，导入事务被报告为成功，但加载的数据不能立即被查询。在这种情况下，并发查询需要等待应用任务完成或超时后才能继续。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `false` 可能会影响查询性能和稳定性。
- 引入版本：v3.2.0

##### label_clean_interval_second

- 默认值：4 * 3600
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：标签清理的时间间隔。单位：秒。我们建议您指定较短的时间间隔，以确保历史标签能够及时清理。
- 引入版本：-

##### transaction_clean_interval_second

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：已完成事务清理的时间间隔。单位：秒。我们建议您指定较短的时间间隔，以确保已完成的事务能够及时清理。
- 引入版本：-

##### transaction_stream_load_coordinator_cache_capacity

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：存储从事务标签到协调节点映射的缓存容量。
- 引入版本：-

##### transaction_stream_load_coordinator_cache_expire_seconds

- 默认值：900
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：在缓存中保留协调器映射的时间（TTL）。
- 引入版本：-

### 存储

##### default_replication_num

- 默认值：3
- 类型：Short
- 单位：-
- 是否可变：是
- 描述：在 StarRocks 中创建表时为每个数据分区设置的默认副本数。此设置可以在创建表时通过在 CREATE TABLE DDL 中指定 `replication_num=x` 来覆盖。
- 引入版本：-

##### enable_strict_storage_medium_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：用户创建表时，FE 是否严格检查 BEs 的存储介质。如果此参数设置为 `TRUE`，则用户创建表时，FE 检查 BEs 的存储介质，如果 BE 的存储介质与 CREATE TABLE 语句中指定的 `storage_medium` 参数不同，则返回错误。例如，CREATE TABLE 语句中指定的存储介质为 SSD，但 BEs 的实际存储介质为 HDD。结果，表创建失败。如果此参数为 `FALSE`，则用户创建表时，FE 不检查 BEs 的存储介质。
- 引入版本：-

##### catalog_trash_expire_second

- 默认值：86400
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：删除数据库、表或分区后，元数据可以保留的最长时间。如果此持续时间到期，数据将被删除，无法通过 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 命令恢复。
- 引入版本：-

##### enable_auto_tablet_distribution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否自动设置桶的数量。
  - 如果此参数设置为 `TRUE`，则在创建表或添加分区时无需指定桶的数量。StarRocks 自动确定桶的数量。
  - 如果此参数设置为 `FALSE`，则在创建表或添加分区时需要手动指定桶的数量。如果在向表添加新分区时未指定桶数，则新分区继承表创建时设置的桶数。但是，您也可以手动为新分区指定桶的数量。
- 引入版本：v2.5.7

##### enable_experimental_rowstore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用 [行列混存](../../table_design/hybrid_table.md) 功能。
- 引入版本：v3.2.3

#### enable_experimental_gin

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用 [全文倒排索引](../../table_design/indexes/inverted_index.md) 功能。
- 引入版本：v3.3.0

##### storage_usage_soft_limit_percent

- 默认值：90
- 别名：storage_high_watermark_usage_percent
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：BE 目录中存储使用百分比的软限制。如果 BE 存储目录的存储使用（以百分比表示）超过此值，并且剩余存储空间小于 `storage_usage_soft_limit_reserve_bytes`，则无法将 tablets 克隆到此目录中。
- 引入版本：-

##### storage_usage_soft_limit_reserve_bytes

- 默认值：200 * 1024 * 1024 * 1024
- 别名：storage_min_left_capacity_bytes
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：BE 目录中剩余存储空间的软限制。如果 BE 存储目录中的剩余存储空间小于此值，并且存储使用（以百分比表示）超过 `storage_usage_soft_limit_percent`，则无法将 tablets 克隆到此目录中。
- 引入版本：-

##### storage_usage_hard_limit_percent

- 默认值：95
- 别名：storage_flood_stage_usage_percent
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：BE 目录中存储使用百分比的硬限制。如果 BE 存储目录的存储使用（以百分比表示）超过此值，并且剩余存储空间小于 `storage_usage_hard_limit_reserve_bytes`，则 Load 和 Restore 作业将被拒绝。您需要与 BE 配置项 `storage_flood_stage_usage_percent` 一起设置此项以使配置生效。
- 引入版本：-

##### storage_usage_hard_limit_reserve_bytes

- 默认值：100 * 1024 * 1024 * 1024
- 别名：storage_flood_stage_left_capacity_bytes
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：BE 目录中剩余存储空间的硬限制。如果 BE 存储目录中的剩余存储空间小于此值，并且存储使用（以百分比表示）超过 `storage_usage_hard_limit_percent`，则 Load 和 Restore 作业将被拒绝。您需要与 BE 配置项 `storage_flood_stage_left_capacity_bytes` 一起设置此项以使配置生效。
- 引入版本：-

##### alter_table_timeout_second

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：schema change 操作（ALTER TABLE）的超时时间。
- 引入版本：-

##### enable_fast_schema_evolution

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 StarRocks 集群中的所有表启用快速 schema 演变。有效值为 `TRUE` 和 `FALSE`（默认）。启用快速 schema 演变可以在添加或删除列时加快 schema 更改速度并减少资源使用。
- 引入版本：v3.2.0

> **注意**
>
> - StarRocks 存算分离集群从 v3.3.0 开始支持此参数。
> - 如果需要为特定表配置快速 schema 演变，例如为特定表禁用快速 schema 演变，可以在表创建时设置表属性 [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution)。

##### recover_with_empty_tablet

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否用空的 tablet 副本替换丢失或损坏的 tablet 副本。如果 tablet 副本丢失或损坏，数据查询可能会失败。用空的 tablet 替换丢失或损坏的 tablet 副本可以确保查询仍然可以执行。然而，由于数据丢失，结果可能不正确。默认值为 `FALSE`，这意味着丢失或损坏的 tablet 副本不会被空的替换，查询将失败。
- 引入版本：-

##### tablet_create_timeout_second

- 默认值：10
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：创建 tablet 的超时时间。从 v3.1 开始，默认值从 1 更改为 10。
- 引入版本：-

##### tablet_delete_timeout_second

- 默认值：2
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：删除 tablet 的超时时间。
- 引入版本：-

##### check_consistency_default_timeout_second

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：副本一致性检查的超时时间。您可以根据 tablet 的大小设置此参数。
- 引入版本：-

##### tablet_sched_slot_num_per_path

- 默认值：8
- 别名：schedule_slot_num_per_path
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：BE 存储目录中可以并发运行的 tablet 相关任务的最大数量。从 v2.5 开始，此参数的默认值从 `4` 更改为 `8`。
- 引入版本：-

##### tablet_sched_max_scheduling_tablets

- 默认值：10000
- 别名：max_scheduling_tablets
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：可以同时调度的 tablet 的最大数量。如果超过此值，将跳过 tablet 平衡和修复检查。
- 引入版本：-

##### tablet_sched_disable_balance

- 默认值：false
- 别名：disable_balance
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否禁用 tablet 平衡。`TRUE` 表示禁用 tablet 平衡。`FALSE` 表示启用 tablet 平衡。
- 引入版本：-

##### tablet_sched_disable_colocate_balance

- 默认值：false
- 别名：disable_colocate_balance
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否禁用 Colocate Table 的副本平衡。`TRUE` 表示禁用副本平衡。`FALSE` 表示启用副本平衡。
- 引入版本：-

##### tablet_sched_be_down_tolerate_time_s

- 默认值：900
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：调度程序允许 BE 节点保持不活动状态的最长时间。达到时间阈值后，该 BE 节点上的 tablets 将迁移到其他活动的 BE 节点。
- 引入版本：v2.5.7

##### tablet_sched_max_balancing_tablets

- 默认值：500
- 别名：max_balancing_tablets
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：可以同时平衡的 tablet 的最大数量。如果超过此值，将跳过 tablet 重新平衡。
- 引入版本：-

##### tablet_sched_storage_cooldown_second

- 默认值：-1
- 别名：storage_cooldown_second
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：从表创建时开始的自动冷却延迟。默认值 `-1` 指定禁用自动冷却。如果要启用自动冷却，请将此参数设置为大于 `-1` 的值。
- 引入版本：-

##### tablet_sched_max_not_being_scheduled_interval_ms

- 默认值：15 * 60 * 1000
- 类型：Long
- 单位：毫秒
- 是否可变：是
- 描述：当 tablet 克隆任务正在调度时，如果 tablet 在此参数指定的时间内未被调度，StarRocks 将优先调度它。
- 引入版本：-

##### tablet_sched_balance_load_score_threshold

- 默认值：0.1
- 别名：balance_load_score_threshold
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：用于确定 BE 负载是否平衡的百分比阈值。如果 BE 的负载低于所有 BEs 的平均负载，并且差异大于此值，则该 BE 处于低负载状态。相反，如果 BE 的负载高于平均负载，并且差异大于此值，则该 BE 处于高负载状态。
- 引入版本：-

##### tablet_sched_num_based_balance_threshold_ratio

- 默认值：0.5
- 别名：-
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：基于数量的平衡可能会破坏磁盘大小平衡，但磁盘之间的最大差距不能超过 tablet_sched_num_based_balance_threshold_ratio * tablet_sched_balance_load_score_threshold。如果集群中有 tablets 不断从 A 平衡到 B，再从 B 平衡到 A，请减少此值。如果希望 tablet 分布更均衡，请增加此值。
- 引入版本：- 3.1

##### tablet_sched_balance_load_disk_safe_threshold

- 默认值：0.5
- 别名：balance_load_disk_safe_threshold
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：用于确定 BEs 的磁盘使用是否平衡的百分比阈值。如果所有 BEs 的磁盘使用低于此值，则认为是平衡的。如果磁盘使用大于此值，并且最高和最低 BE 磁盘使用之间的差异大于 10%，则认为磁盘使用不平衡，并触发 tablet 重新平衡。
- 引入版本：-

##### tablet_sched_repair_delay_factor_second

- 默认值：60
- 别名：tablet_repair_delay_factor_second
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：副本修复的间隔，以秒为单位。
- 引入版本：-

##### tablet_sched_min_clone_task_timeout_sec

- 默认值：3 * 60
- 别名：min_clone_task_timeout_sec
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：克隆 tablet 的最小超时时间。
- 引入版本：-

##### tablet_sched_max_clone_task_timeout_sec

- 默认值：2 * 60 * 60
- 别名：max_clone_task_timeout_sec
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：克隆 tablet 的最大超时时间。
- 引入版本：-

##### tablet_stat_update_interval_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：FE 从每个 BE 检索 tablet 统计信息的时间间隔。
- 引入版本：-

##### max_automatic_partition_number

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：自动创建的最大分区数。
- 引入版本：v3.1

##### auto_partition_max_creation_number_per_load

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：通过加载任务在表（具有表达式分区策略）中可以创建的最大分区数。
- 引入版本：v3.3.2

##### max_partition_number_per_table

- 默认值：100000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：表中可以创建的最大分区数。
- 引入版本：v3.3.2

##### max_bucket_number_per_partition

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：分区中可以创建的最大桶数。
- 引入版本：v3.3.2

##### max_column_number_per_table

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：表中可以创建的最大列数。
- 引入版本：v3.3.2

### 存算分离

##### run_mode

- 默认值：shared_nothing
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing`（默认）。
  - `shared_data` 表示在存算分离模式下运行 StarRocks。
  - `shared_nothing` 表示在存算一体模式下运行 StarRocks。

  > **注意**
  >
  > - 您不能同时为 StarRocks 集群采用 `shared_data` 和 `shared_nothing` 模式。不支持混合部署。
  > - 集群部署后请勿更改 `run_mode`。否则，集群将无法重启。不支持从存算一体集群转换为存算分离集群或反之。

- 引入版本：-

##### cloud_native_meta_port

- 默认值：6090
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 云原生元数据服务器 RPC 监听端口。
- 引入版本：-

##### enable_load_volume_from_conf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否允许 StarRocks 使用 FE 配置文件中指定的对象存储相关属性创建内置存储卷。从 v3.4.1 开始，默认值从 `true` 更改为 `false`。
- 引入版本：v3.1.0

##### cloud_native_storage_type

- 默认值：S3
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您使用的对象存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS、Azure Blob（从 v3.1.1 开始支持）、Azure Data Lake Storage Gen2（从 v3.4.1 开始支持）、Google Storage（使用原生 SDK，从 v3.5.1 开始支持）和兼容 S3 协议的对象存储系统（如 AWS S3 和 MinIO）中。有效值：`S3`（默认）、`HDFS`、`AZBLOB`、`ADLS2` 和 `GS`。如果将此参数指定为 `S3`，则必须添加以 `aws_s3` 为前缀的参数。如果将此参数指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的参数。如果将此参数指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的参数。如果将此参数指定为 `GS`，则必须添加以 `gcp_gcs` 为前缀的参数。如果将此参数指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本：-

##### cloud_native_hdfs_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本：-

##### aws_s3_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 S3 路径。它由 S3 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本：v3.0

##### aws_s3_region

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 S3 存储桶所在的区域，例如 `us-west-2`。
- 引入版本：v3.0

##### aws_s3_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：访问您的 S3 存储桶的端点，例如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本：v3.0

##### aws_s3_use_aws_sdk_default_behavior

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用 AWS SDK 的默认身份验证凭证。有效值：true 和 false（默认）。
- 引入版本：v3.0

##### aws_s3_use_instance_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用实例配置文件和假设角色作为访问 S3 的凭证方法。有效值：true 和 false（默认）。
  - 如果您使用基于 IAM 用户的凭证（访问密钥和秘密密钥）访问 S3，则必须将此项指定为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。
  - 如果您使用实例配置文件访问 S3，则必须将此项指定为 `true`。
  - 如果您使用假设角色访问 S3，则必须将此项指定为 `true`，并指定 `aws_s3_iam_role_arn`。
  - 如果您使用外部 AWS 账户，还必须指定 `aws_s3_external_id`。
- 引入版本：v3.0

##### aws_s3_access_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于访问您的 S3 存储桶的访问密钥 ID。
- 引入版本：v3.0

##### aws_s3_secret_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于访问您的 S3 存储桶的秘密访问密钥。
- 引入版本：v3.0

##### aws_s3_iam_role_arn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在存储数据文件的 S3 存储桶上具有权限的 IAM 角色的 ARN。
- 引入版本：v3.0

##### aws_s3_external_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于跨账户访问您的 S3 存储桶的 AWS 账户的外部 ID。
- 引入版本：v3.0

##### azure_blob_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 Azure Blob 存储账户的端点，例如 `https://test.blob.core.windows.net`。
- 引入版本：v3.1

##### azure_blob_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Azure Blob 存储路径。它由存储账户中的容器名称和容器下的子路径（如果有）组成，例如 `testcontainer/subpath`。
- 引入版本：v3.1

##### azure_blob_shared_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Blob 存储的共享密钥。
- 引入版本：v3.1

##### azure_blob_sas_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Blob 存储的共享访问签名（SAS）。
- 引入版本：v3.1

##### azure_adls2_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 Azure Data Lake Storage Gen2 账户的端点，例如 `https://test.dfs.core.windows.net`。
- 引入版本：v3.4.1

##### azure_adls2_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Azure Data Lake Storage Gen2 路径。它由文件系统名称和目录名称组成，例如 `testfilesystem/starrocks`。
- 引入版本：v3.4.1

##### azure_adls2_shared_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Data Lake Storage Gen2 的共享密钥。
- 引入版本：v3.4.1

##### azure_adls2_sas_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Data Lake Storage Gen2 的共享访问签名（SAS）。
- 引入版本：v3.4.1

##### azure_adls2_oauth2_use_managed_identity

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用托管身份授权请求 Azure Data Lake Storage Gen2。
- 引入版本：v3.4.4

##### azure_adls2_oauth2_tenant_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Data Lake Storage Gen2 的托管身份的租户 ID。
- 引入版本：v3.4.4

##### azure_adls2_oauth2_client_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权请求 Azure Data Lake Storage Gen2 的托管身份的客户端 ID。
- 引入版本：v3.4.4

##### azure_use_native_sdk

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否使用原生 SDK 访问 Azure Blob 存储，从而允许使用托管身份和服务主体进行身份验证。如果此项设置为 `false`，则仅允许使用共享密钥和 SAS 令牌进行身份验证。
- 引入版本：v3.4.4

##### gcp_gcs_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Google Cloud 路径。它由 Google Cloud 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_email

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建服务账户时生成的 JSON 文件中的电子邮件地址，例如 `user@hello.iam.gserviceaccount.com`。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_private_key_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建服务账户时生成的 JSON 文件中的私钥 ID。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_private_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建服务账户时生成的 JSON 文件中的私钥，例如 `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 引入版本：v3.5.1

##### gcp_gcs_impersonation_service_account

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：如果您使用基于模拟的身份验证访问 Google Storage，您希望模拟的服务账户。
- 引入版本：v3.5.1

##### gcp_gcs_use_compute_engine_service_account

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用绑定到您的 Compute Engine 的服务账户。
- 引入版本：v3.5.1

##### lake_compaction_score_selector_min_score

- 默认值：10.0
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中触发 Compaction 操作的 Compaction Score 阈值。当分区的 Compaction Score 大于或等于此值时，系统对该分区执行 Compaction。
- 引入版本：v3.1.0

##### lake_compaction_max_tasks

- 默认值：-1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中允许的最大并发 Compaction 任务数。将此项设置为 `-1` 表示以自适应方式计算并发任务数。将此值设置为 `0` 将禁用 Compaction。
- 引入版本：v3.1.0

##### lake_compaction_history_size

- 默认值：20
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中 Leader FE 节点的内存中保留的最近成功 Compaction 任务记录的数量。您可以使用 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录存储在 FE 进程内存中，如果 FE 进程重启，将会丢失。
- 引入版本：v3.1.0

##### lake_publish_version_max_threads

- 默认值：512
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中版本发布任务的最大线程数。
- 引入版本：v3.2.0

##### lake_autovacuum_parallel_partitions

- 默认值：8
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：在存算分离集群中可以同时进行 AutoVacuum 的最大分区数。AutoVacuum 是 Compactions 之后的垃圾回收。
- 引入版本：v3.1.0

##### lake_autovacuum_partition_naptime_seconds

- 默认值：180
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：在存算分离集群中对同一分区进行 AutoVacuum 操作的最小间隔。
- 引入版本：v3.1.0

##### lake_autovacuum_grace_period_minutes

- 默认值：30
- 类型：Long
- 单位：分钟
- 是否可变：是
- 描述：在存算分离集群中保留历史数据版本的时间范围。在此时间范围内的历史数据版本不会在 Compactions 之后通过 AutoVacuum 自动清理。您需要将此值设置为大于最大查询时间，以避免正在运行的查询访问的数据在查询完成之前被删除。默认值从 v3.3.0、v3.2.5 和 v3.1.10 开始从 `5` 更改为 `30`。
- 引入版本：v3.1.0

##### lake_autovacuum_stale_partition_threshold

- 默认值：12
- 类型：Long
- 单位：小时
- 是否可变：是
- 描述：如果分区在此时间范围内没有更新（加载、DELETE 或 Compactions），系统将不对该分区执行 AutoVacuum。
- 引入版本：v3.1.0

##### lake_enable_ingest_slowdown

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在存算分离集群中启用数据摄取减速。当启用数据摄取减速时，如果分区的 Compaction Score 超过 `lake_ingest_slowdown_threshold`，则对该分区的加载任务进行限速。此配置仅在 `run_mode` 设置为 `shared_data` 时生效。从 v3.3.6 开始，默认值从 `false` 更改为 `true`。
- 引入版本：v3.2.0

##### lake_ingest_slowdown_threshold

- 默认值：100
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中触发数据摄取减速的 Compaction Score 阈值。此配置仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。
- 引入版本：v3.2.0

##### lake_ingest_slowdown_ratio

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：当触发数据摄取减速时的加载速率减速比。

  数据加载任务由两个阶段组成：数据写入和数据提交（COMMIT）。数据摄取减速通过延迟数据提交来实现。延迟比率通过以下公式计算：`(compaction_score - lake_ingest_slowdown_threshold) * lake_ingest_slowdown_ratio`。例如，如果数据写入阶段需要 5 分钟，`lake_ingest_slowdown_ratio` 为 0.1，并且 Compaction Score 比 `lake_ingest_slowdown_threshold` 高 10，则数据提交时间的延迟为 `5 * 10 * 0.1 = 5` 分钟，这意味着平均加载速度减半。

- 引入版本：v3.2.0

> **注意**
>
> - 如果加载任务同时写入多个分区，则使用所有分区中最大的 Compaction Score 来计算提交时间的延迟。
> - 提交时间的延迟在第一次尝试提交时计算。一旦设置，将不会更改。一旦延迟时间结束，只要 Compaction Score 不超过 `lake_compaction_score_upper_bound`，系统将执行数据提交操作。
> - 如果提交时间的延迟超过加载任务的超时时间，任务将直接失败。

##### lake_compaction_score_upper_bound

- 默认值：2000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中分区的 Compaction Score 上限。`0` 表示没有上限。此项仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。当分区的 Compaction Score 达到或超过此上限时，将拒绝传入的加载任务。从 v3.3.6 开始，默认值从 `0` 更改为 `2000`。
- 引入版本：v3.2.0

##### lake_compaction_disable_ids

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：是
- 描述：在存算分离模式下禁用 compaction 的表或分区列表。格式为 `tableId1;partitionId2`，用分号分隔，例如 `12345;98765`。
- 引入版本：v3.4.4

##### lake_compaction_allow_partial_success

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：如果此项设置为 `true`，则系统将在存算分离集群中将 Compaction 操作视为成功，当其中一个子任务成功时。
- 引入版本：v3.5.2

##### lake_enable_balance_tablets_between_workers

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在存算分离集群中，云原生表的 tablet 迁移期间是否在计算节点之间平衡 tablet 数量。`true` 表示在计算节点之间平衡 tablet，`false` 表示禁用此功能。
- 引入版本：v3.3.4

##### lake_balance_tablets_threshold

- 默认值：0.15
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：系统用于判断存算分离集群中 worker 之间 tablet 平衡的阈值，不平衡因子计算为 `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`。如果因子大于 `lake_balance_tablets_threshold`，将触发 tablet 平衡。此项仅在 `lake_enable_balance_tablets_between_workers` 设置为 `true` 时生效。
- 引入版本：v3.3.4

##### shard_group_clean_threshold_sec

- 默认值：3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：在存算分离集群中，FE 清理未使用的 tablet 和 shard 组之前的时间。此阈值内创建的 tablets 和 shard 组不会被清理。
- 引入版本：-

##### star_mgr_meta_sync_interval_sec

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：在存算分离集群中，FE 运行与 StarMgr 的周期性元数据同步的间隔。
- 引入版本：-

##### meta_sync_force_delete_shard_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否允许直接删除存算分离集群的元数据，绕过清理远端存储文件。建议仅在需要清理的 shard 数量过多，导致 FE JVM 的内存压力极大时，将此项设置为 `true`。请注意，启用此功能后，属于 shard 或 tablet 的数据文件无法自动清理。
- 引入版本：v3.2.10, v3.3.3

### 其他

##### tmp_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储临时文件的目录，例如在备份和恢复过程中生成的文件。这些过程完成后，生成的临时文件将被删除。
- 引入版本：-

##### plugin_dir

- 默认值：System.getenv("STARROCKS_HOME") + "/plugins"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储插件安装包的目录。
- 引入版本：-

##### plugin_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否可以在 FEs 上安装插件。插件只能在 Leader FE 上安装或卸载。
- 引入版本：-

##### max_agent_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：代理任务线程池中允许的最大线程数。
- 引入版本：-

##### agent_task_resend_wait_time_ms

- 默认值：5000
- 类型：Long
- 单位：毫秒
- 是否可变：是
- 描述：FE 在可以重新发送代理任务之前必须等待的时间。只有当任务创建时间与当前时间之间的间隔超过此参数的值时，代理任务才能重新发送。此参数用于防止代理任务的重复发送。
- 引入版本：-

##### backup_job_default_timeout_ms

- 默认值：86400 * 1000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：备份作业的超时时间。如果超过此值，备份作业将失败。
- 引入版本：-

##### locale

- 默认值：zh_CN.UTF-8
- 类型：String
- 单位：-
- 是否可变：否
- 描述：FE 使用的字符集。
- 引入版本：-

##### report_queue_size (已弃用)

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：报告队列中可以等待的最大作业数。报告涉及 BEs 的磁盘、任务和 tablet 信息。如果队列中积压了太多报告作业，将会发生 OOM。
- 引入版本：-

##### enable_metric_calculator

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：指定是否启用用于定期收集指标的功能。有效值：`TRUE` 和 `FALSE`。`TRUE` 指定启用此功能，`FALSE` 指定禁用此功能。
- 引入版本：-

##### max_small_file_number

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：可以存储在 FE 目录中的小文件的最大数量。
- 引入版本：-

##### max_small_file_size_bytes

- 默认值：1024 * 1024
- 类型：Int
- 单位：字节
- 是否可变：是
- 描述：小文件的最大大小。
- 引入版本：-

##### small_file_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：小文件的根目录。
- 引入版本：-

##### enable_auth_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：指定是否启用身份验证检查功能。有效值：`TRUE` 和 `FALSE`。`TRUE` 指定启用此功能，`FALSE` 指定禁用此功能。
- 引入版本：-

##### authentication_ldap_simple_server_host

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：是
- 描述：LDAP 服务器运行的主机。
- 引入版本：-

##### authentication_ldap_simple_server_port

- 默认值：389
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：LDAP 服务器的端口。
- 引入版本：-

##### authentication_ldap_simple_bind_base_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：是
- 描述：LDAP 服务器开始搜索用户身份验证信息的基准 DN。
- 引入版本：-

##### authentication_ldap_simple_user_search_attr

- 默认值：uid
- 类型：String
- 单位：-
- 是否可变：是
- 描述：LDAP 对象中标识用户的属性名称。
- 引入版本：-

##### authentication_ldap_simple_bind_root_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：是
- 描述：用于搜索用户身份验证信息的管理员 DN。
- 引入版本：-

##### authentication_ldap_simple_bind_root_pwd

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：是
- 描述：用于搜索用户身份验证信息的管理员密码。
- 引入版本：-

##### jwt_jwks_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下的公钥本地文件路径。
- 引入版本：v3.5.0

##### jwt_principal_field

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本：v3.5.0

##### jwt_required_issuer

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。只有当列表中的一个值与 JWT 发行者匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### jwt_required_audience

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。只有当列表中的一个值与 JWT 受众匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### oauth2_auth_server_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：授权 URL。用户浏览器将被重定向到此 URL 以开始 OAuth 2.0 授权过程。
- 引入版本：v3.5.0

##### oauth2_token_server_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 从中获取访问令牌的授权服务器端点的 URL。
- 引入版本：v3.5.0

##### oauth2_client_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 客户端的公共标识符。
- 引入版本：v3.5.0

##### oauth2_client_secret

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权 StarRocks 客户端与授权服务器的秘密。
- 引入版本：v3.5.0

##### oauth2_redirect_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：OAuth 2.0 身份验证成功后，用户浏览器将被重定向到的 URL。授权代码将发送到此 URL。在大多数情况下，需要配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- 引入版本：v3.5.0

##### oauth2_jwks_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下的本地文件路径。
- 引入版本：v3.5.0

##### oauth2_principal_field

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本：v3.5.0

##### oauth2_required_issuer

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。只有当列表中的一个值与 JWT 发行者匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### oauth2_required_audience

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。只有当列表中的一个值与 JWT 受众匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### auth_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于 StarRocks 集群内身份认证的令牌。如果此参数未指定，StarRocks 在集群的 Leader FE 首次启动时为集群生成一个随机令牌。
- 引入版本：-

##### hive_meta_load_concurrency

- 默认值：4
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：Hive 元数据支持的最大并发线程数。
- 引入版本：-

##### hive_meta_cache_refresh_interval_s

- 默认值：3600 * 2
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：Hive 外部表的缓存元数据更新的时间间隔。
- 引入版本：-

##### hive_meta_cache_ttl_s

- 默认值：3600 * 24
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：Hive 外部表的缓存元数据过期的时间。
- 引入版本：-

##### hive_meta_store_timeout_s

- 默认值：10
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：连接到 Hive metastore 的超时时间。
- 引入版本：-

##### es_state_sync_interval_second

- 默认值：10
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：FE 获取 Elasticsearch 索引并同步 StarRocks 外部表元数据的时间间隔。
- 引入版本：-

##### max_upload_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在每个 BACKUP 操作中，StarRocks 分配给 BE 节点的最大上传任务数。当此项设置为小于或等于 0 时，不对任务数量施加限制。
- 引入版本：v3.1.0

##### max_download_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在每个 RESTORE 操作中，StarRocks 分配给 BE 节点的最大下载任务数。当此项设置为小于或等于 0 时，不对任务数量施加限制。
- 引入版本：v3.1.0

##### enable_colocate_restore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用 Colocate Tables 的备份和恢复。`true` 表示启用 Colocate Tables 的备份和恢复，`false` 表示禁用。
- 引入版本：v3.2.10, v3.3.3

##### mv_plan_cache_expire_interval_sec

- 默认值：24 * 60 * 60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：物化视图计划缓存（用于物化视图改写）在过期前的有效时间。默认值为 1 天。
- 引入版本：v3.2

##### mv_plan_cache_thread_pool_size

- 默认值：3
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：物化视图计划缓存（用于物化视图改写）的默认线程池大小。
- 引入版本：v3.2

##### mv_plan_cache_max_size

- 默认值：1000
- 类型：Long
- 单位：
- 是否可变：是
- 描述：物化视图计划缓存（用于物化视图改写）的最大大小。如果有许多物化视图用于透明查询改写，您可以增加此值。
- 引入版本：v3.2

##### enable_materialized_view_concurrent_prepare

- 默认值：true
- 类型：Boolean
- 单位：
- 是否可变：是
- 描述：是否并发准备物化视图以提高性能。
- 引入版本：v3.4.4

##### enable_mv_query_context_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用查询级别的物化视图改写缓存以提高查询改写性能。
- 引入版本：v3.3

##### mv_query_context_cache_max_size

- 默认值：1000
- 类型：-
- 单位：-
- 是否可变：是
- 描述：在一个查询的生命周期中，物化视图改写缓存的最大大小。缓存可用于避免重复计算以减少物化视图改写中的优化器时间，但可能会占用一些额外的 FE 内存。当有许多相关的物化视图（超过 10 个）或查询复杂（涉及多个表的连接）时，它可以带来更好的性能。
- 引入版本：v3.3

##### allow_system_reserved_names

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否允许用户创建以 `__op` 和 `__row` 开头的列名。要启用此功能，请将此参数设置为 `TRUE`。请注意，这些名称格式在 StarRocks 中保留用于特殊用途，创建此类列可能会导致未定义的行为。因此，此功能默认禁用。
- 引入版本：v3.2.0

##### replication_interval_ms

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：复制任务调度的最小时间间隔。
- 引入版本：v3.3.5

##### replication_max_parallel_table_count

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：允许的最大并发数据同步任务数。StarRocks 为每个表创建一个同步任务。
- 引入版本：v3.3.5

##### replication_max_parallel_replica_count

- 默认值：10240
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：允许的最大并发同步 tablet 副本数。
- 引入版本：v3.3.5

##### replication_max_parallel_data_size_mb

- 默认值：1048576
- 类型：Int
- 单位：MB
- 是否可变：是
- 描述：允许的最大并发同步数据大小。
- 引入版本：v3.3.5

##### replication_transaction_timeout_sec

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：同步任务的超时时间。
- 引入版本：v3.3.5

##### jdbc_meta_default_cache_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：JDBC Catalog 元数据缓存是否启用的默认值。设置为 True 时，新创建的 JDBC Catalog 将默认启用元数据缓存。
- 引入版本：-

##### jdbc_meta_default_cache_expire_sec

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：JDBC Catalog 元数据缓存的默认过期时间。当 `jdbc_meta_default_cache_enable` 设置为 true 时，新创建的 JDBC Catalog 将默认设置元数据缓存的过期时间。
- 引入版本：-

##### jdbc_connection_pool_size

- 默认值：8
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：用于访问 JDBC catalogs 的 JDBC 连接池的最大容量。
- 引入版本：-

##### jdbc_minimum_idle_connections

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：用于访问 JDBC catalogs 的 JDBC 连接池中的最小空闲连接数。
- 引入版本：-

##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：毫秒
- 是否可变：否
- 描述：用于访问 JDBC catalog 的连接超时的最大时间。超时的连接被视为空闲。
- 引入版本：-

##### query_detail_explain_level

- 默认值：COSTS
- 类型：String
- 单位：-
- 是否可变：是
- 描述：EXPLAIN 语句返回的查询计划的详细级别。有效值：COSTS、NORMAL、VERBOSE。
- 引入版本：v3.2.12, v3.3.5

##### mv_refresh_fail_on_filter_data

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：如果在刷新时有过滤数据，mv 刷新失败，默认情况下为 true，否则忽略过滤数据返回成功。
- 引入版本：-

##### mv_create_partition_batch_interval_ms

- 默认值：1000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：在物化视图刷新期间，如果需要批量创建多个分区，系统将它们分为每批 64 个分区。为了减少频繁创建分区导致的失败风险，设置了默认间隔（以毫秒为单位）以控制每批之间的创建频率。
- 引入版本：v3.3

##### max_mv_refresh_failure_retry_times

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：物化视图刷新失败时的最大重试次数。
- 引入版本：v3.3.0

##### max_mv_refresh_try_lock_failure_retry_times

- 默认值：3
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：物化视图刷新失败时尝试锁定的最大重试次数。
- 引入版本：v3.3.0

##### mv_refresh_try_lock_timeout_ms

- 默认值：30000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：物化视图刷新尝试其基表/物化视图的 DB 锁的默认尝试锁定超时时间。
- 引入版本：v3.3.0

##### enable_mv_refresh_collect_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否默认为所有物化视图启用刷新物化视图时的 profile。
- 引入版本：v3.3.0

##### max_mv_task_run_meta_message_values_length

- 默认值：16
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：物化视图任务运行中 "extra message" 值（在 set 或 map 中）的最大长度。您可以设置此项以避免占用过多的元内存。
- 引入版本：v3.3.0

##### max_mv_check_base_table_change_retry_times

- 默认值：10
- 类型：-
- 单位：-
- 是否可变：是
- 描述：刷新物化视图时检测基表更改的最大重试次数。
- 引入版本：v3.3.0

##### mv_refresh_default_planner_optimize_timeout

- 默认值：30000
- 类型：-
- 单位：-
- 是否可变：是
- 描述：刷新物化视图时优化器规划阶段的默认超时时间。
- 引入版本：v3.3.0

##### enable_mv_refresh_query_rewrite

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在物化视图刷新期间启用查询重写，以便查询可以直接使用重写的 mv 而不是基表来提高查询性能。
- 引入版本：v3.3

##### enable_mv_refresh_extra_prefix_logging

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在日志中启用带有物化视图名称的前缀以便更好地调试。
- 引入版本：v3.4.0

##### enable_mv_post_image_reload_cache

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：FE 加载图像后是否执行重新加载标志检查。如果对基物化视图执行检查，则不需要对与其相关的其他物化视图执行检查。
- 引入版本：v3.5.0

##### enable_trace_historical_node

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否允许系统跟踪历史节点。通过将此项设置为 `true`，您可以启用缓存共享功能，并允许系统在弹性扩展期间选择正确的缓存节点。
- 引入版本：v3.5.1