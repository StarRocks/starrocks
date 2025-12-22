---
displayed_sidebar: docs
keywords: ['Canshu']
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

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

##### audit_log_delete_age

- 默认值：30d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：审计日志文件的保留时长。默认值 `30d` 表示审计日志文件可以保留 30 天，保留时长超过 30 天的审计日志文件会被删除。
- 引入版本：-

##### audit_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：审计日志文件的保存目录。
- 引入版本：-

##### audit_log_modules

- 默认值：slow_query, query
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：打印审计日志的模块。默认打印 `slow_query` 和 `query` 模块的日志。自 v3.0 起 支持 `connection` 模块，即连接日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
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

##### audit_log_roll_num

- 默认值：90
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `audit_log_roll_interval` 时间段内，允许保留的审计日志文件的最大数目。
- 引入版本：-

##### bdbje_log_level

- 默认值: INFO
- 类型: String
- 单位: -
- 是否动态：否
- 描述: 控制 StarRocks 中 BDB JE 使用的日志级别。在 BDB 环境初始化期间，此值将会应用到 `com.sleepycat.je` 包的 Java logger 以及 BDB JE 环境的文件日志级别。有效值：`SEVERE`、`WARNING`、`INFO`、`CONFIG`、`FINE`、`FINER`、`FINEST`、`ALL`、`OFF`。设置为 `ALL` 将启用所有日志消息。增加冗长度会提高日志量，可能影响磁盘 I/O 和性能。
- 引入版本: v3.2.0

##### big_query_log_modules

- 默认值: `{"query"}`
- 类型: String[]
- 单位: -
- 是否动态：否
- 描述: 启用按模块大查询日志的模块名后缀列表。典型值为逻辑组件名称。例如，默认的 `query` 会产生 `big_query.query`。
- 引入版本: v3.2.0

##### big_query_log_roll_num

- 默认值: 10
- 类型: Int
- 单位: -
- 是否动态：否
- 描述: 每个 `big_query_log_roll_interval` 周期内要保留的已滚动 FE 大查询日志文件的最大数量。当日志按时间或按 `log_roll_size_mb` 滚动时，StarRocks 最多保留 `big_query_log_roll_num` 个带索引的文件。超过此数量的旧文件可能会被 rollover 删除，且可以通过 `big_query_log_delete_age` 根据最后修改时间进一步删除文件。
- 引入版本: v3.2.0

##### dump_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Dump 日志文件的保留时长。默认值 `7d` 表示 Dump 日志文件可以保留 7 天，保留时长超过 7 天的 Dump 日志文件会被删除。
- 引入版本：-

##### dump_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Dump 日志文件的保存目录。
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

##### dump_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `dump_log_roll_interval` 时间内，允许保留的 Dump 日志文件的最大数目。
- 引入版本：-

##### enable_audit_sql

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 是否启用 SQL 审计。当此项设置为 `true` 时，FE 审计子系统会将由 ConnectProcessor 处理的语句的 SQL 文本记录到 FE 审计日志（`fe.audit.log`）。存储的语句遵循其他控制：加密语句会被脱敏（`AuditEncryptionChecker`），如果设置了 `enable_sql_desensitize_in_log`，敏感凭据可能会被屏蔽或脱敏，摘要记录由 `enable_sql_digest` 控制。当设置为 `false` 时，ConnectProcessor 会在审计事件中将语句文本替换为 "?" —— 其他审计字段（user、host、duration、status、通过 `qe_slow_log_ms` 检测的慢查询以及指标）仍然会被记录。启用 SQL 审计可以提高取证和故障排查的可见性，但可能暴露敏感的 SQL 内容并增加日志量和 I/O；禁用它可提高隐私，但代价是审计日志中失去完整语句的可见性。
- 引入版本: -

##### internal_log_dir

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 是否动态：否
- 描述: FE 日志子系统用于存放内部日志（`fe.internal.log`）的目录。此配置会被替换到 Log4j 配置中，用于决定 InternalFile appender 将内部/物化视图/统计日志写入何处，以及 `internal.<module>` 下的各模块 logger 将其文件放置到哪里。请确保该目录存在、可写且有足够的磁盘空间。该目录中文件的日志轮转与保留由 `log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age` 和 `internal_log_roll_interval` 控制。如果启用了 `sys_log_to_console`，内部日志可能会写到控制台而不是该目录。
- 引入版本: v3.2.4

##### internal_log_modules

- 默认值: `{"base", "statistic"}`
- 类型: String[]
- 单位: -
- 是否动态：否
- 描述: 一个将接收专用内部日志记录的模块标识符列表。对于每个条目 X，Log4j 会创建一个名为 `internal.&lt;X&gt;` 的 logger，级别为 INFO 且 additivity="false"。这些 logger 会被路由到 internal appender（写入 `fe.internal.log`），当启用 `sys_log_to_console` 时则输出到控制台。根据需要使用简短名称或包片段 —— 精确的 logger 名称将成为 `internal.` + 配置的字符串。内部日志文件的滚动和保留遵循 `internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval` 和 `log_roll_size_mb` 的设置。添加模块会将其运行时消息分离到内部 logger 流中，便于调试和审计。
- 引入版本: v3.2.4

##### internal_log_roll_interval

- 默认值: DAY
- 类型: String
- 单位: -
- 是否动态：否
- 描述: 控制 FE 内部日志 appender 的基于时间的滚动间隔。接受（不区分大小写）的值为 `HOUR` 和 `DAY`。`HOUR` 生成每小时的文件模式 (`"%d{yyyyMMddHH}"`)，`DAY` 生成每日的文件模式 (`"%d{yyyyMMdd}"`)，这些模式由 RollingFile TimeBasedTriggeringPolicy 用于命名旋转后的 `fe.internal.log` 文件。无效的值会导致初始化失败（在构建活动 Log4j 配置时会抛出 IOException）。滚动行为还取决于相关设置，例如 `internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num` 和 `internal_log_delete_age`。
- 引入版本: v3.2.4

##### internal_log_roll_num

- 默认值: 90
- 类型: Int
- 单位: -
- 是否动态：否
- 描述: 内部 appender（`fe.internal.log`）保留的最大已滚动内部 FE 日志文件数。该值作为 Log4j DefaultRolloverStrategy 的 `max` 属性使用；在发生滚动时，StarRocks 会保留最多 `internal_log_roll_num` 个归档文件并删除更旧的文件（也受 `internal_log_delete_age` 控制）。较低的值可减少磁盘使用但缩短日志历史；较高的值可保留更多历史内部日志。该项与 `internal_log_dir`、`internal_log_roll_interval` 和 `internal_roll_maxsize` 配合工作。
- 引入版本: v3.2.4

##### log_cleaner_audit_log_min_retention_days

- 默认值：3
- 类型：Int
- 单位：天
- 是否动态：是
- 描述：审计日志文件的最小保留天数。即使磁盘使用率很高，也不会删除比此值更新的审计日志文件。这确保了审计日志能够保留用于合规和故障排除目的。
- 引入版本：-

##### log_cleaner_check_interval_second

- 默认值：300
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：检查磁盘使用率和清理日志的时间间隔（秒）。清理器会定期检查每个日志目录的磁盘使用率，并在必要时触发清理。默认值为 300 秒（5 分钟）。
- 引入版本：-

##### log_cleaner_disk_usage_target

- 默认值：60
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：日志清理后的目标磁盘使用率（百分比）。日志清理将持续进行，直到磁盘使用率降至低于此阈值。清理器会逐个删除最旧的日志文件，直到达到目标值。
- 引入版本：-

##### log_cleaner_disk_usage_threshold

- 默认值：80
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：触发日志清理的磁盘使用率阈值（百分比）。当日志目录的磁盘使用率超过此阈值时，将开始清理日志。清理器会独立检查每个配置的日志目录，并处理超过此阈值的目录。
- 引入版本：-

##### log_cleaner_disk_util_based_enable

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：启用基于磁盘使用率的自动日志清理功能。启用后，当日志目录的磁盘使用率超过阈值时，将自动清理日志文件。日志清理器作为后台守护进程在 FE 节点上运行，有助于防止日志文件积累导致的磁盘空间耗尽。
- 引入版本：-

##### log_roll_size_mb

- 默认值：1024
- 类型：Int
- 单位：MB
- 是否动态：否
- 描述：单个系统日志或审计日志文件的大小上限。
- 引入版本：-

##### profile_log_delete_age

- 默认值: 1d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制 FE profile 日志文件在可删除之前保留的时长。该值被注入到 Log4j 的 `&lt;IfLastModified age="..."/&gt;` 策略（通过 `Log4jConfig`）中，并与诸如 `profile_log_roll_interval` 和 `profile_log_roll_num` 的轮转设置一起生效。支持的后缀：`d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。例如：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。
- 引入版本: v3.2.5

##### profile_log_dir

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 是否动态：否
- 描述: FE profile 日志写入的目录路径。`Log4jConfig` 使用此值来放置与 profile 相关的 appender（在该目录下创建类似 `fe.profile.log` 和 `fe.features.log` 的文件）。这些文件的轮换和保留由 `profile_log_roll_size_mb`、`profile_log_roll_num` 和 `profile_log_delete_age` 控制；时间戳后缀格式由 `profile_log_roll_interval` 控制（支持 DAY 或 HOUR）。由于默认值位于 `STARROCKS_HOME_DIR` 内，请确保 FE 进程对该目录具有写入及轮换/删除权限。
- 引入版本: v3.2.5

##### profile_log_roll_num

- 默认值: 5
- 类型: Int
- 单位: Number of files
- 是否动态：否
- 描述: 指定 Log4j 的 DefaultRolloverStrategy 为 profile logger 保留的最大轮换 profile 日志文件数。该值会作为 `${profile_log_roll_num}` 注入到日志 XML 中（例如 `&lt;DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min"&gt;`）。轮换由 `profile_log_roll_size_mb` 或 `profile_log_roll_interval` 触发；发生轮换时，Log4j 最多保留这么多带索引的文件，较旧的索引文件将成为可删除对象。磁盘上的实际保留还受 `profile_log_delete_age` 和 `profile_log_dir` 位置的影响。较低的值可降低磁盘使用，但限制可保留的历史；较高的值则保留更多历史 profile 日志。
- 引入版本: v3.2.5

##### profile_log_roll_size_mb

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: 否
- 描述: 设置触发基于大小的 FE profile 日志文件滚动的阈值（以兆字节为单位）。该值被 Log4j 的 RollingFile SizeBasedTriggeringPolicy 用于 `ProfileFile` appender；当 profile 日志超过 `profile_log_roll_size_mb` 时会发生轮转。达到 `profile_log_roll_interval` 时也会按时间发生轮转 —— 任一条件满足都会触发轮转。结合 `profile_log_roll_num` 和 `profile_log_delete_age` 使用，该项控制保留多少历史 profile 文件以及何时删除旧文件。轮转文件的压缩由 `enable_profile_log_compress` 控制。
- 引入版本: v3.2.5

##### qe_slow_log_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：Slow query 的认定时长。如果查询的响应时间超过此阈值，则会在审计日志 `fe.audit.log` 中记录为 slow query。
- 引入版本：-

##### slow_lock_log_every_ms

- 默认值: 3000L
- 类型: Long
- 单位: 毫秒
- 是否可变: 是
- 描述: 在对同一个 SlowLockLogStats 实例再次输出“慢锁”警告之前，至少要等待的最小间隔（毫秒）。当锁等待时间超过 `slow_lock_threshold_ms` 后，LockUtils 会检查此值并在最近一次记录的慢锁事件发生到现在未达到 `slow_lock_log_every_ms` 毫秒时抑制额外警告。在长时间争用期间使用较大的值以减少日志量，或使用较小的值以获取更频繁的诊断信息。更改在运行时对后续检查生效。
- 引入版本: v3.2.0

##### slow_lock_threshold_ms

- 默认值: 3000L
- 类型: long
- 单位: 毫秒
- 是否可变: 是
- 描述: 用于将一个锁操作或持有的锁归类为“慢”的阈值（毫秒）。当锁的等待时间或持有时间超过此值时，StarRocks 将（视上下文）输出诊断日志、包含堆栈跟踪或 waiter/owner 信息，并且在 LockManager 中在该延迟后启动死锁检测。此配置由 LockUtils（慢锁日志）、QueryableReentrantReadWriteLock（筛选慢读取者）、LockManager（死锁检测延迟和慢锁跟踪）、LockChecker（周期性慢锁检测）以及其他调用方（例如 DiskAndTabletLoadReBalancer 日志）使用。降低该值会提高敏感度并增加日志/诊断开销；将其设置为 0 或负值会禁用基于初始等待的死锁检测延迟行为。请与 `slow_lock_log_every_ms`、`slow_lock_print_stack` 和 `slow_lock_stack_trace_reserve_levels` 一起调整。
- 引入版本: 3.2.0

##### sys_log_delete_age

- 默认值：7d
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志文件的保留时长。默认值 `7d` 表示系统日志文件可以保留 7 天，保留时长超过 7 天的系统日志文件会被删除。
- 引入版本：-

##### sys_log_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志文件的保存目录。
- 引入版本：-

##### sys_log_enable_compress

- 默认值: false
- 类型: boolean
- 单位: -
- 是否动态：否
- 描述: 当此项设置为 `true` 时，系统会在轮转的系统日志文件名后追加 ".gz" 后缀，从而使 Log4j 在轮转时生成 gzip 压缩的 FE 系统日志（例如，`fe.log.*`）。此值在生成 Log4j 配置时读取（Log4jConfig.initLogging / generateActiveLog4jXmlConfig），并控制用于 RollingFile filePattern 的 `sys_file_postfix` 属性。启用此功能可减少保留日志的磁盘占用，但会在轮转期间增加 CPU 和 I/O 开销，并改变日志文件名，因此读取日志的工具或脚本必须能够处理 .gz 文件。注意，审计日志使用单独的压缩配置，即 `audit_log_enable_compress`。
- 引入版本: v3.2.12

##### sys_log_format

- 默认值: "plaintext"
- 类型: String
- 单位: -
- 是否动态：否
- 描述: 选择用于 FE 日志的 Log4j layout。有效值：`"plaintext"`（默认）和 `"json"`。值不区分大小写。`"plaintext"` 配置 PatternLayout，包含可读的时间戳、级别、线程、class.method:line 以及 WARN/ERROR 的堆栈跟踪。`"json"` 配置 JsonTemplateLayout，输出结构化 JSON 事件（UTC 时间戳、级别、线程 id/name、源文件/方法/行、消息、异常 stackTrace），适合日志聚合器（ELK、Splunk）。JSON 输出遵守 `sys_log_json_max_string_length` 和 `sys_log_json_profile_max_string_length` 关于最大字符串长度的限制。
- 引入版本: v3.2.10

##### sys_log_json_max_string_length

- 默认值: 1048576
- 类型: Int
- 单位: Bytes
- 是否可变: 否
- 描述: 设置用于系统 JSON 格式系统日志的 JsonTemplateLayout 的 "maxStringLength" 值。当 `sys_log_format` 被设置为 `"json"` 时，如果字符串类型字段（例如 "message" 和字符串化的异常堆栈跟踪）的长度超过此限制则会被截断。该值在 `Log4jConfig.generateActiveLog4jXmlConfig()` 中注入到生成的 Log4j XML，并应用于 default、warning、audit、dump 和 bigquery 布局；Profile 布局使用单独的配置（`sys_log_json_profile_max_string_length`）。降低此值可减少日志大小，但可能截断有用信息。由于此设置不可变，更改需要重启或重新配置负责重新生成 Log4j 配置的进程。
- 引入版本: v3.2.11

##### sys_log_json_profile_max_string_length

- 默认值: 104857600 (100 MB)
- 类型: Int
- 单位: Bytes
- 是否可变: 否
- 描述: 当 `sys_log_format` 为 `"json"` 时，为 Profile（及相关功能）日志 Appender 设置 JsonTemplateLayout 的 maxStringLength。JSON 格式的 Profile 日志中的字符串字段值将被截断到此字节长度；非字符串字段不受影响。此设置在 Log4jConfig（JsonTemplateLayout 的 maxStringLength）中应用，在使用纯文本日志时被忽略。请将值设置为能够包含所需完整消息的足够大值，但注意过大的值会增加日志大小和 I/O。
- 引入版本: v3.2.11

##### sys_log_level

- 默认值：INFO
- 类型：String
- 单位：-
- 是否动态：否
- 描述：系统日志的级别，从低到高依次为 `INFO`、`WARN`、`ERROR`、`FATAL`。
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

##### sys_log_roll_num

- 默认值：10
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：每个 `sys_log_roll_interval` 时间段内，允许保留的系统日志文件的最大数目。
- 引入版本：-

##### sys_log_to_console

- 默认值: false (除非环境变量 `SYS_LOG_TO_CONSOLE` 被设置为 "1")
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 当此项设置为 `true` 时，系统会配置 Log4j 将所有日志发送到控制台（ConsoleErr appender），而不是基于文件的 appender。该值在生成活动的 Log4j XML 配置时读取（影响 root logger 和按模块的 logger appender 选择）。其值在进程启动时从环境变量 `SYS_LOG_TO_CONSOLE` 捕获。运行时更改不会生效。该配置常用于容器化或 CI 环境，在这些环境中更偏向于通过 stdout/stderr 收集日志而不是写入日志文件。
- 引入版本: v3.2.0

##### sys_log_verbose_modules

- 默认值：空字符串
- 类型：String[]
- 单位：-
- 是否动态：否
- 描述：打印系统日志的模块。如果设置参数取值为 `org.apache.starrocks.catalog`，则表示只打印 Catalog 模块下的日志。
- 引入版本：-

##### sys_log_warn_modules

- 默认值: `{}`
- 类型: String[]
- 单位: -
- 是否动态：否
- 描述: 一个记录器名称或包前缀列表，系统在启动时会将其配置为 WARN 级别的记录器并路由到警告 appender（SysWF）— 即 `fe.warn.log` 文件。条目会被插入到生成的 Log4j 配置中（与内置的 warn 模块一起，例如 org.apache.kafka、org.apache.hudi 和 org.apache.hadoop.io.compress），并生成类似 `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` 的 logger 元素。建议使用完全限定的包和类前缀（例如 "com.example.lib"）来抑制在常规日志中产生噪声的 INFO/DEBUG 输出，并允许将警告单独捕获。
- 引入版本: v3.2.13

### 服务器

##### brpc_idle_wait_max_time

- 默认值：10000
- 类型：Int
- 单位：ms
- 是否动态：否
- 描述：bRPC 的空闲等待时间。
- 引入版本：-

##### cluster_name

- 默认值：StarRocks Cluster
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所在 StarRocks 集群的名称，显示为网页标题。
- 引入版本：-

##### enable_http_async_handler

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许系统异步处理 HTTP 请求。如果启用此功能，Netty 工作线程收到的 HTTP 请求将提交到单独的线程池进行业务逻辑处理，以避免阻塞 HTTP 服务器。如果禁用，Netty 工作线程将处理业务逻辑。
- 引入版本：4.0.0

##### enable_https

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：No
- 描述：是否在 FE 节点上同时启用 HTTPS 服务器和 HTTP 服务器。
- 引入版本：v4.0

##### frontend_address

- 默认值：0.0.0.0
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 节点的 IP 地址。
- 引入版本：-

##### http_async_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：用于异步处理 HTTP 请求的线程池大小。别名为 `max_http_sql_service_task_threads_num`。
- 引入版本：4.0.0

##### http_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：HTTP 服务器支持的 Backlog 队列长度。
- 引入版本：-

##### http_max_initial_line_length

- 默认值: `4096`
- 类型: Int
- 单位: Bytes
- 是否可变: 否
- 描述: 设置由 HttpServer 中使用的 Netty `HttpServerCodec` 接受的 HTTP 初始请求行（method + request-target + HTTP version）的最大允许长度（字节）。该值会传递给 Netty 的解码器，初始行长度超过此值的请求将被拒绝（TooLongFrameException）。仅在必须支持非常长的请求 URI 时才增大此值；更大的值会增加内存使用并可能提高对畸形请求或滥用请求的暴露面。应与 `http_max_header_size` 和 `http_max_chunk_size` 一起调整。
- 引入版本: `v3.2.0`

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

##### https_port

- 默认值：8443
- 类型：Int
- 单位：-
- 是否动态：No
- 描述：FE 节点中 HTTPS 服务器监听的端口。
- 引入版本：v4.0

##### max_mysql_service_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器中用于处理任务的最大线程数。
- 引入版本：-

##### mysql_nio_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器支持的 Backlog 队列长度。
- 引入版本：-

##### mysql_server_version

- 默认值：8.0.33
- 类型：String
- 单位：-
- 是否动态：是
- 描述：MySQL 服务器的版本。修改该参数配置会影响以下场景中返回的版本号：
  1. `select version();`
  2. Handshake packet 版本
  3. 全局变量 `version` 的取值 (`show variables like 'version';`)
- 引入版本：-

##### mysql_service_io_threads_num

- 默认值：4
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：MySQL 服务器中用于处理 I/O 事件的最大线程数。
- 引入版本：-

##### mysql_service_nio_enable_keep_alive

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 否
- 描述: 是否为 MySQL 连接启用 TCP Keep-Alive。适用于位于负载均衡器后且长时间空闲的连接。
- 引入版本: -

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

##### qe_max_connection

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 支持的最大连接数，包括所有用户发起的连接。默认值由 v3.1.12、v3.2.7 起由 `1024` 变为 `4096`。
- 引入版本：-

##### query_port

- 默认值：9030
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 节点上 MySQL 服务器的端口。
- 引入版本：-

##### rpc_port

- 默认值：9020
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：FE 节点上 Thrift 服务器的端口。
- 引入版本：-

##### ssl_cipher_blacklist

- 默认值: Empty string
- 类型: String
- 单位: -
- 是否动态：否
- 描述:基于 IANA 名称的 SSL 密文 Suite 黑名单，多项以逗号分隔，支持正则表达式。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

##### ssl_cipher_whitelist

- 默认值: Empty string
- 类型: String
- 单位: -
- 是否动态：否
- 描述: 基于 IANA 名称的 SSL 密文 Suite 白名单，多项以逗号分隔，支持正则表达式。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

##### thrift_backlog_num

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Thrift 服务器支持的 Backlog 队列长度。
- 引入版本：-

##### thrift_client_timeout_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：Thrift 客户端链接的空闲超时时间，即链接超过该时间无新请求后则将链接断开。
- 引入版本：-

##### thrift_rpc_max_body_size

- 默认值: `-1`
- 类型: Int
- 单位: Bytes
- 是否动态：否
- 描述: 控制在构造服务器的 Thrift 协议（传递给 `ThriftServer` 中的 TBinaryProtocol.Factory）时允许的最大 Thrift RPC 消息体大小（以字节为单位）。值为 `-1` 表示禁用限制（无界）。设置为正值会强制上限，使得超过该大小的消息会被 Thrift 层拒绝，这有助于限制内存使用并降低超大请求或 DoS 风险。请将此值设置为足够大的大小以容纳预期的有效载荷（大型 struct 或批量数据），以避免拒绝合法请求。
- 引入版本: `v3.2.0`

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

### 元数据与集群管理

##### alter_max_worker_queue_size

- 默认值: `4096`
- 类型: Int
- 单位: Tasks
- 是否动态：否
- 描述: 控制 alter 子系统使用的内部工作线程池队列的容量。该值与 `alter_max_worker_threads` 一起在 `AlterHandler` 中传递给 `ThreadPoolManager.newDaemonCacheThreadPool`。当待处理的 alter 任务数量超过 `alter_max_worker_queue_size` 时，新提交的任务将被拒绝，可能会抛出 `RejectedExecutionException`（参见 `AlterHandler.handleFinishAlterTask`）。调整此值以在内存使用与允许的并发 alter 任务积压量之间取得平衡。
- 引入版本: `v3.2.0`

##### automated_cluster_snapshot_interval_seconds

- 默认值：600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：自动化集群快照任务的触发间隔。
- 引入版本：v3.4.2

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

##### bdbje_heartbeat_timeout_second

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中 Leader FE 和 Follower FE 之间的 BDB JE 心跳超时时间。
- 引入版本：-

##### bdbje_lock_timeout_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：BDB JE 操作的锁超时时间。
- 引入版本：-

##### bdbje_replica_ack_timeout_second

- 默认值：10
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中，元数据从 Leader FE 写入到多个 Follower FE 时，Leader FE 等待足够多的 Follower FE 发送 ACK 消息的超时时间。当写入的元数据较多时，可能返回 ACK 的时间较长，进而导致等待超时。如果超时，会导致写元数据失败，FE 进程退出，此时可以适当地调大该参数取值。
- 引入版本：-

##### bdbje_reserved_disk_size

- 默认值: `512L * 1024 * 1024` (536870912)
- 类型: Long
- 单位: Bytes
- 是否动态：否
- 描述: 限制 Berkeley DB JE 将被保留为“未保护”（可删除）的日志/数据文件的字节数。StarRocks 通过 BDBEnvironment 中的 `EnvironmentConfig.RESERVED_DISK` 将此值传递给 JE；JE 的内置默认值为 0（无限制）。StarRocks 的默认值（512 MiB）可防止 JE 为未保护文件保留过多磁盘空间，同时允许安全清理过期文件。在磁盘受限的系统上调整此值：减小它可以让 JE 更早释放文件，增大它可以让 JE 保留更多保留空间。更改需要重启进程才能生效。
- 引入版本: `v3.2.0`

##### bdbje_reset_election_group

- 默认值：false
- 类型：String
- 单位：-
- 是否动态：否
- 描述：是否重置 BDBJE 复制组。如果设置为 `TRUE`，FE 将重置 BDBJE 复制组（即删除所有 FE 节点的信息）并以 Leader 身份启动。重置后，该 FE 将成为集群中唯一的成员，其他 FE 节点通过 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入该集群。仅当无法成功选举出 leader FE 时（因为大部分 follower FE 数据已损坏）才使用此配置。该参数用来替代 `metadata_failure_recovery`。
- 引入版本：-

##### black_host_connect_failures_within_time

- 默认值：5
- 类型：Int
- Unit:
- 是否动态：是
- 描述：黑名单中的 BE 节点允许连接失败的上限。如果一个 BE 节点被自动添加到 BE 黑名单中，StarRocks 将评估其连接状态，并判断是否可以将其从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，StarRocks 才会将其从 BE 黑名单中移除。
- 引入版本：v3.3.0

##### black_host_history_sec

- 默认值：2 * 60
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：黑名单中 BE 节点连接失败记录的保留时长。如果一个 BE 节点被自动添加到 BE 黑名单中，StarRocks 将评估其连接状态，并判断是否可以将其从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当黑名单中的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，StarRocks 才会将其从 BE 黑名单中移除。
- 引入版本：v3.3.0

##### brpc_connection_pool_size

- 默认值: `16`
- 类型: Int
- 单位: Connections
- 是否动态：否
- 描述: FE 的 BrpcProxy 为每个端点使用的最大池化 BRPC 连接数。此值通过 `setMaxTotoal` 和 `setMaxIdleSize` 应用到 RpcClientOptions，因此它直接限制了并发的出站 BRPC 请求数，因为每个请求都必须从连接池借用一个连接。在高并发场景下增加此值以避免请求排队；增加会提高 socket 和内存使用，并可能增加远端服务器负载。调优时需考虑相关设置，例如 `brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr` 和 `brpc_min_evictable_idle_time_ms`。更改此值不会热重载，需要重启生效。
- 引入版本: `v3.2.0`

##### catalog_try_lock_timeout_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：全局锁（Global Lock）获取的超时时长。
- 引入版本：-

##### checkpoint_timeout_seconds

- 默认值: `24 * 3600`
- 类型: Long
- 单位: 秒
- 是否动态：是
- 描述: leader 的 CheckpointController 在等待某个 checkpoint worker 完成 checkpoint 时的最长等待时间（单位：秒）。控制器会将该值转换为纳秒并轮询 worker 的结果队列；如果在此超时时间内未收到成功完成，则将该 checkpoint 视为失败，createImage 返回失败。增大此值可以容纳运行时间更长的 checkpoint，但会延迟失败检测及随后镜像传播；减小此值可以更快触发故障转移/重试，但可能对较慢的 worker 产生误超时。此设置仅控制 CheckpointController 在创建 checkpoint 时的等待时长，不会改变 worker 的内部 checkpoint 行为。
- 引入版本: `v3.4.0, v3.5.0`

##### db_used_data_quota_update_interval_secs

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：数据库已使用数据配额的更新间隔。StarRocks 会定期更新所有数据库的已使用数据配额以跟踪存储消耗情况。此值用于配额管控和指标采集。允许的最小间隔为 30 秒，以防止系统负载过高。如果配置的值小于 30，将被拒绝。
- 引入版本：-

##### drop_backend_after_decommission

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：BE 被下线后，是否删除该 BE。true 代表 BE 被下线后会立即删除该 BE。False 代表下线完成后不删除 BE。
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

##### edit_log_type

- 默认值：BDB
- 类型：String
- 单位：-
- 是否动态：否
- 描述：编辑日志的类型。取值只能为 `BDB`。
- 引入版本：-

##### enable_background_refresh_connector_metadata

- 默认值：true in v3.0 and later and false in v2.5
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。
- 引入版本：v2.5.5

##### enable_collect_query_detail_info

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否收集查询的 Profile 信息。设置为 `true` 时，系统会收集查询的 Profile。设置为 `false` 时，系统不会收集查询的 profile。
- 引入版本：-

##### enable_internal_sql

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：否
- 描述: 当此项设置为 `true` 时，内部组件（例如 SimpleExecutor）执行的内部 SQL 语句会被保留并写入内部审计或日志消息（如果设置了 `enable_sql_desensitize_in_log`，还可以进一步脱敏）。当设置为 `false` 时，内部 SQL 文本会被压制：格式化代码（`SimpleExecutor.formatSQL`）返回 "?"，实际语句不会输出到内部审计或日志消息。此配置不会改变内部语句的执行语义，仅控制内部 SQL 的日志记录和可见性以保护隐私或安全。
- 引入版本: -

##### enable_legacy_compatibility_for_replication

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为跨集群数据迁移开启旧版本兼容。新旧版本的集群间可能存在行为差异，从而导致跨集群数据迁移时出现问题。因此在数据迁移前，您需要为目标集群开启旧版本兼容，并在数据迁移完成后关闭。`true` 表示开启兼容。
- 引入版本：v3.1.10, v3.2.6

##### enable_statistics_collect_profile

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：统计信息查询时是否生成 Profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计查询生成 Profile。
- 引入版本：v3.1.5

##### enable_table_name_case_insensitive

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否启用针对 Catalog 名称、数据库名称、表名称、视图名称以及异步物化视图名称的大小写不敏感处理。目前，表名称默认区分大小写。
  - 启用此功能后，所有相关名称将以小写形式存储，且所有包含这些名称的 SQL 命令将自动将其转换为小写。
  - 您只能在创建集群时启用此功能。**集群启动后，此配置项的值无法通过任何方式修改**。任何修改尝试均会导致错误。当 FE 检测到此配置项的值与集群首次启动时不一致时，FE 将无法启动。  
  - 目前，此功能不支持 JDBC Catalog 和表名。若需对 JDBC 或 ODBC 数据源进行大小写不敏感处理，请勿启用此功能。
- 引入版本：v4.0

##### enable_task_history_archive

- 默认值: `true`
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 启用后，已完成的 task-run 记录会归档到持久化的 task-run history 表，并记录到 edit log，这样查找（例如 `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`）会包含归档结果。归档由 FE leader 执行，在单元测试期间会跳过（`FeConstants.runningUnitTest`）。启用时，会绕过内存过期和强制 GC 路径（代码会在 `removeExpiredRuns` 和 `forceGC` 中提前返回），因此保留/驱逐由持久化归档处理，而不是由 `task_runs_ttl_second` 和 `task_runs_max_history_number` 控制。禁用时，历史保留在内存中并由这些配置进行裁剪。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### enable_task_run_fe_evaluation

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 启用后，FE 将在 `TaskRunsSystemTable.supportFeEvaluation` 中对系统表 `task_runs` 执行本地评估。FE 端评估仅允许对列与常量进行的合取等值谓词，并且仅限于列 `QUERY_ID` 和 `TASK_NAME`。启用此项可通过避免更广泛的扫描或额外的远程处理来提高针对性查找的性能；禁用会强制 planner 跳过对 `task_runs` 的 FE 评估，可能降低谓词裁剪效果并影响这些过滤条件的查询延迟。
- 引入版本: v3.3.13, v3.4.3, v3.5.0

##### heartbeat_mgr_blocking_queue_size

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中存储心跳任务的阻塞队列大小。
- 引入版本：-

##### heartbeat_mgr_threads_num

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：Heartbeat Manager 中用于发送心跳任务的最大线程数。
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

##### max_bdbje_clock_delta_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：否
- 描述：FE 所在 StarRocks 集群中 Leader FE 与非 Leader FE 之间能够容忍的最大时钟偏移。
- 引入版本：-

##### meta_delay_toleration_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：FE 所在 StarRocks 集群中，非 Leader FE 能够容忍的元数据落后的最大时间。如果非 Leader FE 上的元数据与 Leader FE 上的元数据之间的延迟时间超过该参数取值，则该非 Leader FE 将停止服务。
- 引入版本：-

##### meta_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：元数据的保存目录。
- 引入版本：-

##### metadata_ignore_unknown_operation_type

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否忽略未知的 logID。当 FE 回滚到低版本时，可能存在低版本 FE 无法识别的 logID。如果设置为 `TRUE`，则 FE 会忽略这些 logID；否则 FE 会退出。
- 引入版本：-

##### profile_info_format

- 默认值：default
- 类型：String
- 单位：-
- 是否动态：是
- 描述：系统输出 Profile 的格式。有效值：`default` 和 `json`。设置为 `default` 时，Profile 为默认格式。设置为 `json` 时，系统输出 JSON 格式 Profile。
- 引入版本：V2.5

##### replica_ack_policy

- 默认值：SIMPLE_MAJORITY
- 类型：String
- 单位：-
- 是否动态：否
- 描述：判定日志是否有效的策略，默认值表示多数 Follower FE 返回确认消息，就认为生效。
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

##### txn_latency_metric_report_groups

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变: 是
- 描述：需要汇报的事务延迟监控指标组，多个指标组通过逗号分隔。导入类型基于监控组分类，被启用的组其名称会作为 'type' 标签添加到监控指标中。常见的组包括 `stream_load`、`routine_load`、`broker_load`、`insert`，以及 `compaction` (仅适用于存算分离集群)。示例：`"stream_load,routine_load"`。
- 引入版本: v4.0

##### txn_rollback_limit

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：允许回滚的最大事务数。
- 引入版本：-

### 用户，角色及权限

##### privilege_max_role_depth

- 默认值：16
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个角色最多的嵌套层数。
- 引入版本：-

##### privilege_max_total_roles_per_user

- 默认值：64
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个用户最多可以拥有的角色数量。
- 引入版本：-

### 查询引擎

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

##### connector_table_query_trigger_analyze_small_table_interval

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表采集间隔。
- 引入版本：v3.4.0

##### connector_table_query_trigger_analyze_small_table_rows

- 默认值：10000000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：查询触发 ANALYZE 任务的小表阈值。
- 引入版本：v3.4.0

##### connector_table_query_trigger_task_schedule_interval

- 默认值：30
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：Schedule 线程调度查询触发的后台任务的周期。该项用于取代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。此处后台任务是指 v3.4 中的 `ANALYZE` 任务，以及 v3.4 之后版本中引入的低基数列字典的收集任务。
- 引入版本：v3.4.2

##### create_table_max_serial_replicas

- 默认值：128
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：串行创建副本的最大数量。如果实际副本数量超过此值，副本将并发创建。如果建表需要长时间才能完成，请尝试减少此值。
- 引入版本：-

##### default_mv_partition_refresh_number

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述： 当一次物化视图刷新涉及多个分区时，该参数用于控制默认每次刷新多少个分区。
从 3.3.0 版本开始，系统默认每次仅刷新一个分区，以避免内存占用过高的问题；而在此前的版本中，系统会尝试一次性刷新所有分区，这可能导致刷新任务因内存不足（OOM）而失败。但也需注意：当每次物化视图刷新涉及大量分区时，默认每次仅刷新一个分区可能导致调度频繁、整体刷新耗时较长，甚至生成过多的刷新记录。因此，在特定场景下，可适当调整该参数值，以提升刷新效率并控制调度开销。
- 引入版本：v3.3.0

##### default_mv_refresh_immediate

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：创建异步物化视图后，是否立即刷新该物化视图。当设置为 `true` 时，异步物化视图创建后会立即刷新。
- 引入版本：v3.2.3

##### dynamic_partition_check_interval_seconds

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：动态分区检查的时间周期。如果有新数据生成，会自动生成分区。
- 引入版本：-

##### dynamic_partition_enable

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启动态分区功能。打开后，您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。
- 引入版本：-

##### enable_active_materialized_view_schema_strict_check

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在激活失效物化视图时是否严格检查数据类型长度一致性。当设置为 `false` 时，如基表的数据类型长度有变化，也不影响物化视图的激活。
- 引入版本：v3.3.4

##### enable_auto_collect_array_ndv

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许自动采集 ARRAY 类型列的 NDV 信息。
- 引入版本：v4.0

##### enable_backup_materialized_view

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在数据库的备份操作中，是否对数据库中的异步物化视图进行备份。如果设置为 `false`，将跳过对异步物化视图的备份。
- 引入版本：v3.2.0

##### enable_collect_full_statistic

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启自动全量统计信息采集，该开关默认打开。
- 引入版本：-

##### enable_colocate_mv_index

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在创建同步物化视图时，是否将同步物化视图的索引与基表加入到相同的 Colocate Group。如果设置为 `true`，TabletSink 将加速同步物化视图的写入性能。
- 引入版本：v3.2.0

##### enable_decimal_v3

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 Decimal V3。
- 引入版本：-

##### enable_experimental_mv

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启异步物化视图功能。`TRUE` 表示开启。从 2.5.2 版本开始，该功能默认开启。2.5.2 版本之前默认值为 `FALSE`。
- 引入版本：v2.4

##### enable_local_replica_selection

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否选择本地副本进行查询。本地副本可以减少数据传输的网络时延。如果设置为 `true`，优化器优先选择与当前 FE 相同 IP 的 BE 节点上的 Tablet 副本。设置为 `false` 表示选择可选择本地或非本地副本进行查询。
- 引入版本：-

##### enable_manual_collect_array_ndv

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许手动采集 ARRAY 类型列的 NDV 信息。
- 引入版本：v4.0

##### enable_materialized_view

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许创建物化视图。
- 引入版本：-

##### enable_materialized_view_metrics_collect

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否默认收集异步物化视图的监控指标。
- 引入版本：v3.1.11，v3.2.5

##### enable_materialized_view_spill

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为物化视图的刷新任务开启中间结果落盘功能。
- 引入版本：v3.1.1

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

##### enable_predicate_columns_collection

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否启用 Predicate Column 采集。如果禁用，在查询优化期间将不会记录 Predicate Column。
- 引入版本：-

##### enable_sql_blacklist

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否开启 SQL Query 黑名单校验。如果开启，在黑名单中的 Query 不能被执行。
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
- 描述：控制数据导入操作触发的自动统计信息采集和维护。包括：
  - 分区首次导入数据时的统计信息采集（分区版本号为 2）。
  - 多分区表的空分区导入数据时的统计信息采集。
  - INSERT OVERWRITE 操作的统计信息复制和更新。

  **统计信息采集类型的决策策略：**
  
  - 对于 INSERT OVERWRITE：`deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load`（默认：0.1），则不采集统计信息，仅复制现有统计信息。
    - 否则，如果 `targetRows > statistic_sample_collect_rows`（默认：200000），则使用抽样统计信息。
    - 否则，使用全量统计信息。
  
  - 对于首次导入：`deltaRatio = loadRows / (totalRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load`（默认：0.1），则不采集统计信息。
    - 否则，如果 `loadRows > statistic_sample_collect_rows`（默认：200000），则使用抽样统计信息。
    - 否则，使用全量统计信息。
  
  **同步行为：**
  
  - DML 语句（INSERT INTO/INSERT OVERWRITE）：同步模式，带表锁。等待统计信息采集完成（最多等待 `semi_sync_collect_statistic_await_seconds` 秒）。
  - Stream Load 和 Broker Load：异步模式，不加锁。统计信息采集在后台运行，不阻塞导入。
  
  :::note
  禁用此配置将阻止所有由导入触发的统计信息操作，包括 INSERT OVERWRITE 的统计信息维护，这可能导致表缺少统计信息。如果系统频繁创建新表并且导入数据，启用此功能会存在一定内存和 CPU 开销。
  :::

- 引入版本：v3.1

##### enable_statistic_collect_on_update

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：控制 UPDATE 语句是否可以触发自动统计信息采集。启用时，修改表数据的 UPDATE 操作可以通过由 `enable_statistic_collect_on_first_load` 控制的基于导入的统计信息框架来调度统计信息采集。禁用此配置将跳过 UPDATE 语句的统计信息采集，同时保持由导入触发的统计信息采集行为不变。
- 引入版本：v3.5.11, v4.0.4

##### enable_udf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启 UDF。
- 引入版本：-

##### expr_children_limit

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个表达式中允许的最大子表达式数量。
- 引入版本：-

##### histogram_buckets_size

- 默认值：64
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：直方图默认分桶数。
- 引入版本：-

##### histogram_max_sample_row_count

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：直方图最大采样行数。
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

##### http_slow_request_threshold_ms

- 默认值：5000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：如果一条 HTTP 请求的时间超过了该参数指定的时长，会生成日志来跟踪该请求。
- 引入版本：v2.5.15，v3.1.5

##### low_cardinality_threshold

- 默认值：255
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：低基数字典阈值。
- 引入版本：v3.5.0

##### max_allowed_in_element_num_of_delete

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：DELETE 语句中 IN 谓词最多允许的元素数量。
- 引入版本：-

##### max_create_table_timeout_second

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：建表的最大超时时间。
- 引入版本：-

##### max_distribution_pruner_recursion_depth

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：: 分区裁剪允许的最大递归深度。增加递归深度可以裁剪更多元素但同时增加 CPU 资源消耗。
- 引入版本：-

##### max_partitions_in_one_batch

- 默认值：4096
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：批量创建分区时，分区数目的最大值。
- 引入版本：-

##### max_planner_scalar_rewrite_num

- 默认值：100000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：优化器重写 ScalarOperator 允许的最大次数。
- 引入版本：-

##### max_query_retry_time

- 默认值：2
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：FE 上查询重试的最大次数。
- 引入版本：-

##### max_running_rollup_job_num_per_table

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：每个 Table 执行 Rollup 任务的最大并发度。
- 引入版本：-

##### max_scalar_operator_flat_children

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：优化器中 ScalarOperator 允许最多后代节点数量, 这个限制通常是避免优化器使用过多内存。
- 引入版本：-

##### max_scalar_operator_optimize_depth

- 默认值：256
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：优化器中 ScalarOperator 进行优化的最大深度。
- 引入版本：-

##### mv_active_checker_interval_seconds

- 默认值：60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：当开启后台 active_checker 线程后，系统将定期自动检查并激活因基表（或视图）发生 Schema 变更或重建而变为失效状态（Inactive）的物化视图。该参数用于控制该线程的调度执行间隔，单位为秒。默认值为系统设定的时间间隔。
- 引入版本：v3.1.6

##### publish_version_interval_ms

- 默认值：10
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：两个版本发布操作之间的时间间隔。
- 引入版本：-

##### query_queue_v2_cpu_costs_per_slot

- 默认值: `1000000000`
- 类型: Long
- 单位: planner CPU cost units
- 是否可变: 是
- 描述: 每个 slot 的 CPU 成本阈值，用于根据查询的 planner CPU cost 估算该查询需要多少 slot。调度器将以 integer(plan_cpu_costs / `query_queue_v2_cpu_costs_per_slot`) 的方式计算 slot 数量，然后将结果限定在 [1, totalSlots] 范围内（totalSlots 来源于查询队列 V2 的 `V2` 参数）。V2 代码会将非正值规范化为 1（Math.max(1, value)），因此非正值实际上等同于 `1`。增大此值会减少每个查询分配的 slot（偏向更少但每个占用更多 slot 的查询）；减小此值会增加每个查询的 slot。应与 `query_queue_v2_num_rows_per_slot` 及并发设置一起调整，以在并行度与资源粒度之间取得平衡。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### semi_sync_collect_statistic_await_seconds

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：DML 操作（INSERT 和 INSERT OVERWRITE 语句）期间半同步统计信息采集的最大等待时间。Stream Load 和 Broker Load 使用异步模式，不受此设置影响。如果统计信息采集超过此超时时间，导入将继续进行而不等待采集完成。此设置与 `enable_statistic_collect_on_first_load` 配合使用。
- 引入版本：v3.1

##### slow_query_analyze_threshold

- 默认值：5
- 类型：Int
- 单位：秒
- 是否动态：是
- 描述：查询触发 Query Feedback 分析的执行时间阈值。
- 引入版本：v3.4.0

##### statistic_analyze_status_keep_second

- 默认值：3 * 24 * 3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：统计信息采集任务的记录保留时间，默认为 3 天。
- 引入版本：-

##### statistic_auto_analyze_end_time

- 默认值：23:59:59
- 类型：String
- 单位：-
- 是否动态：是
- 描述：用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。
- 引入版本：-

##### statistic_auto_analyze_start_time

- 默认值：00:00:00
- 类型：String
- 单位：-
- 是否动态：是
- 描述：用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。
- 引入版本：-

##### statistic_auto_collect_ratio

- 默认值：0.8
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。
- 引入版本：-

##### statistic_auto_collect_small_table_rows

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：自动收集中，用于判断外部数据源下的表 (Hive, Iceberg, Hudi) 是否为小表的行数门限。
- 引入版本：v3.2

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

##### statistic_collect_interval_sec

- 默认值：5 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：自动定期采集任务中，检测数据更新的间隔时间。
- 引入版本：-

##### statistic_max_full_collect_data_size

- 默认值：100 * 1024 * 1024 * 1024
- 类型：Long
- 单位：bytes
- 是否动态：是
- 描述：自动统计信息采集的单次任务最大数据量。如果超过该值，则放弃全量采集，转为对该表进行抽样采集。
- 引入版本：-

##### statistic_sample_collect_rows

- 默认值：200000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：导入触发的统计信息采集操作中用于决定抽样采集和全量采集的行数阈值。如果加载或更改的行数超过此阈值（默认 200,000 行），则使用抽样统计信息采集；否则使用全量统计信息采集。此设置与 `enable_statistic_collect_on_first_load` 和 `statistic_sample_collect_ratio_threshold_of_first_load` 配合使用。
- 引入版本：-

##### statistic_update_interval_sec

- 默认值：24 * 60 * 60
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：统计信息内存 Cache 失效时间。
- 引入版本：-

##### task_check_interval_second

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否动态：是
- 描述: 任务后台作业执行之间的间隔。减小该值会使后台维护（任务清理、检查）更频繁地运行并更快地响应，但会增加 CPU 和 I/O 开销；增大该值则减少开销但会延迟清理和过期任务的检测。请根据维护响应速度和资源使用情况来调整此值。
- 引入版本: v3.2.0

### 导入导出

##### broker_load_default_timeout_second

- 默认值：14400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Broker Load 的超时时间。
- 引入版本：-

##### desired_max_waiting_jobs

- 默认值：1024
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：最多等待的任务数，适用于所有的任务，建表、导入、Schema Change。如果 FE 中处于 PENDING 状态的作业数目达到该值，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。从 2.5 版本开始，该参数默认值从 100 变为 1024。
- 引入版本：-

##### disable_load_job

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当集群遇到错误时是否禁用导入。这可以防止因集群错误而造成的任何损失。默认值为 `FALSE`，表示导入未禁用。`TRUE` 表示导入已禁用，集群处于只读状态。
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

##### enable_file_bundling

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为云原生表启用 File Bundling 优化功能。当启用该功能（设置为 `true`）时，系统会自动将导入、Compaction 或 Publish 操作生成的数据文件进行打包，从而减少因频繁访问外部存储系统而产生的 API 成本。您还可以通过 CREATE TABLE 语句的 `file_bundling` 属性在表级别控制此行为。有关详细说明，请参阅 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。
- 引入版本：v4.0

##### enable_routine_load_lag_metrics

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否收集 Routine Load Partition Offset Lag 的指标。请注意，将此项目设置为 `true` 会调用 Kafka API 来获取 Partition 的最新 Offset。
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

##### export_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：导出作业调度器的调度间隔。
- 引入版本：-

##### export_max_bytes_per_be_per_task

- 默认值：268435456
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个导出任务在单个 BE 上导出的最大数据量。
- 引入版本：-

##### export_running_job_num_limit

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：导出作业最大的运行数目。
- 引入版本：-

##### export_task_default_timeout_second

- 默认值：2 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导出作业的超时时长。
- 引入版本：-

##### export_task_pool_size

- 默认值：5
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：导出任务线程池的大小。
- 引入版本：-

##### external_table_commit_timeout_ms

- 默认值：10000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：发布写事务到 StarRocks 外表的超时时长，单位为毫秒。默认值 `10000` 表示超时时长为 10 秒。
- 引入版本：-

##### finish_transaction_default_lock_timeout_ms

- 默认值: 1000
- 类型: Int
- 单位: MilliSeconds
- 是否可变: 是
- 描述: 完成事务时获取数据库和表锁的默认超时时间。
- 引入版本: v4.0.0, v3.5.8

##### history_job_keep_max_second

- 默认值：7 * 24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：历史任务最大的保留时长，例如 Schema Change 任务。
- 引入版本：-

##### insert_load_default_timeout_second

- 默认值：3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Insert Into 语句的超时时间。
- 引入版本：-

##### label_clean_interval_second

- 默认值：4 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：作业标签的清理间隔。建议清理间隔尽量短，从而确保历史作业的标签能够及时清理掉。
- 引入版本：-

##### label_keep_max_num

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：一定时间内所保留导入任务的最大数量。超过之后历史导入作业的信息会被删除。
- 引入版本：-

##### label_keep_max_second

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：已经完成、且处于 FINISHED 或 CANCELLED 状态的导入作业记录在 StarRocks 系统 label 的保留时长，默认值为 3 天。该参数配置适用于所有模式的导入作业。设定过大将会消耗大量内存。
- 引入版本：-

##### load_checker_interval_second

- 默认值：5
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：导入作业的轮询间隔。
- 引入版本：-

##### load_straggler_wait_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：控制 BE 副本最大容忍的导入落后时长，超过这个时长就进行克隆。
- 引入版本：-

##### loads_history_retained_days

- 默认值: 30
- 类型: Int
- 单位: 天
- 是否动态：是
- 描述: 在内部表 `_statistics_.loads_history` 中保留导入历史信息的天数。增加或减少此值会调整已完成导入作业在每日分区中保留的时长；它影响新表的创建和 keeper 的修剪行为，但不会自动重建过去的分区。`LoadsHistorySyncer` 在管理导入历史生命周期时依赖此保留策略；其同步节奏由 `loads_history_sync_interval_second` 控制。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### loads_history_sync_interval_second

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否动态：是
- 描述: 由 LoadsHistorySyncer 用于调度周期性同步的时间间隔（秒），用于将已完成的 load 作业从 `information_schema.loads` 同步到内部表 `_statistics_.loads_history`。构造函数中将该值乘以 1000L 以设置 FrontendDaemon 的间隔。syncer 在每次运行时也会重新读取该配置，并在变化时调用 setInterval，因此更新可以在运行时生效而无需重启。syncer 会跳过首次运行（以允许表创建），且仅导入结束时间超过一分钟的 load；较小的频繁值会增加 DML 和 executor 负载，而较大的值会延迟历史 load 记录的可用性。关于目标表的保留/分区行为，请参见 `loads_history_retained_days`。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### max_broker_load_job_concurrency

- 默认值：5
- 别名：async_load_task_pool_size
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：StarRocks 集群中可以并行执行的 Broker Load 作业的最大数量。本参数仅适用于 Broker Load。取值必须小于 `max_running_txn_num_per_db`。从 2.5 版本开始，该参数默认值从 `10` 变为 `5`。
- 引入版本：-

##### max_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导入作业的最大超时时间，适用于所有导入。
- 引入版本：-

##### max_routine_load_batch_size

- 默认值：4294967296
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：每个 Routine Load Task 导入的最大数据量。
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

##### max_running_txn_num_per_db

- 默认值：1000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：StarRocks 集群每个数据库中正在运行的导入相关事务的最大个数，默认值为 `1000`。自 3.1 版本起，默认值由 100 变为 1000。当数据库中正在运行的导入相关事务超过最大个数限制时，后续的导入不会执行。如果是同步的导入作业请求，作业会被拒绝；如果是异步的导入作业请求，作业会在队列中等待。不建议调大该值，会增加系统负载。
- 引入版本：-

##### max_stream_load_timeout_second

- 默认值：259200
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Stream Load 的最大超时时间。
- 引入版本：-

##### max_tolerable_backend_down_num

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许的最大故障 BE 数。如果故障的 BE 节点数超过该阈值，则不能自动恢复 Routine Load 作业。
- 引入版本：-

##### min_bytes_per_broker_scanner

- 默认值：67108864
- 类型：Long
- 单位：Bytes
- 是否动态：是
- 描述：单个 Broker Load 实例处理的最小数据量。
- 引入版本：-

##### min_load_timeout_second

- 默认值：1
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：导入作业的最小超时时间，适用于所有导入。
- 引入版本：-

##### min_routine_load_lag_for_metrics

- 默认值：10000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：要在监控指标中显示的 Routine Load 任务的最小 Offset Lag。Offset Lag 大于此值的 Routine Load 任务将显示在指标中。
- 引入版本：-

##### period_of_auto_resume_min

- 默认值：5
- 类型：Int
- 单位：Minutes
- 是否动态：是
- 描述：自动恢复 Routine Load 的时间间隔。
- 引入版本：-

##### prepared_transaction_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：预提交事务的默认超时时间。
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

##### routine_load_unstable_threshold_second

- 默认值：3600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：Routine Load 导入作业的任一导入任务消费延迟，即正在消费的消息时间戳与当前时间的差值超过该阈值，且数据源中存在未被消费的消息，则导入作业置为 UNSTABLE 状态。
- 引入版本：-

##### spark_dpp_version

- 默认值：1.0.0
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark DPP 特性的版本。
- 引入版本：-

##### spark_home_default_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 客户端根目录。
- 引入版本：-

##### spark_launcher_log_dir

- 默认值：sys_log_dir + "/spark_launcher_log"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 日志的保存目录。
- 引入版本：-

##### spark_load_default_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Spark 导入的超时时间。
- 引入版本：-

##### spark_resource_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Spark 依赖包的根目录。
- 引入版本：-

##### stream_load_default_timeout_second

- 默认值：600
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Stream Load 的默认超时时间。
- 引入版本：-

##### stream_load_task_keep_max_second

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否动态：是
- 描述: 已完成或已取消的 StreamLoad 任务的保留窗口（秒）。当任务达到最终状态且其结束时间戳早于此阈值时（代码以毫秒比较：currentMs - endTimeMs > stream_load_task_keep_max_second * 1000），该任务将有资格被 `StreamLoadMgr.cleanOldStreamLoadTasks` 移除，并在加载持久化状态时被丢弃。适用于 `StreamLoadTask` 和 `StreamLoadMultiStmtTask`。如果总任务数超过 `stream_load_task_keep_max_num`，可能会提前触发清理（同步任务由 `cleanSyncStreamLoadTasks` 优先处理）。请根据历史记录/可调试性与内存使用之间的权衡设置此值。
- 引入版本: v3.2.0

##### transaction_clean_interval_second

- 默认值：30
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：已结束事务的清理间隔。建议清理间隔尽量短，从而确保已完成的事务能够及时清理掉。
- 引入版本：-

##### transaction_stream_load_coordinator_cache_capacity

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存储事务标签到coordinator节点映射的缓存容量。
- 引入版本：-

##### transaction_stream_load_coordinator_cache_expire_seconds

- 默认值：900
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：事务标签与coordinator节点映射关系在缓存中的存活时间(TTL)。
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

### 统计信息

### 存储

##### alter_table_timeout_second

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：Schema Change 超时时间。
- 引入版本：-

##### catalog_trash_expire_second

- 默认值：86400
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：删除表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以通过[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 语句恢复。
- 引入版本：-

##### check_consistency_default_timeout_second

- 默认值：600
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：副本一致性检测的超时时间。
- 引入版本：-

##### default_replication_num

- 默认值：3
- 类型：Short
- 单位：-
- 是否动态：是
- 描述：用于配置分区默认的副本数。如果建表时指定了 `replication_num` 属性，则该属性优先生效；如果建表时未指定 `replication_num`，则配置的 `default_replication_num` 生效。建议该参数的取值不要超过集群内 BE 节点数。
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

##### enable_online_optimize_table

- 默认值: `true`
- 类型: Boolean
- 单位: -
- 是否动态：是
- 描述: 控制在创建优化（optimize）任务时 StarRocks 是否使用非阻塞的在线优化路径。当 `enable_online_optimize_table` 为 true 且目标表满足兼容性检查（没有分区/键/排序规范，分发方式不是 `RandomDistributionDesc`，存储类型不是 `COLUMN_WITH_ROW`，启用了副本存储，并且该表不是云原生表或物化视图）时，planner 会创建 `OnlineOptimizeJobV2` 来执行优化而不阻塞写入。如果为 false 或任一兼容性条件不满足，StarRocks 将回退到 `OptimizeJobV2`，在优化期间可能会阻塞写操作。
- 引入版本: `v3.3.3, v3.4.0, v3.5.0`

##### enable_strict_storage_medium_check

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：建表时，是否严格校验存储介质类型。该值为 `true` 时表示在建表时，会严格校验 BE 上的存储介质。比如建表时指定 `storage_medium = HDD`，而 BE 上只配置了 SSD，那么建表失败。该值为 `false` 时则忽略介质匹配，建表成功。
- 引入版本：-

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

##### max_partition_number_per_table

- 默认值：100000
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单个表中最多可以创建的分区数量。
- 引入版本：v3.3.2

##### partition_recycle_retention_period_secs

- 默认值：1800
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：因 INSERT OVERWRITE 或物化视图刷新操作而被删除的分区的元数据保留时间。请注意，此类元数据无法通过执行 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 命令恢复。
- 引入版本：v3.5.9

##### recover_with_empty_tablet

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：在 Tablet 副本丢失/损坏时，是否使用空的 Tablet 代替。这样可以保证在有 Tablet 副本丢失/损坏时，查询依然能被执行（但是由于缺失了数据，结果可能是错误的）。默认为 `false`，表示不进行替代，查询会失败。
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

##### tablet_checker_lock_time_per_cycle_ms

- 默认值：1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Tablet Checker 在释放并重新获取表锁之前，每个周期持有锁的最大时间（毫秒）。小于 100 的值将被视为 100。
- 引入版本：v3.5.9, v4.0.2

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

##### tablet_sched_balance_load_disk_safe_threshold

- 默认值：0.5
- 别名：balance_load_disk_safe_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：判断 BE 磁盘使用率是否均衡的百分比阈值。如果所有 BE 的磁盘使用率低于该值，认为磁盘使用均衡。当有 BE 磁盘使用率超过该阈值时，如果最大和最小 BE 磁盘使用率之差高于 10%，则认为磁盘使用不均衡，会触发 Tablet 重新均衡。
- 引入版本：-

##### tablet_sched_balance_load_score_threshold

- 默认值：0.1
- 别名：balance_load_score_threshold
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：用于判断 BE 负载是否均衡的百分比阈值。如果一个 BE 的负载低于所有 BE 的平均负载，且差值大于该阈值，则认为该 BE 处于低负载状态。相反，如果一个 BE 的负载比平均负载高且差值大于该阈值，则认为该 BE 处于高负载状态。
- 引入版本：-

##### tablet_sched_be_down_tolerate_time_s

- 默认值：900
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：调度器容忍 BE 节点保持不存活状态的最长时间。超时之后该节点上的 Tablet 会被迁移到其他存活的 BE 节点上。
- 引入版本：2.5.7

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

##### tablet_sched_max_balancing_tablets

- 默认值：500
- 别名：max_balancing_tablets
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：正在均衡的 Tablet 数量的最大值。如果正在均衡的 Tablet 数量超过该值，跳过 Tablet 重新均衡。
- 引入版本：-

##### tablet_sched_max_clone_task_timeout_sec

- 默认值：2 * 60 * 60
- 别名：max_clone_task_timeout_sec
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：克隆 Tablet 的最大超时时间。
- 引入版本：-

##### tablet_sched_max_not_being_scheduled_interval_ms

- 默认值：15 * 60 * 1000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：克隆 Tablet 调度时，如果超过该时间一直未被调度，则将该 Tablet 的调度优先级升高，以尽可能优先调度。
- 引入版本：-

##### tablet_sched_max_scheduling_tablets

- 默认值：10000
- 别名：max_scheduling_tablets
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：可同时调度的 Tablet 的数量。如果正在调度的 Tablet 数量超过该值，跳过 Tablet 均衡和修复检查。
- 引入版本：-

##### tablet_sched_min_clone_task_timeout_sec

- 默认值：3 * 60
- 别名：min_clone_task_timeout_sec
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：克隆 Tablet 的最小超时时间。
- 引入版本：-

##### tablet_sched_num_based_balance_threshold_ratio

- 默认值：0.5
- 别名：-
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：做分布均衡时可能会打破磁盘大小均衡，但磁盘间的最大差距不能超过tablet_sched_num_based_balance_threshold_ratio * table_sched_balance_load_score_threshold。 如果集群中存在不断从 A 到 B、从 B 到 A 的克隆，请减小该值。 如果希望tablet分布更加均衡，请调大该值。
- 引入版本：3.1

##### tablet_sched_repair_delay_factor_second

- 默认值：60
- 别名：tablet_repair_delay_factor_second
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：FE 进行副本修复的间隔。
- 引入版本：-

##### tablet_sched_slot_num_per_path

- 默认值：8
- 别名：schedule_slot_num_per_path
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：一个 BE 存储目录能够同时执行 tablet 相关任务的数目。参数别名 `schedule_slot_num_per_path`。从 2.5 版本开始，该参数默认值从 2.4 版本的 `4` 变为 `8`。
- 引入版本：-

##### tablet_sched_storage_cooldown_second

- 默认值：-1
- 别名：storage_cooldown_second
- 类型：Long
- 单位：Seconds
- 是否动态：是
- 描述：从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br />参数别名为 `storage_cooldown_second`。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 `-1`。
- 引入版本：-

##### tablet_stat_update_interval_second

- 默认值：300
- 类型：Int
- 单位：Seconds
- 是否动态：否
- 描述：FE 向每个 BE 请求收集 Tablet 统计信息的时间间隔。
- 引入版本：-

### 存算分离

##### aws_s3_access_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的 Access Key。
- 引入版本：v3.0

##### aws_s3_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本：v3.0

##### aws_s3_external_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于跨 AWS 账户访问 S3 存储空间的外部 ID。
- 引入版本：v3.0

##### aws_s3_iam_role_arn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：有访问 S3 存储空间权限 IAM Role 的 ARN。
- 引入版本：v3.0

##### aws_s3_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于存储数据的 S3 路径。它由 S3 存储桶的名称及其下的子路径（如有）组成，例如，`testbucket/subpath`。
- 引入版本：v3.0

##### aws_s3_region

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：需访问的 S3 存储空间的地区，如 `us-west-2`。
- 引入版本：v3.0

##### aws_s3_secret_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 S3 存储空间的 Secret Key。
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

##### azure_adls2_endpoint

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：Azure Data Lake Storage Gen2 帐户的端点。示例：`https://test.dfs.core.windows.net`。
- 引入版本：v3.4.1

##### azure_adls2_oauth2_client_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于授权 Azure Data Lake Storage Gen2 请求的 Managed Identity 的 Client ID。
- 引入版本：v3.4.4

##### azure_adls2_oauth2_tenant_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于授权 Azure Data Lake Storage Gen2 请求的 Managed Identity 的 Tenant ID。
- 引入版本：v3.4.4

##### azure_adls2_oauth2_use_managed_identity

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 Managed Identity 用于授权 Azure Data Lake Storage Gen2 请求。
- 引入版本：v3.4.4

##### azure_adls2_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于存储数据的 Azure Data Lake Storage Gen2 路径，由文件系统名称和目录名称组成。示例：`testfilesystem/starrocks`。
- 引入版本：v3.4.1

##### azure_adls2_sas_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于授权 Azure Data Lake Storage Gen2 请求的共享访问签名 (SAS)。
- 引入版本：v3.4.1

##### azure_adls2_shared_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于授权 Azure Data Lake Storage Gen2 请求的 Shared Key。
- 引入版本：v3.4.1

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

##### azure_blob_sas_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 Azure Blob Storage 的共享访问签名（SAS）。
- 引入版本：v3.1

##### azure_blob_shared_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：访问 Azure Blob Storage 的 Shared Key。
- 引入版本：v3.1

##### azure_use_native_sdk

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否使用 Native SDK 访问 Azure Blob Storage，从而允许使用 Managed Identity 和 Service Principal 进行身份验证。如果该项设置为 `false`，则只允许使用 Shared Key 和 SAS 令牌进行身份验证。
- 引入版本：v3.4.4

##### cloud_native_hdfs_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本：-

##### cloud_native_meta_port

- 默认值：6090
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：云原生元数据服务监听端口。
- 引入版本：-

##### cloud_native_storage_type

- 默认值：S3
- 类型：String
- 单位：-
- 是否动态：否
- 描述：您使用的存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS 、Azure Blob（自 v3.1.1 起支持）、Azure Data Lake Storage Gen2（自 v3.4.1 起支持）、Google Storage（Native SDK 自 v3.5.1 起支持）以及兼容 S3 协议的对象存储中（例如 AWS S3、阿里云 OSS 以及 MinIO）。有效值：`S3`（默认）、`AZBLOB`、`ADLS2` 和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果您将此项指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的配置项。如果您将此项指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的配置项。如果您将此项指定为 `GS`，则必须添加以 `gcp_gcs` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本：-

##### enable_load_volume_from_conf

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否允许 StarRocks 使用 FE 配置文件中指定的存储相关属性创建默认存储卷。自 v3.4.1 起，默认值由 `true` 变为 `false`。
- 引入版本：v3.1.0

##### gcp_gcs_impersonation_service_account

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：如果使用基于模拟身份的身份验证，需要要模拟的 Service Account。
- 引入版本：v3.5.1

##### gcp_gcs_path

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于存储数据的 Google Storage 路径。它由 Google Storage 存储桶的名称及其下的子路径（如有）组成，例如，`testbucket/subpath`。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_email

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：创建 Service Account 时生成的 JSON 文件中的 Email。示例：`user@hello.iam.gserviceaccount.com`。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_private_key

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：创建 Service Account 时生成的 JSON 文件中的 Private Key。示例：`-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 引入版本：v3.5.1

##### gcp_gcs_service_account_private_key_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：创建 Service Account 时生成的 JSON 文件中的 Private Key ID。
- 引入版本：v3.5.1

##### gcp_gcs_use_compute_engine_service_account

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否使用 Compute Engine 上面绑定的 Service Account。
- 引入版本：v3.5.1

##### hdfs_file_system_expire_seconds

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否动态：是
- 描述: HdfsFsManager 管理的未使用缓存 HDFS/ObjectStore FileSystem 的生存时间（以秒为单位）。FileSystemExpirationChecker（每 60s 运行一次）使用此值调用每个 HdfsFs.isExpired(...)；当过期时，管理器会关闭底层 FileSystem 并将其从缓存中移除。访问器方法（例如 `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`）会更新最后访问时间戳，因此过期基于不活动时间。较低的值可以减少空闲资源占用但会增加重开销；较高的值会更长时间保留句柄，可能会消耗更多资源。
- 引入版本: v3.2.0

##### lake_autovacuum_grace_period_minutes

- 默认值：30
- 类型：Long
- 单位：Minutes
- 是否动态：是
- 描述：存算分离集群下保留历史数据版本的时间范围。此时间范围内的历史数据版本不会被自动清理。您需要将该值设置为大于最大查询时间，以避免正在访问中的数据被删除导致查询失败。自 v3.3.0，v3.2.5 及 v3.1.10 起，默认值由 `5` 变更为 `30`。
- 引入版本：v3.1.0

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

##### lake_autovacuum_stale_partition_threshold

- 默认值：12
- 类型：Long
- 单位：Hours
- 是否动态：是
- 描述：存算分离集群下，如果某个表分区在该阈值范围内没有任何更新操作(导入、删除或 Compaction)，将不再触发该分区的自动垃圾数据清理操作。
- 引入版本：v3.1.0

##### lake_compaction_allow_partial_success

- 默认值: true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：如果该项设置为 `true`，存算分离集群中的 Compaction 操作即使只有其中一个子任务成功，系统也将认为操作成功。
- 引入版本：v3.5.2

##### lake_compaction_disable_ids

- 默认值：""
- 类型：String
- 单位：-
- 是否动态：是
- 描述：禁止存算分离内表 compaction 的 table 或 partition id 名单。格式为 `tableId1;partitionId2`，id 之间用分号隔开，例如 `12345;98765`。
- 引入版本：v3.4.4

##### lake_compaction_history_size

- 默认值：20
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下在 Leader FE 节点内存中保留多少条最近成功的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 引入版本：v3.1.0

##### lake_compaction_max_tasks

- 默认值：-1
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下允许同时执行的 Compaction 任务数。系统依据分区中 Tablet 数量来计算 Compaction 任务数。如果一个分区有 10 个 Tablet，那么对该分区作一次 Compaction 就会创建 10 个 Compaction 任务。如果正在执行中的 Compaction 任务数超过该阈值，系统将不会创建新的 Compaction 任务。将该值设置为 `0` 表示禁止 Compaction，设置为 `-1` 表示系统依据自适应策略自动计算该值。
- 引入版本：v3.1.0

##### lake_compaction_score_selector_min_score

- 默认值：10.0
- 类型：Double
- 单位：-
- 是否动态：是
- 描述：存算分离集群下，触发 Compaction 操作的 Compaction Score 阈值。当一个表分区的 Compaction Score 大于或等于该值时，系统会对该分区执行 Compaction 操作。
- 引入版本：v3.1.0

Compaction Score 代表了一个表分区是否值得进行 Compaction 的评分，您可以通过 [SHOW PARTITIONS](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_PARTITIONS.md) 语句返回中的 `MaxCS` 一列的值来查看某个分区的 Compaction Score。Compaction Score 和分区中的文件数量有关系。文件数量过多将影响查询性能，因此系统后台会定期执行 Compaction 操作来合并小文件，减少文件数量。

##### lake_compaction_score_upper_bound

- 默认值：2000
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：表分区的 Compaction Score 的上限, `0` 表示没有上限。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。当表分区 Compaction Score 达到或超过该上限后，新的导入会被拒绝。自 v3.3.6 起，默认值由 `0` 变为 `2000`。
- 引入版本：v3.2.0

##### lake_enable_balance_tablets_between_workers

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否在存算分离集群内表的 Tablet 调度过程中平衡 CN 节点之间的 Tablet 数量。`true` 表示启用平衡 Tablet 数量，`false` 表示禁用此功能。
- 引入版本：v3.3.4

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

##### lake_publish_version_max_threads

- 默认值：512
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：存算分离集群下发送生效版本（Publish Version）任务的最大线程数。
- 引入版本：v3.2.0

##### meta_sync_force_delete_shard_meta

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许直接删除存算分离集群元数据，不清理远程存储上对应的数据。建议只在存算分离集群中待清理 Shard 数量过多，导致 FE 节点 JVM 内存压力过大的情况下将此项设为 `true`。注意，开启此功能会导致元数据被清理的 Shard 对应在远程存储上的数据文件无法被自动清理。
- 引入版本：v3.2.10, v3.3.3

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

##### shard_group_clean_threshold_sec

- 默认值：3600
- 类型：Long
- 单位：秒
- 是否动态：是
- 描述：存算分离集群中，FE 清理未使用的 Tablet 和 Shard Group 的时间阈值。在此时间阈值内创建的 Tablet 和 Shard Group 不会被自动清理。
- 引入版本：-

##### star_mgr_meta_sync_interval_sec

- 默认值：600
- 类型：Long
- 单位：秒
- 是否动态：否
- 描述：存算分离集群下 FE 与 StarMgr 定期同步元数据的时间间隔。
- 引入版本：-

### 数据湖

##### lake_batch_publish_min_version_num

- 默认值: `1`
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 设置形成 lake 表发布批次所需的连续事务版本的最小数量。DatabaseTransactionMgr.getReadyToPublishTxnListBatch 将此值与 `lake_batch_publish_max_version_num` 一起传递给 transactionGraph.getTxnsWithTxnDependencyBatch 用于选择依赖事务。值为 `1` 允许单事务发布（不进行批处理）。值 >1 要求至少有该数量的连续版本、单表、非复制事务可用；如果版本不连续、出现复制事务，或架构变更消耗了某个版本，则中止批处理。增大此值可以通过将提交分组来提高发布吞吐，但在等待足够数量的连续事务时可能会延迟发布。
- 引入版本: v3.2.0

##### lake_enable_batch_publish_version

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：当启用时，PublishVersionDaemon 会在存算分离模式（RunMode 为 shared_data）下，将同一Lake表/分区的待发布事务批量收集并一次性发布版本，而不是逐事务发布。它通过调用 `getReadyPublishTransactionsBatch()` 并使用 `publishVersionForLakeTableBatch(...)` 执行分组发布，以减少 RPC 次数并提升吞吐。如果关闭，则退回到 `publishVersionForLakeTable(...)` 的逐事务发布。实现内部使用集合协调在途任务以避免开关切换时的重复发布，线程池规模受 `lake_publish_version_max_threads` 影响。
- 引入版本：v3.2.0

##### lake_use_combined_txn_log

- 默认值: `false`
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 启用时，StarRocks 允许 Lake 表对相关事务使用 combined transaction log 路径。仅在集群以 shared-data 模式运行（RunMode.isSharedDataMode()）时此标志才会被考虑。设置 `lake_use_combined_txn_log = true` 时，类型为 BACKEND_STREAMING、ROUTINE_LOAD_TASK、INSERT_STREAMING 和 BATCH_LOAD_JOB 的加载事务将有资格使用 combined txn logs（参见 LakeTableHelper.supportCombinedTxnLog）。包含 compaction 的代码路径通过 isTransactionSupportCombinedTxnLog 检查 combined-log 支持。如果禁用或不在 shared-data 模式下，则不使用 combined transaction log 行为。
- 引入版本: `v3.3.7, v3.4.0, v3.5.0`

### 其他

##### agent_task_resend_wait_time_ms

- 默认值：5000
- 类型：Long
- 单位：Milliseconds
- 是否动态：是
- 描述：Agent task 重新发送前的等待时间。当代理任务的创建时间已设置，并且距离现在超过该值，才能重新发送代理任务。该参数用于防止过于频繁的代理任务发送。
- 引入版本：-

##### allow_system_reserved_names

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否允许用户创建以 `__op` 或 `__row` 开头命名的列。TRUE 表示启用此功能。请注意，在 StarRocks 中，这样的列名被保留用于特殊目的，创建这样的列可能导致未知行为，因此系统默认禁止使用这类名字。
- 引入版本：v3.2.0

##### auth_token

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于内部身份验证的集群令牌。为空则在 Leader FE 第一次启动时随机生成一个。
- 引入版本：-

##### authentication_ldap_simple_bind_base_dn

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：是
- 描述：检索用户时，使用的 Base DN，用于指定 LDAP 服务器检索用户鉴权信息的起始点。
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

##### authentication_ldap_simple_user_search_attr

- 默认值：uid
- 类型：String
- 单位：-
- 是否动态：是
- 描述：LDAP 对象中标识用户的属性名称。
- 引入版本：-

##### backup_job_default_timeout_ms

- 默认值：86400 * 1000
- 类型：Int
- 单位：Milliseconds
- 是否动态：是
- 描述：Backup 作业的超时时间。
- 引入版本：-

##### enable_collect_tablet_num_in_show_proc_backend_disk_path

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否在 `SHOW PROC /BACKENDS/{id}` 命令中启用对每个磁盘的 tablet 数量的采集
- 引入版本: v4.0.1, v3.5.8

##### enable_colocate_restore

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：是否为 Colocate 表启用备份恢复。`true` 表示启用 Colocate 表备份恢复，`false` 表示禁用。
- 引入版本：v3.2.10、v3.3.3

##### enable_metric_calculator

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否动态：否
- 描述：是否开启定期收集指标 (Metrics) 的功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 引入版本：-

##### enable_mv_post_image_reload_cache

- 默认值：true
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：FE 加载镜像后是否进行重载标志检测。如果某个 Base MV 已完成重载，其他依赖它的 MV 则无需再次重载。
- 引入版本：v3.5.0

##### enable_mv_query_context_cache

- 默认值：true
- 类型：Boolean
- Unit:
- 是否动态：是
- 描述：是否开启Query级别的用于透明加速改写的物化视图Cache，用于加速改写性能。
- 引入版本： v3.3

##### enable_mv_refresh_collect_profile

- 默认值：false
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：是否为所有物化视图默认启用刷新时的 Profile 信息收集。
- 引入版本：v3.3.0

##### enable_mv_refresh_extra_prefix_logging

- 默认值：true
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：是否启用附加物化视图名称前缀的日志记录，用于提升调试能力。
- 引入版本：v3.4.0

##### enable_mv_refresh_query_rewrite

- 默认值：false
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：是否开启物化视图刷新时的查询改写功能，从而可以使用重写后的物化视图代替原始基表以提升性能。
- 引入版本：v3.3.0

##### enable_trace_historical_node

- 默认值：false
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：是否允许系统跟踪历史节点。将此项设置为 `true`，就可以启用 Cache Sharing 功能，并允许系统在弹性扩展过程中选择正确的缓存节点。
- 引入版本：v3.5.1

##### es_state_sync_interval_second

- 默认值：10
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：FE 获取 Elasticsearch Index 和同步 StarRocks 外部表元数据的时间间隔。
- 引入版本：-

##### hive_meta_cache_refresh_interval_s

- 默认值：3600 * 2
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：刷新 Hive 外表元数据缓存的时间间隔。
- 引入版本：-

##### hive_meta_store_timeout_s

- 默认值：10
- 类型：Long
- 单位：Seconds
- 是否动态：否
- 描述：连接 Hive Metastore 的超时时间。
- 引入版本：-

##### jdbc_connection_idle_timeout_ms

- 默认值：600000
- 类型：Int
- 单位：Milliseconds
- 是否动态：否
- 描述：访问 JDBC Catalog 时，连接建立的超时时长。超过参数取值时间的连接被认为是 idle 状态。
- 引入版本：-

##### jdbc_connection_pool_size

- 默认值：8
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 的容量上限。
- 引入版本：-

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

##### jdbc_minimum_idle_connections

- 默认值：1
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：访问 JDBC Catalog 时，JDBC Connection Pool 中处于 idle 状态的连接最低数量。
- 引入版本：-

##### jwt_jwks_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下公钥本地文件的路径。
- 引入版本：v3.5.0

##### jwt_principal_field

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本：v3.5.0

##### jwt_required_audience

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### jwt_required_issuer

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### locale

- 默认值：zh_CN.UTF-8
- 类型：String
- 单位：-
- 是否动态：否
- 描述：FE 所使用的字符集。
- 引入版本：-

##### max_agent_task_threads_num

- 默认值：4096
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：代理任务线程池中用于处理代理任务的最大线程数。
- 引入版本：-

##### max_download_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 RESTORE 操作下，系统向单个 BE 节点下发的最大下载任务数。设置为小于或等于 0 时表示不限制任务数。
- 引入版本：v3.1.0

##### max_mv_check_base_table_change_retry_times

- 默认值：10
- 类型：整数
- 单位：-
- 是否动态：是
- 描述：刷新物化视图时，检测基表变更的最大重试次数。
- 引入版本：v3.3.0

##### max_mv_refresh_failure_retry_times

- 默认值：1
- 类型：整数
- 单位：-
- 是否动态：是
- 描述：物化视图刷新失败时的最大重试次数。
- 引入版本：v3.3.0

##### max_mv_refresh_try_lock_failure_retry_times

- 默认值：3
- 类型：整数
- 单位：-
- 是否动态：是
- 描述：物化视图刷新过程中尝试加锁失败时的最大重试次数。
- 引入版本：v3.3.0

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

##### max_upload_task_per_be

- 默认值：0
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：单次 BACKUP 操作下，系统向单个 BE 节点下发的最大上传任务数。设置为小于或等于 0 时表示不限制任务数。
- 引入版本：v3.1.0

##### mv_create_partition_batch_interval_ms

- 默认值：1000
- 类型：布尔值
- 单位：ms
- 是否动态：是
- 描述：在物化视图刷新时，如果需要一次性创建多个分区，系统将每 64 个分区划分为一个批次进行创建。为了降低因频繁创建导致失败的风险，系统在不同批次之间设置了默认的间隔时间（单位为毫秒），用于控制创建频率。
- 引入版本：v3.3.0

##### mv_plan_cache_max_size

- 默认值：1000
- 类型：Long
- Unit:
- 是否动态：是
- 描述：用于物化视图改写的 MV 计划缓存的最大大小。如果存在大量用于透明改写的物化视图，可以考虑增加此配置项的大小。默认1000个。
- 引入版本： v3.2

##### mv_plan_cache_thread_pool_size

- 默认值：3
- 类型：Int
- Unit:
- 是否动态：是
- 描述：用于物化视图改写的 MV 计划缓存的默认线程池大小，默认3个。
- 引入版本： v3.2

##### mv_refresh_default_planner_optimize_timeout

- 默认值：30000
- 类型：整数
- 单位：ms
- 是否动态：是
- 描述：刷新物化视图时优化器规划阶段的默认超时时间（单位为毫秒），默认 30 秒。
- 引入版本：v3.3.0

##### mv_refresh_fail_on_filter_data

- 默认值：true
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：当物化视图刷新过程中存在被过滤的数据时，刷新会失败（默认值为 true）。如果设置为 false，则会忽略被过滤的数据并返回刷新成功。
- 引入版本：-

##### mv_refresh_try_lock_timeout_ms

- 默认值：30000
- 类型：整数
- 单位：ms
- 是否动态：是
- 描述：物化视图刷新尝试获取基表或物化视图数据库锁的默认超时时间（单位为毫秒）。
- 引入版本：v3.3.0

##### oauth2_auth_server_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：授权 URL。用户浏览器将被重定向到此 URL，以开始 OAuth 2.0 授权过程。
- 引入版本：v3.5.0

##### oauth2_client_id

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：StarRocks 客户端的公共标识符。
- 引入版本：v3.5.0

##### oauth2_client_secret

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于授权 StarRocks 客户端与授权服务器通信的密钥。
- 引入版本：v3.5.0

##### oauth2_jwks_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
- 引入版本：v3.5.0

##### oauth2_principal_field

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中表示主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本：v3.5.0

##### oauth2_redirect_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：OAuth 2.0 认证成功后，用户浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- 引入版本：v3.5.0

##### oauth2_required_audience

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### oauth2_required_issuer

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。
- 引入版本：v3.5.0

##### oauth2_token_server_url

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否动态：否
- 描述：授权服务器上用于获取访问令牌的端点 URL。
- 引入版本：v3.5.0

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

##### query_detail_explain_level

- 默认值：COSTS
- 类型：String
- 单位：-
- 是否动态：是
- 描述：EXPLAIN 语句返回的查询计划的解释级别。有效值：COSTS、NORMAL、VERBOSE。
- 引入版本：v3.2.12，v3.3.5

##### replication_interval_ms

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：否
- 描述：调度执行同步任务的最小时间间隔。
- 引入版本：v3.3.5

##### replication_max_parallel_data_size_mb

- 默认值：1048576
- 类型：Int
- 单位：MB
- 是否动态：是
- 描述：允许并发同步的数据量。
- 引入版本：v3.3.5

##### replication_max_parallel_replica_count

- 默认值：10240
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许并发同步的 Tablet 副本数。
- 引入版本：v3.3.5

##### replication_max_parallel_table_count

- 默认值：100
- 类型：Int
- 单位：-
- 是否动态：是
- 描述：允许并发执行的数据同步任务数。StarRocks 为一张表创建一个同步任务。
- 引入版本：v3.3.5

##### replication_transaction_timeout_sec

- 默认值：86400
- 类型：Int
- 单位：Seconds
- 是否动态：是
- 描述：同步任务的超时时间。
- 引入版本：v3.3.5

##### small_file_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：小文件的根目录。
- 引入版本：-

##### task_runs_max_history_number

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 内存中保留的任务运行记录的最大数量，并在查询归档的 task-run 历史时用作默认的 LIMIT。当 `enable_task_history_archive` 为 false 时，此值约束内存中的历史：Force GC 会修剪较旧的条目，使得只保留最新的 `task_runs_max_history_number` 条记录。当查询归档历史（且未提供显式 LIMIT）时，如果该值大于 0，`TaskRunHistoryTable.lookup` 会使用 `"ORDER BY create_time DESC LIMIT <value>"`。注意：将此值设置为 0 会禁用查询侧的 LIMIT（无限制），但会导致内存中的历史被截断为零（除非启用了归档）。
- 引入版本: v3.2.0

##### tmp_dir

- 默认值：StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- 类型：String
- 单位：-
- 是否动态：否
- 描述：临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br />这些过程完成以后，所产生的临时文件会被清除掉。
- 引入版本：-

##### transform_type_prefer_string_for_varchar

- 默认值：true
- 类型：布尔值
- 单位：-
- 是否动态：是
- 描述：在物化视图创建和 CTAS 操作中，是否优先对固定长度的 VARCHAR 列使用 STRING 类型。
- 引入版本：v4.0.0




<EditionSpecificFEItem />
