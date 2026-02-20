---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 配置

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令配置或修改 FE 动态参数。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

## 理解 FE 参数

### 日志

##### `audit_log_delete_age`

- 默认值: 30d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 审计日志文件的保留期限。默认值 `30d` 指定每个审计日志文件可以保留 30 天。StarRocks 会检查每个审计日志文件，并删除 30 天前生成的那些。
- 引入版本: -

##### `audit_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储审计日志文件的目录。
- 引入版本: -

##### `audit_log_enable_compress`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: No
- 描述: 当为 true 时，生成的 Log4j2 配置会将 ".gz" 后缀附加到轮转的审计日志文件名 (fe.audit.log.*) 中，以便 Log4j2 在轮转时生成压缩的 (.gz) 归档审计日志文件。此设置在 FE 启动期间在 Log4jConfig.initLogging 中读取，并应用于审计日志的 RollingFile appender；它仅影响轮转/归档文件，而不影响活动审计日志。由于该值在启动时初始化，因此更改它需要重启 FE 才能生效。与审计日志轮转设置 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) 一起使用。
- 引入版本: 3.2.12

##### `audit_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: Yes
- 描述: 当为 true 时，FE 审计事件将以结构化 JSON (Jackson ObjectMapper 序列化带注解的 AuditEvent 字段的 Map) 的形式发出，而不是默认的管道分隔的 "key=value" 字符串。此设置会影响 AuditLogBuilder 处理的所有内置审计接收器：连接审计、查询审计、大查询审计（当事件符合条件时，大查询阈值字段会添加到 JSON 中）和慢审计输出。用于大查询阈值的字段和 "features" 字段会进行特殊处理（从普通审计条目中排除；根据适用情况包含在大查询或功能日志中）。启用此功能可使日志可供日志收集器或 SIEM 机器解析；请注意，它会更改日志格式，可能需要更新任何期望旧版管道分隔格式的现有解析器。
- 引入版本: 3.2.7

##### `audit_log_modules`

- 默认值: `slow_query`, query
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成审计日志条目的模块。默认情况下，StarRocks 为 `slow_query` 模块和 `query` 模块生成审计日志。`connection` 模块从 v3.0 版本开始支持。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

##### `audit_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转审计日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在审计日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在审计日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `audit_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `audit_log_roll_interval` 参数指定的每个保留期内，可以保留的审计日志文件的最大数量。
- 引入版本: -

##### `bdbje_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 StarRocks 中 Berkeley DB Java Edition (BDB JE) 使用的日志级别。在 BDB 环境初始化 BDBEnvironment.initConfigs() 期间，此值将应用于 `com.sleepycat.je` 包的 Java 日志记录器和 BDB JE 环境文件日志记录级别 (`EnvironmentConfig.FILE_LOGGING_LEVEL`)。接受标准的 java.util.logging.Level 名称，例如 SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF。设置为 ALL 可启用所有日志消息。增加详细程度将提高日志量，并可能影响磁盘 I/O 和性能；该值在 BDB 环境初始化时读取，因此仅在环境（重新）初始化后生效。
- 引入版本: v3.2.0

##### `big_query_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE 大查询日志文件 (`fe.big_query.log.*`) 在自动删除前的保留时间。该值作为 IfLastModified age 传递给 Log4j 的删除策略 — 任何最后修改时间早于此值的轮转大查询日志都将被删除。支持的后缀包括 `d`（天）、`h`（小时）、`m`（分钟）和 `s`（秒）。示例：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）和 `120s`（120 秒）。此项与 `big_query_log_roll_interval` 和 `big_query_log_roll_num` 共同决定哪些文件被保留或清除。
- 引入版本: v3.2.0

##### `big_query_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 写入大查询 dump 日志 (`fe.big_query.log.*`) 的目录。Log4j 配置使用此路径为 `fe.big_query.log` 及其轮转文件创建 RollingFile appender。轮转和保留由 `big_query_log_roll_interval`（基于时间的后缀）、`log_roll_size_mb`（大小触发器）、`big_query_log_roll_num`（最大文件数）和 `big_query_log_delete_age`（基于年龄的删除）控制。对于超过用户定义阈值（例如 `big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold` 或 `big_query_log_scan_bytes_threshold`）的查询，会记录大查询记录。使用 `big_query_log_modules` 控制哪些模块记录到此文件中。
- 引入版本: v3.2.0

##### `big_query_log_modules`

- 默认值: `{"query"}`
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 启用每个模块大查询日志记录的模块名称后缀列表。典型值是逻辑组件名称。例如，默认的 `query` 会生成 `big_query.query`。
- 引入版本: v3.2.0

##### `big_query_log_roll_interval`

- 默认值: `"DAY"`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 指定用于构建 `big_query` 日志 appender 滚动文件名称日期部分的时间间隔。有效值（不区分大小写）为 `DAY`（默认）和 `HOUR`。`DAY` 生成每日模式 (`"%d{yyyyMMdd}"`)，`HOUR` 生成每小时模式 (`"%d{yyyyMMddHH}"`)。该值与基于大小的轮转 (`big_query_roll_maxsize`) 和基于索引的轮转 (`big_query_log_roll_num`) 结合，形成 RollingFile filePattern。无效值会导致日志配置生成失败 (IOException)，并可能阻止日志初始化或重新配置。与 `big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num` 和 `big_query_log_delete_age` 一起使用。
- 引入版本: v3.2.0

##### `big_query_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 每个 `big_query_log_roll_interval` 保留的轮转 FE 大查询日志文件的最大数量。此值绑定到 RollingFile appender 的 DefaultRolloverStrategy `max` 属性，用于 `fe.big_query.log`；当日志轮转时（按时间或按 `log_roll_size_mb`），StarRocks 最多保留 `big_query_log_roll_num` 个索引文件（filePattern 使用时间后缀加索引）。超过此数量的文件可能会被轮转删除，`big_query_log_delete_age` 还可以根据最后修改时间删除文件。
- 引入版本: v3.2.0

##### `dump_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: dump 日志文件的保留期限。默认值 `7d` 指定每个 dump 日志文件可以保留 7 天。StarRocks 会检查每个 dump 日志文件，并删除 7 天前生成的那些。
- 引入版本: -

##### `dump_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 dump 日志文件的目录。
- 引入版本: -

##### `dump_log_modules`

- 默认值: query
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成 dump 日志条目的模块。默认情况下，StarRocks 为 query 模块生成 dump 日志。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

##### `dump_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转 dump 日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在 dump 日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在 dump 日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `dump_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `dump_log_roll_interval` 参数指定的每个保留期内，可以保留的 dump 日志文件的最大数量。
- 引入版本: -

##### `edit_log_write_slow_log_threshold_ms`

- 默认值: 2000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: JournalWriter 用于检测和记录慢速 edit-log 批量写入的阈值（单位为毫秒）。批量提交后，如果批量持续时间超过此值，JournalWriter 将发出 WARN 日志，其中包含批量大小、持续时间和当前 Journal 队列大小（以每约 2 秒一次的速率限制）。此设置仅控制 FE Leader 上潜在 IO 或复制延迟的日志记录/警报；它不改变提交或轮转行为（请参阅 `edit_log_roll_num` 和与提交相关的设置）。无论此阈值如何，指标更新仍会发生。
- 引入版本: v3.2.3

##### `enable_audit_sql`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，FE 审计子系统会将 ConnectProcessor 处理的 SQL 语句文本记录到 FE 审计日志 (`fe.audit.log`) 中。存储的语句遵循其他控制：加密语句将被 redacted (`AuditEncryptionChecker`)，如果设置了 `enable_sql_desensitize_in_log`，敏感凭据可能会被 redacted 或脱敏，并且 digest 记录由 `enable_sql_digest` 控制。当设置为 `false` 时，ConnectProcessor 会在审计事件中将语句文本替换为 "?" — 其他审计字段（用户、主机、持续时间、状态、通过 `qe_slow_log_ms` 进行的慢查询检测以及指标）仍会记录。启用 SQL 审计会增加取证和故障排除的可见性，但可能会暴露敏感的 SQL 内容并增加日志量和 I/O；禁用它会提高隐私性，但代价是审计日志中会丢失完整的语句可见性。
- 引入版本: -

##### `enable_profile_log`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否启用 profile 日志。启用此功能后，FE 会将每个查询的 profile 日志（由 `ProfileManager` 生成的序列化 `queryDetail` JSON）写入 profile 日志接收器。此日志记录仅在 `enable_collect_query_detail_info` 也启用时执行；当 `enable_profile_log_compress` 启用时，JSON 可能会在日志记录前进行 gzip 压缩。Profile 日志文件由 `profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` 管理，并根据 `profile_log_delete_age` 进行轮转/删除（支持 `7d`、`10h`、`60m`、`120s` 等格式）。禁用此功能会停止写入 profile 日志（减少磁盘 I/O、压缩 CPU 和存储使用）。
- 引入版本: v3.2.5

##### `enable_qe_slow_log`

- 默认值: true
- 类型: Boolean
- 单位: N/A
- 是否可变: Yes
- 描述: 当启用时，FE 内置审计插件 (AuditLogBuilder) 将把其测量执行时间（"Time" 字段）超过 `qe_slow_log_ms` 配置阈值的查询事件写入慢查询审计日志 (AuditLog.getSlowAudit)。如果禁用，这些慢查询条目将被抑制（常规查询和连接审计日志不受影响）。慢审计条目遵循全局 `audit_log_json_format` 设置（JSON 与纯字符串）。使用此标志可以独立于常规审计日志记录控制慢查询审计生成量；当 `qe_slow_log_ms` 较低或工作负载产生许多长时间运行的查询时，关闭它可能会减少日志 I/O。
- 引入版本: 3.2.11

##### `enable_sql_desensitize_in_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会在将敏感 SQL 内容写入日志和查询详细记录之前替换或隐藏这些内容。遵循此配置的代码路径包括 ConnectProcessor.formatStmt（审计日志）、StmtExecutor.addRunningQueryDetail（查询详细信息）和 SimpleExecutor.formatSQL（内部执行器日志）。启用此功能后，无效的 SQL 可能会被替换为固定的脱敏消息，凭据（用户/密码）将被隐藏，并且 SQL 格式化程序必须生成 sanitized 表示（它还可以启用摘要式输出）。这减少了审计/内部日志中敏感文字和凭据的泄露，但也意味着日志和查询详细信息不再包含原始完整 SQL 文本（这可能会影响回放或调试）。
- 引入版本: -

##### `internal_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 指定 FE 内部日志文件（写入 `internal_log_dir`）的保留期限。该值是一个持续时间字符串。支持的后缀：`d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。示例：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。此项作为 `<IfLastModified age="..."/>` 谓词替换到 Log4j 配置中，该谓词由 RollingFile Delete 策略使用。最后修改时间早于此持续时间的文件将在日志轮转期间删除。增加此值可更快释放磁盘空间，或减少此值可更长时间保留内部物化视图或统计信息日志。
- 引入版本: v3.2.4

##### `internal_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 日志记录子系统用于存储内部日志 (`fe.internal.log`) 的目录。此配置将替换到 Log4j 配置中，并决定 InternalFile appender 在何处写入内部/物化视图/统计信息日志，以及 `internal.<module>` 下的每个模块日志记录器在何处放置其文件。确保目录存在、可写并具有足够的磁盘空间。此目录中文件的日志轮转和保留由 `log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age` 和 `internal_log_roll_interval` 控制。如果 `sys_log_to_console` 启用，内部日志可能会写入控制台而不是此目录。
- 引入版本: v3.2.4

##### `internal_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，内部统计/审计条目将作为紧凑的 JSON 对象写入统计审计日志记录器。JSON 包含键 "executeType" (InternalType: QUERY 或 DML)、"queryId"、"sql" 和 "time"（已用毫秒）。当设置为 `false` 时，相同的信息将记录为单个格式化文本行（"statistic execute: ... | QueryId: [...] | SQL: ..."）。启用 JSON 可改进机器解析并与日志处理器集成，但也会导致原始 SQL 文本包含在日志中，这可能会暴露敏感信息并增加日志大小。
- 引入版本: -

##### `internal_log_modules`

- 默认值: `{"base", "statistic"}`
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 将接收专用内部日志记录的模块标识符列表。对于每个条目 X，Log4j 将创建一个名为 `internal.<X>` 的日志记录器，其级别为 INFO，并且 additivity="false"。这些日志记录器被路由到内部 appender（写入 `fe.internal.log`），或者在 `sys_log_to_console` 启用时路由到控制台。根据需要使用短名称或包片段 — 确切的日志记录器名称变为 `internal.` + 配置的字符串。内部日志文件轮转和保留遵循 `internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval` 和 `log_roll_size_mb`。添加模块会导致其运行时消息分离到内部日志记录器流中，以便于调试和审计。
- 引入版本: v3.2.4

##### `internal_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE 内部日志 appender 的基于时间的滚动间隔。接受的值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成每小时的文件模式 (`"%d{yyyyMMddHH}"`)，`DAY` 生成每日文件模式 (`"%d{yyyyMMdd}"`)，这些模式由 RollingFile TimeBasedTriggeringPolicy 用于命名轮转的 `fe.internal.log` 文件。无效值会导致初始化失败（构建活动 Log4j 配置时抛出 IOException）。滚动行为还取决于相关设置，例如 `internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num` 和 `internal_log_delete_age`。
- 引入版本: v3.2.4

##### `internal_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 为内部 appender (`fe.internal.log`) 保留的轮转 FE 内部日志文件的最大数量。此值用作 Log4j DefaultRolloverStrategy `max` 属性；当发生轮转时，StarRocks 最多保留 `internal_log_roll_num` 个归档文件并删除旧文件（也受 `internal_log_delete_age` 控制）。较低的值会减少磁盘使用，但会缩短日志历史记录；较高的值会保留更多的历史内部日志。此项与 `internal_log_dir`、`internal_log_roll_interval` 和 `internal_roll_maxsize` 协同工作。
- 引入版本: v3.2.4

##### `log_cleaner_audit_log_min_retention_days`

- 默认值: 3
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 审计日志文件的最小保留天数。早于此时间的审计日志文件即使磁盘使用率很高也不会被删除。这确保了审计日志为合规性和故障排除目的而保留。
- 引入版本: -

##### `log_cleaner_check_interval_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 检查磁盘使用情况和清理日志的间隔（秒）。清理器会定期检查每个日志目录的磁盘使用情况，并在必要时触发清理。默认值为 300 秒（5 分钟）。
- 引入版本: -

##### `log_cleaner_disk_usage_target`

- 默认值: 60
- 类型: Int
- 单位: 百分比
- 是否可变: Yes
- 描述: 日志清理后的目标磁盘使用率（百分比）。日志清理将持续进行，直到磁盘使用率降至此阈值以下。清理器会逐个删除最旧的日志文件，直到达到目标。
- 引入版本: -

##### `log_cleaner_disk_usage_threshold`

- 默认值: 80
- 类型: Int
- 单位: 百分比
- 是否可变: Yes
- 描述: 触发日志清理的磁盘使用率阈值（百分比）。当磁盘使用率超过此阈值时，日志清理将开始。清理器会独立检查每个配置的日志目录，并处理超过此阈值的目录。
- 引入版本: -

##### `log_cleaner_disk_util_based_enable`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用基于磁盘使用率的自动日志清理。启用后，当磁盘使用率超过阈值时，将清理日志。日志清理器作为 FE 节点上的后台守护程序运行，有助于防止日志文件累积导致磁盘空间耗尽。
- 引入版本: -

##### `log_plan_cancelled_by_crash_be`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当查询因 BE 崩溃或 RPC 异常而取消时，是否启用查询执行计划日志记录。启用此功能后，当查询因 BE 崩溃或 `RpcException` 而取消时，StarRocks 会将查询执行计划（`TExplainLevel.COSTS` 级别）记录为 WARN 条目。日志条目包括 QueryId、SQL 和 COSTS 计划；在 ExecuteExceptionHandler 路径中，还会记录异常堆栈跟踪。当 `enable_collect_query_detail_info` 启用时，日志记录会被跳过（计划随后存储在查询详细信息中）—— 在代码路径中，通过验证查询详细信息是否为 null 来执行检查。请注意，在 ExecuteExceptionHandler 中，计划仅在第一次重试时 (`retryTime == 0`) 记录。启用此功能可能会增加日志量，因为完整的 COSTS 计划可能很大。
- 引入版本: v3.2.0

##### `log_register_and_unregister_query_id`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许 FE 记录来自 QeProcessorImpl 的查询注册和注销消息（例如，`"register query id = {}"` 和 `"deregister query id = {}"`）。仅当查询具有非空 ConnectContext 且命令不是 `COM_STMT_EXECUTE` 或会话变量 `isAuditExecuteStmt()` 为 true 时才发出日志。由于这些消息是为每个查询生命周期事件写入的，因此启用此功能可能会产生大量日志并成为高并发环境中的吞吐量瓶颈。启用它用于调试或审计；禁用它以减少日志记录开销并提高性能。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: No
- 描述: 系统日志文件或审计日志文件的最大大小。
- 引入版本: -

##### `proc_profile_file_retained_days`

- 默认值: 1
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 保留 `sys_log_dir/proc_profile` 下生成的进程分析文件（CPU 和内存）的天数。ProcProfileCollector 通过将 `proc_profile_file_retained_days` 天数从当前时间（格式为 yyyyMMdd-HHmmss）中减去来计算截止时间，并删除时间戳部分在字典序上早于该截止时间的分析文件（即 `timePart.compareTo(timeToDelete) < 0`）。文件删除还遵循由 `proc_profile_file_retained_size_bytes` 控制的基于大小的截止时间。分析文件使用 `cpu-profile-` 和 `mem-profile-` 前缀，并在收集后进行压缩。
- 引入版本: v3.2.12

##### `proc_profile_file_retained_size_bytes`

- 默认值: 2L * 1024 * 1024 * 1024 (2147483648)
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 在分析目录下保留的收集到的 CPU 和内存分析文件（文件名前缀为 `cpu-profile-` 和 `mem-profile-`）的最大总字节数。当有效分析文件的总和超过 `proc_profile_file_retained_size_bytes` 时，收集器将删除最旧的分析文件，直到剩余总大小小于或等于 `proc_profile_file_retained_size_bytes`。早于 `proc_profile_file_retained_days` 的文件也将被删除，无论大小如何。此设置控制分析归档的磁盘使用情况，并与 `proc_profile_file_retained_days` 交互以确定删除顺序和保留。
- 引入版本: v3.2.12

##### `profile_log_delete_age`

- 默认值: 1d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE profile 日志文件在符合删除条件之前保留多长时间。该值注入到 Log4j 的 `<IfLastModified age="..."/>` 策略（通过 `Log4jConfig`）中，并与轮转设置（如 `profile_log_roll_interval` 和 `profile_log_roll_num`）一起应用。支持的后缀：`d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。例如：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。
- 引入版本: v3.2.5

##### `profile_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE profile 日志的写入目录。Log4jConfig 使用此值放置与 profile 相关的 appender（在此目录下创建 `fe.profile.log` 和 `fe.features.log` 等文件）。这些文件的轮转和保留由 `profile_log_roll_size_mb`、`profile_log_roll_num` 和 `profile_log_delete_age` 控制；时间戳后缀格式由 `profile_log_roll_interval` 控制（支持 DAY 或 HOUR）。由于默认目录位于 `STARROCKS_HOME_DIR` 下，请确保 FE 进程对此目录具有写入和轮转/删除权限。
- 引入版本: v3.2.5

##### `profile_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制用于生成 profile 日志文件名日期部分的时间粒度。有效值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成模式 `"%d{yyyyMMddHH}"`（每小时时间桶），`DAY` 生成 `"%d{yyyyMMdd}"`（每日时间桶）。此值在 Log4j 配置中计算 `profile_file_pattern` 时使用，并且仅影响轮转文件名称中基于时间的组件；基于大小的轮转仍由 `profile_log_roll_size_mb` 控制，保留由 `profile_log_roll_num` / `profile_log_delete_age` 控制。无效值会导致日志初始化期间发生 IOException（错误消息：`"profile_log_roll_interval config error: <value>"`）。对于高容量 profiling，选择 `HOUR` 以限制每小时的文件大小，或选择 `DAY` 进行每日聚合。
- 引入版本: v3.2.5

##### `profile_log_roll_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 指定 Log4j 的 DefaultRolloverStrategy 为 profile 日志记录器保留的最大轮转 profile 日志文件数。此值作为 `${profile_log_roll_num}` 注入到日志 XML 中（例如 `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`）。轮转由 `profile_log_roll_size_mb` 或 `profile_log_roll_interval` 触发；当发生轮转时，Log4j 最多保留这些索引文件，旧的索引文件将符合删除条件。磁盘上的实际保留也受 `profile_log_delete_age` 和 `profile_log_dir` 位置的影响。较低的值会减少磁盘使用，但会限制保留的历史记录；较高的值会保留更多的历史 profile 日志。
- 引入版本: v3.2.5

##### `profile_log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: No
- 描述: 设置触发 FE profile 日志文件基于大小轮转的大小阈值（以兆字节为单位）。此值由 Log4j RollingFile SizeBasedTriggeringPolicy 用于 `ProfileFile` appender；当 profile 日志超过 `profile_log_roll_size_mb` 时，它将被轮转。当达到 `profile_log_roll_interval` 时，也可以按时间进行轮转——任一条件都会触发轮转。结合 `profile_log_roll_num` 和 `profile_log_delete_age`，此项控制保留多少历史 profile 文件以及何时删除旧文件。轮转文件的压缩由 `enable_profile_log_compress` 控制。
- 引入版本: v3.2.5

##### `qe_slow_log_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用于判断查询是否为慢查询的阈值。如果查询的响应时间超过此阈值，则会在 **fe.audit.log** 中记录为慢查询。
- 引入版本: -

##### `slow_lock_log_every_ms`

- 默认值: 3000L
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在为同一 SlowLockLogStats 实例发出另一个“慢锁”警告之前，等待的最短间隔（以毫秒为单位）。LockUtils 在锁等待超过 `slow_lock_threshold_ms` 后检查此值，并会抑制额外的警告，直到自上次记录慢锁事件以来已过去 `slow_lock_log_every_ms` 毫秒。使用更大的值可在长时间争用期间减少日志量，或使用更小的值以获得更频繁的诊断。更改在运行时对后续检查生效。
- 引入版本: v3.2.0

##### `slow_lock_print_stack`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许 LockManager 在 `logSlowLockTrace` 发出的慢锁警告的 JSON 负载中包含拥有线程的完整堆栈跟踪（"stack" 数组通过 `LogUtil.getStackTraceToJsonArray` 填充，`start=0` 且 `max=Short.MAX_VALUE`）。此配置仅控制当锁获取超过由 `slow_lock_threshold_ms` 配置的阈值时显示的锁所有者的额外堆栈信息。启用此功能通过提供持有锁的精确线程堆栈来帮助调试；禁用它可减少日志量和在高并发环境中捕获和序列化堆栈跟踪导致的 CPU/内存开销。
- 引入版本: v3.3.16, v3.4.5, v3.5.1

##### `slow_lock_threshold_ms`

- 默认值: 3000L
- 类型: long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用于将锁操作或持有的锁分类为“慢”的阈值（以毫秒为单位）。当锁的等待或持有时间超过此值时，StarRocks 将（根据上下文）发出诊断日志，包括堆栈跟踪或等待/所有者信息，并在 LockManager 中在此延迟后开始死锁检测。它由 LockUtils（慢锁日志记录）、QueryableReentrantReadWriteLock（过滤慢速读取器）、LockManager（死锁检测延迟和慢锁跟踪）、LockChecker（周期性慢锁检测）和其他调用者（例如 DiskAndTabletLoadReBalancer 日志记录）使用。降低此值会增加敏感性和日志记录/诊断开销；将其设置为 0 或负值会禁用初始基于等待的死锁检测延迟行为。与 `slow_lock_log_every_ms`、`slow_lock_print_stack` 和 `slow_lock_stack_trace_reserve_levels` 一起调整。
- 引入版本: 3.2.0

##### `sys_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 系统日志文件的保留期限。默认值 `7d` 指定每个系统日志文件可以保留 7 天。StarRocks 会检查每个系统日志文件，并删除 7 天前生成的那些。
- 引入版本: -

##### `sys_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储系统日志文件的目录。
- 引入版本: -

##### `sys_log_enable_compress`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会将 ".gz" 后缀附加到轮转的系统日志文件名中，以便 Log4j 会生成 gzip 压缩的轮转 FE 系统日志（例如，fe.log.*）。此值在 Log4j 配置生成期间（Log4jConfig.initLogging / generateActiveLog4jXmlConfig）读取，并控制 RollingFile filePattern 中使用的 `sys_file_postfix` 属性。启用此功能可减少保留日志的磁盘使用，但会增加轮转期间的 CPU 和 I/O，并更改日志文件名，因此读取日志的工具或脚本必须能够处理 .gz 文件。请注意，审计日志使用单独的压缩配置，即 `audit_log_enable_compress`。
- 引入版本: v3.2.12

##### `sys_log_format`

- 默认值: "plaintext"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 选择 FE 日志使用的 Log4j 布局。有效值：`"plaintext"`（默认）和 `"json"`。值不区分大小写。`"plaintext"` 配置 PatternLayout，具有人类可读的时间戳、级别、线程、类.方法:行，以及 WARN/ERROR 的堆栈跟踪。`"json"` 配置 JsonTemplateLayout 并发出结构化 JSON 事件（UTC 时间戳、级别、线程 ID/名称、源文件/方法/行、消息、异常堆栈跟踪），适用于日志聚合器（ELK、Splunk）。JSON 输出遵循 `sys_log_json_max_string_length` 和 `sys_log_json_profile_max_string_length` 以获取最大字符串长度。
- 引入版本: v3.2.10

##### `sys_log_json_max_string_length`

- 默认值: 1048576
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置用于 JSON 格式系统日志的 JsonTemplateLayout "maxStringLength" 值。当 `sys_log_format` 设置为 `"json"` 时，如果字符串值字段（例如 "message" 和字符串化的异常堆栈跟踪）的长度超过此限制，它们将被截断。该值注入到 `Log4jConfig.generateActiveLog4jXmlConfig()` 中生成的 Log4j XML 中，并应用于默认、警告、审计、dump 和大查询布局。profile 布局使用单独的配置 (`sys_log_json_profile_max_string_length`)。降低此值会减小日志大小，但可能会截断有用信息。
- 引入版本: 3.2.11

##### `sys_log_json_profile_max_string_length`

- 默认值: 104857600 (100 MB)
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 当 `sys_log_format` 为 "json" 时，设置 profile（及相关功能）日志 appender 的 JsonTemplateLayout 的 maxStringLength。JSON 格式 profile 日志中的字符串字段值将被截断为此外字节长度；非字符串字段不受影响。此项应用于 Log4jConfig `JsonTemplateLayout maxStringLength`，并在使用 `plaintext` 日志记录时被忽略。保持足够大的值以获取您需要的完整消息，但请注意，较大的值会增加日志大小和 I/O。
- 引入版本: v3.2.11

##### `sys_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 系统日志条目分类的严重性级别。有效值：`INFO`、`WARN`、`ERROR` 和 `FATAL`。
- 引入版本: -

##### `sys_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转系统日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在系统日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在系统日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `sys_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `sys_log_roll_interval` 参数指定的每个保留期内，可以保留的系统日志文件的最大数量。
- 引入版本: -

##### `sys_log_to_console`

- 默认值: false（除非环境变量 `SYS_LOG_TO_CONSOLE` 设置为 "1"）
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会将 Log4j 配置为将所有日志发送到控制台 (ConsoleErr appender)，而不是基于文件的 appender。此值在生成活动 Log4j XML 配置时读取（这会影响根日志记录器和每个模块日志记录器 appender 的选择）。其值在进程启动时从 `SYS_LOG_TO_CONSOLE` 环境变量中捕获。在运行时更改它无效。此配置通常用于容器化或 CI 环境中，其中 stdout/stderr 日志收集优于写入日志文件。
- 引入版本: v3.2.0

##### `sys_log_verbose_modules`

- 默认值: 空字符串
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成系统日志的模块。如果此参数设置为 `org.apache.starrocks.catalog`，则 StarRocks 仅为 catalog 模块生成系统日志。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

##### `sys_log_warn_modules`

- 默认值: {}
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 启动时系统将配置为 WARN 级别日志记录器并路由到警告 appender (SysWF) — `fe.warn.log` 文件的日志记录器名称或包前缀列表。条目插入到生成的 Log4j 配置中（与内置警告模块如 org.apache.kafka、org.apache.hudi 和 org.apache.hadoop.io.compress 一起），并生成类似 `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` 的日志记录器元素。建议使用完全限定的包和类前缀（例如 "com.example.lib"），以抑制常规日志中的嘈杂 INFO/DEBUG 输出，并允许单独捕获警告。
- 引入版本: v3.2.13

### 服务

##### `brpc_idle_wait_max_time`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: bRPC 客户端在空闲状态下等待的最长时间。
- 引入版本: -

##### `brpc_inner_reuse_pool`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 控制底层 BRPC 客户端是否为连接/通道使用内部共享重用池。StarRocks 在 BrpcProxy 构造 RpcClientOptions 时（通过 `rpcOptions.setInnerResuePool(...)`）读取 `brpc_inner_reuse_pool`。启用时 (true)，RPC 客户端重用内部池以减少每次调用的连接创建，降低 FE 到 BE / LakeService RPC 的连接 churn、内存和文件描述符使用量。禁用时 (false)，客户端可能会创建更隔离的池（以更高的资源使用量为代价增加并发隔离）。更改此值需要重启进程才能生效。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `brpc_min_evictable_idle_time_ms`

- 默认值: 120000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 空闲的 BRPC 连接在连接池中必须保持空闲状态才能被驱逐的时间（毫秒）。应用于 `BrpcProxy` 使用的 RpcClientOptions（通过 RpcClientOptions.setMinEvictableIdleTime）。增加此值以保持空闲连接更长时间（减少重新连接的 churn）；降低此值可更快释放未使用的套接字（减少资源使用）。与 `brpc_connection_pool_size` 和 `brpc_idle_wait_max_time` 一起调整，以平衡连接重用、池增长和驱逐行为。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `brpc_reuse_addr`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，StarRocks 会设置 socket 选项，允许 brpc RpcClient 创建的客户端 socket（通过 RpcClientOptions.setReuseAddress）重用本地地址。启用此选项可减少绑定失败，并允许在套接字关闭后更快地重新绑定本地端口，这对于高速率连接 churn 或快速重启非常有用。当为 false 时，地址/端口重用被禁用，这可以降低意外端口共享的可能性，但可能会增加瞬时绑定错误。此选项与 `brpc_connection_pool_size` 和 `brpc_short_connection` 配置的连接行为交互，因为它会影响客户端套接字可以多快地重新绑定和重用。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `cluster_name`

- 默认值: StarRocks Cluster
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 所属的 StarRocks 集群的名称。集群名称显示在网页的 `Title` 上。
- 引入版本: -

##### `dns_cache_ttl_seconds`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 成功 DNS 查找的 DNS 缓存 TTL（存活时间，Time-To-Live），单位为秒。这设置了 Java 安全属性 `networkaddress.cache.ttl`，它控制 JVM 缓存成功 DNS 查找的时间。将此项设置为 `-1` 以允许系统始终缓存信息，或设置为 `0` 以禁用缓存。这在 IP 地址经常变化的环境中特别有用，例如 Kubernetes 部署或使用动态 DNS 时。
- 引入版本: v3.5.11, v4.0.4

##### `enable_http_async_handler`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许系统异步处理 HTTP 请求。如果启用此功能，Netty 工作线程收到的 HTTP 请求将提交到单独的线程池进行服务逻辑处理，以避免阻塞 HTTP 服务器。如果禁用，Netty 工作线程将处理服务逻辑。
- 引入版本: 4.0.0

##### `enable_http_validate_headers`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制 Netty 的 HttpServerCodec 是否执行严格的 HTTP 头部验证。该值在 `HttpServer` 初始化 HTTP 管道时传递给 HttpServerCodec（参见 UseLocations）。默认值为 false 以保持向后兼容性，因为较新的 Netty 版本强制执行更严格的头部规则 (https://github.com/netty/netty/pull/12760)。设置为 true 以强制执行符合 RFC 的头部检查；这样做可能会导致来自旧客户端或代理的格式错误或不符合规范的请求被拒绝。更改需要重启 HTTP 服务器才能生效。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `enable_https`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否在 FE 节点中与 HTTP 服务器一起启用 HTTPS 服务器。
- 引入版本: v4.0

##### `frontend_address`

- 默认值: 0.0.0.0
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 节点的 IP 地址。
- 引入版本: -

##### `http_async_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 异步 HTTP 请求处理的线程池大小。别名为 `max_http_sql_service_task_threads_num`。
- 引入版本: 4.0.0

##### `http_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTP 服务器持有的 backlog 队列的长度。
- 引入版本: -

##### `http_max_chunk_size`

- 默认值: 8192
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置 FE HTTP 服务器中 Netty 的 HttpServerCodec 处理的单个 HTTP 块的最大允许大小（以字节为单位）。它作为第三个参数传递给 HttpServerCodec，并限制分块传输或流式请求/响应期间的块长度。如果传入块超过此值，Netty 将引发帧过大错误（例如 TooLongFrameException），并且请求可能会被拒绝。对于合法的分块上传，请增加此值；保持较小以减少内存压力并减小 DoS 攻击的表面积。此设置与 `http_max_initial_line_length`、`http_max_header_size` 和 `enable_http_validate_headers` 一起使用。
- 引入版本: v3.2.0

##### `http_max_header_size`

- 默认值: 32768
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: Netty 的 `HttpServerCodec` 解析的 HTTP 请求头块的最大允许大小（以字节为单位）。StarRocks 将此值传递给 `HttpServerCodec`（作为 `Config.http_max_header_size`）；如果传入请求的头（名称和值组合）超过此限制，编解码器将拒绝该请求（解码器异常），并且连接/请求将失败。仅当客户端合法发送非常大的头（大型 cookie 或许多自定义头）时才增加此值；较大的值会增加每个连接的内存使用。与 `http_max_initial_line_length` 和 `http_max_chunk_size` 一起调整。更改需要重启 FE。
- 引入版本: v3.2.0

##### `http_max_initial_line_length`

- 默认值: 4096
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置 HttpServer 中使用的 Netty `HttpServerCodec` 接受的 HTTP 初始请求行（方法 + 请求目标 + HTTP 版本）的最大允许长度（以字节为单位）。该值传递给 Netty 的解码器，并且初始行长于此值的请求将被拒绝 (TooLongFrameException)。仅当您必须支持非常长的请求 URI 时才增加此值；较大的值会增加内存使用，并可能增加暴露于格式错误/请求滥用的风险。与 `http_max_header_size` 和 `http_max_chunk_size` 一起调整。
- 引入版本: v3.2.0

##### `http_port`

- 默认值: 8030
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTP 服务器监听的端口。
- 引入版本: -

##### `http_web_page_display_hardware`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，HTTP 索引页面 (/index) 将包含一个通过 oshi 库填充的硬件信息部分（CPU、内存、进程、磁盘、文件系统、网络等）。oshi 可能会间接调用系统工具或读取系统文件（例如，它可以执行 `getent passwd` 等命令），这可能会暴露敏感的系统数据。如果您需要更严格的安全性或希望避免在主机上执行这些间接命令，请将此配置设置为 false 以禁用 Web UI 上硬件详细信息的收集和显示。
- 引入版本: v3.2.0

##### `http_worker_threads_num`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: HTTP 服务器处理 HTTP 请求的工作线程数。如果为负值或 0，则线程数将是 CPU 核心数的两倍。
- 引入版本: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### `https_port`

- 默认值: 8443
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTPS 服务器监听的端口。
- 引入版本: v4.0

##### `max_mysql_service_task_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器可运行以处理任务的最大线程数。
- 引入版本: -

##### `max_task_runs_threads_num`

- 默认值: 512
- 类型: Int
- 单位: 线程
- 是否可变: No
- 描述: 控制任务运行执行器线程池中的最大线程数。此值是并发任务运行执行的上限；增加它会提高并行度，但也会增加 CPU、内存和网络使用率，而减少它可能导致任务运行积压和更高的延迟。根据预期的并发调度作业和可用的系统资源调整此值。
- 引入版本: v3.2.0

##### `memory_tracker_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用 FE 内存跟踪器子系统。当 `memory_tracker_enable` 设置为 `true` 时，`MemoryUsageTracker` 定期扫描注册的元数据模块，更新内存中的 `MemoryUsageTracker.MEMORY_USAGE` map，记录总计，并使 `MetricRepo` 在指标输出中暴露内存使用和对象计数 gauge。使用 `memory_tracker_interval_seconds` 控制采样间隔。启用此功能有助于监控和调试内存消耗，但会引入 CPU 和 I/O 开销以及额外的指标基数。
- 引入版本: v3.2.4

##### `memory_tracker_interval_seconds`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: FE `MemoryUsageTracker` 守护程序轮询和记录 FE 进程和已注册 `MemoryTrackable` 模块内存使用情况的间隔（秒）。当 `memory_tracker_enable` 设置为 `true` 时，跟踪器以此频率运行，更新 `MEMORY_USAGE`，并记录聚合的 JVM 和跟踪模块使用情况。
- 引入版本: v3.2.4

##### `mysql_nio_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器持有的 backlog 队列的长度。
- 引入版本: -

##### `mysql_server_version`

- 默认值: 8.0.33
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 返回给客户端的 MySQL 服务器版本。修改此参数将影响以下情况的版本信息：
  1. `select version();`
  2. 握手包版本
  3. 全局变量 `version` 的值 (`show variables like 'version';`)
- 引入版本: -

##### `mysql_service_io_threads_num`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器可运行以处理 I/O 事件的最大线程数。
- 引入版本: -

##### `mysql_service_kill_after_disconnect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制当检测到 MySQL TCP 连接关闭（读取时 EOF）时服务器如何处理会话。如果设置为 `true`，服务器会立即杀死该连接的所有正在运行的查询并立即执行清理。如果设置为 `false`，服务器在断开连接时不会杀死正在运行的查询，并且仅在没有待处理请求任务时执行清理，允许长时间运行的查询在客户端断开连接后继续。注意：尽管有一条简短的注释建议 TCP keep-alive，但此参数专门管理断开连接后的杀死行为，应根据您是希望终止孤立查询（在不可靠/负载均衡客户端后推荐）还是允许其完成进行设置。
- 引入版本: -

##### `mysql_service_nio_enable_keep_alive`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 启用 MySQL 连接的 TCP Keep-Alive。对于负载均衡器后面的长时间空闲连接很有用。
- 引入版本: -

##### `net_use_ipv6_when_priority_networks_empty`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 一个布尔值，用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本: v3.3.0

##### `priority_networks`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 声明具有多个 IP 地址的服务器的选择策略。请注意，最多一个 IP 地址必须与此参数指定的列表匹配。此参数的值是一个列表，由 CIDR 表示法中用分号 (;) 分隔的条目组成，例如 10.10.10.0/24。如果没有 IP 地址与此列表中的条目匹配，则将随机选择服务器的可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，则系统默认使用 IPv4 地址。您可以将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本: -

##### `proc_profile_cpu_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，后台 `ProcProfileCollector` 将使用 `AsyncProfiler` 收集 CPU profile，并将 HTML 报告写入 `sys_log_dir/proc_profile` 下。每次收集运行都会记录 `proc_profile_collect_time_s` 配置持续时间内的 CPU 堆栈，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的 profile 会被压缩，并根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 清理旧文件。`AsyncProfiler` 需要原生库 (`libasyncProfiler.so`)；`one.profiler.extractPath` 设置为 `STARROCKS_HOME_DIR/bin` 以避免 `/tmp` 上的 noexec 问题。
- 引入版本: v3.2.12

##### `qe_max_connection`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 所有用户可以与 FE 节点建立的最大连接数。从 v3.1.12 和 v3.2.7 开始，默认值已从 `1024` 更改为 `4096`。
- 引入版本: -

##### `query_port`

- 默认值: 9030
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器监听的端口。
- 引入版本: -

##### `rpc_port`

- 默认值: 9020
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 Thrift 服务器监听的端口。
- 引入版本: -

##### `slow_lock_stack_trace_reserve_levels`

- 默认值: 15
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 为慢速或持有的锁转储锁调试信息时捕获和发出的堆栈跟踪帧数。此值由 `QueryableReentrantReadWriteLock` 在生成排他锁所有者、当前线程和最旧/共享读取器的 JSON 时传递给 `LogUtil.getStackTraceToJsonArray`。增加此值可为诊断慢锁或死锁问题提供更多上下文，但代价是 JSON 负载更大，并且堆栈捕获的 CPU/内存略高；减少此值可降低开销。注意：当只记录慢锁时，读取器条目可以通过 `slow_lock_threshold_ms` 过滤。
- 引入版本: v3.4.0, v3.5.0

##### `ssl_cipher_blacklist`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 以逗号分隔的列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入黑名单。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

##### `ssl_cipher_whitelist`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 以逗号分隔的列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入白名单。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

##### `task_runs_concurrency`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 并发运行的 TaskRun 实例的全局限制。当当前运行计数大于或等于 `task_runs_concurrency` 时，`TaskRunScheduler` 会停止调度新运行，因此此值限制了调度器中并行 TaskRun 执行的上限。它还被 `MVPCTRefreshPartitioner` 用于计算每个 TaskRun 分区刷新粒度。增加该值会提高并行度并增加资源使用；减少它会降低并发并使分区刷新在每次运行中更大。除非有意禁用调度，否则不要设置为 0 或负值：0（或负值）将有效地阻止 `TaskRunScheduler` 调度新的 TaskRun。
- 引入版本: v3.2.0

##### `task_runs_queue_length`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 限制待处理队列中保留的待处理 TaskRun 项的最大数量。`TaskRunManager` 检查当前待处理计数，并在有效待处理 TaskRun 计数大于或等于 `task_runs_queue_length` 时拒绝新的提交。在添加合并/接受的 TaskRun 之前，会重新检查相同的限制。调整此值以平衡内存和调度积压：对于大量突发工作负载，设置为较高值以避免拒绝，或设置为较低值以限制内存并减少待处理积压。
- 引入版本: v3.2.0

##### `thrift_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 Thrift 服务器持有的 backlog 队列的长度。
- 引入版本: -

##### `thrift_client_timeout_ms`

- 默认值: 5000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 空闲客户端连接超时的时间长度。
- 引入版本: -

##### `thrift_rpc_max_body_size`

- 默认值: -1
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 控制构建服务器 Thrift 协议时使用的 Thrift RPC 消息体最大允许大小（以字节为单位）（传递给 `ThriftServer` 中的 TBinaryProtocol.Factory）。值为 `-1` 表示禁用限制（无界）。设置正值会强制执行上限，以便大于此值的消息被 Thrift 层拒绝，这有助于限制内存使用并缓解超大请求或 DoS 风险。将其设置为足够大的值以适应预期负载（大型结构或批量数据），以避免拒绝合法请求。
- 引入版本: v3.2.0

##### `thrift_server_max_worker_threads`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 节点中 Thrift 服务器支持的最大工作线程数。
- 引入版本: -

##### `thrift_server_queue_size`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 请求待处理队列的长度。如果 Thrift 服务器中正在处理的线程数超过 `thrift_server_max_worker_threads` 中指定的值，则新请求将添加到待处理队列。
- 引入版本: -

### 元数据和集群管理

##### `alter_max_worker_queue_size`

- 默认值: 4096
- 类型: Int
- 单位: 任务数
- 是否可变: No
- 描述: 控制 alter 子系统使用的内部工作线程池队列的容量。它与 `alter_max_worker_threads` 一起传递给 `AlterHandler` 中的 `ThreadPoolManager.newDaemonCacheThreadPool`。当待处理的 alter 任务数超过 `alter_max_worker_queue_size` 时，新的提交将被拒绝，并可能抛出 `RejectedExecutionException`（参见 `AlterHandler.handleFinishAlterTask`）。调整此值以平衡内存使用和允许并发 alter 任务的积压量。
- 引入版本: v3.2.0

##### `alter_max_worker_threads`

- 默认值: 4
- 类型: Int
- 单位: 线程
- 是否可变: No
- 描述: 设置 AlterHandler 线程池中的最大工作线程数。AlterHandler 使用此值构造执行器来运行和完成与 alter 相关的任务（例如，通过 handleFinishAlterTask 提交 `AlterReplicaTask`）。此值限制了 alter 操作的并发执行；增加它会提高并行度并增加资源使用，降低它会限制并发 alters 并可能成为瓶颈。执行器与 `alter_max_worker_queue_size` 一起创建，并且处理程序调度使用 `alter_scheduler_interval_millisecond`。
- 引入版本: v3.2.0

##### `automated_cluster_snapshot_interval_seconds`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 触发自动化集群快照任务的间隔。
- 引入版本: v3.4.2

##### `background_refresh_metadata_interval_millis`

- 默认值: 600000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 两次 Hive 元数据缓存刷新之间的间隔。
- 引入版本: v2.5.5

##### `background_refresh_metadata_time_secs_since_last_access_secs`

- 默认值: 3600 * 24
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: Hive 元数据缓存刷新任务的过期时间。对于已访问的 Hive Catalog，如果超过指定时间未访问，StarRocks 将停止刷新其缓存的元数据。对于未访问的 Hive Catalog，StarRocks 不会刷新其缓存的元数据。
- 引入版本: v2.5.5

##### `bdbje_cleaner_threads`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: StarRocks journal 使用的 Berkeley DB Java Edition (JE) 环境的后台清理线程数。此值在 `BDBEnvironment.initConfigs` 中的环境初始化期间读取，并使用 `Config.bdbje_cleaner_threads` 应用于 `EnvironmentConfig.CLEANER_THREADS`。它控制 JE 日志清理和空间回收的并行度；增加它可以加快清理速度，但代价是会增加额外的 CPU 和 I/O 干扰前台操作。更改仅在 BDB 环境（重新）初始化时生效，因此需要重启前端才能应用新值。
- 引入版本: v3.2.0

##### `bdbje_heartbeat_timeout_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: StarRocks 集群中 Leader、Follower 和 Observer FE 之间心跳超时的时间。
- 引入版本: -

##### `bdbje_lock_timeout_second`

- 默认值: 1
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 基于 BDB JE 的 FE 中的锁超时时间。
- 引入版本: -

##### `bdbje_replay_cost_percent`

- 默认值: 150
- 类型: Int
- 单位: 百分比
- 是否可变: No
- 描述: 设置从 BDB JE 日志重放事务相对于通过网络恢复相同数据的相对成本（以百分比表示）。该值提供给底层 JE 复制参数 `REPLAY_COST_PERCENT`，通常 `>100` 表示重放通常比网络恢复更昂贵。当决定是否保留清理过的日志文件以进行潜在重放时，系统会将重放成本乘以日志大小与网络恢复成本进行比较；如果判断网络恢复更有效，则将删除文件。值为 0 禁用基于此成本比较的保留。`REP_STREAM_TIMEOUT` 内的副本或任何活动复制所需的日志文件始终保留。
- 引入版本: v3.2.0

##### `bdbje_replica_ack_timeout_second`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 当元数据从 Leader FE 写入 Follower FE 时，Leader FE 等待指定数量的 Follower FE 返回 ACK 消息的最长时间。单位：秒。如果正在写入大量元数据，Follower FE 需要很长时间才能向 Leader FE 返回 ACK 消息，从而导致 ACK 超时。在这种情况下，元数据写入失败，FE 进程退出。建议您增加此参数的值以防止这种情况。
- 引入版本: -

##### `bdbje_reserved_disk_size`

- 默认值: 512 * 1024 * 1024 (536870912)
- 类型: Long
- 单位: 字节
- 是否可变: No
- 描述: 限制 Berkeley DB JE 将保留的“非保护”（可删除）日志/数据文件的字节数。StarRocks 通过 `BDBEnvironment` 中的 `EnvironmentConfig.RESERVED_DISK` 将此值传递给 JE；JE 的内置默认值为 0（无限制）。StarRocks 默认值（512 MiB）可防止 JE 为非保护文件保留过多的磁盘空间，同时允许安全清理过时文件。在磁盘受限的系统上调整此值：减小它可让 JE 更早释放更多文件，增加它可让 JE 保留更多保留空间。更改需要重启进程才能生效。
- 引入版本: v3.2.0

##### `bdbje_reset_election_group`

- 默认值: false
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 是否重置 BDBJE 复制组。如果此参数设置为 `TRUE`，FE 将重置 BDBJE 复制组（即移除所有可选举 FE 节点的信息）并作为 Leader FE 启动。重置后，此 FE 将是集群中唯一的成员，其他 FE 可以通过使用 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入此集群。仅当由于大多数 Follower FE 的数据已损坏而无法选举 Leader FE 时才使用此设置。`reset_election_group` 用于替换 `metadata_failure_recovery`。
- 引入版本: -

##### `black_host_connect_failures_within_time`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 黑名单 BE 节点允许的连接失败阈值。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以将其从 BE 黑名单中删除。在 `black_host_history_sec` 内，只有当黑名单 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中删除。
- 引入版本: v3.3.0

##### `black_host_history_sec`

- 默认值: 2 * 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 保留 BE 节点历史连接失败的持续时间（BE 黑名单中）。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以将其从 BE 黑名单中删除。在 `black_host_history_sec` 内，只有当黑名单 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中删除。
- 引入版本: v3.3.0

##### `brpc_connection_pool_size`

- 默认值: 16
- 类型: Int
- 单位: 连接数
- 是否可变: No
- 描述: FE 的 BrpcProxy 使用的每个端点的最大池化 BRPC 连接数。此值通过 `setMaxTotoal` 和 `setMaxIdleSize` 应用于 RpcClientOptions，因此它直接限制并发传出 BRPC 请求，因为每个请求必须从池中借用一个连接。在高并发场景中，增加此值以避免请求排队；增加它会增加套接字和内存使用量，并可能增加远程服务器负载。调整时，请考虑相关设置，例如 `brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr` 和 `brpc_min_evictable_idle_time_ms`。更改此值不可热重载，需要重启。
- 引入版本: v3.2.0

##### `brpc_short_connection`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 控制底层 brpc RpcClient 是否使用短连接。启用时 (`true`)，设置 RpcClientOptions.setShortConnection，并且连接在请求完成后关闭，从而减少长时间连接套接字的数量，但代价是更高的连接设置开销和增加的延迟。禁用时 (`false`，默认值) 使用持久连接和连接池。启用此选项会影响连接池行为，应与 `brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr` 和 `brpc_inner_reuse_pool` 一起考虑。对于典型的高吞吐量部署，保持禁用；仅在需要限制套接字生命周期或网络策略需要短连接时才启用。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `catalog_try_lock_timeout_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 获取全局锁的超时时长。
- 引入版本: -

##### `checkpoint_only_on_leader`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当 `true` 时，CheckpointController 将仅选择 Leader FE 作为 checkpoint worker；当 `false` 时，控制器可能会选择任何前端，并优先选择堆使用率较低的节点。当为 `false` 时，worker 按最近的失败时间和 `heapUsedPercent` 排序（Leader 被视为具有无限使用率以避免选择它）。对于需要集群快照元数据的操作，控制器无论此标志如何都已强制选择 Leader。启用 `true` 会将 checkpoint 工作集中在 Leader 上（更简单，但增加了 Leader 的 CPU/内存和网络负载）；保持 `false` 会将 checkpoint 负载分配到负载较低的 FE。此设置影响 worker 选择以及与 `checkpoint_timeout_seconds` 等超时和 `thrift_rpc_timeout_ms` 等 RPC 设置的交互。
- 引入版本: v3.4.0, v3.5.0

##### `checkpoint_timeout_seconds`

- 默认值: 24 * 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: Leader 的 CheckpointController 将等待 checkpoint worker 完成 checkpoint 的最长时间（以秒为单位）。控制器将此值转换为纳秒并轮询 worker 结果队列；如果在此超时内未收到成功完成，则 checkpoint 被视为失败，并且 createImage 返回失败。增加此值可适应更长时间运行的 checkpoint，但会延迟故障检测和随后的镜像传播；减少此值会导致更快的故障转移/重试，但可能会因慢速 worker 而产生误报超时。此设置仅控制 `CheckpointController` 在 checkpoint 创建期间的等待时间，不改变 worker 的内部 checkpointing 行为。
- 引入版本: v3.4.0, v3.5.0

##### `db_used_data_quota_update_interval_secs`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 数据库已用数据配额更新的间隔。StarRocks 会定期更新所有数据库的已用数据配额，以跟踪存储消耗。此值用于配额强制执行和指标收集。允许的最小间隔为 30 秒，以防止过高的系统负载。小于 30 的值将被拒绝。
- 引入版本: -

##### `drop_backend_after_decommission`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: BE 下线后是否删除 BE。`TRUE` 表示 BE 下线后立即删除 BE。`FALSE` 表示 BE 下线后不删除 BE。
- 引入版本: -

##### `edit_log_port`

- 默认值: 9010
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 集群中 Leader、Follower 和 Observer FE 之间通信使用的端口。
- 引入版本: -

##### `edit_log_roll_num`

- 默认值: 50000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在为日志条目创建日志文件之前可以写入的元数据日志条目的最大数量。此参数用于控制日志文件的大小。新的日志文件写入 BDBJE 数据库。
- 引入版本: -

##### `edit_log_type`

- 默认值: BDB
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 可以生成的 edit log 类型。将值设置为 `BDB`。
- 引入版本: -

##### `enable_background_refresh_connector_metadata`

- 默认值: v3.0 及更高版本为 true，v2.5 为 false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用周期性 Hive 元数据缓存刷新。启用后，StarRocks 会轮询 Hive 集群的 metastore（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Hive Catalog 的缓存元数据，以感知数据变化。`true` 表示启用 Hive 元数据缓存刷新，`false` 表示禁用。
- 引入版本: v2.5.5

##### `enable_collect_query_detail_info`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集查询的 profile。如果此参数设置为 `TRUE`，系统将收集查询的 profile。如果此参数设置为 `FALSE`，系统将不收集查询的 profile。
- 引入版本: -

##### `enable_create_partial_partition_in_batch`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `false`（默认）时，StarRocks 会强制批量创建的范围分区与标准时间单位边界对齐。它将拒绝非对齐的范围以避免创建空洞。将此项设置为 `true` 会禁用该对齐检查，并允许批量创建部分（非标准）分区，这可能会产生间隙或错位的分区范围。仅当您有意需要部分批量分区并接受相关风险时才应将其设置为 `true`。
- 引入版本: v3.2.0

##### `enable_internal_sql`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，内部组件（例如 SimpleExecutor）执行的内部 SQL 语句将保留并写入内部审计或日志消息中（如果设置了 `enable_sql_desensitize_in_log`，还可以进一步脱敏）。当设置为 `false` 时，内部 SQL 文本将被抑制：格式化代码 (SimpleExecutor.formatSQL) 返回 "?"，并且实际语句不会发出到内部审计或日志消息中。此配置不改变内部语句的执行语义——它仅控制内部 SQL 的日志记录和可见性，用于隐私或安全目的。
- 引入版本: -

##### `enable_legacy_compatibility_for_replication`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 Legacy 复制兼容性。StarRocks 在新旧版本之间可能表现不同，导致跨集群数据迁移时出现问题。因此，在数据迁移之前，您必须为目标集群启用 Legacy 兼容性，并在数据迁移完成后禁用它。`true` 表示启用此模式。
- 引入版本: v3.1.10, v3.2.6

##### `enable_show_materialized_views_include_all_task_runs`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 SHOW MATERIALIZED VIEWS 命令如何返回 TaskRun。当此项设置为 `false` 时，StarRocks 只返回每个任务的最新 TaskRun（为兼容性而保留的旧行为）。当设置为 `true`（默认）时，`TaskManager` 可能会为同一任务包含额外的 TaskRun，但仅当它们共享相同的启动 TaskRun ID（例如，属于同一作业）时，从而防止出现不相关的重复运行，同时允许显示与一个作业相关的多个状态。将此项设置为 `false` 可恢复单次运行输出，或用于调试和监控的多运行作业历史记录。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `enable_statistics_collect_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为统计信息查询生成 profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计信息查询生成查询 profile。
- 引入版本: v3.1.5

##### `enable_table_name_case_insensitive`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否对 catalog 名称、数据库名称、表名称、视图名称和物化视图名称启用不区分大小写的处理。当前，表名称默认区分大小写。
  - 启用此功能后，所有相关名称将以小写形式存储，并且所有包含这些名称的 SQL 命令将自动将其转换为小写。
  - 您只能在创建集群时启用此功能。**集群启动后，此配置的值无法通过任何方式修改**。任何修改尝试都将导致错误。FE 在检测到此配置项的值与集群首次启动时不一致时将无法启动。
  - 当前，此功能不支持 JDBC catalog 和表名称。如果您想对 JDBC 或 ODBC 数据源执行不区分大小写的处理，请不要启用此功能。
- 引入版本: v4.0

##### `enable_task_history_archive`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，已完成的任务运行记录将存档到持久的任务运行历史表中，并记录到 edit log 中，以便查找（例如，`lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`）包含存档结果。存档由 FE Leader 执行，并在单元测试 (`FeConstants.runningUnitTest`) 期间跳过。启用后，会绕过内存中的过期和强制 GC 路径（代码从 `removeExpiredRuns` 和 `forceGC` 中提前返回），因此保留/驱逐由持久存档处理，而不是 `task_runs_ttl_second` 和 `task_runs_max_history_number`。禁用后，历史记录保留在内存中，并由这些配置进行清理。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `enable_task_run_fe_evaluation`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，FE 将在 `TaskRunsSystemTable.supportFeEvaluation` 中对系统表 `task_runs` 执行本地评估。FE 侧评估仅允许用于将列与常量进行比较的合取相等谓词，并且仅限于 `QUERY_ID` 和 `TASK_NAME` 列。启用此功能可提高定向查找的性能，避免更广泛的扫描或额外的远程处理；禁用它会强制规划器跳过对 `task_runs` 的 FE 评估，这可能会减少谓词剪枝并影响这些过滤器的查询延迟。
- 引入版本: v3.3.13, v3.4.3, v3.5.0

##### `heartbeat_mgr_blocking_queue_size`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 存储 Heartbeat Manager 运行的心跳任务的阻塞队列的大小。
- 引入版本: -

##### `heartbeat_mgr_threads_num`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: Heartbeat Manager 可运行以运行心跳任务的线程数。
- 引入版本: -

##### `ignore_materialized_view_error`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 是否忽略物化视图错误导致的元数据异常。如果 FE 因物化视图错误导致的元数据异常而无法启动，您可以将此参数设置为 `true` 以允许 FE 忽略该异常。
- 引入版本: v2.5.10

##### `ignore_meta_check`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 非 Leader FE 是否忽略 Leader FE 的元数据差距。如果值为 TRUE，非 Leader FE 忽略 Leader FE 的元数据差距并继续提供数据读取服务。此参数确保即使您长时间停止 Leader FE，也能持续提供数据读取服务。如果值为 FALSE，非 Leader FE 不忽略 Leader FE 的元数据差距并停止提供数据读取服务。
- 引入版本: -

##### `ignore_task_run_history_replay_error`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当 StarRocks 反序列化 `information_schema.task_runs` 的 TaskRun 历史行时，损坏或无效的 JSON 行通常会导致反序列化记录警告并抛出 RuntimeException。如果此项设置为 `true`，系统将捕获反序列化错误，跳过格式错误的记录，并继续处理剩余行而不是使查询失败。这将使 `information_schema.task_runs` 查询能够容忍 `_statistics_.task_run_history` 表中的错误条目。请注意，启用它将静默丢弃损坏的历史记录（潜在数据丢失），而不是显式报错。
- 引入版本: v3.3.3, v3.4.0, v3.5.0

##### `lock_checker_interval_second`

- 默认值: 30
- 类型: long
- 单位: 秒
- 是否可变: Yes
- 描述: LockChecker 前端守护程序（名为 "deadlock-checker"）执行的间隔（秒）。守护程序执行死锁检测和慢锁扫描；配置值乘以 1000 以设置计时器（毫秒）。减小此值可减少检测延迟但增加调度和 CPU 开销；增加此值可减少开销但延迟检测和慢锁报告。更改在运行时生效，因为守护程序每次运行都会重置其间隔。此设置与 `lock_checker_enable_deadlock_check`（启用死锁检查）和 `slow_lock_threshold_ms`（定义慢锁的构成）交互。
- 引入版本: v3.2.0

##### `master_sync_policy`

- 默认值: SYNC
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Leader FE 将日志刷新到磁盘的策略。此参数仅当当前 FE 是 Leader FE 时有效。有效值：
  - `SYNC`: 事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`: 事务提交时，日志条目的生成和刷新不同时发生。
  - `WRITE_NO_SYNC`: 事务提交时，日志条目同时生成但不刷新到磁盘。

  如果您只部署了一个 Follower FE，建议您将此参数设置为 `SYNC`。如果您部署了三个或更多 Follower FE，建议您将此参数和 `replica_sync_policy` 都设置为 `WRITE_NO_SYNC`。

- 引入版本: -

##### `max_bdbje_clock_delta_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: No
- 描述: StarRocks 集群中 Leader FE 与 Follower 或 Observer FE 之间允许的最大时钟偏移量。
- 引入版本: -

##### `meta_delay_toleration_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Follower 和 Observer FE 上的元数据可以比 Leader FE 上的元数据落后最长时间。单位：秒。如果超过此持续时间，非 Leader FE 将停止提供服务。
- 引入版本: -

##### `meta_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储元数据的目录。
- 引入版本: -

##### `metadata_ignore_unknown_operation_type`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否忽略未知日志 ID。当 FE 回滚时，早期版本的 FE 可能无法识别某些日志 ID。如果值为 `TRUE`，FE 将忽略未知日志 ID。如果值为 `FALSE`，FE 将退出。
- 引入版本: -

##### `profile_info_format`

- 默认值: default
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 系统输出的 Profile 格式。有效值：`default` 和 `json`。设置为 `default` 时，Profile 为默认格式。设置为 `json` 时，系统输出 JSON 格式的 Profile。
- 引入版本: v2.5

##### `replica_ack_policy`

- 默认值: `SIMPLE_MAJORITY`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 日志条目被认为有效的策略。默认值 `SIMPLE_MAJORITY` 指定如果大多数 Follower FE 返回 ACK 消息，则日志条目被认为有效。
- 引入版本: -

##### `replica_sync_policy`

- 默认值: SYNC
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Follower FE 将日志刷新到磁盘的策略。此参数仅当当前 FE 是 Follower FE 时有效。有效值：
  - `SYNC`: 事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`: 事务提交时，日志条目的生成和刷新不同时发生。
  - `WRITE_NO_SYNC`: 事务提交时，日志条目同时生成但不刷新到磁盘。
- 引入版本: -

##### `start_with_incomplete_meta`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，如果镜像数据存在但 Berkeley DB JE (BDB) 日志文件丢失或损坏，FE 将允许启动。`MetaHelper.checkMetaDir()` 使用此标志绕过安全检查，否则会阻止从没有相应 BDB 日志的镜像启动；以这种方式启动可能会产生陈旧或不一致的元数据，应仅用于紧急恢复。`RestoreClusterSnapshotMgr` 在恢复集群快照时暂时将此标志设置为 true，然后将其回滚；该组件在恢复期间也会切换 `bdbje_reset_election_group`。在正常操作中不要启用它 — 仅在从损坏的 BDB 数据恢复或显式恢复基于镜像的快照时启用。
- 引入版本: v3.2.0

##### `table_keeper_interval_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: TableKeeper 守护程序执行的间隔（秒）。TableKeeperDaemon 使用此值（乘以 1000）设置其内部计时器，并定期运行 keeper 任务，以确保历史表存在、正确的表属性（复制数量）并更新分区 TTL。守护程序仅在 Leader 节点上执行工作，并在 `table_keeper_interval_second` 更改时通过 setInterval 更新其运行时间隔。增加此值可减少调度频率和负载；减少此值可更快响应缺失或陈旧的历史表。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `task_runs_ttl_second`

- 默认值: 7 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 控制任务运行历史记录的存活时间 (TTL)。降低此值会缩短历史记录保留时间并减少内存/磁盘使用；提高此值会保留更长时间的历史记录，但会增加资源使用。与 `task_runs_max_history_number` 和 `enable_task_history_archive` 一起调整，以实现可预测的保留和存储行为。
- 引入版本: v3.2.0

##### `task_ttl_second`

- 默认值: 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 任务的存活时间 (TTL)。对于手动任务（未设置调度），TaskBuilder 使用此值计算任务的 `expireTime` (`expireTime = now + task_ttl_second * 1000L`)。TaskRun 也将此值用作计算运行执行超时的上限 — 有效执行超时为 `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`。调整此值会更改手动创建任务的有效时间，并可以间接限制任务运行的最大允许执行时间。
- 引入版本: v3.2.0

##### `thrift_rpc_retry_times`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 Thrift RPC 调用将尝试的总次数。此值由 `ThriftRPCRequestExecutor`（以及 `NodeMgr` 和 `VariableMgr` 等调用者）用作重试的循环计数 — 即，值为 3 允许最多三次尝试，包括初始尝试。在 `TTransportException` 上，执行器将尝试重新打开连接并重试此计数；当原因是 `SocketTimeoutException` 或重新打开失败时，它不会重试。每次尝试都受 `thrift_rpc_timeout_ms` 配置的每次尝试超时限制。增加此值可提高对瞬时连接失败的弹性，但会增加整体 RPC 延迟和资源使用。
- 引入版本: v3.2.0

##### `thrift_rpc_strict_mode`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制 Thrift 服务器使用的 TBinaryProtocol“严格读取”模式。此值作为第一个参数传递给 Thrift 服务器堆栈中的 org.apache.thrift.protocol.TBinaryProtocol.Factory，并影响如何解析和验证传入的 Thrift 消息。当 `true`（默认）时，服务器强制执行严格的 Thrift 编码/版本检查并遵守配置的 `thrift_rpc_max_body_size` 限制；当 `false` 时，服务器接受非严格（旧版/宽松）消息格式，这可以提高与旧客户端的兼容性，但可能会绕过某些协议验证。在运行中的集群上更改此值请谨慎，因为它不可变并影响互操作性和解析安全性。
- 引入版本: v3.2.0

##### `thrift_rpc_timeout_ms`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用作 Thrift RPC 调用默认网络/套接字超时的持续时间（毫秒）。它在 `ThriftConnectionPool`（前端和后端池使用）创建 Thrift 客户端时传递给 TSocket，并且在 `ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr` 和 `CheckpointWorker` 等地方计算 RPC 调用超时时也添加到操作的执行超时（例如 ExecTimeout*1000 + `thrift_rpc_timeout_ms`）。增加此值会使 RPC 调用能够容忍更长的网络或远程处理延迟；减少此值会导致慢速网络上的故障转移更快。更改此值会影响执行 Thrift RPC 的 FE 代码路径中的连接创建和请求截止日期。
- 引入版本: v3.2.0

##### `txn_latency_metric_report_groups`

- 默认值: 一个空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 逗号分隔的事务延迟指标组列表，用于报告。加载类型被归类为逻辑组以进行监控。当启用某个组时，其名称将作为“类型”标签添加到事务指标中。有效值：`stream_load`、`routine_load`、`broker_load`、`insert` 和 `compaction`（仅适用于共享数据集群）。示例：`"stream_load,routine_load"`。
- 引入版本: v4.0

##### `txn_rollback_limit`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 可回滚的最大事务数。
- 引入版本: -

### 用户、角色和权限

##### `enable_task_info_mask_credential`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，StarRocks 会在将凭据从任务 SQL 定义返回到 `information_schema.tasks` 和 `information_schema.task_runs` 之前，通过对 DEFINITION 列应用 SqlCredentialRedactor.redact 来屏蔽凭据。在 `information_schema.task_runs` 中，无论定义是来自任务运行状态还是在为空时来自任务定义查找，都应用相同的屏蔽。当为 false 时，返回原始任务定义（可能会暴露凭据）。屏蔽是 CPU/字符串处理工作，当任务或 `task_runs` 数量很大时可能非常耗时；仅当您需要未屏蔽的定义并接受安全风险时才禁用。
- 引入版本: v3.5.6

##### `privilege_max_role_depth`

- 默认值: 16
- 类型: Int
- 单位:
- 是否可变: Yes
- 描述: 角色的最大角色深度（继承级别）。
- 引入版本: v3.0.0

##### `privilege_max_total_roles_per_user`

- 默认值: 64
- 类型: Int
- 单位:
- 是否可变: Yes
- 描述: 用户可以拥有的最大角色数量。
- 引入版本: v3.0.0

### 查询引擎

##### `brpc_send_plan_fragment_timeout_ms`

- 默认值: 60000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在发送计划片段之前应用于 BRPC TalkTimeoutController 的超时（毫秒）。`BackendServiceClient.sendPlanFragmentAsync` 在调用后端 `execPlanFragmentAsync` 之前设置此值。它控制 BRPC 在从连接池借用空闲连接以及执行发送时将等待多长时间；如果超过，RPC 将失败并可能触发该方法的重试逻辑。在争用情况下，将此值设置得更低以快速失败，或提高它以容忍瞬时池耗尽或慢速网络。请谨慎：非常大的值可能会延迟故障检测并阻塞请求线程。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- 默认值: 12 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 大型表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- 默认值: 2
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 小型表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_rows`

- 默认值: 10000000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 用于确定查询触发 ANALYZE 任务的表是否为小型表的阈值。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_task_schedule_interval`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 调度器线程调度查询触发后台任务的间隔。此项旨在取代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。在此处，后台任务指 v3.4 中的 `ANALYZE` 任务，以及 v3.4 之后版本中低基数列字典的收集任务。
- 引入版本: v3.4.2

##### `create_table_max_serial_replicas`

- 默认值: 128
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 串行创建副本的最大数量。如果实际副本数量超过此值，则将并发创建副本。如果表创建时间过长，请尝试减小此值。
- 引入版本: -

##### `default_mv_partition_refresh_number`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 当物化视图刷新涉及多个分区时，此参数控制默认情况下单个批次中刷新多少个分区。
从 3.3.0 版本开始，系统默认一次刷新一个分区，以避免潜在的内存溢出 (OOM) 问题。在早期版本中，所有分区默认一次刷新，这可能导致内存耗尽和任务失败。但是，请注意，当物化视图刷新涉及大量分区时，一次只刷新一个分区可能会导致过多的调度开销、更长的整体刷新时间以及大量的刷新记录。在这种情况下，建议适当调整此参数以提高刷新效率并降低调度成本。
- 引入版本: v3.3.0

##### `default_mv_refresh_immediate`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在创建后立即刷新异步物化视图。当此项设置为 `true` 时，新创建的物化视图将立即刷新。
- 引入版本: v3.2.3

##### `dynamic_partition_check_interval_seconds`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 检查新数据的间隔。如果检测到新数据，StarRocks 会自动为数据创建分区。
- 引入版本: -

##### `dynamic_partition_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用动态分区功能。启用此功能后，StarRocks 会为新数据动态创建分区，并自动删除过期分区以确保数据的及时性。
- 引入版本: -

##### `enable_active_materialized_view_schema_strict_check`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 激活非活动物化视图时是否严格检查数据类型的长度一致性。当此项设置为 `false` 时，如果基表中的数据类型长度发生变化，物化视图的激活不受影响。
- 引入版本: v3.3.4

##### `enable_auto_collect_array_ndv`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 ARRAY 类型 NDV 信息的自动收集。
- 引入版本: v4.0

##### `enable_backup_materialized_view`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在备份或恢复特定数据库时，是否启用异步物化视图的 BACKUP 和 RESTORE。如果此项设置为 `false`，StarRocks 将跳过备份异步物化视图。
- 引入版本: v3.2.0

##### `enable_collect_full_statistic`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用自动全量统计信息收集。此功能默认启用。
- 引入版本: -

##### `enable_colocate_mv_index`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 创建同步物化视图时是否支持将同步物化视图索引与基表进行 Colocate。如果此项设置为 `true`，tablet sink 将加速同步物化视图的写入性能。
- 引入版本: v3.2.0

##### `enable_decimal_v3`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否支持 DECIMAL V3 数据类型。
- 引入版本: -

##### `enable_experimental_mv`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用异步物化视图功能。TRUE 表示启用此功能。从 v2.5.2 开始，此功能默认启用。对于 v2.5.2 之前的版本，此功能默认禁用。
- 引入版本: v2.4

##### `enable_local_replica_selection`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为查询选择本地副本。本地副本可降低网络传输成本。如果此参数设置为 TRUE，CBO 优先选择与当前 FE 具有相同 IP 地址的 BE 上的 tablet 副本。如果此参数设置为 `FALSE`，则可以选择本地副本和非本地副本。
- 引入版本: -

##### `enable_manual_collect_array_ndv`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 ARRAY 类型 NDV 信息的手动收集。
- 引入版本: v4.0

##### `enable_materialized_view`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许创建物化视图。
- 引入版本: -

##### `enable_materialized_view_external_table_precise_refresh`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 将此项设置为 `true` 可为物化视图刷新启用内部优化，当基表是外部（非云原生）表时。启用后，物化视图刷新处理器会计算候选分区并仅刷新受影响的基表分区，而不是所有分区，从而减少 I/O 和刷新成本。将其设置为 `false` 以强制对外部表进行全分区刷新。
- 引入版本: v3.2.9

##### `enable_materialized_view_metrics_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认收集异步物化视图的监控指标。
- 引入版本: v3.1.11, v3.2.5

##### `enable_materialized_view_spill`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为物化视图刷新任务启用中间结果 spilling。
- 引入版本: v3.1.1

##### `enable_materialized_view_text_based_rewrite`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认启用基于文本的查询重写。如果此项设置为 `true`，系统将在创建异步物化视图时构建抽象语法树。
- 引入版本: v3.2.5

##### `enable_mv_automatic_active_check`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用系统自动检查和重新激活那些因基表（视图）进行 Schema Change 或被删除和重新创建而变为 Inactive 的异步物化视图。请注意，此功能不会重新激活用户手动设置为 Inactive 的物化视图。
- 引入版本: v3.1.6

##### `enable_mv_automatic_repairing_for_broken_base_tables`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，StarRocks 将尝试在基外部表被删除并重新创建或其表标识符更改时自动修复物化视图基表元数据。修复流程可以更新物化视图的基表信息，收集外部表分区的分区级修复信息，并推动异步自动刷新物化视图的分区刷新决策，同时遵守 `autoRefreshPartitionsLimit`。目前自动修复支持 Hive 外部表；不支持的表类型将导致物化视图设置为非活动状态并引发修复异常。分区信息收集是非阻塞的，失败将被记录。
- 引入版本: v3.3.19, v3.4.8, v3.5.6

##### `enable_predicate_columns_collection`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用谓词列收集。如果禁用，谓词列在查询优化期间将不会被记录。
- 引入版本: -

##### `enable_query_queue_v2`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，将 FE 基于槽位的查询调度器切换到查询队列 V2。槽位管理器和跟踪器（例如 `BaseSlotManager.isEnableQueryQueueV2` 和 `SlotTracker#createSlotSelectionStrategy`）会读取此标志以选择 `SlotSelectionStrategyV2` 而不是旧版策略。`query_queue_v2_xxx` 配置选项和 `QueryQueueOptions` 仅在此标志启用时生效。从 v4.1 开始，默认值从 `false` 更改为 `true`。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `enable_sql_blacklist`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 SQL 查询的黑名单检查。启用此功能后，黑名单中的查询无法执行。
- 引入版本: -

##### `enable_statistic_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集 CBO 的统计信息。此功能默认启用。
- 引入版本: -

##### `enable_statistic_collect_on_first_load`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制由数据加载操作触发的自动统计信息收集和维护。这包括：
  - 当数据首次加载到分区时（分区版本等于 2）的统计信息收集。
  - 当数据加载到多分区表的空分区时（分区版本等于 2）的统计信息收集。
  - INSERT OVERWRITE 操作的统计信息复制和更新。

  **统计信息收集类型决策策略：**
  
  - 对于 INSERT OVERWRITE：`deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。仅复制现有统计信息。
    - 否则，如果 `targetRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  - 对于首次加载：`deltaRatio = loadRows / (totalRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。
    - 否则，如果 `loadRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  **同步行为：**
  
  - 对于 DML 语句（INSERT INTO/INSERT OVERWRITE）：同步模式，带表锁。加载操作等待统计信息收集完成（最长 `semi_sync_collect_statistic_await_seconds`）。
  - 对于 Stream Load 和 Broker Load：异步模式，无锁。统计信息收集在后台运行，不阻塞加载操作。
  
  :::note
  禁用此配置将阻止所有加载触发的统计信息操作，包括 INSERT OVERWRITE 的统计信息维护，这可能导致表缺少统计信息。如果经常创建新表并频繁加载数据，启用此功能将增加内存和 CPU 开销。
  :::

- 引入版本: v3.1

##### `enable_statistic_collect_on_update`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 UPDATE 语句是否可以触发自动统计信息收集。启用后，修改表数据的 UPDATE 操作可能会通过与 `enable_statistic_collect_on_first_load` 控制的基于摄取统计信息框架调度统计信息收集。禁用此配置会跳过 UPDATE 语句的统计信息收集，同时保持加载触发的统计信息收集行为不变。
- 引入版本: v3.5.11, v4.0.4

##### `enable_udf`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否启用 UDF。
- 引入版本: -

##### `expr_children_limit`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 表达式中允许的最大子表达式数量。
- 引入版本: -

##### `histogram_buckets_size`

- 默认值: 64
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图的默认 bucket 数量。
- 引入版本: -

##### `histogram_max_sample_row_count`

- 默认值: 10000000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图收集的最大行数。
- 引入版本: -

##### `histogram_mcv_size`

- 默认值: 100
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图的最常见值 (MCV) 数量。
- 引入版本: -

##### `histogram_sample_ratio`

- 默认值: 0.1
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 直方图的采样率。
- 引入版本: -

##### `http_slow_request_threshold_ms`

- 默认值: 5000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 如果 HTTP 请求的响应时间超过此参数指定的值，则生成日志以跟踪此请求。
- 引入版本: v2.5.15, v3.1.5

##### `lock_checker_enable_deadlock_check`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，LockChecker 线程使用 ThreadMXBean.findDeadlockedThreads() 执行 JVM 级死锁检测，并记录违规线程的堆栈跟踪。检查在 LockChecker 守护程序（其频率由 `lock_checker_interval_second` 控制）内部运行，并将详细的堆栈信息写入日志，这可能耗费 CPU 和 I/O。仅在调试实时或可重现的死锁问题时才启用此选项；在正常操作中保持启用状态可能会增加开销和日志量。
- 引入版本: v3.2.0

##### `low_cardinality_threshold`

- 默认值: 255
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 低基数字典的阈值。
- 引入版本: v3.5.0

##### `materialized_view_min_refresh_interval`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: ASYNC 物化视图调度的最小允许刷新间隔（以秒为单位）。当以基于时间的间隔创建物化视图时，该间隔将转换为秒，并且不得小于此值；否则 CREATE/ALTER 操作将因 DDL 错误而失败。如果此值大于 0，则强制执行检查；将其设置为 0 或负值以禁用限制，这可以防止TaskManager 过度调度和因刷新过于频繁而导致 FE 内存/CPU 使用率过高。此项不适用于 `EVENT_TRIGGERED` 刷新。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `materialized_view_refresh_ascending`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，物化视图分区刷新将以升序分区键顺序（从最旧到最新）迭代分区。当设置为 `false`（默认）时，系统以降序（从最新到最旧）迭代。StarRocks 在列表分区和范围分区物化视图刷新逻辑中都使用此项来选择在应用分区刷新限制时要处理的分区，并计算后续 TaskRun 执行的下一个开始/结束分区边界。更改此项会改变首先刷新哪些分区以及如何导出下一个分区范围；对于范围分区物化视图，调度器会验证新的开始/结束，如果更改会创建重复边界（死循环），则会引发错误，因此请谨慎设置此项。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `max_allowed_in_element_num_of_delete`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: DELETE 语句中 IN 谓词允许的最大元素数量。
- 引入版本: -

##### `max_create_table_timeout_second`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 创建表的超时时长上限。
- 引入版本: -

##### `max_distribution_pruner_recursion_depth`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 分区剪枝器允许的最大递归深度。增加递归深度可以剪枝更多元素，但也会增加 CPU 消耗。
- 引入版本: -

##### `max_partitions_in_one_batch`

- 默认值: 4096
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 批量创建分区时可创建的最大分区数。
- 引入版本: -

##### `max_planner_scalar_rewrite_num`

- 默认值: 100000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 优化器可以重写标量操作符的最大次数。
- 引入版本: -

##### `max_query_queue_history_slots_number`

- 默认值: 0
- 类型: Int
- 单位: 槽位
- 是否可变: Yes
- 描述: 控制每个查询队列保留多少最近释放的（历史）已分配槽位用于监控和可观察性。当 `max_query_queue_history_slots_number` 设置为 `> 0` 的值时，BaseSlotTracker 在内存队列中保留最多指定数量的最新释放的 LogicalSlot 条目，当超出限制时驱逐最旧的条目。启用此功能会导致 getSlots() 包含这些历史条目（最新的在前），允许 BaseSlotTracker 尝试使用 ConnectContext 注册槽位以获取更丰富的 ExtraMessage 数据，并允许 LogicalSlot.ConnectContextListener 将查询完成元数据附加到历史槽位。当 `max_query_queue_history_slots_number` `<= 0` 时，历史机制被禁用（不使用额外的内存）。使用合理的值来平衡可观察性和内存开销。
- 引入版本: v3.5.0

##### `max_query_retry_time`

- 默认值: 2
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上查询重试的最大次数。
- 引入版本: -

##### `max_running_rollup_job_num_per_table`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每张表可以并行运行的 rollup 作业的最大数量。
- 引入版本: -

##### `max_scalar_operator_flat_children`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: ScalarOperator 的最大扁平子节点数。您可以设置此限制以防止优化器使用过多内存。
- 引入版本: -

##### `max_scalar_operator_optimize_depth`

- 默认值: 256
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: ScalarOperator 优化可以应用的最大深度。
- 引入版本: -

##### `mv_active_checker_interval_seconds`

- 默认值: 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 当后台 `active_checker` 线程启用时，系统会定期检测并自动重新激活因其基表（或视图）的 schema 变更或重建而变为 Inactive 的物化视图。此参数以秒为单位控制检查器线程的调度间隔。默认值由系统定义。
- 引入版本: v3.1.6

##### `mv_rewrite_consider_data_layout_mode`

- 默认值: `enable`
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 控制物化视图重写在选择最佳物化视图时是否应考虑基表数据布局。有效值：
  - `disable`: 在选择候选物化视图时，从不使用数据布局标准。
  - `enable`: 仅当查询被识别为布局敏感时才使用数据布局标准。
  - `force`: 在选择最佳物化视图时始终应用数据布局标准。
  更改此项会影响 `BestMvSelector` 的行为，并可以根据物理布局是否影响计划正确性或性能来改进或扩大重写的适用性。
- 引入版本: -

##### `publish_version_interval_ms`

- 默认值: 10
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 发布验证任务发出的时间间隔。
- 引入版本: -

##### `query_queue_slots_estimator_strategy`

- 默认值: MAX
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 当 `enable_query_queue_v2` 为 true 时，选择用于基于队列的查询的槽位估算策略。有效值：MBE（基于内存）、PBE（基于并行度）、MAX（取 MBE 和 PBE 的最大值）和 MIN（取 MBE 和 PBE 的最小值）。MBE 根据预测内存或计划成本除以每个槽位内存目标来估算槽位，并受 `totalSlots` 限制。PBE 根据片段并行度（扫描范围计数或基数/每槽位行数）和基于 CPU 成本的计算（使用每槽位 CPU 成本）推导出槽位，然后将结果限制在 [numSlots/2, numSlots] 范围内。MAX 和 MIN 通过取其最大值或最小值来组合 MBE 和 PBE。如果配置值无效，则使用默认值 (`MAX`)。
- 引入版本: v3.5.0

##### `query_queue_v2_concurrency_level`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制计算系统总查询槽位时使用的逻辑并发“层数”。在 shared-nothing 模式下，总槽位 = `query_queue_v2_concurrency_level` * BE 数量 * 每个 BE 的核心数（来自 BackendResourceStat）。在多仓库模式下，有效并发会缩减为 max(1, `query_queue_v2_concurrency_level` / 4)。如果配置值为非正数，则视为 `4`。更改此值会增加或减少 totalSlots（以及因此的并发查询容量），并影响每个槽位的资源：memBytesPerSlot 通过将每个 worker 内存除以（每个 worker 的核心数 * 并发）得出，并且 CPU 记账使用 `query_queue_v2_cpu_costs_per_slot`。将其设置为与集群大小成比例；非常大的值可能会减少每个槽位的内存并导致资源碎片。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_cpu_costs_per_slot`

- 默认值: 1000000000
- 类型: Long
- 单位: 规划器 CPU 成本单位
- 是否可变: Yes
- 描述: 每个槽位的 CPU 成本阈值，用于根据查询的规划器 CPU 成本估算查询所需的槽位数量。调度器计算槽位为整数（`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`），然后将结果限制在 [1, totalSlots] 范围内（totalSlots 来自查询队列 V2 `V2` 参数）。V2 代码将非正值规范化为 1 (Math.max(1, value))，因此非正值实际上变为 `1`。增加此值会减少每个查询分配的槽位（有利于更少、更大槽位的查询）；减少此值会增加每个查询的槽位。与 `query_queue_v2_num_rows_per_slot` 和并发设置一起调整，以控制并行度与资源粒度。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_num_rows_per_slot`

- 默认值: 4096
- 类型: Int
- 单位: 行
- 是否可变: Yes
- 描述: 当估算每个查询的槽位计数时，分配给单个调度槽位的目标源行记录数。StarRocks 计算 `estimated_slots` = (源节点的基数) / `query_queue_v2_num_rows_per_slot`，然后将结果限制在 [1, totalSlots] 范围内，如果计算值为非正数，则强制最小值为 1。totalSlots 来自可用资源（大致为 DOP * `query_queue_v2_concurrency_level` * worker/BE 数量），因此取决于集群/核心计数。增加此值以减少槽位计数（每个槽位处理更多行）并降低调度开销；减少此值以增加并行度（更多、更小的槽位），直至达到资源限制。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_schedule_strategy`

- 默认值: SWRR
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 选择 Query Queue V2 用于对待处理查询进行排序的调度策略。支持的值（不区分大小写）为 `SWRR` (Smooth Weighted Round Robin) - 默认值，适用于需要公平加权共享的混合/混合工作负载 - 和 `SJF` (Short Job First + Aging) - 优先处理短作业，同时使用老化机制避免饥饿。该值通过不区分大小写的枚举查找进行解析；无法识别的值将记录为错误并使用默认策略。此配置仅在 Query Queue V2 启用时影响行为，并与 V2 大小设置（如 `query_queue_v2_concurrency_level`）交互。
- 引入版本: v3.3.12, v3.4.2, v3.5.0

##### `semi_sync_collect_statistic_await_seconds`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: DML 操作（INSERT INTO 和 INSERT OVERWRITE 语句）期间半同步统计信息收集的最大等待时间。Stream Load 和 Broker Load 使用异步模式，不受此配置影响。如果统计信息收集时间超过此值，加载操作将继续，而不等待收集完成。此配置与 `enable_statistic_collect_on_first_load` 协同工作。
- 引入版本: v3.1

##### `slow_query_analyze_threshold`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 查询执行时间阈值，用于触发查询反馈分析。
- 引入版本: v3.4.0

##### `statistic_analyze_status_keep_second`

- 默认值: 3 * 24 * 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 保留收集任务历史记录的持续时间。默认值为 3 天。
- 引入版本: -

##### `statistic_auto_analyze_end_time`

- 默认值: 23:59:59
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 自动收集的结束时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

##### `statistic_auto_analyze_start_time`

- 默认值: 00:00:00
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 自动收集的开始时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

##### `statistic_auto_collect_ratio`

- 默认值: 0.8
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断自动收集统计信息是否健康的阈值。如果统计信息健康度低于此阈值，将触发自动收集。
- 引入版本: -

##### `statistic_auto_collect_small_table_rows`

- 默认值: 10000000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 自动收集期间，判断外部数据源（Hive、Iceberg、Hudi）中的表是否为小型表的阈值。如果表的行数小于此值，则认为该表为小型表。
- 引入版本: v3.2

##### `statistic_cache_columns`

- 默认值: 100000
- 类型: Long
- 单位: -
- 是否可变: No
- 描述: 统计信息表可缓存的行数。
- 引入版本: -

##### `statistic_cache_thread_pool_size`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 用于刷新统计信息缓存的线程池大小。
- 引入版本: -

##### `statistic_collect_interval_sec`

- 默认值: 5 * 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 自动收集期间检查数据更新的间隔。
- 引入版本: -

##### `statistic_max_full_collect_data_size`

- 默认值: 100 * 1024 * 1024 * 1024
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 统计信息自动收集的数据大小阈值。如果总大小超过此值，则执行采样收集而不是全量收集。
- 引入版本: -

##### `statistic_sample_collect_rows`

- 默认值: 200000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 在加载触发的统计信息操作期间，用于决定 SAMPLE 和 FULL 统计信息收集之间的行数阈值。如果加载或更改的行数超过此阈值（默认 200,000），则使用 SAMPLE 统计信息收集；否则，使用 FULL 统计信息收集。此设置与 `enable_statistic_collect_on_first_load` 和 `statistic_sample_collect_ratio_threshold_of_first_load` 协同工作。
- 引入版本: -

##### `statistic_update_interval_sec`

- 默认值: 24 * 60 * 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 统计信息缓存的更新间隔。
- 引入版本: -

##### `task_check_interval_second`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 任务后台作业执行之间的间隔。GlobalStateMgr 使用此值调度 TaskCleaner FrontendDaemon，该守护程序调用 `doTaskBackgroundJob()`；该值乘以 1000 以设置守护程序间隔（毫秒）。减小此值可使后台维护（任务清理、检查）运行更频繁并更快响应，但会增加 CPU/IO 开销；增加此值可减少开销，但会延迟清理和陈旧任务的检测。调整此值以平衡维护响应性和资源使用。
- 引入版本: v3.2.0

##### `task_min_schedule_interval_s`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: SQL 层检查的任务调度的最小允许调度间隔（以秒为单位）。提交任务时，TaskAnalyzer 将调度周期转换为秒，如果周期小于 `task_min_schedule_interval_s`，则以 `ERR_INVALID_PARAMETER` 拒绝提交。这可以防止创建运行过于频繁的任务，并保护调度器免受高频任务的影响。如果调度没有显式开始时间，TaskAnalyzer 会将开始时间设置为当前纪元秒。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `task_runs_timeout_second`

- 默认值: 4 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: TaskRun 的默认执行超时（以秒为单位）。此项用作 TaskRun 执行的基线超时。如果任务运行的属性包含会话变量 `query_timeout` 或 `insert_timeout` 且具有正整数值，则运行时使用会话超时和 `task_runs_timeout_second` 之间的较大值。有效超时随后被限制为不超过配置的 `task_runs_ttl_second` 和 `task_ttl_second`。设置此项以限制任务运行的执行时间。非常大的值可能会被任务/任务运行 TTL 设置截断。
- 引入版本: -

### 加载和卸载

##### `broker_load_default_timeout_second`

- 默认值: 14400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Broker Load 作业的超时时长。
- 引入版本: -

##### `desired_max_waiting_jobs`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 中待处理作业的最大数量。该数量指的是所有作业，例如表创建、加载和 schema 变更作业。如果 FE 中待处理作业的数量达到此值，FE 将拒绝新的加载请求。此参数仅对异步加载生效。从 v2.5 开始，默认值从 100 更改为 1024。
- 引入版本: -

##### `disable_load_job`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 集群发生错误时是否禁用加载。这可以防止因集群错误造成的任何损失。默认值为 `FALSE`，表示不禁用加载。`TRUE` 表示禁用加载，集群处于只读状态。
- 引入版本: -

##### `empty_load_as_error`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果没有数据加载，是否返回错误消息 "all partitions have no load data"。有效值：
  - `true`: 如果没有数据加载，系统会显示失败消息并返回错误 "all partitions have no load data"。
  - `false`: 如果没有数据加载，系统会显示成功消息并返回 OK，而不是错误。
- 引入版本: -

##### `enable_file_bundling`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为云原生表启用 File Bundling 优化。启用此功能 (设置为 `true`) 后，系统会自动捆绑加载、Compaction 或 Publish 操作生成的数据文件，从而降低因频繁访问外部存储系统而产生的 API 成本。您还可以使用 CREATE TABLE 属性 `file_bundling` 在表级别控制此行为。有关详细说明，请参阅 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。
- 引入版本: v4.0

##### `enable_routine_load_lag_metrics`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集 Routine Load Kafka 分区偏移量滞后指标。请注意，将此项设置为 `true` 将调用 Kafka API 以获取分区的最新偏移量。
- 引入版本: -

##### `enable_sync_publish`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在加载事务的发布阶段同步执行应用任务。此参数仅适用于 Primary Key 表。有效值：
  - `TRUE`（默认）：应用任务在加载事务的发布阶段同步执行。这意味着只有在应用任务完成后，加载事务才会被报告为成功，并且加载的数据才能真正被查询。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `true` 可以提高查询性能和稳定性，但可能会增加加载延迟。
  - `FALSE`: 应用任务在加载事务的发布阶段异步执行。这意味着在应用任务提交后，加载事务被报告为成功，但加载的数据不能立即被查询。在这种情况下，并发查询需要等待应用任务完成或超时才能继续。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `false` 可能会影响查询性能和稳定性。
- 引入版本: v3.2.0

##### `export_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 调度加载作业的时间间隔。
- 引入版本: -

##### `export_max_bytes_per_be_per_task`

- 默认值: 268435456
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 单个数据卸载任务从单个 BE 导出的最大数据量。
- 引入版本: -

##### `export_running_job_num_limit`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可并行运行的数据导出任务的最大数量。
- 引入版本: -

##### `export_task_default_timeout_second`

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 数据导出任务的超时时长。
- 引入版本: -

##### `export_task_pool_size`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 卸载任务线程池的大小。
- 引入版本: -

##### `external_table_commit_timeout_ms`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 提交（发布）写入事务到 StarRocks 外部表的超时时长。默认值 `10000` 表示 10 秒超时时长。
- 引入版本: -

##### `finish_transaction_default_lock_timeout_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 完成事务期间获取数据库和表锁的默认超时。
- 引入版本: v4.0.0, v3.5.8

##### `history_job_keep_max_second`

- 默认值: 7 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 历史作业（例如 schema 变更作业）可保留的最长时间。
- 引入版本: -

##### `insert_load_default_timeout_second`

- 默认值: 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 用于加载数据的 INSERT INTO 语句的超时时长。
- 引入版本: -

##### `label_clean_interval_second`

- 默认值: 4 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 标签清理的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保可以及时清理历史标签。
- 引入版本: -

##### `label_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在一段时间内可以保留的最大加载作业数。如果超过此数量，将删除历史作业信息。
- 引入版本: -

##### `label_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 已完成且处于 FINISHED 或 CANCELLED 状态的加载作业标签的最长保留时间（秒）。默认值为 3 天。此持续时间过后，标签将被删除。此参数适用于所有类型的加载作业。值过大会消耗大量内存。
- 引入版本: -

##### `load_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 加载作业滚动处理的时间间隔。
- 引入版本: -

##### `load_parallel_instance_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制为单个主机上的 broker 和 stream loads 创建的并行加载片段实例的数量。LoadPlanner 将此值用作每个主机的并行度，除非会话启用自适应 sink DOP；如果会话变量 `enable_adaptive_sink_dop` 为 true，则会话的 `sink_degree_of_parallelism` 将覆盖此配置。当需要 shuffle 时，此值应用于片段并行执行（扫描片段和 sink 片段并行执行实例）。当不需要 shuffle 时，它用作 sink 管道 DOP。注意：从本地文件加载被强制为单个实例（管道 DOP = 1，并行执行 = 1）以避免本地磁盘争用。增加此数字会提高每个主机的并发性和吞吐量，但可能会增加 CPU、内存和 I/O 争用。
- 引入版本: v3.2.0

##### `load_straggler_wait_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: BE 副本可容忍的最大加载延迟。如果超过此值，将执行克隆以从其他副本克隆数据。
- 引入版本: -

##### `loads_history_retained_days`

- 默认值: 30
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 内部 `_statistics_.loads_history` 表中加载历史记录的保留天数。此值用于表创建以设置表属性 `partition_live_number`，并传递给 `TableKeeper`（最小值为 1）以确定要保留多少个每日分区。增加或减少此值可调整完成的加载作业在每日分区中保留的时间；它会影响新表创建和 keeper 的清理行为，但不会自动重新创建过去的分区。`LoadsHistorySyncer` 在管理加载历史生命周期时依赖此保留；其同步节奏由 `loads_history_sync_interval_second` 控制。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### `loads_history_sync_interval_second`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: LoadsHistorySyncer 用于调度从 `information_schema.loads` 到内部 `_statistics_.loads_history` 表的周期性同步的间隔（以秒为单位）。该值在构造函数中乘以 1000 以设置 FrontendDaemon 间隔。同步器跳过第一次运行（以允许表创建），并且仅导入一分钟前完成的加载；较小的值会增加 DML 和执行器负载，而较大的值会延迟历史加载记录的可用性。有关目标表的保留/分区行为，请参阅 `loads_history_retained_days`。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### `max_broker_load_job_concurrency`

- 默认值: 5
- 别名: `async_load_task_pool_size`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StarRocks 集群中允许的最大并发 Broker Load 作业数。此参数仅对 Broker Load 有效。此参数的值必须小于 `max_running_txn_num_per_db` 的值。从 v2.5 开始，默认值从 `10` 更改为 `5`。
- 引入版本: -

##### `max_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 加载作业允许的最大超时时长。如果超过此限制，加载作业将失败。此限制适用于所有类型的加载作业。
- 引入版本: -

##### `max_routine_load_batch_size`

- 默认值: 4294967296
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: Routine Load 任务可加载的最大数据量。
- 引入版本: -

##### `max_routine_load_task_concurrent_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个 Routine Load 作业的最大并发任务数。
- 引入版本: -

##### `max_routine_load_task_num_per_be`

- 默认值: 16
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个 BE 上的最大并发 Routine Load 任务数。从 v3.1.0 开始，此参数的默认值从 5 增加到 16，并且不再需要小于或等于 BE 静态参数 `routine_load_thread_pool_size`（已弃用）的值。
- 引入版本: -

##### `max_running_txn_num_per_db`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StarRocks 集群中每个数据库允许运行的最大加载事务数。默认值为 `1000`。从 v3.1 开始，默认值从 `100` 更改为 `1000`。当数据库的实际运行加载事务数超过此参数的值时，新的加载请求将不会被处理。同步加载作业的新请求将被拒绝，异步加载作业的新请求将排队。不建议您增加此参数的值，因为这会增加系统负载。
- 引入版本: -

##### `max_stream_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Stream Load 作业允许的最大超时时长。
- 引入版本: -

##### `max_tolerable_backend_down_num`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许的最大故障 BE 节点数。如果超过此数量，Routine Load 作业无法自动恢复。
- 引入版本: -

##### `min_bytes_per_broker_scanner`

- 默认值: 67108864
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: Broker Load 实例可处理的最小数据量。
- 引入版本: -

##### `min_load_timeout_second`

- 默认值: 1
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 加载作业允许的最小超时时长。此限制适用于所有类型的加载作业。
- 引入版本: -

##### `min_routine_load_lag_for_metrics`

- 默认值: 10000
- 类型: INT
- 单位: -
- 是否可变: Yes
- 描述: 在监控指标中显示的 Routine Load 作业的最小偏移量滞后。偏移量滞后大于此值的 Routine Load 作业将显示在指标中。
- 引入版本: -

##### `period_of_auto_resume_min`

- 默认值: 5
- 类型: Int
- 单位: 分钟
- 是否可变: Yes
- 描述: Routine Load 作业自动恢复的间隔。
- 引入版本: -

##### `prepared_transaction_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 准备事务的默认超时时长。
- 引入版本: -

##### `routine_load_task_consume_second`

- 默认值: 15
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 集群中每个 Routine Load 任务消耗数据的最大时间。从 v3.1.0 开始，Routine Load 作业在 [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中支持一个新的参数 `task_consume_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本: -

##### `routine_load_task_timeout_second`

- 默认值: 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 集群中每个 Routine Load 任务的超时时长。从 v3.1.0 开始，Routine Load 作业在 [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中支持一个新的参数 `task_timeout_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本: -

##### `routine_load_unstable_threshold_second`

- 默认值: 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 如果 Routine Load 作业中的任何任务滞后，则将其设置为 UNSTABLE 状态。具体来说，消耗的消息时间戳与当前时间的差值超过此阈值，并且数据源中存在未消耗的消息。
- 引入版本: -

##### `spark_dpp_version`

- 默认值: 1.0.0
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 使用的 Spark 动态分区剪枝 (DPP) 版本。
- 引入版本: -

##### `spark_home_default_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Spark 客户端的根目录。
- 引入版本: -

##### `spark_launcher_log_dir`

- 默认值: `sys_log_dir` + "/spark_launcher_log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 Spark 日志文件的目录。
- 引入版本: -

##### `spark_load_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 每个 Spark Load 作业的超时时长。
- 引入版本: -

##### `spark_load_submit_timeout_second`

- 默认值: 300
- 类型: long
- 单位: 秒
- 是否可变: No
- 描述: 提交 Spark 应用程序后等待 YARN 响应的最大时间（秒）。`SparkLauncherMonitor.LogMonitor` 将此值转换为毫秒，如果作业在 UNKNOWN/CONNECTED/SUBMITTED 状态停留时间超过此超时，它将停止监控并强制杀死 Spark 启动器进程。`SparkLoadJob` 将此配置作为默认值读取，并允许通过 `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` 属性进行按加载覆盖。将其设置得足够高以适应 YARN 排队延迟；设置得过低可能会中止合法排队的作业，而设置得过高可能会延迟故障处理和资源清理。
- 引入版本: v3.2.0

##### `spark_resource_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Spark 依赖包的根目录。
- 引入版本: -

##### `stream_load_default_timeout_second`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 每个 Stream Load 作业的默认超时时长。
- 引入版本: -

##### `stream_load_max_txn_num_per_be`

- 默认值: -1
- 类型: Int
- 单位: 事务数
- 是否可变: Yes
- 描述: 限制从单个 BE（后端）主机接受的并发 Stream Load 事务数。当设置为非负整数时，FrontendServiceImpl 检查 BE（按客户端 IP）的当前事务计数，如果计数 `>=` 此限制，则拒绝新的 Stream Load 开始请求。值 `< 0` 禁用限制（无限制）。此检查发生在 Stream Load 开始期间，当超出限制时可能会导致 `streamload txn num per be exceeds limit` 错误。相关的运行时行为使用 `stream_load_default_timeout_second` 作为请求超时回退。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `stream_load_task_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StreamLoadMgr 在内存中保留的 Stream Load 任务的最大数量（全局范围，跨所有数据库）。当跟踪的任务数量 (`idToStreamLoadTask`) 超过此阈值时，StreamLoadMgr 首先调用 `cleanSyncStreamLoadTasks()` 删除已完成的同步 Stream Load 任务；如果大小仍然大于此阈值的一半，它会调用 `cleanOldStreamLoadTasks(true)` 强制删除较旧或已完成的任务。增加此值可在内存中保留更多任务历史记录；减少此值可减少内存使用并使清理更具侵略性。此值仅控制内存中的保留，不影响持久化/重放的任务。
- 引入版本: v3.2.0

##### `stream_load_task_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 已完成或取消的 Stream Load 任务的保留窗口。当任务达到最终状态且其结束时间戳早于此阈值（`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`）时，它就有资格由 `StreamLoadMgr.cleanOldStreamLoadTasks` 删除，并在加载持久化状态时被丢弃。适用于 `StreamLoadTask` 和 `StreamLoadMultiStmtTask`。如果总任务计数超过 `stream_load_task_keep_max_num`，清理可能会更早触发（同步任务由 `cleanSyncStreamLoadTasks` 优先处理）。设置此值以平衡历史/可调试性与内存使用。
- 引入版本: v3.2.0

##### `transaction_clean_interval_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 已完成事务清理的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保可以及时清理已完成的事务。
- 引入版本: -

##### `transaction_stream_load_coordinator_cache_capacity`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 存储事务标签到协调器节点映射的缓存容量。
- 引入版本: -

##### `transaction_stream_load_coordinator_cache_expire_seconds`

- 默认值: 900
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 在协调器映射被驱逐（TTL）之前，在缓存中保留的时间。
- 引入版本: -

##### `yarn_client_path`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Yarn 客户端包的根目录。
- 引入版本: -

##### `yarn_config_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 Yarn 配置文件的目录。
- 引入版本: -

### 统计报告

##### `enable_collect_warehouse_metrics`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，系统将收集并导出每个仓库的指标。启用它会将仓库级别的指标（槽位/使用率/可用性）添加到指标输出中，并增加指标基数和收集开销。禁用它可省略仓库特定指标，并减少 CPU/网络和监控存储成本。
- 引入版本: v3.5.0

##### `enable_http_detail_metrics`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，HTTP 服务器计算并暴露详细的 HTTP worker 指标（特别是 `HTTP_WORKER_PENDING_TASKS_NUM` gauge）。启用此功能会导致服务器迭代 Netty worker 执行器并调用每个 `NioEventLoop` 上的 `pendingTasks()` 以汇总待处理任务计数；禁用时，gauge 返回 0 以避免此成本。这种额外的收集可能会耗费 CPU 和延迟 — 仅在调试或详细调查时启用。
- 引入版本: v3.2.3

##### `proc_profile_collect_time_s`

- 默认值: 120
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 单次进程 profile 收集的持续时间（秒）。当 `proc_profile_cpu_enable` 或 `proc_profile_mem_enable` 设置为 `true` 时，AsyncProfiler 启动，收集器线程休眠此持续时间，然后 profiler 停止并写入 profile。较大的值会增加样本覆盖率和文件大小，但会延长 profiler 运行时并延迟后续收集；较小的值会减少开销，但可能会产生不足的样本。确保此值与 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 等保留设置对齐。
- 引入版本: v3.2.12

### 存储

##### `alter_table_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: schema 变更操作 (ALTER TABLE) 的超时时长。
- 引入版本: -

##### `capacity_used_percent_high_water`

- 默认值: 0.75
- 类型: double
- 单位: 分数 (0.0–1.0)
- 是否可变: Yes
- 描述: 计算后端负载分数时使用的磁盘容量使用百分比（总容量的百分比）的高水位阈值。`BackendLoadStatistic.calcSore` 使用 `capacity_used_percent_high_water` 设置 `LoadScore.capacityCoefficient`：如果后端的使用百分比小于 0.5，则系数等于 0.5；如果使用百分比 `>` `capacity_used_percent_high_water`，则系数 = 1.0；否则，系数通过 (2 * usedPercent - 0.5) 随使用百分比线性变化。当系数为 1.0 时，负载分数完全由容量比例决定；较低的值会增加副本计数的权重。调整此值会改变平衡器惩罚磁盘利用率高的后端的积极程度。
- 引入版本: v3.2.0

##### `catalog_trash_expire_second`

- 默认值: 86400
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 数据库、表或分区删除后，元数据可保留的最长时间。如果此持续时间过期，数据将被删除，并且无法通过 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 命令恢复。
- 引入版本: -

##### `check_consistency_default_timeout_second`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 副本一致性检查的超时时长。您可以根据 tablet 的大小设置此参数。
- 引入版本: -

##### `consistency_check_cooldown_time_second`

- 默认值: 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 控制同一 tablet 两次一致性检查之间的最小间隔（以秒为单位）。在 tablet 选择期间，只有当 `tablet.getLastCheckTime()` 小于 `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` 时，tablet 才被视为符合条件。默认值 (24 * 3600) 强制每个 tablet 大约每天检查一次，以减少后端磁盘 I/O。降低此值会增加检查频率和资源使用；提高此值会以更慢地检测不一致为代价减少 I/O。该值在从索引的 tablet 列表中过滤冷却的 tablet 时全局应用。
- 引入版本: v3.5.5

##### `consistency_check_end_time`

- 默认值: "4"
- 类型: String
- 单位: 一天中的小时 (0-23)
- 是否可变: No
- 描述: 指定 ConsistencyChecker 工作窗口的结束小时（一天中的小时）。该值使用 SimpleDateFormat("HH") 在系统时区中解析，并接受 0-23（一位或两位数）。StarRocks 将其与 `consistency_check_start_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口跨越午夜（例如，默认 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器从不运行。解析失败将导致 FE 启动记录错误并退出，因此请提供有效的小时字符串。
- 引入版本: v3.2.0

##### `consistency_check_start_time`

- 默认值: "23"
- 类型: String
- 单位: 一天中的小时 (00-23)
- 是否可变: No
- 描述: 指定 ConsistencyChecker 工作窗口的开始小时（一天中的小时）。该值使用 SimpleDateFormat("HH") 在系统时区中解析，并接受 0-23（一位或两位数）。StarRocks 将其与 `consistency_check_end_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口跨越午夜（例如，默认 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器从不运行。解析失败将导致 FE 启动记录错误并退出，因此请提供有效的小时字符串。
- 引入版本: v3.2.0

##### `consistency_tablet_meta_check_interval_ms`

- 默认值: 2 * 3600 * 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: ConsistencyChecker 用于在 `TabletInvertedIndex` 和 `LocalMetastore` 之间运行完整 tablet 元数据一致性扫描的间隔。`runAfterCatalogReady` 中的守护程序在 `current time - lastTabletMetaCheckTime` 超过此值时触发 checkTabletMetaConsistency。当首次检测到无效 tablet 时，其 `toBeCleanedTime` 设置为 `now + (consistency_tablet_meta_check_interval_ms / 2)`，因此实际删除会延迟到后续扫描。增加此值可减少扫描频率和负载（清理较慢）；减少此值可更快检测和删除陈旧 tablet（开销较高）。
- 引入版本: v3.2.0

##### `default_replication_num`

- 默认值: 3
- 类型: Short
- 单位: -
- 是否可变: Yes
- 描述: 设置在 StarRocks 中创建表时每个数据分区的默认副本数。此设置可以在创建表时通过在 CREATE TABLE DDL 中指定 `replication_num=x` 来覆盖。
- 引入版本: -

##### `enable_auto_tablet_distribution`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否自动设置 bucket 数量。
  - 如果此参数设置为 `TRUE`，您在创建表或添加分区时无需指定 bucket 数量。StarRocks 会自动确定 bucket 数量。
  - 如果此参数设置为 `FALSE`，您在创建表或添加分区时需要手动指定 bucket 数量。如果您在向表添加新分区时不指定 bucket 数量，新分区将继承表创建时设置的 bucket 数量。但是，您也可以手动为新分区指定 bucket 数量。
- 引入版本: v2.5.7

##### `enable_experimental_rowstore`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 [混合行存和列存](../../table_design/hybrid_table.md) 功能。
- 引入版本: v3.2.3

##### `enable_fast_schema_evolution`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 StarRocks 集群中的所有表启用快速 schema 演进。有效值为 `TRUE` 和 `FALSE`（默认）。启用快速 schema 演进可以提高 schema 变更的速度，并在添加或删除列时减少资源使用。
- 引入版本: v3.2.0

> **NOTE**
>
> - StarRocks 共享数据集群从 v3.3.0 开始支持此参数。
> - 如果您需要为特定表配置快速 schema 演进，例如禁用特定表的快速 schema 演进，您可以在表创建时设置表属性 [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution)。

##### `enable_online_optimize_table`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 在创建优化作业时是否使用非阻塞在线优化路径。当 `enable_online_optimize_table` 为 true 且目标表满足兼容性检查（无分区/键/排序规范，分布不是 `RandomDistributionDesc`，存储类型不是 `COLUMN_WITH_ROW`，已启用复制存储，且该表不是云原生表或物化视图）时，规划器会创建一个 `OnlineOptimizeJobV2` 以在不阻塞写入的情况下执行优化。如果为 false 或任何兼容性条件失败，StarRocks 将回退到 `OptimizeJobV2`，这可能会在优化期间阻塞写入操作。
- 引入版本: v3.3.3, v3.4.0, v3.5.0

##### `enable_strict_storage_medium_check`

- Default: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 在用户创建表时是否严格检查 BE 的存储介质。如果此参数设置为 `TRUE`，FE 会在用户创建表时检查 BE 的存储介质，如果 BE 的存储介质与 CREATE TABLE 语句中指定的 `storage_medium` 参数不同，则返回错误。例如，CREATE TABLE 语句中指定的存储介质是 SSD，但 BE 的实际存储介质是 HDD。因此，表创建失败。如果此参数为 `FALSE`，FE 在用户创建表时不会检查 BE 的存储介质。
- 引入版本: -

##### `max_bucket_number_per_partition`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个分区中可以创建的最大 bucket 数量。
- 引入版本: v3.3.2

##### `max_column_number_per_table`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个表中可以创建的最大列数。
- 引入版本: v3.3.2

##### `max_dynamic_partition_num`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 限制分析或创建动态分区表时可一次性创建的最大分区数。在动态分区属性验证期间，`systemtask_runs_max_history_number` 计算预期分区（结束偏移量 + 历史分区号），如果总数超过 `max_dynamic_partition_num`，则抛出 DDL 错误。仅当您预期合法的大分区范围时才增加此值；增加它允许创建更多分区，但会增加元数据大小、调度工作和操作复杂性。
- 引入版本: v3.2.0

##### `max_partition_number_per_table`

- 默认值: 100000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个表中可以创建的最大分区数。
- 引入版本: v3.3.2

##### `max_task_consecutive_fail_count`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 任务在调度器自动暂停它之前可能连续失败的最大次数。当 `TaskSource.MV.equals(task.getSource())` 且 `max_task_consecutive_fail_count` 大于 0 时，如果任务的连续失败计数达到或超过 `max_task_consecutive_fail_count`，任务将通过 TaskManager 暂停，并且对于物化视图任务，物化视图将失效。抛出异常，指示暂停以及如何重新激活（例如，`ALTER MATERIALIZED VIEW <mv_name> ACTIVE`）。将此项设置为 0 或负值以禁用自动暂停。
- 引入版本: -

##### `partition_recycle_retention_period_secs`

- 默认值: 1800
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: INSERT OVERWRITE 或物化视图刷新操作删除的分区的元数据保留时间。请注意，此类元数据无法通过执行 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 恢复。
- 引入版本: v3.5.9

##### `recover_with_empty_tablet`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否用空的 tablet 替换丢失或损坏的 tablet 副本。如果 tablet 副本丢失或损坏，对此 tablet 或其他健康 tablet 的数据查询可能会失败。用空的 tablet 替换丢失或损坏的 tablet 副本可确保查询仍能执行。但是，由于数据丢失，结果可能不正确。默认值为 `FALSE`，这意味着丢失或损坏的 tablet 副本不会被空的副本替换，并且查询失败。
- 引入版本: -

##### `storage_usage_hard_limit_percent`

- 默认值: 95
- 别名: `storage_flood_stage_usage_percent`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: BE 目录中存储使用率的硬限制（百分比）。如果 BE 存储目录的存储使用率（百分比）超过此值，并且剩余存储空间小于 `storage_usage_hard_limit_reserve_bytes`，则加载和恢复作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_usage_percent` 一起设置，以使配置生效。
- 引入版本: -

##### `storage_usage_hard_limit_reserve_bytes`

- 默认值: 100 * 1024 * 1024 * 1024
- 别名: `storage_flood_stage_left_capacity_bytes`
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: BE 目录中剩余存储空间的硬限制。如果 BE 存储目录中剩余存储空间小于此值，并且存储使用率（百分比）超过 `storage_usage_hard_limit_percent`，则加载和恢复作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_left_capacity_bytes` 一起设置，以使配置生效。
- 引入版本: -

##### `storage_usage_soft_limit_percent`

- 默认值: 90
- 别名: `storage_high_watermark_usage_percent`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: BE 目录中存储使用率的软限制（百分比）。如果 BE 存储目录的存储使用率（百分比）超过此值，并且剩余存储空间小于 `storage_usage_soft_limit_reserve_bytes`，则 tablet 无法克隆到此目录。
- 引入版本: -

##### `storage_usage_soft_limit_reserve_bytes`

- 默认值: 200 * 1024 * 1024 * 1024
- 别名: `storage_min_left_capacity_bytes`
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: BE 目录中剩余存储空间的软限制。如果 BE 存储目录中剩余存储空间小于此值，并且存储使用率（百分比）超过 `storage_usage_soft_limit_percent`，则 tablet 无法克隆到此目录。
- 引入版本: -

##### `tablet_checker_lock_time_per_cycle_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: tablet 检查器在释放并重新获取表锁之前，每个周期最大锁持有时间。小于 100 的值将被视为 100。
- 引入版本: v3.5.9, v4.0.2

##### `tablet_create_timeout_second`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 创建 tablet 的超时时长。从 v3.1 开始，默认值从 1 更改为 10。
- 引入版本: -

##### `tablet_delete_timeout_second`

- 默认值: 2
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 删除 tablet 的超时时长。
- 引入版本: -

##### `tablet_sched_balance_load_disk_safe_threshold`

- 默认值: 0.5
- 别名: `balance_load_disk_safe_threshold`
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断 BE 磁盘使用率是否平衡的百分比阈值。如果所有 BE 的磁盘使用率都低于此值，则认为平衡。如果磁盘使用率大于此值，并且最高和最低 BE 磁盘使用率之间的差异大于 10%，则认为磁盘使用率不平衡，并触发 tablet 重新平衡。
- 引入版本: -

##### `tablet_sched_balance_load_score_threshold`

- 默认值: 0.1
- 别名: `balance_load_score_threshold`
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断 BE 负载是否平衡的百分比阈值。如果 BE 的负载低于所有 BE 的平均负载，并且差异大于此值，则此 BE 处于低负载状态。相反，如果 BE 的负载高于平均负载，并且差异大于此值，则此 BE 处于高负载状态。
- 引入版本: -

##### `tablet_sched_be_down_tolerate_time_s`

- 默认值: 900
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 调度器允许 BE 节点保持非活动状态的最大持续时间。达到时间阈值后，该 BE 节点上的 tablet 将迁移到其他活动 BE 节点。
- 引入版本: v2.5.7

##### `tablet_sched_disable_balance`

- 默认值: false
- 别名: `disable_balance`
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否禁用 tablet 平衡。`TRUE` 表示禁用 tablet 平衡。`FALSE` 表示启用 tablet 平衡。
- 引入版本: -

##### `tablet_sched_disable_colocate_balance`

- 默认值: false
- 别名: `disable_colocate_balance`
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否禁用 Colocate 表的副本平衡。`TRUE` 表示禁用副本平衡。`FALSE` 表示启用副本平衡。
- 引入版本: -

##### `tablet_sched_max_balancing_tablets`

- 默认值: 500
- 别名: `max_balancing_tablets`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可同时平衡的最大 tablet 数量。如果超过此值，将跳过 tablet 重新平衡。
- 引入版本: -

##### `tablet_sched_max_clone_task_timeout_sec`

- 默认值: 2 * 60 * 60
- 别名: `max_clone_task_timeout_sec`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 克隆 tablet 的最大超时时长。
- 引入版本: -

##### `tablet_sched_max_not_being_scheduled_interval_ms`

- 默认值: 15 * 60 * 1000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 当 tablet 克隆任务正在调度时，如果 tablet 在此参数指定的时间内未被调度，StarRocks 会给予其更高的优先级，以便尽快调度。
- 引入版本: -

##### `tablet_sched_max_scheduling_tablets`

- 默认值: 10000
- 别名: `max_scheduling_tablets`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可同时调度的最大 tablet 数量。如果超过此值，将跳过 tablet 平衡和修复检查。
- 引入版本: -

##### `tablet_sched_min_clone_task_timeout_sec`

- 默认值: 3 * 60
- 别名: `min_clone_task_timeout_sec`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 克隆 tablet 的最小超时时长。
- 引入版本: -

##### `tablet_sched_num_based_balance_threshold_ratio`

- 默认值: 0.5
- 别名: -
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 基于数量的平衡可能会破坏磁盘大小平衡，但磁盘之间的最大差距不能超过 `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold`。如果集群中有 tablet 不断从 A 平衡到 B，又从 B 平衡到 A，请减小此值。如果您希望 tablet 分布更平衡，请增加此值。
- 引入版本: - 3.1

##### `tablet_sched_repair_delay_factor_second`

- 默认值: 60
- 别名: `tablet_repair_delay_factor_second`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 副本修复的间隔，单位为秒。
- 引入版本: -

##### `tablet_sched_slot_num_per_path`

- 默认值: 8
- 别名: `schedule_slot_num_per_path`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 单个 BE 存储目录中可并发运行的 tablet 相关任务的最大数量。从 v2.5 开始，此参数的默认值从 `4` 更改为 `8`。
- 引入版本: -

##### `tablet_sched_storage_cooldown_second`

- 默认值: -1
- 别名: `storage_cooldown_second`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 从表创建时间开始的自动冷却延迟。默认值 `-1` 表示禁用自动冷却。如果需要启用自动冷却，请将此参数设置为大于 `-1` 的值。
- 引入版本: -

##### `tablet_stat_update_interval_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: FE 从每个 BE 获取 tablet 统计信息的时间间隔。
- 引入版本: -

### 共享数据

##### `aws_s3_access_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 Access Key ID。
- 引入版本: v3.0

##### `aws_s3_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 endpoint，例如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本: v3.0

##### `aws_s3_external_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于跨账户访问 S3 存储桶的 AWS 账户的外部 ID。
- 引入版本: v3.0

##### `aws_s3_iam_role_arn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 对存储数据文件的 S3 存储桶具有权限的 IAM 角色的 ARN。
- 引入版本: v3.0

##### `aws_s3_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 S3 路径。它由 S3 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本: v3.0

##### `aws_s3_region`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: S3 存储桶所在的区域，例如 `us-west-2`。
- 引入版本: v3.0

##### `aws_s3_secret_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 Secret Access Key。
- 引入版本: v3.0

##### `aws_s3_use_aws_sdk_default_behavior`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用 AWS SDK 的默认认证凭据。有效值：true 和 false (默认)。
- 引入版本: v3.0

##### `aws_s3_use_instance_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用实例配置文件和 assumed role 作为访问 S3 的凭证方法。有效值：true 和 false (默认)。
  - 如果您使用基于 IAM 用户（Access Key 和 Secret Key）的凭证访问 S3，则必须将此项指定为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。
  - 如果您使用实例配置文件访问 S3，则必须将此项指定为 `true`。
  - 如果您使用 assumed role 访问 S3，则必须将此项指定为 `true`，并指定 `aws_s3_iam_role_arn`。
  - 如果您使用外部 AWS 账户，则还必须指定 `aws_s3_external_id`。
- 引入版本: v3.0

##### `azure_adls2_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Azure Data Lake Storage Gen2 账户的 endpoint，例如 `https://test.dfs.core.windows.net`。
- 引入版本: v3.4.1

##### `azure_adls2_oauth2_client_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的托管标识的客户端 ID。
- 引入版本: v3.4.4

##### `azure_adls2_oauth2_tenant_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的托管标识的租户 ID。
- 引入版本: v3.4.4

##### `azure_adls2_oauth2_use_managed_identity`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用托管标识授权 Azure Data Lake Storage Gen2 请求。
- 引入版本: v3.4.4

##### `azure_adls2_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Azure Data Lake Storage Gen2 路径。它由文件系统名称和目录名称组成，例如 `testfilesystem/starrocks`。
- 引入版本: v3.4.1

##### `azure_adls2_sas_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的共享访问签名 (SAS)。
- 引入版本: v3.4.1

##### `azure_adls2_shared_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的共享密钥。
- 引入版本: v3.4.1

##### `azure_blob_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Azure Blob Storage 账户的 endpoint，例如 `https://test.blob.core.windows.net`。
- 引入版本: v3.1

##### `azure_blob_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Azure Blob Storage 路径。它由存储账户中容器的名称和容器下的子路径（如果有）组成，例如 `testcontainer/subpath`。
- 引入版本: v3.1

##### `azure_blob_sas_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Blob Storage 请求的共享访问签名 (SAS)。
- 引入版本: v3.1

##### `azure_blob_shared_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Blob Storage 请求的共享密钥。
- 引入版本: v3.1

##### `azure_use_native_sdk`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否使用原生 SDK 访问 Azure Blob Storage，从而允许使用托管标识和服务主体进行身份验证。如果此项设置为 `false`，则仅允许使用共享密钥和 SAS Token 进行身份验证。
- 引入版本: v3.4.4

##### `cloud_native_hdfs_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本: -

##### `cloud_native_meta_port`

- 默认值: 6090
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 云原生元数据服务器 RPC 监听端口。
- 引入版本: -

##### `cloud_native_storage_type`

- 默认值: S3
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 您使用的对象存储类型。在共享数据模式下，StarRocks 支持将数据存储在 HDFS、Azure Blob（v3.1.1 起支持）、Azure Data Lake Storage Gen2（v3.4.1 起支持）、Google Storage（带原生 SDK，v3.5.1 起支持）以及与 S3 协议兼容的对象存储系统（如 AWS S3 和 MinIO）中。有效值：`S3`（默认）、`HDFS`、`AZBLOB`、`ADLS2` 和 `GS`。如果您将此参数指定为 `S3`，则必须添加以 `aws_s3` 为前缀的参数。如果您将此参数指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的参数。如果您将此参数指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的参数。如果您将此参数指定为 `GS`，则必须添加以 `gcp_gcs` 为前缀的参数。如果您将此参数指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本: -

##### `enable_load_volume_from_conf`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否允许 StarRocks 使用 FE 配置文件中指定的对象存储相关属性创建内置存储卷。从 v3.4.1 开始，默认值从 `true` 更改为 `false`。
- 引入版本: v3.1.0

##### `gcp_gcs_impersonation_service_account`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 如果您使用基于模拟的身份验证访问 Google Storage，则要模拟的服务账户。
- 引入版本: v3.5.1

##### `gcp_gcs_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Google Cloud 路径。它由 Google Cloud 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本: v3.5.1

##### `gcp_gcs_service_account_email`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的电子邮件地址，例如 `user@hello.iam.gserviceaccount.com`。
- 引入版本: v3.5.1

##### `gcp_gcs_service_account_private_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的私钥，例如 `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 引入版本: v3.5.1

##### `gcp_gcs_service_account_private_key_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的私钥 ID。
- 引入版本: v3.5.1

##### `gcp_gcs_use_compute_engine_service_account`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用绑定到 Compute Engine 的服务账户。
- 引入版本: v3.5.1

##### `hdfs_file_system_expire_seconds`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 由 HdfsFsManager 管理的未使用的缓存 HDFS/ObjectStore FileSystem 的存活时间（秒）。FileSystemExpirationChecker（每 60 秒运行一次）使用此值调用每个 HdfsFs.isExpired(...)；过期时，管理器关闭底层 FileSystem 并将其从缓存中删除。访问器方法（例如 `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`）更新最后访问时间戳，因此过期基于不活动。较低的值会减少空闲资源占用，但会增加重新打开的开销；较高的值会保持句柄更长时间，并可能消耗更多资源。
- 引入版本: v3.2.0

##### `lake_autovacuum_grace_period_minutes`

- 默认值: 30
- 类型: Long
- 单位: 分钟
- 是否可变: Yes
- 描述: 共享数据集群中保留历史数据版本的时间范围。在此时间范围内的历史数据版本不会在 Compactions 后通过 AutoVacuum 自动清理。您需要将此值设置得大于最大查询时间，以避免正在运行的查询访问的数据在查询完成之前被删除。从 v3.3.0、v3.2.5 和 v3.1.10 开始，默认值已从 `5` 更改为 `30`。
- 引入版本: v3.1.0

##### `lake_autovacuum_parallel_partitions`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 共享数据集群中可同时进行 AutoVacuum 的分区最大数量。AutoVacuum 是 Compactions 后的垃圾回收。
- 引入版本: v3.1.0

##### `lake_autovacuum_partition_naptime_seconds`

- 默认值: 180
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 共享数据集群中同一分区两次 AutoVacuum 操作之间的最小间隔。
- 引入版本: v3.1.0

##### `lake_autovacuum_stale_partition_threshold`

- 默认值: 12
- 类型: Long
- 单位: 小时
- 是否可变: Yes
- 描述: 如果分区在此时间范围内没有更新（加载、DELETE 或 Compactions），系统将不会对此分区执行 AutoVacuum。
- 引入版本: v3.1.0

##### `lake_compaction_allow_partial_success`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果此项设置为 `true`，则在共享数据集群中，当子任务之一成功时，系统将认为 Compaction 操作成功。
- 引入版本: v3.5.2

##### `lake_compaction_disable_ids`

- 默认值: ""
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据模式下禁用 Compaction 的表或分区列表。格式为 `tableId1;partitionId2`，以分号分隔，例如 `12345;98765`。
- 引入版本: v3.4.4

##### `lake_compaction_history_size`

- 默认值: 20
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据集群中，Leader FE 内存中保留的最近成功 Compaction 任务记录数。您可以使用 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录存储在 FE 进程内存中，如果 FE 进程重启，它将丢失。
- 引入版本: v3.1.0

##### `lake_compaction_max_tasks`

- 默认值: -1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中允许的最大并发 Compaction 任务数。将此项设置为 `-1` 表示以自适应方式计算并发任务数。将此值设置为 `0` 将禁用 Compaction。
- 引入版本: v3.1.0

##### `lake_compaction_score_selector_min_score`

- 默认值: 10.0
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 触发共享数据集群中 Compaction 操作的 Compaction Score 阈值。当分区的 Compaction Score 大于或等于此值时，系统会对该分区执行 Compaction。
- 引入版本: v3.1.0

##### `lake_compaction_score_upper_bound`

- 默认值: 2000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中分区的 Compaction Score 上限。`0` 表示无上限。此项仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。当分区的 Compaction Score 达到或超过此上限时，传入的加载任务将被拒绝。从 v3.3.6 开始，默认值从 `0` 更改为 `2000`。
- 引入版本: v3.2.0

##### `lake_enable_balance_tablets_between_workers`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据集群中，云原生表 tablet 迁移期间是否平衡计算节点之间的 tablet 数量。`true` 表示在计算节点之间平衡 tablet，`false` 表示禁用此功能。
- 引入版本: v3.3.4

##### `lake_enable_ingest_slowdown`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在共享数据集群中启用数据摄取减速。当数据摄取减速启用时，如果分区的 Compaction Score 超过 `lake_ingest_slowdown_threshold`，则该分区上的加载任务将受到限制。此配置仅在 `run_mode` 设置为 `shared_data` 时生效。从 v3.3.6 开始，默认值从 `false` 更改为 `true`。
- 引入版本: v3.2.0

##### `lake_ingest_slowdown_threshold`

- 默认值: 100
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 触发共享数据集群中数据摄取减速的 Compaction Score 阈值。此配置仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。
- 引入版本: v3.2.0

##### `lake_publish_version_max_threads`

- 默认值: 512
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中版本发布任务的最大线程数。
- 引入版本: v3.2.0

##### `meta_sync_force_delete_shard_meta`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许直接删除共享数据集群的元数据，绕过清理远程存储文件。建议仅在存在大量待清理分片，导致 FE JVM 内存压力过大时才将此项设置为 `true`。请注意，启用此功能后，属于分片或 tablet 的数据文件无法自动清理。
- 引入版本: v3.2.10, v3.3.3

##### `run_mode`

- 默认值: `shared_nothing`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing`（默认）。
  - `shared_data` 表示以共享数据模式运行 StarRocks。
  - `shared_nothing` 表示以共享无数据模式运行 StarRocks。

  > **CAUTION**
  >
  > - StarRocks 集群不能同时采用 `shared_data` 和 `shared_nothing` 模式。不支持混合部署。
  > - 集群部署后，请勿更改 `run_mode`。否则，集群将无法重启。不支持从共享无数据集群转换为共享数据集群，反之亦然。

- 引入版本: -

##### `shard_group_clean_threshold_sec`

- 默认值: 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: FE 清理共享数据集群中未使用的 tablet 和 shard 组的时间。在此阈值内创建的 tablet 和 shard 组将不会被清理。
- 引入版本: -

##### `star_mgr_meta_sync_interval_sec`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: FE 在共享数据集群中与 StarMgr 进行周期性元数据同步的间隔。
- 引入版本: -

##### `starmgr_grpc_server_max_worker_threads`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE starmgr 模块中 grpc 服务器使用的最大工作线程数。
- 引入版本: v4.0.0, v3.5.8

##### `starmgr_grpc_timeout_seconds`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述:
- 引入版本: -

### 数据湖

##### `files_enable_insert_push_down_schema`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，分析器将尝试将目标表 schema 推送到 `files()` 表函数中，用于 INSERT ... FROM files() 操作。这仅适用于源是 FileTableFunctionRelation、目标是原生表且 SELECT 列表中包含相应的 slot-ref 列（或 *）的情况。分析器会将选择的列与目标列匹配（计数必须匹配），短暂锁定目标表，并用非复杂类型（Parquet JSON 等复杂类型 `->` `array<varchar>` 会跳过）的深拷贝目标列类型替换文件列类型。原始文件表中的列名会保留。这减少了摄取期间文件类型推断引起的类型不匹配和松散性。
- 引入版本: v3.4.0, v3.5.0

##### `hdfs_read_buffer_size_kb`

- 默认值: 8192
- 类型: Int
- 单位: 千字节
- 是否可变: Yes
- 描述: HDFS 读取缓冲区的大小（以千字节为单位）。StarRocks 将此值转换为字节（`<< 10`），并用它来初始化 `HdfsFsManager` 中的 HDFS 读取缓冲区，并在不使用 broker 访问时填充发送给 BE 任务的 thrift 字段 `hdfs_read_buffer_size_kb`（例如 `TBrokerScanRangeParams`、`TDownloadReq`）。增加 `hdfs_read_buffer_size_kb` 可以提高顺序读取吞吐量并减少系统调用开销，但代价是每个流的内存使用量更高；减小它会减少内存占用，但可能会降低 I/O 效率。调整时请考虑工作负载（许多小流与少数大型顺序读取）。
- 引入版本: v3.2.0

##### `hdfs_write_buffer_size_kb`

- 默认值: 1024
- 类型: Int
- 单位: 千字节
- 是否可变: Yes
- 描述: 设置用于直接写入 HDFS 或对象存储时（不使用 broker）的 HDFS 写入缓冲区大小（以 KB 为单位）。FE 将此值转换为字节（`<< 10`），并初始化 HdfsFsManager 中的本地写入缓冲区，并在 Thrift 请求中传播（例如 TUploadReq、TExportSink、sink options），以便后端/代理使用相同的缓冲区大小。增加此值可以提高大型顺序写入的吞吐量，但代价是每个写入器占用更多内存；减小此值可以减少每个流的内存使用量，并可能降低小型写入的延迟。与 `hdfs_read_buffer_size_kb` 一起调整，并考虑可用内存和并发写入器。
- 引入版本: v3.2.0

##### `lake_batch_publish_max_version_num`

- 默认值: 10
- 类型: Int
- 单位: 计数
- 是否可变: Yes
- 描述: 设置在为 lake（云原生）表构建发布批次时，可能分组的连续事务版本的上限。该值传递给事务图批处理例程（参见 getReadyToPublishTxnListBatch），并与 `lake_batch_publish_min_version_num` 一起确定 TransactionStateBatch 的候选范围大小。较大的值可以通过批处理更多提交来提高发布吞吐量，但会增加原子发布的范围（更长的可见性延迟和更大的回滚表面），并且当版本不连续时可能会在运行时受到限制。根据工作负载和可见性/延迟要求进行调整。
- 引入版本: v3.2.0

##### `lake_batch_publish_min_version_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 设置构成 lake 表发布批次所需的最小连续事务版本数。DatabaseTransactionMgr.getReadyToPublishTxnListBatch 将此值与 `lake_batch_publish_max_version_num` 一起传递给 transactionGraph.getTxnsWithTxnDependencyBatch 以选择依赖事务。值为 `1` 允许单事务发布（不批处理）。值 `>1` 要求至少有相同数量的连续版本、单表、非复制事务可用；如果版本不连续，出现复制事务，或 schema 更改消耗版本，则批处理中止。增加此值可以通过分组提交来提高发布吞吐量，但可能会在等待足够连续的事务时延迟发布。
- 引入版本: v3.2.0

##### `lake_enable_batch_publish_version`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，PublishVersionDaemon 会为同一个 Lake（共享数据）表/分区批处理就绪事务，并将其版本一起发布，而不是为每个事务单独发布。在 RunMode shared-data 中，守护进程调用 getReadyPublishTransactionsBatch() 并使用 publishVersionForLakeTableBatch(...) 执行分组发布操作（减少 RPC 并提高吞吐量）。禁用时，守护进程回退到通过 publishVersionForLakeTable(...) 进行的逐事务发布。实现通过内部集合协调进行中的工作，以避免在切换开关时重复发布，并且受 `lake_publish_version_max_threads` 的线程池大小影响。
- 引入版本: v3.2.0

##### `lake_enable_tablet_creation_optimization`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，StarRocks 在共享数据模式下优化云原生表和物化视图的 tablet 创建，通过为物理分区下的所有 tablet 创建单个共享 tablet 元数据，而不是为每个 tablet 创建不同的元数据。这减少了表创建、rollup 和 schema 变更作业期间创建的 tablet 任务和元数据/文件数量。优化仅适用于云原生表/物化视图，并与 `file_bundling` 结合（后者重用相同的优化逻辑）。注意：schema 变更和 rollup 作业明确禁用使用 `file_bundling` 的表的优化，以避免使用相同名称的文件被覆盖。谨慎启用——它改变了创建的 tablet 元数据的粒度，并可能影响副本创建和文件命名行为。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `lake_use_combined_txn_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，系统允许 Lake 表使用组合事务日志路径进行相关事务。仅适用于共享数据集群。
- 引入版本: v3.3.7, v3.4.0, v3.5.0

##### `enable_iceberg_commit_queue`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 Iceberg 表启用提交队列以避免并发提交冲突。Iceberg 使用乐观并发控制 (OCC) 进行元数据提交。当多个线程同时提交到同一表时，可能会发生冲突，并出现诸如“无法提交：基元数据位置与当前表元数据位置不同”之类的错误。启用后，每个 Iceberg 表都有自己的单线程执行器用于提交操作，确保对同一表的提交是序列化的，并防止 OCC 冲突。不同的表可以并发提交，从而保持整体吞吐量。这是一项系统级优化，旨在提高可靠性，应默认启用。如果禁用，并发提交可能会因乐观锁定冲突而失败。
- 引入版本: v4.1.0

##### `iceberg_commit_queue_timeout_seconds`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 等待 Iceberg 提交操作完成的超时时间（秒）。当使用提交队列 (`enable_iceberg_commit_queue=true`) 时，每个提交操作必须在此超时时间内完成。如果提交时间超过此超时，它将被取消并引发错误。影响提交时间的因素包括：正在提交的数据文件数量、表的元数据大小、底层存储（例如 S3、HDFS）的性能。
- 引入版本: v4.1.0

##### `iceberg_commit_queue_max_size`

- 默认值: 1000
- 类型: Int
- 单位: 计数
- 是否可变: No
- 描述: 每个 Iceberg 表待处理提交操作的最大数量。当使用提交队列 (`enable_iceberg_commit_queue=true`) 时，这限制了可以为一个表排队的提交操作的数量。当达到限制时，额外的提交操作将在调用者线程中执行（阻塞直到容量可用）。此配置在 FE 启动时读取，并应用于新创建的表执行器。需要重启 FE 才能生效。如果您预期对同一表有许多并发提交，请增加此值。如果此值过低，在高并发期间提交可能会在调用者线程中阻塞。
- 引入版本: v4.1.0

### 其他

##### `agent_task_resend_wait_time_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: FE 在重新发送 agent 任务之前必须等待的持续时间。仅当任务创建时间与当前时间之间的间隔超过此参数的值时，才能重新发送 agent 任务。此参数用于防止重复发送 agent 任务。
- 引入版本: -

##### `allow_system_reserved_names`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许用户创建以 `__op` 和 `__row` 开头的列名。要启用此功能，请将此参数设置为 `TRUE`。请注意，这些名称格式在 StarRocks 中保留用于特殊目的，创建此类列可能导致未定义的行为。因此，此功能默认禁用。
- 引入版本: v3.2.0

##### `auth_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于 StarRocks 集群内部身份验证的令牌。如果未指定此参数，StarRocks 会在集群 Leader FE 首次启动时为集群生成一个随机令牌。
- 引入版本: -

##### `authentication_ldap_simple_bind_base_dn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器开始搜索用户身份验证信息的基准 DN。
- 引入版本: -

##### `authentication_ldap_simple_bind_root_dn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 用于搜索用户身份验证信息的管理员 DN。
- 引入版本: -

##### `authentication_ldap_simple_bind_root_pwd`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 用于搜索用户身份验证信息的管理员密码。
- 引入版本: -

##### `authentication_ldap_simple_server_host`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器运行的主机。
- 引入版本: -

##### `authentication_ldap_simple_server_port`

- 默认值: 389
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器的端口。
- 引入版本: -

##### `authentication_ldap_simple_user_search_attr`

- 默认值: uid
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 在 LDAP 对象中标识用户的属性名称。
- 引入版本: -

##### `backup_job_default_timeout_ms`

- 默认值: 86400 * 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 备份作业的超时时长。如果超过此值，备份作业将失败。
- 引入版本: -

##### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在 `SHOW PROC /BACKENDS/{id}` 命令中启用收集每个磁盘的 tablet 数量。
- 引入版本: v4.0.1, v3.5.8

##### `enable_colocate_restore`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 Colocate 表启用备份和恢复。`true` 表示为 Colocate 表启用备份和恢复，`false` 表示禁用。
- 引入版本: v3.2.10, v3.3.3

##### `enable_materialized_view_concurrent_prepare`

- 默认值: true
- 类型: Boolean
- 单位:
- 是否可变: Yes
- 描述: 是否并发准备物化视图以提高性能。
- 引入版本: v3.4.4

##### `enable_metric_calculator`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 指定是否启用用于定期收集指标的功能。有效值：`TRUE` 和 `FALSE`。`TRUE` 指定启用此功能，`FALSE` 指定禁用此功能。
- 引入版本: -

##### `enable_table_metrics_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 中是否导出表级别指标。禁用后，FE 将跳过导出表指标（如表扫描/加载计数器和表大小指标），但仍将计数器记录在内存中。
- 引入版本: -

##### `enable_mv_post_image_reload_cache`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 加载镜像后是否执行重新加载标志检查。如果对一个基本物化视图执行检查，则对于其他与之相关的物化视图则不需要。
- 引入版本: v3.5.0

##### `enable_mv_query_context_cache`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用查询级别物化视图重写缓存以提高查询重写性能。
- 引入版本: v3.3

##### `enable_mv_refresh_collect_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认对所有物化视图在刷新时启用 profile。
- 引入版本: v3.3.0

##### `enable_mv_refresh_extra_prefix_logging`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在日志中启用带有物化视图名称的前缀，以便更好地调试。
- 引入版本: v3.4.0

##### `enable_mv_refresh_query_rewrite`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在物化视图刷新期间启用重写查询，以便查询可以直接使用重写的物化视图而不是基表来提高查询性能。
- 引入版本: v3.3

##### `enable_trace_historical_node`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许系统跟踪历史节点。通过将此项设置为 `true`，您可以启用缓存共享功能，并允许系统在弹性伸缩期间选择正确的缓存节点。
- 引入版本: v3.5.1

##### `es_state_sync_interval_second`

- 默认值: 10
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: FE 获取 Elasticsearch 索引并同步 StarRocks 外部表元数据的时间间隔。
- 引入版本: -

##### `hive_meta_cache_refresh_interval_s`

- 默认值: 3600 * 2
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: Hive 外部表缓存元数据的更新时间间隔。
- 引入版本: -

##### `hive_meta_store_timeout_s`

- 默认值: 10
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: 连接 Hive metastore 的超时时长。
- 引入版本: -

##### `jdbc_connection_idle_timeout_ms`

- 默认值: 600000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 访问 JDBC Catalog 的连接超时最长时间。超时连接被视为空闲。
- 引入版本: -

##### `jdbc_connection_timeout_ms`

- 默认值: 10000
- 类型: Long
- 单位: 毫秒
- 是否可变: No
- 描述: HikariCP 连接池获取连接的超时时间（毫秒）。如果在此时限内无法从池中获取连接，则操作将失败。
- 引入版本: v3.5.13

##### `jdbc_query_timeout_ms`

- 默认值: 30000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: JDBC 语句查询执行的超时时间（毫秒）。此超时应用于通过 JDBC Catalog 执行的所有 SQL 查询（例如，分区元数据查询）。该值在传递给 JDBC 驱动程序时转换为秒。
- 引入版本: v3.5.13

##### `jdbc_network_timeout_ms`

- 默认值: 30000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: JDBC 网络操作（套接字读取）的超时时间（毫秒）。此超时应用于数据库元数据调用（例如，getSchemas()、getTables()、getColumns()），以防止外部数据库无响应时无限期阻塞。
- 引入版本: v3.5.13

##### `jdbc_connection_pool_size`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 访问 JDBC Catalog 的 JDBC 连接池的最大容量。
- 引入版本: -

##### `jdbc_meta_default_cache_enable`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: JDBC Catalog 元数据缓存是否启用的默认值。设置为 True 时，新创建的 JDBC Catalog 将默认启用元数据缓存。
- 引入版本: -

##### `jdbc_meta_default_cache_expire_sec`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: JDBC Catalog 元数据缓存的默认过期时间。当 `jdbc_meta_default_cache_enable` 设置为 true 时，新创建的 JDBC Catalog 将默认设置元数据缓存的过期时间。
- 引入版本: -

##### `jdbc_minimum_idle_connections`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 访问 JDBC Catalog 的 JDBC 连接池中的最小空闲连接数。
- 引入版本: -

##### `jwt_jwks_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下公共密钥本地文件的路径。
- 引入版本: v3.5.0

##### `jwt_principal_field`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中指示 subject (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本: v3.5.0

##### `jwt_required_audience`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 audience (`aud`) 的字符串列表。只有当列表中一个值与 JWT audience 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

##### `jwt_required_issuer`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 issuer (`iss`) 的字符串列表。只有当列表中一个值与 JWT issuer 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

##### locale

- 默认值: `zh_CN.UTF-8`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 使用的字符集。
- 引入版本: -

##### `max_agent_task_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: agent 任务线程池中允许的最大线程数。
- 引入版本: -

##### `max_download_task_per_be`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在每个 RESTORE 操作中，StarRocks 分配给 BE 节点的最大下载任务数。当此项设置为小于或等于 0 时，任务数量不受限制。
- 引入版本: v3.1.0

##### `max_mv_check_base_table_change_retry_times`

- 默认值: 10
- 类型: -
- 单位: -
- 是否可变: Yes
- 描述: 刷新物化视图时检测基表变更的最大重试次数。
- 引入版本: v3.3.0

##### `max_mv_refresh_failure_retry_times`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图刷新失败时的最大重试次数。
- 引入版本: v3.3.0

##### `max_mv_refresh_try_lock_failure_retry_times`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图刷新失败时尝试锁定的最大重试次数。
- 引入版本: v3.3.0

##### `max_small_file_number`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可存储在 FE 目录中的小型文件最大数量。
- 引入版本: -

##### `max_small_file_size_bytes`

- 默认值: 1024 * 1024
- 类型: Int
- 单位: 字节
- 是否可变: Yes
- 描述: 小型文件的最大大小。
- 引入版本: -

##### `max_upload_task_per_be`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在每个 BACKUP 操作中，StarRocks 分配给 BE 节点的最大上传任务数。当此项设置为小于或等于 0 时，任务数量不受限制。
- 引入版本: v3.1.0

##### `mv_create_partition_batch_interval_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在物化视图刷新期间，如果需要批量创建多个分区，系统会将其分为每批 64 个分区。为降低频繁创建分区导致失败的风险，每个批次之间设置了默认间隔（以毫秒为单位）以控制创建频率。
- 引入版本: v3.3

##### `mv_plan_cache_max_size`

- 默认值: 1000
- 类型: Long
- 单位:
- 是否可变: Yes
- 描述: 物化视图计划缓存的最大大小（用于物化视图重写）。如果有很多物化视图用于透明查询重写，您可以增加此值。
- 引入版本: v3.2

##### `mv_plan_cache_thread_pool_size`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图计划缓存的默认线程池大小（用于物化视图重写）。
- 引入版本: v3.2

##### `mv_refresh_default_planner_optimize_timeout`

- 默认值: 30000
- 类型: -
- 单位: -
- 是否可变: Yes
- 描述: 刷新物化视图时优化器规划阶段的默认超时。
- 引入版本: v3.3.0

##### `mv_refresh_fail_on_filter_data`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果刷新过程中存在过滤数据，物化视图刷新失败，默认为 true，否则忽略过滤数据并返回成功。
- 引入版本: -

##### `mv_refresh_try_lock_timeout_ms`

- 默认值: 30000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 物化视图刷新尝试对其基表/物化视图进行数据库锁定的默认尝试锁定超时。
- 引入版本: v3.3.0

##### `oauth2_auth_server_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 授权 URL。用户浏览器将被重定向到此 URL，以开始 OAuth 2.0 授权过程。
- 引入版本: v3.5.0

##### `oauth2_client_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 客户端的公共标识符。
- 引入版本: v3.5.0

##### `oauth2_client_secret`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 StarRocks 客户端与授权服务器通信的密钥。
- 引入版本: v3.5.0

##### `oauth2_jwks_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
- 引入版本: v3.5.0

##### `oauth2_principal_field`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中指示 subject (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本: v3.5.0

##### `oauth2_redirect_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: OAuth 2.0 认证成功后，用户浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要将其配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- 引入版本: v3.5.0

##### `oauth2_required_audience`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 audience (`aud`) 的字符串列表。只有当列表中一个值与 JWT audience 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

##### `oauth2_required_issuer`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 issuer (`iss`) 的字符串列表。只有当列表中一个值与 JWT issuer 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

##### `oauth2_token_server_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 授权服务器上 StarRocks 获取访问令牌的 endpoint 的 URL。
- 引入版本: v3.5.0

##### `plugin_dir`

- 默认值: `System.getenv("STARROCKS_HOME")` + "/plugins"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储插件安装包的目录。
- 引入版本: -

##### `plugin_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许在 FE 上安装插件。插件只能在 Leader FE 上安装或卸载。
- 引入版本: -

##### `proc_profile_jstack_depth`

- 默认值: 128
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 系统收集 CPU 和内存 profile 时的最大 Java 堆栈深度。此值控制每个采样堆栈捕获的 Java 堆栈帧数：较大的值会增加跟踪详细信息和输出大小，并可能增加 profiling 开销，而较小的值会减少详细信息。此设置在 CPU 和内存 profiling 启动 profiler 时使用，因此请根据诊断需求和性能影响进行调整。
- 引入版本: -

##### `proc_profile_mem_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用进程内存分配 profile 收集。当此项设置为 `true` 时，系统会在 `sys_log_dir/proc_profile` 下生成名为 `mem-profile-<timestamp>.html` 的 HTML profile，在采样期间休眠 `proc_profile_collect_time_s` 秒，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的文件会根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 进行压缩和清除。原生提取路径使用 `STARROCKS_HOME_DIR` 以避免 `/tmp` noexec 问题。此项旨在用于故障排除内存分配热点。启用它会增加 CPU、I/O 和磁盘使用率，并可能生成大文件。
- 引入版本: v3.2.12

##### `query_detail_explain_level`

- 默认值: COSTS
- 类型: String
- 单位: -
- 是否可变: true
- 描述: EXPLAIN 语句返回的查询计划的详细级别。有效值：COSTS, NORMAL, VERBOSE。
- 引入版本: v3.2.12, v3.3.5

##### `replication_interval_ms`

- 默认值: 100
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 调度复制任务的最小时间间隔。
- 引入版本: v3.3.5

##### `replication_max_parallel_data_size_mb`

- 默认值: 1048576
- 类型: Int
- 单位: MB
- 是否可变: Yes
- 描述: 允许并发同步的最大数据大小。
- 引入版本: v3.3.5

##### `replication_max_parallel_replica_count`

- 默认值: 10240
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许并发同步的 tablet 副本的最大数量。
- 引入版本: v3.3.5

##### `replication_max_parallel_table_count`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许的最大并发数据同步任务数。StarRocks 为每张表创建一个同步任务。
- 引入版本: v3.3.5

##### `replication_transaction_timeout_sec`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 同步任务的超时时长。
- 引入版本: v3.3.5

##### `skip_whole_phase_lock_mv_limit`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 何时对具有相关物化视图的表应用“无锁”优化。当此项设置为小于 0 时，系统始终应用无锁优化，并且不为查询复制相关物化视图（减少 FE 内存使用和元数据复制/锁争用，但可能增加元数据并发问题的风险）。当设置为 0 时，禁用无锁优化（系统始终使用安全的复制和锁定路径）。当设置为大于 0 时，仅当相关物化视图的数量小于或等于配置阈值时才应用无锁优化。此外，当值大于或等于 0 时，规划器会将查询 OLAP 表记录到优化器上下文中以启用与物化视图相关的重写路径；当值小于 0 时，此步骤将被跳过。
- 引入版本: v3.2.1

##### `small_file_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/small_files"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 小型文件的根目录。
- 引入版本: -

##### `task_runs_max_history_number`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在内存中保留的任务运行记录的最大数量，并用作查询存档任务运行历史记录时的默认 LIMIT。当 `enable_task_history_archive` 为 false 时，此值限制内存中的历史记录：强制 GC 会修剪较旧的条目，因此只保留最新的 `task_runs_max_history_number`。当查询存档历史记录时（且未提供显式 LIMIT），如果此值大于 0，`TaskRunHistoryTable.lookup` 将使用 `"ORDER BY create_time DESC LIMIT <value>"`。注意：将此值设置为 0 会禁用查询侧的 LIMIT（无上限），但会导致内存中的历史记录被截断为零（除非启用了存档）。
- 引入版本: v3.2.0

##### `tmp_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/temp_dir"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储临时文件的目录，例如备份和恢复过程中生成的文件。这些过程完成后，生成的临时文件将被删除。
- 引入版本: -

##### `transform_type_prefer_string_for_varchar`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在物化视图创建和 CTAS 操作中，是否更倾向于为固定长度的 varchar 列使用 string 类型。
- 引入版本: v4.0.0

<EditionSpecificFEItem />
