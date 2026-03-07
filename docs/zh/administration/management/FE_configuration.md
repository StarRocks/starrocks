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

FE 启动后，您可以在 MySQL 客户端上运行 ADMIN SHOW FRONTEND CONFIG 命令来检查参数配置。如果您想查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须拥有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 配置或修改 FE 动态参数的设置。

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
- 是否可变: 否
- 描述: 审计日志文件的保留期限。默认值 `30d` 表示每个审计日志文件可以保留 30 天。StarRocks 会检查每个审计日志文件，并删除 30 天前生成的日志文件。
- 引入版本: -

##### `audit_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 存储审计日志文件的目录。
- 引入版本: -

##### `audit_log_enable_compress`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: 否
- 描述: 当设置为 true 时，生成的 Log4j2 配置会在轮转的审计日志文件名 (fe.audit.log.*) 后附加 ".gz" 后缀，以便 Log4j2 在日志轮转时生成压缩的 (.gz) 归档审计日志文件。此设置在 FE 启动时由 Log4jConfig.initLogging 读取，并应用于审计日志的 RollingFile appender；它只影响轮转/归档文件，不影响活动的审计日志。由于该值在启动时初始化，因此更改它需要重启 FE 才能生效。请与审计日志轮转设置 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) 一起使用。
- 引入版本: 3.2.12

##### `audit_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: 是
- 描述: 当设置为 true 时，FE 审计事件将以结构化 JSON 格式（Jackson ObjectMapper 序列化带注解的 AuditEvent 字段的 Map）发出，而不是默认的管道分隔的 "key=value" 字符串。此设置影响 AuditLogBuilder 处理的所有内置审计接收器：连接审计、查询审计、大查询审计（当事件符合条件时，大查询阈值字段会添加到 JSON 中）和慢查询审计输出。针对大查询阈值注解的字段和 "features" 字段会特殊处理（从普通审计条目中排除；根据适用情况包含在大查询或特性日志中）。启用此功能可使日志易于日志收集器或 SIEMs 进行机器解析；请注意，它会更改日志格式，可能需要更新任何期望旧版管道分隔格式的现有解析器。
- 引入版本: 3.2.7

##### `audit_log_modules`

- 默认值: `slow_query`, query
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: StarRocks 生成审计日志条目的模块。默认情况下，StarRocks 为 `slow_query` 模块和 `query` 模块生成审计日志。`connection` 模块从 v3.0 版本开始支持。模块名称之间用逗号 (,) 和空格分隔。
- 引入版本: -

##### `audit_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: StarRocks 轮转审计日志条目的时间间隔。有效值: `DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则审计日志文件名称中会添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则审计日志文件名称中会添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `audit_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 在 `audit_log_roll_interval` 参数指定的每个保留期内，可以保留的审计日志文件的最大数量。
- 引入版本: -

##### `bdbje_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制 StarRocks 中 Berkeley DB Java Edition (BDB JE) 使用的日志级别。在 BDB 环境初始化期间，BDBEnvironment.initConfigs() 将此值应用于 `com.sleepycat.je` 包的 Java 日志记录器和 BDB JE 环境文件日志级别 (`EnvironmentConfig.FILE_LOGGING_LEVEL`)。接受标准的 java.util.logging.Level 名称，例如 SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF。设置为 ALL 将启用所有日志消息。增加详细程度会提高日志量，并可能影响磁盘 I/O 和性能；该值在 BDB 环境初始化时读取，因此仅在环境（重新）初始化后生效。
- 引入版本: v3.2.0

##### `big_query_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制 FE 大查询日志文件 (`fe.big_query.log.*`) 在自动删除前保留多长时间。该值作为 IfLastModified age 传递给 Log4j 的删除策略——任何最后修改时间早于此值的轮转大查询日志都将被删除。支持的后缀包括 `d`（天）、`h`（小时）、`m`（分钟）和 `s`（秒）。示例: `7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）和 `120s`（120 秒）。此项与 `big_query_log_roll_interval` 和 `big_query_log_roll_num` 协同工作，以确定哪些文件被保留或清除。
- 引入版本: v3.2.0

##### `big_query_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: FE 写入大查询转储日志 (`fe.big_query.log.*`) 的目录。Log4j 配置使用此路径为 `fe.big_query.log` 及其轮转文件创建 RollingFile appender。轮转和保留由 `big_query_log_roll_interval`（基于时间的后缀）、`log_roll_size_mb`（大小触发器）、`big_query_log_roll_num`（最大文件数）和 `big_query_log_delete_age`（基于年龄的删除）控制。对于超过用户定义阈值（例如 `big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold` 或 `big_query_log_scan_bytes_threshold`）的查询，会记录大查询记录。使用 `big_query_log_modules` 控制哪些模块记录到此文件。
- 引入版本: v3.2.0

##### `big_query_log_modules`

- 默认值: `{"query"}`
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: 启用每个模块大查询日志记录的模块名称后缀列表。典型值为逻辑组件名称。例如，默认的 `query` 会生成 `big_query.query`。
- 引入版本: v3.2.0

##### `big_query_log_roll_interval`

- 默认值: `"DAY"`
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 指定用于构建 `big_query` 日志 appender 轮转文件名日期组件的时间间隔。有效值（不区分大小写）为 `DAY`（默认）和 `HOUR`。`DAY` 生成每日模式 (`"%d{yyyyMMdd}"`)，`HOUR` 生成每小时模式 (`"%d{yyyyMMddHH}"`)。该值与基于大小的轮转 (`big_query_roll_maxsize`) 和基于索引的轮转 (`big_query_log_roll_num`) 结合，形成 RollingFile 的 filePattern。无效值会导致日志配置生成失败 (IOException)，并可能阻止日志初始化或重新配置。请与 `big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num` 和 `big_query_log_delete_age` 一起使用。
- 引入版本: v3.2.0

##### `big_query_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 每个 `big_query_log_roll_interval` 保留的轮转 FE 大查询日志文件的最大数量。此值绑定到 RollingFile appender 的 DefaultRolloverStrategy `max` 属性，用于 `fe.big_query.log`；当日志轮转时（按时间或按 `log_roll_size_mb`），StarRocks 最多保留 `big_query_log_roll_num` 个带索引的文件（filePattern 使用时间后缀加索引）。超过此计数的文件可能会被轮转删除，`big_query_log_delete_age` 还可以根据最后修改时间删除文件。
- 引入版本: v3.2.0

##### `dump_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 转储日志文件的保留期限。默认值 `7d` 表示每个转储日志文件可以保留 7 天。StarRocks 会检查每个转储日志文件，并删除 7 天前生成的日志文件。
- 引入版本: -

##### `dump_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 存储转储日志文件的目录。
- 引入版本: -

##### `dump_log_modules`

- 默认值: query
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: StarRocks 生成转储日志条目的模块。默认情况下，StarRocks 为 query 模块生成转储日志。模块名称之间用逗号 (,) 和空格分隔。
- 引入版本: -

##### `dump_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: StarRocks 轮转转储日志条目的时间间隔。有效值: `DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则转储日志文件名称中会添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则转储日志文件名称中会添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `dump_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 在 `dump_log_roll_interval` 参数指定的每个保留期内，可以保留的转储日志文件的最大数量。
- 引入版本: -

##### `edit_log_write_slow_log_threshold_ms`

- 默认值: 2000
- 类型: Int
- 单位: 毫秒
- 是否可变: 是
- 描述: JournalWriter 用于检测和记录慢速编辑日志批次写入的阈值（单位：毫秒）。在批次提交后，如果批次持续时间超过此值，JournalWriter 将发出 WARN 消息，其中包含批次大小、持续时间和当前日志队列大小（限速为大约每 2 秒一次）。此设置仅控制 FE Leader 上潜在的 IO 或复制延迟的日志记录/警报；它不改变提交或轮转行为（参见 `edit_log_roll_num` 和与提交相关的设置）。无论此阈值如何，指标更新仍会发生。
- 引入版本: v3.2.3

##### `enable_audit_sql`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 否
- 描述: 当此项设置为 `true` 时，FE 审计子系统会将 ConnectProcessor 处理的语句的 SQL 文本记录到 FE 审计日志 (`fe.audit.log`) 中。存储的语句遵循其他控制：加密语句会被编辑 (`AuditEncryptionChecker`)，如果设置了 `enable_sql_desensitize_in_log`，敏感凭据可能会被编辑或脱敏，摘要记录由 `enable_sql_digest` 控制。当设置为 `false` 时，ConnectProcessor 会在审计事件中将语句文本替换为 "?"——其他审计字段（用户、主机、持续时间、状态、通过 `qe_slow_log_ms` 进行的慢查询检测以及指标）仍会被记录。启用 SQL 审计会增加取证和故障排除的可见性，但可能会暴露敏感的 SQL 内容并增加日志量和 I/O；禁用它会提高隐私性，但代价是审计日志中会丢失完整的语句可见性。
- 引入版本: -

##### `enable_profile_log`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 否
- 描述: 是否启用 Profile 日志记录。启用此功能后，FE 会将每个查询的 Profile 日志（由 `ProfileManager` 生成的序列化 `queryDetail` JSON）写入 Profile 日志接收器。仅当 `enable_collect_query_detail_info` 也启用时才执行此日志记录；当 `enable_profile_log_compress` 启用时，JSON 可能会在记录前进行 gzip 压缩。Profile 日志文件由 `profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` 管理，并根据 `profile_log_delete_age` 进行轮转/删除（支持 `7d`、`10h`、`60m`、`120s` 等格式）。禁用此功能会停止写入 Profile 日志（减少磁盘 I/O、压缩 CPU 和存储使用）。
- 引入版本: v3.2.5

##### `enable_qe_slow_log`

- 默认值: true
- 类型: Boolean
- 单位: N/A
- 是否可变: 是
- 描述: 启用后，FE 内置审计插件 (AuditLogBuilder) 会将执行时间（"Time" 字段）超过 `qe_slow_log_ms` 配置阈值的查询事件写入慢查询审计日志 (AuditLog.getSlowAudit)。如果禁用，这些慢查询条目将被抑制（常规查询和连接审计日志不受影响）。慢查询审计条目遵循全局 `audit_log_json_format` 设置（JSON 或纯字符串）。使用此标志可以独立于常规审计日志记录来控制慢查询审计量的生成；当 `qe_slow_log_ms` 较低或工作负载产生许多长时间运行的查询时，关闭它可以减少日志 I/O。
- 引入版本: 3.2.11

##### `enable_sql_desensitize_in_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 否
- 描述: 当此项设置为 `true` 时，系统会在敏感 SQL 内容写入日志和查询详情记录之前对其进行替换或隐藏。遵循此配置的代码路径包括 ConnectProcessor.formatStmt（审计日志）、StmtExecutor.addRunningQueryDetail（查询详情）和 SimpleExecutor.formatSQL（内部执行器日志）。启用此功能后，无效 SQL 可能会被替换为固定的脱敏消息，凭据（用户/密码）会被隐藏，并且 SQL 格式化程序需要生成一个清理后的表示（它还可以启用摘要式输出）。这减少了审计/内部日志中敏感字面量和凭据的泄露，但也意味着日志和查询详情不再包含原始的完整 SQL 文本（这可能会影响重放或调试）。
- 引入版本: -

##### `internal_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 指定 FE 内部日志文件（写入 `internal_log_dir`）的保留期限。该值是一个持续时间字符串。支持的后缀: `d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。示例: `7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。此项作为 `<IfLastModified age="..."/>` 谓词替换到 log4j 配置中，由 RollingFile Delete 策略使用。在日志轮转期间，最后修改时间早于此持续时间的文件将被删除。增加此值可以更快地释放磁盘空间，或减少此值以更长时间地保留内部物化视图或统计日志。
- 引入版本: v3.2.4

##### `internal_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: FE 日志子系统用于存储内部日志 (`fe.internal.log`) 的目录。此配置被替换到 Log4j 配置中，并决定 InternalFile appender 写入内部/物化视图/统计日志的位置，以及 `internal.<module>` 下的每个模块日志记录器放置其文件的位置。确保目录存在、可写且有足够的磁盘空间。此目录中文件的日志轮转和保留由 `log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age` 和 `internal_log_roll_interval` 控制。如果 `sys_log_to_console` 启用，内部日志可能会写入控制台而不是此目录。
- 引入版本: v3.2.4

##### `internal_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 当此项设置为 `true` 时，内部统计/审计条目将作为紧凑的 JSON 对象写入统计审计日志记录器。JSON 包含键 "executeType" (InternalType: QUERY 或 DML)、"queryId"、"sql" 和 "time"（经过的毫秒数）。当设置为 `false` 时，相同的信息将作为单个格式化文本行 ("statistic execute: ... | QueryId: [...] | SQL: ...") 记录。启用 JSON 改进了机器解析和与日志处理器的集成，但也会导致原始 SQL 文本包含在日志中，这可能会暴露敏感信息并增加日志大小。
- 引入版本: -

##### `internal_log_modules`

- 默认值: `{"base", "statistic"}`
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: 将接收专用内部日志记录的模块标识符列表。对于每个条目 X，Log4j 会创建一个名为 `internal.<X>` 的日志记录器，其级别为 INFO 且 additivity="false"。这些日志记录器被路由到内部 appender（写入 `fe.internal.log`），或者当 `sys_log_to_console` 启用时路由到控制台。根据需要使用短名称或包片段——确切的日志记录器名称变为 `internal.` + 配置的字符串。内部日志文件轮转和保留遵循 `internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval` 和 `log_roll_size_mb`。添加模块会导致其运行时消息分离到内部日志流中，以便于调试和审计。
- 引入版本: v3.2.4

##### `internal_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制 FE 内部日志 appender 的基于时间的轮转间隔。接受的值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成每小时文件模式 (`"%d{yyyyMMddHH}"`)，`DAY` 生成每日文件模式 (`"%d{yyyyMMdd}"`)，这些模式由 RollingFile TimeBasedTriggeringPolicy 用于命名轮转的 `fe.internal.log` 文件。无效值会导致初始化失败（在构建活动 Log4j 配置时抛出 IOException）。轮转行为还取决于相关设置，例如 `internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num` 和 `internal_log_delete_age`。
- 引入版本: v3.2.4

##### `internal_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 内部 appender (`fe.internal.log`) 保留的轮转内部 FE 日志文件的最大数量。此值用作 Log4j DefaultRolloverStrategy 的 `max` 属性；当发生轮转时，StarRocks 最多保留 `internal_log_roll_num` 个归档文件并删除旧文件（也受 `internal_log_delete_age` 控制）。较低的值会减少磁盘使用量但缩短日志历史记录；较高的值会保留更多历史内部日志。此项与 `internal_log_dir`、`internal_log_roll_interval` 和 `internal_roll_maxsize` 协同工作。
- 引入版本: v3.2.4

##### `log_cleaner_audit_log_min_retention_days`

- 默认值: 3
- 类型: Int
- 单位: 天
- 是否可变: 是
- 描述: 审计日志文件的最小保留天数。即使磁盘使用率很高，早于此日期的审计日志文件也不会被删除。这确保了审计日志用于合规性和故障排除目的而得到保留。
- 引入版本: -

##### `log_cleaner_check_interval_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 检查磁盘使用情况和清理日志的间隔（秒）。清理器会定期检查每个日志目录的磁盘使用情况，并在必要时触发清理。默认值为 300 秒（5 分钟）。
- 引入版本: -

##### `log_cleaner_disk_usage_target`

- 默认值: 60
- 类型: Int
- 单位: 百分比
- 是否可变: 是
- 描述: 日志清理后的目标磁盘使用率（百分比）。日志清理将持续进行，直到磁盘使用率降至此阈值以下。清理器会逐个删除最旧的日志文件，直到达到目标。
- 引入版本: -

##### `log_cleaner_disk_usage_threshold`

- 默认值: 80
- 类型: Int
- 单位: 百分比
- 是否可变: 是
- 描述: 触发日志清理的磁盘使用率阈值（百分比）。当磁盘使用率超过此阈值时，将开始日志清理。清理器会独立检查每个配置的日志目录，并处理超过此阈值的目录。
- 引入版本: -

##### `log_cleaner_disk_util_based_enable`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 启用基于磁盘使用率的自动日志清理。启用后，当磁盘使用率超过阈值时，将清理日志。日志清理器作为 FE 节点上的后台守护进程运行，有助于防止日志文件堆积导致磁盘空间耗尽。
- 引入版本: -

##### `log_plan_cancelled_by_crash_be`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: 是
- 描述: 当查询因 BE 崩溃或 RPC 异常而取消时，是否启用查询执行计划日志记录。启用此功能后，当查询因 BE 崩溃或 `RpcException` 而取消时，StarRocks 会将查询执行计划（`TExplainLevel.COSTS` 级别）作为 WARN 条目记录。日志条目包括 QueryId、SQL 和 COSTS 计划；在 ExecuteExceptionHandler 路径中，还会记录异常堆栈跟踪。当 `enable_collect_query_detail_info` 启用时，日志记录会被跳过（计划此时存储在查询详情中）——在代码路径中，通过验证查询详情是否为 null 来执行检查。请注意，在 ExecuteExceptionHandler 中，计划仅在第一次重试 (`retryTime == 0`) 时记录。启用此功能可能会增加日志量，因为完整的 COSTS 计划可能很大。
- 引入版本: v3.2.0

##### `log_register_and_unregister_query_id`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否允许 FE 记录来自 QeProcessorImpl 的查询注册和注销消息（例如，`"register query id = {}"` 和 `"deregister query id = {}"`）。仅当查询具有非空 ConnectContext 且命令不是 `COM_STMT_EXECUTE` 或会话变量 `isAuditExecuteStmt()` 为 true 时，才会发出日志。由于这些消息是为每个查询生命周期事件写入的，因此启用此功能可能会产生大量日志，并在高并发环境中成为吞吐量瓶颈。启用它用于调试或审计；禁用它以减少日志记录开销并提高性能。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: 否
- 描述: 系统日志文件或审计日志文件的最大大小。
- 引入版本: -

##### `proc_profile_file_retained_days`

- 默认值: 1
- 类型: Int
- 单位: 天
- 是否可变: 是
- 描述: 保留在 `sys_log_dir/proc_profile` 下生成的进程分析文件（CPU 和内存）的天数。ProcProfileCollector 通过从当前时间（格式为 yyyyMMdd-HHmmss）减去 `proc_profile_file_retained_days` 天来计算截止日期，并删除时间戳部分按字典顺序早于该截止日期的分析文件（即 `timePart.compareTo(timeToDelete) < 0`）。文件删除也遵循由 `proc_profile_file_retained_size_bytes` 控制的基于大小的截止日期。分析文件使用 `cpu-profile-` 和 `mem-profile-` 前缀，并在收集后进行压缩。
- 引入版本: v3.2.12

##### `proc_profile_file_retained_size_bytes`

- 默认值: 2L * 1024 * 1024 * 1024 (2147483648)
- 类型: Long
- 单位: 字节
- 是否可变: 是
- 描述: 在分析目录下保留的已收集 CPU 和内存分析文件（文件名前缀为 `cpu-profile-` 和 `mem-profile-`）的最大总字节数。当有效分析文件的总和超过 `proc_profile_file_retained_size_bytes` 时，收集器会删除最旧的分析文件，直到剩余总大小小于或等于 `proc_profile_file_retained_size_bytes`。早于 `proc_profile_file_retained_days` 的文件也会被删除，无论大小如何。此设置控制分析归档的磁盘使用量，并与 `proc_profile_file_retained_days` 交互以确定删除顺序和保留。
- 引入版本: v3.2.12

##### `profile_log_delete_age`

- 默认值: 1d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制 FE Profile 日志文件在符合删除条件之前保留多长时间。该值通过 `Log4jConfig` 注入到 Log4j 的 `<IfLastModified age="..."/>` 策略中，并与 `profile_log_roll_interval` 和 `profile_log_roll_num` 等轮转设置一起应用。支持的后缀: `d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。例如: `7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。
- 引入版本: v3.2.5

##### `profile_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: FE Profile 日志的写入目录。Log4jConfig 使用此值来放置与 Profile 相关的 appender（在此目录下创建 `fe.profile.log` 和 `fe.features.log` 等文件）。这些文件的轮转和保留由 `profile_log_roll_size_mb`、`profile_log_roll_num` 和 `profile_log_delete_age` 控制；时间戳后缀格式由 `profile_log_roll_interval` 控制（支持 DAY 或 HOUR）。由于默认目录在 `STARROCKS_HOME_DIR` 下，请确保 FE 进程对此目录具有写入和轮转/删除权限。
- 引入版本: v3.2.5

##### `profile_log_latency_threshold_ms`

- 默认值：0
- 类型：Long
- 单位：毫秒
- 是否动态：是
- 描述：写入 `fe.profile.log` 的查询最小延迟（毫秒）。仅当查询执行时间大于或等于该值时才记录 profile。设为 0 表示记录所有 profile（无阈值）。设为正数可仅记录较慢的查询以降低日志量。
- 引入版本：-

##### `profile_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 控制用于生成 Profile 日志文件名日期部分的时间粒度。有效值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成 `"%d{yyyyMMddHH}"` 模式（每小时时间桶），`DAY` 生成 `"%d{yyyyMMdd}"` 模式（每日时间桶）。此值在 Log4j 配置中计算 `profile_file_pattern` 时使用，并且仅影响轮转文件名的基于时间的组件；基于大小的轮转仍由 `profile_log_roll_size_mb` 控制，保留由 `profile_log_roll_num` / `profile_log_delete_age` 控制。无效值会导致日志初始化期间抛出 IOException（错误消息: `"profile_log_roll_interval config error: <value>"`）。对于高容量分析，选择 `HOUR` 以限制每小时的文件大小，或选择 `DAY` 进行每日聚合。
- 引入版本: v3.2.5

##### `profile_log_roll_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 指定 Log4j 的 DefaultRolloverStrategy 为 Profile 日志记录器保留的轮转 Profile 日志文件的最大数量。此值作为 `${profile_log_roll_num}` 注入到日志 XML 中（例如 `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`）。轮转由 `profile_log_roll_size_mb` 或 `profile_log_roll_interval` 触发；当发生轮转时，Log4j 最多保留这些带索引的文件，旧的索引文件将符合删除条件。磁盘上的实际保留也受 `profile_log_delete_age` 和 `profile_log_dir` 位置的影响。较低的值会减少磁盘使用量但限制保留历史记录；较高的值会保留更多历史 Profile 日志。
- 引入版本: v3.2.5

##### `profile_log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: 否
- 描述: 设置触发 FE Profile 日志文件基于大小轮转的大小阈值（单位：兆字节）。此值由 Log4j RollingFile SizeBasedTriggeringPolicy 用于 `ProfileFile` appender；当 Profile 日志超过 `profile_log_roll_size_mb` 时，它将被轮转。当达到 `profile_log_roll_interval` 时，也可以按时间进行轮转——任一条件都会触发轮转。结合 `profile_log_roll_num` 和 `profile_log_delete_age`，此项控制保留多少历史 Profile 文件以及何时删除旧文件。轮转文件的压缩由 `enable_profile_log_compress` 控制。
- 引入版本: v3.2.5

##### `qe_slow_log_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: 是
- 描述: 用于判断查询是否为慢查询的阈值。如果查询的响应时间超过此阈值，则将其记录为 **fe.audit.log** 中的慢查询。
- 引入版本: -

##### `slow_lock_log_every_ms`

- 默认值: 3000L
- 类型: Long
- 单位: 毫秒
- 是否可变: 是
- 描述: 在为同一 SlowLockLogStats 实例发出另一个“慢锁”警告之前等待的最小间隔（单位：毫秒）。在锁等待超过 `slow_lock_threshold_ms` 后，LockUtils 会检查此值，并会抑制额外的警告，直到自上次记录慢锁事件以来已过去 `slow_lock_log_every_ms` 毫秒。使用较大的值可以在长时间争用期间减少日志量，或使用较小的值以获得更频繁的诊断。更改在运行时对后续检查生效。
- 引入版本: v3.2.0

##### `slow_lock_print_stack`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否允许 LockManager 在 `logSlowLockTrace` 发出的慢锁警告的 JSON 负载中包含持有线程的完整堆栈跟踪（"stack" 数组通过 `LogUtil.getStackTraceToJsonArray` 填充，`start=0` 且 `max=Short.MAX_VALUE`）。此配置仅控制当锁获取超过 `slow_lock_threshold_ms` 配置的阈值时显示的锁持有者的额外堆栈信息。启用此功能通过提供精确的持有锁的线程堆栈来帮助调试；禁用它会减少日志量以及在高并发环境中捕获和序列化堆栈跟踪所导致的 CPU/内存开销。
- 引入版本: v3.3.16, v3.4.5, v3.5.1

##### `slow_lock_threshold_ms`

- 默认值: 3000L
- 类型: long
- 单位: 毫秒
- 是否可变: 是
- 描述: 用于将锁操作或持有的锁归类为“慢”的阈值（单位：毫秒）。当锁的等待或持有时间超过此值时，StarRocks 将（根据上下文）发出诊断日志，包含堆栈跟踪或等待者/持有者信息，并且——在 LockManager 中——在此延迟后启动死锁检测。它被 LockUtils（慢锁日志记录）、QueryableReentrantReadWriteLock（过滤慢速读取器）、LockManager（死锁检测延迟和慢锁跟踪）、LockChecker（周期性慢锁检测）以及其他调用者（例如 DiskAndTabletLoadReBalancer 日志记录）使用。降低此值会增加敏感度和日志记录/诊断开销；将其设置为 0 或负值会禁用初始的基于等待的死锁检测延迟行为。请与 `slow_lock_log_every_ms`、`slow_lock_print_stack` 和 `slow_lock_stack_trace_reserve_levels` 一起调整。
- 引入版本: 3.2.0

##### `sys_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 系统日志文件的保留期限。默认值 `7d` 表示每个系统日志文件可以保留 7 天。StarRocks 会检查每个系统日志文件，并删除 7 天前生成的日志文件。
- 引入版本: -

##### `sys_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 存储系统日志文件的目录。
- 引入版本: -

##### `sys_log_enable_compress`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: 否
- 描述: 当此项设置为 `true` 时，系统会在轮转的系统日志文件名后附加 ".gz" 后缀，以便 Log4j 生成 gzip 压缩的轮转 FE 系统日志（例如 fe.log.*）。此值在 Log4j 配置生成期间（Log4jConfig.initLogging / generateActiveLog4jXmlConfig）读取，并控制 RollingFile filePattern 中使用的 `sys_file_postfix` 属性。启用此功能可减少保留日志的磁盘使用量，但会增加轮转期间的 CPU 和 I/O，并更改日志文件名，因此读取日志的工具或脚本必须能够处理 .gz 文件。请注意，审计日志使用单独的压缩配置，即 `audit_log_enable_compress`。
- 引入版本: v3.2.12

##### `sys_log_format`

- 默认值: "plaintext"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 选择用于 FE 日志的 Log4j 布局。有效值: `"plaintext"`（默认）和 `"json"`。这些值不区分大小写。`"plaintext"` 配置 PatternLayout，具有人类可读的时间戳、级别、线程、类.方法:行以及 WARN/ERROR 的堆栈跟踪。`"json"` 配置 JsonTemplateLayout 并发出结构化 JSON 事件（UTC 时间戳、级别、线程 ID/名称、源文件/方法/行、消息、异常堆栈跟踪），适用于日志聚合器（ELK、Splunk）。JSON 输出遵循 `sys_log_json_max_string_length` 和 `sys_log_json_profile_max_string_length` 以限制最大字符串长度。
- 引入版本: v3.2.10

##### `sys_log_json_max_string_length`

- 默认值: 1048576
- 类型: Int
- 单位: 字节
- 是否可变: 否
- 描述: 设置用于 JSON 格式系统日志的 JsonTemplateLayout "maxStringLength" 值。当 `sys_log_format` 设置为 `"json"` 时，如果字符串类型字段（例如 "message" 和字符串化的异常堆栈跟踪）的长度超过此限制，它们将被截断。该值被注入到 Log4jConfig.generateActiveLog4jXmlConfig() 中生成的 Log4j XML 中，并应用于默认、警告、审计、转储和大查询布局。Profile 布局使用单独的配置 (`sys_log_json_profile_max_string_length`)。降低此值会减少日志大小，但可能会截断有用信息。
- 引入版本: 3.2.11

##### `sys_log_json_profile_max_string_length`

- 默认值: 104857600 (100 MB)
- 类型: Int
- 单位: 字节
- 是否可变: 否
- 描述: 当 `sys_log_format` 为 "json" 时，设置 Profile（及相关功能）日志 appender 的 JsonTemplateLayout 的 maxStringLength。JSON 格式 Profile 日志中的字符串字段值将被截断为此字节长度；非字符串字段不受影响。此项应用于 Log4jConfig `JsonTemplateLayout maxStringLength`，并在使用 `plaintext` 日志记录时被忽略。保持该值足够大以容纳您需要的完整消息，但请注意，较大的值会增加日志大小和 I/O。
- 引入版本: v3.2.11

##### `sys_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 系统日志条目分类的严重性级别。有效值: `INFO`、`WARN`、`ERROR` 和 `FATAL`。
- 引入版本: -

##### `sys_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: StarRocks 轮转系统日志条目的时间间隔。有效值: `DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则系统日志文件名称中会添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则系统日志文件名称中会添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `sys_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 在 `sys_log_roll_interval` 参数指定的每个保留期内，可以保留的系统日志文件的最大数量。
- 引入版本: -

##### `sys_log_to_console`

- 默认值: false (除非环境变量 `SYS_LOG_TO_CONSOLE` 设置为 "1")
- 类型: Boolean
- 单位: -
- 是否可变: 否
- 描述: 当此项设置为 `true` 时，系统会将 Log4j 配置为将所有日志发送到控制台 (ConsoleErr appender)，而不是基于文件的 appender。此值在生成活动 Log4j XML 配置时读取（这会影响根日志记录器和每个模块日志记录器 appender 的选择）。其值在进程启动时从 `SYS_LOG_TO_CONSOLE` 环境变量中捕获。在运行时更改它无效。此配置通常用于容器化或 CI 环境中，这些环境更倾向于 stdout/stderr 日志收集，而不是写入日志文件。
- 引入版本: v3.2.0

##### `sys_log_verbose_modules`

- 默认值: 空字符串
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: StarRocks 生成系统日志的模块。如果此参数设置为 `org.apache.starrocks.catalog`，StarRocks 将仅为 catalog 模块生成系统日志。模块名称之间用逗号 (,) 和空格分隔。
- 引入版本: -

##### `sys_log_warn_modules`

- 默认值: {}
- 类型: String[]
- 单位: -
- 是否可变: 否
- 描述: 一个日志记录器名称或包前缀列表，系统将在启动时将其配置为 WARN 级别日志记录器，并路由到警告 appender (SysWF)——即 `fe.warn.log` 文件。条目被插入到生成的 Log4j 配置中（与 org.apache.kafka、org.apache.hudi 和 org.apache.hadoop.io.compress 等内置警告模块一起），并生成 `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` 这样的日志记录器元素。建议使用完全限定的包和类前缀（例如 "com.example.lib"），以抑制常规日志中嘈杂的 INFO/DEBUG 输出，并允许单独捕获警告。
- 引入版本: v3.2.13

### Server

##### `brpc_idle_wait_max_time`

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: No
- Description: bRPC 客户端在空闲状态下等待的最长时间。
- Introduced in: -

##### `brpc_inner_reuse_pool`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 控制底层 BRPC 客户端是否使用内部共享复用池进行连接/通道。StarRocks 在通过 `rpcOptions.setInnerResuePool(...)` 构造 RpcClientOptions 时，会在 BrpcProxy 中读取 `brpc_inner_reuse_pool`。启用（true）时，RPC 客户端会复用内部池，以减少每次调用的连接创建，从而降低 FE 到 BE / LakeService RPC 的连接流失、内存和文件描述符使用量。禁用（false）时，客户端可能会创建更多隔离的池（以更高的资源使用量为代价增加并发隔离）。更改此值需要重启进程才能生效。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_min_evictable_idle_time_ms`

- Default: 120000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: 空闲 BRPC 连接在连接池中必须保持的毫秒数，之后才能被驱逐。此设置应用于 `BrpcProxy` 使用的 RpcClientOptions（通过 RpcClientOptions.setMinEvictableIdleTime）。提高此值可以使空闲连接保持更长时间（减少重新连接的流失）；降低此值可以更快地释放未使用的套接字（减少资源使用）。与 `brpc_connection_pool_size` 和 `brpc_idle_wait_max_time` 一起调整，以平衡连接复用、池增长和驱逐行为。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_reuse_addr`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 当为 true 时，StarRocks 会设置套接字选项，允许 brpc RpcClient 创建的客户端套接字（通过 RpcClientOptions.setReuseAddress）复用本地地址。启用此功能可减少绑定失败，并允许在套接字关闭后更快地重新绑定本地端口，这对于高连接流失率或快速重启很有帮助。当为 false 时，地址/端口复用被禁用，这可以减少意外端口共享的可能性，但可能会增加瞬时绑定错误。此选项与 `brpc_connection_pool_size` 和 `brpc_short_connection` 配置的连接行为相互作用，因为它会影响客户端套接字重新绑定和复用的速度。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `cluster_name`

- Default: StarRocks 集群
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 节点所属的 StarRocks 集群的名称。该集群名称显示在网页的 `Title` 位置。
- Introduced in: -

##### `dns_cache_ttl_seconds`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: No
- Description: 成功 DNS 查询的 DNS 缓存 TTL（Time-To-Live），单位为秒。此项设置 Java 安全属性 `networkaddress.cache.ttl`，它控制 JVM 缓存成功 DNS 查询的时长。将此项设置为 `-1` 以允许系统始终缓存信息，或设置为 `0` 以禁用缓存。这在 IP 地址频繁变化的环境中特别有用，例如 Kubernetes 部署或使用动态 DNS 时。
- Introduced in: v3.5.11, v4.0.4

##### `enable_http_async_handler`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否允许系统异步处理 HTTP 请求。如果启用此功能，Netty worker 线程接收到的 HTTP 请求将被提交到单独的线程池进行服务逻辑处理，以避免阻塞 HTTP 服务器。如果禁用，Netty worker 将直接处理服务逻辑。
- Introduced in: 4.0.0

##### `enable_http_validate_headers`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 控制 Netty 的 HttpServerCodec 是否执行严格的 HTTP 头部验证。在 `HttpServer` 中初始化 HTTP 管道时，该值会传递给 HttpServerCodec（参见 UseLocations）。默认值为 false 是为了向后兼容，因为较新的 Netty 版本强制执行更严格的头部规则 (https://github.com/netty/netty/pull/12760)。设置为 true 以强制执行符合 RFC 的头部检查；这样做可能会导致来自旧客户端或代理的格式错误或不符合规范的请求被拒绝。更改此设置需要重启 HTTP 服务器才能生效。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_https`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 是否在 FE 节点中同时启用 HTTPS 服务器和 HTTP 服务器。
- Introduced in: v4.0

##### `frontend_address`

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 节点的 IP 地址。
- Introduced in: -

##### `http_async_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 异步 HTTP 请求处理的线程池大小。别名为 `max_http_sql_service_task_threads_num`。
- Introduced in: 4.0.0

##### `http_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 HTTP 服务器持有的积压队列的长度。
- Introduced in: -

##### `http_max_chunk_size`

- Default: 8192
- Type: Int
- Unit: 字节
- Is mutable: No
- Description: 设置 FE HTTP 服务器中 Netty 的 HttpServerCodec 处理的单个 HTTP 块的最大允许大小（以字节为单位）。它作为第三个参数传递给 HttpServerCodec，并限制分块传输或流式请求/响应期间块的长度。如果传入的块超过此值，Netty 将引发帧过大错误（例如 TooLongFrameException），并且请求可能会被拒绝。对于合法的分块上传，请增加此值；保持较小以减少内存压力和 DoS 攻击的受攻击面。此设置与 `http_max_initial_line_length`、`http_max_header_size` 和 `enable_http_validate_headers` 一起使用。
- Introduced in: v3.2.0

##### `http_max_header_size`

- Default: 32768
- Type: Int
- Unit: 字节
- Is mutable: No
- Description: Netty 的 `HttpServerCodec` 解析的 HTTP 请求头部块的最大允许大小（以字节为单位）。StarRocks 将此值传递给 `HttpServerCodec`（作为 `Config.http_max_header_size`）；如果传入请求的头部（名称和值组合）超过此限制，编解码器将拒绝该请求（解码器异常），并且连接/请求将失败。仅当客户端合法发送非常大的头部（大型 cookie 或许多自定义头部）时才增加此值；较大的值会增加每个连接的内存使用量。与 `http_max_initial_line_length` 和 `http_max_chunk_size` 结合调整。更改需要重启 FE。
- Introduced in: v3.2.0

##### `http_max_initial_line_length`

- Default: 4096
- Type: Int
- Unit: 字节
- Is mutable: No
- Description: 设置 HttpServer 中使用的 Netty `HttpServerCodec` 接受的 HTTP 初始请求行（方法 + 请求目标 + HTTP 版本）的最大允许长度（以字节为单位）。该值传递给 Netty 的解码器，初始行长度超过此值的请求将被拒绝（TooLongFrameException）。仅当您必须支持非常长的请求 URI 时才增加此值；较大的值会增加内存使用量，并可能增加遭受格式错误/请求滥用的风险。与 `http_max_header_size` 和 `http_max_chunk_size` 一起调整。
- Introduced in: v3.2.0

##### `http_port`

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 HTTP 服务器监听的端口。
- Introduced in: -

##### `http_web_page_display_hardware`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 当为 true 时，HTTP 索引页面 (/index) 将包含一个通过 oshi 库（CPU、内存、进程、磁盘、文件系统、网络等）填充的硬件信息部分。oshi 可能会间接调用系统实用程序或读取系统文件（例如，它可以执行 `getent passwd` 等命令），这可能会暴露敏感的系统数据。如果您需要更严格的安全性或希望避免在主机上执行这些间接命令，请将此配置设置为 false 以禁用在 Web UI 上收集和显示硬件详细信息。
- Introduced in: v3.2.0

##### `http_worker_threads_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: HTTP 服务器处理 HTTP 请求的工作线程数。如果值为负数或 0，线程数将是 CPU 核心数的两倍。
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### `https_port`

- Default: 8443
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 HTTPS 服务器监听的端口。
- Introduced in: v4.0

##### `max_mysql_service_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 MySQL 服务器可用于处理任务的最大线程数。
- Introduced in: -

##### `max_task_runs_threads_num`

- Default: 512
- Type: Int
- Unit: 线程
- Is mutable: No
- Description: 控制任务运行执行器线程池中的最大线程数。此值是并发任务运行执行的上限；增加它会提高并行度，但也会增加 CPU、内存和网络使用量，而减少它可能导致任务运行积压和更高的延迟。根据预期的并发调度作业和可用的系统资源调整此值。
- Introduced in: v3.2.0

##### `memory_tracker_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 启用 FE 内存跟踪器子系统。当 `memory_tracker_enable` 设置为 `true` 时，`MemoryUsageTracker` 会定期扫描注册的元数据模块，更新内存中的 `MemoryUsageTracker.MEMORY_USAGE` 映射，记录总计，并使 `MetricRepo` 在指标输出中暴露内存使用量和对象计数仪表。使用 `memory_tracker_interval_seconds` 控制采样间隔。启用此功能有助于监控和调试内存消耗，但会引入 CPU 和 I/O 开销以及额外的指标基数。
- Introduced in: v3.2.4

##### `memory_tracker_interval_seconds`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: FE `MemoryUsageTracker` 守护进程轮询和记录 FE 进程以及注册的 `MemoryTrackable` 模块内存使用情况的间隔（秒）。当 `memory_tracker_enable` 设置为 `true` 时，跟踪器会以此频率运行，更新 `MEMORY_USAGE`，并记录聚合的 JVM 和跟踪模块使用情况。
- Introduced in: v3.2.4

##### `mysql_nio_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 MySQL 服务器持有的积压队列的长度。
- Introduced in: -

##### `mysql_server_version`

- Default: 8.0.33
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 返回给客户端的 MySQL 服务器版本。修改此参数将影响以下情况下的版本信息：
  1. `select version();`
  2. 握手包版本
  3. 全局变量 `version` 的值 (`show variables like 'version';`)
- Introduced in: -

##### `mysql_service_io_threads_num`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 MySQL 服务器可用于处理 I/O 事件的最大线程数。
- Introduced in: -

##### `mysql_service_kill_after_disconnect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 控制当检测到 MySQL TCP 连接关闭（读取时 EOF）时服务器如何处理会话。如果设置为 `true`，服务器会立即终止该连接的任何正在运行的查询并立即执行清理。如果设置为 `false`，服务器在断开连接时不会终止正在运行的查询，并且仅在没有待处理请求任务时才执行清理，从而允许长时间运行的查询在客户端断开连接后继续执行。注意：尽管有一个简短的注释建议 TCP keep-alive，但此参数专门管理断开连接后的终止行为，应根据您是希望终止孤立查询（建议在不可靠/负载均衡客户端后面使用）还是允许其完成来设置。
- Introduced in: -

##### `mysql_service_nio_enable_keep_alive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 为 MySQL 连接启用 TCP Keep-Alive。对于负载均衡器后面的长时间空闲连接很有用。
- Introduced in: -

##### `net_use_ipv6_when_priority_networks_empty`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 一个布尔值，用于控制当未指定 `priority_networks` 时是否优先使用 IPv6 地址。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- Introduced in: v3.3.0

##### `priority_networks`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: No
- Description: 声明具有多个 IP 地址的服务器的选择策略。请注意，最多只能有一个 IP 地址与此参数指定的列表匹配。此参数的值是一个列表，由以分号 (;) 分隔的 CIDR 表示法条目组成，例如 10.10.10.0/24。如果没有 IP 地址与此列表中的条目匹配，则将随机选择服务器的一个可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，系统默认使用 IPv4 地址。您可以通过将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- Introduced in: -

##### `proc_profile_cpu_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 当此项设置为 `true` 时，后台 `ProcProfileCollector` 将使用 `AsyncProfiler` 收集 CPU 配置文件，并将 HTML 报告写入 `sys_log_dir/proc_profile` 下。每次收集运行都会记录由 `proc_profile_collect_time_s` 配置的持续时间的 CPU 堆栈，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的配置文件会根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 进行压缩和旧文件修剪。`AsyncProfiler` 需要本地库 (`libasyncProfiler.so`)；`one.profiler.extractPath` 设置为 `STARROCKS_HOME_DIR/bin` 以避免 `/tmp` 上的 noexec 问题。
- Introduced in: v3.2.12

##### `qe_max_connection`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: 所有用户可以与 FE 节点建立的最大连接数。从 v3.1.12 和 v3.2.7 开始，默认值已从 `1024` 更改为 `4096`。
- Introduced in: -

##### `query_port`

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 MySQL 服务器监听的端口。
- Introduced in: -

##### `rpc_port`

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 Thrift 服务器监听的端口。
- Introduced in: -

##### `slow_lock_stack_trace_reserve_levels`

- Default: 15
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 控制 StarRocks 在为慢锁或持有锁转储锁调试信息时捕获和发出的堆栈跟踪帧数。当 `QueryableReentrantReadWriteLock` 为排他锁所有者、当前线程和最旧/共享读取器生成 JSON 时，此值会传递给 `LogUtil.getStackTraceToJsonArray`。增加此值可以为诊断慢锁或死锁问题提供更多上下文，但代价是更大的 JSON 有效负载和略高的 CPU/内存用于堆栈捕获；减少此值可以降低开销。注意：当仅记录慢锁时，读取器条目可以通过 `slow_lock_threshold_ms` 进行过滤。
- Introduced in: v3.4.0, v3.5.0

##### `ssl_cipher_blacklist`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: No
- Description: 逗号分隔列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入黑名单。如果同时设置了白名单和黑名单，则黑名单优先。
- Introduced in: v4.0

##### `ssl_cipher_whitelist`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: No
- Description: 逗号分隔列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入白名单。如果同时设置了白名单和黑名单，则黑名单优先。
- Introduced in: v4.0

##### `task_runs_concurrency`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 并发运行的 TaskRun 实例的全局限制。当当前运行计数大于或等于 `task_runs_concurrency` 时，`TaskRunScheduler` 会停止调度新的运行，因此此值限制了调度器中 TaskRun 的并行执行。它也被 `MVPCTRefreshPartitioner` 用于计算每个 TaskRun 的分区刷新粒度。增加此值会提高并行度和资源使用量；减少此值会降低并发性并使每次运行的分区刷新更大。除非有意禁用调度，否则不要设置为 0 或负数：0（或负数）将有效地阻止 `TaskRunScheduler` 调度新的 TaskRun。
- Introduced in: v3.2.0

##### `task_runs_queue_length`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 限制待处理队列中保留的待处理 TaskRun 项的最大数量。当有效的待处理 TaskRun 计数大于或等于 `task_runs_queue_length` 时，`TaskRunManager` 会检查当前待处理计数并拒绝新的提交。在添加合并/接受的 TaskRun 之前，会重新检查相同的限制。调整此值以平衡内存和调度积压：对于大型突发工作负载，将其设置得更高以避免拒绝；或将其设置得更低以限制内存并减少待处理积压。
- Introduced in: v3.2.0

##### `thrift_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE 节点中 Thrift 服务器持有的积压队列的长度。
- Introduced in: -

##### `thrift_client_timeout_ms`

- Default: 5000
- Type: Int
- Unit: 毫秒
- Is mutable: No
- Description: 空闲客户端连接超时的时间长度。
- Introduced in: -

##### `thrift_rpc_max_body_size`

- Default: -1
- Type: Int
- Unit: 字节
- Is mutable: No
- Description: 控制构建服务器 Thrift 协议时（传递给 `ThriftServer` 中的 TBinaryProtocol.Factory）允许的最大 Thrift RPC 消息体大小（以字节为单位）。值为 `-1` 表示禁用限制（无限制）。设置正值会强制执行上限，以便大于此值的消息会被 Thrift 层拒绝，这有助于限制内存使用并减轻超大请求或 DoS 风险。将其设置为足够大的值以适应预期的有效负载（大型结构体或批量数据）以避免拒绝合法请求。
- Introduced in: v3.2.0

##### `thrift_server_max_worker_threads`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE 节点中 Thrift 服务器支持的最大工作线程数。
- Introduced in: -

##### `thrift_server_queue_size`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: 请求待处理队列的长度。如果 Thrift 服务器中正在处理的线程数超过 `thrift_server_max_worker_threads` 中指定的值，则新请求将添加到待处理队列中。
- Introduced in: -

### 元数据和集群管理

##### `alter_max_worker_queue_size`

- Default: 4096
- Type: Int
- Unit: 任务
- Is mutable: No
- Description: 控制 alter 子系统使用的内部工作线程池队列的容量。它与 `alter_max_worker_threads` 一起传递给 `AlterHandler` 中的 `ThreadPoolManager.newDaemonCacheThreadPool`。当待处理的 alter 任务数量超过 `alter_max_worker_queue_size` 时，新的提交将被拒绝，并可能抛出 `RejectedExecutionException`（参见 `AlterHandler.handleFinishAlterTask`）。调整此值以平衡内存使用和允许并发 alter 任务的积压量。
- Introduced in: v3.2.0

##### `alter_max_worker_threads`

- Default: 4
- Type: Int
- Unit: 线程
- Is mutable: No
- Description: 设置 AlterHandler 线程池中工作线程的最大数量。AlterHandler 使用此值构建执行器，以运行和完成与 alter 相关的任务（例如，通过 handleFinishAlterTask 提交 `AlterReplicaTask`）。此值限制了 alter 操作的并发执行；提高它会增加并行度和资源使用，降低它会限制并发 alter 并可能成为瓶颈。执行器与 `alter_max_worker_queue_size` 一起创建，处理程序调度使用 `alter_scheduler_interval_millisecond`。
- Introduced in: v3.2.0

##### `automated_cluster_snapshot_interval_seconds`

- Default: 600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 触发自动集群快照任务的时间间隔。
- Introduced in: v3.4.2

##### `background_refresh_metadata_interval_millis`

- Default: 600000
- Type: Int
- Unit: 毫秒
- Is mutable: Yes
- Description: 两次连续 Hive 元数据缓存刷新的时间间隔。
- Introduced in: v2.5.5

##### `background_refresh_metadata_time_secs_since_last_access_secs`

- Default: 3600 * 24
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: Hive 元数据缓存刷新任务的过期时间。对于已访问的 Hive Catalog，如果超过指定时间未被访问，StarRocks 将停止刷新其缓存的元数据。对于未访问的 Hive Catalog，StarRocks 将不会刷新其缓存的元数据。
- Introduced in: v2.5.5

##### `bdbje_cleaner_threads`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: StarRocks 日志使用的 Berkeley DB Java Edition (JE) 环境的后台清理线程数。此值在 `BDBEnvironment.initConfigs` 中的环境初始化期间读取，并使用 `Config.bdbje_cleaner_threads` 应用于 `EnvironmentConfig.CLEANER_THREADS`。它控制 JE 日志清理和空间回收的并行度；增加它可以加快清理速度，但会以额外的 CPU 和 I/O 干扰前台操作为代价。更改仅在 BDB 环境（重新）初始化时生效，因此需要重新启动前端以应用新值。
- Introduced in: v3.2.0

##### `bdbje_heartbeat_timeout_second`

- Default: 30
- Type: Int
- Unit: 秒
- Is mutable: No
- Description: StarRocks 集群中 Leader、Follower 和 Observer FE 之间心跳超时的时间。
- Introduced in: -

##### `bdbje_lock_timeout_second`

- Default: 1
- Type: Int
- Unit: 秒
- Is mutable: No
- Description: 基于 BDB JE 的 FE 中锁超时的时间。
- Introduced in: -

##### `bdbje_replay_cost_percent`

- Default: 150
- Type: Int
- Unit: 百分比
- Is mutable: No
- Description: 设置从 BDB JE 日志重放事务与通过网络恢复获取相同数据的相对成本（以百分比表示）。该值提供给底层 JE 复制参数 `REPLAY_COST_PERCENT`，通常 `>100` 表示重放通常比网络恢复更昂贵。在决定是否保留已清理的日志文件以进行潜在重放时，系统会将重放成本乘以日志大小与网络恢复成本进行比较；如果网络恢复被认为更有效，则将删除文件。值为 0 会禁用基于此成本比较的保留。`REP_STREAM_TIMEOUT` 内的副本或任何活动复制所需的日志文件始终保留。
- Introduced in: v3.2.0

##### `bdbje_replica_ack_timeout_second`

- Default: 10
- Type: Int
- Unit: 秒
- Is mutable: No
- Description: Leader FE 将元数据写入 Follower FE 时，Leader FE 等待指定数量的 Follower FE 返回 ACK 消息的最长时间。单位：秒。如果写入大量元数据，Follower FE 需要很长时间才能向 Leader FE 返回 ACK 消息，从而导致 ACK 超时。在这种情况下，元数据写入失败，FE 进程退出。建议您增加此参数的值以防止这种情况发生。
- Introduced in: -

##### `bdbje_reserved_disk_size`

- Default: 512 * 1024 * 1024 (536870912)
- Type: Long
- Unit: 字节
- Is mutable: No
- Description: 限制 Berkeley DB JE 将保留为“未受保护”（可删除）日志/数据文件的字节数。StarRocks 通过 BDBEnvironment 中的 `EnvironmentConfig.RESERVED_DISK` 将此值传递给 JE；JE 的内置默认值为 0（无限制）。StarRocks 的默认值（512 MiB）可防止 JE 为未受保护的文件保留过多的磁盘空间，同时允许安全清理过时文件。在磁盘受限的系统上调整此值：减小它可让 JE 更早释放更多文件，增加它可让 JE 保留更多预留空间。更改需要重新启动进程才能生效。
- Introduced in: v3.2.0

##### `bdbje_reset_election_group`

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: 是否重置 BDBJE 复制组。如果此参数设置为 `TRUE`，FE 将重置 BDBJE 复制组（即，删除所有可选举 FE 节点的信息）并作为 Leader FE 启动。重置后，此 FE 将是集群中唯一的成员，其他 FE 可以通过使用 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入此集群。仅当大多数 Follower FE 的数据已损坏导致无法选举 Leader FE 时才使用此设置。`reset_election_group` 用于替换 `metadata_failure_recovery`。
- Introduced in: -

##### `black_host_connect_failures_within_time`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 允许列入黑名单的 BE 节点的连接失败阈值。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当列入黑名单的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中移除。
- Introduced in: v3.3.0

##### `black_host_history_sec`

- Default: 2 * 60
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 在 BE 黑名单中保留 BE 节点历史连接失败的时间长度。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以从 BE 黑名单中移除。在 `black_host_history_sec` 内，只有当列入黑名单的 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中移除。
- Introduced in: v3.3.0

##### `brpc_connection_pool_size`

- Default: 16
- Type: Int
- Unit: 连接
- Is mutable: No
- Description: FE 的 BrpcProxy 每个端点使用的最大 BRPC 连接池数量。此值通过 `setMaxTotoal` 和 `setMaxIdleSize` 应用于 RpcClientOptions，因此它直接限制了并发的出站 BRPC 请求，因为每个请求都必须从连接池中借用一个连接。在高并发场景中，增加此值以避免请求排队；增加它会提高套接字和内存使用率，并可能增加远程服务器负载。调整时，请考虑相关设置，例如 `brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr` 和 `brpc_min_evictable_idle_time_ms`。更改此值不可热加载，需要重新启动。
- Introduced in: v3.2.0

##### `brpc_short_connection`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 控制底层 brpc RpcClient 是否使用短连接。启用时 (`true`)，将设置 RpcClientOptions.setShortConnection，并在请求完成后关闭连接，从而减少长连接套接字的数量，但代价是更高的连接建立开销和增加的延迟。禁用时 (`false`，默认值) 使用持久连接和连接池。启用此选项会影响连接池行为，应与 `brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr` 和 `brpc_inner_reuse_pool` 一起考虑。对于典型的高吞吐量部署，请保持禁用状态；仅在限制套接字生命周期或网络策略要求短连接时启用。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `catalog_try_lock_timeout_ms`

- Default: 5000
- Type: Long
- Unit: 毫秒
- Is mutable: Yes
- Description: 获取全局锁的超时时长。
- Introduced in: -

##### `checkpoint_only_on_leader`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 当为 `true` 时，CheckpointController 将只选择 Leader FE 作为检查点工作节点；当为 `false` 时，控制器可以选择任何前端，并优先选择堆使用率较低的节点。当为 `false` 时，工作节点按最近失败时间和 `heapUsedPercent` 排序（Leader 被视为具有无限使用率以避免选择它）。对于需要集群快照元数据的操作，控制器无论此标志如何，都已强制选择 Leader。启用 `true` 将检查点工作集中在 Leader 上（更简单，但会增加 Leader 的 CPU/内存和网络负载）；保持 `false` 将检查点负载分配给负载较低的 FE。此设置影响工作节点选择以及与 `checkpoint_timeout_seconds` 等超时和 `thrift_rpc_timeout_ms` 等 RPC 设置的交互。
- Introduced in: v3.4.0, v3.5.0

##### `checkpoint_timeout_seconds`

- Default: 24 * 3600
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: Leader 的 CheckpointController 等待检查点工作节点完成检查点的最长时间（以秒为单位）。控制器将此值转换为纳秒并轮询工作节点结果队列；如果在此超时时间内未收到成功完成，则检查点被视为失败，并且 createImage 返回失败。增加此值可以适应运行时间更长的检查点，但会延迟故障检测和后续镜像传播；减小它会导致更快的故障转移/重试，但可能会为慢速工作节点产生错误的超时。此设置仅控制 `CheckpointController` 在检查点创建期间的等待时间，不改变工作节点的内部检查点行为。
- Introduced in: v3.4.0, v3.5.0

##### `db_used_data_quota_update_interval_secs`

- Default: 300
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 数据库已用数据配额的更新间隔。StarRocks 定期更新所有数据库的已用数据配额以跟踪存储消耗。此值用于配额强制执行和指标收集。允许的最小间隔为 30 秒，以防止系统负载过高。小于 30 的值将被拒绝。
- Introduced in: -

##### `drop_backend_after_decommission`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: BE 下线后是否删除 BE。`TRUE` 表示 BE 下线后立即删除。`FALSE` 表示 BE 下线后不删除。
- Introduced in: -

##### `edit_log_port`

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: 集群中 Leader、Follower 和 Observer FE 之间用于通信的端口。
- Introduced in: -

##### `edit_log_roll_num`

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 在为这些日志条目创建日志文件之前可以写入的最大元数据日志条目数。此参数用于控制日志文件的大小。新日志文件写入 BDBJE 数据库。
- Introduced in: -

##### `edit_log_type`

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: 可以生成的编辑日志类型。将值设置为 `BDB`。
- Introduced in: -

##### `enable_background_refresh_connector_metadata`

- Default: v3.0 及更高版本为 true，v2.5 为 false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否启用周期性 Hive 元数据缓存刷新。启用后，StarRocks 会轮询您的 Hive 集群的 metastore（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Hive Catalog 的缓存元数据以感知数据变化。`true` 表示启用 Hive 元数据缓存刷新，`false` 表示禁用。
- Introduced in: v2.5.5

##### `enable_collect_query_detail_info`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否收集查询的 Profile。如果此参数设置为 `TRUE`，系统将收集查询的 Profile。如果此参数设置为 `FALSE`，系统将不收集查询的 Profile。
- Introduced in: -

##### `enable_create_partial_partition_in_batch`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: 当此项设置为 `false`（默认值）时，StarRocks 强制批量创建的范围分区与标准时间单位边界对齐。它将拒绝非对齐范围以避免创建空洞。将此项设置为 `true` 会禁用该对齐检查，并允许批量创建部分（非标准）分区，这可能会产生间隙或错位的分区范围。您只应在有意需要部分批量分区并接受相关风险时将其设置为 `true`。
- Introduced in: v3.2.0

##### `enable_internal_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 当此项设置为 `true` 时，内部组件（例如 SimpleExecutor）执行的内部 SQL 语句将被保留并写入内部审计或日志消息中（如果设置了 `enable_sql_desensitize_in_log`，还可以进一步脱敏）。当设置为 `false` 时，内部 SQL 文本将被抑制：格式化代码 (SimpleExecutor.formatSQL) 返回“?”，并且实际语句不会发送到内部审计或日志消息。此配置不改变内部语句的执行语义——它仅控制内部 SQL 的日志记录和可见性，以实现隐私或安全。
- Introduced in: -

##### `enable_legacy_compatibility_for_replication`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否启用复制的旧版兼容性。StarRocks 在新旧版本之间可能行为不同，导致跨集群数据迁移期间出现问题。因此，您必须在数据迁移之前为目标集群启用旧版兼容性，并在数据迁移完成后禁用它。`true` 表示启用此模式。
- Introduced in: v3.1.10, v3.2.6

##### `enable_show_materialized_views_include_all_task_runs`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 控制 `SHOW MATERIALIZED VIEWS` 命令如何返回 TaskRuns。当此项设置为 `false` 时，StarRocks 只返回每个任务的最新 TaskRun（为了兼容性的旧行为）。当设置为 `true`（默认值）时，`TaskManager` 可能会为同一任务包含额外的 TaskRuns，但仅当它们共享相同的启动 TaskRun ID（例如，属于同一作业）时，这可以防止出现不相关的重复运行，同时允许显示与一个作业相关的多个状态。将此项设置为 `false` 以恢复单次运行输出，或显示多次运行的作业历史以进行调试和监控。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_statistics_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否为统计信息查询生成 Profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计信息查询生成查询 Profile。
- Introduced in: v3.1.5

##### `enable_table_name_case_insensitive`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 是否对 Catalog 名称、数据库名称、表名称、视图名称和物化视图名称启用不区分大小写处理。目前，表名称默认区分大小写。
  - 启用此功能后，所有相关名称将以小写形式存储，并且所有包含这些名称的 SQL 命令将自动将其转换为小写。
  - 您只能在创建集群时启用此功能。**集群启动后，此配置的值不能以任何方式修改**。任何修改尝试都将导致错误。当 FE 检测到此配置项的值与集群首次启动时不一致时，FE 将无法启动。
  - 目前，此功能不支持 JDBC Catalog 和表名称。如果您想对 JDBC 或 ODBC 数据源执行不区分大小写处理，请勿启用此功能。
- Introduced in: v4.0

##### `enable_task_history_archive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 启用后，已完成的任务运行记录将归档到持久任务运行历史表并记录到编辑日志中，以便查找（例如 `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`）包含归档结果。归档由 FE Leader 执行，并在单元测试 (`FeConstants.runningUnitTest`) 期间跳过。启用后，内存中的过期和强制 GC 路径将被绕过（代码从 `removeExpiredRuns` 和 `forceGC` 提前返回），因此保留/逐出由持久归档处理，而不是 `task_runs_ttl_second` 和 `task_runs_max_history_number`。禁用时，历史记录保留在内存中并由这些配置进行修剪。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `enable_task_run_fe_evaluation`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 启用后，FE 将在 `TaskRunsSystemTable.supportFeEvaluation` 中对系统表 `task_runs` 执行本地评估。FE 侧评估仅允许用于将列与常量进行比较的合取相等谓词，并且仅限于 `QUERY_ID` 和 `TASK_NAME` 列。启用此功能可以通过避免更广泛的扫描或额外的远程处理来提高目标查找的性能；禁用它会强制规划器跳过 `task_runs` 的 FE 评估，这可能会减少谓词修剪并影响这些过滤器的查询延迟。
- Introduced in: v3.3.13, v3.4.3, v3.5.0

##### `heartbeat_mgr_blocking_queue_size`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: 存储由心跳管理器运行的心跳任务的阻塞队列的大小。
- Introduced in: -

##### `heartbeat_mgr_threads_num`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: 心跳管理器可以运行心跳任务的线程数。
- Introduced in: -

##### `ignore_materialized_view_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE 是否忽略由物化视图错误引起的元数据异常。如果 FE 因物化视图错误引起的元数据异常而无法启动，您可以将此参数设置为 `true` 以允许 FE 忽略该异常。
- Introduced in: v2.5.10

##### `ignore_meta_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非 Leader FE 是否忽略与 Leader FE 的元数据差距。如果值为 TRUE，非 Leader FE 将忽略与 Leader FE 的元数据差距并继续提供数据读取服务。此参数确保即使您长时间停止 Leader FE，也能提供连续的数据读取服务。如果值为 FALSE，非 Leader FE 将不忽略与 Leader FE 的元数据差距并停止提供数据读取服务。
- Introduced in: -

##### `ignore_task_run_history_replay_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 当 StarRocks 为 `information_schema.task_runs` 反序列化 TaskRun 历史行时，损坏或无效的 JSON 行通常会导致反序列化记录警告并抛出 RuntimeException。如果此项设置为 `true`，系统将捕获反序列化错误，跳过格式错误的记录，并继续处理剩余的行，而不是使查询失败。这将使 `information_schema.task_runs` 查询能够容忍 `_statistics_.task_run_history` 表中的错误条目。请注意，启用它将静默丢弃损坏的历史记录（潜在数据丢失），而不是显示明确的错误。
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### `lock_checker_interval_second`

- Default: 30
- Type: long
- Unit: 秒
- Is mutable: Yes
- Description: LockChecker 前端守护程序（名为“deadlock-checker”）执行之间的间隔，以秒为单位。该守护程序执行死锁检测和慢锁扫描；配置的值乘以 1000 以毫秒为单位设置计时器。减小此值可减少检测延迟，但会增加调度和 CPU 开销；增加它可减少开销，但会延迟检测和慢锁报告。更改在运行时生效，因为守护程序每次运行时都会重置其间隔。此设置与 `lock_checker_enable_deadlock_check`（启用死锁检查）和 `slow_lock_threshold_ms`（定义慢锁的构成）交互。
- Introduced in: v3.2.0

##### `master_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: Leader FE 将日志刷新到磁盘的策略。此参数仅在当前 FE 是 Leader FE 时有效。有效值：
  - `SYNC`：事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`：事务提交时，日志条目的生成和刷新不同时发生。
  - `WRITE_NO_SYNC`：事务提交时，日志条目同时生成但不刷新到磁盘。

  如果您只部署了一个 Follower FE，建议您将此参数设置为 `SYNC`。如果您部署了三个或更多 Follower FE，建议您将此参数和 `replica_sync_policy` 都设置为 `WRITE_NO_SYNC`。

- Introduced in: -

##### `max_bdbje_clock_delta_ms`

- Default: 5000
- Type: Long
- Unit: 毫秒
- Is mutable: No
- Description: StarRocks 集群中 Leader FE 与 Follower 或 Observer FE 之间允许的最大时钟偏移。
- Introduced in: -

##### `meta_delay_toleration_second`

- Default: 300
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: Follower 和 Observer FE 上的元数据落后于 Leader FE 上的元数据的最大持续时间。单位：秒。如果超过此持续时间，非 Leader FE 将停止提供服务。
- Introduced in: -

##### `meta_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- Type: String
- Unit: -
- Is mutable: No
- Description: 存储元数据的目录。
- Introduced in: -

##### `metadata_ignore_unknown_operation_type`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 是否忽略未知日志 ID。当 FE 回滚时，早期版本的 FE 可能无法识别某些日志 ID。如果值为 `TRUE`，FE 将忽略未知日志 ID。如果值为 `FALSE`，FE 将退出。
- Introduced in: -

##### `profile_info_format`

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 系统输出的 Profile 格式。有效值：`default` 和 `json`。当设置为 `default` 时，Profile 为默认格式。当设置为 `json` 时，系统以 JSON 格式输出 Profile。
- Introduced in: v2.5

##### `replica_ack_policy`

- Default: `SIMPLE_MAJORITY`
- Type: String
- Unit: -
- Is mutable: No
- Description: 判断日志条目是否有效的策略。默认值 `SIMPLE_MAJORITY` 指定如果大多数 Follower FE 返回 ACK 消息，则日志条目被认为是有效的。
- Introduced in: -

##### `replica_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: Follower FE 将日志刷新到磁盘的策略。此参数仅在当前 FE 是 Follower FE 时有效。有效值：
  - `SYNC`：事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`：事务提交时，日志条目的生成和刷新不同时发生。
  - `WRITE_NO_SYNC`：事务提交时，日志条目同时生成但不刷新到磁盘。
- Introduced in: -

##### `start_with_incomplete_meta`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 当为 true 时，如果镜像数据存在但 Berkeley DB JE (BDB) 日志文件丢失或损坏，FE 将允许启动。`MetaHelper.checkMetaDir()` 使用此标志绕过安全检查，否则该检查会阻止在没有相应 BDB 日志的情况下从镜像启动；以这种方式启动可能会产生陈旧或不一致的元数据，并且只能用于紧急恢复。`RestoreClusterSnapshotMgr` 在恢复集群快照时暂时将此标志设置为 true，然后将其回滚；该组件在恢复期间还会切换 `bdbje_reset_election_group`。请勿在正常操作中启用——仅在从损坏的 BDB 数据恢复或明确恢复基于镜像的快照时启用。
- Introduced in: v3.2.0

##### `table_keeper_interval_second`

- Default: 30
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: TableKeeper 守护程序执行之间的间隔，以秒为单位。TableKeeperDaemon 使用此值（乘以 1000）设置其内部计时器，并定期运行 keeper 任务，以确保历史表存在、更正表属性（副本数）并更新分区 TTL。守护程序仅在 Leader 节点上执行工作，并在 `table_keeper_interval_second` 更改时通过 setInterval 更新其运行时间隔。增加此值可降低调度频率和负载；减小此值可更快地响应缺失或陈旧的历史表。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `task_runs_ttl_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 控制任务运行历史记录的生存时间 (TTL)。降低此值会缩短历史记录保留时间并减少内存/磁盘使用；提高此值会延长历史记录保留时间但增加资源使用。与 `task_runs_max_history_number` 和 `enable_task_history_archive` 一起调整，以实现可预测的保留和存储行为。
- Introduced in: v3.2.0

##### `task_ttl_second`

- Default: 24 * 3600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 任务的生存时间 (TTL)。对于手动任务（未设置调度时），TaskBuilder 使用此值计算任务的 `expireTime` (`expireTime = now + task_ttl_second * 1000L`)。TaskRun 在计算运行的执行超时时也使用此值作为上限——有效的执行超时是 `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`。调整此值会改变手动创建任务的有效时间，并可以间接限制任务运行的最大允许执行时间。
- Introduced in: v3.2.0

##### `thrift_rpc_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 控制 Thrift RPC 调用将尝试的总次数。此值由 `ThriftRPCRequestExecutor`（以及 `NodeMgr` 和 `VariableMgr` 等调用者）用作重试的循环计数——即，值为 3 允许最多三次尝试，包括初始尝试。在 `TTransportException` 上，执行器将尝试重新打开连接并重试达到此计数；当原因是 `SocketTimeoutException` 或重新打开失败时，它将不会重试。每次尝试都受 `thrift_rpc_timeout_ms` 配置的每次尝试超时限制。增加此值可提高对瞬态连接故障的弹性，但可能会增加整体 RPC 延迟和资源使用。
- Introduced in: v3.2.0

##### `thrift_rpc_strict_mode`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: 控制 Thrift 服务器使用的 TBinaryProtocol“严格读取”模式。此值作为第一个参数传递给 Thrift 服务器堆栈中的 org.apache.thrift.protocol.TBinaryProtocol.Factory，并影响传入 Thrift 消息的解析和验证方式。当为 `true`（默认值）时，服务器强制执行严格的 Thrift 编码/版本检查并遵守配置的 `thrift_rpc_max_body_size` 限制；当为 `false` 时，服务器接受非严格（旧版/宽松）消息格式，这可以提高与旧客户端的兼容性，但可能会绕过一些协议验证。在运行中的集群上更改此设置时请谨慎，因为它不可变且会影响互操作性和解析安全性。
- Introduced in: v3.2.0

##### `thrift_rpc_timeout_ms`

- Default: 10000
- Type: Int
- Unit: 毫秒
- Is mutable: Yes
- Description: 用作 Thrift RPC 调用的默认网络/套接字超时时间（以毫秒为单位）。在 `ThriftConnectionPool` 中创建 Thrift 客户端时（由前端和后端池使用）会将其传递给 TSocket，并且在 `ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr` 和 `CheckpointWorker` 等地方计算 RPC 调用超时时，也会将其添加到操作的执行超时（例如，ExecTimeout*1000 + `thrift_rpc_timeout_ms`）。增加此值可使 RPC 调用容忍更长的网络或远程处理延迟；减小此值可在慢速网络上实现更快的故障转移。更改此值会影响执行 Thrift RPC 的 FE 代码路径中的连接创建和请求截止时间。
- Introduced in: v3.2.0

##### `txn_latency_metric_report_groups`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 要报告的事务延迟指标组的逗号分隔列表。加载类型被分类为逻辑组以进行监控。当启用一个组时，其名称将作为“type”标签添加到事务指标中。有效值：`stream_load`、`routine_load`、`broker_load`、`insert` 和 `compaction`（仅适用于共享数据集群）。示例：`"stream_load,routine_load"`。
- Introduced in: v4.0

##### `txn_rollback_limit`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: 可以回滚的最大事务数。
- Introduced in: -

### 用户、角色和权限

##### `enable_task_info_mask_credential`

- 默认值: true
- 类型: 布尔
- 单位: -
- 可变: 是
- 描述: 当为 true 时，StarRocks 会在将任务 SQL 定义返回到 `information_schema.tasks` 和 `information_schema.task_runs` 之前，通过对 DEFINITION 列应用 SqlCredentialRedactor.redact 来编辑凭证。在 `information_schema.task_runs` 中，无论定义是来自任务运行状态，还是在为空时来自任务定义查找，都会应用相同的编辑。当为 false 时，将返回原始任务定义（可能会暴露凭证）。掩码是 CPU/字符串处理工作，当任务或 `task_runs` 的数量很大时，可能会非常耗时；仅当您需要未编辑的定义并接受安全风险时才禁用。
- 引入版本: v3.5.6

##### `privilege_max_role_depth`

- 默认值: 16
- 类型: 整型
- 单位:
- 可变: 是
- 描述: 角色的最大深度（继承级别）。
- 引入版本: v3.0.0

##### `privilege_max_total_roles_per_user`

- 默认值: 64
- 类型: 整型
- 单位:
- 可变: 是
- 描述: 用户可以拥有的最大角色数量。
- 引入版本: v3.0.0

### 查询引擎

##### `brpc_send_plan_fragment_timeout_ms`

- 默认值: 60000
- 类型: 整型
- 单位: 毫秒
- 可变: 是
- 描述: 在发送计划片段之前，应用于 BRPC TalkTimeoutController 的超时时间（毫秒）。`BackendServiceClient.sendPlanFragmentAsync` 在调用后端 `execPlanFragmentAsync` 之前设置此值。它控制 BRPC 在从连接池借用空闲连接以及执行发送操作时将等待多长时间；如果超出此时间，RPC 将失败并可能触发方法的重试逻辑。将其设置得更低可以在争用情况下快速失败，或者提高它以容忍瞬时连接池耗尽或慢速网络。请注意：非常大的值可能会延迟故障检测并阻塞请求线程。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- 默认值: 12 * 3600
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 大表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- 默认值: 100
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- 默认值: 2
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- 默认值: 2 * 3600
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 小表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_rows`

- 默认值: 10000000
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 用于查询触发 ANALYZE 任务，判断表是否为小表的阈值。
- 引入版本: v3.4.0

##### `connector_table_query_trigger_task_schedule_interval`

- 默认值: 30
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 调度器线程调度查询触发后台任务的间隔。此项用于替换 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。这里，后台任务在 v3.4 中指 `ANALYZE` 任务，在 v3.4 之后的版本中指低基数列字典的收集任务。
- 引入版本: v3.4.2

##### `create_table_max_serial_replicas`

- 默认值: 128
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 串行创建副本的最大数量。如果实际副本数量超过此值，副本将并发创建。如果表创建时间过长，请尝试减小此值。
- 引入版本: -

##### `default_mv_partition_refresh_number`

- 默认值: 1
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 当物化视图刷新涉及多个分区时，此参数默认控制单批次刷新多少个分区。
从 3.3.0 版本开始，系统默认一次刷新一个分区，以避免潜在的内存溢出 (OOM) 问题。在早期版本中，所有分区默认一次刷新，这可能导致内存耗尽和任务失败。但是，请注意，当物化视图刷新涉及大量分区时，一次只刷新一个分区可能会导致过多的调度开销、更长的整体刷新时间以及大量的刷新记录。在这种情况下，建议适当调整此参数以提高刷新效率并降低调度成本。
- 引入版本: v3.3.0

##### `default_mv_refresh_immediate`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否在创建异步物化视图后立即刷新。当此项设置为 `true` 时，新创建的物化视图将立即刷新。
- 引入版本: v3.2.3

##### `dynamic_partition_check_interval_seconds`

- 默认值: 600
- 类型: 长整型
- 单位: 秒
- 可变: 是
- 描述: 检查新数据的间隔。如果检测到新数据，StarRocks 会自动为数据创建分区。
- 引入版本: -

##### `dynamic_partition_enable`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用动态分区功能。启用此功能后，StarRocks 会为新数据动态创建分区，并自动删除过期分区以确保数据的时效性。
- 引入版本: -

##### `enable_active_materialized_view_schema_strict_check`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 激活非活跃物化视图时是否严格检查数据类型长度一致性。当此项设置为 `false` 时，如果基表中的数据类型长度发生变化，物化视图的激活不受影响。
- 引入版本: v3.3.4

##### `enable_auto_collect_array_ndv`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用 ARRAY 类型 NDV 信息的自动收集。
- 引入版本: v4.0

##### `enable_backup_materialized_view`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 在备份或恢复特定数据库时，是否启用异步物化视图的 BACKUP 和 RESTORE。如果此项设置为 `false`，StarRocks 将跳过备份异步物化视图。
- 引入版本: v3.2.0

##### `enable_collect_full_statistic`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用自动全量统计信息收集。此功能默认启用。
- 引入版本: -

##### `enable_colocate_mv_index`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 创建同步物化视图时是否支持将同步物化视图索引与基表进行 Colocate。如果此项设置为 `true`，tablet sink 将加快同步物化视图的写入性能。
- 引入版本: v3.2.0

##### `enable_decimal_v3`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否支持 DECIMAL V3 数据类型。
- 引入版本: -

##### `enable_experimental_mv`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用异步物化视图功能。TRUE 表示此功能已启用。从 v2.5.2 版本开始，此功能默认启用。对于 v2.5.2 之前的版本，此功能默认禁用。
- 引入版本: v2.4

##### `enable_local_replica_selection`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否为查询选择本地副本。本地副本可降低网络传输成本。如果此参数设置为 TRUE，CBO 优先选择与当前 FE 具有相同 IP 地址的 BE 上的 tablet 副本。如果此参数设置为 `FALSE`，则本地副本和非本地副本都可以选择。
- 引入版本: -

##### `enable_manual_collect_array_ndv`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用 ARRAY 类型 NDV 信息的手动收集。
- 引入版本: v4.0

##### `enable_materialized_view`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用物化视图的创建。
- 引入版本: -

##### `enable_materialized_view_external_table_precise_refresh`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 将此项设置为 `true` 以启用物化视图刷新时的内部优化，当基表是外部（非云原生）表时。启用后，物化视图刷新处理器会计算候选分区并仅刷新受影响的基表分区，而不是所有分区，从而减少 I/O 和刷新成本。将其设置为 `false` 以强制对外部表进行全分区刷新。
- 引入版本: v3.2.9

##### `enable_materialized_view_metrics_collect`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否默认收集异步物化视图的监控指标。
- 引入版本: v3.1.11, v3.2.5

##### `enable_materialized_view_spill`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否为物化视图刷新任务启用中间结果溢出。
- 引入版本: v3.1.1

##### `enable_materialized_view_text_based_rewrite`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否默认启用基于文本的查询重写。如果此项设置为 `true`，系统在创建异步物化视图时会构建抽象语法树。
- 引入版本: v3.2.5

##### `enable_mv_automatic_active_check`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用系统自动检查并重新激活因基表（视图）发生 Schema Change 或被删除并重新创建而设置为非活跃的异步物化视图。请注意，此功能不会重新激活用户手动设置为非活跃的物化视图。
- 引入版本: v3.1.6

##### `enable_mv_automatic_repairing_for_broken_base_tables`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 当此项设置为 `true` 时，当基外部表被删除并重新创建或其表标识符发生变化时，StarRocks 将尝试自动修复物化视图基表元数据。修复流程可以更新物化视图的基表信息，收集外部表分区的分区级修复信息，并根据 `autoRefreshPartitionsLimit` 驱动异步自动刷新物化视图的分区刷新决策。目前，自动修复支持 Hive 外部表；不支持的表类型将导致物化视图被设置为非活跃状态并引发修复异常。分区信息收集是非阻塞的，失败将被记录。
- 引入版本: v3.3.19, v3.4.8, v3.5.6

##### `enable_predicate_columns_collection`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用谓词列收集。如果禁用，谓词列在查询优化期间将不会被记录。
- 引入版本: -

##### `enable_query_queue_v2`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 否
- 描述: 当为 true 时，将基于 FE slot 的查询调度器切换到 Query Queue V2。该标志由 slot 管理器和跟踪器（例如，`BaseSlotManager.isEnableQueryQueueV2` 和 `SlotTracker#createSlotSelectionStrategy`）读取，以选择 `SlotSelectionStrategyV2` 而不是旧版策略。`query_queue_v2_xxx` 配置选项和 `QueryQueueOptions` 仅在此标志启用时生效。从 v4.1 开始，默认值从 `false` 更改为 `true`。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `enable_sql_blacklist`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否启用 SQL 查询黑名单检查。启用此功能后，黑名单中的查询无法执行。
- 引入版本: -

##### `enable_statistic_collect`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 是否为 CBO 收集统计信息。此功能默认启用。
- 引入版本: -

##### `enable_statistic_collect_on_first_load`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 控制由数据加载操作触发的自动统计信息收集和维护。这包括：
  - 首次将数据加载到分区时（分区版本等于 2）的统计信息收集。
  - 将数据加载到多分区表的空分区时的统计信息收集。
  - INSERT OVERWRITE 操作的统计信息复制和更新。

  **统计信息收集类型决策策略：**
  
  - 对于 INSERT OVERWRITE: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。仅复制现有统计信息。
    - 否则，如果 `targetRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  - 对于首次加载: `deltaRatio = loadRows / (totalRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。
    - 否则，如果 `loadRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  **同步行为：**
  
  - 对于 DML 语句 (INSERT INTO/INSERT OVERWRITE): 同步模式，带表锁。加载操作等待统计信息收集完成（最长 `semi_sync_collect_statistic_await_seconds`）。
  - 对于 Stream Load 和 Broker Load: 异步模式，无锁。统计信息收集在后台运行，不阻塞加载操作。
  
  :::note
  禁用此配置将阻止所有加载触发的统计信息操作，包括 INSERT OVERWRITE 的统计信息维护，这可能导致表缺少统计信息。如果频繁创建新表并频繁加载数据，启用此功能将增加内存和 CPU 开销。
  :::

- 引入版本: v3.1

##### `enable_statistic_collect_on_update`

- 默认值: true
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 控制 UPDATE 语句是否可以触发自动统计信息收集。启用后，修改表数据的 UPDATE 操作可能会通过与 `enable_statistic_collect_on_first_load` 控制的基于摄取的统计信息框架相同的机制调度统计信息收集。禁用此配置将跳过 UPDATE 语句的统计信息收集，同时保持加载触发的统计信息收集行为不变。
- 引入版本: v3.5.11, v4.0.4

##### `enable_udf`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 否
- 描述: 是否启用 UDF。
- 引入版本: -

##### `expr_children_limit`

- 默认值: 10000
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 表达式中允许的最大子表达式数量。
- 引入版本: -

##### `histogram_buckets_size`

- 默认值: 64
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 直方图的默认桶数量。
- 引入版本: -

##### `histogram_max_sample_row_count`

- 默认值: 10000000
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 直方图收集的最大行数。
- 引入版本: -

##### `histogram_mcv_size`

- 默认值: 100
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 直方图中最常见值 (MCV) 的数量。
- 引入版本: -

##### `histogram_sample_ratio`

- 默认值: 0.1
- 类型: 双精度浮点型
- 单位: -
- 可变: 是
- 描述: 直方图的采样率。
- 引入版本: -

##### `http_slow_request_threshold_ms`

- 默认值: 5000
- 类型: 整型
- 单位: 毫秒
- 可变: 是
- 描述: 如果 HTTP 请求的响应时间超过此参数指定的值，则会生成日志以跟踪此请求。
- 引入版本: v2.5.15, v3.1.5

##### `lock_checker_enable_deadlock_check`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 启用后，LockChecker 线程使用 ThreadMXBean.findDeadlockedThreads() 执行 JVM 级别的死锁检测，并记录违规线程的堆栈跟踪。检查在 LockChecker 守护进程（其频率由 `lock_checker_interval_second` 控制）内部运行，并将详细的堆栈信息写入日志，这可能会占用 CPU 和 I/O 资源。仅在排查实时或可重现的死锁问题时启用此选项；在正常操作中保持启用状态可能会增加开销和日志量。
- 引入版本: v3.2.0

##### `low_cardinality_threshold`

- 默认值: 255
- 类型: 整型
- 单位: -
- 可变: 否
- 描述: 低基数字典的阈值。
- 引入版本: v3.5.0

##### `materialized_view_min_refresh_interval`

- 默认值: 60
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: ASYNC 物化视图调度允许的最小刷新间隔（秒）。当创建具有基于时间的间隔的物化视图时，该间隔将转换为秒，并且不得小于此值；否则 CREATE/ALTER 操作将因 DDL 错误而失败。如果此值大于 0，则强制执行检查；将其设置为 0 或负值以禁用此限制，这可以防止因过于频繁的刷新而导致过多的 TaskManager 调度和高 FE 内存/CPU 使用率。此项不适用于 `EVENT_TRIGGERED` 刷新。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `materialized_view_refresh_ascending`

- 默认值: false
- 类型: 布尔型
- 单位: -
- 可变: 是
- 描述: 当此项设置为 `true` 时，物化视图分区刷新将按分区键升序（从最旧到最新）迭代分区。当设置为 `false`（默认值）时，系统按降序（从最新到最旧）迭代。StarRocks 在列表分区和范围分区物化视图刷新逻辑中都使用此项，以在应用分区刷新限制时选择要处理的分区，并计算后续 TaskRun 执行的下一个开始/结束分区边界。更改此项会改变首先刷新哪些分区以及如何派生下一个分区范围；对于范围分区物化视图，调度器会验证新的开始/结束，如果更改会创建重复边界（死循环），则会引发错误，因此请谨慎设置此项。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `max_allowed_in_element_num_of_delete`

- 默认值: 10000
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: DELETE 语句中 IN 谓词允许的最大元素数量。
- 引入版本: -

##### `max_create_table_timeout_second`

- 默认值: 600
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 创建表的最大超时时间。
- 引入版本: -

##### `max_distribution_pruner_recursion_depth`

- 默认值: 100
- 类型: 整型
- 单位: -
- 可变: 是
- 描述:: 分区剪枝器允许的最大递归深度。增加递归深度可以剪枝更多元素，但也会增加 CPU 消耗。
- 引入版本: -

##### `max_partitions_in_one_batch`

- 默认值: 4096
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 批量创建分区时可以创建的最大分区数量。
- 引入版本: -

##### `max_planner_scalar_rewrite_num`

- 默认值: 100000
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 优化器可以重写标量操作符的最大次数。
- 引入版本: -

##### `max_query_queue_history_slots_number`

- 默认值: 0
- 类型: 整型
- 单位: Slots
- 可变: 是
- 描述: 控制每个查询队列保留多少最近释放（历史）的已分配 slot 用于监控和可观测性。当 `max_query_queue_history_slots_number` 设置为 `> 0` 的值时，BaseSlotTracker 会在内存队列中保留最多指定数量的最近释放的 LogicalSlot 条目，当超出限制时，会逐出最旧的条目。启用此功能会导致 getSlots() 包含这些历史条目（最新优先），允许 BaseSlotTracker 尝试向 ConnectContext 注册 slot 以获取更丰富的 ExtraMessage 数据，并允许 LogicalSlot.ConnectContextListener 将查询完成元数据附加到历史 slot。当 `max_query_queue_history_slots_number` `<= 0` 时，历史机制被禁用（不使用额外内存）。使用合理的值来平衡可观测性和内存开销。
- 引入版本: v3.5.0

##### `max_query_retry_time`

- 默认值: 2
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: FE 上查询的最大重试次数。
- 引入版本: -

##### `max_running_rollup_job_num_per_table`

- 默认值: 1
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 单个表可以并行运行的 rollup 任务的最大数量。
- 引入版本: -

##### `max_scalar_operator_flat_children`

- 默认值：10000
- 类型：整型
- 单位：-
- 可变: 是
- 描述：ScalarOperator 的最大扁平子节点数量。您可以设置此限制以防止优化器使用过多内存。
- 引入版本: -

##### `max_scalar_operator_optimize_depth`

- 默认值：256
- 类型：整型
- 单位：-
- 可变: 是
- 描述: ScalarOperator 优化可以应用的最大深度。
- 引入版本: -

##### `mv_active_checker_interval_seconds`

- 默认值: 60
- 类型: 长整型
- 单位: 秒
- 可变: 是
- 描述: 当后台 `active_checker` 线程启用时，系统将定期检测并自动重新激活因基表（或视图）的 Schema Change 或重建而变为 Inactive 的物化视图。此参数控制检查器线程的调度间隔，以秒为单位。默认值由系统定义。
- 引入版本: v3.1.6

##### `mv_rewrite_consider_data_layout_mode`

- 默认值: `enable`
- 类型: 字符串
- 单位: -
- 可变: 是
- 描述: 控制物化视图重写在选择最佳物化视图时是否考虑基表数据布局。有效值：
  - `disable`: 在候选物化视图之间选择时，从不使用数据布局标准。
  - `enable`: 仅当查询被识别为对布局敏感时才使用数据布局标准。
  - `force`: 在选择最佳物化视图时始终应用数据布局标准。
  更改此项会影响 `BestMvSelector` 的行为，并根据物理布局是否对计划正确性或性能重要来改进或拓宽重写适用性。
- 引入版本: -

##### `publish_version_interval_ms`

- 默认值: 10
- 类型: 整型
- 单位: 毫秒
- 可变: 否
- 描述: 发布验证任务的发出时间间隔。
- 引入版本: -

##### `query_queue_slots_estimator_strategy`

- 默认值: MAX
- 类型: 字符串
- 单位: -
- 可变: 是
- 描述: 当 `enable_query_queue_v2` 为 true 时，选择用于基于队列的查询的 slot 估算策略。有效值：MBE（基于内存）、PBE（基于并行度）、MAX（取 MBE 和 PBE 的最大值）和 MIN（取 MBE 和 PBE 的最小值）。MBE 根据预测内存或计划成本除以每个 slot 的内存目标来估算 slot，并受 `totalSlots` 限制。PBE 根据片段并行度（扫描范围计数或基数 / 每 slot 行数）和基于 CPU 成本的计算（使用每个 slot 的 CPU 成本）推导 slot，然后将结果限制在 [numSlots/2, numSlots] 范围内。MAX 和 MIN 通过分别取 MBE 和 PBE 的最大值或最小值来组合它们。如果配置值无效，则使用默认值 (`MAX`)。
- 引入版本: v3.5.0

##### `query_queue_v2_concurrency_level`

- 默认值: 4
- 类型: 整型
- 单位: -
- 可变: 是
- 描述: 控制在计算系统总查询 slot 时使用的逻辑并发“层”数量。在 shared-nothing 模式下，总 slot = `query_queue_v2_concurrency_level` * BE 数量 * 每个 BE 的核心数（从 BackendResourceStat 派生）。在多仓库模式下，有效并发会按比例缩小到 max(1, `query_queue_v2_concurrency_level` / 4)。如果配置值为非正数，则视为 `4`。更改此值会增加或减少 totalSlots（从而增加或减少并发查询容量）并影响每个 slot 的资源：memBytesPerSlot 是通过将每个 worker 的内存除以 (cores_per_worker * concurrency) 得出的，CPU 记账使用 `query_queue_v2_cpu_costs_per_slot`。将其设置为与集群大小成比例；非常大的值可能会减少每个 slot 的内存并导致资源碎片化。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_cpu_costs_per_slot`

- 默认值: 1000000000
- 类型: 长整型
- 单位: planner CPU cost units
- 可变: 是
- 描述: 每个 slot 的 CPU 成本阈值，用于根据查询的 planner CPU 成本估算查询所需的 slot 数量。调度器将 slot 计算为整数(`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`)，然后将结果限制在 [1, totalSlots] 范围内（totalSlots 从查询队列 V2 `V2` 参数派生）。V2 代码将非正设置规范化为 1 (Math.max(1, value))，因此非正值实际上变为 `1`。增加此值会减少每个查询分配的 slot（倾向于更少、更大 slot 的查询）；减少此值会增加每个查询的 slot。与 `query_queue_v2_num_rows_per_slot` 和并发设置一起调整，以控制并行度与资源粒度。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_num_rows_per_slot`

- 默认值: 4096
- 类型: 整型
- 单位: 行
- 可变: 是
- 描述: 在估算每个查询的 slot 数量时，分配给单个调度 slot 的目标源行记录数。StarRocks 计算 `estimated_slots` = (Source Node 的基数) / `query_queue_v2_num_rows_per_slot`，然后将结果限制在 [1, totalSlots] 范围内，如果计算值为非正数，则强制最小值为 1。totalSlots 从可用资源（大致为 DOP * `query_queue_v2_concurrency_level` * worker/BE 数量）派生，因此取决于集群/核心计数。增加此值以减少 slot 计数（每个 slot 处理更多行）并降低调度开销；减少此值以增加并行度（更多、更小的 slot），直至达到资源限制。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_schedule_strategy`

- 默认值: SWRR
- 类型: 字符串
- 单位: -
- 可变: 是
- 描述: 选择 Query Queue V2 用于对待处理查询进行排序的调度策略。支持的值（不区分大小写）有 `SWRR` (Smooth Weighted Round Robin) — 默认值，适用于需要公平加权共享的混合/混合工作负载 — 和 `SJF` (Short Job First + Aging) — 优先处理短作业，同时使用老化机制避免饥饿。该值通过不区分大小写的枚举查找进行解析；无法识别的值将记录为错误并使用默认策略。此配置仅在 Query Queue V2 启用时影响行为，并与 V2 大小设置（例如 `query_queue_v2_concurrency_level`）交互。
- 引入版本: v3.3.12, v3.4.2, v3.5.0

##### `semi_sync_collect_statistic_await_seconds`

- 默认值: 30
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: DML 操作（INSERT INTO 和 INSERT OVERWRITE 语句）期间半同步统计信息收集的最大等待时间。Stream Load 和 Broker Load 使用异步模式，不受此配置影响。如果统计信息收集时间超过此值，加载操作将继续，而不等待收集完成。此配置与 `enable_statistic_collect_on_first_load` 协同工作。
- 引入版本: v3.1

##### `slow_query_analyze_threshold`

- 默认值: 5
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述:: 查询触发查询反馈分析的执行时间阈值。
- 引入版本: v3.4.0

##### `statistic_analyze_status_keep_second`

- 默认值: 3 * 24 * 3600
- 类型: 长整型
- 单位: 秒
- 可变: 是
- 描述: 收集任务历史记录的保留时长。默认值为 3 天。
- 引入版本: -

##### `statistic_auto_analyze_end_time`

- 默认值: 23:59:59
- 类型: 字符串
- 单位: -
- 可变: 是
- 描述: 自动收集的结束时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

##### `statistic_auto_analyze_start_time`

- 默认值: 00:00:00
- 类型: 字符串
- 单位: -
- 可变: 是
- 描述: 自动收集的开始时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

##### `statistic_auto_collect_ratio`

- 默认值: 0.8
- 类型: 双精度浮点型
- 单位: -
- 可变: 是
- 描述: 判断自动收集的统计信息是否健康的阈值。如果统计信息健康度低于此阈值，则触发自动收集。
- 引入版本: -

##### `statistic_auto_collect_small_table_rows`

- 默认值: 10000000
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 在自动收集期间，判断外部数据源（Hive, Iceberg, Hudi）中的表是否为小表的阈值。如果表的行数小于此值，则该表被视为小表。
- 引入版本: v3.2

##### `statistic_cache_columns`

- 默认值: 100000
- 类型: 长整型
- 单位: -
- 可变: 否
- 描述: 统计信息表可以缓存的行数。
- 引入版本: -

##### `statistic_cache_thread_pool_size`

- 默认值: 10
- 类型: 整型
- 单位: -
- 可变: 否
- 描述: 用于刷新统计信息缓存的线程池大小。
- 引入版本: -

##### `statistic_collect_interval_sec`

- 默认值: 5 * 60
- 类型: 长整型
- 单位: 秒
- 可变: 是
- 描述: 自动收集期间检查数据更新的间隔。
- 引入版本: -

##### `statistic_max_full_collect_data_size`

- 默认值: 100 * 1024 * 1024 * 1024
- 类型: 长整型
- 单位: 字节
- 可变: 是
- 描述: 统计信息自动收集的数据大小阈值。如果总大小超过此值，则执行采样收集而不是全量收集。
- 引入版本: -

##### `statistic_sample_collect_rows`

- 默认值: 200000
- 类型: 长整型
- 单位: -
- 可变: 是
- 描述: 在加载触发的统计信息操作期间，用于决定 SAMPLE 和 FULL 统计信息收集的行数阈值。如果加载或更改的行数超过此阈值（默认 200,000），则使用 SAMPLE 统计信息收集；否则，使用 FULL 统计信息收集。此设置与 `enable_statistic_collect_on_first_load` 和 `statistic_sample_collect_ratio_threshold_of_first_load` 协同工作。
- 引入版本: -

##### `statistic_update_interval_sec`

- 默认值: 24 * 60 * 60
- 类型: 长整型
- 单位: 秒
- 可变: 是
- 描述: 统计信息缓存的更新间隔。
- 引入版本: -

##### `task_check_interval_second`

- 默认值: 60
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 任务后台作业执行之间的间隔。GlobalStateMgr 使用此值调度 TaskCleaner FrontendDaemon，该守护进程调用 `doTaskBackgroundJob()`；该值乘以 1000 以设置守护进程的间隔（毫秒）。减小该值会使后台维护（任务清理、检查）运行更频繁，响应更快，但会增加 CPU/IO 开销；增加该值会减少开销，但会延迟清理和陈旧任务的检测。调整此值以平衡维护响应能力和资源使用。
- 引入版本: v3.2.0

##### `task_min_schedule_interval_s`

- 默认值: 10
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: SQL 层检查的任务调度允许的最小调度间隔（秒）。提交任务时，TaskAnalyzer 将调度周期转换为秒，如果周期小于 `task_min_schedule_interval_s`，则以 `ERR_INVALID_PARAMETER` 拒绝提交。这可以防止创建运行过于频繁的任务，并保护调度器免受高频任务的影响。如果调度没有明确的开始时间，TaskAnalyzer 会将开始时间设置为当前纪元秒。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `task_runs_timeout_second`

- 默认值: 4 * 3600
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: TaskRun 的默认执行超时时间（秒）。此项用作 TaskRun 执行的基线超时。如果任务运行的属性包含会话变量 `query_timeout` 或 `insert_timeout`，且其值为正整数，则运行时使用该会话超时和 `task_runs_timeout_second` 中较大的值。然后，有效超时将被限制，不得超过配置的 `task_runs_ttl_second` 和 `task_ttl_second`。设置此项以限制任务运行的执行时间。非常大的值可能会被任务/任务运行 TTL 设置截断。
- 引入版本: -

### 加载与卸载

##### `broker_load_default_timeout_second`

- 默认值: 14400
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: Broker Load 作业的超时时长。
- 引入版本: -

##### `desired_max_waiting_jobs`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: FE 中待处理作业的最大数量。该数量指所有作业，例如表创建、加载和 Schema 变更作业。如果 FE 中待处理作业的数量达到此值，FE 将拒绝新的加载请求。此参数仅对异步加载生效。从 v2.5 版本开始，默认值从 100 更改为 1024。
- 引入版本: -

##### `disable_load_job`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 当集群遇到错误时是否禁用加载。这可以防止集群错误造成的任何损失。默认值为 `FALSE`，表示未禁用加载。`TRUE` 表示禁用加载，集群处于只读状态。
- 引入版本: -

##### `empty_load_as_error`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 如果没有加载任何数据，是否返回错误消息 "all partitions have no load data"。有效值：
  - `true`: 如果没有加载任何数据，系统将显示失败消息并返回错误 "all partitions have no load data"。
  - `false`: 如果没有加载任何数据，系统将显示成功消息并返回 OK，而不是错误。
- 引入版本: -

##### `enable_file_bundling`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否为云原生表启用 File Bundling 优化。当此功能启用（设置为 `true`）时，系统会自动捆绑加载、Compaction 或 Publish 操作生成的数据文件，从而降低高频访问外部存储系统造成的 API 成本。您还可以使用 CREATE TABLE 属性 `file_bundling` 在表级别控制此行为。有关详细说明，请参阅 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。
- 引入版本: v4.0

##### `enable_routine_load_lag_metrics`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否收集 Routine Load Kafka 分区偏移量滞后指标。请注意，将此项设置为 `true` 将调用 Kafka API 获取分区的最新偏移量。
- 引入版本: -

##### `enable_sync_publish`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: 是
- 描述: 是否在加载事务的发布阶段同步执行 apply 任务。此参数仅适用于主键表。有效值：
  - `TRUE` (默认值): apply 任务在加载事务的发布阶段同步执行。这意味着只有在 apply 任务完成后，加载事务才会被报告为成功，并且加载的数据才能真正被查询。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `true` 可以提高查询性能和稳定性，但可能会增加加载延迟。
  - `FALSE`: apply 任务在加载事务的发布阶段异步执行。这意味着在 apply 任务提交后，加载事务即被报告为成功，但加载的数据不能立即被查询。在这种情况下，并发查询需要等待 apply 任务完成或超时后才能继续。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `false` 可能会影响查询性能和稳定性。
- 引入版本: v3.2.0

##### `export_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: 否
- 描述: 加载作业的调度时间间隔。
- 引入版本: -

##### `export_max_bytes_per_be_per_task`

- 默认值: 268435456
- 类型: Long
- 单位: 字节
- 是否可变: 是
- 描述: 单个数据卸载任务可从单个 BE 导出的最大数据量。
- 引入版本: -

##### `export_running_job_num_limit`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 可并行运行的数据导出任务的最大数量。
- 引入版本: -

##### `export_task_default_timeout_second`

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 数据导出任务的超时时长。
- 引入版本: -

##### `export_task_pool_size`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 卸载任务线程池的大小。
- 引入版本: -

##### `external_table_commit_timeout_ms`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: 是
- 描述: 将写入事务提交（发布）到 StarRocks 外部表的超时时长。默认值 `10000` 表示 10 秒的超时时长。
- 引入版本: -

##### `finish_transaction_default_lock_timeout_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: 是
- 描述: 在完成事务期间获取数据库和表锁的默认超时时间。
- 引入版本: v4.0.0, v3.5.8

##### `history_job_keep_max_second`

- 默认值: 7 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 历史作业（例如 Schema 变更作业）可保留的最长时间。
- 引入版本: -

##### `insert_load_default_timeout_second`

- 默认值: 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 用于加载数据的 INSERT INTO 语句的超时时长。
- 引入版本: -

##### `label_clean_interval_second`

- 默认值: 4 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 否
- 描述: 清理标签的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保历史标签能够及时清理。
- 引入版本: -

##### `label_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 在一段时间内可保留的加载作业的最大数量。如果超出此数量，历史作业的信息将被删除。
- 引入版本: -

##### `label_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 保留已完成且处于 FINISHED 或 CANCELLED 状态的加载作业标签的最长持续时间（秒）。默认值为 3 天。此持续时间到期后，标签将被删除。此参数适用于所有类型的加载作业。值过大会消耗大量内存。
- 引入版本: -

##### `load_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: 否
- 描述: 加载作业滚动处理的时间间隔。
- 引入版本: -

##### `load_parallel_instance_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 控制为 Broker Load 和 Stream Load 在单个主机上创建的并行加载片段实例的数量。LoadPlanner 使用此值作为每主机的并行度，除非会话启用自适应 sink DOP；如果会话变量 `enable_adaptive_sink_dop` 为 true，则会话的 `sink_degree_of_parallelism` 将覆盖此配置。当需要 shuffle 时，此值应用于片段并行执行（扫描片段和 sink 片段并行执行实例）。当不需要 shuffle 时，它用作 sink pipeline DOP。注意：从本地文件加载时，强制使用单个实例（pipeline DOP = 1，parallel exec = 1）以避免本地磁盘争用。增加此数量会提高每主机的并发性和吞吐量，但可能会增加 CPU、内存和 I/O 争用。
- 引入版本: v3.2.0

##### `load_straggler_wait_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: BE 副本可容忍的最大加载延迟。如果超过此值，将执行克隆操作以从其他副本克隆数据。
- 引入版本: -

##### `loads_history_retained_days`

- 默认值: 30
- 类型: Int
- 单位: 天
- 是否可变: 是
- 描述: 在内部 `_statistics_.loads_history` 表中保留加载历史记录的天数。此值用于表创建，以设置表属性 `partition_live_number`，并传递给 `TableKeeper`（最小值为 1）以确定要保留多少个每日分区。增加或减少此值会调整已完成的加载作业在每日分区中保留的时间；它会影响新表的创建和 keeper 的修剪行为，但不会自动重新创建过去的 partitions。`LoadsHistorySyncer` 在管理加载历史记录生命周期时依赖此保留策略；其同步频率由 `loads_history_sync_interval_second` 控制。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### `loads_history_sync_interval_second`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: `LoadsHistorySyncer` 用于调度从 `information_schema.loads` 到内部 `_statistics_.loads_history` 表的已完成加载作业的定期同步的时间间隔（秒）。该值在构造函数中乘以 1000 以设置 FrontendDaemon 间隔。同步器跳过第一次运行（以允许表创建），并且只导入一分钟前完成的加载；较小的值会增加 DML 和执行器负载，而较大的值会延迟历史加载记录的可用性。有关目标表的保留/分区行为，请参阅 `loads_history_retained_days`。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

##### `max_broker_load_job_concurrency`

- 默认值: 5
- 别名: `async_load_task_pool_size`
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: StarRocks 集群中允许的最大并发 Broker Load 作业数量。此参数仅对 Broker Load 有效。此参数的值必须小于 `max_running_txn_num_per_db` 的值。从 v2.5 版本开始，默认值从 `10` 更改为 `5`。
- 引入版本: -

##### `max_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 加载作业允许的最大超时时长。如果超出此限制，加载作业将失败。此限制适用于所有类型的加载作业。
- 引入版本: -

##### `max_routine_load_batch_size`

- 默认值: 4294967296
- 类型: Long
- 单位: 字节
- 是否可变: 是
- 描述: Routine Load 任务可加载的最大数据量。
- 引入版本: -

##### `max_routine_load_task_concurrent_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 每个 Routine Load 作业的最大并发任务数量。
- 引入版本: -

##### `max_routine_load_task_num_per_be`

- 默认值: 16
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 每个 BE 上最大并发 Routine Load 任务数量。从 v3.1.0 版本开始，此参数的默认值从 5 增加到 16，并且不再需要小于或等于 BE 静态参数 `routine_load_thread_pool_size`（已弃用）的值。
- 引入版本: -

##### `max_running_txn_num_per_db`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: StarRocks 集群中每个数据库允许运行的最大加载事务数量。默认值为 `1000`。从 v3.1 版本开始，默认值从 `100` 更改为 `1000`。当数据库实际运行的加载事务数量超过此参数的值时，新的加载请求将不会被处理。同步加载作业的新请求将被拒绝，异步加载作业的新请求将被放入队列。我们不建议您增加此参数的值，因为这会增加系统负载。
- 引入版本: -

##### `max_stream_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: Stream Load 作业允许的最大超时时长。
- 引入版本: -

##### `max_tolerable_backend_down_num`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 允许的最大故障 BE 节点数量。如果超过此数量，Routine Load 作业将无法自动恢复。
- 引入版本: -

##### `min_bytes_per_broker_scanner`

- 默认值: 67108864
- 类型: Long
- 单位: 字节
- 是否可变: 是
- 描述: Broker Load 实例可处理的最小数据量。
- 引入版本: -

##### `min_load_timeout_second`

- 默认值: 1
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 加载作业允许的最小超时时长。此限制适用于所有类型的加载作业。
- 引入版本: -

##### `min_routine_load_lag_for_metrics`

- 默认值: 10000
- 类型: INT
- 单位: -
- 是否可变: 是
- 描述: 在监控指标中显示的 Routine Load 作业的最小偏移量滞后。偏移量滞后大于此值的 Routine Load 作业将显示在指标中。
- 引入版本: -

##### `period_of_auto_resume_min`

- 默认值: 5
- 类型: Int
- 单位: 分钟
- 是否可变: 是
- 描述: Routine Load 作业自动恢复的时间间隔。
- 引入版本: -

##### `prepared_transaction_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 预处理事务的默认超时时长。
- 引入版本: -

##### `routine_load_task_consume_second`

- 默认值: 15
- 类型: Long
- 单位: 秒
- 是否可变: 是
- 描述: 集群中每个 Routine Load 任务消耗数据的最长时间。从 v3.1.0 版本开始，Routine Load 作业在 [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中支持一个新的参数 `task_consume_second`。此参数适用于 Routine Load 作业中的单个加载任务，这更灵活。
- 引入版本: -

##### `routine_load_task_timeout_second`

- 默认值: 60
- 类型: Long
- 单位: 秒
- 是否可变: 是
- 描述: 集群中每个 Routine Load 任务的超时时长。从 v3.1.0 版本开始，Routine Load 作业在 [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) 中支持一个新的参数 `task_timeout_second`。此参数适用于 Routine Load 作业中的单个加载任务，这更灵活。
- 引入版本: -

##### `routine_load_unstable_threshold_second`

- 默认值: 3600
- 类型: Long
- 单位: 秒
- 是否可变: 是
- 描述: 如果 Routine Load 作业中的任何任务滞后，则 Routine Load 作业将设置为 UNSTABLE 状态。具体来说，如果正在消费的消息的时间戳与当前时间之间的差值超过此阈值，并且数据源中存在未消费的消息。
- 引入版本: -

##### `spark_dpp_version`

- 默认值: 1.0.0
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 使用的 Spark 动态分区裁剪 (DPP) 版本。
- 引入版本: -

##### `spark_home_default_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: Spark 客户端的根目录。
- 引入版本: -

##### `spark_launcher_log_dir`

- 默认值: `sys_log_dir` + "/spark_launcher_log"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 存储 Spark 日志文件的目录。
- 引入版本: -

##### `spark_load_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 每个 Spark Load 作业的超时时长。
- 引入版本: -

##### `spark_load_submit_timeout_second`

- 默认值: 300
- 类型: long
- 单位: 秒
- 是否可变: 否
- 描述: 提交 Spark 应用程序后等待 YARN 响应的最长时间（秒）。`SparkLauncherMonitor.LogMonitor` 将此值转换为毫秒，如果作业在 UNKNOWN/CONNECTED/SUBMITTED 状态停留时间超过此超时，将停止监控并强制终止 Spark 启动器进程。`SparkLoadJob` 将此配置作为默认值读取，并允许通过 `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` 属性进行每个加载的覆盖。将其设置得足够高以适应 YARN 队列延迟；设置过低可能会中止合法排队的作业，而设置过高可能会延迟故障处理和资源清理。
- 引入版本: v3.2.0

##### `spark_resource_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: Spark 依赖包的根目录。
- 引入版本: -

##### `stream_load_default_timeout_second`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 每个 Stream Load 作业的默认超时时长。
- 引入版本: -

##### `stream_load_max_txn_num_per_be`

- 默认值: -1
- 类型: Int
- 单位: 事务
- 是否可变: 是
- 描述: 限制单个 BE（后端）主机接受的并发 Stream Load 事务的数量。当设置为非负整数时，FrontendServiceImpl 会检查 BE（按客户端 IP）的当前事务计数，如果计数 `>=` 此限制，则拒绝新的 Stream Load 开始请求。值 `< 0` 会禁用此限制（无限制）。此检查发生在 Stream Load 开始期间，超出时可能会导致 `streamload txn num per be exceeds limit` 错误。相关的运行时行为使用 `stream_load_default_timeout_second` 作为请求超时回退。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

##### `stream_load_task_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: `StreamLoadMgr` 在内存中保留的 Stream Load 任务的最大数量（跨所有数据库全局）。当跟踪任务的数量 (`idToStreamLoadTask`) 超过此阈值时，`StreamLoadMgr` 首先调用 `cleanSyncStreamLoadTasks()` 来删除已完成的同步 Stream Load 任务；如果大小仍然大于此阈值的一半，它会调用 `cleanOldStreamLoadTasks(true)` 来强制删除较旧或已完成的任务。增加此值可在内存中保留更多任务历史记录；减小此值可减少内存使用并使清理更积极。此值仅控制内存中的保留，不影响持久化/重放的任务。
- 引入版本: v3.2.0

##### `stream_load_task_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 已完成或已取消的 Stream Load 任务的保留窗口。当任务达到最终状态且其结束时间戳早于此阈值 (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`) 时，它将有资格被 `StreamLoadMgr.cleanOldStreamLoadTasks` 删除，并在加载持久化状态时被丢弃。适用于 `StreamLoadTask` 和 `StreamLoadMultiStmtTask`。如果总任务计数超过 `stream_load_task_keep_max_num`，清理可能会提前触发（同步任务由 `cleanSyncStreamLoadTasks` 优先处理）。设置此值以平衡历史记录/可调试性与内存使用。
- 引入版本: v3.2.0

##### `transaction_clean_interval_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: 否
- 描述: 清理已完成事务的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保已完成的事务能够及时清理。
- 引入版本: -

##### `transaction_stream_load_coordinator_cache_capacity`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 存储从事务标签到协调器节点映射的缓存容量。
- 引入版本: -

##### `transaction_stream_load_coordinator_cache_expire_seconds`

- 默认值: 900
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 协调器映射在缓存中保留的时间（TTL），之后将被逐出。
- 引入版本: -

##### `yarn_client_path`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: Yarn 客户端包的根目录。
- 引入版本: -

##### `yarn_config_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- 类型: String
- 单位: -
- 是否可变: 否
- 描述: 存储 Yarn 配置文件的目录。
- 引入版本: -

### 统计报告

##### `enable_collect_warehouse_metrics`

- 默认值: true
- 类型: 布尔
- 单位: -
- 可变: 是
- 描述: 当此项设置为 `true` 时，系统将收集并导出每个仓库的指标。启用它会将仓库级别的指标（slot/usage/availability）添加到指标输出中，并增加指标基数和收集开销。禁用它将省略特定于仓库的指标，并减少 CPU/网络和监控存储成本。
- 引入版本: v3.5.0

##### `enable_http_detail_metrics`

- 默认值: false
- 类型: 布尔
- 单位: -
- 可变: 是
- 描述: 当为 true 时，HTTP 服务器会计算并公开详细的 HTTP worker 指标（特别是 `HTTP_WORKER_PENDING_TASKS_NUM` 仪表）。启用此功能会导致服务器遍历 Netty worker 执行器，并在每个 `NioEventLoop` 上调用 `pendingTasks()` 以汇总待处理任务计数；禁用时，仪表返回 0 以避免此开销。这种额外的收集可能对 CPU 和延迟敏感——仅在调试或详细调查时启用。
- 引入版本: v3.2.3

##### `proc_profile_collect_time_s`

- 默认值: 120
- 类型: 整型
- 单位: 秒
- 可变: 是
- 描述: 单次进程配置文件收集的持续时间（秒）。当 `proc_profile_cpu_enable` 或 `proc_profile_mem_enable` 设置为 `true` 时，AsyncProfiler 启动，收集器线程休眠此持续时间，然后分析器停止并写入配置文件。较大的值会增加样本覆盖率和文件大小，但会延长分析器运行时并延迟后续收集；较小的值会减少开销，但可能产生不足的样本。确保此值与保留设置（例如 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes`）保持一致。
- 引入版本: v3.2.12

### 存储

##### `alter_table_timeout_second`

- Default: 86400
- Type: Int
- Unit: 秒
- Is mutable: 是
- Description: 模式变更操作 (ALTER TABLE) 的超时时长。
- Introduced in: -

##### `capacity_used_percent_high_water`

- Default: 0.75
- Type: double
- Unit: 分数 (0.0–1.0)
- Is mutable: 是
- Description: 计算 BE 负载分数时使用的磁盘容量使用百分比（总容量的百分比）的高水位阈值。`BackendLoadStatistic.calcSore` 使用 `capacity_used_percent_high_water` 来设置 `LoadScore.capacityCoefficient`：如果 BE 的使用百分比小于 0.5，则系数等于 0.5；如果使用百分比 `>` `capacity_used_percent_high_water`，则系数等于 1.0；否则，系数通过 (2 * usedPercent - 0.5) 与使用百分比线性过渡。当系数为 1.0 时，负载分数完全由容量比例决定；较低的值会增加副本数量的权重。调整此值会改变均衡器惩罚磁盘利用率高的 BE 的激进程度。
- Introduced in: v3.2.0

##### `catalog_trash_expire_second`

- Default: 86400
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 数据库、表或分区被删除后，元数据可以保留的最长时间。如果此持续时间过期，数据将被删除，并且无法通过 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 命令恢复。
- Introduced in: -

##### `catalog_recycle_bin_erase_min_latency_ms`

- 默认值: 600000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 数据库、表或分区被删除后，擦除元数据前的最小延迟时间（毫秒）。用于避免擦除日志先于删除日志写入。
- 引入版本: -

##### `catalog_recycle_bin_erase_max_operations_per_cycle`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个周期内从回收站中实际删除数据库、表或分区的最大擦除操作数。擦除操作会持有锁，因此单批次不宜过大。
- 引入版本: -

##### `catalog_recycle_bin_erase_fail_retry_interval_ms`

- 默认值: 60000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 回收站擦除操作失败后的重试间隔时间（毫秒）。
- 引入版本: -

##### `check_consistency_default_timeout_second`

- Default: 600
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 副本一致性检查的超时时长。您可以根据 tablet 的大小设置此参数。
- Introduced in: -

##### `consistency_check_cooldown_time_second`

- Default: 24 * 3600
- Type: Int
- Unit: 秒
- Is mutable: 是
- Description: 控制同一 tablet 之间一致性检查所需的最小间隔（秒）。在 tablet 选择期间，只有当 `tablet.getLastCheckTime()` 小于 `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` 时，tablet 才被视为符合条件。默认值 (24 * 3600) 大致强制每个 tablet 每天检查一次，以减少 BE 磁盘 I/O。降低此值会增加检查频率和资源使用；提高此值会减少 I/O，但会以较慢地检测不一致为代价。在从索引的 tablet 列表中过滤掉冷却中的 tablet 时，此值是全局应用的。
- Introduced in: v3.5.5

##### `consistency_check_end_time`

- Default: "4"
- Type: String
- Unit: 一天中的小时 (0-23)
- Is mutable: 否
- Description: 指定 ConsistencyChecker 工作窗口的结束小时（一天中的小时）。该值使用系统时区中的 SimpleDateFormat("HH") 进行解析，并接受 0-23（一位或两位数字）。StarRocks 将其与 `consistency_check_start_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口会跨越午夜（例如，默认值为 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器永远不会运行。解析失败将导致 FE 启动时记录错误并退出，因此请提供一个有效的小时字符串。
- Introduced in: v3.2.0

##### `consistency_check_start_time`

- Default: "23"
- Type: String
- Unit: 一天中的小时 (00-23)
- Is mutable: 否
- Description: 指定 ConsistencyChecker 工作窗口的开始小时（一天中的小时）。该值使用系统时区中的 SimpleDateFormat("HH") 进行解析，并接受 0-23（一位或两位数字）。StarRocks 将其与 `consistency_check_end_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口会跨越午夜（例如，默认值为 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器永远不会运行。解析失败将导致 FE 启动时记录错误并退出，因此请提供一个有效的小时字符串。
- Introduced in: v3.2.0

##### `consistency_tablet_meta_check_interval_ms`

- Default: 2 * 3600 * 1000
- Type: Int
- Unit: 毫秒
- Is mutable: 是
- Description: ConsistencyChecker 用于在 `TabletInvertedIndex` 和 `LocalMetastore` 之间运行完整 tablet 元数据一致性扫描的间隔。`runAfterCatalogReady` 中的守护进程在 `current time - lastTabletMetaCheckTime` 超过此值时触发 checkTabletMetaConsistency。当首次检测到无效 tablet 时，其 `toBeCleanedTime` 设置为 `now + (consistency_tablet_meta_check_interval_ms / 2)`，因此实际删除会延迟到后续扫描。增加此值可减少扫描频率和负载（清理速度较慢）；减少此值可更快地检测和删除过时 tablet（开销较高）。
- Introduced in: v3.2.0

##### `default_replication_num`

- Default: 3
- Type: Short
- Unit: -
- Is mutable: 是
- Description: 在 StarRocks 中创建表时，设置每个数据分区的默认副本数。在创建表时，可以通过在 CREATE TABLE DDL 中指定 `replication_num=x` 来覆盖此设置。
- Introduced in: -

##### `enable_auto_tablet_distribution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否自动设置分桶数量。
  - 如果此参数设置为 `TRUE`，则在创建表或添加分区时无需指定分桶数量。StarRocks 会自动确定分桶数量。
  - 如果此参数设置为 `FALSE`，则在创建表或添加分区时需要手动指定分桶数量。如果为表添加新分区时未指定分桶数量，则新分区将继承表创建时设置的分桶数量。但是，您也可以手动为新分区指定分桶数量。
- Introduced in: v2.5.7

##### `enable_experimental_rowstore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否启用 [行存列存混合存储](../../table_design/hybrid_table.md) 功能。
- Introduced in: v3.2.3

##### `enable_fast_schema_evolution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否为 StarRocks 集群中的所有表启用快速 Schema 变更。有效值为 `TRUE` 和 `FALSE`（默认）。启用快速 Schema 变更可以提高 Schema 变更的速度，并在添加或删除列时减少资源使用。
- Introduced in: v3.2.0

> **注意**
>
> - StarRocks 存算分离集群从 v3.3.0 开始支持此参数。
> - 如果您需要为特定表配置快速 Schema 变更，例如禁用特定表的快速 Schema 变更，您可以在表创建时设置表属性 [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution)。

##### `enable_online_optimize_table`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 控制 StarRocks 在创建优化作业时是否使用非阻塞的在线优化路径。当 `enable_online_optimize_table` 为 true 且目标表满足兼容性检查（没有分区/键/排序规范，分布不是 `RandomDistributionDesc`，存储类型不是 `COLUMN_WITH_ROW`，启用了副本存储，并且该表不是云原生表或物化视图）时，规划器会创建一个 `OnlineOptimizeJobV2` 来执行优化而不会阻塞写入。如果为 false 或任何兼容性条件失败，StarRocks 将回退到 `OptimizeJobV2`，这可能会在优化期间阻塞写入操作。
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### `enable_strict_storage_medium_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 用户创建表时，FE 是否严格检查 BE 的存储介质。如果此参数设置为 `TRUE`，则 FE 在用户创建表时会检查 BE 的存储介质，如果 BE 的存储介质与 CREATE TABLE 语句中指定的 `storage_medium` 参数不同，则返回错误。例如，CREATE TABLE 语句中指定的存储介质是 SSD，但 BE 的实际存储介质是 HDD。结果，表创建失败。如果此参数为 `FALSE`，则 FE 在用户创建表时不会检查 BE 的存储介质。
- Introduced in: -

##### `max_bucket_number_per_partition`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 一个分区中可以创建的最大分桶数量。
- Introduced in: v3.3.2

##### `max_column_number_per_table`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 一个表中可以创建的最大列数。
- Introduced in: v3.3.2

##### `max_dynamic_partition_num`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 限制在分析或创建动态分区表时一次可以创建的最大分区数量。在动态分区属性验证期间，`systemtask_runs_max_history_number` 计算预期分区（结束偏移量 + 历史分区数量），如果总数超过 `max_dynamic_partition_num`，则抛出 DDL 错误。仅当您预期有合法的大分区范围时才提高此值；增加此值允许创建更多分区，但会增加元数据大小、调度工作和操作复杂性。
- Introduced in: v3.2.0

##### `max_partition_number_per_table`

- Default: 100000
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 一个表中可以创建的最大分区数量。
- Introduced in: v3.3.2

##### `max_task_consecutive_fail_count`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 任务在调度器自动暂停之前可能连续失败的最大次数。当 `TaskSource.MV.equals(task.getSource())` 且 `max_task_consecutive_fail_count` 大于 0 时，如果任务的连续失败计数器达到或超过 `max_task_consecutive_fail_count`，则该任务将通过 TaskManager 暂停，对于物化视图任务，物化视图将被停用。将抛出异常，指示暂停以及如何重新激活（例如，`ALTER MATERIALIZED VIEW <mv_name> ACTIVE`）。将此项设置为 0 或负值以禁用自动暂停。
- Introduced in: -

##### `partition_recycle_retention_period_secs`

- Default: 1800
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 通过 INSERT OVERWRITE 或物化视图刷新操作删除的分区的元数据保留时间。请注意，此类元数据无法通过执行 [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) 恢复。
- Introduced in: v3.5.9

##### `recover_with_empty_tablet`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否用空 tablet 替换丢失或损坏的 tablet 副本。如果 tablet 副本丢失或损坏，对此 tablet 或其他健康 tablet 的数据查询可能会失败。用空 tablet 替换丢失或损坏的 tablet 副本可确保查询仍能执行。但是，结果可能不正确，因为数据已丢失。默认值为 `FALSE`，这意味着丢失或损坏的 tablet 副本不会被空 tablet 替换，并且查询会失败。
- Introduced in: -

##### `storage_usage_hard_limit_percent`

- Default: 95
- Alias: `storage_flood_stage_usage_percent`
- Type: Int
- Unit: -
- Is mutable: 是
- Description: BE 目录中存储使用百分比的硬限制。如果 BE 存储目录的存储使用率（百分比）超过此值且剩余存储空间小于 `storage_usage_hard_limit_reserve_bytes`，则 Load 和 Restore 作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_usage_percent` 一起设置，以使配置生效。
- Introduced in: -

##### `storage_usage_hard_limit_reserve_bytes`

- Default: 100 * 1024 * 1024 * 1024
- Alias: `storage_flood_stage_left_capacity_bytes`
- Type: Long
- Unit: 字节
- Is mutable: 是
- Description: BE 目录中剩余存储空间的硬限制。如果 BE 存储目录中剩余存储空间小于此值且存储使用率（百分比）超过 `storage_usage_hard_limit_percent`，则 Load 和 Restore 作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_left_capacity_bytes` 一起设置，以使配置生效。
- Introduced in: -

##### `storage_usage_soft_limit_percent`

- Default: 90
- Alias: `storage_high_watermark_usage_percent`
- Type: Int
- Unit: -
- Is mutable: 是
- Description: BE 目录中存储使用百分比的软限制。如果 BE 存储目录的存储使用率（百分比）超过此值且剩余存储空间小于 `storage_usage_soft_limit_reserve_bytes`，则 tablet 无法克隆到此目录。
- Introduced in: -

##### `storage_usage_soft_limit_reserve_bytes`

- Default: 200 * 1024 * 1024 * 1024
- Alias: `storage_min_left_capacity_bytes`
- Type: Long
- Unit: 字节
- Is mutable: 是
- Description: BE 目录中剩余存储空间的软限制。如果 BE 存储目录中剩余存储空间小于此值且存储使用率（百分比）超过 `storage_usage_soft_limit_percent`，则 tablet 无法克隆到此目录。
- Introduced in: -

##### `tablet_checker_lock_time_per_cycle_ms`

- Default: 1000
- Type: Int
- Unit: 毫秒
- Is mutable: 是
- Description: tablet 检查器在释放并重新获取表锁之前，每个周期最大持有锁的时间。小于 100 的值将被视为 100。
- Introduced in: v3.5.9, v4.0.2

##### `tablet_create_timeout_second`

- Default: 10
- Type: Int
- Unit: 秒
- Is mutable: 是
- Description: 创建 tablet 的超时时长。从 v3.1 开始，默认值从 1 更改为 10。
- Introduced in: -

##### `tablet_delete_timeout_second`

- Default: 2
- Type: Int
- Unit: 秒
- Is mutable: 是
- Description: 删除 tablet 的超时时长。
- Introduced in: -

##### `tablet_sched_balance_load_disk_safe_threshold`

- Default: 0.5
- Alias: `balance_load_disk_safe_threshold`
- Type: Double
- Unit: -
- Is mutable: 是
- Description: 判断 BE 磁盘使用率是否均衡的百分比阈值。如果所有 BE 的磁盘使用率都低于此值，则认为均衡。如果磁盘使用率高于此值，并且最高和最低 BE 磁盘使用率之间的差异大于 10%，则认为磁盘使用率不均衡，并触发 tablet 重新均衡。
- Introduced in: -

##### `tablet_sched_balance_load_score_threshold`

- Default: 0.1
- Alias: `balance_load_score_threshold`
- Type: Double
- Unit: -
- Is mutable: 是
- Description: 判断 BE 负载是否均衡的百分比阈值。如果 BE 的负载低于所有 BE 的平均负载且差异大于此值，则该 BE 处于低负载状态。相反，如果 BE 的负载高于平均负载且差异大于此值，则该 BE 处于高负载状态。
- Introduced in: -

##### `tablet_sched_be_down_tolerate_time_s`

- Default: 900
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 调度器允许 BE 节点保持非活动状态的最长时间。达到时间阈值后，该 BE 节点上的 tablet 将迁移到其他活动 BE 节点。
- Introduced in: v2.5.7

##### `tablet_sched_disable_balance`

- Default: false
- Alias: `disable_balance`
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否禁用 tablet 均衡。`TRUE` 表示禁用 tablet 均衡。`FALSE` 表示启用 tablet 均衡。
- Introduced in: -

##### `tablet_sched_disable_colocate_balance`

- Default: false
- Alias: `disable_colocate_balance`
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否禁用 Colocate Table 的副本均衡。`TRUE` 表示禁用副本均衡。`FALSE` 表示启用副本均衡。
- Introduced in: -

##### `tablet_sched_max_balancing_tablets`

- Default: 500
- Alias: `max_balancing_tablets`
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 可以同时进行均衡的 tablet 的最大数量。如果超过此值，将跳过 tablet 重新均衡。
- Introduced in: -

##### `tablet_sched_max_clone_task_timeout_sec`

- Default: 2 * 60 * 60
- Alias: `max_clone_task_timeout_sec`
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 克隆 tablet 的最大超时时长。
- Introduced in: -

##### `tablet_sched_max_not_being_scheduled_interval_ms`

- Default: 15 * 60 * 1000
- Type: Long
- Unit: 毫秒
- Is mutable: 是
- Description: 当 tablet 克隆任务正在调度时，如果一个 tablet 在此参数指定的时间内未被调度，StarRocks 会赋予它更高的优先级，以便尽快调度。
- Introduced in: -

##### `tablet_sched_max_scheduling_tablets`

- Default: 10000
- Alias: `max_scheduling_tablets`
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 可以同时调度的 tablet 的最大数量。如果超过此值，将跳过 tablet 均衡和修复检查。
- Introduced in: -

##### `tablet_sched_min_clone_task_timeout_sec`

- Default: 3 * 60
- Alias: `min_clone_task_timeout_sec`
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 克隆 tablet 的最小超时时长。
- Introduced in: -

##### `tablet_sched_num_based_balance_threshold_ratio`

- Default: 0.5
- Alias: -
- Type: Double
- Unit: -
- Is mutable: 是
- Description: 基于数量的均衡可能会破坏磁盘大小均衡，但磁盘之间的最大差距不能超过 `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold`。如果集群中存在 tablet 不断从 A 均衡到 B，又从 B 均衡到 A 的情况，请减小此值。如果您希望 tablet 分布更均衡，请增大此值。
- Introduced in: - 3.1

##### `tablet_sched_repair_delay_factor_second`

- Default: 60
- Alias: `tablet_repair_delay_factor_second`
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 副本修复的间隔时间，单位为秒。
- Introduced in: -

##### `tablet_sched_slot_num_per_path`

- Default: 8
- Alias: `schedule_slot_num_per_path`
- Type: Int
- Unit: -
- Is mutable: 是
- Description: BE 存储目录中可以同时运行的 tablet 相关任务的最大数量。从 v2.5 开始，此参数的默认值从 `4` 更改为 `8`。
- Introduced in: -

##### `tablet_sched_storage_cooldown_second`

- Default: -1
- Alias: `storage_cooldown_second`
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: 从表创建时间开始的自动冷却延迟。默认值 `-1` 表示禁用自动冷却。如果要启用自动冷却，请将此参数设置为大于 `-1` 的值。
- Introduced in: -

##### `tablet_stat_update_interval_second`

- Default: 300
- Type: Int
- Unit: 秒
- Is mutable: 否
- Description: FE 从每个 BE 获取 tablet 统计信息的时间间隔。
- Introduced in: -

### 共享数据

##### `aws_s3_access_key`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于访问您的 S3 存储桶的 Access Key ID。
- 引入版本：v3.0

##### `aws_s3_endpoint`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于访问您的 S3 存储桶的端点，例如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本：v3.0

##### `aws_s3_external_id`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于跨账号访问您的 S3 存储桶的 AWS 账号的外部 ID。
- 引入版本：v3.0

##### `aws_s3_iam_role_arn`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：具有访问您的 S3 存储桶（存储数据文件）权限的 IAM 角色的 ARN。
- 引入版本：v3.0

##### `aws_s3_path`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 S3 路径。它由您的 S3 存储桶名称及其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本：v3.0

##### `aws_s3_region`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 S3 存储桶所在的区域，例如 `us-west-2`。
- 引入版本：v3.0

##### `aws_s3_secret_key`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于访问您的 S3 存储桶的 Secret Access Key。
- 引入版本：v3.0

##### `aws_s3_use_aws_sdk_default_behavior`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用 AWS SDK 的默认认证凭证。有效值：true 和 false（默认）。
- 引入版本：v3.0

##### `aws_s3_use_instance_profile`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用 Instance Profile 和 Assumed Role 作为访问 S3 的凭证方法。有效值：true 和 false（默认）。
  - 如果您使用基于 IAM 用户的凭证（Access Key 和 Secret Key）访问 S3，则必须将此项指定为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。
  - 如果您使用 Instance Profile 访问 S3，则必须将此项指定为 `true`。
  - 如果您使用 Assumed Role 访问 S3，则必须将此项指定为 `true`，并指定 `aws_s3_iam_role_arn`。
  - 如果您使用外部 AWS 账号，则还必须指定 `aws_s3_external_id`。
- 引入版本：v3.0

##### `azure_adls2_endpoint`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 Azure Data Lake Storage Gen2 账号的端点，例如 `https://test.dfs.core.windows.net`。
- 引入版本：v3.4.1

##### `azure_adls2_oauth2_client_id`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Data Lake Storage Gen2 请求的 Managed Identity 的客户端 ID。
- 引入版本：v3.4.4

##### `azure_adls2_oauth2_tenant_id`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Data Lake Storage Gen2 请求的 Managed Identity 的租户 ID。
- 引入版本：v3.4.4

##### `azure_adls2_oauth2_use_managed_identity`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用 Managed Identity 授权对您的 Azure Data Lake Storage Gen2 的请求。
- 引入版本：v3.4.4

##### `azure_adls2_path`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Azure Data Lake Storage Gen2 路径。它由文件系统名称和目录名称组成，例如 `testfilesystem/starrocks`。
- 引入版本：v3.4.1

##### `azure_adls2_sas_token`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Data Lake Storage Gen2 请求的共享访问签名 (SAS)。
- 引入版本：v3.4.1

##### `azure_adls2_shared_key`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Data Lake Storage Gen2 请求的共享密钥。
- 引入版本：v3.4.1

##### `azure_blob_endpoint`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您的 Azure Blob Storage 账号的端点，例如 `https://test.blob.core.windows.net`。
- 引入版本：v3.1

##### `azure_blob_path`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Azure Blob Storage 路径。它由您的存储账号中的容器名称及其下的子路径（如果有）组成，例如 `testcontainer/subpath`。
- 引入版本：v3.1

##### `azure_blob_sas_token`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Blob Storage 请求的共享访问签名 (SAS)。
- 引入版本：v3.1

##### `azure_blob_shared_key`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于授权对您的 Azure Blob Storage 请求的共享密钥。
- 引入版本：v3.1

##### `azure_use_native_sdk`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否使用原生 SDK 访问 Azure Blob Storage，从而允许使用 Managed Identities 和 Service Principals 进行身份验证。如果此项设置为 `false`，则只允许使用 Shared Key 和 SAS Token 进行身份验证。
- 引入版本：v3.4.4

##### `cloud_native_hdfs_url`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本：-

##### `cloud_native_meta_port`

- 默认值：6090
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：FE 云原生元数据服务器 RPC 监听端口。
- 引入版本：-

##### `cloud_native_storage_type`

- 默认值：S3
- 类型：String
- 单位：-
- 是否可变：否
- 描述：您使用的对象存储类型。在共享数据模式下，StarRocks 支持将数据存储在 HDFS、Azure Blob（v3.1.1 及更高版本支持）、Azure Data Lake Storage Gen2（v3.4.1 及更高版本支持）、Google Storage（使用原生 SDK，v3.5.1 及更高版本支持）以及与 S3 协议兼容的对象存储系统（如 AWS S3 和 MinIO）中。有效值：`S3`（默认）、`HDFS`、`AZBLOB`、`ADLS2` 和 `GS`。如果您将此参数指定为 `S3`，则必须添加以 `aws_s3` 为前缀的参数。如果您将此参数指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的参数。如果您将此参数指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的参数。如果您将此参数指定为 `GS`，则必须添加以 `gcp_gcs` 为前缀的参数。如果您将此参数指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本：-

##### `enable_load_volume_from_conf`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否允许 StarRocks 使用 FE 配置文件中指定的对象存储相关属性创建内置存储卷。从 v3.4.1 起，默认值从 `true` 更改为 `false`。
- 引入版本：v3.1.0

##### `gcp_gcs_impersonation_service_account`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：如果您使用基于模拟的身份验证访问 Google Storage，则要模拟的 Service Account。
- 引入版本：v3.5.1

##### `gcp_gcs_path`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：用于存储数据的 Google Cloud 路径。它由您的 Google Cloud 存储桶名称及其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本：v3.5.1

##### `gcp_gcs_service_account_email`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建 Service Account 时生成的 JSON 文件中的电子邮件地址，例如 `user@hello.iam.gserviceaccount.com`。
- 引入版本：v3.5.1

##### `gcp_gcs_service_account_private_key`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建 Service Account 时生成的 JSON 文件中的私钥，例如 `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 引入版本：v3.5.1

##### `gcp_gcs_service_account_private_key_id`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：在创建 Service Account 时生成的 JSON 文件中的私钥 ID。
- 引入版本：v3.5.1

##### `gcp_gcs_use_compute_engine_service_account`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否使用绑定到您的 Compute Engine 的 Service Account。
- 引入版本：v3.5.1

##### `hdfs_file_system_expire_seconds`

- 默认值：300
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：由 HdfsFsManager 管理的未使用的缓存 HDFS/ObjectStore FileSystem 的存活时间（秒）。FileSystemExpirationChecker（每 60 秒运行一次）使用此值调用每个 HdfsFs.isExpired(...)；当过期时，管理器会关闭底层 FileSystem 并将其从缓存中移除。访问器方法（例如 `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`）会更新上次访问时间戳，因此过期是基于不活动时间。较低的值会减少空闲资源占用，但会增加重新打开的开销；较高的值会使句柄保持更长时间，并可能消耗更多资源。
- 引入版本：v3.2.0

##### `lake_autovacuum_grace_period_minutes`

- 默认值：30
- 类型：Long
- 单位：分钟
- 是否可变：是
- 描述：共享数据集群中保留历史数据版本的时间范围。在此时间范围内的历史数据版本在 Compactions 后不会通过 AutoVacuum 自动清理。您需要将此值设置得大于最大查询时间，以避免正在运行的查询访问的数据在查询完成之前被删除。从 v3.3.0、v3.2.5 和 v3.1.10 起，默认值已从 `5` 更改为 `30`。
- 引入版本：v3.1.0

##### `lake_autovacuum_parallel_partitions`

- 默认值：8
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：共享数据集群中可以同时进行 AutoVacuum 的最大分区数。AutoVacuum 是 Compactions 后的垃圾回收。
- 引入版本：v3.1.0

##### `lake_autovacuum_partition_naptime_seconds`

- 默认值：180
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：共享数据集群中同一分区上 AutoVacuum 操作之间的最小间隔。
- 引入版本：v3.1.0

##### `lake_autovacuum_stale_partition_threshold`

- 默认值：12
- 类型：Long
- 单位：小时
- 是否可变：是
- 描述：如果分区在此时间范围内没有更新（加载、DELETE 或 Compactions），系统将不会对此分区执行 AutoVacuum。
- 引入版本：v3.1.0

##### `lake_compaction_allow_partial_success`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：如果此项设置为 `true`，当共享数据集群中的 Compaction 操作的其中一个子任务成功时，系统将认为该 Compaction 操作成功。
- 引入版本：v3.5.2

##### `lake_compaction_disable_ids`

- 默认值：""
- 类型：String
- 单位：-
- 是否可变：是
- 描述：在共享数据模式下禁用 Compaction 的表或分区列表。格式为 `tableId1;partitionId2`，以分号分隔，例如 `12345;98765`。
- 引入版本：v3.4.4

##### `lake_compaction_history_size`

- 默认值：20
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：共享数据集群中 Leader FE 节点内存中保留的最近成功 Compaction 任务记录的数量。您可以使用 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录存储在 FE 进程内存中，如果 FE 进程重启，它将丢失。
- 引入版本：v3.1.0

##### `lake_compaction_max_parallel_default`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 当建表时未指定 `lake_compaction_max_parallel` 表属性时，每个 tablet 的默认最大并行 Compaction 子任务数。`0` 表示禁用并行 Compaction。此配置作为表属性 `lake_compaction_max_parallel` 的默认值。

##### `lake_compaction_max_tasks`

- 默认值：-1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：共享数据集群中允许的最大并发 Compaction 任务数。将此项设置为 `-1` 表示以自适应方式计算并发任务数。将此值设置为 `0` 将禁用 Compaction。
- 引入版本：v3.1.0

##### `lake_compaction_score_selector_min_score`

- 默认值：10.0
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：触发共享数据集群中 Compaction 操作的 Compaction Score 阈值。当分区的 Compaction Score 大于或等于此值时，系统将对该分区执行 Compaction。
- 引入版本：v3.1.0

##### `lake_compaction_score_upper_bound`

- 默认值：2000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：共享数据集群中分区的 Compaction Score 上限。`0` 表示没有上限。此项仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。当分区的 Compaction Score 达到或超过此上限时，传入的加载任务将被拒绝。从 v3.3.6 起，默认值从 `0` 更改为 `2000`。
- 引入版本：v3.2.0


##### `lake_compaction_interval_ms_on_success`

- 默认值：10000
- 类型：Long
- 单位：毫秒
- 是否动态：是
- 描述：存算分离集群某个分区 Compaction 成功后，间隔多少时间，对这个分区发起下一次 Compaction。别名为 `lake_min_compaction_interval_ms_on_success`。
- 引入版本：v3.2.0

##### `lake_enable_balance_tablets_between_workers`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在共享数据集群中云原生表的 Tablet 迁移期间，是否在 Compute Node 之间平衡 Tablet 数量。`true` 表示在 Compute Node 之间平衡 Tablet，`false` 表示禁用此功能。
- 引入版本：v3.3.4

##### `lake_enable_ingest_slowdown`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在共享数据集群中启用数据摄取减速 (Data Ingestion Slowdown)。当启用数据摄取减速时，如果分区的 Compaction Score 超过 `lake_ingest_slowdown_threshold`，则该分区上的加载任务将被限流。此配置仅在 `run_mode` 设置为 `shared_data` 时生效。从 v3.3.6 起，默认值从 `false` 更改为 `true`。
- 引入版本：v3.2.0

##### `lake_ingest_slowdown_threshold`

- 默认值：100
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：触发共享数据集群中数据摄取减速的 Compaction Score 阈值。此配置仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。
- 引入版本：v3.2.0

##### `lake_publish_version_max_threads`

- 默认值：512
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：共享数据集群中版本发布任务的最大线程数。
- 引入版本：v3.2.0

##### `meta_sync_force_delete_shard_meta`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否允许直接删除共享数据集群的元数据，绕过清理远程存储文件。建议仅在需要清理的 shard 数量过多，导致 FE JVM 内存压力过大时，才将此项设置为 `true`。请注意，启用此功能后，属于 shard 或 tablet 的数据文件无法自动清理。
- 引入版本：v3.2.10, v3.3.3

##### `run_mode`

- 默认值：`shared_nothing`
- 类型：String
- 单位：-
- 是否可变：否
- 描述：StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing`（默认）。
  - `shared_data` 表示以共享数据模式运行 StarRocks。
  - `shared_nothing` 表示以共享无数据模式运行 StarRocks。

  > **注意**
  >
  > - StarRocks 集群不能同时采用 `shared_data` 和 `shared_nothing` 模式。不支持混合部署。
  > - 集群部署后，请勿更改 `run_mode`。否则，集群将无法重启。不支持从共享无数据集群转换为共享数据集群，反之亦然。

- 引入版本：-

##### `shard_group_clean_threshold_sec`

- 默认值：3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：FE 清理共享数据集群中未使用的 Tablet 和 shard 组之前的时间。在此阈值内创建的 Tablet 和 shard 组将不会被清理。
- 引入版本：-

##### `star_mgr_meta_sync_interval_sec`

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：否
- 描述：FE 在共享数据集群中与 StarMgr 进行周期性元数据同步的间隔。
- 引入版本：-

##### `starmgr_grpc_server_max_worker_threads`

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE starmgr 模块中 grpc 服务器使用的最大工作线程数。
- 引入版本：v4.0.0, v3.5.8

##### `starmgr_grpc_timeout_seconds`

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：
- 引入版本：-

### 数据湖

##### `files_enable_insert_push_down_schema`

- 默认值: true
- 类型: Boolean
- 单位: -
- 可变: 是
- 描述: 启用此项后，分析器将尝试在 `INSERT ... FROM files()` 操作中，将目标表 schema 推送到 `files()` 表函数中。这仅适用于源为 `FileTableFunctionRelation`、目标为原生表，且 `SELECT` 列表中包含相应的 `slot-ref` 列（或 `*`）的情况。分析器会将 `SELECT` 列与目标列进行匹配（数量必须一致），短暂锁定目标表，并用深拷贝的目标列类型替换非复杂类型的文件列类型（`Parquet JSON -> array<varchar>` 等复杂类型会被跳过）。原始 `files` 表中的列名将保留。这减少了摄取过程中因基于文件的类型推断导致的类型不匹配和松散性。
- 引入版本: v3.4.0, v3.5.0

##### `hdfs_read_buffer_size_kb`

- 默认值: 8192
- 类型: Int
- 单位: 千字节
- 可变: 是
- 描述: HDFS 读取缓冲区的大小，单位为千字节。StarRocks 会将此值转换为字节（`<< 10`），并用它来初始化 `HdfsFsManager` 中的 HDFS 读取缓冲区，以及在不使用 broker 访问时，填充发送给 BE 任务的 Thrift 字段 `hdfs_read_buffer_size_kb`（例如 `TBrokerScanRangeParams`、`TDownloadReq`）。增加 `hdfs_read_buffer_size_kb` 可以提高顺序读取吞吐量并减少系统调用开销，但会增加每个流的内存使用量；减小此值会减少内存占用，但可能会降低 IO 效率。在调优时请考虑工作负载（许多小流与少量大顺序读取）。
- 引入版本: v3.2.0

##### `hdfs_write_buffer_size_kb`

- 默认值: 1024
- 类型: Int
- 单位: 千字节
- 可变: 是
- 描述: 设置 HDFS 写入缓冲区的大小（单位为 KB），用于在不使用 broker 时直接写入 HDFS 或对象存储。FE 会将此值转换为字节（`<< 10`）并初始化 `HdfsFsManager` 中的本地写入缓冲区，并通过 Thrift 请求（例如 `TUploadReq`、`TExportSink`、`sink options`）进行传播，以便后端/代理使用相同的缓冲区大小。增加此值可以提高大型顺序写入的吞吐量，但会增加每个写入器的内存消耗；减小此值会减少每个流的内存使用量，并可能降低小型写入的延迟。请与 `hdfs_read_buffer_size_kb` 一起调优，并考虑可用内存和并发写入器。
- 引入版本: v3.2.0

##### `lake_enable_drop_tablet_cache`

- 默认值：`true`
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：shared-data 模式下，在实际数据被删除前，清理 BE/CN 上的缓存。
- 引入版本：v4.0

##### `lake_batch_publish_max_version_num`

- 默认值: 10
- 类型: Int
- 单位: 数量
- 可变: 是
- 描述: 设置为 Lake（云原生）表构建发布批次时，可以分组的连续事务版本的上限。此值会传递给事务图批处理例程（参见 `getReadyToPublishTxnListBatch`），并与 `lake_batch_publish_min_version_num` 协同工作，以确定 `TransactionStateBatch` 的候选范围大小。较大的值可以通过批处理更多提交来提高发布吞吐量，但会增加原子发布的范围（更长的可见性延迟和更大的回滚面），并且在版本不连续时可能会在运行时受到限制。请根据工作负载和可见性/延迟要求进行调优。
- 引入版本: v3.2.0

##### `lake_batch_publish_min_version_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 可变: 是
- 描述: 设置为 Lake 表形成发布批次所需的最小连续事务版本数。`DatabaseTransactionMgr.getReadyToPublishTxnListBatch` 会将此值与 `lake_batch_publish_max_version_num` 一起传递给 `transactionGraph.getTxnsWithTxnDependencyBatch` 以选择依赖事务。值为 `1` 允许单个事务发布（不进行批处理）。值 `>1` 要求至少有相同数量的连续版本、单表、非复制事务可用；如果版本不连续、出现复制事务或 schema 变更消耗了一个版本，则批处理将被中止。增加此值可以通过分组提交来提高发布吞吐量，但可能会因等待足够的连续事务而延迟发布。
- 引入版本: v3.2.0

##### `lake_enable_batch_publish_version`

- 默认值: true
- 类型: Boolean
- 单位: -
- 可变: 是
- 描述: 启用此项后，`PublishVersionDaemon` 会将同一 Lake（共享数据）表/分区的就绪事务进行批处理，并将其版本一起发布，而不是逐个事务发布。在 `RunMode shared-data` 模式下，守护进程会调用 `getReadyPublishTransactionsBatch()` 并使用 `publishVersionForLakeTableBatch(...)` 执行分组发布操作（减少 RPC 并提高吞吐量）。禁用时，守护进程会通过 `publishVersionForLakeTable(...)` 回退到逐个事务发布。该实现通过内部集合协调正在进行的工作，以避免在开关切换时重复发布，并受 `lake_publish_version_max_threads` 配置的线程池大小影响。
- 引入版本: v3.2.0

##### `lake_enable_tablet_creation_optimization`

- 默认值: false
- 类型: boolean
- 单位: -
- 可变: 是
- 描述: 启用此项后，StarRocks 会在共享数据模式下优化云原生表和物化视图的 tablet 创建，通过为物理分区下的所有 tablet 创建一个共享的 tablet 元数据，而不是为每个 tablet 创建独立的元数据。这减少了表创建、rollup 和 schema 变更作业期间生成的 tablet 创建任务和元数据/文件数量。此优化仅适用于云原生表/物化视图，并与 `file_bundling` 结合使用（后者重用相同的优化逻辑）。注意：schema 变更和 rollup 作业会明确禁用使用 `file_bundling` 的表的此优化，以避免覆盖同名文件。请谨慎启用——它会改变创建的 tablet 元数据的粒度，并可能影响副本创建和文件命名行为。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

##### `lake_use_combined_txn_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 可变: 是
- 描述: 当此项设置为 `true` 时，系统允许 Lake 表对相关事务使用组合事务日志路径。仅适用于共享数据集群。
- 引入版本: v3.3.7, v3.4.0, v3.5.0

##### `enable_iceberg_commit_queue`

- 默认值: true
- 类型: Boolean
- 单位: -
- 可变: 是
- 描述: 是否为 Iceberg 表启用提交队列以避免并发提交冲突。Iceberg 使用乐观并发控制（OCC）进行元数据提交。当多个线程并发提交到同一个表时，可能会发生冲突，并出现类似“Cannot commit: Base metadata location is not same as the current table metadata location”的错误。启用此项后，每个 Iceberg 表都有自己的单线程执行器用于提交操作，确保对同一表的提交是序列化的，从而防止 OCC 冲突。不同的表可以并发提交，从而保持整体吞吐量。这是一项系统级优化，旨在提高可靠性，应默认启用。如果禁用，并发提交可能会因乐观锁冲突而失败。
- 引入版本: v4.1.0

##### `iceberg_commit_queue_timeout_seconds`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 可变: 是
- 描述: 等待 Iceberg 提交操作完成的超时时间，单位为秒。当使用提交队列（`enable_iceberg_commit_queue=true`）时，每个提交操作必须在此超时时间内完成。如果提交时间超过此超时，它将被取消并引发错误。影响提交时间的因素包括：提交的数据文件数量、表的元数据大小、底层存储（例如 S3、HDFS）的性能。
- 引入版本: v4.1.0

##### `iceberg_commit_queue_max_size`

- 默认值: 1000
- 类型: Int
- 单位: 数量
- 可变: 否
- 描述: 每个 Iceberg 表的最大待处理提交操作数。当使用提交队列（`enable_iceberg_commit_queue=true`）时，此项限制了单个表可以排队的提交操作数量。当达到限制时，额外的提交操作将在调用者线程中执行（阻塞直到有可用容量）。此配置在 FE 启动时读取，并适用于新创建的表执行器。需要重启 FE 才能生效。如果您预计对同一表有许多并发提交，请增加此值。如果此值过低，在高并发期间提交可能会在调用者线程中阻塞。
- 引入版本: v4.1.0

### 其他

##### `agent_task_resend_wait_time_ms`

- Default: 5000
- Type: Long
- Unit: 毫秒
- Is mutable: 是
- Description: FE 在重新发送 Agent 任务之前必须等待的时长。只有当任务创建时间与当前时间之间的间隔超过此参数的值时，才能重新发送 Agent 任务。此参数用于防止重复发送 Agent 任务。
- Introduced in: -

##### `allow_system_reserved_names`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否允许用户创建以 `__op` 和 `__row` 开头的列。要启用此功能，请将此参数设置为 `TRUE`。请注意，这些名称格式在 StarRocks 中保留用于特殊目的，创建此类列可能会导致未定义的行为。因此，此功能默认禁用。
- Introduced in: v3.2.0

##### `auth_token`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于 FE 所属 StarRocks 集群内身份验证的令牌。如果未指定此参数，StarRocks 会在集群的 Leader FE 首次启动时为集群生成一个随机令牌。
- Introduced in: -

##### `authentication_ldap_simple_bind_base_dn`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 是
- Description: 基本 DN，LDAP 服务器从该点开始搜索用户的认证信息。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_dn`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 是
- Description: 用于搜索用户认证信息的管理员 DN。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_pwd`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 是
- Description: 用于搜索用户认证信息的管理员密码。
- Introduced in: -

##### `authentication_ldap_simple_server_host`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 是
- Description: LDAP 服务器运行的主机。
- Introduced in: -

##### `authentication_ldap_simple_server_port`

- Default: 389
- Type: Int
- Unit: -
- Is mutable: 是
- Description: LDAP 服务器的端口。
- Introduced in: -

##### `authentication_ldap_simple_user_search_attr`

- Default: uid
- Type: String
- Unit: -
- Is mutable: 是
- Description: 在 LDAP 对象中标识用户的属性名称。
- Introduced in: -

##### `backup_job_default_timeout_ms`

- Default: 86400 * 1000
- Type: Int
- Unit: 毫秒
- Is mutable: 是
- Description: 备份作业的超时时长。如果超过此值，备份作业将失败。
- Introduced in: -

##### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否在 `SHOW PROC /BACKENDS/{id}` 命令中启用收集每个磁盘的 tablet 数量。
- Introduced in: v4.0.1, v3.5.8

##### `enable_colocate_restore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否为 Colocate 表启用备份和恢复。`true` 表示启用 Colocate 表的备份和恢复，`false` 表示禁用。
- Introduced in: v3.2.10, v3.3.3

##### `enable_materialized_view_concurrent_prepare`

- Default: true
- Type: Boolean
- Unit:
- Is mutable: 是
- Description: 是否并发准备物化视图以提高性能。
- Introduced in: v3.4.4

##### `enable_metric_calculator`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 否
- Description: 指定是否启用用于定期收集指标的功能。有效值：`TRUE` 和 `FALSE`。`TRUE` 表示启用此功能，`FALSE` 表示禁用此功能。
- Introduced in: -

##### `enable_table_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否在 FE 中导出表级别指标。禁用时，FE 将跳过导出表指标（例如表扫描/加载计数器和表大小指标），但仍会在内存中记录计数器。
- Introduced in: -

##### `enable_mv_post_image_reload_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: FE 加载镜像后是否执行重载标志检查。如果对基础物化视图执行了检查，则与其相关的其他物化视图无需再次检查。
- Introduced in: v3.5.0

##### `enable_mv_query_context_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否启用查询级别的物化视图重写缓存以提高查询重写性能。
- Introduced in: v3.3

##### `enable_mv_refresh_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否默认对所有物化视图在刷新时启用 profile。
- Introduced in: v3.3.0

##### `enable_mv_refresh_extra_prefix_logging`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否在日志中启用物化视图名称前缀以便更好地调试。
- Introduced in: v3.4.0

##### `enable_mv_refresh_query_rewrite`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否在物化视图刷新期间启用查询重写，以便查询可以直接使用重写的物化视图而不是基表来提高查询性能。
- Introduced in: v3.3

##### `enable_trace_historical_node`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否允许系统跟踪历史节点。通过将此项设置为 `true`，您可以启用缓存共享功能，并允许系统在弹性伸缩期间选择正确的缓存节点。
- Introduced in: v3.5.1

##### `es_state_sync_interval_second`

- Default: 10
- Type: Long
- Unit: 秒
- Is mutable: 否
- Description: FE 获取 Elasticsearch 索引并同步 StarRocks 外部表元数据的时间间隔。
- Introduced in: -

##### `hive_meta_cache_refresh_interval_s`

- Default: 3600 * 2
- Type: Long
- Unit: 秒
- Is mutable: 否
- Description: Hive 外部表的缓存元数据更新的时间间隔。
- Introduced in: -

##### `hive_meta_store_timeout_s`

- Default: 10
- Type: Long
- Unit: 秒
- Is mutable: 否
- Description: 连接到 Hive metastore 超时的时间。
- Introduced in: -

##### `jdbc_connection_idle_timeout_ms`

- Default: 600000
- Type: Int
- Unit: 毫秒
- Is mutable: 否
- Description: 访问 JDBC Catalog 的连接超时后的最大时长。超时连接被视为空闲连接。
- Introduced in: -

##### `jdbc_connection_timeout_ms`

- Default: 10000
- Type: Long
- Unit: 毫秒
- Is mutable: 否
- Description: HikariCP 连接池获取连接的超时时间（毫秒）。如果在此时间内无法从连接池获取连接，操作将失败。
- Introduced in: v3.5.13

##### `jdbc_query_timeout_ms`

- Default: 30000
- Type: Long
- Unit: 毫秒
- Is mutable: 是
- Description: JDBC 语句查询执行的超时时间（毫秒）。此超时适用于通过 JDBC Catalog 执行的所有 SQL 查询（例如，分区元数据查询）。该值在传递给 JDBC 驱动程序时会转换为秒。
- Introduced in: v3.5.13

##### `jdbc_network_timeout_ms`

- Default: 30000
- Type: Long
- Unit: 毫秒
- Is mutable: 是
- Description: JDBC 网络操作（socket 读取）的超时时间（毫秒）。此超时适用于数据库元数据调用（例如，getSchemas()、getTables()、getColumns()），以防止外部数据库无响应时无限期阻塞。
- Introduced in: v3.5.13

##### `jdbc_connection_pool_size`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: 否
- Description: 访问 JDBC Catalog 的 JDBC 连接池的最大容量。
- Introduced in: -

##### `jdbc_meta_default_cache_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: JDBC Catalog 元数据缓存是否启用的默认值。当设置为 True 时，新创建的 JDBC Catalog 将默认启用元数据缓存。
- Introduced in: -

##### `jdbc_meta_default_cache_expire_sec`

- Default: 600
- Type: Long
- Unit: 秒
- Is mutable: 是
- Description: JDBC Catalog 元数据缓存的默认过期时间。当 `jdbc_meta_default_cache_enable` 设置为 true 时，新创建的 JDBC Catalog 将默认设置元数据缓存的过期时间。
- Introduced in: -

##### `jdbc_minimum_idle_connections`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: 否
- Description: 访问 JDBC Catalog 的 JDBC 连接池中的最小空闲连接数。
- Introduced in: -

##### `jwt_jwks_url`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下公钥本地文件的路径。
- Introduced in: v3.5.0

##### `jwt_principal_field`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中表示主题 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- Introduced in: v3.5.0

##### `jwt_required_audience`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中受众 (`aud`) 的字符串列表。只有当列表中的一个值与 JWT 受众匹配时，JWT 才被视为有效。
- Introduced in: v3.5.0

##### `jwt_required_issuer`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中颁发者 (`iss`) 的字符串列表。只有当列表中的一个值与 JWT 颁发者匹配时，JWT 才被视为有效。
- Introduced in: v3.5.0

##### locale

- Default: `zh_CN.UTF-8`
- Type: String
- Unit: -
- Is mutable: 否
- Description: FE 使用的字符集。
- Introduced in: -

##### `max_agent_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: 否
- Description: Agent 任务线程池中允许的最大线程数。
- Introduced in: -

##### `max_download_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 在每次 RESTORE 操作中，StarRocks 分配给 BE 节点的最大下载任务数。当此项设置为小于或等于 0 时，对任务数量不施加限制。
- Introduced in: v3.1.0

##### `max_mv_check_base_table_change_retry_times`

- Default: 10
- Type: -
- Unit: -
- Is mutable: 是
- Description: 刷新物化视图时检测基表更改的最大重试次数。
- Introduced in: v3.3.0

##### `max_mv_refresh_failure_retry_times`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 物化视图刷新失败时的最大重试次数。
- Introduced in: v3.3.0

##### `max_mv_refresh_try_lock_failure_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 物化视图刷新失败时尝试获取锁的最大重试次数。
- Introduced in: v3.3.0

##### `max_small_file_number`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: 是
- Description: FE 目录下可以存储的小文件最大数量。
- Introduced in: -

##### `max_small_file_size_bytes`

- Default: 1024 * 1024
- Type: Int
- Unit: 字节
- Is mutable: 是
- Description: 小文件的最大大小。
- Introduced in: -

##### `max_upload_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 在每次 BACKUP 操作中，StarRocks 分配给 BE 节点的最大上传任务数。当此项设置为小于或等于 0 时，对任务数量不施加限制。
- Introduced in: v3.1.0

##### `mv_create_partition_batch_interval_ms`

- Default: 1000
- Type: Int
- Unit: 毫秒
- Is mutable: 是
- Description: 在物化视图刷新期间，如果需要批量创建多个分区，系统会将其分成每批 64 个分区。为了降低频繁创建分区导致失败的风险，在每个批次之间设置了一个默认间隔（毫秒），以控制创建频率。
- Introduced in: v3.3

##### `mv_plan_cache_max_size`

- Default: 1000
- Type: Long
- Unit:
- Is mutable: 是
- Description: 物化视图计划缓存的最大大小（用于物化视图重写）。如果有很多物化视图用于透明查询重写，您可以增加此值。
- Introduced in: v3.2

##### `mv_plan_cache_thread_pool_size`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 物化视图计划缓存的默认线程池大小（用于物化视图重写）。
- Introduced in: v3.2

##### `mv_refresh_default_planner_optimize_timeout`

- Default: 30000
- Type: -
- Unit: -
- Is mutable: 是
- Description: 刷新物化视图时优化器规划阶段的默认超时时间。
- Introduced in: v3.3.0

##### `mv_refresh_fail_on_filter_data`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 如果刷新时存在过滤数据，物化视图刷新将失败，默认为 true；否则，通过忽略过滤数据返回成功。
- Introduced in: -

##### `mv_refresh_try_lock_timeout_ms`

- Default: 30000
- Type: Int
- Unit: 毫秒
- Is mutable: 是
- Description: 物化视图刷新尝试获取其基表/物化视图的 DB 锁的默认尝试锁超时时间。
- Introduced in: v3.3.0

##### `oauth2_auth_server_url`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 授权 URL。用户的浏览器将被重定向到此 URL 以开始 OAuth 2.0 授权过程。
- Introduced in: v3.5.0

##### `oauth2_client_id`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: StarRocks 客户端的公共标识符。
- Introduced in: v3.5.0

##### `oauth2_client_secret`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于授权 StarRocks 客户端与授权服务器通信的密钥。
- Introduced in: v3.5.0

##### `oauth2_jwks_url`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
- Introduced in: v3.5.0

##### `oauth2_principal_field`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中表示主题 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- Introduced in: v3.5.0

##### `oauth2_redirect_url`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: OAuth 2.0 认证成功后，用户的浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要将其配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- Introduced in: v3.5.0

##### `oauth2_required_audience`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中受众 (`aud`) 的字符串列表。只有当列表中的一个值与 JWT 受众匹配时，JWT 才被视为有效。
- Introduced in: v3.5.0

##### `oauth2_required_issuer`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 用于标识 JWT 中颁发者 (`iss`) 的字符串列表。只有当列表中的一个值与 JWT 颁发者匹配时，JWT 才被视为有效。
- Introduced in: v3.5.0

##### `oauth2_token_server_url`

- Default: 空字符串
- Type: String
- Unit: -
- Is mutable: 否
- Description: 授权服务器上 StarRocks 获取访问令牌的端点 URL。
- Introduced in: v3.5.0

##### `plugin_dir`

- Default: `System.getenv("STARROCKS_HOME")` + "/plugins"
- Type: String
- Unit: -
- Is mutable: 否
- Description: 存储插件安装包的目录。
- Introduced in: -

##### `plugin_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否可以在 FE 上安装插件。插件只能在 Leader FE 上安装或卸载。
- Introduced in: -

##### `proc_profile_jstack_depth`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 系统收集 CPU 和内存 profile 时 Java 堆栈的最大深度。此值控制每个采样堆栈捕获的 Java 堆栈帧数量：较大的值会增加跟踪细节和输出大小，并可能增加 profile 开销，而较小的值会减少细节。此设置在启动 CPU 和内存 profile 时使用，因此请调整它以平衡诊断需求和性能影响。
- Introduced in: -

##### `proc_profile_mem_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 是否启用收集进程内存分配 profile。当此项设置为 `true` 时，系统会在 `sys_log_dir/proc_profile` 下生成一个名为 `mem-profile-<timestamp>.html` 的 HTML profile，在采样期间休眠 `proc_profile_collect_time_s` 秒，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的文件会根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 进行压缩和清除。原生提取路径使用 `STARROCKS_HOME_DIR` 以避免 `/tmp` noexec 问题。此项旨在解决内存分配热点问题。启用它会增加 CPU、I/O 和磁盘使用率，并可能生成大文件。
- Introduced in: v3.2.12

##### `query_detail_explain_level`

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: 是
- Description: EXPLAIN 语句返回的查询计划的详细级别。有效值：COSTS, NORMAL, VERBOSE。
- Introduced in: v3.2.12, v3.3.5

##### `replication_interval_ms`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: 否
- Description: 调度复制任务的最小时间间隔。
- Introduced in: v3.3.5

##### `replication_max_parallel_data_size_mb`

- Default: 1048576
- Type: Int
- Unit: MB
- Is mutable: 是
- Description: 允许并发同步的最大数据大小。
- Introduced in: v3.3.5

##### `replication_max_parallel_replica_count`

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 允许并发同步的 tablet 副本的最大数量。
- Introduced in: v3.3.5

##### `replication_max_parallel_table_count`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 允许的最大并发数据同步任务数。StarRocks 为每个表创建一个同步任务。
- Introduced in: v3.3.5

##### `replication_transaction_timeout_sec`

- Default: 86400
- Type: Int
- Unit: 秒
- Is mutable: 是
- Description: 同步任务的超时时长。
- Introduced in: v3.3.5

##### `skip_whole_phase_lock_mv_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 控制 StarRocks 何时对具有相关物化视图的表应用“无锁”优化。当此项设置为小于 0 时，系统始终应用无锁优化，并且不为查询复制相关的物化视图（FE 内存使用和元数据复制/锁争用减少，但元数据并发问题的风险可能增加）。当设置为 0 时，无锁优化被禁用（系统始终使用安全的复制和加锁路径）。当设置为大于 0 时，无锁优化仅适用于相关物化视图数量小于或等于配置阈值的表。此外，当值大于或等于 0 时，规划器会将查询 OLAP 表记录到优化器上下文中，以启用物化视图相关的重写路径；当值小于 0 时，此步骤将被跳过。
- Introduced in: v3.2.1

##### `small_file_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/small_files"
- Type: String
- Unit: -
- Is mutable: 否
- Description: 小文件的根目录。
- Introduced in: -

##### `task_runs_max_history_number`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: 是
- Description: 在内存中保留的任务运行记录的最大数量，以及在查询归档任务运行历史记录时用作默认 LIMIT 的值。当 `enable_task_history_archive` 为 false 时，此值限制内存中的历史记录：强制 GC 会修剪较旧的条目，只保留最新的 `task_runs_max_history_number`。当查询归档历史记录（且未提供显式 LIMIT）时，如果此值大于 0，`TaskRunHistoryTable.lookup` 将使用 `"ORDER BY create_time DESC LIMIT <value>"`。注意：将其设置为 0 会禁用查询侧的 LIMIT（无上限），但会导致内存中的历史记录被截断为零（除非启用了归档）。
- Introduced in: v3.2.0

##### `tmp_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/temp_dir"
- Type: String
- Unit: -
- Is mutable: 否
- Description: 存储临时文件的目录，例如在备份和恢复过程中生成的文件。这些过程完成后，生成的临时文件将被删除。
- Introduced in: -

##### `transform_type_prefer_string_for_varchar`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: 是
- Description: 在物化视图创建和 CTAS 操作中，是否优先为定长 varchar 列选择 string 类型。
- Introduced in: v4.0.0

<EditionSpecificFEItem />
