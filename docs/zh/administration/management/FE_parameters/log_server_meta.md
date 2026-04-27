---
displayed_sidebar: docs
sidebar_label: "日志、服务器和元数据"
---

# FE 配置 - 日志、服务器和元数据

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令配置或修改 FE 动态参数。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

---

当前主题包含以下类型的 FE 配置：
- [日志](#日志)
- [服务器](#服务器)
- [元数据和集群管理](#元数据和集群管理)

## 日志

### `audit_log_delete_age`

- 默认值: 30d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 审计日志文件的保留期限。默认值 `30d` 指定每个审计日志文件可以保留 30 天。StarRocks 会检查每个审计日志文件，并删除 30 天前生成的那些。
- 引入版本: -

### `audit_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储审计日志文件的目录。
- 引入版本: -

### `audit_log_enable_compress`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: No
- 描述: 当为 true 时，生成的 Log4j2 配置会将 ".gz" 后缀附加到轮转的审计日志文件名 (fe.audit.log.*) 中，以便 Log4j2 在轮转时生成压缩的 (.gz) 归档审计日志文件。此设置在 FE 启动期间在 Log4jConfig.initLogging 中读取，并应用于审计日志的 RollingFile appender；它仅影响轮转/归档文件，而不影响活动审计日志。由于该值在启动时初始化，因此更改它需要重启 FE 才能生效。与审计日志轮转设置 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) 一起使用。
- 引入版本: 3.2.12

### `audit_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: N/A
- 是否可变: Yes
- 描述: 当为 true 时，FE 审计事件将以结构化 JSON (Jackson ObjectMapper 序列化带注解的 AuditEvent 字段的 Map) 的形式发出，而不是默认的管道分隔的 "key=value" 字符串。此设置会影响 AuditLogBuilder 处理的所有内置审计接收器：连接审计、查询审计、大查询审计（当事件符合条件时，大查询阈值字段会添加到 JSON 中）和慢审计输出。用于大查询阈值的字段和 "features" 字段会进行特殊处理（从普通审计条目中排除；根据适用情况包含在大查询或功能日志中）。启用此功能可使日志可供日志收集器或 SIEM 机器解析；请注意，它会更改日志格式，可能需要更新任何期望旧版管道分隔格式的现有解析器。
- 引入版本: 3.2.7

### `audit_log_modules`

- 默认值: `slow_query`, query
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成审计日志条目的模块。默认情况下，StarRocks 为 `slow_query` 模块和 `query` 模块生成审计日志。`connection` 模块从 v3.0 版本开始支持。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

### `audit_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转审计日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在审计日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在审计日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

### `audit_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `audit_log_roll_interval` 参数指定的每个保留期内，可以保留的审计日志文件的最大数量。
- 引入版本: -

### `audit_stmt_before_execute`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 FE 是否在每条语句真正执行前额外输出一条 `BEFORE_QUERY` 审计事件。启用后，ConnectProcessor 会在语句执行前记录一条审计日志，并在语句执行完成后继续记录原有的 `AFTER_QUERY` 审计日志。对于 multi-statement 请求，该行为按每条实际执行到的 stmt 生效：失败前成功执行的语句都会各自产生一对 before/after 审计，失败语句也会产生这两条审计，而失败后的语句因为不会执行，所以不会产生任何审计。parse 失败的行为不变，仍然只为原始 SQL 文本输出现有的失败 after-audit。这是一个 FE 级别的全局开关，因此修改后会影响该 FE 上的所有会话。
- 引入版本: v4.1

### `bdbje_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 StarRocks 中 Berkeley DB Java Edition (BDB JE) 使用的日志级别。在 BDB 环境初始化 BDBEnvironment.initConfigs() 期间，此值将应用于 `com.sleepycat.je` 包的 Java 日志记录器和 BDB JE 环境文件日志记录级别 (`EnvironmentConfig.FILE_LOGGING_LEVEL`)。接受标准的 java.util.logging.Level 名称，例如 SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF。设置为 ALL 可启用所有日志消息。增加详细程度将提高日志量，并可能影响磁盘 I/O 和性能；该值在 BDB 环境初始化时读取，因此仅在环境（重新）初始化后生效。
- 引入版本: v3.2.0

### `big_query_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE 大查询日志文件 (`fe.big_query.log.*`) 在自动删除前的保留时间。该值作为 IfLastModified age 传递给 Log4j 的删除策略 — 任何最后修改时间早于此值的轮转大查询日志都将被删除。支持的后缀包括 `d`（天）、`h`（小时）、`m`（分钟）和 `s`（秒）。示例：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）和 `120s`（120 秒）。此项与 `big_query_log_roll_interval` 和 `big_query_log_roll_num` 共同决定哪些文件被保留或清除。
- 引入版本: v3.2.0

### `big_query_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 写入大查询 dump 日志 (`fe.big_query.log.*`) 的目录。Log4j 配置使用此路径为 `fe.big_query.log` 及其轮转文件创建 RollingFile appender。轮转和保留由 `big_query_log_roll_interval`（基于时间的后缀）、`log_roll_size_mb`（大小触发器）、`big_query_log_roll_num`（最大文件数）和 `big_query_log_delete_age`（基于年龄的删除）控制。对于超过用户定义阈值（例如 `big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold` 或 `big_query_log_scan_bytes_threshold`）的查询，会记录大查询记录。使用 `big_query_log_modules` 控制哪些模块记录到此文件中。
- 引入版本: v3.2.0

### `big_query_log_modules`

- 默认值: `{"query"}`
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 启用每个模块大查询日志记录的模块名称后缀列表。典型值是逻辑组件名称。例如，默认的 `query` 会生成 `big_query.query`。
- 引入版本: v3.2.0

### `big_query_log_roll_interval`

- 默认值: `"DAY"`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 指定用于构建 `big_query` 日志 appender 滚动文件名称日期部分的时间间隔。有效值（不区分大小写）为 `DAY`（默认）和 `HOUR`。`DAY` 生成每日模式 (`"%d{yyyyMMdd}"`)，`HOUR` 生成每小时模式 (`"%d{yyyyMMddHH}"`)。该值与基于大小的轮转 (`big_query_roll_maxsize`) 和基于索引的轮转 (`big_query_log_roll_num`) 结合，形成 RollingFile filePattern。无效值会导致日志配置生成失败 (IOException)，并可能阻止日志初始化或重新配置。与 `big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num` 和 `big_query_log_delete_age` 一起使用。
- 引入版本: v3.2.0

### `big_query_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 每个 `big_query_log_roll_interval` 保留的轮转 FE 大查询日志文件的最大数量。此值绑定到 RollingFile appender 的 DefaultRolloverStrategy `max` 属性，用于 `fe.big_query.log`；当日志轮转时（按时间或按 `log_roll_size_mb`），StarRocks 最多保留 `big_query_log_roll_num` 个索引文件（filePattern 使用时间后缀加索引）。超过此数量的文件可能会被轮转删除，`big_query_log_delete_age` 还可以根据最后修改时间删除文件。
- 引入版本: v3.2.0

### `dump_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: dump 日志文件的保留期限。默认值 `7d` 指定每个 dump 日志文件可以保留 7 天。StarRocks 会检查每个 dump 日志文件，并删除 7 天前生成的那些。
- 引入版本: -

### `dump_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 dump 日志文件的目录。
- 引入版本: -

### `dump_log_modules`

- 默认值: query
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成 dump 日志条目的模块。默认情况下，StarRocks 为 query 模块生成 dump 日志。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

### `dump_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转 dump 日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在 dump 日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在 dump 日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

### `dump_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `dump_log_roll_interval` 参数指定的每个保留期内，可以保留的 dump 日志文件的最大数量。
- 引入版本: -

### `edit_log_write_slow_log_threshold_ms`

- 默认值: 2000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: JournalWriter 用于检测和记录慢速 edit-log 批量写入的阈值（单位为毫秒）。批量提交后，如果批量持续时间超过此值，JournalWriter 将发出 WARN 日志，其中包含批量大小、持续时间和当前 Journal 队列大小（以每约 2 秒一次的速率限制）。此设置仅控制 FE Leader 上潜在 IO 或复制延迟的日志记录/警报；它不改变提交或轮转行为（请参阅 `edit_log_roll_num` 和与提交相关的设置）。无论此阈值如何，指标更新仍会发生。
- 引入版本: v3.2.3

### `enable_audit_sql`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，FE 审计子系统会将 ConnectProcessor 处理的 SQL 语句文本记录到 FE 审计日志 (`fe.audit.log`) 中。存储的语句遵循其他控制：加密语句将被 redacted (`AuditEncryptionChecker`)，如果设置了 `enable_sql_desensitize_in_log`，敏感凭据可能会被 redacted 或脱敏，并且 digest 记录由 `enable_sql_digest` 控制。当设置为 `false` 时，ConnectProcessor 会在审计事件中将语句文本替换为 "?" — 其他审计字段（用户、主机、持续时间、状态、通过 `qe_slow_log_ms` 进行的慢查询检测以及指标）仍会记录。启用 SQL 审计会增加取证和故障排除的可见性，但可能会暴露敏感的 SQL 内容并增加日志量和 I/O；禁用它会提高隐私性，但代价是审计日志中会丢失完整的语句可见性。
- 引入版本: -

### `enable_profile_log`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否启用 profile 日志。启用此功能后，FE 会将每个查询的 profile 日志（由 `ProfileManager` 生成的序列化 `queryDetail` JSON）写入 profile 日志接收器。此日志记录仅在 `enable_collect_query_detail_info` 也启用时执行；当 `enable_profile_log_compress` 启用时，JSON 可能会在日志记录前进行 gzip 压缩。Profile 日志文件由 `profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` 管理，并根据 `profile_log_delete_age` 进行轮转/删除（支持 `7d`、`10h`、`60m`、`120s` 等格式）。禁用此功能会停止写入 profile 日志（减少磁盘 I/O、压缩 CPU 和存储使用）。
- 引入版本: v3.2.5

### `enable_qe_slow_log`

- 默认值: true
- 类型: Boolean
- 单位: N/A
- 是否可变: Yes
- 描述: 当启用时，FE 内置审计插件 (AuditLogBuilder) 将把其测量执行时间（"Time" 字段）超过 `qe_slow_log_ms` 配置阈值的查询事件写入慢查询审计日志 (AuditLog.getSlowAudit)。如果禁用，这些慢查询条目将被抑制（常规查询和连接审计日志不受影响）。慢审计条目遵循全局 `audit_log_json_format` 设置（JSON 与纯字符串）。使用此标志可以独立于常规审计日志记录控制慢查询审计生成量；当 `qe_slow_log_ms` 较低或工作负载产生许多长时间运行的查询时，关闭它可能会减少日志 I/O。
- 引入版本: 3.2.11

### `enable_sql_desensitize_in_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会在将敏感 SQL 内容写入日志和查询详细记录之前替换或隐藏这些内容。遵循此配置的代码路径包括 ConnectProcessor.formatStmt（审计日志）、StmtExecutor.addRunningQueryDetail（查询详细信息）和 SimpleExecutor.formatSQL（内部执行器日志）。启用此功能后，无效的 SQL 可能会被替换为固定的脱敏消息，凭据（用户/密码）将被隐藏，并且 SQL 格式化程序必须生成 sanitized 表示（它还可以启用摘要式输出）。这减少了审计/内部日志中敏感文字和凭据的泄露，但也意味着日志和查询详细信息不再包含原始完整 SQL 文本（这可能会影响回放或调试）。
- 引入版本: -

### `internal_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 指定 FE 内部日志文件（写入 `internal_log_dir`）的保留期限。该值是一个持续时间字符串。支持的后缀：`d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。示例：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。此项作为 `<IfLastModified age="..."/>` 谓词替换到 Log4j 配置中，该谓词由 RollingFile Delete 策略使用。最后修改时间早于此持续时间的文件将在日志轮转期间删除。增加此值可更快释放磁盘空间，或减少此值可更长时间保留内部物化视图或统计信息日志。
- 引入版本: v3.2.4

### `internal_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 日志记录子系统用于存储内部日志 (`fe.internal.log`) 的目录。此配置将替换到 Log4j 配置中，并决定 InternalFile appender 在何处写入内部/物化视图/统计信息日志，以及 `internal.<module>` 下的每个模块日志记录器在何处放置其文件。确保目录存在、可写并具有足够的磁盘空间。此目录中文件的日志轮转和保留由 `log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age` 和 `internal_log_roll_interval` 控制。如果 `sys_log_to_console` 启用，内部日志可能会写入控制台而不是此目录。
- 引入版本: v3.2.4

### `internal_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，内部统计/审计条目将作为紧凑的 JSON 对象写入统计审计日志记录器。JSON 包含键 "executeType" (Internal类型: QUERY 或 DML)、"queryId"、"sql" 和 "time"（已用毫秒）。当设置为 `false` 时，相同的信息将记录为单个格式化文本行（"statistic execute: ... | QueryId: [...] | SQL: ..."）。启用 JSON 可改进机器解析并与日志处理器集成，但也会导致原始 SQL 文本包含在日志中，这可能会暴露敏感信息并增加日志大小。
- 引入版本: -

### `internal_log_modules`

- 默认值: `{"base", "statistic"}`
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 将接收专用内部日志记录的模块标识符列表。对于每个条目 X，Log4j 将创建一个名为 `internal.<X>` 的日志记录器，其级别为 INFO，并且 additivity="false"。这些日志记录器被路由到内部 appender（写入 `fe.internal.log`），或者在 `sys_log_to_console` 启用时路由到控制台。根据需要使用短名称或包片段 — 确切的日志记录器名称变为 `internal.` + 配置的字符串。内部日志文件轮转和保留遵循 `internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval` 和 `log_roll_size_mb`。添加模块会导致其运行时消息分离到内部日志记录器流中，以便于调试和审计。
- 引入版本: v3.2.4

### `internal_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE 内部日志 appender 的基于时间的滚动间隔。接受的值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成每小时的文件模式 (`"%d{yyyyMMddHH}"`)，`DAY` 生成每日文件模式 (`"%d{yyyyMMdd}"`)，这些模式由 RollingFile TimeBasedTriggeringPolicy 用于命名轮转的 `fe.internal.log` 文件。无效值会导致初始化失败（构建活动 Log4j 配置时抛出 IOException）。滚动行为还取决于相关设置，例如 `internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num` 和 `internal_log_delete_age`。
- 引入版本: v3.2.4

### `internal_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 为内部 appender (`fe.internal.log`) 保留的轮转 FE 内部日志文件的最大数量。此值用作 Log4j DefaultRolloverStrategy `max` 属性；当发生轮转时，StarRocks 最多保留 `internal_log_roll_num` 个归档文件并删除旧文件（也受 `internal_log_delete_age` 控制）。较低的值会减少磁盘使用，但会缩短日志历史记录；较高的值会保留更多的历史内部日志。此项与 `internal_log_dir`、`internal_log_roll_interval` 和 `internal_roll_maxsize` 协同工作。
- 引入版本: v3.2.4

### `log_cleaner_audit_log_min_retention_days`

- 默认值: 3
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 审计日志文件的最小保留天数。早于此时间的审计日志文件即使磁盘使用率很高也不会被删除。这确保了审计日志为合规性和故障排除目的而保留。
- 引入版本: -

### `log_cleaner_check_interval_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 检查磁盘使用情况和清理日志的间隔（秒）。清理器会定期检查每个日志目录的磁盘使用情况，并在必要时触发清理。默认值为 300 秒（5 分钟）。
- 引入版本: -

### `log_cleaner_disk_usage_target`

- 默认值: 60
- 类型: Int
- 单位: 百分比
- 是否可变: Yes
- 描述: 日志清理后的目标磁盘使用率（百分比）。日志清理将持续进行，直到磁盘使用率降至此阈值以下。清理器会逐个删除最旧的日志文件，直到达到目标。
- 引入版本: -

### `log_cleaner_disk_usage_threshold`

- 默认值: 80
- 类型: Int
- 单位: 百分比
- 是否可变: Yes
- 描述: 触发日志清理的磁盘使用率阈值（百分比）。当磁盘使用率超过此阈值时，日志清理将开始。清理器会独立检查每个配置的日志目录，并处理超过此阈值的目录。
- 引入版本: -

### `log_cleaner_disk_util_based_enable`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用基于磁盘使用率的自动日志清理。启用后，当磁盘使用率超过阈值时，将清理日志。日志清理器作为 FE 节点上的后台守护程序运行，有助于防止日志文件累积导致磁盘空间耗尽。
- 引入版本: -

### `log_plan_cancelled_by_crash_be`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当查询因 BE 崩溃或 RPC 异常而取消时，是否启用查询执行计划日志记录。启用此功能后，当查询因 BE 崩溃或 `RpcException` 而取消时，StarRocks 会将查询执行计划（`TExplainLevel.COSTS` 级别）记录为 WARN 条目。日志条目包括 QueryId、SQL 和 COSTS 计划；在 ExecuteExceptionHandler 路径中，还会记录异常堆栈跟踪。当 `enable_collect_query_detail_info` 启用时，日志记录会被跳过（计划随后存储在查询详细信息中）—— 在代码路径中，通过验证查询详细信息是否为 null 来执行检查。请注意，在 ExecuteExceptionHandler 中，计划仅在第一次重试时 (`retryTime == 0`) 记录。启用此功能可能会增加日志量，因为完整的 COSTS 计划可能很大。
- 引入版本: v3.2.0

### `log_register_and_unregister_query_id`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许 FE 记录来自 QeProcessorImpl 的查询注册和注销消息（例如，`"register query id = {}"` 和 `"deregister query id = {}"`）。仅当查询具有非空 ConnectContext 且命令不是 `COM_STMT_EXECUTE` 或会话变量 `isAuditExecuteStmt()` 为 true 时才发出日志。由于这些消息是为每个查询生命周期事件写入的，因此启用此功能可能会产生大量日志并成为高并发环境中的吞吐量瓶颈。启用它用于调试或审计；禁用它以减少日志记录开销并提高性能。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: No
- 描述: 系统日志文件或审计日志文件的最大大小。
- 引入版本: -

### `proc_profile_file_retained_days`

- 默认值: 1
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 保留 `sys_log_dir/proc_profile` 下生成的进程分析文件（CPU 和内存）的天数。ProcProfileCollector 通过将 `proc_profile_file_retained_days` 天数从当前时间（格式为 yyyyMMdd-HHmmss）中减去来计算截止时间，并删除时间戳部分在字典序上早于该截止时间的分析文件（即 `timePart.compareTo(timeToDelete) < 0`）。文件删除还遵循由 `proc_profile_file_retained_size_bytes` 控制的基于大小的截止时间。分析文件使用 `cpu-profile-` 和 `mem-profile-` 前缀，并在收集后进行压缩。
- 引入版本: v3.2.12

### `proc_profile_file_retained_size_bytes`

- 默认值: 2L * 1024 * 1024 * 1024 (2147483648)
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 在分析目录下保留的收集到的 CPU 和内存分析文件（文件名前缀为 `cpu-profile-` 和 `mem-profile-`）的最大总字节数。当有效分析文件的总和超过 `proc_profile_file_retained_size_bytes` 时，收集器将删除最旧的分析文件，直到剩余总大小小于或等于 `proc_profile_file_retained_size_bytes`。早于 `proc_profile_file_retained_days` 的文件也将被删除，无论大小如何。此设置控制分析归档的磁盘使用情况，并与 `proc_profile_file_retained_days` 交互以确定删除顺序和保留。
- 引入版本: v3.2.12

### `profile_log_delete_age`

- 默认值: 1d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制 FE profile 日志文件在符合删除条件之前保留多长时间。该值注入到 Log4j 的 `<IfLastModified age="..."/>` 策略（通过 `Log4jConfig`）中，并与轮转设置（如 `profile_log_roll_interval` 和 `profile_log_roll_num`）一起应用。支持的后缀：`d`（天）、`h`（小时）、`m`（分钟）、`s`（秒）。例如：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）、`120s`（120 秒）。
- 引入版本: v3.2.5

### `profile_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE profile 日志的写入目录。Log4jConfig 使用此值放置与 profile 相关的 appender（在此目录下创建 `fe.profile.log` 和 `fe.features.log` 等文件）。这些文件的轮转和保留由 `profile_log_roll_size_mb`、`profile_log_roll_num` 和 `profile_log_delete_age` 控制；时间戳后缀格式由 `profile_log_roll_interval` 控制（支持 DAY 或 HOUR）。由于默认目录位于 `STARROCKS_HOME_DIR` 下，请确保 FE 进程对此目录具有写入和轮转/删除权限。
- 引入版本: v3.2.5

### `profile_log_latency_threshold_ms`

- 默认值：0
- 类型：Long
- 单位：毫秒
- 是否动态：是
- 描述：写入 `fe.profile.log` 的查询最小延迟（毫秒）。仅当查询执行时间大于或等于该值时才记录 profile。设为 0 表示记录所有 profile（无阈值）。设为正数可仅记录较慢的查询以降低日志量。
- 引入版本：-

### `profile_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 控制用于生成 profile 日志文件名日期部分的时间粒度。有效值（不区分大小写）为 `HOUR` 和 `DAY`。`HOUR` 生成模式 `"%d{yyyyMMddHH}"`（每小时时间桶），`DAY` 生成 `"%d{yyyyMMdd}"`（每日时间桶）。此值在 Log4j 配置中计算 `profile_file_pattern` 时使用，并且仅影响轮转文件名称中基于时间的组件；基于大小的轮转仍由 `profile_log_roll_size_mb` 控制，保留由 `profile_log_roll_num` / `profile_log_delete_age` 控制。无效值会导致日志初始化期间发生 IOException（错误消息：`"profile_log_roll_interval config error: <value>"`）。对于高容量 profiling，选择 `HOUR` 以限制每小时的文件大小，或选择 `DAY` 进行每日聚合。
- 引入版本: v3.2.5

### `profile_log_roll_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 指定 Log4j 的 DefaultRolloverStrategy 为 profile 日志记录器保留的最大轮转 profile 日志文件数。此值作为 `${profile_log_roll_num}` 注入到日志 XML 中（例如 `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`）。轮转由 `profile_log_roll_size_mb` 或 `profile_log_roll_interval` 触发；当发生轮转时，Log4j 最多保留这些索引文件，旧的索引文件将符合删除条件。磁盘上的实际保留也受 `profile_log_delete_age` 和 `profile_log_dir` 位置的影响。较低的值会减少磁盘使用，但会限制保留的历史记录；较高的值会保留更多的历史 profile 日志。
- 引入版本: v3.2.5

### `profile_log_roll_size_mb`

- 默认值: 1024
- 类型: Int
- 单位: MB
- 是否可变: No
- 描述: 设置触发 FE profile 日志文件基于大小轮转的大小阈值（以兆字节为单位）。此值由 Log4j RollingFile SizeBasedTriggeringPolicy 用于 `ProfileFile` appender；当 profile 日志超过 `profile_log_roll_size_mb` 时，它将被轮转。当达到 `profile_log_roll_interval` 时，也可以按时间进行轮转——任一条件都会触发轮转。结合 `profile_log_roll_num` 和 `profile_log_delete_age`，此项控制保留多少历史 profile 文件以及何时删除旧文件。轮转文件的压缩由 `enable_profile_log_compress` 控制。
- 引入版本: v3.2.5

### `qe_slow_log_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用于判断查询是否为慢查询的阈值。如果查询的响应时间超过此阈值，则会在 **fe.audit.log** 中记录为慢查询。
- 引入版本: -

### `slow_lock_log_every_ms`

- 默认值: 3000L
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在为同一 SlowLockLogStats 实例发出另一个“慢锁”警告之前，等待的最短间隔（以毫秒为单位）。LockUtils 在锁等待超过 `slow_lock_threshold_ms` 后检查此值，并会抑制额外的警告，直到自上次记录慢锁事件以来已过去 `slow_lock_log_every_ms` 毫秒。使用更大的值可在长时间争用期间减少日志量，或使用更小的值以获得更频繁的诊断。更改在运行时对后续检查生效。
- 引入版本: v3.2.0

### `slow_lock_print_stack`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许 LockManager 在 `logSlowLockTrace` 发出的慢锁警告的 JSON 负载中包含拥有线程的完整堆栈跟踪（"stack" 数组通过 `LogUtil.getStackTraceToJsonArray` 填充，`start=0` 且 `max=Short.MAX_VALUE`）。此配置仅控制当锁获取超过由 `slow_lock_threshold_ms` 配置的阈值时显示的锁所有者的额外堆栈信息。启用此功能通过提供持有锁的精确线程堆栈来帮助调试；禁用它可减少日志量和在高并发环境中捕获和序列化堆栈跟踪导致的 CPU/内存开销。
- 引入版本: v3.3.16, v3.4.5, v3.5.1

### `slow_lock_threshold_ms`

- 默认值: 3000L
- 类型: long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用于将锁操作或持有的锁分类为“慢”的阈值（以毫秒为单位）。当锁的等待或持有时间超过此值时，StarRocks 将（根据上下文）发出诊断日志，包括堆栈跟踪或等待/所有者信息，并在 LockManager 中在此延迟后开始死锁检测。它由 LockUtils（慢锁日志记录）、QueryableReentrantReadWriteLock（过滤慢速读取器）、LockManager（死锁检测延迟和慢锁跟踪）、LockChecker（周期性慢锁检测）和其他调用者（例如 DiskAndTabletLoadReBalancer 日志记录）使用。降低此值会增加敏感性和日志记录/诊断开销；将其设置为 0 或负值会禁用初始基于等待的死锁检测延迟行为。与 `slow_lock_log_every_ms`、`slow_lock_print_stack` 和 `slow_lock_stack_trace_reserve_levels` 一起调整。
- 引入版本: 3.2.0

### `sys_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 系统日志文件的保留期限。默认值 `7d` 指定每个系统日志文件可以保留 7 天。StarRocks 会检查每个系统日志文件，并删除 7 天前生成的那些。
- 引入版本: -

### `sys_log_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储系统日志文件的目录。
- 引入版本: -

### `sys_log_enable_compress`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会将 ".gz" 后缀附加到轮转的系统日志文件名中，以便 Log4j 会生成 gzip 压缩的轮转 FE 系统日志（例如，fe.log.*）。此值在 Log4j 配置生成期间（Log4jConfig.initLogging / generateActiveLog4jXmlConfig）读取，并控制 RollingFile filePattern 中使用的 `sys_file_postfix` 属性。启用此功能可减少保留日志的磁盘使用，但会增加轮转期间的 CPU 和 I/O，并更改日志文件名，因此读取日志的工具或脚本必须能够处理 .gz 文件。请注意，审计日志使用单独的压缩配置，即 `audit_log_enable_compress`。
- 引入版本: v3.2.12

### `sys_log_format`

- 默认值: "plaintext"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 选择 FE 日志使用的 Log4j 布局。有效值：`"plaintext"`（默认）和 `"json"`。值不区分大小写。`"plaintext"` 配置 PatternLayout，具有人类可读的时间戳、级别、线程、类.方法:行，以及 WARN/ERROR 的堆栈跟踪。`"json"` 配置 JsonTemplateLayout 并发出结构化 JSON 事件（UTC 时间戳、级别、线程 ID/名称、源文件/方法/行、消息、异常堆栈跟踪），适用于日志聚合器（ELK、Splunk）。JSON 输出遵循 `sys_log_json_max_string_length` 和 `sys_log_json_profile_max_string_length` 以获取最大字符串长度。
- 引入版本: v3.2.10

### `sys_log_json_max_string_length`

- 默认值: 1048576
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置用于 JSON 格式系统日志的 JsonTemplateLayout "maxStringLength" 值。当 `sys_log_format` 设置为 `"json"` 时，如果字符串值字段（例如 "message" 和字符串化的异常堆栈跟踪）的长度超过此限制，它们将被截断。该值注入到 `Log4jConfig.generateActiveLog4jXmlConfig()` 中生成的 Log4j XML 中，并应用于默认、警告、审计、dump 和大查询布局。profile 布局使用单独的配置 (`sys_log_json_profile_max_string_length`)。降低此值会减小日志大小，但可能会截断有用信息。
- 引入版本: 3.2.11

### `sys_log_json_profile_max_string_length`

- 默认值: 104857600 (100 MB)
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 当 `sys_log_format` 为 "json" 时，设置 profile（及相关功能）日志 appender 的 JsonTemplateLayout 的 maxStringLength。JSON 格式 profile 日志中的字符串字段值将被截断为此外字节长度；非字符串字段不受影响。此项应用于 Log4jConfig `JsonTemplateLayout maxStringLength`，并在使用 `plaintext` 日志记录时被忽略。保持足够大的值以获取您需要的完整消息，但请注意，较大的值会增加日志大小和 I/O。
- 引入版本: v3.2.11

### `sys_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 系统日志条目分类的严重性级别。有效值：`INFO`、`WARN`、`ERROR` 和 `FATAL`。
- 引入版本: -

### `sys_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 轮转系统日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则在系统日志文件名称中添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则在系统日志文件名称中添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

### `sys_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 在 `sys_log_roll_interval` 参数指定的每个保留期内，可以保留的系统日志文件的最大数量。
- 引入版本: -

### `sys_log_to_console`

- 默认值: false（除非环境变量 `SYS_LOG_TO_CONSOLE` 设置为 "1"）
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，系统会将 Log4j 配置为将所有日志发送到控制台 (ConsoleErr appender)，而不是基于文件的 appender。此值在生成活动 Log4j XML 配置时读取（这会影响根日志记录器和每个模块日志记录器 appender 的选择）。其值在进程启动时从 `SYS_LOG_TO_CONSOLE` 环境变量中捕获。在运行时更改它无效。此配置通常用于容器化或 CI 环境中，其中 stdout/stderr 日志收集优于写入日志文件。
- 引入版本: v3.2.0

### `sys_log_verbose_modules`

- 默认值: 空字符串
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: StarRocks 为其生成系统日志的模块。如果此参数设置为 `org.apache.starrocks.catalog`，则 StarRocks 仅为 catalog 模块生成系统日志。模块名称用逗号 (,) 和空格分隔。
- 引入版本: -

### `sys_log_warn_modules`

- 默认值: {}
- 类型: String[]
- 单位: -
- 是否可变: No
- 描述: 启动时系统将配置为 WARN 级别日志记录器并路由到警告 appender (SysWF) — `fe.warn.log` 文件的日志记录器名称或包前缀列表。条目插入到生成的 Log4j 配置中（与内置警告模块如 org.apache.kafka、org.apache.hudi 和 org.apache.hadoop.io.compress 一起），并生成类似 `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` 的日志记录器元素。建议使用完全限定的包和类前缀（例如 "com.example.lib"），以抑制常规日志中的嘈杂 INFO/DEBUG 输出，并允许单独捕获警告。
- 引入版本: v3.2.13

## 服务器

<EditionSpecificFEItem />

### `brpc_idle_wait_max_time`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: bRPC 客户端在空闲状态下等待的最长时间。
- 引入版本: -

### `brpc_inner_reuse_pool`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 控制底层 BRPC 客户端是否为连接/通道使用内部共享重用池。StarRocks 在 BrpcProxy 构造 RpcClientOptions 时（通过 `rpcOptions.setInnerResuePool(...)`）读取 `brpc_inner_reuse_pool`。启用时 (true)，RPC 客户端重用内部池以减少每次调用的连接创建，降低 FE 到 BE / LakeService RPC 的连接 churn、内存和文件描述符使用量。禁用时 (false)，客户端可能会创建更隔离的池（以更高的资源使用量为代价增加并发隔离）。更改此值需要重启进程才能生效。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

### `brpc_min_evictable_idle_time_ms`

- 默认值: 120000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 空闲的 BRPC 连接在连接池中必须保持空闲状态才能被驱逐的时间（毫秒）。应用于 `BrpcProxy` 使用的 RpcClientOptions（通过 RpcClientOptions.setMinEvictableIdleTime）。增加此值以保持空闲连接更长时间（减少重新连接的 churn）；降低此值可更快释放未使用的套接字（减少资源使用）。与 `brpc_connection_pool_size` 和 `brpc_idle_wait_max_time` 一起调整，以平衡连接重用、池增长和驱逐行为。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

### `brpc_reuse_addr`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，StarRocks 会设置 socket 选项，允许 brpc RpcClient 创建的客户端 socket（通过 RpcClientOptions.setReuseAddress）重用本地地址。启用此选项可减少绑定失败，并允许在套接字关闭后更快地重新绑定本地端口，这对于高速率连接 churn 或快速重启非常有用。当为 false 时，地址/端口重用被禁用，这可以降低意外端口共享的可能性，但可能会增加瞬时绑定错误。此选项与 `brpc_connection_pool_size` 和 `brpc_short_connection` 配置的连接行为交互，因为它会影响客户端套接字可以多快地重新绑定和重用。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

### `cluster_name`

- 默认值: StarRocks Cluster
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 所属的 StarRocks 集群的名称。集群名称显示在网页的 `Title` 上。
- 引入版本: -

### `dns_cache_ttl_seconds`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 成功 DNS 查找的 DNS 缓存 TTL（存活时间，Time-To-Live），单位为秒。这设置了 Java 安全属性 `networkaddress.cache.ttl`，它控制 JVM 缓存成功 DNS 查找的时间。将此项设置为 `-1` 以允许系统始终缓存信息，或设置为 `0` 以禁用缓存。这在 IP 地址经常变化的环境中特别有用，例如 Kubernetes 部署或使用动态 DNS 时。
- 引入版本: v3.5.11, v4.0.4

### `enable_http_async_handler`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许系统异步处理 HTTP 请求。如果启用此功能，Netty 工作线程收到的 HTTP 请求将提交到单独的线程池进行服务逻辑处理，以避免阻塞 HTTP 服务器。如果禁用，Netty 工作线程将处理服务逻辑。
- 引入版本: 4.0.0

### `enable_http_validate_headers`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制 Netty 的 HttpServerCodec 是否执行严格的 HTTP 头部验证。该值在 `HttpServer` 初始化 HTTP 管道时传递给 HttpServerCodec（参见 UseLocations）。默认值为 false 以保持向后兼容性，因为较新的 Netty 版本强制执行更严格的头部规则 (https://github.com/netty/netty/pull/12760)。设置为 true 以强制执行符合 RFC 的头部检查；这样做可能会导致来自旧客户端或代理的格式错误或不符合规范的请求被拒绝。更改需要重启 HTTP 服务器才能生效。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `enable_https`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否在 FE 节点中与 HTTP 服务器一起启用 HTTPS 服务器。
- 引入版本: v4.0

### `frontend_address`

- 默认值: 0.0.0.0
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 节点的 IP 地址。
- 引入版本: -

### `http_async_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 异步 HTTP 请求处理的线程池大小。别名为 `max_http_sql_service_task_threads_num`。
- 引入版本: 4.0.0

### `http_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTP 服务器持有的 backlog 队列的长度。
- 引入版本: -

### `http_max_chunk_size`

- 默认值: 8192
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置 FE HTTP 服务器中 Netty 的 HttpServerCodec 处理的单个 HTTP 块的最大允许大小（以字节为单位）。它作为第三个参数传递给 HttpServerCodec，并限制分块传输或流式请求/响应期间的块长度。如果传入块超过此值，Netty 将引发帧过大错误（例如 TooLongFrameException），并且请求可能会被拒绝。对于合法的分块上传，请增加此值；保持较小以减少内存压力并减小 DoS 攻击的表面积。此设置与 `http_max_initial_line_length`、`http_max_header_size` 和 `enable_http_validate_headers` 一起使用。
- 引入版本: v3.2.0

### `http_max_header_size`

- 默认值: 32768
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: Netty 的 `HttpServerCodec` 解析的 HTTP 请求头块的最大允许大小（以字节为单位）。StarRocks 将此值传递给 `HttpServerCodec`（作为 `Config.http_max_header_size`）；如果传入请求的头（名称和值组合）超过此限制，编解码器将拒绝该请求（解码器异常），并且连接/请求将失败。仅当客户端合法发送非常大的头（大型 cookie 或许多自定义头）时才增加此值；较大的值会增加每个连接的内存使用。与 `http_max_initial_line_length` 和 `http_max_chunk_size` 一起调整。更改需要重启 FE。
- 引入版本: v3.2.0

### `http_max_initial_line_length`

- 默认值: 4096
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 设置 HttpServer 中使用的 Netty `HttpServerCodec` 接受的 HTTP 初始请求行（方法 + 请求目标 + HTTP 版本）的最大允许长度（以字节为单位）。该值传递给 Netty 的解码器，并且初始行长于此值的请求将被拒绝 (TooLongFrameException)。仅当您必须支持非常长的请求 URI 时才增加此值；较大的值会增加内存使用，并可能增加暴露于格式错误/请求滥用的风险。与 `http_max_header_size` 和 `http_max_chunk_size` 一起调整。
- 引入版本: v3.2.0

### `http_port`

- 默认值: 8030
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTP 服务器监听的端口。
- 引入版本: -

### `http_web_page_display_hardware`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，HTTP 索引页面 (/index) 将包含一个通过 oshi 库填充的硬件信息部分（CPU、内存、进程、磁盘、文件系统、网络等）。oshi 可能会间接调用系统工具或读取系统文件（例如，它可以执行 `getent passwd` 等命令），这可能会暴露敏感的系统数据。如果您需要更严格的安全性或希望避免在主机上执行这些间接命令，请将此配置设置为 false 以禁用 Web UI 上硬件详细信息的收集和显示。
- 引入版本: v3.2.0

### `http_worker_threads_num`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: HTTP 服务器处理 HTTP 请求的工作线程数。如果为负值或 0，则线程数将是 CPU 核心数的两倍。
- 引入版本: v2.5.18, v3.0.10, v3.1.7, v3.2.2

### `https_port`

- 默认值: 8443
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 HTTPS 服务器监听的端口。
- 引入版本: v4.0

### `max_mysql_service_task_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器可运行以处理任务的最大线程数。
- 引入版本: -

### `max_task_runs_threads_num`

- 默认值: 512
- 类型: Int
- 单位: 线程
- 是否可变: No
- 描述: 控制任务运行执行器线程池中的最大线程数。此值是并发任务运行执行的上限；增加它会提高并行度，但也会增加 CPU、内存和网络使用率，而减少它可能导致任务运行积压和更高的延迟。根据预期的并发调度作业和可用的系统资源调整此值。
- 引入版本: v3.2.0

### `memory_tracker_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用 FE 内存跟踪器子系统。当 `memory_tracker_enable` 设置为 `true` 时，`MemoryUsageTracker` 定期扫描注册的元数据模块，更新内存中的 `MemoryUsageTracker.MEMORY_USAGE` map，记录总计，并使 `MetricRepo` 在指标输出中暴露内存使用和对象计数 gauge。使用 `memory_tracker_interval_seconds` 控制采样间隔。启用此功能有助于监控和调试内存消耗，但会引入 CPU 和 I/O 开销以及额外的指标基数。
- 引入版本: v3.2.4

### `memory_tracker_interval_seconds`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: FE `MemoryUsageTracker` 守护程序轮询和记录 FE 进程和已注册 `MemoryTrackable` 模块内存使用情况的间隔（秒）。当 `memory_tracker_enable` 设置为 `true` 时，跟踪器以此频率运行，更新 `MEMORY_USAGE`，并记录聚合的 JVM 和跟踪模块使用情况。
- 引入版本: v3.2.4

### `mysql_nio_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器持有的 backlog 队列的长度。
- 引入版本: -

### `mysql_server_version`

- 默认值: 8.0.33
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 返回给客户端的 MySQL 服务器版本。修改此参数将影响以下情况的版本信息：
  1. `select version();`
  2. 握手包版本
  3. 全局变量 `version` 的值 (`show variables like 'version';`)
- 引入版本: -

### `mysql_service_io_threads_num`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器可运行以处理 I/O 事件的最大线程数。
- 引入版本: -

### `mysql_service_kill_after_disconnect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制当检测到 MySQL TCP 连接关闭（读取时 EOF）时服务器如何处理会话。如果设置为 `true`，服务器会立即杀死该连接的所有正在运行的查询并立即执行清理。如果设置为 `false`，服务器在断开连接时不会杀死正在运行的查询，并且仅在没有待处理请求任务时执行清理，允许长时间运行的查询在客户端断开连接后继续。注意：尽管有一条简短的注释建议 TCP keep-alive，但此参数专门管理断开连接后的杀死行为，应根据您是希望终止孤立查询（在不可靠/负载均衡客户端后推荐）还是允许其完成进行设置。
- 引入版本: -

### `mysql_service_nio_enable_keep_alive`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 启用 MySQL 连接的 TCP Keep-Alive。对于负载均衡器后面的长时间空闲连接很有用。
- 引入版本: -

### `net_use_ipv6_when_priority_networks_empty`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 一个布尔值，用于控制在未指定 `priority_networks` 时是否优先使用 IPv6 地址。`true` 表示当托管节点的服务器同时具有 IPv4 和 IPv6 地址且未指定 `priority_networks` 时，允许系统优先使用 IPv6 地址。
- 引入版本: v3.3.0

### `priority_networks`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 声明具有多个 IP 地址的服务器的选择策略。请注意，最多一个 IP 地址必须与此参数指定的列表匹配。此参数的值是一个列表，由 CIDR 表示法中用分号 (;) 分隔的条目组成，例如 10.10.10.0/24。如果没有 IP 地址与此列表中的条目匹配，则将随机选择服务器的可用 IP 地址。从 v3.3.0 开始，StarRocks 支持基于 IPv6 的部署。如果服务器同时具有 IPv4 和 IPv6 地址，并且未指定此参数，则系统默认使用 IPv4 地址。您可以将 `net_use_ipv6_when_priority_networks_empty` 设置为 `true` 来更改此行为。
- 引入版本: -

### `proc_profile_cpu_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，后台 `ProcProfileCollector` 将使用 `AsyncProfiler` 收集 CPU profile，并将 HTML 报告写入 `sys_log_dir/proc_profile` 下。每次收集运行都会记录 `proc_profile_collect_time_s` 配置持续时间内的 CPU 堆栈，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的 profile 会被压缩，并根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 清理旧文件。`AsyncProfiler` 需要原生库 (`libasyncProfiler.so`)；`one.profiler.extractPath` 设置为 `STARROCKS_HOME_DIR/bin` 以避免 `/tmp` 上的 noexec 问题。
- 引入版本: v3.2.12

### `qe_max_connection`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 所有用户可以与 FE 节点建立的最大连接数。从 v3.1.12 和 v3.2.7 开始，默认值已从 `1024` 更改为 `4096`。
- 引入版本: -

### `query_port`

- 默认值: 9030
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 MySQL 服务器监听的端口。
- 引入版本: -

### `rpc_port`

- 默认值: 9020
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 Thrift 服务器监听的端口。
- 引入版本: -

### `slow_lock_stack_trace_reserve_levels`

- 默认值: 15
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 为慢速或持有的锁转储锁调试信息时捕获和发出的堆栈跟踪帧数。此值由 `QueryableReentrantReadWriteLock` 在生成排他锁所有者、当前线程和最旧/共享读取器的 JSON 时传递给 `LogUtil.getStackTraceToJsonArray`。增加此值可为诊断慢锁或死锁问题提供更多上下文，但代价是 JSON 负载更大，并且堆栈捕获的 CPU/内存略高；减少此值可降低开销。注意：当只记录慢锁时，读取器条目可以通过 `slow_lock_threshold_ms` 过滤。
- 引入版本: v3.4.0, v3.5.0

### `ssl_cipher_blacklist`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 以逗号分隔的列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入黑名单。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

### `ssl_cipher_whitelist`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 以逗号分隔的列表，支持正则表达式，用于通过 IANA 名称将 SSL 密码套件列入白名单。如果同时设置了白名单和黑名单，则黑名单优先。
- 引入版本: v4.0

### `task_runs_concurrency`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 并发运行的 TaskRun 实例的全局限制。当当前运行计数大于或等于 `task_runs_concurrency` 时，`TaskRunScheduler` 会停止调度新运行，因此此值限制了调度器中并行 TaskRun 执行的上限。它还被 `MVPCTRefreshPartitioner` 用于计算每个 TaskRun 分区刷新粒度。增加该值会提高并行度并增加资源使用；减少它会降低并发并使分区刷新在每次运行中更大。除非有意禁用调度，否则不要设置为 0 或负值：0（或负值）将有效地阻止 `TaskRunScheduler` 调度新的 TaskRun。
- 引入版本: v3.2.0

### `task_runs_queue_length`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 限制待处理队列中保留的待处理 TaskRun 项的最大数量。`TaskRunManager` 检查当前待处理计数，并在有效待处理 TaskRun 计数大于或等于 `task_runs_queue_length` 时拒绝新的提交。在添加合并/接受的 TaskRun 之前，会重新检查相同的限制。调整此值以平衡内存和调度积压：对于大量突发工作负载，设置为较高值以避免拒绝，或设置为较低值以限制内存并减少待处理积压。
- 引入版本: v3.2.0

### `thrift_backlog_num`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 节点中 Thrift 服务器持有的 backlog 队列的长度。
- 引入版本: -

### `thrift_client_timeout_ms`

- 默认值: 5000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 空闲客户端连接超时的时间长度。
- 引入版本: -

### `thrift_rpc_max_body_size`

- 默认值: -1
- 类型: Int
- 单位: 字节
- 是否可变: No
- 描述: 控制构建服务器 Thrift 协议时使用的 Thrift RPC 消息体最大允许大小（以字节为单位）（传递给 `ThriftServer` 中的 TBinaryProtocol.Factory）。值为 `-1` 表示禁用限制（无界）。设置正值会强制执行上限，以便大于此值的消息被 Thrift 层拒绝，这有助于限制内存使用并缓解超大请求或 DoS 风险。将其设置为足够大的值以适应预期负载（大型结构或批量数据），以避免拒绝合法请求。
- 引入版本: v3.2.0

### `thrift_server_max_worker_threads`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 节点中 Thrift 服务器支持的最大工作线程数。
- 引入版本: -

### `thrift_server_queue_size`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 请求待处理队列的长度。如果 Thrift 服务器中正在处理的线程数超过 `thrift_server_max_worker_threads` 中指定的值，则新请求将添加到待处理队列。
- 引入版本: -

## 元数据和集群管理

### `alter_max_worker_queue_size`

- 默认值: 4096
- 类型: Int
- 单位: 任务数
- 是否可变: No
- 描述: 控制 alter 子系统使用的内部工作线程池队列的容量。它与 `alter_max_worker_threads` 一起传递给 `AlterHandler` 中的 `ThreadPoolManager.newDaemonCacheThreadPool`。当待处理的 alter 任务数超过 `alter_max_worker_queue_size` 时，新的提交将被拒绝，并可能抛出 `RejectedExecutionException`（参见 `AlterHandler.handleFinishAlterTask`）。调整此值以平衡内存使用和允许并发 alter 任务的积压量。
- 引入版本: v3.2.0

### `alter_max_worker_threads`

- 默认值: 4
- 类型: Int
- 单位: 线程
- 是否可变: No
- 描述: 设置 AlterHandler 线程池中的最大工作线程数。AlterHandler 使用此值构造执行器来运行和完成与 alter 相关的任务（例如，通过 handleFinishAlterTask 提交 `AlterReplicaTask`）。此值限制了 alter 操作的并发执行；增加它会提高并行度并增加资源使用，降低它会限制并发 alters 并可能成为瓶颈。执行器与 `alter_max_worker_queue_size` 一起创建，并且处理程序调度使用 `alter_scheduler_interval_millisecond`。
- 引入版本: v3.2.0

### `automated_cluster_snapshot_interval_seconds`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 触发自动化集群快照任务的间隔。
- 引入版本: v3.4.2

### `background_refresh_metadata_interval_millis`

- 默认值: 600000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 两次 Hive 元数据缓存刷新之间的间隔。
- 引入版本: v2.5.5

### `background_refresh_metadata_time_secs_since_last_access_secs`

- 默认值: 3600 * 24
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: Hive 元数据缓存刷新任务的过期时间。对于已访问的 Hive Catalog，如果超过指定时间未访问，StarRocks 将停止刷新其缓存的元数据。对于未访问的 Hive Catalog，StarRocks 不会刷新其缓存的元数据。
- 引入版本: v2.5.5

### `bdbje_cleaner_threads`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: StarRocks journal 使用的 Berkeley DB Java Edition (JE) 环境的后台清理线程数。此值在 `BDBEnvironment.initConfigs` 中的环境初始化期间读取，并使用 `Config.bdbje_cleaner_threads` 应用于 `EnvironmentConfig.CLEANER_THREADS`。它控制 JE 日志清理和空间回收的并行度；增加它可以加快清理速度，但代价是会增加额外的 CPU 和 I/O 干扰前台操作。更改仅在 BDB 环境（重新）初始化时生效，因此需要重启前端才能应用新值。
- 引入版本: v3.2.0

### `bdbje_heartbeat_timeout_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: StarRocks 集群中 Leader、Follower 和 Observer FE 之间心跳超时的时间。
- 引入版本: -

### `bdbje_lock_timeout_second`

- 默认值: 1
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 基于 BDB JE 的 FE 中的锁超时时间。
- 引入版本: -

### `bdbje_replay_cost_percent`

- 默认值: 150
- 类型: Int
- 单位: 百分比
- 是否可变: No
- 描述: 设置从 BDB JE 日志重放事务相对于通过网络恢复相同数据的相对成本（以百分比表示）。该值提供给底层 JE 复制参数 `REPLAY_COST_PERCENT`，通常 `>100` 表示重放通常比网络恢复更昂贵。当决定是否保留清理过的日志文件以进行潜在重放时，系统会将重放成本乘以日志大小与网络恢复成本进行比较；如果判断网络恢复更有效，则将删除文件。值为 0 禁用基于此成本比较的保留。`REP_STREAM_TIMEOUT` 内的副本或任何活动复制所需的日志文件始终保留。
- 引入版本: v3.2.0

### `bdbje_replica_ack_timeout_second`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 当元数据从 Leader FE 写入 Follower FE 时，Leader FE 等待指定数量的 Follower FE 返回 ACK 消息的最长时间。单位：秒。如果正在写入大量元数据，Follower FE 需要很长时间才能向 Leader FE 返回 ACK 消息，从而导致 ACK 超时。在这种情况下，元数据写入失败，FE 进程退出。建议您增加此参数的值以防止这种情况。
- 引入版本: -

### `bdbje_reserved_disk_size`

- 默认值: 512 * 1024 * 1024 (536870912)
- 类型: Long
- 单位: 字节
- 是否可变: No
- 描述: 限制 Berkeley DB JE 将保留的“非保护”（可删除）日志/数据文件的字节数。StarRocks 通过 `BDBEnvironment` 中的 `EnvironmentConfig.RESERVED_DISK` 将此值传递给 JE；JE 的内置默认值为 0（无限制）。StarRocks 默认值（512 MiB）可防止 JE 为非保护文件保留过多的磁盘空间，同时允许安全清理过时文件。在磁盘受限的系统上调整此值：减小它可让 JE 更早释放更多文件，增加它可让 JE 保留更多保留空间。更改需要重启进程才能生效。
- 引入版本: v3.2.0

### `bdbje_reset_election_group`

- 默认值: false
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 是否重置 BDBJE 复制组。如果此参数设置为 `TRUE`，FE 将重置 BDBJE 复制组（即移除所有可选举 FE 节点的信息）并作为 Leader FE 启动。重置后，此 FE 将是集群中唯一的成员，其他 FE 可以通过使用 `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` 重新加入此集群。仅当由于大多数 Follower FE 的数据已损坏而无法选举 Leader FE 时才使用此设置。`reset_election_group` 用于替换 `metadata_failure_recovery`。
- 引入版本: -

### `black_host_connect_failures_within_time`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 黑名单 BE 节点允许的连接失败阈值。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以将其从 BE 黑名单中删除。在 `black_host_history_sec` 内，只有当黑名单 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中删除。
- 引入版本: v3.3.0

### `black_host_history_sec`

- 默认值: 2 * 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 保留 BE 节点历史连接失败的持续时间（BE 黑名单中）。如果 BE 节点自动添加到 BE 黑名单，StarRocks 将评估其连接性并判断是否可以将其从 BE 黑名单中删除。在 `black_host_history_sec` 内，只有当黑名单 BE 节点的连接失败次数少于 `black_host_connect_failures_within_time` 中设置的阈值时，才能将其从 BE 黑名单中删除。
- 引入版本: v3.3.0

### `brpc_connection_pool_size`

- 默认值: 16
- 类型: Int
- 单位: 连接数
- 是否可变: No
- 描述: FE 的 BrpcProxy 使用的每个端点的最大池化 BRPC 连接数。此值通过 `setMaxTotoal` 和 `setMaxIdleSize` 应用于 RpcClientOptions，因此它直接限制并发传出 BRPC 请求，因为每个请求必须从池中借用一个连接。在高并发场景中，增加此值以避免请求排队；增加它会增加套接字和内存使用量，并可能增加远程服务器负载。调整时，请考虑相关设置，例如 `brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr` 和 `brpc_min_evictable_idle_time_ms`。更改此值不可热重载，需要重启。
- 引入版本: v3.2.0

### `brpc_short_connection`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 控制底层 brpc RpcClient 是否使用短连接。启用时 (`true`)，设置 RpcClientOptions.setShortConnection，并且连接在请求完成后关闭，从而减少长时间连接套接字的数量，但代价是更高的连接设置开销和增加的延迟。禁用时 (`false`，默认值) 使用持久连接和连接池。启用此选项会影响连接池行为，应与 `brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr` 和 `brpc_inner_reuse_pool` 一起考虑。对于典型的高吞吐量部署，保持禁用；仅在需要限制套接字生命周期或网络策略需要短连接时才启用。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

### `catalog_try_lock_timeout_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 获取全局锁的超时时长。
- 引入版本: -

### `checkpoint_only_on_leader`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当 `true` 时，CheckpointController 将仅选择 Leader FE 作为 checkpoint worker；当 `false` 时，控制器可能会选择任何前端，并优先选择堆使用率较低的节点。当为 `false` 时，worker 按最近的失败时间和 `heapUsedPercent` 排序（Leader 被视为具有无限使用率以避免选择它）。对于需要集群快照元数据的操作，控制器无论此标志如何都已强制选择 Leader。启用 `true` 会将 checkpoint 工作集中在 Leader 上（更简单，但增加了 Leader 的 CPU/内存和网络负载）；保持 `false` 会将 checkpoint 负载分配到负载较低的 FE。此设置影响 worker 选择以及与 `checkpoint_timeout_seconds` 等超时和 `thrift_rpc_timeout_ms` 等 RPC 设置的交互。
- 引入版本: v3.4.0, v3.5.0

### `checkpoint_timeout_seconds`

- 默认值: 24 * 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: Leader 的 CheckpointController 将等待 checkpoint worker 完成 checkpoint 的最长时间（以秒为单位）。控制器将此值转换为纳秒并轮询 worker 结果队列；如果在此超时内未收到成功完成，则 checkpoint 被视为失败，并且 createImage 返回失败。增加此值可适应更长时间运行的 checkpoint，但会延迟故障检测和随后的镜像传播；减少此值会导致更快的故障转移/重试，但可能会因慢速 worker 而产生误报超时。此设置仅控制 `CheckpointController` 在 checkpoint 创建期间的等待时间，不改变 worker 的内部 checkpointing 行为。
- 引入版本: v3.4.0, v3.5.0

### `db_used_data_quota_update_interval_secs`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 数据库已用数据配额更新的间隔。StarRocks 会定期更新所有数据库的已用数据配额，以跟踪存储消耗。此值用于配额强制执行和指标收集。允许的最小间隔为 30 秒，以防止过高的系统负载。小于 30 的值将被拒绝。
- 引入版本: -

### `drop_backend_after_decommission`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: BE 下线后是否删除 BE。`TRUE` 表示 BE 下线后立即删除 BE。`FALSE` 表示 BE 下线后不删除 BE。
- 引入版本: -

### `edit_log_port`

- 默认值: 9010
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 集群中 Leader、Follower 和 Observer FE 之间通信使用的端口。
- 引入版本: -

### `edit_log_roll_num`

- 默认值: 50000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在为日志条目创建日志文件之前可以写入的元数据日志条目的最大数量。此参数用于控制日志文件的大小。新的日志文件写入 BDBJE 数据库。
- 引入版本: -

### `edit_log_type`

- 默认值: BDB
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 可以生成的 edit log 类型。将值设置为 `BDB`。
- 引入版本: -

### `enable_background_refresh_connector_metadata`

- 默认值: v3.0 及更高版本为 true，v2.5 为 false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用周期性 Hive 元数据缓存刷新。启用后，StarRocks 会轮询 Hive 集群的 metastore（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Hive Catalog 的缓存元数据，以感知数据变化。`true` 表示启用 Hive 元数据缓存刷新，`false` 表示禁用。
- 引入版本: v2.5.5

### `enable_collect_query_detail_info`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集查询的 profile。如果此参数设置为 `TRUE`，系统将收集查询的 profile。如果此参数设置为 `FALSE`，系统将不收集查询的 profile。
- 引入版本: -

### `enable_create_partial_partition_in_batch`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `false`（默认）时，StarRocks 会强制批量创建的范围分区与标准时间单位边界对齐。它将拒绝非对齐的范围以避免创建空洞。将此项设置为 `true` 会禁用该对齐检查，并允许批量创建部分（非标准）分区，这可能会产生间隙或错位的分区范围。仅当您有意需要部分批量分区并接受相关风险时才应将其设置为 `true`。
- 引入版本: v3.2.0

### `enable_internal_sql`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 当此项设置为 `true` 时，内部组件（例如 SimpleExecutor）执行的内部 SQL 语句将保留并写入内部审计或日志消息中（如果设置了 `enable_sql_desensitize_in_log`，还可以进一步脱敏）。当设置为 `false` 时，内部 SQL 文本将被抑制：格式化代码 (SimpleExecutor.formatSQL) 返回 "?"，并且实际语句不会发出到内部审计或日志消息中。此配置不改变内部语句的执行语义——它仅控制内部 SQL 的日志记录和可见性，用于隐私或安全目的。
- 引入版本: -

### `enable_legacy_compatibility_for_replication`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 Legacy 复制兼容性。StarRocks 在新旧版本之间可能表现不同，导致跨集群数据迁移时出现问题。因此，在数据迁移之前，您必须为目标集群启用 Legacy 兼容性，并在数据迁移完成后禁用它。`true` 表示启用此模式。
- 引入版本: v3.1.10, v3.2.6

### `enable_show_materialized_views_include_all_task_runs`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 SHOW MATERIALIZED VIEWS 命令如何返回 TaskRun。当此项设置为 `false` 时，StarRocks 只返回每个任务的最新 TaskRun（为兼容性而保留的旧行为）。当设置为 `true`（默认）时，`TaskManager` 可能会为同一任务包含额外的 TaskRun，但仅当它们共享相同的启动 TaskRun ID（例如，属于同一作业）时，从而防止出现不相关的重复运行，同时允许显示与一个作业相关的多个状态。将此项设置为 `false` 可恢复单次运行输出，或用于调试和监控的多运行作业历史记录。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `enable_statistics_collect_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为统计信息查询生成 profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计信息查询生成查询 profile。
- 引入版本: v3.1.5

### `enable_table_name_case_insensitive`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否对 catalog 名称、数据库名称、表名称、视图名称和物化视图名称启用不区分大小写的处理。当前，表名称默认区分大小写。
  - 启用此功能后，所有相关名称将以小写形式存储，并且所有包含这些名称的 SQL 命令将自动将其转换为小写。
  - 您只能在创建集群时启用此功能。**集群启动后，此配置的值无法通过任何方式修改**。任何修改尝试都将导致错误。FE 在检测到此配置项的值与集群首次启动时不一致时将无法启动。
  - 当前，此功能不支持 JDBC catalog 和表名称。如果您想对 JDBC 或 ODBC 数据源执行不区分大小写的处理，请不要启用此功能。
- 引入版本: v4.0

### `enable_task_history_archive`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，已完成的任务运行记录将存档到持久的任务运行历史表中，并记录到 edit log 中，以便查找（例如，`lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`）包含存档结果。存档由 FE Leader 执行，并在单元测试 (`FeConstants.runningUnitTest`) 期间跳过。启用后，会绕过内存中的过期和强制 GC 路径（代码从 `removeExpiredRuns` 和 `forceGC` 中提前返回），因此保留/驱逐由持久存档处理，而不是 `task_runs_ttl_second` 和 `task_runs_max_history_number`。禁用后，历史记录保留在内存中，并由这些配置进行清理。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

### `enable_task_run_fe_evaluation`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，FE 将在 `TaskRunsSystemTable.supportFeEvaluation` 中对系统表 `task_runs` 执行本地评估。FE 侧评估仅允许用于将列与常量进行比较的合取相等谓词，并且仅限于 `QUERY_ID` 和 `TASK_NAME` 列。启用此功能可提高定向查找的性能，避免更广泛的扫描或额外的远程处理；禁用它会强制规划器跳过对 `task_runs` 的 FE 评估，这可能会减少谓词剪枝并影响这些过滤器的查询延迟。
- 引入版本: v3.3.13, v3.4.3, v3.5.0

### `heartbeat_mgr_blocking_queue_size`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 存储 Heartbeat Manager 运行的心跳任务的阻塞队列的大小。
- 引入版本: -

### `heartbeat_mgr_threads_num`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: Heartbeat Manager 可运行以运行心跳任务的线程数。
- 引入版本: -

### `ignore_materialized_view_error`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 是否忽略物化视图错误导致的元数据异常。如果 FE 因物化视图错误导致的元数据异常而无法启动，您可以将此参数设置为 `true` 以允许 FE 忽略该异常。
- 引入版本: v2.5.10

### `ignore_meta_check`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 非 Leader FE 是否忽略 Leader FE 的元数据差距。如果值为 TRUE，非 Leader FE 忽略 Leader FE 的元数据差距并继续提供数据读取服务。此参数确保即使您长时间停止 Leader FE，也能持续提供数据读取服务。如果值为 FALSE，非 Leader FE 不忽略 Leader FE 的元数据差距并停止提供数据读取服务。
- 引入版本: -

### `ignore_task_run_history_replay_error`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当 StarRocks 反序列化 `information_schema.task_runs` 的 TaskRun 历史行时，损坏或无效的 JSON 行通常会导致反序列化记录警告并抛出 RuntimeException。如果此项设置为 `true`，系统将捕获反序列化错误，跳过格式错误的记录，并继续处理剩余行而不是使查询失败。这将使 `information_schema.task_runs` 查询能够容忍 `_statistics_.task_run_history` 表中的错误条目。请注意，启用它将静默丢弃损坏的历史记录（潜在数据丢失），而不是显式报错。
- 引入版本: v3.3.3, v3.4.0, v3.5.0

### `leader_demotion_drain_timeout_sec`

- 默认值: 180
- 类型: Int
- 单位: 秒
- 是否动态: 是
- 描述: Leader 降级期间,用于等待每一个仅 Leader 运行的后台 daemon 线程在被中断后退出的超时时间。若超时后仍有线程存活,FE 进程会被终止,因为残留运行的线程再加上后续重新当选时启动的新线程,会同时操作同一批单例状态,危害大于进程重启。当 heartbeat / report / publish / tablet-scheduler 这些 daemon 需要的排空时间超出默认值时,可调大该值。
- 引入版本: v4.1

### `lock_checker_interval_second`

- 默认值: 30
- 类型: long
- 单位: 秒
- 是否可变: Yes
- 描述: LockChecker 前端守护程序（名为 "deadlock-checker"）执行的间隔（秒）。守护程序执行死锁检测和慢锁扫描；配置值乘以 1000 以设置计时器（毫秒）。减小此值可减少检测延迟但增加调度和 CPU 开销；增加此值可减少开销但延迟检测和慢锁报告。更改在运行时生效，因为守护程序每次运行都会重置其间隔。此设置与 `lock_checker_enable_deadlock_check`（启用死锁检查）和 `slow_lock_threshold_ms`（定义慢锁的构成）交互。
- 引入版本: v3.2.0

### `master_sync_policy`

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

### `max_bdbje_clock_delta_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: No
- 描述: StarRocks 集群中 Leader FE 与 Follower 或 Observer FE 之间允许的最大时钟偏移量。
- 引入版本: -

### `meta_delay_toleration_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Follower 和 Observer FE 上的元数据可以比 Leader FE 上的元数据落后最长时间。单位：秒。如果超过此持续时间，非 Leader FE 将停止提供服务。
- 引入版本: -

### `meta_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储元数据的目录。
- 引入版本: -

### `metadata_ignore_unknown_operation_type`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否忽略未知日志 ID。当 FE 回滚时，早期版本的 FE 可能无法识别某些日志 ID。如果值为 `TRUE`，FE 将忽略未知日志 ID。如果值为 `FALSE`，FE 将退出。
- 引入版本: -

### `profile_info_format`

- 默认值: default
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 系统输出的 Profile 格式。有效值：`default` 和 `json`。设置为 `default` 时，Profile 为默认格式。设置为 `json` 时，系统输出 JSON 格式的 Profile。
- 引入版本: v2.5

### `replica_ack_policy`

- 默认值: `SIMPLE_MAJORITY`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 日志条目被认为有效的策略。默认值 `SIMPLE_MAJORITY` 指定如果大多数 Follower FE 返回 ACK 消息，则日志条目被认为有效。
- 引入版本: -

### `replica_sync_policy`

- 默认值: SYNC
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Follower FE 将日志刷新到磁盘的策略。此参数仅当当前 FE 是 Follower FE 时有效。有效值：
  - `SYNC`: 事务提交时，日志条目同时生成并刷新到磁盘。
  - `NO_SYNC`: 事务提交时，日志条目的生成和刷新不同时发生。
  - `WRITE_NO_SYNC`: 事务提交时，日志条目同时生成但不刷新到磁盘。
- 引入版本: -

### `start_with_incomplete_meta`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，如果镜像数据存在但 Berkeley DB JE (BDB) 日志文件丢失或损坏，FE 将允许启动。`MetaHelper.checkMetaDir()` 使用此标志绕过安全检查，否则会阻止从没有相应 BDB 日志的镜像启动；以这种方式启动可能会产生陈旧或不一致的元数据，应仅用于紧急恢复。`RestoreClusterSnapshotMgr` 在恢复集群快照时暂时将此标志设置为 true，然后将其回滚；该组件在恢复期间也会切换 `bdbje_reset_election_group`。在正常操作中不要启用它 — 仅在从损坏的 BDB 数据恢复或显式恢复基于镜像的快照时启用。
- 引入版本: v3.2.0

### `table_keeper_interval_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: TableKeeper 守护程序执行的间隔（秒）。TableKeeperDaemon 使用此值（乘以 1000）设置其内部计时器，并定期运行 keeper 任务，以确保历史表存在、正确的表属性（复制数量）并更新分区 TTL。守护程序仅在 Leader 节点上执行工作，并在 `table_keeper_interval_second` 更改时通过 setInterval 更新其运行时间隔。增加此值可减少调度频率和负载；减少此值可更快响应缺失或陈旧的历史表。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

### `task_runs_ttl_second`

- 默认值: 7 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 控制任务运行历史记录的存活时间 (TTL)。降低此值会缩短历史记录保留时间并减少内存/磁盘使用；提高此值会保留更长时间的历史记录，但会增加资源使用。与 `task_runs_max_history_number` 和 `enable_task_history_archive` 一起调整，以实现可预测的保留和存储行为。
- 引入版本: v3.2.0

### `task_ttl_second`

- 默认值: 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 任务的存活时间 (TTL)。对于手动任务（未设置调度），TaskBuilder 使用此值计算任务的 `expireTime` (`expireTime = now + task_ttl_second * 1000L`)。TaskRun 也将此值用作计算运行执行超时的上限 — 有效执行超时为 `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`。调整此值会更改手动创建任务的有效时间，并可以间接限制任务运行的最大允许执行时间。
- 引入版本: v3.2.0

### `thrift_rpc_retry_times`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 Thrift RPC 调用将尝试的总次数。此值由 `ThriftRPCRequestExecutor`（以及 `NodeMgr` 和 `VariableMgr` 等调用者）用作重试的循环计数 — 即，值为 3 允许最多三次尝试，包括初始尝试。在 `TTransportException` 上，执行器将尝试重新打开连接并重试此计数；当原因是 `SocketTimeoutException` 或重新打开失败时，它不会重试。每次尝试都受 `thrift_rpc_timeout_ms` 配置的每次尝试超时限制。增加此值可提高对瞬时连接失败的弹性，但会增加整体 RPC 延迟和资源使用。
- 引入版本: v3.2.0

### `thrift_rpc_strict_mode`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 控制 Thrift 服务器使用的 TBinaryProtocol“严格读取”模式。此值作为第一个参数传递给 Thrift 服务器堆栈中的 org.apache.thrift.protocol.TBinaryProtocol.Factory，并影响如何解析和验证传入的 Thrift 消息。当 `true`（默认）时，服务器强制执行严格的 Thrift 编码/版本检查并遵守配置的 `thrift_rpc_max_body_size` 限制；当 `false` 时，服务器接受非严格（旧版/宽松）消息格式，这可以提高与旧客户端的兼容性，但可能会绕过某些协议验证。在运行中的集群上更改此值请谨慎，因为它不可变并影响互操作性和解析安全性。
- 引入版本: v3.2.0

### `thrift_rpc_timeout_ms`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 用作 Thrift RPC 调用默认网络/套接字超时的持续时间（毫秒）。它在 `ThriftConnectionPool`（前端和后端池使用）创建 Thrift 客户端时传递给 TSocket，并且在 `ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr` 和 `CheckpointWorker` 等地方计算 RPC 调用超时时也添加到操作的执行超时（例如 ExecTimeout*1000 + `thrift_rpc_timeout_ms`）。增加此值会使 RPC 调用能够容忍更长的网络或远程处理延迟；减少此值会导致慢速网络上的故障转移更快。更改此值会影响执行 Thrift RPC 的 FE 代码路径中的连接创建和请求截止日期。
- 引入版本: v3.2.0

### `txn_latency_metric_report_groups`

- 默认值: 一个空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 逗号分隔的事务延迟指标组列表，用于报告。加载类型被归类为逻辑组以进行监控。当启用某个组时，其名称将作为“类型”标签添加到事务指标中。有效值：`stream_load`、`routine_load`、`broker_load`、`insert` 和 `compaction`（仅适用于存算分离集群）。示例：`"stream_load,routine_load"`。
- 引入版本: v4.0

### `txn_rollback_limit`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 可回滚的最大事务数。
- 引入版本: -
