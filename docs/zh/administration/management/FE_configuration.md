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

FE 启动后，您可以在 MySQL 客户端中运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返回字段的详细说明，请参见 [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具备管理员权限才能运行集群管理相关命令。
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

##### `audit_log_delete_age`

- 默认值: 30d
- 类型: String
- 单位: -
- 可变: 否
- 描述: 审计日志文件的保留期限。默认值 `30d` 表示每个审计日志文件可以保留 30 天。StarRocks 会检查每个审计日志文件并删除 30 天前生成的那些。
- 引入版本: -

##### `audit_log_dir`

- 默认值: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型: String
- 单位: -
- 可变: 否
- 描述: 存储审计日志文件的目录。
- 引入版本: -

##### `audit_log_enable_compress`

- 默认值: false
- 类型: Boolean
- 单位: 不适用
- 可变: 否
- 描述: 当设置为 true 时，生成的 Log4j2 配置会为轮转的审计日志文件名 (fe.audit.log.*) 附加 ".gz" 后缀，以便 Log4j2 在轮转时生成压缩 (.gz) 的归档审计日志文件。此设置在 FE 启动期间在 Log4jConfig.initLogging 中读取，并应用于审计日志的 RollingFile appender；它只影响轮转/归档的文件，不影响活动审计日志。由于该值在启动时初始化，因此更改它需要重启 FE 才能生效。与审计日志轮转设置 (audit_log_dir, audit_log_roll_interval, audit_roll_maxsize, audit_log_roll_num) 一起使用。
- 引入版本: 3.2.12

##### `audit_log_json_format`

- 默认值: false
- 类型: Boolean
- 单位: 不适用
- 可变: 是
- 描述: 当为 true 时，FE 审计事件将以结构化 JSON 格式（Jackson ObjectMapper 序列化带注解的 AuditEvent 字段的 Map）发出，而不是默认的管道分隔的“key=value”字符串。此设置影响 AuditLogBuilder 处理的所有内置审计接收器：连接审计、查询审计、大查询审计（当事件符合条件时，大查询阈值字段会添加到 JSON 中）和慢查询审计输出。用于大查询阈值和“features”字段的注解字段会被特殊处理（从普通审计条目中排除；根据适用情况包含在大查询或功能日志中）。启用此功能可使日志机器可解析，便于日志收集器或 SIEM 使用；请注意，它会更改日志格式，并且可能需要更新任何期望传统管道分隔格式的现有解析器。
- 引入版本: 3.2.7

##### `audit_log_modules`

- 默认值: slow_query, query
- 类型: String[]
- 单位: -
- 可变: 否
- 描述: StarRocks 生成审计日志条目的模块。默认情况下，StarRocks 为 `slow_query` 模块和 `query` 模块生成审计日志。从 v3.0 版本开始支持 `connection` 模块。模块名称之间用逗号 (,) 和空格分隔。
- 引入版本: -

##### `audit_log_roll_interval`

- 默认值: DAY
- 类型: String
- 单位: -
- 可变: 否
- 描述: StarRocks 轮转审计日志条目的时间间隔。有效值：`DAY` 和 `HOUR`。
  - 如果此参数设置为 `DAY`，则审计日志文件名称中将添加 `yyyyMMdd` 格式的后缀。
  - 如果此参数设置为 `HOUR`，则审计日志文件名称中将添加 `yyyyMMddHH` 格式的后缀。
- 引入版本: -

##### `audit_log_roll_num`

- 默认值: 90
- 类型: Int
- 单位: -
- 可变: 否
- 描述: 在 `audit_log_roll_interval` 参数指定每个保留期内可保留的审计日志文件的最大数量。
- 引入版本: -

##### `bdbje_log_level`

- 默认值: INFO
- 类型: String
- 单位: -
- 可变: 否
- 描述: 控制 StarRocks 中 Berkeley DB Java Edition (BDB JE) 使用的日志记录级别。在 BDB 环境初始化 BDBEnvironment.initConfigs() 期间，此值将应用于 `com.sleepycat.je` 包的 Java 日志记录器和 BDB JE 环境文件日志记录级别 (EnvironmentConfig.FILE_LOGGING_LEVEL)。接受标准 `java.util.logging.Level` 名称，例如 `SEVERE`、`WARNING`、`INFO`、`CONFIG`、`FINE`、`FINER`、`FINEST`、`ALL`、`OFF`。设置为 `ALL` 将启用所有日志消息。增加详细程度会提高日志量，并可能影响磁盘 I/O 和性能；该值在 BDB 环境初始化时读取，因此仅在环境（重新）初始化后才生效。
- 引入版本: v3.2.0

##### `big_query_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 可变: 否
- 描述: 控制 FE 大查询日志文件 (`fe.big_query.log.*`) 在自动删除前保留多长时间。该值作为 IfLastModified age 传递给 Log4j 的删除策略 — 任何最后修改时间早于此值的轮转大查询日志都将被删除。支持的后缀包括 `d`（天）、`h`（小时）、`m`（分钟）和 `s`（秒）。示例：`7d`（7 天）、`10h`（10 小时）、`60m`（60 分钟）和 `120s`（120 秒）。此项与 `big_query_log_roll_interval` 和 `big_query_log_roll_num` 一起确定哪些文件被保留或清除。
- 引入版本: v3.2.0

##### `big_query_log_dir`

- 默认值: `Config.STARROCKS_HOME_DIR + "/log"`
- 类型: String
- 单位: -
- 可变: 否
- 描述: FE 写入大查询转储日志 (`fe.big_query.log.*`) 的目录。Log4j 配置使用此路径为 `fe.big_query.log` 及其轮转文件创建 RollingFile appender。轮转和保留由 `big_query_log_roll_interval`（基于时间后缀）、`log_roll_size_mb`（大小触发）、`big_query_log_roll_num`（最大文件数）和 `big_query_log_delete_age`（基于年龄删除）控制。对于超过用户定义阈值（例如 `big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold` 或 `big_query_log_scan_bytes_threshold`）的查询，会记录大查询记录。使用 `big_query_log_modules` 控制哪些模块记录到此文件中。
- 引入版本: v3.2.0

##### `big_query_log_modules`

- 默认值: `{"query"}`
- 类型: String[]
- 单位: -
- 可变: 否
- 描述: 启用每个模块大查询日志记录的模块名称后缀列表。典型值是逻辑组件名称。例如，默认的 `query` 会产生 `big_query.query`。
- 引入版本: v3.2.0

##### `big_query_log_roll_interval`

- 默认值: `"DAY"`
- 类型: String
- 单位: -
- 可变: 否
- 描述: 指定用于构造 `big_query` 日志 appender 滚动文件名日期部分的时间间隔。有效值（不区分大小写）为 `DAY`（默认值）和 `HOUR`。`DAY` 产生每日模式 (`"%d{yyyyMMdd}"`)，`HOUR` 产生每小时模式 (`"%d{yyyyMMddHH}"`)。该值与基于大小的滚动 (`big_query_roll_maxsize`) 和基于索引的滚动 (`big_query_log_roll_num`) 结合，形成 RollingFile filePattern。无效值会导致日志配置生成失败 (IOException)，并可能阻止日志初始化或重新配置。与 `big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num` 和 `big_query_log_delete_age` 一起使用。
- 引入版本: v3.2.0

##### `big_query_log_roll_num`

- 默认值: 10
- 类型: Int
- 单位: -
- 可变: 否
- 描述: 每个 `big_query_log_roll_interval` 要保留的轮转 FE 大查询日志文件的最大数量。此值绑定到 RollingFile appender 的 DefaultRolloverStrategy `max` 属性，用于 `fe.big_query.log`；当日志轮转时（按时间或按 `log_roll_size_mb`），StarRocks 最多保留 `big_query_log_roll_num` 个索引文件（filePattern 使用时间后缀加索引）。旧于此计数的文件可能会在轮转时被删除，并且 `big_query_log_delete_age` 可以额外根据最后修改时间删除文件。
- 引入版本: v3.2.0

##### `dump_log_delete_age`

- 默认值: 7d
- 类型: String
- 单位: -
- 可变: 否
- 描述: 转储日志文件的保留期限。默认值 `7d` 表示每个转储日志文件可以保留 7 天。StarRocks 会检查每个转储日志文件并删除 7 天前生成的那些。
- 引入版本: -

##### `dump_log_dir`

- 默认值: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- 类型: String
- 单位: -
- 可变: 否
- 描述
