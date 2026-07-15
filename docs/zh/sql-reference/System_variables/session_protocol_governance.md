---
displayed_sidebar: docs
sidebar_label: "会话、协议与治理"
sidebar_position: 8
description: "用于 MySQL 协议兼容、查询队列、超时、资源组以及性能分析的会话变量。"
---

# 系统变量 - 会话、协议与治理

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### activate_all_roles_on_login (global)

* 描述：用于控制是否在用户登录时默认激活所有角色（包括默认角色和授予的角色）。
  * 开启后，在用户登录时默认激活所有角色，优先级高于通过 [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) 设置的角色。
  * 如果不开启，则会默认激活 SET DEFAULT ROLE 中设置的角色。
* 默认值：false，表示不开启。
* 引入版本：v3.0

如果要在当前会话中激活一个角色，可以使用 [SET ROLE](sql-statements/account-management/SET_ROLE.md)。

### auto_increment_increment

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：1
* 类型：Int

### binary_encoding_format

* **作用域**: Session
* **描述**: 控制 StarRocks 在 MySQL 文本结果中如何编码 `BINARY` / `VARBINARY` 值。可选值为 `raw`、`hex` 和 `base64`，默认值为 `hex`。该变量需要结合 `binary_encoding_level` 一起理解。对于顶层二进制列，MySQL 客户端通常可以直接处理；但当二进制值出现在 `ARRAY`、`MAP`、`STRUCT` 等嵌套类型中时，结果会以类 JSON 字符串的形式返回，此时为了保证内容可打印且格式稳定，往往需要额外编码。若希望结果更紧凑可读，可以使用 `base64`；若希望完全保留原始字节，可以设置为 `raw`。
* **默认值**: `hex`
* **数据类型**: String
* **引入版本**: v4.1

### binary_encoding_level

* **作用域**: Session
* **描述**: 控制 MySQL 文本结果中哪些二进制值需要编码。可选值为 `nested` 和 `all`，默认值为 `nested`。`nested` 用于兼容历史行为，即仅对 `ARRAY`、`MAP`、`STRUCT` 等嵌套类型中的二进制值进行编码，而顶层二进制列保持原有行为。若团队希望所有二进制输出都遵循统一的编码规范，可以设置为 `all`，此时顶层二进制值也会一起编码。若 `binary_encoding_format = raw`，则不会额外执行二进制编码，即使这里设置为 `nested` 或 `all`，嵌套输出的可读性也可能下降。
* **默认值**: `nested`
* **数据类型**: String
* **引入版本**: v4.1

### big_query_profile_threshold

* 描述：用于设定大查询的阈值。当会话变量 `enable_profile` 设置为 `false` 且查询时间超过 `big_query_profile_threshold` 设定的阈值时，则会生成 Profile。

  NOTE：在 v3.1.5 至 v3.1.7 以及 v3.2.0 至 v3.2.2 中，引入了 `big_query_profile_second_threshold` 参数，用于设定大查询的阈值。而在 v3.1.8、v3.2.3 及后续版本中，此参数被 `big_query_profile_threshold` 替代，以便提供更加灵活的配置选项。
* 默认值：0
* 单位：秒
* 类型：String
* 引入版本：v3.1

### catalog（3.2.4 及以后）

* 描述：用于指定当前会话所在的 Catalog。
* 默认值：default_catalog
* 类型：String
* 引入版本：v3.2.4

### character_set_database（global）

* 描述：StarRocks 数据库支持的字符集，当前仅支持 UTF8 编码（`utf8`）。
* 默认值：utf8
* 类型：String

### collation_server

* **范围**: Session
* **描述**: 会话级别的服务器校对名，由 FE 用于为该会话呈现兼容 MySQL 的校对行为。该变量设置 FE 向客户端报告并与 `character_set_server` / `collation_connection` / `collation_database` 关联的默认校对标识符（例如 `utf8_general_ci`）。它会以会话变量 JSON 的形式持久化（参见 SessionVariable#getJsonString / replayFromJson），并通过变量管理器暴露（`@VarAttr(name = COLLATION_SERVER)`），因此会出现在 SHOW VARIABLES 中并且可以按会话更改。该值以普通 String 存储在 SessionVariable 中，通常保存标准的 MySQL 校对名（例如 `utf8_general_ci`、`utf8mb4_unicode_ci`）；代码在此处不强制固定枚举或额外校验，因此实际行为取决于下游解释校对名称以用于比较、排序和其他与校对相关操作的组件。
* **默认值**: `utf8_general_ci`
* **类型**: String
* **引入版本**: `v3.2.0`

### custom_query_id (session)

* **描述**: 用于将某些外部标识绑定到当前查询。在执行查询前可以使用 `SET SESSION custom_query_id = 'my-query-id';` 进行设置。查询结束后该值会被重置。该值可以传递给 `KILL QUERY 'my-query-id'`。在审计日志中可以作为 `customQueryId` 字段找到该值。
* **默认值**: ""
* **类型**: String
* **引入版本**: v3.4.0

### default_authentication_plugin

* **范围**: Session
* **描述**: 会话范围的变量，指定本会话的默认 MySQL 认证插件名称。它以 SessionVariable.defaultAuthenticationPlugin 的形式存储，并由 StarRocks 的 MySQL 协议兼容层在服务器需要通告或使用默认认证插件时使用（例如在握手期间或未指定插件时）。接受服务器支持的标准 MySQL 认证插件标识（例如 `mysql_native_password`、`caching_sha2_password`）。此变量仅影响会话行为；持久化的用户账号认证配置由其他机制单独管理。参见相关会话变量 `authentication_policy`。
* **默认值**: `mysql_native_password`
* **类型**: String
* **引入版本**: -

### default_rowset_type (global)

全局变量，仅支持全局生效。用于设置计算节点存储引擎默认的存储格式。当前支持的存储格式包括：alpha/beta。

### default_table_compression

* 描述：存储表格数据时使用的默认压缩算法，支持 LZ4、Zstandard（或 zstd）、zlib 和 Snappy。如果您建表时在 PROPERTIES 设置了 `compression`，则 `compression` 指定的压缩算法生效。
* 默认值：lz4_frame
* 类型：String
* 引入版本：v3.0

### default_tmp_storage_engine

* **描述**: 会话变量，用于控制临时表的默认存储引擎（包括显式的 `CREATE TEMPORARY TABLE` 以及引擎创建的内部/隐式临时表）。在 `SessionVariable.java` 中以 `@VariableMgr.VarAttr` 注解声明，主要用于 MySQL 8.0 的兼容性，以便期望 MySQL 行为的客户端和工具可以在每个会话级别查看或更改临时表引擎。更改此值会影响在不同引擎下（例如基于内存 vs 基于磁盘的引擎）如何在存储层上存放/管理临时表数据。
* **范围**: Session
* **默认值**: `InnoDB`
* **类型**: String
* **引入版本**: v3.4.2, v3.5.0

### default_view_sql_security

* **描述**: 创建视图时，如果 `CREATE VIEW` 语句未显式指定 `SECURITY` 子句，则使用该变量作为默认的 SQL SECURITY 特性。`NONE`（等价于显式的 `SECURITY NONE` 子句）表示查询视图时只需要执行者拥有该视图本身的 `SELECT` 权限，不会针对执行者校验视图所引用的表的权限；`INVOKER`（等价于 `SECURITY INVOKER`）表示执行者还必须拥有视图所引用的表的 `SELECT` 权限。语句中显式指定的 `SECURITY NONE` 或 `SECURITY INVOKER` 子句始终优先于该变量。该变量仅影响 `CREATE VIEW`，不影响 `ALTER VIEW`。
* **范围**: Session
* **默认值**: `NONE`
* **类型**: String
* **取值范围**: `NONE`, `INVOKER`
* **引入版本**: v4.1.1

### div_precision_increment

* 描述：用于兼容 MySQL 客户端，无实际作用。
* 默认值：4
* 类型：Int

### enable_color_explain_output

* **范围**: Session
* **描述**: 控制在文本形式的 EXPLAIN / PROFILE 输出中是否包含 ANSI 颜色转义序列。启用时（`true`），StmtExecutor 会将会话设置传递到 EXPLAIN/PROFILE 流水线，以便 EXPLAIN、EXPLAIN ANALYZE 和 ANALYZE PROFILE 输出在支持 ANSI 的终端中包含颜色高亮以提高可读性。禁用时（`false`），输出将不包含 ANSI 序列，适用于日志记录、不支持 ANSI 的客户端或将输出重定向到文件的场景。该项不改变执行语义，仅影响 EXPLAIN/PROFILE 文本的展示。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.5.0

### enable_group_level_query_queue (global)

* 描述：是否开启资源组粒度的[查询队列](../administration/management/resource_management/query_queues.md)。
* 默认值：false，表示不开启。
* 引入版本：v3.1.4

### max_unknown_string_meta_length (global)

* 描述：当字符串列的最大长度未知时用于元数据的回退长度。如果客户端依赖该元数据且报告的长度小于真实值，部分 BI 工具可能返回空值或截断。小于等于 0 时回退为 `64`；有效范围为 `1` ~ `1048576`。
* 默认值：64
* 数据类型：Int
* 引入版本：v3.5.16、v4.0.9

### enable_metadata_profile

* 描述：是否为 Iceberg Catalog 的元数据收集查询开启 Profile。
* 默认值：true
* 引入版本：v3.3.3

### enable_profile

用于设置是否需要查看查询的 profile。默认为 `false`，即不需要查看 profile。2.5 版本之前，该变量名称为 `is_report_success`，2.5 版本之后更名为 `enable_profile`。

默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 `true` 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面（地址：fe_host:fe_http_port/query）查看 profile。该页面会显示最近 100 条开启了 `enable_profile` 的查询的 profile。

### enable_explain_in_profile

* **范围**: Session
* **描述**: 当该变量为 `true` 且该查询会生成 profile 时，会将已执行计划的 `EXPLAIN COSTS` 文本嵌入到 profile 的 `Summary` 段中，键名为 `ExplainPlan`。这样在离线分析 profile 工件（无需访问运行中的集群）时，可以同时查看优化器的基数估算、列统计、谓词下推、Runtime Filter 声明和总体计划代价等信息，便于排查慢查询。

  嵌入到 profile 中的计划与其他持久化的 SQL 工件遵循一致的脱敏控制：包含凭据的字面量（例如 `FILES(...)`）始终会被屏蔽；当集群级 FE 配置 `enable_sql_desensitize_in_log` 或会话变量 `enable_desensitize_explain` 任一项开启时，谓词 / 投影中的字面量将以摘要形式渲染。
* **默认值**: false
* **类型**: boolean

### profile_log_latency_threshold_ms

* **范围**: Session
* **描述**: 写入 `fe.profile.log` 的查询最小延迟（毫秒）。仅当查询执行时间大于或等于该值时才记录 profile。设为 `-1`（默认）时使用 FE 配置项 `profile_log_latency_threshold_ms`。设为 `0` 时记录所有 profile。设为正数（如 `1000`）时仅记录延迟 ≥ 该值（毫秒）的查询。可通过该会话变量按连接覆盖集群级配置。
* **默认值**: -1
* **类型**: long
* **单位**: 毫秒

### enable_query_queue_load (global)

* 描述：用于控制是否为导入任务启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_query_queue_select (global)

* 描述：用于控制是否为 SELECT 查询启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_query_queue_statistic (global)

* 描述：用于控制是否为统计信息查询启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_strict_order_by

* 描述：是否校验 ORDER BY 引用列是否有歧义。设置为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。
* 默认值：true
* 引入版本：v2.5.18，v3.1.7

### event_scheduler

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：OFF
* 类型：String

### forward_to_leader

用于设置是否将一些命令转发到 Leader FE 节点执行。默认为 `false`，即不转发。StarRocks 中存在多个 FE 节点，其中一个为 Leader 节点。通常用户可以连接任意 FE 节点进行全功能操作。但部分信息查看指令只有从 Leader FE 节点才能获取详细信息。别名 `forward_to_master`。

如 `SHOW BACKENDS;` 命令，如果不转发到 Leader FE 节点，则仅能看到节点是否存活等一些基本信息，而转发到 Leader FE 则可以获取包括节点启动时间、最后一次心跳时间等更详细的信息。

当前受该参数影响的命令如下：

* SHOW FRONTENDS;

  转发到 Leader 可以查看最后一次心跳信息。

* SHOW BACKENDS;

  转发到 Leader 可以查看启动时间、最后一次心跳信息、磁盘容量信息。

* SHOW BROKER;

  转发到 Leader 可以查看启动时间、最后一次心跳信息。

* SHOW TABLET;
* ADMIN SHOW REPLICA DISTRIBUTION;
* ADMIN SHOW REPLICA STATUS;

  转发到 Leader 可以查看 Leader FE 元数据中存储的 tablet 信息。正常情况下，不同 FE 元数据中 tablet 信息应该是一致的。当出现问题时，可以通过这个方法比较当前 FE 和 Leader FE 元数据的差异。

* SHOW PROC;

  转发到 Leader 可以查看 Leader FE 元数据中存储的相关 PROC 的信息。主要用于元数据比对。

### group_concat_max_len

* 描述：[group_concat](sql-functions/string-functions/group_concat.md) 函数返回的字符串的最大长度，单位为字符。
* 默认值：1024
* 最小值：4
* 类型：Long

### historical_nodes_min_update_interval

- 描述：历史节点记录两次更新之间的最小间隔。如果集群的节点在短时间内频繁变化（即小于此变量中设置的值），一些中间状态将不会被记录为有效的历史节点快照。历史节点是 Cache Sharing 功能在集群扩展时选择正确缓存节点的主要依据。
- 默认值：600
- 单位：秒
- 引入版本：v3.5.1

### init_connect (global)

用于兼容 MySQL 客户端。无实际作用。

### innodb_read_only

* **描述**: 会话级别标志（兼容 MySQL），指示会话的 InnoDB 只读模式。该变量在会话中以 Java 字段 `innodbReadOnly`（定义于 `SessionVariable.java`）声明并存储，可通过 `isInnodbReadOnly()` 和 `setInnodbReadOnly(boolean)` 访问。SessionVariable 类仅保存该标志；任何强制执行（例如阻止对 InnoDB 表的写/DDL 或改变事务行为）必须由事务/存储/授权层来实现，这些层应读取此会话标志。对尊重该标志的组件而言，使用此变量在当前会话中传达客户端的只读意图。
* **范围**: Session
* **默认值**: `true`
* **类型**: boolean
* **引入版本**: v3.2.0

### interactive_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：1024
* 单位：秒
* 类型：Int

### language (global)

用于兼容 MySQL 客户端。无实际作用。

### license (global)

* 描述：显示 StarRocks 的 license。无其他作用。
* 默认值：Apache License 2.0
* 类型：String

### lower_case_table_names (global)

用于兼容 MySQL 客户端，无实际作用。StarRocks 中的表名是大小写敏感的。

### max_allowed_packet

* 描述：用于兼容 JDBC 连接池 C3P0。该变量值决定了服务端发送给客户端或客户端发送给服务端的最大 packet 大小。当客户端报 `PacketTooBigException` 异常时，可以考虑调大该值。
* 默认值：33554432 (32 MB)
* 单位：Byte
* 类型：Int

### net_buffer_length

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：16384
* 类型：Int

### net_read_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：60
* 单位：秒
* 类型：Int

### net_write_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：60
* 单位：秒
* 类型：Int

### performance_schema (global)

用于兼容 8.0.16 及以上版本的 MySQL JDBC。无实际作用。

### pipeline_profile_level

* 描述：用于控制 profile 的等级。一个 profile 通常有 5 个层级：Fragment、FragmentInstance、Pipeline、PipelineDriver、Operator。不同等级下，profile 的详细程度有所区别：
  * 0：在此等级下，StarRocks 会合并 profile，只显示几个核心指标。
  * 1：默认值。在此等级下，StarRocks 会对 profile 进行简化处理，将同一个 pipeline 的指标做合并来缩减层级。
  * 2：在此等级下，StarRocks 会保留 Profile 所有的层级，不做简化。该设置下 profile 的体积会非常大，特别是 SQL 较复杂时，因此不推荐该设置。
* 默认值：1
* 类型：Int

### query_queue_concurrency_limit (global)

* 描述：单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：`0`
* 单位：-
* 类型：Int

### query_queue_cpu_used_permille_limit (global)

* 描述：单个 BE 节点中内存使用千分比上限（即 CPU 使用率）。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：0。
* 取值范围：[0, 1000]

### query_queue_max_queued_queries (global)

* 描述：队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：1024。

### query_queue_mem_used_pct_limit (global)

* 描述：单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：`0`
* 取值范围：[0, 1]

### query_queue_pending_timeout_second (global)

* 描述：队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。
* 默认值：300
* 单位：秒

### query_timeout

* 描述：用于设置查询超时时间，单位为秒。该变量会作用于当前连接中所有的查询语句。从 v3.4.0 起，`query_timeout` 不再适用于涉及 INSERT 的操作（例如，UPDATE、DELETE、CTAS、物化视图刷新、统计数据收集和 PIPE）。
* 默认值：300 （5 分钟）
* 单位：秒
* 类型：Int
* 取值范围：1 ~ 259200

### resource_group

* **描述**: 此会话指定的 resource group
* **默认值**: ""
* **数据类型**: String
* **引入版本**: 3.2.0

### runtime_profile_report_interval

* 描述：Runtime Profile 的上报间隔。
* 默认值：10
* 单位：秒
* 类型：Int
* 引入版本：v3.1.0

### sql_dialect

* 描述：设置生效的 SQL 语法。例如，执行 `set sql_dialect = 'trino';` 命令可以切换为 Trino 语法，这样您就可以在查询中使用 Trino 特有的 SQL 语法和函数。

  > **注意**
  >
  > 设置使用 Trino 语法后，查询默认对大小写不敏感。因此，您在 StarRocks 内建库、表时必须使用小写的库、表名称，否则查询会失败。
* 默认值：StarRocks
* 类型：String
* 引入版本：v3.0

### sql_mode

用于指定 SQL 模式，以适应某些 SQL 方言。有效值包括：

* `PIPES_AS_CONCAT`：管道符号 `|` 用于连接字符串。例如：`select 'hello ' || 'world'`。
* `ONLY_FULL_GROUP_BY` (默认值)：SELECT LIST 中只能包含 GROUP BY 列或者聚合函数。
* `ALLOW_THROW_EXCEPTION`：类型转换报错而不是返回 NULL。
* `FORBID_INVALID_DATE`：禁止非法的日期。
* `MODE_DOUBLE_LITERAL`：将浮点类型解释为 DOUBLE 而非 DECIMAL。
* `SORT_NULLS_LAST`：排序后，将 NULL 值放到最后。
* `ERROR_IF_OVERFLOW`：运算溢出时，报错而不是返回 NULL，目前仅 DECIMAL 支持这一行为。
* `GROUP_CONCAT_LEGACY`：使用 2.5 及以前的 `group_concat` 的语法。该选项从 3.0.9，3.1.6 开始支持。
* `FORBID_INVALID_IMPLICIT_CAST`：在计划阶段启用类似 Trino 的严格类型检查。仅允许同一类型族内的扩宽（widening）隐式转换，例如 `TINYINT`→`INT`→`BIGINT`→`DECIMAL`→`DOUBLE`、`DATE`→`DATETIME`。`VARCHAR`/`CHAR` 之间的隐式转换不校验声明长度，仍然允许。跨类型族的转换（例如 `string`↔`numeric`、`string`↔`date`、`numeric`↔`date`、`boolean` 与其他类型之间）以及数值窄化转换（例如 `BIGINT`→`INT`、`DOUBLE`→`FLOAT`）会被拒绝并返回语义错误。如需进行此类转换，请使用显式 `CAST`。
* `STRUCT_CAST_BY_NAME`：在 STRUCT 类型之间进行类型转换时，启用基于名称的字段匹配，而非默认的基于位置的匹配。启用此模式后，源 Struct 中的字段将根据字段名称（不区分大小写）与目标 Struct 中的字段进行匹配，无论它们的声明顺序如何。源 Struct 中存在而目标 Struct 中缺失的字段将被忽略；目标 Struct 中存在而源 Struct 中缺失的字段将被填充为 NULL。此模式同时影响 FE 类型解析（UNION ALL 的通用超类型计算和可转换性检查）以及 BE 转换评估（CastStructExpr 中的运行时字段重新排序）。当对 STRUCT 列执行 UNION ALL 操作时，若各分支中字段的定义顺序不同，此模式尤为有用。

不同模式之间可以独立设置，您可以单独开启某一个模式，例如：

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

或者，您也可以同时设置多个模式，例如：

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_safe_updates

用于兼容 MySQL 客户端。无实际作用。

### sql_select_limit

* 描述：用于限制查询返回的结果集的最大行数，可以防止因查询返回过多的数据而导致内存不足或网络拥堵等问题。
* 默认值：无限制
* 类型：Long

### storage_engine

指定系统使用的存储引擎。StarRocks 支持的引擎类型包括：

* olap (默认值)：StarRocks 系统自有引擎。
* mysql：使用 MySQL 外部表。
* broker：通过 Broker 程序访问外部表。
* ELASTICSEARCH 或者 es：使用 Elasticsearch 外部表。
* HIVE：使用 Hive 外部表。
* ICEBERG：使用 Iceberg 外部表。从 2.1 版本开始支持。
* HUDI: 使用 Hudi 外部表。从 2.2 版本开始支持。
* jdbc: 使用 JDBC 外部表。从2.3 版本开始支持。

### system_time_zone (global)

显示当前系统时区。不可更改。

### time_zone

用于设置当前会话的时区。时区会对某些时间函数的结果产生影响。

### version (global)

MySQL 服务器的版本，取值等于 FE 参数 `mysql_server_version`。

### version_comment (global)

用于显示 StarRocks 的版本，不可更改。

### wait_timeout

* 描述：用于设置客户端与 StarRocks 数据库交互时的最大空闲时长。如果一个空闲连接在该时长内与 StarRocks 数据库没有任何交互，StarRocks 会主动断开这个连接。
* 默认值：28800（即 8 小时）
* 单位：秒
* 类型：Int
