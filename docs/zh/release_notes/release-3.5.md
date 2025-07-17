---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

升级至 v3.5 后，请勿直接将集群降级至 v3.4.0 ~ v3.4.4，否则会导致元数据不兼容。您必须降级到 v3.4.5 或更高版本以避免出现此问题。

:::

## v3.5.1

发布日期：2025 年 7 月 1 日

### 新增特性

- [Experimental] StarRocks 自 3.5.1 版本起，引入基于 Apache Arrow Flight SQL 协议的高性能数据传输链路，全面优化数据读取路径，显著提升传输效率。该方案打通了从 StarRocks 列式执行引擎到客户端的全链路列式传输，避免了传统 JDB 和 ODBC 接口中频繁的行列转换与序列化开销，真正实现了零拷贝、低延迟、高吞吐的数据传输能力。[#57956](https://github.com/StarRocks/starrocks/pull/57956)
- Java Scalar UDF（用户自定义函数）的输入参数支持 ARRAY 和 MAP 类型。[#55356](https://github.com/StarRocks/starrocks/pull/55356)
- **跨节点数据缓存共享功能**：支持在计算节点之间通过网络共享远程数据湖上外表数据的缓存。当某个计算节点本地缓存未命中时，会优先尝试从同一集群内其他节点的缓存中获取数据，只有在集群内所有节点缓存均未命中的情况下，才会从远程存储重新拉取数据。此功能可有效降低在弹性扩缩容过程中缓存失效导致的性能抖动，确保查询性能稳定。新增 FE 配置参数 `enable_trace_historical_node` 控制该行为，默认为 `false`。 [#57083](https://github.com/StarRocks/starrocks/pull/57083)
- **Storage Volume 新增对 Google Cloud Storage (GCS) 的原生支持**：支持以 GCS 作为后端存储卷，以及通过原生的 SDK 管理和访问 GCS 存储资源。[#58815](https://github.com/StarRocks/starrocks/pull/58815)

### 功能优化

- 优化创建 Hive 外表失败时的报错信息。[#60076](https://github.com/StarRocks/starrocks/pull/60076)
- 通过 Iceberg Metadata 中的 `file_record_count` 优化 `count(1)` 查询性能。[#60022](https://github.com/StarRocks/starrocks/pull/60022)
- 优化 Compaction 调度逻辑，避免在所有子任务都成功的情况下依然延迟调度的情况发生。[#59998](https://github.com/StarRocks/starrocks/pull/59998)
- BE 和 CN 节点升级到 JDK17 后，新增 `JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED"`。[#59947](https://github.com/StarRocks/starrocks/pull/59947)
- 支持在 Kafka Broker 的 Endpoint 发生变更时通过 ALTER ROUTINE LOAD 命令修改 `kafka_broker_list` 属性。[#59787](https://github.com/StarRocks/starrocks/pull/59787)
- 支持通过参数精简 Docker Base Image 构建时的依赖。[#59772](https://github.com/StarRocks/starrocks/pull/59772)
- 支持通过 Managed Identity 认证访问 Azure。[#59657](https://github.com/StarRocks/starrocks/pull/59657)
- 优化通过 `Files()` 函数查询外部数据时路径列重名的报错信息。[#59597](https://github.com/StarRocks/starrocks/pull/59597)
- 优化 LIMIT 下推逻辑。[#59265](https://github.com/StarRocks/starrocks/pull/59265)

### 问题修复

修复了如下问题：

- 当查询包含 Max 和 Min 且包含空分区时的分区裁剪的问题。[#60162](https://github.com/StarRocks/starrocks/pull/60162)
- 物化视图改写查询时丢失 Null 分区的而导致的查询结果不正确问题。[#60087](https://github.com/StarRocks/starrocks/pull/60087)
- Iceberg 外表使用基于 `str2date` 函数的分区表达式时导致的刷新异常。[#60089](https://github.com/StarRocks/starrocks/pull/60089)
- 使用 START END 方式创建的临时分区的分区范围不正确的问题。[#60014](https://github.com/StarRocks/starrocks/pull/60014)
- Routine Load 指标在 非 Leader FE 节点上显示不正确的问题。[#59985](https://github.com/StarRocks/starrocks/pull/59985)
- 执行包含 `COUNT(*)` 窗口函数的查询会触发 BE/CN 崩溃。[#60003](https://github.com/StarRocks/starrocks/pull/60003)
- 通过 Stream Load 导入时目标表表名包含中文时导入失败的问题。[#59722](https://github.com/StarRocks/starrocks/pull/59722)
- 导入至三副本表时，某个 Secondary 副本导入失败而导致导入整体失败的问题。[#59762](https://github.com/StarRocks/starrocks/pull/59762)
- SHOW CREATE VIEW 丢失参数的问题。[#59714](https://github.com/StarRocks/starrocks/pull/59714)

### 行为变更

- 部分 FE 指标新增 `is_leader` 标签。[#59883](https://github.com/StarRocks/starrocks/pull/59883)

## v3.5.0

发布日期：2025 年 6 月 13 日

### 升级注意事项

- 从 StarRocks v3.5.0 起，需使用 JDK 17 或更高版本。
  - 如从 v3.4 或更早版本升级集群，需先升级 JDK，并在 FE 配置文件 **fe.conf** 中移除 `JAVA_OPTS` 中与 JDK 17 不兼容的参数（如 CMS 和 GC 参数）。推荐直接使用 v3.5 版本的 `JAVA_OPTS` 默认值。
  - 对于使用 External Catalog 的集群，需要在 BE 配置文件 **be.conf** 的配置项 `JAVA_OPTS` 中添加 `--add-opens=java.base/java.util=ALL-UNNAMED`。
  - 此外，自 v3.5.0 起，StarRocks 不再提供特定 JDK 版本的 JVM 配置，所有 JDK 版本统一使用 `JAVA_OPTS`。

### 存算分离

- 存算分离集群支持生成列。[#53526](https://github.com/StarRocks/starrocks/pull/53526)
- 存算分离集群中的云原生主键表支持重建指定索引，并优化了索引性能。[#53971](https://github.com/StarRocks/starrocks/pull/53971) [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- 优化了大规模数据导入的执行逻辑，避免因内存限制在 Rowset 中生成过多小文件。导入执行过程中，系统会将临时数据块进行合并，减少小文件的生成，从而提升导入后的查询性能，也减少了后续的 Compaction 操作，提升系统资源利用率。[#53954](https://github.com/StarRocks/starrocks/issues/53954)

### 数据湖分析

- **[Beta]** 支持在集成 Hive Metastore 的 Iceberg Catalog 中创建 Iceberg 视图，并支持通过 ALTER VIEW 语句添加或修改 Iceberg 视图的 SQL 方言，以增强与外部系统的语法兼容性。[#56120](https://github.com/StarRocks/starrocks/pull/56120)
- 支持 [Iceberg REST Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) 的嵌套命名空间。[#58016](https://github.com/StarRocks/starrocks/pull/58016)
- 支持使用 `IcebergAwsClientFactory` 在 [Iceberg REST Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) 创建 AWS 客户端以支持 Vended Credential。[#58296](https://github.com/StarRocks/starrocks/pull/58296)
- Parquet Reader 支持使用 Bloom Filter 进行数据过滤。[#56445](https://github.com/StarRocks/starrocks/pull/56445)
- 查询时，支持为 Parquet 格式的 Hive/Iceberg 表中低基数列自动创建全局字典。[#55167](https://github.com/StarRocks/starrocks/pull/55167)

### 性能提升与查询优化

- 统计信息优化：
  - 支持 Table Sample，通过对物理文件中的数据块采样，提升统计信息的准确性和查询性能。[#52787](https://github.com/StarRocks/starrocks/issues/52787)
  - 支持[记录查询中的谓词列](https://docs.starrocks.io/zh/docs/using_starrocks/Cost_based_optimizer/#predicate-column)，便于进行有针对性的统计信息收集。[#53204](https://github.com/StarRocks/starrocks/issues/53204)
  - 支持分区级基数估算。系统复用了 `_statistics_.column_statistics` 视图记录各分区的 NDV。[#51513](https://github.com/StarRocks/starrocks/pull/51513)
  - 支持[多列联合 NDV 收集](https://docs.starrocks.io/zh/docs/using_starrocks/Cost_based_optimizer/#%E5%A4%9A%E5%88%97%E8%81%94%E5%90%88%E7%BB%9F%E8%AE%A1%E4%BF%A1%E6%81%AF)，用于优化 CBO 在列间存在关联场景下的查询计划生成。[#56481](https://github.com/StarRocks/starrocks/pull/56481) [#56715](https://github.com/StarRocks/starrocks/pull/56715) [#56766](https://github.com/StarRocks/starrocks/pull/56766) [#56836](https://github.com/StarRocks/starrocks/pull/56836)
  - 支持使用直方图估算 Join 节点的基数和 in_predicate 的选择率，提高数据倾斜场景下的估算精度。[#57874](https://github.com/StarRocks/starrocks/pull/57874)
  - 优化 [Query Feedback](https://docs.starrocks.io/zh/docs/using_starrocks/query_feedback/) 功能：结构相同但参数值不同的查询会归为同一类型，共享 Tuning Guide 信息。[#58306](https://github.com/StarRocks/starrocks/pull/58306)
- 在特定场景下，支持使用 Runtime Bitset Filter 替代 Bloom Filter 进行优化。[#57157](https://github.com/StarRocks/starrocks/pull/57157)
- 支持将 Join Runtime Filter 下推到存储层。[#55124](https://github.com/StarRocks/starrocks/pull/55124)
- 支持 Pipeline Event Scheduler。[#54259](https://github.com/StarRocks/starrocks/pull/54259)

### 分区管理

- 支持通过 ALTER TABLE [合并基于时间函数的表达式分区](https://docs.starrocks.io/zh/docs/table_design/data_distribution/expression_partitioning/#%E8%A1%A8%E8%BE%BE%E5%BC%8F%E5%88%86%E5%8C%BA%E5%90%88%E5%B9%B6)，提升存储效率和查询性能。[#56840](https://github.com/StarRocks/starrocks/pull/56840)
- 支持为 List 分区表和物化视图设置分区 TTL（Time-to-live），并支持 `partition_retention_condition` 属性以设置更灵活的分区删除策略。[#53117](https://github.com/StarRocks/starrocks/issues/53117)
- 支持通过 ALTER TABLE [删除通用表达式指定的分区](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#%E5%88%A0%E9%99%A4%E5%88%86%E5%8C%BA)，便于批量删除。[#53118](https://github.com/StarRocks/starrocks/pull/53118)

### 集群管理

- FE 编译的 Java 目标版本从 Java 11 升级至 Java 17，提升系统稳定性和性能。[#53617](https://github.com/StarRocks/starrocks/pull/53617)

### 安全认证

- 支持基于 MySQL 协议的 [SSL 加密连接](https://docs.starrocks.io/zh/docs/administration/user_privs/ssl_authentication/)。[#54877](https://github.com/StarRocks/starrocks/pull/54877)
- 增强外部认证集成：
  - 支持使用 [OAuth 2.0](https://docs.starrocks.io/zh/docs/administration/user_privs/authentication/oauth2_authentication/) 和 [JSON Web Token（JWT）](https://docs.starrocks.io/zh/docs/administration/user_privs/authentication/jwt_authentication/)创建 StarRocks 用户。
  - 支持[安全集成](https://docs.starrocks.io/zh/docs/administration/user_privs/authentication/security_integration/)机制，简化与外部认证系统（如 LDAP、OAuth 2.0、JWT）的集成。[#55846](https://github.com/StarRocks/starrocks/pull/55846)
- 支持 [Group Provider](https://docs.starrocks.io/zh/docs/administration/user_privs/group_provider/) 从外部认证服务中获取用户组信息，并可用于认证和权限控制。支持从 LDAP、操作系统或文件中获取组信息。用户可通过 `current_group()` 查询所属的组。[#56670](https://github.com/StarRocks/starrocks/pull/56670)

### 物化视图

- 支持创建多个分区列的物化视图，实现更灵活的数据分区策略。[#52576](https://github.com/StarRocks/starrocks/issues/52576)
- 支持将 `query_rewrite_consistency` 设置为 `force_mv`，强制系统在改写查询时使用物化视图，以保证性能的稳定性（可能牺牲部分数据实时性）。[#53819](https://github.com/StarRocks/starrocks/pull/53819)

### 数据导入与导出

- 支持设置 `pause_on_json_parse_error` 为 `true`，在 JSON 解析失败时暂停 Routine Load 作业。[#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[Beta]** 支持包含[多个 SQL 语句的事务](https://docs.starrocks.io/zh/docs/loading/SQL_transaction/)（目前仅支持 INSERT 语句）。用户可启动、提交或撤销事务，以实现多次导入操作的 ACID 事务保障。[#53978](https://github.com/StarRocks/starrocks/issues/53978)

### 函数支持

- 引入系统变量 `lower_upper_support_utf8`（Session 级和全局级），增强大小写转换函数（如 `upper()`、`lower()`）对 UTF-8（特别是非 ASCII 字符）的支持。[#56192](https://github.com/StarRocks/starrocks/pull/56192)
- 新增函数：
  - [`field()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/field/) [#55331](https://github.com/StarRocks/starrocks/pull/55331)
  - [`ds_theta_count_distinct()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/ds_theta_count_distinct/) [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - [`array_flatten()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_flatten/) [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - [`inet_aton()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/inet_aton/) [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - [`percentile_approx_weight()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/percentile_approx_weight/) [#57410](https://github.com/StarRocks/starrocks/pull/57410)
