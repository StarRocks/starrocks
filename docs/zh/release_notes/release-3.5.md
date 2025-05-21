---
displayed_sidebar: docs
---

# StarRocks version 3.5

## v3.5.0-RC01

发布日期：2025 年 5 月 21 日

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
  - 支持 [Query Feedback](https://docs.starrocks.io/zh/docs/using_starrocks/query_feedback/) 功能：结构相同但参数不同的查询会归为同一类型，共享 Tuning Guide 信息。[#58306](https://github.com/StarRocks/starrocks/pull/58306)
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

- 支持指定多个分区列或表达式，实现更灵活的数据分区策略。[#52576](https://github.com/StarRocks/starrocks/issues/52576)
- 支持将 `query_rewrite_consistency` 设置为 `force_mv`，强制系统在改写查询时使用物化视图，以保证性能的稳定性（可能牺牲部分数据实时性）。[#53819](https://github.com/StarRocks/starrocks/pull/53819)

### 数据导入与导出

- 支持设置 `pause_on_json_parse_error` 为 `true`，在 JSON 解析失败时暂停 Routine Load 作业。[#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[实验性功能]** 支持包含[多个 SQL 语句的事务](https://docs.starrocks.io/zh/docs/loading/SQL_transaction/)（目前仅支持 INSERT 语句）。用户可启动、提交或撤销事务，以实现多次导入操作的 ACID 事务保障。[#53978](https://github.com/StarRocks/starrocks/issues/53978)

### 函数支持

- 引入系统变量 `lower_upper_support_utf8`（Session 级和全局级），增强大小写转换函数（如 `upper()`、`lower()`）对 UTF-8（特别是非 ASCII 字符）的支持。[#56192](https://github.com/StarRocks/starrocks/pull/56192)
- 新增函数：
  - [`field()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/field/) [#55331](https://github.com/StarRocks/starrocks/pull/55331)
  - `ds_theta_count_distinct()` [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - [`array_flatten()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_flatten/) [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - [`inet_aton()`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/inet_aton/) [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - `percentile_approx_weight()` [#57410](https://github.com/StarRocks/starrocks/pull/57410)

### 升级注意事项

- 从 StarRocks v3.5.0 起，需使用 JDK 17 或更高版本。如从 v3.4 或更早版本升级集群，需先升级 JDK，并在 FE 配置文件 **fe.conf** 中移除 `JAVA_OPTS` 中与 JDK 17 不兼容的参数（如 CMS 和 GC 参数）。此外，自 v3.5.0 起，StarRocks 不再提供特定 JDK 版本的 JVM 配置，所有 JDK 版本统一使用 `JAVA_OPTS`。

