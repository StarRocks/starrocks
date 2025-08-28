---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

升级至 v3.5 后，请勿直接将集群降级至 v3.4.0 ~ v3.4.4，否则会导致元数据不兼容。您必须降级到 v3.4.5 或更高版本以避免出现此问题。

:::

## 3.5.4

发布日期: 2025年8月22日

### 功能增强

- 增加日志以明确 Tablet 无法修复的原因。 [#61959](https://github.com/StarRocks/starrocks/pull/61959)
- 优化日志中的 DROP PARTITION 信息。 [#61787](https://github.com/StarRocks/starrocks/pull/61787)
- 为统计信息未知的表分配一个较大但可配置的行数，用于统计估算。 [#61332](https://github.com/StarRocks/starrocks/pull/61332)
- 增加基于标签位置的均衡统计。 [#61905](https://github.com/StarRocks/starrocks/pull/61905)
- 增加 Colocate Group 均衡统计以提升集群监控能力。 [#61736](https://github.com/StarRocks/starrocks/pull/61736)
- 当健康副本数超过默认副本数时，跳过 Publish 等待阶段。 [#61820](https://github.com/StarRocks/starrocks/pull/61820)
- 在 Tablet 报告中加入 Tablet 信息的收集时间。 [#61643](https://github.com/StarRocks/starrocks/pull/61643)
- 支持写入带标签的 Starlet 文件。 [#61605](https://github.com/StarRocks/starrocks/pull/61605)
- 支持通过 SHOW PROC 查看集群均衡统计。 [#61578](https://github.com/StarRocks/starrocks/pull/61578)
- 升级 librdkafka 至 2.11.0 以支持 Kafka 4.0，并移除废弃配置。 [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- 在 Stream Load 事务接口中新增 `prepared_timeout` 配置。 [#61539](https://github.com/StarRocks/starrocks/pull/61539)
- 升级 StarOS 至 v3.5-rc3。 [#61685](https://github.com/StarRocks/starrocks/pull/61685)

### 问题修复

修复了以下问题：

- 随机分布表的 Dict 版本错误。 [#61933](https://github.com/StarRocks/starrocks/pull/61933)
- 在 Context Condition 中的 Query Context 错误。 [#61929](https://github.com/StarRocks/starrocks/pull/61929)
- ALTER 操作中因 Shadow Tablet 的同步 Publish 导致 Publish 失败。 [#61887](https://github.com/StarRocks/starrocks/pull/61887)
- 修复 CVE-2025-55163 漏洞。 [#62041](https://github.com/StarRocks/starrocks/pull/62041)
- 从 Apache Kafka 实时导入数据时发生内存泄漏。 [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Lake Persistent Index 中 Rebuild 文件数量统计错误。 [#61859](https://github.com/StarRocks/starrocks/pull/61859)
- 在生成表达式列上收集统计信息导致跨库查询失败。 [#61829](https://github.com/StarRocks/starrocks/pull/61829)
- Query Cache 在存算一体集群中不一致，导致结果不一致。 [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- CatalogRecycleBin 保留已删除分区信息导致内存占用过高。 [#61582](https://github.com/StarRocks/starrocks/pull/61582)
- SQL Server JDBC 连接在超时超过 65,535 毫秒时失败。 [#61719](https://github.com/StarRocks/starrocks/pull/61719)
- 安全集成未能加密密码，导致敏感信息泄露。 [#60666](https://github.com/StarRocks/starrocks/pull/60666)
- Iceberg 分区列上的 `MIN()` 和 `MAX()` 异常返回 NULL。 [#61858](https://github.com/StarRocks/starrocks/pull/61858)
- 含不可下推子字段的 Join 谓词被错误改写。 [#61868](https://github.com/StarRocks/starrocks/pull/61868)
- 取消 QueryContext 可能导致 use-after-free。 [#61897](https://github.com/StarRocks/starrocks/pull/61897)
- CBO 表裁剪逻辑错误忽略其他谓词。 [#61881](https://github.com/StarRocks/starrocks/pull/61881)
- `COLUMN_UPSERT_MODE` 部分更新将自增列覆盖为 0。 [#61341](https://github.com/StarRocks/starrocks/pull/61341)
- JDBC TIME 类型转换使用错误的时区偏移，导致时间值错误。 [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- Routine Load 作业未序列化 `max_filter_ratio`。 [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Stream Load 的 `now(precision)` 函数存在精度参数丢失。 [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- 取消查询可能导致“query id not found”错误。 [#61667](https://github.com/StarRocks/starrocks/pull/61667)
- LDAP 认证在查询过程中可能漏报 PartialResultException 导致查询结果不完整。 [#60667](https://github.com/StarRocks/starrocks/pull/60667)
- 查询条件包含 DATETIME 时，Paimon Timestamp 的时区转换错误。 [#60473](https://github.com/StarRocks/starrocks/pull/60473)

## 3.5.3

发布日期： 2025 年 8 月 11 日

### 功能增强

- Lake Compaction 增加 Segment 写入耗时统计信息。[#60891](https://github.com/StarRocks/starrocks/pull/60891)
- 禁用 Data Cache 写入的 inline 模式以避免性能下降。[#60530](https://github.com/StarRocks/starrocks/pull/60530)
- Iceberg 元数据扫描支持共享文件 I/O。[#61012](https://github.com/StarRocks/starrocks/pull/61012)
- 支持终止所有 PENDING 状态的 ANALYZE 任务。[#61118](https://github.com/StarRocks/starrocks/pull/61118)
- CTE 节点过多时强制复用以避免优化耗时过长。[#60983](https://github.com/StarRocks/starrocks/pull/60983)
- 集群均衡结果中新增 `BALANCE` 类型。[#61081](https://github.com/StarRocks/starrocks/pull/61081)
- 优化含外部表的物化视图改写。[#61037](https://github.com/StarRocks/starrocks/pull/61037)
- 系统变量 `enable_materialized_view_agg_pushdown_rewrite` 默认值修改为 `true`，即默认为物化视图查询改写启用聚合函数下推。 [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- 优化分区统计锁竞争。[#61041](https://github.com/StarRocks/starrocks/pull/61041)

### 问题修复

修复了以下问题：

- 列裁剪后 Chunk 列大小不一致。[#61271](https://github.com/StarRocks/starrocks/pull/61271)
- 非异步执行分区统计加载可能造成死锁。[#61300](https://github.com/StarRocks/starrocks/pull/61300)
-  `array_map` 处理常量数组列时崩溃。[#61309](https://github.com/StarRocks/starrocks/pull/61309)
- 将自增列设为 NULL 时，系统错误拒绝同一 Chunk 内的有效数据。[#61255](https://github.com/StarRocks/starrocks/pull/61255)
- JDBC 实际连接数可能超过 `jdbc_connection_pool_size` 限制。[#61038](https://github.com/StarRocks/starrocks/pull/61038)
- FQDN 模式下未使用 IP 地址作为缓存键。[#61203](https://github.com/StarRocks/starrocks/pull/61203)
- 数组比较过程中数组列克隆错误。[#61036](https://github.com/StarRocks/starrocks/pull/61036)
- 部署序列化线程池阻塞导致查询性能下降。[#61150](https://github.com/StarRocks/starrocks/pull/61150)
- 心跳重试计数器重置后 OK 响应未同步。[#61249](https://github.com/StarRocks/starrocks/pull/61249)
- `hour_from_unixtime` 函数结果错误。[#61206](https://github.com/StarRocks/starrocks/pull/61206)
- ALTER TABLE 任务与分区创建冲突。[#60890](https://github.com/StarRocks/starrocks/pull/60890)
- 从 v3.3 升级至 v3.4 或更新版本后缓存不生效。[#60973](https://github.com/StarRocks/starrocks/pull/60973)
- 向量索引指标 `hit_count` 未设置。[#61102](https://github.com/StarRocks/starrocks/pull/61102)
- Stream Load 事务导入无法找到协调节点。[#60154](https://github.com/StarRocks/starrocks/pull/60154)
- BE 在加载 OOM 分区时崩溃。[#60778](https://github.com/StarRocks/starrocks/pull/60778)
- 手动创建的分区在执行 INSERT OVERWRITE 时失败。[#60858](https://github.com/StarRocks/starrocks/pull/60858)
- 当分区的值不同但名称在不区分大小写的情况下相同时，分区创建失败。 [#60909](https://github.com/StarRocks/starrocks/pull/60909)
- 不支持 PostgreSQL UUID 类型。[#61021](https://github.com/StarRocks/starrocks/pull/61021)
- 通过 `FILES()` 导入 Parquet 数据时列名大小写敏感的问题。[#61059](https://github.com/StarRocks/starrocks/pull/61059)

## 3.5.2

发布日期： 2025 年 7 月 18 日

### 功能增强

- 为数组列实现了 NDV 统计信息采集，以提高查询计划的准确性。[#60623](https://github.com/StarRocks/starrocks/pull/60623)
- 禁止存算分离集群中 Colocate 表的副本均衡以及 Tablet 调度，减少无用的日志输出。[#60737](https://github.com/StarRocks/starrocks/pull/60737)
- 优化 Catalog 访问机制，在 FE 启动时，系统默延迟异步访问外部数据源，避免外部服务不可用导致 FE 启动卡死。 [#60614](https://github.com/StarRocks/starrocks/pull/60614)
- 新增 Session 变量 `enable_predicate_expr_reuse` 以控制是否开启谓词下推。 [#60603](https://github.com/StarRocks/starrocks/pull/60603)
- 支持获取 Kafka Partition 信息失败后重试。[#60513](https://github.com/StarRocks/starrocks/pull/60513)
- 移除物化视图和基表的分区列必须一对一匹配的限制。[#60565](https://github.com/StarRocks/starrocks/pull/60565)
- 支持构建 Runtime In-Filter 增强聚合操作，通过在聚合阶段过滤数据来优化查询性能。[#59288](https://github.com/StarRocks/starrocks/pull/59288)

### 问题修复

修复了以下问题：

- 低基数优化导致多列 COUNT DISTINCT 查询 Crash。[ #60664](https://github.com/StarRocks/starrocks/pull/60664)
- 当存在多个同名全局用户定义函数（UDF）时，系统错误匹配这些函数。[#60550](https://github.com/StarRocks/starrocks/pull/60550)
- Stream Load 导入时的空指针问题。[#60755](https://github.com/StarRocks/starrocks/pull/60755)
- 使用集群快照进行恢复时 FE 启动报空指针问题。[#60604](https://github.com/StarRocks/starrocks/pull/60604)
- 在处理乱序值列的短路查询时因读取列模式不匹配导致的 BE 崩溃。[#60466](https://github.com/StarRocks/starrocks/pull/60466)
- 在 SUBMIT TASK 语句中通过 PROPERTIES 设置 Session 变量不生效。[#60584](https://github.com/StarRocks/starrocks/pull/60584)
- SELECT min/max 查询在部分条件下查询结果不正确。[#60601](https://github.com/StarRocks/starrocks/pull/60601)
- 当谓词左侧为函数时，系统调用错误的分片裁剪逻辑，导致查询命中错误的 bucket 进而导致查询结果不正确。[#60467](https://github.com/StarRocks/starrocks/pull/60467)
- 通过 Arrow Flight SQL 协议查询不存在的 `query_id` 导致系统崩溃。 [#60497](https://github.com/StarRocks/starrocks/pull/60497)

### 行为变更

- `lake_compaction_allow_partial_success`  默认值变更为 `true`。Compaction 操作在部分成功后可以标记为成功，避免阻塞后续的 Compaction 任务。 [#60643](https://github.com/StarRocks/starrocks/pull/60643)

## 3.5.1

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

## 3.5.0

发布日期：2025 年 6 月 13 日

### 升级注意事项

- 从 StarRocks v3.5.0 起，需使用 JDK 17 或更高版本。
  - 如从 v3.4 或更早版本升级集群，需先升级 JDK，并在 FE 配置文件 **fe.conf** 中移除 `JAVA_OPTS` 中与 JDK 17 不兼容的参数（如 CMS 和 GC 参数）。推荐直接使用 v3.5 版本的 `JAVA_OPTS` 默认值。
  - 对于使用 External Catalog 的集群，需要在 BE 配置文件 **be.conf** 的配置项 `JAVA_OPTS` 中添加 `--add-opens=java.base/java.util=ALL-UNNAMED`。
  - 对于使用 Java UDF 的集群，需要在 BE 配置文件 **be.conf** 的配置项 `JAVA_OPTS` 中添加 `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`。
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
