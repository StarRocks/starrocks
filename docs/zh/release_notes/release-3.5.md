---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

升级至 v3.5 后，请勿直接将集群降级至 v3.4.0 ~ v3.4.4，否则会导致元数据不兼容。您必须降级到 v3.4.5 或更高版本以避免出现此问题。

:::

## 3.5.7

发布日期：2025年10月21日

### 功能增强

- 通过在内存竞争严重的情况下引入重试回退机制，提升了 Scan Operator 的内存统计准确性。[#63788](https://github.com/StarRocks/starrocks/pull/63788)
- 通过利用现有的分区分布，优化了物化视图桶的推理，防止了过多桶的创建。[#63367](https://github.com/StarRocks/starrocks/pull/63367)
- 修改了 Iceberg 表缓存机制，提高了一致性并减少了频繁元数据更新时的缓存失效风险。[#63388](https://github.com/StarRocks/starrocks/pull/63388)
- 在 `QueryDetail` 和 `AuditEvent` 中增加了 `querySource` 字段，以便更好地追踪查询来源，跨 API 和调度器。[#63480](https://github.com/StarRocks/starrocks/pull/63480)
- 通过在 MemTable 写入时检测到 Duplicate Key 时打印详细的上下文，增强了持久化索引诊断功能。[#63560](https://github.com/StarRocks/starrocks/pull/63560)
- 通过优化锁粒度和并发场景中的顺序，减少了物化视图操作中的锁竞争。[#63481](https://github.com/StarRocks/starrocks/pull/63481)

### 问题修复

修复了以下问题：

- 由于类型不匹配导致的物化视图重写失败。[#63659](https://github.com/StarRocks/starrocks/pull/63659)
- `regexp_extract_all` 行为不正确，且不支持 `pos=0`。[#63626](https://github.com/StarRocks/starrocks/pull/63626)
- 由于对带有复杂函数的 CASE WHEN 简化不当，导致扫描性能下降。[#63732](https://github.com/StarRocks/starrocks/pull/63732)
- 在部分更新时，从列模式切换到行模式时，DCG 数据读取不正确。[#61529](https://github.com/StarRocks/starrocks/pull/61529)
- 初始化 `ExceptionStackContext` 时可能发生死锁。[#63776](https://github.com/StarRocks/starrocks/pull/63776)
- ARM 架构机器上 Parquet 数值转换崩溃。[#63294](https://github.com/StarRocks/starrocks/pull/63294)
- 聚合中间类型使用 `ARRAY<NULL_TYPE>` 引发的问题。[#63371](https://github.com/StarRocks/starrocks/pull/63371)
- 在边缘情况下（例如，INT128_MIN）将 LARGEINT 转换为 DECIMAL128 时，溢出检测不正确导致的稳定性问题。[#63559](https://github.com/StarRocks/starrocks/pull/63559)
- 无法感知 LZ4 压缩和解压缩错误。[#63629](https://github.com/StarRocks/starrocks/pull/63629)
- 查询由 `FROM_UNIXTIME` 分区的表时，出现 `ClassCastException`。[#63684](https://github.com/StarRocks/starrocks/pull/63684)
- 当唯一有效的源副本被标记为 `DECOMMISSION` 时，平衡触发迁移后的分区无法修复。[#62942](https://github.com/StarRocks/starrocks/pull/62942)
- 使用 PREPARE 语句时，Profile 丢失 SQL 语句和 Planner Trace。[#63519](https://github.com/StarRocks/starrocks/pull/63519)
- `extract_number`，`extract_bool`和`extract_string`函数不具备异常安全性。[#63575](https://github.com/StarRocks/starrocks/pull/63575)
- 关闭的分区无法正确进行垃圾回收。[#63595](https://github.com/StarRocks/starrocks/pull/63595)
- `PREPARE`/`EXECUTE`语句的返回结果在 Profile 中显示为`omit`。[#62988](https://github.com/StarRocks/starrocks/pull/62988)
- `date_trunc` 的分区裁剪与组合谓词错误地产生了 EMPTYSET。[#63464](https://github.com/StarRocks/starrocks/pull/63464)
- `NullableColumn` 中的 CHECK 导致 Release Build 发生崩溃。[#63553](https://github.com/StarRocks/starrocks/pull/63553)

## 3.5.6

发布日期: 2025年9月22日

### 功能增强

- 当被 Decommission 的 BE 的所有 Tablet 都在回收站中时，会强制删除该 BE，以避免 Decommission 过程被这些 Tablet 阻塞。 [#62781](https://github.com/StarRocks/starrocks/pull/62781)
- 当 Vacuum 成功时会更新 Vacuum 指标。 [#62540](https://github.com/StarRocks/starrocks/pull/62540)
- 在 Fragment 实例执行状态报告中新增线程池指标，包括活动线程数、队列数量和运行线程数。 [#63067](https://github.com/StarRocks/starrocks/pull/63067)
- 在存算分离集群中支持 S3 路径风格访问，以提升与 MinIO 等 S3 兼容存储系统的兼容性。可在创建存储卷时将 `aws.s3.enable_path_style_access` 设置为 `true` 以启用。 [#62591](https://github.com/StarRocks/starrocks/pull/62591)
- 支持通过 `ALTER TABLE <table_name> AUTO_INCREMENT = 10000;` 重置 AUTO_INCREMENT 值的起始点。 [#62767](https://github.com/StarRocks/starrocks/pull/62767)
- 在 Group Provider 中支持使用 Distinguished Name (DN) 进行组匹配，以改善 LDAP/Microsoft Active Directory 环境下的用户组方案。 [#62711](https://github.com/StarRocks/starrocks/pull/62711)
- 支持 Azure Data Lake Storage Gen2 的 Azure Workload Identity 认证。 [#62754](https://github.com/StarRocks/starrocks/pull/62754)
- 在 `information_schema.loads` 视图中新增事务错误消息，以便于故障诊断。 [#61364](https://github.com/StarRocks/starrocks/pull/61364)
- 支持在包含复杂 CASE WHEN 表达式的 Scan 谓词中复用公共表达式，以减少重复计算。 [#62779](https://github.com/StarRocks/starrocks/pull/62779)
- 使用 REFRESH 权限（而不是 ALTER 权限）来执行 REFRESH 语句。 [#62636](https://github.com/StarRocks/starrocks/pull/62636)
- 默认禁用 Lake 表的低基数优化，以避免潜在问题。 [#62586](https://github.com/StarRocks/starrocks/pull/62586)
- 默认启用存算分离集群中不同 Worker 之间的 Tablet 负载均衡。 [#62661](https://github.com/StarRocks/starrocks/pull/62661)
- 支持在外连接 WHERE 谓词中复用表达式，以减少重复计算。 [#62139](https://github.com/StarRocks/starrocks/pull/62139)
- 在 FE 中新增 Clone 指标。 [#62421](https://github.com/StarRocks/starrocks/pull/62421)
- 在 BE 中新增 Clone 指标。 [#62479](https://github.com/StarRocks/starrocks/pull/62479)
- 新增 FE 配置项 `enable_statistic_cache_refresh_after_write`，默认禁用统计缓存的延迟刷新。 [#62518](https://github.com/StarRocks/starrocks/pull/62518)
- 在 SUBMIT TASK 中屏蔽凭据信息，以提高安全性。 [#62311](https://github.com/StarRocks/starrocks/pull/62311)
- Trino 方言下的 `json_extract` 返回 JSON 类型。 [#59718](https://github.com/StarRocks/starrocks/pull/59718)
- 在 `null_or_empty` 中支持 ARRAY 类型。 [#62207](https://github.com/StarRocks/starrocks/pull/62207)
- 调整 Iceberg 清单缓存的大小限制。 [#61966](https://github.com/StarRocks/starrocks/pull/61966)
- 为 Hive 新增远程文件缓存限制。 [#62288](https://github.com/StarRocks/starrocks/pull/62288)

### 问题修复

修复了以下问题：

- 由于负超时值导致时间戳比较错误，次副本会无限期挂起。 [#62805](https://github.com/StarRocks/starrocks/pull/62805)
- 当 TransactionState 为 REPLICATION 时，PublishTask 可能被阻塞。 [#61664](https://github.com/StarRocks/starrocks/pull/61664)
- 在物化视图刷新过程中，Hive 表被删除并重新创建时的修复机制错误。 [#63072](https://github.com/StarRocks/starrocks/pull/63072)
- 物化视图聚合下推改写后生成了错误的执行计划。 [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- PlanTuningGuide 在查询配置文件中生成无法识别的字符串（null explainString），导致 ANALYZE PROFILE 失败。 [#63024](https://github.com/StarRocks/starrocks/pull/63024)
- `hour_from_unixtime` 的返回类型不正确，`CAST` 的改写规则错误。 [#63006](https://github.com/StarRocks/starrocks/pull/63006)
- Iceberg 清单缓存中在数据竞争情况下出现 NPE。 [#63043](https://github.com/StarRocks/starrocks/pull/63043)
- 存算分离集群缺乏物化视图的 Colocation 支持。 [#62941](https://github.com/StarRocks/starrocks/pull/62941)
- Scan Range 部署期间 Iceberg 表扫描异常。 [#62994](https://github.com/StarRocks/starrocks/pull/62994)
- 基于视图的改写生成了错误的执行计划。 [#62918](https://github.com/StarRocks/starrocks/pull/62918)
- Compute Node 未在退出时正常关闭，导致错误和任务中断。 [#62916](https://github.com/StarRocks/starrocks/pull/62916)
- Stream Load 执行状态更新时出现 NPE。 [#62921](https://github.com/StarRocks/starrocks/pull/62921)
- 当列名与 PARTITION BY 子句中的名称大小写不一致时，统计信息出错。 [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- 当 `LEAST` 函数用作谓词时返回错误结果。 [#62826](https://github.com/StarRocks/starrocks/pull/62826)
- 在表裁剪边界 CTEConsumer 之上的 ProjectOperator 无效。 [#62914](https://github.com/StarRocks/starrocks/pull/62914)
- Clone 后副本处理冗余。 [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- 无法收集 Stream Load 配置文件。 [#62802](https://github.com/StarRocks/starrocks/pull/62802)
- 由于错误的 BE 选择导致磁盘再平衡无效。 [#62776](https://github.com/StarRocks/starrocks/pull/62776)
- 当缺少 `tablet_id` 导致 delta writer 为 null 时，LocalTabletsChannel 中可能发生 NPE 崩溃。 [#62861](https://github.com/StarRocks/starrocks/pull/62861)
- KILL ANALYZE 不生效。 [#62842](https://github.com/StarRocks/starrocks/pull/62842)
- 当 MCV 值包含单引号时，直方图统计中的 SQL 语法错误。 [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- Prometheus 指标输出格式错误。 [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- 在删除数据库后查询 `information_schema.analyze_status` 时出现 NPE。 [#62796](https://github.com/StarRocks/starrocks/pull/62796)
- CVE-2025-58056。 [#62801](https://github.com/StarRocks/starrocks/pull/62801)
- 执行 SHOW CREATE ROUTINE LOAD 时，如果未指定数据库，会被视为 null，从而返回错误结果。 [#62745](https://github.com/StarRocks/starrocks/pull/62745)
- 在 `files()` 中错误跳过 CSV 头部导致数据丢失。 [#62719](https://github.com/StarRocks/starrocks/pull/62719)
- 回放批量事务 upsert 时发生 NPE。 [#62715](https://github.com/StarRocks/starrocks/pull/62715)
- 在存算一体集群中优雅关闭期间，Publish 被错误地报告为成功。 [#62417](https://github.com/StarRocks/starrocks/pull/62417)
- 由于空指针导致异步 delta writer 崩溃。 [#62626](https://github.com/StarRocks/starrocks/pull/62626)
- 在恢复任务失败后，由于未清除物化视图版本映射，跳过物化视图刷新。 [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- 物化视图分析器中区分大小写的分区列校验引发问题。 [#62598](https://github.com/StarRocks/starrocks/pull/62598)
- 语法错误的语句生成重复的 ID。 [#62258](https://github.com/StarRocks/starrocks/pull/62258)
- CancelableAnalyzeTask 中冗余状态赋值覆盖了 StatisticsExecutor 状态。 [#62538](https://github.com/StarRocks/starrocks/pull/62538)
- 统计信息收集产生错误的错误消息。 [#62533](https://github.com/StarRocks/starrocks/pull/62533)
- 外部用户的默认最大连接数不足，导致过早限流。 [#62523](https://github.com/StarRocks/starrocks/pull/62523)
- 物化视图备份和恢复操作中可能出现 NPE。 [#62514](https://github.com/StarRocks/starrocks/pull/62514)
- `http_workers_num` 指标不正确。 [#62457](https://github.com/StarRocks/starrocks/pull/62457)
- 构建运行时过滤器时未能找到对应的执行组。 [#62465](https://github.com/StarRocks/starrocks/pull/62465)
- 在 Scan 节点中，由于简化了包含复杂函数的 CASE WHEN，导致结果冗余。 [#62505](https://github.com/StarRocks/starrocks/pull/62505)
- `gmtime` 线程不安全。 [#60483](https://github.com/StarRocks/starrocks/pull/60483)
- 获取 Hive 分区时对转义字符串处理错误。 [#59032](https://github.com/StarRocks/starrocks/pull/59032)

## 3.5.5

发布日期: 2025 年 9 月 5 日

### 功能增强

- 新增系统变量 `enable_drop_table_check_mv_dependency`（默认值：`false`）。设置为 `true` 后，若被删除的对象被下游物化视图所依赖，系统将阻止执行该 `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW` 操作。错误信息会列出依赖的物化视图，并提示查看 `sys.object_dependencies` 视图获取详细信息。[#61584](https://github.com/StarRocks/starrocks/pull/61584)
- 日志新增构建的 Linux 发行版与 CPU 架构信息，便于问题复现与排障。相关日志格式为 `... build <hash> distro <id> arch <arch>`。[#62017](https://github.com/StarRocks/starrocks/pull/62017)
- 通过在每个 Tablet 缓存持久化索引与增量列组文件大小，替代按需目录扫描，加速 BE 的 Tablet 状态上报并降低高 I/O 场景延迟。 [#61901](https://github.com/StarRocks/starrocks/pull/61901)
- 将 FE 与 BE 日志中多处高频 INFO 日志降级为 VLOG，并对任务提交日志做聚合，显著减少存储相关冗余日志与高负载下的日志量。[#62121](https://github.com/StarRocks/starrocks/pull/62121)
- 通过 `information_schema` 数据库查询 External Catalog 元数据时，通过将表过滤下推到调用 `getTable` 之前，加速此类查询，避免逐表 RPC，并提升性能。[#62404](https://github.com/StarRocks/starrocks/pull/62404)

### 问题修复

修复了以下问题：

- 在 Plan 阶段获取分区级列统计信息时，因缺失而产生 NullPointerException 的问题。[#61935](https://github.com/StarRocks/starrocks/pull/61935)
- Parquet 写出在 NULL 数组非零大小场景下的问题，并纠正 `SPLIT(NULL, …)` 行为保持输出为 NULL，避免数据损坏与运行时错误。[#61999](https://github.com/StarRocks/starrocks/pull/61999)
- 创建使用 `CASE WHEN` 表达式的物化视图时，因 VARCHAR 类型返回不兼容导致的失败（修复后，系统确保刷新前后一致性，新增 FE 配置 `transform_type_prefer_string_for_varchar` 以优先用 STRING，避免长度不匹配）。[#61996](https://github.com/StarRocks/starrocks/pull/61996)
- 当 `enable_rbo_table_prune` 为 `false` 时，在表裁剪时系统无法在 memo 之外计算嵌套 CTE 统计信息的问题。[#62070](https://github.com/StarRocks/starrocks/pull/62070)
- Audit Log 中，INSERT INTO SELECT 语句的 Scan Rows 结果不准确。[#61381](https://github.com/StarRocks/starrocks/pull/61381)
- 初始化阶段出现 ExceptionInInitializerError/NullPointerException 问题，导致启用 Query Queue v2 时 FE 重启失败。 [#62161](https://github.com/StarRocks/starrocks/pull/62161)
- BE 在 `LakePersistentIndex` 初始化失败时因清理 `_memtable` 而崩溃。[#62279](https://github.com/StarRocks/starrocks/pull/62279)
- 物化视图刷新时创建者的所有角色未被激活导致的权限问题（修复后，新增 FE 配置 `mv_use_creator_based_authorization`，设置为 `false` 时系统以 root 身份刷新物化视图，用于适配 LDAP 验证方式的集群）。[#62396](https://github.com/StarRocks/starrocks/pull/62396)
- 因 List 分区表名仅大小写不同而导致的物化视图刷新失败（修复后，对分区名实施大小写不敏感的唯一性校验，与 OLAP 表语义一致）。[#62389](https://github.com/StarRocks/starrocks/pull/62389)

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
