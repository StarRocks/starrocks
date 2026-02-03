---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**降级说明**

- 升级至 v4.0 后，请勿直接将集群降级至 v3.5.0 和 v3.5.1，否则会导致元数据不兼容和 FE Crash。您必须降级到 v3.5.2 或更高版本以避免出现此问题。
- 在将集群从 v4.0.2 降级到 v4.0.1、v4.0.0 以及 v3.5.2~v3.5.10 之前，请先执行以下语句：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  重新升级至 v4.0.2及以上版本后，请执行以下语句：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.5

发布日期：2026 年 2 月 3 日

### 功能优化

- 将 Paimon 版本升级至 1.3.1。[#67098](https://github.com/StarRocks/starrocks/pull/67098)
- 恢复 DP 统计信息估算中缺失的优化，减少冗余计算。[#67852](https://github.com/StarRocks/starrocks/pull/67852)
- 改进 DP Join 重排序中的剪枝逻辑，更早跳过代价高昂的候选执行计划。[#67828](https://github.com/StarRocks/starrocks/pull/67828)
- 优化 JoinReorderDP 的分区枚举逻辑，减少对象分配，并新增原子数量上限（≤ 62）。[#67643](https://github.com/StarRocks/starrocks/pull/67643)
- 优化 DP Join 重排序的剪枝逻辑，并在 BitSet 中增加检查以减少流式操作的开销。[#67644](https://github.com/StarRocks/starrocks/pull/67644)
- 在 DP 统计信息估算过程中跳过谓词列的统计信息收集，以降低 CPU 开销。[#67663](https://github.com/StarRocks/starrocks/pull/67663)
- 优化相关 Join 的行数估算，避免重复构建 `Statistics` 对象。[#67773](https://github.com/StarRocks/starrocks/pull/67773)
- 减少 `Statistics.getUsedColumns` 中的内存分配。[#67786](https://github.com/StarRocks/starrocks/pull/67786)
- 当仅更新行数时，避免冗余复制 `Statistics` 映射。[#67777](https://github.com/StarRocks/starrocks/pull/67777)
- 当查询中不存在聚合时，跳过聚合下推逻辑以减少开销。[#67603](https://github.com/StarRocks/starrocks/pull/67603)
- 改进窗口函数中的 COUNT DISTINCT，新增对融合多 DISTINCT 聚合的支持，并优化 CTE 生成。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Trino 方言中支持 `map_agg` 函数。[#66673](https://github.com/StarRocks/starrocks/pull/66673)
- 在物理规划阶段支持批量获取 LakeTablet 位置信息，以减少存算分离集群中的 RPC 调用。[#67325](https://github.com/StarRocks/starrocks/pull/67325)
- 在存算一体集群中为 Publish Version 事务新增线程池，以提升并发能力。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- 优化 LocalMetastore 的锁粒度，将数据库级锁替换为表级锁。[#67658](https://github.com/StarRocks/starrocks/pull/67658)
- 重构 MergeCommitTask 的生命周期管理，并新增任务取消支持。[#67425](https://github.com/StarRocks/starrocks/pull/67425)
- 支持为自动化集群快照配置执行间隔。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- 在 MemTrackerManager 中自动清理未使用的 `mem_pool` 条目。[#67347](https://github.com/StarRocks/starrocks/pull/67347)
- 在仓库空闲检查中忽略 `information_schema` 查询。[#67958](https://github.com/StarRocks/starrocks/pull/67958)
- 支持根据数据分布动态为 Iceberg 表写入启用全局 Shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- 为 Connector Sink 模块新增 Profile 指标。[#67761](https://github.com/StarRocks/starrocks/pull/67761)
- 改进 Profile 中加载（load）溢写指标的采集和展示，区分本地 I/O 与远端 I/O。[#67527](https://github.com/StarRocks/starrocks/pull/67527)
- 将 Async-Profiler 的日志级别调整为 Error，避免重复打印警告日志。[#67297](https://github.com/StarRocks/starrocks/pull/67297)
- 在 BE 关闭时通知 Starlet 向 StarMgr 上报 SHUTDOWN 状态。[#67461](https://github.com/StarRocks/starrocks/pull/67461)

### 问题修复

已修复以下问题：

- 不支持包含连字符（`-`）的合法简单路径。[#67988](https://github.com/StarRocks/starrocks/pull/67988)
- 当聚合下推发生在包含 JSON 类型的分组键上时出现运行时错误。[#68142](https://github.com/StarRocks/starrocks/pull/68142)
- JSON 路径重写规则错误地裁剪了分区谓词中引用的分区列。[#67986](https://github.com/StarRocks/starrocks/pull/67986)
- 使用统计信息重写简单聚合时出现类型不匹配问题。[#67829](https://github.com/StarRocks/starrocks/pull/67829)
- 分区 Join 中可能存在堆缓冲区溢出风险。[#67435](https://github.com/StarRocks/starrocks/pull/67435)
- 在下推复杂表达式时引入了重复的 `slot_ids`。[#67477](https://github.com/StarRocks/starrocks/pull/67477)
- 由于缺少前置条件检查，ExecutionDAG 中的 Fragment 连接可能出现除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- 在单 BE 场景下，Fragment 并行准备可能导致潜在问题。[#67798](https://github.com/StarRocks/starrocks/pull/67798)
- RawValuesSourceOperator 缺少 `set_finished` 方法，导致算子异常终止。[#67609](https://github.com/StarRocks/starrocks/pull/67609)
- 列聚合器中不支持 DECIMAL256 类型（精度 > 38）导致 BE 崩溃。[#68134](https://github.com/StarRocks/starrocks/pull/68134)
- 存算分离集群在 DELETE 操作中未通过请求携带 `schema_key`，导致不支持 Fast Schema Evolution v2。[#67456](https://github.com/StarRocks/starrocks/pull/67456)
- 存算分离集群在同步物化视图和传统 Schema 变更中不支持 Fast Schema Evolution v2。[#67443](https://github.com/StarRocks/starrocks/pull/67443)
- 在 FE 降级且禁用文件打包时，Vacuum 可能误删文件。[#67849](https://github.com/StarRocks/starrocks/pull/67849)
- MySQLReadListener 中优雅退出处理不正确。[#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

发布日期： 2026 年 1 月 16 日

### 功能优化

- 支持算子（Operator）和驱动（Driver）的并行 Prepare，以及单节点批量 Fragment 部署，以提升查询调度性能。[#63956](https://github.com/StarRocks/starrocks/pull/63956)
- 对大分区表的 `deltaRows` 计算引入惰性求值（Lazy Evaluation）机制，提升性能。[#66381](https://github.com/StarRocks/starrocks/pull/66381)
- 优化 Flat JSON 处理逻辑，采用顺序迭代并改进路径派生方式。[#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- 支持更早释放 Spill Operator 的内存，以降低 Group Execution 场景下的内存占用。[#66669](https://github.com/StarRocks/starrocks/pull/66669)
- 优化逻辑以减少字符串比较开销。[#66570](https://github.com/StarRocks/starrocks/pull/66570)
- 增强 `GroupByCountDistinctDataSkewEliminateRule` 和 `SkewJoinOptimizeRule` 中的数据倾斜检测能力，支持基于直方图和 NULL 的策略。[#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- 在 Chunk 中通过 Move 语义增强 Column 所有权管理，减少 Copy-On-Write 的开销。[#66805](https://github.com/StarRocks/starrocks/pull/66805)
- 针对存算分离集群，新增 FE `TableSchemaService`，并更新 `MetaScanNode`，支持 Fast Schema Evolution v2 的 Schema 获取方式。[#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- 支持多 Warehouse 的 Backend 资源统计及并行度（DOP）计算，提升资源隔离能力。[#66632](https://github.com/StarRocks/starrocks/pull/66632)
- 支持通过 StarRocks 会话变量 `connector_huge_file_size` 配置 Iceberg 的 Split 大小。[#67044](https://github.com/StarRocks/starrocks/pull/67044)
- `QueryDumpDeserializer` 支持标签格式（Label-formatted）的统计信息。[#66656](https://github.com/StarRocks/starrocks/pull/66656)
- 新增 FE 配置项 `lake_enable_fullvacuum`（默认值：`false`），用于在存算分离集群中禁用 Full Vacuum。[#63859](https://github.com/StarRocks/starrocks/pull/63859)
- 将 lz4 依赖升级至 v1.10.0。[#67045](https://github.com/StarRocks/starrocks/pull/67045)
- 当行数为 0 时，为基于采样的基数估计增加回退逻辑。[#65599](https://github.com/StarRocks/starrocks/pull/65599)
- 验证 `array_sort` 中 Lambda Comparator 的 Strict Weak Ordering 属性。[#66951](https://github.com/StarRocks/starrocks/pull/66951)
- 优化外表（Delta / Hive / Hudi / Iceberg）元数据获取失败时的错误信息，展示根因。[#66916](https://github.com/StarRocks/starrocks/pull/66916)
- 支持在查询超时时 Dump Pipeline 状态，并在 FE 中以 `TIMEOUT` 状态取消查询。[#66540](https://github.com/StarRocks/starrocks/pull/66540)
- SQL 黑名单错误信息中显示命中的规则索引。[#66618](https://github.com/StarRocks/starrocks/pull/66618)
- 在 `EXPLAIN` 输出中为列统计信息添加标签。[#65899](https://github.com/StarRocks/starrocks/pull/65899)
- 过滤正常查询结束（如达到 LIMIT）时的 “cancel fragment” 日志。[#66506](https://github.com/StarRocks/starrocks/pull/66506)
- 在 Warehouse 挂起时，减少 Backend 心跳失败日志。[#66733](https://github.com/StarRocks/starrocks/pull/66733)
- `ALTER STORAGE VOLUME` 语法支持 `IF EXISTS`。[#66691](https://github.com/StarRocks/starrocks/pull/66691)

### 问题修复

修复了以下问题：

- 在 Low Cardinality 优化下，由于缺少 `withLocalShuffle`，导致 `DISTINCT` 和 `GROUP BY` 结果错误。[#66768](https://github.com/StarRocks/starrocks/pull/66768)
- 带 Lambda 表达式的 JSON v2 函数在重写阶段报错。[#66550](https://github.com/StarRocks/starrocks/pull/66550)
- 相关子查询中 Null-aware Left Anti Join 错误地应用了 Partition Join。[#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Meta Scan 重写规则中的行数计算错误。[#66852](https://github.com/StarRocks/starrocks/pull/66852)
- 基于统计信息重写 Meta Scan 时，Union Node 的 Nullable 属性不一致。[#67051](https://github.com/StarRocks/starrocks/pull/67051)
- 当 Ranking 窗口函数缺少 `PARTITION BY` 和 `ORDER BY` 时，优化逻辑导致 BE 崩溃。[#67094](https://github.com/StarRocks/starrocks/pull/67094)
- Group Execution Join 与窗口函数组合使用时可能产生错误结果。[#66441](https://github.com/StarRocks/starrocks/pull/66441)
- 特定过滤条件下，`PartitionColumnMinMaxRewriteRule` 产生错误结果。[#66356](https://github.com/StarRocks/starrocks/pull/66356)
- 聚合后 Union 操作中 Nullable 属性推导错误。[#65429](https://github.com/StarRocks/starrocks/pull/65429)
- `percentile_approx_weighted` 在处理压缩参数时发生崩溃。[#64838](https://github.com/StarRocks/starrocks/pull/64838)
- 大字符串编码场景下 Spill 导致崩溃。[#61495](https://github.com/StarRocks/starrocks/pull/61495)
- 下推本地 TopN 时，多次调用 `set_collector` 触发崩溃。[#66199](https://github.com/StarRocks/starrocks/pull/66199)
- LowCardinality 重写逻辑中的依赖推导错误。[#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset 提交失败时发生 Rowset ID 泄漏。[#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache 锁竞争问题。[#66637](https://github.com/StarRocks/starrocks/pull/66637)
- 在使用列模式部分更新并带条件更新时，导入失败。[#66139](https://github.com/StarRocks/starrocks/pull/66139)
- ALTER 操作期间 Tablet 被删除导致并发导入失败。[#65396](https://github.com/StarRocks/starrocks/pull/65396)
- RocksDB 迭代超时导致 Tablet 元数据加载失败。[#65146](https://github.com/StarRocks/starrocks/pull/65146)
- 存算分离集群中，建表和 Schema Change 时压缩配置未生效。[#65673](https://github.com/StarRocks/starrocks/pull/65673)
- 升级过程中 Delete Vector 的 CRC32 兼容性问题。[#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Clone 任务失败后文件清理阶段的状态检查逻辑错误。[#65709](https://github.com/StarRocks/starrocks/pull/65709)
- `INSERT OVERWRITE` 后统计信息收集逻辑异常。[#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- FE 重启后外键约束丢失。[#66474](https://github.com/StarRocks/starrocks/pull/66474)
- 删除 Warehouse 后元数据获取失败。[#66436](https://github.com/StarRocks/starrocks/pull/66436)
- 高选择性过滤条件下，审计日志扫描统计信息不准确。[#66280](https://github.com/StarRocks/starrocks/pull/66280)
- 查询错误率指标的计算逻辑不正确。[#65891](https://github.com/StarRocks/starrocks/pull/65891)
- 任务退出时可能发生 MySQL 连接泄漏。[#66829](https://github.com/StarRocks/starrocks/pull/66829)
- SIGSEGV 崩溃后 BE 状态未能及时更新。[#66212](https://github.com/StarRocks/starrocks/pull/66212)
- LDAP 用户登录过程中出现 NPE。[#65843](https://github.com/StarRocks/starrocks/pull/65843)
- HTTP SQL 请求切换用户时错误日志不准确。[#65371](https://github.com/StarRocks/starrocks/pull/65371)
- TCP 连接复用过程中发生 HTTP 上下文泄漏。[#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Follower 转发的查询在 Profile 日志中缺少 QueryDetail。[#64395](https://github.com/StarRocks/starrocks/pull/64395)
- 审计日志中缺少 Prepare / Execute 的详细信息。[#65448](https://github.com/StarRocks/starrocks/pull/65448)
- HyperLogLog 内存分配失败导致崩溃。[#66747](https://github.com/StarRocks/starrocks/pull/66747)
- `trim` 函数的内存预留逻辑存在问题。[#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- 修复 CVE-2025-66566 和 CVE-2025-12183。[#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053)
- Exec Group Driver 提交过程中的竞态条件问题。[#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Pipeline 倒计时中的 use-after-free 风险。[#65940](https://github.com/StarRocks/starrocks/pull/65940)
- `MemoryScratchSinkOperator` 在队列关闭时发生阻塞。[#66041](https://github.com/StarRocks/starrocks/pull/66041)
- 文件系统缓存 Key 冲突问题。[#65823](https://github.com/StarRocks/starrocks/pull/65823)
- `SHOW PROC '/compactions'` 中子任务数量显示错误。[#67209](https://github.com/StarRocks/starrocks/pull/67209)
- Query Profile API 未返回统一的 JSON 格式。[#67077](https://github.com/StarRocks/starrocks/pull/67077)
- `getTable` 异常处理不当，影响物化视图检查。[#67224](https://github.com/StarRocks/starrocks/pull/67224)
- 原生表与云原生表的 `DESC` 语句中 `Extra` 列输出不一致。[#67238](https://github.com/StarRocks/starrocks/pull/67238)
- 单节点部署场景下的竞态条件问题。[#67215](https://github.com/StarRocks/starrocks/pull/67215)
- 第三方库日志泄漏问题。[#67129](https://github.com/StarRocks/starrocks/pull/67129)
- REST Catalog 认证逻辑错误导致认证失败。[#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

发布日期：2025 年 12 月 25 日

### 功能优化

- 支持对 STRUCT 数据类型使用 `ORDER BY` 子句。[#66035](https://github.com/StarRocks/starrocks/pull/66035)
- 支持创建带属性的 Iceberg 视图，并在 `SHOW CREATE VIEW` 的输出中显示这些属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 支持通过 `ALTER TABLE ADD/DROP PARTITION COLUMN` 修改 Iceberg 表的分区 Spec。[#65922](https://github.com/StarRocks/starrocks/pull/65922)
- 支持在带窗口框架（例如 `ORDER BY` / `PARTITION BY`）的窗口函数中使用 `COUNT/SUM/AVG(DISTINCT)` 聚合，并提供优化选项。[#65815](https://github.com/StarRocks/starrocks/pull/65815)
- 对 CSV 解析进行性能优化，对单字符分隔符使用 `memchr`。[#63715](https://github.com/StarRocks/starrocks/pull/63715)
- 新增优化器规则，将 Partial TopN 下推到预聚合（Pre-Aggregation）阶段，以减少网络开销。[#61497](https://github.com/StarRocks/starrocks/pull/61497)
- 增强 Data Cache 监控能力：
  - 新增内存/磁盘配额及使用量相关指标。[#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - 在 `api/datacache/stat` HTTP 接口中新增 Page Cache 统计信息。[#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - 新增内表的命中率统计。[#66198](https://github.com/StarRocks/starrocks/pull/66198)
- 优化 Sort 和 Aggregation 算子，在 OOM 场景下支持更快速地释放内存。[#66157](https://github.com/StarRocks/starrocks/pull/66157)
- 在 FE 中新增 `TableSchemaService`，用于存算分离集群中 CN 按需获取指定表结构。[#66142](https://github.com/StarRocks/starrocks/pull/66142)
- 优化快速 Schema Evolution，在所有依赖的导入作业完成前保留历史 Schema。[#65799](https://github.com/StarRocks/starrocks/pull/65799)
- 增强 `filterPartitionsByTTL`，正确处理 NULL 分区值，避免误过滤所有分区。[#65923](https://github.com/StarRocks/starrocks/pull/65923)
- 优化 `FusedMultiDistinctState`，在重置时清理关联的 MemPool。[#66073](https://github.com/StarRocks/starrocks/pull/66073)
- 在 Iceberg REST Catalog 中，将 `ICEBERG_CATALOG_SECURITY` 属性检查改为大小写不敏感。[#66028](https://github.com/StarRocks/starrocks/pull/66028)
- 在存算分离集群中新增 HTTP 接口 `GET /service_id`，用于获取 StarOS Service ID。[#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Kafka 消费者配置中使用 `bootstrap.servers` 替代已废弃的 `metadata.broker.list`。[#65437](https://github.com/StarRocks/starrocks/pull/65437)
- 新增 FE 配置项 `lake_enable_fullvacuum`（默认值：false），用于禁用 Full Vacuum Daemon。[#66685](https://github.com/StarRocks/starrocks/pull/66685)
- 将 lz4 库升级至 v1.10.0。[#67080](https://github.com/StarRocks/starrocks/pull/67080)

### 问题修复

修复了以下问题：

- `latest_cached_tablet_metadata` 在批量 Publish 过程中可能导致版本被错误跳过。[#66558](https://github.com/StarRocks/starrocks/pull/66558)
- 在存算一体集群中，`CatalogRecycleBin` 中的 `ClusterSnapshot` 相对检查可能引发的问题。[#66501](https://github.com/StarRocks/starrocks/pull/66501)
- 在 Spill 过程中向 Iceberg 表写入复杂数据类型（ARRAY / MAP / STRUCT）时导致 BE 崩溃。[#66209](https://github.com/StarRocks/starrocks/pull/66209)
- 当 writer 初始化或首次写入失败时，Connector Chunk Sink 可能发生卡死。[#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink 中，`PartitionChunkWriter` 初始化失败后，在关闭阶段触发空指针异常的问题。[#66097](https://github.com/StarRocks/starrocks/pull/66097)
- 设置不存在的系统变量时未报错而是静默成功的问题。[#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Data Cache 损坏时，Bundle 元数据解析失败的问题。[#66021](https://github.com/StarRocks/starrocks/pull/66021)
- 当结果为空时，MetaScan 在 count 列上返回 NULL 而非 0。[#66010](https://github.com/StarRocks/starrocks/pull/66010)
- 对于早期版本创建的资源组，`SHOW VERBOSE RESOURCE GROUP ALL` 显示 NULL 而非 `default_mem_pool`。[#65982](https://github.com/StarRocks/starrocks/pull/65982)
- 禁用 `flat_json` 表配置后，查询执行过程中抛出 `RuntimeException`。[#65921](https://github.com/StarRocks/starrocks/pull/65921)
- 在存算分离集群中，Schema Change 后将 `min` / `max` 统计信息重写到 MetaScan 时导致的类型不匹配问题。[#65911](https://github.com/StarRocks/starrocks/pull/65911)
- 在缺少 `PARTITION BY` 和 `ORDER BY` 时，排名窗口优化导致 BE 崩溃的问题。[#67093](https://github.com/StarRocks/starrocks/pull/67093)
- 合并运行时过滤器时，`can_use_bf` 判断不正确，可能导致错误结果或崩溃。[#67062](https://github.com/StarRocks/starrocks/pull/67062)
- 将运行时 bitset 过滤器下推到嵌套 OR 谓词中导致结果不正确的问题。[#67061](https://github.com/StarRocks/starrocks/pull/67061)
- DeltaWriter 完成后仍执行写入或 flush 操作，可能导致数据竞争和数据丢失的问题。[#66966](https://github.com/StarRocks/starrocks/pull/66966)
- 将简单聚合重写为 MetaScan 时，由于 nullable 属性不匹配导致的执行错误。[#67068](https://github.com/StarRocks/starrocks/pull/67068)
- MetaScan 重写规则中行数计算不正确的问题。[#66967](https://github.com/StarRocks/starrocks/pull/66967)
- 由于 Tablet 元数据缓存不一致，批量 Publish 过程中版本可能被错误跳过。[#66575](https://github.com/StarRocks/starrocks/pull/66575)
- HyperLogLog 操作中，内存分配失败时错误处理不当的问题。[#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

发布日期：2025 年 12 月 4 日

### 新增功能

- 新增资源组属性 `mem_pool`，允许多个资源组共享同一个内存池，并为该内存池设置联合内存限制。该功能向后兼容。如果未指定 `mem_pool`，将使用 `default_mem_pool`。[#64112](https://github.com/StarRocks/starrocks/pull/64112)

### 功能优化

- 在启用 File Bundling 后减少 Vacuum 过程中的远程存储访问量。[#65793](https://github.com/StarRocks/starrocks/pull/65793)
- File Bundling 功能会缓存最新的 Tablet 元数据。[#65640](https://github.com/StarRocks/starrocks/pull/65640)
- 提升长字符串场景下的安全性与稳定性。[#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)
- 优化 `SplitTopNAggregateRule` 的逻辑，避免性能回退。[#65478](https://github.com/StarRocks/starrocks/pull/65478)
- 将 Iceberg/DeltaLake 的表统计信息收集策略应用到其他外部数据源，以避免在单表场景下收集不必要的统计信息。[#65430](https://github.com/StarRocks/starrocks/pull/65430)
- 在 Data Cache HTTP API `api/datacache/app_stat` 中新增 Page Cache 相关指标。[#65341](https://github.com/StarRocks/starrocks/pull/65341)
- 支持 ORC 文件切分，从而并行扫描单个大型 ORC 文件。[#65188](https://github.com/StarRocks/starrocks/pull/65188)
- 在优化器中增加 IF 谓词的选择率估算。[#64962](https://github.com/StarRocks/starrocks/pull/64962)
- FE 支持对 `DATE` 和 `DATETIME` 类型的 `hour`、`minute`、`second` 进行常量折叠。[#64953](https://github.com/StarRocks/starrocks/pull/64953)
- 默认启用将简单聚合重写为 MetaScan。[#64698](https://github.com/StarRocks/starrocks/pull/64698)
- 提升存算分离集群中多副本分配处理的可靠性。[#64245](https://github.com/StarRocks/starrocks/pull/64245)
- 在审计日志和指标中暴露缓存命中率。[#63964](https://github.com/StarRocks/starrocks/pull/63964)
- 使用 HyperLogLog 或采样方法估算直方图的分桶级别去重计数（NDV），从而为谓词和 JOIN 提供更准确的 NDV。[#58516](https://github.com/StarRocks/starrocks/pull/58516)
- 支持 SQL 标准语义的 FULL OUTER JOIN USING。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- 当优化器超时时输出内存信息以辅助诊断。[#65206](https://github.com/StarRocks/starrocks/pull/65206)

### 问题修复

以下问题已修复：

- DECIMAL56 的 `mod` 运算相关问题。[#65795](https://github.com/StarRocks/starrocks/pull/65795)
- Iceberg 扫描范围处理相关问题。[#65658](https://github.com/StarRocks/starrocks/pull/65658)
- 临时分区与随机分桶下的 MetaScan 重写问题。[#65617](https://github.com/StarRocks/starrocks/pull/65617)
- `JsonPathRewriteRule` 在透明物化视图改写后使用了错误的表。[#65597](https://github.com/StarRocks/starrocks/pull/65597)
- 当 `partition_retention_condition` 引用了生成列时，物化视图刷新失败。[#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg min/max 值类型问题。[#65551](https://github.com/StarRocks/starrocks/pull/65551)
- 当 `enable_evaluate_schema_scan_rule` 设置为 `true` 时跨库查询 `information_schema.tables` 与 `views` 的问题。[#65533](https://github.com/StarRocks/starrocks/pull/65533)
- JSON 数组比较时的整数溢出问题。[#64981](https://github.com/StarRocks/starrocks/pull/64981)
- MySQL Reader 不支持 SSL。[#65291](https://github.com/StarRocks/starrocks/pull/65291)
- 由于 SVE 构建不兼容导致的 ARM 构建问题。[#65268](https://github.com/StarRocks/starrocks/pull/65268)
- 基于 Bucket-aware 执行的查询在分桶 Iceberg 表上可能卡住。[#65261](https://github.com/StarRocks/starrocks/pull/65261)
- OLAP 表扫描缺少内存限制检查导致的错误传播与内存安全问题。[#65131](https://github.com/StarRocks/starrocks/pull/65131)

### 行为变更

- 当物化视图被置为 Inactive（失效）时，系统会递归失效依赖其的其他物化视图。[#65317](https://github.com/StarRocks/starrocks/pull/65317)
- 在生成 SHOW CREATE 输出时使用原始的物化视图查询 SQL（包含注释和格式）。[#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

发布日期：2025 年 11 月 17 日

### 功能优化

- 优化了 TaskRun 会话变量的处理逻辑，仅处理已知变量。 [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- 默认支持基于元数据收集 Iceberg 和 Delta Lake 表的统计信息。 [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- 支持收集使用 Bucket 和 Truncate 分区转换的 Iceberg 表的统计信息。 [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- 支持 FE `/proc` Profile 用于调试。 [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- 增强了 Iceberg REST Catalog 的 OAuth2 和 JWT 认证支持。 [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- 改进了 Bundle Tablet 元数据的校验与恢复机制。 [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- 优化了扫描范围的内存估算逻辑。 [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### 问题修复

修复了以下问题：

- 在发布 Bundle Tablet 时事务日志被删除。 [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- Join 算法无法保证排序属性，因为在 Join 操作后未重置排序属性。 [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- 透明物化视图改写相关问题。 [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### 行为变更

- 为 Iceberg Catalog 新增属性 `enable_iceberg_table_cache`，可选择禁用 Iceberg 表缓存，从而始终读取最新数据。 [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- 在执行 `INSERT ... SELECT` 前刷新外部表，确保读取最新元数据。 [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- 将锁表槽位数量增加至 256，并在 Slow Lock 日志中新增 `rid` 字段。 [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- 由于与 Event-based Scheduling 不兼容，暂时禁用了 `shared_scan`。 [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- 将 Hive Catalog 的缓存 TTL 默认值修改为 24 小时，并移除了未使用的参数。 [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- 根据会话变量和插入列数量自动确定 Partial Update 模式。 [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

发布日期：2025 年 10 月 17 日

### 数据湖分析

- 统一了 BE 元数据的 Page Cache 和 Data Cache，并采用自适应策略进行扩展。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- 优化了 Iceberg 统计信息的元数据文件解析，避免重复解析。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- 优化了针对 Iceberg 元数据的 COUNT/MIN/MAX 查询，通过有效跳过数据文件扫描，显著提升了大分区表的聚合查询性能并减少资源消耗。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- 支持通过 Procedure `rewrite_data_files` 对 Iceberg 表进行 Compaction。
- 支持带有隐藏分区（Hidden Partition）的 Iceberg 表，包括建表、写入和读取。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- 支持 Paimon Catalog 中的 TIME 数据类型。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- 支持在创建 Iceberg 表时设定排序键。
- 优化 Iceberg 表的写入性能。
  - Iceberg Sink 支持大算子落盘（Spill）、全局 Shuffle 以及本地排序，优化内存与小文件问题。 [#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg Sink 优化基于 Spill Partition Writer 的本地排序，提升写入效率。[#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg Sink 支持分区全局 Shuffle，进一步减少小文件。[#62123](https://github.com/StarRocks/starrocks/pull/62123)
- 增强 Iceberg Bucket-aware 执行，提升分桶表并发与分布能力。[#61756](https://github.com/StarRocks/starrocks/pull/61756)
- 支持 Paimon Catalog 中的 TIME 数据类型。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Iceberg 版本升级至 1.10.0。[#63667](https://github.com/StarRocks/starrocks/pull/63667)

### 安全与认证

- 在使用 JWT 认证和 Iceberg REST Catalog 的场景下，StarRocks 支持通过 REST Session Catalog 将用户登录信息透传到 Iceberg，用于后续数据访问认证。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg Catalog 支持  Vended Credential。
- 支持对 Group Provider 获取的外部组授权 StarRocks 内部角色。[#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- 外表新增 REFRESH 权限，用于控制外表刷新权限。[#62636](https://github.com/StarRocks/starrocks/pull/62636)

<!--
- 支持在 StarRocks FE 侧配置证书以启用 HTTPS，提升系统访问安全性，满足云端或内网的加密传输需求。[#56394](https://github.com/StarRocks/starrocks/pull/56394)
- 支持 BE 节点之间的 HTTPS 通信，确保数据传输加密与完整性，防止内部数据泄露和中间人攻击。[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### 存储优化与集群管理

- 为存算分离集群中的云原生表引入了文件捆绑（File Bundling）优化，自动捆绑由导入、Compaction 或 Publish 操作生成的数据文件，从而减少因高频访问外部存储系统带来的 API 成本。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- 支持多表写写事务（Multi-Table Write-Write Transaction），允许用户控制 INSERT、UPDATE 和 DELETE 操作的原子提交。该事务支持 Stream Load 和 INSERT INTO 接口，有效保证 ETL 与实时写入场景下的跨表一致性。[#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Routine Load 支持 Kafka 4.0。
- 支持在存算一体集群的主键表上使用全文倒排索引。
- 支持修改聚合表的聚合键。[#62253](https://github.com/StarRocks/starrocks/issues/62253)
- 支持对 Catalog、Database、Table、View 和物化视图的名称启用大小写不敏感处理。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 支持在存算分离集群中对 Compute Node 进行黑名单管理。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- 支持全局连接 ID。[#57256](https://github.com/StarRocks/starrocks/pull/57276)
- Information Schema 新增元数据视图 `recyclebin_catalogs`，展示可恢复的已删除元信息。[#51007](https://github.com/StarRocks/starrocks/pull/51007)

### 查询与性能优化

- 支持 DECIMAL256 数据类型，将精度上限从 38 扩展到 76 位。其 256 位存储可以更好地适应高精度金融与科学计算场景，有效缓解 DECIMAL128 在超大规模聚合与高阶运算中的精度溢出问题。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- 基础算子性能提升。[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- 优化了 JOIN 与 AGG 算子性能。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] 引入 SQL Plan Manager，允许用户将查询与查询计划绑定，避免因系统状态变化（如数据更新、统计信息更新）导致查询计划变更，从而稳定查询性能。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- 引入 Partition-wise Spillable Aggregate/Distinct 算子，替代原有基于排序型聚合的 Spill 实现，大幅提升复杂和高基数 GROUP BY 场景下的聚合性能，并降低读写开销。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - 支持在表级别配置 Flat JSON。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - 通过保留 V1 机制并新增 Page 级和 Segment 级索引（ZoneMap、Bloom Filter）、带有延迟物化的谓词下推、字典编码以及集成低基数全局字典，显著提升 JSON 列式存储的执行效率。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- 为 STRING 数据类型支持自适应 ZoneMap 索引创建策略。[#61960](https://github.com/StarRocks/starrocks/issues/61960)
- 增强查询可观测性：
  - 优化 EXPLAIN ANALYZE 返回信息，分算子分组展示执行指标，提升可读性。[#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` 和 `QueryProfileActionV2` 支持 JSON 格式，提升跨 FE 查询能力。[#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - 支持基于所有 FE 获取 QUery Profile 信息。[#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST 命令展示 Catalog、Query ID 等信息。[#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - 增强查询队列以及进程监控，支持显示 Running/Pending 状态。[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- 物化视图改写考虑原表的分布和排序键，提升最佳物化视图选择能力。 [#62830](https://github.com/StarRocks/starrocks/pull/62830)

### 函数与 SQL 语法

- 新增以下函数：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` 和 `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` 和 `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- 提供以下语法扩展：
  - CREATE ANALYZE FULL TABLE 支持 IF NOT EXISTS 关键字。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT 支持 EXCLUDE 子句。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 聚合函数支持 FILTER 子句，提升条件聚合的可读性和执行效率。[#58937](https://github.com/StarRocks/starrocks/pull/58937)

### 行为变更

- 调整物化视图参数 `auto_partition_refresh_number` 的逻辑，无论自动刷新还是手动刷新，均限制需刷新分区的数量。[#62301](https://github.com/StarRocks/starrocks/pull/62301)
- 默认启用 Flat JSON。[#62097](https://github.com/StarRocks/starrocks/pull/62097)
- 系统变量 `enable_materialized_view_agg_pushdown_rewrite` 的默认值设为 `true`，表示默认启用物化视图查询改写中的聚合下推功能。[#60976](https://github.com/StarRocks/starrocks/pull/60976)
- 将 `information_schema.materialized_views` 中部分列的类型调整为更符合对应数据的类型。[#60054](https://github.com/StarRocks/starrocks/pull/60054)
- 当分隔符不匹配时，`split_part` 函数返回 NULL。[#56967](https://github.com/StarRocks/starrocks/pull/56967)
- 在 CTAS/CREATE MATERIALIZED VIEW 中使用 STRING 替代固定长度 CHAR，避免推断错误的列长度导致物化视图刷新失败。[#63114](https://github.com/StarRocks/starrocks/pull/63114) [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- 简化了 Data Cache 相关配置。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` 和 `datacache_disk_size` 现已生效。
  - `storage_page_cache_limit`、`block_cache_mem_size`、`block_cache_disk_size` 已弃用。
- 新增 Catalog 属性（Hive 的 `remote_file_cache_memory_ratio`，Iceberg 的 `iceberg_data_file_cache_memory_usage_ratio` 和 `iceberg_delete_file_cache_memory_usage_ratio`），用于限制 Hive 和 Iceberg 元数据缓存的内存资源，默认值设为 `0.1`（10%），并将元数据缓存 TTL 调整为24小时。[#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION 现不再合并具有相同桶序列号的所有物化索引的统计信息，仅在物化索引级别显示数据分布。[#59656](https://github.com/StarRocks/starrocks/pull/59656)
- 自动分桶表的默认桶大小从 4GB 调整为 1GB，以提升性能和资源利用率。[#63168](https://github.com/StarRocks/starrocks/pull/63168)
- 系统根据对应会话变量和 INSERT 语句的列数确定部分更新模式。[#62091](https://github.com/StarRocks/starrocks/pull/62091)
- 优化 Information Schema 中的 `fe_tablet_schedules` 视图。[#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - `TABLET_STATUS` 列更名为 `SCHEDULE_REASON`，`CLONE_SRC` 列更名为 `SRC_BE_ID`，`CLONE_DEST` 列更名为 `DEST_BE_ID`。
  - `CREATE_TIME`、`SCHEDULE_TIME` 和 `FINISH_TIME` 列类型从 `DOUBLE` 改为 `DATETIME`。
- 部分 FE 指标增加了 `is_leader` 标签。[#63004](https://github.com/StarRocks/starrocks/pull/63004)
- 使用 Microsoft Azure Blob Storage 以及 Data Lake Storage Gen 2 作为对象存储的存算分离集群，升级到 v4.0 后 Data Cache 失效，系统将自动重新加载。

### 降级说明

启用 File Bundling 功能后，您只能将集群降级到 v3.5.2 或更高版本。如果降级到 v3.5.2 之前的版本，将无法读写已启用 File Bundling 功能的表。File Bundling 功能在 v4.0 或更高版本中创建的表格中默认启用。
