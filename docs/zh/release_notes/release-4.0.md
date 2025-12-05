---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

升级至 v4.0 后，请勿直接将集群降级至 v3.5.0 和 v3.5.1，否则会导致元数据不兼容和 FE Crash。您必须降级到 v3.5.2 或更高版本以避免出现此问题。

:::

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
