---
displayed_sidebar: docs
---

# StarRocks version 4.0

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
  - `CREATE_TIME` 和 `SCHEDULE_TIME` 列类型从 `DOUBLE` 改为 `DATETIME`。
- 一些 FE 指标增加了 `is_leader` 标签。[#63004](https://github.com/StarRocks/starrocks/pull/63004)
