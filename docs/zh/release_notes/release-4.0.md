---
displayed_sidebar: docs
---

# StarRocks version 4.0

## 4.0.0-RC02

发布日期：2025 年 9 月 29 日

### 新增功能

- 创建 Iceberg 表时支持设定排序键。
- 支持多表写写事务（Multi-Table Write-Write Transaction），允许用户控制 INSERT、UPDATE 和 DELETE 操作的原子提交。该事务支持 Stream Load 和 INSERT INTO 接口，有效保证 ETL 与实时写入场景下的跨表一致性。
- 支持修改聚合表的聚合键。

### 功能优化

- 优化 Delta Lake Catalog 缓存配置：调整 Delta Lake Catalog Property `DELTA_LAKE_JSON_META_CACHE_TTL` 和 `DELTA_LAKE_CHECKPOINT_META_CACHE_TTL` 的默认值为 24 小时，并简化 Parquet handler 逻辑。 [#63441](https://github.com/StarRocks/starrocks/pull/63441)
- 优化 Delta Lake Catalog 错误日志格式与内容，提升调试体验。 [#63389](https://github.com/StarRocks/starrocks/pull/63389)
- 外部组（如 LDAP Group）支持角色授权、回收和展示，完善 SQL 语法和测试用例，提升权限管控能力。 [#63385](https://github.com/StarRocks/starrocks/pull/63385)
- 加强 Stream Load 的参数一致性校验，避免参数漂移带来的风险。 [#63347](https://github.com/StarRocks/starrocks/pull/63347)
- 优化 Stream Load 的 Label 传递机制，减少依赖。 [#63334](https://github.com/StarRocks/starrocks/pull/63334)
- 优化 ANALYZE PROFILE 格式，ExplainAnalyzer 支持按算子分组显示指标。 [#63326](https://github.com/StarRocks/starrocks/pull/63326)
- 优化 QueryDetailActionV2 和 QueryProfileActionV2 API，返回 JSON 格式结果。 [#63235](https://github.com/StarRocks/starrocks/pull/63235)
- 优化大量 CompoundPredicates 场景下的谓词解析。 [#63139](https://github.com/StarRocks/starrocks/pull/63139)
- 调整部分 FE 指标为 leader 感知。 [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- 优化 SHOW PROCESS LIST，增加 Catalog 和 Query ID 信息。 [#62552](https://github.com/StarRocks/starrocks/pull/62552)
- 优化 BE JVM 内存监控指标。 [#62210](https://github.com/StarRocks/starrocks/pull/62210)
- 优化物化视图改写逻辑和日志输出。 [#62985](https://github.com/StarRocks/starrocks/pull/62985)
- 优化随机分桶策略。 [#63168](https://github.com/StarRocks/starrocks/pull/63168)
- 支持通过 `ALTER TABLE <table_name> AUTO_INCREMENT = 10000;` 重置 AUTO_INCREMENT 值的起始点。 [#62767](https://github.com/StarRocks/starrocks/pull/62767)
- Group Provider 支持通过 DN 匹配 Group。 [#62711](https://github.com/StarRocks/starrocks/pull/62711)

### 问题修复

修复了以下问题：

- ARRAY 低基数优化问题导致的 Left Join 结果不全。[#63419](https://github.com/StarRocks/starrocks/pull/63419)
- 物化视图聚合下推改写后生成了错误的执行计划。 [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- 在部分 JSON 字段裁剪场景，Schema 找不到字段时系统打印多余 Warning 日志。 [#63414](https://github.com/StarRocks/starrocks/pull/63414)
- ARM 环境下插入 DECIMAL256 类型数据时 SIMD Batch 计算参数错误导致死循环。 [#63406](https://github.com/StarRocks/starrocks/pull/63406)
- 三类存储相关问题。 [#63398](https://github.com/StarRocks/starrocks/pull/63398)
  - 磁盘路径为空缓存异常。
  - AZURE 缓存 Key 前缀错误。
  - S3 多段上传异常。
- 启用 Fast Schema Evolution 进行 CHAR 到 VARCHAR 的 Schema Change 后 ZoneMap 过滤失效。[#63377](https://github.com/StarRocks/starrocks/pull/63377)
- 中间类型为 `ARRAY<NULL_TYPE>` 导致的 ARRAY 聚合类型分析错误。[#63371](https://github.com/StarRocks/starrocks/pull/63371)
- 基于自增列进行 Partial Update 时元数据不一致问题。[#63370](https://github.com/StarRocks/starrocks/pull/63370)
- 并发删除 Tablet 或查询时元信息不一致。 [#63291](https://github.com/StarRocks/starrocks/pull/63291)
- Iceberg 表写入时创建 `s``pill` 目录失败。 [#63278](https://github.com/StarRocks/starrocks/pull/63278)
- 变更 Ranger Hive Service 权限不生效。 [#63251](https://github.com/StarRocks/starrocks/pull/63251)
- Group Provider 不支持 `IF NOT EXISTS` 与 `IF EXISTS` 子句。 [#63248](https://github.com/StarRocks/starrocks/pull/63248)
- Iceberg 分区使用保留关键字导致的异常。 [#63243](https://github.com/StarRocks/starrocks/pull/63243)
- Prometheus 指标格式问题。 [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- 启用 Compaction 的情况下开始复制事务，版本校验失败。 [#62663](https://github.com/StarRocks/starrocks/pull/62663)
- 启用 File Bunding 后缺少 Compaction Profile。 [#62638](https://github.com/StarRocks/starrocks/pull/62638)
- Clone 后冗余副本处理的问题。 [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- Delta Lake 表找不到分区列。 [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- 存算分离集群物化视图不支持 Colocation。 [#62941](https://github.com/StarRocks/starrocks/pull/62941)
- 读取 Iceberg 表 NULL 分区的问题。 [#62934](https://github.com/StarRocks/starrocks/pull/62934)
- Histogram 统计中 Most Common Values (MCV) 包含单引号导致 SQL 语法错误。 [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- KILL ANALYZE 命令不生效。 [#62842](https://github.com/StarRocks/starrocks/pull/62842)
- 采集 Stream Load Profile 失败。 [#62802](https://github.com/StarRocks/starrocks/pull/62802)
- CTE Reuse 计划提取错误。 [#62784](https://github.com/StarRocks/starrocks/pull/62784)
- BE 选择异常导致 Rebalance 失效。 [#62776](https://github.com/StarRocks/starrocks/pull/62776)
- User Property 优先级低于 Session Variable 的问题。 [#63173](https://github.com/StarRocks/starrocks/pull/63173)

## 4.0.0-RC

发布日期：2025 年 9 月 9 日

### 数据湖分析

- 统一了 BE 元数据的 Page Cache 和 Data Cache，并采用自适应策略进行扩展。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- 优化了 Iceberg 统计信息的元数据文件解析，避免重复解析。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- 优化了针对 Iceberg 元数据的 COUNT/MIN/MAX 查询，通过有效跳过数据文件扫描，显著提升了大分区表的聚合查询性能并减少资源消耗。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- 支持通过 Procedure `rewrite_data_files` 对 Iceberg 表进行 Compaction。
- 支持带有隐藏分区（Hidden Partition）的 Iceberg 表，包括建表、写入和读取。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- 支持 Paimon Catalog 中的 TIME 数据类型。[#58292](https://github.com/StarRocks/starrocks/pull/58292)

<!--
- 优化了 Iceberg 表的排序。
-->

### 安全与认证

- 在使用 JWT 认证和 Iceberg REST Catalog 的场景下，StarRocks 支持通过 REST Session Catalog 将用户登录信息透传到 Iceberg，用于后续数据访问认证。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg Catalog 支持  Vended Credential。

<!--
- 支持在 StarRocks FE 侧配置证书以启用 HTTPS，提升系统访问安全性，满足云端或内网的加密传输需求。[#56394](https://github.com/StarRocks/starrocks/pull/56394)
- 支持 BE 节点之间的 HTTPS 通信，确保数据传输加密与完整性，防止内部数据泄露和中间人攻击。[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### 存储优化与集群管理

- 为存算分离集群中的云原生表引入了文件捆绑（File Bundling）优化，自动捆绑由导入、Compaction 或 Publish 操作生成的数据文件，从而减少因高频访问外部存储系统带来的 API 成本。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Routine Load 支持 Kafka 4.0。
- 支持在存算一体集群的主键表上使用全文倒排索引。
- 支持对 Catalog、Database、Table、View 和物化视图的名称启用大小写不敏感处理。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 支持在存算分离集群中对 Compute Node 进行黑名单管理。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- 支持全局连接 ID。[#57256](https://github.com/StarRocks/starrocks/pull/57276)

<!--
- 支持多表写写事务（Multi-Table Write-Write Transaction），允许用户控制 INSERT、UPDATE 和 DELETE 操作的原子提交。该事务支持 Stream Load 和 INSERT INTO 接口，有效保证 ETL 与实时写入场景下的跨表一致性。
- 支持修改聚合表的聚合键。
-->

### 查询与性能优化

- 支持 DECIMAL256 数据类型，将精度上限从 38 扩展到 76 位。其 256 位存储可以更好地适应高精度金融与科学计算场景，有效缓解 DECIMAL128 在超大规模聚合与高阶运算中的精度溢出问题。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- 优化了 JOIN 与 AGG 算子性能。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] 引入 SQL Plan Manager，允许用户将查询与查询计划绑定，避免因系统状态变化（如数据更新、统计信息更新）导致查询计划变更，从而稳定查询性能。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- 引入 Partition-wise Spillable Aggregate/Distinct 算子，替代原有基于排序型聚合的 Spill 实现，大幅提升复杂和高基数 GROUP BY 场景下的聚合性能，并降低读写开销。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - 支持在表级别配置 Flat JSON。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - 通过保留 V1 机制并新增 Page 级和 Segment 级索引（ZoneMap、Bloom Filter）、带有延迟物化的谓词下推、字典编码以及集成低基数全局字典，显著提升 JSON 列式存储的执行效率。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- 为 STRING 数据类型支持自适应 ZoneMap 索引创建策略。[#61960](https://github.com/StarRocks/starrocks/issues/61960)

### 函数与 SQL 语法

- 新增以下函数：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` 和 `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
- 提供以下语法扩展：
  - CREATE ANALYZE FULL TABLE 支持 IF NOT EXISTS 关键字。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT 支持 EXCLUDE 子句。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 聚合函数支持 FILTER 子句，提升条件聚合的可读性和执行效率。[#58937](https://github.com/StarRocks/starrocks/pull/58937)
