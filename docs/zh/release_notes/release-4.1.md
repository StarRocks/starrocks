---
displayed_sidebar: docs
---

# StarRocks version 4.1

:::warning

**降级注意事项**

- 将 StarRocks 升级到 v4.1 后，请勿降级到 v4.0.6 以下的任何 v4.0 版本。

  由于 v4.1 引入了数据布局的内部变更（与 Tablet 分割和数据分布机制相关），升级至 v4.1 的集群生成的元数据和存储结构可能与早期版本不完全兼容。因此，从 v4.1 降级仅支持降至 v4.0.6 或更高版本。不支持降级至 v4.0.6 之前的版本。此限制源于早期版本在解析 Tablet 布局和分布元数据时的向后兼容性约束。

:::

## 4.1.0

发布日期：2026 年 4 月 13 日

### 存算分离架构

- **新的多租户数据管理**

  存算分离集群现在支持基于范围的数据分布以及 Tablet 的自动拆分和合并。当 Tablet 过大或成为热点时，可以自动拆分，无需更改 Schema、修改 SQL 或重新导入数据。此功能可以显著提高可用性，直接解决多租户工作负载中的数据倾斜和热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 Tablet 支持（第一阶段）**

  支持存算分离集群中更大的单 Tablet 数据容量，长期目标是每个 Tablet 100 GB。第一阶段侧重于在单个 Lake Tablet 中实现并行 Compaction 和并行 MemTable 最终化，从而随着 Tablet 大小增长而减少导入和 Compaction 开销。[#66586](https://github.com/StarRocks/starrocks/pull/66586) [#68677](https://github.com/StarRocks/starrocks/pull/68677)

- **Fast Schema Evolution V2**

  存算分离集群现在支持快速 Schema 变更 V2，可实现秒级 DDL 执行 Schema 操作，并进一步将支持扩展到物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 存算分离上的倒排索引**

  为存算分离集群启用内置倒排索引，以加速文本过滤和全文搜索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  缓存命中率指标在审计日志和监控系统中公开，以提高缓存透明度和延迟可预测性。详细的数据缓存指标包括内存和磁盘配额、页面缓存统计信息以及每表命中率。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表添加了段元数据过滤器，可在扫描期间根据排序键范围跳过不相关的段，从而减少范围谓词查询的 I/O。[#68124](https://github.com/StarRocks/starrocks/pull/68124)
- 支持 Lake DeltaWriter 的快速取消，减少共享数据集群中已取消的摄取作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 新增支持基于时间间隔的调度，用于自动化集群快照。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- 支持 MemTable 刷写和合并的管道执行，提高共享数据集群中云原生表的摄取吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)
- 支持 `dry_run` 模式修复云原生表，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)
- 在无共享集群中为发布事务添加了线程池，提高了发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- 支持动态修改云原生表的 `datacache.enable` 属性。[#69011](https://github.com/StarRocks/starrocks/pull/69011)

### 数据湖分析

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入位置删除文件，从而可以直接从 StarRocks 对 Iceberg 表执行 DELETE 操作。此支持涵盖了计划、写入、提交和审计的完整管道。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive 和 Iceberg 表的 TRUNCATE**

  支持对外部 Hive 和 Iceberg 表执行 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上的增量物化视图**

  将增量物化视图刷新支持扩展到 Iceberg 仅追加表，从而无需完全刷新表即可实现查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg 中半结构化数据的 VARIANT 类型**

  支持 Iceberg Catalog 中的 VARIANT 数据类型，用于灵活的、读时模式的半结构化数据存储和查询。支持读、写、类型转换和 Parquet 集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增支持 Iceberg v3 默认值特性和行血缘。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增支持 `rewrite_manifests` 过程，并扩展了 `expire_snapshots` 和 `remove_orphan_files` 过程，增加了额外参数以实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- **Iceberg `$properties` 元数据表**

  通过 `$properties` 元数据表添加了对查询 Iceberg 表属性的支持。[#68504](https://github.com/StarRocks/starrocks/pull/68504)

- 支持从 Iceberg 表读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)
- 支持从 Iceberg v3 表读取 `_row_id`，并支持 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- 支持创建具有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中显示属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 支持使用特定分支、标签、版本或时间戳查询 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)
- 支持 Paimon 表的复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)
- 支持 Paimon 视图。[#56058](https://github.com/StarRocks/starrocks/pull/56058)
- 支持 Paimon 表的 TRUNCATE 操作。[#67559](https://github.com/StarRocks/starrocks/pull/67559)
- 在创建 Iceberg 表时，支持带括号语法的 Partition Transforms。[#68945](https://github.com/StarRocks/starrocks/pull/68945)
- 支持 Iceberg 表的 ALTER TABLE REPLACE PARTITION COLUMN。[#70508](https://github.com/StarRocks/starrocks/pull/70508)
- 支持基于 Transform Partition 的 Iceberg 全局 shuffle，以改进数据组织。[#70009](https://github.com/StarRocks/starrocks/pull/70009)
- 支持为 Iceberg 表 sink 动态启用全局 shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- 为 Iceberg 表 sink 引入了 Commit 队列，以避免并发 Commit 冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)
- 为 Iceberg 表 sink 添加了主机级排序，以改进数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)
- 默认启用 ETL 执行模式下的额外优化，无需显式配置即可提高 INSERT INTO SELECT、CREATE TABLE AS SELECT 和类似批处理操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- 为 Iceberg 表上的 INSERT 和 DELETE 操作添加了提交审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)
- 支持在 Iceberg REST Catalog 中启用或禁用视图端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)
- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)
- 支持对各种 Iceberg catalog 类型执行 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)
- 支持 AWS Glue Catalog 表中的分区投影。[#67601](https://github.com/StarRocks/starrocks/pull/67601)
- 为 AWS Glue `GetDatabases` API 添加了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)
- 支持 Azure ABFS/WASB 路径映射，并带有端点注入（`azblob`/`adls2`）。[#67847](https://github.com/StarRocks/starrocks/pull/67847)
- 为 JDBC 目录添加了数据库元数据缓存，以减少远程 RPC 开销和外部系统故障的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)
- 为 JDBC 目录添加了 `schema_resolver` 属性，以支持自定义模式解析。[#68682](https://github.com/StarRocks/starrocks/pull/68682)
- 支持 `information_schema` 中 PostgreSQL 表的列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)
- 改进了 Oracle 和 PostgreSQL JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎

- **递归 CTE**

  支持递归公共表表达式 (CTE)，用于分层遍历、图查询和迭代 SQL 计算。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 重写，支持基于统计信息的倾斜检测、直方图支持和 NULL 倾斜感知。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- 改进了窗口上的 COUNT DISTINCT，并增加了对融合多 DISTINCT 聚合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- 支持窗口函数的显式 Skew hint，通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739) [#67944](https://github.com/StarRocks/starrocks/pull/67944)
- 支持 CTE 的物化 Hint。[#70802](https://github.com/StarRocks/starrocks/pull/70802)
- 默认启用全局延迟物化，通过将列读取推迟到需要时进行，从而提高查询性能。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- 在 Trino 解析器中支持 INSERT 语句的 EXPLAIN 和 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)
- 支持 EXPLAIN 以提高查询队列可见性。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数和 SQL 语法

- 添加了以下函数：
  - `array_top_n`：返回按值排序的数组中的前 N 个元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：将多个数组按元素组合成一个结构体数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: 使用缩进格式化 JSON 字符串。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: 在 JSON 字符串中指定路径设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: 将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: 对具有相同键的行中的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: 返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: 返回当前仓库的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: 将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: 从 SQL 调用外部 AI 模型以进行推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `min_n` / `max_n`: 返回前 N 个最小/最大值的聚合函数。[#63807](https://github.com/StarRocks/starrocks/pull/63807)
  - `regexp_position`: 返回字符串中正则表达式匹配的位置。[#67252](https://github.com/StarRocks/starrocks/pull/67252)
  - `is_json_scalar`: 返回 JSON 值是否为标量。[#66050](https://github.com/StarRocks/starrocks/pull/66050)
  - `get_json_scalar`: 从 JSON 字符串中提取标量值。[#68815](https://github.com/StarRocks/starrocks/pull/68815)
  - `raise_error`: 在 SQL 表达式中引发用户定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
  - `uuid_v7`: 生成时间有序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
  - `STRING_AGG`: GROUP_CONCAT 的语法糖。[#64704](https://github.com/StarRocks/starrocks/pull/64704)
- 提供以下函数或语法扩展：
  - 在 `array_sort` 中支持 lambda 比较器以实现自定义排序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持 FULL OUTER JOIN 的 USING 子句，具有 SQL 标准语义。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持对带有 ORDER BY/PARTITION BY 的框架窗口函数进行 DISTINCT 聚合。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中支持 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 支持 VARBINARY 用于类似 count distinct 的聚合函数。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持 `MULTIPLY`/`DIVIDE` 用于区间操作。[#68407](https://github.com/StarRocks/starrocks/pull/68407)
  - 支持 IN 表达式中的日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 支持 BEGIN/START TRANSACTION 的 WITH LABEL 语法。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 支持 SHOW 语句中的 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持 `ALTER TASK` 语句用于任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
  - 支持从 S3 加载 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
  - 支持 Scala 函数中的命名参数。[#66344](https://github.com/StarRocks/starrocks/pull/66344)
  - 支持 CSV 文件导出多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)
  - 支持 `ANALYZE PROFILE` 中的 `last_query_id()`，以便轻松进行查询配置文件分析。[#64557](https://github.com/StarRocks/starrocks/pull/64557)

### 管理与可观测性

- 支持资源组的 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改善多仓库 CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图以检查 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest 黑名单，以在集群级别阻止特定的查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法访问的节点检索 Arrow Flight 数据。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，将全局变量更改传播到现有连接，而无需重新连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 添加了内置 UI 功能，用于分析查询配置文件和查看格式化 SQL，使查询调优更易于访问。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 端点，以提供结构化的集群概览。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增了一个全局只读系统变量 `@@run_mode`，用于查询当前集群运行模式（共享数据或无共享）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2` 以改进查询队列管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持 Stream Load 和 Merge Commit 操作的用户级默认仓库。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 新增 `skip_black_list` 会话变量，以便在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为指标 API 添加了 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 添加了模拟用户支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 将 `table_query_timeout` 添加为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 新增 FE 配置文件日志记录，具有可配置的延迟阈值。[#69396](https://github.com/StarRocks/starrocks/pull/69396)
- 支持添加 FE 观察者节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 在 `information_schema.loads` 中支持 Merge Commit 信息，以提高加载作业的可见性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在云原生表中显示 tablet 状态，以便更好地进行故障排除。[#69616](https://github.com/StarRocks/starrocks/pull/69616)
- 为外部目录可观测性添加了按目录类型划分的查询指标。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 为 FE 和 BE 添加了 Debian (.deb) 打包支持。[#68821](https://github.com/StarRocks/starrocks/pull/68821)

### 安全

- [CVE-2026-33870] [CVE-2026-33871] 替换了 AWS bundle 并将 Netty 升级到 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级到 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级到 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### 错误修复

修复了以下问题：

- 通过跳过范围分布 Tablet 的数据文件删除，修复了 Tablet 分裂后的数据丢失问题。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 中复杂类型的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了由 `shared_ptr` 在 `BatchUnit` 和 `FetchTaskContext` 之间循环导致的内存泄漏。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了错误路径上并行段/行集加载中的 use-after-free 问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 修复了聚合溢出 `set_finishing` 中潜在的哈希表数据丢失问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 修复了 SystemMetrics 中由于并发 getline 访问导致的 double-free 崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在急切合并消耗所有块时发生的崩溃问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了当字典支持表被删除时 `visitDictionaryGetExpr` 中的 NPE 问题。[#71109](https://github.com/StarRocks/starrocks/pull/71109)
- 修复了在 Stream Load/Broker Load 中分析生成列时，如果引用列缺失导致的 NPE 问题。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 修复了当自动创建的分区被 TTL 清理器删除时导致的 NPE 问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了当快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中的 NPE 问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了外连接中带有常量侧列引用的谓词重写不正确的问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了磁盘重新迁移 (A→B→A) 期间 GC 竞争导致的 PK tablet 行集元数据丢失问题。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- 修复了 SharedDataStorageVolumeMgr 中的 DB 读锁泄漏问题。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- 修复了在共享数据中修改 CHAR 列长度后查询结果错误的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了多表情况下 MV 刷新错误。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新后 MV 回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步 MV 中全空值处理错误。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了快速模式更改 ADD COLUMN 后查询 MV 时重复列 ID 错误。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了 IVM 刷新记录不完整的 PCT 分区元数据问题。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 修复了由共享 DecodeInfo 导致的低基数重写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数连接谓词类型不匹配问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了 Parquet Page Index Filter 在 `null_counts` 为空时的段错误。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 展平数组和对象在相同路径上的冲突。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算器不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB FileSystem 缓存键以包含容器。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池满时发生的死锁。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon 目录刷新时在 ObjectTable 上发生 ClassCastException 导致的崩溃。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了 PaimonView 将表引用解析为 default_catalog 而非 Paimon 目录的问题。[#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 修复了带有常量子查询的 FULL OUTER JOIN USING。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了带有 CTE 范围的 join on 子句错误。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 修复了短路点查找中缺少分区谓词的问题。[#71124](https://github.com/StarRocks/starrocks/pull/71124)
- 通过使用 bindScope() 模式修复了 ConnectContext 内存泄漏。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了无共享集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift 接受线程在遇到任何异常时退出。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了例行加载列映射中的 UDF 解析。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了字典页过大时扫描结果错误。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了范围分区重叠。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了查询队列分配时间和挂起超时问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了处理空字面量数组时 `array_map` 崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 的堆栈溢出问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了优化器超时问题。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- 修复了 LDAP 认证中不区分大小写的用户名规范化问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计和 SQL 编校中屏蔽了用户认证字符串。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更

- ETL 执行模式优化现在默认启用。这使得 INSERT INTO SELECT、CREATE TABLE AS SELECT 和类似批处理工作负载无需显式配置更改即可受益。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除了常量值外，还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：USING 列在输出中只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- 全局惰性物化现在默认启用。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- `query_queue_v2` 现在默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL 事务默认由会话变量 `enable_sql_transaction` 控制。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
