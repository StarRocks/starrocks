---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.3

## 3.3.0-RC01

发布日期：2024 年 5 月 10 日

### 存算分离

- 优化了存算分离集群的 Schema Evolution 性能，DDL 变更降低到秒级别。具体信息，参考 [设置 fast schema evolution](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_TABLE/#设置-fast-schema-evolution)。
- 优化了存算分离集群的垃圾回收机制，支持手动 Manual Compaction，可以更高效的回收对象存储上的数据。具体信息，参考 [Manual Compaction](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-statements/data-definition/ALTER_TABLE/#手动-compaction31-版本起)。
- 为了满足从存算一体到存算分离架构的数据迁移需求，社区正式发布 [StarRocks 数据迁移工具](https://docs.starrocks.io/zh/docs/3.3/administration/data_migration_tool/)。该工具同样可用于实现存算一体集群之间的数据同步和容灾方案。
- 存算分离集群存储卷适配 AWS Express One Zone Storage，提升数倍读写性能。具体信息，参考 [CREATE STORAGE VOLUME](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/#参数说明)。
- 优化存算分离集群下主键表的 Compaction 事务 Publish 执行逻辑，通过避免读取主键索引，降低执行过程中的 I/O 和内存开销。

### 数据湖分析

- **Data Cache 增强**：
  - 新增 [缓存预热 (Warmup)](https://docs.starrocks.io/zh/docs/3.3/data_source/data_cache_warmup/) 命令 CACHE SELECT，用于填充查询热点数据，可以结合 SUBMIT TASK 完成周期性填充。
  - 增加了多项 [Data Cache 可观测性指标](https://docs.starrocks.io/zh/docs/3.3/data_source/data_cache_observe/)。
- **Parquet Reader 性能提升**：
  - 优化 Page Index，显著减少 Scan 数据规模。
  - 在有 Page Index 的情况下，降低 Page 多读的情况。
  - 使用 SIMD 加速计算判断数据行是否为空。
- **ORC Reader性能提升**：
  - 使用 column id 下推谓词，从而支持读取 Schema Change 后的 ORC 文件。
  - 优化 ORC tiny stripe 处理逻辑。
- **Iceberg 文件格式能力升级**：
  - Parquet 格式的 Iceberg v2 表查询支持 [equality-delete](https://docs.starrocks.io/zh/docs/3.3/data_source/catalog/iceberg_catalog/#使用说明)。
- **[外表统计信息收集优化](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/Cost_based_optimizer/#采集-hiveiceberghudi-表的统计信息)**：
  - ANALYZE TABLE 命令支持收集外表的直方图统计信息，可以有效应对数据倾斜场景。
  - 支持收集 STRUCT 子列的统计信息。
- **数据湖格式写入性能提升**：
  - Sink 算子性能比 Trino 提高一倍。
  - Hive 及 INSERT INTO FILES 新增支持 Textfile 和 ORC 格式数据的写入。
- 支持 Alibaba Cloud [MaxCompute catalog](https://docs.starrocks.io/zh/docs/3.3/data_source/catalog/maxcompute_catalog/)，不需要执行数据导入即可查询 MaxCompute 里的数据，还可以结合 INSERT INTO 能力实现数据转换和导入。

### 性能提升和查询优化

- **中间结果落盘（Spill to Disk）能力 GA**：优化复杂查询的内存占用，优化 Spill 的调度，保证大查询都能稳定执行，不会 OOM。
- **支持更多索引**：
  - 支持[全文倒排索引](https://docs.starrocks.io/zh/docs/3.3/table_design/indexes/inverted_index/)，以加速全文检索。
  - 支持 [N-Gram bloom filter 索引](https://docs.starrocks.io/zh/docs/3.3/table_design/indexes/Ngram_Bloom_Filter_Index/)，以加速 `LIKE` 查询或 `ngram_search` 和 `ngram_search_case_insensitive` 函数的运算速度。
- 提升 Bitmap 系列函数的性能并优化内存占用，补充了 Bitmap 导出到 Hive 的能力以及配套的 [Hive Bitmap UDF](https://docs.starrocks.io/zh/docs/3.3/integrations/hive_bitmap_udf/)。
- **支持 [Flat JSON](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/Flat_json/)**：导入时自动检测 JSON 数据并提取公共字段，自动创建 JSON 的列式存储结构，JSON 查询性能达到 Struct 的水平。
- **优化全局字典**：提供字典对象，将字典表中的键值对映射关系存在各个 BE 节点的内存中。通过 `dictionary_get()` 函数直接查询 BE 内存中的字典对象，相对于原先使用 `dict_mapping` 函数查询字典表，查询速度更快。并且字典对象还可以作为维度表，可以通过 `dictionary_get()` 函数直接查询字典对象来获取维度值，相对于原先通过 JOIN 维度表来获取维度值，查询速度更快。
- 支持 Colocate Group Execution: 大幅降低在 colocated 表上执行 Join 和 Agg 算子时的内存占用，能够更稳定的执行大查询。
- 优化 CodeGen 性能：默认打开 JIT，在复杂表达式计算场景下性能提升 5 倍。
- 支持使用向量化技术来进行正则表达式匹配，可以降低 `regexp_replace` 函数计算的 CPU 消耗。
- 优化 Broadcast Join，在右表为空时可以提前结束 Join 操作。
- 优化数据倾斜情况下的 Shuffle Join，避免 OOM。
- 聚合查询包含 `Limit` 时，多 Pipeline 线程可以共享 Limit 条件从而防止计算资源浪费。

### 存储优化与集群管理

- **[增强 Range 分区的灵活性](https://docs.starrocks.io/zh/docs/3.3/table_design/Data_distribution/#range-分区)**：新增支持三个特定时间函数作为分区列，可以将时间戳或字符串的分区列值转成日期，然后按转换后的日期划分分区。
- **FE 内存可观测性**：提供 FE 内各模块的详细内存使用指标，以便更好地管理资源。
- **[优化 FE 中的元数据锁](https://docs.starrocks.io/zh/docs/3.3/administration/management/FE_configuration/#lock_manager_enabled)**：提供 Lock manager，可以对 FE 中的元数据锁实现集中管理，例如将元数据锁的粒度从库级别细化为表级别，可以提高导入和查询的并发性能。在 100 并发的导入场景下，导入耗时减少 35%。
- **[使用标签管理 BE](https://docs.starrocks.io/zh/docs/3.3/administration/management/resource_management/be_label/)**：支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，以保证数据在机架或数据中心等之间均匀分布，应对某些机架断电或数据中心故障情况下的灾备需求。
- **[优化排序键](https://docs.starrocks.io/zh/docs/3.3/table_design/indexes/Prefix_index_sort_key/)**：明细表、聚合表和更新表均支持通过 `ORDER BY` 子句指定排序键。
- **优化非字符串标量类型数据的存储效率**：这类数据支持字典编码，存储空间下降 12%。
- **主键表支持 Size-tiered compaction 策略**：降低执行 compaction 时写 I/O 和内存开销。存算分离和存算一体集群均支持该优化。
- **优化主键表持久化索引的读 I/O**：支持按照更小的粒度（页）读取持久化索引，并且改进持久化索引的 bloom filter。存算分离和存算一体集群均支持该优化。

### 物化视图

- **基于视图的改写**：针对视图的查询，可以改写至基于视图创建的物化视图上。具体信息，参考 [基于视图的物化视图查询改写](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#基于视图的物化视图查询改写)。
- **基于文本的改写**：引入基于文本的改写能力，用于改写具有相同抽象语法树的查询（或其子查询）。具体信息，参考 [基于文本的物化视图改写](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/query_rewrite_with_materialized_views/#基于文本的物化视图改写)。
- **增加控制物化视图改写的新属性**：通过 `enable_query_rewrite` 属性实现禁用查询改写，减少整体开销。具体信息，参考 [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#参数-1)。
- **物化视图改写代价优化**：增加候选物化视图个数的控制，以及更好的筛选算法。增加 MV plan cache，从而整体降低改写阶段 Optimizer 耗时。具体信息，参考 [`cbo_materialized_view_rewrite_related_mvs_limit`](https://docs.starrocks.io/zh/docs/3.3/reference/System_variable/#cbo_materialized_view_rewrite_related_mvs_limit)。
- **Iceberg 物化视图更新**：Iceberg 物化视图现支持分区更新触发的增量刷新和 Iceberg Partition Transforms。具体信息，参考 [使用物化视图加速数据湖查询](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/data_lake_query_acceleration_with_materialized_views/#选择合适的刷新策略)。
- **增强的物化视图可观测性**：改进物化视图的监控和管理，以获得更好的系统洞察。具体信息，参考 [异步物化视图监控项](https://docs.starrocks.io/zh/docs/3.3/administration/management/monitoring/metrics/#异步物化视图监控项)。
- **提升大规模物化视图刷新的效率。**
- **多事实表分区刷新**：基于多事实表创建的物化视图支持任意事实表更新后，物化视图都可以进行分区级别增量刷新，增加数据管理的灵活性。具体信息，参考 [多基表对齐分区](https://docs.starrocks.io/zh/docs/3.3/using_starrocks/create_partitioned_materialized_view/#多基表对齐分区)。

### 函数支持

- DATETIME 完整支持微秒（microsecond），相关的时间函数和导入均支持了新的单位。
- 新增如下函数：
  - [字符串函数](https://docs.starrocks.io/zh/docs/3.3/cover_pages/functions_string/)：crc32、url_extract_host、ngram_search
  - Array 函数：[array_contains_seq](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - 时间日期函数：[yearweek](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-functions/date-time-functions/yearweek/)
  - 数学函数：[cbrt](https://docs.starrocks.io/zh/docs/3.3/sql-reference/sql-functions/math-functions/cbrt/)

### 生态支持

- 新增 ClickHouse 语法转化工具 [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer)。
- StarRocks 提供的 Flink connector v1.2.9 已与 Flink CDC 3.0 框架集成，构建从 CDC 数据源到 StarRocks 的流式 ELT 管道。该管道可以将整个数据库、分库分表以及来自源端的 schema change 都同步到 StarRocks。更多信息，参考 [Flink CDC 同步（支持 schema change）](https://docs.starrocks.io/zh/docs/loading/Flink-connector-starrocks/#使用-flink-cdc-30-同步数据支持-schema-change)。

### 问题修复

修复了如下问题：

- 查询改写至使用 UNION ALL 创建的物化视图，查询结果错误。[#42949](https://github.com/StarRocks/starrocks/issues/42949)
- 如果启用 ASAN 编译 BE，使用集群后 BE Crash，并且 `be.warning` 日志显示 `dict_func_expr == nullptr`。[#44551](https://github.com/StarRocks/starrocks/issues/44551)
- 聚合查询单副本表，查询结果错误。[#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join 改写失败。[#43788](https://github.com/StarRocks/starrocks/issues/43788)
- 修改列类型 VARCHAR 为 DECIMAL 后，BE crash。[#44406](https://github.com/StarRocks/starrocks/issues/44406)
- 使用 not equal 运算符查询 List 分区的表，分区裁剪存在问题，导致查询结果出错。[#42907](https://github.com/StarRocks/starrocks/issues/42907)
- 随着较多使用非事务接口的 Stream Load 完成，Leader FE 的堆大小迅速增加。[#43715](https://github.com/StarRocks/starrocks/issues/43715)
