---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.3

## 3.3.0

发布日期：2024 年 6 月 21 日

### 功能及优化

#### 存算分离

- 优化了存算分离集群的 Fast Schema Evolution 能力，DDL 变更降低到秒级别。具体信息，参考 [设置 Fast Schema Evolution](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/#设置-fast-schema-evolution)。
- 为了满足从存算一体到存算分离架构的数据迁移需求，社区正式发布 [StarRocks 数据迁移工具](https://docs.starrocks.io/zh/docs/administration/data_migration_tool/)。该工具同样可用于实现存算一体集群之间的数据同步和容灾方案。
- [Preview] 存算分离集群存储卷适配 AWS Express One Zone Storage，提升数倍读写性能。具体信息，参考 [CREATE STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/#参数说明)。
- 优化了存算分离集群的垃圾回收机制，支持手动 Manual Compaction，可以更高效的回收对象存储上的数据。具体信息，参考 [Manual Compaction](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/#手动-compaction31-版本起)。
- 优化存算分离集群下主键表的 Compaction 事务 Publish 执行逻辑，通过避免读取主键索引，降低执行过程中的 I/O 和内存开销。
- 存算分离集群支持 Tablet 内并行 Scan，优化在建表时 Bucket 较少场景下，查询的并行度受限于 Tablet 数量的问题，提升查询性能。用户可以通过设置以下系统变量启用并行 Scan 功能。

  ```SQL
  SET GLOBAL enable_lake_tablet_internal_parallel = true;
  SET GLOBAL tablet_internal_parallel_mode = "force_split";
  ```

#### 数据湖分析

- **Data Cache 增强：**
  - 新增 [缓存预热 (Warmup)](https://docs.starrocks.io/zh/docs/data_source/data_cache_warmup/) 命令 CACHE SELECT，用于填充查询热点数据，可以结合 SUBMIT TASK 完成周期性填充。该功能同时支持外表和存算分离的内表。
  - 增加了多项 [Data Cache 可观测性指标](https://docs.starrocks.io/zh/docs/data_source/data_cache_observe/)。
- **Parquet Reader 性能提升：**
  - 针对 Page Index 的优化，显著减少 Scan 数据规模。
  - 在有 Page Index 的情况下，降低 Page 多读的情况。
  - 使用 SIMD 加速计算判断数据行是否为空。
- **ORC Reader 性能提升：**
  - 使用 Column ID 下推谓词，从而支持读取 Schema Change 后的 ORC 文件。
  - 优化 ORC Tiny Stripe 处理逻辑。
- **Iceberg 文件格式能力升级：**
  - 大幅提升 Iceberg Catalog 的元数据访问性能，重构并行 Scan 逻辑，解决了 Iceberg 原生 SDK 在处理大量元数据文件时的单线程 I/O 瓶颈，在执行有元数据瓶颈的查询时带来 10 倍以上的性能提升。
  - Parquet 格式的 Iceberg v2 表查询支持 [equality-delete](https://docs.starrocks.io/zh/docs/data_source/catalog/iceberg_catalog/#使用说明)。
- **[Experimental] Paimon Catalog 优化：**
  - 基于 Paimon 外表创建的物化视图支持自动查询改写。
  - 优化针对 Paimon Catalog 查询的 Scan Range 调度，提高 I/O 并发。
  - 支持查询 Paimon 系统表。
  - Paimon 外表支持 DELETE Vector，以提升更新删除场景下的查询效率。
- **[外表统计信息收集优化](https://docs.starrocks.io/zh/docs/using_starrocks/Cost_based_optimizer/#采集-hiveiceberghudi-表的统计信息)：**
  - ANALYZE TABLE 命令支持收集直方图统计信息，可以有效应对数据倾斜场景。
  - 支持 STRUCT 子列统计信息收集。
- **数据湖格式写入性能提升：**
  - Sink 算子性能比 Trino 提高一倍。
  - Hive 及 INSERT INTO FILES 新增支持 Textfile 和 ORC 格式数据的写入。
- [Preview] 支持 Alibaba Cloud [MaxCompute catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/maxcompute_catalog/)，不需要执行数据导入即可查询 MaxCompute 里的数据，还可以结合 INSERT INTO 能力实现数据转换和导入。
- [Experimental] 支持 ClickHouse Catalog。
- [Experimental] 支持 [Kudu Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/kudu_catalog/)。

#### 性能提升和查询优化

- **ARM 性能优化：**
  -  针对 ARM 架构指令集大幅优化性能。在使用 AWS Gravinton 机型测试的情况下，在 SSB 100G 测试中，使用 ARM 架构时的性能比 x86 快 11%；在 Clickbench 测试中，使用 ARM 架构时的性能比 x86 快 39%，在 TPC-H 100G  测试中，使用 ARM 架构时的性能比 x86 快 13%，在 TPC-DS 100G  测试中，使用 ARM 架构时的性能比 x86 快 35%。
- **中间结果落盘（Spill to Disk）能力 GA**：优化复杂查询的内存占用，优化 Spill 的调度，保证大查询都能稳定执行，不会 OOM。
- [Preview] 支持将中间结果 [落盘至对象存储](https://docs.starrocks.io/zh/docs/administration/management/resource_management/spill_to_disk/)。
- **支持更多索引**：
  - [Preview] 支持[全文倒排索引](https://docs.starrocks.io/zh/docs/table_design/indexes/inverted_index/)，以加速全文检索。
  - [Preview] 支持 [N-Gram bloom filter 索引](https://docs.starrocks.io/zh/docs/table_design/indexes/Ngram_Bloom_Filter_Index/)，以加速 `LIKE` 查询或 `ngram_search` 和 `ngram_search_case_insensitive` 函数的运算速度。
- 提升 Bitmap 系列函数的性能并优化内存占用，补充了 Bitmap 导出到 Hive 的能力以及配套的 [Hive Bitmap UDF](https://docs.starrocks.io/zh/docs/integrations/hive_bitmap_udf/)。
- **[Preview] 支持 [Flat JSON](https://docs.starrocks.io/zh/docs/using_starrocks/Flat_json/)**：导入时自动检测 JSON 数据并提取公共字段，自动创建 JSON 的列式存储结构，JSON 查询性能达到 Struct 的水平。
- **[Preview] 优化全局字典**：提供字典对象，将字典表中的键值对映射关系存在各个 BE 节点的内存中。通过 `dictionary_get()` 函数直接查询 BE 内存中的字典对象，相对于原先使用 `dict_mapping` 函数查询字典表，查询速度更快。并且字典对象还可以作为维度表，可以通过 `dictionary_get()` 函数直接查询字典对象来获取维度值，相对于原先通过 JOIN 维度表来获取维度值，查询速度更快。
- [Preview] 支持 Colocate Group Execution: 大幅降低在 colocated 表上执行 Join 和 Agg 算子时的内存占用，能够更稳定的执行大查询。
- 优化 CodeGen 性能：默认打开 JIT，在复杂表达式计算场景下性能提升 5 倍。
- 支持使用向量化技术来进行正则表达式匹配，可以降低 `regexp_replace` 函数计算的 CPU 消耗。
- 优化 Broadcast Join，在右表为空时可以提前结束 Join 操作。
- 优化数据倾斜情况下的 Shuffle Join，避免 OOM。
- 聚合查询包含 `Limit` 时，多 Pipeline 线程可以共享 Limit 条件从而防止计算资源浪费。

#### 存储优化与集群管理

- **[增强 Range 分区的灵活性](https://docs.starrocks.io/zh/docs/table_design/Data_distribution/#range-分区)：**新增支持三个特定时间函数作为分区列，可以将时间戳或字符串的分区列值转成日期，然后按转换后的日期划分分区。
- **FE 内存可观测性：**提供 FE 内各模块的详细内存使用指标，以便更好地管理资源。
- **[优化 FE 中的元数据锁](https://docs.starrocks.io/zh/docs/administration/management/FE_configuration/#lock_manager_enabled)：**提供 Lock Manager，可以对 FE  中的元数据锁实现集中管理，例如将元数据锁的粒度从库级别细化为表级别，可以提高导入和查询的并发性能。在 100 并发的导入场景下，导入耗时减少 35%。
- **[使用标签管理 BE](https://docs.starrocks.io/zh/docs/administration/management/resource_management/be_label/)**：支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，以保证数据在机架或数据中心等之间均匀分布，应对某些机架断电或数据中心故障情况下的灾备需求。
- **[优化排序键](https://docs.starrocks.io/zh/docs/table_design/indexes/Prefix_index_sort_key/)：**明细表、聚合表和更新表均支持通过 `ORDER BY` 子句指定排序键。
- **[Experimental] 优化非字符串标量类型数据的存储效率**：这类数据支持字典编码，存储空间下降 12%。
- **主键表支持 Size-tiered Compaction 策略**：降低执行 Compaction 时写 I/O 和内存开销。存算分离和存算一体集群均支持该优化。（同时 backport 至 3.2 和 3.1 版本）。
- **优化主键表持久化索引的读 I/O：**支持按照更小的粒度（页）读取持久化索引，并且改进持久化索引的 bloom filter。存算分离和存算一体集群均支持该优化。（同时 backport 至 3.2和3.1 版本）。
- 支持 IPv6 部署：可以 IPv6 网络。

#### 物化视图

- **基于视图的改写：**针对视图的查询，支持改写至基于视图创建的物化视图上。具体信息，参考 [基于视图的物化视图查询改写](https://docs.starrocks.io/zh/docs/using_starrocks/query_rewrite_with_materialized_views/#基于视图的物化视图查询改写)。
- **基于文本的改写：**引入基于文本的改写能力，用于改写具有相同抽象语法树的查询（或其子查询）。具体信息，参考 [基于文本的物化视图改写](https://docs.starrocks.io/zh/docs/using_starrocks/query_rewrite_with_materialized_views/#基于文本的物化视图改写)。
- **[Preview] 支持为直接针对物化视图的查询指定透明改写模式**：开启物化视图属性 `transparent_mv_rewrite_mode` 后，当用户直接查询物化视图时，StarRocks 会自动改写查询，将已经刷新的物化视图分区中的数据和未刷新分区对应的原始数据做自动 Union 合并。该模式适用于建模场景下，需要保证数据一致，但同时还希望控制刷新频率降低刷新成本的场景。具体信息，参考 [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#参数-1)。
- 支持物化视图聚合下推：开启系统变量 `enable_materialized_view_agg_pushdown_rewrite` 后，用户可以利用单表[聚合上卷](https://docs.starrocks.io/zh/docs/using_starrocks/query_rewrite_with_materialized_views/#聚合上卷改写)类的物化视图来加速多表关联场景，把聚合下推到 Join 以下的 Scan 层来显著提升查询效率。具体信息，参考 [聚合下推](https://docs.starrocks.io/zh/docs/using_starrocks/query_rewrite_with_materialized_views/#聚合下推)。
- **物化视图改写控制的新属性：**通过 `enable_query_rewrite` 属性实现禁用查询改写，减少整体开销。如果物化视图仅用于建模后直接查询，没有改写需求，则禁用改写。具体信息，参考 [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#参数-1)。
- **物化视图改写代价优化：**增加候选物化视图个数的控制，以及更好的筛选算法。增加 MV plan cache。从而整体降低改写阶段 Optimizer 耗时。具体信息，参考 `cbo_materialized_view_rewrite_related_mvs_limit`。
- **Iceberg 物化视图更新：**Iceberg 物化视图现支持分区更新触发的增量刷新和 Iceberg Partition Transforms。具体信息，参考 [使用物化视图加速数据湖查询](https://docs.starrocks.io/zh/docs/using_starrocks/data_lake_query_acceleration_with_materialized_views/#选择合适的刷新策略)。
- **增强的物化视图可观测性：**改进物化视图的监控和管理，以获得更好的系统洞察。具体信息，参考 [异步物化视图监控项](https://docs.starrocks.io/zh/docs/administration/management/monitoring/metrics/#异步物化视图监控项)。
- **提升大规模物化视图刷新的效率：**支持全局 FIFO 调度，优化嵌套物化视图级联刷新策略，修复高频刷新场景下部分问题。
- **多事实表分区刷新：**基于多事实表创建的物化视图支持任意事实表更新后，物化视图都可以进行分区级别增量刷新，增加数据管理的灵活性。具体信息，参考 [多基表对齐分区](https://docs.starrocks.io/zh/docs/using_starrocks/create_partitioned_materialized_view/#多基表对齐分区)。

#### 函数支持

- DATETIME 完整支持微秒（Microsecond），相关的时间函数和导入均支持了新的单位。
- 新增如下函数：
  - [字符串函数](https://docs.starrocks.io/zh/docs/cover_pages/functions_string/)：crc32、url_extract_host、ngram_search
  - Array 函数：[array_contains_seq](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - 时间日期函数：[yearweek](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/yearweek/)
  - 数学函数：[crbt](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/cbrt/)

#### 生态支持

- [Experimental] 新增 ClickHouse 语法转化工具 [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer)。
- StarRocks 提供的 Flink connector v1.2.9 已与 Flink CDC 3.0 框架集成，构建从 CDC 数据源到 StarRocks 的流式 ELT 管道。该管道可以将整个数据库、分库分表以及来自源端的 Schema Change 都同步到 StarRocks。更多信息，您参见 [Flink CDC 同步（支持 Schema Change）](https://docs.starrocks.io/zh/docs/loading/Flink-connector-starrocks/#使用-flink-cdc-30-同步数据支持-schema-change)。

### 行为变更（待补充）

- 物化视图属性 `partition_refresh_num` 默认值从 `-1` 调整为 `1`，当物化视图有多个分区需要刷新时，原来在一个刷新任务重刷新所有的分区，当前会一个分区一个分区增量刷新，避免先前行为消耗过多资源。可以通过 FE 参数 `default_mv_partition_refresh_number` 调整默认行为。
- 系统原先按照 GMT+8 时区的时间调度数据库一致性检查，现在将按照当地时区的时间进行调度。[#45748](https://github.com/StarRocks/starrocks/issues/45748)
- 默认启用 Data Cache 来加速数据湖查询。用户也可通过 `SET enable_scan_datacache = false` 手动关闭 Data Cache。
- 对于存算分离场景，在降级到 v3.2.8 及其之前版本时，如需复用先前 Block Cache 中的缓存数据，需要手动修改 **starlet_cache** 目录下 Blockfile 文件名，将文件名格式从 `blockfile_{n}.{version}` 改为 `blockfile_{n}`，即去掉版本后缀。具体可参考 [Block Cache 使用说明](https://docs.starrocks.io/zh/docs/using_starrocks/block_cache/#使用说明)。v3.2.9 及以后版本自动兼容 v3.3 文件名，无需手动执行该操作。

### 参数变更（待补充）

- 支持动态修改 FE 参数 `sys_log_level`。[#45062](https://github.com/StarRocks/starrocks/issues/45062)

### 问题修复

修复了如下问题：

- 查询改写至使用 UNION ALL 创建的物化视图，查询结果错误。[#42949](https://github.com/StarRocks/starrocks/issues/42949)
- 执行带谓词的查询，并且查询改写至物化视图时，读取了多余的列。[#45272](https://github.com/StarRocks/starrocks/issues/45272)
- 函数 next_day、previous_day 的结果出错。[#45343](https://github.com/StarRocks/starrocks/issues/45343)
- 副本迁移导致 schema change 失败。[#45384](https://github.com/StarRocks/starrocks/issues/45384)
- RESTORE 全文倒排索引的表后，BE crash。[#45010](https://github.com/StarRocks/starrocks/issues/45010)
- 使用 Iceberg Catalog 查询时，返回重复的数据行。[#44753](https://github.com/StarRocks/starrocks/issues/44753)
- 低基数字典优化无法在聚合表中字符串数组（`ARRAY<VARCHAR>`）类型的列上生效。[#44702](https://github.com/StarRocks/starrocks/issues/44702)
- 查询改写至使用 UNION ALL 创建的物化视图，查询结果错误。[#42949](https://github.com/StarRocks/starrocks/issues/42949)
- 如果启用 ASAN 编译 BE，使用集群后 BE Crash，并且 `be.warning` 日志显示 `dict_func_expr == nullptr`。[#44551](https://github.com/StarRocks/starrocks/issues/44551)
- 聚合查询单副本表，查询结果错误。[#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join 改写失败。[#43788](https://github.com/StarRocks/starrocks/issues/43788)
- 修改列类型 VARCHAR 为 DECIMAL 后，BE crash。[#44406](https://github.com/StarRocks/starrocks/issues/44406)
- 使用 not equal 运算符查询 List 分区的表，分区裁剪存在问题，导致查询结果出错。[#42907](https://github.com/StarRocks/starrocks/issues/42907)
- 随着较多使用非事务接口的 Stream Load 完成，Leader FE 的堆大小迅速增加。[#43715](https://github.com/StarRocks/starrocks/issues/43715)

### 降级说明

如需将 v3.3.0 及以上集群降级至 v3.2，用户需执行以下操作：

1. 确保降级前的 v3.3 集群中发起的所有 ALTER TABLE SCHEMA CHANGE 事物已完成或取消。
2. 通过以下命令清理所有事务历史记录：

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. 通过以下命令确认无历史记录遗留：

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```

4. 执行以下语句为元数据创建镜像文件：

   ```sql
   ALTER SYSTEM CREATE IMAGE;
   ```

5. 在新的镜像文件传输到所有 FE 节点的目录 **meta/image** 之后，降级其中一个 Follower FE 节点，确认没有问题后继续降级集群中的其他节点。

