---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.1

发布日期：2025 年 3 月 12 日

### 功能优化

- 湖分析中支持 Delta Lake 的 Deletion Vector。
- 支持安全视图。通过创建安全视图，您可以禁止没有对应基表 SELECT 权限的用户查询视图（即使该用户有视图的 SELETE 权限）。
- 支持 Sketch HLL ([`ds_hll_count_distinct`](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/ds_hll_count_distinct/))。对比 `approx_count_distinct`，该函数可以提供更高精度的模糊去重。
- 存算分离架构支持自动创建 Snapshot，以便进行集群恢复操作。
- 存算分离架构中的 Storage Volume 支持 Azure Data Lake Storage Gen2。
- 通过 MySQL 协议连接 StarRocks 时支持 SSL 认证，确保客户端与 StarRocks 集群之间传输的数据不会被未经授权的用户读取。

### 问题修复

修复了如下问题：

- OLAP 视图参与物化视图处理逻辑导致的问题。[#52989](https://github.com/StarRocks/starrocks/pull/52989)
- 当未找到其中一个副本（Replica）时，无论有多少个副本提交成功，写入事务都会提交失败（修复后，多数副本成功后写入事务即可成功）。 [#55212](https://github.com/StarRocks/starrocks/pull/55212)
- Stream Load 调度到 Alive 状态为 false 的节点时，导入失败。[#55371](https://github.com/StarRocks/starrocks/pull/55371)
- 集群 Snapshot 下文件被错误删除。 [#56338](https://github.com/StarRocks/starrocks/pull/56338)

### 行为变更

- 优雅退出由默认关闭变更为打开。相关 BE/CN 参数 `loop_count_wait_fragments_finish` 默认值修改为 `2`，也即系统最多会等待 20 秒来等已经运行中的查询继续执行完。[#56002](https://github.com/StarRocks/starrocks/pull/56002)

## 3.4.0

发布日期：2025 年 1 月 24 日

### 数据湖分析

- 优化了针对 Iceberg V2 的查询性能：通过减少对 Delete-file 的重复读取，提升了查询性能并降低了内存使用量。
- 支持 Delta Lake 表的列映射功能，允许查询经过 Delta Schema Evolution 的 Delta Lake 数据。更多内容，参考 [Delta Lake Catalog - 功能支持](https://docs.starrocks.io/zh/docs/data_source/catalog/deltalake_catalog/#%E5%8A%9F%E8%83%BD%E6%94%AF%E6%8C%81)。
- Data Cache 相关优化： 
  - 引入了分段 LRU (SLRU) 缓存淘汰策略，有效防止偶发大查询导致的缓存污染，提高缓存命中率，减少查询性能波动。在有大查询污染的模拟测试中，基于 SLRU 的查询性能能提升 70% 至数倍。 更多内容，参考 [Data Cache - 缓存淘汰机制](https://docs.starrocks.io/zh/docs/data_source/data_cache/#%E7%BC%93%E5%AD%98%E6%B7%98%E6%B1%B0%E6%9C%BA%E5%88%B6)。
  - 优化了 Data Cache 的自适应 I/O 策略。系统会根据缓存磁盘的负载和性能，自适应地将部分查询请求路由到远端存储，从而提升整体访问吞吐能力。
  - 统一了存算分离架构和数据湖查询场景中使用的 Data Cache 实例，以及相关的参数和指标，简化配置，并提升资源使用率。更多内容，参考 [Data Cache](https://docs.starrocks.io/zh/docs/using_starrocks/caching/block_cache/)。
  - 支持了数据湖查询场景中 Data Cache 缓存数据持久化，BE 重启后可复用之前的缓存数据，减少查询性能的波动。
- 支持通过查询自动触发 ANALYZE 任务自动收集外部表统计信息，相较于元数据文件，可提供更准确的 NDV 信息，从而优化查询计划并提升查询性能。更多内容，参考 [查询触发采集](https://docs.starrocks.io/zh/docs/using_starrocks/Cost_based_optimizer/#%E6%9F%A5%E8%AF%A2%E8%A7%A6%E5%8F%91%E9%87%87%E9%9B%86)。
- 提供针对 Iceberg 的 Time Travel 查询功能，可通过指定 TIMESTAMP 或 VERSION，从指定的 BRANCH 或 TAG 读取数据。
- 支持数据湖查询的异步查询片段投递。通过让 FE 获取文件和 BE 执行查询并行执行，消除了 BE 必须在 FE 获取所有文件之后才能执行查询的限制，从而降低涉及大量未缓存文件的数据湖查询的整体延迟。同时减少因缓存文件列表而带给 FE 的内存压力，提升查询稳定性。（目前已实现对 Hudi 和 Delta Lake 的优化，对 Iceberg 的优化仍在开发中。）

### 性能提升与查询优化

- [Experimental] 初步支持 Query Feedback 功能，用于慢查询的自动优化。系统将收集慢查询的执行详情，自动分析查询计划中是否存在需要调优的地方，并生成专属的 Tuning Guide。当后续相同查询生成相同的 Bad Plan 时，系统会基于先前生成的 Tuning Guide 局部调优该 Query Plan。更多内容，参考 [Query Feedback](https://docs.starrocks.io/zh/docs/using_starrocks/query_feedback/)。
- [Experimental] 支持 Python UDF，相较于 Java UDF 提供了更便捷的函数自定义能力。更多内容，参考 [Python UDF](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Python_UDF/)。
- [Experimental] 支持 Arrow Flight 接口，可更高效读取大数据量的查询结果，并使 BE 替代 FE 直接处理返回结果，显著降低 FE 压力，特别适用于大数据分析、处理和机器学习等场景。
- 支持多列 OR 谓词的下推，允许带有多列 OR 条件（如 `a = xxx OR b = yyy`）的查询利用对应列索引，从而减少数据读取量并提升查询性能。
- 优化了 TPC-DS 查询性能。在 TPC-DS 1TB Iceberg 数据集下，查询性能提升20%。优化手段包括利用主外键做表裁剪和聚合列裁剪，以及聚合下推位置改进等。

### 存算分离增强

- 支持 Query Cache，对齐存算一体架构。
- 支持同步物化视图，对齐存算一体架构。

### 存储引擎

- 统一多种分区方式为表达式分区，支持多级分区，每级分区均可为任意表达式。更多内容，参考 [表达式分区](https://docs.starrocks.io/zh/docs/table_design/data_distribution/expression_partitioning/)。
- [Preview] 通过引入通用聚合函数状态存储框架，聚合表可以支持所有 StarRocks 原生聚合函数。
- 支持向量索引，能够在对大规模、高维向量数据下进行快速近似最近邻搜索（ANNS），满足深度学习和机器学习等场景需求。目前支持两种向量索引类型：IVFPQ 和 HNSW。

### 数据导入

- INSERT OVERWRITE 新增 Dynamic Overwrite 语义，启用后，系统将根据导入的数据自动创建分区或覆盖对应的现有分区，导入不涉及的分区不会被清空或删除，适用于恢复特定分区数据的场景。更多内容，参考 [INSERT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/loading_unloading/INSERT/)。
- 优化了 INSERT from FILES 导入，使其可以基本取代 Broker Load 成为首选导入方式： 
  - FILES 支持 LIST 远程存储中的文件，并提供文件的基本统计信息。更多内容，参考 [FILES - list_files_only](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/#list_files_only)。
  - INSERT 支持按名称匹配列，特别适用于导入列很多且列名相同的数据。（默认按位置匹配列。）更多内容，参考 [INSERT 按名称匹配列](https://docs.starrocks.io/zh/docs/loading/InsertInto/#insert-%E6%8C%89%E5%90%8D%E7%A7%B0%E5%8C%B9%E9%85%8D%E5%88%97)。
  - INSERT 支持指定 PROPERTIES，与其他导入方式保持一致。用户可通过指定 `strict_mode`、`max_filter_ratio` 和 `timeout` 来控制数据导入的行为和质量。更多内容，参考 [INSERT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/loading_unloading/INSERT/)。
  - INSERT from FILES 支持将目标表的 Schema 检查下推到 FILES 的扫描阶段，从而更准确地推断源数据 Schema。更多内容，参考 [Target Table Schema 检查下推](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/#target-table-schema-%E6%A3%80%E6%9F%A5%E4%B8%8B%E6%8E%A8)。
  - FILES 支持合并不同 Schema 的文件。Parquet 和 ORC 文件基于列名合并，CSV 文件基于列位置（顺序）合并。对于不匹配的列，用户可通过指定 `fill_mismatch_column_with` 属性选择填充 NULL 值或报错。更多内容，参考 [合并具有不同 Schema 的文件](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/#%E5%90%88%E5%B9%B6%E5%85%B7%E6%9C%89%E4%B8%8D%E5%90%8C-schema-%E7%9A%84%E6%96%87%E4%BB%B6)。
  - FILES 支持从 Parquet 文件推断 STRUCT 类型数据。（在早期版本中，STRUCT 数据被推断为 STRING 类型。）更多内容，参考 [推断 Parquet 文件中的 STRUCT 类型](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/#%E6%8E%A8%E6%96%AD-parquet-%E6%96%87%E4%BB%B6%E4%B8%AD%E7%9A%84-struct-%E7%B1%BB%E5%9E%8B)。
- 支持将多个并发的 Stream Load 请求合并为一个事务批量提交，从而提高实时数据导入的吞吐能力，对于高并发、小批量（KB到几十MB）实时导入场景特别有用，可以减少频繁导入操作导致的数据版本过多、避免 Compaction 过程中的大量资源消耗，并且降低过多小文件带来的 IOPS 和 I/O 延迟。

### 其他

- 优化 BE 和 CN 的优雅退出流程，将退出过程中的 BE 或 CN 节点状态展示为 `SHUTDOWN`。
- 优化日志打印，避免占用过多磁盘空间。
- 存算一体集群支持备份恢复更多对象：逻辑视图、External Catalog 元数据，以及表达式分区和 List 分区。
- [Preview] 支持在 Follower FE 上进行 CheckPoint，以避免 CheckPoint 期间 Leader FE 内存大量消耗，从而提升 Leader FE 的稳定性。

### 降级说明

- 集群只可从 v3.4.0 降级至 v3.3.9 或更高版本。

