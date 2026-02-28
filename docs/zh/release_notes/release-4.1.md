---
displayed_sidebar: docs
---

# StarRocks version 4.1

## 4.1.0-RC

发布日期：2026 年 2 月 28 日

### 存算分离架构

- **全新多租户数据管理**
  存算分离集群现已支持基于范围（Range-based）的数据分布，以及 Tablet 的自动拆分。当 Tablet 体积过大或出现热点时，可自动进行拆分，无需修改 Schema、SQL 或重新导入数据。该特性显著提升了易用性，可直接缓解多租户负载中的数据倾斜与热点问题。 [#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743)
- **大容量 Tablet 支持（第一阶段）**
  存算分离集群现支持显著提升单 Tablet 数据容量，长期目标为单 Tablet 100 GB。第一阶段聚焦于在单个 Lake Tablet 内支持并行 Compaction 和并行 MemTable Finalization，在 Tablet 容量增长时降低数据导入与 Compaction 的开销。 [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#68677](https://github.com/StarRocks/starrocks/pull/68677)
- **Fast Schema Evolution V2**
  存算分离集群现已支持 Fast Schema Evolution V2，实现秒级 DDL 执行，并将支持范围扩展至物化视图。 [#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)
- **[Beta] 存算分离架构上的倒排索引**
  在存算分离集群中支持内置倒排索引，加速文本过滤与全文检索类负载。 [#66541](https://github.com/StarRocks/starrocks/pull/66541)
- **缓存可观测性**
  在审计日志和监控系统中暴露缓存命中率指标，提高缓存透明度和延迟可预测性。提供更详细的 Data Cache 指标，包括内存与磁盘配额、Page Cache 统计以及分表命中率。 [#63964](https://github.com/StarRocks/starrocks/pull/63964)
- 为数据湖表新增 Segment 元数据过滤能力，在扫描时根据排序键范围跳过无关 Segment，降低范围查询的 I/O 开销。 [#68124](https://github.com/StarRocks/starrocks/pull/68124)
- Lake DeltaWriter 支持快速取消，降低存算分离集群中被取消导入任务的延迟。 [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 支持基于时间间隔的自动集群快照调度。 [#67525](https://github.com/StarRocks/starrocks/pull/67525)

### 数据湖分析

- **Iceberg DELETE 支持**
  支持为 Iceberg 表写入 position delete 文件，从而可以直接在 StarRocks 中对 Iceberg 表执行 DELETE 操作。覆盖 Plan、Sink、Commit 和 Audit 全流程。 [#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)
- **Hive 与 Iceberg 表的 TRUNCATE**
  支持对外部 Hive 和 Iceberg 表执行 TRUNCATE TABLE。 [#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)
- **Iceberg 与 Paimon 上的增量物化视图**
  将增量物化视图刷新能力扩展至 Iceberg Append-only 表和 Paimon 表，无需全表刷新即可实现查询加速。 [#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)
- 支持读取 Iceberg 表中的文件路径和行位置元数据列。 [#67003](https://github.com/StarRocks/starrocks/pull/67003)
- 支持读取 Iceberg v3 表中的 `_row_id`，并支持 Iceberg v3 的全局延迟物化（global late materialization）。 [#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- 支持创建带自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中展示属性。 [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 支持基于指定 Branch、Tag、Version 或 Timestamp 查询 Paimon 表。 [#63316](https://github.com/StarRocks/starrocks/pull/63316)
- 默认启用 ETL 执行模式的更多优化，在无需显式配置的情况下提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 等批处理性能。 [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- 为 Iceberg 表上的 INSERT 和 DELETE 操作增加提交审计信息。 [#69198](https://github.com/StarRocks/starrocks/pull/69198)
- 支持在 Iceberg REST Catalog 中启用或禁用 View Endpoint 操作。 [#66083](https://github.com/StarRocks/starrocks/pull/66083)
- 优化 CachingIcebergCatalog 的缓存查找效率。 [#66388](https://github.com/StarRocks/starrocks/pull/66388)
- 支持对多种 Iceberg Catalog 类型执行 EXPLAIN。 [#66563](https://github.com/StarRocks/starrocks/pull/66563)

### 查询引擎

- **ASOF JOIN**
  引入 ASOF JOIN，用于时间序列和事件关联查询，支持基于时间或有序键在两个数据集之间高效匹配最接近的记录。 [#63070](https://github.com/StarRocks/starrocks/pull/63070) [#63236](https://github.com/StarRocks/starrocks/pull/63236)
- **面向半结构化数据的 VARIANT 类型**
  引入 VARIANT 数据类型，用于灵活的 Schema-on-read 半结构化数据存储与查询。支持读写、类型转换及 Parquet 集成。 [#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)
- **递归 CTE**
  支持递归公共表表达式（Recursive CTE），用于层级遍历、图查询和迭代式 SQL 计算。 [#65932](https://github.com/StarRocks/starrocks/pull/65932)
- 改进 Skew Join v2 重写逻辑，支持基于统计信息的倾斜检测、直方图支持及 NULL 倾斜感知。 [#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- 优化窗口函数中的 COUNT DISTINCT，并支持融合多 DISTINCT 聚合。 [#67453](https://github.com/StarRocks/starrocks/pull/67453)

### 函数与 SQL 语法

- 新增以下函数：
  - `array_top_n`：返回数组中按值排序后的前 N 个元素。 [#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：按元素位置合并多个数组为结构体数组。 [#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：格式化 JSON 字符串（带缩进）。 [#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：在 JSON 字符串指定路径设置值。 [#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：将每个单词的首字母转换为大写。 [#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：对具有相同 key 的 MAP 值进行跨行求和。 [#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：返回当前会话时区。 [#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：返回当前 warehouse 名称。 [#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：将秒数转换为 TIME 类型。 [#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：在 SQL 中调用外部 AI 模型进行推理。 [#61583](https://github.com/StarRocks/starrocks/pull/61583)
- 语法与功能扩展：
  - `array_sort` 支持使用 lambda 比较器实现自定义排序。 [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - FULL OUTER JOIN 支持 USING 子句并遵循 SQL 标准语义。 [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持在带 ORDER BY/PARTITION BY 的窗口函数中对 DISTINCT 聚合进行 framed 计算。 [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中支持 ARRAY 类型。 [#63547](https://github.com/StarRocks/starrocks/pull/63547)

### 管理与可观测性

- 资源组支持 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，提升多 Warehouse 场景下的 CPU 资源隔离能力。 [#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 新增 `information_schema.fe_threads` 系统视图，用于查看 FE 线程状态。 [#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest 黑名单，在集群级别屏蔽特定查询模式。 [#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持通过 Arrow Flight 从受网络拓扑限制无法直接访问的节点获取数据。 [#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，在无需重连的情况下将全局变量变更同步至现有连接。 [#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 内置 UI 功能支持查询 Profile 分析与格式化 SQL 查看，降低查询调优门槛。 [#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 接口，提供结构化集群概览。 [#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增全局只读系统变量 `@@run_mode`，用于查询当前集群运行模式（shared-data 或 shared-nothing）。 [#69247](https://github.com/StarRocks/starrocks/pull/69247)

### 行为变更

- 默认启用 ETL 执行模式优化，提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 等批处理性能。 [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现支持列引用（此前仅支持常量）。 [#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现遵循 SQL 标准语义：USING 列在输出中只出现一次。 [#65122](https://github.com/StarRocks/starrocks/pull/65122)

