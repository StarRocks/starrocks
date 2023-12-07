---
displayed_sidebar: "Chinese"
---

# StarRocks version 2.2

## 2.2.10

发布日期： 2022 年 12 月 2 日

### 功能优化

- 优化 Routine Load 错误提示信息。([#12203](https://github.com/StarRocks/starrocks/pull/12203))
- 支持逻辑算子 `&&`。 ([#11819](https://github.com/StarRocks/starrocks/issues/11819))
- BE crash 时，快速取消查询，避免长时间查询超时卡住。（[#12954](https://github.com/StarRocks/starrocks/pull/12954)）
- 优化 FE 启动脚本，检查 Java 版本。（[#14094](https://github.com/StarRocks/starrocks/pull/14094)）
- 支持 Primary Key 中大批量数据删除。（[#4772](https://github.com/StarRocks/starrocks/issues/4772)）

### 问题修复

修复了以下问题：

- 多表合并 (union) 视图时，如果左深节点存在 NULL 常量，会导致 BE crash。([#13792](https://github.com/StarRocks/starrocks/pull/13792))
- Parquet 文件和 Hive 表中的列类型不一致时，会导致查询过程中 BE crash。([#8848](https://github.com/StarRocks/starrocks/issues/8848))
- 太多 OR 语句导致 Planner 过多递归引发查询超时。([#12788](https://github.com/StarRocks/starrocks/pull/12788))
- 子查询中有 LIMIT 可能导致结果错误。（[#12466](https://github.com/StarRocks/starrocks/pull/12466)）
- 创建视图时语句中无法包含引号。（[#13102](https://github.com/StarRocks/starrocks/pull/13102)）

## 2.2.9

发布日期： 2022 年 11 月 15 日

### 功能优化

- 增加参数 `hive_partition_stats_sample_size`，用于限制获取统计信息的分区数，防止分区数过多导致 FE 获取 Hive 元数据异常。([#12700](https://github.com/StarRocks/starrocks/pull/12700))
- Elasticsearch 外表支持自定义时区。([#12662](https://github.com/StarRocks/starrocks/pull/12662))

### 问题修复

修复了如下问题：

- 外表元数据同步问题导致下线节点 (Decommission) 卡住。([#12369](https://github.com/StarRocks/starrocks/pull/12368))
- 在做增加列操作时如果删除该列可能导致 compaction crash。([#12907](https://github.com/StarRocks/starrocks/pull/12907))
- SHOW CREATE VIEW 没有展示注释字段。([#4163](https://github.com/StarRocks/starrocks/issues/4163))
- UDF 中可能存在内存泄漏导致 OOM 的问题。([#12418](https://github.com/StarRocks/starrocks/pull/12418))
- Follower FE 上存储的节点存活状态 (alive) 依赖于 `heartbeatRetryTimes`，某些场景下不准确。新版本在 `HeartbeatResponse` 中新增属性 `aliveStatus` 来表示节点存活状态。([#12481](https://github.com/StarRocks/starrocks/pull/12481))

### 行为变更

Hive 外表字符串支持的长度从 64 KB 扩展为 1 MB。长度超过 1 MB 时，查询设置成 Null。([#12986](https://github.com/StarRocks/starrocks/pull/12986))

## 2.2.8

发布日期： 2022 年 10 月 17 日

### 问题修复

修复了如下问题：

- 表达式在初始阶段发生错误时可能导致 BE 停止服务。 ([#11395](https://github.com/StarRocks/starrocks/pull/11395))
- 导入时无效的 JSON 可能会导致 BE 停止服务。（[#10804](https://github.com/StarRocks/starrocks/issues/10804)）
- 开启 Pipeline 引擎会导致并行写入出错。（[#11451](https://github.com/StarRocks/starrocks/issues/11451)）
- ORDER BY NULL LIMIT 会导致 BE 停止服务。（[#11648](https://github.com/StarRocks/starrocks/issues/11648)）
- 外表的列类型和 Parquet 表中类型不一致时导致 BE 停止服务。（[#11839](https://github.com/StarRocks/starrocks/issues/11839))

## 2.2.7

发布日期： 2022 年 9 月 23 日

### 问题修复

修复了如下问题：

- 导入 JSON 数据时可能出现数据丢失。 ([#11054](https://github.com/StarRocks/starrocks/issues/11054))
- SHOW FULL TABLES 返回结果错误。 （[#11126](https://github.com/StarRocks/starrocks/issues/11126)）
- 视图权限问题，之前版本需要同时拥有 base 表和视图的权限才能访问视图的数据，修复后只需要拥有视图权限就可以访问。 ([#11290](https://github.com/StarRocks/starrocks/pull/11290))
- 复杂查询中一个 exists/in 子查询的bug。 ([#11415](https://github.com/StarRocks/starrocks/pull/11415))
- REFRESH EXTERNAL TABLE 在 Hive 中做过 schema change 以后会失败。([#11406](https://github.com/StarRocks/starrocks/pull/11406))
- FE在回放创建 bitmap 索引操作时可能出错。（[#11261](https://github.com/StarRocks/starrocks/pull/11261)）

## 2.2.6

发布日期： 2022 年 9 月 14 日

### 问题修复

修复了以下问题：

- 子查询中有 LIMIT 时，`order by...limit...offset...` 结果不准确。（[#9698](https://github.com/StarRocks/starrocks/issues/9698)）
- 大规模数据的 Partial update 导致 BE crash。([#9809](https://github.com/StarRocks/starrocks/issues/9809))
- 当 Bitmap 超过 2 GB 时，compaction 会导致 crash。([#11159](https://github.com/StarRocks/starrocks/pull/11159))
- like() 和 regexp() 函数中 pattern 超过 16 KB 无法使用。([#10364](https://github.com/StarRocks/starrocks/issues/10364))

### 行为调整

- 修改返回结果中`ARRAY<JSON>`的展示形式，输出结果使用单引号而不是转义符。 ([#10790](https://github.com/StarRocks/starrocks/issues/10790))

## 2.2.5

发布日期： 2022 年 8 月 18 日

### 功能优化

- 优化 Pipeline 引擎开启后的系统性能。 [#9580](https://github.com/StarRocks/starrocks/pull/9580)
- 优化索引元数据的内存统计准确度。[#9837](https://github.com/StarRocks/starrocks/pull/9837)

### 问题修复

修复了如下问题：

- BE 在执行 Routine Load 时可能在 `get_partition_offset` 时卡住。 [#9937](https://github.com/StarRocks/starrocks/pull/9937)
- 不同集群使用 Broker Load 导入相同的 HDFS 文件导致出错。 [#9507](https://github.com/StarRocks/starrocks/pull/9507)

## 2.2.4

发布日期： 2022 年 8 月 3 日

### 功能优化

- Hive 外表支持 Schema Change 同步。[#9010](https://github.com/StarRocks/starrocks/pull/9010)
- Broker Load 支持导入 Parquet 文件中的 ARRAY 类型数据。[#9131](https://github.com/StarRocks/starrocks/pull/9131)

### 问题修复

修复了如下问题：

- 通过 Kerberos 认证使用 Broker Load 时无法使用多个 keytab 文件。[#8820](https://github.com/StarRocks/starrocks/pull/8820) [#8837](https://github.com/StarRocks/starrocks/pull/8837)
- 执行 **stop_be.sh** 后立刻退出进程，Supervisor 重新拉起服务可能失败。[#9175](https://github.com/StarRocks/starrocks/pull/9175)
- 错误的 Join Reorder 优先级导致 Join 字段报错 “Column cannot be resolved”。[#9063](https://github.com/StarRocks/starrocks/pull/9063) [#9487](https://github.com/StarRocks/starrocks/pull/9487)

## 2.2.3

发布日期： 2022 年 7 月 24 日

### 问题修复

- 修复资源组删除过程中的错误。[#8036](https://github.com/StarRocks/starrocks/pull/8036)
- 线程资源不足导致 Thrift server 退出。[#7974](https://github.com/StarRocks/starrocks/pull/7974)
- CBO 在一些场景下 join reorder 会无法输出结果。 [#7099](https://github.com/StarRocks/starrocks/pull/7099) [#7831](https://github.com/StarRocks/starrocks/pull/7831) [#6866](https://github.com/StarRocks/starrocks/pull/6866)

## 2.2.2

发布日期： 2022 年 6 月 29 日

### 功能优化

- 支持跨数据库使用 UDF 。 [#6865](https://github.com/StarRocks/starrocks/pull/6865) [#7211](https://github.com/StarRocks/starrocks/pull/7211)

- 优化表结构变更 (Schema Change) 等内部处理的并发控制，降低对 FE 元数据的压力，减少在高并发、大数据量导入场景下容易发生导入积压和变慢的问题。[#6838](https://github.com/StarRocks/starrocks/pull/6838)

### 问题修复

修复了如下问题：

- 执行 CTAS 时创建的新表副本数错误（ `replication_num` ）。[#7036](https://github.com/StarRocks/starrocks/pull/7036)
- 执行 ALTER ROUTINE LOAD 后可能造成元数据丢失。 [#7068](https://github.com/StarRocks/starrocks/pull/7068)
- Runtime filter 无法下推到窗口。 [#7206](https://github.com/StarRocks/starrocks/pull/7206) [#7258](https://github.com/StarRocks/starrocks/pull/7258)
- Pipeline 中潜在的内存泄漏问题。 [#7295](https://github.com/StarRocks/starrocks/pull/7295)

- 停止 Routine Load 任务可能导致死锁。[#6849](https://github.com/StarRocks/starrocks/pull/6849)

- 一些 profile 统计信息的修正。  [#7074](https://github.com/StarRocks/starrocks/pull/7074) [#6789](https://github.com/StarRocks/starrocks/pull/6789)

- get_json_string 函数对 JSON 数组处理错误。 [#7671](https://github.com/StarRocks/starrocks/pull/7671)

## 2.2.1

发布日期： 2022 年 6 月 2 日

### 功能优化

- 通过重构部分热点代码和降低锁粒度优化导入性能，减少长尾延迟。 [#6641](https://github.com/StarRocks/starrocks/pull/6641)
- 在 FE 的审计日志中添加每个查询所消耗部署 BE 机器的 CPU 和内存信息。 [#6208](https://github.com/StarRocks/starrocks/pull/6208) [#6209](https://github.com/StarRocks/starrocks/pull/6209)
- 支持在主键模型表和更新模型表中使用 JSON 数据类型。 [#6544](https://github.com/StarRocks/starrocks/pull/6544)
- 通过降低锁粒度和 BE 汇报 (report) 请求去重减少 FE 负荷，优化部署大量 BE 时的汇报性能并解决大规模集群中 Routine Load 任务卡住的问题。 [#6293](https://github.com/StarRocks/starrocks/pull/6293)

### 问题修复

修复了如下问题：

- 修复 SHOW FULL TABLES FROM DatabaseName 语句中转义字符解析报错的问题。 [#6559](https://github.com/StarRocks/starrocks/issues/6559)
- FE 磁盘空间占用过快的问题（通过回滚 BDBJE 版本修复该bug）。[#6708](https://github.com/StarRocks/starrocks/pull/6708)
- 修复启用列式扫描 (`enable_docvalue_scan=true`) 后，因返回的数据中没有相关字段导致 BE 宕机的问题。[#6600](https://github.com/StarRocks/starrocks/pull/6600)

## 2.2.0

发布日期： 2022 年 5 月 22 日

### 新功能

- 【公测中】发布资源组管理功能。通过使用资源组来控制 CPU、内存的资源使用，让不同租户的大小查询在同一集群执行时，既能实现资源隔离，又能合理使用资源。相关文档，请参见[资源组](../administration/resource_group.md)。
- 【公测中】实现 Java UDF 框架，支持使用 Java 语法编写 UDF（用户自定义函数），扩展 StarRocks 的函数功能。相关文档，请参见 [Java UDF](../sql-reference/sql-functions/JAVA_UDF.md)。
- 【公测中】导入数据至主键模型时，支持更新部分列。在订单更新、多流 JOIN 等实时数据更新场景下，仅需要更新与业务相关的列。相关文档，请参见 [主键模型的表支持部分更新](../loading/Load_to_Primary_Key_tables.md#部分更新)。
- 【公测中】支持 JSON 数据类型和函数。相关文档，请参见 [JSON](../sql-reference/sql-statements/data-types/JSON.md)。
- 支持通过外表查询 Apache Hudi 的数据，进一步完善了数据湖分析的功能。相关文档，请参见 [Apache Hudi 外表](../data_source/External_table.md#apache-hudi-外表)。
- 新增如下函数:
  - ARRAY 函数，[array_agg](../sql-reference/sql-functions/array-functions/array_agg.md)、[array_sort](../sql-reference/sql-functions/array-functions/array_sort.md)、[array_distinct](../sql-reference/sql-functions/array-functions/array_distinct.md)、[array_join](../sql-reference/sql-functions/array-functions/array_join.md)、[reverse](../sql-reference/sql-functions/string-functions/reverse.md)、[array_slice](../sql-reference/sql-functions/array-functions/array_slice.md)、[array_concat](../sql-reference/sql-functions/array-functions/array_concat.md)、[array_difference](../sql-reference/sql-functions/array-functions/array_difference.md)、[arrays_overlap](../sql-reference/sql-functions/array-functions/arrays_overlap.md)、[array_intersect](../sql-reference/sql-functions/array-functions/array_intersect.md)。
  - BITMAP 函数，包括 [bitmap_max](../sql-reference/sql-functions/bitmap-functions/bitmap_max.md)、[bitmap_min](../sql-reference/sql-functions/bitmap-functions/bitmap_min.md)。
  - 其他函数：[retention](../sql-reference/sql-functions/aggregate-functions/retention.md)、[square](../sql-reference/sql-functions/math-functions/square.md)。

### 功能优化

- 重构CBO优化器的 Parser 和 Analyzer，优化代码结构并支持 Insert with CTE 等语法。提升复杂查询的性能，包括公用表表达式（Common Table Expression，CTE）复用等。
- 优化查询Apache Hive外表中基于对象存储（Amazon S3、阿里云OSS、腾讯云COS）的外部表的性能，优化后基于对象存储的查询性能可以与基于HDFS的查询性能基本持平。支持ORC格式文件的延迟物化，提升小文件查询性能。相关文档，请参见 [Apache Hive 外表](../data_source/External_table.md#hive-外表)。
- 通过外表查询 Apache Hive 的数据时，缓存更新通过定期消费 Hive Metastore 的事件（包括数据变更、分区变更等），实现自动增量更新元数据。并且，还支持查询 Apache Hive 中 DECIMAL 和 ARRAY 类型的数据。相关文档，请参见 [Apache Hive 外表](../data_source/External_table.md#hive-外表)。
- 优化 UNION ALL 算子性能，性能提升可达2-25倍。
- 正式发布 Pipeline 引擎，支持自适应调节查询的并行度，并且优化了 Pipeline 引擎的 Profile。提升了高并发场景下小查询的性能。
- 导入 CSV 文件时，支持使用多个字符作为行分隔符。

### 问题修复

- 修复主键模型的表导入数据和 COMMIT 时产生死锁的问题。[#4998](https://github.com/StarRocks/starrocks/pull/4998)
- 解决 FE（包含 BDBJE）的一系列稳定性问题。[#4428](https://github.com/StarRocks/starrocks/pull/4428)、[#4666](https://github.com/StarRocks/starrocks/pull/4666)、[#2](https://github.com/StarRocks/bdb-je/pull/2)
- 修复 SUM 函数对大量数据求和时返回结果溢出的问题。[#3944](https://github.com/StarRocks/starrocks/pull/3944)
- 修复 ROUND 和 TRUNCATE 函数返回结果的精度问题。[#4256](https://github.com/StarRocks/starrocks/pull/4256)
- 修复 SQLancer 发现的一系列问题，请参见 [SQLancer 相关 issues](https://github.com/StarRocks/starrocks/issues?q=is%3Aissue++label%3Asqlancer++milestone%3A2.2)。

### 其他

Flink 连接器 flink-connector-starrocks 支持 Flink 1.14 版本。

### 升级注意事项

- 版本号低于 2.0.4 或者 2.1.x 中低于 2.1.6 的用户，升级参考 [StarRocks 升级注意事项](https://forum.starrocks.com/t/topic/2228).
- 升级后如果碰到问题需要回滚，请在 fe.conf 文件中增加 `ignore_unknown_log_id=true`。这是因为新版本的元数据日志新增了类型，如果不加这个参数，则无法回滚。最好等做完 checkpoint 之后再设置 `ignore_unknown_log_id=false` 并重启 FE，恢复正常配置。
