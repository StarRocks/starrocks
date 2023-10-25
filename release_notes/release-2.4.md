# StarRocks version 2.4

## 2.4.3

发布日期：2023 年 1 月 19 日

### 功能优化

- 在 Analyze 时，StarRocks 提前检查 Database 和 Table 是否存在以避免 NPE。[#14467](https://github.com/StarRocks/starrocks/pull/14467)
- 在查询外表时，如果列的数据类型不支持，将不物化该列。[#13305](https://github.com/StarRocks/starrocks/pull/13305)
- 为 FE 启动脚本 **start_fe.sh** 添加 Java 版本检查。[#14333](https://github.com/StarRocks/starrocks/pull/14333)

### 问题修复

修复了如下问题：

- 未设置 Timeout 导致 Stream Load 失败。[#16241](https://github.com/StarRocks/starrocks/pull/16241)
- BRPC Send 在内存使用多的时候会崩溃。[#16046](https://github.com/StarRocks/starrocks/issues/16046)
- 无法通过低版本 StarRocks 外表的方式导入数据。[#16130](https://github.com/StarRocks/starrocks/pull/16130)
- 物化视图刷新失败会导致内存泄漏。[#16041](https://github.com/StarRocks/starrocks/pull/16041)
- Schema Change 在 Publish 阶段卡住。[#14148](https://github.com/StarRocks/starrocks/issues/14148)
- 物化视图 QeProcessorImpl 问题可能导致内存泄漏。[#15699](https://github.com/StarRocks/starrocks/pull/15699)
- Limit 查询结果不一致。[#13574](https://github.com/StarRocks/starrocks/pull/13574)
- INSERT 导入导致内存泄漏。[#14718](https://github.com/StarRocks/starrocks/pull/14718)
- Primary Key 表执行 Tablet Migration。[#13720](https://github.com/StarRocks/starrocks/pull/13720)
- 当 Broker Load 作业持续运行时 Broker Kerberos Ticket 会超时。[#16149](https://github.com/StarRocks/starrocks/pull/16149)
- 基于表生成视图过程中，列的 `nullable` 信息转化错误。[#15744](https://github.com/StarRocks/starrocks/pull/15744)

### 行为变更

- 修改 Thrift Listen 的 Backlog 的默认值为 `1024`。 [#13911](https://github.com/StarRocks/starrocks/pull/13911)
- 添加 `FORBID_INVALID_DATES` 的 SQL 模式，默认关闭。打开后会校验日期类型的输入，当日期为非法时间时会报错。[#14143](https://github.com/StarRocks/starrocks/pull/14143)

## 2.4.2

发布日期：2022 年 12 月 14 日

### 功能优化

- 优化了 Bucket Hint 在存在大量 Bucket 时候的性能。[#13142](https://github.com/StarRocks/starrocks/pull/13142)

### 问题修复

修复了如下问题：

- 主键索引落盘可能导致 BE 崩溃。[#14857](https://github.com/StarRocks/starrocks/pull/14857) [#14819](https://github.com/StarRocks/starrocks/pull/14819)
- 物化视图表类型不能被`SHOW FULL TABLES` 正确识别。[#13954](https://github.com/StarRocks/starrocks/pull/13954)
- StarRocks 从 v2.2 升级到 v2.4 可能导致 BE 崩溃。[#13795](https://github.com/StarRocks/starrocks/pull/13795)
- Broker Load 可能导致 BE 崩溃。[#13973](https://github.com/StarRocks/starrocks/pull/13973)
- Session 变量 `statistic_collect_parallel` 不生效。[#14352](https://github.com/StarRocks/starrocks/pull/14352)
- INSERT INTO 可能导致 BE 崩溃。[#14818](https://github.com/StarRocks/starrocks/pull/14818)
- JAVA UDF 可能导致 BE 崩溃。[#13947](https://github.com/StarRocks/starrocks/pull/13947)
- Partial Update 时副本 Clone 可能导致 BE 崩溃且无法重启。[#13683](https://github.com/StarRocks/starrocks/pull/13683)
- Colocate Join 可能不生效。[#13561](https://github.com/StarRocks/starrocks/pull/13561)

### 行为变更

- Session 变量 `query_timeout` 添加最大值 `259200` 和最小值 `1` 的限制。

## 2.4.1

发布日期：2022 年 11 月 14 日

### 新增特性

- 新增非等值 LEFT SEMI/ANTI JOIN 支持，完善 JOIN 功能。[#13019](https://github.com/StarRocks/starrocks/pull/13019)

### 功能优化

- 在 `HeartbeatResponse` 添加 `aliveStatus` 属性，用以判断节点在线状态，优化节点在线判断逻辑。[#12713](https://github.com/StarRocks/starrocks/pull/12713)

- 优化 Routine Load 的报错信息显示。[#12155](https://github.com/StarRocks/starrocks/pull/12155)

### 问题修复

- 因自 2.4.0 RC 升级至 2.4.0 导致 BE 崩溃。[#13128](https://github.com/StarRocks/starrocks/pull/13128)

- 查询数据湖时，延迟物化会导致查询结果错误。[#13133](https://github.com/StarRocks/starrocks/pull/13133)

- 函数 get_json_int 报错。[#12997](https://github.com/StarRocks/starrocks/pull/12997)

- 索引落盘的主键表删除数据时，可能导致数据不一致。[#12719](https://github.com/StarRocks/starrocks/pull/12719)

- 主键表 Compaction 可能会导致 BE 崩溃。[#12914](https://github.com/StarRocks/starrocks/pull/12914)

- 函数 json_object 输入含有空字符串时，返回错误结果。[#13030](https://github.com/StarRocks/starrocks/issues/13030)

- RuntimeFilter 会导致 BE 崩溃。[#12807](https://github.com/StarRocks/starrocks/pull/12807)

- CBO 内过多递归计算导致 FE 挂起。[#12788](https://github.com/StarRocks/starrocks/pull/12788)

- 优雅退出时 BE 可能会崩溃或报错。[#12852](https://github.com/StarRocks/starrocks/pull/12852)

- 添加新列后，删除会造成 Compaction 崩溃的问题。[#12907](https://github.com/StarRocks/starrocks/pull/12907)

- OLAP 外表元数据同步会导致数据不一致。[#12368](https://github.com/StarRocks/starrocks/pull/12368)

- 其中一个 BE 崩溃后，相关查询小概率在其他 BE 一直运行直到超时。[#12954](https://github.com/StarRocks/starrocks/pull/12954)

### 行为变更

- Hive 外表解析出错时，StarRocks 会报错，不会将相关列设置为 NULL。 [#12382](https://github.com/StarRocks/starrocks/pull/12382)

## 2.4.0

发布日期： 2022 年 10 月 20 日

### 新增特性

- 支持构建异步多表物化视图，实现多表 JOIN 查询加速。异步物化视图支持所有[数据模型](../table_design/table_types/table_types.md)。相关文档，请参见 [物化视图](../using_starrocks/Materialized_view.md)。

- 支持通过 INSERT OVERWRITE 语句批量写入并覆盖数据。相关文档，请参见 [INSERT 导入](../loading/InsertInto.md)。

- [公测中] 提供无状态的计算节点（Compute Node，简称 CN 节点）。计算节点支持无状态扩缩容，您可通过 StarRocks Operator 部署，并基于 Kubernetes 管理容器化的计算节点，以此实现自动感知系统负载并水平扩展计算节点。相关文档，请参见使用 StarRocks Operator 在 Kubernetes 部署和管理 CN

- Outer Join 支持通过 `<`、`<=`、`>`、`>=`、`<>` 等比较操作符对多表进行非等值关联。相关文档，请参见 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md)。

- 支持创建 Iceberg Catalog 和 Hudi Catalog，创建后即可查询 Apache Iceberg 和 Apache Hudi 数据。相关文档，请参见 [Iceberg catalog](../data_source/catalog/iceberg_catalog.md) 和 [Hudi catalog](../data_source/catalog/hudi_catalog.md)。

- 支持查询 CSV 格式 Apache Hive™ 表中的 ARRAY 列。相关文档，请参见[外部表](../data_source/External_table.md#创建-hive-外表)。

- 支持通过 DESC 语句查看外部数据的表结构。相关文档，请参见 [DESC](../sql-reference/sql-statements/Utility/DESCRIBE.md)。

- 支持通过 GRANT 或 REVOKE 语句授予或撤销用户特定角色或 IMPERSONATE 权限，并支持通过 EXECUTE AS 语句使用 IMPERSONATE 权限执行当前会话。相关文档，请参见 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 、 [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) 和 [EXECUTE AS](../sql-reference/sql-statements/account-management/EXECUTE_AS.md)。

- 支持 FQDN 访问：您可以用域名或结合主机名与端口的方式作为 FE 或 BE 节点的唯一标识，有效避免因 IP 变更导致无法访问的问题。相关文档，请参见 [启用 FQDN 访问](../administration/enable_fqdn.md)。

- flink-connector-starrocks 支持主键模型 Partial Update。相关文档，请参见[使用 flink-connector-starrocks 导入至 StarRocks](../loading/Flink-connector-starrocks.md)。

- 函数相关：
  - 新增 array_contains_all 函数，用于判断特定数组是否为另一数组的子集。相关文档，请参见[array_contains_all](../sql-reference/sql-functions/array-functions/array_contains_all.md)。
  - 新增 percentile_cont 函数，用于通过线性插值法计算百分位数。相关文档，请参见[percentile_cont](../sql-reference/sql-functions/aggregate-functions/percentile_cont.md)。

### 功能优化

- 主键模型支持持久化 VARCHAR 类型主键索引。自 2.4.0 版本起，主键模型的主键索引磁盘持久化模式和常驻内存模式支持相同的数据类型。

- 优化外表查询性能。
  - 支持查询 Parquet 格式文件时延迟物化，提升小范围过滤场景下的数据湖查询性能。
  - 查询数据湖时，支持通过合并小型 I/O 以降低存储系统的访问延迟，进而提升外表查询性能。

- 优化窗口函数性能。

- Cross Join 支持谓词下推，性能提升。

- 统计信息支持直方图，并进一步完善全量统计信息采集。相关文档，请参见[CBO统计信息](../using_starrocks/Cost_based_optimizer.md)。

- 支持 Tablet 自适应多线程 Scan，降低 Scan 性能对同磁盘 Tablet 数量的依赖，从而可以简化分桶数量的设定。相关文档，请参见[确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

- 支持查询 Apache  Hive 中的压缩文本（.txt）文件。

- 调整了计算默认 PageCache Size 和一致性校验内存的方法，避免多实例部署时的 OOM 问题。

- 去除数据导入主键模型时的 final_merge 操作，主键模型大数据量单批次导入性能提升至两倍。

- 支持 Stream Load 事务接口：支持和 Apache Flink®、Apache Kafka® 等其他系统之间实现跨系统的两阶段提交，并提升高并发 Stream Load 导入场景下的性能。

- 函数相关：
  - 支持在一条 SELECT 语句中使用多个 COUNT(DISTINCT)。相关文档，请参见[count](../sql-reference/sql-functions/aggregate-functions/count.md)。
  - 窗口函数 min 和 max 支持滑动窗口。相关文档，请参见[窗口函数](../sql-reference/sql-functions/Window_function.md#使用-MAX()-窗口函数)。
  - 优化函数 window_funnel 性能。相关文档，请参见[window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)。

### 问题修复

修复了如下 Bug：

- 使用 DESC 查看表结构信息显示的字段类型与创建表指定的字段类型不同。[#7309](https://github.com/StarRocks/starrocks/pull/7309)

- 影响 FE 稳定性的元数据问题。[#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- 导入相关问题：
  - Broker Load 导入时设定 ARRAY 列失败。 [#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - 通过 Broker Load 向非明细模型表导入数据后，副本数据不一致。[#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - 执行 ALTER ROUTINE LOAD 过程中出现 NPE 错误。 [#7804](https://github.com/StarRocks/starrocks/pull/7804)

- 数据湖分析相关问题：
  - 查询 HIVE 外表中 Parquet 格式数据失败。 [#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Elasticsearch 外表 Limit 查询结果不正确。[#9226](https://github.com/StarRocks/starrocks/pull/9226)
  - 查询存有复杂数据类型的 Apache Iceberg 表返回未知错误。[#11298](https://github.com/StarRocks/starrocks/pull/11298)

- Leader FE 节点和 Follower FE 节点间元数据不同步。[#11215](https://github.com/StarRocks/starrocks/pull/11215)

- 当 BITMAP 类型数据大于 2GB 时，BE 停止服务。[#11178](https://github.com/StarRocks/starrocks/pull/11178)

### 行为变更

默认开启 Page Cache，Cache Size 为系统内存大小的 20% 。

### 其他

- 现已正式支持资源隔离功能。
- 现已正式支持 JSON 数据类型及相关函数。
