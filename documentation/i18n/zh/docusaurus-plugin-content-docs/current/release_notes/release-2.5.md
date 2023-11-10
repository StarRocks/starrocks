---
displayed_sidebar: "Chinese"
---

# StarRocks version 2.5

## 2.5.13

发布日期：2023 年 9 月 28 日

### 功能优化

- 窗口函数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、STDDEV_SAMP 支持 ORDER BY 子句和 Window 子句。 [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- DECIMAL 类型数据查询结果越界时，返回报错而不是 NULL。 [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- 执行带有不合法注释的 SQL 命令返回结果与 MySQL 保持一致。[#30210](https://github.com/StarRocks/starrocks/pull/30210)
- 清理已经删除的 Tablet 对应的 Rowset，减少 BE 启动时加载所消耗的内存。 [#30625](https://github.com/StarRocks/starrocks/pull/30625)

### 问题修复

修复了如下问题：

- 使用 Spark Connector 或 Flink Connector 读取 StarRocks 数据时报错："Set cancelled by MemoryScratchSinkOperator"。 [#30702](https://github.com/StarRocks/starrocks/pull/30702) [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- ORDER BY 子句中包含聚合函数时报错："java.lang.IllegalStateException: null"。 [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- 如果有 Inactive 的物化视图，FE 会重启失败。 [#30015](https://github.com/StarRocks/starrocks/pull/30015)
- 有重复的分区时，INSERT OVERWRITE 会把元数据写坏，导致后续 FE 重启失败 。[#27545](https://github.com/StarRocks/starrocks/pull/27545)
- 修改主键模型表中不存在的列会报错："java.lang.NullPointerException: null"。 [#30366](https://github.com/StarRocks/starrocks/pull/30366)
- 向有分区的 StarRocks 外表写入数据时有报错："get TableMeta failed from TNetworkAddress"。 [#30124](https://github.com/StarRocks/starrocks/pull/30124)
- 某些场景下，CloudCanal 导入数据会报错。 [#30799](https://github.com/StarRocks/starrocks/pull/30799)
- 通过 Flink Connector 写入，或者执行 DELETE、INSERT 等操作时报错："current running txns on db xxx is 200, larger than limit 200"。 [#18393](https://github.com/StarRocks/starrocks/pull/18393)
- 使用 HAVING 子句中包含聚合函数的查询创建的异步物化视图无法正确改写查询。 [#29976](https://github.com/StarRocks/starrocks/pull/29976)

## 2.5.12

发布日期：2023 年 9 月 4 日

### 功能优化

- Audit Log 文件中会保留 SQL 中的 Comment 信息。 [#29747](https://github.com/StarRocks/starrocks/pull/29747)
- Audit Log 中增加了 INSERT INTO SELECT 的 CPU 和 Memory 统计信息。 [#29901](https://github.com/StarRocks/starrocks/pull/29901)

### 问题修复

修复了如下问题：

- 使用 Broker Load 导入数据时，某些字段的 NOT NULL 约束会导致 BE crash 或者报 "msg:mismatched row count" 错误。 [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- 由于未合入上游 Apache ORC 的 BugFix ORC-1304（[apache/orc#1299](https://github.com/apache/orc/pull/1299)）而导致 ORC 文件查询失败。 [#29804](https://github.com/StarRocks/starrocks/pull/29804)
- 主键模型表 Restore 之后，BE 重启后元数据发生错误，导致元数据不一致。 [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 2.5.11

发布日期：2023 年 8 月 28 日

### 功能优化

- 对所有复合谓词以及 WHERE 子句中的表达式支持隐式转换，可通过[会话变量](https://docs.starrocks.io/zh-cn/latest/reference/System_variable) `enable_strict_type` 控制是否打开隐式转换（默认取值为 `false`）。 [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- 优化了创建 Iceberg Catalog 时如果没有指定 `hive.metastore.uri` 时返回的报错，报错信息中的描述更准确。 [#16543](https://github.com/StarRocks/starrocks/issues/16543)
- 在报错信息 `xxx too many versions xxx` 中增加了如何处理的建议说明。 [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- 动态分区新增支持分区粒度为年。 [#28386](https://github.com/StarRocks/starrocks/pull/28386)

### 问题修复

修复了如下问题：

- 向多副本的表中导入数据时，如果某些分区没有数据，则会写入很多无用日志。 [#28824](https://github.com/StarRocks/starrocks/pull/28824)
- DELETE 时如果 WHERE 条件中字段类型是 BITMAP 或 HLL 会失败。 [#28592](https://github.com/StarRocks/starrocks/pull/28592)
- 当异步物化视图的刷新策略为手动刷新且同步调用刷新任务（SYNC MODE）时，手动刷新后`information_schema.task_run` 表中有多条 INSERT OVERWRITE 的记录。 [#28060](https://github.com/StarRocks/starrocks/pull/28060)
- 某个 Tablet 出现某种 ERROR 状态之后触发 Clone 操作，会导致磁盘使用率上升。 [#28488](https://github.com/StarRocks/starrocks/pull/28488)
- 开启 Join Reorder 时，查询列如果是常量，查询结果不正确。 [#29239](https://github.com/StarRocks/starrocks/pull/29239)
- 冷热数据迁移时下发任务过多，导致 BE OOM。 [#29055](https://github.com/StarRocks/starrocks/pull/29055)
- `/apache_hdfs_broker/lib/log4j-1.2.17.jar` 存在安全漏洞。 [#28866](https://github.com/StarRocks/starrocks/pull/28866)
- Hive Catalog 查询时，如果 WHERE 子句中使用分区列且包含 OR 条件，查询结果不正确。 [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- 查询时偶尔会报错 "java.util.ConcurrentModificationException: null"。 [#29296](https://github.com/StarRocks/starrocks/pull/29296)
- 异步物化视图的基表被删除，FE 重启时会报错。 [#29318](https://github.com/StarRocks/starrocks/pull/29318)
- 跨库异步物化视图的基表在数据写入时，偶尔会出现 FE leader 死锁的情况。 [#29432](https://github.com/StarRocks/starrocks/pull/29432)

## 2.5.10

发布日期：2023 年 8 月 7 日

### 新增特性

- 支持聚合函数 [COVAR_SAMP](../sql-reference/sql-functions/aggregate-functions/covar_samp.md)、[COVAR_POP](../sql-reference/sql-functions/aggregate-functions/covar_pop.md)、[CORR](../sql-reference/sql-functions/aggregate-functions/corr.md)。
- 支持[窗口函数](../sql-reference/sql-functions/Window_function.md) COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、 STDDEV_SAMP。

### 功能优化

- 优化 TabletChecker 的调度逻辑，避免重复调度暂时无法修复的 Tablet。 [#27648](https://github.com/StarRocks/starrocks/pull/27648)
- 优化了 Schema Change 和 Routine load 同时执行，当 Schema Change 先完成时可能引起 Routine Load 失败时的报错信息。[#28425](https://github.com/StarRocks/starrocks/pull/28425)
- 禁止创建外表时定义 Not Null 列（如果原来定义了 Not Null 列，升级后会报错，需重建表）。建议 2.3.0 版本后使用 Catalog，不要再使用外表。 [#25485](https://github.com/StarRocks/starrocks/pull/25441)
- 增加 Broker Load 重试过程中出现报错时的错误信息，方便导入出现问题时进行排查调试。 [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- 主键模型导入时，包含 UPSERT 和 DELETE 的大量数据写入也可以支持。 [#17264](https://github.com/StarRocks/starrocks/pull/17264)
- 优化物化视图改写功能。[#27934](https://github.com/StarRocks/starrocks/pull/27934) [#25542](https://github.com/StarRocks/starrocks/pull/25542) [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#27557](https://github.com/StarRocks/starrocks/pull/27557)  [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#26957](https://github.com/StarRocks/starrocks/pull/26957) [#27728](https://github.com/StarRocks/starrocks/pull/27728) [#27900](https://github.com/StarRocks/starrocks/pull/27900)

### 问题修复

修复了如下问题：

- 使用 cast 函数将字符串转换为数组时，如果输入包括常量，无法返回正确结果。 [#19793](https://github.com/StarRocks/starrocks/pull/19793)
- SHOW TABLET 如果包括 ORDER BY 和 LIMIT 返回结果不正确。 [#23375](https://github.com/StarRocks/starrocks/pull/23375)
- 物化视图 Outer join 和 Anti join 改写错误。 [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- FE 中表级别 scan 统计信息错误，导致表查询和导入的 metrics 信息不正确。 [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- 如果 HMS 上通过配置事件侦听器来自动增量更新 Hive 元数据，FE 日志中会报 `An exception occurred when using the current long link to access metastore. msg: Failed to get next notification based on last event id: 707602`。 [#21056](https://github.com/StarRocks/starrocks/pull/21056)
- 分区表中修改 sort key 列后查询结果不稳定。 [#27850](https://github.com/StarRocks/starrocks/pull/27850)
- 使用 DATE，DATETIME，DECIMAL 做分桶列时，Spark Load 导入数据会被导入到错误的分桶。 [#27005](https://github.com/StarRocks/starrocks/pull/27005)
- 某些情况下 regex_replace 函数会导致 BE crash。 [#27117](https://github.com/StarRocks/starrocks/pull/27117)
- sub_bitmap 函数的参数取值不是 BITMAP 类型时，会导致 BE crash。 [#27982](https://github.com/StarRocks/starrocks/pull/27982)
- 开启 Join Reorder 后，某些情况下查询会报 unknown error。 [#27472](https://github.com/StarRocks/starrocks/pull/27472)
- 主键模型部分列更新时平均 row size 预估不准导致内存占用过多。 [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- 低基数优化开启时，某些情况下 INSERT 导入报错 `[42000][1064] Dict Decode failed, Dict can't take cover all key :0`。 [#26463](https://github.com/StarRocks/starrocks/pull/26463)
- 使用 Broker Load 从 HDFS 导入数据时，如果作业中认证方式 (`hadoop.security.authentication`) 设置为 `simple` 则会导致作业失败。 [#27774](https://github.com/StarRocks/starrocks/pull/27774)
- 物化视图在修改刷新模式时会导致元数据不一致。[#28082](https://github.com/StarRocks/starrocks/pull/28082) [#28097](https://github.com/StarRocks/starrocks/pull/28097)
- 使用 SHOW CREATE CATALOG、SHOW RESOURCES 查看某些特殊信息时，PASSWORD 未被隐藏。 [#28059](https://github.com/StarRocks/starrocks/pull/28059)
- LabelCleaner 线程卡死导致 FE 内存泄漏。 [#28311](https://github.com/StarRocks/starrocks/pull/28311)

## 2.5.9

发布日期：2023 年 7 月 19 日

### 新增特性

- 查询和物化视图的 Join 类型不同时，也支持对查询进行改写。[#25099](https://github.com/StarRocks/starrocks/pull/25099)

### 功能优化

- 禁止创建目标集群是当前集群的 StarRocks 外表。[#25441](https://github.com/StarRocks/starrocks/pull/25441)
- 如果查询的字段不包含在物化视图的 output 列但是包含在其谓词条件中，仍可使用该物化视图进行查询改写。[#23028](https://github.com/StarRocks/starrocks/issues/23028)
- `Information_schema.tables_config` 表中增加了 `table_id` 字段。您可以基于 `table_id` 字段关联数据库 `Information_schema` 中的表 `tables_config` 和 `be_tablets`，来查询 tablet 所属数据库和表。[#24061](https://github.com/StarRocks/starrocks/pull/24061)

### 问题修复

修复了如下问题：

- 明细模型表 Count Distinct 结果异常。[#24222](https://github.com/StarRocks/starrocks/pull/24222)
- 当 Join 列是 BINARY 类型且过大时 BE 会 crash。[#25084](https://github.com/StarRocks/starrocks/pull/25084)
- 插入数据长度超出建表时 STRUCT 定义的 CHAR 长度时，插入无响应。 [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- Coalesce 函数查询结果不正确。[#26250](https://github.com/StarRocks/starrocks/pull/26250)
- Restore 后同一个 tablet 在 BE 和 FE 上的 version 不一致。[#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- Recover 的表自动创建分区失败。[#26813](https://github.com/StarRocks/starrocks/pull/26813)

## 2.5.8

发布日期：2023 年 6 月 30 日

### 功能优化

- 优化了非分区表增加分区时的报错信息。 [#25266](https://github.com/StarRocks/starrocks/pull/25266)
- 优化表的[自动分桶策略](../table_design/Data_distribution.md#确定分桶数量)。 [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- 优化建表时 Comment 中的默认值。[#24803](https://github.com/StarRocks/starrocks/pull/24803)
- 优化异步物化视图的手动刷新策略。支持通过 REFRESH MATERIALIZED VIEW WITH SYNC MODE 同步调用物化视图刷新任务。[#25910](https://github.com/StarRocks/starrocks/pull/25910)

### 问题修复

修复了如下问题：

- 异步物化视图在创建时包含 Union，会导致 Count 结果不准确。 [#24460](https://github.com/StarRocks/starrocks/issues/24460)
- 强制重置 root 密码时报 "Unknown error" 错误。 [#25492](https://github.com/StarRocks/starrocks/pull/25492)
- 集群不满足 3 个 Alive BE 时，INSERT OVERWRITE 报错信息不准确。 [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 2.5.7

发布日期：2023 年 6 月 14 日

### 新增特性

- 失效物化视图支持通过 `ALTER MATERIALIZED VIEW <mv_name> ACTIVE` 手动激活，可用于激活因基表 (base table) 被删除而失效的物化视图。更多信息，参见 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md)。[#24001](https://github.com/StarRocks/starrocks/pull/24001)

- 支持在建表和新增分区时自动设置适当的分桶数量。更多信息，参见[确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。[#10614](https://github.com/StarRocks/starrocks/pull/10614)

### 功能优化

- 优化外表 Scan 节点的 I/O 并发数量，减少内存使用，并限制内存使用比例，提升外表导入稳定性。[#23617](https://github.com/StarRocks/starrocks/pull/23617) [#23624](https://github.com/StarRocks/starrocks/pull/23624) [#23626](https://github.com/StarRocks/starrocks/pull/23626)
- 优化 Broker Load 的报错信息，增加出错文件的名称提示以及重试信息。[#18038](https://github.com/StarRocks/starrocks/pull/18038) [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- 优化 CREATE TABLE 的超时报错信息，增加参数调整建议。[#24510](https://github.com/StarRocks/starrocks/pull/24510)
- 优化因表状态为非 Normal 导致 ALTER TABLE 失败场景下的报错信息。[#24381](https://github.com/StarRocks/starrocks/pull/24381)
- 建表时忽略中文空格。[#23885](https://github.com/StarRocks/starrocks/pull/23885)
- 优化 Broker 访问超时时间从而降低 Broker Load 导入失败率。[#22699](https://github.com/StarRocks/starrocks/pull/22699)
- 主键模型 SHOW TABLET 返回的 `VersionCount` 字段包含 Pending 状态的 Rowsets。[#23847](https://github.com/StarRocks/starrocks/pull/23847)
- 优化 Persistent Index 策略。[#22140](https://github.com/StarRocks/starrocks/pull/22140)

### 问题修复

修复了如下问题：

- 向 StarRocks 导入 Parquet 格式数据时，因日期类型转换溢出而产生数据错误。 [#22356](https://github.com/StarRocks/starrocks/pull/22356)
- 禁用动态分区后，分桶信息丢失。[#22595](https://github.com/StarRocks/starrocks/pull/22595)
- 创建分区表时指定不支持的 Properties 会导致空指针。[#21374](https://github.com/StarRocks/starrocks/issues/21374)
- Information Schema 中的表权限过滤失效。[#23804](https://github.com/StarRocks/starrocks/pull/23804)
- SHOW TABLE STATUS 结果展示不全。[#24279](https://github.com/StarRocks/starrocks/issues/24279)
- Schema change 和数据导入同时进行时 Schema change 偶尔会卡住。[#23456](https://github.com/StarRocks/starrocks/pull/23456)
- 因为等待 RocksDB WAL flush 导致 brpc worker 同步等待，从而无法切换处理其他 bthread，导致主键模型高频导入出现断点。[#22489](https://github.com/StarRocks/starrocks/pull/22489)
- 建表时可以成功创建非法数据类型 TIME 列。 [#23474](https://github.com/StarRocks/starrocks/pull/23474)
- 物化视图 Union 查询改写失败。[#22922](https://github.com/StarRocks/starrocks/pull/22922)

## 2.5.6

发布日期：2023 年 5 月 19 日

### 功能优化

- 优化了因 `thrift_server_max_worker_threads` 过小导致 INSERT INTO SELECT 超时场景下的报错信息。 [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- CTAS 创建的表与普通表一致，默认为 3 副本。 [#22854](https://github.com/StarRocks/starrocks/pull/22854)

### 问题修复

- Truncate 操作对分区名大小写敏感导致 Truncate Partition 失败。 [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- 物化视图创建临时分区失败导致 BE 下线卡住。 [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- 动态修改 FE 参数时，不支持设置空 array。 [#22225](https://github.com/StarRocks/starrocks/pull/22225)
- 设置了 `partition_refresh_number` property 的物化视图可能无法完全刷新完成。[#21619](https://github.com/StarRocks/starrocks/pull/21619)
- SHOW CREATE TABLE 导致内存中鉴权信息错误。[#21311](https://github.com/StarRocks/starrocks/pull/21311)
- 在外表查询中，对于部分 ORC 文件，谓词会失效。[#21901](https://github.com/StarRocks/starrocks/pull/21901)
- 过滤条件无法正确处理列名大小写问题。[#22626](https://github.com/StarRocks/starrocks/pull/22626)
- 延迟物化导致查询复杂数据类型（STRUCT 或 MAP）错误。[#22862](https://github.com/StarRocks/starrocks/pull/22862)
- 主键模型表在备份恢复中出现的问题。[#23384](https://github.com/StarRocks/starrocks/pull/23384)

## 2.5.5

发布日期：2023 年 4 月 28 日

### 新增特性

新增对主键模型表 tablet 状态的监控，包括：

- FE 新增 `err_state_metric` 监控项。
- `SHOW PROC '/statistic/'` 返回结果中新增统计列 `ErrorStateTabletNum`，用于统计错误状态 (err_state) 的 Tablet 数量。
- `SHOW PROC '/statistic/<db_id>/'` 返回结果中新增统计列 `ErrorStateTablets`，用于展示当前数据库下处于错误状态的 Tablet ID。[# 19517](https://github.com/StarRocks/starrocks/pull/19517)

更多信息，参见 [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW_PROC.md)。

### 功能优化

- 优化添加多个 BE 时的磁盘均衡速度。[# 19418](https://github.com/StarRocks/starrocks/pull/19418)
- 优化 `storage_medium` 的推导机制。当 BE 同时使用 SSD 和 HDD 作为存储介质时，根据 `storage_cooldown_time` 的配置来决定默认存储类型。如果配置了 `storage_cooldown_time`，StarRocks 设置 `storage_medium` 为 `SSD`。如果未配置，则设置 `storage_medium` 为 `HDD`。[#18649](https://github.com/StarRocks/starrocks/pull/18649)
- 通过禁止收集 Unique Key 表的 Value 列统计信息来优化 Unique Key 表性能。[# 19563](https://github.com/StarRocks/starrocks/pull/19563)

### 问题修复

- 对于 Colocation 表，可以通过命令手动指定副本状态为 bad：`ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`，如果 BE 数量小于等于副本数量，则该副本无法被修复。[# 17876](https://github.com/StarRocks/starrocks/issues/17876)
- 启动 BE 后进程存在但是端口无法启动。[# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- 子查询使用窗口函数时，聚合结果不准确。[# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- 首次刷新物化视图时 `auto_refresh_partitions_limit` 设置的限制不生效，导致所有分区都做了刷新。  [# 19759](https://github.com/StarRocks/starrocks/issues/19759)
- 查询 CSV 格式的 Hive 表时，由于 ARRAY 数组中嵌套了复杂数据类型 (MAP 和 STRUCT)而导致的问题。[# 20233](https://github.com/StarRocks/starrocks/pull/20233)
- 使用 Spark connector 查询超时。[# 20264](https://github.com/StarRocks/starrocks/pull/20264)
- 两副本的表如果其中一个副本出现问题，无法自动修复。[# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- 物化视图查询改写失败而导致查询失败。[# 19549](https://github.com/StarRocks/starrocks/issues/19549)
- 因 db 锁引起的 metrics 接口超时。[# 20790](https://github.com/StarRocks/starrocks/pull/20790)
- Broadcast Join 查询结果错误。[# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- 建表时使用不支持的数据类型时返回空指针。[# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- 开启 Query Cache 后使用 window_funnel 函数导致的问题。[# 21474](https://github.com/StarRocks/starrocks/issues/21474)
- CTE 优化查询改写后导致选择优化计划耗时过长。[# 16515](https://github.com/StarRocks/starrocks/pull/16515)

## 2.5.4

发布日期： 2023 年 4 月 4 日

### 功能优化

- 优化查询规划阶段物化视图查询改写的性能，降低约 70% 的规划耗时。[#19579](https://github.com/StarRocks/starrocks/pull/19579)
- 优化类型推断，如果查询 `SELECT sum(CASE WHEN XXX)FROM xxx;` 中包含常量 `0`，例如 `SELECT sum(CASE WHEN k1 = 1 THEN v1 ELSE 0 END) FROM test;`，则预聚合自动开启以加速查询。[#19474](https://github.com/StarRocks/starrocks/pull/19474)
- 支持使用 `SHOW CREATE VIEW` 查看物化视图的创建语句。[#19999](https://github.com/StarRocks/starrocks/pull/19999)
- BE 节点之间单次 bRPC 请求支持传输超过 2 GB 的数据包。[#20283](https://github.com/StarRocks/starrocks/pull/20283) [#20230](https://github.com/StarRocks/starrocks/pull/20230)
- External Catalog 支持通过 [SHOW CREATE CATALOG](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md) 查看 Catalog 的创建信息。

### 问题修复

修复了如下问题：

- 物化视图查询改写后，低基数全局字典优化不生效。[#19615](https://github.com/StarRocks/starrocks/pull/19615)
- 物化视图查询无法改写，导致查询失败。[#19774](https://github.com/StarRocks/starrocks/pull/19774)
- 基于主键模型或更新模型的表创建物化视图，物化视图查询无法改写。[#19600](https://github.com/StarRocks/starrocks/pull/19600)
- 物化视图的列名大小写敏感， 建表时 `PROPERTIES` 中列名大小写错误，仍然返回建表成功，未能返回报错提示，并且基于该表的物化视图查询无法改写。[#19780](https://github.com/StarRocks/starrocks/pull/19780)
- 物化视图查询改写后，执行计划中可能产生基于分区列的无效谓词，影响查询速度。[#19784](https://github.com/StarRocks/starrocks/pull/19784)
- 导入数据至新创建的分区后，物化视图查询可能无法改写。[#20323](https://github.com/StarRocks/starrocks/pull/20323)
- 创建物化视图时配置 `"storage_medium" = "SSD"` ，导致物化视图刷新失败。[#19539](https://github.com/StarRocks/starrocks/pull/19539) [#19626](https://github.com/StarRocks/starrocks/pull/19626)
- 主键模型的表可能会并行 Compaction。[#19692](https://github.com/StarRocks/starrocks/pull/19692)
- 大量 DELETE 操作后 Compaction 不及时。[#19623](https://github.com/StarRocks/starrocks/pull/19623)
- 如果语句的表达式中含有多个低基数列时，表达式改写可能出错，进而导致低基数全局字典优化不生效。[#20161](https://github.com/StarRocks/starrocks/pull/20161)

## 2.5.3

发布日期： 2023 年 3 月 10 日

### 功能优化

- 优化物化视图的查询改写：
  - 支持对 Outer Join 和 Cross Join 的查询改写。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - 优化物化视图数据扫描逻辑，进一步加速查询。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - 增强了单表聚合查询的改写能力。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - 增强了 View Delta 场景的改写能力。即，查询的关联表为物化视图的关联表的子集时的改写能力。 [#18800](https://github.com/StarRocks/starrocks/pull/18800)
- 优化使用 Rank 窗口函数作为过滤条件或排序键的性能和内存占用。 [#17553](https://github.com/StarRocks/starrocks/issues/17553)

### 问题修复

修复了如下问题：

- ARRAY 类型空字面量导致的问题。 [#18563](https://github.com/StarRocks/starrocks/pull/18563)
- 某些复杂查询场景下，低基数字典可能被误用。增加了应用低基数字典优化前的检查来避免此类错误。 [#17318](https://github.com/StarRocks/starrocks/pull/17318)
- 在单 BE 环境下，Local Shuffle 导致 GROUP BY 包含重复结果。 [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- 创建**非分区**物化视图时错误使用**分区**相关参数。增加物化视图创建检查，如果创建的是非分区物化视图，则自动禁用分区相关参数。 [#18741](https://github.com/StarRocks/starrocks/pull/18741)
- Parquet Repetition Column 的解析问题。 [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- 列的 nullable 信息获取错误。在使用 CTAS 创建主键模型表时，仅将主键列设置为 non-nullable，非主键列设置为 nullable。 [#16431](https://github.com/StarRocks/starrocks/pull/16431)
- 删除主键模型表数据可能导致的问题。 [#18768](https://github.com/StarRocks/starrocks/pull/18768)

## 2.5.2

发布日期： 2023 年 2 月 21 日

### 新增特性

- 访问 AWS S3 以及 AWS Glue 时支持基于 Instance Profile 和 Assumed Role 来进行认证和鉴权。 [#15958](https://github.com/StarRocks/starrocks/pull/15958)
- 新增 3 个 bit 函数：[bit_shift_left](../sql-reference/sql-functions/bit-functions/bit_shift_left.md)，[bit_shift_right](../sql-reference/sql-functions/bit-functions/bit_shift_right.md)，[bit_shift_right_logical](../sql-reference/sql-functions/bit-functions/bit_shift_right_logical.md)。 [#14151](https://github.com/StarRocks/starrocks/pull/14151)

### 功能优化

- 优化了部分内存释放的逻辑，在查询中包含大量的聚合查询时可以显著的降低内存峰值使用。 [#16913](https://github.com/StarRocks/starrocks/pull/16913)
- 优化了排序的内存使用，对于部分带有窗口函数的查询或者是排序查询，可以减少一倍以上的内存消耗。 [#16937](https://github.com/StarRocks/starrocks/pull/16937) [#17362](https://github.com/StarRocks/starrocks/pull/17362) [#17408](https://github.com/StarRocks/starrocks/pull/17408)

### 问题修复

修复了如下问题：

- 无法刷新含有 MAP 或 ARRAY 数据类型的 Apache Hive 外表。[#17548](https://github.com/StarRocks/starrocks/pull/17548)
- Superset 无法识别出物化视图列类型。[#17686](https://github.com/StarRocks/starrocks/pull/17686)
- 在对接 BI 时因无法解析 SET GLOBAL/SESSION TRANSACTION 而导致的连接性问题。[#17295](https://github.com/StarRocks/starrocks/pull/17295)
- Colocate 组内的动态分区表无法修改分桶数，并返回报错信息。[#17418](https://github.com/StarRocks/starrocks/pull/17418/)
- 修复了在 prepare 阶段失败可能导致的潜在问题。 [#17323](https://github.com/StarRocks/starrocks/pull/17323)

### 行为变更

- 修改 FE 参数 `enable_experimental_mv` 默认值为 `true`，即异步物化视图功能默认开启。
- 新增保留关键字 CHARACTER。[#17488](https://github.com/StarRocks/starrocks/pull/17488)

## 2.5.1

发布日期： 2023 年 2 月 5 日

### 功能优化

- 外表物化视图支持查询改写。 [#11116](https://github.com/StarRocks/starrocks/issues/11116)[#15791](https://github.com/StarRocks/starrocks/issues/15791)
- CBO 自动全量采集支持用户设置采集时间段，防止因集中采集而导致的集群性能抖动。 [#14996](https://github.com/StarRocks/starrocks/pull/14996)
- 增加 Thrift server 队列，避免 INSERT INTO SELECT 时因为 Thrift server 中的请求过于繁忙而失败。 [#14571](https://github.com/StarRocks/starrocks/pull/14571)
- 建表时如果没有显式指定 `storage_medium` 属性，则系统根据 BE 节点磁盘类型自动推导并设定表的存储类型。参见[CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 中的参数描述。[#14394](https://github.com/StarRocks/starrocks/pull/14394)

### 问题修复

修复了如下问题：

- SET PASSWORD 导致空指针。 [#15247](https://github.com/StarRocks/starrocks/pull/15247)
- 当 KEY 为空时无法解析 JSON 数据。 [#16852](https://github.com/StarRocks/starrocks/pull/16852)
- 非法数据类型可以成功转换至 ARRAY 类型。 [#16866](https://github.com/StarRocks/starrocks/pull/16866)
- 异常场景下 Nested Loop Join 无法中断。  [#16875](https://github.com/StarRocks/starrocks/pull/16875)

### 行为变更

- 取消 FE 参数 `default_storage_medium`，表的存储介质改为系统自动推导。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.5.0

发布日期： 2023 年 1 月 22 日

### 新增特性

- [Hudi catalog](../data_source/catalog/hudi_catalog.md) 和 [Hudi 外部表](../data_source/External_table.md#deprecated-hudi-外部表)支持查询 Merge On Read 表。[#6780](https://github.com/StarRocks/starrocks/pull/6780)
- [Hive catalog](../data_source/catalog/hive_catalog.md)、Hudi catalog 和 [Iceberg catalog](../data_source/catalog/iceberg_catalog.md) 支持查询 STRUCT 和 MAP 类型数据。[#10677](https://github.com/StarRocks/starrocks/issues/10677)
- 提供 [Data Cache](../data_source/data_cache.md) 特性，在查询外部 HDFS 或对象存储上的热数据时，大幅优化数据查询效率。[#11597](https://github.com/StarRocks/starrocks/pull/11579)
- 支持 [Delta Lake catalog](../data_source/catalog/deltalake_catalog.md)，无需导入数据或创建外部表即可查询 Delta Lake 数据。[#11972](https://github.com/StarRocks/starrocks/issues/11972)
- Hive catalog、Hudi catalog 和 Iceberg catalog 兼容 AWS Glue。[#12249](https://github.com/StarRocks/starrocks/issues/12249)
- 支持通过[文件外部表](../data_source/file_external_table.md)查询 HDFS 或对象存储上的 Parquet 和 ORC 文件。[#13064](https://github.com/StarRocks/starrocks/pull/13064)
- 支持基于 Hive、Hudi 或 Iceberg catalog 创建物化视图，以及基于物化视图创建物化视图。相关文档，请参见[物化视图](../using_starrocks/Materialized_view.md)。[#11116](https://github.com/StarRocks/starrocks/issues/11116) [#11873](https://github.com/StarRocks/starrocks/pull/11873)
- 主键模型表支持条件更新。相关文档，请参见[通过导入实现数据变更](../loading/Load_to_Primary_Key_tables.md#条件更新)。[#12159](https://github.com/StarRocks/starrocks/pull/12159)
- 支持 [Query Cache](../using_starrocks/query_cache.md)，通过保存查询的中间计算结果提升简单高并发查询的 QPS 并降低平均时延。[#9194](https://github.com/StarRocks/starrocks/pull/9194)
- 支持为 Broker Load 作业指定优先级。相关文档，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。[#11029](https://github.com/StarRocks/starrocks/pull/11029)
- 支持为 StarRocks 原生表手动设置数据导入的副本数。相关文档，请参见 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。[#11253](https://github.com/StarRocks/starrocks/pull/11253)
- 支持查询队列功能。相关文档，请参见[查询队列](../administration/query_queues.md)。[#12594](https://github.com/StarRocks/starrocks/pull/12594)
- 支持通过资源组对导入计算进行资源隔离，从而间接控制导入任务对集群资源的消耗。相关文档，请参见[资源隔离](../administration/resource_group.md)。[#12606](https://github.com/StarRocks/starrocks/pull/12606)
- 支持为 StarRocks 原生表手动设置数据压缩算法：LZ4、Zstd、Snappy 和 Zlib。相关文档，请参见[数据压缩](../table_design/data_compression.md)。[#10097](https://github.com/StarRocks/starrocks/pull/10097) [#12020](https://github.com/StarRocks/starrocks/pull/12020)
- 支持[用户自定义变量](../reference/user_defined_variables.md) (user-defined variables)。[#10011](https://github.com/StarRocks/starrocks/pull/10011)
- 支持 [Lambda 表达式](../sql-reference/sql-functions/Lambda_expression.md)及高阶函数，包括：[array_map](../sql-reference/sql-functions/array-functions/array_map.md)、[array_sum](../sql-reference/sql-functions/array-functions/array_sum.md) 和 [array_sortby](../sql-reference/sql-functions/array-functions/array_sortby.md)。[#9461](https://github.com/StarRocks/starrocks/pull/9461) [#9806](https://github.com/StarRocks/starrocks/pull/9806) [#10323](https://github.com/StarRocks/starrocks/pull/10323) [#14034](https://github.com/StarRocks/starrocks/pull/14034)
- [窗口函数](../sql-reference/sql-functions/Window_function.md)中支持使用 QUALIFY 来筛选查询结果。[#13239](https://github.com/StarRocks/starrocks/pull/13239)
- 建表时，支持指定 uuid 或 uuid_numeric 函数返回的结果作为列默认值。相关文档，请参见 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md)。[#11155](https://github.com/StarRocks/starrocks/pull/11155)
- 新增以下函数：[map_size](../sql-reference/sql-functions/map-functions/map_size.md)、[map_keys](../sql-reference/sql-functions/map-functions/map_keys.md)、[map_values](../sql-reference/sql-functions/map-functions/map_values.md)、[max_by](../sql-reference/sql-functions/aggregate-functions/max_by.md)、[sub_bitmap](../sql-reference/sql-functions/bitmap-functions/sub_bitmap.md)、[bitmap_to_base64](../sql-reference/sql-functions/bitmap-functions/bitmap_to_base64.md)、[host_name](../sql-reference/sql-functions/utility-functions/host_name.md) 和 [date_slice](../sql-reference/sql-functions/date-time-functions/date_slice.md)。[#11299](https://github.com/StarRocks/starrocks/pull/11299) [#11323](https://github.com/StarRocks/starrocks/pull/11323) [#12243](https://github.com/StarRocks/starrocks/pull/12243) [#11776](https://github.com/StarRocks/starrocks/pull/11776) [#12634](https://github.com/StarRocks/starrocks/pull/12634) [#14225](https://github.com/StarRocks/starrocks/pull/14225)

### 功能优化

- 优化了 [Hive catalog](../data_source/catalog/hive_catalog.md)、[Hudi catalog](../data_source/catalog/hudi_catalog.md) 和 [Iceberg catalog](../data_source/catalog/iceberg_catalog.md) 的元数据访问速度。[#11349](https://github.com/StarRocks/starrocks/issues/11349)
- [Elasticsearch 外部表](../data_source/External_table.md#deprecated-elasticsearch-外部表)支持查询 ARRAY 类型数据。[#9693](https://github.com/StarRocks/starrocks/pull/9693)
- 物化视图优化
  - 异步物化视图支持 SPJG 类型的物化视图查询的自动透明改写。相关文档，请参见[物化视图](../using_starrocks/Materialized_view.md#使用异步物化视图改写加速查询)。[#13193](https://github.com/StarRocks/starrocks/issues/13193)
  - 异步物化视图支持多种异步刷新机制。相关文档，请参见[物化视图](../using_starrocks/Materialized_view.md)。[#12712](https://github.com/StarRocks/starrocks/pull/12712) [#13171](https://github.com/StarRocks/starrocks/pull/13171) [#13229](https://github.com/StarRocks/starrocks/pull/13229) [#12926](https://github.com/StarRocks/starrocks/pull/12926)
  - 优化了物化视图的刷新效率。[#13167](https://github.com/StarRocks/starrocks/issues/13167)
- 导入优化
  - 优化多副本导入性能，支持 single leader replication 模式，导入性能提升 1 倍。关于该模式的详细信息，参见 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 的 `replicated_storage` 参数。[#10138](https://github.com/StarRocks/starrocks/pull/10138)
  - 在单 HDFS 集群或单 Kerberos 用户下无需部署 broker 即可通过 Broker Load 或 Spark Load 进行数据导入。如果您配置了多个 HDFS 集群或者多个 Kerberos 用户，需要继续通过 Broker 进程执行导入。相关文档，请参见[从 HDFS 或外部云存储系统导入数据](../loading/BrokerLoad.md)和[使用 Apache Spark™ 批量导入](../loading/SparkLoad.md)。[#9049](https://github.com/starrocks/starrocks/pull/9049) [#9228](https://github.com/StarRocks/starrocks/pull/9228)
  - 优化了 Broker Load 在大量 ORC 小文件场景下的导入性能。[#11380](https://github.com/StarRocks/starrocks/pull/11380)
  - 优化了向主键模型表导入数据时的内存占用。[#12068](https://github.com/StarRocks/starrocks/pull/12068)
- 优化了 StarRocks 内置的 `information_schema` 数据库以及其中的 `tables` 表和 `columns` 表；新增 `table_config` 表。相关文档，请参见 [Information Schema](../administration/information_schema.md)。[#10033](https://github.com/StarRocks/starrocks/pull/10033)
- 优化备份恢复：
  - 支持数据库级别的备份恢复。相关文档，请参见[备份与恢复](../administration/Backup_and_restore.md)。[#11619](https://github.com/StarRocks/starrocks/issues/11619)
  - 支持主键模型表的备份恢复。相关文档，请参见备份与恢复。[#11885](https://github.com/StarRocks/starrocks/pull/11885)
- 函数优化：
  - [time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md) 增加参数，可以计算时间区间的起始点和终点。[#11216](https://github.com/StarRocks/starrocks/pull/11216)
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md) 支持严格递增模式，防止计算重复的时间戳。[#10134](https://github.com/StarRocks/starrocks/pull/10134)
  - [unnest](../sql-reference/sql-functions/array-functions/unnest.md) 支持变参。[#12484](https://github.com/StarRocks/starrocks/pull/12484)
  - lead 和 lag 窗口函数支持查询 HLL 和 BITMAP 类型数据。相关文档，请参见[窗口函数](../sql-reference/sql-functions/Window_function.md)。[#12108](https://github.com/StarRocks/starrocks/pull/12108)
  - [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md)、[array_sort](../sql-reference/sql-functions/array-functions/array_sort.md)、[array_concat](../sql-reference/sql-functions/array-functions/array_concat.md)、[array_slice](../sql-reference/sql-functions/array-functions/array_slice.md) 和 [reverse](../sql-reference/sql-functions/string-functions/reverse.md) 函数支持查询 JSON 数据。[#13155](https://github.com/StarRocks/starrocks/pull/13155)
  - `current_date`、`current_timestamp`、`current_time`、`localtimestamp`、`localtime` 函数后不加`()`即可执行。例如 `select current_date;`。[#14319](https://github.com/StarRocks/starrocks/pull/14319)
- 优化了 FE 日志，去掉了部分冗余信息。[#15374](https://github.com/StarRocks/starrocks/pull/15374)

### 问题修复

修复了如下问题：

- append_trailing_char_if_absent 函数对空值操作有误。[#13762](https://github.com/StarRocks/starrocks/pull/13762)
- 使用 RECOVER 语句恢复删除的表后，表不存在。[#13921](https://github.com/StarRocks/starrocks/pull/13921)
- SHOW CREATE MATERIALIZED VIEW 返回的结果缺少 catalog 及 database 信息。 [#12833](https://github.com/StarRocks/starrocks/pull/12833)
- waiting_stable 状态下的 schema change 任务无法取消。 [#12530](https://github.com/StarRocks/starrocks/pull/12530)
- `SHOW PROC '/statistic';` 命令在 leader FE 和非 leader FE 上返回的结果不同。 [#12491](https://github.com/StarRocks/starrocks/issues/12491)
- FE 生成的执行计划缺少 partition ID，导致 BE 获取 Hive partition 数据失败。[#15486](https://github.com/StarRocks/starrocks/pull/15486)
- SHOW CREATE TABLE 返回结果中 ORDER BY 子句位置错误。[#13809](https://github.com/StarRocks/starrocks/pull/13809)

### 行为变更

- `AWS_EC2_METADATA_DISABLED` 参数默认设置为 `False`，即默认获取 Amazon EC2 的元数据，用于访问 AWS resource。
- 会话变量 `is_report_success` 更名为 `enable_profile`，可通过 SHOW VARIABLES 语句查看。
- 新增四个关键字：`CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIME`, `LOCALTIMESTAMP`。[#14319](https://github.com/StarRocks/starrocks/pull/14319)
- 表名和库名的长度限制放宽至不超过 1023 个字符。 [#14929](https://github.com/StarRocks/starrocks/pull/14929) [#15020](https://github.com/StarRocks/starrocks/pull/15020)
- BE配置项 `enable_event_based_compaction_framework` 和 `enable_size_tiered_compaction_strategy` 默认开启，能够在 tablet 数比较多或者单个 tablet 数据量比较大的场景下大幅降低 compaction 的开销。

### 升级注意事项

- 可以从 2.0.x，2.1.x，2.2.x，2.3.x 或 2.4.x 升级。一般不建议回滚版本，如需回滚，建议只回滚到 2.4.x。
