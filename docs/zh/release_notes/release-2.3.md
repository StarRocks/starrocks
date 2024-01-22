---
displayed_sidebar: "Chinese"
---

# StarRocks version 2.3

## 2.3.14

发布日期： 2023 年 6 月 28 日

### 功能优化

- 优化 CREATE TABLE 超时报错信息，增加参数调整建议。[#24510](https://github.com/StarRocks/starrocks/pull/24510)
<<<<<<< HEAD
- 主键模型表积累大量 Tablet 版本后内存占用的优化。[#20760](https://github.com/StarRocks/starrocks/pull/20760)
- OLAP 外表元数据的同步改为数据加载时进行。[#24739](https://github.com/StarRocks/starrocks/pull/24739)
=======
- 主键表积累大量 Tablet 版本后内存占用的优化。[#20760](https://github.com/StarRocks/starrocks/pull/20760)
- StarRocks 外表元数据的同步改为数据加载时进行。[#24739](https://github.com/StarRocks/starrocks/pull/24739)
>>>>>>> 53dc0006b6 ([Doc] change the Chinese proper name "data model" to table type  (#39474))
- 解除 NetworkTime 对系统时钟的依赖，以解决系统时钟误差导致 Exchange 网络耗时估算异常的问题。[#24858](https://github.com/StarRocks/starrocks/pull/24858)

### 问题修复

修复了如下问题：

- 对于频繁 TRUNCATE 的小表，应用低基数字典优化时查询报错。[#23185](https://github.com/StarRocks/starrocks/pull/23185)
- 对包含 UNION 且第一个孩子是常量 NULL 的 View 进行查询时，会导致 BE crash。[#13792](https://github.com/StarRocks/starrocks/pull/13792)  
- 基于 Bitmap Index 的查询有些情况下会返回错误。[#23484](https://github.com/StarRocks/starrocks/pull/23484)
- BE 中 Round DOUBLE/FLOAT 转换成 DECIMAL 和 FE 不一致。[#23152](https://github.com/StarRocks/starrocks/pull/23152)
- Schema change 和数据导入同时进行时 Schema change 偶尔会卡住。[#23456](https://github.com/StarRocks/starrocks/pull/23456)
- 使用 Broker Load、Spark Connector、或 Flink Connector 导入 Parquet 文件时偶尔会导致 BE OOM。[#25254](https://github.com/StarRocks/starrocks/pull/25254)
- 查询语句中 ORDER BY 后是常量且有 LIMIT 时会报错 `unknown error`。[#25538](https://github.com/StarRocks/starrocks/pull/25538)

## 2.3.13

发布日期： 2023 年 6 月 1 日

### 功能优化

- 优化了因 `thrift_server_max_worker_threads` 设置过小导致 INSERT INTO ... SELECT 超时场景下的报错信息。 [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- 降低多表关联时，使用 `bitmap_contains` 函数的内存消耗，并优化性能。 [#20617](https://github.com/StarRocks/starrocks/pull/20617)、[#20653](https://github.com/StarRocks/starrocks/pull/20653)

### 问题修复

修复了如下问题：

- Truncate 操作对分区名大小写敏感导致 Truncate Partition 失败。 [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- 导入的 Parquet 格式文件中含 int96 timestamp 类型数据时，会导致数据溢出。[#22355](https://github.com/StarRocks/starrocks/issues/22355)
- 删除物化视图后使用 DECOMMISSION 下线 BE 节点失败。[#22743](https://github.com/StarRocks/starrocks/issues/22743)
- 当查询的执行计划包括从 BroadcastJoin 节点至 BucketShuffleJoin 节点，例如 `SELECT * FROM t1 JOIN [Broadcast] t2 ON t1.a = t2.b JOIN [Bucket] t3 ON t2.b = t3.c;`，并且 BroadcastJoin 左表等值 Join 的 Key 的数据在进行 BucketShuffleJoin 之前被删除掉了，则会导致 BE crash。[#23227](https://github.com/StarRocks/starrocks/pull/23227)
- 当查询的执行计划包括从 CrossJoin 节点至 HashJoin 节点、并且一个 fragment instance 中 HashJoin 的右表为空，则返回结果会不正确。[#23877](https://github.com/StarRocks/starrocks/pull/23877)
- 物化视图创建临时分区失败导致 BE 下线卡住。 [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- 如果 SQL 语句中 STRING 类型的值包含多个转义字符，则该 SQL 语句解析失败。[#23119](https://github.com/StarRocks/starrocks/issues/23119)
- 无法查询分区列最大值的数据。[#23153](https://github.com/StarRocks/starrocks/issues/23153)
- StarRocks 2.4 回退到 2.3 后导入作业报错。[#23642](https://github.com/StarRocks/starrocks/pull/23642)
- 列裁剪复用问题。[#16624](https://github.com/StarRocks/starrocks/issues/16624)

## 2.3.12

发布日期： 2023 年 4 月 25 日

### 功能优化

如果表达式的返回值可以合法转换为 Boolean 值，则会对其进行隐式转换。[# 21792](https://github.com/StarRocks/starrocks/pull/21792)

### 问题修复

修复了如下问题：

- 如果用户的 LOAD 权限为表级别，则导入作业失败后导入事务回滚时报错提示 `Access denied; you need (at least one of) the LOAD privilege(s) for this operation`。[# 21129](https://github.com/StarRocks/starrocks/issues/21129)
- 执行 ALTER SYSTEM DROP BACKEND 删除一个 BE 后，可能会导致该 BE 上的两副本表的副本不能修复，进而导致导入作业因为没有可用的副本而失败。[# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- 建表时使用不支持的数据类型，报错信息有误。[# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- Broadcast Join 的短路逻辑异常，导致查询结果不正确。[# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- 使用物化视图后磁盘占用率大幅增加。[# 20590](https://github.com/StarRocks/starrocks/pull/20590)
- 未能彻底卸载插件 Audit Loader。[# 20468](https://github.com/StarRocks/starrocks/issues/20468)
- `INSERT INTO XXX SELECT` 返回结果显示的数据行数和 `SELECT COUNT(*) FROM XXX` 返回结果显示的数据行数不一致。[# 20084](https://github.com/StarRocks/starrocks/issues/20084)
- 如果子查询使用窗口函数，父查询使用 GROUP BY 子句，则查询结果无法聚合。[# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- 启动 BE 后，BE 进程存在但是 BE 所有端口无法开启。[# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- 如果磁盘 IO 利用率过高，导致主键表的事务提交过慢，则查询该表时可能会返回报错 `backend not found`。[# 18835](https://github.com/StarRocks/starrocks/issues/18835)

## 2.3.11

发布日期： 2023 年 3 月 28 日

### 功能优化

- 复杂查询会导致大量 `ColumnRefOperators`。原先通过 `BitSet` 来存储 `ColumnRefOperator::id`，会占用较多内存。为了减少内存占用，现在用 `RoaringBitMap` 取代原来的 `BitSet` 来存储  `ColumnRefOperator::id`。[#16499](https://github.com/StarRocks/starrocks/pull/16499)
- 增加新的 I/O 调度策略，降低大查询对小查询的性能影响。如果需要开启新的 I/O 调度策略，则需要在 **be.conf** 中设置 BE 静态参数 `pipeline_scan_queue_mode=1` 并重启 BE。[#19009](https://github.com/StarRocks/starrocks/pull/19009)

### 问题修复

修复了如下问题：

- 当表的过期数据未正常回收时，该表会占用较高的磁盘容量。[#19796](https://github.com/StarRocks/starrocks/pull/19796)
- 使用 Broker Load 导入 Parquet 文件，导入 `NULL` 值至 NOT NULL 的列时，报错提示不明确。[#19885](https://github.com/StarRocks/starrocks/pull/19885)
- 频繁创建大量临时分区以替换原有分区时，导致 FE 内存泄露和 Full GC。[#19283](https://github.com/StarRocks/starrocks/pull/19283)
- 对于 Colocation 表，可以通过命令手动指定副本状态为 bad  `ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`，如果 BE 数量小于等于副本数量，则该副本无法被修复。[#19443](https://github.com/StarRocks/starrocks/pull/19443)
- 向 Follower FE 发起 INSERT INTO SELECT 请求时，参数 `parallel_fragment_exec_instance_num` 不生效。[#18841](https://github.com/StarRocks/starrocks/pull/18841)
- 使用运算符 `<=>` 对某个值和 `NULL` 值进行比较时，比较结果不正确。[#19210](https://github.com/StarRocks/starrocks/pull/19210)
- 在持续触发资源隔离的并发限制时，查询并发数量指标下降缓慢。[#19363](https://github.com/StarRocks/starrocks/pull/19363)
- 高并发导入时可能报错 `"get database read lock timeout, database=xxx"`。[#16748](https://github.com/StarRocks/starrocks/pull/16748) [#18992](https://github.com/StarRocks/starrocks/pull/18992)

## 2.3.10

发布日期： 2023 年 3 月 9 日

### 功能优化

优化 `storage_medium` 的推导机制。当 BE 同时使用 SSD 和 HDD 作为存储介质时，如果配置了 `storage_cooldown_time`，StarRocks 设置 `storage_medium` 为 `SSD`。反之，则 StarRocks 设置 `storage_medium` 为 `HDD`。[#18649](https://github.com/StarRocks/starrocks/pull/18649)

### 问题修复

修复了如下问题：

- 查询数据湖 Parquet 文件中 ARRAY 类型数据时查询可能会失败。 [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- 程序发起的 Stream Load 导入任务卡住，FE 接收不到程序发起的 HTTP 请求。[#18559](https://github.com/StarRocks/starrocks/pull/18559)
- 查询 Elasticsearch 外表可能会报错。[#13727](https://github.com/StarRocks/starrocks/pull/13727)
- 表达式在初始阶段发生错误，可能导致会 BE 停止服务。[#11396](https://github.com/StarRocks/starrocks/pull/11396)
- 查询语句使用空数组字面量 `[]`，可能会导致查询失败。[#18550](https://github.com/StarRocks/starrocks/pull/18550)
- 版本从 2.2 及以上升级至 2.3.9 及以上后，创建 Routine Load 时 COLUMN 包含计算表达式，可能会报错 `No match for <expr> with operand types xxx and xxx`。[#17856](https://github.com/StarRocks/starrocks/pull/17856)
- 重启 BE 后导入操作卡住。[#18488](https://github.com/StarRocks/starrocks/pull/18488)
- 当查询语句的 WHERE 子句包含 OR 运算符时，多余分区被扫描。[#18610](https://github.com/StarRocks/starrocks/pull/18610)

## 2.3.9

发布日期： 2023 年 2 月 20 日

### 问题修复

- Schema Change 过程中，如果发生 Tablet Clone，并且副本所在 BE 节点发生变化，会导致 Schema Change 失败。[#16948](https://github.com/StarRocks/starrocks/pull/16948)
- `group_concat()` 函数返回结果被截断。[#16948](https://github.com/StarRocks/starrocks/pull/16948)
- 执行 Broker Load 通过腾讯大数据处理套件（Tencent Big Data Suite，TBDS） 从 HDFS 导入数据时报 `invalid hadoop.security.authentication.tbds.securekey` 的错误，显示不支持通过 TBDS 提供的 HDFS 权限认证访问 HDFS。[#14125](https://github.com/StarRocks/starrocks/pull/14125)、[#15693](https://github.com/StarRocks/starrocks/pull/15693)
- 在一些情况下，CBO 比较算子是否相等的逻辑发生错误。[#17227](https://github.com/StarRocks/starrocks/pull/17227)、[#17199](https://github.com/StarRocks/starrocks/pull/17199)
- 连接非 Leader FE 节点，发送请求 `USE <catalog_name>.<database_name>`，非 Leader 节点中将请求转发给 Leader FE 时，没有转发 `catalog_name`，导致 Leader FE 使用 `default_catalog`，因此无法找到对应的 database。[#17302](https://github.com/StarRocks/starrocks/pull/17302)

## 2.3.8

发布日期： 2023 年 2 月 2 日

### 问题修复

修复了如下问题：

- 在大查询完成后释放资源时，小概率导致其他查询变慢，特别是在开启资源组或大查询异常结束时。[#16454](https://github.com/StarRocks/starrocks/pull/16454) [#16602](https://github.com/StarRocks/starrocks/pull/16602)
- 针对主键表，如果一个副本的元数据版本比较落后，增量克隆会触发大量版本的元数据积压没有及时 GC，可能会导致 BE 上发生 OOM。 [#15935](https://github.com/StarRocks/starrocks/pull/15935)
- 如果 FE 向 BE 发送单次偶发的心跳，心跳连接超时，FE 会认为该 BE 不可用，最终导致该 BE 上的事务运行失败。[# 16386](https://github.com/StarRocks/starrocks/pull/16386)
- 通过 StarRocks 外表功能导入数据时，如果源 StarRocks 集群为低版本，目标 StarRocks 集群为高版本（并且高版本为 2.2.8 ~ 2.2.11，2.3.4 ~ 2.3.7，2.4.1 或 2.4.2)，则数据导入失败。[#16173](https://github.com/StarRocks/starrocks/pull/16173)
- 当查询高并发并且内存使用率比较高时 BE 崩溃。 [#16047](https://github.com/StarRocks/starrocks/pull/16047)
- 如果表开启了动态分区，表的部分分区被动态删除，此时执行 TRUNCATE TABLE 会导致报 `NullPointerException` 错误，此时导入数据会导致 FE 崩溃且无法重启。[#16822](https://github.com/StarRocks/starrocks/pull/16822)

## 2.3.7

发布日期： 2022 年 12 月 30 日

### 问题修复

修复了如下问题：

- 当 StarRocks 表某些列支持为 NULL，但是当基于该表创建视图 时，视图中该列会被错误设置为 NOT NULL。 [#15749](https://github.com/StarRocks/starrocks/pull/15749)
- 导入时生成了新的 Tablet 版本，但是 FE 可能尚未感知新的 tablet 版本，因此此时 FE 下发的查询执行计划中依然要求 BE 读取该 Tablet 的历史版本。如果此时垃圾回收机制回收了该历史版本，则该时间点发起的查询无法读取该历史版本，从而查询报错 “Not found: get_applied_rowsets(version xxxx) failed tablet:xxx #version:x [xxxxxx]”。 [#15726](https://github.com/StarRocks/starrocks/pull/15726)
- 高频导入时 FE 占用较多内存。 [#15377](https://github.com/StarRocks/starrocks/pull/15377)
- 对于聚合和多表 JOIN 的查询，统计信息收集不准确，出现 CROSS JOIN，导致查询耗时过长。 [#12067](https://github.com/StarRocks/starrocks/pull/12067) [#14780](https://github.com/StarRocks/starrocks/pull/14780)

## 2.3.6

发布日期： 2022 年 12 月 22 日

### 功能优化

- Pipeline 执行引擎支持 INSERT INTO 语句。如果需要启用，则需要设置 FE 配置项 `enable_pipeline_load_for_insert` 为 `true`。 [#14723](https://github.com/StarRocks/starrocks/pull/14723)
- 优化主键表 Compaction 阶段所占内存。[#13861](https://github.com/StarRocks/starrocks/pull/13861) 、[#13862](https://github.com/StarRocks/starrocks/pull/13862)

### 问题修复

修复了如下问题：

- 修复开启资源隔离后，多个资源组同时执行查询，可能会导致 BE 挂死的问题。[#14905](https://github.com/StarRocks/starrocks/pull/14905)
- 创建物化视图 `CREATE MATERIALIZED VIEW AS SELECT`，如果 `SELECT` 查询中未使用聚合函数，使用 GROUP BY，例如`CREATE MATERIALIZED VIEW test_view AS` `select a,b from test group by b,a order by a;`，则 BE 节点全部崩溃。[#13743](https://github.com/StarRocks/starrocks/pull/13743)
- 执行 INSERT INTO 高频导入至主键表，进行数据变更后，立即重启 BE，重启缓慢。[#15128](https://github.com/StarRocks/starrocks/pull/15128)
- 如果环境中只安装 JRE 未安装 JDK，则重启 FE 后查询失败。修复后，在该环境无法成功重启 FE，会直接报错 `Error: JAVA_HOME can not be jre`，您需要在环境中安装 JDK。[#14332](https://github.com/StarRocks/starrocks/pull/14332)
- 查询导致 BE 崩溃。[#14221](https://github.com/StarRocks/starrocks/pull/14221)
- 设置 exec_mem_limit 时不支持使用表达式。[#13647](https://github.com/StarRocks/starrocks/pull/13647)
- 根据子查询结果创建同步刷新的物化视图，创建失败。[#13507](https://github.com/StarRocks/starrocks/pull/13507)
- 手动刷新 Hive 外表后列的 comment 注释被清空。[#13742](https://github.com/StarRocks/starrocks/pull/13742)
- 关联查询表 A 和 B 时，先计算右表 B 再计算左表 A，如果右表 B 特别大，在计算右表 B 过程中，左表 A 的数据执行了 Compaction，导致 BE 节点崩溃。[#14070](https://github.com/StarRocks/starrocks/pull/14070)
- 如果 Parquet 文件列名大小写敏感，此时查询条件中使用了 Parquet 文件中的大写列名，则查询结果为空。[#13860](https://github.com/StarRocks/starrocks/pull/13860) 、[#14773](https://github.com/StarRocks/starrocks/pull/14773)
- 批量导入时，Broker 的连接数过多，超过默认最大连接数，导致 Broker 连接断开，最终导入失败，并且报错 list path error。[#13911](https://github.com/StarRocks/starrocks/pull/13911)
- BE 负载很高时，资源组的监控指标 starrocks_be_resource_group_running_queries 统计错误。[#14043](https://github.com/StarRocks/starrocks/pull/14043)
- 如果查询语句使用 OUTER JOIN，可能会导致 BE 节点崩溃。[#14840](https://github.com/StarRocks/starrocks/pull/14840)
- 2.4 版本使用了异步物化视图后回滚到 2.3 版本，导致 FE 无法启动。[#14400](https://github.com/StarRocks/starrocks/pull/14400)
- 主键表部分地方使用了delete_range，如果性能不佳可能会导致后续从 RocksDB 读取数据过慢，导致 CPU 资源占用率过高。[#15130](https://github.com/StarRocks/starrocks/pull/15130)

### 行为变更

- 取消 FE 参数 `default_storage_medium`，表的存储介质改为系统自动推导。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.3.5

发布日期： 2022 年 11 月 30 日

### 功能优化

- Colocate Join 支持等值 Join。[#13546](https://github.com/StarRocks/starrocks/pull/13546)
- 解决高频导入时可能不断追加 WAL 记录而导致主键索引文件过大的问题。[#12862](https://github.com/StarRocks/starrocks/pull/12862)
- 优化 FE 后台线程在全量检测 tablet 时持有 DB 锁时间太长的问题。[#13070](https://github.com/StarRocks/starrocks/pull/13070)

### 问题修复

修复了如下问题：

- 修复在 UNION ALL 之上建立的 view 中，当 UNION ALL 输入列有常量 NULL，则 view 的 schema 错误，正常情况下列的类型是 UNION ALL 所有输入列的类型，但实际上是 NULL_TYPE 的问题。[#13917](https://github.com/StarRocks/starrocks/pull/13917)
- 修复 `SELECT * FROM ...` 和 `SELECT * FROM ... LIMIT ...` 查询结果不一致的问题。[#13585](https://github.com/StarRocks/starrocks/pull/13585)
- 修复 FE 同步外表 tablet 元数据时可能会覆盖本地 tablet 元数据导致从 Flink 导入数据失败的问题。[#12579](https://github.com/StarRocks/starrocks/pull/12579)
- 修复 Runtime Filter 中 null filter 在处理常量字面量的时候导致 BE 节点崩溃的问题。[#13526](https://github.com/StarRocks/starrocks/pull/13526)
- 修复执行 CTAS 报错的问题。[#12388](https://github.com/StarRocks/starrocks/pull/12388)
- 修复 audit log 中 pipeline engine 收集的 `ScanRows` 有误的问题。[#12185](https://github.com/StarRocks/starrocks/pull/12185)
- 修复查询压缩格式的 HIVE 数据返回结果不正确的问题。[#11546](https://github.com/StarRocks/starrocks/pull/11546)
- 修复一个 BE 节点崩溃后，查询超时并且响应缓慢的问题。[#12955](https://github.com/StarRocks/starrocks/pull/12955)
- 修复使用 Broker Load 导入数据时报错 Kerberos 认证失败的问题。[#13355](https://github.com/StarRocks/starrocks/pull/13355)
- 修复过多的 OR 谓词导致统计信息估算耗时过久的问题。[#13086](https://github.com/StarRocks/starrocks/pull/13086)
- 修复 Broker Load 导入包含大写列名的 ORC 文件 ( Snappy 压缩) 后 BE 节点崩溃的问题。[#12724](https://github.com/StarRocks/starrocks/pull/12724)
- 修复导出或者查询 Primary Key 表时间超过 30 分钟报错的问题。[#13403](https://github.com/StarRocks/starrocks/pull/13403)
- 修复使用 broker 备份至 HDFS 时，大数据量场景下备份失败的问题。[#12836](https://github.com/StarRocks/starrocks/pull/12836)
- 修复 `parquet_late_materialization_enable` 参数引起的读取 Iceberg 数据正确性的问题。[#13132](https://github.com/StarRocks/starrocks/pull/13132)
- 修复创建视图报错 `failed to init view stmt` 的问题。[#13102](https://github.com/StarRocks/starrocks/pull/13102)
- 修复通过 JDBC 连接 StarRock 执行 SQL 报错的问题。[#13526](https://github.com/StarRocks/starrocks/pull/13526)
- 修复分桶过多时查询时使用 tablet hint 导致查询超时的问题。[#13272](https://github.com/StarRocks/starrocks/pull/13272)
- 修复一个 BE 节点崩溃无法重新启动，并且此时建表后导入作业报错的问题。[#13701](https://github.com/StarRocks/starrocks/pull/13701)
- 修复创建物化视图导致全部 BE 节点崩溃的问题。[#13184](https://github.com/StarRocks/starrocks/pull/13184)
- 修复执行 ALTER ROUTINE LOAD 更新消费分区的 offset，可能会报错 `The specified partition 1 is not in the consumed partitions`，并且最终 Follower 崩溃的问题。[#12227](https://github.com/StarRocks/starrocks/pull/12227)

## 2.3.4

发布日期： 2022 年 11 月 10 日

### 功能优化

- 优化 Routine Load 创建失败时的报错提示。[#12204]( https://github.com/StarRocks/starrocks/pull/12204)
- 查询 Hive 时解析 CSV 数据失败后会直接报错。[#13013](https://github.com/StarRocks/starrocks/pull/13013)

### 问题修复

修复了如下问题：

- 修复 HDFS 文件路径带有 `()` 导致查询报错的问题。[#12660](https://github.com/StarRocks/starrocks/pull/12660)
- 修复子查询带有 LIMIT，并且使用 ORDER BY ... LIMIT ... OFFSET 对结果集进行排序后查询结果错误的问题。[#9698](https://github.com/StarRocks/starrocks/issues/9698)
- 修复查询 ORC 文件时大小写不敏感的问题。[#12724](https://github.com/StarRocks/starrocks/pull/12724)
- 修复 RuntimeFilter 没有正常关闭导致 BE 崩溃问题。[#12895](https://github.com/StarRocks/starrocks/pull/12895)
- 修复新增列后立即删除数据可能导致查询结果错误的问题。[#12907](https://github.com/StarRocks/starrocks/pull/12923)
- 修复当 StarRocks 和 MySQL 客户端不在同一局域网时，执行一次 KILL 无法成功终止 INSERT INTO SELECT 导入作业的问题。[#11879](https://github.com/StarRocks/starrocks/pull/11897)
- 修复 audit log 中 pipeline engine 收集的 `ScanRows` 有误的问题。[#12185](https://github.com/StarRocks/starrocks/pull/12185)

## 2.3.3

发布日期： 2022 年 9 月 27 日

### 问题修复

修复了如下问题：

- 修复查询文件格式为 Textfile 的 Hive 表，结果不准确的问题。[#11546](https://github.com/StarRocks/starrocks/pull/11546)
- 修复查询 Parquet 格式的文件时不支持查询嵌套的 ARRAY 的问题。[#10983](https://github.com/StarRocks/starrocks/pull/10983)
- 修复一个查询读取外部数据源和 StarRocks 表，或者并行查询读取外部数据源和 StarRocks 表，并且查询路由至一个资源组，则可能会导致查询超时的问题。[#10983](https://github.com/StarRocks/starrocks/pull/10983)
- 修复默认开启 Pipeline 执行引擎后，参数 parallel_fragment_exec_instance_num 变为 1，导致 INSERT INTO 导入变慢的问题。[#11462](https://github.com/StarRocks/starrocks/pull/11462)
- 修复表达式在初始阶段发生错误时可能导致 BE 停止服务的问题。[#11396](https://github.com/StarRocks/starrocks/pull/11396)
- 修复执行 ORDER BY LIMIT 时导致堆溢出的问题。 [#11185](https://github.com/StarRocks/starrocks/pull/11185)
- 修复重启 Leader FE，会造成 schema change 执行失败的问题。 [#11561](https://github.com/StarRocks/starrocks/pull/11561)

## 2.3.2

发布日期： 2022 年 9 月 7 日

### 新特性

- 支持延迟物化，提升小范围过滤场景下 Parquet 外表的查询性能。 [#9738](https://github.com/StarRocks/starrocks/pull/9738)
- 新增 SHOW AUTHENTICATION 语句，用于查询用户认证相关的信息。 [#9996](https://github.com/StarRocks/starrocks/pull/9996)

### 功能优化

- 查询 HDFS 集群里配置了分桶的 Hive 表时，支持设置是否递归遍历该分桶 Hive 表的所有数据文件。 [#10239](https://github.com/StarRocks/starrocks/pull/10239)
- 修改资源组类型 `realtime` 为 `short_query`。 [#10247](https://github.com/StarRocks/starrocks/pull/10247)
- 优化外表查询机制，在查询 Hive 外表时默认忽略大小写。 [#10187](https://github.com/StarRocks/starrocks/pull/10187)

### 问题修复

修复了如下问题：

- 修复 Elasticsearch 外表在多个 Shard 下时查询可能意外退出的问题。 [#10369](https://github.com/StarRocks/starrocks/pull/10369)
- 修复重写子查询为公用表表达式 (CTE) 时报错的问题。 [#10397](https://github.com/StarRocks/starrocks/pull/10397)
- 修复导入大批量数据时报错的问题。 [#10370](https://github.com/StarRocks/starrocks/issues/10370) [#10380](https://github.com/StarRocks/starrocks/issues/10380)
- 修复多个 Catalog 配置为相同 Thrift 服务地址时，删除其中一个 Catalog 会导致其他 Catalog 的增量元数据同步失效的问题。 [#10511](https://github.com/StarRocks/starrocks/pull/10511)
- 修复 BE 内存占用统计不准确的问题。 [#9837](https://github.com/StarRocks/starrocks/pull/9837)
- 修复完整克隆 (Full Clone) 后，查询主键表时报错的问题。[#10811](https://github.com/StarRocks/starrocks/pull/10811)
- 修复拥有逻辑视图的 SELECT 权限但无法查询的问题。[#10563](https://github.com/StarRocks/starrocks/pull/10563)
- 修复逻辑视图命名无限制的问题。逻辑视图的命名规范同数据库表的命名规范。[#10558](https://github.com/StarRocks/starrocks/pull/10558)

### 行为变更

- 增加 BE 配置项 `max_length_for_bitmap_function` （默认为 1000000 字节）和 `max_length_for_to_base64`（默认为 200000 字节），以控制 bitmap 函数和to_base64() 函数的输入值的最大长度。 [#10851](https://github.com/StarRocks/starrocks/pull/10851)

## 2.3.1

发布日期： 2022 年 8 月 22 日

### 功能优化

- Broker Load 支持将 Parquet 文件的 List 列转化为非嵌套 ARRAY 数据类型。[#9150](https://github.com/StarRocks/starrocks/pull/9150)
- 优化 JSON 类型相关函数（json_query、get_json_string 和 get_json_int）性能。[#9623](https://github.com/StarRocks/starrocks/pull/9623)
- 优化报错信息：查询 Hive、Iceberg、Hudi 外表时，如果查询的数据类型未被支持，系统针对相应的列报错。[#10139](https://github.com/StarRocks/starrocks/pull/10139)
- 降低资源组调度延迟，优化资源隔离性能。[#10122](https://github.com/StarRocks/starrocks/pull/10122)

### 问题修复

修复了如下问题：

- 查询 Elasticsearch 外表时，`limit` 算子下推问题导致返回错误结果。[#9952](https://github.com/StarRocks/starrocks/pull/9952)
- 使用 `limit` 算子查询 Oracle 外表失败。[#9542](https://github.com/StarRocks/starrocks/pull/9542)
- Routine Load 中，Kafka Broker 停止工作导致 BE 卡死。[#9935](https://github.com/StarRocks/starrocks/pull/9935)
- 查询与外表数据类型不匹配的 Parquet 文件导致 BE 停止工作。[#10107](https://github.com/StarRocks/starrocks/pull/10107)
- 外表 Scan 范围为空，导致查询阻塞超时。[#10091](https://github.com/StarRocks/starrocks/pull/10091)
- 子查询中包含 ORDER BY 子句时，系统报错。[#10180](https://github.com/StarRocks/starrocks/pull/10180)
- 并发重加载 Hive 元数据导致 Hive Metastore 挂起。[#10132](https://github.com/StarRocks/starrocks/pull/10132)

## 2.3.0

发布日期： 2022 年 7 月 29 日

### 新增特性

<<<<<<< HEAD
- 主键模型支持完整的 DELETE WHERE 语法。相关文档，请参见 [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#delete-与主键类型表)。
- 主键模型支持持久化主键索引，基于磁盘而不是内存维护索引，大幅降低内存使用。相关文档，请参见[主键模型](../table_design/table_types/primary_key_table.md#使用说明)。
=======
- 主键表支持完整的 DELETE WHERE 语法。相关文档，请参见 [DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/DELETE#delete-与主键类型表)。
- 主键表支持持久化主键索引，基于磁盘而不是内存维护索引，大幅降低内存使用。相关文档，请参见[主键表](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table#使用说明)。
>>>>>>> 53dc0006b6 ([Doc] change the Chinese proper name "data model" to table type  (#39474))
- 全局低基数字典优化支持实时数据导入，实时场景下字符串数据的查询性能提升一倍。
- 支持以异步的方式执行 CTAS，并将结果写入新表。相关文档，请参见 [CREATE TABLE AS SELECT](../sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md)。
- 资源组相关功能：
  - 支持监控资源组：可在审计日志中查看查询所属的资源组，并通过相关 API 获取资源组的监控信息。相关文档，请参见[监控指标](../administration/Monitor_and_Alert.md#监控指标)。
  - 支持限制大查询的 CPU、内存、或 I/O 资源；可通过匹配分类器将查询路由至资源组，或者设置会话变量直接为查询指定资源组。相关文档，请参见[资源隔离](../administration/resource_group.md)。
- 支持 JDBC 外表，可以轻松访问Oracle、PostgreSQL、MySQL、SQLServer、ClickHouse 等数据库，并且查询时支持谓词下推，提高查询性能。相关文档，请参见 [更多数据库（JDBC）的外部表](../data_source/External_table.md#更多数据库jdbc的外部表)。
- 【Preview】发布全新数据源 Connector 框架，支持创建外部数据目录（External Catalog），从而无需创建外部表，即可直接查询 Apache Hive™。相关文档，请参见[查询外部数据](../data_source/catalog/query_external_data.md)。
- 新增如下函数：
  - [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md)
  - [ntile](../sql-reference/sql-functions/Window_function.md)
  - [bitmap_union_count](../sql-reference/sql-functions/bitmap-functions/bitmap_union_count.md)、[base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md)、[array_to_bitmap](../sql-reference/sql-functions/array-functions/array_to_bitmap.md)
  - [week](../sql-reference/sql-functions/date-time-functions/week.md)、[time_slice](../sql-reference/sql-functions/date-time-functions/time_slice.md)

### 功能优化

- 优化合并机制（Compaction），对较大的元数据进行合并操作，避免因数据高频更新而导致短时间内元数据挤压，占用较多磁盘空间。
- 优化导入 Parquet 文件和压缩文件格式的性能。
- 优化创建物化视图的性能，在部分场景下创建速度提升近 10 倍。
- 优化算子性能：
  - TopN，sort 算子。
  - 包含函数的等值比较运算符下推至 scan 算子时，支持使用 Zone Map 索引。
- 优化 Apache Hive™ 外表功能。
  - 当 Apache Hive™ 的数据存储采用 Parquet、ORC、CSV 格式时，支持 Hive 表执行 ADD COLUMN、REPLACE COLUMN 等表结构变更（Schema Change）。相关文档，请参见 [Hive 外部表](../data_source/External_table.md#deprecated-hive-外部表)。
  - 支持 Hive 资源修改 `hive.metastore.uris`。相关文档，请参见 [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER_RESOURCE.md)。
- 优化 Apache Iceberg 外表功能，创建 Iceberg 资源时支持使用自定义目录（Catalog）。相关文档，请参见 [Apache Iceberg 外表](../data_source/External_table.md#deprecated-iceberg-外部表)。
- 优化 Elasticsearch 外表功能，支持取消探测 Elasticsearch 集群数据节点的地址。相关文档，请参见 [Elasticsearch 外部表](../data_source/External_table.md#deprecated-elasticsearch-外部表)。
- 当 sum() 中输入的值为 STRING 类型且为数字时，则自动进行隐式转换。
- year、month、day 函数支持 DATE 数据类型。

### 问题修复

修复了如下问题：

- Tablet 过多导致 CPU 占用率过高的问题。
- 导致出现"fail to prepare tablet reader"报错提示的问题。
- FE 重启失败的问题。[#5642](https://github.com/StarRocks/starrocks/issues/5642 )、[#4969](https://github.com/StarRocks/starrocks/issues/4969 )、[#5580](https://github.com/StarRocks/starrocks/issues/5580)
- CTAS 语句中调用 JSON 函数时报错的问题。[#6498](https://github.com/StarRocks/starrocks/issues/6498)

### 其他

- 【Preview】提供集群管理工具 StarGo，提供集群部署、启停、升级、回滚、多集群管理等多种能力。相关文档，请参见[通过 StarGo 部署 StarRocks 集群](../administration/stargo.md)。
- 部署或者升级至 2.3 版本，默认开启 Pipeline 执行引擎，预期在高并发小查询、复杂大查询场景下获得明显的性能优势。如果使用 2.3 版本时遇到明显的性能回退，则可以通过设置 `SET GLOBAL enable_pipeline_engine = false;`，关闭 Pipeline 执行引擎。
- [SHOW GRANTS](../sql-reference/sql-statements/account-management/SHOW_GRANTS.md) 语句兼容 MySQL语法，显示授权 GRANT 语句。
- 建议单个 Schema Change 任务数据占用内存上限 `memory_limitation_per_thread_for_schema_change`(BE 配置项)保持为默认值 2 GB，数据超过上限后写入磁盘。因此如果您之前调大该参数，则建议恢复为 2 GB，否则可能会出现单个 Schema Change 任务数据占用大量内存的问题。

### 升级注意事项

升级后如果碰到问题需要回滚，请在 **fe.conf** 文件中增加 `ignore_unknown_log_id=true`。这是因为新版本的元数据日志新增了类型，如果不加这个参数，则无法回滚。最好等做完 checkpoint 之后再设置 `ignore_unknown_log_id=false` 并重启 FE，恢复正常配置。
