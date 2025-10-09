---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.8

发布日期：2025年9月30日

### 行为变更

- 参数 `enable_lake_tablet_internal_parallel` 默认设置为 `true`，存算分离集群中的云原生表默认开启并行扫描，以提升单查询的内部并行度。但这可能会增加峰值资源使用量。 [#62159](https://github.com/StarRocks/starrocks/pull/62159)

### 问题修复

修复了以下问题：

- Delta Lake 分区列名被强制转换为小写，导致与实际列名不一致。 [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- Iceberg 清理 Manifest 缓存的并发竞争可能触发 NullPointerException (NPE)。 [#63052](https://github.com/StarRocks/starrocks/pull/63052) [#63043](https://github.com/StarRocks/starrocks/pull/63043)
- Iceberg 扫描阶段未捕获的通用异常会中断扫描范围提交，且未生成指标。 [#62994](https://github.com/StarRocks/starrocks/pull/62994)
- 复杂的多层投影视图在物化视图改写中可能生成无效执行计划或缺失列统计信息。 [#62918](https://github.com/StarRocks/starrocks/pull/62918) [#62198](https://github.com/StarRocks/starrocks/pull/62198)
- Hive 表构建的物化视图中分区列大小写不一致时被错误拒绝。 [#62598](https://github.com/StarRocks/starrocks/pull/62598)
- 物化视图刷新仅使用创建者的默认角色，可能导致权限不足问题。 [#62396](https://github.com/StarRocks/starrocks/pull/62396)
- 分区名大小写不敏感时，基于 List 分区的物化视图可能触发重复名称错误。 [#62389](https://github.com/StarRocks/starrocks/pull/62389)
- 物化视图恢复失败后残留的版本映射导致后续增量刷新被跳过，返回空结果。 [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- 物化视图恢复后的异常分区可能导致 FE 重启时触发 NullPointerException。 [#62563](https://github.com/StarRocks/starrocks/pull/62563)
- 非全局聚合查询错误地应用了聚合下推改写，生成无效计划。 [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- Tablet 删除状态仅在内存中更新而未持久化，导致 GC 仍将其视为运行中并跳过回收。 [#63623](https://github.com/StarRocks/starrocks/pull/63623)
- 查询与删除 Tablet 并发执行可能导致 delvec 过早清理并报错 "no delete vector found"。 [#63291](https://github.com/StarRocks/starrocks/pull/63291)
- 主键索引的 Base Compaction 和 Cumulative Compaction 共用 `max_rss_rowid` 的问题。 [#63277](https://github.com/StarRocks/starrocks/pull/63277)
- LakePersistentIndex 析构函数在初始化失败后运行可能导致 BE 崩溃。 [#62279](https://github.com/StarRocks/starrocks/pull/62279)
- Publish 线程池优雅关闭时静默丢弃队列任务且未标记失败，导致版本缺口并错误显示“全部成功”。 [#62417](https://github.com/StarRocks/starrocks/pull/62417)
- Rebalance 过程中新增 BE 上新克隆的副本被立即判定为冗余并删除，阻止数据迁移至新节点。 [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- 读取 Tablet 最大版本时缺少锁，导致副本事务决策不一致。 [#62238](https://github.com/StarRocks/starrocks/pull/62238)
- `date_trunc` 等值条件与原始列范围谓词组合时被化简为点区间，可能返回空结果集（例如 `date_trunc('month', dt)='2025-09-01' AND dt>'2025-09-23'`）。 [#63464](https://github.com/StarRocks/starrocks/pull/63464)
- 非确定性谓词（如随机/时间函数）下推导致结果不一致。 [#63495](https://github.com/StarRocks/starrocks/pull/63495)
- CTE 重用决策后缺失 Consumer 节点，导致执行计划不完整。 [#62784](https://github.com/StarRocks/starrocks/pull/62784)
- 表函数与低基数字典编码共存时的类型不匹配可能导致崩溃。 [#62466](https://github.com/StarRocks/starrocks/pull/62466) [#62292](https://github.com/StarRocks/starrocks/pull/62292)
- 过大的 CSV 被拆分为并行片段时，每个片段都会跳过表头行，导致数据丢失。 [#62719](https://github.com/StarRocks/starrocks/pull/62719)
- 在未指定数据库的情况下，`SHOW CREATE ROUTINE LOAD` 返回了同名的其他数据库中的任务。 [#62745](https://github.com/StarRocks/starrocks/pull/62745)
- 并发清理导入任务时 `sameLabelJobs` 变为 null，触发 NullPointerException。 [#63042](https://github.com/StarRocks/starrocks/pull/63042)
- 当所有 Tablet 已进入回收站时，BE 下线操作仍被阻塞。 [#62781](https://github.com/StarRocks/starrocks/pull/62781)
- `OPTIMIZE TABLE` 任务在线程池拒绝后卡在 PENDING 状态。 [#62300](https://github.com/StarRocks/starrocks/pull/62300)
- 清理脏 Tablet 元数据时 GTID 参数顺序错误。 [#62275](https://github.com/StarRocks/starrocks/pull/62275)

## 3.4.7

发布日期：2025 年 9 月 1 日

### 问题修复

修复了如下问题：

- Routine Load 作业未序列化 `max_filter_ratio`。 [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Stream Load 的 `now(precision)` 函数存在精度参数丢失。 [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- Audit Log 中，INSERT INTO SELECT 语句的 Scan Rows 结果不准确。[#61381](https://github.com/StarRocks/starrocks/pull/61381)
- 升级集群至 v3.4.5 后，`fslib read iops` 指标相较升级之前升高。[#61724](https://github.com/StarRocks/starrocks/pull/61724)
- 使用 JDBC Catalog 查询 SQLServer，查询经常卡住。[#61719](https://github.com/StarRocks/starrocks/pull/61719)

## 3.4.6

发布日期：2025 年 8 月 7 日

### 功能优化

- INSERT INTO FILES 导出数据到 Parquet 文件时，可以使用 [`parquet.version`](https://docs.starrocks.io/docs/zh/sql-reference/sql-functions/table-functions/files.md#parquetversion) 来指定导出 Parquet 文件的版本，以能让其他工具读取导出的 Parquet 文件更好地兼容。[#60843](https://github.com/StarRocks/starrocks/pull/60843)

### 问题修复

修复了如下问题：

- TableMetricsManager 中使用的锁粒度过大导致导入作业失败。[#58911](https://github.com/StarRocks/starrocks/pull/58911)
- 通过 `FILES()` 导入 Parquet 数据时列名大小写敏感的问题。[#61059](https://github.com/StarRocks/starrocks/pull/61059)
- 存算分离集群从 v3.3 升级至 v3.4 或更新版本后缓存不生效。[#60973](https://github.com/StarRocks/starrocks/pull/60973)
- 分区 ID 为空时，业务触发除零错误导致 BE Crash。[#60842](https://github.com/StarRocks/starrocks/pull/60842)
- BE 扩容过程中 Broker Load 作业报错。[#60224](https://github.com/StarRocks/starrocks/pull/60224)

### 行为变更

- `information_schema.keywords` 视图中的 `keyword` 列改名为 `word` ，以兼容 MySQL 中的定义。[#60863](https://github.com/StarRocks/starrocks/pull/60863)

## 3.4.5

发布日期：2025 年 7 月 10 日

### 功能优化

- 优化导入作业运行情况的可观测性信息：将导入任务的运行信息统一至 `information_schema.loads` 视图中。用户可以在此视图中查看所有 INSERT、Broker Load、Stream Load 以及 Routine Load 的子任务的运行信息。同时为视图增加了更多字段，让用户能更清晰地查看导入任务的运行情况，以及父作业（PIPES、Routint Load Job）的关联信息。
- 支持通过 ALTER ROUTINE LOAD 语句修改 `kafka_broker_list`。

### 问题修复

修复了如下问题：

- 高频导入下 Compaction 可能延迟。[#59998](https://github.com/StarRocks/starrocks/pull/59998)
- 通过 Unified Catalog 查询 Iceberg 外表报错: `not support getting unified metadata table factory`。[#59412](https://github.com/StarRocks/starrocks/pull/59412)
- 通过 DESC FILES() 查看远端存储中的 CSV 文件，返回结果错误（原因为系统错误将 `xinf` 推断为 FLOAT 类型）。[#59574](https://github.com/StarRocks/starrocks/pull/59574)
- INSERT INTO 遇到空分区导致 BE Crash。[#59553](https://github.com/StarRocks/starrocks/pull/59553)
- StarRocks 读取 Iceberg 中 Equality Delete 文件时，如果 Iceberg 表中数据已经删除，StarRocks 中依然可以读取到已删除数据。[#59709](https://github.com/StarRocks/starrocks/pull/59709)
- 给列重命名后导致的查询失败。[#59178](https://github.com/StarRocks/starrocks/pull/59178)

### 行为变更

- BE 配置项 `skip_pk_preload` 的默认值由 `false` 改为 `true`，导致系统会跳过主键表的 Primary Key Index 预读，以减少报错 `Reached Timeout` 的可能性。该变更可能会导致部分需要加载 Primary Key Index 的查询耗时增加。

## 3.4.4

发布日期：2025 年 6 月 10 日

### 功能优化

- Storage Volume 支持基于 Managed Identity 方式认证的 ADLS2 存储。[#58454](https://github.com/StarRocks/starrocks/pull/58454)
- 在[混合表达式分区](https://docs.starrocks.io/zh/docs/table_design/data_distribution/expression_partitioning/#混合表达式分区-自-v34)中，大多数基于日期时间相关的函数都能有效支持分区裁剪。
- 支持使用 `FILES` 函数从 Azure 导入 Avro 数据文件。[#58131](https://github.com/StarRocks/starrocks/pull/58131)
- Routine Load 导入非法 JSON 格式的数据时，增加打印当前消费的 Partition 和 Offset 信息到 Error Log 中，方便定位问题。[#55772](https://github.com/StarRocks/starrocks/pull/55772)

### 问题修复

修复了如下问题：

- 并发查询分区表的相同分区导致 Hive Metastore 卡住。[#58089](https://github.com/StarRocks/starrocks/pull/58089)
- `INSERT` 任务异常退出，导致对应作业一直处于 `QUEUEING` 状态。[#58603](https://github.com/StarRocks/starrocks/pull/58603)
- 自 v3.4.0 升级至 v3.4.2 之后，大量 Tablet 的副本数据出现异常。[#58518](https://github.com/StarRocks/starrocks/pull/58518)
- 错误的 UNION 执行计划造成 FE OOM。[#59040](https://github.com/StarRocks/starrocks/pull/59040)
- 回收分区时无效的数据库 ID 会导致 FE 启动失败。[#59666](https://github.com/StarRocks/starrocks/pull/59666)
- FE CheckPoint 操作失败后无法正常结束导致阻塞。[#58602](https://github.com/StarRocks/starrocks/pull/58602)

## 3.4.3

发布日期：2025 年 4 月 30 日

### 功能优化

- Routine Load 以及 Stream Load 支持在 `columns` 参数中使用 Lambda 表达式以实现复杂的列数据提取。用户可以使用 `array_filter`/`map_filter` 过滤提取 ARRAY / MAP 数据。通过结合 `cast` 函数将 JSON Array / JSON Object 转为 ARRAY 和 MAP 类型，可以实现对 JSON 数据的复杂过滤提取。例如通过 `COLUMNS (js, col=array_filter(i -> json_query(i, '$.type')=='t1' , cast(js as Array<JSON>))[1] )` 可以提取 `js` 这个 JSON Array 中 `type` 为 `t1` 的第一个 JSON Object。[#58149](https://github.com/StarRocks/starrocks/pull/58149)
- 支持将 JSON Object 通过 `cast` 函数转为 MAP 类型的数据，并结合 `map_filter` 提取 JSON Object 中满足条件子项。例如通过 `map_filter((k, v) -> json_query(v, '$.type') == 't1', cast(js AS MAP<String, JSON>))` 可以提取 `js` 这个 JSON Object 中 `type` 为 `t1` 的 JSON Object。[#58045](https://github.com/StarRocks/starrocks/pull/58045)
- 查询 `information_schema.task_runs` 视图时支持 LIMIT。[#57404](https://github.com/StarRocks/starrocks/pull/57404)

### 问题修复

修复了如下问题：

- 查询 ORC 格式的 Hive 表时报错 `OrcChunkReader::lazy_seek_to failed. reason = bad read in RleDecoderV2: :readByte`。[#57454](https://github.com/StarRocks/starrocks/pull/57454)
- 查询包含 Equality Delete 文件的 Iceberg 表时，上层的 RuntimeFilter 无法下推。[#57651](https://github.com/StarRocks/starrocks/pull/57651)
- 启用大算子落盘预聚合策略导致查询 Crash。[#58022](https://github.com/StarRocks/starrocks/pull/58022)
- 查询报错 `ConstantRef-cmp-ConstantRef not supported here, null != 111 should be eliminated earlier`。[#57735](https://github.com/StarRocks/starrocks/pull/57735)
- 在查询队列功能未启用状态下，查询触发 `query_queue_pending_timeout_second` 超时。[#57719](https://github.com/StarRocks/starrocks/pull/57719)

## 3.4.2

发布日期：2025 年 4 月 10 日

### 功能优化

- FE 支持优雅退出，从而提升系统的可用性。通过 `./stop_fe.sh -g` 退出结束 FE 时，FE 会先通过 `/api/health` API 向前端的 Load Balancer 返回 500 状态码以告知准备退出中，以便 Load Balancer 可以切换其他可用 FE 节点；同时，继续运行正在执行的查询直到结束或超时（默认 60 秒）。[#56823](https://github.com/StarRocks/starrocks/pull/56823)

### 问题修复

修复了如下问题：

- 分区列如果是生成列的话，分区裁剪可能失效。[#54543](https://github.com/StarRocks/starrocks/pull/54543)
- `concat` 函数的参数处理逻辑问题导致查询时 BE Crash。[#57522](https://github.com/StarRocks/starrocks/pull/57522)
- 使用 Broker Load 导入数据时，`ssl_enable` 属性不生效。[#57229](https://github.com/StarRocks/starrocks/pull/57229)
- 当存在 NULL 数据时，查询 STRUCT 类型列的子字段会导致 BE Crash。[#56496](https://github.com/StarRocks/starrocks/pull/56496)
- 通过 `ALTER TABLE {table} PARTITIONS (p1, p1) DISTRIBUTED BY ...` 语句修改表的分桶方式时，如果重复指定分区名，内部生成的临时分区无法删除。[#57005](https://github.com/StarRocks/starrocks/pull/57005)
- 在存算分离集群执行 `SHOW PROC '/current_queries'` 时报错 "Error 1064 (HY000): Sending collect query statistics request fails"。[#56597](https://github.com/StarRocks/starrocks/pull/56597)
- 并行执行 INSERT OVERWRITE 导入任务时报错 "ConcurrentModificationException: null"，导致导入任务失败。[#56557](https://github.com/StarRocks/starrocks/pull/56557)
- 自 v2.5.21 升级到 v3.1.17 后，多个 Broker Load 任务并行运行时可能会导致异常。[#56512](https://github.com/StarRocks/starrocks/pull/56512)

### 行为变更

- BE 配置项 `avro_ignore_union_type_tag` 默认值修改为 `true`，使得 `["NULL", "STRING"]` 可以直接作为 STRING 类型数据解析，更符合一般用户的使用需求。[#57553](https://github.com/StarRocks/starrocks/pull/57553)
- Session 变量 `big_query_profile_threshold` 默认值从 0 修改为 30（秒）。[#57177](https://github.com/StarRocks/starrocks/pull/57177)
- 增加 FE 配置项 `enable_mv_refresh_collect_profile`，用以控制物化视图刷新中是否收集 Profile 信息，默认值为 `false`（先前系统默认收集 Profile）。[#56971](https://github.com/StarRocks/starrocks/pull/56971)

## 3.4.1（已下线）

发布日期：2025 年 3 月 12 日

:::tip

此版本由于**存算分离集群**存在元数据丢失问题已经下线。

- **问题**：当存算分离集群中的 Leader FE 节点切换期间有已 Commit 但尚未 Publish 的 Compaction 事务时，节点切换后可能会发生元数据丢失。

- **影响范围**：此问题仅影响存算分离群集。存算一体集群不受影响。

- **临时解决方法**：当 Publish 任务返回错误时，可以执行 `SHOW PROC ‘compactions’` 检查是否有分区同时有两个 `FinishTime` 为空的 Compaction 事务。您可以执行 `ALTER TABLE DROP PARTITION FORCE` 来删除该分区，以避免 Publish 任务卡住。

:::

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

### 行为变更

由于存算分离架构和数据湖查询场景中使用统一的 Data Cache 实例，升级到 v3.4.0 后将会有以下行为变更：

- BE 配置项 `datacache_disk_path` 现已废弃。数据将缓存在 `${storage_root_path}/datacache` 目录下。如果要为 Data Cache 分配专用磁盘，可以使用 Symlink 手动将该目录指向上述目录。
- 存算分离集群中的缓存数据将自动迁移到 `${storage_root_path}/datacache`，升级后可重新使用。
- `datacache_disk_size` 的行为变更：

  - 当 `datacache_disk_size` 为 `0`（默认值）时，将启用缓存容量自动调整（与升级前的行为一致）。
  - 当 `datacache_disk_size` 设置为大于 `0` 时，系统将在 `datacache_disk_size` 和 `starlet_star_cache_disk_size_percent` 之间选择一个较大的值作为缓存容量。

### 降级说明

- 集群只可从 v3.4.0 降级至 v3.3.9 或更高版本。

