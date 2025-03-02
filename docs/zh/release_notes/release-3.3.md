---
displayed_sidebar: docs
---

# StarRocks version 3.3

:::warning

升级至 v3.3 后，请勿直接将集群降级至 v3.2.0、v3.2.1 或 v3.2.2，否则会导致元数据丢失。您必须降级到 v3.2.3 或更高版本以避免出现此问题。

:::

## 3.3.10 (已下线)

发布日期：2025 年 2 月 21 日

:::tip

此版本由于**存算分离集群**存在元数据丢失问题已经下线。

- **问题**：当存算分离集群中的 Leader FE 节点切换期间有已 Commit 但尚未 Publish 的 Compaction 事务时，节点切换后可能会发生元数据丢失。

- **影响范围**：此问题仅影响存算分离群集。存算一体集群不受影响。

- **临时解决方法**：当 Publish 任务返回错误时，可以执行 `SHOW PROC ‘compactions’` 检查是否有分区同时有两个 `FinishTime` 为空的 Compaction 事务。您可以执行 `ALTER TABLE DROP PARTITION FORCE` 来删除该分区，以避免 Publish 任务卡住。

:::

### 新增功能

- 窗口函数支持 `max_by` 和 `min_by`。[#54961](https://github.com/StarRocks/starrocks/pull/54961)

### 功能优化

- 支持复杂类型的子列在表函数中下推。[#55425](https://github.com/StarRocks/starrocks/pull/55425)
- 支持 MariaDB 客户端的 LDAP 用户登陆。[#55720](https://github.com/StarRocks/starrocks/pull/55720)
- Paimon 版本升级到 1.0.1。[#54796](https://github.com/StarRocks/starrocks/pull/54796) [#55760](https://github.com/StarRocks/starrocks/pull/55760)
- 查询执行过程中消除非必要的 `unnest` 计算，降低开销。[#55431](https://github.com/StarRocks/starrocks/pull/55431)
- 跨集群同步过程中存算分离源集群支持开启 Compaction。[#54787](https://github.com/StarRocks/starrocks/pull/54787)
- 当 DECIMAL 类型的除法这类代价较高出现在在 topN 计算之下时，将其提前至 topN 之上降低开销。[#55417](https://github.com/StarRocks/starrocks/pull/55417)
- 基于 ARM 架构的性能提升。[#55072](https://github.com/StarRocks/starrocks/pull/55072) [#55510](https://github.com/StarRocks/starrocks/pull/55510)
- 基于 Hive 外表的物化视图，如基表删除重建，刷新时会检查实际的分区更新而非全表重刷。[#45118](https://github.com/StarRocks/starrocks/pull/45118)
- DELETE 操作支持分区裁剪。[#55400](https://github.com/StarRocks/starrocks/pull/55400)
- 优化内表统计信息收集的优先级策略，以优化在表数量较多时的效率。[#55446](https://github.com/StarRocks/starrocks/pull/55446)
- 导入任务涉及多个分区时，合并记录 Transaction Log 以提高导入性能。[#55143](https://github.com/StarRocks/starrocks/pull/55143)
- 优化 Translate SQL 的报错信息。[#55327](https://github.com/StarRocks/starrocks/pull/55327)
- 增加 Session 变量 `parallel_merge_late_materialization_mode` 控制并行合并行为。[#55082](https://github.com/StarRocks/starrocks/pull/55082)
- 优化生成列的报错信息。[#54949](https://github.com/StarRocks/starrocks/pull/54949)
- 优化 SHOW MATERIALIZED VIEWS 命令的性能。[#54374](https://github.com/StarRocks/starrocks/pull/54374)

### 问题修复

修复了如下问题：

- CTE 优先下推 LIMIT 再下推谓词导致的错误。[#55768](https://github.com/StarRocks/starrocks/pull/55768)
- Stream Load 因表 Schema Change 导致的错误。[#55773](https://github.com/StarRocks/starrocks/pull/55773)
- DELETE 语句的执行计划包含 SELECT 导致的权限问题。[#55695](https://github.com/StarRocks/starrocks/pull/55695)
- CN 下线时未停止 Compaction 任务导致的问题。[#55503](https://github.com/StarRocks/starrocks/pull/55503)
- Follower FE 节点无法获取更新的导入信息。[#55758](https://github.com/StarRocks/starrocks/pull/55758)
- 落盘路径容量统计错误。[#55703](https://github.com/StarRocks/starrocks/pull/55703)
- 因缺少足够分区检查导致 List 分区的物化视图创建失败。[#55673](https://github.com/StarRocks/starrocks/pull/55673)
- ALTER TABLE 因缺少元数据锁导致的问题。[#55605](https://github.com/StarRocks/starrocks/pull/55605)
- SHOW CREATE TABLE 因 constraints 导致的错误。[#55592](https://github.com/StarRocks/starrocks/pull/55592)
- Nestloop Join 因大 ARRAY 导致的 OOM。[#55603](https://github.com/StarRocks/starrocks/pull/55603)
- DROP PARTITION 的锁问题。[#55549](https://github.com/StarRocks/starrocks/pull/55549)
- min/max 窗口函数因未支持字符串类型导致的问题。[#55537](https://github.com/StarRocks/starrocks/pull/55537)
- Parser 性能回退问题。[#54830](https://github.com/StarRocks/starrocks/pull/54830)
- 部分更新时列名大小写敏感问题。[#55442](https://github.com/StarRocks/starrocks/pull/55442)
- Stream Load 调度到 Alive 状态为 false 的节点时，导入失败。[#55371](https://github.com/StarRocks/starrocks/pull/55371)
- 物化视图包含 ORDER BY，但输出列顺序错误。[#55355](https://github.com/StarRocks/starrocks/pull/55355)
- 磁盘故障导致 BE Crash。[#55042](https://github.com/StarRocks/starrocks/pull/55042)
- Query Cache 导致的结果错误。[#55287](https://github.com/StarRocks/starrocks/pull/55287)
- Parquet Writer 写入包含时区的 TIMESTAMP 类型时未能进行时区转换的问题。[#55194](https://github.com/StarRocks/starrocks/pull/55194)
- 因 ALTER 任务超时而导致导入任务卡住。[#55207](https://github.com/StarRocks/starrocks/pull/55207)
- `date_format` 函数输入毫秒时间类型导致的错误。[#54854](https://github.com/StarRocks/starrocks/pull/54854)
- 分区键为 DATE 类型导致的物化视图改写失败。[#54804](https://github.com/StarRocks/starrocks/pull/54804)

### 行为变更

- Iceberg 中 UUID 类型的映射更改为 BINARY。[#54978](https://github.com/StarRocks/starrocks/pull/54978)
- 使用分区变更的行数而非分区可见时间来判断是否需要重新收集统计信息。[#55373](https://github.com/StarRocks/starrocks/pull/55373)

## 3.3.9

发布日期：2025 年 1 月 12 日

### 新增功能

- 支持将 Trino SQL 翻译为 StarRocks SQL。[#54185](https://github.com/StarRocks/starrocks/pull/54185)

### 功能优化

- 在 FE 节点以 `bdbje_reset_election_group` 开头时，纠正 FE 节点名称以方便理解。[#54399](https://github.com/StarRocks/starrocks/pull/54399)
- 在 ARM 结构上实现 IF 函数的向量化。[#53093](https://github.com/StarRocks/starrocks/pull/53093)
- ALTER SYSTEM CREATE IMAGE 支持创建 StarManager 的 Image。[#54370](https://github.com/StarRocks/starrocks/pull/54370)
- 存算分离集群支持删除主键表的云原生索引。[#53971](https://github.com/StarRocks/starrocks/pull/53971)
- 指定 FORCE 关键词时，系统强制刷新物化视图。[#52081](https://github.com/StarRocks/starrocks/pull/52081)
- CACHE SELECT 支持指定 Hint。[#54697](https://github.com/StarRocks/starrocks/pull/54697)
- 支持通过 FILES() 函数导入压缩的 CSV 文件，支持的压缩格式包括 gzip、bz2、lz4、deflate 以及 zstd。[#54626](https://github.com/StarRocks/starrocks/pull/54626)
- 允许在 UPDATE 中对同一列进行多个赋值。[#54534](https://github.com/StarRocks/starrocks/pull/54534)

### 问题修复

修复了如下问题：

- JDBC Catalog 物化视图刷新时出现的意外错误。[#54487](https://github.com/StarRocks/starrocks/pull/54487)
- Delta Lake 表 Join 自身时出现的结果不稳定。[#54473](https://github.com/StarRocks/starrocks/pull/54473)
- 使用 HDFS 文件系统接口备份文件时的重试上传失败问题。[#53679](https://github.com/StarRocks/starrocks/pull/53679)
- aarch64 架构上的 BFD 初始化错误。[#54372](https://github.com/StarRocks/starrocks/pull/54372)
- BE 日志中记录敏感信息。[#54677](https://github.com/StarRocks/starrocks/pull/54677)
- Profile 中 Compaction 相关的指标错误。[#54678](https://github.com/StarRocks/starrocks/pull/54678)
- 创建 TIME 嵌套类型的表导致的 BE 崩溃。[#54601](https://github.com/StarRocks/starrocks/pull/54601)
- 带有子查询 TOP-N 的 `LIMIT` 查询计划错误。[#54507](https://github.com/StarRocks/starrocks/pull/54507)

### 降级说明

- 集群只可从 v3.3.9 降级至 v3.2.11 或更高版本。

## 3.3.8

发布日期：2025 年 1 月 3 日

### 功能优化

- 添加集群空闲 API 以帮助判断集群状态。[#53850](https://github.com/StarRocks/starrocks/pull/53850)
- JSON 指标中添加节点信息和直方图指标。[#53735](https://github.com/StarRocks/starrocks/pull/53735)
- 优化存算分离集群中主键表的 MemTable。[#54178](https://github.com/StarRocks/starrocks/pull/54178)
- 优化存算分离集群中主键表的内存占用和统计信息。[#54358](https://github.com/StarRocks/starrocks/pull/54358)
- 当查询需要扫描全表或大量分区时，增加单节点扫描分区数量限制以控制单 BE 或 CN 节点的扫描压力，提升系统稳定性。[#53747](https://github.com/StarRocks/starrocks/pull/53747)
- 支持收集 Paimon 表的统计信息。[#52858](https://github.com/StarRocks/starrocks/pull/52858)
- 存算分离集群支持配置 S3 Client 请求超时时间。[#54211](https://github.com/StarRocks/starrocks/pull/54211)

### 问题修复

- 主键表 DelVec 不一致导致 BE 退出。[#53460](https://github.com/StarRocks/starrocks/pull/53460)
- 存算分离集群中主键表的锁释放问题。[#53878](https://github.com/StarRocks/starrocks/pull/53878)
- 函数嵌套中 UDF 遇到错误时，查询不会返回错误。[#44297](https://github.com/StarRocks/starrocks/pull/44297)
- 事务因依赖原始副本，导致其状态阻塞在 Decommission 阶段。[#49349](https://github.com/StarRocks/starrocks/pull/49349)
- 查询 Delta Lake 表时，使用相对路径而不是文件名来获取文件。[#53949](https://github.com/StarRocks/starrocks/pull/53949)
- 查询 Delta Lake Shallow Clone 表报错。[#54044](https://github.com/StarRocks/starrocks/pull/54044)
- 使用 JNI 读取 Paimon 时的大小写敏感问题。[#54041](https://github.com/StarRocks/starrocks/pull/54041)
- 使用 INSERT OVERWRITE 覆盖写 Hive Catalog 中由 Hive 创建的表报错。[#53792](https://github.com/StarRocks/starrocks/pull/53792)
- SHOW TABLE STATUS 命令没有校验视图权限。[#53811](https://github.com/StarRocks/starrocks/pull/53811)
- FE 指标缺失。[#53058](https://github.com/StarRocks/starrocks/pull/53058)
- INSERT 任务内存泄露问题。[#53809](https://github.com/StarRocks/starrocks/pull/53809)
- 副本复制任务中提交的事务没有写锁引发的并发问题。[#54061](https://github.com/StarRocks/starrocks/pull/54061)
- 统计信息数据库中的表 `parti``ti``on_ttl` 不生效。[#54398](https://github.com/StarRocks/starrocks/pull/54398)
- Query Cache 相关问题：
  - 在开启 Group Execution 时 Query Cache 发生 Crash。[#54363](https://github.com/StarRocks/starrocks/pull/54363) 
  - Runtime Filter 发生 Crash 的问题。[#54305](https://github.com/StarRocks/starrocks/pull/54305)
- 物化视图 Union Rewrite 的问题。[#54293](https://github.com/StarRocks/starrocks/pull/54293)
- 主键表部分列更新导致的字符串填充缺位。[#54182](https://github.com/StarRocks/starrocks/pull/54182)
- 在启用低基数优化时 `max(count(distinct))` 的错误执行计划。[#53403](https://github.com/StarRocks/starrocks/pull/53403)
- 变更物化视图参数 `excluded_refresh_tables` 的问题。[#53394](https://github.com/StarRocks/starrocks/pull/53394)

### 行为变更

- 存算分离集群中主键表 `persistent_index_type` 默认值修改为 `CLOUD_NATIVE`，即默认打开 Persistent Index。[#52209](https://github.com/StarRocks/starrocks/pull/52209)

## 3.3.7

发布日期：2024 年 11 月 29 日

### 新增功能
- 物化视图新增 `excluded_refresh_tables` 参数，物化视图刷新的时候不会触发数据同步的基表。[#50926](https://github.com/StarRocks/starrocks/pull/50926)

### 功能优化

- 将 `unnest(bitmap_to_array)` 改写为 `unnest_bitmap` 以提升效率。[#52870](https://github.com/StarRocks/starrocks/pull/52870)
- 降低 TXN Log 的写入和删除操作。[#42542](https://github.com/StarRocks/starrocks/pull/42542)

### 问题修复

修复了如下问题：

- Power BI 连接外表失败。[#52977](https://github.com/StarRocks/starrocks/pull/52977)
- 日志中误导性的 FE Thrift RPC 失败信息。[#52706](https://github.com/StarRocks/starrocks/pull/52706)
- Routine Load 因事务过期而导致任务取消（目前仅有数据库或表不存在任务才会被取消）。[#50334](https://github.com/StarRocks/starrocks/pull/50334) 
- 通过 HTTP 1.0 提交的 Stream Load 失败。[#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- 分区 ID 整数溢出。 [#52965](https://github.com/StarRocks/starrocks/pull/52965)
- Hive Text Reader 无法识别最后一个空元素。[#52990](https://github.com/StarRocks/starrocks/pull/52990)
- `array_map` 出现在 Join 条件中导致的问题。[#52911](https://github.com/StarRocks/starrocks/pull/52911)
- 元数据 Cache 在高并发场景下的问题。[#52968](https://github.com/StarRocks/starrocks/pull/52968)
- 从物化视图基表删除分区后，整个物化视图会被重新刷新。[#52740](https://github.com/StarRocks/starrocks/pull/52740)

## 3.3.6

发布日期：2024 年 11 月 18 日

### 功能优化

- 优化主键表内部修复的逻辑。[#52707](https://github.com/StarRocks/starrocks/pull/52707)
- 优化统计信息直方图的内部实现。[#52400](https://github.com/StarRocks/starrocks/pull/52400)
- 支持通过 FE 参数 `sys_log_warn_modules` 选择日志级别以降低 Hudi Catalog 日志的打印量。[#52709](https://github.com/StarRocks/starrocks/pull/52709)
- 函数 `yearweek` 支持进行常量折叠。[#52714](https://github.com/StarRocks/starrocks/pull/52714)
- 防止 Lambda 函数被下推。[#52655](https://github.com/StarRocks/starrocks/pull/52655)
- 拆分监控项 Query Error 为三个部分：Internal Error Rate、Analysis Error Rate 以及 Timeout Rate。[#52646](https://github.com/StarRocks/starrocks/pull/52646)
- 避免将常量表达式提取为 `array_map` 中的公共表达式。[#52541](https://github.com/StarRocks/starrocks/pull/52541)
- 优化物化视图的 Text-based Rewrite。[#52498](https://github.com/StarRocks/starrocks/pull/52498)

### 问题修复

修复了如下问题：

- 对存算分离集群内表运行 SHOW CREATE TABLE 时 `unique_constraints` 和 `foreign_constraints` 参数显示不全。[#52804](https://github.com/StarRocks/starrocks/pull/52804)
- 设置 `enable_mv_automatic_active_check` 为 `false` 后系统仍会将部分物化视图设置为 Active。 [#52799](https://github.com/StarRocks/starrocks/pull/52799)
- 刷新过时内存后内存占用未能降低。[#52613](https://github.com/StarRocks/starrocks/pull/52613)
- Hudi File-system View 导致的资源泄漏。[#52738](https://github.com/StarRocks/starrocks/pull/52738)
- 主键表数据 Publish 和 Update 操作同时发生导致的问题。[#52687](https://github.com/StarRocks/starrocks/pull/52687)
- 客户端无法正常终止查询。[#52185](https://github.com/StarRocks/starrocks/pull/52185)
- 多列 List 分区无法下推。[#51036](https://github.com/StarRocks/starrocks/pull/51036)
- 因 ORC 文件缺少 `hasnull` 导致返回错误结果。[#52555](https://github.com/StarRocks/starrocks/pull/52555)
- 因建表时 ORDER BY 列使用大写列名导致的问题。[#52513](https://github.com/StarRocks/starrocks/pull/52513)
- 运行 `ALTER TABLE PARTITION (*) SET ("storage_cooldown_ttl" = "xxx")` 报错。[#52482](https://github.com/StarRocks/starrocks/pull/52482)

### 行为变更

- 历史版本中，缩容操作会因统计信息数据库 `_statistics_` 中的视图副本数不足而失败。v3.3.6 及以后版本中，如缩容至 3 个或以上节点，则系统会将视图的副本数设置为 3，如缩容至 1 个节点，则会将视图的副本数设置为 1，使缩容操作正常进行。[#51799](https://github.com/StarRocks/starrocks/pull/51799)

  受影响的视图包括：

  - `column_statistics`
  - `histogram_statistics`
  - `table_statistic_v1`
  - `external_column_statistics`
  - `external_histogram_statistics`
  - `pipe_file_list`
  - `loads_history`
  -  `task_run_history`

- 新创建的主键表将不再允许使用 `__op` 作为列名，即使已经将 FE 配置项 `allow_system_reserved_names` 设置为 `true`。历史表不受此变更影响。 [#52621](https://github.com/StarRocks/starrocks/pull/52621)
- 表达式分区的表不允许变更分区名。[#52557](https://github.com/StarRocks/starrocks/pull/52557)
- 废弃 FE 配置项 `heartbeat_mgr_blocking_queue_size` 和 `profile_process_threads_num`。[#52236](https://github.com/StarRocks/starrocks/pull/52236)
- 存算分离集群中的主键表默认开启基于对象存储的持久化索引。 [#52209](https://github.com/StarRocks/starrocks/pull/52209)
- 使用随机分桶方式的表禁止手动更改分桶方式。 [#52120](https://github.com/StarRocks/starrocks/pull/52120)
- 备份还原相关参数变更：[#52111](https://github.com/StarRocks/starrocks/pull/52111)
  - `make_snapshot_worker_count` 支持动态配置。
  - `release_snapshot_worker_count` 支持动态配置。
  - `upload_worker_count` 支持动态配置，默认值从 `1` 改为等同于节点 CPU 核数。
  - `download_worker_count` 支持动态配置，默认值从 `1` 改为等同于节点 CPU 核数。
- `SELECT @@autocommit` 的返回值从 BOOLEAN 类型改为 BIGINT 类型。 [#51946](https://github.com/StarRocks/starrocks/pull/51946)
- 新增 FE 参数 `max_bucket_number_per_partition`，为单个分区增加分桶上限控制。 [#47852](https://github.com/StarRocks/starrocks/pull/47852)
- 主键表默认开启内存占用检查。 [#52393](https://github.com/StarRocks/starrocks/pull/52393)
- 优化导入策略，当 Compaction 任务无法及时完成时降低导入速度。[#52269](https://github.com/StarRocks/starrocks/pull/52269)

## 3.3.5

发布日期：2024 年 10 月 23 日

### 新增功能

- DATETIME 类型支持毫秒和微秒。
- 资源组支持 CPU 硬隔离。

### 功能优化

- 优化了 Flat JSON 的性能和抽取策略。[#50696](https://github.com/StarRocks/starrocks/pull/50696)
- 优化了以下 ARRAY 函数的内存占用：
  - array_contains/array_position [#50912](https://github.com/StarRocks/starrocks/pull/50912)
  - array_filter [#51363](https://github.com/StarRocks/starrocks/pull/51363)
  - array_match [#51377](https://github.com/StarRocks/starrocks/pull/51377)
  - array_map [#51244](https://github.com/StarRocks/starrocks/pull/51244)
- 优化了 Not Null 属性的 List 分区键导入 Null 值时的报错信息。[#51086](https://github.com/StarRocks/starrocks/pull/51086)
- 优化了 Files 函数在认证信息错误时的报错信息。[#51697](https://github.com/StarRocks/starrocks/pull/51697)
- 优化了 INSERT OVERWRITE 内部的统计信息。[#50417](https://github.com/StarRocks/starrocks/pull/50417)
- 存算分离集群支持持久化索引文件的 GC。[#51684](https://github.com/StarRocks/starrocks/pull/51684)
- 增加 FE 日志以定位 FE OOM 问题。[#51528](https://github.com/StarRocks/starrocks/pull/51528)
- 支持从 FE 的元数据路径中恢复元数据。[#51040](https://github.com/StarRocks/starrocks/pull/51040)

### 问题修复

修复如下问题：

- PIPE 因异常而导致死锁。 [#50841](https://github.com/StarRocks/starrocks/pull/50841)
- 动态分区创建失败而堵塞后续分区创建。[#51440](https://github.com/StarRocks/starrocks/pull/51440)
- UNION ALL 查询中加入 ORDER BY 报错。[#51647](https://github.com/StarRocks/starrocks/pull/51647)
- UPDATE 语句中包含 CTE 导致 Hint 失效。[#51458](https://github.com/StarRocks/starrocks/pull/51458)
- 系统视图 `statistics.loads_history` 中的 `load_finish_time` 字段在导入任务结束后依旧不符合预期的变更。[#51174](https://github.com/StarRocks/starrocks/pull/51174)
- UDTF 错漏多字节 UTF-8。[#51232](https://github.com/StarRocks/starrocks/pull/51232)

### 行为变更

- 更改 EXPLAIN 语句的返回内容，更改后其返回内容等同于 EXPLAIN COST。您可以通过 FE 的动态参数`query_detail_explain_level` 来设置 EXPLAIN 返回的信息级别，默认为 `COSTS`，其他有效值包括 `NORMAL` 和 `VERBOSE`。 [#51439](https://github.com/StarRocks/starrocks/pull/51439)

## 3.3.4

发布日期：2024 年 9 月 30 日

### 新增功能

- 支持在 List Partition 的表上创建异步物化视图。[ #46680](https://github.com/StarRocks/starrocks/pull/46680) [#46808](https://github.com/StarRocks/starrocks/pull/46808/files)
- List Partition 表支持 Nullable 分区列。 [#47797](https://github.com/StarRocks/starrocks/pull/47797)
- 支持通过 `DESC FILES()` 查看外部文件 Schema 信息。[#50527](https://github.com/StarRocks/starrocks/pull/50527)
- 支持通过 `SHOW PROC '/replications'` 查看数据复制任务指标。[#50483](https://github.com/StarRocks/starrocks/pull/50483)

### 功能优化

- 优化存算分离架构下 `TRUNCATE TABLE` 的数据回收速度。[#49975](https://github.com/StarRocks/starrocks/pull/49975)
- CTE 算子支持中间结果落盘。[#47982](https://github.com/StarRocks/starrocks/pull/47982)
- 支持自适应分阶段调度，缓解复杂查询导致的 OOM。[#47868](https://github.com/StarRocks/starrocks/pull/47868)
- 在一些特定场景下支持了 STRING 类型的 date 或 datetime 列的查询下推。[#50643](https://github.com/StarRocks/starrocks/pull/50643)
- 支持基于常量的半结构化数据上计算 `COUNT DISTINCT`。[#48273](https://github.com/StarRocks/starrocks/pull/48273)
- 新增 FE 参数 `lake_enable_balance_tablets_between_workers`，用于启用存算分离表的 Tablet 均衡。[#50843](https://github.com/StarRocks/starrocks/pull/50843)
- 优化生成列的改写能力。[#50398](https://github.com/StarRocks/starrocks/pull/50398)
- Partial Update 支持自动填充默认值为 `CURRENT_TIMESTAMP` 的列的值。[#50287](https://github.com/StarRocks/starrocks/pull/50287)

### 问题修复

修复了如下问题：

- Tablet Clone 时 FE 侧死循环导致报错 "version has been compacted"。[#50561](https://github.com/StarRocks/starrocks/pull/50561)
- ISO 格式 DATETIME 类型在查询时无法下推。[#49358](https://github.com/StarRocks/starrocks/pull/49358)
- 并发场景下，当 Tablet 被删除后，数据仍旧存在。[#50382](https://github.com/StarRocks/starrocks/pull/50382)
- 函数 `yearweek` 结果错误。[#51065](https://github.com/StarRocks/starrocks/pull/51065)
- ARRAY 低基数字典在 CTE 查询中的问题。[#51148](https://github.com/StarRocks/starrocks/pull/51148)
- FE 重启后，物化视图的分区 TTL 相关参数丢失。[#51028](https://github.com/StarRocks/starrocks/pull/51028)
- 升级后，表中使用 `CURRENT_TIMESTAMP` 定义的列数据丢失。[#50911](https://github.com/StarRocks/starrocks/pull/50911)
- 函数 `array_distinct` 导致堆栈溢出。[#51017](https://github.com/StarRocks/starrocks/pull/51017)
- 升级后，因字段默认长度变化而导致的物化视图激活（active）失败。您可以通过设置`enable_active_materialized_view_schema_strict_check` 为 `false` 规避此类问题。[#50869](https://github.com/StarRocks/starrocks/pull/50869)
- 资源组属性 `cpu_weight` 可以设置为负。[#51005](https://github.com/StarRocks/starrocks/pull/51005)
- 磁盘容量的统计信息统计错误。[#50669](https://github.com/StarRocks/starrocks/pull/50669)
- 函数 `replace` 常量折叠。[#50828](https://github.com/StarRocks/starrocks/pull/50828)

### 行为变更

- 更改外表物化视图的默认副本数，从默认 `1` 副本改为遵循 FE 参数 `default_replication_num` 的值（默认值：`3`）。[#50931](https://github.com/StarRocks/starrocks/pull/50931)

## 3.3.3

发布日期：2024 年 9 月 5 日

### 新增功能

- 支持设置用户级别变量。[#48477](https://github.com/StarRocks/starrocks/pull/48477)
- 支持了 Delta Lake Catalog 的元数据缓存、元数据手动刷新以及周期性刷新策略。[#46526](https://github.com/StarRocks/starrocks/pull/46526) [#49069](https://github.com/StarRocks/starrocks/pull/49069)
- 支持导入 Parquet 文件的 JSON 类型。[#49385](https://github.com/StarRocks/starrocks/pull/49385)
- JDBC SQL Server Catalog 支持 LIMIT 查询。[#48248](https://github.com/StarRocks/starrocks/pull/48248)
- 存算分离集群支持通过 INSERT INTO 执行部分列更新。[#49336](https://github.com/StarRocks/starrocks/pull/49336)

### 功能优化

- 优化导入报错信息：
  - 在导入内存超限时返回对应 BE 节点的 IP 以便于定位问题。[#49335](https://github.com/StarRocks/starrocks/pull/49335)
  - 导入 CSV 数据时，在目标表列长度不足时给出更加明确的信息提示。[#49713](https://github.com/StarRocks/starrocks/pull/49713)
  - 因 Kerberos 认证失败而导致的 Broker Load 报错，给出具体认证失败的节点信息。[#46085](https://github.com/StarRocks/starrocks/pull/46085)
- 优化导入时的分区机制，降低初始阶段的内存占用。[#47976](https://github.com/StarRocks/starrocks/pull/47976)
- 优化存算一体集群的内存占用问题。增加元数据内存占用限制，避免在 Tablet、Segment 文件过多时可能引发的问题。[#49170](https://github.com/StarRocks/starrocks/pull/49170)
- 优化了 `max(partition_column)` 的查询性能。[#49391](https://github.com/StarRocks/starrocks/pull/49391)
- 如果分区列是生成列（即基于表中某个原生列计算所得），且查询的谓词过滤条件包含原生列，则可以使用分区裁剪优化查询性能。 [#48692](https://github.com/StarRocks/starrocks/pull/48692)
- 对 Files()、PIPE 相关操作中的敏感信息进行脱敏。[#47629](https://github.com/StarRocks/starrocks/pull/47629)
- 提供新的命令 `SHOW PROC '/global_current_queries'`，用以查看在所有 FE 节点上运行的查询。而相对应的命令 `SHOW PROC '/current_queries'` 只能查看当前连接的 FE 节点上运行的查询。[#49826](https://github.com/StarRocks/starrocks/pull/49826)

### 问题修复

修复了如下问题：

- 在通过 StarRocks 外表将数据导出至目标集群时，系统将源集群 BE 误添加到当前集群。[#49323](https://github.com/StarRocks/starrocks/pull/49323)
- aarch64 类机器上部署的 StarRocks 集群在通过 `select * from files` 读取 ORC 文件时，TINYINT 数据类型返回 NULL。[#49517](https://github.com/StarRocks/starrocks/pull/49517)
- Stream Load 导入包含大 Integer 类型的 JSON 格式文件失败。 [#49927](https://github.com/StarRocks/starrocks/pull/49927)
- Files() 导入 CSV 文件时，对非可见字符的错误处理而导致的 Schema 获取错误。[#49718](https://github.com/StarRocks/starrocks/pull/49718)
- 多列分区表替换临时产生的分区错误。  [#49764](https://github.com/StarRocks/starrocks/pull/49764)

### 行为变更

- 为了更好的适应向云上对象存储备份的场景，引入新的参数 `object_storage_rename_file_request_timeout_ms`。系统会优先使用该参数作为备份的超时时间。默认为 30 秒。 [#49706](https://github.com/StarRocks/starrocks/pull/49706)
- `to_json`、`CAST(AS MAP)` 以及 `STRUCT AS JSON` 时，默认转换失败不报错，返回为 NULL。您可以通过设置系统变量 `sql_mode` 为 `ALLOW_THROW_EXCEPTION` 来使查询允许报错。[#50157](https://github.com/StarRocks/starrocks/pull/50157)

## 3.3.2

发布日期：2024 年 8 月 8 日

### 新增功能

- 支持重命名 StarRocks 内表的列。[#47851](https://github.com/StarRocks/starrocks/pull/47851)
- 支持读取 Iceberg 的视图（View）。目前仅支持读取通过 StarRocks 创建的视图。[#46273](https://github.com/StarRocks/starrocks/issues/46273)
- [Experimental] 支持 STRUCT 类型数据增减子列。[#46452](https://github.com/StarRocks/starrocks/issues/46452)
- 支持在建表时指定 ZSTD 压缩格式的压缩级别。 [#46839](https://github.com/StarRocks/starrocks/issues/46839)
- 增加以下 FE 动态参数以限制表的边界。[#47896](https://github.com/StarRocks/starrocks/pull/47869)

  包括 ：

  - `auto_partition_max_creation_number_per_load`
  - `max_partition_number_per_table`
  - `max_bucket_number_per_partition` 
  - `max_column_number_per_table`

- 支持在线优化表数据分布，确保优化任务与针对该表的 DML 运行不冲突。[#43747](https://github.com/StarRocks/starrocks/pull/43747)
- 支持全局 Data Cache 命中率可观测性接口。 [#48450](https://github.com/StarRocks/starrocks/pull/48450)
- 支持函数 array_repeat。 [#47862](https://github.com/StarRocks/starrocks/pull/47862)

### 功能优化

- 优化因 Kafka 鉴权失败而导致的 Routine Load 失败的报错信息。[#46136](https://github.com/StarRocks/starrocks/pull/46136) [#47649](https://github.com/StarRocks/starrocks/pull/47649)
- Stream Load 支持将 `\t` 和 `\n` 分别作为行列分割符，无需转成对应的十六进制 ASCII 码。[#47302](https://github.com/StarRocks/starrocks/pull/47302)
- 优化写入算子的的异步统计信息收集方式，解决导入任务较多时延迟变高的问题。[#48162](https://github.com/StarRocks/starrocks/pull/48162)
-  增加以下 BE 动态参数以控制导入过程中的资源硬限制，从而降低写入大量 Tablet 时对 BE 稳定性的影响。[#48495](https://github.com/StarRocks/starrocks/pull/48495)

  包括：

  - `load_process_max_memory_hard_limit_ratio`
  - `enable_new_load_on_memory_limit_exceeded`

- 增加同一表内 Column ID 的一致性检查，防止引起 Compaction 错误。[#48498](https://github.com/StarRocks/starrocks/pull/48628)
- 持久化 PIPE 元数据，防止因 FE 重启而导致元数据丢失。[#48852](https://github.com/StarRocks/starrocks/pull/48852)

### 问题修复

修复了如下问题：

- 在 FE Follower 上创建字典时进程无法结束。 [#47802](https://github.com/StarRocks/starrocks/pull/47802)
- SHOW PARTITIONS 命令在存算分离集群和存算一体集群中返回的信息不一致。[#48647](https://github.com/StarRocks/starrocks/pull/48647)
- 从 JSON 类型的子列导入至 `ARRAY<BOOLERAN>` 类型列时，因类型处理错误而导致的数据错误。[#48387](https://github.com/StarRocks/starrocks/pull/48387)
- `information_schema.task_runs` 中的 `query_id` 列无法查询。[#48876](https://github.com/StarRocks/starrocks/pull/48879)
- 备份时针对同一操作的多份请求会提交给不同 Broker，导致请求报错。[#48856](https://github.com/StarRocks/starrocks/pull/48856)
- 降级至 v3.1.11、v3.2.4 之前的版本，导致主键表的索引解压失败，进而导致查询报错。[#48659](https://github.com/StarRocks/starrocks/pull/48659)

### 降级说明

如果您已经使用列重命名功能，在您降级到旧版本前，请您将所有列的名字改回历史名字，以防降级出现问题。您可以通过 Audit Log 来查看自升级以来是否出现 `ALTER TABLE RENAME COLUMN` 相关操作，并找到历史名称。

## 3.3.1（已下线）

发布日期：2024 年 7 月 18 日

:::tip

此版本因存在主键模型表升级兼容性问题已经下线。

- 问题：从 3.1.11、3.2.4 之前的版本升级至 3.3.1 时，查询主键模型表时，索引解压失败，导致查询报错。
- 影响范围：对于不涉及主键模型表的查询，不受影响。
- 临时解决方法：请回滚至 3.3.0 及以下版本规避此问题，此问题会在 3.3.2 版本中解决。

:::

### 新增特性

- [Preview] 支持临时表。
- [Preview] JDBC Catalog 支持 Oracle 和 SQL Server。
- [Preview] Unified Catalog 支持 Kudu。
- INSERT INTO 导入主键表，支持通过指定 Column List 实现部分列更新。
- 用户自定义变量支持 ARRAY 类型。 [#42631](https://github.com/StarRocks/starrocks/pull/42613)
- Stream Load 支持将 JSON 类型转化并导入至 STRUCT/MAP/ARRAY 类型目标列。[ #45406](https://github.com/StarRocks/starrocks/pull/45406)
- 支持全局字典 Cache。
- 支持批量删除分区。[#44744](https://github.com/StarRocks/starrocks/issues/44744)
- 支持在 Apache Ranger 中设置列级别权限（物化视图和视图的列级别权限需要在表对象下设置）。 [#47702](https://github.com/StarRocks/starrocks/pull/47702)
- 存算分离主键模型表支持列模式部份更新。[#46516](https://github.com/StarRocks/starrocks/issues/46516)
- Stream Load 支持在传输过程中对数据进行压缩，减少网络带宽开销。可以通过 `compression` 或 `Content-Encoding` 参数指定不同的压缩方式，支持 GZIP、BZIP2、LZ4_FRAME、ZSTD 压缩算法。[#43732](https://github.com/StarRocks/starrocks/pull/43732)

### 功能优化

- 优化当前 IdChain 的 hashcode 实现，降低 FE 启动耗时。 [#47599](https://github.com/StarRocks/starrocks/pull/47599)
- 优化 FILES() 函数中 `csv.trim_space` 参数的报错信息，检查非法字符并给出合理提示。 [#44740](https://github.com/StarRocks/starrocks/pull/44740)
- Stream Load 支持将 `\t` 和 `\n` 分别作为行列分割符，无需转成对应的十六进制 ASCII 码。[#47302](https://github.com/StarRocks/starrocks/pull/47302)

### 问题修复

修复了如下问题：

- 在 Schema Change 过程中，因 Tablet 迁移，文件位置变化导致的 Schema Change 失败。[#45517](https://github.com/StarRocks/starrocks/pull/45517)
- 因为字段默认值中包含 `\`、`\r` 等控制字符导致跨集群迁移工具在目标集群中建表失败。 [#47861](https://github.com/StarRocks/starrocks/pull/47861)
- BE 重启后 bRPC 持续失败。 [#40229](https://github.com/StarRocks/starrocks/pull/40229)
- `user_admin` 角色可以通过 ALTER USER 命令修改 root 密码。[#47801](https://github.com/StarRocks/starrocks/pull/47801)
- 主键索引写入失败，导致数据写入报错。[#48045](https://github.com/StarRocks/starrocks/pull/48045) 

### 行为变更

- 写出 Hive、Iceberg 默认打开 Spill。 [#47118](https://github.com/StarRocks/starrocks/pull/47118)
- 修改 BE 配置项 `max_cumulative_compaction_num_singleton_deltas` 默认值为 `500`。[#47621](https://github.com/StarRocks/starrocks/pull/47621)
- 用户创建分区表但未设置分桶数时，当分区数量超过 5 个后，系统自动设置分桶数的规则更改为 `max(2 * BE 或 CN 数量, 根据最大历史分区数据量计算得出的分桶数)`。（原来的规则是根据最大历史分区数据量计算的分桶数）。[#47949](https://github.com/StarRocks/starrocks/pull/47949)
- INSERT INTO 导入主键表时指定 Column List 会执行部分列更新。先前版本中，指定 Column List 仍然导致 Full Upsert。

### 降级说明

如需将 v3.3.1 及以上集群降级至 v3.2，用户需要在回滚前清理所有的临时表。步骤如下：

1. 禁止用户创建新的临时表：

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_experimental_temporary_table"="false"); 
   ```

2. 查询系统内是否存在临时表：

   ```SQL
   SELECT * FROM information_schema.temp_tables;
   ```

3. 如系统内存在临时表，通过以下命令清理系统内的临时表（需要 SYSTEM 级 OPERATE 权限）：

   ```SQL
   CLEAN TEMPORARY TABLE ON SESSION 'session';
   ```

## 3.3.0

发布日期：2024 年 6 月 21 日

### 功能及优化

#### 存算分离

- 优化了存算分离集群的 Fast Schema Evolution 能力，DDL 变更降低到秒级别。具体信息，参考 [设置 Fast Schema Evolution](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#设置-fast-schema-evolution)。
- 为了满足从存算一体到存算分离架构的数据迁移需求，社区正式发布 [StarRocks 数据迁移工具](https://docs.starrocks.io/zh/docs/administration/data_migration_tool/)。该工具同样可用于实现存算一体集群之间的数据同步和容灾方案。
- [Preview] 存算分离集群存储卷适配 AWS Express One Zone Storage，提升数倍读写性能。具体信息，参考 [CREATE STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME/#参数说明)。
- 优化了存算分离集群的垃圾回收机制，支持手动 Manual Compaction，可以更高效的回收对象存储上的数据。具体信息，参考 [手动 Compaction](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#手动-compaction31-版本起)。
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
  - Parquet 格式的 Iceberg v2 表查询支持 [equality-delete](https://docs.starrocks.io/zh/docs/data_source/catalog/iceberg/iceberg_catalog#使用说明)。
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

  针对 ARM 架构指令集大幅优化性能。在使用 AWS Gravinton 机型测试的情况下，在 SSB 100G 测试中，使用 ARM 架构时的性能比 x86 快 11%；在 Clickbench 测试中，使用 ARM 架构时的性能比 x86 快 39%，在 TPC-H 100G  测试中，使用 ARM 架构时的性能比 x86 快 13%，在 TPC-DS 100G  测试中，使用 ARM 架构时的性能比 x86 快 35%。

- **中间结果落盘（Spill to Disk）能力 GA**：优化复杂查询的内存占用，优化 Spill 的调度，保证大查询都能稳定执行，不会 OOM。
- [Preview] 支持将中间结果 [落盘至对象存储](https://docs.starrocks.io/zh/docs/administration/management/resource_management/spill_to_disk/#preview-%E5%B0%86%E4%B8%AD%E9%97%B4%E7%BB%93%E6%9E%9C%E8%90%BD%E7%9B%98%E8%87%B3%E5%AF%B9%E8%B1%A1%E5%AD%98%E5%82%A8)。
- **支持更多索引**：
  - [Preview] 支持[全文倒排索引](https://docs.starrocks.io/zh/docs/table_design/indexes/inverted_index/)，以加速全文检索。
  - [Preview] 支持 [N-Gram bloom filter 索引](https://docs.starrocks.io/zh/docs/table_design/indexes/Ngram_Bloom_Filter_Index/)，以加速 `LIKE` 查询或 `ngram_search` 和 `ngram_search_case_insensitive` 函数的运算速度。
- 提升 Bitmap 系列函数的性能并优化内存占用，补充了 Bitmap 导出到 Hive 的能力以及配套的 [Hive Bitmap UDF](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/hive_bitmap_udf/)。
- **[Preview] 支持 [Flat JSON](https://docs.starrocks.io/zh/docs/using_starrocks/Flat_json/)**：导入时自动检测 JSON 数据并提取公共字段，自动创建 JSON 的列式存储结构，JSON 查询性能达到 Struct 的水平。
- **[Preview] 优化全局字典**：提供字典对象，将字典表中的键值对映射关系存在各个 BE 节点的内存中。通过 `dictionary_get()` 函数直接查询 BE 内存中的字典对象，相对于原先使用 `dict_mapping` 函数查询字典表，查询速度更快。并且字典对象还可以作为维度表，可以通过 `dictionary_get()` 函数直接查询字典对象来获取维度值，相对于原先通过 JOIN 维度表来获取维度值，查询速度更快。
- [Preview] 支持 Colocate Group Execution: 大幅降低在 colocated 表上执行 Join 和 Agg 算子时的内存占用，能够更稳定的执行大查询。
- 优化 CodeGen 性能：默认打开 JIT，在复杂表达式计算场景下性能提升 5 倍。
- 支持使用向量化技术来进行正则表达式匹配，可以降低 `regexp_replace` 函数计算的 CPU 消耗。
- 优化 Broadcast Join，在右表为空时可以提前结束 Join 操作。
- 优化数据倾斜情况下的 Shuffle Join，避免 OOM。
- 聚合查询包含 `Limit` 时，多 Pipeline 线程可以共享 Limit 条件从而防止计算资源浪费。

#### 存储优化与集群管理

- **[增强 Range 分区的灵活性](https://docs.starrocks.io/zh/docs/table_design/Data_distribution/#range-分区)**：新增支持三个特定时间函数作为分区列，可以将时间戳或字符串的分区列值转成日期，然后按转换后的日期划分分区。
- **FE 内存可观测性**：提供 FE 内各模块的详细内存使用指标，以便更好地管理资源。
- **[优化 FE 中的元数据锁](https://docs.starrocks.io/zh/docs/administration/management/FE_configuration/#lock_manager_enabled)**：提供 Lock Manager，可以对 FE  中的元数据锁实现集中管理，例如将元数据锁的粒度从库级别细化为表级别，可以提高导入和查询的并发性能。在 100 并发小数据量导入场景下，导入耗时减少 35%。
- **[使用标签管理 BE](https://docs.starrocks.io/zh/docs/administration/management/resource_management/be_label/)**：支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，以保证数据在机架或数据中心等之间均匀分布，应对某些机架断电或数据中心故障情况下的灾备需求。
- **[优化排序键](https://docs.starrocks.io/zh/docs/table_design/indexes/Prefix_index_sort_key/)**：明细表、聚合表和更新表均支持通过 `ORDER BY` 子句指定排序键。
- **[Experimental] 优化非字符串标量类型数据的存储效率**：这类数据支持字典编码，存储空间下降 12%。
- **主键表支持 Size-tiered Compaction 策略**：降低执行 Compaction 时写 I/O 和内存开销。存算分离和存算一体集群均支持该优化。您可以通过 BE 配置项 `enable_pk_size_tiered_compaction_strategy` 控制是否启用该功能。默认开启。  
- **优化主键表持久化索引的读 I/O**：支持按照更小的粒度（页）读取持久化索引，并且改进持久化索引的 bloom filter。存算分离和存算一体集群均支持该优化。
- 支持 IPv6 部署：可以基于 IPv6 网络部署集群。

#### 物化视图

- **基于视图的改写**：针对视图的查询，支持改写至基于视图创建的物化视图上。具体信息，参考 [基于视图的物化视图查询改写](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#基于视图的物化视图查询改写)。
- **基于文本的改写**：引入基于文本的改写能力，用于改写具有相同抽象语法树的查询（或其子查询）。具体信息，参考 [基于文本的物化视图改写](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#基于文本的物化视图改写)。
- **[Preview] 支持为直接针对物化视图的查询指定透明改写模式**：开启物化视图属性 `transparent_mv_rewrite_mode` 后，当用户直接查询物化视图时，StarRocks 会自动改写查询，将已经刷新的物化视图分区中的数据和未刷新分区对应的原始数据做自动 Union 合并。该模式适用于建模场景下，需要保证数据一致，但同时还希望控制刷新频率降低刷新成本的场景。具体信息，参考 [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/#参数-1)。
- 支持物化视图聚合下推：开启系统变量 `enable_materialized_view_agg_pushdown_rewrite` 后，用户可以利用单表[聚合上卷](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#聚合上卷改写)类的物化视图来加速多表关联场景，把聚合下推到 Join 以下的 Scan 层来显著提升查询效率。具体信息，参考 [聚合下推](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#聚合下推)。
- **物化视图改写控制的新属性**：通过 `enable_query_rewrite` 属性实现禁用查询改写，减少整体开销。如果物化视图仅用于建模后直接查询，没有改写需求，则禁用改写。具体信息，参考 [CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/#参数-1)。
- **物化视图改写代价优化**：增加候选物化视图个数的控制，以及更好的筛选算法。增加 MV plan cache。从而整体降低改写阶段 Optimizer 耗时。具体信息，参考 `cbo_materialized_view_rewrite_related_mvs_limit`。
- **Iceberg 物化视图更新**：Iceberg 物化视图现支持分区更新触发的增量刷新和 Iceberg Partition Transforms。具体信息，参考 [使用物化视图加速数据湖查询](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/data_lake_query_acceleration_with_materialized_views/#选择合适的刷新策略)。
- **增强的物化视图可观测性**：改进物化视图的监控和管理，以获得更好的系统洞察。具体信息，参考 [异步物化视图监控项](https://docs.starrocks.io/zh/docs/administration/management/monitoring/metrics/#异步物化视图监控项)。
- **提升大规模物化视图刷新的效率**：支持全局 FIFO 调度，优化嵌套物化视图级联刷新策略，修复高频刷新场景下部分问题。
- **多事实表分区刷新**：基于多事实表创建的物化视图支持任意事实表更新后，物化视图都可以进行分区级别增量刷新，增加数据管理的灵活性。具体信息，参考 [多基表对齐分区](https://docs.starrocks.io/zh/docs/using_starrocks/async_mv/use_cases/create_partitioned_materialized_view/#多基表对齐分区)。

#### 函数支持

- DATETIME 完整支持微秒（Microsecond），相关的时间函数和导入均支持了新的单位。
- 新增如下函数：
  - [字符串函数](https://docs.starrocks.io/zh/docs/category/string-1/)：crc32、url_extract_host、ngram_search
  - Array 函数：[array_contains_seq](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - 时间日期函数：[yearweek](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/yearweek/)
  - 数学函数：[crbt](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/cbrt/)

#### 生态支持

- [Experimental] 新增 ClickHouse 语法转化工具 [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer)。
- StarRocks 提供的 Flink connector v1.2.9 已与 Flink CDC 3.0 框架集成，构建从 CDC 数据源到 StarRocks 的流式 ELT 管道。该管道可以将整个数据库、分库分表以及来自源端的 Schema Change 都同步到 StarRocks。更多信息，您参见 [Flink CDC 同步（支持 Schema Change）](https://docs.starrocks.io/zh/docs/loading/Flink-connector-starrocks/#使用-flink-cdc-30-同步数据支持-schema-change)。

### 行为及参数变更

#### 建表与分区分桶

- 用户使用 CTAS 创建 Colocate 表时，必须指定 Distribution Key。[#45537](https://github.com/StarRocks/starrocks/pull/45537)
- 用户创建非分区表但未设置分桶数时，系统自动设置的分桶数最小值修改为 `16`（原来的规则是 `2 * BE 或 CN 数量`，也即最小会创建 2 个 Tablet）。如果是小数据且想要更小的分桶数，需要手动设置。[#47005](https://github.com/StarRocks/starrocks/pull/47005)

#### 导入与导出

- `__op` 被系统保留作为特殊列名，默认设置下不再允许用户创建名字为 `__op` 前缀的列。如果需要使用，需要设置 FE 配置项 `allow_system_reserved_names` 为 `true`。请注意，在主键表中创建 `__op` 前缀的列，会导致未知行为。[#46239](https://github.com/StarRocks/starrocks/pull/46239)
- Routine Load 作业运行中，如果无法消费到数据的时间超过了 FE 配置项 `routine_load_unstable_threshold_second` 中设置的时间（默认 1 小时），则作业状态会变成 `UNSTABLE`，但作业会继续运行。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- FE 配置项 `enable_automatic_bucket` 的默认值改为 `TRUE`。为 `TRUE` 时，系统会为新建表自动设置 `bucket_size` 参数，从而新建表就启用了自动分桶（优化版的随机分桶）。但在 V3.2 中，设置 `enable_automatic_bucket` 为 `true` 不会生效，用户必须设置 `bucket_size` 以开启自动分桶。此举可以方便从 V3.3 回滚到 V3.2，减少回滚风险。

#### 查询与半结构化

- 单个查询在 Pipeline 框架中执行时所使用的内存限制不再受 `exec_mem_limit` 限制，仅由 `query_mem_limit` 限制。取值为 `0` 表示没有限制。 [#34120](https://github.com/StarRocks/starrocks/pull/34120)
- JSON 对象中 NULL 值，在 IS NULL 和 IS NOT NULL 的处理中，被当作 SQL NULL 值对待。如 `parse_json('{"a": null}') -> 'a' IS NULL` 返回 `1`，`parse_json('{"a": null}') -> 'a' IS NOT NULL` 返回 `0`。[#42765](https://github.com/StarRocks/starrocks/pull/42765) [#42909](https://github.com/StarRocks/starrocks/pull/42909)
- 新增会话变量 `cbo_decimal_cast_string_strict` 用于优化器控制 DECIMAL 类型转为 STRING 类型的行为。当取值为 `true` 时，使用 v2.5.x 及之后版本的处理逻辑，执行严格转换（按 Scale 截断补 `0`）；当取值为 `false`时，保留 v2.5.x 之前版本的处理逻辑（按有效数字处理）。默认值是 `true`。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- 系统变量 `cbo_eq_base_type` 的默认值修改为 `decimal`，表示 DECIMAL 类型和 STRING 类型的数据比较时的，默认按 DECIMAL 类型进行比较。[#43443](https://github.com/StarRocks/starrocks/pull/43443)

#### 其他

- 物化视图属性 `partition_refresh_num` 默认值从 `-1` 调整为 `1`，当物化视图有多个分区需要刷新时，原来在一个刷新任务重刷新所有的分区，当前会一个分区一个分区增量刷新，避免先前行为消耗过多资源。可以通过 FE 参数 `default_mv_partition_refresh_number` 调整默认行为。
- 系统原先按照 GMT+8 时区的时间调度数据库一致性检查，现在将按照当地时区的时间进行调度。[#45748](https://github.com/StarRocks/starrocks/issues/45748)
- 默认启用 Data Cache 来加速数据湖查询。用户也可通过 `SET enable_scan_datacache = false` 手动关闭 Data Cache。
- 对于存算分离场景，在降级到 v3.2.8 及其之前版本时，如需复用先前 Data Cache 中的缓存数据，需要手动修改 **starlet_cache** 目录下 Blockfile 文件名，将文件名格式从 `blockfile_{n}.{version}` 改为 `blockfile_{n}`，即去掉版本后缀。具体可参考 [Data Cache 使用说明](https://docs.starrocks.io/zh/docs/using_starrocks/caching/block_cache/#使用说明)。v3.2.9 及以后版本自动兼容 v3.3 文件名，无需手动执行该操作。
- 支持动态修改 FE 参数 `sys_log_level`。[#45062](https://github.com/StarRocks/starrocks/issues/45062)
- Hive Catalog 属性 `metastore_cache_refresh_interval_sec` 默认值由 `7200` (两小时) 变为 `60` (一分钟)。 [#46681](https://github.com/StarRocks/starrocks/pull/46681)

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

4. 如果您想将集群降级至 v3.2.8 或 v3.1.14 之前的小版本，则必须删除所有使用 `PROPERTIES('compression' = 'lz4')` 属性创建的异步物化视图。

5. 执行以下语句为元数据创建镜像文件：

   ```sql
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 在新的镜像文件传输到所有 FE 节点的目录 **meta/image** 之后，降级其中一个 Follower FE 节点，确认没有问题后继续降级集群中的其他节点。

