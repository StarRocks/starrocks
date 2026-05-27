---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**降级说明**

- 升级至 v4.0 后，请勿直接将集群降级至 v3.5.0 和 v3.5.1，否则会导致元数据不兼容和 FE Crash。您必须降级到 v3.5.2 或更高版本以避免出现此问题。
- 在将集群从 v4.0.2 降级到 v4.0.1、v4.0.0 以及 v3.5.2~v3.5.10 之前，请先执行以下语句：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  重新升级至 v4.0.2及以上版本后，请执行以下语句：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.10

发布日期：2026 年 5 月 9 日

### 行为变更

- `INSERT INTO FILES` 错误信息中的云存储凭证现已被脱敏，避免凭证通过错误日志或 `SHOW LOAD` 输出意外泄露。[#71245](https://github.com/StarRocks/starrocks/pull/71245)
- 不再允许在 Hive Catalog 中查询 insert-only ACID Hive 表。此前由于无法识别此类表的 INSERT OVERWRITE 操作，查询结果可能比实际可见行多。现在此类表的查询将直接返回错误，避免静默的数据正确性问题。[#71460](https://github.com/StarRocks/starrocks/pull/71460)

### 功能优化

- 在 Iceberg `PartitionData` 构造路径中新增 Avro Schema 缓存，避免分区数较多时反复的 Jackson `ObjectMapper` 分配，显著降低分区加载内存开销。[#72215](https://github.com/StarRocks/starrocks/pull/72215)
- 优化 `CatalogRecycleBin.getAdjustedRecycleTimestamp`，避免每次调用都重新构建 table-id 映射，降低回收站清理和 Tablet 调度的额外开销。[#72128](https://github.com/StarRocks/starrocks/pull/72128)
- 存算分离模式下 `OlapTableSink.createLocation` 现在会批量获取 Tablet 位置信息，消除按 Tablet 单独发起 StarOS RPC 的开销，显著缩短 Planner 临界区时间。[#72041](https://github.com/StarRocks/starrocks/pull/72041)
- Java UDAF 现在按查询粒度只加载和初始化一次，并在多个 Pipeline Driver 间复用，消除高 `pipeline_dop` 下 Driver 准备阶段的线性开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- 新增 BE 指标 `starrocks_be_staros_shard_info_fallback_total` 和 `starrocks_be_staros_shard_info_fallback_failed_total`，用于追踪 StarOS Worker 在本地缓存未命中后回退到 starmgr 拉取 Shard 信息的次数。[#71620](https://github.com/StarRocks/starrocks/pull/71620)
- File Bundle 写入现在优先选择 Tablet 本地的聚合节点，避免跨节点查找 Shard 信息。[#71613](https://github.com/StarRocks/starrocks/pull/71613)
- Audit Log 中现在记录每个查询直接引用的表和视图。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- `INSERT INTO FILES` 导出 CSV 时支持 `csv.enclose` 和 `csv.escape` 属性，可控制字段引用与转义行为。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 支持基于 DN 模式的 LDAP 直接绑定认证，在单租户 LDAP 场景下无需配置管理员搜索账号。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- 为存算分离集群新增 `starrocks_fe_tablet_num` 指标，与存算一体集群的指标对齐。[#71444](https://github.com/StarRocks/starrocks/pull/71444)
- `star_mgr_meta_sync_interval_sec` 现在支持通过 `ADMIN SET FRONTEND CONFIG` 在运行时动态修改，新值在下一个同步周期生效，无需重启 FE。[#71675](https://github.com/StarRocks/starrocks/pull/71675)

### 问题修复

修复了以下问题：

- 存算分离 Combined Txn Log 模式下，按分区 Coordinator 分发的 INSERT 中存在跨 Sender 竞态，可能将合法 txn log 错误识别为 orphan 并丢弃，导致事务长时间不可见。[#72237](https://github.com/StarRocks/starrocks/pull/72237)
- 存算分离 Combined Txn Log 模式下，运行时通过 `_incremental_open_node_channel` 打开的增量 Channel 因沿用旧的 "sender_id == 0 收集所有日志" 规则，会静默丢失 txn log。[#71992](https://github.com/StarRocks/starrocks/pull/71992)
- `RuntimeProfile::to_thrift()` 在序列化期间被其他线程重置 Counter min/max 时，会因 `std::bad_optional_access` 导致 BE 崩溃。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- Flat JSON merge 在某一侧为空时结果不一致的问题。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 用户在 `CREATE TABLE ... PROPERTIES (...)` 中显式指定 `format-version` 时，创建 Iceberg 表会因 "Multiple entries with same key: format-version" 报错的问题。[#72828](https://github.com/StarRocks/starrocks/pull/72828)
- `CompactionScheduler.startCompaction` 在单表临界区内持有了库级 READ 锁，阻塞同库其他表的并发 DDL；现已改为库 IS + 表 READ。[#72178](https://github.com/StarRocks/starrocks/pull/72178)
- `StarMgrMetaSyncer.syncTableMetaInternal` 和 `syncTableColocationInfo` 在外部 StarOS RPC 期间持有库级 READ/WRITE 锁，导致同库内所有表的 CREATE/DROP/ALTER/RENAME 在 RPC 期间被冻结。[#72108](https://github.com/StarRocks/starrocks/pull/72108)
- `StarMgrMetaSyncer.getAllPartitionShardGroupId` 在遍历所有云原生表和物理分区期间持续持有库 READ 锁，在大型 Catalog 上会阻塞等待库写锁的 FE 线程。[#71614](https://github.com/StarRocks/starrocks/pull/71614)
- `getTableNamesViewWithLock` 中冗余的库 READ 锁。底层 `nameToTable` 已是 `ConcurrentHashMap`，外层锁仅引入竞争。[#72042](https://github.com/StarRocks/starrocks/pull/72042)
- 只读的 `/api/{db}/{table}/_count` REST 端点在计算 `proximateRowCount()` 时不必要地获取了库 WRITE 锁的问题。[#72053](https://github.com/StarRocks/starrocks/pull/72053)
- Tablet Split、Schema Change、ALTER 等操作通过推进 `nextVersion` 预占版本号但无对应 publish 形成版本空洞，进而引发批量 publish 死锁的问题。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 存算一体模式下，Rowset 元数据 LRU 缓存满时执行 warmup 引发的死锁问题。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `PipelineTimerTask` 因消费者注册与完成信号顺序错误，可能在 `waitUtilFinished` 中卡住的问题。[#72058](https://github.com/StarRocks/starrocks/pull/72058)
- `ConnectorSinkPassthroughExchanger::accept` 中 `_writer_count` 的条件竞态导致 BE 因 vector 越界访问触发 SIGSEGV 的问题。[#71848](https://github.com/StarRocks/starrocks/pull/71848)
- `LoadChannel::get_load_replica_status` 中临时 `shared_ptr` 析构导致的 use-after-free 问题。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- Information Schema Sink 在异步 RPC 闭包处理中缺少引用计数，导致 use-after-free 的问题。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- `reverse(DecimalV3)` 由于对 Decimal 宽度处理不当导致 BE 崩溃的问题。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- `UNNEST` 生成的列其 define 表达式被错误地标记为 ARRAY 类型，下游全局字典生成时引发 BE 崩溃的问题。[#72027](https://github.com/StarRocks/starrocks/pull/72027)
- 创建 Iceberg 外表时若 Transform 参数顺序非法（如 `bucket(4, region)`、`truncate(4, region)`），FE 会抛出 NPE 而不是正常的 Analyzer 错误的问题。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 当某 Iceberg 表的首个查询不需要列统计（例如 `SELECT *`）时，Manifest Data File 缓存条目缺失列统计信息的问题。[#71913](https://github.com/StarRocks/starrocks/pull/71913)
- Iceberg 表按 `bucket(col, N)` 分区时，因 `PruneHDFSScanColumnRule` 注入占位物化列导致 min/max 优化被静默跳过、回退到全文件扫描的问题。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- `AggregateJoinPushDownRule` 通过 `Table.getId()` 比较外表（如 IcebergTable）身份，而 Connector 表 ID 可能在 Plan 重建时发生变化，导致基于 Iceberg 的物化视图改写失败的问题。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- Hive 动态分区 INSERT OVERWRITE 提交时，若 Metastore 仍记录某分区但其 Location 在文件系统中不存在则失败的问题；现在会在提交前创建缺失的分区目录。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- Arrow 返回 Dictionary 类型列（含 ARRAY、STRUCT、MAP 内嵌套的 Dictionary）时，Parquet Scanner 报 `Illegal converting from arrow type(dictionary) ...` 的问题。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- `ColocatedBackendSelector.Assignment` 在增量批次处理中保留了上一批次的 Scan Range，导致这些文件被重复部署和扫描的问题。[#71789](https://github.com/StarRocks/starrocks/pull/71789)
- `PruneShuffleColumnRule` 在剪除 Exchange Shuffle 列后未更新 Join 的 `outputProperty`，导致下游分布信息错误的问题。[#72003](https://github.com/StarRocks/starrocks/pull/72003)
- 多阶段物化视图改写第一阶段禁用 `JoinPredicatePushDown` 时，`PushDownJoinOnExpressionToChildProject` 不能正常工作，且第二阶段缺失 Project 节点导致 Shuffle 分布错误的问题。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- 谓词归一化使同一标量子查询占位符多次出现时，`ReplaceSubqueryRewriteRule` 重复挂接 `Apply` 节点引入冗余子查询分支的问题。[#71155](https://github.com/StarRocks/starrocks/pull/71155)
- `EventScheduler` 中 Join Probe 已完成时 short-circuit 检测未将 Pipeline 整体置为完成状态的问题。[#71740](https://github.com/StarRocks/starrocks/pull/71740)
- 通过 `aws.s3.iam_role_arn` 配置的 AWS Assume Role 未应用到 JNI Scanner（RCFile/Avro/SequenceFile/Hudi），导致 S3 403 的问题。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- Oracle JDBC 谓词下推时日期字面值未匹配 Oracle NLS 格式导致 SQL 报错的问题；现在以 `date '...'` 形式发出。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- 存算分离模式下，Follower FE 转发 DDL 给 Leader 后仅等待 FE Journal 回放而未等待 StarMgr Journal，导致紧随建表的查询出现 "no queryable replica" 的问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 主键表 `get_tablet_stats` 对每个 Segment 都通过 `get_del_vec_in_meta()` 重新加载完整 `TabletMetadata` 的问题。[#71672](https://github.com/StarRocks/starrocks/pull/71672)
- Arrow Flight 在空结果集情况下返回的列名为 `r`（占位名）而不是真实 Schema 的问题。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- 调整 `parallel_clone_task_per_path` 时 CLONE 线程池大小未将 Store Path 数纳入计算的问题。[#71484](https://github.com/StarRocks/starrocks/pull/71484)
- 资源组用户分类器拒绝以数字开头的用户名（而 `CREATE USER` 允许）的问题；现在使用与 `CREATE USER` 相同的校验规则。[#71470](https://github.com/StarRocks/starrocks/pull/71470)
- `HttpServerHandler.channelInactive` 在 `isRegistered()` 为 false 时跳过 `unregisterConnection`，导致早期失败请求泄漏 Connection Map 条目的问题。[#72006](https://github.com/StarRocks/starrocks/pull/72006)
- Java UDF 中 JNI 调用（如 `NewObject`、`NewArray`、`NewStringUTF`）未检查异常或 null 返回，可能导致静默失败或未定义行为的问题。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `be_tablets.DATA_SIZE` 错误地报告 `total_disk_size`（包括 Rowset 内嵌索引以及 Lake PK 持久化索引），现统一改为报告 Rowset 列数据字节数。[#70735](https://github.com/StarRocks/starrocks/pull/70735)
- `StarMgrMetaSyncer` 在没有需删除的 Shard 时仍打印 "Failed to batch drop tablets" 告警日志的问题。[#72209](https://github.com/StarRocks/starrocks/pull/72209)
- CVE-2026-42198（pgjdbc）和 CVE-2026-5598（BouncyCastle）：将 `org.postgresql:postgresql` 升级到 42.7.11，BouncyCastle 升级到 1.84。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- Netty CVE：将 Netty 升级到 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- 升级 Broker 的 netty / jetty / awssdk / jackson 依赖以处理已知 CVE。[#72184](https://github.com/StarRocks/starrocks/pull/72184)
- 将 jetty-http 升级到 9.4.58.v20250814，修复旧版本中的已知 CVE。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 由于 jetty 9.x 已 EOL 且上游不再发布修复版本，临时屏蔽 CVE-2026-2332 以解锁构建流程。[#71914](https://github.com/StarRocks/starrocks/pull/71914)

## 4.0.9

发布日期：2026 年 4 月 16 日

### 行为变更

- 当 VARBINARY 列嵌套在复合类型（ARRAY、MAP 或 STRUCT）内时，StarRocks 现在在 MySQL 结果集中以正确的二进制格式编码这些值。此前，系统直接输出原始字节，当数据中包含空字节或不可打印字符时，会破坏文本协议的解析。此变更可能影响在嵌套类型中处理 VARBINARY 数据的下游客户端或工具。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- Routine Load 任务在遇到不可重试错误（例如行数据导致主键大小超出限制）时，现在会自动暂停 Job。此前，此类错误不会被 FE 事务状态处理器识别为不可重试错误，导致 Job 无限重试。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- `SHOW CREATE TABLE` 和 `DESC` 语句现在会显示 Paimon 外表的主键列信息。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- 云原生 Tablet 元数据获取操作（如 `get_tablet_stats`、`get_tablet_metadatas`）现在使用专属线程池，不再与其他任务共享 `UPDATE_TABLET_META_INFO` 线程池。此变更避免了元数据获取与修复等任务之间的资源竞争，新增 BE 配置项用于调整该线程池的大小。[#70492](https://github.com/StarRocks/starrocks/pull/70492)

### 功能优化

- 新增 Session 变量，用于控制 MySQL 协议响应中 VARBINARY 值的编码行为，支持按连接级别精细控制二进制结果编码。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 集群快照中新增 `snapshot_meta.json` 标记文件，支持在恢复快照前进行完整性校验。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- 在 `WarehouseManager` 中为被静默吞没的异常新增 Warning 日志，提升异常可观测性。[#71215](https://github.com/StarRocks/starrocks/pull/71215)
- 新增 Iceberg 元数据表查询指标，支持性能监控与诊断。[#70825](https://github.com/StarRocks/starrocks/pull/70825)
- `regexp_replace()` 函数现在支持在 FE 查询规划阶段进行常量折叠，降低参数为常量字符串时的规划开销。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 为 Iceberg 时间旅行查询新增分类指标，提升监控与性能分析能力。[#70788](https://github.com/StarRocks/starrocks/pull/70788)
- Update Compaction 被暂停时新增日志输出，提升 Compaction 生命周期的可见性。[#70538](https://github.com/StarRocks/starrocks/pull/70538)
- `SHOW COLUMNS` 现在返回 PostgreSQL 外表的列注释信息。[#70520](https://github.com/StarRocks/starrocks/pull/70520)
- 支持在查询发生异常时导出查询执行计划，提升运行时故障的可诊断性。[#70387](https://github.com/StarRocks/starrocks/pull/70387)
- DDL 操作中的 Tablet 删除现在采用批量方式，降低对 Tablet 元数据写锁的竞争。[#70052](https://github.com/StarRocks/starrocks/pull/70052)
- 新增 Force Drop 恢复机制，用于强制删除因陷入错误状态而无法通过常规方式删除的同步物化视图。[#70029](https://github.com/StarRocks/starrocks/pull/70029)

### 问题修复

- Profile 中 `START_TIME` 和 `END_TIME` 未按 Session 时区显示的问题。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `PushDownAggregateRewriter` 在处理 CASE-WHEN/IF 表达式时发生共享对象变更的问题，该问题可能导致查询结果错误。[#71309](https://github.com/StarRocks/starrocks/pull/71309)
- `ThreadPool::do_submit` 在线程创建失败时触发的 use-after-free 问题。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- `information_schema.tables` 在等值谓词中未对特殊字符进行转义导致结果错误的问题。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- 物化视图变为 Inactive 状态后，其调度器仍继续运行的问题。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 并发 ALTER 任务中 `UpdateTabletSchemaTask` 存在签名冲突，导致 Schema 更新任务被跳过的问题。[#71242](https://github.com/StarRocks/starrocks/pull/71242)
- 仅包含 MCV（高频值）条目的直方图在行数估算时产生 NaN 的问题。[#71241](https://github.com/StarRocks/starrocks/pull/71241)
- AWS SDK 集成中缺少 S3 Transfer Manager 依赖的问题。[#71230](https://github.com/StarRocks/starrocks/pull/71230)
- `TaskManager` 调度回调未校验当前节点是否为 Leader 的问题，该问题可能导致 Follower 节点重复执行任务。[#71156](https://github.com/StarRocks/starrocks/pull/71156)
- Leader 转发请求完成后未清理 `ConnectContext` 线程本地信息，导致上下文污染后续请求的问题。[#71141](https://github.com/StarRocks/starrocks/pull/71141)
- 短路点查中分区谓词丢失导致查询结果错误的问题。[#71124](https://github.com/StarRocks/starrocks/pull/71124)
- 在 Stream Load 或 Broker Load 中分析生成列时，若 Load Schema 中缺少被引用列则抛出 NullPointerException 的问题。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 并行 Segment/Rowset 加载错误处理路径中的 use-after-free 问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 在同一 Publish Batch 中写入操作先于 Compaction 执行时遗留 delvec 孤立条目的问题。[#71049](https://github.com/StarRocks/starrocks/pull/71049)
- 内部通过 HTTP 回环检查查询进度时，查询被计入 `current_queries` 结果的问题。[#71032](https://github.com/StarRocks/starrocks/pull/71032)
- CVE-2026-33870 和 CVE-2026-33871。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- `SharedDataStorageVolumeMgr` 中读锁泄漏的问题。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- `locate()` 函数的输入列与结果列在 BinaryColumns 内部共享同一 NullColumn 引用导致结果错误的问题。[#70957](https://github.com/StarRocks/starrocks/pull/70957)
- 存算一体模式下 ALTER 操作中安全删除校验被错误执行的问题。[#70934](https://github.com/StarRocks/starrocks/pull/70934)
- `_all_global_rf_ready_or_timeout` 中的竞态条件，该问题可能导致全局 Runtime Filter 未能正确应用。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- 指标宏 `ACCUMULATED` 中 int32 溢出导致指标值静默溢出的问题。[#70889](https://github.com/StarRocks/starrocks/pull/70889)
- 字典编码合并 GROUP BY 查询结果错误的问题。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- CVE-2025-54920。[#70862](https://github.com/StarRocks/starrocks/pull/70862)
- 聚合 Spill 中 `set_finishing` 阶段哈希表状态处理不当导致潜在数据丢失的问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- `proxy_pass_request_body` 关闭时 `content-length` 头未被重置的问题。[#70821](https://github.com/StarRocks/starrocks/pull/70821)
- Load 操作的 Spill 目录在对象析构函数中被清理而非在 `DeltaWriter::close()` 中清理，导致 Spill 数据被提前删除的问题。[#70778](https://github.com/StarRocks/starrocks/pull/70778)
- `INSERT INTO ... BY NAME` 从 `FILES()` 导入时，对部分列集合的 Schema 下推处理不正确的问题。[#70774](https://github.com/StarRocks/starrocks/pull/70774)
- Connector Scan 节点在查询重试时未重置 Scan Range 来源，导致重试结果错误的问题。[#70762](https://github.com/StarRocks/starrocks/pull/70762)
- 主键模型 Tablet 在磁盘迁移路径为 A→B→A 时，因 GC 竞态导致 Rowset 元数据丢失的问题。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- 查询级 Warehouse Hint 导致 `ComputeResource` 对象泄漏到 `ConnectContext` 中，影响同连接后续查询的问题。[#70706](https://github.com/StarRocks/starrocks/pull/70706)
- `MySqlScanNode` 和 `JDBCScanNode` 中的冗余 Conjunct 导致 BE 报告 `VectorizedInPredicate` 类型不匹配错误的问题。[#70694](https://github.com/StarRocks/starrocks/pull/70694)
- Ubuntu 运行时环境缺少 `libssl-dev` 依赖的问题。[#70688](https://github.com/StarRocks/starrocks/pull/70688)
- Iceberg Manifest 缓存读取时未校验完整性，导致缓存部分写入时扫描结果错误的问题。[#70675](https://github.com/StarRocks/starrocks/pull/70675)
- `_tablet_multi_get_rpc` 中重复的闭包引用可能导致 use-after-free 的问题。[#70657](https://github.com/StarRocks/starrocks/pull/70657)
- Iceberg `ManifestReader` 中 Manifest 缓存写入不完整，导致缓存条目残缺的问题。[#70652](https://github.com/StarRocks/starrocks/pull/70652)
- `array_map()` 在处理包含 null 字面量元素的数组时崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64()` 函数处理大输入时发生栈溢出的问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- `INSERT INTO ... BY NAME` 从 `FILES()` 导入时使用位置映射而非名称映射进行 Schema 下推，导致数据写入错误列的问题。[#70622](https://github.com/StarRocks/starrocks/pull/70622)
- `NOT NULL` 约束被错误下推至 `FILES()` Schema 推断中，导致可空列加载失败的问题。[#70621](https://github.com/StarRocks/starrocks/pull/70621)
- 精确外部物化视图刷新在 Iceberg-like Connector 场景下未能正确回退的问题。[#70589](https://github.com/StarRocks/starrocks/pull/70589)
- 构建部分 Tablet Schema 时 `num_short_key_columns` 不匹配导致数据读取异常的问题。[#70586](https://github.com/StarRocks/starrocks/pull/70586)
- `MaskMergeIterator` 中子迭代器耗尽时 BE 崩溃的问题。[#70539](https://github.com/StarRocks/starrocks/pull/70539)
- 物化视图刷新任务对对应 Iceberg Snapshot 已过期的分区重复刷新的问题。[#70523](https://github.com/StarRocks/starrocks/pull/70523)
- starlet 配置参数无法设置的问题。[#70482](https://github.com/StarRocks/starrocks/pull/70482)
- 免锁物化视图改写路径错误回退到实时元数据，导致改写行为不一致的问题。[#70475](https://github.com/StarRocks/starrocks/pull/70475)
- `JoinHashTable::merge_ht` 在处理基于表达式的 Join Key 列时未跳过哑元行，导致 Join 结果错误的问题。[#70465](https://github.com/StarRocks/starrocks/pull/70465)
- `InformationFunction` 等值比较逻辑错误，导致特定查询结果不正确的问题。[#70464](https://github.com/StarRocks/starrocks/pull/70464)
- 内部函数 `__iceberg_transform_bucket` 的列类型不匹配问题。[#70443](https://github.com/StarRocks/starrocks/pull/70443)
- Iceberg 快照时间戳非单调递增时，Iceberg 物化视图刷新失败的问题。[#70382](https://github.com/StarRocks/starrocks/pull/70382)
- 用户认证信息在审计日志和 SQL 脱敏输出中被暴露的问题。[#70360](https://github.com/StarRocks/starrocks/pull/70360)
- 开启 Physical Split 时扫描空 Tablet 导致 CN 崩溃的问题。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 查询优化阶段消除冗余 CAST 后未保留 VARCHAR 列长度的问题。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- brpc 连接重试逻辑未正确处理被包装的 `NoSuchElementException`，导致重试后连接失败的问题。[#70203](https://github.com/StarRocks/starrocks/pull/70203)
- 统计信息估算时外连接列的 null fraction 未被保留，导致查询计划次优的问题。[#70144](https://github.com/StarRocks/starrocks/pull/70144)
- Connector Sink 操作在 Poller 线程上运行时内存追踪器泄漏的问题。[#70121](https://github.com/StarRocks/starrocks/pull/70121)

## 4.0.8

发布日期：2026 年 3 月 25 日

### 行为变更

- 改进 `sql_mode` 处理逻辑：当设置了 `DIVISION_BY_ZERO` 或 `FAIL_PARSE_DATE` 模式时，`str_to_date`/`str2date` 函数中的除以零及日期解析失败不再被静默忽略，而是返回错误。[#70004](https://github.com/StarRocks/starrocks/pull/70004)
- `sql_mode` 设置为 `FORBID_INVALID_DATE` 时，`INSERT VALUES` 子句中的无效日期现在会被正确拒绝，而非绕过校验。[#69803](https://github.com/StarRocks/starrocks/pull/69803)
- 表达式分区生成列现在不再显示在 `DESC` 和 `SHOW CREATE TABLE` 的输出中。[#69793](https://github.com/StarRocks/starrocks/pull/69793)
- 审计日志（Audit Log）中不再包含客户端 ID。[#69383](https://github.com/StarRocks/starrocks/pull/69383)

### 功能优化

- 新增配置项 `local_exchange_buffer_mem_limit_per_driver`，将本地交换（Local Exchange）缓冲区大小限制为 `dop × local_exchange_buffer_mem_limit_per_driver`。[#70393](https://github.com/StarRocks/starrocks/pull/70393)
- 在 `check_missing_files` 中对跨版本的文件存在检查结果进行缓存，减少冗余存储 I/O。[#70364](https://github.com/StarRocks/starrocks/pull/70364)
- 支持在 `desc_hint_split_range` 设置为 ≤ 0 时，禁用降序 TopN 运行时过滤器的范围拆分与逆向扫描优化。[#70307](https://github.com/StarRocks/starrocks/pull/70307)
- 在 Trino 方言中为 `INSERT` 语句新增 `EXPLAIN` 和 `EXPLAIN ANALYZE` 支持。[#70174](https://github.com/StarRocks/starrocks/pull/70174)
- 优化存在 Position Delete 时的 Iceberg 读取性能。[#69717](https://github.com/StarRocks/starrocks/pull/69717)
- 优化基于分布键的物化视图最优选择策略，提升物化视图命中准确率。[#69679](https://github.com/StarRocks/starrocks/pull/69679)

### 问题修复

已修复以下问题：

- JDBC MySQL 下推不支持某些 CAST 操作导致查询失败。[#70415](https://github.com/StarRocks/starrocks/pull/70415)
- 物化视图刷新时的分区类型不匹配问题。新增 `mv_refresh_force_partition_type` 配置项，用于强制指定物化视图刷新时的分区类型。[#70381](https://github.com/StarRocks/starrocks/pull/70381)
- 从备份恢复时 `dataVersion` 未正确设置。[#70373](https://github.com/StarRocks/starrocks/pull/70373)
- 物化视图刷新任务中出现重复的分区名。[#70354](https://github.com/StarRocks/starrocks/pull/70354)
- SLF4J 参数化日志使用字符串拼接而非占位符参数的问题。[#70330](https://github.com/StarRocks/starrocks/pull/70330)
- 创建 Hive 表时注释（comment）未被正确设置。[#70318](https://github.com/StarRocks/starrocks/pull/70318)
- `FileSystemExpirationChecker` 在 HDFS 关闭缓慢时发生阻塞。[#70311](https://github.com/StarRocks/starrocks/pull/70311)
- `OlapTableSink` 中未对不同分区的分布列进行一致性校验。[#70310](https://github.com/StarRocks/starrocks/pull/70310)
- 常量折叠中双精度浮点数相加溢出时返回 INF 而非报错。[#70309](https://github.com/StarRocks/starrocks/pull/70309)
- Iceberg 表创建时字段名拼写错误：`common` 应为 `comment`。[#70267](https://github.com/StarRocks/starrocks/pull/70267)
- 某些场景下 root 用户未能绕过所有 Ranger 权限检查。[#70254](https://github.com/StarRocks/starrocks/pull/70254)
- 数据导入期间 `query_pool` 内存跟踪器出现负值。[#70228](https://github.com/StarRocks/starrocks/pull/70228)
- `AuditEventProcessor` 线程因 `OutOfMemoryException` 异常退出。[#70206](https://github.com/StarRocks/starrocks/pull/70206)
- `SplitTopNRule` 未正确应用分区裁剪。[#70154](https://github.com/StarRocks/starrocks/pull/70154)
- Schema Change 发布阶段 `cal_new_base_version` 中存在越界访问。[#70132](https://github.com/StarRocks/starrocks/pull/70132)
- 物化视图重写时忽略了基表中已删除的分区。[#70130](https://github.com/StarRocks/starrocks/pull/70130)
- 分区边界比较中类型不匹配导致分区谓词被意外裁剪。[#70097](https://github.com/StarRocks/starrocks/pull/70097)
- `str_to_date` 在 BE 运行时丢失微秒精度。[#70068](https://github.com/StarRocks/starrocks/pull/70068)
- Join Spill 过程在 `set_callback_function` 中发生崩溃。[#70030](https://github.com/StarRocks/starrocks/pull/70030)
- `gcs-connector` 升级至 3.0.13 版本后，Broker Load 的 GCS 鉴权失败。[#70012](https://github.com/StarRocks/starrocks/pull/70012)
- `DeltaWriter::close()` 在 bthread 上下文中调用时触发 DCHECK 失败。[#69960](https://github.com/StarRocks/starrocks/pull/69960)
- `AsyncDeltaWriter` 关闭/完成生命周期中存在释放后使用（use-after-free）竞态条件。[#69940](https://github.com/StarRocks/starrocks/pull/69940)
- 竞态条件导致写事务 EditLog 条目丢失。[#69899](https://github.com/StarRocks/starrocks/pull/69899)
- 已知 CVE 漏洞。[#69863](https://github.com/StarRocks/starrocks/pull/69863)
- Follower FE 在 `changeCatalogDb` 中未等待 Journal 回放完成。[#69834](https://github.com/StarRocks/starrocks/pull/69834)
- 包含反斜杠转义序列的 `LIKE` 模式匹配结果不正确。[#69775](https://github.com/StarRocks/starrocks/pull/69775)
- 重命名分区列后表达式分析失败。[#69771](https://github.com/StarRocks/starrocks/pull/69771)
- `AsyncDeltaWriter::close` 中存在释放后使用（use-after-free）崩溃。[#69770](https://github.com/StarRocks/starrocks/pull/69770)
- 本地分区 TopN 执行时崩溃。[#69752](https://github.com/StarRocks/starrocks/pull/69752)
- `PartitionColumnMinMaxRewriteRule` 因 `Partition.hasStorageData` 导致行为不正确。[#69751](https://github.com/StarRocks/starrocks/pull/69751)
- File Sink 输出文件名中出现重复的 CSV 压缩后缀。[#69749](https://github.com/StarRocks/starrocks/pull/69749)
- `lake_capture_tablet_and_rowsets` 操作未通过实验性配置项进行隔离控制。[#69748](https://github.com/StarRocks/starrocks/pull/69748)
- 存在影子分区时分区最小值裁剪结果不正确。[#69641](https://github.com/StarRocks/starrocks/pull/69641)
- Java UDTF/UDAF 在方法参数使用泛型类型时崩溃。[#69197](https://github.com/StarRocks/starrocks/pull/69197)
- 查询规划完成后未释放查询级别的元数据，导致并发查询执行时 FE OOM。[#68444](https://github.com/StarRocks/starrocks/pull/68444)
- 查询级别的 Warehouse hint 导致 `ConnectContext` 中的 `ComputeResource` 泄漏。[#70706](https://github.com/StarRocks/starrocks/pull/70706)
- 无锁物化视图重写错误地回退到实时元数据。[#70475](https://github.com/StarRocks/starrocks/pull/70475)
- `_tablet_multi_get_rpc` 中存在重复的闭包引用。[#70657](https://github.com/StarRocks/starrocks/pull/70657)
- `ReplaceColumnRefRewriter` 中存在无限递归。[#66974](https://github.com/StarRocks/starrocks/pull/66974)
- `NOT NULL` 约束被错误地下推到 `FILES()` 表函数的 Schema 中。[#70621](https://github.com/StarRocks/starrocks/pull/70621)
- 部分 Tablet Schema 中 `num_short_key_columns` 不匹配。[#70586](https://github.com/StarRocks/starrocks/pull/70586)
- 存算分离集群中 `COLUMN_UPSERT_MODE` 校验和错误。[#65320](https://github.com/StarRocks/starrocks/pull/65320)
- `__iceberg_transform_bucket` 列类型不匹配。[#70443](https://github.com/StarRocks/starrocks/pull/70443)
- Starlet 配置项设置后不生效。[#70482](https://github.com/StarRocks/starrocks/pull/70482)
- 部分更新从列模式切换为行模式时，DCG 数据读取不正确。[#61529](https://github.com/StarRocks/starrocks/pull/61529)

## 4.0.7

发布日期：2026 年 3 月 12 日

### 行为变更

- 禁止基于 Iceberg 视图创建物化视图。 [#69471](https://github.com/StarRocks/starrocks/pull/69471)
- 修复多语句 Stream Load 事务行为不一致的问题。 [#68542](https://github.com/StarRocks/starrocks/pull/68542)

### 功能优化

- 在 Publish 阶段为 `LakePersistentIndex` 添加了更细粒度的 trace 计数器。 [#69640](https://github.com/StarRocks/starrocks/pull/69640)
- 当重建行数超过阈值时，在 `LakePersistentIndex` 中提前触发 flush。 [#69698](https://github.com/StarRocks/starrocks/pull/69698)
- 在 `meta_tool` 中新增 `dump_lake_persistent_index_sst` 操作。 [#69682](https://github.com/StarRocks/starrocks/pull/69682)
- 改进 `REPAIR TABLE` 功能以及 `SHOW TABLET` 的状态展示。 [#69656](https://github.com/StarRocks/starrocks/pull/69656)
- 云原生表支持 `ADMIN SHOW TABLET STATUS`。 [#69616](https://github.com/StarRocks/starrocks/pull/69616)
- 将 `hadoop-client` 从 3.4.2 升级至 3.4.3。 [#69503](https://github.com/StarRocks/starrocks/pull/69503)
- 防止在反序列化不匹配时发生崩溃。 [#69481](https://github.com/StarRocks/starrocks/pull/69481)
- 在查询 `information_schema.loads` 时将谓词下推至 FE。 [#69472](https://github.com/StarRocks/starrocks/pull/69472)
- 优化物化视图刷新 TaskRun 显示的 SQL。 [#69437](https://github.com/StarRocks/starrocks/pull/69437)
- 在启用凭证分发（vended credentials）时，`CachingIcebergCatalog` 绕过缓存。 [#69434](https://github.com/StarRocks/starrocks/pull/69434)
- 在 `canTxnFinished` 中使用带超时的 `tryLock` 以减少锁竞争。 [#69427](https://github.com/StarRocks/starrocks/pull/69427)
- 新增全局只读变量 `@@run_mode`。 [#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 在 `DeltaLakeMetastore` 中使用 Estimator 估算缓存条目权重。 [#69244](https://github.com/StarRocks/starrocks/pull/69244)
- 为 AWS Glue `GetDatabases` API 增加 resource share 类型支持。 [#69056](https://github.com/StarRocks/starrocks/pull/69056)
- 从包含 `convert_tz` 的标量子查询中提取范围谓词。 [#69055](https://github.com/StarRocks/starrocks/pull/69055)
- 新增 ByteBuffer Estimator。 [#69042](https://github.com/StarRocks/starrocks/pull/69042)
- 在存算分离集群中为 Lake DeltaWriter 支持快速取消。 [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 支持为随机分布表添加物理分区的接口。 [#68503](https://github.com/StarRocks/starrocks/pull/68503)
- 通过会话变量 `enable_sql_transaction` 控制 SQL 事务（默认：true）。 [#63535](https://github.com/StarRocks/starrocks/pull/63535)
- 在查询外部表时新增分区扫描数量限制。 [#68480](https://github.com/StarRocks/starrocks/pull/68480)

### 问题修复

已修复以下问题：

- 在资源繁忙时，指标 `g_publish_version_failed_tasks` 的值不正确。 [#69526](https://github.com/StarRocks/starrocks/pull/69526)
- 当快照过期时，`IcebergCatalog.getPartitionLastUpdatedTime` 出现 NPE。 [#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 在 bthread 上下文中调用 `DeltaWriter::close()` 时触发 DCHECK 失败。 [#70057](https://github.com/StarRocks/starrocks/pull/70057)
- 多个 use-after-free 问题。 [#69968](https://github.com/StarRocks/starrocks/pull/69968)
- AsyncDeltaWriter 在 close/finish 生命周期中的 use-after-free 竞争问题。 [#69961](https://github.com/StarRocks/starrocks/pull/69961)
- PK SST 表损坏的缓存未被清理。 [#69693](https://github.com/StarRocks/starrocks/pull/69693)
- `AsyncFlushOutputStream` 的 use-after-free 问题。 [#69688](https://github.com/StarRocks/starrocks/pull/69688)
- `disableRecoverPartitionWithSameName` 中的保留时间时钟重置及扫描不完整问题。 [#69677](https://github.com/StarRocks/starrocks/pull/69677)
- 反序列化后 `StreamLoadMultiStmtTask.cancelAfterRestart` 出现 NPE。 [#69662](https://github.com/StarRocks/starrocks/pull/69662)
- `SchemaBeTabletsScanner` 逻辑错误导致不必要的 RPC 和元数据查询。 [#69645](https://github.com/StarRocks/starrocks/pull/69645)
- 优雅退出导致不同事务发布相同版本。 [#69639](https://github.com/StarRocks/starrocks/pull/69639)
- 当 Primary Key Index 中包含指向已被 compact 的 rowset 的过期条目时，`TabletUpdates::get_column_values` 发生 SIGSEGV 崩溃。 [#69617](https://github.com/StarRocks/starrocks/pull/69617)
- `KILL ANALYZE` 无法停止 `ANALYZE TABLE` 任务。 [#69592](https://github.com/StarRocks/starrocks/pull/69592)
- 未捕获 RowGroupWriter 的所有异常导致异常行为。 [#69568](https://github.com/StarRocks/starrocks/pull/69568)
- 修改物化视图的 warehouse 后 TaskRun 的 warehouse 显示异常。 [#69567](https://github.com/StarRocks/starrocks/pull/69567)
- 在聚合表和唯一键表执行 schema change 后，排序键未包含新增的键列。 [#69529](https://github.com/StarRocks/starrocks/pull/69529)
- `isInternalCancelError` 使用 `equals` 导致的问题。 [#69523](https://github.com/StarRocks/starrocks/pull/69523)
- `ALTER MATERIALIZED VIEW` 后 TaskManager 调度错误。 [#69504](https://github.com/StarRocks/starrocks/pull/69504)
- 未捕获 `ParquetFileWriter::close` 的所有异常，导致 Pipeline 阻塞或崩溃。 [#69492](https://github.com/StarRocks/starrocks/pull/69492)
- 分区表物化视图强制刷新问题。 [#69488](https://github.com/StarRocks/starrocks/pull/69488)
- 某些 writer flush 数据失败时返回错误状态。 [#69473](https://github.com/StarRocks/starrocks/pull/69473)
- 当 Primary Key tablet 被移动到回收站时，rowset 文件被删除。 [#69438](https://github.com/StarRocks/starrocks/pull/69438)
- 当自动分区范围被已存在的合并分区包含时，INSERT 失败。 [#69429](https://github.com/StarRocks/starrocks/pull/69429)
- 物化视图 tablet 元数据在 FE leader 与 follower 之间不一致。 [#69428](https://github.com/StarRocks/starrocks/pull/69428)
- 与函数字段相关的并发问题。 [#69315](https://github.com/StarRocks/starrocks/pull/69315)
- `computeMinActiveTxnId` 未考虑 rollup handler 的活跃事务 ID，导致数据被过早删除。 [#69285](https://github.com/StarRocks/starrocks/pull/69285)
- 并发 SWAP 后按名称查表导致 `addPartitions` 出现锁泄漏。 [#69284](https://github.com/StarRocks/starrocks/pull/69284)
- `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志。 [#69216](https://github.com/StarRocks/starrocks/pull/69216)
- `TypeParser` 中 `CAST(... AS SIGNED)` 的行为与 MySQL 兼容语法不一致。 [#69181](https://github.com/StarRocks/starrocks/pull/69181)
- 查询表复制中的分区大小写不敏感查找问题。 [#69173](https://github.com/StarRocks/starrocks/pull/69173)
- MIN/MAX 统计信息重写失败时缺失聚合函数。 [#69149](https://github.com/StarRocks/starrocks/pull/69149)
- CVE-2025-67721。 [#69138](https://github.com/StarRocks/starrocks/pull/69138)
- 同步物化视图在处理全 NULL 值时的问题。 [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 由于共享可变状态导致物化视图重写时 projection 丢失。 [#69063](https://github.com/StarRocks/starrocks/pull/69063)
- Iceberg 缓存 Weigher 估算不正确。 [#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 常量子查询场景下 `FULL OUTER JOIN USING` 的问题。 [#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 重复常量情况下 `DISTINCT ORDER BY` 的别名解析问题。 [#69014](https://github.com/StarRocks/starrocks/pull/69014)
- 物化视图 reload 时访问外部 catalog 的问题。 [#68926](https://github.com/StarRocks/starrocks/pull/68926)
- Iceberg `getPartitions` 出现 NPE。 [#68907](https://github.com/StarRocks/starrocks/pull/68907)
- Azure ABFS/WASB FileSystem 缓存 key 未正确包含 container。 [#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 在存算分离集群中修改 `CHAR` 列长度后查询结果错误。 [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- LDAP 认证中用户名大小写不敏感问题。 [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 为表添加 `storage_cooldown_ttl` 后无法创建分区。 [#60290](https://github.com/StarRocks/starrocks/pull/60290)

## 4.0.6

发布日期：2026 年 2 月 14 日

### 功能优化

- 创建 Iceberg 表时支持使用带括号的分区转换（例如，`PARTITION BY (bucket(k1, 3))`）。[#68945](https://github.com/StarRocks/starrocks/pull/68945)
- 移除了 Iceberg 表中分区列必须位于列列表末尾的限制，现在可以在任意位置定义。[#68340](https://github.com/StarRocks/starrocks/pull/68340)
- 为 Iceberg 表的 Sink 引入主机级排序功能，通过系统变量 `connector_sink_sort_scope` 控制（默认值：FILE），以优化数据布局并提升读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)
- 改进了 Iceberg 分区转换函数（例如 `bucket`、`truncate`）在参数数量错误时的错误提示信息。[#68349](https://github.com/StarRocks/starrocks/pull/68349)
- 重构了表属性处理逻辑，以增强对 Iceberg 表不同文件格式（ORC/Parquet）和压缩编码的支持。[#68588](https://github.com/StarRocks/starrocks/pull/68588)
- 新增表级查询超时配置 `table_query_timeout`，支持更细粒度的控制（优先级：Session &gt; Table &gt; Cluster）。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 支持使用 `ADMIN SHOW AUTOMATED CLUSTER SNAPSHOT` 语句查看自动快照的状态和调度信息。[#68455](https://github.com/StarRocks/starrocks/pull/68455)
- 在 `SHOW CREATE VIEW` 中支持显示包含注释的原始用户自定义 SQL。[#68040](https://github.com/StarRocks/starrocks/pull/68040)
- 在 `information_schema.loads` 中暴露启用 Merge Commit 的 Stream Load 任务，以增强可观测性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 新增 FE 内存评估工具 API `/api/memory_usage`。[#68287](https://github.com/StarRocks/starrocks/pull/68287)
- 减少了 `CatalogRecycleBin` 在分区回收过程中的不必要日志输出。[#68533](https://github.com/StarRocks/starrocks/pull/68533)
- 当基表执行 Swap/Drop/Replace Partition 操作时，触发相关异步物化视图的刷新。[#68430](https://github.com/StarRocks/starrocks/pull/68430)
- `count distinct` 类聚合函数支持 `VARBINARY` 类型。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
- 增强表达式统计信息，对语义安全表达式（例如 `cast(k as bigint) + 10`）传播直方图 MCV 信息，以提升数据倾斜检测能力。[#68292](https://github.com/StarRocks/starrocks/pull/68292)

### 问题修复

以下问题：

- Skew Join V2 Runtime Filter 可能导致的崩溃问题。[#67611](https://github.com/StarRocks/starrocks/pull/67611)
- 低基数重写导致的 Join 谓词类型不匹配问题（例如 INT = VARCHAR）。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 查询队列分配时间和等待超时逻辑相关问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- Schema Change 后 Flat JSON 扩展列的 `unique_id` 冲突问题。[#68279](https://github.com/StarRocks/starrocks/pull/68279)
- `OlapTableSink.complete()` 中的分区并发访问问题。[#68853](https://github.com/StarRocks/starrocks/pull/68853)
- 手动下载的集群快照恢复时元数据跟踪不正确的问题。[#68368](https://github.com/StarRocks/starrocks/pull/68368)
- 当仓库路径以 `/` 结尾时，备份路径中出现双斜杠的问题。[#68764](https://github.com/StarRocks/starrocks/pull/68764)
- `SHOW CREATE CATALOG` 输出中的 OBS AK/SK 凭证未进行脱敏处理的问题。[#65462](https://github.com/StarRocks/starrocks/pull/65462)

## 4.0.5

发布日期：2026 年 2 月 3 日

### 功能优化

- 将 Paimon 版本升级至 1.3.1。[#67098](https://github.com/StarRocks/starrocks/pull/67098)
- 恢复 DP 统计信息估算中缺失的优化，减少冗余计算。[#67852](https://github.com/StarRocks/starrocks/pull/67852)
- 改进 DP Join 重排序中的剪枝逻辑，更早跳过代价高昂的候选执行计划。[#67828](https://github.com/StarRocks/starrocks/pull/67828)
- 优化 JoinReorderDP 的分区枚举逻辑，减少对象分配，并新增原子数量上限（≤ 62）。[#67643](https://github.com/StarRocks/starrocks/pull/67643)
- 优化 DP Join 重排序的剪枝逻辑，并在 BitSet 中增加检查以减少流式操作的开销。[#67644](https://github.com/StarRocks/starrocks/pull/67644)
- 在 DP 统计信息估算过程中跳过谓词列的统计信息收集，以降低 CPU 开销。[#67663](https://github.com/StarRocks/starrocks/pull/67663)
- 优化相关 Join 的行数估算，避免重复构建 `Statistics` 对象。[#67773](https://github.com/StarRocks/starrocks/pull/67773)
- 减少 `Statistics.getUsedColumns` 中的内存分配。[#67786](https://github.com/StarRocks/starrocks/pull/67786)
- 当仅更新行数时，避免冗余复制 `Statistics` 映射。[#67777](https://github.com/StarRocks/starrocks/pull/67777)
- 当查询中不存在聚合时，跳过聚合下推逻辑以减少开销。[#67603](https://github.com/StarRocks/starrocks/pull/67603)
- 改进窗口函数中的 COUNT DISTINCT，新增对融合多 DISTINCT 聚合的支持，并优化 CTE 生成。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Trino 方言中支持 `map_agg` 函数。[#66673](https://github.com/StarRocks/starrocks/pull/66673)
- 在物理规划阶段支持批量获取 LakeTablet 位置信息，以减少存算分离集群中的 RPC 调用。[#67325](https://github.com/StarRocks/starrocks/pull/67325)
- 在存算一体集群中为 Publish Version 事务新增线程池，以提升并发能力。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- 优化 LocalMetastore 的锁粒度，将数据库级锁替换为表级锁。[#67658](https://github.com/StarRocks/starrocks/pull/67658)
- 重构 MergeCommitTask 的生命周期管理，并新增任务取消支持。[#67425](https://github.com/StarRocks/starrocks/pull/67425)
- 支持为自动化集群快照配置执行间隔。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- 在 MemTrackerManager 中自动清理未使用的 `mem_pool` 条目。[#67347](https://github.com/StarRocks/starrocks/pull/67347)
- 在仓库空闲检查中忽略 `information_schema` 查询。[#67958](https://github.com/StarRocks/starrocks/pull/67958)
- 支持根据数据分布动态为 Iceberg 表写入启用全局 Shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- 为 Connector Sink 模块新增 Profile 指标。[#67761](https://github.com/StarRocks/starrocks/pull/67761)
- 改进 Profile 中加载（load）溢写指标的采集和展示，区分本地 I/O 与远端 I/O。[#67527](https://github.com/StarRocks/starrocks/pull/67527)
- 将 Async-Profiler 的日志级别调整为 Error，避免重复打印警告日志。[#67297](https://github.com/StarRocks/starrocks/pull/67297)
- 在 BE 关闭时通知 Starlet 向 StarMgr 上报 SHUTDOWN 状态。[#67461](https://github.com/StarRocks/starrocks/pull/67461)

### 问题修复

已修复以下问题：

- 不支持包含连字符（`-`）的合法简单路径。[#67988](https://github.com/StarRocks/starrocks/pull/67988)
- 当聚合下推发生在包含 JSON 类型的分组键上时出现运行时错误。[#68142](https://github.com/StarRocks/starrocks/pull/68142)
- JSON 路径重写规则错误地裁剪了分区谓词中引用的分区列。[#67986](https://github.com/StarRocks/starrocks/pull/67986)
- 使用统计信息重写简单聚合时出现类型不匹配问题。[#67829](https://github.com/StarRocks/starrocks/pull/67829)
- 分区 Join 中可能存在堆缓冲区溢出风险。[#67435](https://github.com/StarRocks/starrocks/pull/67435)
- 在下推复杂表达式时引入了重复的 `slot_ids`。[#67477](https://github.com/StarRocks/starrocks/pull/67477)
- 由于缺少前置条件检查，ExecutionDAG 中的 Fragment 连接可能出现除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- 在单 BE 场景下，Fragment 并行准备可能导致潜在问题。[#67798](https://github.com/StarRocks/starrocks/pull/67798)
- RawValuesSourceOperator 缺少 `set_finished` 方法，导致算子异常终止。[#67609](https://github.com/StarRocks/starrocks/pull/67609)
- 列聚合器中不支持 DECIMAL256 类型（精度 > 38）导致 BE 崩溃。[#68134](https://github.com/StarRocks/starrocks/pull/68134)
- 存算分离集群在 DELETE 操作中未通过请求携带 `schema_key`，导致不支持 Fast Schema Evolution v2。[#67456](https://github.com/StarRocks/starrocks/pull/67456)
- 存算分离集群在同步物化视图和传统 Schema 变更中不支持 Fast Schema Evolution v2。[#67443](https://github.com/StarRocks/starrocks/pull/67443)
- 在 FE 降级且禁用文件打包时，Vacuum 可能误删文件。[#67849](https://github.com/StarRocks/starrocks/pull/67849)
- MySQLReadListener 中优雅退出处理不正确。[#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

发布日期： 2026 年 1 月 16 日

### 功能优化

- 支持算子（Operator）和驱动（Driver）的并行 Prepare，以及单节点批量 Fragment 部署，以提升查询调度性能。[#63956](https://github.com/StarRocks/starrocks/pull/63956)
- 对大分区表的 `deltaRows` 计算引入惰性求值（Lazy Evaluation）机制，提升性能。[#66381](https://github.com/StarRocks/starrocks/pull/66381)
- 优化 Flat JSON 处理逻辑，采用顺序迭代并改进路径派生方式。[#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- 支持更早释放 Spill Operator 的内存，以降低 Group Execution 场景下的内存占用。[#66669](https://github.com/StarRocks/starrocks/pull/66669)
- 优化逻辑以减少字符串比较开销。[#66570](https://github.com/StarRocks/starrocks/pull/66570)
- 增强 `GroupByCountDistinctDataSkewEliminateRule` 和 `SkewJoinOptimizeRule` 中的数据倾斜检测能力，支持基于直方图和 NULL 的策略。[#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- 在 Chunk 中通过 Move 语义增强 Column 所有权管理，减少 Copy-On-Write 的开销。[#66805](https://github.com/StarRocks/starrocks/pull/66805)
- 针对存算分离集群，新增 FE `TableSchemaService`，并更新 `MetaScanNode`，支持 Fast Schema Evolution v2 的 Schema 获取方式。[#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- 支持多 Warehouse 的 Backend 资源统计及并行度（DOP）计算，提升资源隔离能力。[#66632](https://github.com/StarRocks/starrocks/pull/66632)
- 支持通过 StarRocks 会话变量 `connector_huge_file_size` 配置 Iceberg 的 Split 大小。[#67044](https://github.com/StarRocks/starrocks/pull/67044)
- `QueryDumpDeserializer` 支持标签格式（Label-formatted）的统计信息。[#66656](https://github.com/StarRocks/starrocks/pull/66656)
- 新增 FE 配置项 `lake_enable_fullvacuum`（默认值：`false`），用于在存算分离集群中禁用 Full Vacuum。[#63859](https://github.com/StarRocks/starrocks/pull/63859)
- 将 lz4 依赖升级至 v1.10.0。[#67045](https://github.com/StarRocks/starrocks/pull/67045)
- 当行数为 0 时，为基于采样的基数估计增加回退逻辑。[#65599](https://github.com/StarRocks/starrocks/pull/65599)
- 验证 `array_sort` 中 Lambda Comparator 的 Strict Weak Ordering 属性。[#66951](https://github.com/StarRocks/starrocks/pull/66951)
- 优化外表（Delta / Hive / Hudi / Iceberg）元数据获取失败时的错误信息，展示根因。[#66916](https://github.com/StarRocks/starrocks/pull/66916)
- 支持在查询超时时 Dump Pipeline 状态，并在 FE 中以 `TIMEOUT` 状态取消查询。[#66540](https://github.com/StarRocks/starrocks/pull/66540)
- SQL 黑名单错误信息中显示命中的规则索引。[#66618](https://github.com/StarRocks/starrocks/pull/66618)
- 在 `EXPLAIN` 输出中为列统计信息添加标签。[#65899](https://github.com/StarRocks/starrocks/pull/65899)
- 过滤正常查询结束（如达到 LIMIT）时的 “cancel fragment” 日志。[#66506](https://github.com/StarRocks/starrocks/pull/66506)
- 在 Warehouse 挂起时，减少 Backend 心跳失败日志。[#66733](https://github.com/StarRocks/starrocks/pull/66733)
- `ALTER STORAGE VOLUME` 语法支持 `IF EXISTS`。[#66691](https://github.com/StarRocks/starrocks/pull/66691)

### 问题修复

以下问题：

- 在 Low Cardinality 优化下，由于缺少 `withLocalShuffle`，导致 `DISTINCT` 和 `GROUP BY` 结果错误。[#66768](https://github.com/StarRocks/starrocks/pull/66768)
- 带 Lambda 表达式的 JSON v2 函数在重写阶段报错。[#66550](https://github.com/StarRocks/starrocks/pull/66550)
- 相关子查询中 Null-aware Left Anti Join 错误地应用了 Partition Join。[#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Meta Scan 重写规则中的行数计算错误。[#66852](https://github.com/StarRocks/starrocks/pull/66852)
- 基于统计信息重写 Meta Scan 时，Union Node 的 Nullable 属性不一致。[#67051](https://github.com/StarRocks/starrocks/pull/67051)
- 当 Ranking 窗口函数缺少 `PARTITION BY` 和 `ORDER BY` 时，优化逻辑导致 BE 崩溃。[#67094](https://github.com/StarRocks/starrocks/pull/67094)
- Group Execution Join 与窗口函数组合使用时可能产生错误结果。[#66441](https://github.com/StarRocks/starrocks/pull/66441)
- 特定过滤条件下，`PartitionColumnMinMaxRewriteRule` 产生错误结果。[#66356](https://github.com/StarRocks/starrocks/pull/66356)
- 聚合后 Union 操作中 Nullable 属性推导错误。[#65429](https://github.com/StarRocks/starrocks/pull/65429)
- `percentile_approx_weighted` 在处理压缩参数时发生崩溃。[#64838](https://github.com/StarRocks/starrocks/pull/64838)
- 大字符串编码场景下 Spill 导致崩溃。[#61495](https://github.com/StarRocks/starrocks/pull/61495)
- 下推本地 TopN 时，多次调用 `set_collector` 触发崩溃。[#66199](https://github.com/StarRocks/starrocks/pull/66199)
- LowCardinality 重写逻辑中的依赖推导错误。[#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset 提交失败时发生 Rowset ID 泄漏。[#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache 锁竞争问题。[#66637](https://github.com/StarRocks/starrocks/pull/66637)
- 在使用列模式部分更新并带条件更新时，导入失败。[#66139](https://github.com/StarRocks/starrocks/pull/66139)
- ALTER 操作期间 Tablet 被删除导致并发导入失败。[#65396](https://github.com/StarRocks/starrocks/pull/65396)
- RocksDB 迭代超时导致 Tablet 元数据加载失败。[#65146](https://github.com/StarRocks/starrocks/pull/65146)
- 存算分离集群中，建表和 Schema Change 时压缩配置未生效。[#65673](https://github.com/StarRocks/starrocks/pull/65673)
- 升级过程中 Delete Vector 的 CRC32 兼容性问题。[#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Clone 任务失败后文件清理阶段的状态检查逻辑错误。[#65709](https://github.com/StarRocks/starrocks/pull/65709)
- `INSERT OVERWRITE` 后统计信息收集逻辑异常。[#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- FE 重启后外键约束丢失。[#66474](https://github.com/StarRocks/starrocks/pull/66474)
- 删除 Warehouse 后元数据获取失败。[#66436](https://github.com/StarRocks/starrocks/pull/66436)
- 高选择性过滤条件下，审计日志扫描统计信息不准确。[#66280](https://github.com/StarRocks/starrocks/pull/66280)
- 查询错误率指标的计算逻辑不正确。[#65891](https://github.com/StarRocks/starrocks/pull/65891)
- 任务退出时可能发生 MySQL 连接泄漏。[#66829](https://github.com/StarRocks/starrocks/pull/66829)
- SIGSEGV 崩溃后 BE 状态未能及时更新。[#66212](https://github.com/StarRocks/starrocks/pull/66212)
- LDAP 用户登录过程中出现 NPE。[#65843](https://github.com/StarRocks/starrocks/pull/65843)
- HTTP SQL 请求切换用户时错误日志不准确。[#65371](https://github.com/StarRocks/starrocks/pull/65371)
- TCP 连接复用过程中发生 HTTP 上下文泄漏。[#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Follower 转发的查询在 Profile 日志中缺少 QueryDetail。[#64395](https://github.com/StarRocks/starrocks/pull/64395)
- 审计日志中缺少 Prepare / Execute 的详细信息。[#65448](https://github.com/StarRocks/starrocks/pull/65448)
- HyperLogLog 内存分配失败导致崩溃。[#66747](https://github.com/StarRocks/starrocks/pull/66747)
- `trim` 函数的内存预留逻辑存在问题。[#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- 修复 CVE-2025-66566 和 CVE-2025-12183。[#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053)
- Exec Group Driver 提交过程中的竞态条件问题。[#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Pipeline 倒计时中的 use-after-free 风险。[#65940](https://github.com/StarRocks/starrocks/pull/65940)
- `MemoryScratchSinkOperator` 在队列关闭时发生阻塞。[#66041](https://github.com/StarRocks/starrocks/pull/66041)
- 文件系统缓存 Key 冲突问题。[#65823](https://github.com/StarRocks/starrocks/pull/65823)
- `SHOW PROC '/compactions'` 中子任务数量显示错误。[#67209](https://github.com/StarRocks/starrocks/pull/67209)
- Query Profile API 未返回统一的 JSON 格式。[#67077](https://github.com/StarRocks/starrocks/pull/67077)
- `getTable` 异常处理不当，影响物化视图检查。[#67224](https://github.com/StarRocks/starrocks/pull/67224)
- 原生表与云原生表的 `DESC` 语句中 `Extra` 列输出不一致。[#67238](https://github.com/StarRocks/starrocks/pull/67238)
- 单节点部署场景下的竞态条件问题。[#67215](https://github.com/StarRocks/starrocks/pull/67215)
- 第三方库日志泄漏问题。[#67129](https://github.com/StarRocks/starrocks/pull/67129)
- REST Catalog 认证逻辑错误导致认证失败。[#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

发布日期：2025 年 12 月 25 日

### 功能优化

- 支持对 STRUCT 数据类型使用 `ORDER BY` 子句。[#66035](https://github.com/StarRocks/starrocks/pull/66035)
- 支持创建带属性的 Iceberg 视图，并在 `SHOW CREATE VIEW` 的输出中显示这些属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 支持通过 `ALTER TABLE ADD/DROP PARTITION COLUMN` 修改 Iceberg 表的分区 Spec。[#65922](https://github.com/StarRocks/starrocks/pull/65922)
- 支持在带窗口框架（例如 `ORDER BY` / `PARTITION BY`）的窗口函数中使用 `COUNT/SUM/AVG(DISTINCT)` 聚合，并提供优化选项。[#65815](https://github.com/StarRocks/starrocks/pull/65815)
- 对 CSV 解析进行性能优化，对单字符分隔符使用 `memchr`。[#63715](https://github.com/StarRocks/starrocks/pull/63715)
- 新增优化器规则，将 Partial TopN 下推到预聚合（Pre-Aggregation）阶段，以减少网络开销。[#61497](https://github.com/StarRocks/starrocks/pull/61497)
- 增强 Data Cache 监控能力：
  - 新增内存/磁盘配额及使用量相关指标。[#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - 在 `api/datacache/stat` HTTP 接口中新增 Page Cache 统计信息。[#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - 新增内表的命中率统计。[#66198](https://github.com/StarRocks/starrocks/pull/66198)
- 优化 Sort 和 Aggregation 算子，在 OOM 场景下支持更快速地释放内存。[#66157](https://github.com/StarRocks/starrocks/pull/66157)
- 在 FE 中新增 `TableSchemaService`，用于存算分离集群中 CN 按需获取指定表结构。[#66142](https://github.com/StarRocks/starrocks/pull/66142)
- 优化快速 Schema Evolution，在所有依赖的导入作业完成前保留历史 Schema。[#65799](https://github.com/StarRocks/starrocks/pull/65799)
- 增强 `filterPartitionsByTTL`，正确处理 NULL 分区值，避免误过滤所有分区。[#65923](https://github.com/StarRocks/starrocks/pull/65923)
- 优化 `FusedMultiDistinctState`，在重置时清理关联的 MemPool。[#66073](https://github.com/StarRocks/starrocks/pull/66073)
- 在 Iceberg REST Catalog 中，将 `ICEBERG_CATALOG_SECURITY` 属性检查改为大小写不敏感。[#66028](https://github.com/StarRocks/starrocks/pull/66028)
- 在存算分离集群中新增 HTTP 接口 `GET /service_id`，用于获取 StarOS Service ID。[#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Kafka 消费者配置中使用 `bootstrap.servers` 替代已废弃的 `metadata.broker.list`。[#65437](https://github.com/StarRocks/starrocks/pull/65437)
- 新增 FE 配置项 `lake_enable_fullvacuum`（默认值：false），用于禁用 Full Vacuum Daemon。[#66685](https://github.com/StarRocks/starrocks/pull/66685)
- 将 lz4 库升级至 v1.10.0。[#67080](https://github.com/StarRocks/starrocks/pull/67080)

### 问题修复

以下问题：

- `latest_cached_tablet_metadata` 在批量 Publish 过程中可能导致版本被错误跳过。[#66558](https://github.com/StarRocks/starrocks/pull/66558)
- 在存算一体集群中，`CatalogRecycleBin` 中的 `ClusterSnapshot` 相对检查可能引发的问题。[#66501](https://github.com/StarRocks/starrocks/pull/66501)
- 在 Spill 过程中向 Iceberg 表写入复杂数据类型（ARRAY / MAP / STRUCT）时导致 BE 崩溃。[#66209](https://github.com/StarRocks/starrocks/pull/66209)
- 当 writer 初始化或首次写入失败时，Connector Chunk Sink 可能发生卡死。[#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink 中，`PartitionChunkWriter` 初始化失败后，在关闭阶段触发空指针异常的问题。[#66097](https://github.com/StarRocks/starrocks/pull/66097)
- 设置不存在的系统变量时未报错而是静默成功的问题。[#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Data Cache 损坏时，Bundle 元数据解析失败的问题。[#66021](https://github.com/StarRocks/starrocks/pull/66021)
- 当结果为空时，MetaScan 在 count 列上返回 NULL 而非 0。[#66010](https://github.com/StarRocks/starrocks/pull/66010)
- 对于早期版本创建的资源组，`SHOW VERBOSE RESOURCE GROUP ALL` 显示 NULL 而非 `default_mem_pool`。[#65982](https://github.com/StarRocks/starrocks/pull/65982)
- 禁用 `flat_json` 表配置后，查询执行过程中抛出 `RuntimeException`。[#65921](https://github.com/StarRocks/starrocks/pull/65921)
- 在存算分离集群中，Schema Change 后将 `min` / `max` 统计信息重写到 MetaScan 时导致的类型不匹配问题。[#65911](https://github.com/StarRocks/starrocks/pull/65911)
- 在缺少 `PARTITION BY` 和 `ORDER BY` 时，排名窗口优化导致 BE 崩溃的问题。[#67093](https://github.com/StarRocks/starrocks/pull/67093)
- 合并运行时过滤器时，`can_use_bf` 判断不正确，可能导致错误结果或崩溃。[#67062](https://github.com/StarRocks/starrocks/pull/67062)
- 将运行时 bitset 过滤器下推到嵌套 OR 谓词中导致结果不正确的问题。[#67061](https://github.com/StarRocks/starrocks/pull/67061)
- DeltaWriter 完成后仍执行写入或 flush 操作，可能导致数据竞争和数据丢失的问题。[#66966](https://github.com/StarRocks/starrocks/pull/66966)
- 将简单聚合重写为 MetaScan 时，由于 nullable 属性不匹配导致的执行错误。[#67068](https://github.com/StarRocks/starrocks/pull/67068)
- MetaScan 重写规则中行数计算不正确的问题。[#66967](https://github.com/StarRocks/starrocks/pull/66967)
- 由于 Tablet 元数据缓存不一致，批量 Publish 过程中版本可能被错误跳过。[#66575](https://github.com/StarRocks/starrocks/pull/66575)
- HyperLogLog 操作中，内存分配失败时错误处理不当的问题。[#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

发布日期：2025 年 12 月 4 日

### 新增功能

- 新增资源组属性 `mem_pool`，允许多个资源组共享同一个内存池，并为该内存池设置联合内存限制。该功能向后兼容。如果未指定 `mem_pool`，将使用 `default_mem_pool`。[#64112](https://github.com/StarRocks/starrocks/pull/64112)

### 功能优化

- 在启用 File Bundling 后减少 Vacuum 过程中的远程存储访问量。[#65793](https://github.com/StarRocks/starrocks/pull/65793)
- File Bundling 功能会缓存最新的 Tablet 元数据。[#65640](https://github.com/StarRocks/starrocks/pull/65640)
- 提升长字符串场景下的安全性与稳定性。[#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)
- 优化 `SplitTopNAggregateRule` 的逻辑，避免性能回退。[#65478](https://github.com/StarRocks/starrocks/pull/65478)
- 将 Iceberg/DeltaLake 的表统计信息收集策略应用到其他外部数据源，以避免在单表场景下收集不必要的统计信息。[#65430](https://github.com/StarRocks/starrocks/pull/65430)
- 在 Data Cache HTTP API `api/datacache/app_stat` 中新增 Page Cache 相关指标。[#65341](https://github.com/StarRocks/starrocks/pull/65341)
- 支持 ORC 文件切分，从而并行扫描单个大型 ORC 文件。[#65188](https://github.com/StarRocks/starrocks/pull/65188)
- 在优化器中增加 IF 谓词的选择率估算。[#64962](https://github.com/StarRocks/starrocks/pull/64962)
- FE 支持对 `DATE` 和 `DATETIME` 类型的 `hour`、`minute`、`second` 进行常量折叠。[#64953](https://github.com/StarRocks/starrocks/pull/64953)
- 默认启用将简单聚合重写为 MetaScan。[#64698](https://github.com/StarRocks/starrocks/pull/64698)
- 提升存算分离集群中多副本分配处理的可靠性。[#64245](https://github.com/StarRocks/starrocks/pull/64245)
- 在审计日志和指标中暴露缓存命中率。[#63964](https://github.com/StarRocks/starrocks/pull/63964)
- 使用 HyperLogLog 或采样方法估算直方图的分桶级别去重计数（NDV），从而为谓词和 JOIN 提供更准确的 NDV。[#58516](https://github.com/StarRocks/starrocks/pull/58516)
- 支持 SQL 标准语义的 FULL OUTER JOIN USING。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- 当优化器超时时输出内存信息以辅助诊断。[#65206](https://github.com/StarRocks/starrocks/pull/65206)

### 问题修复

以下问题已修复：

- DECIMAL56 的 `mod` 运算相关问题。[#65795](https://github.com/StarRocks/starrocks/pull/65795)
- Iceberg 扫描范围处理相关问题。[#65658](https://github.com/StarRocks/starrocks/pull/65658)
- 临时分区与随机分桶下的 MetaScan 重写问题。[#65617](https://github.com/StarRocks/starrocks/pull/65617)
- `JsonPathRewriteRule` 在透明物化视图改写后使用了错误的表。[#65597](https://github.com/StarRocks/starrocks/pull/65597)
- 当 `partition_retention_condition` 引用了生成列时，物化视图刷新失败。[#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg min/max 值类型问题。[#65551](https://github.com/StarRocks/starrocks/pull/65551)
- 当 `enable_evaluate_schema_scan_rule` 设置为 `true` 时跨库查询 `information_schema.tables` 与 `views` 的问题。[#65533](https://github.com/StarRocks/starrocks/pull/65533)
- JSON 数组比较时的整数溢出问题。[#64981](https://github.com/StarRocks/starrocks/pull/64981)
- MySQL Reader 不支持 SSL。[#65291](https://github.com/StarRocks/starrocks/pull/65291)
- 由于 SVE 构建不兼容导致的 ARM 构建问题。[#65268](https://github.com/StarRocks/starrocks/pull/65268)
- 基于 Bucket-aware 执行的查询在分桶 Iceberg 表上可能卡住。[#65261](https://github.com/StarRocks/starrocks/pull/65261)
- OLAP 表扫描缺少内存限制检查导致的错误传播与内存安全问题。[#65131](https://github.com/StarRocks/starrocks/pull/65131)

### 行为变更

- 当物化视图被置为 Inactive（失效）时，系统会递归失效依赖其的其他物化视图。[#65317](https://github.com/StarRocks/starrocks/pull/65317)
- 在生成 SHOW CREATE 输出时使用原始的物化视图查询 SQL（包含注释和格式）。[#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

发布日期：2025 年 11 月 17 日

### 功能优化

- 优化了 TaskRun 会话变量的处理逻辑，仅处理已知变量。 [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- 默认支持基于元数据收集 Iceberg 和 Delta Lake 表的统计信息。 [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- 支持收集使用 Bucket 和 Truncate 分区转换的 Iceberg 表的统计信息。 [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- 支持 FE `/proc` Profile 用于调试。 [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- 增强了 Iceberg REST Catalog 的 OAuth2 和 JWT 认证支持。 [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- 改进了 Bundle Tablet 元数据的校验与恢复机制。 [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- 优化了扫描范围的内存估算逻辑。 [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### 问题修复

以下问题：

- 在发布 Bundle Tablet 时事务日志被删除。 [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- Join 算法无法保证排序属性，因为在 Join 操作后未重置排序属性。 [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- 透明物化视图改写相关问题。 [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### 行为变更

- 为 Iceberg Catalog 新增属性 `enable_iceberg_table_cache`，可选择禁用 Iceberg 表缓存，从而始终读取最新数据。 [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- 在执行 `INSERT ... SELECT` 前刷新外部表，确保读取最新元数据。 [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- 将锁表槽位数量增加至 256，并在 Slow Lock 日志中新增 `rid` 字段。 [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- 由于与 Event-based Scheduling 不兼容，暂时禁用了 `shared_scan`。 [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- 将 Hive Catalog 的缓存 TTL 默认值修改为 24 小时，并移除了未使用的参数。 [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- 根据会话变量和插入列数量自动确定 Partial Update 模式。 [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

发布日期：2025 年 10 月 17 日

### 数据湖分析

- 统一了 BE 元数据的 Page Cache 和 Data Cache，并采用自适应策略进行扩展。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- 优化了 Iceberg 统计信息的元数据文件解析，避免重复解析。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- 优化了针对 Iceberg 元数据的 COUNT/MIN/MAX 查询，通过有效跳过数据文件扫描，显著提升了大分区表的聚合查询性能并减少资源消耗。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- 支持通过 Procedure `rewrite_data_files` 对 Iceberg 表进行 Compaction。
- 支持带有隐藏分区（Hidden Partition）的 Iceberg 表，包括建表、写入和读取。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- 支持 Paimon Catalog 中的 TIME 数据类型。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- 支持在创建 Iceberg 表时设定排序键。
- 优化 Iceberg 表的写入性能。
  - Iceberg Sink 支持大算子落盘（Spill）、全局 Shuffle 以及本地排序，优化内存与小文件问题。 [#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg Sink 优化基于 Spill Partition Writer 的本地排序，提升写入效率。[#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg Sink 支持分区全局 Shuffle，进一步减少小文件。[#62123](https://github.com/StarRocks/starrocks/pull/62123)
- 增强 Iceberg Bucket-aware 执行，提升分桶表并发与分布能力。[#61756](https://github.com/StarRocks/starrocks/pull/61756)
- 支持 Paimon Catalog 中的 TIME 数据类型。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Iceberg 版本升级至 1.10.0。[#63667](https://github.com/StarRocks/starrocks/pull/63667)

### 安全与认证

- 在使用 JWT 认证和 Iceberg REST Catalog 的场景下，StarRocks 支持通过 REST Session Catalog 将用户登录信息透传到 Iceberg，用于后续数据访问认证。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg Catalog 支持  Vended Credential。
- 支持对 Group Provider 获取的外部组授权 StarRocks 内部角色。[#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- 外表新增 REFRESH 权限，用于控制外表刷新权限。[#62636](https://github.com/StarRocks/starrocks/pull/62636)

### 存储优化与集群管理

- 为存算分离集群中的云原生表引入了文件捆绑（File Bundling）优化，自动捆绑由导入、Compaction 或 Publish 操作生成的数据文件，从而减少因高频访问外部存储系统带来的 API 成本。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- 支持多表写写事务（Multi-Table Write-Write Transaction），允许用户控制 INSERT、UPDATE 和 DELETE 操作的原子提交。该事务支持 Stream Load 和 INSERT INTO 接口，有效保证 ETL 与实时写入场景下的跨表一致性。[#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Routine Load 支持 Kafka 4.0。
- 支持在存算一体集群的主键表上使用全文倒排索引。
- 支持修改聚合表的聚合键。[#62253](https://github.com/StarRocks/starrocks/issues/62253)
- 支持对 Catalog、Database、Table、View 和物化视图的名称启用大小写不敏感处理。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 支持在存算分离集群中对 Compute Node 进行黑名单管理。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- 支持全局连接 ID。[#57256](https://github.com/StarRocks/starrocks/pull/57276)
- Information Schema 新增元数据视图 `recyclebin_catalogs`，展示可恢复的已删除元信息。[#51007](https://github.com/StarRocks/starrocks/pull/51007)

### 查询与性能优化

- 支持 DECIMAL256 数据类型，将精度上限从 38 扩展到 76 位。其 256 位存储可以更好地适应高精度金融与科学计算场景，有效缓解 DECIMAL128 在超大规模聚合与高阶运算中的精度溢出问题。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- 基础算子性能提升。[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- 优化了 JOIN 与 AGG 算子性能。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] 引入 SQL Plan Manager，允许用户将查询与查询计划绑定，避免因系统状态变化（如数据更新、统计信息更新）导致查询计划变更，从而稳定查询性能。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- 引入 Partition-wise Spillable Aggregate/Distinct 算子，替代原有基于排序型聚合的 Spill 实现，大幅提升复杂和高基数 GROUP BY 场景下的聚合性能，并降低读写开销。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - 支持在表级别配置 Flat JSON。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - 通过保留 V1 机制并新增 Page 级和 Segment 级索引（ZoneMap、Bloom Filter）、带有延迟物化的谓词下推、字典编码以及集成低基数全局字典，显著提升 JSON 列式存储的执行效率。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- 为 STRING 数据类型支持自适应 ZoneMap 索引创建策略。[#61960](https://github.com/StarRocks/starrocks/issues/61960)
- 增强查询可观测性：
  - 优化 EXPLAIN ANALYZE 返回信息，分算子分组展示执行指标，提升可读性。[#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` 和 `QueryProfileActionV2` 支持 JSON 格式，提升跨 FE 查询能力。[#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - 支持基于所有 FE 获取 QUery Profile 信息。[#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST 命令展示 Catalog、Query ID 等信息。[#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - 增强查询队列以及进程监控，支持显示 Running/Pending 状态。[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- 物化视图改写考虑原表的分布和排序键，提升最佳物化视图选择能力。 [#62830](https://github.com/StarRocks/starrocks/pull/62830)

### 函数与 SQL 语法

- 新增以下函数：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` 和 `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` 和 `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- 提供以下语法扩展：
  - CREATE ANALYZE FULL TABLE 支持 IF NOT EXISTS 关键字。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT 支持 EXCLUDE 子句。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 聚合函数支持 FILTER 子句，提升条件聚合的可读性和执行效率。[#58937](https://github.com/StarRocks/starrocks/pull/58937)

### 行为变更

- 调整物化视图参数 `auto_partition_refresh_number` 的逻辑，无论自动刷新还是手动刷新，均限制需刷新分区的数量。[#62301](https://github.com/StarRocks/starrocks/pull/62301)
- 默认启用 Flat JSON。[#62097](https://github.com/StarRocks/starrocks/pull/62097)
- 系统变量 `enable_materialized_view_agg_pushdown_rewrite` 的默认值设为 `true`，表示默认启用物化视图查询改写中的聚合下推功能。[#60976](https://github.com/StarRocks/starrocks/pull/60976)
- 将 `information_schema.materialized_views` 中部分列的类型调整为更符合对应数据的类型。[#60054](https://github.com/StarRocks/starrocks/pull/60054)
- 当分隔符不匹配时，`split_part` 函数返回 NULL。[#56967](https://github.com/StarRocks/starrocks/pull/56967)
- 在 CTAS/CREATE MATERIALIZED VIEW 中使用 STRING 替代固定长度 CHAR，避免推断错误的列长度导致物化视图刷新失败。[#63114](https://github.com/StarRocks/starrocks/pull/63114) [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- 简化了 Data Cache 相关配置。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` 和 `datacache_disk_size` 现已生效。
  - `storage_page_cache_limit`、`block_cache_mem_size`、`block_cache_disk_size` 已弃用。
- 新增 Catalog 属性（Hive 的 `remote_file_cache_memory_ratio`，Iceberg 的 `iceberg_data_file_cache_memory_usage_ratio` 和 `iceberg_delete_file_cache_memory_usage_ratio`），用于限制 Hive 和 Iceberg 元数据缓存的内存资源，默认值设为 `0.1`（10%），并将元数据缓存 TTL 调整为24小时。[#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION 现不再合并具有相同桶序列号的所有物化索引的统计信息，仅在物化索引级别显示数据分布。[#59656](https://github.com/StarRocks/starrocks/pull/59656)
- 自动分桶表的默认桶大小从 4GB 调整为 1GB，以提升性能和资源利用率。[#63168](https://github.com/StarRocks/starrocks/pull/63168)
- 系统根据对应会话变量和 INSERT 语句的列数确定部分更新模式。[#62091](https://github.com/StarRocks/starrocks/pull/62091)
- 优化 Information Schema 中的 `fe_tablet_schedules` 视图。[#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - `TABLET_STATUS` 列更名为 `SCHEDULE_REASON`，`CLONE_SRC` 列更名为 `SRC_BE_ID`，`CLONE_DEST` 列更名为 `DEST_BE_ID`。
  - `CREATE_TIME`、`SCHEDULE_TIME` 和 `FINISH_TIME` 列类型从 `DOUBLE` 改为 `DATETIME`。
- 部分 FE 指标增加了 `is_leader` 标签。[#63004](https://github.com/StarRocks/starrocks/pull/63004)
- 使用 Microsoft Azure Blob Storage 以及 Data Lake Storage Gen 2 作为对象存储的存算分离集群，升级到 v4.0 后 Data Cache 失效，系统将自动重新加载。

### 降级说明

启用 File Bundling 功能后，您只能将集群降级到 v3.5.2 或更高版本。如果降级到 v3.5.2 之前的版本，将无法读写已启用 File Bundling 功能的表。File Bundling 功能在 v4.0 或更高版本中创建的表格中默认启用。
