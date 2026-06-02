---
displayed_sidebar: docs
---

# StarRocks version 4.1

:::danger

**容器镜像问题（v4.1.0）**

由于 v4.1.0 容器镜像存在加载顺序不稳定的问题，BE 进程在容器环境中可能无法可靠启动。**容器环境用户请勿升级至 v4.1.0。** 请等待包含修复的 v4.1.1（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**降级注意事项**

- 将 StarRocks 升级到 v4.1 后，请勿降级到 v4.0.6 以下的任何 v4.0 版本。

  由于 v4.1 引入了数据布局的内部变更（与 Tablet 分割和数据分布机制相关），升级至 v4.1 的集群生成的元数据和存储结构可能与早期版本不完全兼容。因此，从 v4.1 降级仅支持降至 v4.0.6 或更高版本。不支持降级至 v4.0.6 之前的版本。此限制源于早期版本在解析 Tablet 布局和分布元数据时的向后兼容性约束。

:::

## 4.1.1

发布日期：2026 年 5 月 29 日

本版本为 v4.1 的维护版本，主要聚焦于稳定性、性能和安全性，不包含重大新功能。

### 行为变更

- Hive Connector 现在默认使用原生 C++ Avro Scanner，替代原有的 JNI Avro Scanner。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 现在禁止对 INCREMENTAL/AUTO 物化视图进行查询改写，并拒绝对 INCREMENTAL/AUTO 物化视图执行 FORCE 刷新和分区刷新。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 功能优化

**UDF**

- Java UDF/UDAF/UDTF 支持更多类型：UDAF/UDTF 支持 STRUCT 类型的参数和返回值，支持嵌套 ARRAY/MAP 类型、DATE/DATETIME、DECIMAL 以及可变参数（varargs）。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- 标量 UDF 支持 STRUCT 类型参数。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF 支持嵌套 ARRAY/MAP 类型。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF 现在仅加载和初始化一次并在多个查询间复用，降低了每次查询的开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)

**数据湖分析**

- 为 Hive Connector 使用原生 C++ Scanner 替代 JNI Avro Scanner，支持直接二进制解码，并支持 `avro.schema.literal` 和 `avro.schema.url`。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 支持 CTAS 语句中的 Trino `WITH` 子句。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- 完善了 Sink 路径上对 Iceberg `timestamptz` 分区转换的支持。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- 为 Iceberg 表聚合启用 TopN Runtime Filter 下推。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- 支持 Iceberg datetime min/max 优化。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 允许在 Catalog 和 BE 中透传 HDFS HA 配置，以支持访问多个 HDFS 集群。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 为外部表查询新增分区扫描数量限制。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- 对不支持的 Iceberg V3 特性进行快速失败处理。[#70242](https://github.com/StarRocks/starrocks/pull/70242)

**导入与导出**

- 通过 INSERT INTO FILES 导出 CSV 时支持 `csv.enclose` 和 `csv.escape`。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 新增 INSERT 属性 `enable_push_down_schema`，支持将完整 Schema 下推到 `files()`。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load 作业在遇到不可重试错误（例如主键大小超限）时会暂停。[#71161](https://github.com/StarRocks/starrocks/pull/71161)

**查询引擎与优化器**

- 支持对来自两个子节点的复杂表达式进行 Join Reorder。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- 改进 CBO 统计估算，包括对 `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION 及常量的 MCV/NULL 比例传播。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- 改进数据倾斜 Join 检测：仅当所有 Join Key 都倾斜时才判定为倾斜，并新增 `force_group_by_skew_eliminate_when_skewed` 开关以强制应用倾斜规则。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- 在 FE 端支持 `regexp_replace` 的常量折叠。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 优化了带常量分区值的日期分区列上的 MIN/MAX。[#69880](https://github.com/StarRocks/starrocks/pull/69880)

**物化视图**

- 引入 `SCHEDULE` 关键字作为物化视图刷新中 `ASYNC` 的同义词。[#72329](https://github.com/StarRocks/starrocks/pull/72329)

**存算分离架构**

- 支持存算分离模式下 Lake 表的 Tablet 创建重试。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- 支持 Lake 列模式部分更新的条件更新。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 并行化部分更新 Publish、持久化索引初始化以及 SSTable 打开，提升导入吞吐。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 支持存算一体到存算分离复制过程中的 DCG 文件同步。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- 支持对 Key 列和非 Key 列加宽 VARCHAR 长度的 Schema Evolution。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- 新增 `snapshot_meta.json` 标记，用于集群快照完整性检查。[#71209](https://github.com/StarRocks/starrocks/pull/71209)

**安全与认证**

- 支持通过 DN 模式进行 LDAP 直接绑定（Direct Bind）认证。[#71559](https://github.com/StarRocks/starrocks/pull/71559)

**管理与可观测性**

- 新增 `get_query_dump_from_query_id` 元函数，便于查询问题排查。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 支持在审计日志中审计查询所涉及的关系（relation）。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- 新增用于 MySQL 二进制结果编码的会话变量。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 新增多个指标以增强可观测性，包括存算分离集群的 `tablet_num`、`MemtableIOSpeed`、`staros_shard_count` 以及 Iceberg 元数据表查询指标。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- 新增 FE 配置项 `deploy_serialization_min_thread_pool_size`。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- 新增配置项 `tablet_reshard_enable_tablet_merge`，用于禁用 MergeTabletJob 的创建。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- 通过 `SO_REUSEPORT` 消除 HTTP Server accept 的惊群效应。[#72956](https://github.com/StarRocks/starrocks/pull/72956)

### 安全

- [CVE] 将 Netty 升级至 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] 将 pgjdbc 升级至 42.7.11（修复因 SCRAM PBKDF2 迭代次数无上限导致的客户端 DoS），将 BouncyCastle 升级至 1.84（修复 FrodoKEM 私钥泄露）。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] 使用 go1.25.9 构建 pprof 以消除 Golang CVE。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- 将 jetty-http 升级至 9.4.58.v20250814。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 清理 Broker 依赖中的 CVE 并移除 `wildfly-openssl`。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- 在 INSERT INTO FILES 的错误信息中对凭证进行脱敏。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### 错误修复

修复了如下问题：

**崩溃与稳定性**

- 修复了由 `hash_util` 静态初始化顺序导致的 CN 启动 Segfault。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 修复了启用物理分裂时扫描空 Tablet 导致的 CN 崩溃。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 修复了查询 `information_schema.warehouse_queries` 时的 BE 崩溃。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- 修复了 Lake Compaction 中 rowset `num_rows` 为零时的 SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- 修复了 ExecutionDAG Fragment 连接中的除零问题。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- 修复了 SinkBuffer 中的优雅退出崩溃。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- 修复了可溢出 Hash Join Probe 崩溃。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 修复了向临时 `std::string` 格式化时的栈缓冲区溢出。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- 修复了 `reverse(DecimalV3)` 崩溃。[#71834](https://github.com/StarRocks/starrocks/pull/71834)

**Use-after-free、内存泄漏与竞态**

- 修复了 `LoadChannel::get_load_replica_status` 中因临时 `shared_ptr` 析构导致的 Use-after-free。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- 修复了线程创建失败时 `ThreadPool::do_submit` 中的 Use-after-free。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- 修复了 Fragment 销毁过程中 Hive 分区描述符的 Use-after-free。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- 修复了 Information Schema Sink 的 Use-after-free。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- 通过复用 HttpClient 实例修复了 FE 文件描述符泄漏。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- 修复了 `JDBCScanner::_init_jdbc_scanner` 中的 JNI 本地引用泄漏。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- 修复了缓存 MV 计划上下文时的内存泄漏。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- 修复了 Local Exchange 中的异常内存占用。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- 修复了 Lake `publish_version` 中 `response->tablet_metas` 上的竞态。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- 修复了 `DeltaWriter::commit()` 中并发 `SegmentFlushTask` 的竞态。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- 修复了序列化过程中 `RuntimeProfile` min/max 的竞态。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- 修复了查询上下文销毁期间 `PipelineTimerTask` 的竞态。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- 修复了 `_all_global_rf_ready_or_timeout` 中的竞态。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- 修复了 `map_apply` 和 `array_length` 中共享 `NullColumn` 的问题。[#71258](https://github.com/StarRocks/starrocks/pull/71258)

**锁与死锁**

- 修复了由分区版本间隙导致的批量 Publish 死锁。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 修复了存算一体模式下预热 Rowset 元数据 LRU 缓存时的死锁。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- 使 `Locker` 回滚具备异常安全性并修正了解锁顺序。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 放松了多个只读和元数据路径上的 DB 锁，以降低与 DDL、StarOS RPC 之间的锁争用。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)

**正确性**

- 修复了因缺少 Project 节点导致的错误 Shuffle 分布。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- 修复了 AGG TopN Runtime Filter `exprOrder` 不匹配导致的崩溃和错误结果。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- 修复了 dict-merge GROUP BY 的错误结果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- 修复了 Query Cache 与 Local Shuffle 聚合的冲突。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- 修复了 Flat JSON 中全局字典生成不一致的问题。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- 修复了 Flat JSON Merge 空值不一致的问题。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 修复了显式声明 Key/Value 类型时 Map 字面量的类型不匹配。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- 在 JOIN USING 转换器中将 COALESCE 子节点转换为公共类型。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- 修复了带全局变量的 reduce-cast 后 VARCHAR 长度未保留的问题。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- 修复了 MySQL 结果集中嵌套类型内 VARBINARY 编码错误的问题。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 修复了小 LIMIT 下禁用聚合溢出时的 HAVING 子句检查问题。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 在日期解析前去除引号，并修复了一个 PostgreSQL 日期/时间问题。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)

**存算分离与存储**

- 通过跳过 Range 分布 Tablet 的数据文件删除，修复了 Tablet 分裂后的数据丢失。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了数据文件共享标记丢失，导致 Vacuum 删除仍被兄弟分裂 Tablet 引用的文件的问题。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- 修复了 split→compaction→merge 序列下的 Tablet Merge 正确性问题。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- 修复了 Tablet 分裂期间 cross-published txn log 的 num_rows/data_size 膨胀问题。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 修复了同一 Publish 批次中 write-before-compaction 导致的 delvec 孤儿条目。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- 通过同步 StarMgr Journal 回放修复了 Follower FE 上 "no queryable replica" 的问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 在应用普通 rowset commit 时保留 `merge_condition`。[#72542](https://github.com/StarRocks/starrocks/pull/72542)

**数据湖 Connector**

- 修复了 Iceberg DELETE 冲突检测使用错误 Snapshot ID 和 Filter 的问题。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 修复了无效 Iceberg Transform 参数导致的 NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 修复了因 Planner 注入额外列导致 Iceberg min/max 优化被跳过的问题。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- 修复了基于 Iceberg 基表的 aggregate-join-pushdown MV 改写问题。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- 在 INSERT OVERWRITE commit 前创建缺失的 Hive 分区目录。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- 修复了 JNI Scanner 未应用 AWS assume-role 的问题。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- 修复了针对剪枝子节点和嵌套可空 Schema 的 Avro 复杂类型解码问题。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 为 Parquet Broker Load 错误添加了文件/列/行上下文信息。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet Scanner 支持 Arrow Dictionary 值。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- 在 SHOW CREATE 和 DESC 语句中显示 Paimon 表的主键。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- 修复了 PostgreSQL/Oracle JDBC 类型兼容性以及带末尾斜杠的 JDBC URL 构造问题。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)

**物化视图**

- 修复了 JDBC Catalog 中 SQL Server 表的 MV 刷新问题。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 修复了 MV over Outer Join 的延迟物化 Slot 可空性问题。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- 拒绝 AUTO 和 INCREMENTAL MV 的分区刷新。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- 物化视图变为 inactive 后停止 MV 调度器。[#71265](https://github.com/StarRocks/starrocks/pull/71265)

**其他**

- 支持 `SHOW GRANTS FOR CURRENT_USER()` 以兼容 MySQL 客户端。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- 允许在显式事务中执行 SHOW 语句。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- 修复了空结果集时 Arrow Flight 返回列名为 `r` 的问题。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- 在 Java UDF 代码中添加了 JNI 异常处理检查。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- 修复了 `ai_query` 函数注册问题。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- 修复了使用 `enable_load_profile` 时的 Stream Load Profile 采集问题。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- 以会话时区显示 Profile 的 START_TIME/END_TIME。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- 使 `star_mgr_meta_sync_interval_sec` 可在运行时动态修改。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- 修复了 `information_schema.tables` 在等值谓词中未转义特殊字符的问题。[#71273](https://github.com/StarRocks/starrocks/pull/71273)

## 4.1.0

发布日期：2026 年 4 月 13 日

### 存算分离架构

- **新的多租户数据管理**

  存算分离集群现在支持基于范围的数据分布以及 Tablet 的自动拆分和合并。当 Tablet 过大或成为热点时，可以自动拆分，无需更改 Schema、修改 SQL 或重新导入数据。此功能可以显著提高可用性，直接解决多租户工作负载中的数据倾斜和热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 Tablet 支持（第一阶段）**

  使存算分离集群能够在每个 Tablet 中存储显著更多的数据，长期目标是达到每个 Tablet 100 GB。第一阶段在整个数据摄取、主键更新和压缩管道中引入了 Tablet 内部并行处理，因此随着 Tablet 规模的扩大，单个 Lake Tablet 不再成为单线程瓶颈。改进内容包括：单个 Tablet 内的并行 Compaction（支持 Segment 级拆分）、Lake 导入过程中的并行 MemTable 最终化、刷新和合并（包括导入溢出路径）、Tablet 内部的并行 Publish 以及主键表的并行条件更新，此外还针对支持远程存储映射文件的云原生主键索引，引入了范围拆分/并行/分级大小的 Compaction 机制。这些改进共同显著降低了大型 Tablet 工作负载的摄入内存开销、Compaction 放大效应以及 FE 元数据压力。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **Fast Schema Evolution V2**

  存算分离集群现在支持快速 Schema 变更 V2，可实现秒级 DDL 执行 Schema 操作，并进一步将支持扩展到物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 存算分离上的倒排索引**

  为存算分离集群启用内置倒排索引，以加速文本过滤和全文搜索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  查询级别的缓存命中率现已在审计日志和监控系统中公开，以提高缓存透明度和延迟诊断能力。额外的数据缓存指标包括内存和磁盘配额使用情况以及页面缓存统计信息。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表添加了段元数据过滤器，可在扫描期间根据排序键范围跳过不相关的段，从而减少范围谓词查询的 I/O。[#68124](https://github.com/StarRocks/starrocks/pull/68124)
- 支持 Lake DeltaWriter 的快速取消，减少存算分离集群中已取消的摄取作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 新增支持基于时间间隔的调度，用于自动化集群快照。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- 支持 MemTable 刷写和合并的管道执行，提高存算分离集群中云原生表的摄取吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)
- 支持 `dry_run` 模式修复云原生表，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)
- 在存算一体集群中为发布事务添加了线程池，提高了发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- 支持动态修改云原生表的 `datacache.enable` 属性。[#69011](https://github.com/StarRocks/starrocks/pull/69011)

### 数据湖分析

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入位置删除文件，从而可以直接从 StarRocks 对 Iceberg 表执行 DELETE 操作。此支持涵盖了计划、写入、提交和审计的完整管道。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive 和 Iceberg 表的 TRUNCATE**

  支持对外部 Hive 和 Iceberg 表执行 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上的增量物化视图**

  将增量物化视图刷新支持扩展到 Iceberg 仅追加表，从而无需完全刷新表即可实现查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg 中半结构化数据的 VARIANT 类型**

  支持 Iceberg Catalog 中的 VARIANT 数据类型，用于灵活的、读时模式的半结构化数据存储和查询。支持读、写、类型转换和 Parquet 集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增支持 Iceberg v3 默认值特性和行血缘。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增支持 `rewrite_manifests` 过程，并扩展了 `expire_snapshots` 和 `remove_orphan_files` 过程，增加了额外参数以实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- **Iceberg `$properties` 元数据表**

  通过 `$properties` 元数据表添加了对查询 Iceberg 表属性的支持。[#68504](https://github.com/StarRocks/starrocks/pull/68504)

- 支持从 Iceberg 表读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)
- 支持从 Iceberg v3 表读取 `_row_id`，并支持 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- 支持创建具有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中显示属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 支持使用特定分支、标签、版本或时间戳查询 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)
- 支持 Paimon 表的复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)
- 支持 Paimon 视图。[#56058](https://github.com/StarRocks/starrocks/pull/56058)
- 支持 Paimon 表的 TRUNCATE 操作。[#67559](https://github.com/StarRocks/starrocks/pull/67559)
- 在创建 Iceberg 表时，支持带括号语法的 Partition Transforms。[#68945](https://github.com/StarRocks/starrocks/pull/68945)
- 支持 Iceberg 表的 ALTER TABLE REPLACE PARTITION COLUMN。[#70508](https://github.com/StarRocks/starrocks/pull/70508)
- 支持基于 Transform Partition 的 Iceberg 全局 shuffle，以改进数据组织。[#70009](https://github.com/StarRocks/starrocks/pull/70009)
- 支持为 Iceberg 表 sink 动态启用全局 shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- 为 Iceberg 表 sink 引入了 Commit 队列，以避免并发 Commit 冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)
- 为 Iceberg 表 sink 添加了主机级排序，以改进数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)
- 默认启用 ETL 执行模式下的额外优化，无需显式配置即可提高 INSERT INTO SELECT、CREATE TABLE AS SELECT 和类似批处理操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- 为 Iceberg 表上的 INSERT 和 DELETE 操作添加了提交审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)
- 支持在 Iceberg REST Catalog 中启用或禁用视图端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)
- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)
- 支持对各种 Iceberg catalog 类型执行 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)
- 支持 AWS Glue Catalog 表中的分区投影。[#67601](https://github.com/StarRocks/starrocks/pull/67601)
- 为 AWS Glue `GetDatabases` API 添加了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)
- 支持 Azure ABFS/WASB 路径映射，并带有端点注入（`azblob`/`adls2`）。[#67847](https://github.com/StarRocks/starrocks/pull/67847)
- 为 JDBC 目录添加了数据库元数据缓存，以减少远程 RPC 开销和外部系统故障的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)
- 为 JDBC 目录添加了 `schema_resolver` 属性，以支持自定义模式解析。[#68682](https://github.com/StarRocks/starrocks/pull/68682)
- 支持 `information_schema` 中 PostgreSQL 表的列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)
- 改进了 Oracle 和 PostgreSQL JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎

- **递归 CTE**

  支持递归公共表表达式 (CTE)，用于分层遍历、图查询和迭代 SQL 计算。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 重写，支持基于统计信息的倾斜检测、直方图支持和 NULL 倾斜感知。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- 改进了窗口上的 COUNT DISTINCT，并增加了对融合多 DISTINCT 聚合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- 支持窗口函数的显式 Skew hint，通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739) [#67944](https://github.com/StarRocks/starrocks/pull/67944)
- 支持 CTE 的物化 Hint。[#70802](https://github.com/StarRocks/starrocks/pull/70802)
- 默认启用全局延迟物化，通过将列读取推迟到需要时进行，从而提高查询性能。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- 在 Trino 解析器中支持 INSERT 语句的 EXPLAIN 和 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)
- 支持 EXPLAIN 以提高查询队列可见性。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数和 SQL 语法

- 添加了以下函数：
  - `array_top_n`：返回按值排序的数组中的前 N 个元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：将多个数组按元素组合成一个结构体数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: 使用缩进格式化 JSON 字符串。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: 在 JSON 字符串中指定路径设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: 将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: 对具有相同键的行中的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: 返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: 返回当前仓库的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: 将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: 从 SQL 调用外部 AI 模型以进行推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `min_n` / `max_n`: 返回前 N 个最小/最大值的聚合函数。[#63807](https://github.com/StarRocks/starrocks/pull/63807)
  - `regexp_position`: 返回字符串中正则表达式匹配的位置。[#67252](https://github.com/StarRocks/starrocks/pull/67252)
  - `is_json_scalar`: 返回 JSON 值是否为标量。[#66050](https://github.com/StarRocks/starrocks/pull/66050)
  - `get_json_scalar`: 从 JSON 字符串中提取标量值。[#68815](https://github.com/StarRocks/starrocks/pull/68815)
  - `raise_error`: 在 SQL 表达式中引发用户定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
  - `uuid_v7`: 生成时间有序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
  - `STRING_AGG`: GROUP_CONCAT 的语法糖。[#64704](https://github.com/StarRocks/starrocks/pull/64704)
- 提供以下函数或语法扩展：
  - 在 `array_sort` 中支持 lambda 比较器以实现自定义排序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持 FULL OUTER JOIN 的 USING 子句，具有 SQL 标准语义。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持对带有 ORDER BY/PARTITION BY 的框架窗口函数进行 DISTINCT 聚合。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中支持 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 支持 VARBINARY 用于类似 count distinct 的聚合函数。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持 `MULTIPLY`/`DIVIDE` 用于区间操作。[#68407](https://github.com/StarRocks/starrocks/pull/68407)
  - 支持 IN 表达式中的日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 支持 BEGIN/START TRANSACTION 的 WITH LABEL 语法。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 支持 SHOW 语句中的 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持 `ALTER TASK` 语句用于任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
  - 支持从 S3 加载 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
  - 支持 Scala 函数中的命名参数。[#66344](https://github.com/StarRocks/starrocks/pull/66344)
  - 支持 CSV 文件导出多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)
  - 支持 `ANALYZE PROFILE` 中的 `last_query_id()`，以便轻松进行查询配置文件分析。[#64557](https://github.com/StarRocks/starrocks/pull/64557)

### 管理与可观测性

- 支持资源组的 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改善多仓库 CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图以检查 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest 黑名单，以在集群级别阻止特定的查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法访问的节点检索 Arrow Flight 数据。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，将全局变量更改传播到现有连接，而无需重新连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 添加了内置 UI 功能，用于分析查询配置文件和查看格式化 SQL，使查询调优更易于访问。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 端点，以提供结构化的集群概览。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增了一个全局只读系统变量 `@@run_mode`，用于查询当前集群运行模式（存算分离或存算一体）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2` 以改进查询队列管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持 Stream Load 和 Merge Commit 操作的用户级默认仓库。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 新增 `skip_black_list` 会话变量，以便在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为指标 API 添加了 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 添加了模拟用户支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 将 `table_query_timeout` 添加为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 新增 FE 配置文件日志记录，具有可配置的延迟阈值。[#69396](https://github.com/StarRocks/starrocks/pull/69396)
- 支持添加 FE 观察者节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 在 `information_schema.loads` 中支持 Merge Commit 信息，以提高加载作业的可见性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在云原生表中显示 tablet 状态，以便更好地进行故障排除。[#69616](https://github.com/StarRocks/starrocks/pull/69616)
- 为外部目录可观测性添加了按目录类型划分的查询指标。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 为 FE 和 BE 添加了 Debian (.deb) 打包支持。[#68821](https://github.com/StarRocks/starrocks/pull/68821)

### 安全

- [CVE-2026-33870] [CVE-2026-33871] 替换了 AWS bundle 并将 Netty 升级到 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级到 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级到 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### 错误修复

修复了以下问题：

- 通过跳过范围分布 Tablet 的数据文件删除，修复了 Tablet 分裂后的数据丢失问题。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 中复杂类型的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了由 `shared_ptr` 在 `BatchUnit` 和 `FetchTaskContext` 之间循环导致的内存泄漏。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了错误路径上并行段/行集加载中的 use-after-free 问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 修复了聚合溢出 `set_finishing` 中潜在的哈希表数据丢失问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 修复了 SystemMetrics 中由于并发 getline 访问导致的 double-free 崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在急切合并消耗所有块时发生的崩溃问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了当字典支持表被删除时 `visitDictionaryGetExpr` 中的 NPE 问题。[#71109](https://github.com/StarRocks/starrocks/pull/71109)
- 修复了在 Stream Load/Broker Load 中分析生成列时，如果引用列缺失导致的 NPE 问题。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 修复了当自动创建的分区被 TTL 清理器删除时导致的 NPE 问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了当快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中的 NPE 问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了外连接中带有常量侧列引用的谓词重写不正确的问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了磁盘重新迁移 (A→B→A) 期间 GC 竞争导致的 PK tablet 行集元数据丢失问题。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- 修复了 SharedDataStorageVolumeMgr 中的 DB 读锁泄漏问题。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- 修复了在存算分离中修改 CHAR 列长度后查询结果错误的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了多表情况下 MV 刷新错误。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新后 MV 回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步 MV 中全空值处理错误。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了快速模式更改 ADD COLUMN 后查询 MV 时重复列 ID 错误。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了 IVM 刷新记录不完整的 PCT 分区元数据问题。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 修复了由共享 DecodeInfo 导致的低基数重写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数连接谓词类型不匹配问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了 Parquet Page Index Filter 在 `null_counts` 为空时的段错误。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 展平数组和对象在相同路径上的冲突。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算器不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB FileSystem 缓存键以包含容器。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池满时发生的死锁。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon 目录刷新时在 ObjectTable 上发生 ClassCastException 导致的崩溃。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了 PaimonView 将表引用解析为 default_catalog 而非 Paimon 目录的问题。[#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 修复了带有常量子查询的 FULL OUTER JOIN USING。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了带有 CTE 范围的 join on 子句错误。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 修复了短路点查找中缺少分区谓词的问题。[#71124](https://github.com/StarRocks/starrocks/pull/71124)
- 通过使用 bindScope() 模式修复了 ConnectContext 内存泄漏。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了存算一体集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift 接受线程在遇到任何异常时退出。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了例行加载列映射中的 UDF 解析。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了字典页过大时扫描结果错误。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了范围分区重叠。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了查询队列分配时间和挂起超时问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了处理空字面量数组时 `array_map` 崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 的堆栈溢出问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了优化器超时问题。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- 修复了 LDAP 认证中不区分大小写的用户名规范化问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计和 SQL 编校中屏蔽了用户认证字符串。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更

- ETL 执行模式优化现在默认启用。这使得 INSERT INTO SELECT、CREATE TABLE AS SELECT 和类似批处理工作负载无需显式配置更改即可受益。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除了常量值外，还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：USING 列在输出中只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- 全局惰性物化现在默认启用。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- `query_queue_v2` 现在默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL 事务默认由会话变量 `enable_sql_transaction` 控制。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
