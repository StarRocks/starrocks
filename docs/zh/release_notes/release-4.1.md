---
displayed_sidebar: docs
description: "StarRocks 4.1 发行说明：多租户基于范围的 tablet 自动分裂、大容量 tablet 支持（目标 100 GB）、快速 Schema 演进 V2，适用于……"
---

# StarRocks 4.1 版本 {#starrocks-version-41}

:::danger

**容器镜像问题（v4.1.0）**

由于 v4.1.0 容器镜像中存在不稳定的加载顺序问题，BE 进程在容器环境中可能无法可靠启动。**容器环境用户NOT应升级到 v4.1.0。**请等待包含修复的 v4.1.1 版本（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**降级说明**

- 将 StarRocks 升级到 v4.1 后，NOT要降级到任何低于 v4.0.6 的 v4.0 版本。

  由于 v4.1 中引入的数据布局内部变更（与 tablet 的拆分和分布机制相关），升级到 v4.1 的集群可能会生成与早期版本不完全兼容的元数据和存储结构。因此，从 v4.1 降级仅支持降级到 v4.0.6 或更高版本，不支持降级到 v4.0.6 之前的版本。此限制源于早期版本在解析 tablet 布局和分布元数据方面的向后兼容性约束。

:::

## 4.1.4 {#414}

发布日期：2026 年 8 月 5 日

### 行为变更 {#behavior-changes}

- 在存算分离模式下，`TABLESAMPLE` / `SAMPLE` 子句和 `ANALYZE SAMPLE TABLE` 现在可以对 lake 表生效。此前采样选项从未传递到 BE，导致执行了全量扫描。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- 对 `flat_json` 配置的 `ALTER TABLE` 变更现在通过带版本的任务传播到 BE 节点，从而能够可靠生效。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- 由 GIN 倒排索引解答的 `NOT MATCH` 谓词不再返回 NULL 行；现在会正确地将 NULL 行从结果中排除。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` 不再为字符类列（优化器不会用到）计算直方图，仅计算 MCV，从而降低了 analyze 的开销。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` 现在会按保序的分区范围对输出 manifest 进行聚类，从而为每个 manifest 生成更紧凑的分区边界，并在拥有大量分区的表上实现更好的 manifest 裁剪。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 多表 stream load 在决定是否使用合并事务日志时，现在会遵循每张表自身的 `file_bundling` 属性，而不再只依据全局的 `lake_use_combined_txn_log` 配置。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- 在主键表上，GIN 倒排索引与列模式部分列更新组合使用时，不再返回损坏的结果；现在索引由 delta column group segment 提供服务。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 针对整数数组的 `array_difference`，现在会先以 64 位计算相邻差值，再扩展为 `BIGINT`，从而修复了 int32 溢出问题。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 除数为非常量的除法表达式不再被视为单调函数，从而修复了可能导致结果错误的 ZoneMap 裁剪问题。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 当某个 chunk 展平后的结果超过 4 GB 时，数组和 map 构造函数现在会报错，而不是静默地损坏数据。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- 主键的插入操作现在能够正确拒绝 L1/L2 持久化索引中已存在的键；此前该存在性检查被跳过。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- 针对 range-colocate 表的基于采样的 tablet 预拆分，现在会将新分片分散到各计算节点，而不是集中堆积到源 tablet 所在的 worker 上，从而修复了 range 批量导入比 hash 慢约 3 倍的问题。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 增量或 AUTO 物化视图现在会在 `AUTO` 刷新模式下为 PCT 回退方案重建维护查询，从而修复了 IVM 不支持的形态所导致的刷新失败问题。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 现在可用于非聚合的 INCREMENTAL 物化视图；存储填充的 `__ROW_ID__` 列不再出现在 MV DDL 的列列表中，从而使处于非活动状态的 IVM 能够被重新激活。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg 分区缓存现在受内存限制（而不仅仅是条目数量限制），并且其使用情况会被暴露出来，从而防止在拥有大量分区的表上出现无限增长的问题。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- 使用 OAuth2 客户端凭据的 Iceberg REST catalog，在后台令牌刷新任务失效后现在能够自我恢复，而不是导致后续所有请求失败。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake full vacuum 现在能够回收此前被跳过的孤立 `.lcrm`（Lake Compaction Rows Mapper）文件。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 存算分离发布现在会拒绝写入缺少 tablet 的 bundle tablet 元数据文件，从而防止分区在发布过程中永久卡住。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 被废弃的外部（Spark/Flink 连接器）扫描上下文，在被回收时现在会取消其 pipeline fragment，并且外部扫描计划会设置 `query_delivery_timeout` 以限定 QueryContext 的生命周期。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- 物化视图的 pinned-range 映射现在使用正确的键（表 UUID），从而修复了 OLAP 表的 MV 引导 pinning 被禁用，以及不同数据库中同名表被错误处理的问题。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- 转发给 leader FE 的语句的审计日志，现在会记录完全限定的被查询关系（与 leader 一致），而不是 CTE 别名和未限定的表名。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 将完整的 `STRUCT` 列与 `ROLLUP`、`CUBE` 或 `GROUPING SETS` 一起聚合时，不再在计划阶段因 `usedStructFiledPos` 错误而失败。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE 内存 `Estimator` 现在会计入容器开销，并且 Parquet 扫描器现在使用受限的 chunk 大小而不是完整的 batch 大小来填充缺失的列，从而提升了内存统计的准确性。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 功能优化 {#improvements}

- 在 Routine Load 中新增支持 `INCLUDE METADATA` 子句，用于暴露 Kafka/Pulsar 消息元数据（如 partition、offset 和 timestamp）。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- 将 Routine Load 元数据别名设为可选。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- 为 `information_schema.materialized_views` 新增了 `LAST_FRESHNESS_CONFIRMED_AT` 列。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- 通过 `/metrics` 端点暴露 Data Cache 指标，并新增了 FE Compaction 指标、`ALTER TABLE` 列操作指标和耗时，以及 exchange sink 上的 `CompressedInputBytes` 指标。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- 向终端用户暴露来自 connector 元数据操作的认证和连接错误详情。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 改进了大列容量限制检查的错误消息。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- 新增了 `hdfs_backend_selector_cache_replica_num` 变量，并使对象存储客户端缓存大小可在运行时修改。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- 将基于采样的 tablet 预分割 meta-tier 读取器扩展到更多排序键类型，包括 `CHAR`、复合排序键，以及经 UTC 调整的 Parquet `TIMESTAMP` / ORC `TIMESTAMP_INSTANT`。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 改进了外部表统计信息收集：现在会跟踪谓词列的使用情况，多个列在单次扫描中一起收集，并为 Iceberg 表应用了有限成本的扫描预算。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 支持 right-outer、semi、anti 和 full-outer range-colocate join。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- 优化了 Query Queue V2 的成本估算器。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- 将通配符字面量前缀下推到 S3 的 `ListObjectsV2` 调用中，用于 `FILES()` 通配符匹配，从而减少列出的对象数量。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- 在 `remove_orphan_files` 中扫描 Iceberg manifest 条目时，现在只投影 `file_path` 列。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- 支持 lake 主键表 tablet 的 base compaction，并为独立排序键的存算分离主键表支持导入落盘（load spill）及主键索引 SST 的即时生成（eager PK-index SST）。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 支持在存算分离 range 分布表上以仅元数据方式添加末尾排序键列。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- 在 fragment 取消时取消正在进行的 exchange sink RPC，并在 reshard 清理期间取消重新分片分区上正在进行的 Compaction。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 在等待之前触发多语句 Stream Load 通道，降低了导入延迟。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- 为 `Analytor::process` 中的列升级新增了内存限制检查。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 在异步 delta writer 停止时保留真实的错误状态。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- 在 lake 主键索引 SSTable 上记录生成版本，在 lake 主键发布时应用 `op_write.seg_delvecs`，并在 Compaction 发布冲突解决期间跳过打开输出 segment footer。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 为 lake 导入和发布版本路径新增了堆栈跟踪和细粒度跟踪计数器，以辅助诊断。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### 安全性 {#security}

- [CVE-2026-44891] 将 Netty 升级至 4.1.136.Final，以修复 STOMP 子帧解码器中的内存耗尽（DoS）漏洞。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] 将 Apache Thrift 升级至 0.24.0，以修复 C++ 绑定中的基于堆的缓冲区溢出漏洞和无限循环漏洞。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 排除了存在漏洞的 Jetty jar 包（客户端 Digest 认证绕过，通过 Hadoop 间接引入），并将 pgjdbc 升级至 42.7.12。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 排除了未使用的 `avro-ipc` jar 包，该包捆绑了存在漏洞的 jQuery 1.4.2（以及其他 jQuery XSS CVE）。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- [CVE-2024-29857] 移除了与已修复版本一起打包的存在漏洞的过期传递依赖项（例如 `bcprov-jdk15on` 1.70 和已停止维护的 `okhttp` 2.x），并新增了依赖禁用规则以防止回归。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### 问题修复 {#bug-fixes}

修复了以下问题：

- 在 join 谓词推导中对空范围（empty range）进行处理时导致的 Planner 崩溃。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- 聚合操作被错误地下推到带有非空常量 `ELSE` 的 `CASE` 之上。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` 的 partition-by 被改写为一个已被裁剪的字典槽（dictionary slot）。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- join shuffle-join 输出属性分支中的运算符优先级错误，以及 `predicateCommonOperators` 在 join operator 构造过程中未被正确传递。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 分析阶段视图列以及 `ROLLUP` 键的可空性（nullability）不正确。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- `regexp_extract_all` 在零长度捕获组上出现无限循环。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` 中错误的 `nullsFraction` 钳制（clamp）。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 字符串转数字的 schema change 转换中 `CAST` 语义不正确。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同步物化视图改写时，对相同列的 `min`/`max` 操作丢失了一个 rollup 列。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- 在将扫描谓词上提时，改写被错误地应用到 `array_map` lambda 内部。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- 在 bucket-aware 执行下，Iceberg bucket 表上的 `COUNT(DISTINCT)` 计数过高。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 对必需的 Iceberg 列执行 `UNNEST` + `GROUP BY` 时，可空输出出现错误。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- 降低了 `INSERT OVERWRITE` 代码路径上的锁竞争，并恢复了 `Operator` salt，以避免在 CBO 表裁剪过程中出现 Memo 自引用。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 当字符串日期分区列与时间值进行比较时，Iceberg 分区裁剪、manifest 行数估算、元数据删除、equality-delete 应用以及 Delta Lake 分区裁剪均出现不正确的结果。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg 读取未遵循目标快照的表结构和分区规范；在 Iceberg V1 表上删除分区字段后查询失败；以及物化视图改写在 `rollback_to_snapshot` 之后返回过期结果。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg manifest 数据文件缓存返回了不完整的文件集；增量扫描范围迭代器在并发关闭时不安全；以及分析时的时间旅行快照绑定现在改为尽力而为方式。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta 元数据派生的统计信息现在会标记为 `StatsSource=TABLE_METADATA`。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake 和 Kudu 非分区物化视图查询改写。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- 由于 `gcs-connector` 3.x 配置重命名，导致 GCS 授权凭证被忽略。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive `getTable()` 现在会在 `get_table_req` 回退之前重新连接，并通过异常类型而非消息文本来检测表不存在的情况。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet 列索引统计信息中不支持 `BOOLEAN` 的最小值/最大值。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 对于名称包含 `.` 的键，Flat-JSON 子字段读取问题，以及重建中间 Flat-JSON 对象而非返回 NULL 的问题；当子字段键存在大小写不敏感冲突时，现在会跳过 JSON 子字段下推。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 当只有一个分支带有位图索引时，保留 OR 嵌套谓词。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- 主键表自增列部分列更新应用时 BE 崩溃。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- 在导入落盘期间，由于 `LoadChunkSpiller` 初始化竞争导致 BE 崩溃。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 原生 Parquet 读取器在不完整的嵌套湖表结构上崩溃，以及针对嵌套在 Avro 复杂类型列中的 `BOOLEAN` 出现 ASAN 崩溃。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 在将无效字符串转换为 `NOT NULL` 数值列时，schema change 期间出现 `bad_variant_access`。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` 错误处理路径中的堆缓冲区溢出，以及多字符 CSV 分隔符跨越缓冲区扩展时出现的释放后使用问题。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- 在扫描销毁期间对 `MorselQueueFactory` 的释放后使用问题、在取消时可落盘 JOIN 构建 `set_finishing` 中的释放后使用问题，以及 `PipelineDriver` 析构函数中的全局运行时过滤器计时器泄漏/未调度计时器问题。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 对 `SparseRangeIterator::has_more()` 进行空值安全处理，以修复物理拆分空 tablet 导致的 CN 崩溃问题，以及 bRPC stub 缓存清理计时器泄漏问题。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- `ConnectContext` 未在查询部署工作节点上恢复。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 为存算分离主键表在事务内保留 upsert/delete 顺序，包括让落盘合并操作感知操作类型。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- 在 `cal_new_base_version` 中读取持久化元数据以避免 `prev_garbage_version` 悬空；使 `base_version` 与 `publish_version` 中的 `base_metadata` 保持同步；以及在常规路径上持久化合并的并行 Compaction 事务日志。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- Range-colocate 相关修复：停止对齐任务风暴并对未对齐的 colocate JOIN 采取失败关闭策略，修复了在 bucket-shuffle 下空值安全 JOIN 丢失匹配的问题，并改进了计划反馈中 range-colocate JOIN 的检测。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- Tablet 拆分/重分片修复：拆分时提升表的乐观版本号，以便并发查询重新生成计划；在相同 tablet 重分片之前刷新主键索引 memtable；在重分片过程中按版本区间保留 lake vacuum 文件；停止对不可拆分的 range 分布 tablet 进行自动拆分任务循环；以及修复拆分后 range 分布排序键上的 `IS NULL` 裁剪问题。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- 拒绝对基于键派生的 range 分布表重新排序其键列的全列 `ORDER BY`。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- 按刷新顺序合并导入落盘并行合并结果，并在文件打包传递中通过 `metaId` 排除已触及的索引。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- 为吸收 NULL 的字典映射分组键预留字典大小 + 1 的空间，并在重新调度 Profile 报告之前检查驱动就绪状态。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- `PlannerMetaLocker` 在锁从未成功获取的情况下跳过了解锁操作。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- 按 Warehouse 过滤资源组。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- 对 `SHOW CREATE ROUTINE LOAD` 输出中的 `jsonpaths` 值进行转义。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- 修复了 Arrow Flight 预处理语句转发问题。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 修复了字典刷新间隔溢出以及非预期自动刷新的问题。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- 修复了在 `ERROR_IF_OVERFLOW` 下最小值/最大值为空时统计信息缓存加载失败的问题。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- 使 catalog 删除存在性检查在写锁下具备原子性。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {#413}

发布日期：2026 年 7 月 14 日

### 行为变更 {#behavior-changes-1}

- `CTAS` 现在会保留显式声明的 `VARCHAR(N)` 列长度，而不是将其扩展为 `VARCHAR(MAX)`。已存在的表不受影响；使用 `CTAS` 创建的新表将在后续写入时强制执行所声明的长度。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- 在没有 `OPERATE ON SYSTEM` 权限的情况下查询 `sys.fe_memory_usage` 或 `sys.fe_locks` 现在会返回明确的拒绝访问错误，而不是让人误解的节点查找失败错误。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` 以及 broker/Stream Load 不再对使用 `isAdjustedToUTC=false` 写入的 `INT64` Parquet 时间戳应用会话时区偏移；这些时间戳现在被视为墙钟值并按原样导入。v4.1.3 之前从此类文件导入的数据可能与升级后导入的数据不同；如需保持一致性，请重新导入。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 成功提交的多表事务 Stream Load 任务现在会在 `information_schema.loads` 和 `SHOW STREAM LOAD` 中正确显示为 `VISIBLE`，而不再停滞在 `PREPARING` 状态。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- 连接器增量扫描范围调度现在始终会复用已部署 fragment 的 driver 布局，从而防止扫描范围被错误分配给不存在的 driver。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` 常量折叠现在与 MySQL 8 的反斜杠转义语义保持一致，修正了诸如 `'a\\\\b'` 之类的模式此前返回相反结果的问题。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load 现在支持 `property.kafka_partition_discovery` 属性，即使指定了 `kafka_partitions` 和 `kafka_offsets` 来设定精确的起始偏移量，也可以继续进行分区自动发现。当未设置 `property.kafka_default_offsets` 时，任务已有消费进度后新发现分区的默认起始偏移量将从 `OFFSET_END` 变更为 `OFFSET_BEGINNING`——这一变更适用于**所有**自动发现任务，而不仅限于使用新属性的任务。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非分组聚合现在会在合并之前下推到 `UNION ALL` 分支，从而减少对联合结果进行聚合的查询的网络传输和内存占用。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM 维护查询现在会在每次刷新时根据当前视图定义重新生成，而不再使用 `CREATE` 时固化存储的查询文本；现有的物化视图无需重建即可自动获得改写器修复的收益。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- 基于样本的 tablet 预拆分现在会将预拆分分片分散到所有计算节点（`SPREAD` 放置策略），而不是集中放置在源 tablet 所在 worker 上（`PACK` 放置策略），从而提升导入并行度。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 仅修改列注释的 `ALTER TABLE ... MODIFY COLUMN` 现在会走轻量级的仅元数据路径，而不再触发完整的 schema change 任务，并且该优化现已支持主键列。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` 和 `CEIL` 现在被视为非保留关键字，可以不加引号直接用作列名。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` 的输出现在始终会在 Properties 列中包含 UDF 和 UDAF 的 `isolation` 属性（`shared` 或 `isolated`）。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` 的默认值从 100 提高到 200，通过在每次 `DeleteObjects` 请求中批量处理更多过期文件删除操作，提升了对象存储上的清理（vacuum）吞吐量。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- 带有下发凭证的 Iceberg REST catalog 表现在会被缓存，其凭证会在后台刷新，从而消除了每次 `getTable()` 都调用 `GetDataAccess` 所导致的 AWS Lake Formation 限流问题。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM 的 `bitmap_union`、`hll_union` 和 `percentile_union` 聚合状态现在在物化视图中只存储一次，而不是两次（可见列 + 隐藏的 `__AGG_STATE_` 列），从而将这些 sketch 类型的存储占用减半。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 增量物化视图现在支持 `bitmap_agg`、`hll_union`、`percentile_union` 和 `bitmap_union` 聚合函数，使精确去重和基于 sketch 的聚合能够以增量方式维护。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- 基于样本的 tablet 预拆分数量现在会向上取整为活跃计算节点数的最近倍数以实现均匀分布，并设有最小 tablet 大小下限，以避免小规模导入产生过多碎片。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改进项 {#improvements-1}

- `ngram_search` 函数现在支持非常量的 needle 参数。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- 新增了由 `enable_http_auth` FE 配置项控制的 HTTP 身份认证框架，用以在所有外部 HTTP 接口上强制执行身份认证和 RBAC。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- 在 `information_schema.materialized_views` 中新增了刷新和放置可观测性列（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- 新增了一个由新 FE 配置项控制的可选延迟刷新外部统计信息缓存的机制，用于日志回放过程中，防止外部元数据存储缓慢或卡住导致 FE 日志回放或启动被阻塞。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- 现在允许在 range-distribution（存算分离）排序键列上通过快速 schema change 方式增加 `VARCHAR` 长度，而无需重写数据。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 当存算分离事务日志写入超过可配置的阈值时，新增了堆栈跟踪转储功能，使缓慢的 `put_txn_log` / `put_combined_txn_log` 调用更易于诊断。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- tablet 预拆分元数据层 footer 读取器现在支持 `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR` 以及 ORC `TIMESTAMP` 排序键，从而减少了必须回退到数据层采样的导入次数。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- 基于样本的 tablet 预拆分现在也适用于 `INSERT INTO ... SELECT ... FROM <OLAP table>` 导入，以及包含所有排序键列的列列表 `INSERT` 语句。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 为存算分离 tablet 元数据和事务日志文件新增了 Adler-32 校验和保护，可在读取时检测静默数据损坏。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- 新增了按数据库统计的 `txn_max_committed_pending_publish_ms` FE 指标，用于报告最早已提交但尚未发布事务的存续时长，以帮助检测版本发布是否停滞。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- tablet 的拆分/合并现在会根据发布版本响应实时触发，从而减少了导入完成到自动触发拆分/合并之间的延迟。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- 通过将无 SST 的条件合并任务路由到 `pk_index_execution` 线程池，优化了 lake 主键表的条件更新比较阶段。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- 将 lake schema change 和 rollup 任务锁的作用范围从整个数据库缩小到表级别，从而减少了同一数据库中其他表并发操作时的锁竞争。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 在存算一体模式下，将若干数据库级写锁收窄为表级密集写锁，减少了 BE 上报回调和降冷操作期间的锁竞争。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load 现在支持原生的 `MAP` 和 `STRUCT` 目标列。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- range-colocate tablet 稳定性判定现在会等待 StarOS 放置策略收敛后再将分组标记为稳定，从而确保 colocate join 能够实现本机执行。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 改进了外部表的 CBO 统计信息：优化器现在无需完整枚举文件即可根据 Iceberg manifest 估算行数，修正了 Hive/Hudi 在 Parquet/ORC 压缩情况下行数估算偏低的问题，为 JDBC 连接器新增了异步行数统计功能，并在 Puffin 统计信息不可用时为 Iceberg 和外部连接器提供 NDV 估算回退方案。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg manifest 列统计信息现在仅针对聚簇列进行选择性缓存，从而降低了包含大量数据文件的宽表对 FE 堆内存的占用。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部表统计信息收集现在支持跨 FE 重启和高可用故障转移的持久化谓词列跟踪，使 auto-ANALYZE 能够针对正确的列进行处理。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- 新增了结构化的 `[ExternalStats]` 日志行，覆盖外部表统计信息收集从调度到执行的完整生命周期。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` 现在会在外部表统计信息任务的 Properties 列中包含分区、列和快照元数据。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 每个外部表的统计信息来源（`TABLE_METADATA`、`ANALYZE` 或 `NONE`）现在会在查询运行时 profile 中展示。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- 为 Iceberg 和 Delta Lake 外部表新增了分区过滤条件要求和分区数量限制的支持（此前仅 Hive、Hudi 和 Paimon 支持）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` 和直方图 `ANALYZE` 现在支持低于 1% 的采样比例，修复了在大表上计算比例被截断为零导致失败的问题。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- 新增了 `jemalloc_conf` BE 配置项，使 jemalloc 运行时选项可以通过 `information_schema.be_configs` 查看。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 新增了 `compaction_chunk_reset_memory_tracker_threshold_percent` BE 配置项，通过释放已保留的 chunk 容量来降低存算一体模式下主键表 Compaction 期间的内存占用。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- 将 staros 升级到 v4.1.1，包括跨重启的持久化 `datacache.enable`、按 worker 分组的分片预热超时覆盖设置，以及改进的 S3 重试抖动策略。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- 通过在 SQL 字符串中不存在凭据标记时跳过正则表达式扫描，优化了审计热路径上的 SQL 凭据脱敏处理。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet 扫描器新增表达式驱动的按需惰性列加载功能，减少了多分支 `OR` 查询中不必要的 I/O。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` 现在使用与顺序无关的复合估计器代替依赖顺序的 HIP 估计器，从而产生稳定的基数估计结果。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### 安全性 {#security-1}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final，以修复 SNI 处理程序堆耗尽（DoS）、IPv6 子网过滤器绕过以及 DNS 缓存污染问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 将 `jackson-databind` 升级到 2.21.4，以修复两个反序列化漏洞。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 从 Hadoop 传递依赖中排除了 `org.jline:jline-remote-telnet`，以修复未经身份验证的 Telnet 服务器 DoS 漏洞。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] 更新了 pprof 预构建版本，以修复 pprof 二进制文件中的一个漏洞。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 修复了 `information_schema.task_runs` 中的 SQL 注入问题，即谓词值中的单引号可能会突破字面量边界。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key` 和 `iceberg.catalog.jdbc.password` 现在在 `SHOW CREATE CATALOG` 输出中被脱敏。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 修复了当输入以被截断的百分号转义序列结尾时，`url_decode` 中出现的越界读取问题。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- 修复了 `HyperLogLog::deserialize` 接受超出范围的 `SPARSE` 寄存器索引的问题，该问题可能导致堆内存损坏，并在输入格式错误时导致 BE 崩溃。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- 修复了 `bar()` 拒绝接受负宽度值的问题，此前该问题允许字符串无限增长，导致 BE 内存耗尽。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### 问题修复 {#bug-fixes-1}

修复了以下问题：

- `add_files` 使用 Parquet 物理编码字节而非逻辑类型化的值填充 Iceberg 文件边界，导致文件级别的最小值/最大值裁剪不正确（例如在 `DECIMAL` 列上）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- 在遍历输入列表以不可变 `List.of(...)` 形式构建的计划节点时，`ApplyTuningGuideRule` 抛出了 `UnsupportedOperationException` 异常。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` 的两阶段重新计划可能会从第一次规划会话中产生过时的 lambda 参数列引用 ID，从而导致 `expr_type does not match slot_type` 错误。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- 当带有 GIN（倒排）索引的表进行部分列更新，且更新中省略了 GIN 索引列时，会导致查询无限挂起或失败。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- 当 rowset 表结构与 tablet 表结构之间出现模式漂移时，Lake PCU（部分列更新）会崩溃或静默损坏数据。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 针对具有多个模式子句的外部 Iceberg 表执行组合 `ALTER TABLE` 时，每次子句调度都会错误地重新执行之前所有已排队的操作。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- 当在分区刷新与资源组取消交错执行期间，`num_rows` 快照超过实际 chunk 行数时，`PartitionedSpillerWriter` 会因 `SIGSEGV` 而崩溃。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- 由于 BE 信号初始化过程中未忽略 SIGPIPE，BE 进程可能在启动期间（通常在部署后立即）意外退出。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 当由于行范围过滤而跳过 struct VARCHAR 子字段填充时，Parquet 临时字典编码列会泄漏到上层，导致类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` 在审计日志中记录的是 `ReturnRows=0`，而不是实际导出的行数。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- 由于锁类型不匹配，`TabletChecker.doCheck()` 在 `blockingAddTabletCtxToScheduler` 中抛出了 `IllegalMonitorStateException` 异常，导致整轮检查静默中止。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` 对 `DATETIME_PRECISION` 始终返回 `NULL`，导致依赖该字段推导列大小的 MySQL 协议客户端出现问题。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- 当查询跨不同数据库或 catalog 关联了两个使用相同非限定名称的表时，物化视图刷新会因 `Duplicate key` 而失败。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 在某些情况下，可落盘哈希 JOIN 探测阶段会发生崩溃。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 当宽度或分桶数参数为零时，Iceberg 的 `truncate` 和 `bucket` 转换函数会导致 BE 因 `SIGFPE` 而崩溃。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 当被除数为 `TYPE_MIN`、除数为 `-1` 时，`mod()` 和 `pmod()` 会导致 BE 因 `SIGFPE` 而崩溃。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- 当 `bucket_num` 为零或负数时，`histogram()` 会导致 BE 因 `SIGFPE` 而崩溃。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- 当所有输入行均为 `NULL` 时，`encode_fingerprint_sha256` 会因 `SIGSEGV` 而崩溃。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 当通过 GIN 倒排索引评估包含单字符通配符 `_` 的 `LIKE` 模式时，会返回不正确的结果。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- 针对 GIN 倒排索引仅执行 AND 的 `MATCH` 查询时，如果目标 segment 为空，会返回一个虚假的错误。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene 的 `match_all` 查询返回了不正确的结果；通过升级 CLucene 依赖项已解决该问题。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- 向量索引重写会直接在共享的表结构上注册一个合成距离列，导致对同一表进行的无关并发查询出现 `Multiple entries with same key` 错误。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- JOIN 重排序裁剪可能会裁剪掉扫描谓词仍在引用的列，导致统计信息估计抛出 `missing statistic of col` 异常。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` 被错误地通过 sum/count 物化视图进行了改写，在存在重复值时静默丢弃了 `DISTINCT`，并返回了错误的结果。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` 抛出了内部 `NullPointerException` 异常，而不是一个清晰的语义错误。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- 当作业没有 `COLUMNS TERMINATED BY` 子句时，`SHOW CREATE ROUTINE LOAD` 会在第一个 load-description 子句前多输出一个虚假的前导逗号。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- 当 CTE 名称被误认为是真实表引用时，带有 CTE 的 `SECURITY INVOKER` 视图在权限检查中可能因 NPE 而失败。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 当日期/日期时间边界字面量偏移会导致超出可表示范围时（例如 `<= '9999-12-31'`），`ReduceCastRule` 会以 `SemanticException` 中断查询规划。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- 当 JOIN 条件使用 null-safe-equal（`<=>`）析取项时，`SplitJoinORToUnionRule` 会产生重复行。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 在多表外部查询中跨并行元数据准备线程共享的 `Tracers`，在 `enable_profile=true` 下会导致 `IllegalStateException`。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 中的分区消费者错误被静默丢弃，导致分区 TopN 在不报错的情况下返回部分或错误的结果。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- 在 FE 调用方超时后，BE 的 vacuum 任务仍以僵尸进程形式继续运行，耗尽了 `RELEASE_SNAPSHOT` 线程池并使 vacuum 吞吐量崩溃。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- autovacuum 竞争可能瞬间计算出一个比正在进行中的事务大 1 的 `minActiveTxnId`，导致 BE 删除仍需要的合并事务日志，并使发布永久卡死。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- 由于 FE EOS-cancel 与 BE stage-2 部署之间存在竞争，查询在成功完成后仍被错误地标记为已取消。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 当聚合 TopN 运行时过滤器的构建键是 `ConstColumn` 时，BE 会在 `AggTopNRuntimeFilterUpdaterImpl` 中因 `SIGSEGV` 而崩溃。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- 当所有非空数组均为空时，`array_map` / `transform` 会静默丢弃 `NULL` 行并返回错误的行数。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 在 JIT 编译的表达式中，超过 2^64 的 `LARGEINT` / `DECIMAL128` 字面量会被静默截断为 64 位。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- 当最后一个字符带有被截断或无效的多字节前导字节且分隔符为空时，UTF-8 字符串函数（`split`、`split_part`、`str_to_map`）会读取超出字符串末尾的内容。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- 对于格式错误的 JSON，即使在 `ALLOW_THROW_EXCEPTION` SQL 模式下，`parse_json()` 也会静默返回 `NULL` 而不是使查询失败。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 严格模式下的数值窄化类型转换会在插槽数据未定义的 `NULL` 行上错误地引发溢出错误。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 由于负数截断除法余数的问题，带有非零亚秒部分的 1970 年之前的 Parquet `INT64` 时间戳会被解码为乱码值。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970 年之前的 ORC `TIMESTAMP` 值在导入时其亚秒部分会被丢弃。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC stripe 的最小/最大时间戳统计信息在 1970 年之前及亚秒边界情况下被错误解码，导致数据文件被错误裁剪。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- 嵌套在 `ARRAY`、`MAP` 或 `STRUCT` 列内的嵌套 `INT96` Parquet 时间戳，在导入时会丢失一个会话时区偏移量。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet `UINT_32` 值在被导入 `BIGINT` 列时进行了符号扩展而不是零扩展，导致高位无符号整数被静默存储为负值。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` 析构函数在销毁 `_pool`（及其 `Expr` 节点）时早于 `_scanner_ctx`（持有引用这些节点的谓词），导致堆使用后释放（heap-use-after-free）。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- 当多字节 UTF-8 字符横跨 8 MB 解压缓冲区边界时，使用 OpenX SerDe 读取经 gzip 压缩的 JSON Hive 外部表会因 `UTF8_ERROR` 而失败。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- 由于客户端无条件访问缺失的 JSON 字段，ADLS2 `ListPaths` 在非 HNS 账户上会因 `SIGSEGV` 而崩溃。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 当多个 `UNNEST` 算子共享同一个输入数组列并消费不同子字段时，`unnest` 会崩溃或返回错误结果。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- 在 `unnest` 执行期间未强制执行 `query_mem_limit`，导致对大数组进行 `unnest` 操作会将 BE OOM 杀死，而不是使查询失败。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- 当排名限制恰好落在数据块边界上时，带有 `RANK` 边界的 `TopN` 会丢失一行。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- 在 `PushDownDistinctAggregateRule` 之后进行的列裁剪可能会生成一个空的分析（窗口）算子，导致规划或执行错误。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` 仅在 scan 算子上设置了行数限制，而没有设置全局限制，导致在并发情况下对受限子查询执行 `COUNT(*)` 会返回比预期更多的行。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- lake 主键持久化索引重建在 segment-range 模式下使用了错误的 segment 迭代器位置，导致键范围过滤器应用不正确。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` 在没有加表锁的情况下修改了 `rebuildPindexVersion`；`RestoreJob` 在恢复后仅在数据库 READ 锁下修改了物化视图基表信息；`FinalizeCreateTableAction` 在迭代器创建过程中传递了数据库级别的锁。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- 如果在获取每数据库锁时循环中途抛出异常，`dumpImage` 可能会使全局元数据锁无限期滞留。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- 多语句 Stream Load 每个事务会泄漏一个 `TxnStateCallbackFactory` 条目，导致其无限增长并最终耗尽 FE 堆内存。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- 当 catalog、数据库、表或分区名称过长时，直方图统计信息的 `information_schema.task_runs` 行数可能会溢出 `primary_key_limit_size`（128 字节）。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVM 指标发出了无效的 Prometheus `# TYPE` 行（标签集出现在指标名称内部），导致 Prometheus 中止整个抓取过程。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- 在存算分离表上，`SHOW PARTITIONS` 和 `information_schema.partitions_meta` 将所有物理分区的分桶数报告为表级默认值，而不是每个分区实际的分桶数。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` 在存算分离（`CLOUD_NATIVE`）表上对所有 rollup 索引都返回了基表结构。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` 空操作子句被错误地路由到了轻量级注释路径，导致批量 `ALTER TABLE` 语句中出现 `MODIFY COLUMN COMMENT can not be combined with other alter operations` 错误。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- 由于 `isKey` / `aggregationType` 归一化不正确，`isCommentOnlyModification` 可能会将主键/聚合列误判为仅注释更改。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` 可能提交一个循环视图定义，导致后续的 `SELECT` 抛出 `StackOverflowError`。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- 当下游消费者修改前一个数据块，而 `accept()` 仍持有指向该数据块的指针时，`OrderedPartitionExchanger` 会导致堆使用后释放（heap-use-after-free）。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- 当构建端（build-side）的槽描述符（slot descriptor）不可为空但运行时状态可为空时，NLJoin 发生崩溃。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 当结构体字段名不是可解析的 JSON 路径时，`CAST(json/variant AS struct)` 会在片段准备阶段导致 BE 崩溃。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- 嵌套字典表达式的字典解码可能在生产者片段和消费者片段之间产生不兼容的字典转换，从而导致运行时出现 `Dict Decode failed` 错误。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- 当 `get_rowset_by_version` 返回 `nullptr` 且 `gtid` 比较被放在空值检查之前时，schema change 会因空指针解引用而崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- 在 tablet 拆分/合并后，存算分离集群快照变得无法恢复，原因是快照管理器在判断是否回收父级 tablet 元数据时未考虑重分片（reshard）任务。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- 文件打包（file-bundling）清理（vacuum）过程错误地将相邻 tablet 中零行的打包段（bundled segment）标记为非共享，导致其打包文件被删除，而其他 tablet 仍在引用该文件。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 存算分离表 Compaction 发布时丢弃了在 Compaction 事务开始后才变为可见的 rollup/同步物化视图索引，导致打包文件中缺失这些索引。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- 当 Compaction 在 tablet 重分片期间被丢弃时，存算分离持久化索引 Compaction 会错误地删除透传复用的 SSTable 文件。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` 到可为空的 flat-JSON 列的 schema change 会导致 Compaction 读取路径出现 `CHECK` 崩溃。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- 在流式预聚合（pre-aggregation）透传路径中，对可为空列执行 `count_combine` 会导致 BE 崩溃并出现 `SIGSEGV`。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- 由于 JDK 21 中移除了某个反射式 `DirectByteBuffer` 构造函数查找方式，Java UDF 在 JDK 21+ 上加载失败。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- 由于解析器不支持 `CREATE TABLE AS SELECT` 中的 `ENGINE` 子句，向 Unified catalog（Hive metastore）执行 `CTAS` 始终失败。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` 反馈驱动的 JOIN 重建丢失了 `predicateCommonOperators`，导致在存在公共子表达式复用的计划上出现 `InputDependenciesChecker` 校验失败。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- 在具有子分区的表上，当空子分区在构建版本列表之前被裁剪时，Query Cache 归一化会崩溃并出现 `Preconditions.checkState`。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` 会默默跳过以旧别名存储的会话变量，导致查询转储重放时回退为默认值。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- 由于对行组（row-group）起始偏移量进行了重复计数，Iceberg 的 `_row_id` 虚拟列在数据文件包含多个 Parquet 行组时返回了不正确的值。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE 规划器无法定位目标扫描节点，因为它按合成表 ID 而非物理表标识进行匹配，从而丢失了基础快照 ID 和冲突检测过滤条件。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- 当 `cancel_plan_fragment` RPC 在 `PipelineExecutorSet::start()` 被调用之前到达时，`FragmentContext::set_final_status` 会崩溃并出现 `SIGSEGV`。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- 当 `FragmentExecutor` 仍在销毁某个片段时，`QueryContext` 可能已被回收，导致 `ResGuard::reset()` 中出现堆使用后释放（heap-use-after-free）问题。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` 未被初始化，使得默认构造的 `search()` 可能对未初始化的指针进行解引用。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` 微秒数使用了 JVM 默认区域设置的数字字符集进行渲染，导致在阿拉伯语或波斯语等区域设置下出现非 ASCII 数字，从而破坏了 tablet 预拆分中的边界值解析。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- 在 `INSERT OVERWRITE` 之后、tablet 统计信息刷新之前，分区行数可能被写入 `_statistics_.column_statistics` 为零，导致优化器压缩分区基数估算。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- 当全局配置禁用首次导入统计信息收集时，`enable_statistic_collect_on_first_load` 的表级覆盖设置无法启用该功能。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- 当某个 UNION 分支没有输入行时，`PushDownNonGroupedAggregateBelowUnion` 会生成带有非可空声明类型的可空输出，从而导致 BE `CHECK` 失败。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {#412}

发布日期：2026 年 6 月 18 日

### 行为变更 {#behavior-changes-2}

- 当用户连接到其无权限访问的数据库时，现在会返回正确的 MySQL 错误数据包，而不是以 ERROR 2013 关闭连接。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- 对于可以通过函数级权限看到某个函数、但不具备创建函数作用域权限的用户，`SHOW FUNCTIONS` 现在会将 UDF 文件和对象文件路径掩码为 `***`。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 在从 external catalog 查询 Hive 视图时，现在会正确应用 Ranger 行过滤和列脱敏策略。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` 现在能正确保留 `current_timestamp` 生成表达式。`DESCRIBE` 和 `information_schema` 现在会反映该表达式，而不是回填时的字面量。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` 的导入时过滤在会话时区与 UTC+8 不同的集群上不再出现过滤边界偏移问题。导入时间现在以 UTC 纪元毫秒的形式在 FE 与 BE 之间交换。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` 会话变量现在能正确应用于 Paimon 扫描切分计算，而不是始终使用默认值。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` 现已默认启用。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive 分区统计信息不再按定时器逐键自动刷新。分区统计信息现在仅在显式调用 `refreshTable()` 时才会刷新，从而降低大分区表上的 HMS 负载。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- 当 Iceberg 或外部-catalog 基表发生 schema 漂移（列类型变更、列删除或表删除）时，依赖它的物化视图现在会在下一次刷新时被标记为不活跃，而不是默默产生 NULL 行或返回难以理解的错误。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg 连接器现在会对 `AND` 复合谓词中可转换的一侧进行部分下推，而不是在只有一侧可转换时丢弃整个谓词，从而改善分区裁剪和数据跳过（data skipping）效果。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 显式事务 `COMMIT` 现在会正确地最多等待 `query_timeout` 秒（而非毫秒）以获取数据库写锁，从而避免在短暂的并发写入活动下出现虚假的锁超时失败。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM 刷新现在会将严格导入过滤（strict-load filter）错误反馈给调用方，而不是默默丢弃被过滤的行。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` 现在能正确排除 NULL 行，与 `COUNT(col)` 语义保持一致。以 `COUNT(<nullable column>)` 为基础的增量物化视图此前会产生虚高的计数结果。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` 现在也会显示由 `ALTER TABLE ... SET (...)` 触发的针对云原生（存算分离）表上 `file_bundling` 和 `enable_persistent_index` 等属性的异步纯元数据变更作业。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 创建带有引用聚合函数的 `HAVING` 子句的增量物化视图时，现在会在 `CREATE` 时立即以明确的错误失败，而不是在首次刷新时产生内部计划错误。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM 现在支持在增量物化视图中使用 `MIN`/`MAX(DECIMAL)` 聚合函数。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- 当第一个增量特征已经超过 `mv_max_rows_per_refresh` 时，IVM 自适应刷新现在能正确限定增量窗口范围，避免在一次任务运行中刷新全部积压数据。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- 仅使用 GROUP-BY 的增量物化视图（例如 `SELECT k FROM t GROUP BY k`）现在能正确将 `__ROW_ID__` 编码为 VARCHAR，修复了第二次刷新时的崩溃问题。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 功能优化 {#improvements-2}

- 支持 Paimon 视图，包括 `CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`，以及从外部 catalog 中查询 Paimon 视图。Paimon 视图内的表引用现在会根据 Paimon catalog 而不是 `default_catalog` 来解析。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 在 `FILES()` 中支持显式的 `schema` 参数，用于在读取存在模式漂移或复杂嵌套类型的文件时进行稳定的表结构控制。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` 现在会获取所有 FE 节点上的查询概要信息，而不仅仅是当前连接的 FE。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 新增内置函数 `query_id()`，用于返回当前正在执行的查询的 UUID。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 存算分离模式下的 `CREATE`/`ALTER STORAGE VOLUME` 现在会在持久化元数据之前校验存储位置的可访问性（凭证和 endpoint），在配置错误时提前失败。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- 在 BE 中新增了对 AWS S3 凭证的 `WebIdentity` token provider 支持，与 FE 上已有的 `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` 支持保持一致。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 新增 `ADMIN SKIP COMMITTED TRANSACTION` 命令，用于在存算分离表上因缺少 `txnlog`、段文件丢失或远程 I/O 缓慢而导致 publish 被永久阻塞时，解除卡住的 `COMMITTED` 事务。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` 现在会将 `table_name` 谓词下推到 FE，大幅降低单表查找的开销。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- 为 `information_schema` 表新增了缺失的 MySQL 8 列，以提升与在连接自省期间检查 MySQL 8 模式的 BI 工具和 JDBC 驱动程序的兼容性。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- 新增 `enable_pipeline_event_scheduler` BE 配置项，作为集群级别的开关，当设置为 `false` 时会覆盖单会话变量。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 新增可选的宽字符串列隔离功能，用于统计信息收集，以在对具有多个宽字符串列的表收集统计信息时降低每次查询的内存峰值。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- 慢锁日志记录现在支持按事件限速以及可配置的堆栈捕获控制，以防止在高锁争用情况下出现 JVM 安全点停顿。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- 物化视图刷新日志条目现在会在前缀中包含数据库名称，使得在多个模式中存在同名物化视图的多租户部署环境下日志行可区分。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 配置项现在是可变的，可以通过 `ADMIN SET FRONTEND CONFIG` 在运行时切换，无需重启 FE。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- 新增 `enable_print_load_profile_to_log` FE 配置项（默认值为 `false`），用于将导入概要信息（Stream Load、Routine Load、Broker Load 以及 merge-commit 导入）写入 `fe.profile.log`，即使内存存储被查询概要突增所驱逐，这些信息也能得以保留。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` 现在会在 `JobProperties` 中正确渲染列映射，而不是 Java 对象引用。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` 现在使用表级锁而不是 catalog 级锁，从而减少了在拥有大量并发活跃表的 catalog 上的刷新序列化延迟。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- 元数据扫描（后台统计信息收集）现在能够优雅地处理 `ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN` 和 `REORDER COLUMN` 的表结构变更，而不是在变更后的段文件上因找不到而失败。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- 基于采样的 tablet 预分裂现在覆盖了多分区 range 分布表以及 Broker Load，即使没有现有的数据层基线，也能实现首次导入并行度。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL 结果序列化不再使用逐行虚函数调用；每个 chunk 只构建一次带类型的列写入器，从而降低了宽结果集或大结果集的序列化开销。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` 到字符串的类型转换现在会直接写入输出缓冲区，消除了逐行的堆分配。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- 查询统计信息的合并路径将原先的 `SpinLock` 替换为无锁并行 map，从而在 worker 发送中间或最终统计信息时降低了大集群上的 CPU 使用率。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 聚合哈希表和哈希集合的预取现在受 L2 缓存驻留情况门控，避免了在桶数组适合放入 L2 缓存时出现 4–9% 的性能回退。预取距离现在可配置。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 针对存算分离主键表上轻量级 compaction publish 的流水线化逐段 `.lcrm` 读取，减少了顺序对象存储的往返次数。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 存算分离模式下的冷 PK 索引重建扫描现在会在各个段之间并行化，减少了在段读取受远程 I/O 限制时的重建时间。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部查询（统计信息收集、任务运行、物化视图刷新）现在可以在 `SHOW PROC '/current_queries'` 中看到，并且可以通过 `KILL QUERY` 终止。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- 新增了 lake vacuum 批次大小和重试次数的 bvar 指标，用于监控 S3 限流并调优 `lake_vacuum_min_batch_delete_size`。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- 新增了 `CatalogRecycleBin` 大小的 gauge 指标，以便在回收站增长给 FE 堆造成压力之前将其暴露出来。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` 分区表现在会在 `OlapTableSink` 中打开所有分区，而不再应用为 range 分区表设计的最新 N 个启发式规则，从而减少了增量打开的 RPC 开销。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- 支持通过 `FILES()` 或 Broker Load 将 `LARGE_LIST` 和 `FIXED_SIZE_LIST` Arrow 类型导入到 JSON 列中。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 支持在存算分离表上对 merge-commit（`FRONTEND_STREAMING`）导入进行事务日志与文件的合并打包，使其与其他导入类型保持一致。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- 新增可变 FE 配置项 `slow_publish_partition_log_threshold_ms`（默认 3000 毫秒），用于在无需重启 FE 的情况下控制 lake publish 阶段耗时分解告警阈值。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### 安全性 {#security-2}

- [CVE-2026-43869] 将 `libthrift` 升级至 0.23.0，以修复不正确的证书主机验证问题。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] 将 Apache Tomcat 升级到 9.0.118 以修复 HTTP/2 请求头验证问题。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final 以修复 SNI 处理程序堆耗尽（拒绝服务）、IPv6 子网过滤器绕过以及 DNS 缓存投毒问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- 将 pprof 预构建二进制文件升级到 Go 1.25.11，以包含 Go 标准库的安全修复。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### 缺陷修复 {#bug-fixes-2}

以下问题已修复：

- 当 URL 中包含的 `:` 位于 `host:port` 模式之外时，`parse_url()` 会返回错误的主机。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 字典翻译表达式在某些不成立的情况下错误地假定 `f(null) = null`（例如 `IF(col = '1', NULL, 'ok')`）。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- 事务型 Stream Load 使用了默认的 RPC 超时时间，而不是用户指定的超时时间，导致过早超时。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- Iceberg 相等删除（equality delete）文件中 identity 列存在 NULL 值时，无法删除匹配的行，因为 `NULL = NULL` 在 JOIN 谓词中的求值结果为 UNKNOWN。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- 对于包含 `INJECTED` 分区投影列的表，现在的错误信息更具描述性，能够显示是哪些列导致了问题。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- 对仅插入型 ACID Hive 表的查询返回的行数多于预期，因为未能识别插入覆盖（insert-overwrite）操作。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 在并发读取期间，当 Iceberg 元数据条目被固定（pinned）时，磁盘缓存超出了其配置的容量。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 在查询 external catalog 时，Paimon 主键列被错误地标记为不可为空。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 当 `MultiDistinctByMultiFuncRewriter` 对具有多个 `ARRAY_AGG(DISTINCT <const>)` 输入的查询反复应用同一规则时，优化器超时。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Oracle JDBC 日期谓词在下推时未带上 `DATE`/`TIMESTAMP` 关键字，导致出现 NLS 格式错误。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- 分区 TopN 可能会丢失其子算子所需的输出列。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- 无法在具有分区演进（partition evolution）的 Iceberg 表上创建非分区物化视图。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- 在 `information_schema.be_cloud_native_compactions` 中，并行子任务的 Compaction 任务统计信息被覆盖并丢失。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 针对同步物化视图的 `SHOW CREATE MATERIALIZED VIEW` 失败，报错"Table is not found"。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- 当语句 `.log` 文件落在 4 段路径时，Lake publish 多语句事务在 schema change 期间发生死锁。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- Sort merge provider 的错误未传播到 fragment 上下文，导致查询静默失败。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 在长期运行的 follower FE 上，`ConnectorTableId` 从 `int` 溢出为负值，导致 Iceberg 和 Hive 查询失败，并出现误导性的"Invalid table type"错误。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 当 `ALTER TABLE` 带有空的 optimize 子句（没有分布或分区规范）时，会被错误解析，并可能在 FE 回放时破坏表的默认分布。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- 在 ADLS2 存算分离容灾期间，FE 启动失败，因为 `AZURE_PATH_KEY` 未被识别为有效的 `StorageVolumeMgr` 参数。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- 当优化器将嵌套类型的部分内容裁剪为 `UNKNOWN_TYPE`，或使用可为空的数组、映射或结构体模式时，Avro 复杂类型解码失败。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 由于两个 `NullableColumn` 共享同一个 `NullColumn` 对象，COW 列变更优化导致 `map_apply` 及类似函数崩溃。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- 由于该 provider 在 FE 上被急切实例化（eagerly instantiated），具有自定义 `LocationProvider` 的 Iceberg 表在使用 `ClassNotFoundException` 进行 `SELECT` 查询时失败。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` 在每次缓存未命中时都会执行一次额外的 `getTableComment()` 往返，延长了高强度的规划阶段锁持有时间，阻塞了并发的 DDL。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- 当嵌套物化视图返回 `FULL` 或 `UNKNOWN` 时效性时，嵌套物化视图刷新会抛出 `NullPointerException`。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE worker 在向速度较慢的 MySQL 客户端发送查询结果时无限期阻塞。结果发送路径现在会强制执行写超时。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- 在将主键表从 V1 编码（存算一体）集群复制到 V2 编码（存算分离）集群，或在两个存算分离集群之间复制时，PK `.del` 文件未进行转码。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 在 `VERSION_INCOMPLETE` 恢复期间，由于在添加存活副本之前未移除过期的副本引用，`TabletInvertedIndex` 中累积了重复副本。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- 由于 `REPLICATE_SNAPSHOT` 任务和逐文件复制子任务共享同一个线程池，存算分离 lake replication 文件复制导致 CN 崩溃。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- 当 BE 使用 `.000` 十进制后缀格式化单位计数器时，`RuntimeProfileParser` 会抛出 `NumberFormatException`。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 存算分离主键表的 tablet 分裂中，共享 segment 的物理 rowid 编码不正确，导致 `rss_rowid` 条目错误。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- 混合 range-colocate × hash 分布的 `JOIN` 查询返回了 `Unknown error` 而不是有效结果。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` 使用了固定的 UTC+8 格式化器；现在输出会遵循会话 `time_zone`。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- 当所有值均为 `NULL` 且该列经过可为空的一元函数路径时，Decimal 类型列会丢失精度（scale），从而导致下游结果类型损坏。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- 嵌套类型的 JSON 部分追加导致了 ASAN 崩溃。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` 角色的权限缓存在 `GRANT`/`REVOKE` 时未被失效，导致过期权限在到期前一直生效。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- 当子写入器没有追加操作时，`FlatJson` 会崩溃。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- 当物化视图本身包含 `HAVING` 谓词时，聚合物化视图改写会被错误地应用，可能返回不完整的结果。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 在进入并行合并模式时，落盘写入器的 `auto_flush` 标志发生数据竞争，导致在 ARM 上出现非预期的 segment 刷新。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load 调度器在进行阻塞式 BE RPC 调用以获取 Kafka 或 Pulsar 分区元数据时，持有每个任务的写锁，导致锁持有时间最长达 33.6 秒。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- 当启用 `tablet_sched_disable_colocate_balance` 时，已宕机 BE 上的 Colocate tablet 会被错误地报告为 `HEALTHY`。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- 当存在 MISSING（幻影）副本行时，`ADMIN SHOW REPLICA STATUS` 会导致 MySQL 结果流不同步，从而使客户端挂起或断开连接。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 在存算分离模式下，每个分区的协调者声明未在每个发送方的 `open` RPC 中重新记录，导致部分发送方在协调者选举中被遗漏，从而使 `combined_txn_log` 文件未被写入。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_.pipe_file_list` 内部表在 `_statistics_` 数据库或表被删除后未被重新创建。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- 被 `TaskCleaner` 强制终止的任务运行未被归档，导致其从 `information_schema.task_runs` 中消失且无迹可寻。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` 和 `SWAP TABLE`/`SWAP MATERIALIZED VIEW` 仅持有密集表锁而非数据库写锁，导致并发读取者可能观察到不完整的名称到表映射中间状态。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- 主键索引 Compaction 输出的 sstable 在打开时未包含 tablet 元数据，导致永久性的 `metadata is null when loading delvec` 失败。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 在显式事务中，针对同一事务内已修改的表执行部分列更新 `INSERT` 会在 `COMMIT` 处静默损坏数据。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- 与 range 分布不兼容的 `ALTER TABLE` 操作（schema change、排序键变更）现在会被拒绝并返回可操作的错误信息，而不再静默损坏元数据。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- 优化器中子节点类型不匹配的聚合函数会导致查询结果不正确。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- `ALTER ROUTINE LOAD` 使用保留关键字作为表名时会写入无法解析的 `origStmt`，导致 FE 重启后列映射丢失。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` 兼容性检查未递归处理嵌套类型（例如 `ARRAY<VARCHAR>`），导致 `CREATE MATERIALIZED VIEW` 对 `ARRAY_AGG` IMV 失败。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- 当扫描范围被完全过滤掉时，Parquet 临时字典编码列会泄漏到上层，导致下游类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `CASE WHEN` 在混合浮点数和整数的 `WHEN` 及结果类型时会生成无效的 JIT IR，导致结果错误或崩溃。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT 编译失败导致 `LLVMContext` 出现悬空使用（use-after-free），从而引发 SIGSEGV。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- 后台统计信息任务覆盖了会话 `WAREHOUSE` 设置，影响了同一连接上下文中后续的用户查询。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- 当从未成功完成过集群快照时，`CatalogRecycleBin` 会停止清除条目，导致在高负载 `INSERT OVERWRITE` 工作负载下 FE 内存无限增长。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- FE 未能检测到非主键副本的版本空洞；查询被永久路由到存在空洞的副本上，导致 `max_version` 被冻结。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（一个在共享读锁下被修改的 `HashSet`）存在数据竞争，可能导致条目丢失或集合损坏。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 当保留边界元数据已被清理时，Vacuum 水位线未被正确报告，导致 `file_bundling` 切换版本清理停滞。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake Vacuum 重试使用了确定性的指数退避；现已加入去相关抖动（decorrelated jitter），以便在 S3 限流下将重试分散到各个 CN 上。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- `OlapTableSink` 中的内存统计被虚高计算，原因是从查询内存池分配的 RPC 请求在进程上下文中被释放。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 当自动分区创建与 `_incremental_open_node_channel` 并发触发时，`TabletSinkSender::_send_chunk_by_node` 中存在竞态条件。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 通过回移上游变更创建的 UDAF 上下文会通过 `unique_ptr::release` 导致内存泄漏。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- 由于 `append_selective` 中内存统计不准确，分区连接探测（partitioned join probe）可能出现越界访问。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 配置字段的名称存在拼写错误。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` 错误地将 range-colocate PACK shard 组当作孤儿进行回收，从而在存算分离模式下永久删除了活跃的 shard。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- 对于 PRIMARY KEY 表以及没有显式 `ORDER BY` 的表，colocate tablet 拆分的排序键个数是根据基础 schema 而非物化 schema 解析的，导致拆分任务完成后并未减小 tablet 大小。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 当分区数据低于合并阈值时，自动合并守护进程会将预拆分的 tablet 重新合并，从而抵消了基于采样的预拆分带来的并行度收益。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 执行 `RESTORE ... AS <new_db>` 后，Follower FE 上缺失了库级 UDF，原因是该函数的 `FunctionName.db` 仍指向源数据库。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- 当仓库拥有多个 CN 组时，在存算分离模式的 `DISTRIBUTED BY RANDOM` CTAS/INSERT 中，为不可变分区的 tablet 位置分配了错误的 CN 组。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 当在统计信息估算期间并发删除分区时，`StatisticsCalcUtils` 中出现 `NullPointerException`。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` 和 `FrontendServiceImpl` 元数据 RPC 处理程序持有完整的数据库 READ 锁，从而阻塞了无关表上的 DDL 操作。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- Pipeline Operator 在切换其完成状态时未通知共享上下文的观察者，可能导致对等 driver 在事件调度器下停滞。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 谓词下推中的非根复合谓词会产生 `NotPushDown` 而非扫描级别的 EOF，导致在 UNION 下存在不可能的嵌套 AND 分支时，`OlapScanNode` 不返回任何行。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` 在只有单一存储介质的 BE 上执行了代价高昂的逐副本扫描；现在对于磁盘同构的 BE，该检查已实现 O(1) 复杂度。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- 数据目录加载线程存在线程名设置竞争，导致每次 BE 启动时都会出现噪声 `failed to set thread name` 警告。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- 任务管理器写入了一条非法的 `RUNNING→RUNNING` 编辑日志，导致任务运行在运行中映射表中一直显示为卡住状态。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK 多语句批量事务未能在复合 rowset 中累加 `num_rows`、`data_size` 和 `num_dels`，导致存算分离主键表上的行数统计不正确。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake 导入溢出清理现在使用基于事务 ID 的 vacuum 驱动回收机制，避免 BE 崩溃或 OOM 后出现孤立的溢出文件。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- 从 `0000` 年开始的 PostgreSQL JDBC 时间值会导致类型映射结果不正确。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- 在 schema change 期间从 rowset 中读取 `gtid` 之前缺少空值检查，导致 NPE 崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {#411}

发布日期：2026 年 5 月 29 日

### 行为变更 {#behavior-changes-3}

- Hive 连接器现在默认使用原生 C++ Avro 扫描器，而不是 JNI Avro 扫描器。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 现已禁用对 INCREMENTAL/AUTO 物化视图的查询改写，并且对 INCREMENTAL/AUTO 物化视图拒绝 FORCE 刷新和分区刷新。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 功能优化 {#improvements-3}

- Java UDF/UDAF/UDTF 现在支持更多类型：UDAF/UDTF 的 STRUCT 参数和返回值、嵌套的 ARRAY/MAP 类型、DATE/DATETIME、DECIMAL 以及可变参数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- 标量 UDF 现在支持 STRUCT 参数。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF 现在支持嵌套的 ARRAY/MAP 类型。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF 现在只加载和初始化一次，并在多次查询间复用，从而降低每次查询的开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- 为 Hive 连接器将 JNI Avro 扫描器替换为原生 C++ 扫描器，支持直接二进制解码，并支持 `avro.schema.literal` 和 `avro.schema.url`。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 在 CTAS 语句中支持 Trino 的 `WITH` 子句。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- 完成了在写入路径上对 Iceberg `timestamptz` 分区转换的支持。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- 为 Iceberg 表聚合启用了 TopN 运行时过滤器下推。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- 支持 Iceberg 日期时间最小值/最大值优化。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 允许在 Catalog 和 BE 中透传 HDFS HA 配置，以支持访问多个 HDFS 集群。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 为外部表查询增加了分区扫描数量限制。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- 对不支持的 Iceberg V3 特性快速失败。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- 通过 INSERT INTO FILES 支持在 CSV 导出中使用 `csv.enclose` 和 `csv.escape`。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 添加了 `enable_push_down_schema` INSERT 属性，用于将完整表结构下推到 `files()`。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load 作业现在会在遇到不可重试错误时（例如超出主键大小限制）暂停。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 支持对来自两个子节点的复杂表达式进行连接重排序。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- 改进了 CBO 统计信息估算，包括针对 `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION 以及常量的 MCV/空值比例传播。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- 改进了数据倾斜连接检测：仅当所有连接键都存在倾斜时才检测倾斜，并新增了 `force_group_by_skew_eliminate_when_skewed` 开关以强制启用倾斜规则。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- 支持在 FE 中对 `regexp_replace` 进行常量折叠。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 优化了在具有常量分区值的日期分区列上的 MIN/MAX。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- 引入了 `SCHEDULE` 关键字，作为物化视图刷新中 `ASYNC` 的同义词。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 支持在存算分离模式下对 Lake 表进行 tablet 创建重试。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- 支持对 Lake 列模式部分列更新进行条件更新。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 将部分列更新发布、持久化索引初始化和 SSTable 打开操作并行化，以提升导入吞吐量。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 支持在存算一体到存算分离复制过程中同步 DCG 文件。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- 支持在主键列和非主键列上对 VARCHAR 长度进行加宽的表结构演进。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- 为集群快照完整性检查添加了 `snapshot_meta.json` 标记。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- 支持通过 DN 模式进行 LDAP 直接绑定身份验证。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- 新增 `get_query_dump_from_query_id` 元函数，便于查询问题排查。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 支持在审计日志中审计所查询的关系。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- 新增用于 MySQL 二进制结果编码的会话变量。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 新增多项指标以增强可观测性，包括用于存算分离集群的 `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`，以及 Iceberg 元数据表查询指标。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- 新增 FE 配置项 `deploy_serialization_min_thread_pool_size`。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- 新增 `tablet_reshard_enable_tablet_merge` 配置项，用于禁用 MergeTabletJob 的创建。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- 通过 `SO_REUSEPORT` 消除了 HTTP 服务器 accept 惊群效应。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- 支持从 S3 导入 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 新增 `uuid_v7` 函数，用于生成按时间排序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- 新增按 catalog 类型统计的查询指标，以增强 external catalog 的可观测性。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 支持为窗口函数显式指定倾斜（skew）提示，可自动通过拆分为 UNION 来优化具有倾斜分区键的窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### 安全性 {#security-3}

- [CVE] 将 Netty 升级至 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] 将 pgjdbc 升级至 42.7.11（修复客户端因无限制 SCRAM PBKDF2 迭代次数导致的 DoS 问题），并将 BouncyCastle 升级至 1.84（修复 FrodoKEM 私钥泄露问题）。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] 使用 go1.25.9 构建 pprof，以消除 Golang CVE 漏洞。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- 将 jetty-http 升级至 9.4.58.v20250814。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 清理了 Broker 依赖中的 CVE 漏洞并移除了 `wildfly-openssl`。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- 对 INSERT INTO FILES 错误信息中的凭据进行了脱敏处理。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### Bug 修复 {#bug-fixes-3}

修复了以下问题：

- 由于 `hash_util` 静态初始化顺序问题导致 CN 启动时发生段错误（segfault）。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 在启用物理拆分（physical split）的情况下扫描空 tablet 时导致 CN 崩溃。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 查询 `information_schema.warehouse_queries` 时导致 BE 崩溃。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- 当 rowset 的 `num_rows` 为零时，Lake Compaction 中出现 SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG fragment 连接中的除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer 中的优雅退出崩溃问题。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- 可落盘哈希 Join 探测阶段崩溃问题。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 格式化写入临时 `std::string` 时出现栈缓冲区溢出。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` 中的崩溃问题。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 由临时 `shared_ptr` 析构导致的 `LoadChannel::get_load_replica_status` 中的悬空指针使用问题。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- 线程创建失败时 `ThreadPool::do_submit` 中出现的悬空指针使用问题。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- Hive 分区描述符在 fragment 销毁过程中出现悬空指针使用问题。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- Information Schema sink 中的悬空指针使用问题。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- 由于复用 HttpClient 实例导致的 FE 文件描述符泄漏问题。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` 中的 JNI 本地引用泄漏问题。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- 缓存物化视图执行计划上下文时出现内存泄漏。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- 本地数据交换（local exchange）中出现异常的内存过度使用问题。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` 中 `response->tablet_metas` 上的竞态问题。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` 中并发 `SegmentFlushTask` 竞态问题。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- 序列化过程中 `RuntimeProfile` min/max 竞争问题。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- 查询上下文销毁期间 `PipelineTimerTask` 中的竞态条件。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` 中的竞态条件。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` 和 `array_length` 中共享 `NullColumn` 的问题。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- 由分区版本间隙导致的批量发布死锁问题。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 在存算一体模式下预热 rowset 元数据的 LRU 缓存时出现死锁。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` 回滚不是异常安全的，且解锁顺序不正确。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 由于在只读和元数据路径上存在多个数据库锁，导致 DDL 与 StarOS RPC 之间出现锁竞争。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- 由于缺少 project 节点而导致的 shuffle 分布不正确。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN runtime filter `exprOrder` 不匹配，导致崩溃和错误结果。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- 来自 dict-merge GROUP BY 的错误结果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query cache 与本地 shuffle 聚合冲突。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- Flat JSON 中全局字典生成不一致的问题。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON 合并空值不一致问题。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 在声明显式键/值类型时，map 字面量中出现类型不匹配问题。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- 在 JOIN USING transformer 中，COALESCE 子项未被转换为公共类型。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- 在使用全局变量进行 reduce-cast 后，VARCHAR 长度未被保留。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- 在 MySQL 结果集中的嵌套类型内，VARBINARY 被错误编码。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 在小型 LIMIT 上禁用聚合落盘时出现的 check-having-clause 问题。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日期解析前未去除引号，以及 PostgreSQL 日期/时间相关的错误。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- 数据文件共享标志丢失，导致 vacuum 删除了仍被兄弟 split tablet 引用的文件。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→merge 序列中的 tablet 合并正确性问题。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- 在 tablet 拆分期间，跨发布事务日志的 num_rows/data_size 膨胀问题。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 在同一发布批次中，先写入后 Compaction 导致的 Delete Vector 孤儿条目问题。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- 通过同步 StarMgr journal 回放，修复了 follower FE 上出现的 “no queryable replica” 问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 在应用正常 rowset 提交时，`merge_condition` 未被保留。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE 冲突检测使用了不正确的快照 ID 和过滤器。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 在 Iceberg transform 参数无效时出现 NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 由于优化器注入了额外的列，导致 Iceberg min/max 优化被跳过。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- 针对 Iceberg 基表的 aggregate-join-pushdown 物化视图改写问题。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- 在 INSERT OVERWRITE 提交之前缺少 Hive 分区目录。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- AWS assume-role 未应用于 JNI scanner。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- Avro 复杂类型解码在裁剪子字段和嵌套可空 schema 时的问题。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Load 错误中缺少文件/列/行上下文信息。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet scanner 缺乏对 Arrow 字典值的支持。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon 表的主键未在 SHOW CREATE 中显示，且 DESC 返回结果有误。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC 类型兼容性以及带有尾随斜杠的 JDBC URL 构造问题。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- 在 JDBC catalog 中，针对 SQL Server 表的物化视图刷新问题。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 针对基于 outer join 的物化视图，存在延迟物化 slot 可空性问题。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO 和 INCREMENTAL 物化视图分区刷新被拒绝的问题。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- 物化视图变为 inactive 状态后，其调度器未被停止。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 缺乏对 `SHOW GRANTS FOR CURRENT_USER()` 的支持，导致 MySQL 客户端兼容性问题。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW 语句在显式事务内不被允许。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flight 在返回空结果集时返回了列名 `r`。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF 代码中缺乏 JNI 异常处理检查。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 函数注册问题。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- 使用 `enable_load_profile` 时的 Stream Load 性能剖析（profile）收集问题。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- Profile 中的 START_TIME/END_TIME 未按会话时区显示。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` 不可在运行时修改。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` 在等值谓词中未对特殊字符进行转义。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- 在错误处理路径上并行加载 segment/rowset 时出现 use-after-free 问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 聚合 spill `set_finishing` 中存在潜在的哈希表数据丢失问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 磁盘重新迁移（A→B→A）过程中因 GC 竞争导致 PK tablet 的 rowset 元数据丢失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` 中存在数据库读锁泄漏问题。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM 刷新记录的 PCT 分区元数据不完整。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 当被引用的列缺失时，在 Stream Load/Broker Load 中分析生成列会出现 NPE。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短路点查（point lookup）中缺少分区谓词。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {#410}

发布日期：2026 年 4 月 13 日

### 存算分离架构 {#shared-data-architecture}

- **全新的多租户数据管理**

  存算分离集群现已支持基于范围的数据分布，以及 tablet 的自动分裂与合并。当 tablet 变得过大或出现热点时，可自动进行分裂，无需 schema change、SQL 修改或数据重新导入。此特性可显著提升易用性，直接解决多租户工作负载中的数据倾斜与热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 Tablet 支持（第一阶段）**

  使存算分离集群能够在每个 tablet 中承载显著更多的数据，长期目标是每个 tablet 达到 100 GB。第一阶段在整个导入、主键更新以及 Compaction 流程中引入了 tablet 内并行处理，使单个 Lake tablet 在数据量增长时不再成为单线程瓶颈。改进内容包括单个 tablet 内的并行 Compaction（支持 segment 级拆分）、Lake 导入的并行 MemTable finalize、flush 和 merge（包括 load-spill 路径）、主键表的 tablet 内并行发布及并行条件更新，以及针对云原生主键索引的范围拆分/并行/分级 Compaction（支持远端存储 mapper 文件）。这些改动共同大幅降低了大 tablet 工作负载下的导入内存开销、Compaction 放大以及 FE 元数据压力。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **快速 Schema 变更 V2**

  存算分离集群现已支持快速 Schema 变更 V2，该特性可实现 schema 操作的秒级 DDL 执行，并进一步将支持范围扩展到了物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 存算分离下的倒排索引**

  为存算分离集群启用内置倒排索引，以加速文本过滤与全文检索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  查询级别的缓存命中率现已在审计日志和监控系统中展示，以提升缓存透明度并便于延迟诊断。新增的 Data Cache 指标包括内存和磁盘配额使用情况，以及 page cache 统计信息。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表新增了 segment 元数据过滤功能，可在扫描时根据排序键范围跳过无关 segment，从而降低范围谓词查询的 I/O 开销。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- 支持 Lake DeltaWriter 的快速取消操作，降低了存算分离集群中被取消导入作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 新增对自动化集群快照按时间间隔调度的支持。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- 支持 MemTable flush 和 merge 的流水线（pipeline）执行方式，提升了存算分离集群中存算分离表的导入吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- 支持以 `dry_run` 模式修复存算分离表，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 为存算一体集群的事务发布新增了线程池，提升了发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### 数据湖分析 {#data-lake-analytics}

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入 position delete 文件，从而可以直接在 StarRocks 中对 Iceberg 表执行 DELETE 操作。该支持覆盖了 Plan、Sink、Commit 和 Audit 的完整流程。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **面向 Hive 和 Iceberg 表的 TRUNCATE**

  支持在外部 Hive 和 Iceberg 表上使用 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 增量物化视图**

  将增量物化视图刷新的支持扩展到 Iceberg 仅追加表，无需全表刷新即可实现查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg 中半结构化数据的 VARIANT 类型**

  在 Iceberg Catalog 中支持 VARIANT 数据类型，用于灵活的读时模式存储和半结构化数据查询。支持读取、写入、类型转换以及 Parquet 集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增对 Iceberg v3 默认值特性和行血缘（row lineage）的支持。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增对 `rewrite_manifests` 过程的支持，并扩展了 `expire_snapshots` 和 `remove_orphan_files` 过程的参数，以实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- 支持从 Iceberg 表中读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- 支持从 Iceberg v3 表中读取 `_row_id`，并支持 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- 支持创建带有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 的输出中显示属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 支持查询具有特定分支、标签、版本或时间戳的 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- 支持 Paimon 表的复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- 在创建 Iceberg 表时支持带括号语法的分区转换（Partition Transforms）。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- 支持基于 Transform Partition 的 Iceberg 全局 shuffle，以改善数据组织。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- 支持为 Iceberg 表 sink 动态启用全局 shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- 为 Iceberg 表 sink 引入了 Commit 队列，以避免并发 Commit 冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- 为 Iceberg 表 sink 新增了主机级排序，以改善数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- 默认在 ETL 执行模式下启用了额外的优化，无需显式配置即可提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 以及类似批量操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- 为 Iceberg 表上的 INSERT 和 DELETE 操作新增了提交审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- 支持在 Iceberg REST Catalog 中启用或禁用视图端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- 支持在各种 Iceberg catalog 类型上使用 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- 支持 AWS Glue Catalog 表中的分区投影（partition projection）。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- 为 AWS Glue `GetDatabases` API 新增了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- 支持带有端点注入（`azblob`/`adls2`）的 Azure ABFS/WASB 路径映射。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- 为 JDBC catalog 新增了数据库元数据缓存，以降低远程 RPC 开销和外部系统故障的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- 在 `information_schema` 中支持 PostgreSQL 表的列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- 改进了 Oracle 和 PostgreSQL 的 JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎 {#query-engine}

- **递归 CTE**

  支持递归公共表表达式（CTE），用于分层遍历、图查询和迭代 SQL 计算。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 重写，支持基于统计信息的倾斜检测、直方图支持以及 NULL 倾斜感知。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- 改进了窗口上的 COUNT DISTINCT，并新增了对融合多重去重聚合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- 支持窗口函数的显式倾斜提示（skew hint），通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- 在 Trino Parser 中为 INSERT 语句支持 EXPLAIN 和 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- 支持 EXPLAIN 用于查询队列可见性。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数和 SQL 语法 {#functions-and-sql-syntax}

- 新增以下函数：
  - `array_top_n`：按值返回数组中排名前 N 的元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：将多个数组按元素合并为一个结构体数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：格式化带缩进的 JSON 字符串。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：在 JSON 字符串中的指定路径设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：对具有相同键的行的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：返回当前 warehouse 的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：从 SQL 调用外部 AI 模型以执行推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：在 SQL 表达式中抛出用户自定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 提供以下函数或语法扩展：
  - 支持在 `array_sort` 中使用 lambda 比较器实现自定义排序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持在 FULL OUTER JOIN 中使用符合 SQL 标准语义的 USING 子句。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持在带有 ORDER BY/PARTITION BY 的窗口函数中进行 DISTINCT 聚合。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 支持在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中使用 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 支持类似 count distinct 的聚合函数使用 VARBINARY。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持在 IN 表达式中进行日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 支持 BEGIN/START TRANSACTION 使用 WITH LABEL 语法。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 支持在 SHOW 语句中使用 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持使用 `ALTER TASK` 语句进行任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持为 CSV 文件导出使用多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式，用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理与可观测性 {#management--observability}

- 资源组支持 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改善多 warehouse 场景下的 CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图，用于查看 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest Blacklist，可在集群级别屏蔽特定的查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法直接访问的节点进行 Arrow Flight 数据检索。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，无需重新连接即可将全局变量的更改传播到现有连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 新增内置 UI 功能，用于分析 Query Profile 并查看格式化的 SQL，使查询调优更加便捷。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 接口，用于提供结构化的集群概览。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增全局只读系统变量 `@@run_mode`，用于查询当前集群的运行模式（存算分离或存算一体）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2`，以改善查询队列管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持为 Stream Load 和 Merge Commit 操作设置用户级别的默认 warehouse。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 新增 `skip_black_list` 会话变量，可在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为 metrics API 新增 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 新增 impersonate 用户支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 新增 `table_query_timeout` 作为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 支持添加 FE observer 节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 支持在 `information_schema.loads` 中显示 Merge Commit 信息，以提升导入作业的可见性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在存算分离表中显示 tablet 状态，以便更好地进行故障排查。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### 安全性 {#security-4}

- [CVE-2026-33870] [CVE-2026-33871] 替换了 AWS bundle 并将 Netty 升级到 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级到 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级到 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### 问题修复 {#bug-fixes-4}

修复了以下问题：

- 通过跳过 range 分布 tablet 的数据文件删除，修复了 tablet 拆分后的数据丢失问题。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 在处理复杂类型时的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了由 `BatchUnit` 和 `FetchTaskContext` 之间的 `shared_ptr` 循环引起的内存泄漏问题。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了 SystemMetrics 中因并发 getline 访问导致的双重释放崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在 eager merge 消耗所有块时发生崩溃的问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了自动创建的分区被 TTL 清理器删除时出现 NPE 的问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中出现 NPE 的问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了带有常量侧列引用的外连接谓词改写不正确的问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了在存算分离模式下修改 CHAR 列长度后查询结果错误的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了在多表情况下物化视图刷新的错误。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新时物化视图回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步物化视图中全空值处理的错误。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了在快速 schema change ADD COLUMN 后查询物化视图时出现重复列 id 错误的问题。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了由共享 DecodeInfo 引起的低基数改写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数 JOIN 谓词类型不匹配的问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了当 `null_counts` 为空时，Parquet Page Index Filter 出现 Segfault 的问题。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 在相同路径上展平数组和对象发生冲突的问题。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制的问题。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性的问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB FileSystem 缓存键未包含 container 的问题。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池已满时发生死锁的问题。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon catalog 刷新时在 ObjectTable 上因 ClassCastException 导致崩溃的问题。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了 FULL OUTER JOIN USING 在常量子查询情况下的问题。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了在 CTE 作用域下 JOIN ON 子句的错误。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 通过使用 bindScope() 模式，修复了 ConnectContext 内存泄漏问题。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了存算一体集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏问题。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift 接收线程在遇到任何异常时退出的问题。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了 Routine Load 列映射中 UDF 解析的问题。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了当 dict page 过大时扫描结果出错的问题。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了 range 分区重叠的问题。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了查询队列分配时间和等待超时的问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了处理 null 字面量数组时 `array_map` 崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 的堆栈溢出问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了 LDAP 身份验证中用户名大小写不敏感规范化的问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计和 SQL 脱敏中隐藏了用户身份验证字符串。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更 {#behavior-changes-4}

- ETL 执行模式优化现在默认启用。这使 INSERT INTO SELECT、CREATE TABLE AS SELECT 以及类似的批处理工作负载无需显式配置更改即可受益。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除支持常量值外还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：USING 列在输出中只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` 现在默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 默认情况下，SQL 事务受会话变量 `enable_sql_transaction` 控制。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
