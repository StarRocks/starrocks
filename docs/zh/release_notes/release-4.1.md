---
displayed_sidebar: docs
description: "StarRocks 4.1 版本发布说明：多租户基于范围的 tablet 自动分裂、大容量 tablet 支持（目标 100 GB）、Fast Schema Evolution V2 等新特性……"
---

# StarRocks 版本 4.1

:::danger

**容器镜像问题（v4.1.0）**

由于 v4.1.0 容器镜像中存在不稳定的加载顺序问题，BE 进程在容器环境中可能无法可靠启动。**容器环境用户请勿升级至 v4.1.0。**请等待 v4.1.1，该版本包含修复（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**降级说明**

- 将 StarRocks 升级至 v4.1 后，请勿降级至任何低于 v4.0.6 的 v4.0 版本。

  由于 v4.1 中引入的数据布局内部变更（与 tablet 拆分和分布机制相关），升级至 v4.1 的集群可能会生成与早期版本不完全兼容的元数据和存储结构。因此，从 v4.1 降级仅支持降至 v4.0.6 或更高版本，不支持降级至 v4.0.6 之前的版本。此限制源于早期版本在解析 tablet 布局和分布元数据时的向后兼容性约束。

:::

## 4.1.3

发布日期：2026 年 7 月 14 日

### 行为变更

- `CTAS` 现在会保留显式声明的 `VARCHAR(N)` 列长度，而不是将其扩展为 `VARCHAR(MAX)`。现有表不受影响；使用 `CTAS` 创建的新表将在后续写入时强制执行声明的长度。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- 在没有 `OPERATE ON SYSTEM` 权限的情况下查询 `sys.fe_memory_usage` 或 `sys.fe_locks`，现在会返回明确的拒绝访问错误，而不是误导性的节点查找失败。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` 和 broker/stream load 不再对以 `isAdjustedToUTC=false` 写入的 `INT64` Parquet 时间戳应用会话时区偏移；这些时间戳现在被视为挂钟时间值并按原样加载。在 v4.1.3 之前从此类文件加载的数据可能与升级后加载的数据不同；如需保持一致性，请重新加载。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 成功提交的多表事务 stream load 作业现在在 `information_schema.loads` 和 `SHOW STREAM LOAD` 中正确显示为 `VISIBLE`，而不是停留在 `PREPARING` 状态。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- Connector 增量扫描范围调度现在始终复用已部署 fragment 的 driver 布局，防止扫描范围被错误分配给不存在的 driver。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` 常量折叠现在与 MySQL 8 的反斜杠转义语义保持一致，修正了诸如 `'a\\\\b'` 等模式之前返回相反结果的情况。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load 现在支持 `property.kafka_partition_discovery` 属性，该属性允许在指定 `kafka_partitions` 和 `kafka_offsets` 以设置精确起始偏移量时，分区自动发现仍可继续进行。当未设置 `property.kafka_default_offsets` 时，在作业已有消费进度后新发现分区的默认起始偏移量从 `OFFSET_END` 变更为 `OFFSET_BEGINNING`——此变更适用于**所有**自动发现作业，而不仅限于使用新属性的作业。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非 group-by 聚合现在在合并之前通过 `UNION ALL` 分支下推，从而减少对 union 进行聚合查询时的网络传输和内存使用。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM 维护查询现在在每次刷新时从当前视图定义重新推导，而不是使用在 `CREATE` 时存储的冻结查询文本；现有 MV 无需重新创建即可自动受益于重写器的错误修复。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- 基于采样的 tablet 预拆分现在将预拆分分片分散到所有计算节点（`SPREAD` 放置策略），而不是将其集中到源 tablet 的 worker 上（`PACK` 放置策略），从而提升加载并行度。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 仅更改列注释的 `ALTER TABLE ... MODIFY COLUMN` 现在采用轻量级纯元数据路径，而不是启动完整的 schema 变更作业，并且此功能现在也适用于主键列。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` 和 `CEIL` 现在被视为非保留关键字，可以不加引号直接用作列名。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` 的输出现在始终在 UDF 和 UDAF 的 Properties 列中包含 `isolation` 属性（`shared` 或 `isolated`）。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` 的默认值从 100 提升至 200，通过在每次 `DeleteObjects` 请求中批量处理更多过期文件删除操作，提高了对象存储上的 vacuum 吞吐量。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- 使用授权凭证的 Iceberg REST catalog 表现在会被缓存，其凭证在后台刷新，从而消除了每次 `getTable()` 操作都调用 `GetDataAccess` 的情况，避免了 AWS Lake Formation 的速率限制。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM 的 `bitmap_union`、`hll_union` 和 `percentile_union` 聚合状态现在在物化视图中只存储一次，而非两次（可见列 + 隐藏的 `__AGG_STATE_` 列），将这些 sketch 类型的存储空间减少了一半。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 增量物化视图现在支持 `bitmap_agg`、`hll_union`、`percentile_union` 和 `bitmap_union` 聚合函数，使精确去重计数和基于 sketch 的聚合能够以增量方式维护。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- 基于采样的 tablet 预分裂数量现在会向上取整至当前活跃计算节点数的最近整数倍，以实现均匀分布，并通过最小 tablet 大小下界进行约束，避免小数据量加载时产生过度碎片化。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改进

- `ngram_search` 函数现在接受非常量的 needle 参数。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- 新增了由 `enable_http_auth` FE 配置项控制的 HTTP 认证框架，对所有外部 HTTP 端点进行认证和 RBAC 权限管控。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- 在 `information_schema.materialized_views` 中新增了刷新和放置可观测性列（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- 新增了在 journal 回放时对外部统计信息缓存进行按需懒加载刷新的选项，由新的 FE 配置项控制，以防止缓慢或卡死的外部元数据存储阻塞 FE journal 回放或启动。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- `VARCHAR` 长度增加现在可通过快速 schema 演进在范围分布（共享数据）排序键列上执行，无需重写数据。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 新增了当共享数据事务日志写入超过可配置阈值时的堆栈跟踪转储功能，使缓慢的 `put_txn_log` / `put_combined_txn_log` 调用更易于诊断。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- Tablet 预分裂元数据层 footer 读取器现在支持 `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR` 以及 ORC `TIMESTAMP` 排序键，减少了必须回退到数据层采样的加载次数。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- 基于样本的 tablet 预分裂现在适用于 `INSERT INTO ... SELECT ... FROM <OLAP table>` 加载，以及包含所有排序键列的列列表 `INSERT` 语句。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 为共享数据 tablet 元数据和事务日志文件添加了 Adler-32 校验和保护，使读取时能够检测到静默损坏。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- 新增了每个数据库的 `txn_max_committed_pending_publish_ms` FE 指标，报告最旧的已提交但尚未发布的事务的时间，以帮助检测版本发布停滞。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- Tablet 分裂/合并现在从发布版本响应中实时触发，减少了加载完成与自动分裂/合并启动之间的延迟。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- 通过将无 SST 条件合并任务路由到 `pk_index_execution` 线程池，优化了湖仓主键表的条件更新比较阶段。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- 将湖仓 schema 变更和 rollup 作业锁的范围从整个数据库缩小到表级别，减少了同一数据库中其他表并发操作时的锁竞争。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 在 shared-nothing 模式下，将多个数据库级写锁缩小为表级密集写锁，减少了 BE 报告回调和冷却操作期间的锁竞争。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load 现在支持原生 `MAP` 和 `STRUCT` 目标列。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- Range-colocate tablet 稳定性门控现在在将组标记为稳定之前等待 StarOS 放置收敛，确保 colocate join 实现主机本地执行。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 改进了外部表的 CBO 统计信息：优化器现在无需完整文件枚举即可从 Iceberg manifest 估算行数，修正了 Parquet/ORC 压缩下 Hive/Hudi 行数低估问题，为 JDBC 连接器添加了异步行数统计，并在 Puffin 统计不可用时为 Iceberg 和外部连接器提供 NDV 估算回退。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg manifest 列统计信息现在仅针对聚簇列进行选择性缓存，减少了具有大量数据文件的宽表的 FE 堆内存消耗。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部表统计信息收集现在支持跨 FE 重启和 HA 故障转移的持久化谓词列跟踪，使 auto-ANALYZE 能够针对正确的列。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- 新增了结构化 `[ExternalStats]` 日志行，涵盖外部表统计信息收集从调度到执行的完整生命周期。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` 现在在外部表统计作业的 Properties 列中包含分区、列和快照元数据。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 每个外部表的统计信息来源（`TABLE_METADATA`、`ANALYZE` 或 `NONE`）现在在查询运行时 profile 中公开。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- 新增对 Iceberg 和 Delta Lake 外部表的分区过滤要求和分区数量限制的支持（此前仅适用于 Hive、Hudi 和 Paimon）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- 在 `TABLE SAMPLE` 和直方图 `ANALYZE` 中支持低于 1% 的采样比例，修复了在大表上计算比例截断为零导致的失败问题。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- 新增 `jemalloc_conf` BE 配置项，使 jemalloc 运行时选项可通过 `information_schema.be_configs` 查看。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 新增 `compaction_chunk_reset_memory_tracker_threshold_percent` BE 配置，通过释放保留的 chunk 容量，降低 shared-nothing 模式下主键压缩时的内存占用。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- 将 staros 升级至 v4.1.1，包括重启后持久化 `datacache.enable`、每 worker 组分片预热超时覆盖，以及改进的 S3 重试抖动。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- 通过在 SQL 字符串中不含凭据标记时跳过正则扫描，优化了审计热路径上的 SQL 凭据脱敏性能。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet 扫描器的表达式驱动按需懒加载列功能，减少了多分支 `OR` 查询中不必要的 I/O。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` 现在通过使用复合估算器替代依赖顺序的 HIP 估算器，产生稳定的基数估计结果。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### 安全

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级至 4.1.135.Final，修复 SNI 处理器堆耗尽（DoS）、IPv6 子网过滤器绕过以及 DNS 缓存投毒问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 将 `jackson-databind` 升级至 2.21.4，修复两个反序列化漏洞。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 从 Hadoop 传递依赖中排除 `org.jline:jline-remote-telnet`，以修复未经身份验证的 Telnet 服务器 DoS 漏洞。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] 更新 pprof 预构建文件，修复 pprof 二进制文件中的漏洞。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 修复了 `information_schema.task_runs` 中的 SQL 注入问题，该问题导致谓词值中的单引号可能逃逸字面量边界。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key` 和 `iceberg.catalog.jdbc.password` 现在在 `SHOW CREATE CATALOG` 输出中被脱敏处理。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 修复了 `url_decode` 在输入以截断的百分号转义序列结尾时发生的越界读取问题。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- 修复了 `HyperLogLog::deserialize` 接受超出范围的 `SPARSE` 寄存器索引的问题，该问题可能在输入格式错误时导致堆内存损坏并使 BE 崩溃。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- 修复了 `bar()` 拒绝负宽度值的问题，此前该问题允许字符串无限增长并导致 BE 内存耗尽。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### Bug 修复

以下问题已修复：

- `add_files` 使用 Parquet 物理编码字节而非逻辑类型值填充 Iceberg 文件边界，导致文件级别的最小/最大值剪枝不正确（例如，在 `DECIMAL` 列上）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` 在遍历输入列表以不可变 `List.of(...)` 构建的计划节点时抛出 `UnsupportedOperationException`。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` 两阶段重新规划可能从第一次规划会话中产生过时的 lambda 参数列引用 ID，导致 `expr_type does not match slot_type` 错误。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- 对带有 GIN（倒排）索引的表进行部分更新时，若更新中省略了 GIN 索引列，查询会无限期挂起或失败。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- 当行集模式与 tablet 模式之间发生模式漂移时，Lake PCU（部分列更新）会崩溃或静默损坏数据。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 对外部 Iceberg 表执行包含多个模式子句的组合 `ALTER TABLE` 时，每次子句分发都会错误地重新执行所有先前排队的操作。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- 在分区刷新与资源组取消交错执行期间，当 `num_rows` 快照超过实际 chunk 行数时，`PartitionedSpillerWriter` 崩溃并抛出 `SIGSEGV`。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- BE 进程在启动期间（通常在部署后立即）可能意外退出，原因是 BE 信号初始化中未忽略 SIGPIPE。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 由于行范围过滤跳过了结构体 VARCHAR 子字段填充，Parquet 临时字典编码列泄漏到上层，导致类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` 在审计日志中记录了 `ReturnRows=0`，而非实际导出的行数。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- `TabletChecker.doCheck()` 由于锁类型不匹配，在 `blockingAddTabletCtxToScheduler` 中抛出 `IllegalMonitorStateException`，导致整个检查轮次静默中止。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` 对 `DATETIME_PRECISION` 始终返回 `NULL`，导致从该字段推导列大小的 MySQL 协议客户端出现异常。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- 当查询跨不同数据库或目录连接两个具有相同非限定名称的表时，MV 刷新失败并报 `Duplicate key`。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 可溢出哈希连接探测在某些条件下崩溃。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 当宽度或桶数参数为零时，Iceberg `truncate` 和 `bucket` 转换函数导致 BE 崩溃并抛出 `SIGFPE`。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 当被除数为 `TYPE_MIN` 且除数为 `-1` 时，`mod()` 和 `pmod()` 导致 BE 崩溃并抛出 `SIGFPE`。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- 当 `bucket_num` 为零或负数时，`histogram()` 导致 BE 崩溃并抛出 `SIGFPE`。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- 当所有输入行均为 `NULL` 时，`encode_fingerprint_sha256` 崩溃并抛出 `SIGSEGV`。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 包含单字符通配符 `_` 的 `LIKE` 模式在通过 GIN 倒排索引求值时返回错误结果。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- 针对 GIN 倒排索引的纯 AND `MATCH` 查询在目标段为空时返回虚假错误。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene `match_all` 查询返回了不正确的结果；通过升级 CLucene 依赖项解决了该问题。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- 向量索引重写直接在共享表 schema 上注册了一个合成距离列，导致同一表上无关的并发查询出现 `Multiple entries with same key` 错误。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- Join 重排剪枝可能会剪掉仍被扫描谓词引用的列，导致统计估算抛出 `missing statistic of col`。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` 被错误地通过 sum/count 物化视图重写，在存在重复值时静默丢弃 `DISTINCT` 并返回错误结果。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` 抛出了内部 `NullPointerException`，而非清晰的语义错误。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `SHOW CREATE ROUTINE LOAD` 在没有 `COLUMNS TERMINATED BY` 子句的作业中，于第一个加载描述子句之前错误地输出了一个多余的前导逗号。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- 包含 CTE 的 `SECURITY INVOKER` 视图在 CTE 名称被误认为真实表引用时，可能因 NPE 导致权限检查失败。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 当日期/日期时间边界字面量移位溢出可表示范围时（例如 `<= '9999-12-31'`），`ReduceCastRule` 会以 `SemanticException` 中止查询规划。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- 当 join 条件使用 null 安全等于（`<=>`）析取时，`SplitJoinORToUnionRule` 会输出重复行。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 在多表外部查询的并行元数据准备线程间共享的 `Tracers` 在 `enable_profile=true` 下导致了 `IllegalStateException`。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 中的分区消费者错误被静默丢弃，导致分区 TopN 在不报错的情况下返回部分或错误结果。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE vacuum 任务在 FE 调用方超时后仍以僵尸进程形式继续运行，耗尽了 `RELEASE_SNAPSHOT` 线程池并导致 vacuum 吞吐量崩溃。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- 自动 vacuum 竞争可能瞬间计算出比飞行中事务大一的 `minActiveTxnId`，导致 BE 删除仍需使用的合并事务日志，并永久阻塞发布。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- 由于 FE EOS 取消与 BE 第二阶段部署之间存在竞争，查询在成功完成后被错误地标记为已取消。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 当聚合 TopN 运行时过滤器构建键为 `ConstColumn` 时，BE 在 `AggTopNRuntimeFilterUpdaterImpl` 中因 `SIGSEGV` 崩溃。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- 当所有非空数组均为空时，`array_map` / `transform` 静默丢弃 `NULL` 行并返回错误的行数。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 在 JIT 编译表达式中，超过 2^64 的 `LARGEINT` / `DECIMAL128` 字面量被静默截断为 64 位。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- 当最后一个字符具有截断或无效的多字节前导字节且分隔符为空时，UTF-8 字符串函数（`split`、`split_part`、`str_to_map`）会读取超出字符串末尾的内容。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- 即使在 `ALLOW_THROW_EXCEPTION` SQL 模式下，`parse_json()` 也会对格式错误的 JSON 静默返回 `NULL`，而不是使查询失败。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 严格模式下的数值窄化转换对槽数据未定义的 `NULL` 行错误地引发了溢出错误。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 由于负数截断除法余数，1970年前的 Parquet `INT64` 时间戳（含非零亚秒部分）被解码为垃圾值。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970年前的 ORC `TIMESTAMP` 值在加载时丢失了亚秒部分。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC stripe 最小/最大时间戳统计信息对1970年前及亚秒边界的解码不正确，导致数据文件被错误地剪枝。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- 嵌套在 `ARRAY`、`MAP` 或 `STRUCT` 列中的 `INT96` Parquet 时间戳在加载时丢失了一个会话时区偏移量。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet `UINT_32` 值在加载到 `BIGINT` 列时进行了符号扩展而非零扩展，导致高位无符号整数被静默存储为负值。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` 析构函数在销毁 `_pool`（及其 `Expr` 节点）之前先销毁了 `_scanner_ctx`（持有引用这些节点的谓词），从而导致堆释放后使用问题。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- 使用 OpenX SerDe 读取 gzip 压缩的 JSON Hive 外部表时，当多字节 UTF-8 字符跨越 8 MB 解压缩缓冲区边界时，出现 `UTF8_ERROR` 错误。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2 `ListPaths` 在非 HNS 账户上因客户端无条件访问缺失的 JSON 字段而崩溃，报错 `SIGSEGV`。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 当多个 `UNNEST` 算子共享同一输入数组列并消费不同子字段时，`unnest` 会崩溃或返回错误结果。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- 在 `unnest` 执行期间未强制执行 `query_mem_limit`，导致对大数组执行 `unnest` 时 OOM 杀死 BE 而非使查询失败。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- 带有 `RANK` 边界的 `TopN` 在排名限制恰好落在块边界时会丢失一行。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` 之后的列剪枝可能生成空的分析（窗口）算子，导致规划或执行错误。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` 仅在扫描算子上设置了行限制而未设置全局限制，导致在并发情况下对有限子查询执行 `COUNT(*)` 时返回超出预期的行数。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake 主键持久化索引重建在段范围模式下使用了错误的段迭代器位置，导致键范围过滤器应用不正确。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` 在未持有表锁的情况下修改了 `rebuildPindexVersion`；`RestoreJob` 在恢复后仅持有数据库读锁的情况下修改了 MV 基表信息；`FinalizeCreateTableAction` 在迭代器创建过程中传递了数据库级锁。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- 如果在循环中途获取每个数据库锁时抛出异常，`dumpImage` 可能会无限期地占用全局元数据锁。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- 多语句流式加载每个事务泄漏一个 `TxnStateCallbackFactory` 条目，无限增长并最终耗尽 FE 堆内存。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- 直方图统计的 `information_schema.task_runs` 行计数在目录、数据库、表或分区名称较长时可能溢出 `primary_key_limit_size`（128 字节）。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVM 指标发出了无效的 Prometheus `# TYPE` 行（指标名称内含标签集），导致 Prometheus 中止整个抓取。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` 和 `information_schema.partitions_meta` 将所有物理分区的桶数报告为表级默认值，而非共享数据表中每个分区的实际桶数。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` 为共享数据（`CLOUD_NATIVE`）表上的所有汇总索引返回了基表 schema。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` 无操作子句被错误地路由到轻量级注释路径，导致批量 `ALTER TABLE` 语句中出现 `MODIFY COLUMN COMMENT can not be combined with other alter operations` 错误。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` 可能由于 `isKey` / `aggregationType` 规范化不正确，将键列/聚合列误判为仅注释变更。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` 可能提交循环视图定义，导致后续 `SELECT` 抛出 `StackOverflowError`。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `OrderedPartitionExchanger` 在下游消费者修改前一个 chunk 时，`accept()` 仍持有指向该 chunk 的指针，导致堆释放后使用（heap-use-after-free）。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- 当构建侧的槽描述符为非可空但运行时状态为可空时，NLJoin 发生崩溃。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- `CAST(json/variant AS struct)` 在结构体字段名无法解析为 JSON 路径时，于 fragment prepare 阶段导致 BE 崩溃。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- 嵌套字典表达式的字典解码可能在生产者与消费者 fragment 之间产生不兼容的字典翻译，导致运行时出现 `Dict Decode failed` 错误。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- 当 `get_rowset_by_version` 返回 `nullptr` 且 `gtid` 比较置于空值检查之前时，Schema 变更因空指针解引用而崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- 共享数据集群快照在 tablet 分裂/合并后变得无法恢复，原因是快照管理器在决定是否回收父 tablet 元数据时未考虑重新分片任务。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- 文件捆绑 vacuum 错误地将兄弟 tablet 的零行捆绑段标记为非共享，导致其捆绑文件被删除，而其他 tablet 仍在引用该文件。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 共享数据表压缩发布丢弃了在压缩事务开始后变为可见的汇总/同步物化视图索引，导致捆绑文件中缺少这些索引。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- 共享数据持久化索引压缩在 tablet 重新分片期间压缩被丢弃时，错误地删除了直通复用的 SSTable 文件。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` 到可空平铺 JSON 列的 schema 演进在压缩读取路径中导致 `CHECK` 崩溃。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- 对可空列执行 `count_combine` 时，在流式预聚合直通路径中因 `SIGSEGV` 导致 BE 崩溃。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- 由于 JDK 21 中移除了反射式 `DirectByteBuffer` 构造函数查找，Java UDF 在 JDK 21+ 上加载失败。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- 向 Unified catalog（Hive metastore）执行 `CTAS` 始终失败，原因是解析器不支持 `CREATE TABLE AS SELECT` 中的 `ENGINE` 子句。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` 反馈驱动的 join 重建丢失了 `predicateCommonOperators`，导致含公共子表达式复用的计划在 `InputDependenciesChecker` 校验时失败。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- 查询缓存规范化在具有子分区的表上崩溃，出现 `Preconditions.checkState`，原因是在构建版本列表之前，空子分区已被剪枝。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` 静默跳过了以旧版别名存储的会话变量，导致查询转储回放回退到默认值。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- Iceberg `_row_id` 虚拟列对于包含多个 Parquet 行组的数据文件返回了错误的值，原因是行组起始偏移量被重复计算。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE 规划器无法定位目标扫描节点，因为它通过合成表 ID 而非物理表标识进行匹配，导致基础快照 ID 和冲突检测过滤器丢失。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `FragmentContext::set_final_status` 在 `cancel_plan_fragment` RPC 到达时尚未调用 `PipelineExecutorSet::start()` 的情况下，出现 `SIGSEGV` 崩溃。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `QueryContext` 可能在 `FragmentExecutor` 仍在拆除 fragment 时被回收，导致 `ResGuard::reset()` 中出现堆释放后使用错误。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` 未初始化，导致默认构造的 `search()` 解引用了未初始化的指针。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` 微秒值使用 JVM 默认语言环境的数字集进行渲染，在阿拉伯语或波斯语等语言环境下产生非 ASCII 数字，导致 tablet 预分裂中的边界值解析失败。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- 在 tablet 统计信息刷新之前，`INSERT OVERWRITE` 之后分区行数可能以零写入 `_statistics_.column_statistics`，导致优化器折叠分区基数估算。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- 当全局配置禁用首次加载统计信息收集时，`enable_statistic_collect_on_first_load` 表级覆盖无法启用该功能。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- 当 union 分支没有输入行时，`PushDownNonGroupedAggregateBelowUnion` 产生了类型声明为非空但实际可空的输出，导致 BE `CHECK` 失败。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2

发布日期：2026 年 6 月 18 日

### 行为变更

- 连接到用户无权限的数据库时，现在将返回正确的 MySQL 错误包，而不是以 ERROR 2013 关闭连接。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- 对于可通过函数级权限查看函数但不具备创建函数范围权限的用户，`SHOW FUNCTIONS` 现在将 UDF 文件和对象文件路径屏蔽为 `***`。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 从外部目录查询 Hive 视图时，Ranger 行过滤和列掩码策略现在可正确应用。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` 现在可正确保留 `current_timestamp` 生成器表达式。`DESCRIBE` 和 `information_schema` 现在反映的是表达式而非回填时的字面量。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` 加载时过滤不再在会话时区与 UTC+8 不同的集群上偏移过滤边界。加载时间现在在 FE–BE 边界之间以 UTC 纪元毫秒进行交换。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` 会话变量现在可正确应用于 Paimon 扫描分片计算，而不再始终使用默认值。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` 现在默认启用。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive 分区统计信息不再按计时器对每个键自动刷新。分区统计信息现在仅在显式调用 `refreshTable()` 时刷新，从而减少大型分区表上的 HMS 负载。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- 当 Iceberg 或外部 Catalog 基表发生 Schema 漂移（列类型变更、列删除或表删除）时，依赖该基表的物化视图现在将在下次刷新时被标记为非活跃状态，而不是静默地产生 NULL 行或不透明的错误。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg 连接器现在会对 `AND` 复合谓词中可转换的一侧进行部分下推，而不是在仅有一侧可转换时丢弃整个谓词，从而改善分区裁剪和数据跳过效果。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 显式事务 `COMMIT` 现在能正确等待最多 `query_timeout` 秒（而非毫秒）以获取数据库写锁，防止在短暂的并发写入活动下出现虚假的锁超时失败。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM 刷新现在会将严格加载过滤器错误返回给调用方，而不是静默地丢弃被过滤的行。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` 现在能正确排除 NULL 行，与 `COUNT(col)` 语义保持一致。由 `COUNT(<nullable column>)` 支持的增量物化视图此前会物化虚高的计数。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` 现在还会显示由 `ALTER TABLE ... SET (...)` 触发的异步仅元数据变更作业，涉及云原生（共享数据）表上的 `file_bundling` 和 `enable_persistent_index` 等属性。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 创建带有引用聚合函数的 `HAVING` 子句的增量物化视图时，现在会在 `CREATE` 阶段以明确的错误信息失败，而不是在首次刷新时产生内部计划错误。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM 现在支持在增量物化视图中使用 `MIN`/`MAX(DECIMAL)` 聚合函数。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVM 自适应刷新现在能在第一个 delta 特征已超过 `mv_max_rows_per_refresh` 时正确限定 delta 窗口范围，防止在单次任务运行中刷新整个积压数据。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- 仅含 GROUP BY 的增量物化视图（例如 `SELECT k FROM t GROUP BY k`）现在能正确将 `__ROW_ID__` 编码为 VARCHAR，修复了第二次刷新时的崩溃问题。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 功能改进

- 支持 Paimon 视图，包括 `CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC` 以及从外部 Catalog 查询 Paimon 视图。Paimon 视图内部的表引用现在将针对 Paimon Catalog 进行解析，而不是 `default_catalog`。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 支持在 `FILES()` 中使用显式 `schema` 参数，以便在读取存在 Schema 漂移或复杂嵌套类型的文件时进行稳定的 Schema 控制。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` 现在可以跨所有 FE 节点检索查询 Profile 信息，而不仅限于已连接的 FE。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 新增内置函数 `query_id()`，用于返回当前正在执行的查询的 UUID。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 共享数据模式下的 `CREATE`/`ALTER STORAGE VOLUME` 现在会在持久化元数据之前验证存储位置的可访问性（凭证和端点），在配置错误时提前失败。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- 在 BE 中新增了对 AWS S3 凭证的 `WebIdentity` 令牌提供程序支持，与 FE 中已有的 `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` 支持保持一致。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 新增 `ADMIN SKIP COMMITTED TRANSACTION` 命令，用于在共享数据表上的发布因缺少 `txnlog`、丢失 Segment 或远程 I/O 缓慢而被永久阻塞时，解除卡住的 `COMMITTED` 事务。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` 现在会将 `table_name` 谓词下推至 FE，大幅降低单表查找的开销。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- 在 `information_schema` 表中添加了缺失的 MySQL 8 列，以提高与在连接自省期间检查 MySQL 8 模式的 BI 工具和 JDBC 驱动程序的兼容性。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- 添加了 `enable_pipeline_event_scheduler` BE 配置，作为集群范围的终止开关，当设置为 `false` 时，将覆盖每会话变量。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 为统计信息收集添加了可选的宽字符串列隔离功能，以减少在对包含多个宽字符串列的表收集统计信息时每个查询的内存峰值。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- 慢锁日志记录现在支持每事件速率限制和可配置的堆栈捕获控制，以防止在高锁争用下发生 JVM 安全点停顿。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- MV 刷新日志条目现在在前缀中包含数据库名称，使日志行在同一 MV 名称存在于多个模式的多租户部署中可区分。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 配置现在是可变的，可以通过 `ADMIN SET FRONTEND CONFIG` 在运行时切换，无需重启 FE。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- 添加了 `enable_print_load_profile_to_log` FE 配置（默认值为 `false`），用于将负载配置文件（Stream Load、Routine Load、Broker Load 和 Merge-Commit Load）写入 `fe.profile.log`，即使内存存储因查询配置文件突发而被驱逐，也能保留这些配置文件。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` 现在可以在 `JobProperties` 中正确渲染列映射，而不是 Java 对象引用。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` 现在使用表级锁定而非目录级锁定，减少了在具有许多并发活跃表的目录上的刷新序列化延迟。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- 元数据扫描（后台统计信息收集）现在可以优雅地处理 `ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN` 和 `REORDER COLUMN` 模式变更，而不会因变更后的段文件未找到而报错失败。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- 基于采样的 Tablet 预分裂现在支持多分区范围分布表和 Broker Load，无需现有数据层基线即可实现首次加载并行性。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL 结果序列化不再使用每行虚拟分派；每个 Chunk 构建一次类型化列写入器，减少了宽结果集或大结果集的序列化开销。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` 到字符串的类型转换现在直接写入输出缓冲区，消除了每行的堆内存分配。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- 查询统计信息合并路径将 `SpinLock` 替换为无锁并行映射，减少了在大型集群中工作节点发送中间或最终统计信息时的 CPU 使用率。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 聚合哈希映射和哈希集预取现在以 L2 缓存驻留为门控条件，避免了当桶数组适合 L2 时出现 4–9% 的性能回退。预取距离现在可配置。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 针对共享数据主键表轻量压缩发布的流水线化每段 `.lcrm` 读取，减少了顺序对象存储往返次数。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 共享数据模式下的冷 PK 索引重建扫描现在跨段并行化，减少了段读取受远程 I/O 限制时的重建时间。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部查询（统计信息收集、任务运行、MV 刷新）现在在 `SHOW PROC '/current_queries'` 中可见，并可通过 `KILL QUERY` 终止。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- 添加了 Lake Vacuum 批量大小和重试次数 bvar 指标，用于监控 S3 限流并调优 `lake_vacuum_min_batch_delete_size`。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- 新增了 `CatalogRecycleBin` 大小gauge指标，用于在回收站增长对FE堆造成压力之前进行预警。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- 以 `LIST` 分区的表现在会在 `OlapTableSink` 中打开所有分区，而不是应用专为范围分区表设计的最新N启发式策略，从而减少增量打开的RPC开销。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- 支持通过 `FILES()` 或Broker Load将 `LARGE_LIST` 和 `FIXED_SIZE_LIST` Arrow类型加载到JSON列中。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 支持对共享数据表的合并提交（`FRONTEND_STREAMING`）加载进行事务日志与文件捆绑的组合操作，使其与其他加载类型保持一致。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- 新增可变FE配置项 `slow_publish_partition_log_threshold_ms`（默认3000毫秒），用于在不重启FE的情况下控制lake发布阶段细分的警告阈值。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### 安全

- [CVE-2026-43869] 将 `libthrift` 升级至0.23.0，以修复不当的证书主机验证问题。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] 将Apache Tomcat升级至9.0.118，以修复HTTP/2请求头验证问题。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将Netty升级至4.1.135.Final，以修复SNI处理程序堆耗尽（DoS）、IPv6子网过滤器绕过以及DNS缓存投毒问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- 将pprof预构建二进制文件升级至Go 1.25.11，以包含Go标准库的安全修复。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### Bug修复

以下问题已修复：

- 当URL在 `host:port` 模式之外包含 `:` 时，`parse_url()` 返回了错误的主机。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 字典翻译表达式对不满足条件的表达式（例如 `IF(col = '1', NULL, 'ok')`）错误地假设了 `f(null) = null`。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- 事务流式加载使用了默认RPC超时而非用户指定的超时时间，导致提前超时。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- Iceberg等值删除文件中标识列含有NULL值时，由于 `NULL = NULL` 在连接谓词中求值为UNKNOWN，导致未能删除匹配的行。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- 对于含有 `INJECTED` 分区投影列的表，错误信息现在更具描述性，会显示导致问题的具体列。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- 对仅插入ACID Hive表的查询返回了超出预期的行数，原因是插入覆盖操作未被识别。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 在并发读取期间Iceberg元数据条目被固定时，磁盘缓存超出了其配置容量。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 在查询外部catalog时，Paimon主键列被错误地标记为不可为空。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 当 `MultiDistinctByMultiFuncRewriter` 在含有多个 `ARRAY_AGG(DISTINCT <const>)` 输入的查询上重复应用同一规则时，优化器发生超时。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Oracle JDBC 日期谓词在没有 `DATE`/`TIMESTAMP` 关键字的情况下被下推，导致 NLS 格式错误。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- Partition TopN 可能会丢失其子算子所需的输出列。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- 无法在具有分区演化的 Iceberg 表上创建未分区的 MV。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- 在 `information_schema.be_cloud_native_compactions` 中，并行子任务的压缩任务统计信息被覆盖并丢失。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- `SHOW CREATE MATERIALIZED VIEW` 同步 MV 时失败，错误信息为 "Table is not found"。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- Lake 发布多语句事务在模式更改期间发生死锁，当语句 `.log` 文件落在 4 段路径时。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- 排序合并提供程序错误未传播到片段上下文，导致查询静默失败。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- `ConnectorTableId` overflowed from `int` to negative values on long-running follower FEs, causing Iceberg and Hive queries to fail with misleading "Invalid table type" errors. [#73344](https://github.com/StarRocks/starrocks/pull/73344)
- `ALTER TABLE` 带有空优化子句（无分布或分区规范）时，解析不正确，可能在 FE 重放时损坏表的默认分布。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- FE 启动在 ADLS2 共享数据灾难恢复期间失败，因为 `AZURE_PATH_KEY` 未被识别为有效的 `StorageVolumeMgr` 参数。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- 当优化器将嵌套类型的一部分裁剪为 `UNKNOWN_TYPE`，或使用可为空的数组、映射或结构体模式时，Avro 复杂类型解码失败。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- COW 列变更优化导致 `map_apply` 及类似函数崩溃，原因是两个 `NullableColumn` 共享了同一个 `NullColumn` 对象。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- 具有自定义 `LocationProvider` 的 Iceberg 表在 `SELECT` 查询中因 `ClassNotFoundException` 而失败，原因是提供程序在 FE 上被提前实例化。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` 在每次缓存未命中时都会额外执行一次 `getTableComment()` 往返，延长了密集规划阶段的锁持有时间，并阻塞了并发 DDL。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- 嵌套 MV 刷新在嵌套 MV 返回 `NullPointerException` 或 `FULL` 及时性时抛出 `UNKNOWN`。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE worker 在向慢速 MySQL 客户端发送查询结果时被无限期阻塞。结果发送路径现在强制执行写入超时。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- 在将主键表从 V1 编码（shared-nothing）集群复制到 V2 编码（shared-data）集群，或在两个 shared-data 集群之间复制时，PK `.del` 文件未被转码。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 在 `VERSION_INCOMPLETE` 恢复期间，`TabletInvertedIndex` 中积累了重复的副本，原因是在添加活跃副本之前未移除过时的副本引用。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- 共享数据湖复制文件拷贝导致 CN 崩溃，原因是 `REPLICATE_SNAPSHOT` 任务与每个文件的拷贝子任务共享同一线程池。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- `RuntimeProfileParser` 在 BE 以 `.000` 小数后缀格式化单位计数器时抛出了 `NumberFormatException`。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 共享数据主键 tablet 分裂中共享段的物理 rowid 编码不正确，导致 `rss_rowid` 条目错误。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- 混合 range-colocate × hash 分布的 `JOIN` 查询返回了 `Unknown error`，而非有效结果。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` 使用了固定的 UTC+8 格式化器；输出现在遵循会话 `time_zone`。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- 当所有值均为 `NULL` 且列经过可空一元函数路径时，Decimal 类型列的精度丢失，导致下游结果类型损坏。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- 嵌套类型的 JSON 部分追加导致 ASAN 崩溃。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` 角色的权限缓存在 `GRANT`/`REVOKE` 时未被失效，导致过期权限在到期前持续生效。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- `FlatJson` 在子写入器没有追加内容时崩溃。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- 当 MV 本身带有 `HAVING` 谓词时，聚合 MV 改写被错误地应用，可能返回不完整的结果。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 进入并行合并模式时，溢出写入器的 `auto_flush` 标志存在数据竞争，导致在 ARM 上发生意外的段刷新。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- 例行加载调度器在向 BE 发起阻塞 RPC 以获取 Kafka 或 Pulsar 分区元数据时持有每个作业的写锁，导致锁持有时间长达 33.6 秒。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- 当启用 `tablet_sched_disable_colocate_balance` 时，已宕机 BE 上的 Colocate tablet 被错误地报告为 `HEALTHY`。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- 当存在 MISSING（幽灵）副本行时，`ADMIN SHOW REPLICA STATUS` 使 MySQL 结果流失去同步，导致客户端挂起或断开连接。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 在共享数据模式下，每个分区的协调器声明未在每个发送方的 `open` RPC 中重新记录，导致部分发送方在协调器选举中被遗漏，`combined_txn_log` 文件未被写入。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- 在 `_statistics_` 数据库或表被删除后，`_statistics_.pipe_file_list` 内部表未被重新创建。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- 被 `TaskCleaner` 强制终止的任务运行未被归档，导致其从 `information_schema.task_runs` 中消失且无任何记录。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` 和 `SWAP TABLE`/`SWAP MATERIALIZED VIEW` 仅持有表级共享锁而非数据库写锁，允许并发读取者观察到不完整的中间名称到表的映射。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK 索引压缩输出的 sstable 在没有 tablet 元数据的情况下被打开，导致永久性 `metadata is null when loading delvec` 失败。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 在显式事务中，针对同一事务中已修改的表执行部分更新 `INSERT`，会在 `COMMIT` 处静默损坏数据。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- 与范围分布不兼容的 `ALTER TABLE` 操作（Schema 变更、排序键变更）现在将以可操作的错误被拒绝，而不再静默损坏元数据。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- 优化器中聚合函数的子节点类型不匹配导致查询结果不正确。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 使用保留关键字表名的 `ALTER ROUTINE LOAD` 写入了无法解析的 `origStmt`，导致 FE 重启后列映射丢失。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` 兼容性检查未递归进入嵌套类型（例如 `ARRAY<VARCHAR>`），导致 `CREATE MATERIALIZED VIEW` 对 `ARRAY_AGG` IMV 失败。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- 当扫描范围被完全过滤掉时，Parquet 临时字典编码列泄漏到上层，导致下游类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- 混合浮点和整数 `WHEN` 及结果类型的 `CASE WHEN` 生成了无效的 JIT IR，导致结果不正确或崩溃。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT 编译失败导致 `LLVMContext` 释放后使用，引发 SIGSEGV。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- 后台统计任务覆盖了会话 `WAREHOUSE` 设置，影响同一连接上下文中后续的用户查询。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- 当没有任何集群快照成功完成时，`CatalogRecycleBin` 停止清除条目，导致在高 `INSERT OVERWRITE` 负载下 FE 内存无限增长。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- FE 未检测到非主键副本的版本空洞；查询被永久路由到存在空洞且 `max_version` 冻结的副本。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（在共享读锁下被修改的 `HashSet`）上的数据竞争可能导致条目丢失或集合损坏。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 当保留边界元数据已被清理时，Vacuum 水位线未被正确上报，导致 `file_bundling` 切换版本清理停滞。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum 重试使用了确定性指数退避；现已添加去相关抖动，以在 S3 限流时将重试分散到各 CN。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- `OlapTableSink` 中的内存统计被虚高，原因是从查询内存池分配的 RPC 请求在进程上下文中释放。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 当自动分区创建并发触发 `_incremental_open_node_channel` 时，`TabletSinkSender::_send_chunk_by_node` 中存在竞争条件。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 由上游变更回移引入的 UDAF 上下文通过 `unique_ptr::release` 导致内存泄漏。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- 由于 `append_selective` 中内存统计不准确，分区连接探测存在潜在的越界问题。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 配置字段名称存在拼写错误。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` 错误地将范围协同 PACK 分片组识别为孤立组并进行回收，在共享数据模式下永久删除了活跃分片。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- 协同 tablet 分裂的排序键元数从基础 schema 而非物化 schema 中解析（适用于主键表及没有显式 `ORDER BY` 的表），导致分裂任务完成后 tablet 大小未减小。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 当分区数据低于合并阈值时，自动合并守护进程将预分裂的 tablet 重新合并，抵消了基于采样预分裂带来的并行性收益。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 在 `RESTORE ... AS <new_db>` 之后，从属 FE 上缺少数据库级别的 UDF，因为函数的 `FunctionName.db` 仍然指向源数据库。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- 在共享数据 `DISTRIBUTED BY RANDOM` CTAS/INSERT 中，当仓库有多个 CN 组时，不可变分区 tablet 位置被分配了错误的 CN 组。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 在统计估算期间并发删除分区时，`StatisticsCalcUtils` 中出现 `NullPointerException`。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` 和 `FrontendServiceImpl` 元数据 RPC 处理程序持有完整的数据库读锁，阻塞了对无关表的 DDL 操作。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 在事件调度器下，Pipeline 算子在未通知共享上下文观察者的情况下翻转其完成状态，可能导致对等驱动程序停滞。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 谓词下推中的非根复合谓词返回 `NotPushDown` 而非扫描级别的 EOF，导致在 UNION 下存在不可能的嵌套 AND 分支时，`OlapScanNode` 不输出任何行。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` 在只有单一存储介质的 BE 上执行了昂贵的逐副本扫描；对于同构磁盘 BE，该检查现在为 O(1)。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- 数据目录加载线程上的线程名称设置竞争导致每次 BE 启动时出现大量 `failed to set thread name` 警告。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- 任务管理器写入了非法的 `RUNNING→RUNNING` 编辑日志，导致任务运行无限期地显示为卡在运行映射中。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK 多语句批量事务未在复合 rowset 之间累积 `num_rows`、`data_size` 和 `num_dels`，导致共享数据主键表上的行数统计不正确。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake 加载溢出清理现在使用基于事务 ID 的 vacuum 驱动回收，防止 BE 崩溃或 OOM 后出现孤立的溢出文件。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- PostgreSQL JDBC 中从 `0000` 年开始的时间值导致类型映射结果不正确。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- 在 schema 变更期间从 rowset 读取 `gtid` 之前缺少空值检查，导致 NPE 崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1

发布日期：2026 年 5 月 29 日

### 行为变更

- Hive 连接器现在默认使用原生 C++ Avro 扫描器，而非 JNI Avro 扫描器。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 针对 INCREMENTAL/AUTO 物化视图的查询改写现已禁用，并且对 INCREMENTAL/AUTO 物化视图的 FORCE 刷新和分区刷新将被拒绝。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改进

- Java UDF/UDAF/UDTF 现在支持更多类型：UDAF/UDTF 的 STRUCT 参数和返回值、嵌套 ARRAY/MAP 类型、DATE/DATETIME、DECIMAL 以及可变参数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- 标量 UDF 现在支持 STRUCT 参数。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF 现在支持嵌套的 ARRAY/MAP 类型。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF 现在在加载和初始化一次后可跨查询复用，从而降低每次查询的开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- 将 Hive 连接器中的 JNI Avro 扫描器替换为原生 C++ 扫描器，支持直接二进制解码以及 `avro.schema.literal` 和 `avro.schema.url`。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 支持在 CTAS 语句中使用 Trino `WITH` 子句。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- 完成了 Iceberg `timestamptz` 分区转换在写入路径上的支持。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- 为 Iceberg 表聚合启用了 TopN 运行时过滤器下推。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- 支持 Iceberg 日期时间最小值/最大值优化。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 允许在 Catalog 和 BE 中透传 HDFS HA 配置，以支持访问多个 HDFS 集群。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 为外部表查询添加了分区扫描数量限制。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- 对不支持的 Iceberg V3 特性快速失败。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- 支持通过 INSERT INTO FILES 进行 CSV 导出时使用 `csv.enclose` 和 `csv.escape`。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 新增 `enable_push_down_schema` INSERT 属性，用于将完整 schema 下推至 `files()`。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load 作业现在会在遇到不可重试的错误时暂停（例如，主键大小超限）。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 支持对来自两个子节点的复杂表达式进行 Join 重排序。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- 改进了 CBO 统计信息估算，包括针对 `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION 和常量的 MCV/空值比例传播。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- 改进了倾斜连接检测：仅当所有连接键均发生倾斜时才检测到倾斜，并添加了 `force_group_by_skew_eliminate_when_skewed` 开关以强制启用倾斜规则。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- 支持在 FE 中对 `regexp_replace` 进行常量折叠。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 优化了具有常量分区值的日期分区列上的 MIN/MAX 操作。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- 在物化视图刷新中引入 `SCHEDULE` 关键字作为 `ASYNC` 的同义词。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 支持在共享数据模式下为 Lake 表重试创建 tablet。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- 支持 Lake 列模式部分更新的条件更新。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 并行化部分更新发布、持久化索引初始化和 SSTable 打开操作，以提升数据摄取吞吐量。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 支持在非共享存储到共享数据的复制过程中同步 DCG 文件。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- 支持对键列和非键列的 VARCHAR 长度扩展进行 Schema 演进。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- 为集群快照完整性检查添加了 `snapshot_meta.json` 标记。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- 支持通过 DN 模式进行 LDAP 直接绑定认证。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- 添加了 `get_query_dump_from_query_id` 元函数，以便更轻松地进行查询故障排查。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 支持在审计日志中记录被查询的关系。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- 添加了用于 MySQL 二进制结果编码的会话变量。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 添加了用于提升可观测性的指标，包括面向共享数据集群的 `tablet_num`、`MemtableIOSpeed`、`staros_shard_count` 以及 Iceberg 元数据表查询指标。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- 添加了 FE 配置项 `deploy_serialization_min_thread_pool_size`。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- 添加了 `tablet_reshard_enable_tablet_merge` 配置项，用于禁止创建 MergeTabletJob。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- 通过 `SO_REUSEPORT` 消除了 HTTP 服务器 accept 惊群效应。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- 支持从 S3 加载 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 添加了 `uuid_v7` 函数，用于生成按时间排序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- 为外部 Catalog 可观测性添加了按 Catalog 类型划分的查询指标。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 支持为窗口函数指定显式倾斜提示，通过拆分为 UNION 的方式自动优化具有倾斜分区键的窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### 安全

- [CVE] 将 Netty 升级至 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] 将 pgjdbc 升级至 42.7.11（修复客户端因无限制 SCRAM PBKDF2 迭代次数导致的 DoS 问题），将 BouncyCastle 升级至 1.84（修复 FrodoKEM 私钥泄露问题）。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] 使用 go1.25.9 构建 pprof，以消除 Golang 相关 CVE。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- 将 jetty-http 升级至 9.4.58.v20250814。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 清理了 Broker 依赖中的 CVE 并移除了 `wildfly-openssl`。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- 在 INSERT INTO FILES 错误信息中对凭证进行了脱敏处理。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### 问题修复

以下问题已修复：

- 由 `hash_util` 静态初始化顺序引起的 CN 启动时段错误。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 启用物理分裂时，扫描空 tablet 导致 CN 崩溃的问题。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 查询 `information_schema.warehouse_queries` 时 BE 崩溃。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- 当 rowset `num_rows` 为零时，Lake 压缩中出现 SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG 片段连接中出现除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer 中的优雅退出崩溃。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- 可溢出哈希连接探测崩溃。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 格式化到临时 `std::string` 时发生栈缓冲区溢出。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` 中的崩溃。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 由临时 `shared_ptr` 析构引起的 `LoadChannel::get_load_replica_status` 中的释放后使用问题。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- 线程创建失败时 `ThreadPool::do_submit` 中的释放后使用问题。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- 跨片段销毁的 Hive 分区描述符释放后使用问题。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- 信息模式 sink 的释放后使用问题。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- FE 通过复用 HttpClient 实例导致的文件描述符泄漏。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` 中的 JNI 本地引用泄漏。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- 缓存 MV 计划上下文时的内存泄漏。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- 本地交换中意外的内存超用。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` 中 `response->tablet_metas` 上的竞争条件。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` 中并发 `SegmentFlushTask` 竞争。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- 序列化期间 `RuntimeProfile` 最小/最大值竞争。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- 查询上下文销毁期间 `PipelineTimerTask` 中的竞争条件。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` 中的竞争条件。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- 在 `NullColumn`、`map_apply` 和 `array_length` 中共享的问题。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- 由分区版本间隔导致的批量发布死锁。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 在无共享模式下预热行集元数据 LRU 缓存时发生死锁。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` 回滚不是异常安全的，且解锁顺序不正确。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 由只读路径和元数据路径上的多个数据库锁引起的 DDL 与 StarOS RPC 之间的锁竞争。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- 由于缺少投影节点导致的错误 shuffle 分布。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN 运行时过滤器 `exprOrder` 不匹配导致崩溃和错误结果。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY 返回错误结果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- 查询缓存与本地 shuffle 聚合冲突。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- flat JSON 中全局字典生成不一致。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON 合并空值不一致。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 在显式声明键/值类型时，map 字面量中存在类型不匹配。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- 在 JOIN USING 转换器中，COALESCE 的子项未被转换为公共类型。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- 使用全局变量进行 reduce-cast 后，VARCHAR 长度未被保留。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- VARBINARY 在 MySQL 结果集的嵌套类型中编码不正确。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 在小 LIMIT 上禁用聚合溢出时，check-having-clause 存在问题。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日期解析前未去除引号，以及 PostgreSQL 日期/时间 bug。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- 数据文件共享标志丢失，导致 vacuum 删除了仍被兄弟分裂 tablet 引用的文件。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- 分片→压缩→合并序列的平板合并正确性。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- 跨发布事务日志在平板分裂期间的 num_rows/data_size 膨胀问题。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 由同一发布批次中写入先于压缩导致的Delvec孤立条目。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- 通过同步 StarMgr 日志回放，在 follower FE 上出现 "no queryable replica" 问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- `merge_condition` 在应用普通行集提交时不会被保留。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE 冲突检测使用了错误的快照 ID 和过滤器。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 无效的 Iceberg 转换参数导致的 NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 由于规划器注入了额外的列，Iceberg 最小/最大优化被跳过。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- 在 Iceberg 基表上进行聚合连接下推物化视图重写。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITE 提交前缺少 Hive 分区目录。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- AWS assume-role 未应用于 JNI 扫描器。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- 针对已剪枝子节点和嵌套可空模式的 Avro 复杂类型解码。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Load 错误中缺少文件/列/行上下文信息。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet 扫描器缺乏对 Arrow 字典值的支持。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon 表的主键在 SHOW CREATE 中不显示，DESC 返回也不显示。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC 类型兼容性及带尾部斜杠的 JDBC URL 构造。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- 在 JDBC 目录中，SQL Server 表的物化视图刷新问题。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 物化视图在外连接上的延迟物化槽可空性问题。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- 拒绝 AUTO 和 INCREMENTAL 物化视图分区刷新。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- 物化视图变为非活跃状态后，物化视图调度器未停止。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 缺少对 `SHOW GRANTS FOR CURRENT_USER()` 的支持以兼容 MySQL 客户端。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW 语句不允许在显式事务内使用。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flight 在结果集为空时返回列名 `r`。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF 代码中缺少 JNI 异常处理检查。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 函数注册问题。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- 使用 `enable_load_profile` 时 Stream Load profile 收集问题。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- Profile 的 START_TIME/END_TIME 未按会话时区显示。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` 不支持运行时动态修改。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` 在等值谓词中未对特殊字符进行转义。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- 在错误路径上并行加载 segment/rowset 时存在释放后使用问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 聚合溢出 `set_finishing` 中存在潜在的哈希表数据丢失问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 磁盘重迁移（A→B→A）期间 GC 竞争导致 PK tablet rowset 元数据丢失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` 中存在 DB 读锁泄漏。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM 刷新记录的 PCT 分区元数据不完整。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 在 Stream Load/Broker Load 中分析生成列时，若引用的列缺失则发生 NPE。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短路点查中缺少分区谓词。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0

发布日期：2026 年 4 月 13 日

### 存算分离架构

- **新增多租户数据管理**

  存算分离集群现支持基于范围的数据分布以及 tablet 的自动分裂与合并。当 tablet 过大或成为热点时，可自动进行分裂，无需变更 Schema、修改 SQL 或重新导入数据。该功能可显著提升易用性，直接解决多租户工作负载中的数据倾斜和热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 Tablet 支持（第一阶段）**

  使共享数据集群能够在每个 tablet 中托管更多数据，长期目标为每个 tablet 100 GB。第一阶段在整个摄取、主键更新和 Compaction 流水线中引入 tablet 内并行机制，使单个 Lake tablet 在数据增长时不再成为单线程瓶颈。改进内容包括：单个 tablet 内的并行 Compaction（支持段级拆分）、Lake 加载的并行 MemTable 最终化、刷写和合并（包括 load-spill 路径）、主键表的 tablet 内部并行发布和并行条件更新，以及云原生主键索引的范围拆分/并行/大小分层 Compaction（支持远程存储映射文件）。这些改进共同显著降低了大 tablet 工作负载的摄取内存开销、Compaction 放大以及 FE 元数据压力。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **快速 Schema 演进 V2**

  共享数据集群现已支持快速 Schema 演进 V2，可实现秒级 DDL 执行，并进一步将支持范围扩展至物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 共享数据上的倒排索引**

  为共享数据集群启用内置倒排索引，以加速文本过滤和全文检索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  查询级别的缓存命中率现已在审计日志和监控系统中暴露，以提升缓存透明度并辅助延迟诊断。新增的数据缓存指标包括内存和磁盘配额使用情况以及页面缓存统计信息。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表新增段元数据过滤功能，在扫描时根据排序键范围跳过无关段，从而减少范围谓词查询的 I/O 开销。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- 支持 Lake DeltaWriter 的快速取消，减少共享数据集群中已取消摄取作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 新增对自动集群快照基于时间间隔调度的支持。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- 支持 MemTable 刷新和合并的流水线执行，提升共享数据集群中云原生表的摄取吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- 支持 `dry_run` 模式用于修复云原生表，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 在 shared-nothing 集群中为发布事务新增线程池，提升发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### 数据湖分析

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入位置删除文件，使用户可以直接从 StarRocks 对 Iceberg 表执行 DELETE 操作。支持涵盖 Plan、Sink、Commit 和 Audit 的完整流水线。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive 和 Iceberg 表的 TRUNCATE**

  支持对外部 Hive 和 Iceberg 表执行 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上的增量物化视图**

  将增量物化视图刷新的支持扩展至 Iceberg 仅追加表，实现无需全表刷新的查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg 中半结构化数据的 VARIANT 类型**

  在 Iceberg Catalog 中支持 VARIANT 数据类型，用于灵活的读时模式存储和半结构化数据查询。支持读取、写入、类型转换及 Parquet 集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增对 Iceberg v3 默认值特性和行血缘的支持。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增对 `rewrite_manifests` 过程的支持，并为 `expire_snapshots` 和 `remove_orphan_files` 过程扩展了额外参数，以实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- 支持从 Iceberg 表读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- 支持从 Iceberg v3 表读取 `_row_id`，并支持 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- 支持创建带有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中显示属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 支持通过特定分支、标签、版本或时间戳查询 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- 支持 Paimon 表的复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- 支持在创建 Iceberg 表时使用带括号语法的分区转换。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- 支持基于 Transform Partition 的 Iceberg 全局 shuffle，以改善数据组织。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- 支持为 Iceberg 表 sink 动态启用全局 shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- 为 Iceberg 表 sink 引入了提交队列，以避免并发提交冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- 为 Iceberg 表 sink 添加了主机级别排序，以改善数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- 在 ETL 执行模式下默认启用了额外的优化，无需显式配置即可提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 及类似批量操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- 为 Iceberg 表上的 INSERT 和 DELETE 操作添加了提交审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- 支持在 Iceberg REST Catalog 中启用或禁用视图端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- 支持对各种 Iceberg catalog 类型执行 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- 支持 AWS Glue Catalog 表的分区投影。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- 为 AWS Glue `GetDatabases` API 添加了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- 支持通过端点注入（`azblob`/`adls2`）进行 Azure ABFS/WASB 路径映射。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- 为 JDBC catalog 添加了数据库元数据缓存，以减少远程 RPC 开销及外部系统故障的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- 支持在 `information_schema` 中为 PostgreSQL 表添加列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- 改进了 Oracle 和 PostgreSQL 的 JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎

- **递归 CTE**

  支持用于层次遍历、图查询和迭代 SQL 计算的递归公用表表达式（CTE）。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 重写，新增基于统计信息的数据倾斜检测、直方图支持以及 NULL 值倾斜感知。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- 改进了窗口上的 COUNT DISTINCT，并新增对融合多重去重聚合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- 支持为窗口函数指定显式倾斜提示，并通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- 在 Trino Parser 中支持对 INSERT 语句使用 EXPLAIN 和 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- 支持通过 EXPLAIN 查看查询队列可见性。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数与 SQL 语法

- 新增以下函数：
  - `array_top_n`：从数组中按值排名返回前 N 个元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：将多个数组按元素合并为结构体数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：以缩进格式化 JSON 字符串。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：在 JSON 字符串中指定路径处设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：对具有相同键的行的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：返回当前数据仓库的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：从 SQL 中调用外部 AI 模型以执行推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：在 SQL 表达式中引发用户自定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 提供以下函数或语法扩展：
  - 支持在 `array_sort` 中使用 lambda 比较器进行自定义排序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持在 FULL OUTER JOIN 中使用 USING 子句，符合 SQL 标准语义。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持在带有 ORDER BY/PARTITION BY 的帧窗口函数中使用 DISTINCT 聚合。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 支持在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中使用 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 支持在类 count distinct 聚合函数中使用 VARBINARY。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持在 IN 表达式中进行日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 支持在 BEGIN/START TRANSACTION 中使用 WITH LABEL 语法。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 支持在 SHOW 语句中使用 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持使用 `ALTER TASK` 语句进行任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）用于 CSV 文件导出。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式，用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理与可观测性

- 支持为资源组设置 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改善多 Warehouse 的 CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图，用于查看 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest 黑名单，在集群级别屏蔽特定查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法直接访问的节点进行 Arrow Flight 数据检索。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，无需重新连接即可将全局变量变更传播至现有连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 新增内置 UI 功能，用于分析查询 Profile 和查看格式化 SQL，使查询调优更加便捷。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 端点，提供结构化的集群概览信息。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 添加了全局只读系统变量 `@@run_mode`，用于查询当前集群运行模式（共享数据或非共享）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2`，以改善查询队列管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持为 Stream Load 和 Merge Commit 操作设置用户级别的默认仓库。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 添加了 `skip_black_list` 会话变量，用于在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为 metrics API 添加了 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 添加了模拟用户支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 将 `table_query_timeout` 添加为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 支持添加 FE 观察者节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 支持在 `information_schema.loads` 中显示 Merge Commit 信息，以提升加载作业的可见性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在云原生表中显示 tablet 状态，以便更好地进行故障排查。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### 安全

- [CVE-2026-33870] [CVE-2026-33871] 替换了 AWS bundle 并将 Netty 升级至 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级至 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级至 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### Bug 修复

以下问题已修复：

- 通过跳过范围分布 tablet 的数据文件删除操作，修复了 tablet 分裂后数据丢失的问题。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 在处理复杂类型时的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了由 `BatchUnit` 与 `FetchTaskContext` 之间的 `shared_ptr` 循环引起的内存泄漏问题。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了 SystemMetrics 中因并发 getline 访问导致的双重释放崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在 eager merge 消耗所有块时发生崩溃的问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了当自动创建的分区被 TTL 清理器删除时出现的 NPE 问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中的 NPE 问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了外连接中常量侧列引用的谓词改写不正确的问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了在共享数据模式下修改 CHAR 列长度后查询结果不正确的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了多表情况下 MV 刷新的 Bug。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新时 MV 回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步 MV 中全空值处理的 Bug。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了快速 Schema 变更 ADD COLUMN 后查询 MV 时出现重复列 ID 错误的问题。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了由共享 DecodeInfo 导致的低基数改写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数 Join 谓词类型不匹配的问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了 `null_counts` 为空时 Parquet Page Index Filter 中的 Segfault 问题。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 展平时数组与对象在相同路径上的冲突问题。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制的问题。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性的问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB FileSystem 缓存键未包含容器的问题。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池满时发生死锁的问题。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon Catalog 刷新时因 ObjectTable 上的 ClassCastException 导致崩溃的问题。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了带常量子查询的 FULL OUTER JOIN USING 问题。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了 CTE 作用域中 join on 子句的错误。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 通过使用 bindScope() 模式修复了 ConnectContext 内存泄漏问题。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了共享无关集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏问题。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift 接受线程在遇到任何异常时退出的问题。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了例行导入列映射中 UDF 解析的问题。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了字典页过大时扫描结果错误的问题。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了范围分区重叠的问题。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了查询队列分配时间和等待超时的问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了 `array_map` 在处理 null 字面量数组时崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 的栈溢出问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了 LDAP 认证中用户名大小写不敏感规范化的问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计日志和 SQL 脱敏中对用户认证字符串进行了掩码处理。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更

- ETL 执行模式优化现已默认启用。这使 INSERT INTO SELECT、CREATE TABLE AS SELECT 及类似批量工作负载无需显式配置更改即可受益。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除常量值外还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：USING 列在输出中只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` 现已默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL 事务默认通过会话变量 `enable_sql_transaction` 进行控制。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
