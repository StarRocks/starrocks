---
displayed_sidebar: docs
description: "StarRocks 4.1 版本发布说明：基于多租户 range 的 tablet 自动分裂、大容量 tablet 支持（目标 100 GB）、用于……的 Fast Schema Evolution V2"
---

# StarRocks 4.1 版本

:::danger
**容器镜像问题（v4.1.0）**

由于 v4.1.0 容器镜像中存在不稳定的加载顺序问题，BE 进程在容器环境中可能无法可靠启动。**容器环境用户应NOT升级到 v4.1.0。**请等待修复该问题的 v4.1.1 版本（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。
:::

:::warning
**降级说明**

- 将 StarRocks 升级到 v4.1 后，NOT要降级到低于 v4.0.6 的任何 v4.0 版本。

  由于 v4.1 中引入了数据布局方面的内部变更（涉及 tablet 的拆分和分布机制），升级到 v4.1 的集群可能会生成与早期版本不完全兼容的元数据和存储结构。因此，从 v4.1 降级仅支持降级到 v4.0.6 或更高版本，不支持降级到 v4.0.6 之前的版本。此限制是由于早期版本在解析 tablet 布局和分布元数据方面存在向后兼容性约束所致。
:::

## 4.1.8 {/* #418 */}

发布日期：2026 年 11 月 16 日

### 行为变更 {/* #behavior-changes */}

- 存算分离主键索引缓存的淘汰策略已变更：缓存新增了一个紧急级别，在每次 publish 后都会进行检查。[#81143](https://github.com/StarRocks/starrocks/pull/81143) [#81168](https://github.com/StarRocks/starrocks/pull/81168)
- 本地（存算一体）主键导入默认不再检查不可变的 L1 和 L2 持久化索引文件中是否存在重复键。新的 BE 配置项 `skip_pindex_l1_l2_check_in_insert` 默认值为 `true`；将其设置为 `false` 可恢复之前的行为。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- `lake_pk_preload_memory_limit_percent`、`lake_pk_index_block_cache_limit_percent` 和 `put_combined_txn_log_thread_pool_num_max` 现在可在运行时生效，无需重启 BE 或 CN。`lake_pk_index_block_cache_limit_percent` 现已支持动态修改。[#81164](https://github.com/StarRocks/starrocks/pull/81164) [#81244](https://github.com/StarRocks/starrocks/pull/81244) [#81677](https://github.com/StarRocks/starrocks/pull/81677)
- 物化视图刷新处理逻辑已变更：被删除的基表分区会一直保持跟踪状态，直到其对应的物化视图分区被刷新；没有任何已跟踪基表分区对应的物化视图分区会被刷新；增量物化视图重建中被取代的批次将被跳过而不是导致失败。[#81187](https://github.com/StarRocks/starrocks/pull/81187) [#81192](https://github.com/StarRocks/starrocks/pull/81192) [#81262](https://github.com/StarRocks/starrocks/pull/81262)
- 增量物化视图对 `__ROW_ID__` 使用无前缀的行 id 指纹。[#81259](https://github.com/StarRocks/starrocks/pull/81259)
- 物化视图在 FE 启动时按层级逐层激活，从而避免占满 `mv-plan-cache` 线程池资源。[#81497](https://github.com/StarRocks/starrocks/pull/81497)
- 针对 external catalog 表的 `DESCRIBE` 以及外部数据库和表的列举操作，现在使用调用者的会话上下文来进行元数据调用。[#80628](https://github.com/StarRocks/starrocks/pull/80628)
- `bool_or` 现在可以合并包含 NULL 值的中间列。[#81237](https://github.com/StarRocks/starrocks/pull/81237)
- 不支持的区间运算现在会报告描述性错误，而不是 `Unknown error`。[#81024](https://github.com/StarRocks/starrocks/pull/81024)
- `CREATE BASELINE` 可在 follower FE 上正常工作。[#81157](https://github.com/StarRocks/starrocks/pull/81157)
- FE 不再出现跨 FE 错误续期全局字典的问题。[#79511](https://github.com/StarRocks/starrocks/pull/79511)
- 读取已删除存算分离 tablet 的第 1 版现在会返回 `NotFound`。[#81196](https://github.com/StarRocks/starrocks/pull/81196)
- 存算分离主键 tablet 恢复机制已重构：publish 的 delete vector 与恢复过程保持一致；在主键不一致时会恢复多语句事务的批量应用；使用物理行 id；因 Compaction 继承的 delete 文件将被丢弃。[#81204](https://github.com/StarRocks/starrocks/pull/81204) [#81206](https://github.com/StarRocks/starrocks/pull/81206) [#81211](https://github.com/StarRocks/starrocks/pull/81211) [#81250](https://github.com/StarRocks/starrocks/pull/81250)
- Kafka Routine Load 按分区跟踪 EOF，并对空导入（即未产生目标行的批次）采取不同的处理方式。[#81412](https://github.com/StarRocks/starrocks/pull/81412)
- 显式事务的导入 id 现在会被持久化，以便在 FE 重启或 Leader 切换后，publish 操作仍能找到对应的事务日志。[#81413](https://github.com/StarRocks/starrocks/pull/81413)
- 为遗留的基于每用户 DN 的 LDAP 用户保留了 Group providers。[#81399](https://github.com/StarRocks/starrocks/pull/81399)
- FE 加载镜像文件时，会跳过被拒绝的会话变量值，而不是导致 FE 启动失败。[#81655](https://github.com/StarRocks/starrocks/pull/81655)
- 对于 `FILES()` 和 Broker Load，Arrow 预缓冲的 Parquet 范围会在扫描线程上读取。[#81771](https://github.com/StarRocks/starrocks/pull/81771)
- Iceberg 分区值现在会根据每个数据文件自身的分区规范进行解码。[#81789](https://github.com/StarRocks/starrocks/pull/81789)
- 单事务 lake publish 批次现在会遵循重试退避机制。[#81803](https://github.com/StarRocks/starrocks/pull/81803)
- 当 trie 边界此前将某个 flat JSON 路径丢弃时，现在该路径会被正确标记为 JSON 类型。[#81816](https://github.com/StarRocks/starrocks/pull/81816)
- 修复了 lake 元数据缓存键以及元数据删除时缓存淘汰的问题。[#81978](https://github.com/StarRocks/starrocks/pull/81978)

### 改进项 {/* #improvements */}

- 新增了扫描执行器工作线程相关指标。[#77435](https://github.com/StarRocks/starrocks/pull/77435)
- 存算分离 publish 版本超时时间现已支持配置。[#79332](https://github.com/StarRocks/starrocks/pull/79332)
- 大型 flat JSON Compaction 的输出现在以流式方式写出，而不是缓冲整个 Segment。[#80470](https://github.com/StarRocks/starrocks/pull/80470)
- `update_memory_limit_percent` 现已支持动态修改。[#81118](https://github.com/StarRocks/starrocks/pull/81118)
- JVM 已提交内存现已计入 BE 进程内存跟踪器中。[#81127](https://github.com/StarRocks/starrocks/pull/81127)
- 不可变索引的存在性检查复用了 `get`。[#81307](https://github.com/StarRocks/starrocks/pull/81307)
- 支持在数据文件为 ORC 格式的 Iceberg 表上执行 `DELETE`。[#81435](https://github.com/StarRocks/starrocks/pull/81435)
- 支持从 ORC 数据文件中读取 Iceberg v3 行血缘信息。[#81440](https://github.com/StarRocks/starrocks/pull/81440)
- 跳过被 runtime filter 拒绝的分区的 Iceberg equality delete 文件，并且在规划 Iceberg 扫描范围时跳过按文件划分的分区工作。[#81437](https://github.com/StarRocks/starrocks/pull/81437) [#81438](https://github.com/StarRocks/starrocks/pull/81438)
- 存算分离备份节点按 tablet 哈希进行分布。[#81436](https://github.com/StarRocks/starrocks/pull/81436)

### 安全性 {/* #security */}

- 将 FE ZooKeeper 升级到 3.9.6。[#81292](https://github.com/StarRocks/starrocks/pull/81292)
- 将 pprof 升级到 release/20261022。[#81300](https://github.com/StarRocks/starrocks/pull/81300)
- 将 guava 升级到 33.7.2，将 snappy-java 升级到 1.1.10.9。[#81859](https://github.com/StarRocks/starrocks/pull/81859)

### 问题修复 {/* #bug-fixes */}

修复了以下问题：

- 修复了 Iceberg 读取和插入中的问题，包括未知的分区转换、ORC 字段 id、提升的边界值、未知类型以及默认值问题。[#81257](https://github.com/StarRocks/starrocks/pull/81257) [#81311](https://github.com/StarRocks/starrocks/pull/81311) [#81432](https://github.com/StarRocks/starrocks/pull/81432)
- 修复了对具有分区演进的表进行 Iceberg 读写时的问题，包括将 position delete 保留在其数据文件规范下。[#81401](https://github.com/StarRocks/starrocks/pull/81401) [#81361](https://github.com/StarRocks/starrocks/pull/81361)
- 修复了 Iceberg DML 目标查找、按行 id 配对的 v3 查找行，以及对没有写入默认值的列的 NULL 写入问题。[#81165](https://github.com/StarRocks/starrocks/pull/81165) [#81188](https://github.com/StarRocks/starrocks/pull/81188) [#81362](https://github.com/StarRocks/starrocks/pull/81362)
- 修复了两个 Iceberg FE 挂起问题（explain 分区列表以及 OSS 预读等待）以及空闲 Iceberg commit executor 线程的问题。[#81711](https://github.com/StarRocks/starrocks/pull/81711) [#81805](https://github.com/StarRocks/starrocks/pull/81805)
- Iceberg、Delta 和 Hudi 的 `INSERT` 数据源在表锁下会被失效处理，而不是刷新。[#81923](https://github.com/StarRocks/starrocks/pull/81923)
- Iceberg REST catalog user agent 的构建不再包含 `NodeMgr`。[#80660](https://github.com/StarRocks/starrocks/pull/80660)
- 在 `OR` 倒排索引回退中对 `MATCH` 取反时返回了 NULL 行。[#76350](https://github.com/StarRocks/starrocks/pull/76350)
- 主键表 Compaction 中 Level -1 rowset 的选择没有限制。[#80471](https://github.com/StarRocks/starrocks/pull/80471)
- 缓冲的直通 chunk 未计入查询和进程内存。[#80910](https://github.com/StarRocks/starrocks/pull/80910)
- 聚合状态组合器在批次销毁时泄漏了被包装的状态。[#81054](https://github.com/StarRocks/starrocks/pull/81054)
- 在 compactor 正在写入合并后的 JSON 列时，该列可能被释放。[#81117](https://github.com/StarRocks/starrocks/pull/81117)
- 当一次 flush 并发完成时，spiller 的 flush-all 回调可能被执行两次。[#81134](https://github.com/StarRocks/starrocks/pull/81134)
- min/max runtime filter 的边界值在未持有 slice 锁的情况下被读取。[#81138](https://github.com/StarRocks/starrocks/pull/81138)
- 可落盘的聚合数据源可能访问已释放的 spiller。[#81199](https://github.com/StarRocks/starrocks/pull/81199)
- `getShardInfo` 调用未对每批次的分片数量进行限制。[#81197](https://github.com/StarRocks/starrocks/pull/81197)
- 导入 sink 关闭提交异常可能导致 BE 或 CN 进程终止。[#81251](https://github.com/StarRocks/starrocks/pull/81251)
- Trino 的 `TIMESTAMP WITH TIME ZONE` 字面量使用了错误的时区方向。[#81256](https://github.com/StarRocks/starrocks/pull/81256)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 在激活失败时丢失了 `Can not active materialized view` 消息。[#81258](https://github.com/StarRocks/starrocks/pull/81258)
- 加载镜像时未从物化视图属性中恢复 `mv_rewrite_staleness_second`。[#81263](https://github.com/StarRocks/starrocks/pull/81263)
- 将 `DOUBLE` 或 `FLOAT` 转换为 `DECIMAL` 在 aarch64 上的结果与 x86 上不同。[#81261](https://github.com/StarRocks/starrocks/pull/81261)
- 当结果超出 `DATETIME` 范围时，`convert_tz` 现在返回 NULL。[#81264](https://github.com/StarRocks/starrocks/pull/81264)
- 对 `VARBINARY` 列进行等值过滤时的规划失败。[#81310](https://github.com/StarRocks/starrocks/pull/81310)
- 物化视图命名与查找：当其引用的基表分区列未被分析时，该物化视图会被命名，并且 `inspect_mv_refresh_info` 通过全名来索引同名的基表。[#81357](https://github.com/StarRocks/starrocks/pull/81357) [#81358](https://github.com/StarRocks/starrocks/pull/81358)
- 在存算一体模式下，全局延迟物化查找无法到达计算节点。[#81359](https://github.com/StarRocks/starrocks/pull/81359)
- 由于 `lastSchemaUpdateTime` 的递增时机过晚，在存算分离 schema change 期间并发查询未能重新规划。[#81363](https://github.com/StarRocks/starrocks/pull/81363)
- Ranger 服务定义不允许在视图和物化视图上授予 `SELECT`。[#81400](https://github.com/StarRocks/starrocks/pull/81400)
- `UNION` 合并可能产生顺序错误的值。[#81420](https://github.com/StarRocks/starrocks/pull/81420)
- 任务运行进程的启动时间未在 Follower 上回放。[#81433](https://github.com/StarRocks/starrocks/pull/81433)
- 在 `IF EXISTS` 下，会对不可导入的外部表检查 `DROP` 权限。[#81434](https://github.com/StarRocks/starrocks/pull/81434)
- 查询被取消后，不再打开 lake segment。[#81598](https://github.com/StarRocks/starrocks/pull/81598)
- 由刷新操作导致的物化视图失活状态会被记录到日志，且其任务保持已调度状态。[#81660](https://github.com/StarRocks/starrocks/pull/81660)
- 当仅约束了 key 前缀时，包含尾部 NULL 的分区被删除了。[#81675](https://github.com/StarRocks/starrocks/pull/81675)
- 字符串 `IN` 谓词在运行时过滤器阶段被当作字典编码读取。[#81676](https://github.com/StarRocks/starrocks/pull/81676)
- 延迟物化视图剪枝未能保留其谓词所需的列。[#81687](https://github.com/StarRocks/starrocks/pull/81687)
- 常量 `IN` 谓词值列表中可能包含列引用。[#81825](https://github.com/StarRocks/starrocks/pull/81825)
- 当常量 GROUP BY 键被折叠进 `HAVING` 后，未对其进行规范化。[#81919](https://github.com/StarRocks/starrocks/pull/81919)
- 基于月份的日期运算被当作可逆运算进行改写。[#81979](https://github.com/StarRocks/starrocks/pull/81979)
- 当保留期为 0 时，物化视图强制删除出现递归。[#81953](https://github.com/StarRocks/starrocks/pull/81953)
- 将 StarOS 和 StarCache 升级到 v4.1.6。[#81508](https://github.com/StarRocks/starrocks/pull/81508)

## 4.1.7 {/* #417 */}

发布日期：2026 年 10 月 26 日

### 行为变更 {/* #behavior-changes-1 */}

- 本地（存算一体）模式下的主键表插入操作默认不再检查不可变的 L1 和 L2 持久化索引文件是否存在重复键。新增的 BE 配置项 `skip_pindex_l1_l2_check_in_insert` 默认值为 `true`；L0（memtable）检查始终执行。将其设置为 `false` 可恢复之前的行为。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- BE JVM 指标默认已启用（`enable_jvm_metrics` 现在默认值为 `true`）。它们仅在 JVM 已存在时才会被采集，因此启用该指标不会再在原本不需要 JVM 的 BE 上启动 JVM。[#81113](https://github.com/StarRocks/starrocks/pull/81113) [#81074](https://github.com/StarRocks/starrocks/pull/81074)
- `spill_max_dir_bytes_ratio` 的默认值由 `0.8` 降低为 `0.5`，因此中间结果落盘最多使用每个落盘目录容量的 50%。[#80189](https://github.com/StarRocks/starrocks/pull/80189)
- Lake 副本文件拷贝现在运行在一个专用的 CN 级别线程池中。`lake_replication_file_copy_threads` 的默认值由 `0`（表示 4 × CPU 核心数）改为 `16`，新增的 `lake_replication_max_parallel_files_per_tablet`（默认值为 `4`）用于限制单个 tablet 并行拷贝的文件数量。[#78078](https://github.com/StarRocks/starrocks/pull/78078)
- 存算分离模式下由删除操作触发的主键表 tablet 基础 Compaction，现在需要通过新增的 BE 配置项 `lake_pk_compaction_base_enable_delete_trigger` 显式开启，该配置项默认值为 `false`。[#79926](https://github.com/StarRocks/starrocks/pull/79926)
- 当 `pk_index_parallel_execution_threadpool_max_threads` 为 `0` 时，主键索引并行执行线程池的大小将设置为 all CPU 核心数，而不是其一半。[#80460](https://github.com/StarRocks/starrocks/pull/80460)
- `CAST(x AS CHAR(N))` 现在会将结果截断为 `N` 个字符，与 MySQL 保持一致。FE 中的常量折叠和 BE 中的执行均会应用此截断。[#75909](https://github.com/StarRocks/starrocks/pull/75909)
- 在展示或记录配置及任务定义的任何位置，凭据值均会被脱敏处理：`ADMIN SHOW FRONTEND CONFIG`、FE 的 `/variable` 页面、BE 的 `/varz`、`information_schema.be_configs`、归档至 `task_run_history` 的任务运行定义，以及写入 FE 和 BE HTTP 日志中的请求 URI 和请求头。通过 `ADMIN SET FRONTEND CONFIG`、FE 的 `/api/_set_config` 接口以及 BE 的 `/api/update_config` 接口设置的值也会被脱敏处理。[#80097](https://github.com/StarRocks/starrocks/pull/80097) [#80040](https://github.com/StarRocks/starrocks/pull/80040) [#80492](https://github.com/StarRocks/starrocks/pull/80492) [#80444](https://github.com/StarRocks/starrocks/pull/80444) [#80280](https://github.com/StarRocks/starrocks/pull/80280) [#80447](https://github.com/StarRocks/starrocks/pull/80447) [#80515](https://github.com/StarRocks/starrocks/pull/80515) [#80203](https://github.com/StarRocks/starrocks/pull/80203)
- FE HTTP 接口 `/service_id`、`/dump` 和 `/dump_starmgr` 现在要求使用管理员用户。[#80749](https://github.com/StarRocks/starrocks/pull/80749) [#80701](https://github.com/StarRocks/starrocks/pull/80701)

### 功能优化 {/* #improvements-1 */}

- 新增表属性 `enable_statistic_auto_collect`。将其设置为 `false`（通过 `CREATE TABLE` 或 `ALTER TABLE`）可将该表排除在自动统计信息采集之外。由数据导入触发的采集不受影响。[#81027](https://github.com/StarRocks/starrocks/pull/81027)
- 通过将新增的 FE 配置项 `authentication_ldap_case_insensitive` 设置为 `true`（默认值为 `false`），可以对 LDAP 和 Active Directory 的用户名及组名进行不区分大小写的匹配。[#79255](https://github.com/StarRocks/starrocks/pull/79255)
- 新增 `ALTER GROUP PROVIDER`，用于原地更新组提供程序的属性。[#79166](https://github.com/StarRocks/starrocks/pull/79166)
- 在 Query Queue V2 下，`query_queue_mem_used_pct_limit` 会应用到执行查询的每个节点上。[#81026](https://github.com/StarRocks/starrocks/pull/81026)
- 通过使用并行反向查找和墓碑 sstable 路径，发布阶段大型主键索引删除操作的速度得到提升。[#76785](https://github.com/StarRocks/starrocks/pull/76785)
- 搜索型 `CASE WHEN` 只会对到达该分支的行求值。[#79484](https://github.com/StarRocks/starrocks/pull/79484)
- `convert_tz` 会缓存夏令时偏移量的查询结果，而不是为每一行都进行时区转换。[#78459](https://github.com/StarRocks/starrocks/pull/78459)
- `UNNEST` 对可空数组实现了零拷贝，并且结合 `LEFT JOIN` 时，每次按一个 chunk 展开多个数组，而不是物化整个输入 chunk；其余表函数也已加入内存溢出防护。[#79221](https://github.com/StarRocks/starrocks/pull/79221) [#79222](https://github.com/StarRocks/starrocks/pull/79222)
- 针对一个 fragment 的 all 实例请求，其执行计划仅序列化一次。[#80716](https://github.com/StarRocks/starrocks/pull/80716)
- Colocate 聚合和窗口函数现在可以在 range-colocate 表上进行规划。[#79946](https://github.com/StarRocks/starrocks/pull/79946)
- 从主分支回合了 tablet 预分裂的相关改进，包括用于限制基于采样的 tablet 预分裂扫描数据量的新增 FE 配置项（`tablet_pre_split_data_tier_scan_byte_limit`、`tablet_pre_split_data_tier_min_scan_files`、`tablet_pre_split_data_tier_max_scan_files` 和 `tablet_pre_split_meta_tier_footer_read_parallelism`）。[#79949](https://github.com/StarRocks/starrocks/pull/79949)

### 安全性 {/* #security-1 */}

- 将 pprof 升级至 release/20261012。[#80439](https://github.com/StarRocks/starrocks/pull/80439)
- 将 lz4-java 升级至 1.11.4。[#80319](https://github.com/StarRocks/starrocks/pull/80319)
- 将 BouncyCastle 升级至 1.86、Jackson 升级至 2.21.7、zstd-jni 升级至 1.5.7-15、Parquet 升级至 1.18.1。[#80248](https://github.com/StarRocks/starrocks/pull/80248)
- 将 Broker 所使用的 Tomcat 升级至 9.0.121。[#79719](https://github.com/StarRocks/starrocks/pull/79719)
- 将 ZooKeeper 升级至 3.8.7 和 3.9.6。[#79707](https://github.com/StarRocks/starrocks/pull/79707)
- `lookup_string` 的参数现已进行转义以防止 SQL 注入，其内部查询以调用者身份运行。[#79991](https://github.com/StarRocks/starrocks/pull/79991) [#79959](https://github.com/StarRocks/starrocks/pull/79959)
- FE Web 控制台会对管理员及主机提供的值和表格单元格进行 HTML 编码，包括 `/system` 和 `/query` 页面，并对 `/proc_profile` 上的 `node` 参数进行校验和编码。[#80656](https://github.com/StarRocks/starrocks/pull/80656) [#80639](https://github.com/StarRocks/starrocks/pull/80639) [#80582](https://github.com/StarRocks/starrocks/pull/80582) [#80565](https://github.com/StarRocks/starrocks/pull/80565) [#80689](https://github.com/StarRocks/starrocks/pull/80689)

### 问题修复 {/* #bug-fixes-1 */}

修复了以下问题：

- Iceberg equality deletes 未限定在其分区范围内。[#81139](https://github.com/StarRocks/starrocks/pull/81139)
- 即使有可用空闲空间，Datacache 内存写入仍可能被淘汰概率拒绝。[#80542](https://github.com/StarRocks/starrocks/pull/80542)
- 分桶属性未随管道数据源的分区类型一起传递。[#80099](https://github.com/StarRocks/starrocks/pull/80099)
- 发送方已经对齐的 Bucket shuffle join 输入被重新分区。[#79769](https://github.com/StarRocks/starrocks/pull/79769)
- `EXCEPT`/`INTERSECT` 与 hash join 输入之间的 Driver 分区未对齐。[#78920](https://github.com/StarRocks/starrocks/pull/78920)
- `yearweek` 在接收到非法 `str_to_date` 输入时导致 BE 崩溃。[#78358](https://github.com/StarRocks/starrocks/pull/78358)
- 在落盘通道关闭期间，排队中的落盘数据任务仍可能继续运行。[#80252](https://github.com/StarRocks/starrocks/pull/80252)
- 存算分离表 Compaction 读缓冲区未受每个 worker 内存预算的限制。[#80229](https://github.com/StarRocks/starrocks/pull/80229)
- 当无法包装其状态时，Java UDAF 合并返回了 `NULL` 而不是直接失败。[#80098](https://github.com/StarRocks/starrocks/pull/80098)
- 落盘追加失败或分区拆分失败时可能导致数据丢失。[#79457](https://github.com/StarRocks/starrocks/pull/79457)
- 在 `char` 为无符号类型的平台上，ORC `TINYINT` 的最小值/最大值统计信息有误。[#81086](https://github.com/StarRocks/starrocks/pull/81086)
- Flat JSON 路径派生 trie 没有大小限制。[#80316](https://github.com/StarRocks/starrocks/pull/80316)
- 在 Compaction 推导读取布局时，标量类型的 Flat JSON 子列可能被丢弃。[#80295](https://github.com/StarRocks/starrocks/pull/80295)
- 当扫描谓词构建器将唯一的合取项折叠掉时，扫描列未被输出。[#80572](https://github.com/StarRocks/starrocks/pull/80572)
- `PSlotDescriptor.global_dict_words` 未被声明为 bytes 类型，导致出现 UTF-8 警告。[#78657](https://github.com/StarRocks/starrocks/pull/78657)
- 通过 `update_memory_limit_percent` 调整主键索引缓存大小时未除以 100。[#81067](https://github.com/StarRocks/starrocks/pull/81067)
- 当基表被删除时，处于 `TRUE` 透明模式下的物化视图停止扫描该物化视图。[#81088](https://github.com/StarRocks/starrocks/pull/81088)
- 超出范围的向量检索查询向量会导致 BE 中止，而不是被拒绝。[#78658](https://github.com/StarRocks/starrocks/pull/78658)
- ANN 距离列无法容忍被复用的扫描 chunk。[#78686](https://github.com/StarRocks/starrocks/pull/78686)
- 对 Flat JSON 列根节点执行 `json_query` 时未重建整个文档。[#80772](https://github.com/StarRocks/starrocks/pull/80772)
- `tokenize()` 的结果大小由查看器的数据列决定，而不是按行数决定。[#80337](https://github.com/StarRocks/starrocks/pull/80337)
- 物化视图刷新所调度的任务，在刷新使物化视图失效时丢失。此修复还解决了 planner lock 和 Hive `INSERT` 变更引发的两个后续问题。[#81033](https://github.com/StarRocks/starrocks/pull/81033)
- `INSERT` 可能会遗漏 Hive 源数据行，而 `SELECT *` 会丢弃资源映射的 Hive 列。[#81032](https://github.com/StarRocks/starrocks/pull/81032)
- 跨作用域的递归 CTE 引用会导致栈溢出，而不是被拒绝。[#78533](https://github.com/StarRocks/starrocks/pull/78533)
- 损坏的 Ranger 审计配置会导致 catalog 访问失败，并且 Solr 审计所需的 ZooKeeper 类未被打包提供。[#81075](https://github.com/StarRocks/starrocks/pull/81075)
- 物化视图刷新在对非引用基表加锁超时时未能失败，并且 `inspect_mv_refresh_info` 在持有锁的情况下仍对基表加锁。[#81036](https://github.com/StarRocks/starrocks/pull/81036)
- 超出 `DATETIME` 定义域范围的统计信息边界未被拒绝。[#79388](https://github.com/StarRocks/starrocks/pull/79388)
- 比较超过 `INT64_MAX` 阈值的 JSON 数字时会抛出异常。[#78969](https://github.com/StarRocks/starrocks/pull/78969)
- 一个断言——即所选内容不应长于其所选自的列——可能会失败。[#80324](https://github.com/StarRocks/starrocks/pull/80324)
- 过大无法容纳的 `array_generate` 范围会导致 BE 中止，而不是被拒绝。[#80205](https://github.com/StarRocks/starrocks/pull/80205)
- 常量 JSON/VARIANT 到 MAP 的类型转换未能保持与其输入相同的生命周期。[#80679](https://github.com/StarRocks/starrocks/pull/80679)
- 标量 shuttle 未注册其必须克隆的字典算子（dict operators）。[#79310](https://github.com/StarRocks/starrocks/pull/79310)
- 快速 schema 演进之后，全局字典未被失效处理。[#78599](https://github.com/StarRocks/starrocks/pull/78599)
- 在 CTE 消费投影（consume project）中，重复列未被克隆。[#80158](https://github.com/StarRocks/starrocks/pull/80158)
- 宽松字典（relaxed dictionaries）未按无符号字节顺序进行合并。[#78659](https://github.com/StarRocks/starrocks/pull/78659)
- 聚合函数中的 struct 字段被 `GROUP BY` 检查拒绝。[#79649](https://github.com/StarRocks/starrocks/pull/79649)
- 并非每次物化视图失效及其版本映射清除都会被记录到日志中。[#81030](https://github.com/StarRocks/starrocks/pull/81030)
- 物化视图版本映射修复被物理分区 ID 查找所跳过。[#78469](https://github.com/StarRocks/starrocks/pull/78469)
- 在发布分析结果 chunk 之前，未检查窗口函数错误。[#81022](https://github.com/StarRocks/starrocks/pull/81022)
- MIN/MAX 的 LIST 分区列可能会读取到数据实际上并不具备的分区值。[#80558](https://github.com/StarRocks/starrocks/pull/80558)
- 某个表失败后，分析任务停止采集剩余表。[#81028](https://github.com/StarRocks/starrocks/pull/81028)
- 在基表分区频繁变动和批量刷新的情况下，物化视图刷新和新鲜度判断出现错误。[#81034](https://github.com/StarRocks/starrocks/pull/81034)
- 三个已存在的 FE 元数据并发问题。[#81037](https://github.com/StarRocks/starrocks/pull/81037)
- 当 skew join v2 替换 shuffle exchanges 时，全局字典表达式被丢弃。[#78672](https://github.com/StarRocks/starrocks/pull/78672)
- 在基分区被删除后仍然存在的物化视图分区未被刷新。[#79057](https://github.com/StarRocks/starrocks/pull/79057)
- 对齐内存分配失败时未抛出 `std::bad_alloc`。[#79054](https://github.com/StarRocks/starrocks/pull/79054)
- ALTER 的完成报告未按任务正确匹配，且排队中的 ALTER 任务已过期。[#81018](https://github.com/StarRocks/starrocks/pull/81018)
- 交换进来的临时分区的 tablet 被强制删除。[#81014](https://github.com/StarRocks/starrocks/pull/81014)
- 残留的 `INSERT` 任务未能根据其事务状态完成或取消。[#80711](https://github.com/StarRocks/starrocks/pull/80711)
- 增量物化视图中复杂类型的行标识键未被拒绝。[#79312](https://github.com/StarRocks/starrocks/pull/79312)
- `greplog` 不支持以年份为前缀的 glog 日志条目。[#80990](https://github.com/StarRocks/starrocks/pull/80990)
- 归档的物化视图任务运行历史中，LIST 分区的值未被收缩。[#80944](https://github.com/StarRocks/starrocks/pull/80944)
- 在增量维护的物化视图上接受了 `auto_refresh_partitions_limit`。[#79051](https://github.com/StarRocks/starrocks/pull/79051)
- 读取校验失败时未重新读取 bundle 元数据。[#79928](https://github.com/StarRocks/starrocks/pull/79928)
- 当源分区在提交时已经消失，`INSERT OVERWRITE` 未报告明确的错误信息。[#79902](https://github.com/StarRocks/starrocks/pull/79902)
- 物化视图版本映射重置、`TRUE` 透明改写以及时效性缓存可能返回过期结果。[#80893](https://github.com/StarRocks/starrocks/pull/80893)
- LIST 分区漂移未与实际表进行比较。[#78470](https://github.com/StarRocks/starrocks/pull/78470)
- 轻量级 Compaction 未能保留主键索引大小。[#80746](https://github.com/StarRocks/starrocks/pull/80746)
- 一个常驻的强制设置重新物化了增量维护的物化视图。[#79366](https://github.com/StarRocks/starrocks/pull/79366)
- 增量维护的物化视图上的 `ALTER` 列未被拒绝。[#78726](https://github.com/StarRocks/starrocks/pull/78726)
- 物化视图的 `lastRefreshTime` 在回放时被重新计算，而不是从日志中读取。[#78467](https://github.com/StarRocks/starrocks/pull/78467)
- IVM 刷新窗口未记录在其提交所对应的事务上。[#79315](https://github.com/StarRocks/starrocks/pull/79315)
- 主键恢复在 V2 编码的单列定长键上失败。[#80446](https://github.com/StarRocks/starrocks/pull/80446)
- 在 cloud 原生主键表的 tablet 中，冲突 Compaction 中未 all 的文件被记录为孤儿文件。[#80445](https://github.com/StarRocks/starrocks/pull/80445)
- 对存算分离模式下物化视图基分区比较时使用了 FE 本地的版本时间。[#79945](https://github.com/StarRocks/starrocks/pull/79945)
- 当 `MultiGet` 提前返回时，索引块迭代器未被释放。[#79380](https://github.com/StarRocks/starrocks/pull/79380)
- memtable 未收到的行也被计入其行数限制。[#79705](https://github.com/StarRocks/starrocks/pull/79705)
- 嵌套的 Iceberg REST 命名空间在被列出时遭到删除会导致报错。[#80846](https://github.com/StarRocks/starrocks/pull/80846)
- 规划器锁修复所遗留的重复策略查找和无限等待问题。[#80943](https://github.com/StarRocks/starrocks/pull/80943)
- tablet 元数据转储未限制在本地缓存范围内。[#78449](https://github.com/StarRocks/starrocks/pull/78449)
- driver 队列计数器在资源组更新过程中未被保留。[#80889](https://github.com/StarRocks/starrocks/pull/80889)
- 格式错误的数值型 HTTP 参数可能导致 BE 崩溃。[#79022](https://github.com/StarRocks/starrocks/pull/79022)
- `convert_hash_set_to_chunk` 中存在读取溢出问题。[#80709](https://github.com/StarRocks/starrocks/pull/80709)
- `ALTER VIEW`、`INSERT OVERWRITE` 及物化视图刷新中已存在的元数据锁竞争问题，以及规划器和物化视图元数据锁方面遗留的空缺。[#80891](https://github.com/StarRocks/starrocks/pull/80891) [#80892](https://github.com/StarRocks/starrocks/pull/80892)
- lake 复制中文件复用的处理问题。[#79359](https://github.com/StarRocks/starrocks/pull/79359)
- 在 Bloom filter 下接受了不支持的列类型。[#80871](https://github.com/StarRocks/starrocks/pull/80871)
- 导入 sink 的关闭操作在 pipeline poller 上运行。[#80761](https://github.com/StarRocks/starrocks/pull/80761)
- 单次采集查询失败时，已采集的外部统计信息被丢弃。[#79518](https://github.com/StarRocks/starrocks/pull/79518)
- BE 的 Thrift RPC 重试未加入抖动，FE 的 Thrift RPC 重试在退避时也未加入抖动。[#80847](https://github.com/StarRocks/starrocks/pull/80847) [#80714](https://github.com/StarRocks/starrocks/pull/80714)
- 流水线驱动队列级别可能导致级别 0 饥饿。[#80760](https://github.com/StarRocks/starrocks/pull/80760)
- 刷新 `INSERT ... SELECT` 数据源时，资源映射的外部表未按声明保留。[#80546](https://github.com/StarRocks/starrocks/pull/80546)
- 相互依赖的重量级表达式被下推。[#80710](https://github.com/StarRocks/starrocks/pull/80710)
- brpc future 超时约定被破坏。[#80783](https://github.com/StarRocks/starrocks/pull/80783)
- 已解析的 `INSERT` 的数据源在获取规划器锁之前未被刷新，并且视图的基表在规划读取之前未被加锁。[#80786](https://github.com/StarRocks/starrocks/pull/80786) [#80784](https://github.com/StarRocks/starrocks/pull/80784)
- Iceberg REST catalog 在其连接器关闭时未被关闭，并且 Iceberg 元数据缓存后台执行器在 catalog 关闭时未被关闭。[#80712](https://github.com/StarRocks/starrocks/pull/80712) [#80598](https://github.com/StarRocks/starrocks/pull/80598)
- `SHOW PROC` 任务详情对无法展示的 ALTER 任务执行了错误的类型转换，而不是拒绝它们。[#80719](https://github.com/StarRocks/starrocks/pull/80719)
- 在 rowset 迁移过程中未复制向量索引文件。[#80391](https://github.com/StarRocks/starrocks/pull/80391)
- 当 `ADD ROLLUP` 失败时，lake rollup 的 tablet 未从倒排索引中移除。[#80717](https://github.com/StarRocks/starrocks/pull/80717)
- optimize 和 merge-partition 任务的重写任务未过期。[#80718](https://github.com/StarRocks/starrocks/pull/80718)
- 远程读取失败后，共享缓冲区可能被重用。[#79965](https://github.com/StarRocks/starrocks/pull/79965)
- 按列采集 min/max 统计信息时未进行限流。[#78544](https://github.com/StarRocks/starrocks/pull/78544)
- 空闲的池化 FE Thrift 连接被重用而不是被替换。[#80715](https://github.com/StarRocks/starrocks/pull/80715)
- 当可见的 `DELETE` 事务被应用时，`hasDelete` 未被恢复。[#80592](https://github.com/StarRocks/starrocks/pull/80592)
- decimal `map_agg` 键的精度丢失。[#80673](https://github.com/StarRocks/starrocks/pull/80673)
- FE brpc 客户端现在使用 `com.starrocks:jprotobuf-rpc-core`。[#80684](https://github.com/StarRocks/starrocks/pull/80684)
- 事务 Stream Load 上非布尔值的 `partial_update` 请求头未被拒绝。[#79350](https://github.com/StarRocks/starrocks/pull/79350)
- 对于拆分的 lake 扫描，未上报物理 segment 和 rowset 数量。[#80359](https://github.com/StarRocks/starrocks/pull/80359)
- 在每次 lake rollup 任务中未跳过 lake compaction。[#80655](https://github.com/StarRocks/starrocks/pull/80655)
- 主键表 Compaction 中出现行来源溢出。[#80472](https://github.com/StarRocks/starrocks/pull/80472)
- FE Web 控制台的 `/system` 页面未查找原始 proc 路径。[#80648](https://github.com/StarRocks/starrocks/pull/80648)
- 统计信息 DML 未绑定内部上下文。[#80473](https://github.com/StarRocks/starrocks/pull/80473)
- 在实例化之前未预留驱动容量。[#80495](https://github.com/StarRocks/starrocks/pull/80495)
- 禁用块缓冲时，未复制对端缓存数据。[#80438](https://github.com/StarRocks/starrocks/pull/80438)
- 当一个分区有多个索引时，并非所有创建任务都被计入统计。[#80640](https://github.com/StarRocks/starrocks/pull/80640)
- 运行时重置为自动模式对主键表并行执行线程不起作用。[#80567](https://github.com/StarRocks/starrocks/pull/80567)
- 本地交换中与事件调度器之间发生了接收端唤醒丢失。[#80547](https://github.com/StarRocks/starrocks/pull/80547)
- 当 Tablet 合并和 tablet 拆分结果的范围与 FE 不一致时，未被拒绝。[#80538](https://github.com/StarRocks/starrocks/pull/80538) [#80418](https://github.com/StarRocks/starrocks/pull/80418)
- 一旦某个分区超出 32 位二进制偏移量的范围，哈希连接就无法保持分区状态。[#80488](https://github.com/StarRocks/starrocks/pull/80488)
- 绕行写入事务开始超时请求头未被按秒处理。[#80548](https://github.com/StarRocks/starrocks/pull/80548)
- 保存在 FE 内存中的 alter 任务历史记录没有上限。[#80465](https://github.com/StarRocks/starrocks/pull/80465)
- 即使记录的 NULL 拒绝并未拒绝 NULL 扩展的行，外连接依然被转换；并且在 limit、top-N 或窗口之下的 join NULL 拒绝转换了外连接。[#80510](https://github.com/StarRocks/starrocks/pull/80510) [#79974](https://github.com/StarRocks/starrocks/pull/79974)
- 无列和非确定性的连接谓词在连接重排序时被丢弃。[#80511](https://github.com/StarRocks/starrocks/pull/80511)
- BE 无法编码的列类型上也接受了 `BITMAP` 索引。[#80459](https://github.com/StarRocks/starrocks/pull/80459)
- 包含重复键的短路 `IN` 列表会多次返回同一行。[#80318](https://github.com/StarRocks/starrocks/pull/80318)
- 设置 `enclose` 或 `escape` 时，CSV 文件被按字节范围拆分。[#80386](https://github.com/StarRocks/starrocks/pull/80386)
- 无法读取按列设置的 ZSTD 压缩字典。[#77355](https://github.com/StarRocks/starrocks/pull/77355)
- 多表事务 Stream Load 提交失败时的错误未被正确上报。[#80351](https://github.com/StarRocks/starrocks/pull/80351)
- 请求 URI 未使用与 action 相同的查询解析器进行净化处理。[#80461](https://github.com/StarRocks/starrocks/pull/80461)
- 不带 ID 的 `CANCEL ALTER TABLE ROLLUP` 未能取消 all rollup 任务。[#80450](https://github.com/StarRocks/starrocks/pull/80450)
- 从被查询的 catalog 返回的 `information_schema.columns`（带有 `LIMIT`）未得到应答。[#80448](https://github.com/StarRocks/starrocks/pull/80448)
- Paimon 分支状态未按表隔离，且已删除的 Paimon 分区未从元数据缓存中移除。[#79879](https://github.com/StarRocks/starrocks/pull/79879) [#79442](https://github.com/StarRocks/starrocks/pull/79442)
- Range 分布的 tablet 在创建时未使用自己的初始元数据。[#80421](https://github.com/StarRocks/starrocks/pull/80421)
- 通过 `INSERT INTO FILES` 导出为 CSV 时，重复的输出列名被拒绝。[#79897](https://github.com/StarRocks/starrocks/pull/79897)
- `FULL OUTER JOIN ... USING` 列在作为 JOIN 键时未保留其自身类型。[#79655](https://github.com/StarRocks/starrocks/pull/79655)
- 连接器 I/O 在 FE 元数据锁下运行。[#80416](https://github.com/StarRocks/starrocks/pull/80416)
- 当 SQL 方言为 Trino 时，Iceberg 时间旅行会丢失。[#79171](https://github.com/StarRocks/starrocks/pull/79171)
- Iceberg decimal manifest 边界未使用最小长度的二进制补码编码。[#78456](https://github.com/StarRocks/starrocks/pull/78456)
- Parquet、ORC 和 CSV 读取路径中的数据驱动分配未受查询内存限制约束。[#78573](https://github.com/StarRocks/starrocks/pull/78573)
- 已卸载或失败的统计信息被缓存为“无统计信息”。[#80236](https://github.com/StarRocks/starrocks/pull/80236)
- 当分区键或排序键列是生成列时，导入未被预先拆分，并且 tablet 的预拆分样本未在导入时区下运行。[#80390](https://github.com/StarRocks/starrocks/pull/80390) [#80362](https://github.com/StarRocks/starrocks/pull/80362)
- 主键表 tablet 清理被计为错误状态。[#79558](https://github.com/StarRocks/starrocks/pull/79558)
- 在 FE 重启后或在 AD 引荐（referral）发生时，LDAP 组提供程序（group provider）变得不可用。[#80361](https://github.com/StarRocks/starrocks/pull/80361)
- 在 gcs-connector 3.x 上，使用内联服务账号密钥从 GCS 进行的 Broker Load 失败。[#78649](https://github.com/StarRocks/starrocks/pull/78649)
- 周函数在处理第 0 年之前的日期时间时中止。[#80228](https://github.com/StarRocks/starrocks/pull/80228)
- 将 StarOS 和 starlet 升级到 v4.1.5。[#80347](https://github.com/StarRocks/starrocks/pull/80347)
- MemSpaceMonitor 线程在析构时未被停止，导致 BE 启动失败时出现 SIGABRT。[#79691](https://github.com/StarRocks/starrocks/pull/79691)
- 由连接器查询触发的 analyze 泄漏了一个正在运行的任务，并留下一个失效的调度器。[#80329](https://github.com/StarRocks/starrocks/pull/80329)
- 当 `createShard` 在没有收到 StarMgr 应答的情况下失败时，未被重放一次。[#80312](https://github.com/StarRocks/starrocks/pull/80312)
- ASOF JOIN 右侧的过滤条件未保留在 JOIN 之上。[#80303](https://github.com/StarRocks/starrocks/pull/80303)
- NGRAMBF 裁剪对于 `NOT LIKE` 不正确。[#79243](https://github.com/StarRocks/starrocks/pull/79243)
- JOIN 和范围谓词的基数估算有误。[#80242](https://github.com/StarRocks/starrocks/pull/80242)
- 在拆分 top-N 聚合时，NULL 分组丢失。[#80143](https://github.com/StarRocks/starrocks/pull/80143)
- 自适应物化视图分区刷新不支持 cloud 内表。[#80270](https://github.com/StarRocks/starrocks/pull/80270)
- 当没有表需要清理时，`AnalyzeMgr.clearStalePartitionStats` 抛出了 `IllegalStateException`。[#80265](https://github.com/StarRocks/starrocks/pull/80265)
- `ViewsSystemTable` 持有的是数据库读锁，而非密集型表锁。[#78344](https://github.com/StarRocks/starrocks/pull/78344)
- `DROP PARTITIONS WHERE` 丢弃了与其条件不匹配的行。[#79958](https://github.com/StarRocks/starrocks/pull/79958)
- Lake `delete_tablets` 的记账未受 tablet 加版本数限制约束。[#80243](https://github.com/StarRocks/starrocks/pull/80243)
- 当最后一个发送方离开一个正在结束的 `SinkBuffer` 时，未通知 sink observer。[#80155](https://github.com/StarRocks/starrocks/pull/80155)
- 刷新过程中未忽略一个旧版物化视图任务 `postRun`。[#79943](https://github.com/StarRocks/starrocks/pull/79943)
- 组提供程序（group provider）文件 I/O 未受限制，并且被重放的提供程序在预热完成前未被桥接。[#79896](https://github.com/StarRocks/starrocks/pull/79896)
- 当 Compaction 等待 apply 时，BE 在主键表 tablet 的 `get_compaction_status` 中崩溃。[#80167](https://github.com/StarRocks/starrocks/pull/80167)
- 在缓存驱逐（eviction）时文件系统被断开连接。[#80087](https://github.com/StarRocks/starrocks/pull/80087)
- 一次 0 行的提交将整张表计为已导入的行数。[#80182](https://github.com/StarRocks/starrocks/pull/80182)
- 混合了捆绑语句和独立语句的多语句事务未被发布。[#79894](https://github.com/StarRocks/starrocks/pull/79894)
- 一个已完成的事务可能在 `finalStatusTransactionStateDeque` 中被多次入队。[#79988](https://github.com/StarRocks/starrocks/pull/79988)
- 模式解析、推断、写入器和读取器中 Parquet 嵌套类型的边界情况。[#80085](https://github.com/StarRocks/starrocks/pull/80085)
- 带有特殊字符的结构体（struct）类型转换的 Variant 路径。[#74168](https://github.com/StarRocks/starrocks/pull/74168)
- 在空值分组键上，NULL 传递的运行时过滤器被下推到了 `RepeatNode` 之下。[#80051](https://github.com/StarRocks/starrocks/pull/80051)
- 主键去重重写丢弃了其他聚合。[#79895](https://github.com/StarRocks/starrocks/pull/79895)
- Iceberg 表缓存刷新可能会覆盖并发的失效操作。[#79968](https://github.com/StarRocks/starrocks/pull/79968)
- 带有 `SET_VAR` 提示的 `custom_query_id` 没有记录在审计日志中。[#79871](https://github.com/StarRocks/starrocks/pull/79871)
- JSON 扩展列被注册到共享的 catalog 表上。[#78611](https://github.com/StarRocks/starrocks/pull/78611)
- 预处理语句在 `COM_STMT_CLOSE` 上发生泄漏。[#79860](https://github.com/StarRocks/starrocks/pull/79860)
- range-colocate 桶分配过时的查询没有被重新规划。[#79927](https://github.com/StarRocks/starrocks/pull/79927)
- MV 落盘 I/O 没有保持存活直到多播交换完成。[#79899](https://github.com/StarRocks/starrocks/pull/79899)
- 集合操作或 null-safe join 的 range 子节点前面的 exchange 被移除了。[#79901](https://github.com/StarRocks/starrocks/pull/79901)
- 当事件调度器关闭时，BE 在 `LookUpOperator` 中崩溃。[#79711](https://github.com/StarRocks/starrocks/pull/79711)
- `sys.fe_memory_usage` 读取了一个空 map 而不是进行收集。[#79809](https://github.com/StarRocks/starrocks/pull/79809)
- 落盘排序的完成没有被同步，完成时的错误也没有被保留。[#79521](https://github.com/StarRocks/starrocks/pull/79521)
- 压缩的分组键序列化器不接受仅为 null 的常量。[#79499](https://github.com/StarRocks/starrocks/pull/79499)
- 物化视图改写在投影中留下了一个聚合。[#79611](https://github.com/StarRocks/starrocks/pull/79611)
- ADLS2 账户作用域没有被保留，其凭据也没有被脱敏。[#79526](https://github.com/StarRocks/starrocks/pull/79526)
- `QueryCumulativeCpuTime` 和 `QuerySpillBytes` 在每个 fragment 实例中被计数了一次。[#79774](https://github.com/StarRocks/starrocks/pull/79774)
- 当 `LIMIT` 偏移量超出末尾时，`SHOW` 语句没有返回空结果。[#79764](https://github.com/StarRocks/starrocks/pull/79764)
- 当一个导入落盘合并任务被取消时，完成回调没有得到响应。[#79360](https://github.com/StarRocks/starrocks/pull/79360)
- `SHOW ALTER TABLE OPTIMIZE` 没有针对其自身的列解析 `ORDER BY`。[#79687](https://github.com/StarRocks/starrocks/pull/79687)
- 安全集成（LDAP）用户无法在非 MySQL 通道上进行身份验证。[#79165](https://github.com/StarRocks/starrocks/pull/79165)
- `EvHttpServer` 在关闭时关闭了无关的文件描述符。[#79582](https://github.com/StarRocks/starrocks/pull/79582)
- 在 proc 目录的行成为答案的情况下，没有检查其行宽度。[#79619](https://github.com/StarRocks/starrocks/pull/79619)
- 非正数的 `compact_threads` 导致了 CN 崩溃，而不是被拒绝。[#79641](https://github.com/StarRocks/starrocks/pull/79641)
- `TableKeeper` 导入了 `jdk.jshell`。[#79520](https://github.com/StarRocks/starrocks/pull/79520)

## 4.1.6 {/* #416 */}

发布日期：2026 年 9 月 30 日

### 行为变更 {/* #behavior-changes-2 */}

- AWS SDK 的默认 HTTP 客户端从 Poco 切换回 SDK 自带的客户端：`enable_poco_client_for_aws_sdk` 现在默认值为 `false`。将其设置为 `true` 可恢复之前的行为。[#78375](https://github.com/StarRocks/starrocks/pull/78375)
- 在线 optimize table 现在是选择性启用的：FE 配置项 `enable_online_optimize_table` 默认值为 `false`，因此 `ALTER TABLE ... DISTRIBUTED BY ...` 会再次阻塞写入，除非该配置项被开启。[#78206](https://github.com/StarRocks/starrocks/pull/78206)
- jemalloc 专用的超大内存区（oversize arena）默认启用，因此大内存分配不再落入调用线程的 per-CPU arena 中。这改善了大缓冲区的复用并减少了碎片化，同时改变了 BE 和 CN 内存在各 arena 之间的分布方式。[#78396](https://github.com/StarRocks/starrocks/pull/78396)
- 存算分离的 tablet 元数据和事务日志文件现在默认使用 Adler-32 校验和写入（`lake_enable_protobuf_file_checksum`），因此可以在读取时检测到损坏。只要文件带有校验和，读取方就已经会对其进行验证，因此无需降级步骤。[#75334](https://github.com/StarRocks/starrocks/pull/75334)
- 存算分离的主键表导入在为同一个键混合 `UPSERT` 和 `DELETE` 行时，现在默认（`lake_enable_pk_preserve_txn_delete_order`）会保留它们在事务内的顺序。[#76211](https://github.com/StarRocks/starrocks/pull/76211)
- 多表事务现在参与存算分离的批量发布，而不是一次发布一个事务，这消除了之前 CDC 微批次扇出到多张表时会遇到的序列化问题。[#77029](https://github.com/StarRocks/starrocks/pull/77029)
- 存算分离的分区现在在一次批量发布中直接推进到最终版本，而不是经过每一个中间版本，因此查询不能再请求一个没有 tablet 元数据对象的中间版本，该问题之前表现为 `The specified key does not exist`。[#79175](https://github.com/StarRocks/starrocks/pull/79175)
- 存算分离下持续失败的发布现在会退避并限制其重试日志的输出频率，而不是在每次尝试时都输出 `previous publish version task has not finished` 消息，后者在故障期间可能会占用 FE leader 的大量 CPU 和日志空间。[#79299](https://github.com/StarRocks/starrocks/pull/79299)
- Del-file 墓碑行现在计入 `cloud_native_pk_index_rebuild_rows_threshold`，因此删除密集型的存算分离工作负载能够按预期触发主键索引的提前刷盘。[#76222](https://github.com/StarRocks/starrocks/pull/76222)
- 存算分离的列更新式 upsert 发布在替代自身 Segment 时，现在会合并 Delete Vector，而之前这可能会为同一个主键留下两行有效数据。该问题的影响只会在很久之后才显现出来，即当 CN 重启并重建持久化索引时，报告了 `insert found duplicate key`。[#78797](https://github.com/StarRocks/starrocks/pull/78797)
- 存算分离的主键索引 Compaction 器不再在刷盘端持续生成 sstable 的同时悄无声息地停止，该问题会导致 tablet 的持久化索引无限增长。[#78812](https://github.com/StarRocks/starrocks/pull/78812)
- HDFS 文件在关闭之前不再被 `hsync`。`hsync` 原本是针对一个已被单独修复的崩溃问题的临时变通方案，而且它会在每次关闭时都对每个 DataNode 副本强制执行一次 `fsync()`。[#78864](https://github.com/StarRocks/starrocks/pull/78864)
- 在解码和裁剪过程中，低报了 NULL 数量的 Parquet `null_count` 不再被直接采信。由较旧版本 parquet-mr 写入的文件之前会返回损坏的值或丢失行。[#78329](https://github.com/StarRocks/starrocks/pull/78329)
- 对 `!=`、`NOT IN` 和 `IS NOT NULL` 进行 List 分区裁剪时，不再仅因为某个多值分区的取值列表中也包含 `NULL` 就将该分区丢弃。针对此类表的查询，包括在连接键上推导出 `IS NOT NULL` 的 JOIN，现在会返回之前丢失的非空行。[#78946](https://github.com/StarRocks/starrocks/pull/78946)
- 批量 DDL 现在可以构建起始年份为 `0000` 的日期范围分区，这是一个合法的 `DATE` 值。此类语句之前会因 `the range of the partition is invalid` 而失败。[#78746](https://github.com/StarRocks/starrocks/pull/78746)
- `max_partitions_in_one_batch` 及分区数限制现在统计的是当前请求即将创建的分区数。超大批量会被提前拒绝，而不是被放行后只在下一批次才失败。[#79379](https://github.com/StarRocks/starrocks/pull/79379)
- `TRUNCATE TABLE <external_catalog>.<db>.<table>` 现在根据语句中指定的 catalog 检查 `DELETE` 权限，而不是根据会话当前的 catalog。此前被允许访问的用户现在可能被拒绝，反之亦然。[#79047](https://github.com/StarRocks/starrocks/pull/79047)
- 流式聚合调优现在依据分组数的基数来驱动，而不是依据 `HAVING` 之后剩余的行数，因此当查询的 `HAVING` 子句过滤掉大部分分组时，不会再丢失本地预聚合。[#79064](https://github.com/StarRocks/starrocks/pull/79064)
- 审计日志行现在对针对外部 Iceberg 或 Hive 表、`FILES()` 表函数的 DML，以及异步物化视图刷新，携带 `scanBytes`、`scanRows`、`returnRows`、`cpuCostNs` 和 `memCostBytes` 字段。这些字段此前报告的是 `0`。[#74072](https://github.com/StarRocks/starrocks/pull/74072) [#74954](https://github.com/StarRocks/starrocks/pull/74954)
- Query Profile 和 `EXPLAIN ANALYZE` 现在报告的是重试查询实际执行的计划，而不是首次尝试时的计划。[#79225](https://github.com/StarRocks/starrocks/pull/79225)
- Broker Load 不再将其提交时对 `enable_tablet_pre_split` 的快照写回提交客户端的活动会话变量。[#79177](https://github.com/StarRocks/starrocks/pull/79177)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 现在可以再次用于非聚合增量物化视图；由存储填充的 `__ROW_ID__` 列不再出现在物化视图的 DDL 列列表中。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- `SHOW ALTER TABLE OPTIMIZE` 的 `Operation` 列现在显示可读的优化操作描述，而不是像 `com.starrocks.sql.ast.OptimizeClause@b5dc069` 这样的内部对象地址。[#75948](https://github.com/StarRocks/starrocks/pull/75948)
- 当 tablet 模式中缺少某列时，在谓词下推期间不再导致 BE 中止；该谓词将被简单地跳过而不下推。[#79234](https://github.com/StarRocks/starrocks/pull/79234)
- 当 `OlapTableSink` 初始化失败时，部分初始化的 sink 不再被带入完整的关闭流程（该流程可能解引用从未创建过的状态并导致 BE 中止）。现在会直接返回原始的初始化错误。[#79370](https://github.com/StarRocks/starrocks/pull/79370)
- 短键索引页的几何结构现在在校验时不会发生 32 位溢出，因此损坏的页脚会被拒绝，而不是发生回绕并导致堆越界写入。[#79048](https://github.com/StarRocks/starrocks/pull/79048)
- TLS 握手超时的 HTTPS 请求不再被视为已连接并在同一 socket 上重试。停滞的请求现在会在大约一个超时周期后失败，而不是在 `connect_timeout` 加上两个接收超时之后才失败。[#79369](https://github.com/StarRocks/starrocks/pull/79369)

### 功能优化 {/* #improvements-2 */}

- 现在可以通过用户的 `memberOf` 属性解析 LDAP 和 Active Directory 组，因此新建的目录组不再需要在集群上配置匹配的组提供程序。[#79254](https://github.com/StarRocks/starrocks/pull/79254)
- 审计日志现在会记录查询的错误消息，而不仅仅是粗粒度的 `ErrorCode`，因此语法错误、未知表以及权限失败在不关联回 `fe.log` 的情况下也能被区分开。[#78798](https://github.com/StarRocks/starrocks/pull/78798)
- 新增了服务器级别的落盘指标，可按本地与远端存储后端划分，汇总 all 个可落盘算子的落盘活动。[#71827](https://github.com/StarRocks/starrocks/pull/71827)
- Hudi 元数据读取器现在会在一次扫描的各个分区之间复用，而不是按分区重建，这在启用 Hudi 元数据表时缩短了拥有大量分区的表的规划时间。[#79367](https://github.com/StarRocks/starrocks/pull/79367)
- Tablet 的分裂点现在根据 segment 现有的索引计算，而不是依赖专门的完整排序键索引页，因此从未发生分裂的表不再需要为写入和存储该页付出代价。[#79050](https://github.com/StarRocks/starrocks/pull/79050)
- `/memz` 现在支持 jemalloc 自身的统计选项，使得每个 arena 的 dirty 和 muzzy 页计数以及 extent 大小类别明细可以从运行中的 BE 中获取。[#79090](https://github.com/StarRocks/starrocks/pull/79090)

### 安全性 {/* #security-2 */}

- [CVE-2026-55971] 将 Apache Thrift 升级到 0.24.0，以修复 C++ 绑定中 `THeaderTransport::untransform()` 的基于堆的缓冲区溢出问题。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-42537, CVE-2026-59646] 将 Apache Ranger 升级到 2.9.0，以修复通过 JDBC URL 注入造成的远程代码执行漏洞；将 Bouncy Castle 升级到 1.85，以修复 DTLS 重组器缓冲区溢出问题。[#79331](https://github.com/StarRocks/starrocks/pull/79331)
- [CVE-2026-71290] 将 Apache HttpClient5 升级到 5.6.4。[#79538](https://github.com/StarRocks/starrocks/pull/79538)

### 问题修复 {/* #bug-fixes-2 */}

修复了以下问题：

- `SHOW PROFILELIST`、`ANALYZE PROFILE`、`get_query_profile()` 和 `/api/profile` 会将所有缓存的 profile（包括其他用户的 SQL 文本和计划）返回给任何已认证用户。新增的 FE 配置项 `authorization_enable_query_profile_access_check` 将缓存的 profile 限制为仅供运行该查询的用户以及拥有 `SYSTEM OPERATE` 权限的用户访问。[#79375](https://github.com/StarRocks/starrocks/pull/79375)
- 权限检查、`INSERT` 规划、视图展开、权限校验以及 `inspect_mv_refresh_info` 在持有 FE 元数据锁的情况下进行了外部 catalog 往返调用，因此一个缓慢的 metastore 会导致所有等待同一把锁的 DDL 操作全部阻塞。现在这些调用都在锁外执行。[#79319](https://github.com/StarRocks/starrocks/pull/79319) [#79226](https://github.com/StarRocks/starrocks/pull/79226)
- gtid 和分区版本 epoch 可能会在非 Leader FE 上生成，因此两个节点可能在同一时刻生成相同的编号。[#78799](https://github.com/StarRocks/starrocks/pull/78799)
- `FULL OUTER JOIN ... USING (c1, c2, ...)` 返回了错误的值：第一个 `USING` 列的结果是正确的，但之后的每一列都携带了前一列的值。在同一查询中添加 `ORDER BY <ordinal>` 会因 `IndexOutOfBoundsException` 而失败。[#79298](https://github.com/StarRocks/starrocks/pull/79298)
- 当 `GROUPING SETS`、`ROLLUP` 或 `CUBE` 被改写为 union 时，`HAVING` 谓词会丢失，导致查询返回了本应被过滤掉的行。[#79415](https://github.com/StarRocks/starrocks/pull/79415)
- `GROUP BY ROLLUP(a, b, a)`，以及 `CUBE` 或 `GROUPING SETS` 在同一集合内对同一列命名两次时，会生成携带同一键两次的分组集。[#78261](https://github.com/StarRocks/starrocks/pull/78261)
- 在聚合键表之上移除聚合操作时，查询的 `LIMIT` 会在到达存储层之前被丢弃，导致 BE 读取整张表。[#79414](https://github.com/StarRocks/starrocks/pull/79414)
- 将 `OR` JOIN 拆分为 union 时，各分支的排列不一致：union 的输出列和第一个分支按列 id 排序，而其他分支则遵循投影映射的迭代顺序。[#79473](https://github.com/StarRocks/starrocks/pull/79473)
- 倾斜 JOIN 改写会复制外连接中被保留的一侧，导致一行输入可能被多次返回，结果在不同次运行间发生变化。其 salt 表也接收到了顺序错误的 `generate_series` 参数，导致该序列未产生任何输出。[#79168](https://github.com/StarRocks/starrocks/pull/79168) [#79268](https://github.com/StarRocks/starrocks/pull/79268)
- `SELECT DISTINCT` 接受了对 `DISTINCT` 会删除的列所施加的 `ORDER BY`，而窗口下方的去重下推会在投影中留下已被删除的列。[#77217](https://github.com/StarRocks/starrocks/pull/77217) [#77218](https://github.com/StarRocks/starrocks/pull/77218)
- 对 `bitmap_union_count()` 这类始终非空的聚合函数进行窗口计算时会丢失其声明的可空性，导致对其应用 `DISTINCT` 或 `GROUP BY` 时因 `error nullablel column` 而失败。[#79009](https://github.com/StarRocks/starrocks/pull/79009)
- `percentile_cont` 合并时是按值而不是按位置跟踪耗尽状态，这可能导致 BE 中止。[#77261](https://github.com/StarRocks/starrocks/pull/77261)
- `count(DISTINCT percentile_hash(...))`，以及任何对 `PERCENTILE` 值执行的 `DISTINCT` 聚合，都会导致 BE 崩溃；此类调用现已被拒绝。[#78795](https://github.com/StarRocks/starrocks/pull/78795)
- 标量 agg-state 组合器在内存池为空时会崩溃，并且在其嵌套函数上下文上存在竞态问题。[#76910](https://github.com/StarRocks/starrocks/pull/76910)
- 对聚合表执行 JSON 子字段聚合时会因原始的 `NullPointerException` 而失败，因为合成的子字段列没有聚合类型。[#77952](https://github.com/StarRocks/starrocks/pull/77952)
- 在 `enable_spill = true` 下，流式预聚合可能会耗尽内存，因为受限的内存预算一经锁定便再也不会更新。[#76702](https://github.com/StarRocks/starrocks/pull/76702)
- 在恢复构建分区期间被取消的溢出哈希连接会导致 BE 崩溃，因为探测算子在未等待溢出 I/O 任务完成的情况下就关闭了。[#79085](https://github.com/StarRocks/starrocks/pull/79085)
- 接收端被取消后，本地直通交换仍会继续追加数据块，并且由于 sink 和接收端没有共享所有权，其通道可能在释放后被使用。sink 就绪通知也选择了 sink 可观察对象却发出了 source 事件，因此阻塞在 `OUTPUT_FULL` 上的 driver 可能一直处于阻塞状态。[#76603](https://github.com/StarRocks/starrocks/pull/76603) [#77088](https://github.com/StarRocks/starrocks/pull/77088) [#76782](https://github.com/StarRocks/starrocks/pull/76782)
- Query Cache 可能存储一个不完整的 per-tablet 结果并将其提供给后续查询，并且包含 ASOF 连接的片段即使时间条件从未进入摘要也会被缓存。[#77066](https://github.com/StarRocks/starrocks/pull/77066) [#77563](https://github.com/StarRocks/starrocks/pull/77563)
- 当 ASOF 连接的时间条件不比较两侧时会导致 BE 崩溃；现在会拒绝此类条件。[#79049](https://github.com/StarRocks/starrocks/pull/79049)
- 在构建运行时过滤器时，没有父元组的嵌套 lambda slot 被误认为已被 TopN 节点的元组集合绑定，而一个 order-by 提示作为重量级表达式下推到扫描中时，解引用了一个没有列的 slot。[#78050](https://github.com/StarRocks/starrocks/pull/78050) [#78527](https://github.com/StarRocks/starrocks/pull/78527)
- 嵌套的 `array_map` 和 `array_sort` lambda 在与 `Input dependency cols check failed` 或 `Expr evaluate meet error: slot_id N not found` 一起使用时无法规划或求值，因为内层 lambda 提升出的公共子表达式逃逸了定义它们的作用域。[#78645](https://github.com/StarRocks/starrocks/pull/78645) [#78769](https://github.com/StarRocks/starrocks/pull/78769) [#78915](https://github.com/StarRocks/starrocks/pull/78915)
- `array_sort` 的比较器校验在多个 driver 间共享同一个错误 `Status`，这可能导致其被重复释放。[#78314](https://github.com/StarRocks/starrocks/pull/78314)
- `array_contains(NULL, [1, 2])` 和 `array_position(NULL, [1, 2])` 失败并报 `class com.starrocks.type.NullType cannot be cast to class com.starrocks.type.ArrayType`。[#76970](https://github.com/StarRocks/starrocks/pull/76970)
- 分析 `__iceberg_transform_truncate` 或 `__iceberg_transform_bucket` 时会就地修改共享的内建函数对象，将第一次分析的查询的精度和标度印到其通配符小数签名上。[#76777](https://github.com/StarRocks/starrocks/pull/76777)
- 参数列表为空的 `PARTITION BY RANGE(<partition-function>())` 会抛出一个原始的 `IndexOutOfBoundsException`，而不是解析错误。[#75080](https://github.com/StarRocks/starrocks/pull/75080)
- 无论表达式是否保序以及保序方向如何，范围谓词都会被映射到分区表达式上，因此 Range 分区和 List 分区裁剪可能会误删持有匹配行的分区。`NULL` 分区键也会在该列类型的最小值处而非严格小于该值处被查找。将 `DATE` 或 `DATETIME` 转换为文本的保留条件现已被重新接受。[#78024](https://github.com/StarRocks/starrocks/pull/78024) [#79111](https://github.com/StarRocks/starrocks/pull/79111) [#79187](https://github.com/StarRocks/starrocks/pull/79187) [#79257](https://github.com/StarRocks/starrocks/pull/79257) [#79688](https://github.com/StarRocks/starrocks/pull/79688)
- Iceberg 谓词转换器无条件地剥离了 `CastOperator`，因此对 `DECIMAL(5,2)` 列使用 `WHERE CAST(d AS BIGINT) = 10` 可能会裁剪掉持有 `10.50` 的文件。[#78351](https://github.com/StarRocks/starrocks/pull/78351)
- 由于增量边界推进到了被跳过的 `REPLACE` 快照，常规的 `rewrite_data_files` 压缩会使基于 Iceberg 基表的增量物化视图被永久性地置为不可用。[#76950](https://github.com/StarRocks/starrocks/pull/76950)
- 分区或 schema DDL 不会使列最小值/最大值统计信息缓存失效，因此可能会无限期地提供过期条目。[#77441](https://github.com/StarRocks/starrocks/pull/77441)
- 当 metastore 或 AWS Glue 拒绝读取列统计信息的权限时，Hive 系列 catalog 会直接失败；现在它们会降级为空统计信息。Glue 列统计信息请求现在也会携带 `catalogId`，因此跨账户部署会从配置的 catalog 读取统计信息。[#78455](https://github.com/StarRocks/starrocks/pull/78455) [#78461](https://github.com/StarRocks/starrocks/pull/78461)
- Elasticsearch 外部表和 Elasticsearch catalog 对运行版本 8 或 9 的集群会直接拒绝并报 `Unsupported/Unknown ES Cluster version`，并且并发查询可能会互相覆盖请求 URL 参数，这会表现为诸如 `Column 'id' cannot be resolved` 之类看似不相关的分析错误。[#78267](https://github.com/StarRocks/starrocks/pull/78267) [#79391](https://github.com/StarRocks/starrocks/pull/79391)
- Paimon 表的 `DECIMAL` 列上的谓词使用了 StarRocks 类型系列的默认精度和标度进行转换，而不是使用该列的 Paimon `DecimalType`，导致字面量无法表示该值。[#78760](https://github.com/StarRocks/starrocks/pull/78760)
- 当 endpoint 配置为完整 URL 时，Azure Blob 凭据查找会失败，因为 Hadoop 配置键中嵌入了协议方案（scheme）。ADLS2 存储卷在序列化时也会丢失其 Azure Workload Identity 令牌文件属性，因此重新加载该存储卷会回退到 Managed Identity 并因 `Identity not found` 而失败。[#78241](https://github.com/StarRocks/starrocks/pull/78241) [#79444](https://github.com/StarRocks/starrocks/pull/79444)
- 带有数组索引（如 `'[0].value.url'`）的 JSON 路径会对每一行数组长度短于该索引的数据抛出 C++ 异常，这会使大规模扫描变成栈展开器内部的锁车队（lock convoy）。现在该索引已加入边界检查。[#78569](https://github.com/StarRocks/starrocks/pull/78569)
- 当两个参数落在同一个月或同一年且第二个参数更早时，`timestampdiff(MONTH, ...)`、`timestampdiff(YEAR, ...)`、`months_diff` 和 `years_diff` 返回了 `-1` 而不是 `0`。[#79037](https://github.com/StarRocks/starrocks/pull/79037)
- 对 `CAST(<varbinary constant> AS VARCHAR)` 的常量折叠返回的是底层字节数组的 Java 身份哈希码（例如 `[B@233c25e5`），而不是该值本身。[#77718](https://github.com/StarRocks/starrocks/pull/77718)
- 当输入列缓存的 `has_null` 标志保守地保持为 true 时，`DICTIONARY_GET` 会将非 NULL 键误判为 NULL。[#76881](https://github.com/StarRocks/starrocks/pull/76881)
- 即使重写器从不对嵌套循环连接的 `ON` 列进行解码，低基数字典仍会被保留在这些列上；字典表达式会通过携带自身编码的派生字典被折叠；并且一个“滚动低基数”列会反复使全局字典失效并强制进行全表重新采集。[#78229](https://github.com/StarRocks/starrocks/pull/78229) [#78633](https://github.com/StarRocks/starrocks/pull/78633) [#78486](https://github.com/StarRocks/starrocks/pull/78486)
- `DECIMAL256` 字面量是根据其字符串表示进行哈希的，而 BE 对行进行分桶时使用的是原始 32 字节值，因此对按 `DECIMAL256` 列分布的表进行点查询时被发送到了错误的分桶。[#78495](https://github.com/StarRocks/starrocks/pull/78495)
- OLAP 扫描是按列名而非列 id 来命名其排序键列的，因此重命名后的列不再与 tablet 的 schema 匹配。[#77967](https://github.com/StarRocks/starrocks/pull/77967)
- `PruneComplexTypeUtil` 一旦遇到一个它无法描述的表达式，就会为该语句中的所有列禁用复杂类型子字段裁剪。现在该放弃行为仅限于导致该问题的表达式本身，并且会丢弃因谓词表达式重用而从扫描中引出的无用子字段 slot。[#79181](https://github.com/StarRocks/starrocks/pull/79181) [#77456](https://github.com/StarRocks/starrocks/pull/77456)
- `AstToSQLBuilder` 以双引号打印了 `SELECT * EXCLUDE` 列表中的列，而语法在任何地方都不接受这种写法，因此重新生成的 SQL 无法被解析。[#78052](https://github.com/StarRocks/starrocks/pull/78052)
- 在聚合表上，当既未给出聚合函数也未给出 `KEY` 关键字时，`ALTER TABLE ... ADD COLUMN` 会悄悄创建一个 key 列，从而改变了该表聚合的粒度。此类语句现在会因存在歧义而被拒绝。[#78359](https://github.com/StarRocks/starrocks/pull/78359)
- 类型没有 BE key 编码器的列（如 `JSON`）曾可被用作排序键并导致 BE 崩溃；现在在所有 all 分布类型下，`CREATE TABLE` 和 `ALTER TABLE` 中的此类定义都会被拒绝。[#76795](https://github.com/StarRocks/starrocks/pull/76795)
- `USING GIN("IMP_LIB" = "builtin")` 被 `CREATE TABLE` 接受后，会破坏所有带有 `Can not get inverted imp type` 的 `INSERT`，因为该属性键以其原始大小写形式传到了 BE。[#77818](https://github.com/StarRocks/starrocks/pull/77818)
- 一个在未持久化其 `gram_num` 属性的情况下保留下来的旧版 `NGRAMBF` 索引，会使 BE 在构建 segment 时读取越过 UTF-8 偏移向量并导致 CN 崩溃。[#76989](https://github.com/StarRocks/starrocks/pull/76989)
- 当下推谓词读取一个稀疏范围时，空值标志是按线性索引而非按序号索引的，因此间隙之后的每一行都会使用被跳过行的空值标志。当下推过滤器拒绝整个字典时，字典页解码器也未能前进，这导致了 BE 中止。[#78389](https://github.com/StarRocks/starrocks/pull/78389) [#79007](https://github.com/StarRocks/starrocks/pull/79007)
- 子表达式错误导致 BE 中止，而不是被正确传播。[#77442](https://github.com/StarRocks/starrocks/pull/77442)
- `ThreadPool` 默认会吞掉任务异常，并将该任务计为已完成。[#76863](https://github.com/StarRocks/starrocks/pull/76863)
- 启用 Jaeger 追踪时，通过 `OlapTableSink` 取消导入会导致 BE 或 CN 出现 SIGSEGV 崩溃。[#76869](https://github.com/StarRocks/starrocks/pull/76869)
- `AsyncFlushOutputStream::close()` 在仍持有该流自身互斥锁的情况下满足了其 promise，因此在等待方销毁该流之后，I/O 线程会在已释放的内存上唤醒 futex。[#79182](https://github.com/StarRocks/starrocks/pull/79182)
- 在 `shutdown_staros_worker()` 释放全局 worker 之后，如果一个正在进行中的导入访问了基于 StarOS 的文件系统，CN 可能会在优雅关闭期间以 SIGSEGV 崩溃。[#79058](https://github.com/StarRocks/starrocks/pull/79058)
- 并行 Compaction 在 brpc bthread 上运行了文件系统 I/O，导致 CN 因 `Resource deadlock avoided` 而中止。[#76925](https://github.com/StarRocks/starrocks/pull/76925)
- 当一个向量索引 rowset 报告了负数的 `data_disk_size` 时，Compaction 会因 SIGFPE 而中止。[#78959](https://github.com/StarRocks/starrocks/pull/78959)
- 当 lake 事务日志读取发生损坏时，损坏的本地缓存副本会在每次重试时被重新读取，而主键索引 sstable 块读取从未校验写入方一贯生成的 crc32c 块尾部。[#78733](https://github.com/StarRocks/starrocks/pull/78733) [#77479](https://github.com/StarRocks/starrocks/pull/77479)
- 退化为拷贝的部分列更新重写操作丢失了源文件的加密元数据，导致生成的 segment 无法被任何读取方打开，Compaction 也无法重写该 segment。[#79053](https://github.com/StarRocks/starrocks/pull/79053)
- 存算分离 schema change 可能记录一个不引用任何数据的 rowset，此后该分区上的每次 `MERGE_TABLET` 都会以 `tablet merge source has an empty ordinary rowset` 失败。[#79167](https://github.com/StarRocks/starrocks/pull/79167)
- Range-colocate 的拆分边界被当作序列化的 protobuf 字节进行比较，而 FE 和 BE 的编码方式并不一致。[#76968](https://github.com/StarRocks/starrocks/pull/76968)
- 对处于不稳定 range-colocate 分组中的成员进行普通单表查询会以 `range colocate group ... is in an unaligned state` 失败。[#76971](https://github.com/StarRocks/starrocks/pull/76971)
- hash-colocate 存算分离表新建物理分区的分片在创建时未携带 colocation 放置信息，事后必须进行迁移。[#79223](https://github.com/StarRocks/starrocks/pull/79223)
- FE 未告知 BE 某个已删除的 tablet 所属表是按 range 分布的，导致在重新分片后出现文件泄漏。[#76861](https://github.com/StarRocks/starrocks/pull/76861)
- 重放重新分片任务的更新操作未刷新正在重新分片的 tablet，因此在 FE leader 切换后，某个分区的发布操作可能在一个从未写入的 tablet 元数据对象上永久返回 404。重放重新分片任务时也可能写入 edit log，即使该表已被删除；此外，重新分片发布的重试操作是空转而非按节奏执行。[#79426](https://github.com/StarRocks/starrocks/pull/79426) [#79485](https://github.com/StarRocks/starrocks/pull/79485) [#77691](https://github.com/StarRocks/starrocks/pull/77691)
- 存算分离分区版本在变更操作记录日志之前就已在内存中被修改，因此在重放日志时版本号可能发生回退。现在该版本号由日志推导得出，并且只会递增。[#79296](https://github.com/StarRocks/starrocks/pull/79296)
- 当目标 tablet 报告没有副本时，复制作业发生了除零错误。[#79329](https://github.com/StarRocks/starrocks/pull/79329)
- DataCache 的 key 哈希取自运行时分发表，因此该分发表的任何变更都会导致升级或回滚后所有缓存的块被孤立。现已固定使用 MurmurHash3。[#79262](https://github.com/StarRocks/starrocks/pull/79262)
- FE 的扫描范围堆安全检查按每个物理分区执行一次，而不是按每个扫描节点执行一次，导致其时间复杂度随物理分区数量呈平方级增长。[#76978](https://github.com/StarRocks/starrocks/pull/76978)
- 在延迟加载的索引增加了 segment 内存占用之后，rowset 元数据缓存条目仍保留其初始占用量，导致缓存低估了实际占用的内存大小。[#77893](https://github.com/StarRocks/starrocks/pull/77893)
- `Tablet::get_average_row_size()` 在未持有 `_meta_lock` 的情况下遍历了 `_rs_version_map`，违反了该 map 声明时约定的约束。[#79250](https://github.com/StarRocks/starrocks/pull/79250)
- FE 的 thrift 接受循环在工作线程池饱和时会停滞数分钟，而工作线程仍在继续处理那些早已排队、其调用方已经消失的连接。[#78838](https://github.com/StarRocks/starrocks/pull/78838) [#79173](https://github.com/StarRocks/starrocks/pull/79173)
- 某个日志应用器卡住的非 leader FE 仍在本地继续提供陈旧的元数据，而不是转发请求，因为就绪状态的判定只有在某次重放周期返回之后才会得出。[#79041](https://github.com/StarRocks/starrocks/pull/79041)
- `getLoads` RPC 会在单条消息中返回所有导入记录，导致繁忙集群的 leader 日志被 `FE RPC failure ... MaxMessageSize reached` 填满，并使 `loads_history` 归档回滚并永久丢失数据行。该 RPC 现已按作业 id 分页。[#78898](https://github.com/StarRocks/starrocks/pull/78898)
- 当某个任务运行在挂起队列、运行中 map、内存中历史记录和已归档历史记录之间迁移时，如果恰好读取了 `task_runs`，可能会被重复报告两次。[#79438](https://github.com/StarRocks/starrocks/pull/79438)
- `SHOW ALTER TABLE ROLLUP` 和 `SHOW ALTER MATERIALIZED VIEW` 接受 `WHERE`、`ORDER BY` 和 `LIMIT`，但随后忽略了它们；只有 `SHOW ALTER TABLE COLUMN` 真正应用了这些值。`FinishedTime` 现在也可以在 `SHOW ALTER TABLE ROLLUP` 谓词中被接受。[#79281](https://github.com/StarRocks/starrocks/pull/79281) [#79498](https://github.com/StarRocks/starrocks/pull/79498)
- 加载 SQL 黑名单时，会按每条规则重建并排序其不可变快照，这使得耗时随规则数量呈平方级增长，并且每次 FE 启动和执行 checkpoint 时都会运行。[#78752](https://github.com/StarRocks/starrocks/pull/78752)
- 消费者线程被中断后，`AsyncTaskQueue` 会在 `getOutputs()` 和 `tryGetOutputs()` 之间忙等自旋，原因是该中断信号被吞掉而未清除 `hasMoreOutput`。[#79297](https://github.com/StarRocks/starrocks/pull/79297)
- 对 `sys_log_level` 的运行时修改被写入了配置，但从未被实际应用，尽管该配置项文档中标明为可动态修改。[#79200](https://github.com/StarRocks/starrocks/pull/79200)
- `has_enable_heap_profile()` 将布尔类型的 jemalloc 节点 `prof.active` 读取为了 `int`，因此它始终报告堆分析（heap profiling）为禁用状态。[#78820](https://github.com/StarRocks/starrocks/pull/78820)
- 对脚本中外部对象执行 `System.print()` 会打印出 `instance of <Class>`，而不是对象的文本内容，原因是 `toString` 绑定被注册为方法而非 getter。[#78877](https://github.com/StarRocks/starrocks/pull/78877)
- 通过多表事务 Stream Load 接口向表达式分区表导入数据时，只要 BE 需要创建分区，提交阶段就会失败，并报出 `automatic create partition failed. error: txn ... not exist`。[#78790](https://github.com/StarRocks/starrocks/pull/78790)
- 在合并事务日志模式下，如果协调节点自身的流结束信号提前到达，并且该导入被取消，协调节点可能会无限期等待事务日志收集器。[#79389](https://github.com/StarRocks/starrocks/pull/79389)
- 一次已完成的 merge-commit 导入可能在 `information_schema.loads` 中报告出小于完整合并窗口的进度。[#78723](https://github.com/StarRocks/starrocks/pull/78723)
- 扫描 `SampleTime` 计数器报告的是块数量，而不是耗时。[#78732](https://github.com/StarRocks/starrocks/pull/78732)
- Arrow Flight SQL 服务的 FE classpath 中缺少 LZ4 和 ZSTD Arrow IPC 编解码器。[#76921](https://github.com/StarRocks/starrocks/pull/76921)
- 将 StarOS、starlet 和 starcache 升级至 v4.1.4。[#79429](https://github.com/StarRocks/starrocks/pull/79429)

## 4.1.5 {/* #415 */}

发布日期：2026 年 9 月 8 日

### 改进项 {/* #improvements-3 */}

- 外部表支持 `ANALYZE SAMPLE TABLE PREDICATE COLUMNS`。[#77243](https://github.com/StarRocks/starrocks/pull/77243)
- 扩展了基于采样的 tablet 预拆分功能：支持非 key 表达式、dynamic-overwrite 临时分区，以及 Iceberg 和 overwrite 导入。[#77843](https://github.com/StarRocks/starrocks/pull/77843) [#77842](https://github.com/StarRocks/starrocks/pull/77842) [#77983](https://github.com/StarRocks/starrocks/pull/77983)
- 对 lake 列更新的源 segment 采用流式处理，以降低内存占用。[#77275](https://github.com/StarRocks/starrocks/pull/77275)
- Paimon catalog 的后台元数据刷新现在只刷新最近被访问和发生变更的表，而不是每个周期都枚举整个数据仓库。[#78057](https://github.com/StarRocks/starrocks/pull/78057)
- 在分区裁剪过程中，将 `limit` 传递给 Paimon 的 `getRemoteFiles` 调用。[#76943](https://github.com/StarRocks/starrocks/pull/76943)
- 扩展了 GROUPING 的 SETS 下推功能，以支持 `AVG` 和 `COUNT`。[#77832](https://github.com/StarRocks/starrocks/pull/77832)
- 允许在 Query Profile 中包含 `EXPLAIN COSTS` 输出。[#73005](https://github.com/StarRocks/starrocks/pull/73005)
- 使运行时可配置 starlet 对象存储上传阈值。[#78448](https://github.com/StarRocks/starrocks/pull/78448)
- 减少锁争用：移除了 lake segment 的全局 metacache 锁，并使 SQL 黑名单验证实现无锁化。[#78260](https://github.com/StarRocks/starrocks/pull/78260) [#78476](https://github.com/StarRocks/starrocks/pull/78476)
- 在 Compaction 和复制归档时删除 `delete_predicate`，以回收 tablet 元数据。[#76870](https://github.com/StarRocks/starrocks/pull/76870)
- 使用 CRC32C 对存算分离的 delete 文件进行校验。[#77798](https://github.com/StarRocks/starrocks/pull/77798)
- 限制已取消查询等待未上报实例的最长时间。[#76909](https://github.com/StarRocks/starrocks/pull/76909)
- 多项 jemalloc 改进，以提升内存效率和可观测性：在 arm64 上除构建 64K 页版本外还构建 4K 页版本，暴露 dirty/muzzy 页以及 mapped/retained 指标，并支持在运行时应用可变的 `jemalloc_conf` 选项。[#77886](https://github.com/StarRocks/starrocks/pull/77886) [#78159](https://github.com/StarRocks/starrocks/pull/78159) [#78225](https://github.com/StarRocks/starrocks/pull/78225) [#78320](https://github.com/StarRocks/starrocks/pull/78320) [#78333](https://github.com/StarRocks/starrocks/pull/78333)
- 增加可观测性：在 leader FE 上新增 `max_journal_replay_lag` gauge 指标，以及额外的 lake publish-version 追踪。[#78382](https://github.com/StarRocks/starrocks/pull/78382) [#76658](https://github.com/StarRocks/starrocks/pull/76658) [#77048](https://github.com/StarRocks/starrocks/pull/77048)
- 任务完成后释放 reshard 输出分片的位置锁定。[#76945](https://github.com/StarRocks/starrocks/pull/76945)
- 将 StarOS 升级至 v4.1.2。[#77657](https://github.com/StarRocks/starrocks/pull/77657)

### 安全性 {/* #security-3 */}

- [CVE-2025-55163] 通过 `grpc-bom` 对齐 all `io.grpc` 构件，使 `grpc-netty` 与 `grpc-core` 保持一致。[#77171](https://github.com/StarRocks/starrocks/pull/77171)
- [CVE-2026-59902] 将 Netty 升级至 4.1.137.Final。[#78287](https://github.com/StarRocks/starrocks/pull/78287)
- [CVE-2026-54399] 将 Apache HttpCore 升级至 5.4.3，修复了 HTTP/1.1 消息解析器中的资源无限制消耗（DoS）漏洞。[#77753](https://github.com/StarRocks/starrocks/pull/77753)
- [CVE-2025-48924] 将 Broker 的 `commons-lang3` 升级至 3.20.0，并移除了未使用的 `commons-lang` 2.6 和 `commons-configuration` 1.6。[#77748](https://github.com/StarRocks/starrocks/pull/77748)
- [CVE] 基于更新版本重新构建了 pprof 二进制文件，并从 BE 容器镜像中移除了未使用的 flamegraph 工具。[#77846](https://github.com/StarRocks/starrocks/pull/77846)

### 问题修复 {/* #bug-fixes-3 */}

修复了以下问题：

**崩溃与稳定性**

- 当 fatal-signal 处理程序在将进程标记为崩溃后挂起时，强制 BE 退出。[#76491](https://github.com/StarRocks/starrocks/pull/76491)
- 修复了以下情况导致的 BE 崩溃：`tokenize`、`ngram_search`、`generate_series`、`corr`/`covar_pop`/`covar_samp` 中的常量/无效参数，`strpos`（INT32_MIN），以及对 decimal 数组执行 `count()`。[#77102](https://github.com/StarRocks/starrocks/pull/77102) [#77103](https://github.com/StarRocks/starrocks/pull/77103) [#77216](https://github.com/StarRocks/starrocks/pull/77216) [#77260](https://github.com/StarRocks/starrocks/pull/77260) [#77092](https://github.com/StarRocks/starrocks/pull/77092)
- 修复了当一个 `ALTER` 同时新增普通列和生成列时发生的 BE 崩溃，以及在 read-schema 重排序后出现的 cache-select 崩溃。[#77436](https://github.com/StarRocks/starrocks/pull/77436) [#77245](https://github.com/StarRocks/starrocks/pull/77245)
- 修复了查询缓存在分区哈希连接中复用 lane 时发生的 BE 崩溃，以及当某个 lane 的连接构建端为空时出现的挂起问题。[#77564](https://github.com/StarRocks/starrocks/pull/77564) [#77571](https://github.com/StarRocks/starrocks/pull/77571)
- 修复了 `JsonColumn` 追加时的越界读取和静默行丢失问题、`BinaryColumnBase::replicate` 中的整数溢出问题，以及当一个数组列被作为多个 lambda 参数传递时 `array_map` 的 DCHECK 失败问题。[#77090](https://github.com/StarRocks/starrocks/pull/77090) [#77545](https://github.com/StarRocks/starrocks/pull/77545) [#77433](https://github.com/StarRocks/starrocks/pull/77433)
- 修复了 `substr` 在遇到无效 UTF-8 时返回相邻行字节的问题，以及 `array_sort` 的 lambda 比较器导致排序越界的问题。[#78722](https://github.com/StarRocks/starrocks/pull/78722) [#77687](https://github.com/StarRocks/starrocks/pull/77687)
- 将 parquet 解码器的临时缓冲区移出栈空间，并在 `SimdJsonConverter` 错误路径中对 raw-JSON 视图加以边界限制。[#77615](https://github.com/StarRocks/starrocks/pull/77615)
- 修复了在没有上报存储介质的磁盘上出现的 tablet-调度器 NPE 问题，以及在反解析 `INSERT ... VALUES (DEFAULT, ...)` 时和优化器没有查询 ID 时在 `PrepareCollectMetaTask` 中出现的 NPE 问题。[#76966](https://github.com/StarRocks/starrocks/pull/76966) [#76963](https://github.com/StarRocks/starrocks/pull/76963) [#77051](https://github.com/StarRocks/starrocks/pull/77051)

**查询引擎与优化器**

- 修复了多个优化器路径中结果错误或缺失的问题：流式 distinct 全局 limit、跨排序分组的排名窗口预聚合、合并阶段重复追加的常量聚合值、未对每个子节点应用的 GROUP BY 规则，以及从 FOLLOWING 偏移开始的分析帧。[#77086](https://github.com/StarRocks/starrocks/pull/77086) [#77068](https://github.com/StarRocks/starrocks/pull/77068) [#77069](https://github.com/StarRocks/starrocks/pull/77069) [#77099](https://github.com/StarRocks/starrocks/pull/77099) [#77342](https://github.com/StarRocks/starrocks/pull/77342)
- 修复了 NL Join 中 null-aware 反连接丢失 probe 行的问题、NL Join probe 阶段对空 chunk 的谓词求值问题，以及基于 group-by 键进行连接的 `GROUPING SETS` 聚合因无可执行计划而失败的问题。[#77970](https://github.com/StarRocks/starrocks/pull/77970) [#77344](https://github.com/StarRocks/starrocks/pull/77344) [#77296](https://github.com/StarRocks/starrocks/pull/77296)
- 修复了 grouping-sets 转 union-all 重写顺序的问题、将 limit 重复为 Set operator 的问题，以及为避免挂起而将 Set operator 排除在 colocate 执行组之外的问题。[#77380](https://github.com/StarRocks/starrocks/pull/77380) [#77692](https://github.com/StarRocks/starrocks/pull/77692) [#77025](https://github.com/StarRocks/starrocks/pull/77025)
- 修复了窗口中 `PARTITION BY`/`ORDER BY` 的子查询规划问题，保留了为多列 `count(distinct)` 构建的 `if()` 的 decimal 类型，以及将扫描重表达式结果列按 slot 的可空性进行归一化。[#77087](https://github.com/StarRocks/starrocks/pull/77087) [#77346](https://github.com/StarRocks/starrocks/pull/77346) [#77242](https://github.com/StarRocks/starrocks/pull/77242)
- 修复了未定类型数组字面量获得错误类型（导致视图损坏）的问题、强制内联 limit 达到上限时的 CTE 锚定问题，以及递归 CTE 复用已推断 CTAS 列的问题。[#76986](https://github.com/StarRocks/starrocks/pull/76986) [#77018](https://github.com/StarRocks/starrocks/pull/77018) [#76453](https://github.com/StarRocks/starrocks/pull/76453)
- 通过在查询缓存摘要中加入扫描 schema、连接谓词公共子表达式以及 ANN 搜索规范，并在 `SortNode` 构建 TOPN 过滤器时禁用缓存，修复了多个查询缓存正确性问题。[#77559](https://github.com/StarRocks/starrocks/pull/77559) [#77565](https://github.com/StarRocks/starrocks/pull/77565) [#77586](https://github.com/StarRocks/starrocks/pull/77586) [#77404](https://github.com/StarRocks/starrocks/pull/77404)
- 修复了基于从未覆盖可见版本的行计数折叠 `COUNT(*)` 的问题、连接重排序中无条件的 UKFK 约束收集问题，以及在倒排索引过滤后未能保留 OR-驻留谓词所读取列的问题。[#77190](https://github.com/StarRocks/starrocks/pull/77190) [#77197](https://github.com/StarRocks/starrocks/pull/77197) [#77659](https://github.com/StarRocks/starrocks/pull/77659)
- 修复了日期时间解析正确性问题：将 Teradata 和 Joda 解析器限制在输入字符串范围内，并在 Julian 值超出范围时使 `DateValue::weekday()` 保持在有效范围内。[#77098](https://github.com/StarRocks/starrocks/pull/77098) [#77310](https://github.com/StarRocks/starrocks/pull/77310)
- 当批缓冲区会变得过大时，按行序列化 `EXCEPT`/`INTERSECT` 键，并在 Set 操作反序列化其键时重建 JSON 键列。[#77821](https://github.com/StarRocks/starrocks/pull/77821) [#77215](https://github.com/StarRocks/starrocks/pull/77215)
- 其他优化器/表达式修复：在解包 `StatusOr` 之前检查状态，在执行 property enforcement 时只判定一次属性满足性，在 `encode_fingerprint_sha256` 中读取索引 0 处的常量参数，在列长度非正数时跳过 `CHAR` 谓词的零填充，不将非正数的排名谓词下推到 TopN 中，在使用推导出的分区谓词进行裁剪时保留 NULL 分区，在 `_switch_context` 中停止越过输出 schema 末尾读取，使用相同的 NULL 约定对一个 run 进行排序并合并多个 run，在 prepare 可能失败之前先填充 `Analytor::_fns`，并停止强制将外部 CTAS 的 List 分区列转换为 `NOT NULL`。[#77052](https://github.com/StarRocks/starrocks/pull/77052) [#77091](https://github.com/StarRocks/starrocks/pull/77091) [#77104](https://github.com/StarRocks/starrocks/pull/77104) [#77444](https://github.com/StarRocks/starrocks/pull/77444) [#77445](https://github.com/StarRocks/starrocks/pull/77445) [#77596](https://github.com/StarRocks/starrocks/pull/77596) [#77660](https://github.com/StarRocks/starrocks/pull/77660) [#77679](https://github.com/StarRocks/starrocks/pull/77679) [#77758](https://github.com/StarRocks/starrocks/pull/77758) [#78468](https://github.com/StarRocks/starrocks/pull/78468)
- 修复了语句/反解析相关问题：为带列列表的 Iceberg/Hive `INSERT` 验证静态分区子句，在反解析分区引用时保留 `TEMPORARY` 限定符，支持 CTAS 反解析以修复 Profile 中空 SQL 语句的问题，并在 `proxyExecute` 中设置 `isSingleStmt`，使转发的语句保留原始 SQL。[#76659](https://github.com/StarRocks/starrocks/pull/76659) [#76987](https://github.com/StarRocks/starrocks/pull/76987) [#77978](https://github.com/StarRocks/starrocks/pull/77978) [#77982](https://github.com/StarRocks/starrocks/pull/77982)

**JSON 与数据类型**

- 修复了一个大型 `BinaryColumn` SerDe 问题、针对列名中含点号的列的统计信息收集问题，以及将 `multi_unnest`/`unnest` 数组偏移量读取为 `uint32` 而不是 `int32` 的问题。[#75504](https://github.com/StarRocks/starrocks/pull/75504) [#71301](https://github.com/StarRocks/starrocks/pull/71301) [#78479](https://github.com/StarRocks/starrocks/pull/78479) [#78480](https://github.com/StarRocks/starrocks/pull/78480)
- 修复了 Flat JSON 垂直 Compaction 内存使用问题，以及在通过 rowid 评估 struct 子字段谓词时读取值本身的问题。[#77682](https://github.com/StarRocks/starrocks/pull/77682) [#77891](https://github.com/StarRocks/starrocks/pull/77891)

**物化视图**

- 修复了 IVM 刷新在外部授权下的问题、MV 改写新鲜度基线问题（现在使用最后一次确认新鲜度的时间），以及跳过时间穿梭查询的 MV 改写问题。[#77210](https://github.com/StarRocks/starrocks/pull/77210) [#76758](https://github.com/StarRocks/starrocks/pull/76758) [#77683](https://github.com/StarRocks/starrocks/pull/77683)
- 修复了为在物化视图上创建的 GIN 索引分配索引 id 的问题，以及在基于视图的 MV 改写中替换回视图扫描投影时保留该投影的问题。[#77340](https://github.com/StarRocks/starrocks/pull/77340) [#77613](https://github.com/StarRocks/starrocks/pull/77613)
- 其他物化视图修复：校验 `default_mv_refresh_mode`、修复增量 MV 在扩展其主键而非排序时的 `ORDER BY` 问题，以及将 `information_schema.materialized_views` 内部查询限制在 `query_timeout` 范围内。[#77231](https://github.com/StarRocks/starrocks/pull/77231) [#77680](https://github.com/StarRocks/starrocks/pull/77680) [#77585](https://github.com/StarRocks/starrocks/pull/77585)

**数据湖连接器（Iceberg、Paimon、Delta、Hive）**

- 修复了 Iceberg 正确性和稳定性问题：在写入路径上强制执行 NOT NULL 列、根据计划时的快照校验改写提交、在 `TRUNCATE` 之后使元数据缓存失效、删除元数据文件缺失的表、在基于字符串分区的元数据删除转换前刷新内表、`rewrite_data_files` 带有 `WHERE` 子句时的静默数据丢失，以及 `remove_orphan_files` 中保留窗口过小的问题。[#77589](https://github.com/StarRocks/starrocks/pull/77589) [#77709](https://github.com/StarRocks/starrocks/pull/77709) [#77385](https://github.com/StarRocks/starrocks/pull/77385) [#77786](https://github.com/StarRocks/starrocks/pull/77786) [#77335](https://github.com/StarRocks/starrocks/pull/77335) [#77797](https://github.com/StarRocks/starrocks/pull/77797) [#77394](https://github.com/StarRocks/starrocks/pull/77394)
- 隔离了 Delta Lake 各表的 cloud 凭据。[#77424](https://github.com/StarRocks/starrocks/pull/77424)
- 支持 OpenX JsonSerDe 为 Hive 外部表进行列名映射，支持一个 JSON 字段映射到多个 Hive 列，并修复了连接器视图在缺少默认 catalog 及 CTE 引用时的表限定问题。[#65515](https://github.com/StarRocks/starrocks/pull/65515) [#77716](https://github.com/StarRocks/starrocks/pull/77716) [#77887](https://github.com/StarRocks/starrocks/pull/77887)
- 修复了外部表的 Ranger 视图/MV 授权问题，并在 FE classpath 上恢复了 Ranger Solr 审计目标。[#56316](https://github.com/StarRocks/starrocks/pull/56316) [#77294](https://github.com/StarRocks/starrocks/pull/77294)
- 修复了返回损坏的 `DELTA_BINARY_PACKED` 头而非抛出异常的问题，以及在 Parquet 读取器中将过滤器排除的 FLBA 字典行标记为 NULL 的问题。[#77446](https://github.com/StarRocks/starrocks/pull/77446) [#78685](https://github.com/StarRocks/starrocks/pull/78685)
- 修复了 Iceberg REST catalog 上 `ALTER TABLE RENAME` 的 NPE 问题、读取带有 decimal 或 null `timestamptz` 分区值的 Iceberg `$partitions` 时的 NPE 问题、跨 Iceberg catalog 共享单个 Hadoop `FileSystem` 的问题，以及重新遵循 `paimon.option.cache-enabled=false` 的问题。[#76498](https://github.com/StarRocks/starrocks/pull/76498) [#77742](https://github.com/StarRocks/starrocks/pull/77742) [#77297](https://github.com/StarRocks/starrocks/pull/77297) [#78271](https://github.com/StarRocks/starrocks/pull/78271)

**存算分离：Compaction、vacuum、reshard 与复制**

- Compaction 修复：缓存文件打包跳过时的 Compaction 事务日志、按最大 tablet 分数对候选项排序、修复读块大小估算问题、修复 Limiter 缩减时丢失令牌的问题，以及跳过仅删除写入时的发布期 Segment 重写。[#77578](https://github.com/StarRocks/starrocks/pull/77578) [#77482](https://github.com/StarRocks/starrocks/pull/77482) [#78192](https://github.com/StarRocks/starrocks/pull/78192) [#78087](https://github.com/StarRocks/starrocks/pull/78087) [#78071](https://github.com/StarRocks/starrocks/pull/78071)
- Vacuum 修复：修复了限期阻塞 vacuum 中的浪费读取和残留 v1 删除问题、将 autovacuum 保留下限保持在或高于 reshard 接管水平，以及避免冗余的 lake 元数据 404 探测。[#78460](https://github.com/StarRocks/starrocks/pull/78460) [#78545](https://github.com/StarRocks/starrocks/pull/78545) [#78466](https://github.com/StarRocks/starrocks/pull/78466)
- Reshard/split 修复：拒绝源 tablet 位于已被取代的物化索引中的 reshard、修复 tablet split 跨发布回退问题、消除预期中的 reshard 历史查找警告，以及防止 lake alter 作业在故障转移后卡住。[#77512](https://github.com/StarRocks/starrocks/pull/77512) [#78270](https://github.com/StarRocks/starrocks/pull/78270) [#78059](https://github.com/StarRocks/starrocks/pull/78059) [#78517](https://github.com/StarRocks/starrocks/pull/78517)
- 复制修复：保留共享 lake 文件、读取远端打包 lake 元数据，以及在复制期间重新加密源 lake 文件。[#77361](https://github.com/StarRocks/starrocks/pull/77361) [#77362](https://github.com/StarRocks/starrocks/pull/77362) [#78080](https://github.com/StarRocks/starrocks/pull/78080)
- 恢复损坏的本地缓存：在 delvec 页校验和失败时丢弃并重试，以及在 PK 索引 SST Compaction 和 tablet 合并 SSTable 重建出现损坏时重建。[#78086](https://github.com/StarRocks/starrocks/pull/78086) [#77481](https://github.com/StarRocks/starrocks/pull/77481) [#78003](https://github.com/StarRocks/starrocks/pull/78003)
- 修复了从分区共享对象读取 lake 版本 1 元数据的问题、保护快照拥有的共享分片组的问题，以及在 `TransactionGraph.remove` 中保留依赖顺序的问题。[#78731](https://github.com/StarRocks/starrocks/pull/78731) [#78800](https://github.com/StarRocks/starrocks/pull/78800) [#78077](https://github.com/StarRocks/starrocks/pull/78077)
- 其他存算分离修复：在最终提交时限制事务、在导入主副本选择时不选取 `DECOMMISSION` 副本、修复在线优化期间带有影子生成列的 DML 问题、使 `spill_enable_direct_io` 真正生效 `O_DIRECT`、修复并发 `BEGIN WITH LABEL` 之间的标签唯一性竞争问题、根据有效查询内存限制推导查询跟踪器保留限制、在 tablet 合并期间安全地重新映射 rowset ID 及引用、将跨发布的 del 文件仅应用于子对象拥有的键、使 tablet 合并遵循范围分布的 colocate、拒绝不完整的合并事务日志并阻止 `replication_num` 削弱存算分离写入法定人数、在 reshard 期间将显式事务保持在单一 tablet 布局上、在校验和失败时丢弃 del 文件的本地缓存并重试，以及在已关闭的 lake delta writer 拒绝任务时报告中止原因。[#77015](https://github.com/StarRocks/starrocks/pull/77015) [#77035](https://github.com/StarRocks/starrocks/pull/77035) [#77252](https://github.com/StarRocks/starrocks/pull/77252) [#77255](https://github.com/StarRocks/starrocks/pull/77255) [#77401](https://github.com/StarRocks/starrocks/pull/77401) [#77408](https://github.com/StarRocks/starrocks/pull/77408) [#77690](https://github.com/StarRocks/starrocks/pull/77690) [#77744](https://github.com/StarRocks/starrocks/pull/77744) [#78113](https://github.com/StarRocks/starrocks/pull/78113) [#78162](https://github.com/StarRocks/starrocks/pull/78162) [#78625](https://github.com/StarRocks/starrocks/pull/78625) [#78022](https://github.com/StarRocks/starrocks/pull/78022) [#78632](https://github.com/StarRocks/starrocks/pull/78632)
- 修复了存算分离排序键和 rowset 身份的边缘情况：在追加尾部键列后将 split 边界投影到当前排序键、在递归 CTE 执行中暴露内部 `INSERT` 故障、不让分摊的行计数决定跨发布 rowset 是否存在、使 `encode_sort_key` 可用于窄的定长 IVM 行 id 并扩展其常量参数、通过根列解析扩展列的 delta 列组、节省范围分割跨发布统计信息、修复 FE 最大 tablet Compaction 分数收集问题、在丢弃自增映射时跳过失效节点、测量未排序 PK SST writer 的映射而非统计分配次数、将 `compression_level` 传播到合成子列元数据，以及为内置倒排索引后端跳过独立 GIN 索引目录处理。[#77504](https://github.com/StarRocks/starrocks/pull/77504) [#77510](https://github.com/StarRocks/starrocks/pull/77510) [#77511](https://github.com/StarRocks/starrocks/pull/77511) [#77751](https://github.com/StarRocks/starrocks/pull/77751) [#77847](https://github.com/StarRocks/starrocks/pull/77847) [#77759](https://github.com/StarRocks/starrocks/pull/77759) [#78269](https://github.com/StarRocks/starrocks/pull/78269) [#78496](https://github.com/StarRocks/starrocks/pull/78496) [#78561](https://github.com/StarRocks/starrocks/pull/78561) [#77498](https://github.com/StarRocks/starrocks/pull/77498) [#76949](https://github.com/StarRocks/starrocks/pull/76949) [#77101](https://github.com/StarRocks/starrocks/pull/77101)

**导入、事务与 DML**

- 修复了在数据导入和列模式部分列更新路径上限制和检查块容量的问题、按所属子对象决定条件更新和部分列更新的问题，以及按语句记录合并 rowset 的 del `op_offset` 的问题。[#77163](https://github.com/StarRocks/starrocks/pull/77163) [#78099](https://github.com/StarRocks/starrocks/pull/78099) [#78313](https://github.com/StarRocks/starrocks/pull/78313)
- 修复了未对已删除表的 insert-overwrite 失败进行日志记录的问题，以及在多个部分列更新路径中保持当前行生命周期正确性的问题。[#77212](https://github.com/StarRocks/starrocks/pull/77212)
- 修复了将断连终止操作交给工作线程而非 I/O 线程处理的问题，以及在 `GroupedExecutionSinkOperator::push_chunk` 中传播 `accept()` 状态的问题。[#77590](https://github.com/StarRocks/starrocks/pull/77590) [#77737](https://github.com/StarRocks/starrocks/pull/77737)
- 使仅记录错误状态的内部 DML/CONTROL 语句失败，从而让故障显现出来，而不是被静默吞掉。[#77388](https://github.com/StarRocks/starrocks/pull/77388)

**统计信息、指标与 FE/元数据**

- 修复了统计信息正确性问题：拒绝直方图中的空桶/MCV，并使 `CancelableAnalyzeTask.cancel` 选出唯一的胜出者。[#77024](https://github.com/StarRocks/starrocks/pull/77024) [#78593](https://github.com/StarRocks/starrocks/pull/78593)
- 注册了 `pipe_driver_queue_len` 指标、为 NotFound 的 lake tablet 元数据读取添加了 BE 指标、跳过了非表 StarOS 分片的表指标和检查点线程池指标，并修复了 profile 字典中拼写错误的 `ZoneMapIndexFilter` 键。[#77890](https://github.com/StarRocks/starrocks/pull/77890) [#78451](https://github.com/StarRocks/starrocks/pull/78451) [#78116](https://github.com/StarRocks/starrocks/pull/78116) [#77367](https://github.com/StarRocks/starrocks/pull/77367) [#78728](https://github.com/StarRocks/starrocks/pull/78728)
- FE/元数据修复：在元数据同步中忽略已回收的缺失分片组、在 bRPC 中恢复每个端点的多个连接组、在发送给 BE 之前先在 FE 中解析计算节点主机名、使被删除的 FE 在 `DROP FOLLOWER`/`OBSERVER` 时自行退出，以及暴露 colocate 组调度采样开关。[#76604](https://github.com/StarRocks/starrocks/pull/76604) [#76765](https://github.com/StarRocks/starrocks/pull/76765) [#78390](https://github.com/StarRocks/starrocks/pull/78390) [#78215](https://github.com/StarRocks/starrocks/pull/78215) [#78450](https://github.com/StarRocks/starrocks/pull/78450)
- 修复了在扩展元数据扫描 tablet 时跳过非扩展访问路径的问题、按列 id 匹配扫描的访问路径的问题、仅在 BE 对其进行裁剪时才缩小 OLAP 扫描复杂类型的问题，以及不对聚合状态列子字段进行裁剪的问题。[#76593](https://github.com/StarRocks/starrocks/pull/76593) [#77987](https://github.com/StarRocks/starrocks/pull/77987) [#77894](https://github.com/StarRocks/starrocks/pull/77894) [#77901](https://github.com/StarRocks/starrocks/pull/77901)
- 不对聚合状态列使用全局字典，也不对返回 `ARRAY`/`STRUCT` 的表达式进行字典化，并在下推谓词读取稀疏区间时按序号索引页面空值标志。[#77096](https://github.com/StarRocks/starrocks/pull/77096) [#78488](https://github.com/StarRocks/starrocks/pull/78488)
- 其他指标和 FE 修复：将 bRPC 方法错误 bvar 上报为计数器、将范围分布分区的真实 tablet 计数上报为 Buckets、修复 `show proc '/global_current_queries'` 中的 NPE、在中间查询统计信息中上报 Data Cache 读取统计、在 jemalloc `malloc_usable_size` 中使用最小 TSD 获取、阻止堆剖析开关写入 `prof.thread_active_init`、防止 FE 连接 ID 在回绕后冲突、修复表统计信息健康度在多个分区过期时固定在约 1.0 的问题、对 FE 特性日志路径使用 `feature_log_dir`、在复制逻辑窗口算子时保留 `inputIsBinary`，以及修复缓存失效后 Thrift 客户端泄漏的问题。[#76956](https://github.com/StarRocks/starrocks/pull/76956) [#77465](https://github.com/StarRocks/starrocks/pull/77465) [#77407](https://github.com/StarRocks/starrocks/pull/77407) [#77696](https://github.com/StarRocks/starrocks/pull/77696) [#77625](https://github.com/StarRocks/starrocks/pull/77625) [#78384](https://github.com/StarRocks/starrocks/pull/78384) [#78217](https://github.com/StarRocks/starrocks/pull/78217) [#76927](https://github.com/StarRocks/starrocks/pull/76927) [#77221](https://github.com/StarRocks/starrocks/pull/77221) [#77058](https://github.com/StarRocks/starrocks/pull/77058) [#77072](https://github.com/StarRocks/starrocks/pull/77072)

**其他**

- 为池化的 Poco 会话应用了请求超时、在 Linux AArch64 上将 Hyperscan 升级到 Vectorscan 5.4.12、提升了 `JAVA_OPTS_FOR_JDK_*` 警告级别，并停止在 java-extensions 读取器库中附带 test-scope jar 包。[#78361](https://github.com/StarRocks/starrocks/pull/78361) [#78326](https://github.com/StarRocks/starrocks/pull/78326) [#78328](https://github.com/StarRocks/starrocks/pull/78328) [#77752](https://github.com/StarRocks/starrocks/pull/77752)
- 按键大小序列化 `dictionary_get` 键、在 thrift processor 中捕获异常，并使 `files()` 模式推断不再占用 CTAS 的 planner 元数据锁。[#78706](https://github.com/StarRocks/starrocks/pull/78706) [#78805](https://github.com/StarRocks/starrocks/pull/78805) [#78772](https://github.com/StarRocks/starrocks/pull/78772)
- 修复文件末尾无换行符时最后一行配置读取失败的问题，将配置 YAML 路径解析改为相对于 cwd 解析，将 GCS 上传阈值 BE 配置命名为 `gcs`（而非 `gs`），将 StarOS 和 Starlet 升级到 4.1.3，并移除了一个过时的转发头拆分配置以及文档文件名中一个多余的 U+200E 字符。[#78065](https://github.com/StarRocks/starrocks/pull/78065) [#77132](https://github.com/StarRocks/starrocks/pull/77132) [#78452](https://github.com/StarRocks/starrocks/pull/78452) [#78038](https://github.com/StarRocks/starrocks/pull/78038) [#78780](https://github.com/StarRocks/starrocks/pull/78780) [#78612](https://github.com/StarRocks/starrocks/pull/78612)

## 4.1.4 {/* #414 */}

发布日期：2026 年 8 月 5 日

### 行为变更 {/* #behavior-changes-3 */}

- 在存算分离模式下，`TABLESAMPLE` / `SAMPLE` 子句和 `ANALYZE SAMPLE TABLE` 现在可以在 lake 表上生效。此前采样选项从未传递到 BE，因此会执行全表扫描。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- 对 `flat_json` 配置的 `ALTER TABLE` 更改现在通过带版本号的任务传播到 BE 节点，从而可靠地生效。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- 由 GIN 倒排索引处理的 `NOT MATCH` 谓词不再返回 NULL 行；NULL 行现已被正确地从结果中排除。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` 不再为 char 系列列（优化器不会使用该列的直方图）计算直方图，而只计算 MCV，从而降低分析成本。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` 现在按保持顺序的分区范围对输出清单进行聚类，从而为每个清单生成更紧凑的分区边界，并在具有大量分区的表上实现更好的清单裁剪。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 多表 Stream Load 现在在决定是否使用合并事务日志时会参考每张表的 `file_bundling` 属性，而不是仅遵循全局的 `lake_use_combined_txn_log` 配置。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- 在主键表上，GIN 倒排索引与列模式部分列更新结合使用时不再返回损坏的结果；该索引现在从 delta column group segment 提供服务。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 对整数数组执行 `array_difference` 时，现在先以 64 位计算相邻差值，再扩展为 `BIGINT`，从而修复了 int32 溢出问题。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 除数非常量的除法表达式不再被视为单调表达式，修复了可能导致结果错误的 ZoneMap 裁剪问题。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 当某个 chunk 展开后的结果超过 4 GB 时，数组和 map 构造函数现在会报错，而不是静默地破坏数据。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- 主键表的插入操作现在能正确拒绝 L1/L2 持久化索引中已存在的主键；此前该存在性检查曾被跳过。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- 在 range-colocate 表上基于样本的 tablet 预分裂现在会将新分片分散到各个计算节点，而不是全部堆积到源 tablet 所在的 worker 上，修复了 range 批量导入比 hash 慢约 3 倍的问题。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 增量或 AUTO 物化视图现在会在 `AUTO` 刷新模式下为 PCT 回退重新构建维护查询，修复了 IVM 不支持的形态所导致的刷新失败问题。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 现在可用于非聚合的 INCREMENTAL 物化视图；存储填充的 `__ROW_ID__` 列已从 MV DDL 的列列表中移除，使得处于非激活状态的 IVM 可以重新激活。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg 分区缓存现在按内存（而不仅按条目数）进行限制，并暴露了其使用情况，防止在具有大量分区的表上出现无限增长。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- 使用 OAuth2 客户端凭证的 Iceberg REST catalog 在后台 token 刷新任务失效后现在可以自愈，而不是导致 all 后续请求都失败。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake full vacuum 现在会回收此前被跳过的孤立 `.lcrm`（Lake Compaction Rows Mapper）文件。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 存算分离发布流程现在会拒绝写入缺少 tablet 的 bundle tablet 元数据文件，防止分区在发布时永久卡住。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 已废弃的外部（Spark/Flink connector）扫描上下文在被回收时现在会取消其 pipeline fragment，并且外部扫描计划会设置 `query_delivery_timeout` 以限定 QueryContext 的生命周期。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- 物化视图的固定范围（pinned-range）映射现在使用正确的键（表 UUID），修复了 OLAP 表上 MV 引导锁定被禁用以及不同数据库中同名表被错误处理的问题。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- 转发给 Leader FE 的语句的审计日志现在会记录完全限定的被查询关系（与 Leader 保持一致），而不是 CTE 别名和未限定的表名。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 将完整的 `STRUCT` 列与 `ROLLUP`、`CUBE` 或 `GROUPING SETS` 一起聚合时，不再在计划阶段因 `usedStructFiledPos` 错误而失败。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE 内存 `Estimator` 现在会统计容器开销，并且 Parquet scanner 现在使用受限的 chunk 大小而非完整的 batch 大小来填充缺失列，从而提高了内存统计的准确性。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改进 {/* #improvements-4 */}

- 在 Routine Load 中新增对 `INCLUDE METADATA` 子句的支持，用于暴露 Kafka/Pulsar 消息元数据（例如 partition、offset 和 timestamp）。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- 将 Routine Load 元数据别名设为可选。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- 为 `information_schema.materialized_views` 新增了 `LAST_FRESHNESS_CONFIRMED_AT` 列。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- 通过 `/metrics` 端点暴露了 Data Cache 指标，并新增了 FE Compaction 指标、`ALTER TABLE` 列操作的指标和耗时，以及 exchange sink 上的 `CompressedInputBytes` 指标。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- 将 connector 元数据操作中的身份验证和连接错误详情暴露给终端用户。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 改进了大列容量限制检查的错误信息。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- 新增了 `hdfs_backend_selector_cache_replica_num` 变量，并使对象存储客户端缓存大小可在运行时修改。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- 将基于样本的 tablet 预分裂的元数据层 reader 扩展到了更多排序键类型，包括 `CHAR`、复合排序键，以及经过 UTC 调整的 Parquet `TIMESTAMP` / ORC `TIMESTAMP_INSTANT`。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 改进了外部表统计信息收集：现在会跟踪谓词列的使用情况，在单次扫描中收集多个列，并为 Iceberg 表应用了有限成本的扫描预算。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 支持 right-outer、semi、anti 和 full-outer 类型的 range-colocate join。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- 优化了 Query Queue V2 的成本估算器。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- 对于 `FILES()` 通配符模式，将通配符字面前缀下推到 S3 的 `ListObjectsV2` 调用中，减少了被列出的对象数量。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- 扫描 `remove_orphan_files` 中的 Iceberg manifest 条目时，现在仅投影 `file_path` 列。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- 支持 lake Primary Key tablet 的 base compaction，并为采用独立排序键的cloud-native PK 表支持导入落盘以及预先构建 PK 索引 SST。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 支持在存算分离 range 分布表上仅通过元数据方式新增末尾排序键列。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- 在 fragment 取消时取消正在进行的 exchange sink RPC，并在 reshard 清理过程中取消已重分片分区上正在进行的 Compaction。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 在等待之前即触发多语句 Stream Load 通道，降低了导入延迟。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- 为 `Analytor::process` 中的列升级增加了内存上限检查。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 在异步 delta writer 停止时保留真实的错误状态。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- 在 lake PK 索引 SSTable 上记录生成版本，在 lake Primary Key 发布阶段应用 `op_write.seg_delvecs`，并在 Compaction 发布冲突解决过程中跳过打开输出 segment footer。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 为 lake 导入和发布版本路径增加了堆栈跟踪以及细粒度的跟踪计数器，以辅助问题诊断。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### 安全性 {/* #security-4 */}

- [CVE-2026-44891] 将 Netty 升级至 4.1.136.Final，修复了 STOMP 子帧解码器中的内存耗尽（DoS）漏洞。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] 将 Apache Thrift 升级至 0.24.0，修复了 C++ 绑定中的堆缓冲区溢出以及一个死循环漏洞。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 排除了存在漏洞的 Jetty jar（客户端 Digest 认证绕过问题，通过 Hadoop 间接引入），并将 pgjdbc 升级至 42.7.12。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 排除了未使用的 `avro-ipc` jar，该 jar 捆绑了存在漏洞的 jQuery 1.4.2（以及其他 jQuery XSS CVE）。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- 移除了与已修复版本一起被引入的过时的、存在漏洞的传递依赖（例如 `bcprov-jdk15on` 1.70 以及已停止维护的 `okhttp` 2.x），并增加了依赖禁用规则以防止回归。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### 问题修复 {/* #bug-fixes-4 */}

修复了以下问题：

- 在 join 谓词推导过程中，对空范围进行处理时 Planner 发生崩溃。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- 聚合被错误地下推到带有非空常量 `ELSE` 的 `CASE` 之上。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` 的 partition-by 被重写为一个已被裁剪的字典槽位。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- join shuffle-join 输出属性分支中的运算符优先级错误，以及 `predicateCommonOperators` 在 join operator 构建过程中未被正确传递的问题。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 分析阶段视图列以及 `ROLLUP` 键的可空性判断不正确的问题。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- `regexp_extract_all` 在遇到零长度捕获组时发生死循环的问题。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` 中 `nullsFraction` 的截断范围错误问题。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 字符串转数值类型的 schema change 转换中 `CAST` 语义不正确的问题。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 针对同一列的 `min`/`max` 场景，同步物化视图改写会丢失 rollup 列的问题。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- 将 scan 谓词上拉时，改写被错误地应用于 `array_map` lambda 内部的问题。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- 在 bucket-aware 执行方式下，Iceberg bucket 表上的 `COUNT(DISTINCT)` 计数过多的问题。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 对 Iceberg 必填列进行 `UNNEST` + `GROUP BY` 时，可空输出结果出错的问题。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- 减少了 `INSERT OVERWRITE` 代码路径上的锁竞争，并恢复了 `Operator` 的 salt 值，以避免 CBO 表裁剪过程中出现 Memo 自引用问题。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 当字符串日期分区列与时间类型值进行比较时，Iceberg 分区裁剪、manifest 行数估算、元数据删除、equality-delete 应用以及 Delta Lake 分区裁剪均出现不正确结果的问题。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg 读取未遵循目标 snapshot 的 schema 与分区规范的问题；在删除分区字段后对 Iceberg V1 表查询失败的问题；以及在 `rollback_to_snapshot` 之后物化视图改写返回过期结果的问题。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg manifest 数据文件缓存返回的文件集不完整的问题；增量 scan-range 迭代器在并发关闭时不安全的问题；以及分析阶段 time-travel snapshot 绑定现在改为尽力而为（best-effort）方式处理。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta 元数据派生的统计信息现在会被打上 `StatsSource=TABLE_METADATA` 标记。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake 和 Kudu 无分区物化视图查询改写问题。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- 由于 `gcs-connector` 3.x 配置项重命名，导致 GCS 下发凭证被忽略的问题。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive `getTable()` 现在会在回退到 `get_table_req` 之前先重新连接，并通过异常类型而非消息文本来检测表未找到的情况。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet 列索引统计信息中不支持 `BOOLEAN` 的最小值/最大值问题。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 对于键名中包含 `.` 的字段，Flat-JSON 子字段读取出现问题，并且会错误地重建中间 flat-JSON 对象而非返回 NULL；当子字段键在忽略大小写的情况下发生冲突时，现在会跳过 JSON 子字段下推。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 当仅有一个分支建立了 bitmap 索引时，保留了OR嵌套谓词的问题。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- 主键自增部分列更新应用过程中出现 BE 崩溃。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- 导入落盘过程中 `LoadChunkSpiller` 初始化竞争导致 BE 崩溃。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 原生 Parquet reader 在不完整的嵌套 lake 表结构上崩溃，以及 `BOOLEAN` 嵌套在 Avro 复杂类型列中时出现 ASAN 崩溃。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 在 schema change 期间将无效字符串转换为 `NOT NULL` 数值列时出现 `bad_variant_access`。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` 错误路径中的堆缓冲区溢出，以及多字符 CSV 分隔符跨越缓冲区扩展时出现的释放后使用问题。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- 扫描销毁期间 `MorselQueueFactory` 上的释放后使用问题，取消时可落盘 JOIN 构建 `set_finishing` 中的释放后使用问题，以及 `PipelineDriver` 析构函数中全局运行时过滤器计时器泄漏／未调度计时器问题。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 对 `SparseRangeIterator::has_more()` 进行空安全处理，以修复物理拆分空 tablet 导致的 CN 崩溃，以及 bRPC stub 缓存清理计时器泄漏问题。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- 查询部署 worker 上的 `ConnectContext` 未被恢复。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 为存算分离主键表保留事务内 upsert/delete 的顺序，包括使落盘合并操作具备操作感知能力。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- 在 `cal_new_base_version` 中读取持久化元数据以避免悬空的 `prev_garbage_version`；使 `base_version` 与 `publish_version` 中的 `base_metadata` 保持同步；并在常规路径上持久化已合并的并行 Compaction 事务日志。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- 范围共位（range-colocate）相关修复：阻止了对齐作业风暴，并对未对齐的共位 JOIN 采取失败即关闭策略；修复了在 bucket-shuffle 下空安全 JOIN 丢失匹配的问题；改进了计划反馈中范围共位 JOIN 的检测。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- tablet 拆分／重分片相关修复：拆分时提升表的乐观版本号，以便并发查询重新制定计划；在相同 tablet 重分片前刷新 PK 索引 memtable；在重分片过程中按版本区间保留 lake vacuum 文件；对不可拆分的 range-distribution tablet 停止自动拆分作业循环；修复拆分后在 range-distributed 排序键上的 `IS NULL` 剪裁问题。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- 拒绝会重新排序由键派生的 range-distribution 表键列顺序的全列 `ORDER BY`。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- 按刷新顺序合并导入落盘的并行合并结果，并在文件打包的结转过程中按 `metaId` 排除已触及的索引。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- 为吸收 NULL 的字典映射分组键预留字典大小 + 1 的空间，并在重新调度性能分析报告前检查 driver 是否就绪。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- 当锁从未成功获取时，`PlannerMetaLocker` 跳过了解锁操作。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- 按 warehouse 过滤资源组。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- 在 `SHOW CREATE ROUTINE LOAD` 输出中对 `jsonpaths` 值进行转义。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- 修复了 Arrow Flight 预处理语句（prepared statement）转发问题。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 修复了字典刷新间隔溢出以及非预期自动刷新的问题。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- 修复了在 `ERROR_IF_OVERFLOW` 下最小/最大值为空时统计信息缓存加载失败的问题。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- 使 catalog 删除存在性检查在写锁下具备原子性。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {/* #413 */}

发布日期：2026 年 7 月 14 日

### 行为变更 {/* #behavior-changes-4 */}

- `CTAS` 现在会保留显式声明的 `VARCHAR(N)` 列长度，而不是将其放宽为 `VARCHAR(MAX)`。现有表不受影响；使用 `CTAS` 创建的新表将在后续写入时强制执行声明的长度。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- 在没有 `OPERATE ON SYSTEM` 权限的情况下查询 `sys.fe_memory_usage` 或 `sys.fe_locks` 现在会返回明确的拒绝访问错误，而不是容易产生误导的节点查找失败。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` 以及 broker/stream load 不再对使用 `isAdjustedToUTC=false` 写入的 `INT64` Parquet 时间戳应用会话时区偏移；这些时间戳现在被视为挂钟（wall-clock）值并按原样导入。v4.1.3 之前从此类文件导入的数据可能与升级后导入的数据不同；如需保持一致性，请重新导入。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 成功提交的多表事务 stream load 作业现在会在 `information_schema.loads` 和 `SHOW STREAM LOAD` 中被正确显示为 `VISIBLE`，而不再卡在 `PREPARING` 状态。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- Connector 增量扫描范围调度现在始终会复用已部署 fragment 的 driver 布局，防止扫描范围被错误地分配给不存在的 driver。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` 常量折叠现在与 MySQL 8 的反斜杠转义语义保持一致，修正了类似 `'a\\\\b'` 这样的模式之前返回相反结果的情况。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load 现在支持 `property.kafka_partition_discovery` 属性，该属性允许在指定 `kafka_partitions` 和 `kafka_offsets` 以设定精确起始偏移量的同时，继续进行分区自动发现。当未设置 `property.kafka_default_offsets` 时，作业已有消费进度后新发现分区的默认起始偏移量将从 `OFFSET_END` 变为 `OFFSET_BEGINNING`——这一变化适用于 **all** 自动发现作业，而不仅限于使用新属性的作业。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非分组聚合现在会在合并前被下推到 `UNION ALL` 分支，从而降低对 union 进行聚合的查询的网络传输量和内存占用。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM 维护查询现在会在每次刷新时根据当前的视图定义重新推导，而不再使用 `CREATE` 时保存的固定查询文本；现有物化视图无需重新创建即可自动受益于改写器的 bug 修复。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- 基于采样的 tablet 预拆分现在会将预拆分分片分散到 all 计算节点（`SPREAD` 放置方式），而不是集中放置到源 tablet 所在的 worker 上（`PACK` 放置方式），从而提升导入并行度。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 仅修改列注释的 `ALTER TABLE ... MODIFY COLUMN` 现在会走轻量级的元数据专用路径，而不再触发完整的 schema change 作业，并且该机制现在也适用于主键列。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` 和 `CEIL` 现在被视为非保留关键字，可以不加引号直接用作列名。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` 输出现在会在 UDF 和 UDAF 的 Properties 列中始终包含 `isolation` 属性（`shared` 或 `isolated`）。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` 的默认值从 100 提高到 200，通过在每次 `DeleteObjects` 请求中批量删除更多过期文件，提升了对象存储上的 vacuum 吞吐量。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- 具有下发凭证（vended credentials）的 Iceberg REST catalog 表现在会被缓存，其凭证也会在后台刷新，从而消除了按 `getTable()` 调用 `GetDataAccess` 导致的 AWS Lake Formation 速率限制问题。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM `bitmap_union`、`hll_union` 和 `percentile_union` 聚合状态现在只在物化视图中存储一次，而不是两次（可见列 + 隐藏 `__AGG_STATE_` 列），使这些 sketch 类型的存储空间减半。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 增量物化视图现在支持 `bitmap_agg`、`hll_union`、`percentile_union` 和 `bitmap_union` 聚合函数，支持精确去重和基于 sketch 的聚合增量维护。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- 基于采样的 tablet 预分裂 tablet 数量现在会向上取整为活跃计算节点数的最接近倍数以实现均匀分布，并以最小 tablet 大小为下限，避免小规模导入时产生过多碎片。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改进 {/* #improvements-5 */}

- `ngram_search` 函数现在支持非常量的 needle 参数。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- 新增由 `enable_http_auth` FE 配置项控制的 HTTP 认证框架，用于在 all 外部 HTTP 端点上控制认证和 RBAC 鉴权。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- 在 `information_schema.materialized_views` 中新增了刷新和放置可观测性列（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- 新增可选的外部统计信息缓存在日志回放时的延迟刷新功能，由新的 FE 配置项控制，以防止缓慢或卡住的外部元存储阻塞 FE 日志回放或启动。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- 现在允许通过快速 schema change 在不进行数据重写的情况下，增加 range 分布（存算分离）排序键列的 `VARCHAR` 长度。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 当存算分离事务日志写入超过可配置阈值时，新增堆栈跟踪转储功能，便于诊断缓慢的 `put_txn_log` / `put_combined_txn_log` 调用。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- tablet 预分裂元数据层页脚读取器现在支持 `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR` 和 ORC `TIMESTAMP` 排序键，减少了需要回退到数据层采样的导入数量。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- 基于采样的 tablet 预分裂现在适用于 `INSERT INTO ... SELECT ... FROM <OLAP table>` 导入，也适用于包含 all 排序键列的列列表 `INSERT` 语句。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 新增针对存算分离 tablet 元数据和事务日志文件的 Adler-32 校验和保护，以便在读取时检测静默损坏。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- 新增按数据库统计的 `txn_max_committed_pending_publish_ms` FE 指标，用于报告最早已提交但尚未发布的事务的存活时长，有助于检测版本发布停滞问题。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- 现在 tablet 分裂/合并会根据发布版本响应实时触发，减少了导入完成与自动分裂/合并启动之间的延迟。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- 通过将无 SST 条件合并任务路由到 `pk_index_execution` 线程池，优化了存算分离主键表的条件更新比较阶段。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- 将存算分离 schema change 和 rollup 作业锁的作用范围缩小到表级别而非整个数据库，减少了同一数据库中其他表并发操作时的锁竞争。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 在存算一体模式下，将若干数据库级写锁收窄为表作用域的密集写锁，减少了 BE 上报回调和降冷操作期间的锁竞争。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load 现在支持原生的 `MAP` 和 `STRUCT` 目标列。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- Range-colocate tablet 稳定性门控现在会等待 StarOS 的放置收敛后，再将分组标记为稳定，以确保 Colocate Join 实现主机本地执行。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 改进了外部表的 CBO 统计信息：优化器现在可以从 Iceberg manifest 中估算行数，而无需枚举全部文件；修正了 Hive/Hudi 在 Parquet/ORC 压缩下的行数低估问题；为 JDBC 连接器新增了异步行数统计；并在 Puffin 统计信息不可用时为 Iceberg 和外部连接器提供 NDV 估算回退方案。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg manifest 列统计信息现在仅针对聚簇列进行选择性缓存，降低了含有大量数据文件的宽表对 FE 堆内存的占用。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部表统计信息收集现在支持在 FE 重启和高可用故障切换期间持久化跟踪谓词列信息，使 auto-ANALYZE 能够定位到正确的列。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- 新增结构化的 `[ExternalStats]` 日志行，覆盖了外部表统计信息收集从调度到执行的完整生命周期。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` 现在会在外部表统计信息作业的 Properties 列中包含分区、列和快照元数据。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 每个外部表的统计信息来源（`TABLE_METADATA`、`ANALYZE` 或 `NONE`）现在会在查询运行时 Profile 中显示。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- 新增对 Iceberg 和 Delta Lake 外部表的分区过滤要求和分区数量限制的支持（此前仅支持 Hive、Hudi 和 Paimon）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- 在 `TABLE SAMPLE` 和直方图 `ANALYZE` 中支持小于 1% 的采样比例，修复了大表上由于计算得到的比例被截断为零而导致失败的问题。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- 新增 `jemalloc_conf` BE 配置项，使 jemalloc 运行时选项可以通过 `information_schema.be_configs` 查看。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 新增 `compaction_chunk_reset_memory_tracker_threshold_percent` BE 配置项，通过释放保留的 chunk 容量来减少存算一体模式下主键表 Compaction 期间的内存使用。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- 将 staros 升级到 v4.1.1，包括跨重启持久化的 `datacache.enable`、按 worker 分组的分片预热超时覆盖配置，以及改进的 S3 重试抖动机制。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- 通过在 SQL 字符串中不存在凭证标记时跳过正则表达式扫描，优化了审计热路径上的 SQL 凭证脱敏处理。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet 扫描器基于表达式驱动的按需延迟列加载，减少了多分支 `OR` 查询中不必要的 I/O。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` 现在通过使用组合估算器而非依赖顺序的 HIP 估算器，产生稳定的基数估算结果。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### 安全性 {/* #security-5 */}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final，以修复 SNI 处理器堆耗尽（拒绝服务）、IPv6 子网过滤器绕过以及 DNS 缓存投毒问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 将 `jackson-databind` 升级到 2.21.4，以修复两个反序列化漏洞。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 从 Hadoop 传递依赖中排除了 `org.jline:jline-remote-telnet`，以修复未经身份验证的 Telnet 服务器拒绝服务漏洞。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] 更新了预编译的 pprof，以修复 pprof 二进制文件中的一个漏洞。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 修复了 `information_schema.task_runs` 中的 SQL 注入问题，该问题是由于谓词值中的单引号可能会突破字面量边界所致。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key` 和 `iceberg.catalog.jdbc.password` 现在会在 `SHOW CREATE CATALOG` 的输出中被屏蔽。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 修复了当输入以被截断的百分号转义序列结尾时，`url_decode` 中出现越界读取的问题。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- 修复了 `HyperLogLog::deserialize` 接受超出范围的 `SPARSE` 寄存器索引的问题，该问题可能导致堆内存损坏，并在输入格式错误时导致 BE 崩溃。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- 修复了 `bar()` 拒绝负宽度值的问题，此前该问题会导致字符串无限增长以及 BE 内存耗尽。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### 问题修复 {/* #bug-fixes-5 */}

修复了以下问题：

- `add_files` 使用 Parquet 物理编码字节而非逻辑类型值填充 Iceberg 文件边界，导致文件级最小值/最大值剪枝不正确（例如在 `DECIMAL` 列上）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- 当遍历以不可变 `List.of(...)` 构建输入列表的计划节点时，`ApplyTuningGuideRule` 抛出 `UnsupportedOperationException`。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` 的两阶段重新规划可能会产生来自第一次规划会话的陈旧 lambda 参数列引用 ID，从而导致 `expr_type does not match slot_type` 错误。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- 对带有 GIN（倒排）索引的表进行部分更新时，如果更新中省略了 GIN 索引列，会导致查询无限挂起或失败。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- 当 rowset 结构和 tablet 结构之间出现结构漂移时，Lake PCU（部分列更新）会崩溃或静默损坏数据。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 对外部 Iceberg 表执行带有多个 schema 子句的组合 `ALTER TABLE` 操作时，会在每次子句分派时错误地重复执行 all 之前已排队的操作。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- 当 `num_rows` 快照在分区刷新与资源组取消交错执行期间超过实际 chunk 行数时，`PartitionedSpillerWriter` 会因 `SIGSEGV` 崩溃。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- 由于 BE 信号初始化时未忽略 SIGPIPE，BE 进程可能在启动期间（通常紧随部署之后）意外退出。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 当由于行范围过滤而跳过某个 struct VARCHAR 子字段填充时，Parquet 临时字典编码列会泄漏到上层，导致类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` 在审计日志中记录的是 `ReturnRows=0`，而非实际导出的行数。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- 由于锁类型不匹配，`TabletChecker.doCheck()` 在 `blockingAddTabletCtxToScheduler` 中抛出 `IllegalMonitorStateException`，导致整轮检查器静默中止。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` 对 `DATETIME_PRECISION` 始终返回 `NULL`，导致依据该字段推导列大小的 MySQL 协议客户端出现问题。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- 当查询联接了来自不同数据库或 catalog、但具有相同未限定名称的两个表时，MV 刷新会因 `Duplicate key` 而失败。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 在某些条件下，可落盘哈希联接探测会崩溃。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 当宽度或桶数参数为零时，Iceberg 的 `truncate` 和 `bucket` 转换函数会导致 BE 因 `SIGFPE` 崩溃。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 当被除数为 `TYPE_MIN` 且除数为 `-1` 时，`mod()` 和 `pmod()` 会导致 BE 因 `SIGFPE` 崩溃。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- 当 `bucket_num` 为零或负数时，`histogram()` 会导致 BE 因 `SIGFPE` 崩溃。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- 当 all 输入行为 `NULL` 时，`encode_fingerprint_sha256` 会因 `SIGSEGV` 崩溃。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 包含单字符通配符 `_` 的 `LIKE` 模式在通过 GIN 倒排索引进行求值时返回了不正确的结果。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- 当目标 segment 为空时，仅针对 AND 的 `MATCH` 查询在使用 GIN 倒排索引时返回了虚假错误。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene 的 `match_all` 查询返回了不正确的结果；通过升级 CLucene 依赖项已解决该问题。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- 向量索引重写直接在共享表结构上注册了一个合成的距离列，导致同一表上不相关的并发查询出现 `Multiple entries with same key` 错误。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- 联接重排序剪枝可能会剪除仍被扫描谓词引用的列，导致统计信息估算抛出 `missing statistic of col`。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` 被错误地通过 sum/count 物化视图重写，静默丢弃了 `DISTINCT`，并在存在重复数据时返回错误结果。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` 抛出了内部 `NullPointerException`，而不是一个清晰的语义错误。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- 对于没有 `COLUMNS TERMINATED BY` 子句的作业，`SHOW CREATE ROUTINE LOAD` 会在第一个导入描述子句前错误地输出一个多余的逗号。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- 当 CTE 名称被误认为是真实表引用时，带有 CTE 的 `SECURITY INVOKER` 视图可能会因 NPE 而导致权限检查失败。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 当日期/日期时间边界字面量的偏移会超出可表示范围时（例如 `<= '9999-12-31'`），`ReduceCastRule` 会因 `SemanticException` 中止查询规划。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- 当联接条件使用了空值安全相等（`<=>`）的析取条件时，`SplitJoinORToUnionRule` 会输出重复行。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 在多表外部查询的并行元数据准备线程之间共享的 `Tracers` 在 `enable_profile=true` 情况下导致了 `IllegalStateException`。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 中的分区消费者错误被静默丢弃，导致分区化的 TopN 在不报错的情况下返回部分或错误的结果。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- 在 FE 调用方超时后，BE 清理（vacuum）任务仍继续以僵尸进程方式运行，耗尽了 `RELEASE_SNAPSHOT` 线程池，并导致清理吞吐量崩溃。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- 自动清理（autovacuum）竞争条件可能会瞬时计算出一个比正在进行中的事务大 1 的 `minActiveTxnId`，导致 BE 删除仍需要的组合事务日志，从而永久卡住发布流程。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- 由于 FE EOS 取消与 BE 第二阶段部署之间存在竞争，查询在成功完成后被错误地标记为已取消。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 当聚合 TopN runtime filter 构建键为 `ConstColumn` 时，BE 在 `AggTopNRuntimeFilterUpdaterImpl` 中因 `SIGSEGV` 崩溃。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- 当all非空数组为空时，`array_map` / `transform` 会静默丢弃 `NULL` 行并返回错误的行数。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 在 JIT 编译的表达式中，超过 2^64 的 `LARGEINT` / `DECIMAL128` 字面量会被静默截断为 64 位。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- 当最后一个字符带有被截断或无效的多字节前导字节且分隔符为空时，UTF-8 字符串函数（`split`、`split_part`、`str_to_map`）会读取超出字符串末尾的内容。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- 即使在 `ALLOW_THROW_EXCEPTION` SQL 模式下，`parse_json()` 对格式错误的JSON也会静默返回 `NULL`，而不是使查询失败。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 严格模式下的数值缩窄类型转换会在槽位数据未定义的 `NULL` 行上错误地引发溢出错误。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 由于负数截断除法余数的问题，带有非零亚秒部分的 1970 年之前的 Parquet `INT64` 时间戳会被解码为垃圾值。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970 年之前的 ORC `TIMESTAMP` 值在导入时其亚秒部分会被丢弃。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC stripe 的最小/最大时间戳统计信息对于 1970 年之前和亚秒边界的解码不正确，导致数据文件被错误地裁剪。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- 嵌套在 `ARRAY`、`MAP` 或 `STRUCT` 列内部的 `INT96` Parquet 时间戳在导入时丢失了一个会话时区偏移量。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- 当 Parquet `UINT_32` 值被导入到 `BIGINT` 列时，会进行符号扩展而非零扩展，导致高位无符号整数被静默存储为负值。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` 析构函数在 `_scanner_ctx`（持有引用这些节点的谓词）之前就销毁了 `_pool`（及其 `Expr` 节点），导致堆使用后释放（heap-use-after-free）。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- 当一个多字节 UTF-8 字符跨越 8 MB 解压缓冲区边界时，使用 OpenX SerDe 读取经 gzip 压缩的JSON Hive外部表会因 `UTF8_ERROR` 而失败。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- 由于缺少客户端无条件访问的JSON字段，ADLS2 的 `ListPaths` 在非 HNS 账户上会因 `SIGSEGV` 崩溃。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 当多个 `UNNEST` 算子共享同一个输入数组列并消费不同的子字段时，`unnest` 会崩溃或返回错误结果。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- 在 `unnest` 执行期间未强制执行 `query_mem_limit`，导致对大数组执行 `unnest` 会使 BE 被 OOM 杀死，而不是使查询失败。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- 当排名限制恰好落在 chunk 边界上时，带有 `RANK` 边界的 `TopN` 会丢失一行。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` 之后的列裁剪可能生成一个空的分析（窗口）算子，从而导致规划或执行错误。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` 仅在扫描算子上设置了行数限制，而未设置全局限制，导致在并发情况下对受限子查询执行 `COUNT(*)` 会返回比预期更多的行。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake 主键持久化索引重建在 segment-range 模式下使用了错误的 segment 迭代器位置，导致键范围过滤器应用不正确。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` 在没有表锁的情况下修改了 `rebuildPindexVersion`；`RestoreJob` 在恢复后仅在数据库READ锁下修改了物化视图基表信息；`FinalizeCreateTableAction` 在迭代器创建过程中传递了一个数据库级锁。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- 如果在获取每个数据库的锁时在循环中途抛出异常，`dumpImage` 可能导致全局元数据锁被无限期搁置。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- 多语句 Stream Load 每个事务会泄漏一个 `TxnStateCallbackFactory` 条目，无限增长并最终耗尽 FE 堆内存。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- 当catalog、数据库、表或分区名称较长时，直方图统计信息的 `information_schema.task_runs` 行数可能会超出 `primary_key_limit_size`（128 字节）。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVM 指标发出了无效的 Prometheus `# TYPE` 行（标签集位于指标名称内部），导致 Prometheus 中止整个抓取过程。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- 在存算分离表上，`SHOW PARTITIONS` 和 `information_schema.partitions_meta` 将all物理分区的 bucket 数报告为表级默认值，而不是每个分区的实际 bucket 数。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` 在存算分离（`CLOUD_NATIVE`）表上为all rollup 索引返回了基表结构。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` 无操作子句被错误地路由到轻量级注释路径，导致批量 `ALTER TABLE` 语句中出现 `MODIFY COLUMN COMMENT can not be combined with other alter operations` 错误。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- 由于 `isKey` / `aggregationType` 归一化不正确，`isCommentOnlyModification` 可能将键/聚合列误判为仅修改了注释。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` 可能提交一个循环视图定义，导致后续 `SELECT` 抛出 `StackOverflowError`。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- 当下游消费者在 `accept()` 仍持有先前 chunk 的指针时修改了该 chunk，`OrderedPartitionExchanger` 会导致堆使用后释放（heap-use-after-free）。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- 当构建端的槽位描述符为不可为空类型，但运行时状态为可为空类型时，NLJoin 会崩溃。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 当结构体字段名不是可解析的JSON路径时，`CAST(json/variant AS struct)` 会在 fragment 准备阶段导致 BE 崩溃。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- 嵌套字典表达式的字典解码可能在生产者和消费者 fragment 之间产生不兼容的字典转换，导致运行时出现 `Dict Decode failed` 错误。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- 当 `get_rowset_by_version` 返回 `nullptr` 且 `gtid` 比较位于空值检查之前时，schema change 会因空指针解引用而崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- 由于快照manager在决定是否回收父级tablet元数据时未考虑 reshard 任务，存算分离集群的快照在tablet分裂/合并后变得无法恢复。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- File-bundling vacuum 错误地将同级 tablet 的零行捆绑 segment 标记为非共享，导致其捆绑文件在仍被其他 tablet 引用的情况下被删除。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 存算分离表 compaction 发布阶段丢弃了在 compaction 事务开始后才变为可见的 rollup/同步物化视图索引，导致捆绑文件中缺少这些索引。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- 当 compaction 在tablet reshard 期间被丢弃时，存算分离持久化索引 compaction 错误地删除了透传复用的 SSTable 文件。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` 转为可为空的 flat-JSON 列的 schema change 在 compaction 读取路径中导致 `CHECK` 崩溃。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- 在流式预聚合透传路径中，对可空列执行 `count_combine` 会导致 BE 崩溃并报 `SIGSEGV` 错误。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- 由于 JDK 21 中移除了反射式 `DirectByteBuffer` 构造函数查找，Java UDF 在 JDK 21+ 上加载失败。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- 由于解析器不支持 `CREATE TABLE AS SELECT` 中的 `ENGINE` 子句，`CTAS` 到统一 catalog（Hive 元数据存储）总是失败。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` 反馈驱动的连接重建丢失了 `predicateCommonOperators`，导致在具有公共子表达式复用的执行计划上出现 `InputDependenciesChecker` 校验失败。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- 在具有子分区的表上，查询缓存归一化出现 `Preconditions.checkState` 崩溃，原因是在构建版本列表之前就裁剪掉了空的子分区。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` 会静默跳过以旧版别名存储的会话变量，导致查询转储回放时回退为默认值。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- 由于对行组起始偏移量进行了重复计算，Iceberg 的 `_row_id` 虚拟列在包含多个 Parquet 行组的数据文件上返回了不正确的值。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE 计划器无法定位目标扫描节点，原因是其按合成表 ID 而非物理表标识进行匹配，导致丢失了基础快照 ID 和冲突检测过滤器。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- 当 `cancel_plan_fragment` RPC 在 `PipelineExecutorSet::start()` 被调用之前到达时，`FragmentContext::set_final_status` 会因 `SIGSEGV` 崩溃。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `QueryContext` 可能在 `FragmentExecutor` 仍在拆除某个 fragment 时被回收，导致 `ResGuard::reset()` 中出现释放后使用（heap-use-after-free）的问题。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` 未初始化，导致默认构造的 `search()` 对未初始化的指针进行了解引用。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` 微秒数使用了 JVM 默认区域设置的数字字符集进行渲染，导致在阿拉伯语或波斯语等区域设置下出现非 ASCII 数字，从而破坏了 tablet 预拆分中的边界值解析。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- 在 tablet 统计信息刷新之前，`INSERT OVERWRITE` 之后分区行数可能被写为零到 `_statistics_.column_statistics` 中，导致优化器将分区基数估算收缩为零。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- 当全局配置禁用了首次导入统计信息采集时，`enable_statistic_collect_on_first_load` 表级覆盖项无法启用该功能。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- 当某个 UNION 分支没有输入行时，`PushDownNonGroupedAggregateBelowUnion` 会在非空类型声明下产生可空的输出，从而导致 BE `CHECK` 校验失败。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {/* #412 */}

发布日期：2026 年 6 月 18 日

### 行为变更 {/* #behavior-changes-5 */}

- 连接到用户无权限访问的数据库时，现在会返回正确的 MySQL 错误包，而不是以 ERROR 2013 关闭连接。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- 对于可以通过函数级权限查看某函数、但未持有创建函数作用域权限的用户，`SHOW FUNCTIONS` 现在会将 UDF 文件和对象文件路径掩盖为 `***`。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 从外部 catalog 查询 Hive 视图时，现在可以正确应用 Ranger 行过滤和列掩码策略。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` 现在可以正确保留 `current_timestamp` 生成表达式。`DESCRIBE` 和 `information_schema` 现在会反映该表达式，而不是回填时的字面量。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- 在会话时区与 UTC+8 不同的集群上，`information_schema.loads` 的导入时过滤不再出现过滤边界偏移问题。导入时间现在以 UTC 纪元毫秒数的形式在 FE 和 BE 之间传递。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` 会话变量现在可以正确应用于 Paimon 扫描切分计算，而不再始终使用默认值。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` 现已默认启用。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive 分区统计信息不再按计时器针对每个键自动刷新。分区统计信息现在只在显式调用 `refreshTable()` 期间刷新，从而降低了大型分区表对 HMS 造成的负载。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- 当 Iceberg 或外部 catalog 基表发生表结构漂移（列类型变更、删除列或删除表）时，依赖该表的物化视图现在会在下一次刷新时被标记为不可用，而不是静默产生 NULL 行或不明确的错误。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg 连接器现在会部分下推 `AND` 复合谓词中可转换的一侧，而不是在仅有一侧可转换时直接丢弃整个谓词，从而改进了分区裁剪和数据跳过效果。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 显式事务 `COMMIT` 现在会正确地等待最多 `query_timeout` 秒（而不是毫秒）以获取数据库写锁，避免在短暂的并发写入活动下出现虚假的锁超时失败。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM 刷新现在会将严格导入过滤错误反馈给调用方，而不是静默丢弃被过滤的行。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` 现在会正确排除 NULL 行，与 `COUNT(col)` 语义保持一致。此前，基于 `COUNT(<nullable column>)` 的增量物化视图会物化出被夸大的计数。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` 现在还会显示由 `ALTER TABLE ... SET (...)` 触发的、针对 cloud 原生（存算分离）表上诸如 `file_bundling` 和 `enable_persistent_index` 等属性的仅元数据异步变更作业。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 创建带有引用聚合函数的 `HAVING` 子句的增量物化视图时，现在会在 `CREATE` 阶段就失败并给出明确的错误，而不是在首次刷新时产生内部计划错误。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM 现在支持在增量物化视图中使用 `MIN`/`MAX(DECIMAL)` 聚合函数。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- 当第一个增量特征已经超过 `mv_max_rows_per_refresh` 时，IVM 自适应刷新现在能够正确限定增量窗口范围，避免在一次任务运行中就刷新全部积压数据。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- 仅 GROUP-BY 的增量物化视图（例如 `SELECT k FROM t GROUP BY k`）现在可以正确地将 `__ROW_ID__` 编码为 VARCHAR，修复了第二次刷新时的崩溃问题。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 功能优化 {/* #improvements-6 */}

- 支持 Paimon 视图，包括 `CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`，以及从外部 catalog 查询 Paimon 视图。Paimon 视图内部的表引用现在会基于 Paimon catalog 进行解析，而不是基于 `default_catalog`。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 在读取存在表结构漂移或具有复杂嵌套类型的文件时，`FILES()` 中支持显式的 `schema` 参数，用于实现稳定的表结构控制。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` 现在可以跨 all 个 FE 节点获取查询概要信息，而不仅限于当前连接的 FE。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 新增 `query_id()` 内置函数，用于返回当前执行查询的 UUID。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 存算分离模式下的 `CREATE`/`ALTER STORAGE VOLUME` 现在会在持久化元数据之前校验存储位置的可访问性（凭证和终端节点），在配置错误时提前失败。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- BE 新增对 AWS S3 凭证的 `WebIdentity` 令牌提供者支持，与 FE 现有的 `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` 支持保持一致。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 新增 `ADMIN SKIP COMMITTED TRANSACTION` 命令，用于在 publish 因缺失 `txnlog`、段文件丢失或远端 I/O 过慢而被永久阻塞时，解除存算分离表上卡住的 `COMMITTED` 事务。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` 现在会将 `table_name` 谓词下推到 FE，大幅降低单表查找的开销。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- 为 `information_schema` 表新增了缺失的 MySQL 8 列，以提升与在连接检测期间检查 MySQL 8 模式的 BI 工具和 JDBC 驱动程序的兼容性。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- 新增 `enable_pipeline_event_scheduler` BE 配置项，作为集群级的开关，在设置为 `false` 时会覆盖会话级变量。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 为统计信息采集新增可选的宽字符串列隔离功能，以降低对具有多个宽字符串列的表进行统计信息采集时的单次查询内存峰值。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- 慢锁日志现在支持按事件限速，并可配置堆栈捕获行为，以避免在高锁竞争下出现 JVM 安全点停顿。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- 物化视图刷新日志条目现在在前缀中包含数据库名称，使日志行在多租户部署中（同名物化视图存在于多个 schema 中）可以被区分。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 配置项现在可动态修改，可通过 `ADMIN SET FRONTEND CONFIG` 在运行时切换，无需重启 FE。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- 新增 `enable_print_load_profile_to_log` FE 配置项（默认值为 `false`），用于将导入概况（Stream Load、Routine Load、Broker Load 以及 merge-commit 导入）写入 `fe.profile.log`，即使内存存储因查询概况突发而被淘汰，相关记录也能得以保留。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` 现在可以在 `JobProperties` 中正确渲染列映射，而不是 Java 对象引用。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` 现在使用表级加锁而不是 catalog 级加锁，减少了在拥有大量并发活跃表的 catalog 上的刷新串行化延迟。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- 元数据扫描（后台统计信息采集）现在可以优雅地处理 `ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN` 和 `REORDER COLUMN` 等 schema 变更，而不会因为变更后的段文件而报找不到错误。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- 基于采样的 tablet 预拆分现在支持多分区的 range-distribution 表以及 Broker Load，可在没有已有数据层基线的情况下实现首次导入的并行度。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL 结果序列化不再使用逐行虚函数分发；现在每个 chunk 只构建一次带类型信息的列写入器，降低了宽或大结果集的序列化开销。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` 到字符串的类型转换现在直接写入输出缓冲区，消除了逐行的堆内存分配。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- 查询统计信息合并路径将 `SpinLock` 替换为无锁并行 map，在 worker 发送中间或最终统计信息时降低了大型集群的 CPU 使用率。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 聚合哈希表和哈希集合的预取现在以 L2 缓存驻留情况为门控，避免了当桶数组可容纳于 L2 时出现的 4%–9% 性能回退。预取距离现已可配置。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 针对存算分离模式下主键表的轻量级 Compaction publish，采用了流水线化的逐段 `.lcrm` 读取，减少了对象存储的串行往返次数。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 存算分离模式下的冷主键索引重建扫描现已实现跨段并行化，降低了段读取受远端 I/O 限制时的重建时间。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部查询（统计信息采集、任务运行、物化视图刷新）现在可在 `SHOW PROC '/current_queries'` 中可见，并可通过 `KILL QUERY` 终止。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- 新增 lake vacuum 批次大小和重试次数的 bvar 指标，用于监控 S3 限流情况并调优 `lake_vacuum_min_batch_delete_size`。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- 新增 `CatalogRecycleBin` 大小仪表指标，可在回收站增长给 FE 堆带来压力之前将其暴露出来。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` 分区表现在在 `OlapTableSink` 中打开 all 分区，而不是应用为 Range 分区表设计的 latest-N 启发式策略，从而降低了增量打开的 RPC 开销。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- 支持通过 `FILES()` 或 Broker Load 将 `LARGE_LIST` 和 `FIXED_SIZE_LIST` Arrow 类型导入到 JSON 列中。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 存算分离表上的 merge-commit（`FRONTEND_STREAMING`）导入现在支持组合事务日志和文件打包，使其与其他导入类型保持一致。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- 新增可动态修改的 FE 配置项 `slow_publish_partition_log_threshold_ms`（默认 3000 毫秒），用于控制 lake publish 阶段耗时分解警告的阈值，无需重启 FE。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### 安全性 {/* #security-6 */}

- [CVE-2026-43869] 将 `libthrift` 升级到 0.23.0 以解决证书主机验证不正确的问题。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] 将 Apache Tomcat 升级到 9.0.118 以解决 HTTP/2 请求头校验问题。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final 以解决 SNI 处理程序堆耗尽（DoS）、IPv6 子网过滤器绕过以及 DNS 缓存污染问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- 将 pprof 预构建二进制文件升级到 Go 1.25.11，以包含 Go 标准库的安全修复。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### 问题修复 {/* #bug-fixes-6 */}

修复了以下问题：

- 当 URL 中的 `:` 出现在 `host:port` 模式之外时，`parse_url()` 返回了错误的主机。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 字典转换表达式在某些不成立的情况下（例如 `IF(col = '1', NULL, 'ok')`）错误地假定 `f(null) = null` 成立。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- 事务型 Stream Load 使用了默认的 RPC 超时时间而非用户指定的超时时间，导致过早超时。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- 当标识列中存在 NULL 值时，Iceberg 相等删除文件未能删除匹配行，因为 `NULL = NULL` 在 join 谓词中的求值结果为 UNKNOWN。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- 包含 `INJECTED` 分区投影列的表的错误消息现在更加详细，会显示具体是哪些列导致了问题。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- 由于未能识别 insert-overwrite 操作，对仅插入型 ACID Hive 表的查询返回的行数多于预期。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 在并发读取期间固定 Iceberg 元数据条目时，磁盘缓存超出了其配置容量。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 查询外部 catalog 时，Paimon 主键列被错误地标记为非空。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 当查询包含多个 `ARRAY_AGG(DISTINCT <const>)` 输入时，`MultiDistinctByMultiFuncRewriter` 重复应用同一规则导致优化器超时。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- 未带 `DATE`/`TIMESTAMP` 关键字下推的 Oracle JDBC 日期谓词会导致 NLS 格式错误。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- 分区 TopN 可能会丢失其子 Operator 所需的输出列。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- 无法在具有分区演化的 Iceberg 表上创建无分区的 MV。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- 在 `information_schema.be_cloud_native_compactions` 中，并行子任务的 Compaction 任务统计信息被覆盖并丢失。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同步 MV 的 `SHOW CREATE MATERIALIZED VIEW` 执行失败，报错 "Table is not found"。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- 当语句 `.log` 文件落在 4-segment 路径时，Lake publish 多语句事务在 schema change 期间发生死锁。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- Sort merge provider 的错误未传递到 fragment 上下文，导致查询静默失败。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 在长时间运行的 Follower FE 上，`ConnectorTableId` 从 `int` 溢出为负值，导致 Iceberg 和 Hive 查询以误导性的 "Invalid table type" 错误失败。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 带有空 optimize 子句（没有 distribution 或 partition spec）的 `ALTER TABLE` 被错误解析，可能在 FE 重放时破坏表的默认分布方式。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- 由于 `AZURE_PATH_KEY` 未被识别为有效的 `StorageVolumeMgr` 参数，FE 在 ADLS2 存算分离容灾期间启动失败。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- 当优化器将嵌套类型的一部分裁剪为 `UNKNOWN_TYPE`，或者使用了可为空的 array、map 或 struct 模式时，Avro 复杂类型解码失败。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 由于两个 `NullableColumn` 共享同一个 `NullColumn` 对象，COW 列变更优化导致 `map_apply` 及类似函数崩溃。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- 由于该 provider 在 FE 上被提前实例化，具有自定义 `LocationProvider` 的 Iceberg 表在使用 `ClassNotFoundException` 执行 `SELECT` 查询时失败。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` 在每次缓存未命中时都会多执行一次 `getTableComment()` 往返请求，延长了高强度规划阶段的锁持有时间，并阻塞了并发 DDL。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- 当嵌套 MV 返回 `FULL` 或 `UNKNOWN` 时效性时，嵌套 MV 刷新会抛出 `NullPointerException`。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE worker 在向响应缓慢的 MySQL 客户端发送查询结果时无限期阻塞。结果发送路径现在会强制执行写超时。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- 在将主键表从 V1 编码（存算一体）复制到 V2 编码（存算分离）集群，或在两个存算分离集群之间复制时，PK `.del` 文件未被转码。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 由于在添加存活副本之前未移除过期副本引用，`VERSION_INCOMPLETE` 恢复期间在 `TabletInvertedIndex` 中累积了重复副本。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- 由于 `REPLICATE_SNAPSHOT` 任务与每文件复制子任务共享同一个线程池，存算分离 lake replication 的文件复制导致 CN 崩溃。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- 当 BE 使用 `.000` 十进制后缀格式化单位计数器时，`RuntimeProfileParser` 抛出 `NumberFormatException`。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 存算分离 PK tablet split 中共享 segment 的物理 rowid 编码不正确，导致 `rss_rowid` 条目错误。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- 混合 range-colocate × hash-distributed 的 `JOIN` 查询返回 `Unknown error` 而非有效结果。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` 使用了固定的 UTC+8 格式化器；现在输出会遵循会话 `time_zone`。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- 当 all 值为 `NULL` 且该列经过可为空一元函数路径时，Decimal 类型列会丢失精度，从而破坏下游结果类型。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- 嵌套类型的 JSON 部分追加操作导致 ASAN 崩溃。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` 角色的权限缓存在 `GRANT`/`REVOKE` 时未失效，导致过期权限在到期前仍然生效。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- 当子 writer 没有追加操作时，`FlatJson` 崩溃。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- 当 MV 本身带有 `HAVING` 谓词时，聚合 MV 改写被错误地应用，可能返回不完整的结果。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 进入并行合并模式时，spill writer 的 `auto_flush` 标志存在数据竞争，导致在 ARM 上发生非预期的 segment 刷盘。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load 调度器在进行阻塞式 BE RPC 调用以获取 Kafka 或 Pulsar 分区元数据时持有每任务写锁，导致锁持有时间最长达 33.6 秒。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- 当启用 `tablet_sched_disable_colocate_balance` 时，已失效 BE 上的 Colocate tablet 被错误地报告为 `HEALTHY`。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- 当存在 MISSING（幻影）副本行时，`ADMIN SHOW REPLICA STATUS` 会使 MySQL 结果流失步，导致客户端挂起或断开连接。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 在存算分离模式下，每个 sender 的 `open` RPC 未重新记录每分区协调者认领信息，导致部分 sender 在协调者选举中被遗漏，从而使 `combined_txn_log` 文件未被写入。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- 在删除 `_statistics_` 数据库或表之后，`_statistics_.pipe_file_list` 内表未被重新创建。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- 被 `TaskCleaner` 强制终止的任务运行未被归档，导致它们从 `information_schema.task_runs` 中消失且无迹可寻。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` 和 `SWAP TABLE`/`SWAP MATERIALIZED VIEW` 仅持有密集表锁而非数据库写锁，导致并发读取者可能观察到不完整的名称到表的映射。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK 索引 Compaction 输出的 sstable 在打开时未带有 tablet 元数据，导致永久性的 `metadata is null when loading delvec` 失败。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 在显式事务中，当一次部分列更新 `INSERT` 作用于同一事务中已修改过的表时，会在 `COMMIT` 处静默导致数据损坏。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- 与 Range 分布不兼容的 `ALTER TABLE` 操作（schema change、排序键变更）现在会返回可操作的错误提示，而不是静默地破坏元数据。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- 优化器中聚合函数子节点类型不匹配会导致查询结果不正确。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 使用保留关键字作为表名的 `ALTER ROUTINE LOAD` 写入了无法解析的 `origStmt`，导致 FE 重启后列映射丢失。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` 兼容性检查未递归处理嵌套类型（例如 `ARRAY<VARCHAR>`），导致 `ARRAY_AGG` IMV 的 `CREATE MATERIALIZED VIEW` 失败。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- 当扫描范围被完全过滤掉时，Parquet 临时字典编码列会泄漏到上层，导致下游类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `CASE WHEN` 在 `WHEN` 和结果类型混用浮点数与整数时会生成无效的 JIT IR，导致结果错误或崩溃。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT 编译失败导致 `LLVMContext` 出现释放后使用（use-after-free），从而引发 SIGSEGV。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- 后台统计信息任务覆盖了会话级 `WAREHOUSE` 设置，影响了同一连接上下文中后续的用户查询。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- 当从未有任何集群快照成功完成时，`CatalogRecycleBin` 停止清除条目，导致在高负载 `INSERT OVERWRITE` 工作场景下 FE 内存无限增长。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- FE 未能检测到非主键表副本的版本空洞；查询被永久路由到存在空洞的副本上，并且 `max_version` 被冻结。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（一个在共享读锁下被修改的 `HashSet`）存在数据竞争，可能导致条目丢失或集合损坏。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 当保留边界元数据已经被清理时，Vacuum 水位未能被正确上报，导致 `file_bundling` 的版本切换清理停滞。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum 重试此前使用确定性的指数退避；现在增加了去相关抖动（jitter），以便在 S3 限流情况下将重试分散到各个 CN 上。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- 由于从查询内存池分配的 RPC 请求是在进程上下文中释放的，`OlapTableSink` 中的内存统计被虚增。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 当自动分区创建并发触发 `_incremental_open_node_channel` 时，`TabletSinkSender::_send_chunk_by_node` 中存在竞态条件。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 由上游变更回合（backport）创建的 UDAF 上下文通过 `unique_ptr::release` 导致了内存泄漏。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- 由于 `append_selective` 中的内存统计不准确，分区 JOIN 探测阶段可能出现越界访问。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 配置项名称存在拼写错误。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` 错误地将 Range Colocate PACK 分片组当作孤立对象回收，导致在存算分离模式下永久删除了活跃分片。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- 对于 PRIMARY KEY 表以及没有显式 `ORDER BY` 的表，Colocate tablet 拆分的排序键元数（arity）是从基础模式而非物化模式中解析的，导致拆分任务完成后并未减小 tablet 的大小。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 当分区数据量低于合并阈值时，自动合并守护进程会将预拆分的 tablet 重新合并在一起，从而抵消了基于采样的预拆分所带来的并行度收益。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 由于函数的 `FunctionName.db` 仍指向源数据库，`RESTORE ... AS <new_db>` 之后，数据库级别的 UDF 在 Follower FE 上缺失。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- 当仓库（warehouse）具有多个 CN 组时，在存算分离 `DISTRIBUTED BY RANDOM` 的 CTAS/INSERT 中，不可变分区的 tablet 位置被分配了错误的 CN 组。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 当统计信息估算过程中分区被并发删除时，`StatisticsCalcUtils` 中出现 `NullPointerException`。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` 和 `FrontendServiceImpl` 元数据 RPC 处理器持有完整的数据库 READ 锁，阻塞了对无关表的 DDL 操作。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 在未通知共享上下文观察者的情况下翻转其完成状态的流水线算子，可能在事件调度器下导致对端驱动器停滞。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 谓词下推中的非根复合谓词会产出 `NotPushDown` 而不是扫描层级的 EOF，导致在 UNION 下存在不可能成立的嵌套 AND 分支时，`OlapScanNode` 不返回任何行。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` 在只有单一存储介质的 BE 上执行了代价高昂的逐副本扫描；现在对于同构磁盘的 BE，该检查已优化为 O(1) 复杂度。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- 数据目录加载线程上的线程名称设置存在竞态，导致每次 BE 启动时都会出现大量干扰性的 `failed to set thread name` 警告。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- 任务 manager 写入了非法的 `RUNNING→RUNNING` 编辑日志，导致任务运行记录在运行中映射表中无限期地呈现卡住状态。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- 主键表多语句批量事务未在复合 Rowset 之间累积 `num_rows`、`data_size` 和 `num_dels`，导致存算分离模式下主键表的行数统计不正确。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake 导入落盘清理现在采用基于事务 ID 的 Vacuum 驱动回收机制，防止 BE 崩溃或 OOM 后出现孤立的落盘文件。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- 从 `0000` 年开始的 PostgreSQL JDBC 时间值会导致类型映射结果不正确。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- 在 schema change 过程中从 rowset 读取 `gtid` 之前缺少空值检查，导致 NPE 崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {/* #411 */}

发布日期：2026 年 5 月 29 日

### 行为变更 {/* #behavior-changes-6 */}

- Hive 连接器现在默认使用原生 C++ Avro 扫描器，而不是 JNI Avro 扫描器。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 现已禁用对 INCREMENTAL/AUTO 物化视图的查询改写，并且对 INCREMENTAL/AUTO 物化视图的 FORCE 刷新和分区刷新也被拒绝。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 功能优化 {/* #improvements-7 */}

- Java UDF/UDAF/UDTF 现支持更多类型：UDAF/UDTF 的 STRUCT 参数和返回值、嵌套的 ARRAY/MAP 类型、DATE/DATETIME、DECIMAL 以及可变参数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- 标量 UDF 现支持 STRUCT 参数。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF 现支持嵌套的 ARRAY/MAP 类型。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF 现在只需加载和初始化一次，即可在多个查询中复用，从而降低每次查询的开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- 为 Hive 连接器将 JNI Avro 扫描器替换为原生 C++ 扫描器，支持直接二进制解码，并支持 `avro.schema.literal` 和 `avro.schema.url`。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 支持在 CTAS 语句中使用 Trino 的 `WITH` 子句。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- 在写入路径上完成了对 Iceberg `timestamptz` 分区转换的支持。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- 为 Iceberg 表聚合启用了 TopN 运行时过滤器下推。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- 支持 Iceberg 日期时间的最小值/最大值优化。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 允许在 Catalog 和 BE 中透传 HDFS HA 配置，以支持访问多个 HDFS 集群。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 为外部表查询添加了分区扫描数量限制。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- 对于不支持的 Iceberg V3 特性，现在会快速失败。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- 通过 INSERT INTO FILES 支持在 CSV 导出中使用 `csv.enclose` 和 `csv.escape`。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 为将完整表结构下推至 `files()`，新增了 `enable_push_down_schema` INSERT 属性。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load 作业现在会在遇到不可重试的错误（例如主键大小超限）时暂停。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 支持对来自两个子节点的复杂表达式进行 JOIN 重排序。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- 改进了 CBO 统计信息估算，包括针对 `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION 以及常量的 MCV/空值比例传播。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- 改进了数据倾斜 JOIN 检测：只有当 all 关联键出现倾斜时才会检测到倾斜，并新增了一个 `force_group_by_skew_eliminate_when_skewed` 开关用于强制启用倾斜规则。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- 支持在 FE 中对 `regexp_replace` 进行常量折叠。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 优化了对使用常量分区值的日期分区列的 MIN/MAX 操作。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- 在物化视图刷新中引入了关键字 `SCHEDULE`，作为 `ASYNC` 的同义词。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 支持存算分离模式下 Lake 表的 tablet 创建重试。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- 支持 Lake 列模式部分列更新的条件更新。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 并行化了部分列更新发布、持久化索引初始化以及 SSTable 打开操作，以提升导入吞吐量。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 支持在从存算一体模式到存算分离模式的复制过程中进行 DCG 文件同步。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- 支持对主键和非主键列的 VARCHAR 长度扩宽进行表结构演进。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- 新增了用于集群快照完整性检查的 `snapshot_meta.json` 标记。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- 支持通过 DN 模式进行 LDAP 直接绑定认证。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- 新增了 `get_query_dump_from_query_id` 元函数，以便于查询问题排查。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 支持在审计日志中记录所查询的关系。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- 新增了用于 MySQL 二进制结果编码的会话变量。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 新增了多项指标以提升可观测性，包括用于存算分离集群的 `tablet_num`、`MemtableIOSpeed`、`staros_shard_count` 以及 Iceberg 元数据表查询指标。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- 新增了 FE 配置项 `deploy_serialization_min_thread_pool_size`。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- 新增了 `tablet_reshard_enable_tablet_merge` 配置项，用于禁用 MergeTabletJob 的创建。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- 通过 `SO_REUSEPORT` 消除了 HTTP 服务器接受连接时的惊群效应。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- 支持从 S3 加载 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 新增 `uuid_v7` 函数，用于生成按时间排序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- 为 external catalog 可观测性新增按 catalog 类型划分的查询指标。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 支持为窗口函数显式指定倾斜提示，通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### 安全性 {/* #security-7 */}

- [CVE] 将 Netty 升级至 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] 将 pgjdbc 升级至 42.7.11（修复由无限制 SCRAM PBKDF2 迭代次数导致的客户端 DoS 问题），并将 BouncyCastle 升级至 1.84（修复 FrodoKEM 私钥泄露问题）。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] 使用 go1.25.9 构建 pprof，以消除 Golang 相关的 CVE 问题。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- 将 jetty-http 升级至 9.4.58.v20250814。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 清理 Broker 依赖相关的 CVE 问题，并移除 `wildfly-openssl`。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- 对 INSERT INTO FILES 错误信息中的凭证信息进行了脱敏处理。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### 问题修复 {/* #bug-fixes-7 */}

修复了以下问题：

- 由 `hash_util` 静态初始化顺序导致的 CN 启动时段错误。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 在启用物理拆分的情况下扫描空 tablet 时导致 CN 崩溃的问题。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 查询 `information_schema.warehouse_queries` 时导致 BE 崩溃的问题。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- 当 rowset `num_rows` 为零时，Lake Compaction 中出现 SIGFPE 问题。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG fragment 连接中的除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer 中优雅退出时发生崩溃的问题。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- 可落盘哈希连接探测阶段崩溃的问题。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 格式化为临时 `std::string` 时发生的栈缓冲区溢出问题。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` 中的崩溃问题。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 由临时 `shared_ptr` 析构导致的 `LoadChannel::get_load_replica_status` 中释放后使用（use-after-free）问题。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- 线程创建失败时 `ThreadPool::do_submit` 中出现的释放后使用（use-after-free）问题。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- fragment 销毁过程中 Hive 分区描述符出现释放后使用（use-after-free）问题。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- information schema sink 中的释放后使用（use-after-free）问题。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- 由于复用 HttpClient 实例导致的 FE 文件描述符泄漏问题。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` 中的 JNI 局部引用泄漏问题。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- 缓存 MV 计划上下文时发生的内存泄漏问题。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- 本地交换（local exchange）中出现的异常内存占用过高问题。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` 中 `response->tablet_metas` 的竞争问题。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` 中 `SegmentFlushTask` 的并发竞争问题。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- 序列化过程中 `RuntimeProfile` 最大/最小值的竞争问题。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- 查询上下文销毁过程中 `PipelineTimerTask` 的竞态条件问题。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` 中的竞态条件问题。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` 和 `array_length` 中共享 `NullColumn` 的问题。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- 由分区版本间隙导致的批量发布死锁问题。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 在存算一体模式下为 rowset 元数据预热 LRU 缓存时发生的死锁问题。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` 回滚非异常安全且解锁顺序不正确的问题。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 在只读和元数据路径上由多个数据库锁导致的与 DDL 及 StarOS RPC 的锁争用问题。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- 由于缺少 project 节点导致的 shuffle 分布不正确问题。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN runtime filter `exprOrder` 不匹配，导致崩溃和结果错误的问题。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY 结果错误的问题。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query cache 与本地 shuffle 聚合冲突的问题。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- flat JSON 中全局字典生成不一致的问题。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON 合并为空时结果不一致的问题。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 当显式声明 key/value 类型时，map 字面量类型不匹配的问题。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- 在 JOIN USING transformer 中，COALESCE 的子项未转换为公共类型的问题。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- 使用全局变量进行 reduce-cast 后，VARCHAR 长度未保留的问题。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- 在 MySQL 结果集中，VARBINARY 在嵌套类型内编码错误的问题。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 在小 LIMIT 上禁用聚合落盘时，check-having-clause 出现问题。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日期解析前未去除引号，以及 PostgreSQL 日期/时间相关的错误。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- 数据文件共享标志丢失，导致 vacuum 删除了仍被同级 split tablet 引用的文件。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→merge 流程中 tablet 合并的正确性问题。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- tablet 拆分期间，跨发布事务日志的 num_rows/data_size 膨胀的问题。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一 publish 批次中先写入后 Compaction 导致的 Delvec 孤立条目问题。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- 通过同步 StarMgr journal 回放来修复 follower FE 上出现的 “no queryable replica” 问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 在应用普通 rowset 提交时，`merge_condition` 未被保留的问题。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE 冲突检测使用了错误的快照 ID 和过滤条件的问题。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- Iceberg transform 参数无效时导致 NPE 的问题。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 由于计划器注入了额外列，导致 Iceberg min/max 优化被跳过的问题。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- 在 Iceberg 基表上进行 aggregate-join-pushdown 物化视图改写的问题。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- 在 INSERT OVERWRITE 提交前缺少 Hive 分区目录的问题。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- AWS assume-role 未应用于 JNI scanner 的问题。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- 对于被裁剪的子项和嵌套可空 schema，Avro 复杂类型解码的问题。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Load 错误中缺少文件/列/行上下文信息的问题。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet scanner 缺乏对 Arrow 字典值支持的问题。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon 表的主键未在 SHOW CREATE 及 DESC 返回结果中显示的问题。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC 类型兼容性问题，以及带尾部斜杠的 JDBC URL 构造问题。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- 在 JDBC catalog 中刷新包含 SQL Server 表的物化视图时出现的问题。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 针对外连接之上的物化视图，延迟物化 slot 可空性问题。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO 和 INCREMENTAL 物化视图分区刷新被拒绝的问题。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- 物化视图变为不活跃状态后，其调度器未停止的问题。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 为兼容 MySQL 客户端，缺乏对 `SHOW GRANTS FOR CURRENT_USER()` 支持的问题。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW 语句不允许在显式事务中使用的问题。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flight 在空结果集中返回列名 `r` 的问题。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF 代码中缺乏 JNI 异常处理检查的问题。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 函数注册问题。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- 使用 `enable_load_profile` 时，Stream Load profile 采集出现问题。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- Profile 中的 START_TIME/END_TIME 未按会话时区显示的问题。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` 不支持运行时修改的问题。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` 在相等谓词中未转义特殊字符。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- 并行 segment/rowset 加载在错误路径上存在释放后使用（use-after-free）问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 聚合 spill `set_finishing` 中可能出现哈希表数据丢失。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 磁盘重新迁移（A→B→A）期间的 GC 竞争导致 PK tablet rowset 元数据丢失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` 中存在数据库读锁泄漏。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM 刷新记录的 PCT 分区元数据不完整。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 当引用列缺失时，分析 Stream Load/Broker Load 中的生成列会出现 NPE。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短路点查（point lookup）中缺少分区谓词。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {/* #410 */}

发布日期：2026 年 4 月 13 日

### 存算分离架构 {/* #shared-data-architecture */}

- **全新多租户数据管理**

  存算分离集群现已支持基于范围的数据分布，以及 tablet 的自动拆分与合并。当 tablet 变得过大或出现热点时可自动拆分，无需进行 schema change、SQL 修改或数据重新导入。该特性可显著提升易用性，直接解决多租户工作负载中的数据倾斜和热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 tablet 支持（第一阶段）**

  使存算分离集群能够在每个 tablet 中承载明显更多的数据，长期目标是每个 tablet 达到 100 GB。第一阶段在整个导入、主键更新和 Compaction 流程中引入了 tablet 内部并行化，因此单个 Lake tablet 在规模增长时不再成为单线程瓶颈。改进内容包括单个 tablet 内的并行 Compaction（支持 segment 级拆分）、Lake 导入中 MemTable 终止化、落盘和合并的并行处理（包括 load-spill 路径）、tablet 内部并行发布以及主键表的并行条件更新，还包括 cloud 原生主键索引的范围拆分 / 并行 / 分层 Compaction，并支持远端存储 mapper 文件。这些改动共同大幅降低了大 tablet 工作负载的导入内存开销、Compaction 放大以及 FE 元数据压力。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **快速 Schema 演进 V2**

  存算分离集群现已支持快速 Schema 演进 V2，可实现 schema 操作的秒级 DDL 执行，并进一步将该支持扩展到物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 存算分离模式下的倒排索引**

  为存算分离集群启用内置倒排索引，以加速文本过滤和全文搜索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  查询级别的缓存命中率现已在审计日志和监控系统中展示，以提升缓存透明度并便于延迟诊断。额外的 Data Cache 指标包括内存和磁盘配额使用情况，以及页缓存统计信息。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表新增了 segment 元数据过滤功能，可在扫描时根据排序键范围跳过无关 segment，从而减少范围谓词查询的 I/O。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- 支持 Lake DeltaWriter 的快速取消，降低了存算分离集群中被取消的导入作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 新增对自动化集群快照基于时间间隔的调度支持。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- 支持 MemTable 落盘和合并的流水线执行，提升了存算分离集群中 cloud 原生表的导入吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- 支持修复 cloud 原生表时使用 `dry_run` 模式，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 为存算一体集群中的发布事务新增了线程池，提升了发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### 数据湖分析 {/* #data-lake-analytics */}

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入 position delete 文件，可直接从 StarRocks 对 Iceberg 表执行 DELETE 操作。该支持涵盖了 Plan、Sink、Commit 和 Audit 的完整流程。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive 和 Iceberg 表的 TRUNCATE**

  支持在外部 Hive 和 Iceberg 表上使用 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上的增量物化视图**

  将增量物化视图刷新的支持扩展到 Iceberg 仅追加（append-only）表，无需全表刷新即可实现查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg 中半结构化数据的 VARIANT 类型**

  在 Iceberg Catalog 中支持 VARIANT 数据类型，可灵活地对半结构化数据进行读时模式（schema-on-read）的存储和查询。支持读取、写入、类型转换以及与 Parquet 的集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增对 Iceberg v3 默认值特性和行血缘（row lineage）的支持。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增对 `rewrite_manifests` 过程的支持，并为 `expire_snapshots` 和 `remove_orphan_files` 过程扩展了更多参数，以实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- 支持从 Iceberg 表中读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- 支持从 Iceberg v3 表中读取 `_row_id`，并支持针对 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- 支持创建带有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中显示相关属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 支持查询指定 branch、tag、版本或时间戳的 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- Paimon 表支持复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- 创建 Iceberg 表时支持使用括号语法的 Partition Transforms。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- 支持基于 Transform Partition 的 Iceberg 全局 shuffle，以改进数据组织方式。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- 支持为 Iceberg 表 sink 动态启用全局 shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- 为 Iceberg 表 sink 引入了 Commit 队列，以避免并发 Commit 冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- 为 Iceberg 表 sink 新增了 host 级别排序，以提升数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- 默认在 ETL 执行模式下启用了额外优化，无需显式配置即可提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 以及类似批处理操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- 为 Iceberg 表上的 INSERT 和 DELETE 操作添加了 commit 审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- 支持在 Iceberg REST Catalog 中启用或禁用 view 端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- 支持在多种 Iceberg catalog 类型上使用 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- 支持 AWS Glue Catalog 表的分区投影。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- 为 AWS Glue `GetDatabases` API 添加了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- 支持通过端点注入（`azblob`/`adls2`）实现 Azure ABFS/WASB 路径映射。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- 为 JDBC catalog 添加了数据库元数据缓存，以降低远程 RPC 开销以及外部系统故障的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- 支持在 `information_schema` 中为 PostgreSQL 表添加列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- 改进了 Oracle 和 PostgreSQL 的 JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎 {/* #query-engine */}

- **Recursive CTE**

  支持使用递归 Common Table Expressions（CTE）进行分层遍历、图查询以及迭代式 SQL 计算。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 的重写逻辑，支持基于统计信息的倾斜检测、直方图支持以及 NULL 倾斜感知。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- 改进了窗口上的 COUNT DISTINCT，并新增了对多重 distinct 聚合融合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- 支持为窗口函数显式指定倾斜提示，并通过拆分为 UNION 自动优化具有倾斜分区键的窗口函数。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- 在 Trino Parser 中为 INSERT 语句支持 EXPLAIN 和 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- 支持 EXPLAIN 以查看查询队列可见性。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数及 SQL 语法 {/* #functions-and-sql-syntax */}

- 新增以下函数：
  - `array_top_n`：按值排名返回数组中的前 N 个元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：按元素逐一组合多个数组，生成一个 struct 数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：对 JSON 字符串进行带缩进的格式化。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：在 JSON 字符串中指定路径处设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：对具有相同键的行中的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：返回当前 warehouse 的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：从 SQL 调用外部 AI 模型以进行推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：在 SQL 表达式中抛出用户自定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 提供以下函数或语法扩展：
  - 支持在 `array_sort` 中使用 lambda 比较器以实现自定义排序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持使用 USING 子句对 FULL OUTER JOIN 应用符合 SQL 标准语义的处理。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持在带框架的窗口函数中使用 DISTINCT 聚合，结合 ORDER BY/PARTITION BY。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 支持在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中使用 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 支持类似 count distinct 的聚合函数使用 VARBINARY。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持在 IN 表达式中进行日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 支持使用 WITH LABEL 语法来处理 BEGIN/START TRANSACTION。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 支持在 SHOW 语句中使用 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持使用 `ALTER TASK` 语句进行任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持 CSV 文件导出使用多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式，用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理与可观测性 {/* #management--observability */}

- 支持为资源组设置 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改善多 Warehouse CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图以检查 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest Blacklist，以在集群级别阻止特定的查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法直接访问的节点进行 Arrow Flight 数据检索。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，可在无需重新连接的情况下将全局变量更改同步到现有连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 新增内置 UI 功能，用于分析查询画像并查看格式化后的 SQL，使查询调优更加便捷。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 接口，以提供结构化的集群概览。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增全局只读系统变量 `@@run_mode`，用于查询当前集群的运行模式（存算分离或存算一体）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2`，以改善查询队列管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持为 Stream Load 和 Merge Commit 操作设置用户级默认 Warehouse。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 新增 `skip_black_list` 会话变量，可在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为指标 API 新增 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 新增模拟用户支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 新增 `table_query_timeout` 作为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 支持新增 FE Observer 节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 支持在 `information_schema.loads` 中显示 Merge Commit 信息，以便更好地查看导入作业情况。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在 cloud 内表中显示 tablet 状态，以便更好地进行故障排查。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### 安全性 {/* #security-8 */}

- [CVE-2026-33870] [CVE-2026-33871] 替换 AWS bundle 并将 Netty 升级到 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级到 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级到 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### 问题修复 {/* #bug-fixes-8 */}

修复了以下问题：

- 修复了 tablet 分裂后数据丢失的问题，方法是跳过对 range 分布 tablet 的数据文件删除。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 在处理复杂类型时出现的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了 `shared_ptr` 在 `BatchUnit` 和 `FetchTaskContext` 之间循环引用导致的内存泄漏问题。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了 SystemMetrics 中由于并发 getline 访问导致的双重释放崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在 eager merge 消费 all 数据块时崩溃的问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了自动创建的分区被 TTL 清理器删除时出现 NPE 的问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中出现 NPE 的问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了外连接中常量侧列引用导致谓词改写不正确的问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了存算分离模式下修改 CHAR 列长度后查询结果错误的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了多表情况下物化视图刷新的问题。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新时物化视图回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步物化视图中 all 空值处理的问题。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了在快速 schema change ADD COLUMN 后查询物化视图时出现重复列 ID 错误的问题。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了由共享 DecodeInfo 导致的低基数改写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数 JOIN 谓词类型不匹配的问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了当 `null_counts` 为空时，Parquet Page Index Filter 出现 Segfault 的问题。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 在相同路径上展平数组和对象发生冲突的问题。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制的问题。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性的问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB 文件系统缓存键未包含 container 的问题。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池已满时出现死锁的问题。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon catalog 刷新时在 ObjectTable 上出现 ClassCastException 导致崩溃的问题。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了含常量子查询时 FULL OUTER JOIN USING 的问题。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了 CTE 作用域下 join on 子句的问题。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 通过使用 bindScope() 模式修复了 ConnectContext 内存泄漏的问题。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了存算一体集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏问题。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift accept 线程遇到异常时退出的问题。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了 Routine Load 列映射中 UDF 解析的问题。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了字典页过大时扫描结果出错的问题。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了 Range 分区重叠的问题。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了大查询队列分配时间和排队超时的问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了处理空字面量数组时 `array_map` 崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 的栈溢出问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了 LDAP 身份验证中用户名大小写不敏感归一化的问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计和 SQL 脱敏中对用户身份验证字符串进行了掩码处理。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更 {/* #behavior-changes-7 */}

- ETL 执行模式优化现已默认启用。此优化可使 INSERT INTO SELECT、CREATE TABLE AS SELECT 以及类似的批处理工作负载受益，无需进行显式配置更改。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除常量值外还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：输出结果中 USING 列只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` 现在默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL 事务默认受会话变量 `enable_sql_transaction` 控制。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
- 此前，FE 配置项 `max_scalar_operator_flat_children`（默认值为 `10000`）仅限制由 `CASE WHEN` 构建的表达式的节点数量。现在它会限制优化器重写的每个标量表达式，并且节点数量会在每次重写后重新计算，而不是复用缓存值。因此，升级后某个此前可以正常执行的查询可能会在分析阶段因 `Expression too complex. Current nodes: N, Limit: M.` 而被拒绝，这通常发生在内联视图、CTE 或子查询扩展了其表达式树时。请简化表达式，或增大 `max_scalar_operator_flat_children`（该参数支持动态修改）。[#66324](https://github.com/StarRocks/starrocks/pull/66324)
