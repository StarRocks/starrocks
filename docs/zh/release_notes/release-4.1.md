---
displayed_sidebar: docs
description: "StarRocks 4.1 版本发布说明：多租户基于范围的 tablet 自动分裂、大容量 tablet 支持（目标 100 GB）、Fast Schema Evolution V2 等新特性……"
products: [oss]
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

<<<<<<< HEAD
## 4.1.5
=======
## 4.1.8 \{#418}

发布日期：2026 年 11 月 16 日

### 行为变更 \{#behavior-changes}

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

### 改进项 \{#improvements}

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

### 安全性 \{#security}

- 将 FE ZooKeeper 升级到 3.9.6。[#81292](https://github.com/StarRocks/starrocks/pull/81292)
- 将 pprof 升级到 release/20261022。[#81300](https://github.com/StarRocks/starrocks/pull/81300)
- 将 guava 升级到 33.7.2，将 snappy-java 升级到 1.1.10.9。[#81859](https://github.com/StarRocks/starrocks/pull/81859)

### 问题修复 \{#bug-fixes}

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

## 4.1.7 \{#417}

发布日期：2026 年 10 月 26 日

### 行为变更 \{#behavior-changes}

- 本地（存算一体）主键表的插入操作默认不再检查不可变的 L1 和 L2 持久化索引文件中是否存在重复键。新的 BE 配置项 `skip_pindex_l1_l2_check_in_insert` 默认值为 `true`；L0（memtable）检查始终执行。将其设置为 `false` 可恢复之前的行为。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- BE JVM 指标默认启用（`enable_jvm_metrics` 现在默认值为 `true`）。这些指标仅在 JVM 存在时才会被采集，因此启用该功能不会在原本不需要 JVM 的 BE 上启动 JVM。[#81113](https://github.com/StarRocks/starrocks/pull/81113) [#81074](https://github.com/StarRocks/starrocks/pull/81074)
- `spill_max_dir_bytes_ratio` 的默认值从 `0.8` 降低为 `0.5`，因此中间结果落盘最多使用每个落盘目录容量的 50%。[#80189](https://github.com/StarRocks/starrocks/pull/80189)
- 存算分离副本同步的文件复制现在在专用的 CN 级别池中运行。`lake_replication_file_copy_threads` 的默认值由 `0`（表示 4 倍 CPU 核心数）改为 `16`，新增的 `lake_replication_max_parallel_files_per_tablet`（默认值 `4`）用于限制单个tablet并行复制的文件数量。[#78078](https://github.com/StarRocks/starrocks/pull/78078)
- 存算分离主键表 tablet 的删除触发 Base Compaction 现在通过新的 BE 配置项 `lake_pk_compaction_base_enable_delete_trigger` 控制是否启用，该配置项默认值为 `false`。[#79926](https://github.com/StarRocks/starrocks/pull/79926)
- 当 `pk_index_parallel_execution_threadpool_max_threads` 为 `0` 时，主键索引的并行执行池大小调整为all CPU 核心数，而不是其一半。[#80460](https://github.com/StarRocks/starrocks/pull/80460)
- `CAST(x AS CHAR(N))` 现在将结果截断为 `N` 个字符，与 MySQL 保持一致。FE 中的常量折叠和 BE 中的执行都会应用该截断。[#75909](https://github.com/StarRocks/starrocks/pull/75909)
- 在展示或记录配置和任务定义的所有位置，凭据值均会被屏蔽：`ADMIN SHOW FRONTEND CONFIG`、FE 的 `/variable` 页面、BE 的 `/varz`、`information_schema.be_configs`、归档到 `task_run_history` 的任务运行定义，以及写入 FE 和 BE HTTP 日志的请求 URI 和请求头。通过 `ADMIN SET FRONTEND CONFIG`、FE 的 `/api/_set_config` 端点以及 BE 的 `/api/update_config` 端点设置的值也同样会被屏蔽。[#80097](https://github.com/StarRocks/starrocks/pull/80097) [#80040](https://github.com/StarRocks/starrocks/pull/80040) [#80492](https://github.com/StarRocks/starrocks/pull/80492) [#80444](https://github.com/StarRocks/starrocks/pull/80444) [#80280](https://github.com/StarRocks/starrocks/pull/80280) [#80447](https://github.com/StarRocks/starrocks/pull/80447) [#80515](https://github.com/StarRocks/starrocks/pull/80515) [#80203](https://github.com/StarRocks/starrocks/pull/80203)
- FE HTTP 端点 `/service_id`、`/dump` 和 `/dump_starmgr` 现在需要管理员用户权限。[#80749](https://github.com/StarRocks/starrocks/pull/80749) [#80701](https://github.com/StarRocks/starrocks/pull/80701)

### 功能优化 \{#improvements}

- 新增表属性 `enable_statistic_auto_collect`。通过 `CREATE TABLE` 或 `ALTER TABLE` 将其设置为 `false` 可将该表排除在自动统计信息采集之外。由数据导入触发的采集不受影响。[#81027](https://github.com/StarRocks/starrocks/pull/81027)
- 通过将新的 FE 配置项 `authentication_ldap_case_insensitive` 设置为 `true`（默认值 `false`），可以实现对 LDAP 和 Active Directory 用户及组名称的大小写不敏感匹配。[#79255](https://github.com/StarRocks/starrocks/pull/79255)
- 新增 `ALTER GROUP PROVIDER`，用于就地更新组提供者的属性。[#79166](https://github.com/StarRocks/starrocks/pull/79166)
- 在 Query Queue V2 下，`query_queue_mem_used_pct_limit` 会应用于所有运行查询的节点。[#81026](https://github.com/StarRocks/starrocks/pull/81026)
- 大规模主键索引在发布阶段的删除操作速度得到提升，采用了并行反向查找以及墓碑 sstable 路径。[#76785](https://github.com/StarRocks/starrocks/pull/76785)
- 带搜索条件的 `CASE WHEN` 仅对实际到达该分支的行进行求值。[#79484](https://github.com/StarRocks/starrocks/pull/79484)
- `convert_tz` 会缓存夏令时偏移量的查找结果，而不是针对每一行都重新转换时区。[#78459](https://github.com/StarRocks/starrocks/pull/78459)
- `UNNEST` 针对可空数组实现了零拷贝，配合 `LEFT JOIN` 时会一次展开一个 chunk 的多个数组，而不是物化整个输入 chunk，其余表函数也增加了防止内存溢出的保护。[#79221](https://github.com/StarRocks/starrocks/pull/79221) [#79222](https://github.com/StarRocks/starrocks/pull/79222)
- 一个 fragment 的执行计划只会针对其all实例请求序列化一次。[#80716](https://github.com/StarRocks/starrocks/pull/80716)
- Colocate 聚合和窗口函数现在可以在 range-colocate 表上进行规划。[#79946](https://github.com/StarRocks/starrocks/pull/79946)
- 回合了主分支中关于tablet预拆分的改进，包括用于限制基于采样的tablet预拆分扫描数据量的新 FE 配置项（`tablet_pre_split_data_tier_scan_byte_limit`、`tablet_pre_split_data_tier_min_scan_files`、`tablet_pre_split_data_tier_max_scan_files` 和 `tablet_pre_split_meta_tier_footer_read_parallelism`）。[#79949](https://github.com/StarRocks/starrocks/pull/79949)

### 安全性 \{#security}

- 将 pprof 升级到 release/20261012。[#80439](https://github.com/StarRocks/starrocks/pull/80439)
- 将 lz4-java 升级到 1.11.4。[#80319](https://github.com/StarRocks/starrocks/pull/80319)
- 将 BouncyCastle 升级到 1.86，Jackson 升级到 2.21.7，zstd-jni 升级到 1.5.7-15，Parquet 升级到 1.18.1。[#80248](https://github.com/StarRocks/starrocks/pull/80248)
- 将 Broker 使用的 Tomcat 升级到 9.0.121。[#79719](https://github.com/StarRocks/starrocks/pull/79719)
- 将 ZooKeeper 升级到 3.8.7 和 3.9.6。[#79707](https://github.com/StarRocks/starrocks/pull/79707)
- 对 `lookup_string` 的参数进行转义以防止SQL注入，其内部查询以调用者的身份运行。[#79991](https://github.com/StarRocks/starrocks/pull/79991) [#79959](https://github.com/StarRocks/starrocks/pull/79959)
- FE Web 控制台对管理员及主机提供的值和表格单元格进行 HTML 编码，包括在 `/system` 和 `/query` 页面上的内容，并对 `/proc_profile` 上的 `node` 参数进行校验和编码。[#80656](https://github.com/StarRocks/starrocks/pull/80656) [#80639](https://github.com/StarRocks/starrocks/pull/80639) [#80582](https://github.com/StarRocks/starrocks/pull/80582) [#80565](https://github.com/StarRocks/starrocks/pull/80565) [#80689](https://github.com/StarRocks/starrocks/pull/80689)

### 问题修复 \{#bug-fixes}

修复了以下问题：

- Iceberg equality deletes 没有限定在其所属分区范围内。[#81139](https://github.com/StarRocks/starrocks/pull/81139)
- 即使有可用空闲空间，Datacache 内存写入仍可能被驱逐概率拒绝。[#80542](https://github.com/StarRocks/starrocks/pull/80542)
- 桶属性未随 Pipeline 源的分区类型一起传递。[#80099](https://github.com/StarRocks/starrocks/pull/80099)
- 发送方已经对齐的 bucket shuffle join 输入被重新分区。[#79769](https://github.com/StarRocks/starrocks/pull/79769)
- Driver 分区在 `EXCEPT`/`INTERSECT` 与哈希 JOIN 输入之间未对齐。[#78920](https://github.com/StarRocks/starrocks/pull/78920)
- `yearweek` 在接收到无效的 `str_to_date` 输入时导致 BE 崩溃。[#78358](https://github.com/StarRocks/starrocks/pull/78358)
- 在 spill channel 关闭期间，排队中的 spill 数据任务仍可能继续运行。[#80252](https://github.com/StarRocks/starrocks/pull/80252)
- Lake Compaction 读缓冲区未受到每个 worker 内存预算的限制。[#80229](https://github.com/StarRocks/starrocks/pull/80229)
- 当 Java UDAF 合并无法包装其状态时返回了 `NULL` 而不是报错失败。[#80098](https://github.com/StarRocks/starrocks/pull/80098)
- 失败的 spill 追加或分区拆分可能会丢失。[#79457](https://github.com/StarRocks/starrocks/pull/79457)
- 在 `char` 为无符号类型的平台上，ORC `TINYINT` 的最小值/最大值统计信息有误。[#81086](https://github.com/StarRocks/starrocks/pull/81086)
- flat JSON 路径推导 trie 没有大小限制。[#80316](https://github.com/StarRocks/starrocks/pull/80316)
- 在 Compaction 推导其读取布局时，标量类型的 flat JSON 子列可能被丢弃。[#80295](https://github.com/StarRocks/starrocks/pull/80295)
- 当扫描谓词构建器折叠掉其唯一的合取项时，被扫描列没有被输出。[#80572](https://github.com/StarRocks/starrocks/pull/80572)
- `PSlotDescriptor.global_dict_words` 未被声明为字节类型，导致出现 UTF-8 警告。[#78657](https://github.com/StarRocks/starrocks/pull/78657)
- 通过 `update_memory_limit_percent` 调整主键索引缓存大小时未除以 100。[#81067](https://github.com/StarRocks/starrocks/pull/81067)
- 处于 `TRUE` 透明模式下的物化视图在某个基表被删除后停止扫描该物化视图。[#81088](https://github.com/StarRocks/starrocks/pull/81088)
- 超出范围的向量搜索查询向量会导致 BE 中止，而不是被拒绝。[#78658](https://github.com/StarRocks/starrocks/pull/78658)
- ANN 距离列不能容忍被复用的扫描 chunk。[#78686](https://github.com/StarRocks/starrocks/pull/78686)
- 对 flat JSON 列的根节点执行 `json_query` 时未重建整个文档。[#80772](https://github.com/StarRocks/starrocks/pull/80772)
- `tokenize()` 的结果大小是按查看者的数据列而不是按行来计算的。[#80337](https://github.com/StarRocks/starrocks/pull/80337)
- 物化视图刷新所调度的任务在刷新使该物化视图失效时丢失。此修复同时解决了 planner lock 和 Hive `INSERT` 变更的两个后续问题。[#81033](https://github.com/StarRocks/starrocks/pull/81033)
- `INSERT` 可能遗漏 Hive 的源行，而 `SELECT *` 丢弃了资源映射的 Hive 列。[#81032](https://github.com/StarRocks/starrocks/pull/81032)
- 跨作用域的递归 CTE 引用导致栈溢出，而不是被拒绝。[#78533](https://github.com/StarRocks/starrocks/pull/78533)
- 损坏的 Ranger 审计配置导致 catalog 访问失败，并且 Solr 审计所需的 ZooKeeper 类未被打包。[#81075](https://github.com/StarRocks/starrocks/pull/81075)
- 物化视图刷新在非引用基表的锁超时时未能失败，并且 `inspect_mv_refresh_info` 在已持有锁的情况下仍获取基表锁。[#81036](https://github.com/StarRocks/starrocks/pull/81036)
- 超出 `DATETIME` 取值范围的统计边界未被拒绝。[#79388](https://github.com/StarRocks/starrocks/pull/79388)
- 比较超过 `INT64_MAX` 阈值的 JSON 数字时抛出异常。[#78969](https://github.com/StarRocks/starrocks/pull/78969)
- 关于某个选择范围不应长于其所选取列的断言可能会失败。[#80324](https://github.com/StarRocks/starrocks/pull/80324)
- 过大而无法容纳的 `array_generate` 范围会导致 BE 中止，而不是被拒绝。[#80205](https://github.com/StarRocks/starrocks/pull/80205)
- 从 JSON/VARIANT 转换为 MAP 的常量类型转换未能保持与其输入等长。[#80679](https://github.com/StarRocks/starrocks/pull/80679)
- scalar shuttle 未注册其需要克隆的 dict 运算符。[#79310](https://github.com/StarRocks/starrocks/pull/79310)
- 全局字典在快速 schema change 之后未被失效。[#78599](https://github.com/StarRocks/starrocks/pull/78599)
- 在 CTE consume project 中重复的列未被克隆。[#80158](https://github.com/StarRocks/starrocks/pull/80158)
- 宽松字典未按无符号字节顺序进行合并。[#78659](https://github.com/StarRocks/starrocks/pull/78659)
- 聚合函数的结构体字段被 `GROUP BY` 检查拒绝。[#79649](https://github.com/StarRocks/starrocks/pull/79649)
- 并非每次物化视图失效及其版本映射清除都被记录到日志中。[#81030](https://github.com/StarRocks/starrocks/pull/81030)
- 物化视图版本映射修复被物理分区 ID 查找跳过。[#78469](https://github.com/StarRocks/starrocks/pull/78469)
- 在分析结果 chunk 发布之前，窗口函数错误未被检查。[#81022](https://github.com/StarRocks/starrocks/pull/81022)
- MIN/MAX 对 LIST 分区列的处理，可能会从数据实际上并不包含的分区值中读取。[#80558](https://github.com/StarRocks/starrocks/pull/80558)
- 某个表失败后，analyze 作业停止收集剩余的表。[#81028](https://github.com/StarRocks/starrocks/pull/81028)
- 在基表分区频繁变动和批量刷新的情况下，物化视图刷新和新鲜度计算出错。[#81034](https://github.com/StarRocks/starrocks/pull/81034)
- 修复了三个较小的历史遗留 FE 元数据并发问题。[#81037](https://github.com/StarRocks/starrocks/pull/81037)
- 当 skew join v2 替换 shuffle exchange 时，全局字典表达式被丢弃。[#78672](https://github.com/StarRocks/starrocks/pull/78672)
- 在被删除的基表分区之后仍然存在的物化视图分区未被刷新。[#79057](https://github.com/StarRocks/starrocks/pull/79057)
- 对齐内存分配失败时未抛出 `std::bad_alloc`。[#79054](https://github.com/StarRocks/starrocks/pull/79054)
- ALTER 完成报告未能与作业匹配，排队中的 ALTER 任务已过期。[#81018](https://github.com/StarRocks/starrocks/pull/81018)
- 换入的临时分区的 tablet 被强制删除。[#81014](https://github.com/StarRocks/starrocks/pull/81014)
- 残留的 `INSERT` 作业未根据其事务状态完成或取消。[#80711](https://github.com/StarRocks/starrocks/pull/80711)
- 增量物化视图中复杂类型的行标识键未被拒绝。[#79312](https://github.com/StarRocks/starrocks/pull/79312)
- `greplog` 不支持带年份前缀的 glog 条目。[#80990](https://github.com/StarRocks/starrocks/pull/80990)
- 归档的物化视图任务运行历史中的 List 分区值未被精简。[#80944](https://github.com/StarRocks/starrocks/pull/80944)
- 在增量维护的物化视图上接受了 `auto_refresh_partitions_limit`。[#79051](https://github.com/StarRocks/starrocks/pull/79051)
- 当读取未通过校验时，未重新读取 bundle 元数据。[#79928](https://github.com/StarRocks/starrocks/pull/79928)
- 当提交时源分区已不存在时，`INSERT OVERWRITE` 没有报告明确的错误。[#79902](https://github.com/StarRocks/starrocks/pull/79902)
- 物化视图版本映射重置、`TRUE` 透明改写以及时效性缓存可能返回过期结果。[#80893](https://github.com/StarRocks/starrocks/pull/80893)
- List 分区漂移未与实时表进行比较。[#78470](https://github.com/StarRocks/starrocks/pull/78470)
- 轻量 Compaction 未保留主键索引大小。[#80746](https://github.com/StarRocks/starrocks/pull/80746)
- 一个常驻的强制设置导致增量维护的物化视图被重新物化。[#79366](https://github.com/StarRocks/starrocks/pull/79366)
- 增量维护的物化视图上的列 `ALTER` 未被拒绝。[#78726](https://github.com/StarRocks/starrocks/pull/78726)
- 物化视图 `lastRefreshTime` 在回放时被重新计算，而不是被记录。[#78467](https://github.com/StarRocks/starrocks/pull/78467)
- IVM 刷新窗口未记录在其提交所依附的事务上。[#79315](https://github.com/StarRocks/starrocks/pull/79315)
- 在 V2 编码的单列定长键上，主键恢复失败。[#80446](https://github.com/StarRocks/starrocks/pull/80446)
- 在 cloud 原生的主键表 tablet 中，冲突 Compaction 中未被 all 的文件被记录为孤立文件。[#80445](https://github.com/StarRocks/starrocks/pull/80445)
- 存算分离模式下物化视图基表分区比较的是 FE 本地版本时间。[#79945](https://github.com/StarRocks/starrocks/pull/79945)
- 当 `MultiGet` 提前返回时，索引块迭代器未被释放。[#79380](https://github.com/StarRocks/starrocks/pull/79380)
- memtable 未接收到的行也被计入其行数限制。[#79705](https://github.com/StarRocks/starrocks/pull/79705)
- 嵌套的 Iceberg REST 命名空间在被列出期间被删除会导致报错。[#80846](https://github.com/StarRocks/starrocks/pull/80846)
- 修复 planner 锁后遗留的重复策略查找和无限等待问题。[#80943](https://github.com/StarRocks/starrocks/pull/80943)
- tablet 元数据导出未被限制在本地缓存范围内。[#78449](https://github.com/StarRocks/starrocks/pull/78449)
- driver 队列计数器在资源组更新时未被保留。[#80889](https://github.com/StarRocks/starrocks/pull/80889)
- 格式错误的数值型 HTTP 参数可能导致 BE 崩溃。[#79022](https://github.com/StarRocks/starrocks/pull/79022)
- `convert_hash_set_to_chunk` 中存在读取溢出问题。[#80709](https://github.com/StarRocks/starrocks/pull/80709)
- `ALTER VIEW`、`INSERT OVERWRITE` 和物化视图刷新中存在历史遗留的元数据锁竞争问题，以及剩余的 planner 和物化视图元数据锁缺口。[#80891](https://github.com/StarRocks/starrocks/pull/80891) [#80892](https://github.com/StarRocks/starrocks/pull/80892)
- lake 复制中文件句柄的复用处理问题。[#79359](https://github.com/StarRocks/starrocks/pull/79359)
- 在 Bloom filter 下接受了不受支持的列类型。[#80871](https://github.com/StarRocks/starrocks/pull/80871)
- 导入 sink 的关闭操作运行在 pipeline poller 上。[#80761](https://github.com/StarRocks/starrocks/pull/80761)
- 当单次采集查询失败时，已采集的外部统计信息被丢弃。[#79518](https://github.com/StarRocks/starrocks/pull/79518)
- BE Thrift RPC 重试没有加入抖动，FE Thrift RPC 重试在退避时也没有加入抖动。[#80847](https://github.com/StarRocks/starrocks/pull/80847) [#80714](https://github.com/StarRocks/starrocks/pull/80714)
- Pipeline driver 队列层级可能导致 0 级饥饿。[#80760](https://github.com/StarRocks/starrocks/pull/80760)
- 当 `INSERT ... SELECT` 源刷新时，资源映射外部表未按声明保留。[#80546](https://github.com/StarRocks/starrocks/pull/80546)
- 相互依赖的重度表达式被下推。[#80710](https://github.com/StarRocks/starrocks/pull/80710)
- brpc future 超时约定被破坏。[#80783](https://github.com/StarRocks/starrocks/pull/80783)
- 在获取 planner 锁之前，一个已分析的 `INSERT` 的源未被刷新，并且在规划读取视图的基表之前未对其加锁。[#80786](https://github.com/StarRocks/starrocks/pull/80786) [#80784](https://github.com/StarRocks/starrocks/pull/80784)
- Iceberg REST catalog 在其连接器关闭时未被关闭，并且 Iceberg 元数据缓存后台执行器在 catalog 关闭时未被关闭。[#80712](https://github.com/StarRocks/starrocks/pull/80712) [#80598](https://github.com/StarRocks/starrocks/pull/80598)
- `SHOW PROC` 作业详情对于无法显示的 ALTER 作业进行了类型转换失败，而不是直接拒绝它们。[#80719](https://github.com/StarRocks/starrocks/pull/80719)
- 在 rowset 迁移期间向量索引文件未被复制。[#80391](https://github.com/StarRocks/starrocks/pull/80391)
- 当 `ADD ROLLUP` 失败时，lake rollup 的 tablet 未从倒排索引中移除。[#80717](https://github.com/StarRocks/starrocks/pull/80717)
- optimize 和 merge-partition 作业的重写任务未过期。[#80718](https://github.com/StarRocks/starrocks/pull/80718)
- 远程读取失败后共享缓冲区可能被重复使用。[#79965](https://github.com/StarRocks/starrocks/pull/79965)
- 按列最小/最大值统计信息采集未被限流。[#78544](https://github.com/StarRocks/starrocks/pull/78544)
- 空闲的 FE Thrift 连接池连接被重用而非替换。[#80715](https://github.com/StarRocks/starrocks/pull/80715)
- 当可见的 `DELETE` 事务被应用时，`hasDelete` 未被恢复。[#80592](https://github.com/StarRocks/starrocks/pull/80592)
- decimal 类型 `map_agg` 键的精度丢失。[#80673](https://github.com/StarRocks/starrocks/pull/80673)
- FE brpc 客户端现在使用 `com.starrocks:jprotobuf-rpc-core`。[#80684](https://github.com/StarRocks/starrocks/pull/80684)
- 事务 Stream Load 上非布尔类型的 `partial_update` 请求头未被拒绝。[#79350](https://github.com/StarRocks/starrocks/pull/79350)
- 针对拆分后的 lake 扫描未报告物理 segment 和 rowset 数量。[#80359](https://github.com/StarRocks/starrocks/pull/80359)
- 并非每次 lake rollup 作业期间都会跳过 lake Compaction。[#80655](https://github.com/StarRocks/starrocks/pull/80655)
- 主键表 Compaction 中出现行来源溢出。[#80472](https://github.com/StarRocks/starrocks/pull/80472)
- FE web 控制台的 `/system` 页面未查找原始 proc 路径。[#80648](https://github.com/StarRocks/starrocks/pull/80648)
- 统计信息 DML 未绑定内部上下文。[#80473](https://github.com/StarRocks/starrocks/pull/80473)
- 实例化之前未预留 driver 容量。[#80495](https://github.com/StarRocks/starrocks/pull/80495)
- 禁用块缓冲时未复制对端缓存数据。[#80438](https://github.com/StarRocks/starrocks/pull/80438)
- 当一个分区具有多个索引时，并非所有的创建任务都被计数。[#80640](https://github.com/StarRocks/starrocks/pull/80640)
- 运行时重置为自动模式对主键表并行执行线程不起作用。[#80567](https://github.com/StarRocks/starrocks/pull/80567)
- 在使用事件调度器的本地交换中出现了丢失的 sink 唤醒。[#80547](https://github.com/StarRocks/starrocks/pull/80547)
- 与 FE 范围不一致的 tablet 合并和 tablet 拆分结果未被拒绝。[#80538](https://github.com/StarRocks/starrocks/pull/80538) [#80418](https://github.com/StarRocks/starrocks/pull/80418)
- 一旦某个分区超出 32 位二进制偏移范围，哈希连接就不再保持分区化。[#80488](https://github.com/StarRocks/starrocks/pull/80488)
- bypass-write 事务开始超时请求头未被视为以秒为单位。[#80548](https://github.com/StarRocks/starrocks/pull/80548)
- FE 内存中保留的 alter 作业历史记录没有上限。[#80465](https://github.com/StarRocks/starrocks/pull/80465)
- 即使外连接没有拒绝 NULL 扩展的行，也会根据记录的 NULL 拒绝结果对其进行转换；并且连接 NULL 拒绝结果会在 limit、top-N 或 window 之下转换外连接。[#80510](https://github.com/StarRocks/starrocks/pull/80510) [#79974](https://github.com/StarRocks/starrocks/pull/79974)
- 连接重排序丢弃了无列及非确定性的连接谓词。[#80511](https://github.com/StarRocks/starrocks/pull/80511)
- `BITMAP` 索引在 BE 无法编码的列类型上被接受。[#80459](https://github.com/StarRocks/starrocks/pull/80459)
- 一个重复包含同一个键的短路 `IN` 列表多次返回了同一行。[#80318](https://github.com/StarRocks/starrocks/pull/80318)
- 当设置了 `enclose` 或 `escape` 时，CSV 文件被拆分为字节范围。[#80386](https://github.com/StarRocks/starrocks/pull/80386)
- 无法读取按列设置的 ZSTD 压缩字典。[#77355](https://github.com/StarRocks/starrocks/pull/77355)
- 失败的多表事务 Stream Load 提交错误未被正确报告。[#80351](https://github.com/StarRocks/starrocks/pull/80351)
- 请求 URI 未使用与各 action 相同的查询解析器进行清理。[#80461](https://github.com/StarRocks/starrocks/pull/80461)
- 不带 ID 的 `CANCEL ALTER TABLE ROLLUP` 未取消 all rollup 作业。[#80450](https://github.com/StarRocks/starrocks/pull/80450)
- 带有 `LIMIT` 的 `information_schema.columns` 未从被查询的 catalog 返回应答。[#80448](https://github.com/StarRocks/starrocks/pull/80448)
- Paimon 分支状态未按表隔离，已删除的 Paimon 分区未从元数据缓存中移除。[#79879](https://github.com/StarRocks/starrocks/pull/79879) [#79442](https://github.com/StarRocks/starrocks/pull/79442)
- Range 分布的 tablet 创建时未带有各自的初始元数据。[#80421](https://github.com/StarRocks/starrocks/pull/80421)
- 通过 `INSERT INTO FILES` 导出为 CSV 时，重复的输出列名被拒绝。[#79897](https://github.com/StarRocks/starrocks/pull/79897)
- `FULL OUTER JOIN ... USING` 列在作为连接键时未保持其自身类型。[#79655](https://github.com/StarRocks/starrocks/pull/79655)
- 连接器的 I/O 操作在 FE 元数据锁下执行。[#80416](https://github.com/StarRocks/starrocks/pull/80416)
- 当 SQL 方言为 Trino 时，Iceberg 的时间旅行功能失效。[#79171](https://github.com/StarRocks/starrocks/pull/79171)
- Iceberg 的 decimal manifest 边界值未使用最小长度的二进制补码编码。[#78456](https://github.com/StarRocks/starrocks/pull/78456)
- Parquet、ORC 和 CSV 读取路径中的数据驱动分配未受查询内存限制约束。[#78573](https://github.com/StarRocks/starrocks/pull/78573)
- 已卸载或失败的统计信息被缓存为“无统计信息”。[#80236](https://github.com/StarRocks/starrocks/pull/80236)
- 分区列或排序键列为生成列的导入未被预先拆分，且 tablet 的预拆分样本未在导入时区下运行。[#80390](https://github.com/StarRocks/starrocks/pull/80390) [#80362](https://github.com/StarRocks/starrocks/pull/80362)
- 主键表 tablet 清理被计为错误状态。[#79558](https://github.com/StarRocks/starrocks/pull/79558)
- LDAP 组提供程序在 FE 重启后或在 AD 引用（referral）发生时变得不可用。[#80361](https://github.com/StarRocks/starrocks/pull/80361)
- 使用内联服务账号密钥从 GCS 执行 Broker Load 时，在 gcs-connector 3.x 上失败。[#78649](https://github.com/StarRocks/starrocks/pull/78649)
- 当日期时间早于第 0 年时，周相关函数会中止。[#80228](https://github.com/StarRocks/starrocks/pull/80228)
- 将 StarOS 和 starlet 升级至 v4.1.5。[#80347](https://github.com/StarRocks/starrocks/pull/80347)
- MemSpaceMonitor 线程在销毁时未被停止，导致 BE 启动失败时发生 SIGABRT。[#79691](https://github.com/StarRocks/starrocks/pull/79691)
- 连接器查询触发的 analyze 操作泄漏了一个正在运行的任务，并留下了一个失效的调度器。[#80329](https://github.com/StarRocks/starrocks/pull/80329)
- 当 `createShard` 在未收到 StarMgr 响应的情况下失败时，未被重新执行一次。[#80312](https://github.com/StarRocks/starrocks/pull/80312)
- ASOF 连接右侧的过滤条件未被保留在连接之上。[#80303](https://github.com/StarRocks/starrocks/pull/80303)
- `NOT LIKE` 的 NGRAMBF 裁剪结果不正确。[#79243](https://github.com/StarRocks/starrocks/pull/79243)
- 连接和范围谓词的基数估算结果有误。[#80242](https://github.com/StarRocks/starrocks/pull/80242)
- 在 Top-N 聚合被拆分时，NULL 分组会丢失。[#80143](https://github.com/StarRocks/starrocks/pull/80143)
- 自适应物化视图分区刷新不支持 cloud 内表。[#80270](https://github.com/StarRocks/starrocks/pull/80270)
- 当没有需要清理的表时，`AnalyzeMgr.clearStalePartitionStats` 会抛出 `IllegalStateException`。[#80265](https://github.com/StarRocks/starrocks/pull/80265)
- `ViewsSystemTable` 持有的是数据库读锁，而非密集型表锁。[#78344](https://github.com/StarRocks/starrocks/pull/78344)
- `DROP PARTITIONS WHERE` 丢弃了其条件不匹配的行。[#79958](https://github.com/StarRocks/starrocks/pull/79958)
- Lake `delete_tablets` 的记录数量未受 tablet 数加版本数的约束。[#80243](https://github.com/StarRocks/starrocks/pull/80243)
- 当最后一个发送方离开正在结束的 `SinkBuffer` 时，未通知 Sink 观察者。[#80155](https://github.com/StarRocks/starrocks/pull/80155)
- 刷新过程中未忽略遗留的物化视图任务 `postRun`。[#79943](https://github.com/StarRocks/starrocks/pull/79943)
- 组提供程序的文件 I/O 未受限制，且在就绪之前，被重放的提供程序未被桥接。[#79896](https://github.com/StarRocks/starrocks/pull/79896)
- 在 Compaction 等待应用时，BE 在主键表的 `get_compaction_status` 中崩溃。[#80167](https://github.com/StarRocks/starrocks/pull/80167)
- 在缓存驱逐时文件系统断开连接。[#80087](https://github.com/StarRocks/starrocks/pull/80087)
- 一次 0 行的提交被计为整张表的已导入行数。[#80182](https://github.com/StarRocks/starrocks/pull/80182)
- 混合了捆绑语句和独立语句的多语句事务未被发布。[#79894](https://github.com/StarRocks/starrocks/pull/79894)
- 已完成的事务可能在 `finalStatusTransactionStateDeque` 中被多次加入队列。[#79988](https://github.com/StarRocks/starrocks/pull/79988)
- Parquet 嵌套类型在模式解析、推断、写入器和读取器中的边界情况问题。[#80085](https://github.com/StarRocks/starrocks/pull/80085)
- 针对带有特殊字符的 struct 转换的 Variant 路径问题。[#74168](https://github.com/StarRocks/starrocks/pull/74168)
- 在分组键为 null 时，NULL 直通运行时过滤器被下推到 `RepeatNode` 之下。[#80051](https://github.com/StarRocks/starrocks/pull/80051)
- 主键去重重写丢弃了其他聚合结果。[#79895](https://github.com/StarRocks/starrocks/pull/79895)
- Iceberg 表的缓存刷新可能会覆盖并发发生的失效操作。[#79968](https://github.com/StarRocks/starrocks/pull/79968)
- `SET_VAR` 提示的 `custom_query_id` 未被记录到审计日志中。[#79871](https://github.com/StarRocks/starrocks/pull/79871)
- JSON 扩展列被注册到了共享的 catalog 表上。[#78611](https://github.com/StarRocks/starrocks/pull/78611)
- 预处理语句在 `COM_STMT_CLOSE` 上发生泄漏。[#79860](https://github.com/StarRocks/starrocks/pull/79860)
- 一个 range-colocate 分桶分配过期的查询未被重新规划。[#79927](https://github.com/StarRocks/starrocks/pull/79927)
- 物化视图落盘的 I/O 在多播交换完成之前未被保持活跃。[#79899](https://github.com/StarRocks/starrocks/pull/79899)
- 集合操作或空值安全连接的 range 子节点前的 exchange 被移除。[#79901](https://github.com/StarRocks/starrocks/pull/79901)
- 当事件调度器关闭时，BE 在 `LookUpOperator` 中崩溃。[#79711](https://github.com/StarRocks/starrocks/pull/79711)
- `sys.fe_memory_usage` 读取了一个空映射，而不是进行收集。[#79809](https://github.com/StarRocks/starrocks/pull/79809)
- 落盘排序完成未被同步，完成错误也未被保留。[#79521](https://github.com/StarRocks/starrocks/pull/79521)
- 压缩分组键序列化器不接受仅为 NULL 的常量。[#79499](https://github.com/StarRocks/starrocks/pull/79499)
- 一次物化视图改写在投影中遗留了一个聚合。[#79611](https://github.com/StarRocks/starrocks/pull/79611)
- ADLS2 账户作用域未被保留，其凭据也未被脱敏。[#79526](https://github.com/StarRocks/starrocks/pull/79526)
- `QueryCumulativeCpuTime` 和 `QuerySpillBytes` 在每个 fragment 实例中被重复计数。[#79774](https://github.com/StarRocks/starrocks/pull/79774)
- 当 `SHOW` 语句的 `LIMIT` 偏移量超出末尾时未返回空结果。[#79764](https://github.com/StarRocks/starrocks/pull/79764)
- 当导入落盘合并任务被取消时，完成回调未被响应。[#79360](https://github.com/StarRocks/starrocks/pull/79360)
- `SHOW ALTER TABLE OPTIMIZE` 未能根据自身列解析 `ORDER BY`。[#79687](https://github.com/StarRocks/starrocks/pull/79687)
- 安全集成（LDAP）用户无法在非 MySQL 通道上进行身份验证。[#79165](https://github.com/StarRocks/starrocks/pull/79165)
- `EvHttpServer` 在关闭时关闭了无关的文件描述符。[#79582](https://github.com/StarRocks/starrocks/pull/79582)
- 在某个 proc 目录的行成为结果的场景中，未检查其行宽。[#79619](https://github.com/StarRocks/starrocks/pull/79619)
- 一个非正数的 `compact_threads` 会导致 CN 崩溃，而不是被拒绝。[#79641](https://github.com/StarRocks/starrocks/pull/79641)
- `TableKeeper` 导入了 `jdk.jshell`。[#79520](https://github.com/StarRocks/starrocks/pull/79520)

## 4.1.6 \{#416}

发布日期：2026 年 9 月 30 日

### 行为变更 \{#behavior-changes-1}

- AWS SDK 的默认 HTTP 客户端从 Poco 切换回 SDK 自带的客户端：`enable_poco_client_for_aws_sdk` 现在默认设置为 `false`。将其设置为 `true` 可恢复之前的行为。[#78375](https://github.com/StarRocks/starrocks/pull/78375)
- 在线优化表现在需要主动开启：FE 配置项 `enable_online_optimize_table` 默认为 `false`，因此 `ALTER TABLE ... DISTRIBUTED BY ...` 会再次阻塞写入，除非该配置被开启。[#78206](https://github.com/StarRocks/starrocks/pull/78206)
- jemalloc 的专用超大对象 arena 默认启用，因此大内存分配不再落在调用线程的每 CPU arena 中。这提升了大缓冲区的复用率，减少了内存碎片，并改变了 BE 和 CN 内存在各 arena 之间的分布方式。[#78396](https://github.com/StarRocks/starrocks/pull/78396)
- 存算分离的 tablet 元数据和事务日志文件默认使用 Adler-32 校验和写入（`lake_enable_protobuf_file_checksum`），因此在读取时可以检测到损坏。读取方此前只要文件携带校验和就已经会进行验证，因此无需任何降级步骤。[#75334](https://github.com/StarRocks/starrocks/pull/75334)
- 存算分离主键导入中，针对同一个键混合了 `UPSERT` 和 `DELETE` 行的场景，现在默认会保留其事务内顺序（`lake_enable_pk_preserve_txn_delete_order`）。[#76211](https://github.com/StarRocks/starrocks/pull/76211)
- 多表事务现在参与存算分离的批量发布，而不是逐个事务发布，这消除了 CDC 微批量扇出到多个表时曾遇到的串行化问题。[#77029](https://github.com/StarRocks/starrocks/pull/77029)
- 存算分离分区现在会直接前进到批量发布的最终版本，而不是经过每一个中间版本，因此查询不会再请求到一个没有 tablet 元数据对象的中间版本，此前该问题会表现为 `The specified key does not exist`。[#79175](https://github.com/StarRocks/starrocks/pull/79175)
- 持续失败的存算分离发布现在会退避并限流其重试日志，而不是在每次尝试时都输出 `previous publish version task has not finished` 消息，此前这可能在故障期间占用 FE Leader 的 CPU 并产生大量日志。[#79299](https://github.com/StarRocks/starrocks/pull/79299)
- Del-file 墓碑行现在会计入 `cloud_native_pk_index_rebuild_rows_threshold`，因此以删除为主的存算分离负载会按预期提前触发主键索引刷盘。[#76222](https://github.com/StarRocks/starrocks/pull/76222)
- 存算分离列更新（column-upsert）发布在覆盖自身 segment 时现在会合并 delete vector，此前这可能导致同一个主键存在两行有效数据。该问题只有在很久之后 CN 重启重建持久化索引并报告 `insert found duplicate key` 时才会显现。[#78797](https://github.com/StarRocks/starrocks/pull/78797)
- 存算分离主键索引 Compaction 器不再在刷盘端持续生成 sstable 的同时悄无声息地停止，此前这会导致某个 tablet 的持久化索引无限增长。[#78812](https://github.com/StarRocks/starrocks/pull/78812)
- HDFS 文件在关闭之前不再被 `hsync`。`hsync` 原本是为了绕开一个崩溃问题而设的权宜之计，而该崩溃问题已通过另一处改动修复，但它会在每次关闭时都对每个 DataNode 副本强制执行一次 `fsync()`。[#78864](https://github.com/StarRocks/starrocks/pull/78864)
- 在解码和裁剪过程中，不再直接信任一个漏报 NULL 数量的 Parquet `null_count`。由较旧版本 parquet-mr 写入的文件此前会返回损坏的值或丢失行。[#78329](https://github.com/StarRocks/starrocks/pull/78329)
- 对 `!=`、`NOT IN` 和 `IS NOT NULL` 的 List 分区裁剪不再仅因为某个多值分区的值列表中也包含 `NULL` 而丢弃该分区。针对此类表的查询（包括在连接键上推导出 `IS NOT NULL` 的连接）会返回此前丢失的非空行。[#78946](https://github.com/StarRocks/starrocks/pull/78946)
- 批量 DDL 现在可以构建起始年份为 `0000` 的日期 Range 分区，该值是合法的 `DATE` 取值。此前这样的语句会失败并报出 `the range of the partition is invalid`。[#78746](https://github.com/StarRocks/starrocks/pull/78746)
- `max_partitions_in_one_batch` 以及分区数量限制现在会计入当前请求即将创建的分区数。超限的批量操作会被提前拒绝，而不是被接受后导致后续批量失败。[#79379](https://github.com/StarRocks/starrocks/pull/79379)
- `TRUNCATE TABLE <external_catalog>.<db>.<table>` 现在会根据语句中指定的 catalog（而不是会话当前的 catalog）检查 `DELETE` 权限。之前被允许的用户现在可能被拒绝，反之亦然。[#79047](https://github.com/StarRocks/starrocks/pull/79047)
- 流式聚合调优现在由分组基数驱动，而不是由 `HAVING` 之后剩余的行数驱动，因此 `HAVING` 子句过滤掉大部分分组的查询不会再丢失本地预聚合。[#79064](https://github.com/StarRocks/starrocks/pull/79064)
- 审计日志行现在为针对外部 Iceberg 或 Hive 表、`FILES()` 表函数的 DML，以及异步物化视图刷新，携带 `scanBytes`、`scanRows`、`returnRows`、`cpuCostNs` 和 `memCostBytes` 字段。这些字段此前报告为 `0`。[#74072](https://github.com/StarRocks/starrocks/pull/74072) [#74954](https://github.com/StarRocks/starrocks/pull/74954)
- Query Profile 和 `EXPLAIN ANALYZE` 现在报告的是重试查询实际执行的计划，而不是第一次尝试时的计划。[#79225](https://github.com/StarRocks/starrocks/pull/79225)
- Broker Load 不再将其提交时的 `enable_tablet_pre_split` 快照写回提交客户端的活动会话变量。[#79177](https://github.com/StarRocks/starrocks/pull/79177)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 对于非聚合增量物化视图再次生效；存储填充的 `__ROW_ID__` 列已从物化视图 DDL 列列表中排除。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- `SHOW ALTER TABLE OPTIMIZE` 的 `Operation` 列现在显示可读的 optimize 操作描述，而不是内部对象地址（如 `com.starrocks.sql.ast.OptimizeClause@b5dc069`）。[#75948](https://github.com/StarRocks/starrocks/pull/75948)
- tablet 架构中缺失的列不再会在谓词下推期间导致 BE 中止；该谓词只是不会被下推。[#79234](https://github.com/StarRocks/starrocks/pull/79234)
- 当 `OlapTableSink` 初始化失败时，部分初始化的 sink 不再会经过完整的关闭流程（该流程可能解引用从未创建的状态并导致 BE 中止）。现在会直接返回原始的初始化错误。[#79370](https://github.com/StarRocks/starrocks/pull/79370)
- 短键索引页的几何信息现在会在不发生 32 位溢出的情况下进行验证，因此损坏的页脚会被拒绝，而不是发生回绕并导致堆越界写入。[#79048](https://github.com/StarRocks/starrocks/pull/79048)
- TLS 握手超时的 HTTPS 请求不再被视为已连接并在同一 socket 上重试。停滞的请求现在会在大约一个超时周期后失败，而不是 `connect_timeout` 加上两个接收超时周期。[#79369](https://github.com/StarRocks/starrocks/pull/79369)

### 改进项 \{#improvements-1}

- 现在可以从用户的 `memberOf` 属性解析 LDAP 和 Active Directory 组，因此新的目录组不再需要在集群上配置匹配的组提供程序。[#79254](https://github.com/StarRocks/starrocks/pull/79254)
- 审计日志现在会记录查询的错误消息，而不仅仅是粗粒度的 `ErrorCode`，因此无需关联回 `fe.log` 即可区分语法错误、未知表和权限失败。[#78798](https://github.com/StarRocks/starrocks/pull/78798)
- 新增了服务器级别的 spill 指标，用于汇总 all 个可落盘算子的落盘活动，并按本地和远端存储后端细分。[#71827](https://github.com/StarRocks/starrocks/pull/71827)
- Hudi 元数据读取器现在会在一次扫描的各分区之间复用，而不是每个分区都重新构建，这缩短了启用 Hudi 元数据表时拥有大量分区的表的规划时间。[#79367](https://github.com/StarRocks/starrocks/pull/79367)
- tablet 拆分点现在根据 segment 现有的索引计算，而不是依赖专用的完整排序键索引页，因此从不拆分的表不再需要为写入和存储该页付出代价。[#79050](https://github.com/StarRocks/starrocks/pull/79050)
- `/memz` 现在接受 jemalloc 自身的统计选项，这使得每个 arena 的 dirty 和 muzzy 页计数以及 extent 大小类细分信息可以从正在运行的 BE 中获取。[#79090](https://github.com/StarRocks/starrocks/pull/79090)

### 安全性 \{#security-1}

- [CVE-2026-55971] 将 Apache Thrift 升级到 0.24.0，修复了 C++ 绑定中 `THeaderTransport::untransform()` 的堆缓冲区溢出问题。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-42537，CVE-2026-59646] 将 Apache Ranger 升级到 2.9.0，修复了通过 JDBC URL 注入实现的 RCE 问题；将 Bouncy Castle 升级到 1.85，修复了 DTLS 重组缓冲区溢出问题。[#79331](https://github.com/StarRocks/starrocks/pull/79331)
- [CVE-2026-71290] 将 Apache HttpClient5 升级到 5.6.4。[#79538](https://github.com/StarRocks/starrocks/pull/79538)

### 问题修复 \{#bug-fixes-1}

修复了以下问题：

- `SHOW PROFILELIST`、`ANALYZE PROFILE`、`get_query_profile()` 和 `/api/profile` 会将所有缓存的 profile（包括其他用户的 SQL 文本和计划）返回给任何已认证的用户。新增的 FE 配置项 `authorization_enable_query_profile_access_check` 将缓存的 profile 限制为仅运行该查询的用户以及拥有 `SYSTEM OPERATE` 权限的用户可见。[#79375](https://github.com/StarRocks/starrocks/pull/79375)
- 授权检查、`INSERT` 规划、视图展开、权限检查以及 `inspect_mv_refresh_info` 在持有 FE 元数据锁的情况下进行了外部 catalog 往返调用，因此缓慢的 metastore 会使所有等待同一把锁的 DDL 都发生停滞。现在这些调用已移至锁外进行。[#79319](https://github.com/StarRocks/starrocks/pull/79319) [#79226](https://github.com/StarRocks/starrocks/pull/79226)
- gtid 和分区版本 epoch 可能会在非 Leader FE 上生成，导致两个节点在同一时刻生成相同的编号。[#78799](https://github.com/StarRocks/starrocks/pull/78799)
- `FULL OUTER JOIN ... USING (c1, c2, ...)` 返回了错误的值：第一个 `USING` 列的值是正确的，而之后每一列都携带了前一列的值。在同一查询中添加 `ORDER BY <ordinal>` 会因 `IndexOutOfBoundsException` 而失败。[#79298](https://github.com/StarRocks/starrocks/pull/79298)
- 当 `GROUPING SETS`、`ROLLUP` 或 `CUBE` 被改写为 union 时，`HAVING` 谓词会丢失，导致查询返回了本应被过滤掉的行。[#79415](https://github.com/StarRocks/starrocks/pull/79415)
- `GROUP BY ROLLUP(a, b, a)`，以及在同一集合中两次命名同一列的 `CUBE` 或 `GROUPING SETS`，会生成携带重复键的分组集。[#78261](https://github.com/StarRocks/starrocks/pull/78261)
- 在聚合键表之上移除聚合操作时，会在查询的 `LIMIT` 到达存储层之前将其丢弃，导致 BE 读取整张表。[#79414](https://github.com/StarRocks/starrocks/pull/79414)
- 将 `OR` JOIN 拆分为 union 时，各分支的对齐不一致，因为 union 的输出列和第一个分支是按列 id 排序的，而其他分支则遵循投影映射的迭代顺序。[#79473](https://github.com/StarRocks/starrocks/pull/79473)
- 倾斜 JOIN 改写会复制外连接中被保留的一侧，导致某一行输入可能被多次返回，并且结果在多次运行之间发生变化。其 salt 表接收 `generate_series` 参数的顺序也有误，导致该序列未产生任何输出。[#79168](https://github.com/StarRocks/starrocks/pull/79168) [#79268](https://github.com/StarRocks/starrocks/pull/79268)
- `SELECT DISTINCT` 接受了针对 `DISTINCT` 所丢弃列的 `ORDER BY`，并且窗口之下的去重下推会在投影中留下已丢弃的列。[#77217](https://github.com/StarRocks/starrocks/pull/77217) [#77218](https://github.com/StarRocks/starrocks/pull/77218)
- 对诸如 `bitmap_union_count()` 这类始终非空的聚合函数进行窗口计算时会丢失其声明的可空性，导致对其执行 `DISTINCT` 或 `GROUP BY` 时因 `error nullablel column` 而失败。[#79009](https://github.com/StarRocks/starrocks/pull/79009)
- `percentile_cont` 合并操作按值而不是按位置跟踪耗尽状态，这可能导致 BE 中止。[#77261](https://github.com/StarRocks/starrocks/pull/77261)
- `count(DISTINCT percentile_hash(...))`，以及任何针对 `PERCENTILE` 值的 `DISTINCT` 聚合函数，都会导致 BE 崩溃；此类调用现已被拒绝。[#78795](https://github.com/StarRocks/starrocks/pull/78795)
- 标量 agg-state 组合算子在空内存池上会崩溃，并且在其嵌套函数上下文上存在竞态问题。[#76910](https://github.com/StarRocks/starrocks/pull/76910)
- 聚合表上的 JSON 子字段聚合会因原始的 `NullPointerException` 而失败，因为合成的子字段列没有聚合类型。[#77952](https://github.com/StarRocks/starrocks/pull/77952)
- 在 `enable_spill = true` 下，流式预聚合可能会耗尽内存，因为有限的内存预算只会锁定一次，之后不会再修订。[#76702](https://github.com/StarRocks/starrocks/pull/76702)
- 一个正在恢复构建分区时被取消的溢出哈希连接导致 BE 崩溃，原因是探测算子在关闭时没有等待溢出 I/O 任务完成。[#79085](https://github.com/StarRocks/starrocks/pull/79085)
- 本地直通交换器在接收端被取消后仍继续追加数据块，由于 sink 和接收端没有共享所有权，其 channel 可能出现释放后使用的问题。sink 就绪通知也错误地选择了 sink observable 却发出了 source 事件，导致阻塞在 `OUTPUT_FULL` 上的 driver 可能一直保持阻塞状态。[#76603](https://github.com/StarRocks/starrocks/pull/76603) [#77088](https://github.com/StarRocks/starrocks/pull/77088) [#76782](https://github.com/StarRocks/starrocks/pull/76782)
- 查询缓存可能存储一个不完整的 per-tablet 结果并将其提供给后续查询，并且即使时间条件从未到达 digest，一个包含 ASOF join 的 fragment 仍被缓存。[#77066](https://github.com/StarRocks/starrocks/pull/77066) [#77563](https://github.com/StarRocks/starrocks/pull/77563)
- 其时间条件未对两侧进行比较的 ASOF join 会导致 BE 崩溃；现已拒绝此类条件。[#79049](https://github.com/StarRocks/starrocks/pull/79049)
- 在构建运行时过滤器时，一个没有父元组的嵌套 lambda slot 被当作 TopN 节点的元组集合所绑定的 slot 处理，并且一个 order-by 提示将一个没有列信息的 slot 作为重量级表达式下推到了 scan 中，从而对其进行了解引用。[#78050](https://github.com/StarRocks/starrocks/pull/78050) [#78527](https://github.com/StarRocks/starrocks/pull/78527)
- 由于内层 lambda 提升出的公共子表达式逃逸出了定义它的作用域，嵌套的 `array_map` 和 `array_sort` lambda 在使用 `Input dependency cols check failed` 或 `Expr evaluate meet error: slot_id N not found` 时无法正确规划或求值。[#78645](https://github.com/StarRocks/starrocks/pull/78645) [#78769](https://github.com/StarRocks/starrocks/pull/78769) [#78915](https://github.com/StarRocks/starrocks/pull/78915)
- `array_sort` 的比较器验证在多个 driver 间共享同一个错误对象 `Status`，这可能导致该对象被重复释放。[#78314](https://github.com/StarRocks/starrocks/pull/78314)
- `array_contains(NULL, [1, 2])` 和 `array_position(NULL, [1, 2])` 执行失败，报错 `class com.starrocks.type.NullType cannot be cast to class com.starrocks.type.ArrayType`。[#76970](https://github.com/StarRocks/starrocks/pull/76970)
- 分析 `__iceberg_transform_truncate` 或 `__iceberg_transform_bucket` 会就地修改共享的内置函数对象，用第一次分析的查询的 precision 和 scale 覆盖了其通配符 decimal 签名。[#76777](https://github.com/StarRocks/starrocks/pull/76777)
- `PARTITION BY RANGE(<partition-function>())` 使用空参数列表时会抛出一个原始的 `IndexOutOfBoundsException`，而不是解析错误。[#75080](https://github.com/StarRocks/starrocks/pull/75080)
- 无论表达式是否保序以及保序的方向如何，范围谓词都会被映射到分区表达式上，这导致 range 和 list 分区裁剪可能会错误地丢弃持有匹配行的分区。一个 `NULL` 分区键也会在该列类型的最小值处进行查找，而不是严格小于该值的位置。将 `DATE` 或 `DATETIME` 转换为 text 的保留条件现已重新被接受。[#78024](https://github.com/StarRocks/starrocks/pull/78024) [#79111](https://github.com/StarRocks/starrocks/pull/79111) [#79187](https://github.com/StarRocks/starrocks/pull/79187) [#79257](https://github.com/StarRocks/starrocks/pull/79257) [#79688](https://github.com/StarRocks/starrocks/pull/79688)
- Iceberg 谓词转换器会无条件地剥离一个 `CastOperator`，因此对 `DECIMAL(5,2)` 列执行 `WHERE CAST(d AS BIGINT) = 10` 可能会错误地裁剪掉持有 `10.50` 的文件。[#78351](https://github.com/StarRocks/starrocks/pull/78351)
- 由于增量边界推进到了一个被跳过的 `REPLACE` 快照上，Iceberg 基表上的增量物化视图会被常规的 `rewrite_data_files` compaction 永久性地置为不可用状态。[#76950](https://github.com/StarRocks/starrocks/pull/76950)
- 列的 min/max 统计信息缓存不会因分区或 schema DDL 而失效，因此可能会无限期地提供过期的缓存条目。[#77441](https://github.com/StarRocks/starrocks/pull/77441)
- 当 metastore 或 AWS Glue 拒绝读取列统计信息的权限时，Hive 系列 catalog 会直接失败；现在它们会降级为空统计信息。Glue 列统计信息请求现在也会携带 `catalogId`，因此跨账户部署会从配置的 catalog 中读取统计信息。[#78455](https://github.com/StarRocks/starrocks/pull/78455) [#78461](https://github.com/StarRocks/starrocks/pull/78461)
- Elasticsearch 外部表和 Elasticsearch catalog 会对运行 8 或 9 版本的集群拒绝连接，并抛出 `Unsupported/Unknown ES Cluster version`，并且并发查询可能会相互覆盖对方的请求 URL 参数，这表现为诸如 `Column 'id' cannot be resolved` 之类无关的分析错误。[#78267](https://github.com/StarRocks/starrocks/pull/78267) [#79391](https://github.com/StarRocks/starrocks/pull/79391)
- Paimon 表 `DECIMAL` 列上的谓词是使用 StarRocks 类型族的默认 precision 和 scale 转换的，而不是使用该列的 Paimon `DecimalType`，导致字面量无法表示该值。[#78760](https://github.com/StarRocks/starrocks/pull/78760)
- 当 endpoint 被配置为完整 URL 时，Azure Blob 凭据查找会失败，原因是 Hadoop 配置 key 中嵌入了 scheme。ADLS2 存储卷在序列化时也会丢失其 Azure Workload Identity token 文件属性，导致重新加载该存储卷时回退到 Managed Identity，并以 `Identity not found` 失败。[#78241](https://github.com/StarRocks/starrocks/pull/78241) [#79444](https://github.com/StarRocks/starrocks/pull/79444)
- 带有数组索引（例如 `'[0].value.url'`）的 JSON 路径，会对每一行数组长度小于该索引的数据抛出 C++ 异常，这会把一次大规模 scan 变成堆栈展开器内部的锁拥堵。该索引现已进行边界检查。[#78569](https://github.com/StarRocks/starrocks/pull/78569)
- 当两个参数落在同一个月或同一年、且第二个参数更早时，`timestampdiff(MONTH, ...)`、`timestampdiff(YEAR, ...)`、`months_diff` 和 `years_diff` 返回了 `-1` 而不是 `0`。[#79037](https://github.com/StarRocks/starrocks/pull/79037)
- 对 `CAST(<varbinary constant> AS VARCHAR)` 进行常量折叠时返回的是底层字节数组的 Java identity hash code（例如 `[B@233c25e5`），而不是实际的值。[#77718](https://github.com/StarRocks/starrocks/pull/77718)
- 当输入列缓存的 `has_null` 标志始终保守地为 true 时，`DICTIONARY_GET` 会将一个非 NULL 的 key 错误地拒绝为 NULL。[#76881](https://github.com/StarRocks/starrocks/pull/76881)
- 即使重写器从不对嵌套循环连接的 `ON` 列进行解码，一个低基数字典仍被保留在这些列上；字典表达式还被折叠穿过了那些携带自身编码的派生字典；此外，一个“滚动低基数”列反复使全局字典失效，并强制进行全表重新采集。[#78229](https://github.com/StarRocks/starrocks/pull/78229) [#78633](https://github.com/StarRocks/starrocks/pull/78633) [#78486](https://github.com/StarRocks/starrocks/pull/78486)
- 一个 `DECIMAL256` 字面量是根据其字符串表示形式进行哈希的，而 BE 对行进行分桶时使用的是原始的 32 字节值，因此在按 `DECIMAL256` 列分布的表上进行点查询会被发送到错误的 bucket。[#78495](https://github.com/StarRocks/starrocks/pull/78495)
- 一次 OLAP scan 是按列名而非按列 id 来命名其排序键列的，因此重命名后的列不再与 tablet schema 匹配。[#77967](https://github.com/StarRocks/starrocks/pull/77967)
- `PruneComplexTypeUtil` 只要遇到一个它无法描述的表达式，就会对该语句中的所有列禁用复杂类型子字段裁剪。现在该放弃逻辑已限定到触发它的那个表达式上，并且通过谓词表达式复用从 scan 中拉出的无用子字段 slot 会被丢弃。[#79181](https://github.com/StarRocks/starrocks/pull/79181) [#77456](https://github.com/StarRocks/starrocks/pull/77456)
- `AstToSQLBuilder` 用双引号打印了 `SELECT * EXCLUDE` 列表中的列，而语法在任何地方都不接受这种写法，导致重新生成的 SQL 无法再被解析。[#78052](https://github.com/StarRocks/starrocks/pull/78052)
- 当既未给出聚合函数也未给出 `KEY` 关键字时，对聚合表执行 `ALTER TABLE ... ADD COLUMN` 会悄悄地创建一个 key 列，从而改变了该表聚合的粒度。此类语句现在会被拒绝，因为其含义不明确。[#78359](https://github.com/StarRocks/starrocks/pull/78359)
- 类型没有 BE key coder 的列（例如 `JSON`）可以被用作排序键，并导致 BE 崩溃；此类定义现已在 `CREATE TABLE` 和 `ALTER TABLE` 中被拒绝，涵盖所有 all 分布类型。[#76795](https://github.com/StarRocks/starrocks/pull/76795)
- `USING GIN("IMP_LIB" = "builtin")` 被 `CREATE TABLE` 接受后，会破坏每一个带有 `Can not get inverted imp type` 的 `INSERT`，原因是该属性 key 以其原始大小写形式传到了 BE。[#77818](https://github.com/StarRocks/starrocks/pull/77818)
- 在没有其 `gram_num` 属性的情况下持久化的旧版 `NGRAMBF` 索引，会导致 BE 在构建 segment 时读取越过 UTF-8 偏移向量，从而导致 CN 崩溃。[#76989](https://github.com/StarRocks/starrocks/pull/76989)
- 当下推谓词读取一个稀疏范围时，空值标志是按线性方式而非按序数索引的，因此间隙之后的每一行都会取被跳过行的空值标志。字典页解码器在下推过滤器拒绝了整个字典时也未能正确前进，导致 BE 中止。[#78389](https://github.com/StarRocks/starrocks/pull/78389) [#79007](https://github.com/StarRocks/starrocks/pull/79007)
- 子表达式错误会导致 BE 中止，而不是被正确地传播出去。[#77442](https://github.com/StarRocks/starrocks/pull/77442)
- `ThreadPool` 默认会吞掉任务异常，并将该任务计为已完成。[#76863](https://github.com/StarRocks/starrocks/pull/76863)
- 启用 Jaeger tracing 时，通过 `OlapTableSink` 取消导入会导致 BE 或 CN 以 SIGSEGV 崩溃。[#76869](https://github.com/StarRocks/starrocks/pull/76869)
- `AsyncFlushOutputStream::close()` 在仍持有该流自身互斥锁的情况下满足了其 promise，导致 I/O 线程在等待者已销毁该流之后，唤醒了作用于已释放内存上的 futex。[#79182](https://github.com/StarRocks/starrocks/pull/79182)
- 在优雅关闭期间，当一个正在进行中的导入在 `shutdown_staros_worker()` 释放全局 worker 之后到达一个由 StarOS 支撑的文件系统时，CN 可能会以 SIGSEGV 崩溃。[#79058](https://github.com/StarRocks/starrocks/pull/79058)
- 并行 compaction 在 brpc bthread 上运行文件系统 I/O，导致 CN 以 `Resource deadlock avoided` 中止。[#76925](https://github.com/StarRocks/starrocks/pull/76925)
- 当一个向量索引 rowset 报告了负的 `data_disk_size` 时，Compaction 会以 SIGFPE 中止。[#78959](https://github.com/StarRocks/starrocks/pull/78959)
- 当一次 lake 事务日志读取遇到损坏时，一份已损坏的本地缓存副本会在每次重试时被重新读取；同时主键索引 sstable 块读取从未校验过写入者一直生成的 crc32c 块尾部。[#78733](https://github.com/StarRocks/starrocks/pull/78733) [#77479](https://github.com/StarRocks/starrocks/pull/77479)
- 一个退化为复制的部分更新重写操作丢弃了源的加密元数据，导致产生一个任何读取器都无法打开、且 Compaction 也无法重写的 segment。[#79053](https://github.com/StarRocks/starrocks/pull/79053)
- 存算分离模式下的 schema change 可能记录一个不引用任何数据的 rowset，此后该分区上的每个 `MERGE_TABLET` 都会因 `tablet merge source has an empty ordinary rowset` 而失败。[#79167](https://github.com/StarRocks/starrocks/pull/79167)
- Range-colocate 的拆分边界以序列化的 protobuf 字节形式进行比较，而 FE 和 BE 的编码方式并不一致。[#76968](https://github.com/StarRocks/starrocks/pull/76968)
- 对不稳定的 range-colocate 组中某个成员执行简单的单表查询会因 `range colocate group ... is in an unaligned state` 而失败。[#76971](https://github.com/StarRocks/starrocks/pull/76971)
- 为存算分离模式下 hash-colocate 表创建的新物理分区的 Shard 未携带 colocation 放置信息，事后必须进行迁移。[#79223](https://github.com/StarRocks/starrocks/pull/79223)
- FE 未告知 BE 已删除的 tablet 所属的表是按 Range 分布的，这在重新分片后导致文件泄漏。[#76861](https://github.com/StarRocks/starrocks/pull/76861)
- 重放 reshard 作业更新操作未刷新正在重分片的 tablet，因此在 FE Leader 切换后，某个分区的发布操作可能在一个从未写入的 tablet 元数据对象上永久返回 404。重放 reshard 作业也可能在表已被删除的情况下写入 edit log，并且 reshard 发布重试会无节制地空转而非按节奏进行。[#79426](https://github.com/StarRocks/starrocks/pull/79426) [#79485](https://github.com/StarRocks/starrocks/pull/79485) [#77691](https://github.com/StarRocks/starrocks/pull/77691)
- 存算分离模式下分区版本在变更操作被写入日志之前就已在内存中被修改，因此重放后的版本号可能回退。现在版本号由日志派生，只会单调递增。[#79296](https://github.com/StarRocks/starrocks/pull/79296)
- 当目标 tablet 报告没有副本时，复制作业出现除零错误。[#79329](https://github.com/StarRocks/starrocks/pull/79329)
- DataCache 的 key 哈希取自运行时分发表，因此对该分发表的任何更改都会使升级或回滚过程中所有缓存块失效。现在该哈希已固定为 MurmurHash3。[#79262](https://github.com/StarRocks/starrocks/pull/79262)
- FE 的扫描范围堆安全检查按每个物理分区运行一次，而不是按每个扫描节点运行一次，导致其复杂度相对于物理分区数量呈平方级增长。[#76978](https://github.com/StarRocks/starrocks/pull/76978)
- 在延迟加载的索引增加了 segment 内存占用之后，rowset 元数据缓存条目仍保留其初始占用量，导致缓存低估了实际占用的内存。[#77893](https://github.com/StarRocks/starrocks/pull/77893)
- `Tablet::get_average_row_size()` 在未持有 `_meta_lock` 的情况下遍历了 `_rs_version_map`，违反了该 map 声明时的约定。[#79250](https://github.com/StarRocks/starrocks/pull/79250)
- FE 的 thrift 接受循环在工作线程池饱和时会停滞数分钟，而工作线程会继续为那些排队很久、调用方早已消失的连接提供服务。[#78838](https://github.com/StarRocks/starrocks/pull/78838) [#79173](https://github.com/StarRocks/starrocks/pull/79173)
- 当一个非 Leader FE 的日志应用器卡住时，它会继续在本地提供陈旧的元数据而不是转发请求，因为就绪状态的判定只在一次重放周期返回后才会得出。[#79041](https://github.com/StarRocks/starrocks/pull/79041)
- `getLoads` RPC 在一条消息中返回了所有导入记录，因此在繁忙集群中会使 Leader 日志充满 `FE RPC failure ... MaxMessageSize reached`，并且 `loads_history` 归档会回滚并永久丢失数据行。该 RPC 现已按作业 ID 分页。[#78898](https://github.com/StarRocks/starrocks/pull/78898)
- 当某个任务运行在待处理队列、运行中 map、内存历史记录和归档历史记录之间迁移、同时 `task_runs` 正被读取时，可能会被重复上报。[#79438](https://github.com/StarRocks/starrocks/pull/79438)
- `SHOW ALTER TABLE ROLLUP` 和 `SHOW ALTER MATERIALIZED VIEW` 接受 `WHERE`、`ORDER BY` 和 `LIMIT`，但随后却忽略了它们；只有 `SHOW ALTER TABLE COLUMN` 才会应用这些参数。`FinishedTime` 也可在 `SHOW ALTER TABLE ROLLUP` 谓词中接受。[#79281](https://github.com/StarRocks/starrocks/pull/79281) [#79498](https://github.com/StarRocks/starrocks/pull/79498)
- 加载 SQL 黑名单时，每条规则都会重建并排序其不可变快照，这使复杂度相对于规则数量呈平方级增长，并且在每次 FE 启动和 checkpoint 时都会执行。[#78752](https://github.com/StarRocks/starrocks/pull/78752)
- 在消费者线程被中断后，`AsyncTaskQueue` 会在 `getOutputs()` 和 `tryGetOutputs()` 之间忙等循环，原因是中断信号被吞掉而未清除 `hasMoreOutput`。[#79297](https://github.com/StarRocks/starrocks/pull/79297)
- 对 `sys_log_level` 的运行时更改会写入配置，但从未真正生效，尽管该项被文档标记为可变的。[#79200](https://github.com/StarRocks/starrocks/pull/79200)
- `has_enable_heap_profile()` 将布尔类型的 jemalloc 节点 `prof.active` 读入了一个 `int` 中，因此它总是将堆内存分析报告为已禁用。[#78820](https://github.com/StarRocks/starrocks/pull/78820)
- 对脚本中外部对象执行 `System.print()` 会打印 `instance of <Class>` 而不是对象的文本内容，原因是 `toString` 的绑定被注册为方法而非 getter。[#78877](https://github.com/StarRocks/starrocks/pull/78877)
- 通过多表事务 Stream Load 接口向表达式分区表导入数据时，只要 BE 需要创建分区，就会在提交阶段失败，并报出 `automatic create partition failed. error: txn ... not exist`。[#78790](https://github.com/StarRocks/starrocks/pull/78790)
- 在合并提交日志（combined txn log）模式下，当导入被取消时，如果协调者自身的流结束信号提前到达，它可能会无限期等待 txn log 收集器。[#79389](https://github.com/StarRocks/starrocks/pull/79389)
- 一个已完成的 merge-commit 导入可能在 `information_schema.loads` 中报告的合并窗口进度小于完整进度。[#78723](https://github.com/StarRocks/starrocks/pull/78723)
- 扫描 `SampleTime` 计数器报告的是块数量而不是耗费的时间。[#78732](https://github.com/StarRocks/starrocks/pull/78732)
- Arrow Flight SQL 服务的 FE classpath 中缺少 LZ4 和 ZSTD 的 Arrow IPC 编解码器。[#76921](https://github.com/StarRocks/starrocks/pull/76921)
- 将 StarOS、starlet 和 starcache 升级至 v4.1.4。[#79429](https://github.com/StarRocks/starrocks/pull/79429)

## 4.1.5 \{#415}
>>>>>>> b4b4c89 ([Doc] Add release notes for StarRocks v4.1.8 (#82273))

发布日期：2026 年 9 月 8 日

### 改进

- 支持对外部表执行 `ANALYZE SAMPLE TABLE PREDICATE COLUMNS`。 [#77243](https://github.com/StarRocks/starrocks/pull/77243)
- 扩展基于采样的 Tablet 预拆分功能：支持非 Key 表达式、动态覆盖临时分区，以及 Iceberg 和覆盖式导入。 [#77843](https://github.com/StarRocks/starrocks/pull/77843) [#77842](https://github.com/StarRocks/starrocks/pull/77842) [#77983](https://github.com/StarRocks/starrocks/pull/77983)
- 流式读取 Lake 列更新所需的 Segment，以降低内存使用量。 [#77275](https://github.com/StarRocks/starrocks/pull/77275)
- Paimon Catalog 的后台元数据刷新现在仅刷新最近访问过且发生变更的表，而不再在每个周期枚举整个 Warehouse。 [#78057](https://github.com/StarRocks/starrocks/pull/78057)
- 在分区裁剪期间将 `limit` 传递给 Paimon `getRemoteFiles` 调用。 [#76943](https://github.com/StarRocks/starrocks/pull/76943)
- 扩展 GROUPING SETS 下推，支持 `AVG` 和 `COUNT`。 [#77832](https://github.com/StarRocks/starrocks/pull/77832)
- 支持将 `EXPLAIN COSTS` 输出包含在查询 Profile 中。 [#73005](https://github.com/StarRocks/starrocks/pull/73005)
- 支持在运行时配置 starlet 对象存储上传阈值。 [#78448](https://github.com/StarRocks/starrocks/pull/78448)
- 降低锁竞争：移除 Lake Segment 的全局 metacache 锁，并使 SQL 黑名单验证无锁化。 [#78260](https://github.com/StarRocks/starrocks/pull/78260) [#78476](https://github.com/StarRocks/starrocks/pull/78476)
- 在 Compaction 和 Replication 归档期间删除 `delete_predicate`，以回收 Tablet 元数据。 [#76870](https://github.com/StarRocks/starrocks/pull/76870)
- 使用 CRC32C 对 存算分离 delete 文件进行校验。 [#77798](https://github.com/StarRocks/starrocks/pull/77798)
- 限制已取消查询等待未上报实例的时间。 [#76909](https://github.com/StarRocks/starrocks/pull/76909)
- 对 jemalloc 进行了多项改进，以提升内存效率和可观测性：在 arm64 上同时构建 4K page 版本和 64K page 版本，暴露 dirty/muzzy page 以及 mapped/retained 指标，并支持在运行时应用可变的 `jemalloc_conf` 选项。 [#77886](https://github.com/StarRocks/starrocks/pull/77886) [#78159](https://github.com/StarRocks/starrocks/pull/78159) [#78225](https://github.com/StarRocks/starrocks/pull/78225) [#78320](https://github.com/StarRocks/starrocks/pull/78320) [#78333](https://github.com/StarRocks/starrocks/pull/78333)
- 增强可观测性：在 Leader FE 上新增 `max_journal_replay_lag` gauge，并增加 Lake publish-version tracing。 [#78382](https://github.com/StarRocks/starrocks/pull/78382) [#76658](https://github.com/StarRocks/starrocks/pull/76658) [#77048](https://github.com/StarRocks/starrocks/pull/77048)
- Reshard 作业完成后释放输出 shard 的 placement pin。 [#76945](https://github.com/StarRocks/starrocks/pull/76945)
- 将 StarOS 升级至 v4.1.2。 [#77657](https://github.com/StarRocks/starrocks/pull/77657)

### 安全

- [CVE-2025-55163] 通过 `grpc-bom` 统一所有 `io.grpc` artifact 的版本，使 `grpc-netty` 与 `grpc-core` 保持一致。 [#77171](https://github.com/StarRocks/starrocks/pull/77171)
- [CVE-2026-59902] 将 Netty 升级至 4.1.137.Final。 [#78287](https://github.com/StarRocks/starrocks/pull/78287)
- [CVE-2026-54399] 将 Apache HttpCore 升级至 5.4.3，以修复 HTTP/1.1 消息解析器中的不受控资源消耗（DoS）漏洞。 [#77753](https://github.com/StarRocks/starrocks/pull/77753)
- [CVE-2025-48924] 将 Broker 的 `commons-lang3` 升级至 3.20.0，并移除未使用的 `commons-lang` 2.6 和 `commons-configuration` 1.6。 [#77748](https://github.com/StarRocks/starrocks/pull/77748)
- [CVE] 在较新的版本中重新构建 pprof binary，并从 BE 容器镜像中移除未使用的 flamegraph 工具。 [#77846](https://github.com/StarRocks/starrocks/pull/77846)

### Bug 修复

以下问题已修复：

**崩溃与稳定性**

- 当 fatal-signal handler 在将进程标记为正在崩溃后发生卡顿时，强制 BE 退出。 [#76491](https://github.com/StarRocks/starrocks/pull/76491)
- 修复 `tokenize`、`ngram_search`、`generate_series`、`corr`/`covar_pop`/`covar_samp`、`strpos`（INT32_MIN）以及对 decimal 数组执行 `count()` 时因常量参数或无效参数导致 BE 崩溃的问题。 [#77102](https://github.com/StarRocks/starrocks/pull/77102) [#77103](https://github.com/StarRocks/starrocks/pull/77103) [#77216](https://github.com/StarRocks/starrocks/pull/77216) [#77260](https://github.com/StarRocks/starrocks/pull/77260) [#77092](https://github.com/StarRocks/starrocks/pull/77092)
- 修复一次 `ALTER` 同时添加普通列和生成列时 BE 崩溃，以及 read-schema 重排后 cache-select 崩溃的问题。 [#77436](https://github.com/StarRocks/starrocks/pull/77436) [#77245](https://github.com/StarRocks/starrocks/pull/77245)
- 修复查询缓存为分区 Hash Join 复用 lane 时 BE 崩溃，以及 lane 的 Join build side 为空时发生卡顿的问题。 [#77564](https://github.com/StarRocks/starrocks/pull/77564) [#77571](https://github.com/StarRocks/starrocks/pull/77571)
- 修复 `JsonColumn` append 时越界读取和静默丢行、`BinaryColumnBase::replicate` 中整数溢出，以及将同一个 array 列作为多个 lambda 参数传入时 `array_map` DCHECK 失败的问题。 [#77090](https://github.com/StarRocks/starrocks/pull/77090) [#77545](https://github.com/StarRocks/starrocks/pull/77545) [#77433](https://github.com/StarRocks/starrocks/pull/77433)
- 修复无效 UTF-8 输入下 `substr` 返回相邻行字节，以及 `array_sort` 的 lambda comparator 导致排序越界的问题。 [#78722](https://github.com/StarRocks/starrocks/pull/78722) [#77687](https://github.com/StarRocks/starrocks/pull/77687)
- 将 parquet decoder 的 scratch buffer 移出栈，并限制 `SimdJsonConverter` 错误路径中的 raw-JSON view。 [#77615](https://github.com/StarRocks/starrocks/pull/77615)
- 修复 tablet scheduler 在未上报存储介质的磁盘上发生 NPE，以及对 `INSERT ... VALUES (DEFAULT, ...)` 进行 deparse 时和 optimizer 没有 query ID 时 `PrepareCollectMetaTask` 发生 NPE 的问题。 [#76966](https://github.com/StarRocks/starrocks/pull/76966) [#76963](https://github.com/StarRocks/starrocks/pull/76963) [#77051](https://github.com/StarRocks/starrocks/pull/77051)

**查询引擎与优化器**

- 修复多个优化器路径中的错误或缺失结果：streaming distinct global limit、跨 sort group 的 ranking window pre-agg、merge 阶段重新追加常量聚合值、GROUP BY 规则未应用于每个 child，以及从 FOLLOWING offset 开始的 analytic frame。 [#77086](https://github.com/StarRocks/starrocks/pull/77086) [#77068](https://github.com/StarRocks/starrocks/pull/77068) [#77069](https://github.com/StarRocks/starrocks/pull/77069) [#77099](https://github.com/StarRocks/starrocks/pull/77099) [#77342](https://github.com/StarRocks/starrocks/pull/77342)
- 修复 NL Join 的 null-aware anti join 丢失 probe 行、NL Join probe 对空 chunk 执行 conjunct evaluation，以及 GROUPING SETS 聚合按 group-by key 进行 Join 时无法生成可执行计划的问题。 [#77970](https://github.com/StarRocks/starrocks/pull/77970) [#77344](https://github.com/StarRocks/starrocks/pull/77344) [#77296](https://github.com/StarRocks/starrocks/pull/77296)
- 修复 grouping-sets-to-union-all 改写顺序、作为 set operator 时重复复制 limit，以及将 Set operator 保持在 colocate exec group 之外以避免卡顿的问题。 [#77380](https://github.com/StarRocks/starrocks/pull/77380) [#77692](https://github.com/StarRocks/starrocks/pull/77692) [#77025](https://github.com/StarRocks/starrocks/pull/77025)
- 修复 window 的 `PARTITION BY`/`ORDER BY` 中的子查询规划、为多列 `count(distinct)` 构建的 `if()` 保持 decimal 类型，以及将 scan heavy-expr result column 规范化为 slot 的 nullability 的问题。 [#77087](https://github.com/StarRocks/starrocks/pull/77087) [#77346](https://github.com/StarRocks/starrocks/pull/77346) [#77242](https://github.com/StarRocks/starrocks/pull/77242)
- 修复无类型 array literal 获得错误类型从而破坏 view、达到 force-inline limit 时的 CTE anchoring 问题，以及 recursive CTE 重用推断出的 CTAS column 的问题。 [#76986](https://github.com/StarRocks/starrocks/pull/76986) [#77018](https://github.com/StarRocks/starrocks/pull/77018) [#76453](https://github.com/StarRocks/starrocks/pull/76453)
- 通过将 scan schema、join predicate common sub-expression 和 ANN search spec 添加到 query cache digest，并在 `SortNode` 构建 TOPN filter 时禁用缓存，修复多个 query cache 正确性问题。 [#77559](https://github.com/StarRocks/starrocks/pull/77559) [#77565](https://github.com/StarRocks/starrocks/pull/77565) [#77586](https://github.com/StarRocks/starrocks/pull/77586) [#77404](https://github.com/StarRocks/starrocks/pull/77404)
- 修复从始终未覆盖 visible version 的 row count 中折叠 `COUNT(*)`、join reorder 中无条件收集 UKFK constraint，以及 inverted-index filtering 后仍保留 OR-resident predicate 所读取列的问题。 [#77190](https://github.com/StarRocks/starrocks/pull/77190) [#77197](https://github.com/StarRocks/starrocks/pull/77197) [#77659](https://github.com/StarRocks/starrocks/pull/77659)
- 修复 datetime parsing 正确性：限制 Teradata 和 Joda parser 的解析范围不超过输入字符串，并确保对于超出范围的 Julian value，`DateValue::weekday()` 仍处于有效范围内。 [#77098](https://github.com/StarRocks/starrocks/pull/77098) [#77310](https://github.com/StarRocks/starrocks/pull/77310)
- 当 batch buffer 可能过大时按行序列化 `EXCEPT`/`INTERSECT` key，并在 set operation 对 key 进行反序列化时重新构建 JSON key column。 [#77821](https://github.com/StarRocks/starrocks/pull/77821) [#77215](https://github.com/StarRocks/starrocks/pull/77215)
- 修复其他优化器/表达式问题：在 unwrap `StatusOr` 前检查 status；强制满足 property 时只进行一次判断；在 `encode_fingerprint_sha256` 中从 index 0 读取常量参数；对于非正 column length 跳过 `CHAR` predicate 的零填充；不将非正 ranking predicate 下推到 TopN；使用推导出的 partition predicate 进行 pruning 时保留 NULL partition；在 `_switch_context` 中停止读取 output schema 末尾之外的内容；使用相同的 NULL 规则对 run 进行排序和合并；在 prepare 可能失败之前填充 `Analytor::_fns`；不再强制 external CTAS list partition column 为 `NOT NULL`。 [#77052](https://github.com/StarRocks/starrocks/pull/77052) [#77091](https://github.com/StarRocks/starrocks/pull/77091) [#77104](https://github.com/StarRocks/starrocks/pull/77104) [#77444](https://github.com/StarRocks/starrocks/pull/77444) [#77445](https://github.com/StarRocks/starrocks/pull/77445) [#77596](https://github.com/StarRocks/starrocks/pull/77596) [#77660](https://github.com/StarRocks/starrocks/pull/77660) [#77679](https://github.com/StarRocks/starrocks/pull/77679) [#77758](https://github.com/StarRocks/starrocks/pull/77758) [#78468](https://github.com/StarRocks/starrocks/pull/78468)
- 修复 statement/deparse 问题：验证带 column list 的 Iceberg/Hive `INSERT` 的 static partition clause；deparse partition reference 时保留 `TEMPORARY` 修饰符；支持 CTAS deparse 以修复 profile 中出现空 SQL statement 的问题；在 `proxyExecute` 中设置 `isSingleStmt`，使 forwarded statement 保留原始 SQL。 [#76659](https://github.com/StarRocks/starrocks/pull/76659) [#76987](https://github.com/StarRocks/starrocks/pull/76987) [#77978](https://github.com/StarRocks/starrocks/pull/77978) [#77982](https://github.com/StarRocks/starrocks/pull/77982)

**JSON 和数据类型**

- 修复大型 `BinaryColumn` SerDe 问题、包含点号的列名的统计信息收集问题，以及将 `multi_unnest`/`unnest` 的 array offset 读取为 `uint32` 而非 `int32` 的问题。 [#75504](https://github.com/StarRocks/starrocks/pull/75504) [#71301](https://github.com/StarRocks/starrocks/pull/71301) [#78479](https://github.com/StarRocks/starrocks/pull/78479) [#78480](https://github.com/StarRocks/starrocks/pull/78480)
- 降低 Flat JSON vertical compaction 的内存使用量，并修复 struct-subfield predicate 通过 rowid 进行评估时读取值本身的问题。 [#77682](https://github.com/StarRocks/starrocks/pull/77682) [#77891](https://github.com/StarRocks/starrocks/pull/77891)

**物化视图**

- 修复使用外部授权时的 IVM refresh、MV rewrite 的 staleness baseline（现在使用 last-freshness-confirmed time），以及对 time-travel query 跳过 MV rewrite 的问题。 [#77210](https://github.com/StarRocks/starrocks/pull/77210) [#76758](https://github.com/StarRocks/starrocks/pull/76758) [#77683](https://github.com/StarRocks/starrocks/pull/77683)
- 修复为物化视图上创建的 GIN index 分配 index ID，以及在基于 view 的 MV rewrite 中替换 view 后保留 view scan projection 的问题。 [#77340](https://github.com/StarRocks/starrocks/pull/77340) [#77613](https://github.com/StarRocks/starrocks/pull/77613)
- 修复其他物化视图问题：验证 `default_mv_refresh_mode`；修复 incremental MV 上的 `ORDER BY` 错误扩展 primary key 而不是执行排序的问题；以及使用 `query_timeout` 限制 `information_schema.materialized_views` 的内部查询。 [#77231](https://github.com/StarRocks/starrocks/pull/77231) [#77680](https://github.com/StarRocks/starrocks/pull/77680) [#77585](https://github.com/StarRocks/starrocks/pull/77585)

**Data Lake Connector（Iceberg、Paimon、Delta、Hive）**

- 修复 Iceberg 的正确性和稳定性问题：在 write path 上强制 NOT NULL 列；根据 plan-time snapshot 验证 rewrite commit；在 `TRUNCATE` 后使 metadata cache 失效；删除 metadata file 缺失的表；在 cast-on-string-partition metadata delete 前刷新 native table；修复带 `WHERE` 子句的 `rewrite_data_files` 中的静默数据丢失问题；以及修复 `remove_orphan_files` 的 retention window 过短问题。 [#77589](https://github.com/StarRocks/starrocks/pull/77589) [#77709](https://github.com/StarRocks/starrocks/pull/77709) [#77385](https://github.com/StarRocks/starrocks/pull/77385) [#77786](https://github.com/StarRocks/starrocks/pull/77786) [#77335](https://github.com/StarRocks/starrocks/pull/77335) [#77797](https://github.com/StarRocks/starrocks/pull/77797) [#77394](https://github.com/StarRocks/starrocks/pull/77394)
- 隔离 Delta Lake 各表的云凭证。 [#77424](https://github.com/StarRocks/starrocks/pull/77424)
- 支持 Hive 外部表的 OpenX JsonSerDe 列名映射、将一个 JSON 字段映射到多个 Hive 列，并修复在缺少 default catalog 和存在 CTE 引用时 connector view 中的 table qualification 问题。 [#65515](https://github.com/StarRocks/starrocks/pull/65515) [#77716](https://github.com/StarRocks/starrocks/pull/77716) [#77887](https://github.com/StarRocks/starrocks/pull/77887)
- 修复外部表的 Ranger view/MV authorization，并恢复 FE classpath 中的 Ranger Solr audit destination。 [#56316](https://github.com/StarRocks/starrocks/pull/56316) [#77294](https://github.com/StarRocks/starrocks/pull/77294)
- 修复返回损坏的 `DELTA_BINARY_PACKED` header 而不是抛出异常的问题，以及 Parquet reader 未将被 filter 排除的 FLBA dictionary row 标记为 NULL 的问题。 [#77446](https://github.com/StarRocks/starrocks/pull/77446) [#78685](https://github.com/StarRocks/starrocks/pull/78685)
- 修复 Iceberg REST catalog 上 `ALTER TABLE RENAME` 的 NPE、读取包含 decimal 或 null `timestamptz` partition value 的 Iceberg `$partitions` 时的 NPE、多个 Iceberg catalog 共享 Hadoop `FileSystem`，以及 `paimon.option.cache-enabled=false` 再次失效的问题。 [#76498](https://github.com/StarRocks/starrocks/pull/76498) [#77742](https://github.com/StarRocks/starrocks/pull/77742) [#77297](https://github.com/StarRocks/starrocks/pull/77297) [#78271](https://github.com/StarRocks/starrocks/pull/78271)

**存算分离：Compaction、Vacuum、Reshard 和 Replication**

- 修复 Compaction 问题：缓存 file bundling 跳过的 compaction txn log；按照 max tablet score 对候选项排序；修复 read-chunk-size 估算；修复 Limiter 缩小时丢失 token 的问题；以及跳过 delete-only write 的 publish-time segment rewrite。 [#77578](https://github.com/StarRocks/starrocks/pull/77578) [#77482](https://github.com/StarRocks/starrocks/pull/77482) [#78192](https://github.com/StarRocks/starrocks/pull/78192) [#78087](https://github.com/StarRocks/starrocks/pull/78087) [#78071](https://github.com/StarRocks/starrocks/pull/78071)
- 修复 Vacuum 问题：修复 grace-blocked vacuum 中的无效读取和多余 v1 delete；使 autovacuum retain floor 保持不低于 reshard takeover；并避免冗余的 lake metadata 404 probe。 [#78460](https://github.com/StarRocks/starrocks/pull/78460) [#78545](https://github.com/StarRocks/starrocks/pull/78545) [#78466](https://github.com/StarRocks/starrocks/pull/78466)
- 修复 Reshard/Split 问题：拒绝 source tablet 位于 superseded materialized index 中的 reshard；修复 tablet split fallback cross-publish；静默预期的 reshard history lookup warning；并防止 failover 后 lake alter job 卡住。 [#77512](https://github.com/StarRocks/starrocks/pull/77512) [#78270](https://github.com/StarRocks/starrocks/pull/78270) [#78059](https://github.com/StarRocks/starrocks/pull/78059) [#78517](https://github.com/StarRocks/starrocks/pull/78517)
- 修复 Replication 问题：保留 shared lake files，读取 remote bundled lake metadata，并在 Replication 期间重新加密 source lake files。 [#77361](https://github.com/StarRocks/starrocks/pull/77361) [#77362](https://github.com/StarRocks/starrocks/pull/77362) [#78080](https://github.com/StarRocks/starrocks/pull/78080)
- 修复损坏的本地缓存：delvec page 校验和失败时删除并重试；PK-index SST compaction 和 tablet-merge SSTable rebuild 发生损坏时重新构建。 [#78086](https://github.com/StarRocks/starrocks/pull/78086) [#77481](https://github.com/StarRocks/starrocks/pull/77481) [#78003](https://github.com/StarRocks/starrocks/pull/78003)
- 修复从 partition-shared object 读取 lake version-1 metadata、保护 snapshot 所拥有的 shared shard group，以及在 `TransactionGraph.remove` 中保持 dependency ordering 的问题。 [#78731](https://github.com/StarRocks/starrocks/pull/78731) [#78800](https://github.com/StarRocks/starrocks/pull/78800) [#78077](https://github.com/StarRocks/starrocks/pull/78077)
- 修复其他 存算分离 问题：在 final commit 阶段限制 transaction；不将 `DECOMMISSION` replica 选为 load primary；修复 online optimize 期间包含 shadow generated column 的 DML；使 `spill_enable_direct_io` 实际使用 `O_DIRECT`；修复并发 `BEGIN WITH LABEL` 之间的 label uniqueness race；从 effective query memory limit 推导 query tracker reserve limit；在 tablet merge 期间安全地重新映射 rowset ID 和引用；仅将 cross-published del file 应用于 child 所拥有的 key；使 tablet merge 遵循 range-distribution colocate；拒绝不完整的 combined txn log，并防止 `replication_num` 弱化 存算分离 write quorum；在 Reshard 期间将 explicit transaction 保持在同一 tablet layout 上；del file 校验和失败时删除其本地缓存并重试；以及在 closed lake delta writer 拒绝 task 时报告 abort reason。 [#77015](https://github.com/StarRocks/starrocks/pull/77015) [#77035](https://github.com/StarRocks/starrocks/pull/77035) [#77252](https://github.com/StarRocks/starrocks/pull/77252) [#77255](https://github.com/StarRocks/starrocks/pull/77255) [#77401](https://github.com/StarRocks/starrocks/pull/77401) [#77408](https://github.com/StarRocks/starrocks/pull/77408) [#77690](https://github.com/StarRocks/starrocks/pull/77690) [#77744](https://github.com/StarRocks/starrocks/pull/77744) [#78113](https://github.com/StarRocks/starrocks/pull/78113) [#78162](https://github.com/StarRocks/starrocks/pull/78162) [#78625](https://github.com/StarRocks/starrocks/pull/78625) [#78022](https://github.com/StarRocks/starrocks/pull/78022) [#78632](https://github.com/StarRocks/starrocks/pull/78632)
- 修复 存算分离 sort key 和 rowset identity 的边界问题：在添加尾部 key column 后将 split boundary 投影到当前 sort key；使 recursive CTE 执行期间的内部 `INSERT` 失败能够被报告；不再通过 apportioned row count 判断 cross-published rowset 是否存在；使窄固定长度 IVM row ID 能够访问 `encode_sort_key` 并展开其常量参数；通过 root column 解析 extended column 的 delta column group；保留 range-split cross-publish statistics；修复 FE max tablet compaction score collection；删除 auto-increment map 时跳过 dead node；测量 unsorted PK SST writer 的 map 而不是统计 allocation 数量；将 `compression_level` 传递至 synthetic sub-column meta；并在 builtin inverted-index backend 中跳过 standalone GIN index directory handling。 [#77504](https://github.com/StarRocks/starrocks/pull/77504) [#77510](https://github.com/StarRocks/starrocks/pull/77510) [#77511](https://github.com/StarRocks/starrocks/pull/77511) [#77751](https://github.com/StarRocks/starrocks/pull/77751) [#77847](https://github.com/StarRocks/starrocks/pull/77847) [#77759](https://github.com/StarRocks/starrocks/pull/77759) [#78269](https://github.com/StarRocks/starrocks/pull/78269) [#78496](https://github.com/StarRocks/starrocks/pull/78496) [#78561](https://github.com/StarRocks/starrocks/pull/78561) [#77498](https://github.com/StarRocks/starrocks/pull/77498) [#76949](https://github.com/StarRocks/starrocks/pull/76949) [#77101](https://github.com/StarRocks/starrocks/pull/77101)

**数据加载、事务和 DML**

- 修复 data-load 和 column-mode partial-update path 中对 chunk capacity 的限制和检查、按 owning child 决定 condition update 和 partial update，以及按 statement 记录 merged rowset 的 del `op_offset` 的问题。 [#77163](https://github.com/StarRocks/starrocks/pull/77163) [#78099](https://github.com/StarRocks/starrocks/pull/78099) [#78313](https://github.com/StarRocks/starrocks/pull/78313)
- 修复 dropped table 的 insert-overwrite failure 未记录到 journal，以及多个 partial-update path 中 current row lifecycle 未正确保持的问题。 [#77212](https://github.com/StarRocks/starrocks/pull/77212)
- 修复将 disconnect kill 交给 I/O thread 而不是 worker，以及 `GroupedExecutionSinkOperator::push_chunk` 未传递 `accept()` status 的问题。 [#77590](https://github.com/StarRocks/starrocks/pull/77590) [#77737](https://github.com/StarRocks/starrocks/pull/77737)
- 使仅记录 error state 的内部 DML/CONTROL statement 失败，从而使 failure 能够被报告，而不是被静默吞掉。 [#77388](https://github.com/StarRocks/starrocks/pull/77388)

**统计信息、指标和 FE/元数据**

- 修复统计信息正确性问题：拒绝 histogram 中的 null bucket/MCV，并使 `CancelableAnalyzeTask.cancel` 只选择一个 winner。 [#77024](https://github.com/StarRocks/starrocks/pull/77024) [#78593](https://github.com/StarRocks/starrocks/pull/78593)
- 注册 `pipe_driver_queue_len` metric；增加用于 NotFound lake tablet metadata read 的 BE metric；跳过非 table StarOS shard 的 table metrics 和 checkpoint thread-pool metrics；并修复 profile dictionary 中拼写错误的 `ZoneMapIndexFilter` key。 [#77890](https://github.com/StarRocks/starrocks/pull/77890) [#78451](https://github.com/StarRocks/starrocks/pull/78451) [#78116](https://github.com/StarRocks/starrocks/pull/78116) [#77367](https://github.com/StarRocks/starrocks/pull/77367) [#78728](https://github.com/StarRocks/starrocks/pull/78728)
- 修复 FE/metadata 问题：meta sync 时忽略 recycled missing shard group；恢复 bRPC 中每个 endpoint 的多个 connection group；在 FE 中解析 compute-node hostname 后再发送至 BE；在 `DROP FOLLOWER`/`OBSERVER` 后使被删除的 FE 自行退出；并暴露 colocate group scheduling sample knobs。 [#76604](https://github.com/StarRocks/starrocks/pull/76604) [#76765](https://github.com/StarRocks/starrocks/pull/76765) [#78390](https://github.com/StarRocks/starrocks/pull/78390) [#78215](https://github.com/StarRocks/starrocks/pull/78215) [#78450](https://github.com/StarRocks/starrocks/pull/78450)
- 修复扩展 meta-scan tablet 时跳过 non-extended access path、按 column ID 匹配 scan 的 access path、仅在 BE 对 OLAP scan 执行 pruning 时缩小其 complex type，以及不对 aggregate-state column 的 subfield 执行 pruning 的问题。 [#76593](https://github.com/StarRocks/starrocks/pull/76593) [#77987](https://github.com/StarRocks/starrocks/pull/77987) [#77894](https://github.com/StarRocks/starrocks/pull/77894) [#77901](https://github.com/StarRocks/starrocks/pull/77901)
- 不再对 aggregate-state column 使用 global dictionary，也不再对返回 `ARRAY`/`STRUCT` 的表达式进行 dictionarize；当下推 predicate 读取 sparse range 时，按 ordinal 索引 index page null flag。 [#77096](https://github.com/StarRocks/starrocks/pull/77096) [#78488](https://github.com/StarRocks/starrocks/pull/78488)
- 修复其他 metrics 和 FE 问题：将 bRPC method error bvar 作为 counter 报告；将实际 tablet count 报告为 range-distribution partition 的 Buckets；修复 `show proc '/global_current_queries'` 中的 NPE；在 intermediate query statistics 中报告 data cache read statistics；在 jemalloc `malloc_usable_size` 中使用最小化的 TSD fetch；停止 heap-profile toggle 向 `prof.thread_active_init` 写入；防止 FE connection ID wrap 后发生 collision；修复大量 partition 过期时 table stats health 固定在约 1.0 的问题；使用 `feature_log_dir` 作为 FE features-log path；复制 logical window operator 时保留 `inputIsBinary`；以及修复 cache invalidation 后的 Thrift client leak。 [#76956](https://github.com/StarRocks/starrocks/pull/76956) [#77465](https://github.com/StarRocks/starrocks/pull/77465) [#77407](https://github.com/StarRocks/starrocks/pull/77407) [#77696](https://github.com/StarRocks/starrocks/pull/77696) [#77625](https://github.com/StarRocks/starrocks/pull/77625) [#78384](https://github.com/StarRocks/starrocks/pull/78384) [#78217](https://github.com/StarRocks/starrocks/pull/78217) [#76927](https://github.com/StarRocks/starrocks/pull/76927) [#77221](https://github.com/StarRocks/starrocks/pull/77221) [#77058](https://github.com/StarRocks/starrocks/pull/77058) [#77072](https://github.com/StarRocks/starrocks/pull/77072)

**其他**

- 为 pooled Poco session 应用 request timeout；将 Linux AArch64 上的 Hyperscan 升级至 Vectorscan 5.4.12；提升 `JAVA_OPTS_FOR_JDK_*` warning 的级别；并停止在 java-extensions reader libs 中发布 test-scope jar。 [#78361](https://github.com/StarRocks/starrocks/pull/78361) [#78326](https://github.com/StarRocks/starrocks/pull/78326) [#78328](https://github.com/StarRocks/starrocks/pull/78328) [#77752](https://github.com/StarRocks/starrocks/pull/77752)
- 按 key size 序列化 `dictionary_get` key；捕获 thrift processor 中的异常；并使 CTAS 的 `files()` schema inference 不受 planner meta lock 影响。 [#78706](https://github.com/StarRocks/starrocks/pull/78706) [#78805](https://github.com/StarRocks/starrocks/pull/78805) [#78772](https://github.com/StarRocks/starrocks/pull/78772)
- 当文件没有末尾换行符时读取最后一行 conf；根据 cwd 解析 config YAML path；将 GCS upload-threshold BE config 命名为 `gcs` 而非 `gs`；将 StarOS 和 Starlet 升级至 4.1.3；并移除过时的 config forward-header split 和文档文件名中的多余 U+200E。 [#78065](https://github.com/StarRocks/starrocks/pull/78065) [#77132](https://github.com/StarRocks/starrocks/pull/77132) [#78452](https://github.com/StarRocks/starrocks/pull/78452) [#78038](https://github.com/StarRocks/starrocks/pull/78038) [#78780](https://github.com/StarRocks/starrocks/pull/78780) [#78612](https://github.com/StarRocks/starrocks/pull/78612)

## 4.1.4

发布日期：2026 年 8 月 5 日

### 行为变更

- 在存算分离模式下，`TABLESAMPLE` / `SAMPLE` 子句和 `ANALYZE SAMPLE TABLE` 现在会在 Lake 表上生效。此前采样选项从未传递到 BE，因此仍执行全表扫描。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- `ALTER TABLE` 对 `flat_json` 配置的修改现在通过版本化任务传播到 BE 节点，以确保可靠生效。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- 由 GIN 倒排索引响应的 `NOT MATCH` 谓词不再返回 NULL 行；NULL 行现在被正确排除在结果之外。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` 不再为 char 系列列计算直方图（优化器并不使用），而仅计算 MCV，降低了 analyze 开销。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` 现在按保序的分区范围对输出 manifest 进行聚簇，在分区较多的表上产生更紧凑的单 manifest 分区边界，改善 manifest 裁剪。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 多表 stream load 现在会根据每张表的 `file_bundling` 属性来决定是否使用合并事务日志，而不再仅遵循全局 `lake_use_combined_txn_log` 配置。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- 在 Primary Key 表上，GIN 倒排索引与列模式部分更新组合使用时不再返回损坏的结果；索引现在从 delta column group segment 提供服务。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- `array_difference` 对整型数组现在先以 64 位计算相邻差值再扩宽为 `BIGINT`，修复了 int32 溢出。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 除数为非常量的除法表达式不再被视为单调，修复了可能导致错误结果的 ZoneMap 裁剪。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 当一个 chunk 展平后的结果超过 4 GB 时，数组和 Map 构造器现在会报错，而不是静默损坏数据。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- Primary Key 插入现在会正确拒绝在 L1/L2 持久化索引中已存在的键；此前该存在性检查被跳过。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- range-colocate 表上的基于采样的 Tablet 预分裂现在会将新分片分散到多个计算节点，而不是打包到源 Tablet 所在节点，修复了 range 批量导入比 hash 慢约 3 倍的问题。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 增量或 AUTO 物化视图现在会在 `AUTO` 刷新模式下为 PCT 回退重建维护查询，修复了 IVM 不支持的形状导致的刷新失败。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 现在对非聚合的 INCREMENTAL 物化视图可用；存储填充的 `__ROW_ID__` 列被排除在 MV DDL 列列表之外，使失活的 IVM 可以重新激活。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg 分区缓存现在按内存（而非仅按条目数）进行限制并暴露其使用量，防止在分区很多的表上无限增长。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- 使用 OAuth2 客户端凭证的 Iceberg REST Catalog 现在会在后台 token 刷新任务失效后自愈，而不是使后续所有请求失败。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake 全量 Vacuum 现在会回收此前被跳过的孤立 `.lcrm`（Lake Compaction Rows Mapper）文件。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 存算分离 publish 现在会拒绝写入缺失 tablet 的 bundle tablet 元数据文件，防止分区在 publish 时被永久卡住。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 被丢弃的外部（Spark/Flink connector）扫描上下文在被回收时现在会取消其 pipeline fragment，且外部扫描计划会设置 `query_delivery_timeout` 以限制 QueryContext 的生命周期。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- 物化视图的 pinned-range map 现在按表 UUID 正确加键，修复了 OLAP 表的 MV bootstrap pinning 被禁用，以及不同数据库中同名表被错误处理的问题。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- 转发到 Leader FE 的语句，其审计日志现在记录完全限定的查询关系（与 Leader 一致），而不是 CTE 别名和非限定表名。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 将完整 `STRUCT` 列与 `ROLLUP`、`CUBE` 或 `GROUPING SETS` 一起聚合时，不再在计划阶段报 `usedStructFiledPos` 错误。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE 内存 `Estimator` 现在会计入容器开销，且 Parquet Scanner 现在使用有界的 chunk 大小（而非整个 batch 大小）为缺失列填充 NULL，提升了内存核算的准确性。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改进

- Routine Load 新增对 `INCLUDE METADATA` 子句的支持，可暴露 Kafka/Pulsar 消息元数据（如 partition、offset、timestamp）。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- 使 Routine Load 的元数据别名可选。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- 为 `information_schema.materialized_views` 新增 `LAST_FRESHNESS_CONFIRMED_AT` 列。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- 通过 `/metrics` 端点暴露 Data Cache 指标，并新增 FE Compaction 指标、`ALTER TABLE` 列操作指标及耗时，以及 exchange sink 的 `CompressedInputBytes` 指标。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- 向最终用户暴露 connector 元数据操作的认证与连通性错误详情。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 改进大列容量限制检查的错误信息。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- 新增 `hdfs_backend_selector_cache_replica_num` 变量，并使对象存储客户端缓存大小可在运行时修改。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- 将基于采样的 Tablet 预分裂 meta-tier 读取器扩展到更多排序键类型，包括 `CHAR`、复合排序键，以及 UTC 校准的 Parquet `TIMESTAMP` / ORC `TIMESTAMP_INSTANT`。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 改进外部表统计信息采集：现在会跟踪谓词列使用情况、在单次扫描中采集多列，并对 Iceberg 表应用有界成本的扫描预算。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 支持 right-outer、semi、anti 和 full-outer 的 range-colocate join。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- 优化 Query Queue V2 的成本估算器。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- 为 `FILES()` glob 将通配符字面量前缀下推到 S3 `ListObjectsV2` 调用，减少列举的对象数量。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- 在 `remove_orphan_files` 中扫描 Iceberg manifest 条目时，现在仅投影 `file_path` 列。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- 支持 Lake Primary Key tablet 的 base compaction，以及分离排序键的云原生 PK 表的 load spill 与 eager PK-index SST。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 支持在存算分离 range-distribution 表上仅通过元数据方式添加末尾排序键列。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- 在 fragment 取消时取消进行中的 exchange sink RPC，并在 reshard 清理期间取消已重分片分区上进行中的 compaction。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 多语句 stream load 通道现在会在等待前先触发，降低导入延迟。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- 为 `Analytor::process` 中的列升级新增内存限制检查。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 在异步 delta writer 被停止时保留真实的错误状态。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- 在 Lake PK-index SSTable 上记录 generation 版本，在 Lake Primary Key publish 时应用 `op_write.seg_delvecs`，并在 compaction publish 冲突解析时跳过打开输出 segment footer。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 为 Lake load 和 publish-version 路径新增堆栈跟踪和细粒度 trace counter 以辅助诊断。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### 安全

- [CVE-2026-44891] 将 Netty 升级至 4.1.136.Final，修复 STOMP subframe 解码器中的内存耗尽（DoS）漏洞。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] 将 Apache Thrift 升级至 0.24.0，修复 C++ 绑定中的堆缓冲区溢出以及一个无限循环漏洞。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 排除存在漏洞的 Jetty jar（经 Hadoop 传递引入的客户端 Digest 认证绕过），并将 pgjdbc 升级至 42.7.12。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 排除捆绑了存在漏洞的 jQuery 1.4.2（及其他 jQuery XSS CVE）的未使用的 `avro-ipc` jar。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- [CVE-2024-29857] 移除与其已修复版本一同随包发布的存在漏洞的过时传递依赖（例如 `bcprov-jdk15on` 1.70 和 EOL 的 `okhttp` 2.x），并新增依赖禁令以防回退。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### Bug 修复

修复了如下问题：

- join 谓词推导中空 range 导致的 Planner 崩溃。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- 聚合被错误地下推到带非空常量 `ELSE` 的 `CASE` 之上。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` 的 partition-by 被改写为已裁剪的字典 slot。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- join shuffle-join 输出属性分支中的运算符优先级错误，以及 `predicateCommonOperators` 未在 join 运算符构造过程中传递。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 视图列以及 `ROLLUP` 键在分析阶段的可空性错误。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- `regexp_extract_all` 在零长度捕获组上的无限循环。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` 中 `nullsFraction` 的错误 clamp。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- string 到 number 的 schema-change 转换中 `CAST` 语义错误。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同步 MV 改写在同列 `min`/`max` 情况下丢失一个 rollup 列。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- 在上拉扫描谓词时错误地在 `array_map` lambda 内部进行改写。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- bucket-aware 执行下 Iceberg bucket 表上的 `COUNT(DISTINCT)` 多计。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 对必填 Iceberg 列进行 `UNNEST` + `GROUP BY` 时的可空输出错误。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- 降低 `INSERT OVERWRITE` 代码路径上的锁争用，并恢复 `Operator` salt 以避免 CBO table prune 中的 Memo 自引用。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 当字符串日期分区列与时间值比较时，Iceberg 分区裁剪、manifest 行数估算、元数据删除、等值删除应用以及 Delta Lake 分区裁剪均存在错误。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg 读取未遵循目标快照的 schema 和分区规格；删除分区字段后查询 Iceberg V1 表失败；以及 `rollback_to_snapshot` 后 MV 改写返回过期结果。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg manifest data-file 缓存提供了不完整的文件集；增量 scan-range 迭代器并发关闭不安全；分析阶段的时间旅行快照绑定现在改为尽力而为。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta 从元数据推导的统计信息现在标记为 `StatsSource=TABLE_METADATA`。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake 和 Kudu 未分区物化视图的查询改写。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- 由于 `gcs-connector` 3.x 配置项重命名导致 GCS vended 凭证被忽略。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive `getTable()` 现在会在 `get_table_req` 回退前重连，并按异常类型（而非消息文本）判断表不存在。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet column-index 统计不支持 `BOOLEAN` 的 min/max。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 名称含 `.` 的键的 flat-JSON 子字段读取，以及重建中间 flat-JSON 对象而非返回 NULL；当子字段键在不区分大小写时冲突，现在会跳过 JSON 子字段下推。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 当仅一个分支具有 bitmap 索引时保留 OR 嵌套谓词。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- Primary Key 自增列部分更新 apply 时的 BE 崩溃。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- load spill 期间 `LoadChunkSpiller` 初始化竞态导致的 BE 崩溃。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 原生 Parquet reader 在不完整的嵌套 lake schema 上的崩溃，以及 Avro 复杂类型列中嵌套 `BOOLEAN` 的 ASAN 崩溃。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- schema change 期间将无效字符串转换为 `NOT NULL` 数值列时的 `bad_variant_access`。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` 错误路径中的堆缓冲区溢出，以及跨缓冲区扩容的多字符 CSV 分隔符上的 use-after-free。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- 扫描销毁时 `MorselQueueFactory` 上的 use-after-free、可溢出 join build `set_finishing` 在取消时的 use-after-free，以及 `PipelineDriver` 析构中的全局 Runtime Filter 定时器泄漏/未取消定时器。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 空值安全的 `SparseRangeIterator::has_more()`，修复物理分裂空 tablet 导致的 CN 崩溃，以及 bRPC stub 缓存清理定时器泄漏。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- 在 query-deploy worker 上未恢复 `ConnectContext`。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 为存算分离 Primary Key 表保留事务内的 upsert/delete 顺序，包括使 spill merge 感知操作类型。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- 在 `cal_new_base_version` 中读取持久化元数据以避免悬挂的 `prev_garbage_version`；在 `publish_version` 中保持 `base_version` 与 `base_metadata` 同步；并在常规路径上持久化合并后的并行 compaction 事务日志。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- range-colocate 相关修复：停止对齐作业风暴并对未对齐的 colocate join 采用 fail-close，修复 bucket-shuffle 下 null-safe join 丢失匹配，以及改进 plan feedback 中的 range-colocate join 检测。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- Tablet 分裂/重分片相关修复：分裂时提升表的乐观版本以便并发查询重新规划、在相同 tablet 重分片前 flush PK-index memtable、跨重分片按版本区间保留 Lake Vacuum 文件、在不可分裂的 range-distribution tablet 上停止自动分裂作业循环，以及修复分裂后 range-distributed 排序键上的 `IS NULL` 裁剪。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- 拒绝会重排 key 派生的 range-distribution 表的键列的全列 `ORDER BY`。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- 按 flush 顺序合并 load-spill 并行 merge 结果，并在 file-bundling carry-forward 中按 `metaId` 排除已触及的索引。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- 为 NULL 吸收的 dict-mapping group-by 键预留字典大小 + 1，并在 profile-report 重新调度前检查 driver 就绪状态。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- `PlannerMetaLocker` 在从未成功加锁时跳过解锁。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- 按 warehouse 过滤资源组。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- 在 `SHOW CREATE ROUTINE LOAD` 输出中转义 `jsonpaths` 值。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- 修复 Arrow Flight 预处理语句转发。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 修复字典刷新间隔溢出和意外的自动刷新。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- 修复 `ERROR_IF_OVERFLOW` 下 min/max 为空时的统计信息缓存加载失败。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- 使 Catalog 删除的存在性检查在写锁下原子化。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

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
- 在 存算一体 模式下，将多个数据库级写锁缩小为表级密集写锁，减少了 BE 报告回调和冷却操作期间的锁竞争。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
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
- 新增 `compaction_chunk_reset_memory_tracker_threshold_percent` BE 配置，通过释放保留的 chunk 容量，降低 存算一体 模式下主键压缩时的内存占用。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
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
- 在将主键表从 V1 编码（存算一体）集群复制到 V2 编码（存算分离）集群，或在两个 存算分离 集群之间复制时，PK `.del` 文件未被转码。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
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

- 在 存算一体 集群中为发布事务新增线程池，提升发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

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
- 此前，FE 配置项 `max_scalar_operator_flat_children`（默认值 `10000`）仅在构造 `CASE WHEN` 表达式时校验节点数。现在，优化器重写的所有标量表达式都会校验，且节点数在每次重写后重新计算，不再复用缓存值。因此，升级后此前可以正常执行的查询可能在分析阶段报错 `Expression too complex. Current nodes: N, Limit: M.`，通常出现在内联视图、CTE 或子查询导致表达式树膨胀的场景。如遇此报错，可简化表达式，或调大 `max_scalar_operator_flat_children`（支持动态修改）。[#66324](https://github.com/StarRocks/starrocks/pull/66324)
