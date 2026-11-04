---
displayed_sidebar: docs
description: "StarRocks 4.1 版本发布说明：多租户基于范围的 tablet 自动拆分、大容量 tablet 支持（100 GB 目标）、用于……的 Fast Schema Evolution V2"
---

# StarRocks 4.1 版本 {#starrocks-version-41}

:::danger
**容器镜像问题（v4.1.0）**

由于 v4.1.0 容器镜像中存在不稳定的加载顺序问题，BE 进程在容器环境中可能无法可靠启动。**容器环境用户NOT升级到 v4.1.0。**请等待修复该问题的 v4.1.1（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。
:::

:::warning
**降级说明**

- 将StarRocks升级到 v4.1 后，NOT降级到低于 v4.0.6 的任何 v4.0 版本。

  由于 v4.1 引入了内部数据布局变更（与tablet的拆分和分布机制相关），升级到 v4.1 的集群可能生成与早期版本不完全兼容的元数据和存储结构。因此，从 v4.1 降级仅支持降级到 v4.0.6 或更高版本，不支持降级到 v4.0.6 之前的版本。此限制是由于早期版本在解析tablet布局和分布元数据方面存在向后兼容性约束。
:::

## 4.1.7 {#417}

发布日期：2026 年 10 月 26 日

### 行为变更 {#behavior-changes}

- 本地（存算一体）主键表的插入操作默认不再检查不可变的 L1 和 L2 持久化索引文件中是否存在重复键。新的 BE 配置项 `skip_pindex_l1_l2_check_in_insert` 默认值为 `true`；L0（memtable）检查始终执行。将其设置为 `false` 可恢复之前的行为。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- BE JVM 指标默认启用（`enable_jvm_metrics` 现在默认值为 `true`）。这些指标仅在 JVM 存在时才会被采集，因此启用该功能不会在原本不需要 JVM 的 BE 上启动 JVM。[#81113](https://github.com/StarRocks/starrocks/pull/81113) [#81074](https://github.com/StarRocks/starrocks/pull/81074)
- `spill_max_dir_bytes_ratio` 的默认值从 `0.8` 降低为 `0.5`，因此中间结果落盘最多使用每个落盘目录容量的 50%。[#80189](https://github.com/StarRocks/starrocks/pull/80189)
- 存算分离副本同步的文件复制现在在专用的 CN 级别池中运行。`lake_replication_file_copy_threads` 的默认值由 `0`（表示 4 倍 CPU 核心数）改为 `16`，新增的 `lake_replication_max_parallel_files_per_tablet`（默认值 `4`）用于限制单个tablet并行复制的文件数量。[#78078](https://github.com/StarRocks/starrocks/pull/78078)
- 存算分离主键表 tablet 的删除触发 Base Compaction 现在通过新的 BE 配置项 `lake_pk_compaction_base_enable_delete_trigger` 控制是否启用，该配置项默认值为 `false`。[#79926](https://github.com/StarRocks/starrocks/pull/79926)
- 当 `pk_index_parallel_execution_threadpool_max_threads` 为 `0` 时，主键索引的并行执行池大小调整为all CPU 核心数，而不是其一半。[#80460](https://github.com/StarRocks/starrocks/pull/80460)
- `CAST(x AS CHAR(N))` 现在将结果截断为 `N` 个字符，与 MySQL 保持一致。FE 中的常量折叠和 BE 中的执行都会应用该截断。[#75909](https://github.com/StarRocks/starrocks/pull/75909)
- 在展示或记录配置和任务定义的所有位置，凭据值均会被屏蔽：`ADMIN SHOW FRONTEND CONFIG`、FE 的 `/variable` 页面、BE 的 `/varz`、`information_schema.be_configs`、归档到 `task_run_history` 的任务运行定义，以及写入 FE 和 BE HTTP 日志的请求 URI 和请求头。通过 `ADMIN SET FRONTEND CONFIG`、FE 的 `/api/_set_config` 端点以及 BE 的 `/api/update_config` 端点设置的值也同样会被屏蔽。[#80097](https://github.com/StarRocks/starrocks/pull/80097) [#80040](https://github.com/StarRocks/starrocks/pull/80040) [#80492](https://github.com/StarRocks/starrocks/pull/80492) [#80444](https://github.com/StarRocks/starrocks/pull/80444) [#80280](https://github.com/StarRocks/starrocks/pull/80280) [#80447](https://github.com/StarRocks/starrocks/pull/80447) [#80515](https://github.com/StarRocks/starrocks/pull/80515) [#80203](https://github.com/StarRocks/starrocks/pull/80203)
- FE HTTP 端点 `/service_id`、`/dump` 和 `/dump_starmgr` 现在需要管理员用户权限。[#80749](https://github.com/StarRocks/starrocks/pull/80749) [#80701](https://github.com/StarRocks/starrocks/pull/80701)

### 功能优化 {#improvements}

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

### 安全性 {#security}

- 将 pprof 升级到 release/20261012。[#80439](https://github.com/StarRocks/starrocks/pull/80439)
- 将 lz4-java 升级到 1.11.4。[#80319](https://github.com/StarRocks/starrocks/pull/80319)
- 将 BouncyCastle 升级到 1.86，Jackson 升级到 2.21.7，zstd-jni 升级到 1.5.7-15，Parquet 升级到 1.18.1。[#80248](https://github.com/StarRocks/starrocks/pull/80248)
- 将 Broker 使用的 Tomcat 升级到 9.0.121。[#79719](https://github.com/StarRocks/starrocks/pull/79719)
- 将 ZooKeeper 升级到 3.8.7 和 3.9.6。[#79707](https://github.com/StarRocks/starrocks/pull/79707)
- 对 `lookup_string` 的参数进行转义以防止SQL注入，其内部查询以调用者的身份运行。[#79991](https://github.com/StarRocks/starrocks/pull/79991) [#79959](https://github.com/StarRocks/starrocks/pull/79959)
- FE Web 控制台对管理员及主机提供的值和表格单元格进行 HTML 编码，包括在 `/system` 和 `/query` 页面上的内容，并对 `/proc_profile` 上的 `node` 参数进行校验和编码。[#80656](https://github.com/StarRocks/starrocks/pull/80656) [#80639](https://github.com/StarRocks/starrocks/pull/80639) [#80582](https://github.com/StarRocks/starrocks/pull/80582) [#80565](https://github.com/StarRocks/starrocks/pull/80565) [#80689](https://github.com/StarRocks/starrocks/pull/80689)

### 问题修复 {#bug-fixes}

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

## 4.1.6 {#416}

发布日期：2026 年 9 月 30 日

### 行为变更 {#behavior-changes-1}

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

### 改进项 {#improvements-1}

- 现在可以从用户的 `memberOf` 属性解析 LDAP 和 Active Directory 组，因此新的目录组不再需要在集群上配置匹配的组提供程序。[#79254](https://github.com/StarRocks/starrocks/pull/79254)
- 审计日志现在会记录查询的错误消息，而不仅仅是粗粒度的 `ErrorCode`，因此无需关联回 `fe.log` 即可区分语法错误、未知表和权限失败。[#78798](https://github.com/StarRocks/starrocks/pull/78798)
- 新增了服务器级别的 spill 指标，用于汇总 all 个可落盘算子的落盘活动，并按本地和远端存储后端细分。[#71827](https://github.com/StarRocks/starrocks/pull/71827)
- Hudi 元数据读取器现在会在一次扫描的各分区之间复用，而不是每个分区都重新构建，这缩短了启用 Hudi 元数据表时拥有大量分区的表的规划时间。[#79367](https://github.com/StarRocks/starrocks/pull/79367)
- tablet 拆分点现在根据 segment 现有的索引计算，而不是依赖专用的完整排序键索引页，因此从不拆分的表不再需要为写入和存储该页付出代价。[#79050](https://github.com/StarRocks/starrocks/pull/79050)
- `/memz` 现在接受 jemalloc 自身的统计选项，这使得每个 arena 的 dirty 和 muzzy 页计数以及 extent 大小类细分信息可以从正在运行的 BE 中获取。[#79090](https://github.com/StarRocks/starrocks/pull/79090)

### 安全性 {#security-1}

- [CVE-2026-55971] 将 Apache Thrift 升级到 0.24.0，修复了 C++ 绑定中 `THeaderTransport::untransform()` 的堆缓冲区溢出问题。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-42537，CVE-2026-59646] 将 Apache Ranger 升级到 2.9.0，修复了通过 JDBC URL 注入实现的 RCE 问题；将 Bouncy Castle 升级到 1.85，修复了 DTLS 重组缓冲区溢出问题。[#79331](https://github.com/StarRocks/starrocks/pull/79331)
- [CVE-2026-71290] 将 Apache HttpClient5 升级到 5.6.4。[#79538](https://github.com/StarRocks/starrocks/pull/79538)

### 问题修复 {#bug-fixes-1}

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

## 4.1.5 {#415}

发布日期：2026 年 9 月 8 日

### 改进项 {#improvements-2}

- 外部表支持 `ANALYZE SAMPLE TABLE PREDICATE COLUMNS`。[#77243](https://github.com/StarRocks/starrocks/pull/77243)
- 扩展了基于采样的 tablet 预拆分功能：支持非 key 表达式、动态覆盖临时分区，以及 Iceberg 和覆盖导入。[#77843](https://github.com/StarRocks/starrocks/pull/77843) [#77842](https://github.com/StarRocks/starrocks/pull/77842) [#77983](https://github.com/StarRocks/starrocks/pull/77983)
- 为 lake 列更新流式传输源 segment，以降低内存占用。[#77275](https://github.com/StarRocks/starrocks/pull/77275)
- Paimon catalog 的后台元数据刷新现在只刷新最近被访问和更改过的表，而不是每个周期都枚举整个数据仓库。[#78057](https://github.com/StarRocks/starrocks/pull/78057)
- 在分区裁剪期间向 Paimon 的 `getRemoteFiles` 调用传递 `limit`。[#76943](https://github.com/StarRocks/starrocks/pull/76943)
- 扩展了 GROUPING 的 SETS 下推功能，支持 `AVG` 和 `COUNT`。[#77832](https://github.com/StarRocks/starrocks/pull/77832)
- 允许在 Query Profile 中包含 `EXPLAIN COSTS` 输出。[#73005](https://github.com/StarRocks/starrocks/pull/73005)
- 使 starlet 对象存储上传阈值可在运行时配置。[#78448](https://github.com/StarRocks/starrocks/pull/78448)
- 降低锁竞争：移除了 lake segment 的全局 metacache 锁，并使 SQL 黑名单验证实现无锁化。[#78260](https://github.com/StarRocks/starrocks/pull/78260) [#78476](https://github.com/StarRocks/starrocks/pull/78476)
- 通过在 Compaction 和复制归档时删除 `delete_predicate` 来回收 tablet 元数据。[#76870](https://github.com/StarRocks/starrocks/pull/76870)
- 使用 CRC32C 对存算分离删除文件进行校验和计算。[#77798](https://github.com/StarRocks/starrocks/pull/77798)
- 限制已取消的查询等待未上报实例的最长时间。[#76909](https://github.com/StarRocks/starrocks/pull/76909)
- 多项 jemalloc 改进以提升内存效率和可观测性：在 arm64 上除 64K 版本外还构建了 4K 页面的变体，暴露 dirty/muzzy 页面和 mapped/retained 指标，并在运行时应用可变的 `jemalloc_conf` 选项。[#77886](https://github.com/StarRocks/starrocks/pull/77886) [#78159](https://github.com/StarRocks/starrocks/pull/78159) [#78225](https://github.com/StarRocks/starrocks/pull/78225) [#78320](https://github.com/StarRocks/starrocks/pull/78320) [#78333](https://github.com/StarRocks/starrocks/pull/78333)
- 增加了可观测性：在 Leader FE 上新增了一个 `max_journal_replay_lag` 仪表盘指标，以及额外的 lake publish-version 追踪。[#78382](https://github.com/StarRocks/starrocks/pull/78382) [#76658](https://github.com/StarRocks/starrocks/pull/76658) [#77048](https://github.com/StarRocks/starrocks/pull/77048)
- 在作业完成时释放 reshard 输出分片的放置锁定。[#76945](https://github.com/StarRocks/starrocks/pull/76945)
- 将 StarOS 升级到 v4.1.2。[#77657](https://github.com/StarRocks/starrocks/pull/77657)

### 安全性 {#security-2}

- [CVE-2025-55163] 通过 `grpc-bom` 对齐了 all 的 `io.grpc` 产物，使 `grpc-netty` 与 `grpc-core` 相匹配。[#77171](https://github.com/StarRocks/starrocks/pull/77171)
- [CVE-2026-59902] 将 Netty 升级到 4.1.137.Final。[#78287](https://github.com/StarRocks/starrocks/pull/78287)
- [CVE-2026-54399] 将 Apache HttpCore 升级到 5.4.3，以修复 HTTP/1.1 消息解析器中的资源消耗不可控（DoS）漏洞。[#77753](https://github.com/StarRocks/starrocks/pull/77753)
- [CVE-2025-48924] 将 Broker 的 `commons-lang3` 升级到 3.20.0，并移除了未使用的 `commons-lang` 2.6 和 `commons-configuration` 1.6。[#77748](https://github.com/StarRocks/starrocks/pull/77748)
- [CVE] 在更新的发行版上重新构建了 pprof 二进制文件，并从 BE 容器镜像中移除了未使用的 flamegraph 工具。[#77846](https://github.com/StarRocks/starrocks/pull/77846)

### Bug 修复 {#bug-fixes-2}

修复了以下问题：

**崩溃与稳定性**

- 当致命信号处理程序在标记进程为崩溃状态后挂起时，强制 BE 退出。[#76491](https://github.com/StarRocks/starrocks/pull/76491)
- 修复了 `tokenize`、`ngram_search`、`generate_series`、`corr`/`covar_pop`/`covar_samp`、`strpos`（INT32_MIN）中因常量/无效参数以及在 decimal 数组上执行 `count()` 导致的 BE 崩溃。[#77102](https://github.com/StarRocks/starrocks/pull/77102) [#77103](https://github.com/StarRocks/starrocks/pull/77103) [#77216](https://github.com/StarRocks/starrocks/pull/77216) [#77260](https://github.com/StarRocks/starrocks/pull/77260) [#77092](https://github.com/StarRocks/starrocks/pull/77092)
- 修复了当一个 `ALTER` 同时添加普通列和生成列时发生的 BE 崩溃，以及在读取 schema 重排序后出现的缓存查询（cache-select）崩溃问题。[#77436](https://github.com/StarRocks/starrocks/pull/77436) [#77245](https://github.com/StarRocks/starrocks/pull/77245)
- 修复了查询缓存在分区哈希 JOIN 中复用通道（lane）时发生的 BE 崩溃，以及当某个通道的 JOIN 构建侧为空时出现的挂起问题。[#77564](https://github.com/StarRocks/starrocks/pull/77564) [#77571](https://github.com/StarRocks/starrocks/pull/77571)
- 修复了 `JsonColumn` 追加时的越界读取和静默行丢失问题、`BinaryColumnBase::replicate` 中的整数溢出问题，以及当一个数组列作为多个 lambda 参数传递时 `array_map` 的 DCHECK 失败问题。[#77090](https://github.com/StarRocks/starrocks/pull/77090) [#77545](https://github.com/StarRocks/starrocks/pull/77545) [#77433](https://github.com/StarRocks/starrocks/pull/77433)
- 修复了 `substr` 在遇到无效 UTF-8 时返回相邻行字节的问题，以及 `array_sort` 的 lambda 比较器导致排序越界的问题。[#78722](https://github.com/StarRocks/starrocks/pull/78722) [#77687](https://github.com/StarRocks/starrocks/pull/77687)
- 将 parquet 解码器的临时缓冲区从栈上移出，并在 `SimdJsonConverter` 的错误路径中限制了 raw-JSON 视图的范围。[#77615](https://github.com/StarRocks/starrocks/pull/77615)
- 修复了当磁盘未上报存储介质时 tablet 调度器出现的 NPE 问题，以及在反解析 `INSERT ... VALUES (DEFAULT, ...)` 时和优化器没有查询 ID 时 `PrepareCollectMetaTask` 中出现的 NPE 问题。[#76966](https://github.com/StarRocks/starrocks/pull/76966) [#76963](https://github.com/StarRocks/starrocks/pull/76963) [#77051](https://github.com/StarRocks/starrocks/pull/77051)

**查询引擎与优化器**

- 修复了多个优化器路径中结果错误或缺失的问题：流式去重全局 limit、跨排序组的排名窗口预聚合、合并阶段中重新追加的常量聚合值、未对每个子节点应用的 GROUP BY 规则，以及从 FOLLOWING 偏移量开始的分析窗口帧。[#77086](https://github.com/StarRocks/starrocks/pull/77086) [#77068](https://github.com/StarRocks/starrocks/pull/77068) [#77069](https://github.com/StarRocks/starrocks/pull/77069) [#77099](https://github.com/StarRocks/starrocks/pull/77099) [#77342](https://github.com/StarRocks/starrocks/pull/77342)
- 修复了 NL Join 在 null-aware 反连接中丢失探测行的问题、NL Join 探测阶段对空 chunk 的连接条件求值问题，以及 `GROUPING SETS` 聚合在其分组键上连接时因没有可执行计划而失败的问题。[#77970](https://github.com/StarRocks/starrocks/pull/77970) [#77344](https://github.com/StarRocks/starrocks/pull/77344) [#77296](https://github.com/StarRocks/starrocks/pull/77296)
- 修复了 grouping-sets-to-union-all 重写顺序的问题、将 limit 重复为集合操作符的问题，以及将 Set 操作符排除在 colocate 执行组之外以避免挂起的问题。[#77380](https://github.com/StarRocks/starrocks/pull/77380) [#77692](https://github.com/StarRocks/starrocks/pull/77692) [#77025](https://github.com/StarRocks/starrocks/pull/77025)
- 修复了窗口的 `PARTITION BY`/`ORDER BY` 中的子查询规划问题、为多列 `count(distinct)` 构建的 `if()` 保留 decimal 类型的问题，以及将扫描重表达式结果列的可空性规范化为 slot 可空性的问题。[#77087](https://github.com/StarRocks/starrocks/pull/77087) [#77346](https://github.com/StarRocks/starrocks/pull/77346) [#77242](https://github.com/StarRocks/starrocks/pull/77242)
- 修复了未类型化数组字面量获得错误类型（导致视图损坏）的问题、触发强制内联限制时的 CTE 锚定问题，以及递归 CTE 复用推断出的 CTAS 列的问题。[#76986](https://github.com/StarRocks/starrocks/pull/76986) [#77018](https://github.com/StarRocks/starrocks/pull/77018) [#76453](https://github.com/StarRocks/starrocks/pull/76453)
- 通过将扫描 schema、JOIN 条件公共子表达式以及 ANN 搜索规范加入查询缓存摘要，并在 `SortNode` 构建 TOPN 过滤器时禁用缓存，修复了多个查询缓存正确性问题。[#77559](https://github.com/StarRocks/starrocks/pull/77559) [#77565](https://github.com/StarRocks/starrocks/pull/77565) [#77586](https://github.com/StarRocks/starrocks/pull/77586) [#77404](https://github.com/StarRocks/starrocks/pull/77404)
- 修复了从从未覆盖可见版本的行数折叠 `COUNT(*)` 的问题、在 JOIN 重排序中无条件收集 UKFK 约束的问题，以及在倒排索引过滤后保留 OR 驻留谓词所需列的问题。[#77190](https://github.com/StarRocks/starrocks/pull/77190) [#77197](https://github.com/StarRocks/starrocks/pull/77197) [#77659](https://github.com/StarRocks/starrocks/pull/77659)
- 修复了日期时间解析正确性问题：将 Teradata 和 Joda 解析器的解析范围限定在输入字符串内，并在儒略日（Julian）值超出范围时保持 `DateValue::weekday()` 在有效范围内。[#77098](https://github.com/StarRocks/starrocks/pull/77098) [#77310](https://github.com/StarRocks/starrocks/pull/77310)
- 当批缓冲区会过大时，按行序列化 `EXCEPT`/`INTERSECT` 键，并在集合操作反序列化其键时重建 JSON 键列。[#77821](https://github.com/StarRocks/starrocks/pull/77821) [#77215](https://github.com/StarRocks/starrocks/pull/77215)
- 其他优化器/表达式修复：在解包 `StatusOr` 前检查状态、在强制执行属性满足时只判定一次、在 `encode_fingerprint_sha256` 中读取索引 0 处的常量参数、对于非正列长度跳过 `CHAR` 谓词的零填充、不将非正排名谓词下推到 TopN 中、在使用推导出的分区谓词进行裁剪时保留 NULL 分区、停止在 `_switch_context` 中读取超出输出 schema 末尾的内容、按相同的 NULL 约定对单个 run 排序并合并多个 run、在 prepare 可能失败之前填充 `Analytor::_fns`，以及停止强制将外部 CTAS LIST 分区列转换为 `NOT NULL`。[#77052](https://github.com/StarRocks/starrocks/pull/77052) [#77091](https://github.com/StarRocks/starrocks/pull/77091) [#77104](https://github.com/StarRocks/starrocks/pull/77104) [#77444](https://github.com/StarRocks/starrocks/pull/77444) [#77445](https://github.com/StarRocks/starrocks/pull/77445) [#77596](https://github.com/StarRocks/starrocks/pull/77596) [#77660](https://github.com/StarRocks/starrocks/pull/77660) [#77679](https://github.com/StarRocks/starrocks/pull/77679) [#77758](https://github.com/StarRocks/starrocks/pull/77758) [#78468](https://github.com/StarRocks/starrocks/pull/78468)
- 修复了语句/反解析相关问题：对带有列列表的 Iceberg/Hive `INSERT` 校验静态分区子句、在反解析分区引用时保留 `TEMPORARY` 限定符、支持 CTAS 反解析以修复 profile 中出现的空 SQL 语句问题，以及在 `proxyExecute` 中设置 `isSingleStmt`，使转发的语句保留原始的 SQL。[#76659](https://github.com/StarRocks/starrocks/pull/76659) [#76987](https://github.com/StarRocks/starrocks/pull/76987) [#77978](https://github.com/StarRocks/starrocks/pull/77978) [#77982](https://github.com/StarRocks/starrocks/pull/77982)

**JSON 与数据类型**

- 修复了一个较大的 `BinaryColumn` 序列化/反序列化（SerDe）问题、针对列名中带点号的列进行统计信息采集的问题，以及将 `multi_unnest`/`unnest` 数组偏移量读取为 `uint32` 而非 `int32` 的问题。[#75504](https://github.com/StarRocks/starrocks/pull/75504) [#71301](https://github.com/StarRocks/starrocks/pull/71301) [#78479](https://github.com/StarRocks/starrocks/pull/78479) [#78480](https://github.com/StarRocks/starrocks/pull/78480)
- 修复了降低 Flat JSON 纵向 Compaction 内存占用的问题，以及按 rowid 对结构体子字段谓词求值时读取值本身的问题。[#77682](https://github.com/StarRocks/starrocks/pull/77682) [#77891](https://github.com/StarRocks/starrocks/pull/77891)

**物化视图**

- 修复了带外部鉴权的 IVM 刷新问题、MV 改写陈旧度基线（现使用最近一次确认新鲜度的时间）以及跳过时间旅行查询的 MV 改写问题。[#77210](https://github.com/StarRocks/starrocks/pull/77210) [#76758](https://github.com/StarRocks/starrocks/pull/76758) [#77683](https://github.com/StarRocks/starrocks/pull/77683)
- 修复了为物化视图上创建的 GIN 索引分配索引 ID 的问题，以及在基于视图的 MV 改写中将视图扫描投影替换回去时保留该投影的问题。[#77340](https://github.com/StarRocks/starrocks/pull/77340) [#77613](https://github.com/StarRocks/starrocks/pull/77613)
- 其他物化视图修复：校验 `default_mv_refresh_mode`；修复增量 MV 在扩展其主键时使用了扩宽而非排序的 `ORDER BY` 问题；以及将 `information_schema.materialized_views` 内部查询限定在 `query_timeout` 范围内。[#77231](https://github.com/StarRocks/starrocks/pull/77231) [#77680](https://github.com/StarRocks/starrocks/pull/77680) [#77585](https://github.com/StarRocks/starrocks/pull/77585)

**数据湖连接器（Iceberg、Paimon、Delta、Hive）**

- 修复了 Iceberg 正确性与稳定性问题：在写入路径上强制 NOT NULL 列；根据计划时刻的快照校验重写提交；在 `TRUNCATE` 之后使元数据缓存失效；删除元数据文件缺失的表；在基于字符串转换的分区元数据删除之前刷新内表；修复了带有 `WHERE` 子句的 `rewrite_data_files` 出现静默数据丢失的问题；以及 `remove_orphan_files` 中保留窗口过小的问题。[#77589](https://github.com/StarRocks/starrocks/pull/77589) [#77709](https://github.com/StarRocks/starrocks/pull/77709) [#77385](https://github.com/StarRocks/starrocks/pull/77385) [#77786](https://github.com/StarRocks/starrocks/pull/77786) [#77335](https://github.com/StarRocks/starrocks/pull/77335) [#77797](https://github.com/StarRocks/starrocks/pull/77797) [#77394](https://github.com/StarRocks/starrocks/pull/77394)
- 隔离了 Delta Lake 每张表的 cloud 凭证。[#77424](https://github.com/StarRocks/starrocks/pull/77424)
- 支持了 Hive 外部表的 OpenX JsonSerDe 列名映射、一个 JSON 字段映射到多个 Hive 列，并修复了连接器视图中因缺少默认 catalog 和 CTE 引用而导致的表限定问题。[#65515](https://github.com/StarRocks/starrocks/pull/65515) [#77716](https://github.com/StarRocks/starrocks/pull/77716) [#77887](https://github.com/StarRocks/starrocks/pull/77887)
- 修复了外部表的 Ranger 视图/MV 鉴权问题，并恢复了 FE classpath 上的 Ranger Solr 审计目标。[#56316](https://github.com/StarRocks/starrocks/pull/56316) [#77294](https://github.com/StarRocks/starrocks/pull/77294)
- 修复了返回损坏的 `DELTA_BINARY_PACKED` 头部而非抛出异常的问题，以及在 Parquet 读取器中将被过滤排除的 FLBA 字典行标记为 NULL 的问题。[#77446](https://github.com/StarRocks/starrocks/pull/77446) [#78685](https://github.com/StarRocks/starrocks/pull/78685)
- 修复了 Iceberg REST catalogs 上 `ALTER TABLE RENAME` 出现 NPE 的问题、读取带有小数或空 `timestamptz` 分区值的 Iceberg `$partitions` 时出现 NPE 的问题、跨 Iceberg catalogs 共享一个 Hadoop `FileSystem` 的问题，以及重新遵循 `paimon.option.cache-enabled=false` 的问题。[#76498](https://github.com/StarRocks/starrocks/pull/76498) [#77742](https://github.com/StarRocks/starrocks/pull/77742) [#77297](https://github.com/StarRocks/starrocks/pull/77297) [#78271](https://github.com/StarRocks/starrocks/pull/78271)

**存算分离：Compaction、vacuum、重分片与复制**

- Compaction 相关修复：缓存被文件打包跳过的 Compaction 事务日志；按最大 tablet 分数对候选项排序；修复了读取块大小估算问题；修复了 Limiter 在收缩时丢失令牌的问题；以及在仅删除写入时跳过发布时刻的 segment 重写。[#77578](https://github.com/StarRocks/starrocks/pull/77578) [#77482](https://github.com/StarRocks/starrocks/pull/77482) [#78192](https://github.com/StarRocks/starrocks/pull/78192) [#78087](https://github.com/StarRocks/starrocks/pull/78087) [#78071](https://github.com/StarRocks/starrocks/pull/78071)
- Vacuum 相关修复：修复了在宽限阻塞的 vacuum 中出现的无谓读取和残留 v1 删除问题；在重分片接管时保持 autovacuum 保留下限不低于该接管值；以及避免了冗余的 lake 元数据 404 探测。[#78460](https://github.com/StarRocks/starrocks/pull/78460) [#78545](https://github.com/StarRocks/starrocks/pull/78545) [#78466](https://github.com/StarRocks/starrocks/pull/78466)
- 重分片/拆分相关修复：拒绝源 tablet 位于已被替代的物化索引中的重分片；修复了 tablet 拆分回退跨发布的问题；屏蔽了预期中的重分片历史查找警告；以及防止 lake alter 任务在故障转移后卡住。[#77512](https://github.com/StarRocks/starrocks/pull/77512) [#78270](https://github.com/StarRocks/starrocks/pull/78270) [#78059](https://github.com/StarRocks/starrocks/pull/78059) [#78517](https://github.com/StarRocks/starrocks/pull/78517)
- 复制相关修复：保留共享的 lake 文件；读取远端打包的 lake 元数据；以及在复制过程中重新加密源 lake 文件。[#77361](https://github.com/StarRocks/starrocks/pull/77361) [#77362](https://github.com/StarRocks/starrocks/pull/77362) [#78080](https://github.com/StarRocks/starrocks/pull/78080)
- 恢复损坏的本地缓存：对 delvec 页面的校验和失败进行丢弃并重试；对 PK 索引 SST Compaction 和 tablet 合并 SSTable 的重建出现损坏时进行重建。[#78086](https://github.com/StarRocks/starrocks/pull/78086) [#77481](https://github.com/StarRocks/starrocks/pull/77481) [#78003](https://github.com/StarRocks/starrocks/pull/78003)
- 修复了从分区共享对象读取 lake 版本 1 元数据的问题、保护被快照占用的共享分片组的问题，以及在 `TransactionGraph.remove` 中保留依赖顺序的问题。[#78731](https://github.com/StarRocks/starrocks/pull/78731) [#78800](https://github.com/StarRocks/starrocks/pull/78800) [#78077](https://github.com/StarRocks/starrocks/pull/78077)
- 其他存算分离相关修复：在最终提交阶段对事务进行限流；不选择 `DECOMMISSION` 副本作为导入主副本；修复了在线优化期间带有阴影生成列的 DML 问题；使 `spill_enable_direct_io` 真正生效 `O_DIRECT`；修复了并发 `BEGIN WITH LABEL` 之间标签唯一性的竞态问题；根据生效的查询内存限制推导查询跟踪器预留限额；在 tablet 合并期间安全地重新映射 rowset ID 与引用；将跨发布的 del 文件仅应用于子节点所拥有的键；使 tablet 合并遵循范围分布的 colocate；拒绝不完整的合并事务日志，并阻止 `replication_num` 削弱存算分离写入法定人数；在重分片期间保持显式事务使用同一个 tablet 布局；对校验和失败的 del 文件丢弃其本地缓存并重试；以及在已关闭的 lake delta writer 拒绝任务时报告中止原因。[#77015](https://github.com/StarRocks/starrocks/pull/77015) [#77035](https://github.com/StarRocks/starrocks/pull/77035) [#77252](https://github.com/StarRocks/starrocks/pull/77252) [#77255](https://github.com/StarRocks/starrocks/pull/77255) [#77401](https://github.com/StarRocks/starrocks/pull/77401) [#77408](https://github.com/StarRocks/starrocks/pull/77408) [#77690](https://github.com/StarRocks/starrocks/pull/77690) [#77744](https://github.com/StarRocks/starrocks/pull/77744) [#78113](https://github.com/StarRocks/starrocks/pull/78113) [#78162](https://github.com/StarRocks/starrocks/pull/78162) [#78625](https://github.com/StarRocks/starrocks/pull/78625) [#78022](https://github.com/StarRocks/starrocks/pull/78022) [#78632](https://github.com/StarRocks/starrocks/pull/78632)
- 修复了存算分离排序键和 rowset 身份标识的边界情形：在追加键列后将拆分边界投影到当前排序键上；在递归 CTE 执行中暴露内部 `INSERT` 失败；不再让分摊的行数来决定跨发布 rowset 是否存在；使 `encode_sort_key` 对窄定长 IVM 行 id 可达并扩展其常量参数；通过根列解析扩展列的 delta 列组；保留范围拆分跨发布统计信息；修复了 FE 最大 tablet Compaction 分数统计问题；在删除自增映射时跳过已失效节点；通过度量未排序 PK SST writer 的映射而非统计分配次数；将 `compression_level` 传播到合成子列元数据；以及为内置倒排索引后端跳过独立的 GIN 索引目录处理。[#77504](https://github.com/StarRocks/starrocks/pull/77504) [#77510](https://github.com/StarRocks/starrocks/pull/77510) [#77511](https://github.com/StarRocks/starrocks/pull/77511) [#77751](https://github.com/StarRocks/starrocks/pull/77751) [#77847](https://github.com/StarRocks/starrocks/pull/77847) [#77759](https://github.com/StarRocks/starrocks/pull/77759) [#78269](https://github.com/StarRocks/starrocks/pull/78269) [#78496](https://github.com/StarRocks/starrocks/pull/78496) [#78561](https://github.com/StarRocks/starrocks/pull/78561) [#77498](https://github.com/StarRocks/starrocks/pull/77498) [#76949](https://github.com/StarRocks/starrocks/pull/76949) [#77101](https://github.com/StarRocks/starrocks/pull/77101)

**导入、事务与 DML**

- 修复了数据导入和列模式部分列更新路径上的 chunk 容量限定与校验问题、按所属子节点决定条件更新和部分列更新的问题，以及按语句记录合并 rowset 的 del `op_offset` 问题。[#77163](https://github.com/StarRocks/starrocks/pull/77163) [#78099](https://github.com/StarRocks/starrocks/pull/78099) [#78313](https://github.com/StarRocks/starrocks/pull/78313)
- 修复了对已删除表未记录插入覆盖失败的问题，以及在多个部分列更新路径上保持当前行生命周期正确的问题。[#77212](https://github.com/StarRocks/starrocks/pull/77212)
- 修复了将断开连接的终止操作交给工作线程而非 I/O 线程处理的问题，以及在 `GroupedExecutionSinkOperator::push_chunk` 中传播 `accept()` 状态的问题。[#77590](https://github.com/StarRocks/starrocks/pull/77590) [#77737](https://github.com/StarRocks/starrocks/pull/77737)
- 使只记录错误状态的内部 DML/CONTROL 语句真正失败，从而暴露失败而不是被静默吞掉。[#77388](https://github.com/StarRocks/starrocks/pull/77388)

**统计信息、指标与 FE/元数据**

- 修复了统计信息正确性问题：拒绝直方图中的空桶/MCV，以及使 `CancelableAnalyzeTask.cancel` 选出唯一的获胜者。[#77024](https://github.com/StarRocks/starrocks/pull/77024) [#78593](https://github.com/StarRocks/starrocks/pull/78593)
- 注册了 `pipe_driver_queue_len` 指标，为 NotFound 的 lake tablet 元数据读取添加了 BE 指标，跳过了非表 StarOS 分片的表指标以及检查点线程池指标，并修复了 profile 字典中 `ZoneMapIndexFilter` 键名拼写错误的问题。[#77890](https://github.com/StarRocks/starrocks/pull/77890) [#78451](https://github.com/StarRocks/starrocks/pull/78451) [#78116](https://github.com/StarRocks/starrocks/pull/78116) [#77367](https://github.com/StarRocks/starrocks/pull/77367) [#78728](https://github.com/StarRocks/starrocks/pull/78728)
- FE/元数据相关修复：在元数据同步中忽略已回收的缺失分片组；在 bRPC 中为每个端点恢复多个连接组；在发往 BE 之前先在 FE 中解析计算节点主机名；使被删除的 FE 在 `DROP FOLLOWER`/`OBSERVER` 上自行退出；以及暴露 colocate 组调度采样开关。[#76604](https://github.com/StarRocks/starrocks/pull/76604) [#76765](https://github.com/StarRocks/starrocks/pull/76765) [#78390](https://github.com/StarRocks/starrocks/pull/78390) [#78215](https://github.com/StarRocks/starrocks/pull/78215) [#78450](https://github.com/StarRocks/starrocks/pull/78450)
- 修复了在扩展 meta-scan tablet 时跳过非扩展访问路径的问题、按列 id 匹配扫描的访问路径的问题、仅在 BE 对其裁剪时才收窄 OLAP 扫描复杂类型的问题，以及不裁剪聚合状态列子字段的问题。[#76593](https://github.com/StarRocks/starrocks/pull/76593) [#77987](https://github.com/StarRocks/starrocks/pull/77987) [#77894](https://github.com/StarRocks/starrocks/pull/77894) [#77901](https://github.com/StarRocks/starrocks/pull/77901)
- 对聚合状态列不使用全局字典，也不对返回 `ARRAY`/`STRUCT` 的表达式进行字典化处理，以及当下推谓词读取稀疏范围时按序号索引页空标志。[#77096](https://github.com/StarRocks/starrocks/pull/77096) [#78488](https://github.com/StarRocks/starrocks/pull/78488)
- 其他指标与 FE 相关修复：将 bRPC 方法错误 bvars 报告为计数器；将范围分布分区的真实 tablet 数量报告为 Buckets；修复了 `show proc '/global_current_queries'` 中的 NPE 问题；在中间查询统计信息中报告 Data Cache 读取统计；在 jemalloc `malloc_usable_size` 中使用最小化 TSD 获取；阻止堆 profile 开关写入 `prof.thread_active_init`；防止 FE 连接 ID 在回绕后发生冲突；修复了当大量分区过时时表统计健康度被钉在约 1.0 的问题；为 FE 特性日志路径使用 `feature_log_dir`；在复制逻辑窗口算子时保留 `inputIsBinary`；以及修复了缓存失效后 Thrift 客户端泄漏的问题。[#76956](https://github.com/StarRocks/starrocks/pull/76956) [#77465](https://github.com/StarRocks/starrocks/pull/77465) [#77407](https://github.com/StarRocks/starrocks/pull/77407) [#77696](https://github.com/StarRocks/starrocks/pull/77696) [#77625](https://github.com/StarRocks/starrocks/pull/77625) [#78384](https://github.com/StarRocks/starrocks/pull/78384) [#78217](https://github.com/StarRocks/starrocks/pull/78217) [#76927](https://github.com/StarRocks/starrocks/pull/76927) [#77221](https://github.com/StarRocks/starrocks/pull/77221) [#77058](https://github.com/StarRocks/starrocks/pull/77058) [#77072](https://github.com/StarRocks/starrocks/pull/77072)

**其他**

- 为池化的 Poco 会话应用了请求超时；在 Linux AArch64 上将 Hyperscan 升级到了 Vectorscan 5.4.12；提升了 `JAVA_OPTS_FOR_JDK_*` 警告级别；并停止在 java-extensions 读取库中附带测试范围的 jar 包。[#78361](https://github.com/StarRocks/starrocks/pull/78361) [#78326](https://github.com/StarRocks/starrocks/pull/78326) [#78328](https://github.com/StarRocks/starrocks/pull/78328) [#77752](https://github.com/StarRocks/starrocks/pull/77752)
- 按键大小对 `dictionary_get` 键进行序列化；捕获 thrift 处理器中的异常；以及在 CTAS 场景中使 `files()` 的模式推断不占用 planner 元数据锁。[#78706](https://github.com/StarRocks/starrocks/pull/78706) [#78805](https://github.com/StarRocks/starrocks/pull/78805) [#78772](https://github.com/StarRocks/starrocks/pull/78772)
- 当文件没有尾随换行符时读取最后一行配置；相对 cwd 解析配置 YAML 路径；将 GCS 上传阈值 BE 配置项命名为 `gcs`（而非 `gs`）；将 StarOS 和 Starlet 升级到 4.1.3；并移除了一个废弃的配置转发头拆分以及文档文件名中的一个多余的 U+200E 字符。[#78065](https://github.com/StarRocks/starrocks/pull/78065) [#77132](https://github.com/StarRocks/starrocks/pull/77132) [#78452](https://github.com/StarRocks/starrocks/pull/78452) [#78038](https://github.com/StarRocks/starrocks/pull/78038) [#78780](https://github.com/StarRocks/starrocks/pull/78780) [#78612](https://github.com/StarRocks/starrocks/pull/78612)

## 4.1.4 {#414}

发布日期：2026 年 8 月 5 日

### 行为变更 {#behavior-changes-2}

- 在存算分离模式下，`TABLESAMPLE` / `SAMPLE` 子句以及 `ANALYZE SAMPLE TABLE` 现在会对 lake 表生效。此前，采样选项无法传递到 BE，因此会执行全表扫描。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- 对 `ALTER TABLE` 中 `flat_json` 配置的更改现在会通过带版本号的任务传播到 BE 节点，从而可靠地生效。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- 由 GIN 倒排索引处理的 `NOT MATCH` 谓词不再返回 NULL 行；NULL 行现已正确地从结果中排除。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` 不再为 char 系列列（优化器未使用该直方图）计算直方图，而仅计算 MCV，从而降低分析成本。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` 现在按保序分区范围对输出 manifest 进行聚类，为每个 manifest 生成更紧凑的分区边界，并在具有大量分区的表上提升 manifest 裁剪效果。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 多表 Stream Load 在决定是否使用合并事务日志时，现在会遵循每张表自身的 `file_bundling` 属性，而不是仅遵循全局的 `lake_use_combined_txn_log` 配置。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- 在主键表上，GIN 倒排索引与列模式部分列更新结合使用时不再返回损坏的结果；该索引现在从 delta column group segment 提供服务。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 对整数数组执行 `array_difference` 时，现在会以 64 位计算相邻差值后再扩宽为 `BIGINT`，修复了 int32 溢出问题。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 除数非常量的除法表达式不再被视为单调的，修复了可能导致结果错误的 ZoneMap 裁剪不正确问题。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 当 chunk 的展平结果超过 4 GB 时，数组和 map 构造函数现在会报错，而不是静默损坏数据。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- 主键导入现在能够正确拒绝 L1/L2 持久化索引中已存在的键；此前的存在性检查曾被跳过。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- 基于采样的 tablet 预拆分在 range-colocate 表上现在会将新分片分散到各计算节点，而不是全部堆积到源 tablet 所在的 worker 上，修复了 range 批量导入比 hash 慢约 3 倍的问题。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 增量或 AUTO 物化视图现在会在 `AUTO` 刷新模式下重建用于 PCT 回退的维护查询，修复了 IVM 不支持的查询形态导致的刷新失败问题。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` 现在可以用于非聚合 INCREMENTAL 物化视图；存储填充的 `__ROW_ID__` 列已从物化视图 DDL 的列列表中剔除，从而可以重新激活处于失效状态的 IVM。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg 分区缓存现在按内存（而不仅是条目数量）进行限制，并暴露了其使用情况，防止在具有大量分区的表上出现无限制增长。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- 使用 OAuth2 客户端凭据的 Iceberg REST catalog，在后台 token 刷新任务失效后现在能够自我恢复，而不是导致 all 后续请求失败。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake full vacuum 现在能够回收此前被跳过的孤立 `.lcrm`（Lake Compaction Rows Mapper）文件。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 存算分离发布现在会拒绝写入缺失 tablet 的 bundle tablet 元数据文件，防止分区在发布过程中永久卡住。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 被遗弃的外部（Spark/Flink 连接器）扫描上下文在被回收时现在会取消其 pipeline fragment，并且外部扫描计划会设置 `query_delivery_timeout` 以限制 QueryContext 的生命周期。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- 物化视图的固定范围映射现在能够正确地（按表 UUID）生成键，修复了 OLAP 表的 MV 引导固定被禁用的问题，以及不同数据库中同名表被错误处理的问题。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- 转发到 Leader FE 的语句对应的审计日志，现在会记录与 Leader 一致的完全限定查询关系，而不是 CTE 别名和未限定的表名。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 将完整的 `STRUCT` 列与 `ROLLUP`、`CUBE` 或 `GROUPING SETS` 一起聚合时，不再在计划阶段因 `usedStructFiledPos` 错误而失败。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE 内存 `Estimator` 现在会计入容器开销，并且 Parquet 扫描器现在使用受限的 chunk 大小而非完整批次大小来填充缺失列，从而提升内存统计的准确性。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改进 {#improvements-3}

- 在 Routine Load 中新增对 `INCLUDE METADATA` 子句的支持，用于暴露 Kafka/Pulsar 消息元数据（如 partition、offset 和 timestamp）。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- 将 Routine Load 元数据别名设置为可选。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- 为 `information_schema.materialized_views` 新增了 `LAST_FRESHNESS_CONFIRMED_AT` 列。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- 通过 `/metrics` 端点暴露了数据缓存指标，并新增了 FE Compaction 指标、`ALTER TABLE` 列操作指标及耗时，以及 exchange sink 上的 `CompressedInputBytes` 指标。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- 向终端用户暴露了来自连接器元数据操作的认证与连接错误详情。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 改进了大列容量限制检查的错误消息。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- 新增了 `hdfs_backend_selector_cache_replica_num` 变量，并使对象存储客户端缓存大小可在运行时修改。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- 将基于采样的 tablet 预拆分元层读取器扩展到了更多排序键类型，包括 `CHAR`、复合排序键，以及经 UTC 调整的 Parquet `TIMESTAMP` / ORC `TIMESTAMP_INSTANT`。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 改进了外部表统计信息采集：现在会跟踪谓词列的使用情况，在单次扫描中采集多列，并为 Iceberg 表应用了有限成本的扫描预算。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 支持 right-outer、semi、anti 和 full-outer 类型的 range-colocate join。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- 优化了 Query Queue V2 的成本估算器。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- 将通配符字面前缀下推到 S3 的 `ListObjectsV2` 调用中以处理 `FILES()` 通配符，减少了被列出的对象数量。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- 在 `remove_orphan_files` 中扫描 Iceberg manifest 条目时，现在仅投影 `file_path` 列。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- 支持 lake 主键 tablet 的 base compaction，以及针对独立排序键的 cloud 原生主键表的导入落盘与预先构建主键索引 SST。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 支持在存算分离的 range-distribution 表上仅以元数据方式新增一个尾部排序键列。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- 在 fragment 取消时取消正在进行的 exchange sink RPC，并在 reshard 清理过程中取消已重分片分区上正在进行的 compaction。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 在等待之前即触发多语句 Stream Load 的通道，从而降低导入延迟。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- 为 `Analytor::process` 中的列升级添加了内存限制检查。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 在异步 delta writer 停止时保留真实的错误状态。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- 在 lake PK-index SSTables 上记录生成版本，在 lake 主键发布时应用 `op_write.seg_delvecs`，并在 Compaction 发布版本冲突解决期间跳过打开输出 segment footer。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 为 lake 导入和发布版本路径添加了堆栈跟踪和细粒度的跟踪计数器，以便辅助诊断。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### 安全性 {#security-3}

- [CVE-2026-44891] 将 Netty 升级到 4.1.136.Final，修复了 STOMP 子帧解码器中的内存耗尽（DoS）漏洞。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] 将 Apache Thrift 升级到 0.24.0，修复了 C++ 绑定中的基于堆的缓冲区溢出以及一个无限循环漏洞。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 排除了存在漏洞的 Jetty jar 包（客户端 Digest 认证绕过漏洞，通过 Hadoop 传递引入），并将 pgjdbc 升级到 42.7.12。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 排除了未使用的 `avro-ipc` jar 包，该包捆绑了存在漏洞的 jQuery 1.4.2（以及其他 jQuery XSS CVE）。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- 移除了随已修复版本一起附带的存在漏洞的过时传递依赖（例如 `bcprov-jdk15on` 1.70 和已停止维护的 `okhttp` 2.x），并新增了依赖禁用规则以防止回归。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### 问题修复 {#bug-fixes-3}

修复了以下问题：

- 在 join 谓词推导中对空范围进行处理时，Planner 崩溃。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- 当 `CASE` 带有非空常量 `ELSE` 时，聚合被错误地下推。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` 的分区依据（partition-by）被重写为一个经过裁剪的字典槽（dictionary slot）。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- join shuffle-join 输出属性分支中的运算符优先级错误，以及 `predicateCommonOperators` 未能在 join 算子构造过程中被传递。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 分析阶段视图列以及 `ROLLUP` 键的可空性（nullability）不正确。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- `regexp_extract_all` 在零长度捕获组上出现无限循环。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` 中的 `nullsFraction` 限幅值错误。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 字符串转数字的 schema change 转换中 `CAST` 语义不正确。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同步物化视图重写在同列 `min`/`max` 的情况下丢失了汇总（rollup）列。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- 在向上提取扫描谓词时，重写被错误地应用于 `array_map` lambda 内部。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- 在支持 bucket 感知执行的 Iceberg bucket 表上，`COUNT(DISTINCT)` 出现了重复计数。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 对必填 Iceberg 列进行 `UNNEST` + `GROUP BY` 操作时，可空输出出现报错。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- 降低了 `INSERT OVERWRITE` 代码路径上的锁争用，并恢复了 `Operator` 盐值，以避免在 CBO 表裁剪期间出现 Memo 自引用。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 当字符串日期分区列与时间值进行比较时，Iceberg 分区裁剪、manifest 行数估算、元数据删除、相等删除（equality-delete）应用以及 Delta Lake 分区裁剪均出现错误。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg 读取未遵循目标快照的表结构和分区规范；在删除一个分区字段后，针对 Iceberg V1 表的查询失败；以及物化视图重写在 `rollback_to_snapshot` 之后返回了过期结果。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg manifest 数据文件缓存返回了不完整的文件集；增量扫描范围迭代器在并发关闭时不安全；分析阶段的时间穿梭（time-travel）快照绑定现已改为尽力而为（best-effort）。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta 元数据派生的统计信息现在会标记为 `StatsSource=TABLE_METADATA`。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- 修复了 Delta Lake 和 Kudu 非分区物化视图的查询重写。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- 由于 `gcs-connector` 3.x 配置项重命名，导致 GCS 下发凭证被忽略。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive `getTable()` 现在会在回退到 `get_table_req` 之前先尝试重新连接，并通过异常类型而不是消息文本来检测“表不存在”。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet 列索引统计信息中不支持 `BOOLEAN` 的最小值/最大值。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 修复了对名称中包含 `.` 的键进行 Flat-JSON 子字段读取，以及重建中间 Flat-JSON 对象而不是返回 NULL 的问题；当子字段键存在大小写不敏感的冲突时，现在会跳过 JSON 子字段下推。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 当只有一个分支建立了 bitmap 索引时，保留了 OR 嵌套谓词。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- 主键表自增列部分列更新应用过程中出现 BE 崩溃。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- 由于导入中间结果落盘期间出现 `LoadChunkSpiller` 初始化竞争，导致 BE 崩溃。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 原生 Parquet 读取器在遇到不完整的嵌套 lake 表结构时崩溃，以及 `BOOLEAN` 嵌套在 Avro 复杂类型列中时出现 ASAN 崩溃。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 在 schema change 期间，将无效字符串转换为 `NOT NULL` 数值列时出现 `bad_variant_access`。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` 错误处理路径中出现堆缓冲区溢出，以及多字符 CSV 分隔符跨越缓冲区扩展时出现 use-after-free。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- 在扫描拆除过程中对 `MorselQueueFactory` 的释放后使用（use-after-free）问题、可落盘 JOIN 构建 `set_finishing` 在取消时的问题，以及 `PipelineDriver` 析构函数中全局运行时过滤器定时器泄漏/未调度定时器的问题。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 对 `SparseRangeIterator::has_more()` 进行空值安全处理，以修复物理拆分空tablet导致的 CN 崩溃问题，以及 bRPC stub 缓存清理定时器泄漏问题。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- 查询部署工作节点上未恢复 `ConnectContext` 的问题。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 为存算分离主键表保留事务内 upsert/delete 的顺序，包括使落盘合并操作具备操作感知能力。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- 在 `cal_new_base_version` 中读取持久化元数据以避免悬空的 `prev_garbage_version`；在 `publish_version` 中保持 `base_version` 与 `base_metadata` 同步；以及在常规路径上持久化合并后的并行 Compaction 事务日志。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- Range-colocate 相关修复：阻止了对齐作业风暴并对未对齐的 colocate join 执行失败关闭（fail-close），修复了在 bucket-shuffle 下空值安全 join 丢失匹配的问题，并改进了计划反馈中 range-colocate join 的检测。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- Tablet 拆分/重分片相关修复：拆分时提升表的乐观版本以使并发查询重新制定计划，在相同tablet重分片前刷新主键索引 memtable，跨重分片按版本区间保留 lake vacuum 文件，在不可拆分的 range 分布 tablet 上停止自动拆分作业循环，并修复了拆分后 range 分布排序键上的 `IS NULL` 剪裁问题。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- 拒绝会重新排列 key 派生 range 分布表键列顺序的全列 `ORDER BY`。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- 按刷新顺序合并导入落盘并行合并结果，并在文件打包结转中按 `metaId` 排除已触及的索引。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- 为吸收NULL的字典映射 group-by 键预留字典大小 + 1 的空间，并在报表重新调度前检查 driver 就绪状态。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- 当锁从未成功获取时，`PlannerMetaLocker` 跳过了解锁。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- 按 warehouse 过滤资源组。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- 在 `SHOW CREATE ROUTINE LOAD` 输出中对 `jsonpaths` 的值进行转义。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- 修复了 Arrow Flight 预处理语句转发问题。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 修复了字典刷新间隔溢出以及意外自动刷新的问题。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- 修复了在 `ERROR_IF_OVERFLOW` 下最小值/最大值为空时统计信息缓存加载失败的问题。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- 将catalog删除的存在性检查在写锁下改为原子操作。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {#413}

发布日期：2026 年 7 月 14 日

### 行为变更 {#behavior-changes-3}

- `CTAS` 现在会保留明确声明的 `VARCHAR(N)` 列长度，而不是将其放宽为 `VARCHAR(MAX)`。现有表不受影响；使用 `CTAS` 创建的新表将在后续写入时强制执行声明的长度。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- 在没有 `OPERATE ON SYSTEM` 权限的情况下查询 `sys.fe_memory_usage` 或 `sys.fe_locks` 现在会返回明确的拒绝访问错误，而不是容易引起误解的节点查找失败。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` 以及 broker/stream load 不再对使用 `isAdjustedToUTC=false` 写入的 `INT64` Parquet 时间戳应用会话时区偏移；这些时间戳现在被视为挂钟值并按原样导入。在 v4.1.3 之前从此类文件导入的数据可能与升级后导入的数据不同；如果需要保持一致性，请重新导入。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 成功提交的多表事务 Stream Load 作业现在会在 `information_schema.loads` 和 `SHOW STREAM LOAD` 中正确显示为 `VISIBLE`，而不是停滞在 `PREPARING` 状态。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- 连接器增量扫描范围调度现在会始终复用已部署 fragment 的 driver 布局，防止扫描范围被错误地分配给不存在的 driver。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` 常量折叠现在与 MySQL 8 的反斜杠转义语义保持一致，修正了类似 `'a\\\\b'` 这样的模式此前返回相反结果的问题。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load现在支持 `property.kafka_partition_discovery` 属性，该属性允许在指定 `kafka_partitions` 和 `kafka_offsets` 以设定精确起始偏移量的同时继续进行分区自动发现。当未设置 `property.kafka_default_offsets` 时，作业已有消费进度后发现的分区的默认起始偏移量由 `OFFSET_END` 变更为 `OFFSET_BEGINNING`——这一变更适用于**所有**自动发现作业，而不仅仅是使用新属性的作业。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非 group-by 聚合现在会在合并之前被下推到 `UNION ALL` 分支中，从而降低对联合（union）进行聚合查询时的网络传输和内存占用。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM 维护查询现在会在每次刷新时根据当前视图定义重新推导，而不是使用在 `CREATE` 时保存的固定查询文本；现有物化视图无需重新创建即可自动受益于改写器的 bug 修复。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- 基于采样的tablet预拆分现在会将预拆分分片分散到所有计算节点上（`SPREAD` 放置方式），而不是将其集中放置在源tablet所在的 worker 上（`PACK` 放置方式），从而提升导入并行度。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 仅修改列注释的 `ALTER TABLE ... MODIFY COLUMN` 现在会走轻量级的仅元数据路径，而不是触发完整的 schema change 作业，并且该行为现在在主键列上同样生效。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` 和 `CEIL` 现在被视为非保留关键字，可以在不加引号的情况下用作列名。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` 输出现在始终会在 Properties 列中包含 UDF 和 UDAF 的 `isolation` 属性（`shared` 或 `isolated`）。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` 的默认值由 100 提升为 200，通过在每个 `DeleteObjects` 请求中批量删除更多过期文件，提升了对象存储上的 vacuum 吞吐量。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- 带有分发凭证的 Iceberg REST catalog表现在会被缓存，并在后台刷新其凭证，从而消除了每次 `getTable()` 调用 `GetDataAccess` 导致 AWS Lake Formation 触发速率限制的问题。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM `bitmap_union`、`hll_union` 和 `percentile_union` 聚合状态现在只在物化视图中存储一份，而不是两份（可见列 + 隐藏的 `__AGG_STATE_` 列），使这些 sketch 类型的存储开销减半。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 增量物化视图现在支持 `bitmap_agg`、`hll_union`、`percentile_union` 和 `bitmap_union` 聚合函数，使精确去重和基于 sketch 的聚合能够以增量方式维护。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- 基于采样的tablet预拆分tablet数量现在会向上取整为活跃计算节点数量的整数倍，以实现均匀分布，并设有最小tablet大小下限，以避免小规模导入时出现过度碎片化。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改进 {#improvements-4}

- `ngram_search` 函数现在接受非常量的 needle 参数。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- 新增由 FE 配置项 `enable_http_auth` 控制的 HTTP 认证框架，用于对 all 外部 HTTP 端点进行认证和 RBAC 强制校验的开关控制。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- 为 `information_schema.materialized_views` 新增刷新与放置可观测性列（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- 新增一项可选的 FE 配置，支持在日志回放时对外部统计信息缓存进行延迟刷新（lazy refresh），以避免缓慢或卡住的外部元数据存储（metastore）阻塞 FE 日志回放或启动过程。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- 现在允许通过快速 schema change 在不进行数据重写的情况下，对 range 分布（存算分离）排序键列进行 `VARCHAR` 长度扩展。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 当存算分离事务日志写入超过可配置阈值时，新增堆栈跟踪（stack-trace）转储，便于诊断缓慢的 `put_txn_log` / `put_combined_txn_log` 调用。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- Tablet 预分裂元信息层 footer 读取器现已支持 `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR` 以及 ORC `TIMESTAMP` 排序键，减少了需要回退到数据层采样的导入数量。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- 基于采样的 tablet 预分裂现已适用于 `INSERT INTO ... SELECT ... FROM <OLAP table>` 导入，也适用于包含 all 排序键列的列列表 `INSERT` 语句。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 为存算分离的 tablet 元数据和事务日志文件新增 Adler-32 校验和保护，可在读取时检测静默损坏。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- 新增每个数据库的 `txn_max_committed_pending_publish_ms` FE 指标，用于报告最早一个已提交但尚未发布的事务的存在时长，帮助检测版本发布停滞的情况。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- Tablet 分裂/合并现在会根据发布版本（publish-version）响应实时触发，减少了导入完成与自动分裂/合并启动之间的延迟。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- 通过将无 SST 的条件合并任务路由到 `pk_index_execution` 线程池，优化了存算分离主键表的条件更新比较阶段。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- 将存算分离的 schema change 和 rollup 任务锁的作用范围从整个数据库收窄到表级别，减少了同一数据库中其他表并发操作时的锁竞争。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 在存算一体模式下，将若干数据库级写锁收窄为表级密集写锁，减少了 BE 上报回调和降冷操作期间的锁竞争。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load 现已原生支持 `MAP` 和 `STRUCT` 目标列。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- Range-colocate tablet 的稳定性门控现在会等待 StarOS 的放置收敛（placement convergence）完成后，才将分组标记为稳定，确保 colocate join 能够实现主机本地执行。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 改进了外部表的 CBO 统计信息：优化器现在可以在不进行完整文件枚举的情况下，从 Iceberg manifest 中估算行数；修正了 Hive/Hudi 在 Parquet/ORC 压缩下行数估算偏低的问题；为 JDBC 连接器新增了异步行数统计；并在 Puffin 统计信息不可用时，为 Iceberg 和外部连接器提供 NDV 估算回退方案。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg manifest 列统计信息现在仅针对 clustered 列进行选择性缓存，降低了具有大量数据文件的宽表的 FE 堆内存消耗。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部表统计信息采集现已支持跨 FE 重启和高可用故障切换的持久化谓词列（predicate-column）跟踪，使 auto-ANALYZE 能够定位到正确的列。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- 新增结构化的 `[ExternalStats]` 日志记录，覆盖外部表统计信息采集从调度到执行的完整生命周期。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` 现在会在外部表统计信息任务的 Properties 列中包含分区、列和快照元数据。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 每个外部表的统计信息来源（`TABLE_METADATA`、`ANALYZE` 或 `NONE`）现已在查询运行时 Profile 中展示。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- 新增对 Iceberg 和 Delta Lake 外部表的分区过滤要求及分区数量限制的支持（此前仅支持 Hive、Hudi 和 Paimon）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` 和直方图 `ANALYZE` 现已支持小于 1% 的采样比例，修复了在大表上计算出的比例会截断为零而导致失败的问题。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- 新增 `jemalloc_conf` BE 配置项，使 jemalloc 运行时选项可通过 `information_schema.be_configs` 查看。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 新增 `compaction_chunk_reset_memory_tracker_threshold_percent` BE 配置项，通过释放保留的 chunk 容量，降低存算一体模式下主键 Compaction 期间的内存占用。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- 将 staros 升级到 v4.1.1，包括跨重启持久化的 `datacache.enable`、按 worker group 覆盖 shard 预热超时时间，以及改进的 S3 重试抖动（jitter）。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- 通过在 SQL 字符串中不存在凭据标记时跳过正则扫描，优化了审计热路径上的 SQL 凭据脱敏处理。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet 扫描器中基于表达式驱动的按需延迟列加载，减少了多分支 `OR` 查询中不必要的 I/O。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` 现在通过使用复合估算器（composite estimator）而非依赖顺序的 HIP 估算器，产生稳定的基数估计结果。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### 安全性 {#security-4}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final，以修复 SNI 处理器堆耗尽（DoS）、IPv6 子网过滤器绕过以及 DNS 缓存污染问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 将 `jackson-databind` 升级到 2.21.4，以修复两个反序列化漏洞。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 从 Hadoop 传递依赖中排除了 `org.jline:jline-remote-telnet`，以修复未经身份验证的 Telnet 服务器拒绝服务（DoS）漏洞。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] 更新了 pprof 预编译二进制文件，修复了 pprof 二进制文件中的一个漏洞。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 修复了 `information_schema.task_runs` 中的 SQL 注入问题，该问题可能因谓词值中的单引号而导致转义出字面量边界。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key` 和 `iceberg.catalog.jdbc.password` 现已在 `SHOW CREATE CATALOG` 输出中被脱敏处理。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 修复了当输入以被截断的百分号转义序列结尾时，`url_decode` 中出现的越界读取问题。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- 修复了 `HyperLogLog::deserialize` 接受超出范围的 `SPARSE` 寄存器索引的问题，该问题可能导致堆内存损坏，并在输入格式异常时使 BE 崩溃。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- 修复了 `bar()` 拒绝负宽度值的问题，此前该问题会导致字符串无限增长及 BE 内存耗尽。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### 问题修复 {#bug-fixes-4}

以下问题已修复：

- `add_files` 使用 Parquet 物理编码字节而非逻辑类型值填充 Iceberg 文件边界，导致文件级 min/max 裁剪不正确（例如在 `DECIMAL` 列上）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` 在遍历输入列表以不可变 `List.of(...)` 方式构建的计划节点时抛出 `UnsupportedOperationException`。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` 二阶段重新规划可能会产生来自第一次规划会话的过期 lambda 参数列引用 ID，从而导致 `expr_type does not match slot_type` 错误。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- 当更新中未包含 GIN（倒排）索引列时，对带有 GIN 索引的表进行部分列更新会导致查询无限期挂起或失败。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- 当 rowset 模式与 tablet 模式之间发生模式漂移时，Lake PCU（部分列更新）会崩溃或静默损坏数据。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 针对包含多个模式子句的外部 Iceberg 表执行组合 `ALTER TABLE` 时，每次子句分发都会错误地重新执行 all 之前已排队的操作。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- 当 `num_rows` 快照在分区刷新与资源组取消交错执行期间超过实际 chunk 行数时，`PartitionedSpillerWriter` 会因 `SIGSEGV` 崩溃。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- 由于 BE 信号初始化时未忽略 SIGPIPE，BE 进程可能在启动期间（通常紧接在部署之后）意外退出。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 当由于行范围过滤而跳过 struct VARCHAR 子字段填充时，Parquet 临时字典编码列会泄漏到上层，导致类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` 在审计日志中记录的是 `ReturnRows=0`，而非实际导出的行数。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- 由于锁类型不匹配，`TabletChecker.doCheck()` 在 `blockingAddTabletCtxToScheduler` 中抛出 `IllegalMonitorStateException`，导致整轮检查静默中止。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` 对于 `DATETIME_PRECISION` 始终返回 `NULL`，破坏了依赖该字段推断列大小的 MySQL 协议客户端。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- 当查询在不同数据库或 catalog 中联接两个具有相同非限定名称的表时，物化视图刷新会因 `Duplicate key` 而失败。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 在某些条件下，可落盘哈希 Join 探测会崩溃。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 当宽度或桶数参数为零时，Iceberg `truncate` 和 `bucket` 转换函数会导致 BE 因 `SIGFPE` 崩溃。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 当被除数为 `TYPE_MIN` 且除数为 `-1` 时，`mod()` 和 `pmod()` 会导致 BE 因 `SIGFPE` 崩溃。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- 当 `bucket_num` 为零或负数时，`histogram()` 会导致 BE 因 `SIGFPE` 崩溃。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- 当 all 输入行为 `NULL` 时，`encode_fingerprint_sha256` 会因 `SIGSEGV` 崩溃。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 通过 GIN 倒排索引评估包含单字符通配符 `_` 的 `LIKE` 模式时返回了不正确的结果。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- 当目标 segment 为空时，针对 GIN 倒排索引的仅 AND `MATCH` 查询会返回虚假错误。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene `match_all` 查询返回了不正确的结果；已通过升级 CLucene 依赖项解决。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- 向量索引重写直接在共享表结构上注册了一个合成距离列，导致对同一张表上不相关的并发查询出现 `Multiple entries with same key` 错误。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- Join 重排序裁剪可能会裁剪仍被扫描谓词引用的列，导致统计估算抛出 `missing statistic of col`。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` 被通过 sum/count 物化视图错误地重写，在存在重复项时静默丢弃了 `DISTINCT` 并返回错误结果。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` 抛出了内部 `NullPointerException`，而非干净的语义错误。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- 对于没有 `COLUMNS TERMINATED BY` 子句的作业，`SHOW CREATE ROUTINE LOAD` 在第一个导入描述子句前错误地发出了多余的逗号。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- 当 CTE 名称被误认为真实表引用时，带有 CTE 的 `SECURITY INVOKER` 视图在权限检查中可能因 NPE 而失败。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 当日期/日期时间边界字面量偏移会导致超出可表示范围时（例如 `<= '9999-12-31'`），`ReduceCastRule` 会因 `SemanticException` 中止查询规划。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- 当联接条件使用了空值安全等于（`<=>`）析取时，`SplitJoinORToUnionRule` 会产生重复行。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 在多表外部查询的并行元数据准备线程间共享的 `Tracers` 在 `enable_profile=true` 下导致 `IllegalStateException`。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 中的分区消费者错误被静默丢弃，导致分区 TopN 返回部分或错误结果而不报告错误。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- 在 FE 调用方超时后，BE vacuum 任务仍以僵尸进程形式继续运行，耗尽了 `RELEASE_SNAPSHOT` 线程池并导致 vacuum 吞吐量崩溃。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- autovacuum 竞争可能会短暂地计算出一个比正在进行的事务大 1 的 `minActiveTxnId`，导致 BE 删除仍需要的合并事务日志，并使发布操作永久卡死。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- 由于 FE 的 EOS 取消与 BE 第二阶段部署之间存在竞争，查询在成功完成后被错误地标记为已取消。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 当聚合 TopN 运行时过滤器构建键为 `ConstColumn` 时，BE 会在 `AggTopNRuntimeFilterUpdaterImpl` 中因 `SIGSEGV` 崩溃。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- 当 all 非空数组为空时，`array_map` / `transform` 会静默丢弃 `NULL` 行并返回错误的行数。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 在 JIT 编译的表达式中，超过 2^64 的 `LARGEINT` / `DECIMAL128` 字面量会被静默截断为 64 位。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- 当最后一个字符存在截断或无效的多字节前导字节且分隔符为空时，UTF-8 字符串函数（`split`、`split_part`、`str_to_map`）会读取超出字符串末尾的内容。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- 即使在 `ALLOW_THROW_EXCEPTION` SQL 模式下，`parse_json()` 对格式错误的 JSON 也会静默返回 `NULL`，而不是使查询失败。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 严格模式下的数值窄化类型转换会对插槽数据未定义的 `NULL` 行错误地抛出溢出错误。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 由于负数截断除法余数处理错误，带有非零亚秒部分的 1970 年之前的 Parquet `INT64` 时间戳被解码为垃圾值。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970 年之前的 ORC `TIMESTAMP` 值在导入时其亚秒部分被丢弃。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC stripe 的最小/最大时间戳统计信息对于 1970 年之前以及含亚秒边界的情况解码错误，导致数据文件被错误裁剪。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- 嵌套在 `ARRAY`、`MAP` 或 `STRUCT` 列内的 `INT96` 类型 Parquet 时间戳在导入时丢失了一次会话时区偏移。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet `UINT_32` 值在导入到 `BIGINT` 列时被符号扩展而非零扩展，导致高位为 1 的无符号整数被静默存储为负值。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` 析构函数在销毁 `_scanner_ctx`（持有引用这些节点的谓词）之前销毁了 `_pool`（及其 `Expr` 节点），从而导致堆释放后使用问题。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- 当多字节 UTF-8 字符跨越 8 MB 解压缓冲区边界时，使用 OpenX SerDe 读取经 gzip 压缩的 JSON Hive 外部表会因 `UTF8_ERROR` 而失败。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- 由于客户端无条件访问了 JSON 字段，ADLS2 `ListPaths` 在非 HNS 账户上会因 `SIGSEGV` 而崩溃。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 当多个 `UNNEST` 算子共享同一个输入数组列并消费不同子字段时，`unnest` 会崩溃或返回错误结果。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- 在 `unnest` 执行期间未强制执行 `query_mem_limit`，导致对大数组执行 `unnest` 时会使 BE 因 OOM 被杀死，而不是使查询失败。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- 当排名限制恰好落在数据块边界上时，带 `RANK` 边界条件的 `TopN` 会丢失一行。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- 在 `PushDownDistinctAggregateRule` 之后进行列裁剪可能会生成一个空的分析（窗口）算子，导致规划或执行错误。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` 仅在 scan 算子上设置了行数限制，而未设置全局限制，导致在并发情况下，对受限子查询执行 `COUNT(*)` 返回的行数多于预期。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- 存算分离主键持久化索引重建在 segment-range 模式下使用了错误的 segment 迭代器位置，导致键范围过滤器应用不正确。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` 在未加表锁的情况下修改了 `rebuildPindexVersion`；`RestoreJob` 在恢复后仅在数据库 READ 锁的保护下修改了物化视图基表信息；`FinalizeCreateTableAction` 在创建迭代器时传递了数据库级别的锁。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- 如果获取某个数据库级锁时在循环中间抛出异常，`dumpImage` 可能会使全局元数据锁被无限期滞留。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- 多语句 Stream Load 每个事务都会泄漏一个 `TxnStateCallbackFactory` 条目，无限增长并最终耗尽 FE 堆内存。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- 当 catalog、数据库、表或分区名称过长时，`information_schema.task_runs` 直方图统计信息的行数可能会超出 `primary_key_limit_size`（128 字节）。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVM 指标生成了无效的 Prometheus `# TYPE` 行（标签集位于指标名称内部），导致 Prometheus 中止整个抓取过程。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- 在存算分离表上，`SHOW PARTITIONS` 和 `information_schema.partitions_meta` 将 all 物理分区的分桶数报告为表级默认值，而不是每个分区的实际分桶数。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` 对存算分离（`CLOUD_NATIVE`）表上的 all rollup 索引返回了基表结构。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` 空操作子句被错误地路由到轻量级注释路径，导致在批量 `ALTER TABLE` 语句中出现 `MODIFY COLUMN COMMENT can not be combined with other alter operations` 错误。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- 由于 `isKey` / `aggregationType` 规范化不正确，`isCommentOnlyModification` 可能会将键/聚合列误判为仅注释更改。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` 可能会提交一个循环视图定义，导致后续的 `SELECT` 抛出 `StackOverflowError`。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- 当下游消费者修改了前一个数据块，而 `accept()` 仍持有指向该数据块的指针时，`OrderedPartitionExchanger` 会导致堆释放后使用问题。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- 当构建端的插槽描述符为非可空但运行时状态为可空时，NLJoin 会崩溃。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 当结构体字段名不是可解析的 JSON 路径时，`CAST(json/variant AS struct)` 会在 fragment 准备阶段导致 BE 崩溃。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- 嵌套字典表达式的字典解码可能会在生产者和消费者 fragment 之间产生不兼容的字典转换，从而在运行时导致 `Dict Decode failed` 错误。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- 当 `get_rowset_by_version` 返回 `nullptr` 且 `gtid` 比较被置于空值检查之前时，schema change 会因空指针解引用而崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- 由于快照 manager 在决定是否回收父 tablet 元数据时未考虑重分片任务，在 tablet 拆分/合并之后，存算分离集群快照变得无法恢复。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- 文件绑定清理（vacuum）错误地将兄弟 tablet 的零行绑定 segment 标记为非共享，导致其 bundle 文件在其他 tablet 仍引用它的情况下被删除。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 存算分离表的 Compaction 发布过程丢弃了在 Compaction 事务开始后变为可见的 rollup/同步物化视图索引，导致 bundle 文件中缺少这些索引。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- 当在 tablet 重分片期间某次 Compaction 被丢弃时，存算分离持久化索引 Compaction 会错误地删除被直通复用的 SSTable 文件。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- 从 `NOT NULL` 到可空的 flat-JSON 列的表结构演变在 Compaction 读取路径中导致了 `CHECK` 崩溃。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- 在流式预聚合直通路径中，对可空列执行 `count_combine` 会导致 BE 以 `SIGSEGV` 崩溃。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- 由于 JDK 21 中移除了某个反射式 `DirectByteBuffer` 构造函数查找，Java UDF 在 JDK 21 及以上版本上加载失败。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- 由于解析器不支持 `CREATE TABLE AS SELECT` 中的 `ENGINE` 子句，导入到统一 catalog（Hive metastore）的 `CTAS` 总是失败。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` 反馈驱动的 JOIN 重建丢失了 `predicateCommonOperators`，导致在具有公共子表达式重用的执行计划上出现 `InputDependenciesChecker` 校验失败。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- 在含有子分区的表上，当空子分区在构建版本列表之前被裁剪时，Query Cache 归一化会以 `Preconditions.checkState` 崩溃。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` 会静默跳过存储在旧别名下的会话变量，导致查询转储重放时回退为默认值。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- 对于包含多个 Parquet row group 的数据文件，Iceberg `_row_id` 虚拟列由于对 row-group 起始偏移量进行了重复计算，返回了错误的值。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE 规划器无法定位目标扫描节点，因为它是按合成表 ID 而非物理表标识进行匹配的，从而丢失了基础快照 ID 和冲突检测过滤器。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- 当 `cancel_plan_fragment` RPC 在调用 `PipelineExecutorSet::start()` 之前到达时，`FragmentContext::set_final_status` 会因 `SIGSEGV` 而崩溃。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- 在 `FragmentExecutor` 仍在拆除某个 fragment 期间，`QueryContext` 可能被回收，从而在 `ResGuard::reset()` 中导致 heap-use-after-free。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` 未被初始化，导致默认构造的 `search()` 会解引用一个未初始化的指针。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` 微秒使用了 JVM 默认区域设置的数字集进行渲染，导致在阿拉伯语或波斯语等区域设置下出现非 ASCII 数字，从而破坏了 tablet 预拆分中的边界值解析。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- 在 `INSERT OVERWRITE` 之后、tablet 统计信息刷新之前，分区行数可能被写入 `_statistics_.column_statistics` 为零，导致优化器将分区基数估算结果坍缩。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- 当全局配置禁用首次导入统计信息收集时，`enable_statistic_collect_on_first_load` 的表级覆盖设置无法启用该功能。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- 当某个 union 分支没有输入行时，`PushDownNonGroupedAggregateBelowUnion` 会产生具有非可空声明类型的可空输出，导致 BE `CHECK` 失败。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {#412}

发布日期：2026 年 6 月 18 日

### 行为变更 {#behavior-changes-4}

- 现在，连接到用户没有权限的数据库时会返回正确的 MySQL 错误数据包，而不是以 ERROR 2013 关闭连接。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- 对于能够通过函数级权限看到某个函数、但未持有创建函数（create-function）范围权限的用户，`SHOW FUNCTIONS` 现在会将 UDF 文件和对象文件路径脱敏为 `***`。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 在从外部 catalog 查询 Hive 视图时，现在会正确应用 Ranger 行过滤和列脱敏策略。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` 现在能正确保留 `current_timestamp` 生成表达式。`DESCRIBE` 和 `information_schema` 现在反映的是该表达式，而不是回填时的字面量。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` 的导入时过滤在会话时区不是 UTC+8 的集群上不再出现过滤边界偏移问题。导入时间现在在 FE 与 BE 之间以 UTC 纪元毫秒数的形式传递。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` 会话变量现在能够正确应用于 Paimon 扫描拆分计算，而不是始终使用默认值。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` 现已默认启用。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive 分区统计信息不再按计时器针对每个 key 自动刷新。分区统计信息现在仅在显式调用 `refreshTable()` 时才会刷新，从而降低了大型分区表上 HMS 的负载。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- 当 Iceberg 或外部 catalog 基表发生模式漂移（列类型变更、列删除或表删除）时，依赖该表的物化视图现在会在下一次刷新时被标记为不活跃状态，而不是静默生成 NULL 行或产生晦涩难懂的错误。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg 连接器现在会部分下推 `AND` 复合谓词中可转换的一侧，而不是在只有一侧可转换时丢弃整个谓词，从而改善了分区裁剪和数据跳过效果。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 显式事务中的 `COMMIT` 现在能正确等待数据库写锁最多 `query_timeout` 秒（而非毫秒），从而避免在短暂并发写入活动期间出现虚假的锁超时失败。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM 刷新现在会将严格导入过滤错误报告给调用方，而不是静默丢弃被过滤的行。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` 现在能正确排除 NULL 行，与 `COUNT(col)` 的语义保持一致。此前由 `COUNT(<nullable column>)` 支持的增量物化视图会物化出虚高的计数值。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` 现在还会显示由 `ALTER TABLE ... SET (...)` 在 cloud 内表（存算分离）上针对 `file_bundling` 和 `enable_persistent_index` 等属性触发的仅元数据异步 alter 任务。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 创建包含引用聚合函数的 `HAVING` 子句的增量物化视图时，现在会在 `CREATE` 阶段就失败并给出明确的错误信息，而不是在首次刷新时产生内部计划错误。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM 现在支持在增量物化视图中使用 `MIN`/`MAX(DECIMAL)` 聚合函数。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- 当第一个增量特征已经超过 `mv_max_rows_per_refresh` 时，IVM 自适应刷新现在能正确限定增量窗口的边界，避免在一次任务运行中刷新全部积压数据。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- 仅包含 GROUP-BY 的增量物化视图（例如 `SELECT k FROM t GROUP BY k`）现在能正确将 `__ROW_ID__` 编码为 VARCHAR，修复了第二次刷新时出现的崩溃问题。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 功能优化 {#improvements-5}

- 支持 Paimon 视图，包括 `CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`，以及从外部 catalog 查询 Paimon 视图。Paimon 视图内部的表引用现在会根据 Paimon catalog 进行解析，而不是 `default_catalog`。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 在 `FILES()` 中支持显式的 `schema` 参数，用于在读取存在模式漂移或复杂嵌套类型的文件时实现稳定的模式控制。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` 现在能够跨 all FE 节点获取查询概况信息，而不仅限于当前连接的 FE。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 新增 `query_id()` 内置函数，用于返回当前执行查询的 UUID。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 存算分离模式下的 `CREATE`/`ALTER STORAGE VOLUME` 现在会在持久化元数据之前验证存储位置的可访问性（凭据和端点），从而在配置错误时提前失败。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- 在 BE 中新增了对 AWS S3 凭据的 `WebIdentity` 令牌提供程序支持，与 FE 中现有的 `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` 支持保持一致。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 新增 `ADMIN SKIP COMMITTED TRANSACTION` 命令，用于在存算分离表上因缺失 `txnlog`、segment 丢失或远端 I/O 缓慢而导致发布（publish）被永久阻塞时，解除卡住的 `COMMITTED` 事务。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` 现在会将 `table_name` 谓词下推到 FE，大幅降低了单表查找的开销。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- 为 `information_schema` 表添加了缺失的 MySQL 8 列，以提升与在连接内省期间检查 MySQL 8 架构的 BI 工具和 JDBC 驱动程序的兼容性。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- 新增了 `enable_pipeline_event_scheduler` BE 配置项，作为集群级的强制关闭开关，当设置为 `false` 时会覆盖会话级变量。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 为统计信息采集新增了可选的宽字符串列隔离功能，用于在对包含多个宽字符串列的表采集统计信息时降低单次查询的内存峰值。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- 慢锁日志现已支持按事件限流以及可配置的堆栈捕获控制，以避免在高锁竞争下出现 JVM 安全点停顿。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- 物化视图刷新日志条目现在在前缀中包含数据库名称，使得在同一物化视图名称存在于多个 schema 的多租户部署环境中，日志行可以被区分开来。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 配置项现已支持动态修改，可通过 `ADMIN SET FRONTEND CONFIG` 在运行时切换，无需重启 FE。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- 新增了 `enable_print_load_profile_to_log` FE 配置项（默认值为 `false`），用于将导入 Profile（Stream Load、Routine Load、Broker Load 以及合并提交导入）写入 `fe.profile.log`，即使内存存储因查询 Profile 突增而被淘汰，这些数据也能保留下来。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` 现在可以在 `JobProperties` 中正确渲染列映射，而不是显示 Java 对象引用。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` 现在使用表级锁而不是 catalog 级锁，减少了在包含大量并发活跃表的 catalog 上出现的刷新串行化延迟。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- 元信息扫描（后台统计信息采集）现在能够优雅地处理 `ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN` 和 `REORDER COLUMN` 等 schema change，而不会在变更后的 Segment 文件上因找不到文件而失败。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- 基于采样的 tablet 预分裂现已支持多分区 Range 分布表以及 Broker Load，使得在没有现有数据分层基线的情况下也能实现首次导入的并行度。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL 结果序列化不再使用逐行虚函数调度；现在每个数据块只构建一次类型化的列写入器，从而降低了在宽结果集或大结果集场景下的序列化开销。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` 转字符串的类型转换现在直接写入输出缓冲区，消除了逐行堆内存分配。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- 查询统计信息合并路径用无锁并行映射取代了 `SpinLock`，在大型集群中，当工作节点发送中间或最终统计信息时降低了 CPU 使用率。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 聚合哈希表和哈希集的预取操作现在以 L2 缓存驻留情况为门控条件，避免了当桶数组能够放入 L2 缓存时出现的 4%–9% 的性能回退。预取距离现已可配置。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 在存算分离主键表上，针对轻量 Compaction 发布的流水线化逐 Segment `.lcrm` 读取，减少了顺序对象存储往返次数。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 存算分离模式下的冷主键索引重建扫描现已实现跨 Segment 的并行化，在 Segment 读取受远程 I/O 限制的情况下缩短了重建时间。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部查询（统计信息采集、Task 运行、物化视图刷新）现在可以在 `SHOW PROC '/current_queries'` 中看到，并可通过 `KILL QUERY` 终止。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- 新增了 Lake Vacuum 批大小和重试次数的 bvar 指标，用于监控 S3 限流情况并调优 `lake_vacuum_min_batch_delete_size`。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- 新增了 `CatalogRecycleBin` 大小 gauge 指标，以便在回收站增长对 FE 堆内存造成压力之前及时发现问题。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` 分区表现在会在 `OlapTableSink` 中打开 all 分区，而不是应用为 Range 分区表设计的“最近 N 个”启发式算法，从而降低了增量打开的 RPC 开销。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- 支持通过 `FILES()` 或 Broker Load 将 `LARGE_LIST` 和 `FIXED_SIZE_LIST` Arrow 类型导入到 JSON 列中。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 存算分离表上的合并提交（`FRONTEND_STREAMING`）导入现已支持事务日志与文件的合并打包，使其与其他导入类型保持一致。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- 新增了可动态修改的 FE 配置项 `slow_publish_partition_log_threshold_ms`（默认值 3000 毫秒），用于在无需重启 FE 的情况下控制 Lake 发布阶段分解告警的阈值。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### 安全性 {#security-5}

- [CVE-2026-43869] 将 `libthrift` 升级到 0.23.0 版本，以修复证书主机校验不当的问题。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] 将 Apache Tomcat 升级到 9.0.118 版本，以修复 HTTP/2 请求头校验问题。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] 将 Netty 升级到 4.1.135.Final 版本，以修复 SNI 处理器堆耗尽（拒绝服务）、IPv6 子网过滤绕过以及 DNS 缓存投毒问题。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- 将 pprof 预编译二进制文件升级到 Go 1.25.11，以纳入 Go 标准库的安全修复。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### 问题修复 {#bug-fixes-5}

修复了以下问题：

- 当 URL 中的 `:` 出现在 `host:port` 模式之外时，`parse_url()` 会返回错误的主机。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 字典翻译表达式在某些不满足假设条件的表达式（例如 `IF(col = '1', NULL, 'ok')`）中错误地假定了 `f(null) = null` 成立。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- 事务型 Stream Load 使用了默认的 RPC 超时时间，而不是用户指定的超时时间，导致出现过早超时。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- 当身份列中的值为 NULL 时，Iceberg 等值删除文件无法删除匹配的行，原因是 `NULL = NULL` 在 Join 谓词中会计算为 UNKNOWN。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- 针对包含 `INJECTED` 分区投影列的表，错误信息现在更加详细，可以显示出具体是哪些列导致了问题。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- 针对仅插入型 ACID Hive 表的查询返回的行数超出预期，原因是插入覆盖（insert-overwrite）操作未被识别。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 当 Iceberg 元数据条目在并发读取期间被固定（pinned）时，磁盘缓存会超出其配置的容量上限。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 在查询外部 Catalog 时，Paimon 主键列被错误地标记为不可为空。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 当 `MultiDistinctByMultiFuncRewriter` 对包含多个 `ARRAY_AGG(DISTINCT <const>)` 输入的查询反复应用同一条规则时，优化器会发生超时。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Oracle JDBC 日期谓词下推时未使用 `DATE`/`TIMESTAMP` 关键字，导致 NLS 格式错误。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- 分区 TopN 可能会丢失其子算子所需的输出列。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- 无法在具有分区演进功能的 Iceberg 表上创建非分区物化视图。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- 在 `information_schema.be_cloud_native_compactions` 中，并行子任务的 Compaction 任务统计信息被覆盖并丢失。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同步物化视图的 `SHOW CREATE MATERIALIZED VIEW` 因"Table is not found"而失败。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- 当语句 `.log` 文件落在 4 段路径下时，Lake 发布多语句事务在 schema change 期间发生死锁。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- 排序合并提供程序的错误未传播到 fragment 上下文，导致查询静默失败。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 在长时间运行的 follower FE 上，`ConnectorTableId` 从 `int` 溢出为负值，导致 Iceberg 和 Hive 查询因误导性的"Invalid table type"错误而失败。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 带有空 optimize 子句（无分布或分区规范）的 `ALTER TABLE` 被错误解析，可能在 FE 重放时损坏表的默认分布。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- 由于 `AZURE_PATH_KEY` 未被识别为有效的 `StorageVolumeMgr` 参数，FE 在 ADLS2 存算分离灾难恢复期间启动失败。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- 当优化器将嵌套类型的一部分裁剪为 `UNKNOWN_TYPE`，或使用可为空的 array、map 或 struct 模式时，Avro 复杂类型解码失败。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 由于两个 `NullableColumn` 共享同一个 `NullColumn` 对象，COW 列变更优化导致 `map_apply` 及类似函数崩溃。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- 由于提供程序在 FE 上被急切实例化，带有自定义 `LocationProvider` 的 Iceberg 表在使用 `ClassNotFoundException` 进行 `SELECT` 查询时失败。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` 在每次缓存未命中时都会执行一次额外的 `getTableComment()` 往返，延长了高负载规划阶段的锁持有时间，并阻塞了并发 DDL。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- 当嵌套物化视图返回 `FULL` 或 `UNKNOWN` 时效性时，嵌套物化视图刷新抛出 `NullPointerException`。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE worker 在向速度较慢的 MySQL 客户端发送查询结果时无限期阻塞。结果发送路径现在强制执行写入超时。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- 在将主键表从 V1 编码（存算一体）复制到 V2 编码（存算分离）集群时，或在两个存算分离集群之间复制时，PK `.del` 文件未被转码。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 在 `VERSION_INCOMPLETE` 恢复期间，由于在添加存活副本之前未移除过期的副本引用，`TabletInvertedIndex` 中积累了重复副本。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- 由于 `REPLICATE_SNAPSHOT` 任务与按文件复制的子任务共享同一个线程池，存算分离 Lake 复制文件拷贝导致 CN 崩溃。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- 当 BE 使用 `.000` 小数后缀格式化单位计数器时，`RuntimeProfileParser` 抛出 `NumberFormatException`。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 存算分离 PK tablet 拆分中共享 segment 的物理 rowid 编码不正确，导致 `rss_rowid` 条目错误。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- 混合 range-colocate × hash 分布的 `JOIN` 查询返回 `Unknown error` 而非有效结果。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` 使用了固定的 UTC+8 格式化器；现在输出会遵循会话 `time_zone`。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- 当 all 值为 `NULL` 且该列经过可空一元函数路径时，Decimal 类型列的小数位数丢失，导致下游结果类型损坏。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- 嵌套类型的 JSON 部分追加导致 ASAN 崩溃。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` 角色的权限缓存在 `GRANT`/`REVOKE` 时未失效，导致过期权限在到期前仍然生效。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- 当子 writer 没有任何追加操作时，`FlatJson` 崩溃。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- 当物化视图本身带有 `HAVING` 谓词时，聚合物化视图改写被错误地应用，可能返回不完整的结果。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 在进入并行合并模式时，落盘 writer 的 `auto_flush` 标志存在数据竞争，导致在 ARM 上出现意外的 segment 刷新。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load 调度器在进行阻塞式 BE RPC 以获取 Kafka 或 Pulsar 分区元数据时持有每任务写锁，导致锁持有时间长达 33.6 秒。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- 当启用 `tablet_sched_disable_colocate_balance` 时，已失效 BE 上的 Colocate Group tablet 被错误地报告为 `HEALTHY`。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- 当存在 MISSING（幻影）副本行时，`ADMIN SHOW REPLICA STATUS` 导致 MySQL 结果流不同步，造成客户端挂起或断开连接。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 在存算分离模式下，每个发送者的 `open` RPC 未重新记录分区协调者的声明，导致部分发送者在协调者选举中被遗漏，使 `combined_txn_log` 文件未被写入。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_.pipe_file_list` 内部表在 `_statistics_` 数据库或表被删除后未被重新创建。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- 被 `TaskCleaner` 强制终止的任务运行未被归档，导致它们从 `information_schema.task_runs` 中消失且没有留下任何痕迹。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` 和 `SWAP TABLE`/`SWAP MATERIALIZED VIEW` 仅持有高负载表锁而非数据库写锁，使并发读取者能够观察到不完整的中间名称到表的映射。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK 索引 Compaction 输出的 sstables 在打开时未附带 tablet 元数据，导致永久性的 `metadata is null when loading delvec` 失败。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 在显式事务中，针对同一事务中已修改的表执行的部分列更新 `INSERT` 会在 `COMMIT` 处静默损坏数据。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- 与 range 分布不兼容的 `ALTER TABLE` 操作（schema change、排序键变更）现在会以可操作的错误被拒绝，而不是静默损坏元数据。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- 优化器中子节点类型不匹配的聚合函数导致查询结果不正确。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- `ALTER ROUTINE LOAD` 使用保留关键字表名时写入了无法解析的 `origStmt`，导致 FE 重启后列映射丢失。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` 兼容性检查未递归处理嵌套类型（例如 `ARRAY<VARCHAR>`），导致 `CREATE MATERIALIZED VIEW` 对于 `ARRAY_AGG` IMV 失败。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- 当扫描范围被完全过滤掉时，Parquet 临时字典编码列泄漏到上层，导致下游类型不匹配。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `CASE WHEN` 中混合使用浮点数和整数 `WHEN` 以及结果类型时会生成无效的 JIT IR，导致结果不正确或崩溃。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT 编译失败导致 `LLVMContext` 的释放后使用（use-after-free），从而引发 SIGSEGV。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- 后台统计任务覆盖了会话 `WAREHOUSE` 设置，影响了同一连接上下文中后续的用户查询。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- 当从未成功完成过集群快照时，`CatalogRecycleBin` 停止清除条目，导致在高负载 `INSERT OVERWRITE` 工作负载下 FE 内存无限增长。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- FE 未检测到非主键副本的版本空洞；查询被永久路由到存在空洞的副本，且 `max_version` 被冻结。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（一个在共享读锁下被修改的 `HashSet`）上存在数据竞争，可能导致条目丢失或集合损坏。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 当保留边界元数据已被清理时，Vacuum 水位线未被正确报告，导致 `file_bundling` 的切换版本清理停滞。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum 重试使用了确定性的指数退避；现已加入去相关抖动（decorrelated jitter），以便在 S3 限流时将重试请求分散到各个 CN 上。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- `OlapTableSink` 中的内存统计被夸大了，因为从查询内存池分配的 RPC 请求在进程上下文中被释放。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 当自动分区创建并发触发 `_incremental_open_node_channel` 时，`TabletSinkSender::_send_chunk_by_node` 中存在竞态条件。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 由上游变更回合（backport）创建的 UDAF 上下文通过 `unique_ptr::release` 导致了内存泄漏。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- 由于 `append_selective` 中内存统计不准确，分区连接（partitioned join）探测过程中可能发生越界访问。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 配置项的名称存在拼写错误。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` 错误地将范围共置（range-colocate）PACK 分片组当作孤立项回收，在存算分离模式下永久删除了活跃分片。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- 对于 PRIMARY KEY 表以及没有显式 `ORDER BY` 的表，colocate tablet 拆分的排序键元数（arity）是从基础 schema 而非物化 schema 中解析的，导致拆分任务完成后未能减小 tablet 大小。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 当分区数据低于合并阈值时，自动合并守护进程会将预先拆分的 tablet 重新合并在一起，从而抵消了基于采样的预拆分带来的并行度收益。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 由于函数的 `FunctionName.db` 仍指向源数据库，`RESTORE ... AS <new_db>` 之后 follower FE 上缺失了数据库级 UDF。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- 当仓库拥有多个 CN 组时，存算分离 `DISTRIBUTED BY RANDOM` CTAS/INSERT 为不可变分区 tablet 位置分配了错误的 CN 组。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 当统计信息估算过程中分区被并发删除时，`StatisticsCalcUtils` 中出现 `NullPointerException`。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` 和 `FrontendServiceImpl` 元数据 RPC 处理程序持有完整的数据库 READ 锁，阻塞了无关表的 DDL。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 在未通知共享上下文观察者的情况下翻转其完成状态的 Pipeline 算子，可能导致事件调度器下的对端驱动器停滞。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 谓词下推中的非根复合谓词产生了 `NotPushDown`，而不是扫描级别的 EOF，导致在 UNION 下存在不可能成立的嵌套 AND 分支时 `OlapScanNode` 不输出任何行。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` 在仅有单一存储介质的 BE 上执行了昂贵的逐副本扫描；该检查现已对同构磁盘的 BE 优化为 O(1) 复杂度。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- 数据目录加载线程上的线程命名竞态导致每次 BE 启动时都出现大量嘈杂的 `failed to set thread name` 警告。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- 任务 manager 写入了非法的 `RUNNING→RUNNING` 编辑日志，导致任务运行记录在运行映射表中看起来无限期卡住。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- 主键多语句批量事务未能跨复合 rowset 累加 `num_rows`、`data_size` 和 `num_dels`，导致存算分离主键表上的行数统计不正确。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake 导入落盘清理现已改为使用基于事务 ID 的 vacuum 驱动回收，防止 BE 崩溃或 OOM 后出现孤立的落盘文件。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- 从 `0000` 年开始的 PostgreSQL JDBC 时间值导致类型映射结果不正确。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- 在 schema change 期间从 rowset 读取 `gtid` 之前缺少空值检查，导致 NPE 崩溃。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {#411}

发布日期：2026 年 5 月 29 日

### 行为变更 {#behavior-changes-5}

- Hive connector 默认现已使用原生 C++ Avro 扫描器，而非 JNI Avro 扫描器。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- 现已禁用针对 INCREMENTAL/AUTO 物化视图的查询改写，并且拒绝针对 INCREMENTAL/AUTO 物化视图进行 FORCE 刷新和分区刷新。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 功能优化 {#improvements-6}

- Java UDF/UDAF/UDTF 现已支持更多类型：UDAF/UDTF 的 STRUCT 参数和返回值、嵌套 ARRAY/MAP 类型、DATE/DATETIME、DECIMAL 以及可变参数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- 标量 UDF 现已支持 STRUCT 参数。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF 现已支持嵌套的 ARRAY/MAP 类型。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF 现在只会被加载和初始化一次，并在多个查询之间复用，从而降低了单次查询的开销。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- 为 Hive connector 用原生 C++ 扫描器替换了 JNI Avro 扫描器，支持直接二进制解码，并支持 `avro.schema.literal` 和 `avro.schema.url`。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS 语句中支持 Trino 的 `WITH` 子句。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- 在写入路径上完成了对 Iceberg `timestamptz` 分区转换的支持。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- 为 Iceberg 表聚合启用了 TopN runtime filter 下推。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- 支持 Iceberg 日期时间的最大值/最小值优化。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 允许在 Catalog 和 BE 中透传 HDFS HA 配置，以支持访问多个 HDFS 集群。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 为外部表查询增加了分区扫描数量限制。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- 对于不支持的 Iceberg V3 特性会快速失败。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- 支持通过 INSERT INTO FILES 为 CSV 导出提供 `csv.enclose` 和 `csv.escape`。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 新增了 `enable_push_down_schema` INSERT 属性，用于将完整 schema 下推到 `files()`。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load 作业现在遇到不可重试的错误（例如主键大小超限）时会被暂停。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 支持对来自两个子节点的复杂表达式进行 join 重排序。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- 改进了 CBO 统计信息估算，包括针对 `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION 以及常量的 MCV/空值比例传播。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- 改进了数据倾斜 join 检测：只有当 all join key 出现倾斜时才会触发检测，并新增了 `force_group_by_skew_eliminate_when_skewed` 开关用于强制应用倾斜规则。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- 在 FE 中支持对 `regexp_replace` 进行常量折叠。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 针对分区值为常量的日期分区列优化了 MIN/MAX。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- 在物化视图刷新中引入了关键字 `SCHEDULE`，作为 `ASYNC` 的同义词。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 在存算分离模式下为 Lake 表支持 tablet 创建重试。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- 为 Lake 列模式的部分列更新支持条件更新。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 对部分列更新发布、持久化索引初始化和 SSTable 打开进行了并行化处理，以提高导入吞吐量。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 支持在存算一体模式到存算分离模式复制过程中进行 DCG 文件同步。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- 支持对主键列和非主键列同时加宽 VARCHAR 长度的 schema change。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- 新增了 `snapshot_meta.json` 标记，用于集群快照完整性检查。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- 支持通过 DN 模式进行 LDAP 直接绑定认证。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- 新增了 `get_query_dump_from_query_id` 元函数，便于查询问题排查。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 支持在审计日志中记录被查询的关系（relation）。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- 新增了用于 MySQL 二进制结果编码的会话变量。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 新增了多项指标以提升可观测性，包括用于存算分离集群的 `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`，以及 Iceberg 元数据表查询指标。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- 新增了 FE 配置项 `deploy_serialization_min_thread_pool_size`。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- 新增了 `tablet_reshard_enable_tablet_merge` 配置项，用于禁用 MergeTabletJob 的创建。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- 通过 `SO_REUSEPORT` 消除了 HTTP server accept 的惊群（thundering-herd）问题。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- 支持通过 `CREATE FUNCTION ... AS <sql_body>` 创建 SQL UDF。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- 支持从 S3 加载 UDF。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 新增了 `uuid_v7` 函数，用于生成按时间排序的 UUID v7 值。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- 为 catalog 类型增加了单独的查询指标，以提升 external catalog 的可观测性。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- 支持为窗口函数显式指定倾斜提示（skew hint），通过将倾斜的分区键拆分为 UNION 来自动优化窗口函数。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### 安全性 {#security-6}

- [CVE] 将 Netty 升级到 4.1.133.Final。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] 将 pgjdbc 升级到 42.7.11（修复通过无限制 SCRAM PBKDF2 迭代次数造成的客户端拒绝服务问题），并将 BouncyCastle 升级到 1.84（修复 FrodoKEM 私钥泄露问题）。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] 使用 go1.25.9 构建 pprof，以消除 Golang CVE 问题。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- 将 jetty-http 升级到 9.4.58.v20250814。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- 清理了 Broker 依赖的 CVE 问题，并移除了 `wildfly-openssl`。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- 对 INSERT INTO FILES 错误信息中的凭据进行了脱敏处理。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### 问题修复 {#bug-fixes-6}

修复了以下问题：

- 由于 `hash_util` 静态初始化顺序问题导致 CN 启动时出现段错误。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 在启用物理切分的情况下扫描空 tablet 时导致 CN 崩溃。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- 查询 `information_schema.warehouse_queries` 时导致 BE 崩溃。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- 当 rowset `num_rows` 为零时，Lake Compaction 中出现 SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG fragment 连接中出现除零错误。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer 中优雅退出时发生崩溃。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- 可落盘哈希 JOIN 探测阶段崩溃。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 格式化到临时 `std::string` 时发生栈缓冲区溢出。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` 中发生崩溃。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 由于临时 `shared_ptr` 被销毁，导致 `LoadChannel::get_load_replica_status` 中出现 use-after-free 问题。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- 线程创建失败时，`ThreadPool::do_submit` 中出现 use-after-free 问题。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- Hive 分区描述符在 fragment 销毁过程中出现跨 fragment 的 use-after-free 问题。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- 信息模式（information schema）Sink 中的 use-after-free 问题。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- 由于复用 HttpClient 实例导致的 FE 文件描述符泄漏问题。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` 中的 JNI 本地引用泄漏问题。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- 缓存物化视图计划上下文时出现内存泄漏。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- 本地 Exchange 中出现意外的内存过度使用问题。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` 中 `response->tablet_metas` 的竞态问题。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` 中并发 `SegmentFlushTask` 竞态问题。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- 序列化期间 `RuntimeProfile` 最小/最大值竞态问题。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- 查询上下文销毁期间 `PipelineTimerTask` 中的竞态条件。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` 中的竞态条件。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` 与 `array_length` 之间共享 `NullColumn` 的问题。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- 由分区版本间隙导致的批量发布死锁问题。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 在存算一体模式下预热 rowset 元数据的 LRU 缓存时出现死锁。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` 回滚不具备异常安全性，且解锁顺序不正确。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 由于只读路径和元数据路径上存在多个数据库锁，导致与 DDL 及 StarOS RPC 之间发生锁竞争。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- 由于缺少 project 节点导致 Shuffle 分布不正确。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN runtime filter `exprOrder` 不匹配，导致崩溃和结果错误。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- 由于 dict-merge GROUP BY 导致结果错误。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query Cache 与本地 Shuffle 聚合之间发生冲突。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- Flat JSON 中的全局字典生成不一致问题。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON 合并空值不一致问题。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 当声明了显式的键/值类型时，map 字面量中存在类型不匹配问题。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- 在 JOIN USING 转换器中，COALESCE 的子节点未转换为公共类型。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- 在使用全局变量进行 reduce-cast 后，VARCHAR 的长度未被保留。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- 在 MySQL 结果集中，VARBINARY 在嵌套类型内部编码错误。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 在小规模 LIMIT 上禁用聚合落盘时，存在 check-having-clause 问题。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日期解析前未去除引号，以及 PostgreSQL 日期/时间相关的 bug。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- 数据文件共享标志丢失，导致 vacuum 删除了仍被兄弟 split tablet 引用的文件。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- 在 split→compaction→merge 流程中的 tablet 合并正确性问题。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- 在 tablet split 过程中，跨发布事务日志的 num_rows/data_size 被夸大。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一发布批次中，先写入后 compaction 导致的 delvec 孤立条目问题。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- 通过同步 StarMgr journal 重放，修复了 follower FE 上出现的 "no queryable replica" 问题。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 在应用普通 rowset 提交时，`merge_condition` 未被保留。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE 冲突检测使用了错误的 snapshot ID 和过滤条件。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 当 Iceberg transform 参数无效时出现 NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- 由于优化器注入了额外的列，导致 Iceberg min/max 优化被跳过。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- 在 Iceberg 基表上进行聚合-关联下推（aggregate-join-pushdown）物化视图改写的问题。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- 在 INSERT OVERWRITE 提交之前缺少 Hive 分区目录。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNI 扫描器未应用 AWS assume-role。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- Avro 复杂类型解码在裁剪子字段和嵌套可空 schema 时存在问题。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Load 错误中缺少文件/列/行上下文信息。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet 扫描器缺乏对 Arrow dictionary 值的支持。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon 表的主键在 SHOW CREATE 和 DESC 的返回结果中未显示。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC 类型兼容性问题，以及末尾带斜杠的 JDBC URL 构造问题。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- 在 JDBC catalog 中针对 SQL Server 表的物化视图刷新问题。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 针对外连接上的物化视图，延迟物化 slot 可空性问题。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO 和 INCREMENTAL 物化视图分区刷新被拒绝的问题。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- 物化视图变为 inactive 后，物化视图调度器未停止。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 为了兼容 MySQL 客户端，缺少对 `SHOW GRANTS FOR CURRENT_USER()` 的支持。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- 在显式事务中不允许使用 SHOW 语句。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- 当结果集为空时，Arrow Flight 返回列名 `r`。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF 代码中缺少 JNI 异常处理检查。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 函数注册问题。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- 使用 `enable_load_profile` 时存在 Stream Load profile 采集问题。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- Profile 中的 START_TIME/END_TIME 未按会话时区显示。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` 不支持运行时修改。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` 在相等谓词中未对特殊字符进行转义。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- 在错误处理路径上并行加载 segment/rowset 时存在释放后使用（use-after-free）问题。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 聚合落盘 `set_finishing` 中潜在的哈希表数据丢失问题。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 磁盘重新迁移（A→B→A）过程中 GC 竞争导致的 PK tablet rowset 元数据丢失问题。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` 中的数据库读锁泄漏问题。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM 刷新记录中 PCT 分区元数据不完整。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 在 Stream Load/Broker Load 中分析生成列时，如果所引用的列缺失，会发生 NPE。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短路点查询中缺少分区谓词。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {#410}

发布日期：2026 年 4 月 13 日

### 存算分离架构 {#shared-data-architecture}

- **全新多租户数据管理**

  存算分离集群现已支持基于范围的数据分布，以及 tablet 的自动拆分与合并。当 tablet 过大或出现热点时，无需进行 schema change、SQL 修改或数据重新导入，即可自动拆分 tablet。此特性可显著提升易用性，直接解决多租户工作负载中的数据倾斜和热点问题。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 tablet 支持（第一阶段）**

  使存算分离集群能够支持每个 tablet 承载更多数据，长期目标为每个 tablet 达到 100 GB。第一阶段在整个导入、主键更新以及 Compaction 流程中引入了 tablet 内部并行能力，使单个 Lake tablet 在数据量增长时不再成为单线程瓶颈。改进内容包括：单个 tablet 内的并行 Compaction（支持 segment 级别拆分）、Lake 导入场景下 MemTable 的并行终结、刷写与合并（包括导入落盘路径）、tablet 内部的并行发布以及主键表的并行条件更新，以及针对 cloud 原生主键索引的范围拆分 / 并行 / 分层 Compaction（支持远端存储映射文件）。这些改进共同大幅降低了大 tablet 工作负载下的导入内存开销、Compaction 放大以及 FE 元数据压力。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **快速 schema change V2**

  存算分离集群现已支持快速 schema change V2，可实现 schema 操作的秒级 DDL 执行，并进一步扩展支持物化视图。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 存算分离模式下的倒排索引**

  为存算分离集群启用内置倒排索引，以加速文本过滤和全文检索工作负载。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **缓存可观测性**

  查询级别的缓存命中率现已在审计日志和监控系统中展示，以提升缓存透明度并便于延迟诊断。其他 Data Cache 指标包括内存和磁盘配额使用情况，以及页面缓存统计信息。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- 为 Lake 表新增 segment 元数据过滤功能，可在扫描过程中根据排序键范围跳过无关 segment，从而降低范围谓词查询的 I/O 开销。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- 支持 Lake DeltaWriter 的快速取消，降低存算分离集群中被取消导入作业的延迟。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 新增支持基于时间间隔的自动集群快照调度。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- 支持 MemTable 刷写与合并的流水线执行，提升存算分离集群中 cloud 原生表的导入吞吐量。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- 支持以 `dry_run` 模式修复 cloud 原生表，允许用户在执行前预览修复操作。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 为存算一体集群中的事务发布新增线程池，提升发布吞吐量。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### 数据湖分析 {#data-lake-analytics}

- **Iceberg DELETE 支持**

  支持为 Iceberg 表写入 position delete 文件，从而可直接在 StarRocks 中对 Iceberg 表执行 DELETE 操作。该支持涵盖了 Plan、Sink、Commit 和 Audit 的完整流程。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **针对 Hive 和 Iceberg 表的 TRUNCATE**

  支持对外部 Hive 和 Iceberg 表执行 TRUNCATE TABLE。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上的增量物化视图**

  将增量物化视图刷新的支持范围扩展到 Iceberg 仅追加（append-only）表，无需全表刷新即可实现查询加速。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **用于 Iceberg 中半结构化数据的 VARIANT 类型**

  支持在 Iceberg Catalog 中使用 VARIANT 数据类型，实现对半结构化数据灵活的读时模式（schema-on-read）存储与查询。支持读取、写入、类型转换以及 Parquet 集成。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 支持**

  新增支持 Iceberg v3 默认值特性和行血缘（row lineage）。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg 表维护过程**

  新增支持 `rewrite_manifests` 过程，并为 `expire_snapshots` 和 `remove_orphan_files` 过程扩展了更多参数，实现更细粒度的表维护。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- 支持从 Iceberg 表读取文件路径和行位置元数据列。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- 支持从 Iceberg v3 表读取 `_row_id`，并支持 Iceberg v3 的全局延迟物化。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- 支持创建带有自定义属性的 Iceberg 视图，并在 SHOW CREATE VIEW 输出中展示属性。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 支持按指定分支、标签、版本或时间戳查询 Paimon 表。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- 支持 Paimon 表的复杂类型（ARRAY、MAP、STRUCT）。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- 创建 Iceberg 表时支持使用括号语法进行 Partition Transforms。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- 支持基于 Transform Partition 的 Iceberg global shuffle，以优化数据组织。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- 支持为 Iceberg 表 sink 动态启用 global shuffle。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- 为 Iceberg 表 sink 引入了 Commit 队列，以避免并发 Commit 冲突。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- 为 Iceberg 表 sink 添加了主机级别排序，以提升数据组织和读取性能。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- 默认在 ETL 执行模式下启用了更多优化，无需显式配置即可提升 INSERT INTO SELECT、CREATE TABLE AS SELECT 及类似批量操作的性能。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- 为 Iceberg 表上的 INSERT 和 DELETE 操作添加了 Commit 审计信息。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- 支持在 Iceberg REST Catalog 中启用或禁用 view 端点操作。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- 优化了 CachingIcebergCatalog 中的缓存查找效率。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- 支持对多种 Iceberg catalog 类型执行 EXPLAIN。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- 支持 AWS Glue Catalog 表的分区投影。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- 为 AWS Glue `GetDatabases` API 添加了资源共享类型支持。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- 支持通过端点注入（`azblob`/`adls2`）实现 Azure ABFS/WASB 路径映射。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- 为 JDBC catalog 添加了数据库元数据缓存，以减少远程 RPC 开销以及外部系统故障带来的影响。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- `information_schema` 支持 PostgreSQL 表的列注释。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- 改进了 Oracle 和 PostgreSQL 的 JDBC 类型映射。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### 查询引擎 {#query-engine}

- **Recursive CTE**

  支持 Recursive Common Table Expressions（CTE），用于层级遍历、图查询和迭代 SQL 计算。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 改进了 Skew Join v2 重写逻辑，基于统计信息进行倾斜检测，支持直方图，并具备 NULL 倾斜感知能力。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- 改进了窗口上的 COUNT DISTINCT，并新增了对融合多重去重聚合的支持。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- 支持为窗口函数显式指定 skew hint，可自动针对分区键存在倾斜的窗口函数进行拆分优化为 UNION。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- 在 Trino Parser 中支持对 INSERT 语句使用 EXPLAIN 及 EXPLAIN ANALYZE。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- 支持 EXPLAIN 以查看查询队列信息。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 函数和 SQL 语法 {#functions-and-sql-syntax}

- 新增以下函数：
  - `array_top_n`：按值排序，返回数组中排名前 N 的元素。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：按元素位置将多个数组合并为一个结构体数组。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：对 JSON 字符串进行带缩进的格式化。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：在 JSON 字符串中的指定路径设置值。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：将每个单词的首字母转换为大写。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：对具有相同 key 的行中的 MAP 值求和。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：返回当前会话时区。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：返回当前 warehouse 的名称。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：将秒数转换为 TIME 值。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：从 SQL 中调用外部 AI 模型，用于推理工作负载。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：在 SQL 表达式中抛出用户自定义错误。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 提供以下函数或语法扩展：
  - 支持在 `array_sort` 中使用 lambda 比较器以实现自定义排序顺序。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - 支持为 FULL OUTER JOIN 添加 USING 子句，具备 SQL 标准语义。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - 支持带 ORDER BY/PARTITION BY 的窗口函数帧内 DISTINCT 聚合。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - 支持在 `lead`/`lag`/`first_value`/`last_value` 窗口函数中使用 ARRAY 类型。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 为类似 count distinct 的聚合函数支持 VARBINARY。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - 支持在 IN 表达式中进行日期和字符串类型转换。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - 为 BEGIN/START TRANSACTION 支持 WITH LABEL 语法。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - 在 SHOW 语句中支持 WHERE/ORDER/LIMIT 子句。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - 支持使用 `ALTER TASK` 语句进行任务管理。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - 支持 CSV 文件导出使用多种压缩格式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 支持 `STRUCT_CAST_BY_NAME` SQL 模式，用于基于名称的结构体字段匹配。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理与可观测性 {#management--observability}

- 支持为资源组设置 `warehouses`、`cpu_weight_percent` 和 `exclusive_cpu_weight` 属性，以改进多 Warehouse CPU 资源隔离。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- 引入 `information_schema.fe_threads` 系统视图，用于查看 FE 线程状态。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- 支持 SQL Digest Blacklist，以在集群级别阻止特定的查询模式。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- 支持从因网络拓扑限制而无法直接访问的节点进行 Arrow Flight 数据检索。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 引入 REFRESH CONNECTIONS 命令，无需重新连接即可将全局变量更改传播到现有连接。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- 新增内置 UI 功能，用于分析查询 Profile 并查看格式化后的 SQL，使查询调优更加便捷。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 实现 `ClusterSummaryActionV2` API 接口，以提供结构化的集群概览。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 新增全局只读系统变量 `@@run_mode`，用于查询当前集群运行模式（存算分离或存算一体）。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- 默认启用 `query_queue_v2`，以改进查询排队管理。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- 支持为 Stream Load 和 Merge Commit 操作设置用户级默认 Warehouse。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 新增 `skip_black_list` 会话变量，可在需要时绕过后端黑名单验证。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- 为指标 API 新增 `enable_table_metrics_collect` 选项。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- 为查询详情 HTTP API 新增模拟用户（impersonate）支持。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- 新增 `table_query_timeout` 作为表级属性。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 支持添加 FE Observer 节点。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- 支持在 `information_schema.loads` 中展示 Merge Commit 信息，以提升导入作业的可见性。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- 支持在 cloud 内表中显示 tablet 状态，便于故障排查。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### 安全 {#security-7}

- [CVE-2026-33870] [CVE-2026-33871] 替换了 AWS bundle 并将 Netty 升级到 4.1.132.Final。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] 将 Hadoop 升级到 v3.4.2。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] 将 `spark-core_2.12` 升级到 3.5.7。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### Bug 修复 {#bug-fixes-7}

修复了以下问题：

- 通过跳过 range 分布 tablet 的数据文件删除，修复了 tablet 拆分后的数据丢失问题。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 修复了 `DefaultValueColumnIterator` 在处理复杂类型时的内存泄漏问题。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- 修复了由 `BatchUnit` 和 `FetchTaskContext` 之间的 `shared_ptr` 循环引用导致的内存泄漏问题。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 修复了 SystemMetrics 中因并发 getline 访问导致的双重释放崩溃问题。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- 修复了 SpillMemTableSink 在 eager merge 消费 all 块时发生崩溃的问题。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 修复了自动创建的分区被 TTL 清理器删除时出现 NPE 的问题。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- 修复了快照过期时 `IcebergCatalog.getPartitionLastUpdatedTime` 中出现 NPE 的问题。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 修复了常量侧列引用的外连接谓词重写错误问题。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 修复了在存算分离模式下修改 CHAR 列长度后查询结果错误的问题。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 修复了多表场景下物化视图刷新的 bug。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 修复了强制刷新时物化视图回收时间不正确的问题。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 修复了同步物化视图中 all 空值处理的 bug。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 修复了在快速 schema change ADD COLUMN 之后查询物化视图时出现重复列 id 错误的问题。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 修复了由共享 DecodeInfo 导致的低基数重写 NPE 问题。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 修复了低基数 JOIN 谓词类型不匹配的问题。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- 修复了当 `null_counts` 为空时，Parquet Page Index Filter 出现 Segfault 的问题。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 修复了 JSON 在相同路径上展开数组和对象时发生冲突的问题。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- 修复了 Iceberg 缓存权重计算不准确的问题。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 修复了 Iceberg 表缓存内存限制的问题。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- 修复了 Iceberg 删除列可空性的问题。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- 修复了 Azure ABFS/WASB FileSystem 缓存键未包含容器信息的问题。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 修复了 HMS 连接池已满时发生死锁的问题。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- 修复了 Paimon Catalog 中 VARCHAR 字段类型长度不正确的问题。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- 修复了 Paimon catalog 刷新时因 ObjectTable 上的 ClassCastException 导致崩溃的问题。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 修复了 FULL OUTER JOIN USING 在常量子查询下的问题。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 修复了在 CTE 作用域下 JOIN ON 子句的 bug。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- 通过使用 bindScope() 模式修复了 ConnectContext 的内存泄漏问题。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 修复了存算一体集群中 `CatalogRecycleBin.asyncDeleteForTables` 的内存泄漏问题。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 修复了 Thrift accept 线程在遇到任何异常时退出的问题。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- 修复了 Routine Load 列映射中 UDF 解析的问题。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- 修复了 `DROP FUNCTION IF EXISTS` 忽略 `ifExists` 标志的问题。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 修复了字典页过大时扫描结果出错的问题。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 修复了 Range 分区重叠的问题。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- 修复了查询队列分配时间和等待超时的问题。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- 修复了处理空字面量数组时 `array_map` 崩溃的问题。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 修复了 `to_base64` 栈溢出的问题。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- 修复了 LDAP 认证时用户名大小写不敏感规范化的问题。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- 降低了 API `proc_file` 的 SSRF 风险。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 在审计和 SQL 脱敏中屏蔽了用户认证字符串。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 行为变更 {#behavior-changes-6}

- ETL 执行模式优化现已默认启用。这将使 INSERT INTO SELECT、CREATE TABLE AS SELECT 以及类似批处理工作负载受益，无需显式更改配置。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` 窗口函数的第三个参数现在除了支持常量值外，还支持列引用。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING 现在遵循 SQL 标准语义：USING 列在输出中只出现一次，而不是两次。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` 现已默认启用。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL 事务默认由会话变量 `enable_sql_transaction` 控制开关。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
- 此前，FE 配置项 `max_scalar_operator_flat_children`（默认值 `10000`）仅限制由 `CASE WHEN` 构建的表达式的节点数量。现在它会限制优化器重写的每一个标量表达式，并且节点数量会在每次重写后重新计算，而不是复用缓存值。因此，升级后某些之前可以正常运行的查询可能会在分析阶段因 `Expression too complex. Current nodes: N, Limit: M.` 而被拒绝，这通常发生在内联视图、CTE 或子查询扩展其表达式树时。请简化表达式，或增大 `max_scalar_operator_flat_children`（该参数可动态修改）。[#66324](https://github.com/StarRocks/starrocks/pull/66324)
