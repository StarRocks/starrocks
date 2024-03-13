---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.1

## 3.1.9

发布日期：2024 年 3 月 8 日

### 新增特性

- 存算分离集群中的云原生主键表支持 Size-tiered 模式 Compaction，以减轻导入较多小文件时 Compaction 的写放大问题。[#41610](https://github.com/StarRocks/starrocks/pull/41610)
- 新增函数 `regexp_extract_all`。[#42178](https://github.com/StarRocks/starrocks/pull/42178)
- 新增 `information_schema.partitions_meta` 视图，提供丰富的 PARTITION 元信息。[#41101](https://github.com/StarRocks/starrocks/pull/41101)
- 新增 `sys.fe_memory_usage` 视图，提供 StarRocks 的内存使用信息。[#41083](https://github.com/StarRocks/starrocks/pull/41083)

### 行为变更

- 动态分区语法修改为“如果分区列是日期（DATE）类型，则不支持小时（hour）时间粒度”。注意，时间日期（DATETIME）类型的分区列还是支持小时时间粒度的。[#40328](https://github.com/StarRocks/starrocks/pull/40328)
- 修改发起物化视图刷新任务的用户，从原本的 `root` 用户变成创建物化视图的用户，已有的物化视图不受影响。[#40698](https://github.com/StarRocks/starrocks/pull/40698)
- 常量和字符串类型的列进行比较时，默认按字符串进行比较，用户可以通过设置变量 `cbo_eq_base_type` 来调整默认行为。将 `cbo_eq_base_type` 设置为 `decimal` 可以改为按数值进行比较。[#41712](https://github.com/StarRocks/starrocks/pull/41712)

### 功能优化

- 支持通过 `s3_compatible_fs_list` 参数设置可以使用 AWS SDK 接入的 S3 兼容对象存储。同时支持通过 `fallback_to_hadoop_fs_list` 参数配置需要通过 HDFS 的 Schema 接入的非 S3 兼容对象存储（该方法需要使用厂商提供的 JAR 包）。[#41612](https://github.com/StarRocks/starrocks/pull/41612)
- 优化 Trino 语法兼容性，支持 Trino 的 `current_catalog`、`current_schema`、`to_char`、`from_hex`、`to_date`、`to_timestamp` 以及 `index` 函数的语法转换。[#41505](https://github.com/StarRocks/starrocks/pull/41505) [#41270](https://github.com/StarRocks/starrocks/pull/41270) [#40838](https://github.com/StarRocks/starrocks/pull/40838)
- 为缓解由于候选物化视图太多而导致查询在 Plan 阶段耗时过高的问题，引入了一个新的会话变量 `cbo_materialized_view_rewrite_related_mvs_limit`，用于控制查询在 Plan 阶段最多拥有的候选物化视图个数，默认是 64 个。[#39829](https://github.com/StarRocks/starrocks/pull/39829)
- 聚合表中 BITMAP 类型的列支持指定聚合类型为 `replace_if_not_null`，从而支持部分列更新。[#42102](https://github.com/StarRocks/starrocks/pull/42102)
- 通过修改会话变量 `cbo_eq_base_type`，控制字符串与数值比较时隐式转换规则，默认按字符串进行比较。[#40619](https://github.com/StarRocks/starrocks/pull/41712)
- 内部支持更多日期类型识别（如 "%Y-%m-%e %H:%i"），从而更好地支持 Iceberg 分区表达。[#40474](https://github.com/StarRocks/starrocks/pull/40474)
- JDBC Connector 支持 TIME 数据类型。[#31940](https://github.com/StarRocks/starrocks/pull/31940)
- File External Table 的 `path` 参数支持使用通配符（`*`），但是和 Broker Load 中 `DATA INFILE` 参数一样最多只能匹配一层目录或文件。[#40844](https://github.com/StarRocks/starrocks/pull/40844)
- 新增内部 SQL 日志，其中包含统计信息和物化视图等相关的日志信息。[#40682](https://github.com/StarRocks/starrocks/pull/40682)

### 问题修复

修复了如下问题：

- 当创建 Hive 视图的查询语句中存在同一个表或视图的名称（或别名）大小写不一致的情况时，会出现“Analyze Error”的问题。[#40921](https://github.com/StarRocks/starrocks/pull/40921)
- 主键表使用持久化索引会导致磁盘 I/O 打满。[#39959](https://github.com/StarRocks/starrocks/pull/39959)
- 存算分离集群中，主键索引目录每 5 小时会被错误删除。[#40745](https://github.com/StarRocks/starrocks/pull/40745)
- List 分区表在 Truncate Table 或 Truncate Partition 之后根据分区键查询不到数据。[#40495](https://github.com/StarRocks/starrocks/pull/40495)
- 手动执行 ALTER TABLE COMPACT 后，Compaction 内存统计有异常。[#41150](https://github.com/StarRocks/starrocks/pull/41150)
- 跨集群数据迁移中，如果执行列模式的部分列更新，目标集群可能会 Crash。[#40692](https://github.com/StarRocks/starrocks/pull/40692)
- 当某些 SQL 语句中包含多个空格、换行符时，SQL 黑名单会不生效。[#40457](https://github.com/StarRocks/starrocks/pull/40457)

## 3.1.8

发布日期：2024 年 2 月 5 日

### 新增特性

- 社区提供 StarRocks 数据迁移工具，支持将数据从存算一体集群迁移数据到存算一体集群或者存算分离集群。
- 支持创建带有 WHERE 子句的同步物化视图。
- MemTracker 中新增了 Data Cache 内存使用的相关指标。[#39600](https://github.com/StarRocks/starrocks/pull/39600)
- 新增函数 `array_unique_agg`。

### 参数变更

- 增加 BE 配置项 `lake_pk_compaction_max_input_rowsets`，用于控制存算分离集群下主键表 Compaction 任务中允许的最大输入 Rowset 数量，优化 Compaction 时资源的使用。[#39611](https://github.com/StarRocks/starrocks/pull/39611)

### 功能优化

- 通过 CTAS 建表时支持配置 ORDER BY 和 INDEX。[#38886](https://github.com/StarRocks/starrocks/pull/38886)
- 新增支持 ORC 格式 Iceberg v2 表的 Equality Delete 查询。[#37419](https://github.com/StarRocks/starrocks/pull/37419)
- 云原生表开启 List 分区表后可以设置 `datacache.partition_duration` 属性且支持动态修改，用于控制 Data Cache 的有效时间。[#35681](https://github.com/StarRocks/starrocks/pull/35681) [#38509](https://github.com/StarRocks/starrocks/pull/38509)
- 优化主键模型表索引的 Major Compaction 的策略，可通过 BE 配置项 `update_compaction_per_tablet_min_interval_seconds` 控制主键模型表索引的 Major Compaction的频率（该参数同时也控制主键模型表 Compaction 的频率）。[#39640](https://github.com/StarRocks/starrocks/pull/39640)
- Parquet Reader 支持将 Parquet 文件中 INT32 类型的数据转换成 DATETIME 类型的数据并保存到 StarRocks中。[#39808](https://github.com/StarRocks/starrocks/pull/39808)

### 问题修复

修复了如下问题：

- 使用非数列（NaN 列）进行排序可能导致 BE Crash。[#30759](https://github.com/StarRocks/starrocks/pull/30759)
- 更新主键索引失败可能导致 "get_applied_rowsets failed"。[#27488](https://github.com/StarRocks/starrocks/pull/27488)
- Compaction 失败后，compaction_state_cache 没有回收仍然占用资源。[#38499](https://github.com/StarRocks/starrocks/pull/38499)
- 如果外表的分区列有取值是 null，查询时会导致 BE Crash。[#38888](https://github.com/StarRocks/starrocks/pull/38888)
- 当基表删除重建后，异步物化视图刷新失败。[#38008](https://github.com/StarRocks/starrocks/pull/38008)
- 如果 Iceberg 表是空表，异步物化视图刷新失败。[#24068](https://starrocks.atlassian.net/browse/SR-24068)

## 3.1.7

发布日期：2024 年 1 月 12日

### 新增特性

- 新增表函数 `unnest_bitmap`。[#38136](https://github.com/StarRocks/starrocks/pull/38136)
- [Broker Load](https://docs.starrocks.io/zh/docs/3.1/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/#opt_properties) 支持条件更新。[#37400](https://github.com/StarRocks/starrocks/pull/37400)

### 行为变更

- 新增 Session 变量 `enable_materialized_view_for_insert`，默认值为 `FALSE`，表示物化视图不再改写 INSERT INTO SELECT 语句中的查询。[#37505](https://github.com/StarRocks/starrocks/pull/37505)
- 将 FE 配置项 `enable_new_publish_mechanism` 改为静态参数，修改后必须重启 FE 才可以生效。[#35338](https://github.com/StarRocks/starrocks/pull/35338)
- 新增 Session 变量 `enable_strict_order_by`。当取值为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。[#37910](https://github.com/StarRocks/starrocks/pull/37910)

### 参数变更

- 增加 FE 配置项 `routine_load_unstable_threshold_second`。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- 新增 FE 配置项 `http_worker_threads_num`，指定 HTTP Server 用于处理 HTTP 请求的线程数。默认取值为 `0`。如果配置为负数或 `0` ，线程数将设置为 CPU 核数的 2 倍。[#37530](https://github.com/StarRocks/starrocks/pull/37530)
- 新增 BE 配置项 `pindex_major_compaction_limit_per_disk`，配置每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题，默认取值为 `1`。[#36681](https://github.com/StarRocks/starrocks/pull/36681)
- 新增 Session 变量 `transaction_read_only` 和 `tx_read_only`，设置事务的访问模式并且兼容 MySQL 5.7.20 以上的版本。[#37249](https://github.com/StarRocks/starrocks/pull/37249)
- 新增 FE 配置项 `default_mv_refresh_immediate`，用于控制物化视图创建完成后是否立刻进行刷新，默认值为 `true`。[#37093](https://github.com/StarRocks/starrocks/pull/37093)
- 新增 BE 配置项 `lake_enable_vertical_compaction_fill_data_cache`，表示存算分离模式下是否允许 Compaction 任务在执行时缓存数据到本地磁盘上，默认取值是 `false`。[#37296](https://github.com/StarRocks/starrocks/pull/37296)

### 功能优化

- INSERT INTO FILE() SELECT FROM 支持读取表中 BINARY 类型的数据并导出至远端存储中的 Parquet 格式文件中。[#36797](https://github.com/StarRocks/starrocks/pull/36797)
- 异步物化视图可以设置 `datacache.partition_duration` 属性且支持动态修改，用于控制 Data Cache 的有效时间。[#35681](https://github.com/StarRocks/starrocks/pull/35681)
- 使用 JDK 时 GC 算法默认采用 G1。[#37386](https://github.com/StarRocks/starrocks/pull/37386)
- `date_trunc`、`adddate`、`time_slice` 函数的 `interval` 参数可支持毫秒和微秒。[#36386](https://github.com/StarRocks/starrocks/pull/36386)
- WHERE 子句中 LIKE 运算符右侧字符串中不包括 `%` 或者 `_` 时，LIKE 运算符会转换成 `=` 运算符。[#37515](https://github.com/StarRocks/starrocks/pull/37515)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/3.1/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加 `LatestSourcePosition`，记录 数据源 Kafka 中 Topic 内各个分区的最新消息位点，便于检查导入延迟情况。[#38298](https://github.com/StarRocks/starrocks/pull/38298)
- 新增资源组属性 `spill_mem_limit_threshold`，用于设置开启自动落盘（即系统变量 `spill_mode` 设置为 `auto`）时，当前资源组触发落盘的内存占用阈值（百分比）。取值范围：(0,1)。默认取值为 `1`，表示不生效。[#36701](https://github.com/StarRocks/starrocks/pull/36701)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/3.1/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加时间戳进度信息，展示各个分区当前消费消息的时间戳。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- 优化 Rountine Load 的调度策略，慢任务不阻塞其他正常任务的执行。[#37638](https://github.com/StarRocks/starrocks/pull/37638)

### 问题修复

修复了如下问题：

- [ANALYZE TABLE](https://docs.starrocks.io/zh/docs/3.1/sql-reference/sql-statements/data-definition/ANALYZE_TABLE/) 偶尔会出现卡住的问题。[#36836](https://github.com/StarRocks/starrocks/pull/36836)
- PageCache 内存占用在有些情况下会超过 BE 动态参数 `storage_page_cache_limit` 设定的阈值。[#37740](https://github.com/StarRocks/starrocks/pull/37740)
- [Hive Catalog](https://docs.starrocks.io/zh/docs/3.1/data_source/catalog/hive_catalog/) 的元数据在 Hive 表新增字段后不会自动刷新。[#37668](https://github.com/StarRocks/starrocks/pull/37668)
- 某些情况下 `bitmap_to_string` 会因为转换时数据类型溢出导致查询结果错误。[#37405](https://github.com/StarRocks/starrocks/pull/37405)
- 空表执行 DELETE 报错“ERROR 1064 (HY000): Index: 0, Size: 0”。[#37461](https://github.com/StarRocks/starrocks/pull/37461)
- 当 FE 动态参数 `enable_sync_publish` 设置为 `TRUE` 时，BE Crash 重启后写入数据可能会出现无法查询的情况。[#37398](https://github.com/StarRocks/starrocks/pull/37398)
- Information Schema 里的视图 `views` 中 `TABLE_CATALOG` 字段取值是 `null`。[#37570](https://github.com/StarRocks/starrocks/pull/37570)
- `SELECT ... FROM ... INTO OUTFILE` 导出至 CSV 时，如果 FROM 子句中包含多个常量，执行时会报错："Unmatched number of columns"。[#38045](https://github.com/StarRocks/starrocks/pull/38045)

## 3.1.6

发布日期：2023 年 12 月 18 日

### 新增特性

- 增加 [now(p)](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/now/) 函数用于获取指定秒精度的时间 (最高到毫秒)。如果不指定 `p`，now() 函数仍然只返回秒级精度的时间。[#36676](https://github.com/StarRocks/starrocks/pull/36676)
- 新增了监控指标 `max_tablet_rowset_num`（用于设置 Rowset 的最大数量），可以协助提前发现 Compaction 是否会出问题并及时干预，减少报错信息“too many versions”的出现。[#36539](https://github.com/StarRocks/starrocks/pull/36539)
- 支持使用命令行获取 Heap Profile，便于发生问题后进行定位分析。[#35322](https://github.com/StarRocks/starrocks/pull/35322)
- 支持使用 Common Table Expression (CTE) 创建异步物化视图。[#36142](https://github.com/StarRocks/starrocks/pull/36142)
- 新增如下 Bitmap 函数：[subdivide_bitmap](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/)、[bitmap_from_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/)、[bitmap_to_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。[#35817](https://github.com/StarRocks/starrocks/pull/35817) [#35621](https://github.com/StarRocks/starrocks/pull/35621)
- 优化主键表 Compaction Score 的取值逻辑，使其和其他类型的表的取值范围看起来更一致。[#36534](https://github.com/StarRocks/starrocks/pull/36534)

### 参数变更

- 调整 Trash 文件的默认过期时间为 1 天（原来是 3 天）。[#37113](https://github.com/StarRocks/starrocks/pull/37113)
- 新增 BE 配置项 `enable_stream_load_verbose_log`，默认取值是 `false`，打开后日志中可以记录 Stream Load 的 HTTP 请求和响应信息，方便出现问题后的定位调试。[#36113](https://github.com/StarRocks/starrocks/pull/36113)
- 新增 BE 配置项 `enable_lazy_delta_column_compaction`，默认取值是 `true`，表示不启用频繁的进行 delta column 的 Compaction。[#36654](https://github.com/StarRocks/starrocks/pull/36654)
- 新增 FE 配置项 `enable_mv_automatic_active_check`，用于指定是否允许系统自动检查和重新激活异步物化视图，默认取值是 `true`。[#36463](https://github.com/StarRocks/starrocks/pull/36463)

### 功能优化

- 系统变量 [sql_mode](https://docs.starrocks.io/zh/docs/reference/System_variable/#sql_mode) 增加 `GROUP_CONCAT_LEGACY` 选项，用以兼容 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 函数在 2.5（不含）版本之前的实现逻辑。[#36150](https://github.com/StarRocks/starrocks/pull/36150)
- 主键表 [SHOW DATA](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_DATA/) 的结果中新增包括 **.cols** 文件（部分列更新和生成列相关的文件）和持久化索引文件的文件大小信息。[#34898](https://github.com/StarRocks/starrocks/pull/34898)
- 支持 MySQL 外部表和 JDBC Catalog 外部表的 WHERE 子句中包含关键字。[#35917](https://github.com/StarRocks/starrocks/pull/35917)
- 加载 StarRocks 的插件失败时，不再抛出异常并导致 FE 无法启动，而是 FE 可正常启动、同时可以通过 [SHOW PLUGINS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/SHOW_PLUGINS/) 查看插件的错误状态。[#36566](https://github.com/StarRocks/starrocks/pull/36566)
- 动态分区支持 Random 分布。[#35513](https://github.com/StarRocks/starrocks/pull/35513)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加 `OtherMsg`，展示最后一个失败的任务的相关信息。[#35806](https://github.com/StarRocks/starrocks/pull/35806)
- 隐藏了审计日志（Audit Log）中 Broker Load 作业里 AWS S3 的鉴权信息 `aws.s3.access_key` 和 `aws.s3.access_secret`。[#36571](https://github.com/StarRocks/starrocks/pull/36571)
- 在 `be_tablets` 表中增加 `INDEX_DISK` 记录持久化索引的磁盘使用量，单位是 Bytes。[#35615](https://github.com/StarRocks/starrocks/pull/35615)

### 问题修复

修复了如下问题：

- 数据损坏情况下，建立持久化索引会引起 BE Crash。[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- 创建包含嵌套查询的异步物化视图时会报错“resolve partition column failed”。[#26078](https://github.com/StarRocks/starrocks/issues/26078)
- 创建异步物化视图时如果基表数据损坏，可能会报错“Unexpected exception: null”。[#30038](https://github.com/StarRocks/starrocks/pull/30038)
- 查询包含窗口函数时会 SQL 错误“[1064] [42000]: Row count of const column reach limit: 4294967296”。[#33561](https://github.com/StarRocks/starrocks/pull/33561)
- 开启 FE 配置项 `enable_collect_query_detail_info` 后，FE 性能下降严重。[#35945](https://github.com/StarRocks/starrocks/pull/35945)
- 存算分离模式下，删除对象存储中文件时可能报错“Reduce your request rate”。[#35566](https://github.com/StarRocks/starrocks/pull/35566)
- 某些情况下，物化视图刷新可能会出现死锁问题。[#35736](https://github.com/StarRocks/starrocks/pull/35736)
- 启用 DISTINCT 下推窗口算子功能时,  对窗口函数的输出列的复杂表达式进行 SELECT DISTINCT 操作会报错。[#36357](https://github.com/StarRocks/starrocks/pull/36357)
- 从 ORC 格式数据文件导入嵌套的 Array 会导致 BE Crash。[#36127](https://github.com/StarRocks/starrocks/pull/36127)
- 某些兼容 S3 协议的对象存储会返回重复的文件，导致 BE Crash。[#36103](https://github.com/StarRocks/starrocks/pull/36103)
- ARRAY_DISTINCT 函数偶发 BE Crash。[#36377](https://github.com/StarRocks/starrocks/pull/36377)
- 特定场景下 Global Runtime Filter 可能会引发 BE crash。[#35776](https://github.com/StarRocks/starrocks/pull/35776)

## 3.1.5

发布日期：2023 年 11 月 28 日

### 新增特性

- 存算分离模式下 CN 节点支持数据导出。[#34018](https://github.com/StarRocks/starrocks/pull/34018)

### 功能优化

- [`INFORMATION_SCHEMA.COLUMNS`](https://docs.starrocks.io/zh/docs/reference/information_schema/columns/) 表支持显示 ARRAY、MAP、STRUCT 类型的字段。 [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- 支持查询 [Hive](https://docs.starrocks.io/zh/docs/data_source/catalog/hive_catalog/) 中使用 LZO 算法压缩的 Parquet、ORC、和 CSV 格式的文件。[#30923](https://github.com/StarRocks/starrocks/pull/30923)  [#30721](https://github.com/StarRocks/starrocks/pull/30721)
- 如果是自动分区表，也支持指定分区名进行更新，如果分区不存在则报错。[#34777](https://github.com/StarRocks/starrocks/pull/34777)
- 当创建物化视图涉及的表、视图及视图内涉及的表、物化视图发生 Swap、Drop 或者 Schema Change 操作后，物化视图可以进行自动刷新。[#32829](https://github.com/StarRocks/starrocks/pull/32829)
- 优化 Bitmap 相关的某些操作的性能，主要包括：
  - 优化 Nested Loop Join 性能。[#340804](https://github.com/StarRocks/starrocks/pull/34804)  [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - 优化 `bitmap_xor` 函数性能。[#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - 支持 Copy on Write（简称 COW），优化性能，并减少内存使用。[#34047](https://github.com/StarRocks/starrocks/pull/34047)

### 问题修复

修复了如下问题：

- 如果提交的 Broker Load 作业包含过滤条件，在数据导入过程中，某些情况下会出现 BE Crash。[#29832](https://github.com/StarRocks/starrocks/pull/29832)
- SHOW GRANTS 时报 `unknown error`。[#30100](https://github.com/StarRocks/starrocks/pull/30100)
- 如果使用表达式作为自动分区列，导入数据时可能会报错 "Error: The row create partition failed since Runtime error: failed to analyse partition value"。[#33513](https://github.com/StarRocks/starrocks/pull/33513)
- 查询时报错 "get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction"。[#33246](https://github.com/StarRocks/starrocks/pull/33246)
- 存算一体模式下，单独查询 Iceberg 或者 Hive 外表容易出现 BE crash。[#34682](https://github.com/StarRocks/starrocks/pull/34682)
- 存算一体模式下，导入数据时同时自动创建多个分区，偶尔会出现数据写错分区的情况。[#34731](https://github.com/StarRocks/starrocks/pull/34731)
- 长时间向持久化索引打开的主键表高频导入，可能会引起 BE crash。[#33220](https://github.com/StarRocks/starrocks/pull/33220)
- 查询时报错 "Exception: java.lang.IllegalStateException: null"。[#33535](https://github.com/StarRocks/starrocks/pull/33535)
- 执行 `show proc '/current_queries';` 时，如果某个查询刚开始执行， 可能会引起 BE Crash。[#34316](https://github.com/StarRocks/starrocks/pull/34316)
- 向打开持久化索引的主键表中导入大量数据，有时会报错。[#34352](https://github.com/StarRocks/starrocks/pull/34352)
- 2.4 及以下的版本升级到高版本，可能会出现 Compaction Score 很高的问题。[#34618](https://github.com/StarRocks/starrocks/pull/34618)
- 使用 MariaDB ODBC Driver 查询 `INFORMATION_SCHEMA` 中的信息时，`schemata` 视图中 `CATALOG_NAME` 列中取值都显示的是 `null`。[#34627](https://github.com/StarRocks/starrocks/pull/34627)
- 导入数据异常导致 FE Crash 后无法重启。[#34590](https://github.com/StarRocks/starrocks/pull/34590)
- Stream Load 导入作业在 **PREPARED** 状态下、同时有 Schema Change 在执行，会导致数据丢失。[#34381](https://github.com/StarRocks/starrocks/pull/34381)
- 如果 HDFS 路径以两个或以上斜杠（`/`）结尾，HDFS 备份恢复会失败。[#34601](https://github.com/StarRocks/starrocks/pull/34601)
- 打开 `enable_load_profile` 后，Stream Load 会很容易失败。[#34544](https://github.com/StarRocks/starrocks/pull/34544)
- 使用列模式进行主键表部分列更新后，会有 Tablet 出现副本之间数据不一致。[#34555](https://github.com/StarRocks/starrocks/pull/34555)
- 使用 ALTER TABLE 增加 `partition_live_number` 属性没有生效。[#34842](https://github.com/StarRocks/starrocks/pull/34842)
- FE 启动失败，报错 "failed to load journal type 118"。[#34590](https://github.com/StarRocks/starrocks/pull/34590)
- 当 `recover_with_empty_tablet` 设置为 `true` 时可能会引起 FE Crash。[#33071](https://github.com/StarRocks/starrocks/pull/33071)
- 副本操作重放失败可能会引起 FE Crash。[#32295](https://github.com/StarRocks/starrocks/pull/32295)

### 参数变更

#### 配置项

- 新增 FE 配置项 [`enable_statistics_collect_profile`](https://docs.starrocks.io/zh/docs/administration/FE_configuration#enable_statistics_collect_profile) 用于控制统计信息查询时是否生成 Profile，默认值是 `false`。[#33815](https://github.com/StarRocks/starrocks/pull/33815)
- FE 配置项 [`mysql_server_version`](https://docs.starrocks.io/zh/docs/administration/FE_configuration#mysql_server_version) 从静态变为动态（`mutable`），修改配置项设置后，无需重启 FE 即可在当前会话动态生效。[#34033](https://github.com/StarRocks/starrocks/pull/34033)
- 新增 BE/CN 配置项 [`update_compaction_ratio_threshold`](https://docs.starrocks.io/zh/docs/administration/BE_configuration#update_compaction_ratio_threshold)，用于手动设置存算分离模式下主键表单次 Compaction 合并的最大数据比例，默认值是 `0.5`。如果单个 Tablet 过大，建议适当调小该配置项取值。存算一体模式下主键表单次 Compaction 合并的最大数据比例仍然保持原来自动调整模式。[#35129](https://github.com/StarRocks/starrocks/pull/35129)

#### 系统变量

- 新增会话变量 `cbo_decimal_cast_string_strict`，用于优化器控制 DECIMAL 类型转为 STRING 类型的行为。当取值为 `true` 时，使用 v2.5.x 及之后版本的处理逻辑，执行严格转换（即，按 Scale 截断补 `0`）；当取值为 `false` 时，保留 v2.5.x 之前版本的处理逻辑（即，按有效数字处理）。默认值是 `true`。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- 新增会话变量 `cbo_eq_base_type`，用于指定 DECIMAL 类型和 STRING 类型的数据比较时的强制类型，默认 `VARCHAR`，可选 `DECIMAL`。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- 新增会话变量 `big_query_profile_second_threshold`，当会话变量 [`enable_profile`](https://docs.starrocks.io/zh/docs/reference/System_variable#enable_profile) 设置为 `false` 且查询时间超过 `big_query_profile_second_threshold` 设定的阈值时，则会生成 Profile。[#33825](https://github.com/StarRocks/starrocks/pull/33825)

## 3.1.4

发布日期：2023 年 11 月 2 日

### 新增特性

- 存算分离下的主键表支持 Sort Key。
- 异步物化视图支持通过 str2date 函数指定分区表达式，可用于外表分区类型为 STRING 类型数据的物化视图的增量刷新和查询改写。[#29923](https://github.com/StarRocks/starrocks/pull/29923) [#31964](https://github.com/StarRocks/starrocks/pull/31964)
- 新增会话变量 [`enable_query_tablet_affinity`](https://docs.starrocks.io/zh/docs/reference/System_variable#enable_query_tablet_affinity25-及以后)，用于控制多次查询同一个 Tablet 时选择固定的同一个副本，默认关闭。[#33049](https://github.com/StarRocks/starrocks/pull/33049)
- 新增工具函数 `is_role_in_session`，用于查看指定角色是否在当前会话下被激活，并且支持查看嵌套的角色被激活的情况。 [#32984](https://github.com/StarRocks/starrocks/pull/32984)
- 增加资源组粒度的查询队列，需要通过全局变量 `enable_group_level_query_queue` 开启（默认值为 `false`）。当全局粒度或资源组粒度任一资源消耗达到阈值时，会对查询进行排队，直到所有资源消耗都没有超过阈值，再执行查询。
  - 每个资源组可以设置 `concurrency_limit` 用于限制单个 BE 节点中并发查询上限。
  - 每个资源组可以设置 `max_cpu_cores` 用于限制单个 BE 节点可以使用的 CPU 上限。
- 资源组分类器增加 `plan_cpu_cost_range` 和 `plan_mem_cost_range` 两个参数。
  - `plan_cpu_cost_range`：系统估计的查询 CPU 开销范围。默认为 `NULL`，表示没有该限制。
  - `plan_mem_cost_range`：系统估计的查询内存开销范围。默认为 `NULL`，表示没有该限制。

### 功能优化

- 窗口函数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、STDDEV_SAMP 支持 ORDER BY 子句和 Window 子句。 [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- DECIMAL 类型数据查询结果越界时，返回报错而不是 NULL。[#30419](https://github.com/StarRocks/starrocks/pull/30419)
- 查询队列的并发查询数量改由 Leader FE 管理，每个 Follower FE 在发起和结束一个查询时，会通知 Leader FE。如果超过全局粒度或资源组粒度的 `concurrency_limit` 则查询会被拒绝或进入查询队列。

### 问题修复

修复了如下问题：

- 由于内存统计不准确，有几率会导致 Spark 或者 Flink 读取数据时报错。[#30702](https://github.com/StarRocks/starrocks/pull/30702)  [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- Metadata Cache 的内存使用统计不准确。[#31978](https://github.com/StarRocks/starrocks/pull/31978)
- 调用 libcurl 时会引起 BE Crash。[#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 刷新基于 Hive 视图创建的 StarRocks 物化视图时会报错“java.lang.ClassCastException: com.starrocks.catalog.HiveView cannot be cast to com.starrocks.catalog.HiveMetaStoreTable”。[#31004](https://github.com/StarRocks/starrocks/pull/31004)
- ORDER BY 子句中包含聚合函数时报错“java.lang.IllegalStateException: null”。[#30108](https://github.com/StarRocks/starrocks/pull/30108)
- 存算分离模式下，表的 Key 信息没有在 `information_schema.COLUMNS` 中记录，导致使用 Flink Connector 导入数据时 DELETE 操作无法执行。[#31458](https://github.com/StarRocks/starrocks/pull/31458)
- 使用 Flink Connector 导入数据时，如果并发高且 HTTP 和 Scan 线程数受限，会发生卡死。[#32251](https://github.com/StarRocks/starrocks/pull/32251)
- 如果添加了一个较小字节类型的字段，变更完前执行 SELECT COUNT(*) 会报错“error: invalid field name”。[#33243](https://github.com/StarRocks/starrocks/pull/33243)
- Query Cache 开启后查询结果有错。[#32781](https://github.com/StarRocks/starrocks/pull/32781)
- 查询在 Hash Join 时失败了，会引起 BE Crash。[#32219](https://github.com/StarRocks/starrocks/pull/32219)
- BINARY 或 VARBINARY 类型在 `information_schema.``columns` 视图里面的 `DATA_TYPE` 和 `COLUMN_TYPE` 显示为 `unknown`。[#32678](https://github.com/StarRocks/starrocks/pull/32678)

### 行为变更

- 从 3.1.4 版本开始，新搭建集群的主键表持久化索引在表创建时默认打开（如若从低版本升级到 3.1.4 版本则保持不变）。[#33374](https://github.com/StarRocks/starrocks/pull/33374)
- 新增 FE 参数 `enable_sync_publish` 且默认开启。设置为 `true` 时，主键表导入的 Publish 过程会等 Apply 完成后才返回结果，这样，导入作业返回成功后数据立即可见，但可能会导致主键表导入比原来有延迟。（之前无此参数，导入时 Publish 过程中 Apply 是异步的）。[#27055](https://github.com/StarRocks/starrocks/pull/27055)

## 3.1.3

发布日期：2023 年 9 月 25 日

### 新增特性

- 存算分离下的主键表支持基于本地磁盘上的持久化索引，使用方式与存算一体一致。
- 聚合函数 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 支持使用 DISTINCT 关键词和 ORDER BY 子句。[#28778](https://github.com/StarRocks/starrocks/pull/28778)
- [Stream Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/)、[Broker Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/)、[Kafka Connector](https://docs.starrocks.io/zh/docs/loading/Kafka-connector-starrocks/)、[Flink Connector](https://docs.starrocks.io/zh/docs/loading/Flink-connector-starrocks/) 和 [Spark Connector](https://docs.starrocks.io/zh/docs/loading/Spark-connector-starrocks/) 均支持对主键表进行部分列更新时启用列模式。[#28288](https://github.com/StarRocks/starrocks/pull/28288)
- 分区中数据可以随着时间推移自动进行降冷操作（[List 分区方式](https://docs.starrocks.io/zh/docs/table_design/list_partitioning/)还不支持）。[#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### 功能优化

执行带有不合法注释的 SQL 命令返回结果与 MySQL 保持一致。[#30210](https://github.com/StarRocks/starrocks/pull/30210)

### 问题修复

修复了如下问题：

- 执行 [DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/DELETE/) 语句时，如果 WHERE 条件中的字段类型是 [BITMAP](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/BITMAP/) 或 [HLL](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/HLL/)，则会导致语句执行失败。[#28592](https://github.com/StarRocks/starrocks/pull/28592)
- 某个 Follower FE 重启后，由于 CpuCores 不同步，导致查询性能受到影响。[#28472](https://github.com/StarRocks/starrocks/pull/28472) [#30434](https://github.com/StarRocks/starrocks/pull/30434)
- [to_bitmap()](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/to_bitmap/) 函数的 Cost 统计计算不正确，导致物化视图改写后选择了错误的执行计划。[#29961](https://github.com/StarRocks/starrocks/pull/29961)
- 存算分离架构特定场景下，当 Follower FE 重启后，发送到该 Follower FE 的查询会返回错误信息“Backend node not found. Check if any backend node is down”。[#28615](https://github.com/StarRocks/starrocks/pull/28615)
- [ALTER TABLE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/) 过程中持续导入可能会报错“Tablet is in error state”。[#29364](https://github.com/StarRocks/starrocks/pull/29364)
- 在线修改 FE 动态参数 `max_broker_load_job_concurrency` 不生效。[#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- 调用 [date_diff()](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/date_diff/) 函数时，如果函数中指定的时间单位是个常量而日期是非常量，会导致 BE Crash。[#29937](https://github.com/StarRocks/starrocks/issues/29937)
- 如果 [Hive Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/hive_catalog/) 是多级目录，且数据存储在腾讯云 COS 中，会导致查询结果不正确。[#30363](https://github.com/StarRocks/starrocks/pull/30363)
- 存算分离架构下，开启数据异步写入后，自动分区不生效。[#29986](https://github.com/StarRocks/starrocks/issues/29986)
- 通过 [CREATE TABLE LIKE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE_LIKE/) 语句创建主键表时，会报错`Unexpected exception: Unknown properties: {persistent_index_type=LOCAL}`。[#30255](https://github.com/StarRocks/starrocks/pull/30255)
- 主键表 Restore 之后，BE 重启后元数据发生错误，导致元数据不一致。[#30135](https://github.com/StarRocks/starrocks/pull/30135)
- 主键表导入时，如果 Truncate 操作和查询并发，在有些情况下会报错“java.lang.NullPointerException”。[#30573](https://github.com/StarRocks/starrocks/pull/30573)
- 物化视图创建语句中包含谓词表达式时，刷新结果不正确。[#29904](https://github.com/StarRocks/starrocks/pull/29904)
- 升级到 3.1.2 版本后，原来创建的表中 Storage Volume 属性被重置成了 `null`。[#30647](https://github.com/StarRocks/starrocks/pull/30647)
- Tablet 元数据做 Checkpoint 与 Restore 操作并行时， 会导致某些副本丢失不可查。[#30603](https://github.com/StarRocks/starrocks/pull/30603)
- 如果表字段为 `NOT NULL` 但没有设置默认值，使用 CloudCanal 导入时会报错“Unsupported dataFormat value is : \N”。[#30799](https://github.com/StarRocks/starrocks/pull/30799)

### 行为变更

- 聚合函数 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 的分隔符必须使用 `SEPARATOR` 关键字声明。
- 会话变量 [`group_concat_max_len`](https://docs.starrocks.io/zh/docs/reference/System_variable#group_concat_max_len)（用于控制聚合函数 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 可以返回的字符串最大长度）的默认值由原来的没有限制变更为默认 `1024`。

## 3.1.2

发布日期：2023 年 8 月 25 日

### 问题修复

修复了如下问题：

- 用户在连接时指定默认数据库，并且仅有该数据库下面表权限，但无该数据库权限时，会报对该数据库无访问权限。[#29767](https://github.com/StarRocks/starrocks/pull/29767)
- RESTful API `show_data` 对于云原生表的返回信息不正确。[#29473](https://github.com/StarRocks/starrocks/pull/29473)
- 在 [array_agg()](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_agg/) 函数运行过程中，如果查询取消，则会发生 BE Crash。[#29400](https://github.com/StarRocks/starrocks/issues/29400)
- [BITMAP](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/BITMAP/) 和 [HLL](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/HLL/) 类型的列在 [SHOW FULL COLUMNS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_FULL_COLUMNS/) 查询结果中返回的 `Default` 字段值不正确。[#29510](https://github.com/StarRocks/starrocks/pull/29510)
- [array_map()](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_map/) 同时涉及多个表时，下推策略问题导致查询失败。[#29504](https://github.com/StarRocks/starrocks/pull/29504)
- 由于未合入上游 Apache ORC 的 BugFix ORC-1304（[apache/orc#1299](https://github.com/apache/orc/pull/1299)）而导致 ORC 文件查询失败。[#29804](https://github.com/StarRocks/starrocks/pull/29804)

### 行为变更

从此版本开始，执行 SET CATALOG 操作必须要有目标 Catalog 的 USAGE 权限。您可以使用 [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/) 命令进行授权操作。

如果是从低版本升级上来的集群，已经做好了已有用户的升级逻辑，不需要重新赋权。[#29389](https://github.com/StarRocks/starrocks/pull/29389)。如果是新增授权，则需要注意赋予目标 Catalog 的 USAGE 权限。

## 3.1.1

发布日期：2023 年 8 月 18 日

### 新增特性

- [存算分离架构](https://docs.starrocks.io/zh/docs/deployment/shared_data/s3/)下，支持如下特性：
  - 数据存储在 Azure Blob Storage 上。
  - List 分区。
- 支持聚合函数 [COVAR_SAMP](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/)、[COVAR_POP](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/)、[CORR](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/corr/)。
- 支持[窗口函数](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function/) COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、STDDEV_SAMP。

### 功能优化

对所有复合谓词以及 WHERE 子句中的表达式支持隐式转换，可通过[会话变量](https://docs.starrocks.io/zh/docs/reference/System_variable/) `enable_strict_type` 控制是否打开隐式转换（默认取值为 `false`）。

### 问题修复

修复了如下问题：

- 向多副本的表中导入数据时，如果某些分区没有数据，则会写入很多无用日志。 [#28824](https://github.com/StarRocks/starrocks/issues/28824)
- 主键表部分列更新时平均 row size 预估不准导致内存占用过多。 [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- 某个 Tablet 出现某种 ERROR 状态之后触发 Clone 操作，会导致磁盘使用上升。 [#28488](https://github.com/StarRocks/starrocks/pull/28488)
- Compaction 会触发冷数据写入 Local Cache。 [#28831](https://github.com/StarRocks/starrocks/pull/28831)

## 3.1.0

发布日期：2023 年 8 月 7 日

### 新增特性

#### 存算分离架构

- 新增支持主键表，暂不支持持久化索引。
- 支持自增列属性 [AUTO_INCREMENT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/auto_increment/)，提供表内全局唯一 ID，简化数据管理。
- 支持[导入时自动创建分区和使用分区表达式定义分区规则](https://docs.starrocks.io/zh/docs/table_design/expression_partitioning/)，提高了分区创建的易用性和灵活性。
- 支持[存储卷（Storage Volume）抽象](https://docs.starrocks.io/zh/docs/deployment/shared_data/s3/)，方便在存算分离架构中配置存储位置及鉴权等相关信息。后续创建库表时可以直接引用，提升易用性。

#### 数据湖分析

- 支持访问 [Hive Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/hive_catalog/) 内的视图。
- 支持访问 Parquet 格式的 Iceberg v2 数据表。
- 支持[写出数据到 Parquet 格式的 Iceberg 表](https://docs.starrocks.io/zh/docs/data_source/catalog/iceberg_catalog#向-iceberg-表中插入数据)。
- 【公测中】支持通过外部 [Elasticsearch catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/elasticsearch_catalog/) 访问 Elasticsearch，简化外表创建等过程。
- 【公测中】支持 [Paimon catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/paimon_catalog/)，帮助用户使用 StarRocks 对流式数据进行湖分析。

#### 存储、导入与查询

- 自动创建分区功能升级为[表达式分区](https://docs.starrocks.io/zh/docs/table_design/expression_partitioning/)，建表时只需要使用简单的分区表达式（时间函数表达式或列表达式）即可配置需要的分区方式，并且数据导入时 StarRocks 会根据数据和分区表达式的定义规则自动创建分区。这种创建分区的方式，更加灵活易用，能满足大部分场景。
- 支持 [List 分区](https://docs.starrocks.io/zh/docs/table_design/list_partitioning/)。数据按照分区列枚举值列表进行分区，可以加速查询和高效管理分类明确的数据。
- `Information_schema` 库新增表 `loads`，支持查询 [Broker Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/) 和 [Insert](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/INSERT/) 作业的结果信息。
- [Stream Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/)、[Broker Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/)、[Spark Load](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SPARK_LOAD/) 支持打印因数据质量不合格而过滤掉的错误数据行，通过 `log_rejected_record_num` 参数设置允许打印的最大数据行数。
- 支持[随机分桶（Random Bucketing）](https://docs.starrocks.io/zh/docs/table_design/Data_distribution#设置分桶)功能，建表时无需选择分桶键，StarRocks 将导入数据随机分发到各个分桶中，同时配合使用 2.5.7 版本起支持的自动设置分桶数量 (`BUCKETS`) 功能，用户可以不再关心分桶配置，大大简化建表语句。不过，在大数据、高性能要求场景中，建议继续使用 Hash 分桶方式，借助分桶裁剪来加速查询。
- 支持在 [INSERT INTO](https://docs.starrocks.io/zh/docs/loading/InsertInto/) 语句中使用表函数 FILES()，从 AWS S3 或 HDFS 直接导入 Parquet 或 ORC 格式文件的数据。FILES() 函数会自动进行表结构 (Table Schema) 推断，不再需要提前创建 External Catalog 或文件外部表，大大简化导入过程。
- 支持[生成列（Generated Column）](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/generated_columns/)功能，自动计算生成列表达式的值并存储，且在查询时可自动改写，以提升查询性能。
- 支持通过 [Spark connector](https://docs.starrocks.io/zh/docs/loading/Spark-connector-starrocks/) 导入 Spark 数据至 StarRocks。相较于 [Spark Load](https://docs.starrocks.io/zh/docs/loading/SparkLoad/)，Spark connector 能力更完善。您可以自定义 Spark 作业，对数据进行 ETL 操作，Spark connector 只作为 Spark 作业中的 sink。
- 支持导入数据到 [MAP](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/Map/)、[STRUCT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/STRUCT/) 类型的字段，并且在 ARRAY、MAP、STRUCT 类型中支持了 Fast Decimal 类型。

#### SQL 语句和函数

- 增加 Storage Volume 相关 SQL 语句：[CREATE STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/)、[ALTER STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/ALTER_STORAGE_VOLUME/)、[DROP STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/DROP_STORAGE_VOLUME/)、[SET DEFAULT STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/SET_DEFAULT_STORAGE_VOLUME/)、[DESC STORAGE VOLUME](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/DESC_STORAGE_VOLUME/)、[SHOW STORAGE VOLUMES](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/SHOW_STORAGE_VOLUMES/)。

- 支持通过 [ALTER TABLE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/) 修改表的注释。[#21035](https://github.com/StarRocks/starrocks/pull/21035)

- 增加如下函数：

  - Struct 函数：[struct (row)](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/struct-functions/row/)、[named_struct](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/struct-functions/named_struct/)
  - Map 函数：[str_to_map](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/str_to_map/)、[map_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_concat/)、[map_from_arrays](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_from_arrays/)、[element_at](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/element_at/)、[distinct_map_keys](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/distinct_map_keys/)、[cardinality](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/cardinality/)
  - Map 高阶函数：[map_filter](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_filter/)、[map_apply](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_apply/)、[transform_keys](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/transform_keys/)、[transform_values](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/transform_values/)
  - Array 函数：[array_agg](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_agg/) 支持 `ORDER BY`、[array_generate](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_generate/)、[element_at](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/element_at/)、[cardinality](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/cardinality/)
  - Array 高阶函数：[all_match](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/all_match/)、[any_match](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/any_match/)
  - 聚合函数：[min_by](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/min_by/)、[percentile_disc](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/percentile_disc/)
  - 表函数 (Table function)：[FILES](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/)、[generate_series](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/generate_series/)
  - 日期函数：[next_day](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/next_day/)、[previous_day](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/previous_day/)、[last_day](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/last_day/)、[makedate](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/makedate/)、[date_diff](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/date-time-functions/date_diff/)
  - Bitmap 函数：[bitmap_subset_limit](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_subset_limit/)、[bitmap_subset_in_range](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_subset_in_range/)
  - 数学函数 [cosine_similarity](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/cos_similarity/)、[cosine_similarity_norm](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/cos_similarity_norm/)

#### 权限与安全

增加 Storage Volume 相关[权限项](https://docs.starrocks.io/zh/docs/administration/privilege_item#存储卷权限-storage-volume)和外部数据目录 (External Catalog) 相关[权限项](https://docs.starrocks.io/zh/docs/administration/privilege_item#数据目录权限-catalog)，支持通过 [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/) 和 [REVOKE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/REVOKE/) 语句进行相关权限的赋予和撤销。

### 功能优化

#### 存算分离架构

优化存算分离架构中的数据缓存功能，可以指定热数据的范围，并防止冷数据查询占用 Local Disk Cache、进而影响热数据查询效率。

#### 物化视图

- 优化异步物化视图的创建：
  - 支持随机分桶 (Random Bucketing)，在创建物化视图时不指定分桶列 (Bucketing Column) 时默认采用随机分桶。
  - 支持通过 `ORDER BY` 指定排序键。
  - 支持使用 `colocate_group`、`storage_medium` 、`storage_cooldown_time` 等属性。
  - 支持使用会话变量 (Session Variable)，通过 `properties("session.<variable_name>" = "<value>")` 设置，灵活调整视图刷新的执行策略。
  - 默认为所有物化视图刷新设置开启 Spill 功能，查询超时时间为 1 小时。
  - 支持基于视图（View）创建物化视图，优化数据建模场景的易用性，可以灵活使用视图和物化视图进行分层建模。
- 优化异步物化视图的查询改写：
  - 支持 Stale Rewrite，即允许指定时间内未刷新的物化视图直接用于查询改写，无论其对应基表数据是否更新。用户可在创建物化视图时通过 `mv_rewrite_staleness_second` 属性配置可容忍未刷新时间。
  - 基于 Hive Catalog 外表创建的物化视图支持 View Delta Join 场景的查询改写（需要定义主键和外键约束）。
  - 支持 Join 派生改写、Count Distinct、time_slice 函数等场景改写，优化 Union 改写。
- 优化异步物化视图的刷新：
  - 优化 Hive Catalog 外表物化视图的刷新机制，StarRocks 可以感知到分区级别的数据变更，自动刷新时仅刷新有数据变更的分区。
  - 支持通过 `REFRESH MATERIALIZED VIEW WITH SYNC MODE` 同步调用物化视图刷新任务。
- 增强异步物化视图的使用：
  - 支持通过 `ALTER MATERIALIZED VIEW {ACTIVE | INACTIVE}`  语句启用或禁用物化视图，已禁用（处于 `INACTIVE` 状态）的物化视图不会被刷新或用于查询改写，但是仍然可以直接查询。
  - 支持通过 `ALTER MATERIALIZED VIEW SWAP WITH` 替换物化视图，可以通过新建一个物化视图并进行原子替换来实现物化视图的 Schema Change。
- 优化同步物化视图：
  - 支持通过 SQL Hint `[_SYNC_MV_]` 直接查询同步物化视图，规避少量无法自动改写的场景。
  - 同步物化视图支持更多表达式，现可使用 `CASE-WHEN`、`CAST`、数学运算等表达式，扩展其使用场景。

#### 数据湖分析

- 优化 Iceberg 元数据缓存与访问，提升查询性能。
- 优化湖分析的数据缓存（Data Cache）功能，进一步提升湖分析性能。

#### 存储、导入与查询

- 正式支持[大算子落盘 (Spill)](https://docs.starrocks.io/zh/docs/administration/spill_to_disk/) 功能，允许将部分阻塞算子的中间结果落盘。开启大算子落盘功能后，当查询中包含聚合、排序或连接算子时，StarRocks 会将以上算子的中间结果缓存到磁盘以减少内存占用，尽量避免查询因内存不足而导致查询失败。
- 支持基数保持 JOIN 表（Cardinality-preserving Joins）的裁剪。在较多表的星型模型（比如 SSB）和雪花模型 (TPC-H) 的建模中、且查询只涉及到少量表的一些情况下，能裁剪掉一些不必要的表，从而提升 JOIN 的性能。
- 执行 [UPDATE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/UPDATE/) 语句对主键表进行部分更新时支持启用列模式，适用于更新少部分列但是大量行的场景，更新性能可提升十倍。
- 优化统计信息收集，以降低对导入影响，提高收集性能。
- 优化并行 Merge 算法，在全排序场景下整体性能最高可提升 2 倍。
- 优化查询逻辑以不再依赖 DB 锁。
- 动态分区新增支持分区粒度为年。[#28386](https://github.com/StarRocks/starrocks/pull/28386)

#### SQL 语句和函数

- 条件函数 case、coalesce、if、ifnull、nullif 支持 ARRAY、MAP、STRUCT、JSON 类型。
- 以下 Array 函数支持嵌套结构类型 MAP、STRUCT、ARRAY：
  - array_agg
  - array_contains、array_contains_all、array_contains_any
  - array_slice、array_concat
  - array_length、array_append、array_remove、array_position
  - reverse、array_distinct、array_intersect、arrays_overlap
  - array_sortby
- 以下 Array 函数支持 Fast Decimal 类型：
  - array_agg
  - array_append、array_remove、array_position、array_contains
  - array_length
  - array_max、array_min、array_sum、array_avg
  - arrays_overlap、array_difference
  - array_slice、array_distinct、array_sort、reverse、array_intersect、array_concat
  - array_sortby、array_contains_all、array_contains_any

### 问题修复

修复了如下问题：

- 执行 Routine Load 时，无法正常处理重连 Kafka 的请求。[#23477](https://github.com/StarRocks/starrocks/issues/23477)
- SQL 查询中涉及多张表、并且含有 `WHERE` 子句时，如果这些 SQL 查询的语义相同但给定表顺序不同，则有些 SQL 查询不能改写成对相关物化视图的使用。[#22875](https://github.com/StarRocks/starrocks/issues/22875)
- 查询包含 `GROUP BY` 子句时，会返回重复的数据结果。[#19640](https://github.com/StarRocks/starrocks/issues/19640)
- 调用 lead() 或 lag() 函数可能会导致 BE 意外退出。[#22945](https://github.com/StarRocks/starrocks/issues/22945)
- 根据 External Catalog 外表物化视图重写部分分区查询失败。[#19011](https://github.com/StarRocks/starrocks/issues/19011)
- SQL 语句同时包含反斜线 (`\`) 和分号 (`;`) 时解析报错。[#16552](https://github.com/StarRocks/starrocks/issues/16552)
- 物化视图删除后，其基表数据无法清空 (Truncate)。[#19802](https://github.com/StarRocks/starrocks/issues/19802)

### 行为变更

- 存算分离架构下删除建表时的 `storage_cache_ttl` 参数，Cache 写满后按 LRU 算法进行淘汰。
- BE 配置项 `disable_storage_page_cache`、`alter_tablet_worker_count` 和 FE 配置项 `lake_compaction_max_tasks` 由静态参数改为动态参数。
- BE 配置项 `block_cache_checksum_enable` 默认值由 `true` 改为 `false`。
- BE 配置项 `enable_new_load_on_memory_limit_exceeded` 默认值由 `false` 改为 `true`。
- FE 配置项 `max_running_txn_num_per_db` 默认值由 `100` 改为 `1000`。
- FE 配置项 `http_max_header_size` 默认值由 `8192` 改为 `32768`。
- FE 配置项 `tablet_create_timeout_second` 默认值由 `1` 改为 `10`。
- FE 配置项 `max_routine_load_task_num_per_be` 默认值由 `5` 改为 `16`，并且当创建的 Routine Load 任务数量较多时，如果发生报错会给出提示。
- FE 配置项 `quorom_publish_wait_time_ms` 更名为 `quorum_publish_wait_time_ms`，`async_load_task_pool_size` 更名为 `max_broker_load_job_concurrency`。
- CN 配置项 `thrift_port` 更名为 `be_port`。
- 废弃 BE 配置项 `routine_load_thread_pool_size`，单 BE 节点上 Routine Load 线程池大小完全由 FE 配置项 `max_routine_load_task_num_per_be` 控制。
- 废弃 BE 配置项 `txn_commit_rpc_timeout_ms` 和系统变量 `tx_visible_wait_timeout`。
- 废弃 FE 配置项 `max_broker_concurrency`、`load_parallel_instance_num`。
- 废弃 FE 配置项 `max_routine_load_job_num`，通过 `max_routine_load_task_num_per_be` 来动态判断每个 BE 节点上支持的 Routine Load 任务最大数，并且在任务失败时给出建议。
- Routine Load 作业新增两个属性 `task_consume_second` 和 `task_timeout_second`，作用于单个 Routine Load 导入作业内的任务，更加灵活。如果作业中没有设置这两个属性，则采用 FE 配置项 `routine_load_task_consume_second` 和 `routine_load_task_timeout_second` 的配置。
- 默认启用[资源组](https://docs.starrocks.io/zh/docs/administration/resource_group/)功能，因此弃用会话变量 `enable_resource_group`。
- 增加如下保留关键字：COMPACTION、TEXT。
