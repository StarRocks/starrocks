---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.0

## 3.0.9

发布日期：2024 年 1 月 2 日

### 新增特性

- 新增聚合函数 [percentile_disc](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/percentile_disc/)。[#36352](https://github.com/StarRocks/starrocks/pull/36352)
- 新增监控指标 `max_tablet_rowset_num`（用于设置 Rowset 的最大数量），可以协助提前发现 Compaction 是否会出问题并及时干预，减少报错信息“too many versions”的出现。[#36539](https://github.com/StarRocks/starrocks/pull/36539)

### 功能优化

- 系统变量 [sql_mode](https://docs.starrocks.io/zh/docs/reference/System_variable#sql_mode) 增加 `GROUP_CONCAT_LEGACY` 选项，用以兼容 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 函数在 2.5（不含）版本之前的实现逻辑。[#36150](https://github.com/StarRocks/starrocks/pull/36150)
- 使用 JDK 时 GC 算法默认采用 G1。[#37386](https://github.com/StarRocks/starrocks/pull/37386)
- 在 `be_tablets` 表中增加 `INDEX_DISK` 记录持久化索引的磁盘使用量，单位是 Bytes。[#35615](https://github.com/StarRocks/starrocks/pull/35615)
- 支持 MySQL 外部表和 JDBC Catalog 外部表的 WHERE 子句中包含关键字。[#35917](https://github.com/StarRocks/starrocks/pull/35917)
- 如果是自动分区表，也支持指定分区名进行更新，如果分区不存在则报错。[#34777](https://github.com/StarRocks/starrocks/pull/34777)
- 主键模型表 [SHOW DATA](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_DATA/) 的结果中新增包括 **.cols** 文件（部分列更新和生成列相关的文件）和持久化索引文件的文件大小信息。[#34898](https://github.com/StarRocks/starrocks/pull/34898)
- 优化主键模型表全部 Rowset 进行 Compaction 时的持久化索引更新性能，降低 I/O 负载。 [#36819](https://github.com/StarRocks/starrocks/pull/36819)
- WHERE 子句中 LIKE 运算符右侧字符串中不包括 `%` 或者 `_` 时，LIKE 运算符会转换成 `=` 运算符。[#37515](https://github.com/StarRocks/starrocks/pull/37515)
- 优化主键模型表 Compaction Score 的取值逻辑，使其和其他模型的表的取值范围看起来更一致。[#36534](https://github.com/StarRocks/starrocks/pull/36534)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加时间戳进度信息，展示各个分区当前消费消息的时间戳。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- 优化 Bitmap 相关的某些操作的性能，主要包括：
  - 优化 Nested Loop Join 性能。[#340804](https://github.com/StarRocks/starrocks/pull/34804)  [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - 优化 `bitmap_xor` 函数性能。[#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - 支持 Copy on Write（简称 COW），优化性能，并减少内存使用。[#34047](https://github.com/StarRocks/starrocks/pull/34047)

### 行为变更

- 新增 Session 变量 `enable_materialized_view_for_insert`，默认值为 `FALSE`，即物化视图默认不改写 INSERT INTO SELECT 语句中的查询。[#37505](https://github.com/StarRocks/starrocks/pull/37505)
- 将 FE 配置项 `enable_new_publish_mechanism` 改为静态参数，修改后必须重启 FE 才可以生效（3.2以后做成动态加载事务依赖关系了）。 [#35338](https://github.com/StarRocks/starrocks/pull/35338)
- 调整 Trash 文件的默认过期时间为 1 天（原来是 3 天）。[#37113](https://github.com/StarRocks/starrocks/pull/37113)

### 参数变更

#### 系统变量

- 新增 Session 变量 `cbo_decimal_cast_string_strict`，用于优化器控制 DECIMAL 类型转为 STRING 类型的行为。当取值为 `true` 时，使用 v2.5.x 及之后版本的处理逻辑，执行严格转换（即，按 Scale 截断补 `0`）；当取值为 `false` 时，保留 v2.5.x 之前版本的处理逻辑（即，按有效数字处理）。默认值是 `true`。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- 新增 Session 变量 `transaction_read_only`和`tx_read_only` ，设置事务的访问模式并且兼容 MySQL 5.7.20 以上的版本。[#37249](https://github.com/StarRocks/starrocks/pull/37249)

#### FE 配置项

- 新增 FE 配置项 `routine_load_unstable_threshold_second`。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- 新增 FE 配置项  `http_worker_threads_num`，HTTP Server 用于处理 HTTP 请求的线程数。默认取值为 0。如果配置为负数或 0 ，线程数将设置为 CPU 核数的 2 倍**。**[#37530](https://github.com/StarRocks/starrocks/pull/37530)
- 新增 FE 配置项 `default_mv_refresh_immediate`，用于控制物化视图创建完成后是否立刻进行刷新，默认值为 `true`。[#37093](https://github.com/StarRocks/starrocks/pull/37093)

#### BE 配置项

- 新增 BE 配置项 `enable_stream_load_verbose_log`，默认取值是 `false`，打开后日志中可以记录 Stream Load 的 HTTP 请求和响应信息，方便出现问题后的定位调试。[#36113](https://github.com/StarRocks/starrocks/pull/36113)
- 新增 BE 配置项 `pindex_major_compaction_limit_per_disk`，配置每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题，默认取值为 `1`。[#36681](https://github.com/StarRocks/starrocks/pull/36681)
- 新增 BE 配置项，配置对象存储连接超时时间。
  - `object_storage_connect_timeout_ms`，对象存储 socket 连接的超时时间，默认取值 `-1`，表示使用 SDK 中的默认时间。
  - `object_storage_request_timeout_ms`，对象存储 http 连接的超时时间，默认取值 `-1`，表示使用 SDK 中的默认时间。

### 问题修复

修复了如下问题：

- 某些情况下，通过 Catalog 读取 ORC 外表时会出现 BE Crash。[#27971](https://github.com/StarRocks/starrocks/pull/27971)
- 数据损坏情况下，建立持久化索引会引起 BE Crash。[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- 添加 Bitmap 索引后，偶尔会出现 BE crash。[#26463](https://github.com/StarRocks/starrocks/pull/26463)
- 副本操作重放失败可能会引起 FE 启动失败。[#32295](https://github.com/StarRocks/starrocks/pull/32295)
- 当 `recover_with_empty_tablet` 设置为 `true` 时可能会引起 FE 启动失败。[#33071](https://github.com/StarRocks/starrocks/pull/33071)
- 查询在 Hash Join 时失败了，会引起 BE Crash。[#32219](https://github.com/StarRocks/starrocks/pull/32219)
- 存算一体模式下，单独查询 Iceberg 或者 Hive 外表容易出现 BE crash。[#34682](https://github.com/StarRocks/starrocks/pull/34682)
- 查询时报错 `"get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction"`。[#33246](https://github.com/StarRocks/starrocks/pull/33246)
- 执行 `show proc '/statistic'` 有概率出现卡住的情况。 [#34237](https://github.com/StarRocks/starrocks/pull/34237/files)
- 开启 FE 配置项 `enable_collect_query_detail_info` 后，FE 性能下降严重。[#35945](https://github.com/StarRocks/starrocks/pull/35945)
- 向打开持久化索引的主键模型表中导入大量数据，有时会报错。[#34352](https://github.com/StarRocks/starrocks/pull/34352)
- 2.4 及以下的版本升级到高版本，可能会出现 Compaction Score 很高的问题。[#34618](https://github.com/StarRocks/starrocks/pull/34618)
- 使用 MariaDB ODBC Driver 查询 `INFORMATION_SCHEMA` 中的信息时，`schemata` 视图中 `CATALOG_NAME` 列中取值都显示的是 `null`。[#34627](https://github.com/StarRocks/starrocks/pull/34627)
- 导入数据异常导致 FE Crash 后无法重启。[#34590](https://github.com/StarRocks/starrocks/pull/34590)
- Stream Load 导入作业在 **PREPARD** 状态下，同时有 Schema Change 在执行，会导致数据丢失。[#34381](https://github.com/StarRocks/starrocks/pull/34381)
- 如果 HDFS 路径以两个或以上斜杠（`/`）结尾，HDFS 备份恢复会失败。[#34601](https://github.com/StarRocks/starrocks/pull/34601)
- 使用 ALTER TABLE 增加 `partition_live_number` 属性没有生效。[#34842](https://github.com/StarRocks/starrocks/pull/34842)
- ARRAY_DISTINCT 函数偶发 BE Crash。[#36377](https://github.com/StarRocks/starrocks/pull/36377)
- 某些情况下，物化视图刷新可能会出现死锁问题。[#35736](https://github.com/StarRocks/starrocks/pull/35736)
- 特定场景下 Global Runtime Filter 可能会引发 BE crash。[#35776](https://github.com/StarRocks/starrocks/pull/35776)
- 某些情况下 `bitmap_to_string` 会因为转换时数据类型溢出导致查询结果错误。[#37405](https://github.com/StarRocks/starrocks/pull/37405)

## 3.0.8

发布日期：2023 年 11 月 17 日

### 功能优化

- `INFORMATION_SCHEMA.COLUMNS` 表支持显示 ARRAY、MAP、STRUCT 类型的字段。[#33431](https://github.com/StarRocks/starrocks/pull/33431)

### 问题修复

修复了如下问题：

- 执行 `show proc '/current_queries';` 时，如果某个查询刚开始执行，可能会引起 BE Crash。[#34316](https://github.com/StarRocks/starrocks/pull/34316)
- 带 Sort Key 的主键模型表持续高频导入时，会小概率出现 Compaction 失败。[#26486](https://github.com/StarRocks/starrocks/pull/26486)
- 修改 LDAP 认证用户的密码后，使用新密码登录时报错。
- 如果提交的 Broker Load 作业包含过滤条件，在数据导入过程中，某些情况下会出现 BE crash。[#29832](https://github.com/StarRocks/starrocks/pull/29832)
- SHOW GRANTS 时报 `unknown error`。[#30100](https://github.com/StarRocks/starrocks/pull/30100)
- 当使用函数 `cast()` 时数据类型转换前后类型一致，某些类型下会导致 BE crash。[#31465](https://github.com/StarRocks/starrocks/pull/31465)
- BINARY 或 VARBINARY 类型在 `information_schema.columns` 视图里面的 `DATA_TYPE` 和 `COLUMN_TYPE` 显示为 `unknown`。[#32678](https://github.com/StarRocks/starrocks/pull/32678)
- 长时间向持久化索引打开的主键模型表高频导入，可能会引起 BE crash。[#33220](https://github.com/StarRocks/starrocks/pull/33220)
- Query Cache 开启后查询结果有错。[#32778](https://github.com/StarRocks/starrocks/pull/32778)
- 重启后 RESTORE 的表中的数据和 BACKUP 前的表中的数据在某些情况下会不一致。[#33567](https://github.com/StarRocks/starrocks/pull/33567)
- 执行 RESTORE 时如果 Compaction 正好同时在进行，可能引起 BE crash。[#32902](https://github.com/StarRocks/starrocks/pull/32902)

## 3.0.7

发布日期：2023 年 10 月 18 日

### 功能优化

- 窗口函数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、STDDEV_SAMP 支持 ORDER BY 子句和 Window 子句。 [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- 主键模型数据写入时的 publish 过程由异步改为同步，导入作业成功返回后数据立即可见。[#27055](https://github.com/StarRocks/starrocks/pull/27055)
- DECIMAL 类型数据查询结果越界时，返回报错而不是 NULL。[#30419](https://github.com/StarRocks/starrocks/pull/30419)
- 执行带有不合法注释的 SQL 命令时，命令返回结果与 MySQL 保持一致。[#30210](https://github.com/StarRocks/starrocks/pull/30210)
- 对于单个分区列的 Range 分区或者表达式分区的 StarRocks 表，SQL 语句的谓词中包含分区列的表达式也可用于分区裁剪。[#30421](https://github.com/StarRocks/starrocks/pull/30421)

### 问题修复

修复了如下问题：

- 并发进行库和表的创建删除操作，某些情况下会引起表找不到而导致数据写入报错。[#28985](https://github.com/StarRocks/starrocks/pull/28985)
- 某些情况下使用 UDF 会存在内存泄露。[#29467](https://github.com/StarRocks/starrocks/pull/29467) [#29465](https://github.com/StarRocks/starrocks/pull/29465)
- ORDER BY 子句中包含聚合函数时报错“java.lang.IllegalStateException: null”。[#30108](https://github.com/StarRocks/starrocks/pull/30108)
- 如果 Hive Catalog 是多级目录，且数据存储在腾讯云 COS 中，会导致查询结果不正确。[#30363](https://github.com/StarRocks/starrocks/pull/30363)
- 如果 ARRAY&lt;STRUCT&gt; 类型的数据中 STRUCT 的某些子列缺失，在读取数据时因为填充默认数据长度错误会导致 BE Crash。[#30263](https://github.com/StarRocks/starrocks/pull/30263)
- 升级 Berkeley DB Java Edition 的版本，避免安全漏洞。[#30029](https://github.com/StarRocks/starrocks/pull/30029)
- 主键模型表导入时，如果 Truncate 操作和查询并发，在有些情况下会报错“java.lang.NullPointerException”。[#30573](https://github.com/StarRocks/starrocks/pull/30573)
- 如果 Schema Change 执行时间过长，会因为 Tablet 版本被垃圾回收而失败。 [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- 如果表字段为 `NOT NULL` 但没有设置默认值，使用 CloudCanal 导入时会报错“Unsupported dataFormat value is : \N”。[#30799](https://github.com/StarRocks/starrocks/pull/30799)
- 存算分离模式下，表的 Key 信息没有在 `information_schema.COLUMNS` 中记录，导致使用 Flink Connector 导入数据时 DELETE 操作无法执行。[#31458](https://github.com/StarRocks/starrocks/pull/31458)
- 升级时如果某些列的类型也升级了（比如 Decimal 升级到 Decimal v3），某些特定特征的表在 Compaction 时会导致 BE crash。[#31626](https://github.com/StarRocks/starrocks/pull/31626)
- 使用 Flink Connector 导入数据时，如果并发高且 HTTP 和 Scan 线程数受限，会发生卡死。[#32251](https://github.com/StarRocks/starrocks/pull/32251)
- 调用 libcurl 时会引起 BE Crash。[#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 向主键模型表增加 BITMAP 类型的列时报错。[#31763](https://github.com/StarRocks/starrocks/pull/31763)

## 3.0.6

发布日期：2023 年 9 月 12 日

### 行为变更

- 聚合函数 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 的分隔符必须使用 `SEPARATOR` 关键字声明。

### 新增特性

- 聚合函数 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 支持使用 DISTINCT 关键词和 ORDER BY 子句。[#28778](https://github.com/StarRocks/starrocks/pull/28778)
- 分区中数据可以随着时间推移自动进行降冷操作（[List 分区方式](https://docs.starrocks.io/zh/docs/table_design/list_partitioning/)还不支持）。[#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### 功能优化

- 对所有复合谓词以及 WHERE 子句中的表达式支持隐式转换，可通过[会话变量](https://docs.starrocks.io/zh/docs/reference/System_variable/) `enable_strict_type` 控制是否打开隐式转换（默认取值为 `false`）。[#21870](https://github.com/StarRocks/starrocks/pull/21870)
- 统一 FE 和 BE 中 STRING 转换成 INT 的处理逻辑。[#29969](https://github.com/StarRocks/starrocks/pull/29969)

### 问题修复

修复了如下问题：

- 如果 `enable_orc_late_materialization` 设置为 `true`，使用 Hive Catalog 查询 ORC 文件中 STRUCT 类型的数据时结果异常。[#27971](https://github.com/StarRocks/starrocks/pull/27971)
- Hive Catalog 查询时，如果 WHERE 子句中使用分区列且包含 OR 条件，查询结果不正确。 [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- RESTful API `show_data` 对于云原生表的返回信息不正确。[#29473](https://github.com/StarRocks/starrocks/pull/29473)
- 如果集群为[存算分离架构](https://docs.starrocks.io/zh/docs/3.0/deployment/deploy_shared_data/)，数据存储在 Azure Blob Storage 上，并且已经建表，则回滚到 3.0 时 FE 无法启动。 [#29433](https://github.com/StarRocks/starrocks/pull/29433)
- 向用户赋予 Iceberg Catalog 下某表权限后，该用户查询该表时显示没有权限。[#29173](https://github.com/StarRocks/starrocks/pull/29173)
- [BITMAP](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/BITMAP/) 和 [HLL](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/HLL/) 类型的列在 [SHOW FULL COLUMNS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/SHOW_FULL_COLUMNS/) 查询结果中返回的 `Default` 字段值不正确。[#29510](https://github.com/StarRocks/starrocks/pull/29510)
- 在线修改 FE 动态参数 `max_broker_load_job_concurrency` 不生效。[#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- Refresh 物化视图，同时并发地修改物化视图的刷新策略，有概率会导致 FE 无法启动。[#29691](https://github.com/StarRocks/starrocks/pull/29691)
- 执行 `select count(distinct(int+double)) from table_name` 会报错 `unknown error`。 [#30054](https://github.com/StarRocks/starrocks/pull/30054)
- 主键模型表 Restore 之后，BE 重启后元数据发生错误，导致元数据不一致。[#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 3.0.5

发布日期：2023 年 8 月 16 日

### 新增特性

- 支持聚合函数 [COVAR_SAMP](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/)、[COVAR_POP](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/)、[CORR](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/aggregate-functions/corr/)。
- 支持[窗口函数](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function/) COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、STDDEV_SAMP。

### 功能优化

- 在报错信息 `xxx too many versions xxx` 中增加了如何处理的建议说明。[#28397](https://github.com/StarRocks/starrocks/pull/28397)
- 动态分区新增支持分区粒度为年。[#28386](https://github.com/StarRocks/starrocks/pull/28386)
- [INSERT OVERWRITE 中使用表达式分区时](https://docs.starrocks.io/zh/docs/table_design/expression_partitioning#%E5%AF%BC%E5%85%A5%E6%95%B0%E6%8D%AE%E8%87%B3%E5%88%86%E5%8C%BA)，分区字段大小写不敏感。[#28309](https://github.com/StarRocks/starrocks/pull/28309)

### 问题修复

修复了如下问题：

- FE 中表级别 scan 统计信息错误，导致表查询和导入的 metrics 信息不正确。 [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- 分区表中修改 sort key 列后查询结果不稳定。[#27850](https://github.com/StarRocks/starrocks/pull/27850)
- Restore 后同一个 tablet 在 BE 和 FE 上的 version 不一致。[#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- 建 Colocation 表时如果不指定 buckets 数量，则 bucket 数量自动推断为 0，后续添加分区会失败。[#27086](https://github.com/StarRocks/starrocks/pull/27086)
- 当 INSERT INTO SELECT 的 SELECT 结果集为空时，SHOW LOAD 显示导入任务状态为 `CANCELED`。[#26913](https://github.com/StarRocks/starrocks/pull/26913)
- 当 sub_bitmap 函数的输入值不是 BITMAP 类型时，会导致 BE crash。[#27982](https://github.com/StarRocks/starrocks/pull/27982)
- 更新 AUTO_INCREMENT 列时会导致 BE crash。[#27199](https://github.com/StarRocks/starrocks/pull/27199)
- 物化视图 Outer join 和 Anti join 改写错误。 [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- 主键模型部分列更新时平均 row size 预估不准导致内存占用过多。 [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- 激活失效物化视图时可能导致 FE crash。[#27959](https://github.com/StarRocks/starrocks/pull/27959)
- 查询无法改写至基于 Hudi Catalog 外部表创建的物化视图。[#28023](https://github.com/StarRocks/starrocks/pull/28023)
- 删除 Hive 表后，即使手动更新元数据缓存，仍然可以查询到 Hive 表数据。[#28223](https://github.com/StarRocks/starrocks/pull/28223)
- 当异步物化视图的刷新策略为手动刷新且同步调用刷新任务（SYNC MODE）时，手动刷新后 `information_schema.task_runs` 表中有多条 INSERT OVERWRITE 的记录。[#28060](https://github.com/StarRocks/starrocks/pull/28060)
- LabelCleaner 线程卡死导致 FE 内存泄漏。 [#28311](https://github.com/StarRocks/starrocks/pull/28311) [#28636](https://github.com/StarRocks/starrocks/pull/28636)

## 3.0.4

发布日期：2023 年 7 月 18 日

### 新增特性

- 查询和物化视图的 Join 类型不同时，也支持对查询进行改写。[#25099](https://github.com/StarRocks/starrocks/pull/25099)

### 功能优化

- 优化异步物化视图的手动刷新策略。支持通过 REFRESH MATERIALIZED VIEW WITH SYNC MODE 同步调用物化视图刷新任务。[#25904](https://github.com/StarRocks/starrocks/pull/25904)
- 如果查询的字段不包含在物化视图的 output 列但是包含在其谓词条件中，仍可使用该物化视图进行查询改写。[#23028](https://github.com/StarRocks/starrocks/issues/23028)
- [切换至 Trino 语法](https://docs.starrocks.io/zh/docs/reference/System_variable/) `set sql_dialect = 'trino';`，查询时表别名大小写不敏感。[#26094](https://github.com/StarRocks/starrocks/pull/26094) [#25282](https://github.com/StarRocks/starrocks/pull/25282)
- `Information_schema.tables_config` 表中增加了 `table_id` 字段。您可以基于 `table_id` 字段关联数据库 `Information_schema` 中的表 `tables_config` 和 `be_tablets`，来查询 tablet 所属数据库和表名称。[#24061](https://github.com/StarRocks/starrocks/pull/24061)

### 问题修复

修复了如下问题：

- sum 聚合函数的查询改写至单表物化视图时，会因为类型推导问题导致 sum 的查询结果出错。[#25512](https://github.com/StarRocks/starrocks/pull/25512)
- 存算分离模式下，使用 SHOW PROC 查看 tablet 信息时报错。
- 插入数据长度超出 STRUCT 定义的 CHAR 长度时，插入无响应。 [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- 当 INSERT INTO SELECT 存在 FULL JOIN 时，返回结果有遗漏。[#26603](https://github.com/StarRocks/starrocks/pull/26603)
- 使用 ALTER TABLE 命令修改表的 `default.storage_medium` 属性时报错 `ERROR xxx: Unknown table property xxx`。[#25870](https://github.com/StarRocks/starrocks/issues/25870)
- Broker Load 导入空文件时报错。[#26212](https://github.com/StarRocks/starrocks/pull/26212)
- BE 下线偶尔会卡住。[#26509](https://github.com/StarRocks/starrocks/pull/26509)

## 3.0.3

发布日期：2023 年 6 月 28 日

### 功能优化

- StarRocks 外表元数据的同步改为数据加载时进行。 [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- 对于使用[表达式分区](https://docs.starrocks.io/zh/docs/table_design/expression_partitioning/)的表，INSERT OVERWRITE 支持指定分区。 [#25005](https://github.com/StarRocks/starrocks/pull/25005)
- 优化了非分区表增加分区时的报错信息。 [#25266](https://github.com/StarRocks/starrocks/pull/25266)

### 问题修复

修复了如下问题：

- Parquet 文件中如果包括复杂类型，最大最小过滤时会获取列错误。 [#23976](https://github.com/StarRocks/starrocks/pull/23976)
- 库或者表已经被 Drop，但是写入任务仍然在队列中。 [#24801](https://github.com/StarRocks/starrocks/pull/24801)
- FE 重启时会小概率导致 BE crash。 [#25037](https://github.com/StarRocks/starrocks/pull/25037)
- "enable_profile = true" 时导入和查询偶尔会卡住。 [#25060](https://github.com/StarRocks/starrocks/pull/25060)
- 集群不满足 3 个 Alive BE 时，INSERT OVERWRITE 报错信息不准确。 [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 3.0.2

发布日期：2023 年 6 月 13 日

### 功能优化

- Union 查询在被物化视图改写后，谓词也可以下推。 [#23312](https://github.com/StarRocks/starrocks/pull/23312)
- 优化表的[自动分桶策略](https://docs.starrocks.io/zh/docs/3.0/table_design/Data_distribution/#%E7%A1%AE%E5%AE%9A%E5%88%86%E6%A1%B6%E6%95%B0%E9%87%8F)。 [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- 解除 NetworkTime 对系统时钟的依赖，以解决系统时钟误差导致 Exchange 网络耗时估算异常的问题。 [#24858](https://github.com/StarRocks/starrocks/pull/24858)

### 问题修复

修复了如下问题：

- Schema change 和数据导入同时进行时 Schema change 偶尔会卡住。 [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- `pipeline_profile_level = 0` 时查询出错。 [#23873](https://github.com/StarRocks/starrocks/pull/23873)
- `cloud_native_storage_type` 配置为 S3 时建表报错。
- LDAP 账号没有密码也能登录。 [#24862](https://github.com/StarRocks/starrocks/pull/24862)
- CANCEL LOAD 在表不存在时会失败。 [#24922](https://github.com/StarRocks/starrocks/pull/24922)

### 升级注意事项

- 如果您系统中有名为 `starrocks` 的数据库，请先通过 ALTER DATABASE RENAME 改名后再进行升级。

## 3.0.1

发布日期：2023 年 6 月 1 日

### 新增特性

- [公测中] 支持大查询算子落盘 ([Spill to disk](https://docs.starrocks.io/zh/docs/administration/spill_to_disk/))，通过将中间结果落盘来降低大查询的内存消耗。
- [Routine Load](https://docs.starrocks.io/zh/docs/loading/RoutineLoad#导入-avro-数据) 支持导入 Avro 格式的数据。
- 支持 [Microsoft Azure Storage](https://docs.starrocks.io/zh/docs/integrations/authenticate_to_azure_storage/) (包括 Azure Blob Storage 和 Azure Data Lake Storage)。

### 功能优化

- 存算分离集群 (shared-data) 支持通过 StarRocks 外表来同步其他 StarRocks 集群的数据。
- [Information Schema](https://docs.starrocks.io/zh/docs/reference/information_schema/load_tracking_logs/) 增加 `load_tracking_logs` 来记录最近的导入错误信息。
- 忽略建表语句中的中文空格。[#23885](https://github.com/StarRocks/starrocks/pull/23885)

### 问题修复

修复了如下问题：

- SHOW CREATE TABLE 返回的**主键模型表**建表信息错误。[#24237](https://github.com/StarRocks/starrocks/issues/24237)
- Routine Load 过程中 BE crash。[#20677](https://github.com/StarRocks/starrocks/issues/20677)
- 创建分区表时指定不支持的 Properties 导致 NPE。[#21374](https://github.com/StarRocks/starrocks/issues/21374)
- SHOW TABLE STATUS 结果展示不全。[#24279](https://github.com/StarRocks/starrocks/issues/24279)

### 升级注意事项

- 如果您系统中有名为 `starrocks` 的数据库，请先通过 ALTER DATABASE RENAME 改名后再进行升级。

## 3.0.0

发布日期：2023 年 4 月 28 日

### 新增特性

**系统架构**

- 支持存算分离架构。可以在 FE 配置文件中开启，开启后数据会持久化到远程对象存储/HDFS 中，本地盘作为缓存使用，可以更灵活的增删节点，支持表级别的缓存生命周期管理。在本地缓存命中的情况下性能可以对齐非存算分离版本。更多信息，参见[部署使用 StarRocks 存算分离集群](https://docs.starrocks.io/zh/docs/3.0/deployment/deploy_shared_data/)。

**存储和导入**

- 支持自增列属性 [AUTO_INCREMENT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/auto_increment/)，提供表内全局唯一 ID，简化数据管理。
- 支持[导入时自动创建分区和使用分区表达式定义分区规则](https://docs.starrocks.io/zh/docs/3.0/table_design/automatic_partitioning/)，提高了分区创建的易用性和灵活性。
- Primary Key 模型表支持更丰富的 [UPDATE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/UPDATE/) 和 [DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/DELETE/) 语法，包括使用 CTE 和对多表的引用。
- Broker Load 和 INSERT INTO 增加 Load Profile，支持通过 profile 查看并分析导入作业详情。使用方法与 [查看分析Query Profile](https://docs.starrocks.io/zh/docs/administration/query_profile/) 相同。

**数据湖分析**

- [Preview] 支持 Presto/Trino 兼容模式，可以自动改写 Presto/Trino 的 SQL。参见[系统变量](https://docs.starrocks.io/zh/docs/reference/System_variable/) 中的 `sql_dialect`。
- [Preview] 支持 [JDBC Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/jdbc_catalog/)。
- 支持使用 [SET CATALOG](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/SET_CATALOG/) 命令来手动选择 Catalog。

**权限与安全**

- 提供了新版的完整 RBAC 功能，支持角色的继承和默认角色。更多信息，参见[权限系统总览](https://docs.starrocks.io/zh/docs/administration/privilege_overview/)。
- 提供更多权限管理对象和更细粒度的权限。更多信息，参见[权限项](https://docs.starrocks.io/zh/docs/administration/privilege_item/)。

**查询**

<!-- - [Preview] 支持大查询的算子落盘，可以在内存不足时利用磁盘空间来保证查询稳定执行成功。
- [Query Cache](https://docs.starrocks.io/zh/docs/using_starrocks/query_cache/) 支持更多使用场景，包括各种 Broadcast Join、Bucket Shuffle Join 场景。-->
- 支持 [Global UDF](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/JAVA_UDF/)。
- 动态自适应并行度，可以根据查询并发度自适应调节 `pipeline_dop`。

**SQL 语句和函数**

- 新增如下权限相关 SQL 语句：[SET DEFAULT ROLE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE/)、[SET ROLE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/SET_ROLE/)、[SHOW ROLES](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/SHOW_ROLES/)、[SHOW USERS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/SHOW_USERS/)。
- 新增半结构化数据分析相关函数：[map_from_arrays](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_from_arrays/)、[map_apply](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/map-functions/map_apply/)。
- [array_agg](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_agg/) 支持 ORDER BY。
- 窗口函数 [lead](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function#使用-lead-窗口函数)、[lag](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function#使用-lag-窗口函数) 支持 IGNORE NULLS。
- 新增 [BINARY/VARBINARY 数据类型](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-types/BINARY/)，新增 [to_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/binary-functions/to_binary/)，[from_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/binary-functions/from_binary/) 函数。
- 新增字符串函数 [replace](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/replace/)、[hex_decode_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/hex_decode_binary/)、[hex_decode_string](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/hex_decode_string/)。
- 新增加密函数 [base64_decode_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_binary/)、[base64_decode_string](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_string/)。
- 新增数学函数 [sinh](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/sinh/)、[cosh](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/cosh/)、[tanh](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/math-functions/tanh/)。
- 新增工具函数 [current_role](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/utility-functions/current_role/)。

### 功能优化

**部署**

- 更新 3.0 版本的 Docker 镜像和相关[部署文档](https://docs.starrocks.io/zh/docs/quick_start/deploy_with_docker/)。 [#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021)

**存储与导入**

- 数据导入提供了更丰富的 CSV 格式参数，包括 `skip_header`、`trim_space`、`enclose` 和 `escape`。参见 [STREAM LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/)、[BROKER LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/) 和 [ROUTINE LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD/)。
- [Primary Key 模型表](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/)解耦了主键和排序键，支持通过 `ORDER BY` 单独指定排序键。
- 优化 Primary Key 模型表在大数据导入、部分列更新、以及开启持久化索引等场景的内存占用。 [#12068](https://github.com/StarRocks/starrocks/pull/12068) [#14187](https://github.com/StarRocks/starrocks/pull/14187) [#15729](https://github.com/StarRocks/starrocks/pull/15729)
- 提供异步 ETL 命令接口，支持创建异步 INSERT 任务。更多信息，参考[INSERT](https://docs.starrocks.io/zh/docs/loading/InsertInto/) 和 [SUBMIT TASK](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SUBMIT_TASK/)。 ([#20609](https://github.com/StarRocks/starrocks/issues/20609))

**物化视图**

- 优化 [物化视图](https://docs.starrocks.io/zh/docs/using_starrocks/Materialized_view/) 的改写能力：
  - 支持 view delta join, 可以改写。
  - 支持对 Outer Join 和 Cross Join 的改写。
  - 优化带分区时 UNION 的 SQL rewrite。
- 完善物化视图的构建能力：支持 CTE、SELECT * 、UNION。
- 优化 [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW/) 命令的返回信息。
- 提升物化视图构建时的分区创建效率。([#21167](https://github.com/StarRocks/starrocks/pull/21167))

**查询**

- 完善算子对 Pipeline 的支持，所有算子都支持 Pipeline。
- 完善[大查询定位](https://docs.starrocks.io/zh/docs/administration/monitor_manage_big_queries/)。[SHOW PROCESSLIST](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/SHOW_PROCESSLIST/) 支持查看 CPU 内存信息，增加大查询日志。
- 优化 Outer Join Reorder 能力。
- 优化 SQL 解析阶段的报错信息，查询的报错位置更明确，信息更清晰。

**数据湖分析**

- 优化元数据统计信息收集。
- Hive Catalog、Iceberg Catalog、Hudi Catalog 和 Delta Lake Catalog 支持通过 [SHOW CREATE TABLE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE/) 查看外表的创建信息。

### 问题修复

修复了以下问题：

- StarRocks 源文件的许可 header 中部分 url 无法访问。[#2224](https://github.com/StarRocks/starrocks/issues/2224)
- SELECT 查询出现 unknown error。 [#19731](https://github.com/StarRocks/starrocks/issues/19731)
- 支持 SHOW/SET CHARACTER。 [#17480](https://github.com/StarRocks/starrocks/issues/17480)
- 当导入的内容超过 StarRocks 字段长度时，ErrorURL 提示信息不准确。提示源端字段为空，而不是内容超长。 [#14](https://github.com/StarRocks/DataX/issues/14)
- 支持 show full fields from 'table'。 [#17233](https://github.com/StarRocks/starrocks/issues/17233)
- 分区修剪导致 MV 改写失败。 [#14641](https://github.com/StarRocks/starrocks/issues/14641)
- 当 MV 创建语句包含 count(distinct) 且 count(distinct) 作用在分布列上时，MV 改写失败。 [#16558](https://github.com/StarRocks/starrocks/issues/16558)
- 使用 VARCHAR 作为物化视图分区列时导致 FE 无法正常启动。 [#19366](https://github.com/StarRocks/starrocks/issues/19366)
- 窗口函数 [lead](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function#使用-lead-窗口函数) 和 [lag](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/Window_function#使用-lag-窗口函数) 对 IGNORE NULLS 的处理不正确。 [#21001](https://github.com/StarRocks/starrocks/pull/21001)
- 插入临时分区和自动创建分区发生冲突。 [#21222](https://github.com/StarRocks/starrocks/issues/21222)

### 行为变更

- RBAC 升级以后会兼容之前的用户和权限，但是 [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/) 和 [REVOKE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/REVOKE/) 等相关语法有大幅改动。
- SHOW MATERIALIZED VIEW 更名为 [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW/)。
- 新增如下[保留关键字](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/keywords/)：AUTO_INCREMENT、CURRENT_ROLE、DEFERRED、ENCLOSE、ESCAPE、IMMEDIATE、PRIVILEGES、SKIP_HEADER、TRIM_SPACE、VARBINARY。

### 升级注意事项

**必须从 2.5 版本升级到 3.0，否则无法回滚 （理论上从 2.5 以下的版本升级到 3.0 也支持。但保险起见，请统一从 2.5 升级，降低风险。）**

#### BDBJE 升级

3.0 版本升级了 BDB 库，但是 BDB 不支持回滚，所以需要在回滚之后仍然使用 3.0 里的 BDB 库。

1. 在 FE 包替换成旧版本之后，将 3.0 包里的`fe/lib/starrocks-bdb-je-18.3.13.jar`放到 2.5 版本`fe/lib`目录下。
2. 删除`fe/lib/je-7.*.jar`。

#### 权限系统升级

升级到 3.0 会默认采用新的 RBAC 权限管理系统，升级后只支持回滚到 2.5。

回滚后需要手动执行 [ALTER SYSTEM CREATE IMAGE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/Administration/ALTER_SYSTEM/) 命令，触发创建新的 image 并等待该 image 同步给所有的 follower 节点。如不执行该命令，可能会导致部分回滚失败。ALTER SYSTEM CREATE IMAGE 在 2.5.3 版本及以后支持。
