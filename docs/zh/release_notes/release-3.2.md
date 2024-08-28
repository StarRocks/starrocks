---
displayed_sidebar: docs
---

# StarRocks version 3.2

## 3.2.4

发布日期：2024 年 3 月 12 日

### 新增特性

- 存算分离集群中的云原生主键表支持 Size-tiered 模式 Compaction，以减轻导入较多小文件时 Compaction 的写放大问题。[#41034](https://github.com/StarRocks/starrocks/pull/41034)
- Storage Volume 支持 HDFS 的参数化配置，包括 Simple 认证方式支持配置 username，Kerberos 认证，NameNode HA，以及 ViewFS。 
- 新增日期函数 `milliseconds_diff`。[#38171](https://github.com/StarRocks/starrocks/pull/38171)
- 新增 Session 变量 `catalog`，用于指定当前会话所在的 Catalog。[#41329](https://github.com/StarRocks/starrocks/pull/41329)
- Hint 中支持设置[用户自定义变量](https://docs.starrocks.io/zh/docs/administration/Query_planning/#%E7%94%A8%E6%88%B7%E8%87%AA%E5%AE%9A%E4%B9%89%E5%8F%98%E9%87%8F-hint)。[#40746](https://github.com/StarRocks/starrocks/pull/40746)
- Hive Catalog 支持 CREATE TABLE LIKE。[#37685](https://github.com/StarRocks/starrocks/pull/37685) 
- 新增 `information_schema.partitions_meta` 视图，提供丰富的 PARTITION 元信息。[#39265](https://github.com/StarRocks/starrocks/pull/39265)
- 新增 `sys.fe_memory_usage` 视图，提供 StarRocks 的内存使用信息。[#40464](https://github.com/StarRocks/starrocks/pull/40464)

### 行为变更

- `cbo_decimal_cast_string_strict` 用于优化器控制 DECIMAL 类型转为 STRING 类型的行为。默认值是 `true`，即执行严格转换（按 Scale 截断补 `0`）。在历史版本中没有严格按照 DECIMAL 类型进行补齐，从而在 DECIMAL 与 STRING 类型进行比等时会产生不同效果。[#40619](https://github.com/StarRocks/starrocks/pull/40619)
- Iceberg Catalog 的参数 `enable_iceberg_metadata_cache` 默认值改为 `false`。在 3.2.1 到 3.2.3 版本，该参数默认值统一为 `true`。自 3.2.4 版本起，如果 Iceberg 集群的元数据服务为 AWS Glue，该参数默认值仍为 `true`，如果 Iceberg 集群的元数据服务为 Hive Metastore（简称 HMS）或其他，则该参数默认值变更为 `false`。[#41826](https://github.com/StarRocks/starrocks/pull/41826)
- 修改能发起物化视图刷新任务的用户，从原本的 `root` 用户变成创建物化视图的用户，已有的物化视图不受影响。[#40670](https://github.com/StarRocks/starrocks/pull/40670)  
- 常量和字符串类型的列进行比较时，默认按字符串进行比较，用户可以通过设置变量 `cbo_eq_base_type` 来调整默认行为。将 `cbo_eq_base_type` 设置为 `decimal` 可以改为按数值进行比较。[#40619](https://github.com/StarRocks/starrocks/pull/40619)

### 功能优化

- 存算分离架构中，支持将数据分区存储于兼容 S3 的存储桶中的不同分区（子路径）中，分区路径使用统一前缀。此举可以提升 StarRocks 对 S3 文件的读写访问效率。[#41627](https://github.com/StarRocks/starrocks/pull/41627)
- 支持通过 `s3_compatible_fs_list` 参数设置可以使用 AWS SDK 接入的 S3 兼容对象存储。同时支持通过 `fallback_to_hadoop_fs_list` 参数配置需要通过 HDFS 的 Schema 接入的非 S3 兼容对象存储（该方法需要使用厂商提供的 JAR 包）。[#41123](https://github.com/StarRocks/starrocks/pull/41123)
- 优化 Trino 语法兼容性，支持 Trino 的 `current_catalog`、`current_schema`、`to_char`、`from_hex`、`to_date`、`to_timestamp` 以及 `index` 函数的语法转换。[#41217](https://github.com/StarRocks/starrocks/pull/41217) [#41319](https://github.com/StarRocks/starrocks/pull/41319) [#40803](https://github.com/StarRocks/starrocks/pull/40803)
- 优化物化视图改写，支持基于逻辑视图创建的物化视图的改写。[#42173](https://github.com/StarRocks/starrocks/pull/42173)
- 优化 STRING 向 DATETIME 类型转换的效率，性能约提升 35%~40%。[#41464](https://github.com/StarRocks/starrocks/pull/41464)
- 聚合表中 BITMAP 类型的列支持指定聚合类型为 `replace_if_not_null`，从而支持部分列更新。[#42034](https://github.com/StarRocks/starrocks/pull/42034)
- 优化 Broker Load 导入 ORC 小文件时的性能。[#41765](https://github.com/StarRocks/starrocks/pull/41765)
- 行列混存表支持 Schema Change。[#40851](https://github.com/StarRocks/starrocks/pull/40851)
- 行列混存表支持 BITMAP、HLL、JSON、ARRAY、MAP 和 STRUCT 等复杂类型。[#41476](https://github.com/StarRocks/starrocks/pull/41476)
- 新增内部 SQL 日志，其中包含统计信息和物化视图等相关的日志信息。[#40453](https://github.com/StarRocks/starrocks/pull/40453)

### 问题修复

修复了如下问题：

- 当创建 Hive 视图的查询语句中存在同一个表或视图的名称或别名大小写不一致的情况时，会出现 "Analyze Error" 的问题。[#40921](https://github.com/StarRocks/starrocks/pull/40921)
- 主键表使用持久化索引会导致磁盘 I/O 打满。[#39959](https://github.com/StarRocks/starrocks/pull/39959)
- 存算分离集群中，主键索引目录每 5 小时会被错误删除。 [#40745](https://github.com/StarRocks/starrocks/pull/40745)
- 手动执行 ALTER TABLE COMPACT 后，Compaction 内存统计有异常。[#41150](https://github.com/StarRocks/starrocks/pull/41150)
- 主键表 Publish 重试时可能会卡住。[#39890](https://github.com/StarRocks/starrocks/pull/39890)

## 3.2.3

发布日期：2024 年 2 月 8 日

### 新增特性

- 【公测中】支持行列混存的表存储格式，对于基于主键的高并发、低延时点查，以及数据部分列更新等场景有更好的性能。但目前还不支持 ALTER，Sort Key 和列模式部分列更新。
- 支持异步物化视图的备份（BACKUP）和恢复（RESTORE）。
- Broker Load 支持 JSON 格式的数据的导入。
- 支持基于视图创建的物化视图的查询改写。例如，直接基于视图创建了物化视图，后续基于该视图的查询可以被改写到物化视图上。
- 支持 CREATE OR REPLACE PIPE。 [#37658](https://github.com/StarRocks/starrocks/pull/37658)

### 行为变更

- 新增 Session 变量 `enable_strict_order_by`。当取值为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。[#37910](https://github.com/StarRocks/starrocks/pull/37910)
- 新增 Session 变量 `enable_materialized_view_for_insert`，默认值为 `FALSE`，即物化视图默认不改写 INSERT INTO SELECT 语句中的查询。[#37505](https://github.com/StarRocks/starrocks/pull/37505)
- 单个查询在 Pipeline 框架中执行时所使用的内存限制不再受 `exec_mem_limit` 限制，仅由 `query_mem_limit` 限制。取值为 `0` 表示没有限制。 [#34120](https://github.com/StarRocks/starrocks/pull/34120) 

### 参数变更

- 新增 FE 配置项  `http_worker_threads_num`，HTTP Server 用于处理 HTTP 请求的线程数。默认取值为 0。如果配置为负数或 0 ，线程数将设置为 CPU 核数的 2 倍。[#37530](https://github.com/StarRocks/starrocks/pull/37530)
- 新增 BE 配置项 `lake_pk_compaction_max_input_rowsets`，用于控制存算分离集群下主键表 Compaction 任务中允许的最大输入 Rowset 数量，优化 Compaction 时资源的使用。[#39611](https://github.com/StarRocks/starrocks/pull/39611)
- 新增 Session 变量 `connector_sink_compression_codec`，用于指定写入 Hive 表或 Iceberg 表时以及使用 Files() 导出数据时的压缩算法，可选算法包括 GZIP、BROTLI、ZSTD 以及 LZ4。 [#37912](https://github.com/StarRocks/starrocks/pull/37912)
- 新增 FE 配置项 `routine_load_unstable_threshold_second`。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- 新增 BE 配置项 `pindex_major_compaction_limit_per_disk`，配置每块盘 Compaction 的最大并发数，用于解决 Compaction 在磁盘之间不均衡导致个别磁盘 I/O 过高的问题，默认取值为 `1`。[#36681](https://github.com/StarRocks/starrocks/pull/36681)
- 新增 BE 配置项 `enable_lazy_delta_column_compaction`，默认取值是 `true`，表示不启用频繁的进行 Delta Column 的 Compaction。[#36654](https://github.com/StarRocks/starrocks/pull/36654)
- 新增 FE 配置项 `default_mv_refresh_immediate`，用于控制物化视图创建完成后是否立刻进行刷新，默认值为 `true`，表示立刻刷新，`false` 表示延迟刷新。 [#37093](https://github.com/StarRocks/starrocks/pull/37093)
- 调整 FE 配置项 `default_mv_refresh_partition_num` 默认值为 `1`，即单次物化视图刷新需更新多个分区时，任务将分批执行，一次只刷新一个分区。此举可以减少每次刷新占用的资源。 [#36560](https://github.com/StarRocks/starrocks/pull/36560)
- 调整 BE/CN 配置项 `starlet_use_star_cache` 默认值为 `true`，即在存算分离模式下默认开启 Data Cache。如果您在升级前将 BE/CN 参数 `starlet_cache_evict_high_water` 配置为 `X`，则需要将 BE/CN 参数 `starlet_star_cache_disk_size_percent` 配置为 `(1.0 - X) * 100`。例如，如果您将 `starlet_cache_evict_high_water` 设置为 0.3，则需要设置 `starlet_star_cache_disk_size_percent` 为 70。此举可以确保 file data cache 和 Data Cache 不会超过磁盘容量上限。[#38200](https://github.com/StarRocks/starrocks/pull/38200)

### 功能优化

- 对于分区字段为 TIMESTAMP 类型的 Iceberg 表，新增 `yyyy-MM-ddTHH:mm` 和 `yyyy-MM-dd HH:mm` 两种数据格式的支持。[#39986](https://github.com/StarRocks/starrocks/pull/39986)
- 监控 API 增加 Data Cache 相关指标。 [#40375](https://github.com/StarRocks/starrocks/pull/40375)
- 优化 BE 的日志打印，避免日志过多。 [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- 视图 `information_schema.be_tablets` 中增加 `storage_medium` 字段。 [#37070](https://github.com/StarRocks/starrocks/pull/37070)
- 支持在多个子查询中使用 `SET_VAR`。 [#36871](https://github.com/StarRocks/starrocks/pull/36871)
- SHOW ROUTINE LOAD 返回结果中增加 `LatestSourcePosition`，记录数据源 Kafka 中 Topic 内各个分区的最新消息位点，便于检查导入延迟情况。[#38298](https://github.com/StarRocks/starrocks/pull/38298)
- WHERE 子句中 LIKE 运算符右侧字符串中不包括 `%` 或者 `_` 时，LIKE 运算符会转换成 `=` 运算符。[#37515](https://github.com/StarRocks/starrocks/pull/37515)
- 调整 Trash 文件的默认过期时间为 1 天（原来是 3 天）。[#37113](https://github.com/StarRocks/starrocks/pull/37113)
- 支持收集带 Partition Transform 的 Iceberg 表的统计信息。 [#39907](https://github.com/StarRocks/starrocks/pull/39907)
- 优化 Rountine Load 的调度策略，慢任务不阻塞其他正常任务的执行。[#37638](https://github.com/StarRocks/starrocks/pull/37638)

### 问题修复

修复了如下问题：

- ANALYZE TABLE 偶尔会卡住。 [#36836](https://github.com/StarRocks/starrocks/pull/36836)
- PageCache 内存占用在有些情况下会超过 BE 动态参数 `storage_page_cache_limit` 设定的阈值。[#37740](https://github.com/StarRocks/starrocks/pull/37740)
- Hive Catalog 的元数据在 Hive 表新增字段后不会自动刷新。[#37549](https://github.com/StarRocks/starrocks/pull/37549)
- 某些情况下 `bitmap_to_string` 会因为转换时数据类型溢出导致查询结果错误。[#37405](https://github.com/StarRocks/starrocks/pull/37405)
- `SELECT ... FROM ... INTO OUTFILE` 导出至 CSV 时，如果 FROM 子句中包含多个常量，执行时会报错："Unmatched number of columns"。[#38045](https://github.com/StarRocks/starrocks/pull/38045)
- 查询表中半结构化数据时，某些情况下会导致 BE Crash。 [#40208](https://github.com/StarRocks/starrocks/pull/40208)

## 3.2.2

发布日期：2023 年 12 月 30 日

### 问题修复

修复了如下问题：

- 从 v3.1.2 及之前版本升级至 v3.2 后，FE 可能启动失败。 [#38172](https://github.com/StarRocks/starrocks/pull/38172)

## 3.2.1

发布日期：2023 年 12 月 21 日

### 新增特性

#### 数据湖分析

- 支持通过 Java Native Interface（JNI）读取 Avro、SequenceFile 以及 RCFile 格式的 [Hive Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/hive_catalog/) 表和文件外部表。

#### 物化视图

- `sys` 数据库新增 `object_dependencies` 视图，可用于查询异步物化视图血缘关系。 [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- 支持创建带有 WHERE 子句的同步物化视图。
- Iceberg 异步物化视图支持分区级别的增量刷新。
- [Preview] 支持基于 Paimon Catalog 外表创建异步物化视图，支持分区级别刷新。

#### 查询和函数

- 支持预处理语句（Prepared Statement）。预处理语句可以提高处理高并发点查查询的性能，同时有效地防止 SQL 注入。
- 新增如下 Bitmap 函数：[subdivide_bitmap](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/)、[bitmap_from_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/)、[bitmap_to_binary](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。
- 新增如下 Array 函数：[array_unique_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_unique_agg/)。

#### 监控指标

- 新增了监控指标 `max_tablet_rowset_num`（用于设置 Rowset 的最大数量），可以协助提前发现 Compaction 是否会出问题并及时干预，减少报错信息“too many versions”的出现。[#36539](https://github.com/StarRocks/starrocks/pull/36539)

### 参数变更

- 新增 BE 配置项 `enable_stream_load_verbose_log`，默认取值是 `false`，打开后日志中可以记录 Stream Load 的 HTTP 请求和响应信息，方便出现问题后的定位调试。[#36113](https://github.com/StarRocks/starrocks/pull/36113)

### 功能优化

- 使用 JDK8 时，默认 GC 算法采用 G1。 [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- 系统变量 [sql_mode](https://docs.starrocks.io/zh/docs/reference/System_variable/#sql_mode) 增加 `GROUP_CONCAT_LEGACY` 选项，用以兼容 [group_concat](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/string-functions/group_concat/) 函数在 2.5（不含）版本之前的实现逻辑。[#36150](https://github.com/StarRocks/starrocks/pull/36150)
- 隐藏了审计日志（Audit Log）中 [Broker Load 作业里 AWS S3](https://docs.starrocks.io/zh/docs/loading/s3/) 的鉴权信息 `aws.s3.access_key` 和 `aws.s3.access_secret`。[#36571](https://github.com/StarRocks/starrocks/pull/36571)
- 在 `be_tablets` 表中增加 `INDEX_DISK` 记录持久化索引的磁盘使用量，单位是 Bytes。[#35615](https://github.com/StarRocks/starrocks/pull/35615)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加 `OtherMsg`，展示最后一个失败的任务的相关信息。[#35806](https://github.com/StarRocks/starrocks/pull/35806)

### 问题修复

修复了如下问题：

- 数据损坏情况下，建立持久化索引会引起 BE Crash。[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- ARRAY_DISTINCT 函数偶发 BE Crash。[#36377](https://github.com/StarRocks/starrocks/pull/36377)
- 启用 DISTINCT 下推窗口算子功能时，对窗口函数的输出列的复杂表达式进行 SELECT DISTINCT 操作会报错。[#36357](https://github.com/StarRocks/starrocks/pull/36357)
- 某些兼容 S3 协议的对象存储会返回重复的文件，导致 BE Crash。[#36103](https://github.com/StarRocks/starrocks/pull/36103)

## 3.2.0

发布日期：2023 年 12 月 1 日

### 新增特性

#### 存算分离

- 支持[主键表](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/)的索引在本地磁盘的持久化。
- 支持 Data Cache 在多磁盘间均匀分布。

#### 物化视图

**异步物化视图**

- 物化视图支持 Query Dump。
- 物化视图的刷新默认开启中间结果落盘，降低刷新的内存消耗。

#### 数据湖分析

- 支持在 [Hive Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/hive_catalog/) 中创建、删除数据库以及 Managed Table，支持使用 INSERT 或 INSERT OVERWRITE 导出数据到 Hive 的 Managed Table。
- 支持 [Unified Catalog](https://docs.starrocks.io/zh/docs/data_source/catalog/unified_catalog/)。如果同一个 Hive Metastore 或 AWS Glue 元数据服务包含多种表格式（Hive、Iceberg、Hudi、Delta Lake 等），则可以通过 Unified Catalog 进行统一访问。
- 支持通过 ANALYZE TABLE 收集 Hive 和 Iceberg 表的统计信息，并存储在 StaRocks 内部，方便优化加速后续查询。
- 支持外表的 Information Schema，为外部系统（如BI）与 StarRocks 的交互提供更多便利。

#### 导入、导出和存储

- 使用表函数 [FILES()](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/table-functions/files/) 进行数据导入新增以下功能：
  - 支持导入 Azure 和 GCP 中的 Parquet 或 ORC 格式文件的数据。
  - 支持 `columns_from_path` 参数，能够从文件路径中提取字段信息。
  - 支持导入复杂类型（JSON、ARRAY、MAP 及 STRUCT）的数据。
- 支持使用 INSERT INTO FILES() 语句将数据导出至 AWS S3 或 HDFS 中的 Parquet 格式的文件。有关详细说明，请参见[使用 INSERT INTO FILES 导出数据](https://docs.starrocks.io/zh/docs/unloading/unload_using_insert_into_files/)。
- 通过增强 ALTER TABLE 命令提供了 [optimize table 功能](https://docs.starrocks.io/zh/docs/table_design/Data_distribution#建表后优化数据分布自-32)，可以调整表结构并重组数据，以优化查询和导入的性能。支持的调整项包括：分桶方式和分桶数、排序键，以及可以只调整部分分区的分桶数。
- 支持使用 PIPE 导入方式从[云存储 S3](https://docs.starrocks.io/zh/docs/loading/s3/#通过-pipe-导入) 或 [HDFS](https://docs.starrocks.io/zh/docs/loading/hdfs_load/#通过-pipe-导入) 中导入大规模数据和持续导入数据。在导入大规模数据时，PIPE 命令会自动根据导入数据大小和导入文件数量将一个大导入任务拆分成很多个小导入任务穿行运行，降低任务出错重试的代价、减少导入中对系统资源的占用，提升数据导入的稳定性。同时，PIPE 也能不断监听云存储目录中的新增文件或文件内容修改，并自动将变化的数据文件数据拆分成一个个小的导入任务，持续地将新数据导入到目标表中。

#### 查询

- 支持 [HTTP SQL API](https://docs.starrocks.io/zh/docs/reference/HTTP_API/SQL/)。用户可以通过 HTTP 方式访问 StarRocks 数据，执行 SELECT、SHOW、EXPLAIN 或 KILL 操作。
- 新增 Runtime Profile，以及基于文本的 Profile 分析指令（SHOW PROFILELIST，ANALYZE PROFILE，EXPLAIN ANALYZE），用户可以通过 MySQL 客户端直接进行 Profile 的分析，方便定位瓶颈点并发现优化机会。

#### SQL 语句和函数

新增如下函数：

- 字符串函数：substring_index、url_extract_parameter、url_encode、url_decode、translate
- 日期函数：dayofweek_iso、week_iso、quarters_add、quarters_sub、milliseconds_add、milliseconds_sub、date_diff、jodatime_format、str_to_jodatime、to_iso8601、to_tera_date、to_tera_timestamp
- 模糊/正则匹配函数：regexp_extract_all
- hash 函数：xx_hash3_64
- 聚合函数：approx_top_k
- 窗口函数：cume_dist、percent_rank、session_number
- 工具函数：get_query_profile、is_role_in_session

#### 权限

支持通过 [Apache Ranger](https://docs.starrocks.io/zh/docs/administration/ranger_plugin/) 实现访问控制，提供更高层次的数据安全保障，并且允许复用原有的外部数据源 Service。StarRocks 集成 Apache Ranger 后可以实现以下权限控制方式：

- 访问 StarRocks 内表、外表或其他对象时，可根据在 Ranger 中创建的 StarRocks Service 配置的访问策略来进行访问控制。
- 访问 External Catalog 时，也可以复用对应数据源原有的 Ranger service（如 Hive Service）来进行访问控制（当前暂未支持导出数据到 Hive 操作的权限控制）。

### 功能优化

#### 数据湖分析

- 优化了 ORC Reader：
  - 优化 ORC Column Reader，VARCHAR 和 CHAR 数据读取性能有接近两倍提升。
  - 优化 ORC 文件 Zlib 压缩格式的解压性能。
- 优化了 Parquet Reader：
  - 支持自适应 I/O 合并，可根据过滤效果自适应是否合并带谓词的列和不带谓词的列，从而减少 I/O。
  - 优化 Dict Filter。针对对字典编码类型文件，支持更快的谓词改写、Dict Filter 支持 STRUCT 子列、按需进行字典列译码。
  - 优化 Dict Decode 性能。
  - 优化延迟物化性能。
  - 支持缓存文件 Footer，从而避免反复计算开销。
  - 支持读取 lzo 压缩格式。
- 优化了 CSV Reader
  - 优化了读取性能。
  - 支持读取 Snappy 和 lzo 压缩格式。
- 优化了 Count 操作的性能。
- 优化了 Iceberg Catalog 能力：
  - 支持收集 Manifest 文件中的列统计信息为查询加速。
  - 支持收集 Puffin 文件中的 NDV（number of distinct values）为查询加速。
  - 支持分区裁剪。
  - 优化 Iceberg 元数据内存占用，提升在元数据量过大或查询并发较高时的稳定性。

#### 物化视图

**异步物化视图**

- 异步物化视图自动刷新：当创建物化视图涉及的表、视图及视图内涉及的表、物化视图发生 Schema Change 或 Swap 操作后，物化视图可以进行自动刷新
- 数据一致性：
  - 创建物化视图时，添加了 `query_rewrite_consistency` 属性。该属性允许用户基于一致性检查结果定义查询改写规则。
  - 创建物化视图时，添加了 `force_external_table_query_rewrite` 属性。该属性用于定义是否为外表物化视图强制开启查询重写。
  - 有关详细信息，请参见[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/)。
- 增加分区列一致性检查：当创建分区物化视图时，如物化视图的查询中涉及带分区的窗口函数，则窗口函数的分区列需要与物化视图的分区列一致。

#### 导入、导出和存储  

- 优化主键表（Primary Key）表持久化索引功能，优化内存使用逻辑，同时降低 I/O 的读写放大。
- 主键表（Primary Key）表支持本地多块磁盘间数据均衡。
- 分区中数据可以随着时间推移自动进行降冷操作（List 分区方式暂不支持）。相对原来的设置，更方便进行分区冷热管理。有关详细信息，请参见[设置数据的初始存储介质、自动降冷时间](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE#设置数据的初始存储介质自动降冷时间和副本数)。
- 主键表数据写入时的 Publish 过程由异步改为同步，导入作业成功返回后数据立即可见。有关详细信息，请参见 [enable_sync_publish](https://docs.starrocks.io/zh/docs/administration/FE_configuration#enable_sync_publish)。
- 支持 Fast Schema Evolution 模式，由表属性 [`fast_schema_evolution`](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE#设置-fast-schema-evolution) 控制。启用该模式可以在进行加减列变更时提高执行速度并降低资源使用。该属性默认值是 `false`（即关闭）。不支持建表后通过 ALTER TABLE 修改该表属性。
- 对于采用随机分桶的**明细表**，系统进行了优化，会根据集群信息及导入中的数据量大小[按需动态调整 Tablet 数量](https://docs.starrocks.io/zh/docs/table_design/Data_distribution#设置分桶数量)。

#### 查询

- Metabase 和 Superset 兼容性提升，支持集成 External Catalog。

#### SQL 语句和函数

- [array_agg](https://docs.starrocks.io/zh/docs/sql-reference/sql-functions/array-functions/array_agg/) 支持使用 DISTINCT 关键词。
- INSERT、UPDATE 以及 DELETE 支持使用 `SET_VAR`。 [#35283](https://github.com/StarRocks/starrocks/pull/35283)

#### 其他优化

- 新增会话变量 `large_decimal_underlying_type = "panic"|"double"|"decimal"`，用以设置超出范围的 DECIMAL 类型数据的转换规则。其中 `panic` 表示直接报错，`double` 表示转换为 DOUBLE 类型，`decimal` 表示转换为 DECIMAL(38,s)。

### 开发者工具

- 异步物化视图支持 Trace Query Profile，用于分析物化视图透明改写的场景。

### 行为变更

待更新。

### 参数变更

#### FE 配置项

- 新增以下 FE 配置项：
  - `catalog_metadata_cache_size`
  - `enable_backup_materialized_view`
  - `enable_colocate_mv_index` 
  - `enable_fast_schema_evolution`
  - `json_file_size_limit`
  - `lake_enable_ingest_slowdown` 
  - `lake_ingest_slowdown_threshold` 
  - `lake_ingest_slowdown_ratio`
  - `lake_compaction_score_upper_bound`
  - `mv_auto_analyze_async`
  - `primary_key_disk_schedule_time`
  - `statistic_auto_collect_small_table_rows`
  - `stream_load_task_keep_max_num`
  - `stream_load_task_keep_max_second`
- 删除 FE 配置项 `enable_pipeline_load`。
- 默认值修改：
  - `enable_sync_publish` 默认值从 `false` 变为 `true`。
  - `enable_persistent_index_by_default` 默认值从 `false` 变为 `true`。

#### BE 配置项

- Data Cache 相关配置项变更。 
  - 新增 `datacache_enable` 以取代 `block_cache_enable`。
  - 新增 `datacache_mem_size` 以取代 `block_cache_mem_size`。
  - 新增 `datacache_disk_size` 以取代 `block_cache_disk_size`。
  - 新增 `datacache_disk_path` 以取代 `block_cache_disk_path`。
  - 新增 `datacache_meta_path` 以取代 `block_cache_meta_path`。
  - 新增 `datacache_block_size` 以取代 `block_cache_block_size`。
  - 新增 `datacache_checksum_enable` 以取代 `block_cache_checksum_enable`。
  - 新增 `datacache_direct_io_enable` 以取代 `block_cache_direct_io_enable`。
  - 新增 `datacache_max_concurrent_inserts` 以取代 `block_cache_max_concurrent_inserts`。
  - 新增 `datacache_max_flying_memory_mb`。
  - 新增 `datacache_engine` 以取代 `block_cache_engine`。
  - 删除 `block_cache_max_parcel_memory_mb`。
  - 删除 `block_cache_report_stats`。
  - 删除 `block_cache_lru_insertion_point`。

  Block Cache 更名为 Data Cache 后，StarRocks 引入一套新的以 `datacache` 为前缀的 BE 参数以取代原有以 `block_cache` 为前缀的参数。升级后，原有参数仍然生效，新参数在启用后将覆盖原有参数。但不支持新老参数混用，否则可能会导致部分配置不生效。未来，StarRocks 计划弃用原有以 `block_cache` 为前缀的参数，所以建议用户使用新的以 `datacache` 为前缀的参数。

- 新增以下 BE 配置项：
  - `spill_max_dir_bytes_ratio`
  - `streaming_agg_limited_memory_size`
  - `streaming_agg_chunk_buffer_size`
- 删除以下 BE 配置项：
  - 动态参数 `tc_use_memory_min`
  - 动态参数 `tc_free_memory_rate`
  - 动态参数 `tc_gc_period`
  - 静态参数 `tc_max_total_thread_cache_bytes`
- 默认值修改：
  - `disable_column_pool` 默认值从 `false` 变为 `true`。
  - `thrift_port` 默认值从 `9060` 变为 `0`。
  - `enable_load_colocate_mv` 默认值从 `false` 变为 `true`。
  - `enable_pindex_minor_compaction` 默认值从 `false` 变为 `true`。

#### 系统变量

- 新增以下会话变量：
  - `enable_per_bucket_optimize`
  - `enable_write_hive_external_table`
  - `hive_temp_staging_dir`
  - `spill_revocable_max_bytes`
  - `thrift_plan_protocol`
- 删除以下会话变量：
  - `enable_pipeline_query_statistic`
  - `enable_deliver_batch_fragments`
- 变量更名：
  - `enable_scan_block_cache` 更名为 `enable_scan_datacache`。
  - `enable_populate_block_cache` 更名为 `enable_populate_datacache`。

#### 保留关键字

新增保留关键字 `OPTIMIZE` 和 `PREPARE`。

### 问题修复

修复了如下问题：

- 调用 libcurl 会引起 BE Crash。[#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 如果 Schema Change 执行时间过长，会因为 Tablet 版本被垃圾回收而失败。[#31376](https://github.com/StarRocks/starrocks/pull/31376)
- 通过文件外部表无法读取存储在 MinIO 上的 Parquet 文件。[#29873] (https://github.com/StarRocks/starrocks/pull/29873)
- `information_schema.columns` 视图中无法正确显示 ARRAY、MAP、STRUCT 类型的字段。[#33431](https://github.com/StarRocks/starrocks/pull/33431)
- Broker Load 导入数据时某些路径形式下会报错 `msg:Fail to parse columnsFromPath, expected: [rec_dt]`。[#32720](https://github.com/StarRocks/starrocks/pull/32720)
- BINARY 或 VARBINARY 类型在 `information_schema.columns` 视图里面的 `DATA_TYPE` 和 `COLUMN_TYPE` 显示为 `unknown`。[#32678](https://github.com/StarRocks/starrocks/pull/32678)
- 包含大量 Union 以及表达式且查询列很多的复杂查询，容易导致单个 FE 节点带宽或者 CPU 短时间内占用较高。[#29888](https://github.com/StarRocks/starrocks/pull/29888)  [#29719](https://github.com/StarRocks/starrocks/pull/29719)
- 某些情况下，物化视图刷新可能会出现死锁问题。[#35736](https://github.com/StarRocks/starrocks/pull/35736)

### 升级注意事项

- 系统默认不开启**随机分桶**优化。如需启用该优化，需要在建表时新增 PROPERTIES `bucket_size`，从而允许系统根据集群信息及导入中的数据量大小按需动态调整 Tablet 数量。但需要注意的是，一旦开启开启该优化后，如需回滚到 v3.1 版本，必须删除开启该优化的表并手动执行元数据 Checkpoint（`ALTER SYSTEM CREATE IMAGE`）成功后才能回滚。
- 从 v3.2.0 开始，StarRocks 禁用了非 Pipeline 查询。因此，在从低版本升级到 v3.2 版本之前，需要先全局打开 Pipeline 引擎（即在 FE 配置文件 **fe.conf** 中添加设置项 `enable_pipeline_engine=true`），否则非 Pipeline 查询会报错。
