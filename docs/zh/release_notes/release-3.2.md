---
displayed_sidebar: docs
---

# StarRocks version 3.2

## 3.2.15

发布日期：2025 年 2 月 14 日

### 新增特性

- 窗口函数支持 `max_by` 和 `min_by`。[#54961](https://github.com/StarRocks/starrocks/pull/54961)

### 功能优化

- 新增 StarClient 超时参数。[#54496](https://github.com/StarRocks/starrocks/pull/54496)
  - star_client_read_timeout_seconds
  - star_client_list_timeout_seconds
  - star_client_write_timeout_seconds
- List 分区表在执行 DELETE 语句时支持分区裁剪。[#55400](https://github.com/StarRocks/starrocks/pull/55400)

### 问题修复

修复了如下问题：

- Stream Load 调度到 Alive 状态为 false 的节点时，导入失败。[#55371](https://github.com/StarRocks/starrocks/pull/55371)
- Stream Load 部分列写入主键表报错。[#53403](https://github.com/StarRocks/starrocks/pull/55430)
- BE 节点重启后 bRPC 持续报错。[#40229](https://github.com/StarRocks/starrocks/pull/40229)

## 3.2.14

发布日期：2025 年 1 月 8 日

### 功能优化

- 支持收集 Paimon 表的统计信息。[#52858](https://github.com/StarRocks/starrocks/pull/52858)
- JSON 指标中添加节点信息和直方图指标。[#53735](https://github.com/StarRocks/starrocks/pull/53735)

### 问题修复

修复了如下问题：

- 主键表索引的 Score 没有在 Commit 阶段进行更新。[#41737](https://github.com/StarRocks/starrocks/pull/41737)
- 在启用低基数优化时，`max(count(distinct))` 执行计划错误。[#53403](https://github.com/StarRocks/starrocks/pull/53403)
- 当 List 分区列含有 NULL 值时，查询分区列的 Min/Max 值会导致分区裁剪错误。[#53235](https://github.com/StarRocks/starrocks/pull/53235)
- 使用 HDFS 备份数据时上传重试失败。[#53679](https://github.com/StarRocks/starrocks/pull/53679)

## 3.2.13

发布日期：2024 年 12 月 13 日

### 功能优化

- 支持对单个表设置禁止进行 Base Compaction 的时间范围。[#50120](https://github.com/StarRocks/starrocks/pull/50120)

### 问题修复

修复了如下问题：

- 执行 SHOW ROUTINE LOAD 后 `loadRowsRate` 字段返回为 `0`。[#52151](https://github.com/StarRocks/starrocks/pull/52151)
- 函数 `Files()` 读取文件时读取未被查询的列。 [#52210](https://github.com/StarRocks/starrocks/pull/52210)
- Prometheus 不能解析含有特殊符号名称的物化视图相关指标（当前物化视图统计指标支持 Tag）。[#52782](https://github.com/StarRocks/starrocks/pull/52782)
- 函数 `array_map` 导致 BE Crash。[#52909](https://github.com/StarRocks/starrocks/pull/52909)
- Metadata Cache 导致 BE Crash 问题。[#52968](https://github.com/StarRocks/starrocks/pull/52968)
- Routine Load 因事务过期而导致任务取消（当前仅有数据库或表不存在任务才会被取消）。[#50334](https://github.com/StarRocks/starrocks/pull/50334)
- 通过 HTTP 1.0 提交的 Stream Load 失败。[#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- 一些和 Glue、S3 集成相关的问题：[#48433](https://github.com/StarRocks/starrocks/pull/48433)
  - 部分报错信息未能展示根源报错原因。
  - 使用 Glue 作为元数据服务时，写入分区列为 SRTING 类型的 Hive 分区表的报错。
  - 删除 Hive 表时，用户权限不足但系统并未报错。
- 物化视图属性 `storage_cooldown_time` 设置为 `maximum` 不生效。[#52079](https://github.com/StarRocks/starrocks/pull/52079)

## 3.2.12

发布日期：2024 年 10 月 23 日

### 功能优化

- 优化在部分复杂查询场景下 BE 内存分配和统计，避免 OOM。[#51382](https://github.com/StarRocks/starrocks/pull/51382)
- 优化在 Schema Change 场景下 FE 的内存使用。[#50855](https://github.com/StarRocks/starrocks/pull/50855)
- 优化从 Follower FE 节点查询系统定义视图 `information_schema.routine_load_jobs` 时 Job 状态的展示。[#51763](https://github.com/StarRocks/starrocks/pull/51763)
- 支持备份还原 List 分区表。[#51993](https://github.com/StarRocks/starrocks/pull/51993)

### 问题修复

修复了如下问题：

- 写入 Hive 失败后，报错信息丢失。[#33167](https://github.com/StarRocks/starrocks/pull/33167)
- 函数 `array_map` 在常量参数过多时导致 Crash。[#51244](https://github.com/StarRocks/starrocks/pull/51244)
- 表达式分区表的分区列里有特殊字符会导致 FE CheckPoint 失败。[#51677](https://github.com/StarRocks/starrocks/pull/51677)
- 访问系统定义视图 `information_schema.fe_locks` 导致 Crash。[#51742](https://github.com/StarRocks/starrocks/pull/51742)
- 查询生成列报错。[#51755](https://github.com/StarRocks/starrocks/pull/51755)
- 表名存在特殊字符时执行 Optimize Table 失败。[#51755](https://github.com/StarRocks/starrocks/pull/51755)
- 某些场景下 Tablet 无法 Balance。[#51828](https://github.com/StarRocks/starrocks/pull/51828)

### 行为变更

- 支持动态修改备份还原相关的参数。[#52111](https://github.com/StarRocks/starrocks/pull/52111)

## 3.2.11

发布日期：2024 年 9 月 9 日

### 功能优化

- 对 Files()、PIPE 相关操作中的敏感信息进行脱敏。[#47629](https://github.com/StarRocks/starrocks/pull/47629)
- 通过 Files() 读取 Parquet 文件支持自动推导 STRUCT 类型。[#50481](https://github.com/StarRocks/starrocks/pull/50481)

### 问题修复

修复了如下问题：

- Equi-join 查询由于全局字典未改写导致报错。[#50690](https://github.com/StarRocks/starrocks/pull/50690)
- Tablet Clone 时 FE 侧死循环导致报错 "version has been compacted"。[#50561](https://github.com/StarRocks/starrocks/pull/50561)
- 数据副本基于 Label 分布后，不健康副本修复调度错误。[#50331](https://github.com/StarRocks/starrocks/pull/50331)
- 统计信息收集日志中报错 "Unknown column '%s' in '%s"。[#50785](https://github.com/StarRocks/starrocks/pull/50785)
- Files() 读取 Parquet 格式文件中复杂类型 TIMESTAMP 时使用的 Timezone 不正确。[#50448](https://github.com/StarRocks/starrocks/pull/50448)

### 行为变更

- 从 v3.3.x 版本降级至 v3.2.11 版本，如果存在不兼容的元数据信息，系统将直接忽略。[#49636](https://github.com/StarRocks/starrocks/pull/49636)

## 3.2.10

发布日期：2024 年 8 月 23 日

### 功能优化

- Files() 读取 Parquet 文件中的 `logical_type` 为 JSON 的 BYTE_ARRAY 数据自动转换为 StarRocks 中的 JSON 类型。[#49385](https://github.com/StarRocks/starrocks/pull/49385)
- 优化 Files() 在缺失 Access Key ID 和 Secret Access Key 时的报错信息。[#49090](https://github.com/StarRocks/starrocks/pull/49090)
- `information_schema.columns` 支持 `GENERATION_EXPRESSION` 字段。[#49734](https://github.com/StarRocks/starrocks/pull/49734)

### 问题修复

修复了如下问题：

- 在 v3.3 存算分离集群中为主键表设置 Property `"persistent_index_type" = "CLOUD_NATIVE"` 后，将集群降级到 v3.2 导致 Crash。[#48149](https://github.com/StarRocks/starrocks/pull/48149)
- SELECT INTO OUTFILE 导出数据至 CSV 文件可能导致数据不一致。[#48052](https://github.com/StarRocks/starrocks/pull/48052)
- 并发执行查询时查询失败。[#48180](https://github.com/StarRocks/starrocks/pull/48180)
- Plan 阶段超时但不退出，导致的查询卡住。[#48405](https://github.com/StarRocks/starrocks/pull/48405)
- 在旧版本中为主键表关闭索引压缩功能后，升级至 v3.1.13 或 v3.2.9，访问索引的 `page_off` 信息时数组越界导致 Crash。[#48230](https://github.com/StarRocks/starrocks/pull/48230)
- 并发执行 ADD/DROP COLUMN 操作导致 BE Crash。[#49355](https://github.com/StarRocks/starrocks/pull/49355)
- 在 aarch64 架构下查询 ORC 格式文件中的 TINYINT 类型负数显示为 None。[#49517](https://github.com/StarRocks/starrocks/pull/49517)
- 当写盘失败时，主键表持久化主键索引的 `l0` 可能会因为无法捕捉错误导致数据丢失。[#48045](https://github.com/StarRocks/starrocks/pull/48045)
- 主键表部分列更新在大量数据更新的场景下写入失败。[#49054](https://github.com/StarRocks/starrocks/pull/49054)
-  v3.3.0 存算分离集群降级到 v3.2.9 后，Fast Schema Evolution 导致 BE Crash。[#42737](https://github.com/StarRocks/starrocks/pull/42737)
- `partition_linve_nubmer` 不生效。[#49213](https://github.com/StarRocks/starrocks/pull/49213)
- 主键表索引落盘和 Compaction 并发的冲突可能导致 Clone 失败。[#49341](https://github.com/StarRocks/starrocks/pull/49341)
- 通过 ALTER TABLE 修改 `partition_linve_nubmer` 不生效。[#49437](https://github.com/StarRocks/starrocks/pull/49437)
- CTE distinct grouping sets 查询改写生成错误计划。[#48765](https://github.com/StarRocks/starrocks/pull/48765)
- RPC 失败导致线程池污染。[#49619](https://github.com/StarRocks/starrocks/pull/49619)
- 通过 PIPE 导入 AWS S3 中的文件时访问鉴权失败。[#49837](https://github.com/StarRocks/starrocks/pull/49837)

### 行为变更

- FE 启动脚本中增加 `meta` 目录检查，如果不存在则自动创建 `meta` 目录。[#48940](https://github.com/StarRocks/starrocks/pull/48940)
- 增加导入内存限制参数 `load_process_max_memory_hard_limit_ratio`，当导入内存超过使用限制后，后续导入任务将失败。[#48495](https://github.com/StarRocks/starrocks/pull/48495)

## 3.2.9

发布日期：2024 年 7 月 11 日

### 新增特性

- Paimon 外表支持 DELETE Vector。  [#45866](https://github.com/StarRocks/starrocks/issues/45866)
- 支持通过 Apache Ranger 实现 Column 级别权限控制。[#47702](https://github.com/StarRocks/starrocks/pull/47702)
- Stream Load 支持在导入时将 JSON 字符串自动转换成 STRUCT/MAP/ARRAY 类型数据。[#45406](https://github.com/StarRocks/starrocks/pull/45406)
- JDBC Catalog支持 Oracle 和 SQL Server。[#35691](https://github.com/StarRocks/starrocks/issues/35691)

### 功能优化

- 优化权限管理，限制 `user_admin` 角色的用户修改 root 密码。[#47801](https://github.com/StarRocks/starrocks/pull/47801)
- Stream Load 支持将 `\t` 和 `\n` 分别作为行列分割符，无需转成对应的十六进制 ASCII 码。[#47302](https://github.com/StarRocks/starrocks/pull/47302)
- 降低导入时的内存占用。[#47047](https://github.com/StarRocks/starrocks/pull/47047)
- 在审计日志中对 Files() 函数的认证信息进行脱敏处理。[#46893](https://github.com/StarRocks/starrocks/pull/46893)
- Hive 外表支持 `skip.header.line.count` 属性。 [#47001](https://github.com/StarRocks/starrocks/pull/47001)
- JDBC Catalog 支持更多的数据类型。[#47618](https://github.com/StarRocks/starrocks/pull/47618)

### 问题修复

修复了如下问题：

- 存算分离集群从 v3.2.x 升级到 v3.3.0 后回滚，ALTER TABLE ADD COLUMN 导致 BE Crash。[#47826](https://github.com/StarRocks/starrocks/pull/47826)
- 通过 SUBMIT TASK 发起的任务 QueryDetail 接口显示状态一直为 Running。[#47619](https://github.com/StarRocks/starrocks/pull/47619)
- 向 FE Leader 节点转发查询导致空指针。[#47559](https://github.com/StarRocks/starrocks/pull/47559)
- 执行 SHOW MATERIALIZED VIEWS 时带 WHERE 条件导致空指针。[#47811](https://github.com/StarRocks/starrocks/pull/47811)
- 存算一体集群中主键表 Vertical Compaction 失败。[#47192](https://github.com/StarRocks/starrocks/pull/47192)
- 写入 Hive 或 Iceberg 表时没有正确处理 I/O Error。[#46979](https://github.com/StarRocks/starrocks/pull/46979)
- 给表属性赋值时添加空格不生效。[#47119](https://github.com/StarRocks/starrocks/pull/47119)
- 对主键表并发执行迁移操作和 Index Compaction 时导致 BE Crash。[#46675](https://github.com/StarRocks/starrocks/pull/46675)

### 行为变更

- 修改 `JAVA_OPTS` 参数继承顺序，如果使用 JDK_9 或 JDK_11 以外的版本，用户需直接在 `JAVA_OPTS` 中配置。[#47495](https://github.com/StarRocks/starrocks/pull/47495)
- 用户创建非分区表但未设置分桶数时，系统自动设置的分桶数最小值修改为 `16`（原来的规则是 `2 * BE 或 CN 数量`，也即最小会创建 2 个 Tablet）。如果是小数据且想要更小的分桶数，需要手动设置。[#47005](https://github.com/StarRocks/starrocks/pull/47005)
- 用户创建分区表但未设置分桶数时，当分区数量超过 5 个后，系统自动设置分桶数的规则更改为 `max(2 * BE 或 CN 数量, 根据最大历史分区数据量计算得出的分桶数)`。原来的规则是根据最大历史分区数据量计算分桶数。[#47949](https://github.com/StarRocks/starrocks/pull/47949)

## 3.2.8

发布日期：2024 年 6 月 7 日

### 新增特性

- **[使用标签管理 BE](https://docs.starrocks.io/zh/docs/3.2/administration/management/resource_management/be_label/)**：支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，以保证数据在机架或数据中心等之间均匀分布，应对某些机架断电或数据中心故障情况下的灾备需求。[#38833](https://github.com/StarRocks/starrocks/pull/38833)

### 问题修复

修复了如下问题：

- 基于 str2date 函数的表达式分区表使用 DELETE 语句删除数据报错。[#45939](https://github.com/StarRocks/starrocks/pull/45939)
- 跨集群迁移工具因获取不到源集群 Schema 信息而导致目标集群 BE Crash。[#46068](https://github.com/StarRocks/starrocks/pull/46068)
- 查询使用非确定性函数时报错 `Multiple entries with same key`。[#46602](https://github.com/StarRocks/starrocks/pull/46602)

## 3.2.7

发布日期：2024 年 5 月 24 日

### 新增特性

- Stream Load 支持在传输过程中对数据进行压缩，减少网络带宽开销。可以通过 `compression` 或 `Content-Encoding` 参数指定不同的压缩方式，支持 GZIP、BZIP2、LZ4_FRAME、ZSTD 压缩算法。[#43732](https://github.com/StarRocks/starrocks/pull/43732)
- 优化了存算分离集群的垃圾回收机制，支持手动对表或分区进行 Compaction 操作，可以更高效的回收对象存储上的数据。[#39532](https://github.com/StarRocks/starrocks/issues/39532)
- 支持从 StarRocks 读取 ARRAY、MAP 和 STRUCT 等复杂类型的数据，并以 Arrow 格式可提供给 Flink connector 读取使用。[#42932](https://github.com/StarRocks/starrocks/pull/42932) [#347](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/347)
- 支持查询时异步填充 Data Cache，从而减少缓存填充对首次查询性能影响。[#40489](https://github.com/StarRocks/starrocks/pull/40489)
- 外表 ANALYZE TABLE 命令支持收集直方图统计信息，可以有效应对数据倾斜场景。参见 [CBO 统计信息](https://docs.starrocks.io/zh/docs/3.2/using_starrocks/Cost_based_optimizer/#%E9%87%87%E9%9B%86-hiveiceberghudi-%E8%A1%A8%E7%9A%84%E7%BB%9F%E8%AE%A1%E4%BF%A1%E6%81%AF)。[#42693](https://github.com/StarRocks/starrocks/pull/42693)
- Lateral Join 结合 [UNNEST](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/array-functions/unnest/) 支持 LEFT JOIN。[#43973](https://github.com/StarRocks/starrocks/pull/43973)
- Query Pool 内存支持通过 BE 静态参数 `query_pool_spill_mem_limit_threshold` 配置 Spill 阈值，如果超过阈值，查询可以通过中间结果落盘的方式降低内存占用减少 OOM。[#44063](https://github.com/StarRocks/starrocks/pull/44063)
- 支持基于 Hive View 创建异步物化视图。[#45085](https://github.com/StarRocks/starrocks/pull/45085)

### 功能优化

- 优化 Broker Load 任务导入 HDFS 数据时对应路径下没有数据时的报错信息。[#43839](https://github.com/StarRocks/starrocks/pull/43839)
- 优化 Files 函数读取 S3 数据场景下没有配置 Access Key 和 Secret Key 的报错信息。[#42450](https://github.com/StarRocks/starrocks/pull/42450)
- 优化 Broker Load 导入时任何分区下均没有数据导入的报错信息。[#44292](https://github.com/StarRocks/starrocks/pull/44292)
- 优化 INSERT INTO SELECT 导入时，目标表与 SELECT 列数据不匹配的场景下的报错信息。[#44331](https://github.com/StarRocks/starrocks/pull/44331)

### 问题修复

修复了如下问题：

- BITMAP 类型在并发读写场景下可能会导致 BE Crash。[#44167](https://github.com/StarRocks/starrocks/pull/44167)
- 主键索引可能会导致 BE Crash。[#43793](https://github.com/StarRocks/starrocks/pull/43793) [#43569](https://github.com/StarRocks/starrocks/pull/43569) [#44034](https://github.com/StarRocks/starrocks/pull/44034)
- str_to_map 函数并发场景下可能会导致 BE Crash。[#43901](https://github.com/StarRocks/starrocks/pull/43901)
- Apache Ranger 的 Masking 策略下，在查询中添加表的别名报错。[#44445](https://github.com/StarRocks/starrocks/pull/44445)
- 存算分离模式下执行过程中某个节点异常，无法路由到备用节点。同时针对该问题，优化部分报错信息。[#43489](https://github.com/StarRocks/starrocks/pull/43489)
- 在容器环境下获取内存信息不正确。[#43225](https://github.com/StarRocks/starrocks/issues/43225)
- 取消 INSERT 任务时抛出异常。[#44239](https://github.com/StarRocks/starrocks/pull/44239)
- 无法动态创建基于表达式的动态分区。[#44163](https://github.com/StarRocks/starrocks/pull/44163)
- 创建分区可能导致 FE 死锁。[#44974](https://github.com/StarRocks/starrocks/pull/44974)

## 3.2.6

发布日期：2024 年 4 月 18 日

### 问题修复

修复了如下问题：

- 外表权限丢失。[#44030](https://github.com/StarRocks/starrocks/pull/44030)


## 3.2.5 (已下线)

发布日期：2024 年 4 月 12 日

:::tip

此版本因存在 Hive/Iceberg catalog 等外表权限相关问题已经下线。

- 问题：查询 Hive/Iceberg catalog 等外表时报错无权限，权限丢失，但用 `SHOW GRANTS` 查询时对应的权限是存在的。
- 影响范围：对于不涉及 Hive/Iceberg catalog 等外表权限的查询，不受影响。
- 临时解决方法：在对 Hive/Iceberg catalog 等外表进行重新授权后，查询可以恢复正常。但是 `SHOW GRANTS` 会出现重复的权限条目。后续在升级 3.2.6 后，通过 `REVOKE` 操作删除其中一条即可。

:::

### 新增特性

- 支持 [dict_mapping](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/dict-functions/dict_mapping/) 列属性，能够极大地方便构建全局字典中的数据导入过程，用以加速计算精确去重等。

### 行为变更

- JSON 中的 null 值通过 `IS NULL` 等方式判断时，修改为按照 SQL 的 NULL 值计算。即，`SELECT parse_json('{"a": null}') -> 'a' IS NULL` 会返回 `true`（原来是返回 `false` ）。 [#42765](https://github.com/StarRocks/starrocks/pull/42765)

### 功能优化

- 优化 FILES 表函数自动探测文件 Schema 时的列类型合并规则。当不同文件中存在同名但类型不同的列时，FILES 会尽可能将更大粒度的类型作为最终的探测类型，比如分别为 FLOAT 和 INT 类型的同名列，最终返回 DOUBLE 类型。[#40959](https://github.com/StarRocks/starrocks/pull/40959)
- 主键表支持 Size-tiered Compaction 以减少 I/O 放大问题。[#41130](https://github.com/StarRocks/starrocks/pull/41130)
- 通过 Broker Load 导入 ORC 格式的数据，在 TIMESTAMP 类型的数据转化为 StarRocks 中的 DATETIME 类型的数据时，新增支持保留微秒信息。[#42179](https://github.com/StarRocks/starrocks/pull/42179)
- 优化 Routine Load 报错信息。[#41306](https://github.com/StarRocks/starrocks/pull/41306)
- 优化 FILES 表函数转换数据类型失败时的报错信息。[#42717](https://github.com/StarRocks/starrocks/pull/42717)

### 问题修复

修复了如下问题：

- 删除系统视图后 FE 启动失败。修复后禁止删除系统视图。[#43552](https://github.com/StarRocks/starrocks/pull/43552)
- 主键表 Sort Key 存在重复列情况下 BE Crash。修复后禁止 Sort Key 存在重复列。[#43206](https://github.com/StarRocks/starrocks/pull/43206)
- 当 JSON 对象为 NULL 时，to_json 函数返回错误。修复后，当 JSON 对象为 NULL 时，该函数返回 NULL 。[#42171](https://github.com/StarRocks/starrocks/pull/42171)
- 对于存算分离中的主键表，本地持久化索引的垃圾回收 (Garbage Collection) 和淘汰线程对 CN 节点没有生效，导致无用数据没有被删除。[#41955](https://github.com/StarRocks/starrocks/pull/41955)
- 存算分离模式下，修改主键表 `enable_persistent_index` 属性报错。[#42890](https://github.com/StarRocks/starrocks/pull/42890)
- 存算分离模式下，主键表部分列更新时未更新列的值被修改为 NULL。[#42355](https://github.com/StarRocks/starrocks/pull/42355)
- 物化视图在基表为逻辑视图情况下改写失败。[#42173](https://github.com/StarRocks/starrocks/pull/42173)
- 跨集群同步工具在迁移主键表到存算分离集群时 CN Crash。[#42260](https://github.com/StarRocks/starrocks/pull/42260)
- 外表物化视图范围分区不连续。[#41957](https://github.com/StarRocks/starrocks/pull/41957)

## 3.2.4 (已下线)

发布日期：2024 年 3 月 12 日

:::tip

此版本因存在 Hive/Iceberg catalog 等外表权限相关问题已经下线。

- 问题：查询 Hive/Iceberg catalog 等外表时报错无权限，权限丢失，但用 `SHOW GRANTS` 查询时对应的权限是存在的。
- 影响范围：对于不涉及 Hive/Iceberg catalog 等外表权限的查询，不受影响。
- 临时解决方法：在对 Hive/Iceberg catalog 等外表进行重新授权后，查询可以恢复正常。但是 `SHOW GRANTS` 会出现重复的权限条目。后续在升级 3.2.6 后，通过 `REVOKE` 操作删除其中一条即可。

:::

### 新增特性

- 存算分离集群中的云原生主键表支持 Size-tiered 模式 Compaction，以减轻导入较多小文件时 Compaction 的写放大问题。[#41034](https://github.com/StarRocks/starrocks/pull/41034)
- Storage Volume 支持 HDFS 的参数化配置，包括 Simple 认证方式支持配置 username，Kerberos 认证，NameNode HA，以及 ViewFS。 
- 新增日期函数 `milliseconds_diff`。[#38171](https://github.com/StarRocks/starrocks/pull/38171)
- 新增 Session 变量 `catalog`，用于指定当前会话所在的 Catalog。[#41329](https://github.com/StarRocks/starrocks/pull/41329)
- Hint 中支持设置[用户自定义变量](https://docs.starrocks.io/zh/docs/3.2/administration/Query_planning/#%E7%94%A8%E6%88%B7%E8%87%AA%E5%AE%9A%E4%B9%89%E5%8F%98%E9%87%8F-hint)。[#40746](https://github.com/StarRocks/starrocks/pull/40746)
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

- 支持通过 Java Native Interface（JNI）读取 Avro、SequenceFile 以及 RCFile 格式的 [Hive Catalog](https://docs.starrocks.io/zh/docs/3.2/data_source/catalog/hive_catalog/) 表和文件外部表。

#### 物化视图

- `sys` 数据库新增 `object_dependencies` 视图，可用于查询异步物化视图血缘关系。 [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- 支持创建带有 WHERE 子句的同步物化视图。
- Iceberg 异步物化视图支持分区级别的增量刷新。
- [Preview] 支持基于 Paimon Catalog 外表创建异步物化视图，支持分区级别刷新。

#### 查询和函数

- 支持预处理语句（Prepared Statement）。预处理语句可以提高处理高并发点查查询的性能，同时有效地防止 SQL 注入。
- 新增如下 Bitmap 函数：[subdivide_bitmap](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/)、[bitmap_from_binary](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/)、[bitmap_to_binary](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。
- 新增如下 Array 函数：[array_unique_agg](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_unique_agg/)。

#### 监控指标

- 新增了监控指标 `max_tablet_rowset_num`（用于设置 Rowset 的最大数量），可以协助提前发现 Compaction 是否会出问题并及时干预，减少报错信息“too many versions”的出现。[#36539](https://github.com/StarRocks/starrocks/pull/36539)

### 参数变更

- 新增 BE 配置项 `enable_stream_load_verbose_log`，默认取值是 `false`，打开后日志中可以记录 Stream Load 的 HTTP 请求和响应信息，方便出现问题后的定位调试。[#36113](https://github.com/StarRocks/starrocks/pull/36113)

### 功能优化

- 使用 JDK8 时，默认 GC 算法采用 G1。 [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- 系统变量 [sql_mode](https://docs.starrocks.io/zh/docs/3.2/reference/System_variable/#sql_mode) 增加 `GROUP_CONCAT_LEGACY` 选项，用以兼容 [group_concat](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/string-functions/group_concat/) 函数在 2.5（不含）版本之前的实现逻辑。[#36150](https://github.com/StarRocks/starrocks/pull/36150)
- 隐藏了审计日志（Audit Log）中 [Broker Load 作业里 AWS S3](https://docs.starrocks.io/zh/docs/3.2/loading/s3/) 的鉴权信息 `aws.s3.access_key` 和 `aws.s3.access_secret`。[#36571](https://github.com/StarRocks/starrocks/pull/36571)
- 在 `be_tablets` 表中增加 `INDEX_DISK` 记录持久化索引的磁盘使用量，单位是 Bytes。[#35615](https://github.com/StarRocks/starrocks/pull/35615)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) 返回结果中增加 `OtherMsg`，展示最后一个失败的任务的相关信息。[#35806](https://github.com/StarRocks/starrocks/pull/35806)

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

- 支持[主键表](https://docs.starrocks.io/zh/docs/3.2/table_design/table_types/primary_key_table/)的索引在本地磁盘的持久化。
- 支持 Data Cache 在多磁盘间均匀分布。

#### 物化视图

**异步物化视图**

- 物化视图支持 Query Dump。
- 物化视图的刷新默认开启中间结果落盘，降低刷新的内存消耗。

#### 数据湖分析

- 支持在 [Hive Catalog](https://docs.starrocks.io/zh/docs/3.2/data_source/catalog/hive_catalog/) 中创建、删除数据库以及 Managed Table，支持使用 INSERT 或 INSERT OVERWRITE 导出数据到 Hive 的 Managed Table。
- 支持 [Unified Catalog](https://docs.starrocks.io/zh/docs/3.2/data_source/catalog/unified_catalog/)。如果同一个 Hive Metastore 或 AWS Glue 元数据服务包含多种表格式（Hive、Iceberg、Hudi、Delta Lake 等），则可以通过 Unified Catalog 进行统一访问。
- 支持通过 ANALYZE TABLE 收集 Hive 和 Iceberg 表的统计信息，并存储在 StaRocks 内部，方便优化加速后续查询。
- 支持外表的 Information Schema，为外部系统（如BI）与 StarRocks 的交互提供更多便利。

#### 导入、导出和存储

- 使用表函数 [FILES()](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/table-functions/files/) 进行数据导入新增以下功能：
  - 支持导入 Azure 和 GCP 中的 Parquet 或 ORC 格式文件的数据。
  - 支持 `columns_from_path` 参数，能够从文件路径中提取字段信息。
  - 支持导入复杂类型（JSON、ARRAY、MAP 及 STRUCT）的数据。
- 支持使用 INSERT INTO FILES() 语句将数据导出至 AWS S3 或 HDFS 中的 Parquet 格式的文件。有关详细说明，请参见[使用 INSERT INTO FILES 导出数据](https://docs.starrocks.io/zh/docs/3.2/unloading/unload_using_insert_into_files/)。
- 通过增强 ALTER TABLE 命令提供了 [optimize table 功能](https://docs.starrocks.io/zh/docs/3.2/table_design/Data_distribution#建表后优化数据分布自-32)，可以调整表结构并重组数据，以优化查询和导入的性能。支持的调整项包括：分桶方式和分桶数、排序键，以及可以只调整部分分区的分桶数。
- 支持使用 PIPE 导入方式从[云存储 S3](https://docs.starrocks.io/zh/docs/3.2/loading/s3/#通过-pipe-导入) 或 [HDFS](https://docs.starrocks.io/zh/docs/3.2/loading/hdfs_load/#通过-pipe-导入) 中导入大规模数据和持续导入数据。在导入大规模数据时，PIPE 命令会自动根据导入数据大小和导入文件数量将一个大导入任务拆分成很多个小导入任务穿行运行，降低任务出错重试的代价、减少导入中对系统资源的占用，提升数据导入的稳定性。同时，PIPE 也能不断监听云存储目录中的新增文件或文件内容修改，并自动将变化的数据文件数据拆分成一个个小的导入任务，持续地将新数据导入到目标表中。

#### 查询

- 支持 [HTTP SQL API](https://docs.starrocks.io/zh/docs/3.2/reference/HTTP_API/SQL/)。用户可以通过 HTTP 方式访问 StarRocks 数据，执行 SELECT、SHOW、EXPLAIN 或 KILL 操作。
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

支持通过 [Apache Ranger](https://docs.starrocks.io/zh/docs/3.2/administration/ranger_plugin/) 实现访问控制，提供更高层次的数据安全保障，并且允许复用原有的外部数据源 Service。StarRocks 集成 Apache Ranger 后可以实现以下权限控制方式：

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
  - 有关详细信息，请参见[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/)。
- 增加分区列一致性检查：当创建分区物化视图时，如物化视图的查询中涉及带分区的窗口函数，则窗口函数的分区列需要与物化视图的分区列一致。

#### 导入、导出和存储  

- 优化主键表（Primary Key）表持久化索引功能，优化内存使用逻辑，同时降低 I/O 的读写放大。
- 主键表（Primary Key）表支持本地多块磁盘间数据均衡。
- 分区中数据可以随着时间推移自动进行降冷操作（List 分区方式暂不支持）。相对原来的设置，更方便进行分区冷热管理。有关详细信息，请参见[设置数据的初始存储介质、自动降冷时间](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#设置数据的初始存储介质自动降冷时间和副本数)。
- 主键表数据写入时的 Publish 过程由异步改为同步，导入作业成功返回后数据立即可见。有关详细信息，请参见 [enable_sync_publish](https://docs.starrocks.io/zh/docs/3.2/administration/FE_configuration#enable_sync_publish)。
- 支持 Fast Schema Evolution 模式，由表属性 [`fast_schema_evolution`](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#设置-fast-schema-evolution) 控制。启用该模式可以在进行加减列变更时提高执行速度并降低资源使用。该属性默认值是 `false`（即关闭）。不支持建表后通过 ALTER TABLE 修改该表属性。
- 对于采用随机分桶的**明细表**，系统进行了优化，会根据集群信息及导入中的数据量大小[按需动态调整 Tablet 数量](https://docs.starrocks.io/zh/docs/3.2/table_design/Data_distribution#设置分桶数量)。

#### 查询

- Metabase 和 Superset 兼容性提升，支持集成 External Catalog。

#### SQL 语句和函数

- [array_agg](https://docs.starrocks.io/zh/docs/3.2/sql-reference/sql-functions/array-functions/array_agg/) 支持使用 DISTINCT 关键词。
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
