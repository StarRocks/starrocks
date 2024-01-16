---
displayed_sidebar: "Chinese"
---

# StarRocks version 3.0

## 3.0.0-rc02

发布日期： 2023 年 4 月 13 日

### 功能优化

<<<<<<< HEAD
- 更新 3.0 版本的 Docker 镜像和相关[部署文档](../quick_start/deploy_with_docker.md)。 ([#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021))
- 提供异步 ETL 命令接口，支持创建异步 INSERT 任务。更多信息，参考[INSERT](../loading/InsertInto.md) 和 [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT_TASK.md)。 ([#20609](https://github.com/StarRocks/starrocks/issues/20609))
- 物化视图支持批量创建分区，提升物化视图构建时的分区创建效率。([#21167](https://github.com/StarRocks/starrocks/pull/21167))
=======
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
>>>>>>> f7ab4d6fcf ([Doc] change the default value of a be config (#39088))

### 问题修复

修复了以下问题：

- 使用 VARCHAR 作为物化视图分区列时导致 FE 无法正常启动。([#19366](https://github.com/StarRocks/starrocks/issues/19366))
- 窗口函数 [lead](../sql-reference/sql-functions/Window_function.md#使用-lead-窗口函数) 和 [lag](../sql-reference/sql-functions/Window_function.md#使用-lag-窗口函数) 对 IGNORE NULLS 的处理不正确。 ([#21001](https://github.com/StarRocks/starrocks/pull/21001))
- 插入临时分区和自动创建分区发生冲突。（[#21222](https://github.com/StarRocks/starrocks/issues/21222)）

## 3.0.0-rc01

发布日期： 2023 年 3 月 31 日

### 新增特性

**系统架构**

- 支持存算分离架构。可以在 FE 配置文件中开启，开启后数据会持久化到远程对象存储/HDFS 中，本地盘作为缓存使用，可以更灵活的增删节点，支持表级别的缓存生命周期管理。在本地缓存命中的情况下性能可以对齐非存算分离版本。更多信息，参见[部署使用 StarRocks 存算分离集群](../deployment/deploy_shared_data.md)。

**存储和导入**

- 支持自增列属性 [AUTO_INCREMENT](../sql-reference/sql-statements/auto_increment.md)，提供表内全局唯一 ID，简化数据管理。
- 支持[导入时自动创建分区和使用分区表达式定义分区规则](https://docs.starrocks.io/zh-cn/3.0/table_design/automatic_partitioning)，提高了分区创建的易用性和灵活性。
- Primary Key 模型表支持更丰富的 [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) 和 [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md) 语法，包括使用 CTE 和对多表的引用。
- Broker Load 和 INSERT INTO 增加 Load Profile，支持通过 profile 查看并分析导入作业详情。使用方法与 [查看分析Query Profile](../administration/query_profile.md) 相同。

**数据湖分析**

- [Preview] 支持 Presto/Trino 兼容模式，可以自动改写 Presto/Trino 的 SQL。参见[系统变量](../reference/System_variable.md) 中的 `sql_dialect`。
- [Preview] 支持 [JDBC Catalog](../data_source/catalog/jdbc_catalog.md)。
- 支持使用 [SET CATALOG](../sql-reference/sql-statements/data-definition/SET_CATALOG.md) 命令来手动选择 Catalog。

**权限与安全**

- 提供了新版的完整 RBAC 功能，支持角色的继承和默认角色。更多信息，参见[权限系统总览](../administration/privilege_overview.md)。
- 提供更多权限管理对象和更细粒度的权限。更多信息，参见[权限项](../administration/privilege_item.md)。

**查询**

<!--- [Preview] 支持大查询的算子落盘，可以在内存不足时利用磁盘空间来保证查询稳定执行成功。-->
- [Query Cache](../using_starrocks/query_cache.md) 支持更多使用场景，包括各种 Broadcast Join、Bucket Shuffle Join 等 Join 场景。
- 支持 [Global UDF](../sql-reference/sql-functions/JAVA_UDF.md)。
- 动态自适应并行度，可以根据查询并发度自适应调节 `pipeline_dop`。

**函数**

- 新增半结构化数据分析相关函数：[map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md)、[map_apply](../sql-reference/sql-functions/map-functions/map_apply.md)。
- [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) 支持 ORDER BY。
- 新增字符串函数 [replace](../sql-reference/sql-functions/string-functions/replace.md)。
- 新增工具函数 [current_role](../sql-reference/sql-functions/utility-functions/current_role.md)。

### 功能优化

**存储与导入**

- 数据导入提供了更丰富的 CSV 格式参数，包括 `skip_header`、`trim_space`、`enclose` 和 `escape`。参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 和 [ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。
- [Primary Key 模型表](../table_design/table_types/primary_key_table.md)解耦了主键和排序键，支持通过 `ORDER BY` 单独指定排序键。
- 优化 Primary Key 模型表在大数据导入、部分列更新、以及开启持久化索引等场景的内存占用。 [#12068](https://github.com/StarRocks/starrocks/pull/12068) [#14187](https://github.com/StarRocks/starrocks/pull/14187) [#15729](https://github.com/StarRocks/starrocks/pull/15729)

**物化视图**

- 优化 [物化视图](../using_starrocks/Materialized_view.md) 的改写能力：
  - 支持 view delta join, 可以改写。
  - 支持对 Outer Join 和 Cross Join 的改写。
  - 优化带分区时 UNION 的 SQL rewrite。
- 完善物化视图的构建能力：支持 CTE、SELECT * 、UNION。
- 优化 [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md) 命令的返回信息。

**查询**

- 完善算子对 Pipeline 的支持，所有算子都支持 Pipeline。
- 完善[大查询定位](../administration/monitor_manage_big_queries.md)。[SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW_PROCESSLIST.md) 支持查看 CPU 内存信息，增加大查询日志。
- 优化 Outer Join Reorder 能力。
- 优化 SQL 解析阶段的报错信息，查询的报错位置更明确，信息更清晰。

**数据湖分析**

- 优化元数据统计信息收集。
- Hive Catalog、Iceberg Catalog、Hudi Catalog 和 Delta Catalog 支持通过 [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md) 查看外表 Schema 信息，支持通过 [SHOW CREATE CATALOG](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md) 查看 Catalog 的创建信息。

**函数**

窗口函数 [lead](../sql-reference/sql-functions/Window_function.md#使用-lead-窗口函数)、[lag](../sql-reference/sql-functions/Window_function.md#使用-lag-窗口函数) 支持 IGNORE NULLS。

### 问题修复

- StarRocks 源文件的许可 header 中部分 url 无法访问。#[2224](https://github.com/StarRocks/starrocks/issues/2224)
- SELECT 查询出现 unknown error。 #[19731](https://github.com/StarRocks/starrocks/issues/19731)
- 支持 SHOW/SET CHARACTER。 #[17480](https://github.com/StarRocks/starrocks/issues/17480)
- 当导入的内容超过 StarRocks 字段长度时，ErrorURL 提示信息不准确。提示源端字段为空，而不是内容超长。 #[14](https://github.com/StarRocks/DataX/issues/14)
- 支持 show full fields from 'table'。# [17233](https://github.com/StarRocks/starrocks/issues/17233)
- 分区修剪导致 MV 改写失败。 #[14641](https://github.com/StarRocks/starrocks/issues/14641)
- 当 MV 创建语句包含 count(distinct) 且 count(distinct) 作用在分布列上时，MV 改写失败。 #[16558](https://github.com/StarRocks/starrocks/issues/16558)

### 行为变更

- RBAC 升级以后会兼容之前的用户和权限，但是 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) 等相关语法有大幅改动。
- SHOW MATERIALIZED VIEW 更名为 [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md)。

### 升级注意事项

**必须从 2.5 版本升级到 3.0，否则无法回滚 （理论上从 2.5 以下的版本升级到 3.0 也支持。但保险起见，请统一从 2.5 升级，降低风险。）**

#### BDBJE 升级

3.0 版本升级了 BDB 库，但是 BDB 不支持回滚，所以需要在回滚之后仍然使用 3.0 里的 BDB 库。

1. 在 FE 包替换成旧版本之后，将 3.0 包里的`fe/lib/starrocks-bdb-je-18.3.13.jar`放到 2.5 版本`fe/lib`目录下。
2. 删除`fe/lib/je-7.*.jar`。

#### 权限系统升级

升级到 3.0 会默认采用新的 RBAC 权限管理系统，升级后只支持回滚到 2.5。

回滚后需要手动执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 命令，触发创建新的 image 并等待该 image 同步给所有的 follower 节点。如不执行该命令，可能会导致部分回滚失败。ALTER SYSTEM CREATE IMAGE 在 2.5.3 版本及以后支持。
