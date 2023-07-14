# StarRocks version 3.0

## 3.0.0-rc02

发布日期： 2023 年 4 月 13 日

### 功能优化

- 更新 3.0 版本的 Docker 镜像和相关[部署文档](../quick_start/deploy_with_docker.md)。 ([#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021))
- 提供异步 ETL 命令接口，支持创建异步 INSERT 任务。更多信息，参考[INSERT](../loading/InsertInto.md) 和 [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT%20TASK.md)。 ([#20609](https://github.com/StarRocks/starrocks/issues/20609))
- 物化视图支持批量创建分区，提升物化视图构建时的分区创建效率。([#21167](https://github.com/StarRocks/starrocks/pull/21167))

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

- 支持自增列 [AUTO_INCREMENT](../sql-reference/auto_increment.md)，提供表内全局唯一 ID，可以简化数据管理。
- 支持[导入时自动创建分区和使用分区表达式定义分区规则](../table_design/automatic_partitioning.md)，提高了分区创建的易用性和灵活性。
- Primary Key 模型表支持更丰富的 [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) 和 [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md) 语法，包括使用 CTE 和对多表的引用。
- Broker Load 和 INSERT INTO 增加 Load Profile，支持通过 profile 查看并分析导入作业详情。使用方法与 [查看分析Query Profile](../administration/query_profile.md) 相同。

**数据湖分析**

- [Preview] 支持 Presto/Trino 兼容模式，可以自动改写 Presto/Trino 的 SQL。参见[系统变量](../reference/System_variable.md) 中的 `sql_dialect`。
- [Preview] 支持 [JDBC Catalog](../data_source/catalog/jdbc_catalog.md)。
- 支持使用 [SET CATALOG](../sql-reference/sql-statements/data-definition/SET%20CATALOG.md) 命令来手动选择 Catalog。

**权限与安全**

- 提供了新版的完整 RBAC 功能，支持角色的继承和默认角色。更多信息，参见[权限系统总览](../administration/privilege_overview.md)。
- 提供更多权限管理对象和更细粒度的权限。更多信息，参见[权限项](../administration/privilege_item.md)。

**查询**

<!--- [Preview] 支持大查询的算子落盘，可以在内存不足时利用磁盘空间来保证查询稳定执行成功。-->
- [Query Cache](../using_starrocks/query_cache.md) 支持更多使用场景，包括各种 Broadcast Join、Bucket Shuffle Join 等 Join 场景。
- 支持 [Global UDF](../sql-reference/sql-functions/JAVA_UDF.md)。
- 动态自适应并行度，可以根据查询并发度自适应调节 `pipeline_dop`。

**函数**

- 新增半结构化数据分析相关函数：[map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md)、[map_apply](../sql-reference/sql-functions/map-functions/map_apply.md)、[transform_keys](../sql-reference/sql-functions/map-functions/transform_keys.md)、[transform_values](../sql-reference/sql-functions/map-functions/transform_values.md)。
- [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) 支持 ORDER BY。
- 新增字符串函数 [replace](../sql-reference/sql-functions/string-functions/replace.md)。
- 新增工具函数 [current_role](../sql-reference/sql-functions/utility-functions/current_role.md)。

### 功能优化

**存储与导入**

- 数据导入提供了更丰富的 CSV 格式参数，包括 `skip_header`、`trim_space`、`enclose` 和 `escape`。参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md)、[BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) 和 [ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md)。
- [Primary Key 模型表](../table_design/table_types/primary_key_table.md)解耦了主键和排序键，支持通过 `ORDER BY` 单独指定排序键。
- 优化 Primary Key 模型表在大数据导入、部分列更新、以及开启持久化索引等场景的内存占用。 [#12068](https://github.com/StarRocks/starrocks/pull/12068) [#14187](https://github.com/StarRocks/starrocks/pull/14187) [#15729](https://github.com/StarRocks/starrocks/pull/15729)

**物化视图**

- 优化 [物化视图](../using_starrocks/Materialized_view.md) 的改写能力：
  - 支持 view delta join, 可以改写。
  - 支持对 Outer Join 和 Cross Join 的改写。
  - 优化带分区时 UNION 的 SQL rewrite。
- 完善物化视图的构建能力：支持 CTE、SELECT * 、UNION。
- 优化 [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md) 命令的返回信息。

**查询**

- 完善算子对 Pipeline 的支持，所有算子都支持 Pipeline。
- 完善[大查询定位](../administration/monitor_manage_big_queries.md)。[SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW%20PROCESSLIST.md) 支持查看 CPU 内存信息，增加大查询日志。
- 优化 Outer Join Reorder 能力。
- 优化 SQL 解析阶段的报错信息，查询的报错位置更明确，信息更清晰。

**数据湖分析**

- 优化元数据统计信息收集。
- Hive Catalog、Iceberg Catalog、Hudi Catalog 和 Delta Catalog 支持通过 [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20TABLE.md) 查看外表 Schema 信息，支持通过 [SHOW CREATE CATALOG](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) 查看 Catalog 的创建信息。

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
- SHOW MATERIALIZED VIEW 更名为 [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md)。

### 升级注意事项

**必须从 2.5 版本升级到 3.0，否则无法回滚 （理论上从 2.5 以下的版本升级到 3.0 也支持。但保险起见，请统一从 2.5 升级，降低风险。）**

#### BDBJE 升级

3.0 版本升级了 BDB 库，但是 BDB 不支持回滚，所以需要在回滚之后仍然使用 3.0 里的 BDB 库。

1. 在 FE 包替换成旧版本之后，将 3.0 包里的`fe/lib/starrocks-bdb-je-18.3.13.jar`放到 2.5 版本`fe/lib`目录下。
2. 删除`fe/lib/je-7.*.jar`。

#### 权限系统升级

升级到 3.0 会默认采用新的 RBAC 权限管理系统，升级后只支持回滚到 2.5。

回滚后需要手动执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER%20SYSTEM.md) 命令，触发创建新的 image 并等待该 image 同步给所有的 follower 节点。如不执行该命令，可能会导致部分回滚失败。ALTER SYSTEM CREATE IMAGE 在 2.5.3 版本及以后支持。
