---
displayed_sidebar: "Chinese"
---

# Catalogs、databases 和 tables

StarRocks 使用 Internal Catalog 来管理内部数据，使用 External Catalog 来连接数据湖中的数据。存储在 StarRocks 中的数据都包含在 Internal Catalog 下，Internal Catalog 可以包含一个或多个数据库。数据库用于存储、管理和操作 StarRocks 中的数据，可用于管理多种对象，包括表、物化视图、视图等。StarRocks 采用权限系统来管理数据访问权限，定义了用户对哪些对象可以执行哪些操作，提高数据安全性。

![img](../assets/table_design/Catalog_db_tbl.png)

## [Catalogs](../data_source/catalog/catalog_overview.md)

Catalog 分为 Internal catalog 和 External catalog。Internal catalog 是内部数据目录，用于管理导入至 StarRocks 中的数据以及内部的物化视图等。每个集群都有且只有一个名为 `default_catalog` 的 Internal catalog，包含一个或多个数据库。StarRocks 作为数据仓库存储数据，能够显著提高查询性能，尤其应对大规模数据的复杂查询分析。

External catalog 是外部数据目录，用于连接数据湖中的数据。您可以将 StarRocks 作为查询引擎，直接查询湖上数据，无需导入数据至 StarRocks。

## 数据库

数据库是包含表、视图、物化视图等对象的集合，用于存储、管理和操作数据。

## [表](./table_types/table_types.md)

StarRocks 中的表分为两类：内部表和外部表。

**内部表**

内部表归属于 Internal catalog 的数据库，数据保存在 StarRocks 中。内部表由行和列构成，每一行数据是一条记录。

:::note

此处内部表的行和列为逻辑概念，在 StarRocks 中数据实际是按列存储的。物理上，一列数据会经过分块编码、压缩等操作，然后持久化存储。

:::

在 StarRocks 中，根据约束的类型将内部表分四种，分别是主键表、明细表、聚合表和更新表，适用于存储和查询多种业务场景中的数据，比如原始日志、实时数据、以及汇总数据。

内部表采用分区+分桶的两级数据分布策略，实现数据均匀分布。并且分桶以多副本形式均匀分布至 BE 节点，保证数据高可用。

**外部表**

外部表是 External catalog 中的表，实际数据存在外部数据源中，StarRocks 只保存表对应的元数据，您可以通过外部表查询外部数据。

## [物化视图](../using_starrocks/Materialized_view.md)

物化视图是特殊的物理表，能够存储基于基表的预计算结果。当您对基表执行复杂查询时，StarRocks 可以自动复用物化视图中的预计算结果，实现查询透明加速、湖仓加速和数据建模等业务需求。物化视图分为同步物化视图和异步物化视图。其中异步物化视图能力更加强大，能够存储基于多个基表（内部表和外部表）的预计算结果，并且支持丰富的聚合算子。

## [视图](../sql-reference/sql-statements/data-definition/CREATE_VIEW.md)

视图（也叫逻辑视图）是虚拟表，不实际存储数据，其中所展示的数据来自于基表生成的查询结果。每次在查询中引用某个视图时，都会运行定义该视图的查询。

## [权限系统](../administration/privilege_overview.md)

权限决定了哪些用户可以对哪些特定对象执行哪些特定的操作。StarRocks 采用了两种权限模型：基于用户标识的访问控制和基于角色的访问控制。您可以将权限赋予给角色然后通过角色传递权限给用户，或直接赋予权限给用户标识。

## [存算分离架构下的存储方式](../introduction/Architecture.md#存算分离)

StarRocks 从 3.0 版本开始引入存算分离架构，数据存储功能从原来的 BE 中抽离，数据可持久存储在更为可靠廉价的远端对象存储（如 S3）或 HDFS 上，本地磁盘只用于缓存热数据来加速查询。
