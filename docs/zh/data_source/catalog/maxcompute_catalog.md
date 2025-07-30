---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# MaxCompute catalog

<Beta />

StarRocks 从 v3.3 开始支持阿里云 MaxCompute（以前称为 ODPS）catalog。

MaxCompute catalog 是一种 external catalog，使您可以在不导入数据的情况下查询 MaxCompute 中的数据。

通过 MaxCompute catalog，您还可以使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 直接转换并导入 MaxCompute 中的数据。

## 使用说明

您只能使用 MaxCompute catalog 查询 MaxCompute 中的数据。您不能使用 MaxCompute catalog 删除、删除或插入数据到您的 MaxCompute 集群中。

## 集成准备

在创建 MaxCompute catalog 之前，请确保您的 StarRocks 集群可以正常访问您的 MaxCompute 服务。

## 创建 MaxCompute catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "odps",
    CatalogParams,
    ScanParams,
    CachingMetaParams
)
```

### 参数

#### catalog_name

MaxCompute catalog 的名称。命名约定如下：

- 名称可以包含字母、数字（0-9）和下划线（_）。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### comment

MaxCompute catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `odps`。

#### CatalogParams

关于 StarRocks 如何访问 MaxCompute 集群元数据的一组参数。

下表描述了您需要在 `CatalogParams` 中配置的参数。

| 参数                  | 必需  | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|----------------------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | 是    | MaxCompute 服务的连接地址（即 endpoint）。您需要根据创建 MaxCompute 项目时选择的区域以及网络连接模式来配置 endpoint。有关不同区域和网络连接模式使用的 endpoint 的详细信息，请参见 [Endpoint](https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints)。请注意，目前仅支持阿里云的两种网络连接模式，以提供最佳体验：VPC 和经典网络。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| odps.project         | 是    | 您要访问的 MaxCompute 项目的名称。如果您创建了标准模式工作区，请在配置此参数时注意生产环境和开发环境（_dev）项目名称之间的差异。您可以登录 [MaxCompute 控制台](https://account.alibabacloud.com/login/login.htm?spm=5176.12901015-2.0.0.593a525cwmiD7c)，在 **工作区** > **项目管理** 页面获取 MaxCompute 项目名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| odps.access.id       | 是    | 阿里云账户或 RAM 用户的 AccessKey ID。您可以进入 [AccessKey 管理](https://ram.console.aliyun.com/manage/ak) 页面获取 AccessKey ID。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| odps.access.key      | 是    | 与 AccessKey ID 匹配的 AccessKey Secret。您可以进入 [AccessKey 管理](https://ram.console.aliyun.com/manage/ak) 页面获取 AccessKey Secret。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| odps.tunnel.endpoint | 否    | Tunnel 服务的公网访问链接。如果您未配置 Tunnel endpoint，Tunnel 将自动路由到与 MaxCompute 服务所在网络匹配的 Tunnel endpoint。如果您已配置 Tunnel endpoint，将按配置使用，不会自动路由。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| odps.tunnel.quota    | 是    | 用于访问 MaxCompute 的配额名称。MaxCompute 提供两种数据传输资源：MaxCompute Tunnel 专用资源组（订阅）和存储 API（按量付费）。您可以根据资源类型执行以下操作以获取配额名称。<p/> **MaxCompute Tunnel 专用资源组**：登录 [MaxCompute 控制台](https://maxcompute.console.aliyun.com/)。在顶部导航栏中选择一个区域。在左侧导航窗格中，选择工作区 > 配额以查看可用配额。有关更多信息，请参见 [在 MaxCompute 控制台中管理计算资源配额](https://help.aliyun.com/zh/maxcompute/user-guide/manage-quotas-in-the-maxcompute-console)。<p/> **存储 API**：登录 [MaxCompute 控制台](https://maxcompute.console.aliyun.com/)。在左侧导航窗格中，选择租户 > 租户属性。在租户页面上，打开存储 API 开关。有关更多信息，请参见 [使用存储 API（按量付费）](https://help.aliyun.com/zh/maxcompute/user-guide/overview-1)。存储 API 的默认名称是 **"pay-as-you-go"**。 |

#### ScanParams

关于 StarRocks 如何访问存储在 MaxCompute 集群中的文件的一组参数。此参数集是可选的。

下表描述了您需要在 `ScanParams` 中配置的参数。

| 参数                  | 必需  | 描述                                                                                                             |
|----------------------|-------|----------------------------------------------------------------------------------------------------------------|
| odps.split.policy    | 否    | 数据扫描时使用的分片策略。<br />有效值：`size`（按数据大小分片）和 `row_offset`（按行数分片）。默认值：`size`。<br />                                  |
| odps.split.size.limit | 否    | 当 `odps.split.policy` 设置为 `size` 时每个分片的最大数据大小。<br />默认值：`32 * 1024 * 1024 = 33554432 (32MB)`。<br />            |
| odps.split.row.count | 否    | 当 `odps.split.policy` 设置为 `row_offset` 时每个分片的最大行数。<br />默认值：`4 * 1024 * 1024 = 4194304`。<br />                 |
| odps.predicate.pushdown.enable  | 否    | 是否启用谓词下推，将谓词下推到 MaxCompute Scan 算子。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示启用过滤条件，取值为 `false` 表示不启用过滤条件。 |

#### CachingMetaParams

关于 StarRocks 如何缓存 Hive 元数据的一组参数。此参数集是可选的。

下表描述了您需要在 `CachingMetaParams` 中配置的参数。

| 参数                          | 必需  | 描述                                                                       |
|------------------------------|-------|---------------------------------------------------------------------------|
| odps.cache.table.enable      | 否    | 指定 StarRocks 是否缓存 MaxCompute 表的元数据。有效值：`true` 和 `false`。默认值：`true`。值 `true` 启用缓存，值 `false` 禁用缓存。                         |
| odps.cache.table.expire      | 否    | StarRocks 自动逐出 MaxCompute 表或分区缓存元数据的时间间隔（以秒为单位）。默认值：`86400`（24 小时）。                                                                 |
| odps.cache.table.size        | 否    | StarRocks 缓存的 MaxCompute 表元数据条目数。默认值：`1000`。      |
| odps.cache.partition.enable  | 否    | 指定 StarRocks 是否缓存 MaxCompute 表的所有分区元数据。有效值：`true` 和 `false`。默认值：`true`。值 `true` 启用缓存，值 `false` 禁用缓存。 |
| odps.cache.partition.expire  | 否    | StarRocks 自动逐出 MaxCompute 表所有分区缓存元数据的时间间隔（以秒为单位）。默认值：`86400`（24 小时）。                                                            |
| odps.cache.partition.size    | 否    | StarRocks 缓存所有分区元数据的 MaxCompute 表数量。默认值：`1000`。 |
| odps.cache.table-name.enable | 否    | 指定 StarRocks 是否缓存 MaxCompute 项目的表信息。有效值：`true` 和 `false`。默认值：`false`。值 `true` 启用缓存，值 `false` 禁用缓存。     |
| odps.cache.table-name.expire | 否    | StarRocks 自动逐出 MaxCompute 项目的表信息缓存的时间间隔（以秒为单位）。默认值：`86400`（24 小时）。                                                                  |
| odps.cache.table-name.size   | 否    | StarRocks 缓存的 MaxCompute 项目数量。默认值：`1000`。   |

### 示例

以下示例创建了一个名为 `odps_catalog` 的 MaxCompute catalog，使用 `odps_project` 作为仓库项目。

```SQL
CREATE EXTERNAL CATALOG odps_catalog 
PROPERTIES (
   "type"="odps",
   "odps.access.id"="<maxcompute_user_access_id>",
   "odps.access.key"="<maxcompute_user_access_key>",
   "odps.endpoint"="<maxcompute_server_endpoint>",
   "odps.project"="odps_project"
);
```

## 查看 MaxCompute catalog

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md)
查询当前 StarRocks 集群中的所有 catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询 external catalog 的创建语句。以下示例查询名为 `odps_catalog` 的 MaxCompute catalog 的创建语句：

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## 删除 MaxCompute catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除 external catalog。

以下示例删除了一个名为 `odps_catalog` 的 MaxCompute catalog：

```SQL
DROP CATALOG odps_catalog;
```

## 查看 MaxCompute 表的 schema

您可以使用以下语法之一查看 MaxCompute 表的 schema：

- 查看 schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 语句中查看 schema 和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 MaxCompute 表

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看 MaxCompute 集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 在当前会话中切换到目标 catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   然后，使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话中的活动数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，您可以使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接指定目标 catalog 中的活动数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 从 MaxCompute 导入数据

假设您的 StarRocks 集群中有一个名为 `olap_tbl` 的 OLAP 表，并且您的 MaxCompute 集群中有一个名为 `mc_table` 的表。您可以将 MaxCompute 表 `mc_table` 中的数据转换并导入到 StarRocks 表 `olap_tbl` 中，如下所示：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## 数据类型映射

MaxCompute catalog 将 MaxCompute 数据类型映射到 StarRocks 数据类型。下表显示了 MaxCompute 数据类型与 StarRocks 数据类型之间的映射。

| MaxCompute 数据类型  | StarRocks 数据类型 |
|-----------------------|---------------------|
| BOOLEAN               | BOOLEAN             |
| TINYINT               | TINYINT             |
| SMALLINT              | SMALLINT            |
| INT                   | INT                 |
| BIGINT                | BIGINT              |
| FLOAT                 | FLOAT               |
| DOUBLE                | DOUBLE              |
| DECIMAL(p, s)         | DECIMAL(p, s)       |
| STRING                | VARCHAR(1073741824) |
| VARCHAR(n)            | VARCHAR(n)          |
| CHAR(n)               | CHAR(n)             |
| JSON                  | VARCHAR(1073741824) |
| BINARY                | VARBINARY           |
| DATE                  | DATE                |
| DATETIME              | DATETIME            |
| TIMESTAMP             | DATETIME            |
| ARRAY                 | ARRAY               |
| MAP                   | MAP                 |
| STRUCT                | STRUCT              |

:::note

由于 StarRocks 中的类型转换，TIMESTAMP 类型会丢失精度。

:::

## 收集 CBO 统计信息

在当前版本中，MaxCompute catalog 无法自动收集 MaxCompute 表的 CBO 统计信息，因此优化器可能无法生成最佳查询计划。因此，手动扫描 MaxCompute 表的 CBO 统计信息并将其导入 StarRocks 可以有效加快查询速度。

假设您的 MaxCompute 集群中有一个名为 `mc_table` 的 MaxCompute 表。您可以使用 [ANALYZE TABLE](../../sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE.md) 创建一个手动收集任务来收集 CBO 统计信息：

```SQL
ANALYZE TABLE mc_table;
```

## 手动更新元数据缓存

默认情况下，StarRocks 缓存 MaxCompute 的元数据以提高查询性能。因此，在对 MaxCompute 表进行 schema 变更或其他更新后，您可以使用 [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) 手动更新表的元数据，从而确保 StarRocks 能够及时获取最新的元数据：

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```