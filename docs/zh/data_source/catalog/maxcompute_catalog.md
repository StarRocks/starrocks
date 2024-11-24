---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# [Preview] MaxCompute catalog

StarRocks 从 3.3 版本开始支持 Alibaba Cloud MaxCompute (即以前的 ODPS) Catalog。

MaxCompute Catalog 是一种 External Catalog。通过 MaxCompute Catalog，您不需要执行数据导入就可以直接查询 MaxCompute 里的数据。

此外，您还可以基于 MaxCompute Catalog，结合 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 能力来实现数据转换和导入。

## 使用说明

MaxCompute Catalog 仅支持查询 MaxCompute 里的数据，不支持针对 MaxCompute 的写/删操作。

## 准备工作

在创建 MaxCompute Catalog 之前，请确保 StarRocks 集群能够正常访问 MaxCompute 的服务。

## 创建 MaxCompute Catalog

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

### 参数说明

#### catalog_name

MaxCompute Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### comment

MaxCompute Catalog 的描述。此参数为可选。

#### type

数据源的类型。设置为 `odps`。

#### CatalogParams

StarRocks 访问 MaxCompute 集群的相关参数配置。

`CatalogParams` 包含如下参数。

| 参数                  | 是否必须   | 说明                                                             |
|----------------------|-----------|-----------------------------------------------------------------|
| odps.endpoint        | 是        | MaxCompute 服务的连接地址 (Endpoint)。您需要根据创建 MaxCompute 项目时选择的地域、以及网络连接方式来配置 Endpoint。有关各地域及网络连接方式对应的 Endpoint 值，请参见 [Endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints)。注意，为提供最佳体验，当前仅支持使用阿里云 VPC 网络及经典网络连接方式。 |
| odps.project         | 是        | 访问的目标 MaxCompute 项目的名称。如果您创建了标准模式的工作空间，在配置此参数时，请注意区分生产环境与开发环境（_dev）的项目名称。您可以登录[MaxCompute 控制台](https://maxcompute.console.aliyun.com/)，在**工作区** > **项目管理**页面获取 MaxCompute 项目名称。                            |
| odps.access.id       | 是        | 阿里云账号或 RAM 用户的 AccessKey ID。您可以进入 [AccessKey 管理页面](https://ram.console.aliyun.com/manage/ak)获取 AccessKey ID。                                                                                      |
| odps.access.key      | 是        | AccessKey ID 对应的 AccessKey Secret。您可以进入 [AccessKey 管理页面](https://ram.console.aliyun.com/manage/ak)获取AccessKey Secret。                                                                           |
| odps.tunnel.endpoint | 否        | Tunnel 服务的外网访问链接。如果您未配置 Tunnel Endpoint，Tunnel 会自动路由到 MaxCompute 服务所在网络对应的 Tunnel Endpoint。如果您配置了 Tunnel Endpoint，则以配置为准，不进行自动路由。                                                                  |
| odps.tunnel.quota    | 是        | 数据传输使用的资源组名称。StarRocks 需要[使用 Maxcompute 独享资源组](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts)，对数据进行拉取。                                   |

#### ScanParams

StarRocks 访问 MaxCompute 集群文件存储的相关参数配置。此组参数为可选。

| 参数                  | 是否必须 | 说明                                                                                        |
|----------------------|---------|--------------------------------------------------------------------------------------------|
| odps.split.policy    | 否      | 扫描数据时所使用的分片策略。<br />取值范围：`size`（按数据大小分片）、`row_offset`（按行数分片）。默认值：`size`。<br />           |
| odps.split.row.count | 否      | 当 `odps.split.policy` 设置为 `row_offset` 时，每个分片的最大行数。<br />默认值：`4 * 1024 * 1024 = 4194304`。<br /> |

##### CachingMetaParams

StarRocks 缓存 Hive 元数据的相关参数配置。此组参数为可选。

`CachingMetaParams` 包含如下参数。

| 参数                          | 是否必须 | 说明                                                                                                |
|------------------------------|---------|----------------------------------------------------------------------------------------------------|
| odps.cache.table.enable      | 否      | 指定 StarRocks 是否缓存 MaxCompute 表的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。     |
| odps.cache.table.expire      | 否      | StarRocks 自动淘汰缓存的 MaxCompute 表或分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。               |
| odps.cache.table.size        | 否      | StarRocks 缓存 MaxCompute 表的元数据的数量。默认值：`1000`。  |
| odps.cache.partition.enable  | 否      | 指定 StarRocks 是否缓存 MaxCompute 表所有分区的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。 |
| odps.cache.partition.expire  | 否      | StarRocks 自动淘汰缓存的 MaxCompute 表所有分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。             |
| odps.cache.partition.size    | 否      | StarRocks 缓存 MaxCompute 表所有分区的表数量。默认值：`1000`。     |
| odps.cache.table-name.enable | 否      | 指定 StarRocks 是否缓存 MaxCompute 项目中表信息。取值范围：`true` 和 `false`。默认值：`false`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。   |
| odps.cache.table-name.expire | 否      | StarRocks 自动淘汰缓存的 MaxCompute 项目中表信息的时间间隔。单位：秒。默认值：`86400`，即 24 小时。           |
| odps.cache.table-name.size   | 否      | StarRocks 缓存的 MaxCompute 项目数量。默认值：`1000`。        |

### 示例

以下示例创建了一个名为 `odps_catalog` 的 MaxCompute Catalog，使用 `odps_project` 作为项目空间：

```SQL
CREATE EXTERNAL CATALOG odps_catalog 
PROPERTIES
(
    "type"="odps",
    "odps.access.id"="<maxcompute_user_access_id>",
    "odps.access.key"="<maxcompute_user_access_key>",
    "odps.endpoint"="<maxcompute_server_endpoint>",
    "odps.project"="odps_project"
);
```

## 查看 MaxCompute Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 MaxCompute Catalog `odps_catalog` 的创建语句：

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## 删除 MaxCompute Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除某个 External Catalog。

例如，通过如下命令删除 MaxCompute Catalog `odps_catalog`：

```SQL
DROP CATALOG odps_catalog;
```

## 查看 MaxCompute 表结构

您可以通过如下方法查看 MaxCompute 表的表结构：

- 查看表结构

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 命令查看表结构和表文件存放位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 MaxCompute 表数据

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看指定 Catalog 所属的 MaxCompute Catalog 中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 切换当前会话生效的 Catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话生效的数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，也可以通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 导入 MaxCompute 数据

假设 StarRocks 集群中有一个 OLAP 表 `olap_tbl`，MaxCompute 集群中有一个表 `mc_table`。您可以通过类似如下的语句来转换 MaxCompute 表 `mc_table` 中的数据，并把转换后的数据导入到 StarRocks 表 `olap_tbl` 中：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## 数据类型映射

MaxCompute Catalog 将 MaxCompute 数据类型映射到 StarRocks 数据类型，类型转换关系如下表所述。

| MaxCompute 数据类型   | StarRocks 数据类型   |
|---------------------|---------------------|
| BOOLEAN             | BOOLEAN             |
| TINYINT             | TINYINT             |
| SMALLINT            | SMALLINT            |
| INT                 | INT                 |
| BIGINT              | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| DECIMAL(p, s)       | DECIMAL(p, s)       |
| STRING              | VARCHAR(1073741824) |
| VARCHAR(n)          | VARCHAR(n)          |
| CHAR(n)             | CHAR(n)             |
| JSON                | VARCHAR(1073741824) |
| BINARY              | VARBINARY           |
| DATE                | DATE                |
| DATETIME            | DATETIME            |
| TIMESTAMP           | DATETIME            |
| ARRAY               | ARRAY               |
| MAP                 | MAP                 |
| STRUCT              | STRUCT              |

:::note

TIMESTAMP 类型在 StarRocks 中由于类型转换，会损失精度。

:::

## CBO 统计信息采集

由于 MaxCompute Catalog 在当前版本还无法自动采集 MaxCompute 表的 CBO 统计信息，导致某些情况下优化器无法做出最优的查询计划。在这种情况下，手动扫描 MaxCompute 表的 CBO 统计信息，并导入到 StarRocks 中，可以有效优化查询时间。

假设有一个 MaxCompute 表，表名为 `mc_table`, 您可以通过 [ANALYZE TABLE](../../sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE.md) 创建手动采集任务，进行 CBO 统计信息采集：

```SQL
ANALYZE TABLE mc_table;
```

## 手动更新元数据缓存

默认情况下，StarRocks 会缓存 MaxCompute 的元数据，从而提高查询性能。因此，当对 MaxCompute 表做了表结构变更或其他表更新后，您可以使用 [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) 手动更新该表的元数据，从而确保 StarRocks 第一时间获取到新的元数据信息：

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```
