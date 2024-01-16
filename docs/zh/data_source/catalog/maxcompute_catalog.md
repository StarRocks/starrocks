---
displayed_sidebar: "Chinese"
---

# MaxCompute(ODPS) catalog

StarRocks 从 3.3 版本开始支持 MaxCompute(ODPS) Catalog。

MaxCompute(ODPS) Catalog 是一种 External Catalog。通过 MaxCompute(ODPS) Catalog，您不需要执行数据导入就可以直接查询
Aliyun MaxCompute(ODPS) 里的数据。

此外，您还可以基于 MaxCompute(ODPS) Catalog
，结合 [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) 能力来实现数据转换和导入。

## 使用说明

MaxCompute Catalog 仅支持查询 MaxCompute(ODPS) 数据，不支持针对 MaxCompute(ODPS) 的写/删操作。

## 准备工作

在创建 MaxCompute Catalog 之前，请确保 StarRocks 集群能够正常访问 MaxCompute(ODPS) 的服务。

## 创建 MaxCompute(ODPS) Catalog

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

MaxCompute(ODPS) Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### comment

MaxCompute(ODPS) Catalog 的描述。此参数为可选。

#### type

数据源的类型。设置为 `odps`。

#### CatalogParams

StarRocks 访问 MaxCompute(ODPS) 集群的相关参数配置。

`CatalogParams` 包含如下参数。

| 参数                   | 是否必须 | 说明                                                                                                                                                                                          |
|----------------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | 是    | MaxCompute服务的连接地址。您需要根据创建MaxCompute项目时选择的地域以及网络连接方式配置Endpoint。各地域及网络对应的Endpoint值，请参见[Endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints)。注意，当前仅支持使用阿里云VPC网络/经典网络，以提供最佳体验。 |
| odps.project         | 是    | 访问的目标MaxCompute项目名称。如果您创建了标准模式的工作空间，在配置此参数时，请注意区分生产环境与开发环境（_dev）的项目名称。您可以登录[MaxCompute控制台](https://maxcompute.console.aliyun.com/)，在工作区> 项目管理页面获取MaxCompute项目名称。                            |
| odps.access.id       | 是    | 阿里云账号或RAM用户的AccessKey ID。您可以进入[AccessKey管理页面](https://ram.console.aliyun.com/manage/ak)获取AccessKey ID。                                                                                      |
| odps.access.key      | 是    | AccessKey ID对应的AccessKey Secret。您可以进入[AccessKey管理页面](https://ram.console.aliyun.com/manage/ak)获取AccessKey Secret。                                                                           |
| odps.tunnel.endpoint | 否    | Tunnel服务的外网访问链接。如果您未配置Tunnel Endpoint，Tunnel会自动路由到MaxCompute服务所在网络对应的Tunnel Endpoint。如果您配置了Tunnel Endpoint，则以配置为准，不进行自动路由。                                                                  |
| odps.tunnel.quota    | 是    | 数据传输使用的资源组名称。StarRocks需要[使用Maxcompute独享资源组](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts)，对数据进行拉取。                                   |

#### ScanParams

StarRocks 访问 MaxCompute(ODPS) 集群文件存储的相关参数配置。此组参数为可选。

| 参数                   | 是否必须 | 说明                                                                                        |
|----------------------|------|-------------------------------------------------------------------------------------------|
| odps.split.policy    | 否    | 扫描数据时所使用的分片策略。<br />可选值为 `size`（按数据大小分片）或 `row_offset`（按行数分片）。默认值：`size`。<br />           |
| odps.split.row.count | 否    | 当`odps.split.policy`为`row_offset`时，每个分片的最大行数。<br />默认值：`4 * 1024 * 1024 = 4194304`。<br /> |

##### CachingMetaParams

指定缓存元数据缓存策略的一组参数。StarRocks 根据该策略缓存的 Hive 元数据。此组参数为可选。

`CachingMetaParams` 包含如下参数。

| 参数                           | 是否必须 | 说明                                                                                                |
|------------------------------|------|---------------------------------------------------------------------------------------------------|
| odps.cache.table.enable      | 否    | 指定 StarRocks 是否缓存表的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。     |
| odps.cache.table.expire      | 否    | StarRocks 自动淘汰缓存的表或分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                          |
| odps.cache.table.size        | 否    | StarRocks 缓存表元数据的数量。默认值：`1000`。                                                                   |
| odps.cache.partition.enable  | 否    | 指定 StarRocks 是否缓存表所有分区的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。 |
| odps.cache.partition.expire  | 否    | StarRocks 自动淘汰缓存的表所有分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                         |
| odps.cache.partition.size    | 否    | StarRocks 缓存表表所有分区的表数量。默认值：`1000`。                                                                |
| odps.cache.table-name.enable | 否    | 指定 StarRocks 是否缓存项目中表信息。取值范围：`true` 和 `false`。默认值：`false`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。   |
| odps.cache.table-name.expire | 否    | StarRocks 自动淘汰缓存的项目中表信息数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                          |
| odps.cache.table-name.size   | 否    | StarRocks 缓存的项目数量。默认值：`1000`。                                                                     |

### 示例

以下示例创建了一个名为 `odps_catalog` 的 MaxCompute(ODPS) Catalog，
使用`odps_project`作为项目空间。

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

## 查看 MaxCompute(ODPS) Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md)
查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md)
查询某个 External Catalog 的创建语句。例如，通过如下命令查询 MaxCompute(ODPS)
Catalog `odps_catalog` 的创建语句：

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## 删除 MaxCompute(ODPS) Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md) 删除某个
External Catalog。

例如，通过如下命令删除 MaxCompute(ODPS) Catalog `odps_catalog`：

```SQL
DROP CATALOG odps_catalog;
```

## 查看 MaxCompute(ODPS) 表结构

您可以通过如下方法查看 MaxCompute(ODPS) 表的表结构：

- 查看表结构

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 命令查看表结构和表文件存放位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 MaxCompute(ODPS) 表数据

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md)
   查看指定 Catalog 所属的 MaxCompute(ODPS) Catalog 中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) 切换当前会话生效的
   Catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   再通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 指定当前会话生效的数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，也可以通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 直接将会话切换到目标
   Catalog 下的指定数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 通过 [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 导入 MaxCompute(ODPS) 数据

假设有一个 OLAP 表，表名为 `olap_tbl`。您可以这样来转换该表中的数据，并把数据导入到 StarRocks 中：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## 类型映射
MaxCompute Catalog 将 MaxCompute(ODPS) 类型映射到 StarRocks 类型，类型转换如下

| MaxCompute(ODPS) 类型 | StarRocks 类型        |
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
| DATETIME            | DATETIME            |
| DATETIME            | DATETIME            |
| TIMESTAMP           | DATETIME            |
| ARRAY               | ARRAY               |
| MAP                 | MAP                 |
| STRUCT              | STRUCT              |

注意：TIMESTAMP类型在 StarRocks 中由于类型转换，会损失精度

## CBO 统计信息采集

由于 MaxCompute(ODPS) Catalog 在当前版本还无法自动采集 MaxCompute(ODPS) 表的 CBO
统计信息，导致某些情况下优化器无法做出最优的查询计划。
因此手动扫描 MaxCompute(ODPS) 表的 CBO 统计信息，并导入到 StarRocks 中，可以有效优化查询时间。

假设有一个 MaxCompute 表，表名为 `mc_table`,
可以通过[ANALYZE TABLE](../../sql-reference/sql-statements/data-definition/ANALYZE_TABLE.md)
创建手动采集任务，进行 CBO 统计信息采集。

```SQL
ANALYZE TABLE mc_table;
```

## 手动更新元数据缓存

默认情况下，StarRocks 会缓存 Maxcompute(ODPS)
的元数据，从而提高查询性能。因此，当对表做了表结构变更或其他表更新后，您也可以使用 [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md)
手动更新该表的元数据，从而确保 StarRocks 第一时间获取到新的元数据信息：

```SQL
REFRESH EXTERNAL TABLE <table_name>
```
