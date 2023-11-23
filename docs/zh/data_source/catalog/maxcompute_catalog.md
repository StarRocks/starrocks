---
displayed_sidebar: "Chinese"
---

# MaxCompute(ODPS) catalog

StarRocks 从 3.3 版本开始支持 MaxCompute(ODPS) Catalog。

MaxCompute(ODPS) Catalog 是一种 External Catalog。通过 MaxCompute(ODPS) Catalog，您不需要执行数据导入就可以直接查询
Aliyun MaxCompute(ODPS) 里的数据。

此外，您还可以基于 MaxCompute(ODPS) Catalog
，结合 [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) 能力来实现数据转换和导入。

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

| 参数                   | 是否必须 | 说明                                                                                                                         |
|----------------------|------|----------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | 是    | 访问的目标MaxCompute项目名称。如果您创建了标准模式的工作空间，在配置project_name时，请注意区分生产环境与开发环境（_dev）的项目名称。                                            |
| odps.project         | 是    | MaxCompute服务的连接地址。您需要根据创建MaxCompute项目时选择的地域以及网络连接方式配置Endpoint。                                                             |
| odps.access.id       | 是    | 阿里云账号或RAM用户的AccessKey ID。                                                                                                  |
| odps.access.key      | 是    | AccessKey ID对应的AccessKey Secret。                                                                                           |
| odps.tunnel.endpoint | 否    | Tunnel服务的外网访问链接。如果您未配置Tunnel Endpoint，Tunnel会自动路由到MaxCompute服务所在网络对应的Tunnel Endpoint。如果您配置了Tunnel Endpoint，则以配置为准，不进行自动路由。 |
| odps.tunnel.quota    | 否    | Tunnel服务所使用的quota name，如未配置，则使用默认quota                                                                                     |

#### ScanParams

StarRocks 访问 MaxCompute(ODPS) 集群文件存储的相关参数配置。此组参数为可选。

| 参数                   | 是否必须 | 说明                                                                                        |
|----------------------|------|-------------------------------------------------------------------------------------------|
| odps.split.policy    | 否    | 扫描数据时所使用的分片策略。<br />取值范围：`size`(根据数据大小分片） 和 `row_offset`（根据行数分片）。默认值：`size`。<br />        |
| odps.split.row.count | 否    | 当`odps.split.policy`为`row_offset`时，每个分片的最大行数。<br />默认值：`4 * 1024 * 1024 = 4194304`。<br /> |

##### CachingMetaParams

指定缓存元数据缓存策略的一组参数。StarRocks 根据该策略缓存的 Hive 元数据。此组参数为可选。

`CachingMetaParams` 包含如下参数。

| 参数                          | 是否必须 | 说明                                                                                                |
|-----------------------------|------|---------------------------------------------------------------------------------------------------|
| odps.cache.table.enable     | 否    | 指定 StarRocks 是否缓存表的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。     |
| odps.cache.table.expire     | 否    | StarRocks 自动淘汰缓存的表或分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                          |
| odps.cache.table.size       | 否    | StarRocks 缓存表元数据的数量。默认值：`1000`。                                                                   |
| odps.cache.partition.enable | 否    | 指定 StarRocks 是否缓存表所有分区的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。 |
| odps.cache.partition.expire | 否    | StarRocks 自动淘汰缓存的表所有分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                         |
| odps.cache.partition.size   | 否    | StarRocks 缓存表表所有分区的表数量。默认值：`1000`。                                                                |
| odps.cache.project.enable   | 否    | 指定 StarRocks 是否缓存项目中表信息。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。    |
| odps.cache.project.expire   | 否    | StarRocks 自动淘汰缓存的项目中表信息数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。                                          |
| odps.cache.project.size     | 否    | StarRocks 缓存的项目数量。默认值：`1000`。                                                                     |

### 示例

以下示例创建了一个名为 `odps_catalog` 的 MaxCompute(ODPS) Catalog，
使用`odps_project`作为项目空间。

```SQL

CREATE EXTERNAL CATALOG odps_catalog PROPERTIES(
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
DROP Catalog odps_catalog;
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
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM MaxCompute(ODPS)_table;
```
