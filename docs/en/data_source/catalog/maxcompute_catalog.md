---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# [Preview] MaxCompute catalog

StarRocks supports Alibaba Cloud MaxCompute (previously known as ODPS) catalogs from v3.3 onwards.

A MaxCompute catalog is a kind of external catalog that enables you to query data from MaxCompute without ingestion.

With MaxCompute catalogs, you also can directly transform and load the data from MaxCompute by using [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md).

## Usage notes

You can use MaxCompute catalogs only to query the data from MaxCompute. You cannot use MaxCompute catalogs to drop, delete, or insert data into your MaxCompute cluster.

## Integration preparations

Before creating a MaxCompute catalog, make sure that your StarRocks cluster can access your MaxCompute service properly.

## Create a MaxCompute catalog

### Syntax

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

### Parameters

#### catalog_name

The name of the MaxCompute catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the MaxCompute catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `odps`.

#### CatalogParams

A set of parameters about how StarRocks accesses the metadata of the MaxCompute cluster.

The following table describes the parameter you need to configure in `CatalogParams`.

| Parameter            | Required  | Description                                                                     |
|----------------------|-----------|---------------------------------------------------------------------------------|
| odps.endpoint        | Yes       | The connection address (namely, endpoint) for the MaxCompute service. You need to configure the endpoint according to the region selected when creating the MaxCompute project as well as the network connection mode. For details about the endpoints used in different regions and network connection modes, see [Endpoint](https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints). Note that currently only two network connection modes of Alibaba Cloud are supported to provide the best experience: VPC and classic network. |
| odps.project         | Yes       | The name of the MaxCompute project you want to access. If you have created a standard mode workspace, pay attention to the differences between the project names for the production environment and the development environment (_dev) when configuring this parameter. You can log in to the [MaxCompute Console](https://account.alibabacloud.com/login/login.htm?spm=5176.12901015-2.0.0.593a525cwmiD7c), and obtain the MaxCompute project name on the **Workspace** > **Project Management** page.                     |
| odps.access.id       | Yes       | The AccessKey ID of the Alibaba Cloud account or RAM user. You can enter the [AccessKey Management](https://ram.console.aliyun.com/manage/ak) page to obtain the AccessKey ID. |
| odps.access.key      | Yes       | The AccessKey Secret matching the AccessKey ID. You can enter the [AccessKey Management](https://ram.console.aliyun.com/manage/ak) page to obtain the AccessKey Secret. |
| odps.tunnel.endpoint | No        | The public network access link for the Tunnel service. If you have not configured the Tunnel endpoint, Tunnel will automatically route to the Tunnel endpoint matching the network where the MaxCompute service is located. If you have configured the Tunnel endpoint, it will be used as configured and not automatically routed. |
| odps.tunnel.quota    | Yes       | The name of the resource group used for data transfer. StarRocks needs to use [MaxCompute's dedicated resource groups](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts) to pull data. |

#### ScanParams

A set of parameters about how StarRocks accesses the files stored in the MaxCompute cluster. This parameter set is optional.

The following table describes the parameter you need to configure in `ScanParams`.

| Parameter            | Required  | Description                                       |
|----------------------|-----------|---------------------------------------------------|
| odps.split.policy    | No        | The shard policy used when for data scanning. <br />Valid values: `size` (shard by data size) and `row_offset` (shard by number of rows). Default value: `size`.<br /> |
| odps.split.row.count | No        | The maximum number of rows per shard when `odps.split.policy` is set to `row_offset`. <br />Default value: `4 * 1024 * 1024 = 4194304`.<br />  |

#### CachingMetaParams

A set of parameters about how StarRocks caches the metadata of Hive. This parameter set is optional.

The following table describes the parameter you need to configure in `CachingMetaParams`.

| Parameter                    | Required | Description                                                                       |
|------------------------------|----------|-----------------------------------------------------------------------------------|
| odps.cache.table.enable      | No       | Specifies whether StarRocks caches the metadata of MaxCompute tables. Valid values: `true` and `false`. Default value: `true`. The value `true` enables the cache, and the value `false` disables the cache.                         |
| odps.cache.table.expire      | No       | The time interval, in seconds, at which StarRocks automatically evicts the cached metadata of MaxCompute tables or partitions. Default value: `86400` (24 hours).                                                                 |
| odps.cache.table.size        | No       | The number of MaxCompute table metadata entries that StarRocks caches. Default value: `1000`.      |
| odps.cache.partition.enable  | No       | Specifies whether StarRocks caches the metadata of all partitions for a MaxCompute table. Valid values: `true` and `false`. Default value: `true`. The value `true` enables the cache, and the value `false` disables the cache. |
| odps.cache.partition.expire  | No       | The time interval, in seconds, at which StarRocks automatically evicts the cached metadata of all partitions for a MaxCompute table. Default value: `86400` (24 hours).                                                            |
| odps.cache.partition.size    | No       | The number of MaxCompute tables for which StarRocks caches the metadata of all partitions. Default value: `1000`. |
| odps.cache.table-name.enable | No       | Specifies whether StarRocks caches the information of the tables from the MaxCompute project. Valid values: `true` and `false`. Default value: `false`. The value `true` enables the cache, and the value `false` disables the cache.     |
| odps.cache.table-name.expire | No       | The time interval, in seconds, at which StarRocks automatically evicts the cached information of the tables from the MaxCompute project. Default value: `86400` (24 hours).                                                                  |
| odps.cache.table-name.size   | No       | The number of MaxCompute projects that StarRocks caches. Default value: `1000`.   |

### Examples

The following example creates a MaxCompute catalog named `odps_catalog` which uses `odps_project` as the warehouse project.

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

## View MaxCompute catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md)
to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a MaxCompute catalog named `odps_catalog`:

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## Drop a MaxCompute catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop an external catalog.

The following example drops a MaxCompute catalog named `odps_catalog`:

```SQL
DROP CATALOG odps_catalog;
```

## View the schema of a MaxCompute table

You can use one of the following syntaxes to view the schema of a MaxCompute table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Query a MaxCompute table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in your MaxCompute cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the destination catalog in the current session:

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database in the current session:

   ```SQL
   USE <db_name>;
   ```

   Or, you can use [USE](../../sql-reference/sql-statements/Database/USE.md) to directly specify the active database in the destination catalog:

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Load data from MaxCompute

Suppose that there is an OLAP table named `olap_tbl` in your StarRock cluster and there is a table named `mc_table` in your MaxCompute cluster. You can transform and load the data from the MaxCompute table `mc_table` into the StarRocks table `olap_tbl` like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## Data type mapping

MaxCompute catalogs map the MaxCompute data types to the StarRocks data types. The following table shows the mapping between the MaxCompute data types to the StarRocks data types.

| MaxCompute data type  | StarRocks data type |
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

The TIMESTAMP type will lose precision due to type conversion in StarRocks.

:::

## Collect CBO statistics

In the current version, MaxCompute catalogs cannot automatically collect CBO statistics for MaxCompute tables, and consequently the optimizer may not be able to generate the optimal query plans. As such, manually scanning the CBO statistics for MaxCompute tables and importing them into StarRocks can effectively expedite queries.

Suppose that there is a MaxCompute table named `mc_table` in your MaxCompute cluster. You can create a manual collection task for collecting CBO statistics by using [ANALYZE TABLE](../../sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE.md):

```SQL
ANALYZE TABLE mc_table;
```

## Manually update metadata cache

By default, StarRocks caches the metadata of MaxCompute to improve query performance. Therefore, after making schema changes or other updates to a MaxCompute table, you can use [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) to manually update the metadata of the table, thereby ensuring that StarRocks can obtain the most recent metadata promptly:

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```
