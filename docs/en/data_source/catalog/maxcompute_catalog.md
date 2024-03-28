---
displayed_sidebar: "English"
---

# MaxCompute(ODPS) catalog

StarRocks supports MaxCompute(ODPS) catalogs from v3.3 onwards.

A MaxCompute(ODPS) catalog is a kind of external catalog that enables you to query data from Aliyun
MaxCompute(ODPS) without ingestion.

Also, you can directly transform and load data from MaxCompute(ODPS) by
using [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) based on
MaxCompute(ODPS) catalogs.

To ensure successful SQL workloads on your MaxCompute(ODPS) cluster, your StarRocks cluster needs to
integrate with two important components:

## Usage notes

You can only use MaxCompute(ODPS) catalogs to query data. You cannot use MaxCompute(ODPS) catalogs
to drop, delete, or insert data into your MaxCompute(ODPS) cluster.

## Integration preparations

Before creating the MaxCompute Catalog, please ensure that the StarRocks cluster can access the
MaxCompute (ODPS) service properly.

## Create a MaxCompute(ODPS) catalog

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

The name of the MaxCompute(ODPS) catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the MaxCompute(ODPS) catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `odps`.

#### CatalogParams

A set of parameters about how StarRocks accesses the metadata of your MaxCompute(ODPS) cluster.

The following table describes the parameter you need to configure in `CatalogParams`.

| Parameter            | Required	 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|----------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | Yes       | The connection address for the MaxCompute service. You need to configure the Endpoint according to the region selected when creating the MaxCompute project and the method of network connection. For the Endpoint values corresponding to each region and network, please refer to [Endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints). Note that currently only the Alibaba Cloud VPC network/classic network is supported to provide the best experience. |
| odps.project         | Yes       | The name of the target MaxCompute project you want to access. If you have created a standard mode workspace, please pay attention to distinguishing between the project names for the production environment and the development environment (_dev) when configuring this parameter. You can log in to the [MaxCompute Console](https://maxcompute.console.aliyun.com/), and obtain the MaxCompute project name on the Workspace > Project Management page.                     |
| odps.access.id       | Yes       | The AccessKey ID of the Alibaba Cloud account or RAM user. You can enter the [AccessKey Management](https://ram.console.aliyun.com/manage/ak) page to obtain the AccessKey ID.                                                                                                                                                                                                                                                                                                  |
| odps.access.key      | Yes       | The AccessKey Secret corresponding to the AccessKey ID. You can enter the [AccessKey Management](https://ram.console.aliyun.com/manage/ak) page to obtain the AccessKey Secret.                                                                                                                                                                                                                                                                                                 |
| odps.tunnel.endpoint | No        | The public network access link for the Tunnel service. If you have not configured the Tunnel Endpoint, Tunnel will automatically route to the Tunnel Endpoint corresponding to the network where the MaxCompute service is located. If you have configured the Tunnel Endpoint, it will be used as configured and not automatically routed.                                                                                                                                     |
| odps.tunnel.quota    | Yes       | The name of the resource group used for data transfer. StarRocks need to use [MaxCompute's dedicated resource groups](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts) to pull data.                                                                                                                                                                                                                                        |

#### ScanParams

Configuration parameters for StarRocks accessing files stored in the MaxCompute (ODPS) cluster. This
parameter group is optional.

| Parameter	           | Required	 | Description                                                                                                                                                           |
|----------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.split.policy	   | No	       | The shard policy used when scanning data. <br />Possible values: `size` (shard by data size) and `row_offset` (shard by number of rows). Default value: `size`.<br /> |
| odps.split.row.count | 	No       | 	The maximum number of rows per shard when `odps.split.policy` is set to `row_offset`. <br />Default value: `4 * 1024 * 1024 = 4194304`.<br />                        |

#### CachingMetaParams

A set of parameters specifying the caching strategy for metadata caching. StarRocks caches Hive
metadata based on this strategy. This parameter group is optional.
CachingMetaParams includes the following parameters.

| Parameter                    | Required | Description                                                                                                                                                                                                          |
|------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.cache.table.enable      | No       | Specifies whether to cache table metadata in StarRocks. Possible values:true and false. Default value: true. Setting it to true enables caching, while setting it to false disables caching.                         |
| odps.cache.table.expire      | No       | The time interval, in seconds, after which StarRocks automatically evicts cached metadata for tables or partitions. Default value: 86400 (24 hours).                                                                 |
| odps.cache.table.size        | No       | The number of table metadata entries that StarRocks caches. Default value:1000.                                                                                                                                      |
| odps.cache.partition.enable  | No       | Specifies whether to cache metadata for all partitions of a table inStarRocks. Possible values: true and false. Default value: true. Setting it to true enables caching, while setting it to false disables caching. |
| odps.cache.partition.expire  | No       | The time interval, in seconds, after which StarRocks automatically evicts cached metadata for all partitions of a table. Default value: 86400 (24 hours).                                                            |
| odps.cache.partition.size    | No       | The number of tables for which StarRocks caches metadata for all partitions. Default value: 1000.                                                                                                                    |
| odps.cache.table-name.enable | No       | Specifies whether to cache table information in the project in StarRocks. Possible values: true and false. Default value: false. Setting it to true enables caching, while setting it to false disables caching.     |
| odps.cache.table-name.expire | No       | The time interval, in seconds, after which StarRocks automatically evicts cached table information in the project. Default value: 86400 (24 hours).                                                                  |
| odps.cache.table-name.size   | No       | The number of projects that StarRocks caches. Default value: 1000.                                                                                                                                                   |

### Examples

The following examples create a MaxCompute(ODPS) catalog named `odps_catalog` whose
use `odps_project` as the warehouse project.

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

## View MaxCompute(ODPS) catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md)
to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also
use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md)
to query the creation statement of an external catalog. The following example queries the creation
statement of a MaxCompute(ODPS) catalog named `odps_catalog`:

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## Drop a MaxCompute(ODPS) catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md) to
drop an external catalog.

The following example drops a MaxCompute(ODPS) catalog named `odps_catalog`:

```SQL
DROP CATALOG odps_catalog;
```

## View the schema of a MaxCompute(ODPS) table

You can use one of the following syntaxes to view the schema of a MaxCompute(ODPS) table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Query a MaxCompute(ODPS) table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) to
   view the databases in your MaxCompute(ODPS) cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to switch to
   the destination catalog in the current session:

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   Then, use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to specify the active
   database in the current session:

   ```SQL
   USE <db_name>;
   ```

   Or, you can use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to directly
   specify the active database in the destination catalog:

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query the
   destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Load data from MaxCompute(ODPS)

Suppose you have an OLAP table named `olap_tbl`, you can transform and load data like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## Type mapping

MaxCompute Catalog maps the MaxCompute(ODPS) type to the StarRocks type, and the type conversion is
as follows

| MaxCompute(ODPS) type | StarRocks type      |
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
| DATETIME              | DATETIME            |
| DATETIME              | DATETIME            |
| TIMESTAMP             | DATETIME            |
| ARRAY                 | ARRAY               |
| MAP                   | MAP                 |
| STRUCT                | STRUCT              |

Note: The TIMESTAMP type will lose precision due to type conversion in StarRocks

## CBO Statistics Collection

Since the MaxCompute (ODPS) Catalog cannot automatically collect CBO statistics for MaxCompute (
ODPS) tables in the current version, in some cases, the optimizer may not be able to generate the
most optimal query plan.

Therefore, manually scanning the CBO statistics for MaxCompute (ODPS) tables and importing them into
StarRocks can effectively optimize query times. Suppose there is a MaxCompute table named mc_table,
you can create a manual collection task
using [ANALYZE TABLE](../../sql-reference/sql-statements/data-definition/ANALYZE_TABLE.md) to
collect CBO statistics.

```SQL
ANALYZE TABLE mc_table;
```

## Manually Updating Metadata Cache

By default, StarRocks caches the metadata of MaxCompute (ODPS) to improve query performance.
Therefore, after making structural changes to the table or other updates, you can also
use [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md)
to manually update the metadata of the table, thus ensuring that StarRocks gets the
new metadata information promptly:

```SQL
REFRESH EXTERNAL TABLE <table_name>
```

