---
displayed_sidebar: "English"
---

# MaxCompute(ODPS) catalog

StarRocks supports MaxCompute(ODPS) catalogs from v3.3 onwards.

A MaxCompute(ODPS) catalog is a kind of external catalog that enables you to query data from Aliyun
MaxCompute(ODPS) without ingestion.

Also, you can directly transform and load data from MaxCompute(ODPS) by
using [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) based on
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

| Parameter            | Required	 | Description                                                                                                                                                                                                                                                                                                                   |
|----------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | Yes       | The target MaxCompute project name to access. If you have created a standard-mode workspace, please differentiate between the project names for production and development environments by specifying the project_name.                                                                                                       |
| odps.project         | Yes       | The connection address of the MaxCompute service. Configure the Endpoint based on the region and network connection method chosen during the creation of the MaxCompute project.                                                                                                                                              |
| odps.access.id       | Yes       | The AccessKey ID of your Alibaba Cloud account or RAM user.                                                                                                                                                                                                                                                                   |
| odps.access.key      | Yes       | The AccessKey Secret corresponding to the AccessKey ID.                                                                                                                                                                                                                                                                       |
| odps.tunnel.endpoint | No        | The public access link for the Tunnel service. If you haven't configured a Tunnel Endpoint, Tunnel will automatically route to the Tunnel Endpoint corresponding to the network where the MaxCompute service resides. If you have configured a Tunnel Endpoint, it will take precedence and automatic routing will not occur. |
| odps.tunnel.quota    | No        | The quota name used by the Tunnel service. If not configured, the default quota will be used.                                                                                                                                                                                                                                 |

#### ScanParams

Configuration parameters for StarRocks accessing files stored in the MaxCompute (ODPS) cluster. This
parameter group is optional.

| Parameter	           | Required	 | Description                                                                                                                                        |
|----------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.split.policy	   | No	       | The shard policy used when scanning data.Possible values: size (shard by data size) and row_offset (shard by number of rows). Default value: size. |
| odps.split.row.count | 	No       | 	The maximum number of rows per shard when odps.split.policy is set to row_offset. Default value: 4 * 1024 * 1024 = 4194304.                       |

#### CachingMetaParams

A set of parameters specifying the caching strategy for metadata caching. StarRocks caches Hive
metadata based on this strategy. This parameter group is optional.
CachingMetaParams includes the following parameters.

| Parameter                   | Required | Description                                                                                                                                                                                                          |
|-----------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.cache.table.enable     | No       | Specifies whether to cache table metadata in StarRocks. Possible values:true and false. Default value: true. Setting it to true enables caching, while setting it to false disables caching.                         |
| odps.cache.table.expire     | No       | The time interval, in seconds, after which StarRocks automatically evicts cached metadata for tables or partitions. Default value: 86400 (24 hours).                                                                 |
| odps.cache.table.size       | No       | The number of table metadata entries that StarRocks caches. Default value:1000.                                                                                                                                      |
| odps.cache.partition.enable | No       | Specifies whether to cache metadata for all partitions of a table inStarRocks. Possible values: true and false. Default value: true. Setting it to true enables caching, while setting it to false disables caching. |
| odps.cache.partition.expire | No       | The time interval, in seconds, after which StarRocks automatically evicts cached metadata for all partitions of a table. Default value: 86400 (24 hours).                                                            |
| odps.cache.partition.size   | No       | The number of tables for which StarRocks caches metadata for all partitions. Default value: 1000.                                                                                                                    |
| odps.cache.project.enable   | No       | Specifies whether to cache table information in the project in StarRocks. Possible values: true and false. Default value: true. Setting it to true enables caching, while setting it to false disables caching.      |
| odps.cache.project.expire   | No       | The time interval, in seconds, after which StarRocks automatically evicts cached table information in the project. Default value: 86400 (24 hours).                                                                  |
| odps.cache.project.size     | No       | The number of projects that StarRocks caches. Default value: 1000.                                                                                                                                                   |

### Examples

The following examples create a MaxCompute(ODPS) catalog named `odps_catalog` whose
use `odps_project` as the warehouse project.

```SQL

CREATE EXTERNAL CATALOG odps_catalog PROPERTIES(
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
DROP Catalog odps_catalog;
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
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM MaxCompute(ODPS)_table;
```
