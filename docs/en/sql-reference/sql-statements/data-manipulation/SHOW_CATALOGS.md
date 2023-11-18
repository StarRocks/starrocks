---
displayed_sidebar: "English"
---

# SHOW CATALOGS

## Description

Query all catalogs in the current StarRocks cluster, including the internal catalog and external catalogs.

> **NOTE**
>
> SHOW CATALOGS returns external catalogs to users who have the USAGE privilege on that external catalog. If users or roles do not have this privilege on any external catalog, this command returns only the default_catalog.

## Syntax

```SQL
SHOW CATALOGS
```

## Output

```SQL
+----------+--------+----------+
| Catalog  | Type   | Comment  |
+----------+--------+----------+
```

The following table describes the fields returned by this statement.

| **Field** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Catalog       | The catalog name.                                            |
| Type          | The catalog type. `Internal` is returned if the catalog is `default_catalog`. The corresponding catalog type is returned if the catalog is an external catalog, such as `Hive`, `Hudi`, or `Iceberg`. |
| Comment       | The comments of a catalog. StarRocks does not support adding comments to an external catalog. Therefore, the value is `NULL` for an external catalog. If the catalog is `default_catalog`, the comment is `An internal catalog contains this cluster's self-managed tables.` by default. `default_catalog` is the only internal catalog in a StarRocks cluster. |

## Examples

Query all catalogs in the current cluster.

```SQL
SHOW CATALOGS\G
*************************** 1. row ***************************
Catalog: default_catalog
   Type: Internal
Comment: An internal catalog contains this cluster's self-managed tables.
*************************** 2. row ***************************
Catalog: hudi_catalog
   Type: Hudi
Comment: NULL
*************************** 3. row ***************************
Catalog: iceberg_catalog
   Type: Iceberg
Comment: NULL
```
