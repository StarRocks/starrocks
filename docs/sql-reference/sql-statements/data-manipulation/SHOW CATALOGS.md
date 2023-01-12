# SHOW CATALOGS

## Description

Query all catalogs in the current StarRocks cluster, including the internal catalog and external catalogs.

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

The following table describes the parameters returned by this statement.

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Catalog       | The catalog name.                                            |
| Type          | The catalog type.                                            |
| Comment       | The comments of a catalog.StarRocks does not support adding comments to an external catalog. Therefore, the parameter value is `NULL` for an external catalog. If the returned catalog is `default_catalog`, the parameter value is `Internal Catalog`. `default_catalog` is the only internal catalog in a StarRocks cluster. |

## Examples

Query all catalogs in the current cluster.

```SQL
SHOW CATALOGS;

+---------------------------------------------------+----------+------------------+
| Catalog                                           | Type     | Comment          |
+---------------------------------------------------+----------+------------------+
| default_catalog                                   | Internal | Internal Catalog |
| hive_catalog_1acaac1c_19e0_11ed_9ca4_00163e0e550b | hive     | NULL             |
| hudi_catalog_dfd748ce_18a2_11ed_9f50_00163e0e550b | hudi     | NULL             |
| iceberg_catalog                                   | iceberg  | NULL             |
+---------------------------------------------------+----------+------------------+
```
