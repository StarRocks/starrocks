# SHOW DATABASES

## Description

Views databases in your current StarRocks cluster or an external data source. Note that you can only view databases of an external data source in StarRocks 2.3 or later versions.

## Syntax

```SQL
SHOW DATABASES [FROM <catalog_name>]
```

## Parameters

| **Parameter**     | **Required** | **Description**                                              |
| ----------------- | ------------ | ------------------------------------------------------------ |
| catalog_name      | No           | The name of the internal catalog or an external catalog.<ul><li>If you do not specified the parameter or set the value of the parameter to the name of the internal catalog, which is default_catalog, you can view databases in your current StarRocks cluster.</li><li>If you set the value of the parameter to the name of an external catalog, you can view databases in the corresponding external data source.</li></ul> |

## Examples

Example 1: View databases in your current StarRocks cluster.

```SQL
SHOW DATABASES;
```

Or

```SQL
SHOW DATABASES FROM default_catalog;
```

The output of the preceding statements is as follows.

```SQL
+----------+
| Database |
+----------+
| db1      |
| db2      |
| db3      |
+----------+
```

Example 2: View databases in a Hive cluster by using the `Hive1` external catalog.

```SQL
SHOW DATABASES FROM hive1;

+-----------+
| Database  |
+-----------+
| hive_db1  |
| hive_db2  |
| hive_db3  |
+-----------+
```
