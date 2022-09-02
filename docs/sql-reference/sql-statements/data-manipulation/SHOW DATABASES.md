# SHOW DATABASES

## Description

Views databases in your current StarRocks cluster or an external data source. Note that you can only view databases of an external data source in StarRocks 2.3 or later versions.

## Syntax

- View databases in your current StarRocks cluster.

    ```SQL
    SHOW DATABASES;
    ```

- View databases in your current StarRocks cluster or an external data source.

    ```SQL
    SHOW DATABASES FROM catalog_name;
    ```

## Parameters

`catalog_name`: the name of the internal catalog or an external catalog.

- If you set the value of the parameter to the name of the internal catalog, which is `default_catalog`, you can view databases in your current StarRocks cluster.
- If you set the value of the parameter to the name of an external catalog, you can view databases in the corresponding external data source.

## Examples

Example 1: View databases in your current StarRocks cluster.

```SQL
SHOW DATABASES;

+----------+
| Database |
+----------+
| db1      |
| db2      |
| db3      |
+----------+
```

Or

```SQL
SHOW DATABASES FROM default_catalog;

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
