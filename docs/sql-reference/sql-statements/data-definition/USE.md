# USE

## Description

Specifies the active database for your session. You can then perform operations, such as creating tables and executing queries.

## Syntax

```SQL
USE [<catalog_name>.]<db_name>
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | No           | The catalog name.<ul><li>If this parameter is not specified, a database in `default_catalog` is used by default.</li><li>You must specify this parameter when you use a database from an external catalog. For more information, see Example 2.</li><li>You must specify this parameter when you switch databases between different catalogs. For more information, see Example 3.</li></ul> |
| db_name       | Yes          | The database name. The database must exist.                  |

## Examples

Example 1: Use `example_db` from `default_catalog` as the active database of your session.

```SQL
USE default_catalog.example_db;
```

Or

```SQL
USE example_db;
```

Example 2: Use `example_db` from `hive_catalog` as the active database of your session.

```SQL
USE hive_catalog.example_db;
```

Example 3: Switch the active database of your session from `hive_catalog.example_table1` to `iceberg_catalog.example_table2`.

```SQL
USE iceberg_catalog.example_table2;
```
