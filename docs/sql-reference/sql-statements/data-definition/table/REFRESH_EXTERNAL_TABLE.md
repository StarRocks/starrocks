---
displayed_sidebar: "English"
---

# REFRESH EXTERNAL TABLE

## Description

Updates Hive and Hudi metadata cached in StarRocks. This statement is used in one of the following scenarios:

- **External table**: When using a Hive external table or Hudi external table to query data in Apache Hive™ or Apache Hudi, you can execute this statement to update the metadata of a Hive table or Hudi table cached in StarRocks.
- **External catalog**: When using a [Hive catalog](../../../../data_source/catalog/hive_catalog.md) or [Hudi catalog](../../../../data_source/catalog/hudi_catalog.md) to query data in Hive or Hudi, you can execute this statement to update the metadata of a Hive table or Hudi table cached in StarRocks.

## Basic concepts

- **Hive external table**: is created and stored in StarRocks. You can use it to query Hive data.
- **Hudi external table**: is created and stored in StarRocks. You can use it to query Hudi data.
- **Hive table**: is created and stored in Hive.
- **Hudi table**: is created and stored in Hudi.

## Syntaxes and parameters

The following describes the syntaxes and parameters based on different cases:

- External table

    ```SQL
    REFRESH EXTERNAL TABLE table_name 
    [PARTITION ('partition_name', ...)]
    ```

    | **Parameter**  | **Required** | **Description**                                              |
    | -------------- | ------------ | ------------------------------------------------------------ |
    | table_name     | Yes          | The name of a Hive external table or Hudi external table.    |
    | partition_name | No           | The names of the partitions of a Hive table or Hudi table. Specifying this parameter updates the metadata of the partitions of the Hive table and Hudi table cached in StarRocks. |

- External catalog

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)]
    ```

    | **Parameter**    | **Required** | **Description**                                              |
    | ---------------- | ------------ | ------------------------------------------------------------ |
    | external_catalog | No           | The name of a Hive catalog or Hudi catalog.                  |
    | db_name          | No           | The name of the database where a Hive table or Hudi table resides. |
    | table_name       | Yes          | The name of a Hive table or a Hudi table.                    |
    | partition_name   | No           | The names of the partitions of a Hive table or Hudi table. Specifying this parameter updates the metadata of the partitions of the Hive table and Hudi table cached in StarRocks. |

## Usage notes

Only users who have the `ALTER_PRIV` privilege can execute this statement to update the metadata of Hive tables and Hudi tables cached in StarRocks.

## Examples

Usage examples in different cases are as follows:

### External table

Example 1: Update the cached metadata of the corresponding Hive table in StarRocks by specifying the external table `hive1`.

```SQL
REFRESH EXTERNAL TABLE hive1;
```

Example 2: Update the cached metadata of the partitions of the corresponding Hudi table in StarRocks by specifying the external table `hudi1` and the partitions of the corresponding Hudi table.

```SQL
REFRESH EXTERNAL TABLE hudi1
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```

### External catalog

Example 1: Update the cached metadata of `hive_table` in StarRocks.

```SQL
REFRESH EXTERNAL TABLE hive_catalog.hive_db.hive_table;
```

Or

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table;
```

Example 2: Update the cached metadata of the partitions of `hudi_table` in StarRocks.

```SQL
REFRESH EXTERNAL TABLE hudi_catalog.hudi_db.hudi_table
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```
