---
displayed_sidebar: docs
---

# REFRESH EXTERNAL TABLE

## Description

Updates metadata cached in StarRocks. The metadata is from tables in data lakes. This statement is used in the following scenarios:

- **External table**: When using a Hive external table or Hudi external table to query data in Apache Hive™ or Apache Hudi, you can execute this statement to update the metadata of a Hive table or Hudi table cached in StarRocks.
- **External catalog**: When using a [Hive catalog](../../../data_source/catalog/hive_catalog.md), [Hudi catalog](../../../data_source/catalog/hudi_catalog.md), [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md), or [MaxCompute Catalog](../../../data_source/catalog/maxcompute_catalog.md) (since v3.3) to query data in the corresponding data source, you can execute this statement to update the metadata cached in StarRocks.

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
    | external_catalog | No           | The name of the external catalog, which supports Hive, Hudi, Delta Lake, and MaxCompute (since v3.3) catalogs.                  |
    | db_name          | No           | The name of the database where the destination table resides. |
    | table_name       | Yes          | The name of the table.                    |
    | partition_name   | No           | The names of the partitions. Specifying this parameter updates the metadata of the partitions of the destination table cached in StarRocks. |

## Usage notes

Only users who have the `ALTER_PRIV` privilege can execute this statement to update the metadata of Hive tables and Hudi tables cached in StarRocks.

## Examples

Usage examples in different cases are as follows:

### External table

Example 1: Update the cached metadata of the corresponding Hive table in StarRocks by specifying the external table `hive1`.

```SQL
REFRESH EXTERNAL TABLE hive1;
```

Example 2: Update the cached metadata of the partitions of the corresponding Hudi table in StarRocks by specifying the external table `hudi1` and the partitions in that table.

```SQL
REFRESH EXTERNAL TABLE hudi1
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```

### External catalog

Example 1: Update the metadata of `hive_table` cached in StarRocks.

```SQL
REFRESH EXTERNAL TABLE hive_catalog.hive_db.hive_table;
```

Or

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table;
```

Example 2: Update the metadata of the second-level partition `p2` in `hive_table` cached in StarRocks.

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table PARTITION ('p1=${date}/p2=${hour}');
```

Example 3: Update the metadata of the partitions of `hudi_table` cached in StarRocks.

```SQL
REFRESH EXTERNAL TABLE hudi_catalog.hudi_db.hudi_table
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```
