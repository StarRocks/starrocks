# DROP TABLE

## Description

Deletes a table. In versions earlier than v3.1, this statement only supports dropping a StarRocks table. From v3.1 onwards, this statement also supports dropping an Iceberg table created within an Iceberg catalog.

## Syntax

```SQL
-- In versions earlier than v3.1
DROP TABLE [IF EXISTS] [db_name.]table_name [FORCE]
-- In v3.1 and later
DROP TABLE [IF EXISTS] [[catalog.]db_name.]table_name [FORCE]
```

## Usage notes

- For StarRocks tables:

  - After you drop a table by using the DROP TABLE statement, you can use the [RECOVER](../data-definition/RECOVER.md) statement to restore the table within a specified period of time (24 hours by default).

  - After you drop a table by using the DROP Table FORCE statement, the table will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally this operation is not recommended.

- For Iceberg tables created within Iceberg catalogs:

  - When you drop a table by using the DROP TABLE statement, StarRocks only deletes the table information from the metastore service (Hive Metastore or AWS Glue). However, the table's `tableLocation`  folder and all files in the folder are retained in Iceberg.

  - When you drop a table by using the DROP TABLE FORCE statement, StarRocks deletes the table information from the metastore service (Hive Metastore or AWS Glue) and deletes all metadata and data files that can be indexed from the table. However, the table's `tableLocation/data` and `tableLocation/metadata` folders and all files in the folders are retained in Iceberg.

## Examples

### Drop the table `my_table`

```SQL
DROP TABLE my_table;
```

### Drop the table `my_table` from the database `example_db` if the table exists

```SQL
DROP TABLE IF EXISTS example_db.my_table;
```

### Drop the table `my_table` from the database `example_db` within the catalog `iceberg_catalog`

```SQL
DROP TABLE iceberg_catalog.iceberg_db.my_table;
```

### Force to drop the table `my_table` and clear its data on disk

```SQL
DROP TABLE my_table FORCE;
```
