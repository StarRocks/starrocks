# DROP TABLE

## Description

This statement is used to delete a table.

## Syntax

```SQL
DROP TABLE [IF EXISTS] [[catalog.]db_name.]table_name [FORCE]
```

## Usage notes

- If a table was deleted within 24 hours by using the DROP TABLE statement, you can use the [RECOVER](../data-definition/RECOVER.md) statement to restore the table.

- If DROP Table FORCE is executed, the table will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally this operation is not recommended.

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
