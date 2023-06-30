# DROP DATABASE

## Description

Drops a database in StarRocks. From v3.1 onwards, this statement can be used to drop a database created within an Iceberg catalog.

## Syntax

```SQL
-- Versions earlier than v2.0
DROP DATABASE [IF EXISTS] [FORCE] db_name
-- v2.0 and later
DROP DATABASE [IF EXISTS] db_name [FORCE]
-- v3.1 and later
DROP DATABASE [IF EXISTS] [catalog.]db_name [FORCE]
```

## Usage notes

- After you drops a StarRocks by using the DROP DATABASE statement, you can use the [RECOVER](../data-definition/RECOVER.md) statement to restore the database within a specified period of time (24 hours by default).

- After you drops a StarRocks by using the DROP DATABASE FORCE statement, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally this operation is not recommended.

## Examples

### Drop the `db_test` database

```SQL
DROP DATABASE db_test;
```

### Drop the `db_test` database within the `iceberg_catalog` catalog

```SQL
DROP DATABASE iceberg_catalog.db_test;
```
