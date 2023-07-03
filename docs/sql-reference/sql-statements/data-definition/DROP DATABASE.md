# DROP DATABASE

## Description

Drops a database. In versions earlier than v3.1, this statement only supports dropping a StarRocks database. From v3.1 onwards, this statement also supports dropping a database created within an Iceberg catalog.

## Syntax

```SQL
-- In versions earlier than v2.0
DROP DATABASE [IF EXISTS] [FORCE] db_name
-- From v2.0 to v3.0
DROP DATABASE [IF EXISTS] db_name [FORCE]
-- In v3.1 and later
DROP DATABASE [IF EXISTS] [catalog.]db_name [FORCE]
```

## Usage notes

- For StarRocks databases:

  - After you drop a database by using the DROP DATABASE statement, you can use the [RECOVER](../data-definition/RECOVER.md) statement to restore the database within a specified period of time (24 hours by default).

  - After you drop a database by using the DROP DATABASE FORCE statement, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally this operation is not recommended.

- For databases created within Iceberg catalogs:

  - A database can be successfully dropped by using the DROP DATABASE statement only when the database is empty (namely, the database does not contain any table).

  - When you drop a database by using the DROP DATABASE statement, the database location will not be dropped along with the database.

## Examples

### Drop the `db_test` database

```SQL
DROP DATABASE db_test;
```

### Drop the `db_test` database within the `iceberg_catalog` catalog

```SQL
DROP DATABASE iceberg_catalog.db_test;
```
