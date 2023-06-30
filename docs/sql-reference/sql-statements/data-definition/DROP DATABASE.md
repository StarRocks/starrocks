# DROP DATABASE

## Description

Drops a database in StarRocks.

## Syntax

```SQL
-- Versions earlier than v2.0
DROP DATABASE [IF EXISTS] [FORCE] db_name
-- v2.0 and later
DROP DATABASE [IF EXISTS]  [catalog.]db_name [FORCE]
```

## Usage notes

- After executing DROP DATABASE for a while, you can restore the dropped database through RECOVER statement. See [RECOVER](../data-definition/RECOVER.md) for more details.

- If DROP DATABASE FORCE is executed, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally this operation is not recommended.

## Examples

The following example drops a database named `db_text`:

```SQL
DROP DATABASE db_test;
```
