---
displayed_sidebar: docs
---

# SHOW TABLE STATUS

## Description

SHOW TABLE STATUS returns metadata for each table or materialized view in a database.
It follows MySQL's SHOW TABLE STATUS for compatibility. Not all columns are
meaningful in StarRocks; some values are empty strings or NULL.

:::tip

You do not need an explicit privilege to run this statement. Results include only
tables or views on which you have any privilege.

:::

## Syntax

```sql
SHOW TABLE STATUS [FROM db | IN db] [LIKE "pattern" | WHERE expr]
```

- db: Target database. Defaults to the current database if omitted.
- pattern: MySQL LIKE pattern using % and _ to filter table names.
- expr: Boolean filter over returned column names, for example
  `WHERE Name = 't1' AND Rows > 0`.

## Returned columns

- Name: Table or materialized view name.
- Engine: Storage engine type (for example, OLAP).
- Version: Always NULL.
- Row_format: Empty string.
- Rows: Estimated number of rows.
- Avg_row_length: Estimated average row length in bytes.
- Data_length: Approximate data size in bytes.
- Max_data_length: NULL.
- Index_length: NULL.
- Data_free: NULL.
- Auto_increment: NULL.
- Create_time: Creation time, formatted in the session time zone.
- Update_time: Last update time if available; may be NULL or empty for some table types.
- Check_time: NULL.
- Collation: utf8_general_ci.
- Checksum: NULL.
- Create_options: Empty string.
- Comment: Table comment or description.

## Examples

1. List all tables in the current database.

    ```SQL
    SHOW TABLE STATUS;
    ```

2. List tables in a specific database whose names contain "example".

    ```SQL
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```

3. Filter by returned columns using WHERE.

    ```SQL
    SHOW TABLE STATUS WHERE Name = 't1';
    ```

4. Combine filters.

    ```SQL
    SHOW TABLE STATUS IN db WHERE Engine = 'OLAP' AND Rows > 0;
    ```
