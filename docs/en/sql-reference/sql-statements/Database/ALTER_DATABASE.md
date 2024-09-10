---
displayed_sidebar: docs
---

# ALTER DATABASE

## Description

Configures the properties of the specified database.

:::tip

This operation requires the ALTER privilege on the target database. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

1. Set database data quota in B/K/KB/M/MB/G/GB/T/TB/P/PB.

    ```sql
    ALTER DATABASE <db_name> SET DATA QUOTA <quota>;
    ```

2. Rename a database.

    ```sql
    ALTER DATABASE <db_name> RENAME <new_db_name>;
    ```

3. Set database replica quota.

    ```sql
    ALTER DATABASE <db_name> SET REPLICA QUOTA <quota>;
    ```

Note:

```plain text
- After renaming the database, use REVOKE and GRANT commands to modify the corresponding user permission if necessary.
- The database's default data quota and the default replica quota are 2^63-1.
```

## Examples

1. Set data quota for a database.

    ```SQL
    ALTER DATABASE example_db SET DATA QUOTA 10995116277760B;
    -- The above unit is bytes, equivalent to the following statement.
    ALTER DATABASE example_db SET DATA QUOTA 10T;
    ALTER DATABASE example_db SET DATA QUOTA 100G;
    ALTER DATABASE example_db SET DATA QUOTA 200M;
    ```

2. Rename the database `example_db` as `example_db2`.

    ```SQL
    ALTER DATABASE example_db RENAME example_db2;
    ```

3. Set database replica quota.

    ```SQL
    ALTER DATABASE example_db SET REPLICA QUOTA 102400;
    ```

## References

- [CREATE DATABASE](CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
