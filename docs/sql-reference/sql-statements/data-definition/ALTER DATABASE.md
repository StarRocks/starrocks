# ALTER DATABASE

## Description

Configures the properties of the specified database. (Administrator only)

1. Set database data quota in B/K/KB/M/MB/G/GB/T/TB/P/PB unit.

    ```sql
    ALTER DATABASE db_name SET DATA QUOTA quota;
    ```

2. Rename the database

    ```sql
    ALTER DATABASE db_name RENAME new_db_name;
    ```

3. Set database replica quota

    ```sql
    ALTER DATABASE db_name SET REPLICA QUOTA quota;
    ```

Note:

```plain text
After renaming the database, use REVOKE and GRANT commands to modify the corresponding user permission if necessary.
The database's default data quota and the default replica quota are 2^63-1.
```

## Examples

1. Set specified database data quota

    ```SQL
    ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
    -- The above units are bytes, equivalent to the following statement
    ALTER DATABASE example_db SET DATA QUOTA 10T;
    
    ALTER DATABASE example_db SET DATA QUOTA 100G;
    
    ALTER DATABASE example_db SET DATA QUOTA 200M;
    ```

2. Rename the database example_db as example_db2

    ```SQL
    ALTER DATABASE example_db RENAME example_db2;
    ```

3. Set specified database replica quota

    ```SQL
    ALTER DATABASE example_db SET REPLICA QUOTA 102400;
    ```
