---
displayed_sidebar: "English"
---

# Default catalog

This topic describes what the default catalog is, and how to query the internal data of StarRocks by using the default catalog.

StarRocks 2.3 and later provide an internal catalog to manage the internal data of StarRocks. Each StarRocks cluster has only one internal catalog named `default_catalog`. Currently, you cannot modify the name of the internal catalog or create a new internal catalog.

## Query internal data

1. Connect your StarRocks cluster.
   - If you use the MySQL client to connect the StarRocks cluster, you go to `default_catalog` by default after connecting.
   - If you use JDBC to connect the StarRocks cluster, you can go directly to the destination database in the default catalog by specifying `default_catalog.db_name` when connecting.

2. (Optional) Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) to view databases:

      ```SQL
      SHOW DATABASES;
      ```

      Or

      ```SQL
      SHOW DATABASES FROM <catalog_name>;
      ```

3. (Optional) Use [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to switch to the destination catalog in the current session:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    Then, use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to specify the active database in the current session:

    ```SQL
    USE <db_name>;
    ```

    Or, you can use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to directly go to the active database in the destination catalog:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

4. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query internal data:

      ```SQL
      SELECT * FROM <table_name>;
      ```

      If you do not specify the active database in the preceding steps, you can directly specify it in a select query:

      ```SQL
      SELECT * FROM <db_name>.<table_name>;
      ```

      Or

      ```SQL
      SELECT * FROM default_catalog.<db_name>.<table_name>;
      ```

## Examples

To query data in `olap_db.olap_table`, you can perform one of the following operations:

```SQL
USE olap_db;
SELECT * FROM olap_table limit 1;
```

Or

```SQL
SELECT * FROM olap_db.olap_table limit 1;     
```

Or

```SQL
SELECT * FROM default_catalog.olap_db.olap_table limit 1;      
```

## References

To query data from external data sources, see [Query external data](../catalog/query_external_data.md).
