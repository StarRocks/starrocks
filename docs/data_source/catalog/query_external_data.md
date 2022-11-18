# Query external data

This topic guides you through querying data from external data sources by using external catalogs.

## Prerequisites

 External catalogs are created based on external data sources. For information about supported types of external catalogs, see [Catalog](../catalog/catalog_overview.md#catalog).

## Procedure

1. Connect your StarRocks cluster.
   - If you use the MySQL client to connect the StarRocks cluster, you go to `default_catalog` by default after connecting.
   - If you use JDBC to connect the StarRocks cluster, you can go directly to the destination database in the default catalog by specifying `default_catalog.db_name` when connecting.

2. (Optional) Execute the following statement to view all the catalogs and find the external catalog you have created. See [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md) to check the output of this statement.

      ```SQL
      SHOW CATALOGS;
      ```

3. (Optional) Execute the following statement to view all the databases in the external catalog. See [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) to check the output of this statement.

      ```SQL
      SHOW DATABASES FROM catalog_name;
      ```

4. (Optional) Execute the following statement to go to the destination database in the external catalog.

      ```SQL
      USE catalog_name.db_name;
      ```

5. Query external data. For more usages of the SELECT statement, see [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md).

      ```SQL
      SELECT * FROM table_name;
      ```

      If you do not specify the external catalog and database in the preceding steps, you can directly specify them in a select query.

      ```SQL
      SELECT * FROM catalog_name.db_name.table_name;
      ```

## Examples

If you already created a Hive catalog named `hive1` and want to use `hive1` to query data from `hive_db.hive_table` of an Apache Hive™ cluster, you can perform one of the following operations:

```SQL
USE hive1.hive_db;
SELECT * FROM hive_table limit 1;
```

Or

```SQL
SELECT * FROM hive1.hive_db.hive_table limit 1;
```

## References

To query data from your StarRocks cluster, see [Default catalog](../catalog/default_catalog.md).
