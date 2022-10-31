# Default catalog

This topic describes what the default catalog is, and how to query the internal data of StarRocks by using the default catalog.

StarRocks 2.3 and later provide an internal catalog to manage the internal data of StarRocks. Each StarRocks cluster has only one internal catalog named `default_catalog`. Currently, you cannot modify the name of the internal catalog or create a new internal catalog.

## Query internal data

1. Connect your StarRocks cluster.
   - If you use the MySQL client to connect the StarRocks cluster, you go to `default_catalog` by default after connecting.
   - If you use JDBC to connect the StarRocks cluster, you can go directly to the destination database in the default catalog by specifying `default_catalog.db_name` when connecting.

2. (Optional) Execute the following statement to view all databases in StarRocks. See [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) to view the output of this statement.

      ```SQL
      SHOW DATABASES;
      ```

      Or

      ```SQL
      SHOW DATABASES FROM catalog_name;
      ```

3. (Optional) Execute the following statement to go to the destination database.

      ```SQL
      USE db_name;
      ```

      Or

      ```SQL
      USE default_catalog.db_name；
      ```

4. Execute the [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) statement to query internal data.

## Examples

To query data in `olap_db.olap_table`, perform the following steps:

1. Use `olap_db` as the current database.

      ```SQL
      USE olap_db;
      ```

2. Query data from `olap_table`.

      ```SQL
      SELECT * FROM olap_table limit 1;
      ```

## References

To query data from external data sources, see [Query external data](../catalog/query_external_data.md).
