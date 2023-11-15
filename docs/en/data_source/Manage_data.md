# Use catalogs to manage internal and external data

 Catalogs are used to manage data. StarRocks 2.3 and later provide the following two types of catalogs:

- **Internal catalog**: stores the databases and tables of StarRocks. You can use an internal catalog to manage internal data. For example, if you execute the CREATE DATABASE and CREATE TABLE statements to create a database and a table, they are stored in an internal catalog. Each StarRocks cluster has a default internal catalog whose name is `default_catalog`. Currently, you cannot modify the name of the internal catalog or create a new internal catalog.

- **External catalog**: is used to manage data in external sources. When you create an external catalog for an external source, you need to specify the access information of the external source. After the catalog is created, you can use it to query external data without the need to create external tables.

## Create an external catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name> PROPERTIES ("key"="value", ...);
```

When you create an external catalog, you need to add configuration items in `PROPERTIES` based on the external data source. For different external data sources, you need to add different configuration items. StarRocks 2.3 only supports creating an external catalog for Apache Hive™. After the catalog is created, you can directly query all data in a Hive cluster without using external tables. Additionally, StarRocks only allows you to query tables in the catalog. It does not allow you to write data to tables in the catalog.

### Examples

Create an external catalog named `hive_catalog0`.

```SQL
CREATE EXTERNAL CATALOG hive_catalog0 
PROPERTIES(
  "type"="hive", 
  "hive.metastore.uris"="thrift://127.0.0.1:9083"
);
```

The following table describes the related parameters.

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| type                | The resource type. Set the value to `hive`.                  |
| hive.metastore.uris | The URI of the Hive metastore. The parameter value is in the following format: `thrift://< IP address of Hive metadata >:< port number >`. The port number defaults to 9083. |

## Query data

### Query internal data

1. After logging in to StarRocks from the MySQL client, you connect to the `default_catalog` by default.

2. Run the `show databases` command or the `show databases from default_catalog` command to view all internal databases in the current cluster.

3. Query data in `default_catalog` by specifying a database name and a table name.

### Query external data

1. After logging in to StarRocks from the MySQL client, you connect to the `default_catalog` by default.

2. Run the `show catalogs` command to view all existing catalogs and find the specific external catalog. Then, run the `show databases from external_catalog` command to view all databases in the external catalog. For example, to view all databases in `hive_table`, run `show databases from hive_catalog`.

3. Run the `use external_catalog.database` command to switch the current session to the specific external catalog and database.

4. Query data in the database by specifying a table name.

### Cross-catalog query

If you want to perform a cross-catalog query, use the `catalog_name.database_name` or `catalog_name.database_name.table_name` format to reference data to be queried in your current catalog.

### Examples

#### Query internal data

For example, to query data in `olap_db.olap_table`, perform the following steps:

1. After you log in to StarRocks from the MySQL client, check all databases in the internal catalog of the current cluster.

    ```SQL
    show databases;
    ```

    or

    ```SQL
    SHOW DATABASES FROM default_catalog;
    ```

2. Use `olap_db` as the current database.

    ```SQL
    USE olap_db;
    ```

    or

    ```SQL
    use default_catalog.olap_db;
    ```

3. Query data from `olap_table`.

    ```SQL
    SELECT * FROM olap_table limit 1;
    ```

#### Query external data

For example, to query data in `hive_catalog.hive_db.hive_table`, perform the following steps:

1. After you log in to StarRocks from the MySQL client, check all catalogs in the current cluster.

    ```SQL
    show catalogs;
    ```

2. View databases in `hive_table`.

    ```SQL
    show databases from hive_catalog;
    ```

3. Switch the current session to `hive_catalog.hive_db`.

    ```SQL
    USE hive_catalog.hive_db;
    ```

4. Query data from `hive_table`.

    ```SQL
    SELECT * FROM hive_table limit 1;
    ```

#### Cross-catalog query

- Query `hive_table` in `hive_table` when the current session is `default_catalog.olap_db`.

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table;
    ```

- Query `olap_table` in `default_catalog` when the current session is `hive_catalog.hive_db`.

    ```SQL
    SELECT * FROM default_catalog.olap_db.olap_table;
    ```

- Perform a JOIN query on `hive_table` in `hive_catalog` and `olap_table` in `default_catalog` when the current session is `hive_catalog.hive_db`.

    ```SQL
    SELECT * FROM hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

- Perform a JOIN query on`hive_tab``le` in `hive_catalog` and `olap_table` in `default_catalog` by using a JOIN clause when the current session is another catalog.

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

## Drop an external catalog

### Syntax

```SQL
DROP EXTERNAL CATALOG <catalog_name>;
```

### Examples

Drop an external catalog named `hive_catalog`.

```SQL
DROP EXTERNAL CATALOG hive_catalog;
```
