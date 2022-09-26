# Overview

This topic describes what the catalog is, and how to manage and query internal data and external data by using the catalog.

> Note: Only StarRocks 2.3 and later supports the catalog feature.

## Basic concepts

- **Internal data**: refers to the data stored in StarRocks.
- **External data**: refers to the data stored in external data sources, such as Apache Hive™, Apache Iceberg, and Apache Hudi.

## Catalog

Catalogs enable you to manage internal and external data in one system. They offer a flexible way for you to easily query and analyze data that is stored in various external systems. Currently, StarRocks provides two types of catalogs: internal catalog and external catalog.

![figure1](../../assets/3.8.1.png)

- **Internal catalog**: manages internal data of StarRocks. For example, if you execute the CREATE DATABASE or CREATE TABLE statements to create a database or a table, the database or table is stored in the internal catalog. Each StarRocks cluster has only one internal catalog named [default catalog](../catalog/default_catalog.md).

- **External catalog**: manages the access information of external data sources, such as the type of external data source. You can query external data by using external catalogs without the need to create external tables or load data into StarRocks. Currently, you can create the following three types of external catalogs:
  - [Hive catalog](../catalog/hive_catalog.md): used to query data from Hive.
  - [Iceberg catalog](../catalog/iceberg_catalog.md): used to query data from Iceberg.
  - [Hudi catalog](../catalog/hudi_catalog.md): used to query data from Hudi.

## Query data

### Query internal data

To query data in StarRocks, see [Default catalog](../catalog/default_catalog.md).

### Query external data

To query data from external data sources, see [Query external data](../catalog/query_external_data.md).

### Cross-catalog query

To perform a cross-catalog federated query from your current catalog, specify the data you want to query in the `catalog_name.database_name` or `catalog_name.database_name.table_name` format.

- Query `hive_table` in `hive_db` when the current session is `default_catalog.olap_db`.

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

- Perform a JOIN query on `hive_table` in `hive_catalog` and `olap_table` in `default_catalog` by using a JOIN clause when the current session is another catalog.

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```
