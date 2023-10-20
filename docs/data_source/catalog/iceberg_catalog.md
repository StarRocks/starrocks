# Iceberg catalog

This topic describes how to create an Iceberg catalog, and how to configure your StarRocks cluster for querying data from Apache Iceberg.

An Iceberg catalog is an external catalog supported in StarRocks 2.4 and later versions. It enables you to query data from Iceberg without loading data into StarRocks or creating external tables.

## Usage notes

- StarRocks supports querying data files of Iceberg in the following formats: Parquet and ORC
- StarRocks supports querying compressed data files of Iceberg in the following formats: gzip, Zstd, LZ4, and Snappy.
- StarRocks supports querying Iceberg data of the following types: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL(P, S), DATE, TIME, TIMESTAMP, STRING, UUID, LIST, FIXED(L), and BINARY. Note that an error occurs when you query Iceberg data of unsupported data types, including TIMESTAMPTZ, STRUCT, and MAP.
- StarRocks supports querying Versions 1 tables (Analytic Data Tables). Versions 2 tables (Row-level Deletes) are not supported. For the differences between these two types of tables, see [Iceberg Table Spec](https://iceberg.apache.org/spec/).
- You can use the [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) statement to view the schema of an Iceberg table in StarRocks 2.4 and later versions.

## Before you begin

Before you create an Iceberg catalog, configure your StarRocks cluster so that StarRocks can access the data storage system and metadata service of your Iceberg cluster. StarRocks supports two data storage systems for Iceberg: HDFS and Amazon S3. StarRocks supports two metadata services for Iceberg: Hive metastore and custom metadata service. The configurations required for an Iceberg catalog are the same as that required for a Hive catalog. Therefore, see [Hive catalog](../catalog/hive_catalog.md) for more information about the configurations.

## Create an Iceberg catalog

After you complete the preceding configurations, you can create an Iceberg catalog.

### Syntax

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

> **Note**
>
> Before querying Iceberg data, you must add the mapping between the domain name and IP address of Hive metastore node to the **/etc/hosts** path. Otherwise, StarRocks may fail to access Hive metastore when you start a query.

### Parameters

- `catalog_name`: the name of the Iceberg catalog. This parameter is required.<br/>The naming conventions are as follows:
  - The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
  - The name cannot exceed 64 characters in length.

- `PROPERTIES`: the properties of the Iceberg catalog. This parameter is required. You need to configure this parameter based on the metadata service used by your Iceberg cluster. In Iceberg, there is a component called [catalog](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties), which is used to store the mapping of Iceberg tables and paths of storing Iceberg tables. If you use different metadata services, you need to configure different types of catalogs for your Iceberg cluster.
  - Hive metastore: If you use Hive metastore, configure HiveCatalog for your Iceberg cluster.
  - Custom metadata service: If you use the custom metadata service, configure a custom catalog for your Iceberg cluster.

#### Hive metastore

If you use Hive metastore for your Iceberg cluster, configure the following properties for the Iceberg catalog.

| **Property**           | **Required** | **Description**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| type                   | Yes          | The type of the data source. Set the value to `iceberg`.     |
| iceberg.catalog.type   | Yes          | The type of the catalog configured your Iceberg cluster. Set the value to `HIVE`. |
| iceberg.catalog.hive.metastore.uris    | Yes          | The URI of the Hive metastore. The parameter value is in the following format: `thrift://<IP address of Hive metastore>:<port number>`. The port number defaults to 9083. |

#### Custom metadata service

If you use a custom metadata service for your Iceberg cluster, you need to create a custom catalog class (The class name of the custom catalog cannot be duplicated with the name of the class that already exists in StarRocks) and implement the related interface in StarRocks so that StarRocks can access the custom metadata service. The custom catalog class needs to inherit the abstract class BaseMetastoreCatalog. For information about how to create a custom catalog in StarRocks, see [IcebergHiveCatalog](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/external/iceberg/IcebergHiveCatalog.java). After the custom catalog is created, package the catalog and its related files, and then place them under the **fe/lib** path of each FE. Then restart each FE.

After you complete the preceding operations, you can create an Iceberg catalog and configure its properties.

| **Property**           | **Required** | **Description**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| type                   | Yes          | The type of the data source. Set the value to `iceberg`.     |
| iceberg.catalog.type   | Yes          | The type of the catalog configured your Iceberg cluster. Set the value to `CUSTOM`. |
| iceberg.catalog-impl   | Yes          | The fully qualified class name of the custom catalog. FEs search for the catalog based on this name. If the custom catalog contains custom configuration items, you must add them to the `PROPERTIES` parameter as key-value pairs when you create an Iceberg catalog. |

## Caching strategy of Iceberg metadata

StarRocks does not cache Iceberg metadata. When you query Iceberg data, Iceberg directly returns the latest data by default.

## Use catalog to query Iceberg data

After you complete all the preceding operations, you can use the Iceberg catalog to query Iceberg data. For more information, see [Query external data](../catalog/query_external_data.md).

## References

- To view examples of creating an external catalog, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md).
- To view all catalogs in the current StarRocks cluster, see [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md).
- To delete an external catalog, see [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md).
