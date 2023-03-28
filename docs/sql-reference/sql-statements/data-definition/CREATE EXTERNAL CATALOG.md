# CREATE EXTERNAL CATALOG

## Description

Creates an external catalog. You can use external catalogs to query data in external data sources without loading data into StarRocks or creating external tables. Currently, you can create the following types of external catalogs:

- [Hive catalog](../../../data_source/catalog/hive_catalog.md): used for querying data from Apache Hive™.
- [Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md): used for querying data from Apache Iceberg.
- [Hudi catalog](../../../data_source/catalog/hudi_catalog.md): used for querying data from Apache Hudi.

This statement requires no privileges for execution. Before you create external catalogs, configure your StarRocks cluster to meet the requirements of the data storage system (such as Amazon S3), metadata service (such as Hive metastore), and authenticating service (such as Kerberos) of external data sources. For more information, see the "Before you begin" section in each external catalog topic.

## Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
PROPERTIES ("key"="value", ...)
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | Yes          | The name of the external catalog. The naming conventions are as follows:<ul><li>The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.</li><li>The name cannot exceed 64 characters in length.</li></ul> |
<<<<<<< HEAD
| PROPERTIES    | Yes          | The properties of an external catalog. Configure properties based on the types of external catalogs. For more information, see Hive catalog, Iceberg catalog and Hudi catalog. |
=======
| PROPERTIES    | Yes          | The properties of an external catalog. Configure properties based on the types of external catalogs. For more information, see [Hive catalog](../../../data_source/catalog/hive_catalog.md), [Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md), [Hudi catalog](../../../data_source/catalog/hudi_catalog.md), and [Delta Lake catalog](../../../data_source/catalog/deltalake_catalog.md). |
>>>>>>> 730ecdfa1 ([Doc] fix CREATE EXTERNAL CATALOG (#20398))

## Examples

Example 1: Create a Hive catalog named `hive1`.

```SQL
CREATE EXTERNAL CATALOG hive1
PROPERTIES(
   "type"="hive", 
   "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

Example 2:  Create an Iceberg catalog named `iceberg1`.

```SQL
CREATE EXTERNAL CATALOG iceberg1
PROPERTIES(
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "iceberg.catalog.hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

Example 3: Create a Hudi catalog named `hudi1`.

```SQL
CREATE EXTERNAL CATALOG hudi1
PROPERTIES(
    "type"="hudi",
    "hive.metastore.uris"="thrift://x.x.x.x:9083"
);
```

## References

- To view all catalogs in your StarRocks cluster, see [SHOW CATALOGS](../data-manipulation/SHOW%20CATALOGS.md).
- To delete an external catalog from your StarRocks cluster, see [DROP CATALOG](../data-definition/DROP%20CATALOG.md).
