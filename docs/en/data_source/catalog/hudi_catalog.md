# Hudi catalog

This topic describes how to create a Hudi catalog, and how to configure your StarRocks cluster for querying data from Apache Hudi.

A Hudi catalog is an external catalog supported in StarRocks 2.4 and later versions. It enables you to query data from Hudi without loading data into StarRocks or creating external tables.

## Usage notes

- StarRocks supports querying data files of Hudi in the following formats: Parquet and ORC.
- StarRocks supports querying compressed data files of Hudi in the following formats: gzip, Zstd, LZ4, and Snappy.
- StarRocks supports querying Hudi data of the following types: BOOLEAN, INTEGER, DATE, TIME, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, and VARCHAR. Note that an error occurs when you query Hudi data of unsupported data types, including ARRAY, MAP, and STRUCT.
- StarRocks supports querying Copy On Write tables. Merge On Read tables are not supported. For the differences between these two types of tables, see [Table & Query Types](https://hudi.apache.org/docs/table_types).
- You can use the [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) statement to view the schema of a Hudi table in StarRocks 2.4 and later versions.

## Before you begin

Before you create a Hudi catalog, configure your StarRocks cluster so that StarRocks can access the data storage system and metadata service of your Hudi cluster. StarRocks supports two data storage systems for Hudi: HDFS and Amazon S3. StarRocks supports one metadata service for Hudi: Hive metastore. The configurations required for a Hudi catalog are the same as that required for a Hive catalog. Therefore, see [Hive catalog](../catalog/hive_catalog.md#before-you-begin) for more information about the configurations.

## Create a Hudi catalog

After you complete the preceding configurations, you can create a Hudi catalog using the following syntax:

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

The parameter description is as follows:

- `catalog_name`: the name of the Hudi catalog. This parameter is required.<br/>The naming conventions are as follows:

  - The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
  - The name cannot exceed 64 characters in length.

- `PROPERTIES`: the properties of the Hudi catalog. <br/> This parameter is required. You can configure the following properties:

    | **Property**        | **Required** | **Description**                                              |
    | ------------------- | ------------ | ------------------------------------------------------------ |
    | type                | Yes          | The type of the data source. Set the value to `hudi`.        |
    | hive.metastore.uris | Yes          | The URI of the Hive metastore. The parameter value is in the following format: `thrift://<IP address of Hive metastore>:<port number>`. The port number defaults to 9083. |

> **Note**
>
> Before querying Hudi data, you must add the mapping between the domain name and IP address of Hive metastore node to the **/etc/hosts** path. Otherwise, StarRocks may fail to access Hive metastore when you start a query.

## Caching strategy of Hudi metadata

StarRocks develops a query execution plan based on the metadata of Hudi tables. Therefore, the response time of Hive metastore directly affects the time consumed by a query. To reduce the impact, StarRocks provides caching strategies, based on which StarRocks can cache and update the metadata of Hudi tables, such as partition statistics and file information of partitions. Currently, StarRocks only supports the asynchronous update strategy.

### How it works

If a query hits a partition of a Hudi table, StarRocks asynchronously caches the metadata of the partition. If another query hits the partition again and the time interval from the last update exceeds the default time interval, StarRocks asynchronously updates the metadata cached in StarRocks. Otherwise, the cached metadata will not be updated. This process of update is called lazy update.

You can set the default time interval by the `hive_meta_cache_refresh_interval_s` parameter. The parameter value defaults to `7200`. Unit: seconds. You can set this parameter in the **fe.conf** file of each FE, and then restart each FE to make the parameter value take effect.

If a query hits a partition and the time interval from the last update exceeds the default time interval, but the metadata cached in StarRocks is not updated, that means the cached metadata is invalid and will be cached again at the next query. You can set the time period during which the cached metadata is valid by the `hive_meta_cache_ttl_s` parameter. The parameter value defaults to `86400`. Unit: Seconds. You can set this parameter in the **fe.conf** file of each FE, and then restart each FE to make the parameter value take effect.

### Examples

For example, there is a Hudi table named `table1`, which has four partitions: `p1`, `p2`, `p3`, and `p4`. A query hit `p1`, and StarRocks cached the metadata of `p1`. If the default time interval to update the metadata cached in StarRocks is 1 hour, there are the following two situations for subsequent updates:

- If another query hits `p1` again and the current time from the last update is more than 1 hour, StarRocks asynchronously updates the cached metadata of `p1`.
- If another query hits `p1` again and the current time from the last update is less than 1 hour, StarRocks does not asynchronously update the cached metadata of `p1`.

### Manual update

To query the latest Hudi data, make sure that the metadata cached in StarRocks is updated to the latest. If the time interval from the last update does not exceed the default time interval, you can manually update the cached metadata before sending a query.

- Execute the following statement to synchronize the schema changes (such as adding columns or removing partitions) of a Hudi table to StarRocks.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- Execute the following statement to synchronize the data changes (such as data ingestion) of a Hudi table to StarRocks.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

For more information about the parameter descriptions and examples of using the REFRESH EXTERNAL TABEL statement, see [REFRESH EXTERNAL TABEL](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md).

## Use catalog to query Hudi data

After you complete all the preceding operations, you can use the Hudi catalog to query Hudi data. For more information, see [Query external data](../catalog/query_external_data.md).

## References

- To view examples of creating an external catalog, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md).
- To view all catalogs in the current StarRocks cluster, see [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md).
- To delete an external catalog, see [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md).
