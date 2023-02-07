# Hudi catalog

This topic describes how to create a Hudi catalog, and how to configure your StarRocks cluster for querying data from Apache Hudi.

A Hudi catalog is an external catalog supported in StarRocks 2.4 and later versions. It enables you to query data from Hudi without loading data into StarRocks or creating external tables.

## Usage notes

- StarRocks supports querying data files of Hudi in the following formats: Parquet and ORC.
- StarRocks supports querying compressed data files of Hudi in the following formats: gzip, Zstd, LZ4, and Snappy.
- StarRocks does not support querying data of the ARRAY type from Hudi. Note that errors occur if you query Hudi data of the ARRAY type.
- StarRocks supports querying Copy On Write and Merge On Read tables (MOR tables are supported from v2.5). For the differences between these two types of tables, see [Table & Query Types](https://hudi.apache.org/docs/table_types).
- StarRocks supports the following two query types of Hudi: Snapshot Queries and Read Optimized Queries (Hudi only supports performing Read Optimized Queries on Merge On Read tables). Incremental Queries are not supported. For more information about the query types of Hudi, see [Table & Query Types](https://hudi.apache.org/docs/next/table_types/#query-types).
- You can use the [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) statement to view the schema of a Hudi table in StarRocks 2.4 and later versions.

## Before you begin

Before you create a Hudi catalog, configure your StarRocks cluster so that StarRocks can access the data storage system and metadata service of your Hudi cluster. StarRocks supports two data storage systems for Hudi: HDFS and Amazon S3. StarRocks supports two metadata services for Hudi: Hive metastore and AWS Glue. The configurations required for a Hudi catalog are the same as that required for a Hive catalog. Therefore, see [Hive catalog](../catalog/hive_catalog.md#before-you-begin) for more information about the configurations.

## Create a Hudi catalog

After you complete the preceding configurations, you can create a Hudi catalog. The syntax and parameters are described below.

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
PROPERTIES ("key"="value", ...);
```

### catalog_name

The name of the Hudi catalog. This parameter is required. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name cannot exceed 64 characters in length.

### PROPERTIES

The properties of the Hudi catalog. This parameter is required. You need to configure this parameter based on the metadata service used by your Hudi cluster. You can configure the following properties:

- Configure the properties based on the metadata service used by your Hudi cluster.
- Set the policy that the Hudi catalog uses to update cached metadata.

#### Properties for metadata services

- If you use Hive metastore for your Hudi cluster, configure the following properties for the Hudi catalog.

  | **Property**        | **Required** | **Description**                                              |
  | ------------------- | ------------ | ------------------------------------------------------------ |
  | type                | Yes          | The type of the data source. Set the value to `hudi`.        |
  | hive.metastore.uris | Yes          | The URI of the Hive metastore. Format: `thrift://<IP address of Hive metastore>:<port number>`. The port number defaults to 9083. |

  > **NOTE**
  >
  > Before querying Hudi data, you must add the mapping between the domain name and IP address of Hive metastore node to the **/etc/hosts** path. Otherwise, StarRocks may fail to access Hive metastore when you start a query.

- If you use AWS Glue for your Hudi cluster, configure the following properties for the Hudi catalog.

  | **Property**                           | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | type                                   | Yes          | The type of the data source. Set the value to `hudi`.        |
  | hive.metastore.type                    | Yes          | The metadata service used by your Hudi cluster. Set the value to `glue`. |
  | aws.hive.metastore.glue.aws-access-key | Yes          | The access key ID of the AWS Glue user.                      |
  | aws.hive.metastore.glue.aws-secret-key | Yes          | The secret access key of the AWS Glue user.                  |
  | aws.hive.metastore.glue.endpoint       | Yes          | The regional endpoint of your AWS Glue service. For information about how to obtain your regional endpoint, see [AWS Glue endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html). |

#### Properties for update policies for cached metadata

StarRocks develops a query execution plan based on the following data:

- Metadata (such as table schema) of Hudi tables or partitions in the metadata service.
- Metadata (such as file size) of the data files of Hudi tables or partitions in the storage system.

Therefore, the time consumed by StarRocks to access the metadata service and storage system of Hudi directly affects the time consumed by a query. To reduce the impact, StarRocks supports caching metadata of Hudi and provides an update policy for cached metadata, based on which StarRocks can cache and update the metadata of Hudi tables or partitions and the metadata of their data files.

Hudi catalogs use the asynchronous update policy to update the cached metadata of Hudi. For more information about the policy, see [Asynchronous update](../catalog/hive_catalog.md#asynchronous-update). In most cases, you do not need to tune the following parameters because the default settings already maximize query performance. However, if the frequency of updating Hudi data is relatively high, you can adjust the time interval of updating and discarding cached metadata to match the data update frequency.

| **Property**                           | **Required** | **Description**                                              |
| -------------------------------------- | ------------ | ------------------------------------------------------------ |
| enable_hive_metastore_cache            | No           | Whether the metadata of Hudi tables or partitions is cached. Valid values:<ul><li>`true`: Cache the metadata of Hudi tables or partitions. The value of this parameter defaults to `true`.</li><li>`false`: Do not cache the metadata of Hudi tables or partitions.</li></ul> |
| enable_remote_file_cache               | No           | Whether the metadata of the data files of Hudi tables or partitions is cached. Valid values:<ul><li>`true`: Cache the metadata of data files of Hudi tables or partitions. The value of this parameter defaults to `true`.</li><li>`false`: Do not cache the metadata of data files of Hudi tables or partitions.</li></ul> |
| metastore_cache_refresh_interval_sec   | No           | The time interval to asynchronously update the metadata of Hudi tables or partitions cached in StarRocks. Unit: seconds. Default value: `7200`, which is 2 hours. |
| remote_file_cache_refresh_interval_sec | No           | The time interval to asynchronously update the metadata of the data files of Hudi tables or partitions cached in StarRocks. Unit: seconds. Default value: `60`. |
| metastore_cache_ttl_sec                | No           | The time interval to automatically discard the metadata of Hudi tables or partitions cached in StarRocks. Unit: seconds. Default value: `86400`, which is 24 hours. |
| remote_file_cache_ttl_sec              | No           | The time interval to automatically discard the metadata of the data files of Hudi tables or partitions cached in StarRocks. Unit: seconds. Default value: `129600`, which is 36 hours. |

If you want to query the latest Hudi data but the time interval for updating cached metadata has not arrived, you can manually update the cached metadata. For example, there is a Hudi table named `table1`, which has four partitions: `p1`, `p2`, `p3`, and `p4`. If StarRocks only cached the two kinds of metadata of `p1`, `p2`, and `p3`, you can update the cached metadata in one of the following ways:

- Updates the two kinds of cached metadata of all partitions (`p1`, `p2`, and `p3`) at the same time.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- Updates the two kinds of cached metadata of given partitions.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

For more information about the parameter descriptions and examples of using the REFRESH EXTERNAL TABEL statement, see [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH%20EXTERNAL%20TABLE.md).

## Use catalog to query Hudi data

After you complete all the preceding operations, you can use the Hudi catalog to query Hudi data. For more information, see [Query external data](../catalog/query_external_data.md).

## References

- To view examples of creating an external catalog, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md).
- To view all catalogs in the current StarRocks cluster, see [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md).
- To delete an external catalog, see [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP%20CATALOG.md).
