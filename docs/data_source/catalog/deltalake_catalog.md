# Delta Lake catalog

This topic describes how to create a Delta Lake catalog and how to configure your StarRocks cluster for querying data from Delta Lake.

A Delta Lake catalog is an external catalog supported in StarRocks 2.5 and later versions. It enables you to query data from Delta Lake without loading data into StarRocks or creating external tables.

## Usage notes

- StarRocks supports querying Parquet data files of Delta Lake.
- StarRocks does not support querying data of the BINARY type from Delta Lake. Note that errors occur if you query data in unsupported data types.
- From StarRocks v2.4 onwards, you can use the [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) statement to view the schema of a Delta Lake table. If the table contains columns of unsupported data types, the values in the columns are displayed as `unknown` in the returned result.

## Before you begin

Before you create a Delta Lake catalog, configure your StarRocks cluster so that StarRocks can access the distributed file system (DFS) and metadata service of your Delta Lake. StarRocks supports four DFSs for Delta Lake: HDFS, Amazon S3, Alibaba Cloud Object Storage Service (OSS), and Tencent Cloud Object Storage (COS). StarRocks supports two metadata services for Delta Lake: Hive metastore and AWS Glue. The configurations required for a Delta Lake catalog are the same as that required for a Hive catalog. Therefore, see [Hive catalog](../catalog/hive_catalog.md#before-you-begin) for more information about the configurations.

## Create a Delta Lake catalog

After you complete the preceding configurations, you can create a Delta Lake catalog.

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name> 
PROPERTIES ("key"="value", ...);
```

> **NOTE**
>
> StarRocks does not cache the metadata of Delta Lake. When you query data from Delta Lake, Delta Lake directly returns the latest data by default.

### Parameters

- `catalog_name`: the name of the Delta Lake catalog. This parameter is required. The naming conventions are as follows:
  - The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
  - The name cannot exceed 64 characters in length.
- `PROPERTIES`: the properties of the Delta Lake catalog. This parameter is required. You need to configure this parameter based on the metadata service used by your Delta Lake.

#### Hive metastore

If you use Hive metastore for your Delta Lake, configure the following properties for the Delta Lake catalog.

| **Property**        | **Required** | **Description**                                              |
| ------------------- | ------------ | ------------------------------------------------------------ |
| type                | Yes          | The type of the data source. Set the value to `deltalake`.   |
| hive.metastore.uris | Yes          | The URI of the Hive metastore. The value of this parameter is in the following format: `thrift://<IP address of Hive metastore>:<port number>`. The port number defaults to `9083`. |

> **NOTE**
>
> Before querying data from Delta Lake, you must add the mapping between the domain name and IP address of the Hive metastore node to the **/etc/hosts** path. Otherwise, StarRocks may fail to access Hive metastore when you start a query.

#### AWS Glue

If you use AWS Glue for your Delta Lake, configure the following properties for the Delta Lake catalog.

| **Property**                           | **Required** | **Description**                                              |
| -------------------------------------- | ------------ | ------------------------------------------------------------ |
| type                                   | Yes          | The type of data source. Set the value to `deltalake`.   |
| hive.metastore.type                    | Yes          | The metadata service used by your Delta Lake. Set the value to `glue`. |
| aws.hive.metastore.glue.aws-access-key | Yes          | The access key ID of the IAM user.                           |
| aws.hive.metastore.glue.aws-secret-key | Yes          | The secret access key of the IAM user.                       |
| aws.hive.metastore.glue.endpoint       | Yes          | The regional endpoint of your AWS Glue service. For example, if your service is in the US East (Ohio) region, the endpoint is `glue.us-east-2.amazonaws.com`. For more information about how to obtain your regional endpoint, see [AWS Glue endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html). |

## What to do next

After you complete all the preceding operations, you can use the Delta Lake catalog to query data from Delta Lake. For more information, see [Query external data](../catalog/query_external_data.md).

## References

- To view examples of creating an external catalog, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE%20EXTERNAL%20CATALOG.md).
- To view all catalogs in the current StarRocks cluster, see [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md).
- To delete an external catalog, see [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP%20CATALOG.md).
