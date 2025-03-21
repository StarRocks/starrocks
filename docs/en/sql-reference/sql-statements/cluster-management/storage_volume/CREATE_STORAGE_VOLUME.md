---
displayed_sidebar: docs
---

# CREATE STORAGE VOLUME

## Description

Creates a storage volume for a remote storage system. This feature is supported from v3.1.

A storage volume consists of the properties and credential information of the remote data storage. You can reference a storage volume when you create databases and cloud-native tables in a shared-data StarRocks cluster.

> **CAUTION**
>
> Only users with the CREATE STORAGE VOLUME privilege on the SYSTEM level can perform this operation.

## Syntax

```SQL
CREATE STORAGE VOLUME [IF NOT EXISTS] <storage_volume_name>
TYPE = { S3 | HDFS | AZBLOB }
LOCATIONS = ('<remote_storage_path>')
[ COMMENT '<comment_string>' ]
PROPERTIES
("key" = "value",...)
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | The name of the storage volume. Please note that you cannot create a storage volume named `builtin_storage_volume` because it is used to create the builtin storage volume. For the naming conventions, see [System limits](../../../System_limit.md).|
| TYPE                | The type of the remote storage system. Valid values: `S3`, `HDFS`, `AZBLOB`, and `ADSL2`. `S3` indicates AWS S3 or S3-compatible storage systems. `AZBLOB` indicates Azure Blob Storage (supported from v3.1.1 onwards). `ADSL2` indicates Azure Data Lake Storage Gen2 (supported from v3.4.1 onwards). `HDFS` indicates an HDFS cluster.  |
| LOCATIONS           | The storage locations. The format is as follows:<ul><li>For AWS S3 or S3 protocol-compatible storage systems: `s3://<s3_path>`. `<s3_path>` must be an absolute path, for example, `s3://testbucket/subpath`. Note that if you want to enable the [Partitioned Prefix](#partitioned-prefix) feature for the storage volume, you can only specify the bucket name, and specifying a sub-path is not allowed.</li><li>For Azure Blob Storage: `azblob://<azblob_path>`. `<azblob_path>` must be an absolute path, for example, `azblob://testcontainer/subpath`.</li><li>For Azure Data Lake Storage Gen2: `adls2://<file_system_name>/<dir_name>`. Example: `adls2://testfilesystem/starrocks`.</li><li>For HDFS: `hdfs://<host>:<port>/<hdfs_path>`. `<hdfs_path>` must be an absolute path, for example, `hdfs://127.0.0.1:9000/user/xxx/starrocks`.</li><li>For WebHDFS: `webhdfs://<host>:<http_port>/<hdfs_path>`, where `<http_port>` is the HTTP port of the NameNode. `<hdfs_path>` must be an absolute path, for example, `webhdfs://127.0.0.1:50070/user/xxx/starrocks`.</li><li>For ViewFS：`viewfs://<ViewFS_cluster>/<viewfs_path>`, where `<ViewFS_cluster>` is the ViewFS cluster name. `<viewfs_path>` must be an absolute path, for example, `viewfs://myviewfscluster/user/xxx/starrocks`.</li></ul> |
| COMMENT             | The comment on the storage volume.                           |
| PROPERTIES          | Parameters in the `"key" = "value"` pairs used to specify the properties and credential information to access the remote storage system. For detailed information, see [PROPERTIES](#properties). |

### PROPERTIES

The table below lists all available properties of storage volumes. Following the table are the usage instructions of these properties, categorized by different scenarios from the perspectives of [Credential information](#credential-information) and [Features](#features).

| **Property**                        | **Description**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| enabled                             | Whether to enable this storage volume. Default: `false`. Disabled storage volume cannot be referenced. |
| aws.s3.region                       | The region in which your S3 bucket resides, for example, `us-west-2`. |
| aws.s3.endpoint                     | The endpoint URL used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`. [Preview] From v3.3.0 onwards, the Amazon S3 Express One Zone storage class is supported, for example, `https://s3express.us-west-2.amazonaws.com`.   |
| aws.s3.use_aws_sdk_default_behavior | Whether to use the default authentication credential of AWS SDK. Valid values: `true` and `false` (Default). |
| aws.s3.use_instance_profile         | Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: `true` and `false` (Default).<ul><li>If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws.s3.access_key` and `aws.s3.secret_key`.</li><li>If you use Instance Profile to access S3, you must specify this item as `true`.</li><li>If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws.s3.iam_role_arn`.</li><li>And if you use an external AWS account, you must specify this item as `true`, and specify `aws.s3.iam_role_arn` and `aws.s3.external_id`.</li></ul> |
| aws.s3.access_key                   | The Access Key ID used to access your S3 bucket.             |
| aws.s3.secret_key                   | The Secret Access Key used to access your S3 bucket.         |
| aws.s3.iam_role_arn                 | The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored. |
| aws.s3.external_id                  | The external ID of the AWS account that is used for cross-account access to your S3 bucket. |
| azure.blob.endpoint                 | The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`. |
| azure.blob.shared_key               | The Shared Key used to authorize requests for your Azure Blob Storage. |
| azure.blob.sas_token                | The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage. |
| azure.adls2.endpoint                | The endpoint of your Azure Data Lake Storage Gen2 Account, for example, `https://test.dfs.core.windows.net`. |
| azure.adls2.shared_key              | The Shared Key used to authorize requests for your Azure Data Lake Storage Gen2e. |
| azure.adls2.sas_token               | The shared access signatures (SAS) used to authorize requests for your Azure Data Lake Storage Gen2. |
| hadoop.security.authentication      | The authentication method. Valid values: `simple`(Default) and `kerberos`. `simple` indicates simple authentication, that is, username. `kerberos` indicates Kerberos authentication. |
| username                            | Username used to access the NameNode in the HDFS cluster.                      |
| hadoop.security.kerberos.ticket.cache.path | The path that stores the kinit-generated Ticket Cache.                   |
| dfs.nameservices                    | Name of the HDFS cluster                                        |
| dfs.ha.namenodes.`<ha_cluster_name>`                   | Name of the NameNode. Multiple names must be separated by commas (,). No space is allowed in the double quotes. `<ha_cluster_name>` is the name of the HDFS service specified in `dfs.nameservices`. |
| dfs.namenode.rpc-address.`<ha_cluster_name>`.`<NameNode>` | The RPC address information of the NameNode. `<NameNode>` is the name of the NameNode specified in `dfs.ha.namenodes.<ha_cluster_name>`. |
| dfs.client.failover.proxy.provider                    | The provider of the NameNode for client connection. The default value is `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`. |
| fs.viewfs.mounttable.`<ViewFS_cluster>`.link./`<viewfs_path>` | The path to the ViewFS cluster to be mounted. Multiple paths must be separated by commas (,). `<ViewFS_cluster>` is the ViewFS cluster name specified in `LOCATIONS`. |
| aws.s3.enable_partitioned_prefix    | Whether to enable the Partitioned Prefix feature for the storage volume. Default: `false`. For more information about this feature, see [Partitioned Prefix](#partitioned-prefix). |
| aws.s3.num_partitioned_prefix       | The number of prefixes to be created for the storage volume. Default: `256`. Valid range: [4, 1024].|

#### Credential information

##### AWS S3

- If you use the default authentication credential of AWS SDK to access S3, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "true"
  ```

- If you use IAM user-based credential (Access Key and Secret Key) to access S3, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- If you use Instance Profile to access S3, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true"
  ```

- If you use Assumed Role to access S3, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>"
  ```

- If you use Assumed Role to access S3 from an external AWS account, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>",
  "aws.s3.external_id" = "<external_id>"
  ```

##### GCS

If you use GCP Cloud Storage, set the following properties:

```SQL
"enabled" = "{ true | false }",

-- For example: us-east-1
"aws.s3.region" = "<region>",

-- For example: https://storage.googleapis.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### MinIO

If you use MinIO, set the following properties:

```SQL
"enabled" = "{ true | false }",

-- For example: us-east-1
"aws.s3.region" = "<region>",

-- For example: http://172.26.xx.xxx:39000
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### Azure Blob Storage

Creating a storage volume on Azure Blob Storage is supported from v3.1.1 onwards.

- If you use Shared Key to access Azure Blob Storage, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

- If you use shared access signatures (SAS) to access Azure Blob Storage, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.sas_token" = "<sas_token>"
  ```

:::note
The hierarchical namespace must be disabled when you create the Azure Blob Storage Account.
:::

##### Azure Data Lake Storage Gen2

Creating a storage volume on Azure Data Lake Storage Gen2 is supported from v3.4.1 onwards.

- If you use Shared Key to access Azure Data Lake Storage Gen2, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "azure.adls2.endpoint" = "<endpoint_url>",
  "azure.adls2.shared_key" = "<shared_key>"
  ```

- If you use shared access signatures (SAS) to access Azure Data Lake Storage Gen2, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "azure.adls2.endpoint" = "<endpoint_url>",
  "azure.adls2.sas_token" = "<sas_token>"
  ```

:::note
Azure Data Lake Storage Gen1 is not supported.
:::

##### HDFS

- If you do not use authentication to access HDFS, set the following properties:

  ```SQL
  "enabled" = "{ true | false }"
  ```

- If you are using simple authentication (supported from v3.2) to access HDFS, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "simple",
  "username" = "<hdfs_username>"
  ```

- If you are using Kerberos Ticket Cache authentication (supported since v3.2) to access HDFS, set the following properties:

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "kerberos",
  "hadoop.security.kerberos.ticket.cache.path" = "<ticket_cache_path>"
  ```

  > **CAUTION**
  >
  > - This setting only forces the system to use KeyTab to access HDFS via Kerberos. Make sure that each BE or CN node has access to the KeyTab files. Also make sure that the **/etc/krb5.conf** file is set up correctly.
  > - The Ticket cache is generated by an external kinit tool. Make sure you have a crontab or similar periodic task to refresh the tickets.

- If your HDFS cluster is enabled for NameNode HA configuration (supported since v3.2), additionally set the following properties:

  ```SQL
  "dfs.nameservices" = "<ha_cluster_name>",
  "dfs.ha.namenodes.<ha_cluster_name>" = "<NameNode1>,<NameNode2> [, ...]",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode1>" = "<hdfs_host>:<hdfs_port>",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode2>" = "<hdfs_host>:<hdfs_port>",
  [...]
  "dfs.client.failover.proxy.provider.<ha_cluster_name>" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  ```

  For more information, see [HDFS HA Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html).

  - If you are using WebHDFS (supported since v3.2), set the following properties:

  ```SQL
  "enabled" = "{ true | false }"
  ```

  For more information, see [WebHDFS Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).

- If you are using Hadoop ViewFS (supported since v3.2), set the following properties:

  ```SQL
  -- Replace <ViewFS_cluster> with the name of the ViewFS cluster.
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_1>" = "hdfs://<hdfs_host_1>:<hdfs_port_1>/<hdfs_path_1>",
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_2>" = "hdfs://<hdfs_host_2>:<hdfs_port_2>/<hdfs_path_2>",
  [, ...]
  ```

  For more information, see [ViewFS Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html).

#### Features

##### Partitioned Prefix

From v3.2.4, StarRocks supports creating storage volumes with the Partitioned Prefix feature for S3-compatible object storage systems. When this feature is enabled, StarRocks stores the data into multiple, uniformly prefixed partitions (sub-paths) under the bucket. It can easily multiply StarRocks' read and write performance on data files stored in the bucket because the QPS or throughput limit of the bucket is per partition.

To enable this feature, set the following properties in addition to the above credential-related parameters:

```SQL
"aws.s3.enable_partitioned_prefix" = "{ true | false }",
"aws.s3.num_partitioned_prefix" = "<INT>"
```

:::note
- The Partitioned Prefix feature is only supported for S3-compatible object storage systems, that is, the `TYPE` of the storage volume must be `S3`.
- `LOCATIONS` of the storage volume must only contain the bucket name, for example, `s3://testbucket`. Specifying a sub-path after the bucket name is not allowed.
- Both properties are immutable once the storage volume is created.
- You cannot enable this feature when create a storage volume by using the FE configuration file **fe.conf**.
:::

## Examples

Example 1: Create a storage volume `my_s3_volume` for the AWS S3 bucket `defaultbucket`, use the IAM user-based credential (Access Key and Secret Key) to access S3, and enable it.

```SQL
CREATE STORAGE VOLUME my_s3_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);
```

Example 2: Create a storage volume `my_hdfs_volume` for HDFS and enable it.

```SQL
CREATE STORAGE VOLUME my_hdfs_volume
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES
(
    "enabled" = "true"
);
```

Example 3: Create a storage volume `hdfsvolumehadoop` for HDFS using simple authentication.

```sql
CREATE STORAGE VOLUME hdfsvolumehadoop
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "simple",
    "username" = "starrocks"
);
```

Example 4: Use Kerberos Ticket Cache authentication to access HDFS and create storage volume `hdfsvolkerberos`.

```sql
CREATE STORAGE VOLUME hdfsvolkerberos
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "kerberos",
    "hadoop.security.kerberos.ticket.cache.path" = "/path/to/ticket/cache/path"
);
```

Example 5: Create storage volume `hdfsvolha` for an HDFS cluster with NameNode HA configuration enabled.

```sql
CREATE STORAGE VOLUME hdfsvolha
TYPE = HDFS
LOCATIONS = ("hdfs://myhacluster/data/sr")
PROPERTIES(
    "dfs.nameservices" = "myhacluster",
    "dfs.ha.namenodes.myhacluster" = "nn1,nn2,nn3",
    "dfs.namenode.rpc-address.myhacluster.nn1" = "machine1.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn2" = "machine2.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn3" = "machine3.example.com:8020",
    "dfs.namenode.http-address.myhacluster.nn1" = "machine1.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn2" = "machine2.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn3" = "machine3.example.com:9870",
    "dfs.client.failover.proxy.provider.myhacluster" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

Example 6: Create a storage volume `webhdfsvol` for WebHDFS.

```sql
CREATE STORAGE VOLUME webhdfsvol
TYPE = HDFS
LOCATIONS = ("webhdfs://namenode:9870/data/sr");
```

Example 7: Create a storage volume `viewfsvol` using Hadoop ViewFS.

```sql
CREATE STORAGE VOLUME viewfsvol
TYPE = HDFS
LOCATIONS = ("viewfs://clusterX/data/sr")
PROPERTIES(
    "fs.viewfs.mounttable.clusterX.link./data" = "hdfs://nn1-clusterx.example.com:8020/data",
    "fs.viewfs.mounttable.clusterX.link./project" = "hdfs://nn2-clusterx.example.com:8020/project"
);
```

Example 8: Create a storage volume `adls2` for Azure Data Lake Storage Gen2 using SAS token.

```SQL
CREATE STORAGE VOLUME adls2
    TYPE = ADLS2
    LOCATIONS = ("adls2://testfilesystem/starrocks")
    PROPERTIES (
        "azure.adls2.endpoint" = "https://test.dfs.core.windows.net",
        "azure.adls2.sas_token" = "xxx"
    );
```

## Relevant SQL statements

- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)
