---
displayed_sidebar: "English"
---

# Hive catalog

A Hive catalog is a kind of external catalog that is supported by StarRocks from v2.4 onwards. Within Hive catalogs, you can:

- Directly query data stored in Hive without the need to manually create tables.
- Use [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) or asynchronous materialized views (which are supported from v2.5 onwards) to process data stored in Hive and load the data into StarRocks.
- Perform operations on StarRocks to create or drop Hive databases and tables, or sink data from StarRocks tables to Parquet-formatted Hive tables by using [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/INSERT.md) (this feature is supported from v3.2 onwards).

To ensure successful SQL workloads on your Hive cluster, your StarRocks cluster needs to integrate with two important components:

- Distributed file system (HDFS) or object storage like AWS S3, Microsoft Azure Storage, Google GCS, or other S3-compatible storage system (for example, MinIO)

- Metastore like Hive metastore or AWS Glue

  > **NOTE**
  >
  > If you choose AWS S3 as storage, you can use HMS or AWS Glue as metastore. If you choose any other storage system, you can only use HMS as metastore.

## Usage notes

- The file formats of Hive that StarRocks supports are Parquet, ORC, CSV, Avro, RCFile, and SequenceFile:

  - Parquet files support the following compression formats: SNAPPY, LZ4, ZSTD, GZIP, and NO_COMPRESSION. From v3.1.5 onwards, Parquet files also support the LZO compression format.
  - ORC files support the following compression formats: ZLIB, SNAPPY, LZO, LZ4, ZSTD, and NO_COMPRESSION.
  - CSV files support the LZO compression format from v3.1.5 onwards.

- The data types of Hive that StarRocks does not support are INTERVAL, BINARY, and UNION. Additionally, StarRocks does not support the MAP and STRUCT data types for CSV-formatted Hive tables.
- You can only use Hive catalogs to query data. You cannot use Hive catalogs to drop, delete, or insert data into your Hive cluster.

## Integration preparations

Before you create a Hive catalog, make sure your StarRocks cluster can integrate with the storage system and metastore of your Hive cluster.

### AWS IAM

If your Hive cluster uses AWS S3 as storage or AWS Glue as metastore, choose your suitable authentication method and make the required preparations to ensure that your StarRocks cluster can access the related AWS cloud resources.

The following authentication methods are recommended:

- Instance profile
- Assumed role
- IAM user

Of the above-mentioned three authentication methods, instance profile is the most widely used.

For more information, see [Preparation for authentication in AWS IAM](../../integrations/authenticate_to_aws_resources.md#preparations).

### HDFS

If you choose HDFS as storage, configure your StarRocks cluster as follows:

- (Optional) Set the username that is used to access your HDFS cluster and Hive metastore. By default, StarRocks uses the username of the FE and BE processes to access your HDFS cluster and Hive metastore. You can also set the username by adding `export HADOOP_USER_NAME="<user_name>"` at the beginning of the **fe/conf/hadoop_env.sh** file of each FE and at the beginning of the **be/conf/hadoop_env.sh** file of each BE. After you set the username in these files, restart each FE and each BE to make the parameter settings take effect. You can set only one username for each StarRocks cluster.
- When you query Hive data, the FEs and BEs of your StarRocks cluster use the HDFS client to access your HDFS cluster. In most cases, you do not need to configure your StarRocks cluster to achieve that purpose, and StarRocks starts the HDFS client using the default configurations. You need to configure your StarRocks cluster only in the following situations:

  - High availability (HA) is enabled for your HDFS cluster: Add the **hdfs-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE.
  - View File System (ViewFs) is enabled for your HDFS cluster: Add the **core-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE.

> **NOTE**
>
> If an error indicating an unknown host is returned when you send a query, you must add the mapping between the host names and IP addresses of your HDFS cluster nodes to the **/etc/hosts** path.

### Kerberos authentication

If Kerberos authentication is enabled for your HDFS cluster or Hive metastore, configure your StarRocks cluster as follows:

- Run the `kinit -kt keytab_path principal` command on each FE and each BE to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC). To run this command, you must have the permissions to access your HDFS cluster and Hive metastore. Note that accessing KDC with this command is time-sensitive. Therefore, you need to use cron to run this command periodically.
- Add `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` to the **$FE_HOME/conf/fe.conf** file of each FE and to the **$BE_HOME/conf/be.conf** file of each BE. In this example, `/etc/krb5.conf` is the save path of the **krb5.conf** file. You can modify the path based on your needs.

## Create a Hive catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "hive",
    GeneralParams,
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### Parameters

#### catalog_name

The name of the Hive catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Hive catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `hive`.

#### GeneralParams

A set of general parameters.

The following table describes the parameters you can configure in `GeneralParams`.

| Parameter                | Required | Description                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| enable_recursive_listing | No       | Specifies whether StarRocks reads data from a table and its partitions and from the subdirectories within the physical locations of the table and its partitions. Valid values: `true` and `false`. Default value: `false`. The value `true` specifies to recursively list subdirectories, and the value `false` specifies to ignore subdirectories. |

#### MetastoreParams

A set of parameters about how StarRocks integrates with the metastore of your data source.

##### Hive metastore

If you choose Hive metastore as the metastore of your data source, configure `MetastoreParams` as follows:

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Before querying Hive data, you must add the mapping between the host names and IP addresses of your Hive metastore nodes to the `/etc/hosts` path. Otherwise, StarRocks may fail to access your Hive metastore when you start a query.

The following table describes the parameter you need to configure in `MetastoreParams`.

| Parameter           | Required | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type | Yes      | The type of metastore that you use for your Hive cluster. Set the value to `hive`. |
| hive.metastore.uris | Yes      | The URI of your Hive metastore. Format: `thrift://<metastore_IP_address>:<metastore_port>`.<br />If high availability (HA) is enabled for your Hive metastore, you can specify multiple metastore URIs and separate them with commas (`,`), for example, `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`. |

##### AWS Glue

If you choose AWS Glue as the metastore of your data source, which is supported only when you choose AWS S3 as storage, take one of the following actions:

- To choose the instance profile-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- To choose the assumed role-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- To choose the IAM user-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

The following table describes the parameters you need to configure in `MetastoreParams`.

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| hive.metastore.type           | Yes      | The type of metastore that you use for your Hive cluster. Set the value to `glue`. |
| aws.glue.use_instance_profile | Yes      | Specifies whether to enable the instance profile-based authentication method and the assumed role-based authentication. Valid values: `true` and `false`. Default value: `false`. |
| aws.glue.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS Glue Data Catalog. If you use the assumed role-based authentication method to access AWS Glue, you must specify this parameter. |
| aws.glue.region               | Yes      | The region in which your AWS Glue Data Catalog resides. Example: `us-west-1`. |
| aws.glue.access_key           | No       | The access key of your AWS IAM user. If you use the IAM user-based authentication method to access AWS Glue, you must specify this parameter. |
| aws.glue.secret_key           | No       | The secret key of your AWS IAM user. If you use the IAM user-based authentication method to access AWS Glue, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS Glue and how to configure an access control policy in the AWS IAM Console, see [Authentication parameters for accessing AWS Glue](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue).

#### StorageCredentialParams

A set of parameters about how StarRocks integrates with your storage system. This parameter set is optional.

If you use HDFS as storage, you do not need to configure `StorageCredentialParams`.

If you use AWS S3, other S3-compatible storage system, Microsoft Azure Storage, or Google GCS as storage, you must configure `StorageCredentialParams`.

##### AWS S3

If you choose AWS S3 as storage for your Hive cluster, take one of the following actions:

- To choose the instance profile-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- To choose the assumed role-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- To choose the IAM user-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | Specifies whether to enable the instance profile-based authentication method and the assumed role-based authentication method. Valid values: `true` and `false`. Default value: `false`. |
| aws.s3.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS S3 bucket. If you use the assumed role-based authentication method to access AWS S3, you must specify this parameter.  |
| aws.s3.region               | Yes      | The region in which your AWS S3 bucket resides. Example: `us-west-1`. |
| aws.s3.access_key           | No       | The access key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.secret_key           | No       | The secret key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS S3 and how to configure an access control policy in AWS IAM Console, see [Authentication parameters for accessing AWS S3](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3).

##### S3-compatible storage system

Hive catalogs support S3-compatible storage systems from v2.5 onwards.

If you choose an S3-compatible storage system, such as MinIO, as storage for your Hive cluster, configure `StorageCredentialParams` as follows to ensure a successful integration:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | Specifies whether to enable SSL connection.<br />Valid values: `true` and `false`. Default value: `true`. |
| aws.s3.enable_path_style_access  | Yes      | Specifies whether to enable path-style access.<br />Valid values: `true` and `false`. Default value: `false`. For MinIO, you must set the value to `true`.<br />Path-style URLs use the following format: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`. For example, if you create a bucket named `DOC-EXAMPLE-BUCKET1` in the US West (Oregon) Region, and you want to access the `alice.jpg` object in that bucket, you can use the following path-style URL: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`. |
| aws.s3.endpoint                  | Yes      | The endpoint that is used to connect to your S3-compatible storage system instead of AWS S3. |
| aws.s3.access_key                | Yes      | The access key of your IAM user. |
| aws.s3.secret_key                | Yes      | The secret key of your IAM user. |

##### Microsoft Azure Storage

Hive catalogs support Microsoft Azure Storage from v3.0 onwards.

###### Azure Blob Storage

If you choose Blob Storage as storage for your Hive cluster, take one of the following actions:

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | The username of your Blob Storage account.   |
  | azure.blob.shared_key      | Yes          | The shared key of your Blob Storage account. |

- To choose the SAS Token authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | The username of your Blob Storage account.                   |
  | azure.blob.container      | Yes          | The name of the blob container that stores your data.        |
  | azure.blob.sas_token      | Yes          | The SAS token that is used to access your Blob Storage account. |

###### Azure Data Lake Storage Gen2

If you choose Data Lake Storage Gen2 as storage for your Hive cluster, take one of the following actions:

- To choose the Managed Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | Specifies whether to enable the Managed Identity authentication method. Set the value to `true`. |
  | azure.adls2.oauth2_tenant_id            | Yes          | The ID of the tenant whose data you want to access.          |
  | azure.adls2.oauth2_client_id            | Yes          | The client (application) ID of the managed identity.         |

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | The username of your Data Lake Storage Gen2 storage account. |
  | azure.adls2.shared_key      | Yes          | The shared key of your Data Lake Storage Gen2 storage account. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  The following table describes the parameters you need to configure `in StorageCredentialParams`.

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | The client (application) ID of the service principal.        |
  | azure.adls2.oauth2_client_secret   | Yes          | The value of the new client (application) secret created.    |
  | azure.adls2.oauth2_client_endpoint | Yes          | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

###### Azure Data Lake Storage Gen1

If you choose Data Lake Storage Gen1 as storage for your Hive cluster, take one of the following actions:

- To choose the Managed Service Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | Specifies whether to enable the Managed Service Identity authentication method. Set the value to `true`. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes          | The client (application) ID of the service principal.        |
  | azure.adls1.oauth2_credential | Yes          | The value of the new client (application) secret created.    |
  | azure.adls1.oauth2_endpoint   | Yes          | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

##### Google GCS

Hive catalogs support Google GCS from v3.0 onwards.

If you choose Google GCS as storage for your Hive cluster, take one of the following actions:

- To choose the VM-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |

- To choose the service account-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | The email address in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the service account. |

- To choose the impersonation-based authentication method, configure `StorageCredentialParams` as follows:

  - Make a VM instance impersonate a service account:
  
    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Specifies whether to directly use the service account that is bound to your Compute Engine. |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | The service account that you want to impersonate.            |

  - Make a service account (temporarily named as meta service account) impersonate another service account (temporarily named as data service account):

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | The email address in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | The data service account that you want to impersonate.       |

#### MetadataUpdateParams

A set of parameters about how StarRocks updates the cached metadata of Hive. This parameter set is optional.

StarRocks implements the [automatic asynchronous update policy](#appendix-understand-metadata-automatic-asynchronous-update) by default.

In most cases, you can ignore `MetadataUpdateParams` and do not need to tune the policy parameters in it, because the default values of these parameters already provide you with an out-of-the-box performance.

However, if the frequency of data updates in Hive is high, you can tune these parameters to further optimize the performance of automatic asynchronous updates.

> **NOTE**
>
> In most cases, if your Hive data is updated at a granularity of 1 hour or less, the data update frequency is considered high.

| Parameter                              | Required | Description                                                  |
|----------------------------------------| -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | No       | Specifies whether StarRocks caches the metadata of Hive tables. Valid values: `true` and `false`. Default value: `true`. The value `true` enables the cache, and the value `false` disables the cache. |
| enable_remote_file_cache               | No       | Specifies whether StarRocks caches the metadata of the underlying data files of Hive tables or partitions. Valid values: `true` and `false`. Default value: `true`. The value `true` enables the cache, and the value `false` disables the cache. |
| metastore_cache_refresh_interval_sec   | No       | The time interval at which StarRocks asynchronously updates the metadata of Hive tables or partitions cached in itself. Unit: seconds. Default value: `7200`, which is 2 hours. |
| remote_file_cache_refresh_interval_sec | No       | The time interval at which StarRocks asynchronously updates the metadata of the underlying data files of Hive tables or partitions cached in itself. Unit: seconds. Default value: `60`. |
| metastore_cache_ttl_sec                | No       | The time interval at which StarRocks automatically discards the metadata of Hive tables or partitions cached in itself. Unit: seconds. Default value: `86400`, which is 24 hours. |
| remote_file_cache_ttl_sec              | No       | The time interval at which StarRocks automatically discards the metadata of the underlying data files of Hive tables or partitions cached in itself. Unit: seconds. Default value: `129600`, which is 36 hours. |
| enable_cache_list_names                | No       | Specifies whether StarRocks caches Hive partition names. Valid values: `true` and `false`. Default value: `true`. The value `true` enables the cache, and the value `false` disables the cache. |

### Examples

The following examples create a Hive catalog named `hive_catalog_hms` or `hive_catalog_glue`, depending on the type of metastore you use, to query data from your Hive cluster.

#### HDFS

If you use HDFS as storage, run a command like below:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_hms
PROPERTIES
(
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### Instance profile-based authentication

- If you use Hive metastore in your Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### Assumed role-based authentication

- If you use Hive metastore in your Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### IAM user-based authentication

- If you use Hive metastore in your Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Hive cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

#### S3-compatible storage system

Use MinIO as an example. Run a command like below:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_hms
PROPERTIES
(
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

#### Microsoft Azure Storage

##### Azure Blob Storage

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- If you choose the SAS Token authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- If you choose the Managed Service Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- If you choose the Managed Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- If you choose the VM-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- If you choose the service account-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- If you choose the impersonation-based authentication method:

  - If you make a VM instance impersonate a service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG hive_catalog_hms
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - If you make a service account impersonate another service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG hive_catalog_hms
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## View Hive catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a Hive catalog named `hive_catalog_glue`:

```SQL
SHOW CREATE CATALOG hive_catalog_glue;
```

## Switch to a Hive Catalog and a database in it

You can use one of the following methods to switch to a Hive catalog and a database in it:

- Use [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to specify a Hive catalog in the current session, and then use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to specify an active database:

  ```SQL
  -- Switch to a specified catalog in the current session:
  SET CATALOG <catalog_name>
  -- Specify the active database in the current session:
  USE <db_name>
  ```

- Directly use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to switch to a Hive catalog and a database in it:

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## Drop a Hive catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md) to drop an external catalog.

The following example drops a Hive catalog named `hive_catalog_glue`:

```SQL
DROP Catalog hive_catalog_glue;
```

## View the schema of a Hive table

You can use one of the following syntaxes to view the schema of a Hive table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Query a Hive table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) to view the databases in your Hive cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [Switch to a Hive Catalog and a database in it](#switch-to-a-hive-catalog-and-a-database-in-it).

3. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Load data from Hive

Suppose you have an OLAP table named `olap_tbl`, you can transform and load data like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM hive_table
```

## Grant privileges on Hive tables and views

You can use the [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) statement to grant the privileges on all tables and views within a Hive catalog to a specific role. The command syntax is as follows:

```SQL
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE <role_name>
```

For example, use the following commands to create a role named `hive_role_table`, switch to the Hive catalog `hive_catalog`, and then grant the role `hive_role_table` the privilege to query all tables and views within the Hive catalog `hive_catalog`:

```SQL
-- Create a role named hive_role_table.
CREATE ROLE hive_role_table;

-- Switch to the Hive catalog hive_catalog.
SET CATALOG hive_catalog;

-- Grant the role hive_role_table the privilege to query all tables and views within the Hive catalog hive_catalog.
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE hive_role_table;
```

## Create a Hive database

Similar to the internal catalog of StarRocks, if you have the [CREATE DATABASE](../../administration/privilege_item.md#catalog) privilege on a Hive catalog, you can use the [CREATE DATABASE](../../sql-reference/sql-statements/data-definition/CREATE_DATABASE.md) statement to create a database in that Hive catalog. This feature is supported from v3.2 onwards.

> **NOTE**
>
> You can grant and revoke privileges by using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

[Switch to a Hive catalog](#switch-to-a-hive-catalog-and-a-database-in-it), and then use the following statement to create a Hive database in that catalog:

```SQL
CREATE DATABASE <database_name>
[PROPERTIES ("location" = "<prefix>://<path_to_database>/<database_name.db>")]
```

The `location` parameter specifies the file path in which you want to create the database, which can be in either HDFS or cloud storage.

- When you use Hive metastore as the metastore of your Hive cluster, the `location` parameter defaults to `<warehouse_location>/<database_name.db>`, which is supported by Hive metastore if you do not specify that parameter at database creation.
- When you use AWS Glue as the metastore of your Hive cluster, the `location` parameter does not have a default value, and therefore you must specify that parameter at database creation.

The `prefix` varies based on the storage system you use:

| **Storage system**                                         | **`Prefix`** **value**                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| HDFS                                                       | `hdfs`                                                       |
| Google GCS                                                 | `gs`                                                         |
| Azure Blob Storage                                         | <ul><li>If your storage account allows access over HTTP, the `prefix` is `wasb`.</li><li>If your storage account allows access over HTTPS, the `prefix` is `wasbs`.</li></ul> |
| Azure Data Lake Storage Gen1                               | `adl`                                                        |
| Azure Data Lake Storage Gen2                               | <ul><li>If your storage account allows access over HTTP, the`prefix` is `abfs`.</li><li>If your storage account allows access over HTTPS, the `prefix` is `abfss`.</li></ul> |
| AWS S3 or other S3-compatible storage (for example, MinIO) | `s3`                                                         |

## Drop a Hive database

Similar to the internal databases of StarRocks, if you have the [DROP](../../administration/privilege_item.md#database) privilege on a Hive database, you can use the [DROP DATABASE](../../sql-reference/sql-statements/data-definition/DROP_DATABASE.md) statement to drop that Hive database. This feature is supported from v3.2 onwards. You can only drop empty databases.

> **NOTE**
>
> You can grant and revoke privileges by using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

When you drop a Hive database, the database's file path on your HDFS cluster or cloud storage will not be dropped along with the database.

[Switch to a Hive catalog](#switch-to-a-hive-catalog-and-a-database-in-it), and then use the following statement to drop a Hive database in that catalog:

```SQL
DROP DATABASE <database_name>
```

## Create a Hive table

Similar to the internal databases of StarRocks, if you have the [CREATE TABLE](../../administration/privilege_item.md#database) privilege on a Hive database, you can use the [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) or [CREATE TABLE AS SELECT (CTAS)](../../sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md) statement to create a managed table in that Hive database. This feature is supported from v3.2 onwards.

> **NOTE**
>
> You can grant and revoke privileges by using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

[Switch to a Hive catalog and a database in it](#switch-to-a-hive-catalog-and-a-database-in-it), and then use the following syntax to create a Hive managed table in that database.

### Syntax

```SQL
CREATE TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...
partition_column_definition1,partition_column_definition2...])
[partition_desc]
[PROPERTIES ("key" = "value", ...)]
[AS SELECT query]
```

### Parameters

#### column_definition

The syntax of `column_definition` is as follows:

```SQL
col_name col_type [COMMENT 'comment']
```

The following table describes the parameters.

| Parameter | Description                                                  |
| --------- | ------------------------------------------------------------ |
| col_name  | The name of the column.                                      |
| col_type  | The data type of the column. The following data types are supported: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, DATETIME, CHAR, VARCHAR[(length)], ARRAY, MAP, and STRUCT. The LARGEINT, HLL, and BITMAP data types are not supported. |

> **NOTICE**
>
> All non-partition columns must use `NULL` as the default value. This means that you must specify `DEFAULT "NULL"` for each of the non-partition columns in the table creation statement. Additionally, partition columns must be defined following non-partition columns and cannot use `NULL` as the default value.

#### partition_desc

The syntax of `partition_desc` is as follows:

```SQL
PARTITION BY (par_col1[, par_col2...])
```

Currently StarRocks only supports identity transforms, which means that StarRocks creates a partition for each unique partition value.

> **NOTICE**
>
> Partition columns must be defined following non-partition columns. Partition columns support all data types excluding FLOAT, DOUBLE, DECIMAL, and DATETIME and cannot use `NULL` as the default value. Additionally, the sequence of the partition columns declared in `partition_desc` must be consistent with the sequence of the columns defined in `column_definition`.

#### PROPERTIES

You can specify the table attributes in the `"key" = "value"` format in `properties`.

The following table describes a few key properties.

| **Property**      | **Description**                                              |
| ----------------- | ------------------------------------------------------------ |
| location          | The file path in which you want to create the managed table. When you use HMS as metastore, you do not need to specify the `location` parameter, because StarRocks will create the table in the default file path of the current Hive catalog. When you use AWS Glue as metadata service:<ul><li>If you have specified the `location` parameter for the database in which you want to create the table, you do not need to specify the `location` parameter for the table. As such, the table defaults to the file path of the database to which it belongs. </li><li>If you have not specified the `location` for the database in which you want to create the table, you must specify the `location` parameter for the table.</li></ul> |
| file_format       | The file format of the managed table. Only the Parquet format is supported. Default value: `parquet`. |
| compression_codec | The compression algorithm used for the managed table. The supported compression algorithms are SNAPPY, GZIP, ZSTD, and LZ4. Default value: `gzip`. |

### Examples

1. Create a non-partitioned table named `unpartition_tbl`. The table consists of two columns, `id` and `score`, as shown below:

   ```SQL
   CREATE TABLE unpartition_tbl
   (
       id int,
       score double
   );
   ```

2. Create a partitioned table named `partition_tbl_1`. The table consists of three columns, `action`, `id`, and `dt`, of which `id` and `dt` are defined as partition columns, as shown below:

   ```SQL
   CREATE TABLE partition_tbl_1
   (
       action varchar(20),
       id int,
       dt date
   )
   PARTITION BY (id,dt);
   ```

3. Query an existing table named `partition_tbl_1`, and create a partitioned table named `partition_tbl_2` based on the query result of `partition_tbl_1`. For `partition_tbl_2`, `id` and `dt` are defined as partition columns, as shown below:

   ```SQL
   CREATE TABLE partition_tbl_2
   PARTITION BY (k1, k2)
   AS SELECT * from partition_tbl_1;
   ```

## Sink data to a Hive table

Similar to the internal tables of StarRocks, if you have the [INSERT](../../administration/privilege_item.md#table) privilege on a Hive table (which can be a managed table or an external table), you can use the [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md) statement to sink the data of a StarRocks table to that Hive table (currently only Parquet-formatted Hive tables are supported). This feature is supported from v3.2 onwards. Sinking data to external tables is disabled by default. To sink data to external tables, you must set the [system variable `ENABLE_WRITE_HIVE_EXTERNAL_TABLE`](../../reference/System_variable.md) to `true`.

> **NOTE**
>
> You can grant and revoke privileges by using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

[Switch to a Hive catalog and a database in it](#switch-to-a-hive-catalog-and-a-database-in-it), and then use the following syntax to sink the data of StarRocks table to a Parquet-formatted Hive table in that database.

### Syntax

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- If you want to sink data to specified partitions, use the following syntax:
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

> **NOTICE**
>
> Partition columns do not allow `NULL` values. Therefore, you must make sure that no empty values are loaded into the partition columns of the Hive table.

### Parameters

| Parameter   | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| INTO        | To append the data of the StarRocks table to the Hive table. |
| OVERWRITE   | To overwrite the existing data of the Hive table with the data of the StarRocks table. |
| column_name | The name of the destination column to which you want to load data. You can specify one or more columns. If you specify multiple columns, separate them with commas (`,`). You can only specify columns that actually exist in the Hive table, and the destination columns that you specify must include the partition columns of the Hive table. The destination columns you specify are mapped one on one in sequence to the columns of the StarRocks table, regardless of what the destination column names are. If no destination columns are specified, the data is loaded into all columns of the Hive table. If a non-partition column of the StarRocks table cannot be mapped to any column of the Hive table, StarRocks writes the default value `NULL` to the Hive table column. If the INSERT statement contains a query statement whose returned column types differ from the data types of the destination columns, StarRocks performs an implicit conversion on the mismatched columns. If the conversion fails, a syntax parsing error will be returned. |
| expression  | Expression that assigns values to the destination column.    |
| DEFAULT     | Assigns a default value to the destination column.           |
| query       | Query statement whose result will be loaded into the Hive table. It can be any SQL statement supported by StarRocks. |
| PARTITION   | The partitions into which you want to load data. You must specify all partition columns of the Hive table in this property. The partition columns that you specify in this property can be in a different sequence than the partition columns that you have defined in the table creation statement. If you specify this property, you cannot specify the `column_name` property. |

### Examples

1. Insert three data rows into the `partition_tbl_1` table:

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. Insert the result of a SELECT query, which contains simple computations, into the `partition_tbl_1` table:

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. Insert the result of a SELECT query, which reads data from the `partition_tbl_1` table, into the same table:

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. Insert the result of a SELECT query into the partitions that meet two conditions, `dt='2023-09-01'` and `id=1`, of the `partition_tbl_2` table:

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   Or

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. Overwrite all `action` column values in the partitions that meet two conditions, `dt='2023-09-01'` and `id=1`, of the `partition_tbl_1` table with `close`:

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   Or

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

## Drop a Hive table

Similar to the internal tables of StarRocks, if you have the [DROP](../../administration/privilege_item.md#table) privilege on a Hive table, you can use the [DROP TABLE](../../sql-reference/sql-statements/data-definition/DROP_TABLE.md) statement to drop that Hive table. This feature is supported from v3.1 onwards. Note that currently StarRocks supports dropping only managed tables of Hive.

> **NOTE**
>
> You can grant and revoke privileges by using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

When you drop a Hive table, you must specify the `FORCE` keyword in the DROP TABLE statement. After the operation is complete, the table's file path is retained, but the table's data on your HDFS cluster or cloud storage is all dropped along with the table. Exercise caution when you perform this operation to drop a Hive table.

[Switch to a Hive catalog and a database in it](#switch-to-a-hive-catalog-and-a-database-in-it), and then use the following statement to drop a Hive table in that database.

```SQL
DROP TABLE <table_name> FORCE
```

## Manually or automatically update metadata cache

### Manual update

By default, StarRocks caches the metadata of Hive and automatically updates the metadata in asynchronous mode to deliver better performance. Additionally, after some schema changes or table updates are made on a Hive table, you can also use [REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md) to manually update its metadata, thereby ensuring that StarRocks can obtain up-to-date metadata at its earliest opportunity and generate appropriate execution plans:

```SQL
REFRESH EXTERNAL TABLE <table_name>
```

You need to manually update metadata in the following situations:

- A data file in an existing partition is changed, for example, by running the `INSERT OVERWRITE ... PARTITION ...` command.
- Schema changes are made on a Hive table.
- An existing Hive table is deleted by using the DROP statement, and a new Hive table with the same name as the deleted Hive table is created.
- You have specified `"enable_cache_list_names" = "true"` in `PROPERTIES` at the creation of your Hive catalog, and you want to query new partitions that you just created on your Hive cluster.

  > **NOTE**
  >
  > From v2.5.5 onwards, StarRocks provides the periodic Hive metadata cache refresh feature. For more information, see the below "[Periodically refresh metadata cache](#periodically-refresh-metadata-cache)" section of this topic. After you enable this feature, StarRocks refreshes your Hive metadata cache every 10 minutes by default. Therefore, manual updates are not needed in most cases. You need to perform a manual update only when you want to query new partitions immediately after the new partitions are created on your Hive cluster.

Note that the REFRESH EXTERNAL TABLE refreshes only the tables and partitions cached in your FEs.

### Automatic incremental update

Unlike the automatic asynchronous update policy, the automatic incremental update policy enables the FEs in your StarRocks cluster to read events, such as adding columns, removing partitions, and updating data, from your Hive metastore. StarRocks can automatically update the metadata cached in the FEs based on these events. This means you do not need to manually update the metadata of your Hive tables.

This feature may cause significant pressure to the HMS, exercise caution when you use this feature. We recommend that you use [Periodically refresh metadata cache](#periodically-refresh-metadata-cache).

To enable automatic incremental update, follow these steps:

#### Step 1: Configure event listener for your Hive metastore

Both Hive metastore v2.x and v3.x support configuring an event listener. This step uses the event listener configuration used for Hive metastore v3.1.2 as an example. Add the following configuration items to the **$HiveMetastore/conf/hive-site.xml** file, and then restart your Hive metastore:

```XML
<property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.notifications.add.thrift.objects</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.alter.notifications.basic</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
<property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>172800s</value>
</property>
<property>
    <name>hive.metastore.server.max.message.size</name>
    <value>858993459</value>
</property>
```

You can search for `event id` in the FE log file to check whether the event listener is successfully configured. If the configuration fails, `event id` values are `0`.

#### Step 2: Enable automatic incremental update on StarRocks

You can enable automatic incremental update for a single Hive catalog or for all Hive catalogs in your StarRocks cluster.

- To enable automatic incremental update for a single Hive catalog, set the `enable_hms_events_incremental_sync` parameter to `true` in `PROPERTIES` like below when you create the Hive catalog:

  ```SQL
  CREATE EXTERNAL CATALOG <catalog_name>
  [COMMENT <comment>]
  PROPERTIES
  (
      "type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
       ....
      "enable_hms_events_incremental_sync" = "true"
  );
  ```

- To enable automatic incremental update for all Hive catalogs, add `"enable_hms_events_incremental_sync" = "true"` to the `$FE_HOME/conf/fe.conf` file of each FE, and then restart each FE to make the parameter setting take effect.

You can also tune the following parameters in the `$FE_HOME/conf/fe.conf` file of each FE based on your business requirements, and then restart each FE to make the parameter settings take effect.

| Parameter                         | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| hms_events_polling_interval_ms    | The time interval at which StarRocks reads events from your Hive metastore. Default value: `5000`. Unit: milliseconds. |
| hms_events_batch_size_per_rpc     | The maximum number of events that StarRocks can read at a time. Default value: `500`. |
| enable_hms_parallel_process_evens | Specifies whether StarRocks processes events in parallel as it reads the events. Valid values: `true` and `false`. Default value: `true`. The value `true` enables parallelism, and the value `false` disables parallelism. |
| hms_process_events_parallel_num   | The maximum number of events that StarRocks can process in parallel. Default value: `4`. |

## Periodically refresh metadata cache

From v2.5.5 onwards, StarRocks can periodically refresh the cached metadata of the frequently accessed Hive catalogs to perceive data changes. You can configure the Hive metadata cache refresh through the following [FE parameters](../../administration/FE_configuration.md):

| Configuration item                                           | Default                              | Description                          |
| ------------------------------------------------------------ | ------------------------------------ | ------------------------------------ |
| enable_background_refresh_connector_metadata                 | `true` in v3.0<br />`false` in v2.5  | Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it. This item is an [FE dynamic parameter](../../administration/FE_configuration.md#configure-fe-dynamic-parameters). You can modify it using the [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) command. |
| background_refresh_metadata_interval_millis                  | `600000` (10 minutes)                | The interval between two consecutive Hive metadata cache refreshes. Unit: millisecond. This item is an [FE dynamic parameter](../../administration/FE_configuration.md#configure-fe-dynamic-parameters). You can modify it using the [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) command. |
| background_refresh_metadata_time_secs_since_last_access_secs | `86400` (24 hours)                   | The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata. Unit: second. This item is an [FE dynamic parameter](../../administration/FE_configuration.md#configure-fe-dynamic-parameters). You can modify it using the [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) command. |

Using the periodic Hive metadata cache refresh feature and the metadata automatic asynchronous update policy together significantly accelerates data access, reduces the read load from external data sources, and improves query performance.

## Appendix: Understand metadata automatic asynchronous update

Automatic asynchronous update is the default policy that StarRocks uses to update the metadata in Hive catalogs.

By default (namely, when the `enable_metastore_cache` and `enable_remote_file_cache` parameters are both set to `true`), if a query hits a partition of a Hive table, StarRocks automatically caches the metadata of the partition and the metadata of the underlying data files of the partition. The cached metadata is updated by using the lazy update policy.

For example, there is a Hive table named `table2`, which has four partitions: `p1`, `p2`, `p3`, and `p4`. A query hits `p1`, and StarRocks caches the metadata of `p1` and the metadata of the underlying data files of `p1`. Assume that the default time intervals to update and discard the cached metadata are as follows:

- The time interval (specified by the `metastore_cache_refresh_interval_sec` parameter) to asynchronously update the cached metadata of `p1` is 2 hours.
- The time interval (specified by the `remote_file_cache_refresh_interval_sec` parameter) to asynchronously update the cached metadata of the underlying data files of `p1` is 60 seconds.
- The time interval (specified by the `metastore_cache_ttl_sec` parameter) to automatically discard the cached metadata of `p1` is 24 hours.
- The time interval (specified by the `remote_file_cache_ttl_sec` parameter) to automatically discard the cached metadata of the underlying data files of `p1` is 36 hours.

The following figure shows the time intervals on a timeline for easier understanding.

![Timeline for updating and discarding cached metadata](../../assets/catalog_timeline.png)

Then StarRocks updates or discards the metadata in compliance with the following rules:

- If another query hits `p1` again and the current time from the last update is less than 60 seconds, StarRocks does not update the cached metadata of `p1` or the cached metadata of the underlying data files of `p1`.
- If another query hits `p1` again and the current time from the last update is more than 60 seconds, StarRocks updates the cached metadata of the underlying data files of `p1`.
- If another query hits `p1` again and the current time from the last update is more than 2 hours, StarRocks updates the cached metadata of `p1`.
- If `p1` has not been accessed within 24 hours from the last update, StarRocks discards the cached metadata of `p1`. The metadata will be cached at the next query.
- If `p1` has not been accessed within 36 hours from the last update, StarRocks discards the cached metadata of the underlying data files of `p1`. The metadata will be cached at the next query.
