# Iceberg catalog

An Iceberg catalog is a kind of external catalog that is supported by StarRocks from v2.4 onwards. With Iceberg catalogs, you can:

- Directly query data stored in Iceberg with the need to manually create tables.
- Use [INSERT INTO](../../sql-reference/sql-statements/data-manipulation/insert.md) or asynchronous materialized views (which are supported from v2.5 onwards) to process data stored in Iceberg and load the data into StarRocks.
- Perform operations on StarRocks to create or drop databases and tables from Iceberg, or write data to Parquet-formatted tables by using [INSERT INTO](../../../docs/sql-reference/sql-statements/data-manipulation/insert.md) (this feature is supported from v3.1 onwards).

To ensure successful SQL workloads on your Iceberg cluster, your StarRocks cluster needs to integrate with two important components:

- Object storage or distributed file system like AWS S3, Microsoft Azure Storage, Google GCS, other S3-compatible storage system (for example, MinIO), or HDFS

- Metastore like Hive metastore, AWS Glue, or REST

  > **NOTE**
  >
  > - If you choose AWS S3 as storage, you can use HMS or AWS Glue as metastore. If you choose any other storage system, you can only use HMS as metastore.
  > - If you use Tabular Iceberg Catalog, you must choose REST as metastore.

## Usage notes

- The file formats of Iceberg that StarRocks supports are Parquet and ORC:

  - Parquet files support the following compression formats: SNAPPY, LZ4, ZSTD, GZIP, and NO_COMPRESSION.
  - ORC files support the following compression formats: ZLIB, SNAPPY, LZO, LZ4, ZSTD, and NO_COMPRESSION.

- Iceberg catalogs support v1 tables, and support ORC-formatted v2 tables from StarRocks v3.0 onwards.
- Iceberg catalogs support v1 tables. Additionally, Iceberg catalogs support ORC-formatted v2 tables from StarRocks v3.0 onwards and support Parquet-formatted v2 tables from StarRocks v3.1 onwards.

## Integration preparations

Before you create an Iceberg catalog, make sure your StarRocks cluster can integrate with the storage system and metastore of your Iceberg cluster.

### AWS IAM

If your Iceberg cluster uses AWS S3 as storage or AWS Glue as metastore, choose your suitable authentication method and make the required preparations to ensure that your StarRocks cluster can access the related AWS cloud resources.

The following authentication methods are recommended:

- Instance profile
- Assumed role
- IAM user

Of the above-mentioned three authentication methods, instance profile is the most widely used.

For more information, see [Preparation for authentication in AWS IAM](../../integrations/authenticate_to_aws_resources.md#preparation-for-authentication-in-aws-iam).

### HDFS

If you choose HDFS as storage, configure your StarRocks cluster as follows:

- (Optional) Set the username that is used to access your HDFS cluster and Hive metastore. By default, StarRocks uses the username of the FE and BE processes to access your HDFS cluster and Hive metastore. You can also set the username by adding `export HADOOP_USER_NAME="<user_name>"` at the beginning of the **fe/conf/hadoop_env.sh** file of each FE and at the beginning of the **be/conf/hadoop_env.sh** file of each BE. After you set the username in these files, restart each FE and each BE to make the parameter settings take effect. You can set only one username for each StarRocks cluster.
- When you query Iceberg data, the FEs and BEs of your StarRocks cluster use the HDFS client to access your HDFS cluster. In most cases, you do not need to configure your StarRocks cluster to achieve that purpose, and StarRocks starts the HDFS client using the default configurations. You need to configure your StarRocks cluster only in the following situations:

  - High availability (HA) is enabled for your HDFS cluster: Add the **hdfs-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE.
  - View File System (ViewFs) is enabled for your HDFS cluster: Add the **core-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE.

> **NOTE**
>
> If an error indicating an unknown host is returned when you send a query, you must add the mapping between the host names and IP addresses of your HDFS cluster nodes to the **/etc/hosts** path.

### Kerberos authentication

If Kerberos authentication is enabled for your HDFS cluster or Hive metastore, configure your StarRocks cluster as follows:

- Run the `kinit -kt keytab_path principal` command on each FE and each BE to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC). To run this command, you must have the permissions to access your HDFS cluster and Hive metastore. Note that accessing KDC with this command is time-sensitive. Therefore, you need to use cron to run this command periodically.
- Add `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` to the **$FE_HOME/conf/fe.conf** file of each FE and to the **$BE_HOME/conf/be.conf** file of each BE. In this example, `/etc/krb5.conf` is the save path of the **krb5.conf** file. You can modify the path based on your needs.

## Create an Iceberg catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "iceberg",
    MetastoreParams,
    StorageCredentialParams
)
```

### Parameters

#### catalog_name

The name of the Iceberg catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Iceberg catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `iceberg`.

#### MetastoreParams

A set of parameters about how StarRocks integrates with the metastore of your data source.

##### Hive metastore

If you choose Hive metastore as the metastore of your data source, configure `MetastoreParams` as follows:

```SQL
"iceberg.catalog.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **NOTE**
>
> Before querying Iceberg data, you must add the mapping between the host names and IP addresses of your Hive metastore nodes to the `/etc/hosts` path. Otherwise, StarRocks may fail to access your Hive metastore when you start a query.

The following table describes the parameter you need to configure in `MetastoreParams`.

| Parameter                           | Required | Description                                                  |
| ----------------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type                | Yes      | The type of metastore that you use for your Iceberg cluster. Set the value to `hive`. |
| hive.metastore.uris                 | Yes      | The URI of your Hive metastore. Format: `thrift://<metastore_IP_address>:<metastore_port>`.<br>If high availability (HA) is enabled for your Hive metastore, you can specify multiple metastore URIs and separate them with commas (`,`), for example, `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`. |

##### AWS Glue

If you choose AWS Glue as the metastore of your data source, which is supported only when you choose AWS S3 as storage, take one of the following actions:

- To choose the instance profile-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- To choose the assumed role-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- To choose the IAM user-based authentication method, configure `MetastoreParams` as follows:

  ```SQL
  "iceberg.catalog.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

The following table describes the parameters you need to configure in `MetastoreParams`.

| Parameter                     | Required | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| iceberg.catalog.type          | Yes      | The type of metastore that you use for your Iceberg cluster. Set the value to `glue`. |
| aws.glue.use_instance_profile | Yes      | Specifies whether to enable the instance profile-based authentication method and the assumed role-based authentication method. Valid values: `true` and `false`. Default value: `false`. |
| aws.glue.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS Glue Data Catalog. If you use the assumed role-based authentication method to access AWS Glue, you must specify this parameter. |
| aws.glue.region               | Yes      | The region in which your AWS Glue Data Catalog resides. Example: `us-west-1`. |
| aws.glue.access_key           | No       | The access key of your AWS IAM user. If you use the IAM user-based authentication method to access AWS Glue, you must specify this parameter. |
| aws.glue.secret_key           | No       | The secret key of your AWS IAM user. If you use the IAM user-based authentication method to access AWS Glue, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS Glue and how to configure an access control policy in the AWS IAM Console, see [Authentication parameters for accessing AWS Glue](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue).

##### REST

If you use Tabular Iceberg Catalog, you must choose REST as metastore and configure `MetastoreParams` as follows:

```SQL
"iceberg.catalog.type"="rest",
"iceberg.catalog.uri"="<rest-server-api-endpoint>",
"iceberg.catalog.credential"="<credential>",
"iceberg.catalog.warehouse"="<identifier-or-path-to-warehouse>"
```

The following table describes the parameters you need to configure in `MetastoreParams`.

| Parameter                  | Required | Description                                                         |
| -------------------------- | -------- | ------------------------------------------------------------------- |
| iceberg.catalog.type       | Yes      | The type of metastore that you use for your Iceberg cluster. Set the value to `rest`.           |
| iceberg.catalog.uri        | Yes      | The URI of the REST service endpoint. Example: `https://api.tabular.io/ws`.      |
| iceberg.catalog.credential | Yes      | The authentication information of the Tabular service.                                        |
| iceberg.catalog.warehouse  | No       | The warehouse location or identifier of the Iceberg catalog. Example: `s3://my_bucket/warehouse_location` or `sandbox`. |

The following example creates an Iceberg catalog named `tabular` that uses REST as metastore:

```SQL
CREATE EXTERNAL CATALOG tabular
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://api.tabular.io/ws",
    "iceberg.catalog.credential" = "t-5Ii8e3FIbT9m0",
    "iceberg.catalog.warehouse" = "sandbox"
);
```

#### `StorageCredentialParams`

A set of parameters about how StarRocks integrates with your storage system. This parameter set is optional.

If you use HDFS as storage, you do not need to configure `StorageCredentialParams`.

If you use AWS S3, other S3-compatible storage system, Microsoft Azure Storage, or Google GCS as storage, you must configure `StorageCredentialParams`.

##### AWS S3

If you choose AWS S3 as storage for your Iceberg cluster, take one of the following actions:

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
| aws.s3.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS S3 bucket. If you use the assumed role-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.region               | Yes      | The region in which your AWS S3 bucket resides. Example: `us-west-1`. |
| aws.s3.access_key           | No       | The access key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.secret_key           | No       | The secret key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS S3 and how to configure an access control policy in AWS IAM Console, see [Authentication parameters for accessing AWS S3](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3).

##### S3-compatible storage system

Iceberg catalogs support S3-compatible storage systems from v2.5 onwards.

If you choose an S3-compatible storage system, such as MinIO, as storage for your Iceberg cluster, configure `StorageCredentialParams` as follows to ensure a successful integration:

```SQL
"aws.s3.enable_ssl" = "{true | false}",
"aws.s3.enable_path_style_access" = "{true | false}",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | Specifies whether to enable SSL connection.<br>Valid values: `true` and `false`. Default value: `true`. |
| aws.s3.enable_path_style_access  | Yes      | Specifies whether to enable path-style access.<br>Valid values: `true` and `false`. Default value: `false`.<br>Path-style URLs use the following format: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`. For example, if you create a bucket named `DOC-EXAMPLE-BUCKET1` in the US West (Oregon) Region, and you want to access the `alice.jpg` object in that bucket, you can use the following path-style URL: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`. |
| aws.s3.endpoint                  | Yes      | The endpoint that is used to connect to your S3-compatible storage system instead of AWS S3. |
| aws.s3.access_key                | Yes      | The access key of your IAM user. |
| aws.s3.secret_key                | Yes      | The secret key of your IAM user. |

##### Microsoft Azure Storage

Iceberg catalogs support Microsoft Azure Storage from v3.0 onwards.

###### Azure Blob Storage

If you choose Blob Storage as storage for your Iceberg cluster, take one of the following actions:

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.storage_account" = "<blob_storage_account_name>",
  "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | The username of your Blob Storage account.   |
  | azure.blob.shared_key      | Yes          | The shared key of your Blob Storage account. |

- To choose the SAS Token authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.account_name" = "<blob_storage_account_name>",
  "azure.blob.container_name" = "<blob_container_name>",
  "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.account_name   | Yes          | The username of your Blob Storage account.                   |
  | azure.blob.container_name | Yes          | The name of the blob container that stores your data.        |
  | azure.blob.sas_token      | Yes          | The SAS token that is used to access your Blob Storage account. |

###### Azure Data Lake Storage Gen1

If you choose Data Lake Storage Gen1 as storage for your Iceberg cluster, take one of the following actions:

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

###### Azure Data Lake Storage Gen2

If you choose Data Lake Storage Gen2 as storage for your Iceberg cluster, take one of the following actions:

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
  "azure.adls2.shared_key" = "<shared_key>"
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

##### Google GCS

Iceberg catalogs support Google GCS from v3.0 onwards.

If you choose Google GCS as storage for your Iceberg cluster, take one of the following actions:

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

### Examples

The following examples create an Iceberg catalog named `iceberg_catalog_hms` or `iceberg_catalog_glue`, depending on the type of metastore you use, to query data from your Iceberg cluster.

#### AWS S3

##### If you choose instance profile-based credential

- If you use Hive metastore in your Iceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Iceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### If you choose assumed role-based credential

- If you use Hive metastore in your HIceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Iceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### If you choose IAM user-based credential

- If you use Hive metastore in your Iceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- If you use AWS Glue in your Amazon EMR Iceberg cluster, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_glue
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "glue",
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
CREATE EXTERNAL CATALOG iceberg_catalog_hms
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://34.132.15.127:9083",
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
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- If you choose the SAS Token authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.blob.account_name" = "<blob_storage_account_name>",
      "azure.blob.container_name" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- If you choose the Managed Service Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- If you choose the Managed Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- If you choose the VM-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- If you choose the service account-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG iceberg_catalog_hms
  PROPERTIES
  (
      "type" = "iceberg",
      "iceberg.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://34.132.15.127:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- If you choose the impersonation-based authentication method:

  - If you make a VM instance impersonate a service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://34.132.15.127:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - If you make a service account impersonate another service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG iceberg_catalog_hms
    PROPERTIES
    (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://34.132.15.127:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

## View Iceberg catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of an Iceberg catalog named `iceberg_catalog_glue`:

```SQL
SHOW CREATE CATALOG iceberg_catalog_glue;
```

## Drop an Iceberg catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP%20CATALOG.md) to drop an external catalog.

The following example drops an Iceberg catalog named `iceberg_catalog_glue`:

```SQL
DROP Catalog iceberg_catalog_glue;
```

## View the schema of an Iceberg table

You can use one of the following syntaxes to view the schema of an Iceberg table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## Query an Iceberg table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md) to view the databases in your Iceberg cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET%20CATALOG.md) to switch to the destination catalog in the current session:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    Then, use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to specify the active database in the current session:

    ```SQL
    USE <db_name>;
    ```

    Or, you can use [USE](../../sql-reference/sql-statements/data-definition/USE.md) to directly specify the active database in the destination catalog:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. Use [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## Load data from Iceberg

Suppose you have an OLAP table named `olap_tbl`, you can transform and load data like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM iceberg_table
```

## Configure metadata caching

The metadata files of your Iceberg cluster may be stored in remote storage such as AWS S3 or HDFS. By default, StarRocks caches Iceberg metadata in memory. To accelerate queries, StarRocks adopts a two-level metadata caching mechanism, with which it can cache metadata both in memory and on disk. For each initial query, StarRocks caches their computation results. If any subsequent query that is semantically equivalent to a previous query is issued, StarRocks first attempts to retrieve the requested metadata from its caches, and it retrieves the metadata from the remote storage only when the metadata cannot be hit in its caches.

StarRocks uses the Least Recently Used (LRU) algorithm to cache and evict data. The basic rules are as follows:

- StarRocks first attempts to retrieve the requested metadata from the memory. If the metadata cannot be hit in the memory, StarRock attempts to retrieve the metadata from the disks. The metadata that StarRocks has retrieved from the disks will be loaded into the memory. If the metadata cannot be hit in the disks either, StarRock retrieves the metadata from the remote storage and caches the retrieved metadata in the memory.
- StarRocks writes the metadata evicted out of the memory into the disks, but it directly discards the metadata evicted out of the disks.

The following table describes the FE configuration items that you can use to configure your Iceberg metadata caching mechanism.

| **Configuration item**                           | **Unit** | **Default value**                                    | **Description**                                              |
| ------------------------------------------------ | -------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| enable_iceberg_metadata_disk_cache               | N/A      | `false`                                              | Specifies whether to enable the disk cache.                  |
| iceberg_metadata_cache_disk_path                 | N/A      | `StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"` | The save path of cached metadata files on disk.              |
| iceberg_metadata_disk_cache_capacity             | Bytes    | `2147483648`, equivalent to 2 GB                     | The maximum size of cached metadata allowed on disk.         |
| iceberg_metadata_memory_cache_capacity           | Bytes    | `536870912`, equivalent to 512 MB                    | The maximum size of cached metadata allowed in memory.       |
| iceberg_metadata_memory_cache_expiration_seconds | Seconds  | `86500`                                              | The amount of time after which a cache entry in memory expires counting from its last access. |
| iceberg_metadata_disk_cache_expiration_seconds   | Seconds  | `604800`, equivalent to one week                     | The amount of time after which a cache entry on disk expires counting from its last access. |
| iceberg_metadata_cache_max_entry_size            | Bytes    | `8388608`, equivalent to 8 MB                        | The maximum size of a file that can be cached. Files whose size exceeds the value of this parameter cannot be cached. If a query requests these files, StarRocks retrieves them from the remote storage. |
