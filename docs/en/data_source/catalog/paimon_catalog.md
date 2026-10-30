---
sidebar_position: 100
displayed_sidebar: docs
toc_max_heading_level: 5
description: "StarRocks supports Paimon catalogs from v3.1 onwards."
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Paimon catalog

<Beta />

StarRocks supports Paimon catalogs from v3.1 onwards.

A Paimon catalog is a kind of external catalog that enables you to query data from Apache Paimon without ingestion.

Also, you can directly transform and load data from Paimon by using [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) based on Paimon catalogs.

To ensure successful SQL workloads on your Paimon cluster, your StarRocks cluster must be able to access the storage system and metastore of your Paimon cluster. StarRocks supports the following storage systems and metastores:

- Distributed file system (HDFS) or object storage like AWS S3, Microsoft Azure Storage, Google GCS, or or other S3-compatible storage system (for example, MinIO)
- Metastore like your file system or Hive metastore

## Usage notes

In addition to queries, the current Paimon integration implements `CREATE/DROP DATABASE`, `CREATE/DROP TABLE`, batch `INSERT INTO`, `INSERT OVERWRITE`, `CREATE TABLE AS SELECT` (CTAS), and same-catalog `CREATE TABLE LIKE`. Row-level `UPDATE` and `DELETE` are not supported. Dropping a database requires it to be empty.

:::caution
DDL and batch writing are still under integration. The create/write sections below describe the current implementation, not runtime-verified release availability. The examples and end-to-end read-back behavior still require runtime validation.
:::

## Paimon to StarRocks data types

| Paimon Type           | StarRocks Type              |
|-----------------------|-----------------------------|
| `BINARY`              | `VARBINARY`                 |
| `VARBINARY`           | `VARBINARY`                 |
| `CHAR`                | `CHAR(length)`              |
| `VARCHAR`             | `VARCHAR`                   |
| `BOOLEAN`             | `BOOLEAN`                   |
| `DECIMAL`             | `DECIMAL(precision, scale)` |
| `TINYINT`             | `TINYINT`                   |
| `SMALLINT`            | `SMALLINT`                  |
| `INT`                 | `INT`                       |
| `BIGINT`              | `BIGINT`                    |
| `FLOAT`               | `FLOAT`                     |
| `DOUBLE`              | `DOUBLE`                    |
| `DATE`                | `DATE`                      |
| `TIME`                | `TIME`                      |
| `TIMESTAMP`           | `DATETIME`                  |
| `LocalZonedTimestamp` | `DATETIME`                  |
| `ARRAY`               | `ARRAY<element_type>`       |
| `MAP`                 | `MAP<key_type, value_type>` |
| `ROW/STRUCT`          | `STRUCT<field1:type1, ...>` |
| `VARIANT`             | `VARIANT`                   |

:::note
`VARIANT` is supported from v4.2 onwards for splits read by the native reader (append-only tables and compacted primary-key data). Reading `VARIANT` from uncompacted primary-key data requires the JNI reader and is not yet supported; such queries are rejected with a plan-time error.
:::

## Integration preparations

Before you create a Paimon catalog, make sure your StarRocks cluster can integrate with the storage system and metastore of your Paimon cluster.

### AWS IAM

If your Paimon cluster uses AWS S3 as storage, choose your suitable authentication method and make the required preparations to ensure that your StarRocks cluster can access the related AWS cloud resources.

The following authentication methods are recommended:

- Instance profile (recommended)
- Assumed role
- IAM user

Of the above-mentioned three authentication methods, instance profile is the most widely used.

For more information, see [Preparation for authentication in AWS IAM](../../integrations/csp_auth/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication).

### HDFS

If you choose HDFS as storage, configure your StarRocks cluster as follows:

- (Optional) Set the username that is used to access your HDFS cluster and Hive metastore. By default, StarRocks uses the username of the FE and BE or CN processes to access your HDFS cluster and Hive metastore. You can also set the username by adding `export HADOOP_USER_NAME="<user_name>"` at the beginning of the **fe/conf/hadoop_env.sh** file of each FE and at the beginning of the **be/conf/hadoop_env.sh** file of each BE or the **cn/conf/hadoop_env.sh** file of each CN. After you set the username in these files, restart each FE and each BE or CN to make the parameter settings take effect. You can set only one username for each StarRocks cluster.
- When you query Paimon data, the FEs and BEs or CNs of your StarRocks cluster use the HDFS client to access your HDFS cluster. In most cases, you do not need to configure your StarRocks cluster to achieve that purpose, and StarRocks starts the HDFS client using the default configurations. You need to configure your StarRocks cluster only in the following situations:
  - High availability (HA) is enabled for your HDFS cluster: Add the **hdfs-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE or the **$CN_HOME/conf** path of each CN.
  - View File System (ViewFs) is enabled for your HDFS cluster: Add the **core-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and to the **$BE_HOME/conf** path of each BE or the **$CN_HOME/conf** path of each CN.

> **NOTE**
>
> If an error indicating an unknown host is returned when you send a query, you must add the mapping between the host names and IP addresses of your HDFS cluster nodes to the **/etc/hosts** path.

### Kerberos authentication

If Kerberos authentication is enabled for your HDFS cluster or Hive metastore, configure your StarRocks cluster as follows:

- Run the `kinit -kt keytab_path principal` command on each FE and each BE or CN to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC). To run this command, you must have the permissions to access your HDFS cluster and Hive metastore. Note that accessing KDC with this command is time-sensitive. Therefore, you need to use cron to run this command periodically.
- Add `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` to the **$FE_HOME/conf/fe.conf** file of each FE and to the **$BE_HOME/conf/be.conf** file of each BE or the **$CN_HOME/conf/cn.conf** file of each CN. In this example, `/etc/krb5.conf` is the save path of the **krb5.conf** file. You can modify the path based on your needs.

## Create a Paimon catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "paimon",
    CatalogParams,
    StorageCredentialParams,
)
```

### Parameters

#### catalog_name

The name of the Paimon catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Paimon catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `paimon`.

#### CatalogParams

A set of parameters about how StarRocks accesses the metadata of your Paimon cluster.

The following table describes the parameter you need to configure in `CatalogParams`.

| Parameter                | Required | Description                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| paimon.catalog.type      | Yes      | The type of metastore that you use for your Paimon cluster. Set this parameter to `filesystem` or `hive`. |
| paimon.catalog.warehouse | Yes      | The warehouse storage path of your Paimon data. |
| hive.metastore.uris      | No       | The URI of your Hive metastore. Format: `thrift://<metastore_IP_address>:<metastore_port>`. If high availability (HA) is enabled for your Hive metastore, you can specify multiple metastore URIs and separate them with commas (`,`), for example, `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`. |

> **NOTE**
>
> If you use Hive metastore, you must add the mapping between the host names and IP addresses of your Hive metastore nodes to the `/etc/hosts` path before you query Paimon data. Otherwise, StarRocks may fail to access your Hive metastore when you start a query.

#### StorageCredentialParams

A set of parameters about how StarRocks integrates with your storage system. This parameter set is optional.

If you use HDFS as storage, you do not need to configure `StorageCredentialParams`.

If you use AWS S3, other S3-compatible storage system, Microsoft Azure Storage, or Google GCS as storage, you must configure `StorageCredentialParams`.

##### AWS S3

If you choose AWS S3 as storage for your Paimon cluster, take one of the following actions:

- To choose the instance profile-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- To choose the assumed role-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- To choose the IAM user-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | Specifies whether to enable the instance profile-based authentication method and the assumed role-based authentication method. Valid values: `true` and `false`. Default value: `false`. |
| aws.s3.iam_role_arn         | No       | The ARN of the IAM role that has privileges on your AWS S3 bucket. If you use the assumed role-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.endpoint             | Yes      | The endpoint that is used to connect to your AWS S3 bucket. For example, `https://s3.us-west-2.amazonaws.com`. |
| aws.s3.access_key           | No       | The access key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.secret_key           | No       | The secret key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |

For information about how to choose an authentication method for accessing AWS S3 and how to configure an access control policy in AWS IAM Console, see [Authentication parameters for accessing AWS S3](../../integrations/csp_auth/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3).

##### S3-compatible storage system

If you choose an S3-compatible storage system, such as MinIO, as storage for your Paimon cluster, configure `StorageCredentialParams` as follows to ensure a successful integration:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                       | Required | Description                                                  |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl               | Yes      | Specifies whether to enable SSL connection. <br />Valid values: `true` and `false`. Default value: `true`. |
| aws.s3.enable_path_style_access | Yes      | Specifies whether to enable path-style access.<br />Valid values: `true` and `false`. Default value: `false`. For MinIO, you must set the value to `true`.<br /> Path-style URLs use the following format: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`. For example, if you create a bucket named `DOC-EXAMPLE-BUCKET1` in the US West (Oregon) Region, and you want to access the `alice.jpg` object in that bucket, you can use the following path-style URL: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`. |
| aws.s3.endpoint                 | Yes      | The endpoint that is used to connect to your S3-compatible storage system instead of AWS S3. |
| aws.s3.access_key               | Yes      | The access key of your IAM user.                             |
| aws.s3.secret_key               | Yes      | The secret key of your IAM user.                             |

##### Microsoft Azure Storage

###### Azure Blob Storage

If you choose Blob Storage as storage for your Paimon cluster, take one of the following actions:

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                  | Required | Description                                  |
  | -------------------------- | -------- | -------------------------------------------- |
  | azure.blob.storage_account | Yes      | The username of your Blob Storage account.   |
  | azure.blob.shared_key      | Yes      | The shared key of your Blob Storage account. |

- To choose the SAS Token authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                 | Required | Description                                                  |
  | ------------------------- | -------- | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes      | The username of your Blob Storage account.                   |
  | azure.blob.container      | Yes      | The name of the blob container that stores your data.        |
  | azure.blob.sas_token      | Yes      | The SAS token that is used to access your Blob Storage account. |

###### Azure Data Lake Storage Gen2

If you choose Data Lake Storage Gen2 as storage for your Paimon cluster, take one of the following actions:

- To choose the Managed Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                               | Required | Description                                                  |
  | --------------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes      | Specifies whether to enable the Managed Identity authentication method. Set the value to `true`. |
  | azure.adls2.oauth2_tenant_id            | Yes      | The ID of the tenant whose data you want to access.          |
  | azure.adls2.oauth2_client_id            | Yes      | The client (application) ID of the managed identity.         |

- To choose the Shared Key authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                   | Required | Description                                                  |
  | --------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes      | The username of your Data Lake Storage Gen2 storage account. |
  | azure.adls2.shared_key      | Yes      | The shared key of your Data Lake Storage Gen2 storage account. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  The following table describes the parameters you need to configure `in StorageCredentialParams`.

  | Parameter                          | Required | Description                                                  |
  | ---------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes      | The client (application) ID of the service principal.        |
  | azure.adls2.oauth2_client_secret   | Yes      | The value of the new client (application) secret created.    |
  | azure.adls2.oauth2_client_endpoint | Yes      | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

- To choose the Workload Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls2.oauth2_token_file" = "<path_to_token>",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_token_file           | Yes          | The absolute file path to the OAuth2 token file projected into the pod by the Azure Workload Identity webhook. |
  | azure.adls2.oauth2_tenant_id            | Yes          | The ID of the tenant whose data you want to access.          |
  | azure.adls2.oauth2_client_id            | Yes          | The client ID (application ID) of the Azure AD application (user-assigned managed identity or app registration) associated with the workload identity. |

###### Azure Data Lake Storage Gen1

If you choose Data Lake Storage Gen1 as storage for your Paimon cluster, take one of the following actions:

- To choose the Managed Service Identity authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                                | Required | Description                                                  |
  | ---------------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes      | Specifies whether to enable the Managed Service Identity authentication method. Set the value to `true`. |

- To choose the Service Principal authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                     | Required | Description                                                  |
  | ----------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | Yes      | The client (application) ID of the service principal.        |
  | azure.adls1.oauth2_credential | Yes      | The value of the new client (application) secret created.    |
  | azure.adls1.oauth2_endpoint   | Yes      | The OAuth 2.0 token endpoint (v1) of the service principal or application. |

##### Google GCS

If you choose Google GCS as storage for your Paimon cluster, take one of the following actions:

- To choose the VM-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                                  | Default value | Value example | Description                                                  |
  | ------------------------------------------ | ------------- | ------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | FALSE         | TRUE          | Specifies whether to directly use the service account that is bound to your Compute Engine. |

- To choose the service account-based authentication method, configure `StorageCredentialParams` as follows:

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  The following table describes the parameters you need to configure in `StorageCredentialParams`.

  | Parameter                              | Default value | Value example                                                | Description                                                  |
  | -------------------------------------- | ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""            | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | The email address in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key_id | ""            | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the service account. |
  | gcp.gcs.service_account_private_key    | ""            | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the service account. |

- To choose the impersonation-based authentication method, configure `StorageCredentialParams` as follows:

  - Make a VM instance impersonate a service account:

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | Parameter                                  | Default value | Value example | Description                                                  |
    | ------------------------------------------ | ------------- | ------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | FALSE         | TRUE          | Specifies whether to directly use the service account that is bound to your Compute Engine. |
    | gcp.gcs.impersonation_service_account      | ""            | "hello"       | The service account that you want to impersonate.            |

  - Make a service account (temporarily named as meta service account) impersonate another service account (temporarily named as data service account):

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    The following table describes the parameters you need to configure in `StorageCredentialParams`.

    | Parameter                              | Default value | Value example                                                | Description                                                  |
    | -------------------------------------- | ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""            | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | The email address in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key_id | ""            | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | The private key ID in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.service_account_private_key    | ""            | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | The private key in the JSON file generated at the creation of the meta service account. |
    | gcp.gcs.impersonation_service_account  | ""            | "hello"                                                      | The data service account that you want to impersonate.       |

#### MetadataCacheParams (optional)

StarRocks caches Paimon metadata and refreshes it in the background. These optional properties tune that cache.

| Parameter                              | Default value | Description                                                  |
| -------------------------------------- | ------------- | ------------------------------------------------------------ |
| paimon_meta_cache_ttl_sec              | 86400         | How long a cached entry lives, in seconds. |
| paimon_table_cache_refresh_interval_sec | 60            | The minimum interval, in seconds, between two refreshes of the same table triggered by a query that read a newer snapshot. The refresh runs in the background and the query does not wait for it. Set to `0` to only refresh in the periodic background round. Supported from v4.2.0 onwards. |

### Examples

The following examples create a Paimon catalog named `paimon_catalog_fs` whose metastore type `paimon.catalog.type` is set to `filesystem` to query data from your Paimon cluster.

#### AWS S3

- If you choose the instance profile-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- If you choose the assumed role-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- If you choose the IAM user-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

#### S3-compatible storage system

Use MinIO as an example. Run a command like below:

```SQL
CREATE EXTERNAL CATALOG paimon_catalog_fs
PROPERTIES
(
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "paimon.catalog.warehouse" = "<paimon_warehouse_path>",
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
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- If you choose the SAS Token authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- If you choose the Managed Service Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.use_managed_service_identity" = "true"
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- If you choose the Managed Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

- If you choose the Workload Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

#### Google GCS

- If you choose the VM-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.use_compute_engine_service_account" = "true"
  );
  ```

- If you choose the service account-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  );
  ```

- If you choose the impersonation-based authentication method:

  - If you make a VM instance impersonate a service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    );
    ```

  - If you make a service account impersonate another service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

## View Paimon catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a Paimon catalog named `paimon_catalog_fs`:

```SQL
SHOW CREATE CATALOG paimon_catalog_fs;
```

## Drop a Paimon catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop an external catalog.

The following example drops a Paimon catalog named `paimon_catalog_fs`:

```SQL
DROP Catalog paimon_catalog_fs;
```

## View the schema of a Paimon table

You can use one of the following syntaxes to view the schema of a Paimon table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Query a Paimon table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in your Paimon cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the destination catalog in the current session:

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database in the current session:

   ```SQL
   USE <db_name>;
   ```

   Or, you can use [USE](../../sql-reference/sql-statements/Database/USE.md) to directly specify the active database in the destination catalog:

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Load data from Paimon

Suppose you have an OLAP table named `olap_tbl`, you can transform and load data like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM paimon_table;
```

## Create and write Paimon tables

The following sections describe the DDL and batch writer currently under integration. Reuse an existing Paimon catalog and its database, or create a database as shown below. Warehouse, metastore, and storage credentials remain in the catalog's `PROPERTIES`; do not put them in table properties. `CREATE DATABASE` does not accept catalog or location overrides.

### Create a database and tables

If `write_db` already exists, skip `CREATE DATABASE` and use that database. The catalog determines the table location. `ENGINE` is inferred as `paimon` from the target catalog.

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS write_db;
USE write_db;

CREATE TABLE orders (
    id BIGINT NOT NULL,
    dt DATE NOT NULL,
    amount DECIMAL(12, 2)
)
PRIMARY KEY (id, dt)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "4",
    "file.format" = "parquet",
    "merge-engine" = "deduplicate"
);

CREATE TABLE event_log (
    id BIGINT,
    dt DATE,
    event_time DATETIME,
    tags ARRAY<STRING>,
    attributes MAP<STRING, STRING>,
    details STRUCT<source STRING, attempt INT>,
    payload VARIANT
)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "-1",
    "file.format" = "parquet"
);
```

`PRIMARY KEY` defines native Paimon primary keys, not an OLAP table model; primary-key columns are non-nullable. Set `bucket > 0` explicitly for a writable primary-key table. A table without primary keys is an append table. Use `PARTITION BY (column, ...)` with ordinary columns, without partition definitions or expressions. Configure native bucketing through `bucket` and, where required, `bucket-key` in `PROPERTIES`, not `DISTRIBUTED BY`. OLAP `ORDER BY`, `ROLLUP`, aggregate columns, generated columns, and `AUTO_INCREMENT` are not supported.

### Write types and defaults

- New tables support representable scalar types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`, `VARCHAR`/`STRING`, `VARBINARY`, `DATE`, and `DATETIME`. `ARRAY`, `MAP`, `STRUCT`, and `VARIANT` are also supported, including supported nested types.
- New `DATETIME` columns become Paimon `TIMESTAMP(6)` without time zone. Writes to existing tables use the native timestamp kind, but the public C++ 0.3 writer supports only precisions `0`, `3`, `6`, and `9`; precisions `1`, `2`, `4`, `5`, `7`, and `8` are rejected during capability validation. For time-zone-bearing targets, the source `DATETIME` is interpreted in the session time zone. If a target precision below `6` cannot exactly represent the source microseconds, the writer rejects the value rather than silently truncating it. This does not add source precision beyond microseconds or change reader-side timestamp mappings and limitations.
- Newly created nested children are nullable. SQL `CREATE TABLE` cannot declare nested `NOT NULL`. Existing native nullability constraints are enforced when writing; `CREATE TABLE LIKE` preserves them.
- Plain `LARGEINT` columns and other unsupported or unrepresentable types are explicitly rejected rather than silently narrowed. `VARIANT` support does not establish full signed 128-bit integer interoperability with public Paimon readers; that interoperability is not yet verified.
- Omit `DEFAULT`, or use `DEFAULT NULL` only for nullable columns. Non-NULL literal or expression defaults are unsupported. When inserting into an existing table with native non-NULL defaults, supply explicit values instead of relying on omitted columns or `DEFAULT`. Required columns and primary-key columns must be supplied.

### Batch INSERT and CTAS

```sql
INSERT INTO paimon_catalog_fs.write_db.orders
VALUES (1, '2026-01-01', 12.34);

CREATE TABLE paimon_catalog_fs.write_db.orders_copy
PROPERTIES ("bucket" = "-1", "file.format" = "parquet")
AS SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders;
```

[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) accepts values or query results. For primary-key targets, it uses the native `deduplicate` merge engine, not StarRocks OLAP partial-update semantics. [CTAS](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) creates a new native table and then writes the query result; it does not clone source keys or table options. The example creates an append table. To create a primary-key target, declare `PRIMARY KEY` and set `bucket > 0` explicitly.

CTAS is not atomic across table creation and data publication. If writing fails, the current implementation conservatively retains the created table because the public catalog cannot perform an atomic, identity-conditional drop. Inspect and reconcile the target before manual cleanup or retry. A table whose publication outcome is unknown must not be automatically dropped.

### CREATE TABLE LIKE

Source and target must be in the same Paimon catalog, but may be in different databases:

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS copy_db;
CREATE TABLE paimon_catalog_fs.copy_db.orders_like
LIKE paimon_catalog_fs.write_db.orders;
```

`LIKE` copies the native schema, including primary/partition keys, native types, timestamp kind and precision, and top-level and nested `NOT NULL` constraints, together with reusable table options. The target receives a new identity and location; no source data files or snapshots are copied. Source-specific locations and credentials are not copied. Creating a table with `LIKE` does not bypass the writer capability checks below.

### Writer boundaries

- Writes target native file-store tables on the main branch and require a single autocommit statement. Explicit multi-statement transactions and exactly-once recovery across FE restarts are not supported.
- Only Parquet data files are supported, including any per-level file-format configuration. External data paths (`data-file.external-paths`), alternate data directories (`data-file.path-directory`), and committables with auxiliary files are not supported by this writer.
- Primary-key tables require fixed buckets (`bucket > 0`) and `merge-engine = deduplicate`. Append tables support unaware buckets (`bucket = -1`) or fixed buckets (`bucket > 0`, with the required native bucket-key configuration).
- Primary-key dynamic buckets (`bucket = -1`), postponed buckets (`bucket = -2`), global indexes, data evolution, row tracking, and `FILEBLOB` are unsupported. Cross-partition primary-key updates are unsupported: every partition column must be included in the primary key.
- Partition values currently support `BOOLEAN`, integer types, `CHAR`/`VARCHAR`, and `DATE`; support for timestamps and decimals as data columns does not extend to partition columns. Nullable partition columns use the native NULL marker. Empty or whitespace-only string partition values are explicitly rejected because the public SDK conflates them with NULL; the reserved default-partition name is also rejected.
- Data writing uses only the native C++ writer through the isolated `libstarrocks_paimon.so` shim. A missing or incompatible writer library fails the operation; there is no JNI writer or JNI fallback. The FE's Java catalog and snapshot-commit protocol remain separate from the data writer and do not use JNI. The public Paimon C++ 0.3 and Java 2.0 SDK baselines are unchanged.

### INSERT OVERWRITE

Paimon overwrites publish a snapshot rather than swapping StarRocks temporary partitions. Named and temporary partitions are not accepted; static partitions use column-value pairs and must specify all partition columns.

| Mode | Selection | Effect with empty input |
| --- | --- | --- |
| Full | No `PARTITION` clause and dynamic overwrite disabled; also used for unpartitioned tables. Replaces the whole table. | Clears the whole table. |
| Static | `PARTITION (column = value, ...)`. Replaces the specified partition. | Clears the specified partition. |
| Dynamic | No static partition clause, a partitioned table, and `dynamic_overwrite = true`. Replaces only partitions touched by input rows; leaves other partitions unchanged. | No partitions are touched; existing data remains unchanged. |

```sql
INSERT OVERWRITE paimon_catalog_fs.write_db.orders
PARTITION (dt = '2026-01-01')
SELECT id, amount FROM paimon_catalog_fs.write_db.orders_copy
WHERE dt = '2026-01-01';

INSERT /*+ SET_VAR(dynamic_overwrite = true) */ OVERWRITE paimon_catalog_fs.write_db.orders
SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders_copy;
```

An empty `INSERT INTO` is also a no-op. Full and static overwrites still perform their clearing operation when the input is empty.

### Commit outcomes and read-back

A nonempty write, or a full/static overwrite with empty input, is reported successful only after the FE confirms Paimon snapshot publication. Finishing BE file writes alone is not success. Empty append and empty dynamic-overwrite operations are confirmed no-ops and need no new snapshot.

:::warning
If the error contains `Paimon commit outcome is unknown for operation ...; preserve its files and reconcile the snapshot before retrying`, publication may already be durable. Preserve the files and any CTAS target, retain the operation identifier, and reconcile Paimon snapshots before deciding whether to retry. Do not blindly rerun the statement or delete its output, even if the client saw a failure or cancellation.
:::

Writing does not remove existing reader limitations, including the `VARIANT` boundary described above or timestamp/time-zone mappings. Read-back validation for this feature is scoped to the native C++ reader and the StarRocks native Parquet reader, not JNI interoperability. Primary-key results follow native merge semantics rather than physical input row counts. Reader selection, compaction, timestamp/time-zone behavior, and native read-back still require end-to-end runtime validation.
