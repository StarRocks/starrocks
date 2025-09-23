# Databricks Unity Catalog

StarRocks supports accessing Unity Catalog by creating Delta Lake or Iceberg catalogs, depending on the table format you want to query.

From v4.0 onwards, StarRocks supports:
- [Service Principal](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m) as a credential method for Databricks Unity Catalog, in addition to the original [Personal Access Token](https://docs.databricks.com/aws/en/dev-tools/auth/pat) credential method.
- Vended credentials for Databricks Unity Catalog.

## Create a Delta Lake catalog for Databricks Unity Catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "deltalake",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### Parameters

#### catalog_name

The name of the Delta Lake catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Delta Lake catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `deltalake`.

#### MetastoreParams

A set of parameters about how the system integrates with the metastore of your data source.

- If you use Personal Access Token as the credential method, configure `MetastoreParams` as follows:

  ```SQL
  "hive.metastore.type" = "unity",
  "databricks.host"= "https://<host>.cloud.databricks.com",
  "databricks.catalog.name" = "<catalog_name>",
  "databricks.token" = "<token>"
  ```

  :::note
  For detailed instructions on creating a personal access token in Databricks, see [Appendix - Create a Databricks personal access token](#create-a-databricks-personal-access-token).
  :::

- If you use Service Principal as the credential method, configure `MetastoreParams` as follows:

  ```SQL
  "hive.metastore.type" = "unity",
  "databricks.host"= "https://<host>.cloud.databricks.com",
  "databricks.catalog.name" = "<catalog_name>",
  "databricks.client.id" = "<client_id>",
  "databricks.client.secret" = "<client_secret>"
  ```

| Parameter                | Required | Description                                                                                               |
| :----------------------- | :------- | :-------------------------------------------------------------------------------------------------------- |
| hive.metastore.type      | Yes      | The type of metastore that you use for your Delta Lake cluster. Set the value to `unity`.                 |
| databricks.host          | Yes      | The host of your Databricks cluster, for example, `https://abcd1234.cloud.databricks.com`. You can refer to [workspace URL](https://docs.databricks.com/aws/en/workspace/workspace-details#workspace-url) to obtain the value of this parameter. |
| databricks.catalog.name  | Yes      | The name of your Unity Catalog.                                                                           |
| databricks.token         | No       | The personal access token used to access your Unity Catalog metastore. Set this parameter if you choose to use the personal access token credential. |
| databricks.client.id     | No       | The ID of the service principal used to access your Unity Catalog metastore. Set this parameter if you choose to use the service principal credential. |
| databricks.client.secret | No       | The secret of the service principal used to access your Unity Catalog metastore. Set this parameter if you choose to use the service principal credential. |

### StorageCredentialParams

Delta Lake catalogs support **AWS S3**, **Microsoft Azure Blob Storage and Data Lake Storage Gen2**, and **Google Cloud Storage** as the object storage. You can refer to [Delta Lake catalog - StorageCredentialParams](./deltalake_catalog.md#storagecredentialparams) for details.

From v4.0 onwards, Databricks Unity Catalogs support Vended Credentials. Vended Credential is enabled by default. You only need to specify `aws.s3.region` in `StorageCredentialParams` for accessing data stored in AWS S3. For Azure Blob Storage, Azure Data Lake Storage Gen2, and Google Cloud Storage, you do not need to specify `StorageCredentialParams`.

:::note
By setting MetastoreParams, StarRocks can access the Unity Catalog to obtain the database names, table names and table locations, but it cannot read the data of the table. It is because both the data and metadata files (delta_log) of the Delta Lake table are stored in the storage system. Therefore, you need to configure the corresponding storage system authentication parameters to ensure that StarRocks has the permission to read the files in Delta Lake table location.
:::

### Examples

The following examples create Delta Lake catalogs in various scenarios, to query data in Databricks Unity Catalog.

#### AWS S3

- If you choose AWS **IAM user-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

- If you choose AWS **instance profile-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

- If you choose AWS **assumed role-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "<iam_role_arn>",
      "aws.s3.region" = "<aws_s3_region>"
  );
  ```

- If you choose **vended credential** for AWS and **Service Principal** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.client.id" = "<service_principal_client_id>",
      "databricks.client.secret" = "<service_principal_client_secret>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

#### Azure Blob Storage

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- If you choose the SAS Token authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

- If you choose **vended credential** for Azure and **Personal Access Token** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "databricks.token" = "<personal_access_token>"
  );
  ```

#### Azure Data Lake Storage Gen2

- If you choose the Managed Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- If you choose the VM-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- If you choose the service account-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.token" = "<personal_access_token>",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- If you choose the impersonation-based authentication method:

  - If you make a VM instance impersonate a service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG databricks_unity_catalog
    PROPERTIES
    (
        "type"="deltalake",
        "hive.metastore.type" = "unity",
        "databricks.host"= "https://xxxxxx.cloud.databricks.com",
        "databricks.token" = "<personal_access_token>",
        "databricks.catalog.name" = "<unity_catalog_name>",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - If you make a service account impersonate another service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG databricks_unity_catalog
    PROPERTIES
    (
        "type"="deltalake",
        "hive.metastore.type" = "unity",
        "databricks.host"= "https://xxxxxx.cloud.databricks.com",
        "databricks.token" = "<personal_access_token>",
        "databricks.catalog.name" = "<unity_catalog_name>",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

- If you choose **vended credential** for GCS and **Service Principal** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_catalog
  PROPERTIES
  (
      "type"="deltalake",
      "hive.metastore.type" = "unity",
      "databricks.host"= "https://xxxxxx.cloud.databricks.com",
      "databricks.catalog.name" = "<unity_catalog_name>",
      "databricks.client.id" = "<service_principal_client_id>",
      "databricks.client.secret" = "<service_principal_client_secret>",
      "databricks.catalog.name" = "<unity_catalog_name>"
  );
  ```

### Feature support

Currently, Delta Lake catalogs support the following table features:

- V2 Checkpoint (From v3.3.0 onwards)
- Timestamp without Timezone (From v3.3.1 onwards)
- Column mapping (From v3.3.6 onwards)
- Deletion Vector (From v3.4.1 onwards)

## Create an Iceberg catalog for Databricks Unity Catalog

StarRocks supports accessing Iceberg tables in Databricks Unity Catalog by creating an Iceberg catalog. This allows you to query Iceberg-format tables managed by Unity Catalog.

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "iceberg",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
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

A set of parameters about how the system integrates with the Unity Catalog metastore for Iceberg tables using the REST catalog interface.

- If you use Personal Access Token as the credential method, configure `MetastoreParams` as follows:

  ```SQL
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "https://<databricks_workspace_url>/api/2.1/unity-catalog/iceberg-rest",
  "iceberg.catalog.security" = "oauth2",
  "iceberg.catalog.oauth2.token" = "<personal_access_token>",
  "iceberg.catalog.warehouse" = "<catalog_name>"
  ```

- If you use Service Principal as the credential method, configure `MetastoreParams` as follows:

  ```SQL
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "https://<databricks_workspace_url>/api/2.1/unity-catalog/iceberg-rest",
  "iceberg.catalog.security" = "oauth2",
  "iceberg.catalog.oauth2.server-uri" = "https://<databricks_workspace_url>/oidc/v1/token",
  "iceberg.catalog.oauth2.credential" = "<client_id>:<client_secret>",
  "iceberg.catalog.warehouse" = "<catalog_name>",
  "iceberg.catalog.oauth2.scope" = "all-apis"
  ```

| Parameter                         | Required | Description                                                                                               |
| :-------------------------------- | :------- | :-------------------------------------------------------------------------------------------------------- |
| iceberg.catalog.type              | Yes      | The type of metastore that you use for your Iceberg cluster. Set the value to `rest`.                     |
| iceberg.catalog.uri               | Yes      | The URI of the Databricks Unity Catalog REST API endpoint. Format: `https://<databricks_workspace_url>/api/2.1/unity-catalog/iceberg-rest`. |
| iceberg.catalog.security          | Yes      | The type of authorization protocol to use. Set the value to `oauth2`.                                     |
| iceberg.catalog.oauth2.token      | No       | The personal access token used to access your Unity Catalog metastore. Set this parameter if you choose to use the personal access token credential. |
| iceberg.catalog.oauth2.server-uri | No       | The URI of the OIDC provider. Set this parameter if you choose to use the service principal credential.   |
| iceberg.catalog.oauth2.credential | No       | The service principal credential in the format `<client_id>:<client_secret>` used to access your Unity Catalog metastore. Set this parameter if you choose to use the service principal credential. |
| iceberg.catalog.warehouse         | Yes      | The name of your Unity Catalog.                                  |

### StorageCredentialParams

Iceberg catalogs support **AWS S3**, **Microsoft Azure Blob Storage and Data Lake Storage Gen2**, and **Google Cloud Storage** as the object storage. You can refer to [Iceberg catalog - StorageCredentialParams](./iceberg/iceberg_catalog.md#storagecredentialparams) for details.

From v4.0 onwards, Databricks Unity Catalogs support Vended Credentials. Vended Credential is enabled by default. You only need to specify `aws.s3.region` in `StorageCredentialParams` for accessing data stored in AWS S3. For Azure Blob Storage, Azure Data Lake Storage Gen2, and Google Cloud Storage, you do not need to specify `StorageCredentialParams`.

:::note
By setting MetastoreParams, StarRocks can access the Unity Catalog to obtain the database names, table names and table locations, but it cannot read the data of the table. It is because both the data and metadata files of the Iceberg table are stored in the storage system. Therefore, you need to configure the corresponding storage system authentication parameters to ensure that StarRocks has the permission to read the files in Iceberg table location.
:::

### Examples

The following examples create Iceberg catalogs in various scenarios, to query Iceberg data in Databricks Unity Catalog.

#### AWS S3

- If you choose AWS **IAM user-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

- If you choose AWS **instance profile-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

- If you choose AWS **assumed role-based credential**, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "<iam_role_arn>",
      "aws.s3.region" = "<aws_s3_region>"
  );
  ```

- If you choose **vended credential** for AWS and **Service Principal** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.server-uri" = "https://<databricks_workspace_url>/oidc/v1/token",
      "iceberg.catalog.oauth2.credential" = "<service_principal_client_id>:<service_principal_client_secret>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "iceberg.catalog.oauth2.scope" = "all-apis",
      "aws.s3.region"  =  "<aws_s3_region>"
  );
  ```

#### Azure Blob Storage

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- If you choose the SAS Token authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

- If you choose **vended credential** for Azure and **Personal Access Token** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>"
  );
  ```

#### Azure Data Lake Storage Gen2

- If you choose the Managed Identity authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- If you choose the Shared Key authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- If you choose the Service Principal authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- If you choose the VM-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- If you choose the service account-based authentication method, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.token" = "<personal_access_token>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- If you choose the impersonation-based authentication method:

  - If you make a VM instance impersonate a service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
    PROPERTIES
    (
        "type"="iceberg",
        "iceberg.catalog.type" = "rest",
        "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
        "iceberg.catalog.security" = "oauth2",
        "iceberg.catalog.oauth2.token" = "<personal_access_token>",
        "iceberg.catalog.warehouse" = "<unity_catalog_name>",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - If you make a service account impersonate another service account, run a command like below:

    ```SQL
    CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
    PROPERTIES
    (
        "type"="iceberg",
        "iceberg.catalog.type" = "rest",
        "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
        "iceberg.catalog.security" = "oauth2",
        "iceberg.catalog.oauth2.token" = "<personal_access_token>",
        "iceberg.catalog.warehouse" = "<unity_catalog_name>",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

- If you choose **vended credential** for GCS and **Service Principal** for Databricks Unity Catalog, run a command like below:

  ```SQL
  CREATE EXTERNAL CATALOG databricks_unity_iceberg_catalog
  PROPERTIES
  (
      "type"="iceberg",
      "iceberg.catalog.type" = "rest",
      "iceberg.catalog.uri" = "https://xxxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
      "iceberg.catalog.security" = "oauth2",
      "iceberg.catalog.oauth2.server-uri" = "https://<databricks_workspace_url>/oidc/v1/token",
      "iceberg.catalog.oauth2.credential" = "<service_principal_client_id>:<service_principal_client_secret>",
      "iceberg.catalog.warehouse" = "<unity_catalog_name>",
      "iceberg.catalog.oauth2.scope" = "all-apis"
  );
  ```

## Limitations

The **table types** supported for querying by StarRocks are as follows:

| Table type                              | Status      |
|:----------------------------------------|-------------|
| Unity Catalog managed table             | Support     |
| Unity Catalog external Delta Lake table | Support     |
| Unity Catalog managed Iceberg table     | Support     |
| Streaming table                         | Not Support |
| Materialized view                       | Not Support |
| View                                    | Not Support |

## Appendix

### Create a Databricks personal access token

To create a Databricks personal access token for your Databricks workspace user, do the following:

1. In your Databricks workspace, click your Databricks username in the top bar, and then select **Settings** from the drop-down menu.

2. Click **Developer**.

3. Next to **Access tokens**, click **Manage**.

4. Click **Generate new token**.

5. Enter a comment that helps you to identify this token in the future.

6. Set the token’s lifetime in days.

    If you leave the **Lifetime (days)** box empty, the token lifetime is set to the maximum lifetime for your workspace. By default, the maximum token lifetime for a workspace is 730 days.
    
7. Click **Generate**.

8. Copy the displayed token to a secure location, and then click **Done**.

:::note
If you are not able to create or use tokens in your workspace, this might be because your workspace administrator has disabled tokens or has not given you permission to create or use tokens. See your workspace administrator or the following topics:
- [Enable or disable personal access token authentication for the workspace](https://docs.databricks.com/aws/en/admin/access-control/tokens#enable-tokens)
- [Personal access token permissions](https://docs.databricks.com/aws/en/security/auth/api-access-permissions#personal-access-token-permissions)
:::
