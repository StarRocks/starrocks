---
sidebar_position: 20
displayed_sidebar: docs
sidebar_label: Microsoft Azure Storage
description: "How to configure StarRocks to authenticate with Azure Blob Storage and Azure Data Lake Storage."
---

# Authenticate to Microsoft Azure Storage

From v3.0 onwards, StarRocks can integrate with Microsoft Azure Storage (Azure Blob Storage or Azure Data Lake Storage) in the following scenarios:

- Query [Hive](../../data_source/catalog/hive_catalog.md), [Iceberg](../../data_source/catalog/iceberg/iceberg.md), [Hudi](../../data_source/catalog/hudi_catalog.md), and [Delta Lake](../../data_source/catalog/deltalake_catalog.md) tables in Azure Storage.
- Batch load data from Azure Storage.
- Back up data from and restore data to Azure Storage.
- Query Parquet and ORC files in Azure Storage.

The following table lists the authentication methods that StarRocks supports for each type of Azure Storage.

| **Azure Storage type**                   | **Authentication method**                                    |
| ---------------------------------------- | ------------------------------------------------------------ |
| Azure Blob Storage                       | <ul><li>Shared Key (access key of storage account)</li><li>SAS Token</li></ul> |
| Azure Data Lake Storage Gen2 (ADLS Gen2) | <ul><li>Managed Identity</li><li>Shared Key (access key of storage account)</li><li>Service Principal</li></ul> |
| Azure Data Lake Storage Gen1 (ADLS Gen1), retired | <ul><li>Managed Service Identity</li><li>Service Principal</li></ul> |

From v3.5.10 onwards, ADLS Gen2 also supports [Workload Identity](#workload-identity).

The Managed Identity method uses the cluster managed identity. In this topic, the cluster managed identity is the user-assigned managed identity attached to the virtual machines (VMs) on which your StarRocks cluster is deployed.

For an external catalog, use the credentials of the storage account that stores the data of the external data source.

In this topic, Hive catalog, file external table, and Broker Load are used as examples. The Broker Load examples load the data file `file1.parquet` into the table `table1` in the database `mydatabase`. `file1.parquet` and `table1` have the same schema. For information about the parameters in the examples, see [Hive catalog](../../data_source/catalog/hive_catalog.md), [File external table](../../data_source/file_external_table.md), and [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md).

## Blob Storage

StarRocks supports using one of the following authentication methods to access Blob Storage:

- Shared Key
- SAS Token

> **NOTE**
>
> When you load data or directly query files from Blob Storage, you must use the wasb or wasbs protocol to access your data:
>
> - If your storage account allows access over HTTP, use the wasb protocol and write the file path as `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>`.
> - If your storage account allows access over HTTPS, use the wasbs protocol and write the file path as `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>`.

### Shared Key (access key of storage account)

Access keys authenticate the requests that your StarRocks cluster sends to your storage account. For more information about access keys, see [Manage account access keys - Azure Storage](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

#### Parameters

| **Parameter**              | **Description**                                           |
| -------------------------- | --------------------------------------------------------- |
| azure.blob.storage_account | The name of your Blob storage account.                    |
| azure.blob.shared_key      | The shared key (access key) of your Blob storage account. |

Follow these steps to obtain the shared key of your Blob storage account:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** page.
2. Find your storage account and click its name.
3. In the left-side navigation pane, choose **Access keys**.
4. Click **Show** next to the key you want to use, and then click the copy icon to copy the key.

#### Examples

##### External catalog

Configure `azure.blob.storage_account` and `azure.blob.shared_key` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
);
```

##### File external table

Configure `azure.blob.storage_account`, `azure.blob.shared_key`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
);
```

##### Broker Load

Configure `azure.blob.storage_account`, `azure.blob.shared_key`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_blob_key
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
);
```

### SAS Token

A shared access signature (SAS) is a token that grants restricted access to your storage account. For more information about SAS tokens, see [Delegate access by using a shared access signature - Azure Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

#### Parameters

| **Parameter**              | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| azure.blob.storage_account | The name of your Blob storage account.                       |
| azure.blob.container       | The name of the Blob container that stores your data in your Blob storage account. |
| azure.blob.sas_token       | The SAS token that is used to access your Blob storage account. |

Follow these steps to obtain a SAS token for your Blob storage account:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** page.
2. Find your storage account and click its name.
3. In the left-side navigation pane, choose **Shared access signature**.
4. Check the settings in the **Allowed services**, **Allowed resource types**, and **Allowed permissions** sections. Then, select a key from the **Signing key** drop-down list and click **Generate SAS and connection string**.
5. Click the copy icon to copy the generated SAS token.

#### Examples

##### External catalog

Configure `azure.blob.storage_account`, `azure.blob.container`, and `azure.blob.sas_token` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.container" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

##### File external table

Configure `azure.blob.storage_account`, `azure.blob.container`, `azure.blob.sas_token`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.container" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

##### Broker Load

Configure `azure.blob.storage_account`, `azure.blob.container`, `azure.blob.sas_token`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_blob_sas
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.container" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

## Data Lake Storage Gen2

StarRocks supports using one of the following authentication methods to access Data Lake Storage Gen2:

- Managed Identity
- Shared Key
- Service Principal
- Workload Identity (from v3.5.10 onwards)

> **NOTE**
>
> When you load data or query files from Data Lake Storage Gen2, you must use the abfs or abfss protocol to access your data:
>
> - If your storage account allows access over HTTP, use the abfs protocol and write the file path as `abfs://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>`.
> - If your storage account allows access over HTTPS, use the abfss protocol and write the file path as `abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>`.

### Managed Identity

A user-assigned managed identity lets Azure resources, such as VMs, authenticate to other Azure services without storing credentials. It is a standalone Azure resource. One VM can use more than one user-assigned managed identity, and one user-assigned managed identity can be shared by more than one VM.

With this method, StarRocks uses the cluster managed identity to access your storage account.

#### Preparation

Before you use this authentication method, make sure that the cluster managed identity exists and has the required permissions on your storage account.

First, make sure that a user-assigned managed identity is attached to the VMs on which your StarRocks cluster is deployed, and note its name. If none is attached, create one and add it to these VMs. For more information, see [Configure managed identities on Azure virtual machines](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-configure-managed-identities).

Then, check that the cluster managed identity has the required permissions on your storage account:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** page.
2. Find your storage account and click its name.
3. In the left-side navigation pane, choose **Access control (IAM)**.
4. On the **Role assignments** tab, check the existing role assignments.
5. If the cluster managed identity does not have the required permissions on the storage account, click **Add** and choose **Add role assignment**. Then, add a role assignment:

   a. On the **Job function roles** tab, select a role and click **Next**. For example, **Storage Blob Data Reader** grants read access, and **Storage Blob Data Contributor** grants read and write access.

   b. On the **Members** tab, select **Managed identity** for **Assign access to**.

   c. For **Members**, click **Select members**. In the pane that appears, select your subscription, select **User-assigned managed identity** from the **Managed identity** drop-down list, select the cluster managed identity, and then click **Select**.

   d. Click **Review + assign**. Review the information, and then click **Review + assign** again to add the role assignment.

#### Parameters

| **Parameter**                           | **Description**                                              |
| --------------------------------------- | ------------------------------------------------------------ |
| azure.adls2.oauth2_use_managed_identity | Specifies whether to enable the Managed Identity authentication method. Set the value to `true`. |
| azure.adls2.oauth2_tenant_id            | The ID of the tenant of your ADLS Gen2 storage account.      |
| azure.adls2.oauth2_client_id            | The client ID of the cluster managed identity.               |

Follow these steps to obtain the tenant ID:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Tenant properties](https://portal.azure.com/#view/Microsoft_AAD_IAM/TenantProperties.ReactView)** page.
2. Find **Tenant ID** and click the copy icon to copy it.

Follow these steps to obtain the client ID of the cluster managed identity:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Managed Identities](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.ManagedIdentity%2FuserAssignedIdentities)** page.
2. Find the cluster managed identity and click its name.
3. On the **Overview** page, find **Client ID** and click the copy icon to copy it.

#### Examples

##### External catalog

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, and `azure.adls2.oauth2_client_id` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<tenant_id>",
    "azure.adls2.oauth2_client_id" = "<managed_identity_client_id>"
);
```

##### File external table

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<tenant_id>",
    "azure.adls2.oauth2_client_id" = "<managed_identity_client_id>"
);
```

##### Broker Load

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen2_identity
(
    DATA INFILE("abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<tenant_id>",
    "azure.adls2.oauth2_client_id" = "<managed_identity_client_id>"
);
```

### Shared Key (access key of storage account)

Using Shared Key to access ADLS Gen2 works in the same way as using Shared Key to access Blob Storage. For information about how to obtain the shared key, see [Blob Storage > Shared Key](#shared-key-access-key-of-storage-account).

#### Parameters

| **Parameter**               | **Description**                                              |
| --------------------------- | ------------------------------------------------------------ |
| azure.adls2.storage_account | The name of your ADLS Gen2 storage account.                  |
| azure.adls2.shared_key      | The shared key (access key) of your ADLS Gen2 storage account. |

#### Examples

##### External catalog

Configure `azure.adls2.storage_account` and `azure.adls2.shared_key` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

##### File external table

Configure `azure.adls2.storage_account`, `azure.adls2.shared_key`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

##### Broker Load

Configure `azure.adls2.storage_account`, `azure.adls2.shared_key`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen2_key
(
    DATA INFILE("abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

### Service Principal

#### Preparation

Before you use this authentication method, make sure that you have a service principal, and that it has the required permissions on your storage account. For information about how to create a service principal and a client secret, see [Register a Microsoft Entra app and create a service principal](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal).

Follow these steps to check the permissions of the service principal:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** page.
2. Find your ADLS Gen2 storage account and click its name.
3. In the left-side navigation pane, choose **Access control (IAM)**.
4. On the **Role assignments** tab, check the existing role assignments.
5. If the service principal does not have the required permissions on the storage account, click **Add** and choose **Add role assignment**. Then, add a role assignment:

   a. On the **Job function roles** tab, select a role and click **Next**. For example, **Storage Blob Data Reader** grants read access, and **Storage Blob Data Contributor** grants read and write access.

   b. On the **Members** tab, select **User, group, or service principal** for **Assign access to**.

   c. For **Members**, click **Select members**. In the pane that appears, search for and select the application that you registered, and then click **Select**.

   d. Click **Review + assign**. Review the information, and then click **Review + assign** again to add the role assignment.

#### Parameters

| **Parameter**                      | **Description**                                             |
| ---------------------------------- | ----------------------------------------------------------- |
| azure.adls2.oauth2_client_id       | The application (client) ID of the service principal.       |
| azure.adls2.oauth2_client_secret   | The value of the client secret of the service principal.    |
| azure.adls2.oauth2_client_endpoint | The OAuth 2.0 token endpoint (v1) of the service principal. |

> **NOTE**
>
> Azure shows the value of a client secret only once, right after you create it. Save the value before you leave the page.

Follow these steps to obtain the application (client) ID and the OAuth 2.0 token endpoint (v1) of the service principal:

1. Sign in to the [Azure portal](https://portal.azure.com/#home) and go to the **[App registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)** page.
2. Find your registered application and click its name.
3. On the **Overview** page, find **Application (client) ID** and click the copy icon to copy it.
4. Click **Endpoints** at the top of the **Overview** page.
5. In the **Endpoints** pane, find **OAuth 2.0 token endpoint (v1)** and click the copy icon to copy it.

#### Examples

##### External catalog

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, and `azure.adls2.oauth2_client_endpoint` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

##### File external table

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, `azure.adls2.oauth2_client_endpoint`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls2.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

##### Broker Load

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, `azure.adls2.oauth2_client_endpoint`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen2_principal
(
    DATA INFILE("abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

### Workload Identity

From v3.5.10 onwards, StarRocks supports Azure Workload Identity as an authentication method when accessing Azure Data Lake Storage Gen2. This authentication method is designed for workloads running inside Azure (for example, AKS pods) where the compute identity is federated with an Azure AD application via a projected token file, avoiding the need to store long-lived credentials.

To use Workload Identity authentication, the following Azure-side setup is required before proceeding:

- Enable the Azure Workload Identity webhook on your AKS cluster (or equivalent).
- Create a federated identity credential linking your Kubernetes service account to an Azure AD application or user-assigned managed identity.
- Annotate the Kubernetes pod/service account so that the webhook injects the token projection. The token file will appear at the path configured in `azure.adls2.oauth2_token_file` (commonly `/var/run/secrets/azure/tokens/azure-identity-token`).
- Grant the Azure AD identity the necessary RBAC role on the storage account (for example, Storage Blob Data Reader or Storage Blob Data Contributor).

#### External catalog

Configure `azure.adls2.oauth2_token_file`, `azure.adls2.oauth2_tenant_id`, and `azure.adls2.oauth2_client_id` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### File external table

Configure `azure.adls2.oauth2_token_file`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### Broker Load

Configure `azure.adls2.oauth2_token_file`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen2_workload
(
    DATA INFILE("abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

## Data Lake Storage Gen1

> **NOTE**
>
> Microsoft retired Azure Data Lake Storage Gen1 on February 29, 2024. The Gen1 sections of this topic are kept for reference.

StarRocks supports using one of the following authentication methods to access Data Lake Storage Gen1:

- Managed Service Identity
- Service Principal

> **NOTE**
>
> When you load data or query files from Data Lake Storage Gen1, you must use the adl protocol to access your data and write the file path as `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`.

### Managed Service Identity

#### Parameters

| **Parameter**                            | **Description**                                              |
| ---------------------------------------- | ------------------------------------------------------------ |
| azure.adls1.use_managed_service_identity | Specifies whether to enable the Managed Service Identity authentication method. Set the value to `true`. |

#### Examples

##### External catalog

Configure `azure.adls1.use_managed_service_identity` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

##### File external table

Configure `azure.adls1.use_managed_service_identity` and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

##### Broker Load

Configure `azure.adls1.use_managed_service_identity` and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen1_identity
(
    DATA INFILE("adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls1.use_managed_service_identity" = "true"
);
```

### Service Principal

For information about how to create a service principal and obtain its application (client) ID, client secret, and OAuth 2.0 token endpoint (v1), see [Data Lake Storage Gen2 > Service Principal](#service-principal).

#### Parameters

| **Parameter**                 | **Description**                                              |
| ----------------------------- | ------------------------------------------------------------ |
| azure.adls1.oauth2_client_id  | The application (client) ID of the service principal.        |
| azure.adls1.oauth2_credential | The value of the client secret of the service principal.     |
| azure.adls1.oauth2_endpoint   | The OAuth 2.0 token endpoint (v1) of the service principal.  |

#### Examples

##### External catalog

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, and `azure.adls1.oauth2_endpoint` as follows in the [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls1.oauth2_credential" = "<service_principal_client_secret>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_token_endpoint_v1>"
);
```

##### File external table

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, `azure.adls1.oauth2_endpoint`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) statement:

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>",
    "format" = "ORC",
    "azure.adls1.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls1.oauth2_credential" = "<service_principal_client_secret>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_token_endpoint_v1>"
);
```

##### Broker Load

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, `azure.adls1.oauth2_endpoint`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) statement:

```SQL
LOAD LABEL mydatabase.label_gen1_principal
(
    DATA INFILE("adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/file1.parquet")
    INTO TABLE table1
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls1.oauth2_client_id" = "<service_principal_client_id>",
    "azure.adls1.oauth2_credential" = "<service_principal_client_secret>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_token_endpoint_v1>"
);
```
