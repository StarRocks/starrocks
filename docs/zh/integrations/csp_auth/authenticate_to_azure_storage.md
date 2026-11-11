---
sidebar_position: 20
displayed_sidebar: docs
sidebar_label: Microsoft Azure Storage
description: "本文介绍如何配置 StarRocks 访问 Azure Blob Storage 和 Azure Data Lake Storage 时使用的认证信息"
---

# 配置 Microsoft Azure Storage 认证信息

StarRocks 从 3.0 版本起支持在以下场景中集成 Microsoft Azure Storage（Azure Blob Storage 或 Azure Data Lake Storage）：

- 查询 Azure Storage 中的 [Hive](../../data_source/catalog/hive_catalog.md)、[Iceberg](../../data_source/catalog/iceberg/iceberg.md)、[Hudi](../../data_source/catalog/hudi_catalog.md) 和 [Delta Lake](../../data_source/catalog/deltalake_catalog.md) 表。
- 从 Azure Storage 批量导入数据。
- 从 Azure Storage 备份数据、或把数据恢复到 Azure Storage。
- 查询 Azure Storage 中的 Parquet 或 ORC 格式的数据文件。

下表列出了 StarRocks 针对每种类型的 Azure Storage 所支持的认证方式。

| **Azure Storage 类型**                   | **认证方式**                                                 |
| ---------------------------------------- | ------------------------------------------------------------ |
| Azure Blob Storage                       | <ul><li>Shared Key（存储账号的访问密钥）</li><li>SAS Token</li></ul> |
| Azure Data Lake Storage Gen2 (ADLS Gen2) | <ul><li>Managed Identity</li><li>Shared Key（存储账号的访问密钥）</li><li>Service Principal</li></ul> |
| Azure Data Lake Storage Gen1 (ADLS Gen1)，已停用 | <ul><li>Managed Service Identity</li><li>Service Principal</li></ul> |

从 v3.5.10 版本起，ADLS Gen2 还支持 [Workload Identity](#基于-workload-identity-认证鉴权)。

Managed Identity 认证方式使用集群托管身份。本文中，集群托管身份是指挂载到 StarRocks 集群部署所在虚拟机 (VM) 上的用户分配的托管身份。

对于 External Catalog，请使用存储外部数据源数据的存储账号的认证信息。

本文以 Hive catalog、文件外部表和 Broker Load 为例。Broker Load 示例将数据文件 `file1.parquet` 导入到数据库 `mydatabase` 中的表 `table1`。`file1.parquet` 与 `table1` 的表结构相同。有关示例中参数的详细说明，参见 [Hive catalog](../../data_source/catalog/hive_catalog.md)、[文件外部表](../../data_source/file_external_table.md)和 [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)。

## Blob Storage

StarRocks 支持通过以下认证方式来访问 Blob Storage：

- Shared Key
- SAS Token

> **说明**
>
> 从 Blob Storage 导入数据或直接查询 Blob Storage 中的数据文件时，需要使用 wasb 或 wasbs 作为文件协议访问目标数据：
>
> - 如果您的存储账号支持通过 HTTP 协议进行访问，请使用 wasb 文件协议，文件路径格式为 `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>`。
> - 如果您的存储账号支持通过 HTTPS 协议进行访问，请使用 wasbs 文件协议，文件路径格式为 `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>`。

### 基于 Shared Key 认证鉴权

访问密钥（即存储账号的 Shared Key）用于对 StarRocks 集群发送到存储账号的请求进行认证。有关访问密钥的详细信息，参见 [Manage account access keys - Azure Storage](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal)。

#### 参数说明

| **参数**                   | **说明**                                                  |
| -------------------------- | --------------------------------------------------------- |
| azure.blob.storage_account | Blob Storage 存储账号的名称。                             |
| azure.blob.shared_key      | Blob Storage 存储账号的 Shared Key（访问密钥）。          |

按照以下步骤获取 Blob Storage 存储账号的 Shared Key：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** 页面。
2. 找到您的存储账号，并单击其名称。
3. 在左侧导航栏中，选择 **Access keys**。
4. 单击要使用的密钥旁边的 **Show**，然后单击复制图标复制该密钥。

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.blob.storage_account` 和 `azure.blob.shared_key`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.shared_key` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.shared_key` 和文件路径 (`DATA INFILE`)：

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

### 基于 SAS Token 认证鉴权

共享访问签名 (SAS) 是一种令牌，用于授予对存储账号的受限访问权限。有关 SAS Token 的详细信息，参见 [Delegate access by using a shared access signature - Azure Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature)。

#### 参数说明

| **参数**                   | **说明**                                                     |
| -------------------------- | ------------------------------------------------------------ |
| azure.blob.storage_account | Blob Storage 存储账号的名称。                                |
| azure.blob.container       | Blob Storage 存储账号中存储数据的 Blob 容器的名称。          |
| azure.blob.sas_token       | 用于访问 Blob Storage 存储账号的 SAS Token。                 |

按照以下步骤获取 Blob Storage 存储账号的 SAS Token：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** 页面。
2. 找到您的存储账号，并单击其名称。
3. 在左侧导航栏中，选择 **Shared access signature**。
4. 检查 **Allowed services**、**Allowed resource types** 和 **Allowed permissions** 区域中的设置。然后，从 **Signing key** 下拉列表中选择一个密钥，并单击 **Generate SAS and connection string**。
5. 单击复制图标复制生成的 SAS Token。

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.container` 和 `azure.blob.sas_token`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token` 和文件路径 (`DATA INFILE`)：

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

StarRocks 支持通过以下认证方式来访问 Data Lake Storage Gen2：

- Managed Identity
- Shared Key
- Service Principal
- Workload Identity（自 v3.5.10 起）

> **说明**
>
> 从 Data Lake Storage Gen2 导入数据或查询 Data Lake Storage Gen2 中的数据文件时，需要使用 abfs 或 abfss 作为文件协议访问目标数据：
>
> - 如果您的存储账号支持通过 HTTP 协议进行访问，请使用 abfs 文件协议，文件路径格式为 `abfs://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>`。
> - 如果您的存储账号支持通过 HTTPS 协议进行访问，请使用 abfss 文件协议，文件路径格式为 `abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>`。

### 基于 Managed Identity 认证鉴权

用户分配的托管身份使 Azure 资源（例如 VM）无需存储凭据即可向其他 Azure 服务进行认证。用户分配的托管身份是一种独立的 Azure 资源。一台 VM 可以使用多个用户分配的托管身份，一个用户分配的托管身份也可以由多台 VM 共享。

使用该认证方式时，StarRocks 通过集群托管身份访问您的存储账号。

#### 准备工作

使用该认证方式前，请确保集群托管身份已存在，并且对您的存储账号拥有所需权限。

首先，确保 StarRocks 集群部署所在的 VM 上已挂载用户分配的托管身份，并记下其名称。如果尚未挂载，请创建一个用户分配的托管身份并将其添加到这些 VM 上。详细信息参见 [Configure managed identities on Azure virtual machines](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-configure-managed-identities)。

然后，检查集群托管身份是否对您的存储账号拥有所需权限：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** 页面。
2. 找到您的存储账号，并单击其名称。
3. 在左侧导航栏中，选择 **Access control (IAM)**。
4. 在 **Role assignments** 页签上，检查现有的角色分配。
5. 如果集群托管身份对该存储账号没有所需权限，请单击 **Add** 并选择 **Add role assignment**。然后，按如下添加角色分配：

   a. 在 **Job function roles** 页签上，选择一个角色并单击 **Next**。例如，**Storage Blob Data Reader** 授予读权限，**Storage Blob Data Contributor** 授予读写权限。

   b. 在 **Members** 页签上，将 **Assign access to** 设置为 **Managed identity**。

   c. 对于 **Members**，单击 **Select members**。在弹出的面板中，选择您的订阅，从 **Managed identity** 下拉列表中选择 **User-assigned managed identity**，选择集群托管身份，然后单击 **Select**。

   d. 单击 **Review + assign**。核对信息后，再次单击 **Review + assign** 添加角色分配。

#### 参数说明

| **参数**                                | **说明**                                                     |
| --------------------------------------- | ------------------------------------------------------------ |
| azure.adls2.oauth2_use_managed_identity | 指定是否开启 Managed Identity 认证方式。设置为 `true`。      |
| azure.adls2.oauth2_tenant_id            | ADLS Gen2 存储账号所属租户的 ID。                            |
| azure.adls2.oauth2_client_id            | 集群托管身份的客户端 ID。                                    |

按照以下步骤获取租户 ID：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Tenant properties](https://portal.azure.com/#view/Microsoft_AAD_IAM/TenantProperties.ReactView)** 页面。
2. 找到 **Tenant ID**，单击复制图标复制该 ID。

按照以下步骤获取集群托管身份的客户端 ID：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Managed Identities](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.ManagedIdentity%2FuserAssignedIdentities)** 页面。
2. 找到集群托管身份，并单击其名称。
3. 在 **Overview** 页面上，找到 **Client ID**，单击复制图标复制该 ID。

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id` 和 `azure.adls2.oauth2_client_id`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`DATA INFILE`)：

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

### 基于 Shared Key 认证鉴权

通过 Shared Key 访问 ADLS Gen2 与通过 Shared Key 访问 Blob Storage 的方式相同。有关如何获取 Shared Key，参见 [Blob Storage > 基于 Shared Key 认证鉴权](#基于-shared-key-认证鉴权)。

#### 参数说明

| **参数**                    | **说明**                                                     |
| --------------------------- | ------------------------------------------------------------ |
| azure.adls2.storage_account | ADLS Gen2 存储账号的名称。                                   |
| azure.adls2.shared_key      | ADLS Gen2 存储账号的 Shared Key（访问密钥）。                |

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.storage_account` 和 `azure.adls2.shared_key`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.storage_account`、`azure.adls2.shared_key` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.storage_account`、`azure.adls2.shared_key` 和文件路径 (`DATA INFILE`)：

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

### 基于 Service Principal 认证鉴权

#### 准备工作

使用该认证方式前，请确保您已有一个 Service Principal，并且该 Service Principal 对您的存储账号拥有所需权限。有关如何创建 Service Principal 和客户端密钥，参见 [Register a Microsoft Entra app and create a service principal](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal)。

按照以下步骤检查 Service Principal 的权限：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** 页面。
2. 找到您的 ADLS Gen2 存储账号，并单击其名称。
3. 在左侧导航栏中，选择 **Access control (IAM)**。
4. 在 **Role assignments** 页签上，检查现有的角色分配。
5. 如果该 Service Principal 对存储账号没有所需权限，请单击 **Add** 并选择 **Add role assignment**。然后，按如下添加角色分配：

   a. 在 **Job function roles** 页签上，选择一个角色并单击 **Next**。例如，**Storage Blob Data Reader** 授予读权限，**Storage Blob Data Contributor** 授予读写权限。

   b. 在 **Members** 页签上，将 **Assign access to** 设置为 **User, group, or service principal**。

   c. 对于 **Members**，单击 **Select members**。在弹出的面板中，搜索并选择您注册的应用程序，然后单击 **Select**。

   d. 单击 **Review + assign**。核对信息后，再次单击 **Review + assign** 添加角色分配。

#### 参数说明

| **参数**                           | **说明**                                                    |
| ---------------------------------- | ----------------------------------------------------------- |
| azure.adls2.oauth2_client_id       | Service Principal 的应用程序（客户端）ID。                  |
| azure.adls2.oauth2_client_secret   | Service Principal 的客户端密钥的值。                        |
| azure.adls2.oauth2_client_endpoint | Service Principal 的 OAuth 2.0 令牌端点 (v1)。              |

> **说明**
>
> Azure 仅在创建客户端密钥后立即显示一次其值。请在离开该页面前保存该值。

按照以下步骤获取 Service Principal 的应用程序（客户端）ID 和 OAuth 2.0 令牌端点 (v1)：

1. 登录 [Azure 门户](https://portal.azure.com/#home)，进入 **[App registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)** 页面。
2. 找到您注册的应用程序，并单击其名称。
3. 在 **Overview** 页面上，找到 **Application (client) ID**，单击复制图标复制该 ID。
4. 单击 **Overview** 页面顶部的 **Endpoints**。
5. 在 **Endpoints** 面板中，找到 **OAuth 2.0 token endpoint (v1)**，单击复制图标复制该端点。

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret` 和 `azure.adls2.oauth2_client_endpoint`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint` 和文件路径 (`DATA INFILE`)：

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

### 基于 Workload Identity 认证鉴权

从 v3.5.10 版本开始，StarRocks 支持将 Azure Workload Identity 作为访问 Azure Data Lake Storage Gen2 时的身份验证方法。该身份验证方法专为在 Azure 内部运行的工作负载（例如 AKS Pod）设计，其中计算身份通过投影令牌文件与 Azure AD 应用程序进行联合，从而避免了存储长期有效凭据的必要。

要使用 Workload Identity 认证，在继续操作前需完成以下 Azure 端的配置：

- 在您的 AKS 集群（或等效环境）上启用 Azure 工作负载身份 Webhook。
- 创建一个联合身份凭据，将您的 Kubernetes 服务账户与 Azure AD 应用程序或用户分配的托管身份关联起来。
- 为 Kubernetes Pod/服务账户添加注解，以便 Webhook 注入令牌投影。令牌文件将出现在 `azure.adls2.oauth2_token_file` 中配置的路径下（通常为 `/var/run/secrets/azure/tokens/azure-identity-token`）。
- 为该 Azure AD 身份授予存储账号上的必要 RBAC 角色（例如，Storage Blob Data Reader 或 Storage Blob Data Contributor）。

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id` 和 `azure.adls2.oauth2_client_id`：

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

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`DATA INFILE`)：

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

> **说明**
>
> Microsoft 已于 2024 年 2 月 29 日停用 Azure Data Lake Storage Gen1。本文中有关 Gen1 的章节仅供参考。

StarRocks 支持通过以下认证方式来访问 Data Lake Storage Gen1：

- Managed Service Identity
- Service Principal

> **说明**
>
> 从 Data Lake Storage Gen1 导入数据或查询 Data Lake Storage Gen1 中的数据文件时，需要使用 adl 作为文件协议访问目标数据，文件路径格式为 `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`。

### 基于 Managed Service Identity 认证鉴权

#### 参数说明

| **参数**                                 | **说明**                                                     |
| ---------------------------------------- | ------------------------------------------------------------ |
| azure.adls1.use_managed_service_identity | 指定是否开启 Managed Service Identity 认证方式。设置为 `true`。 |

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity` 和文件路径 (`DATA INFILE`)：

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

### 基于 Service Principal 认证鉴权

有关如何创建 Service Principal 并获取其应用程序（客户端）ID、客户端密钥和 OAuth 2.0 令牌端点 (v1)，参见 [Data Lake Storage Gen2 > 基于 Service Principal 认证鉴权](#基于-service-principal-认证鉴权)。

#### 参数说明

| **参数**                      | **说明**                                                     |
| ----------------------------- | ------------------------------------------------------------ |
| azure.adls1.oauth2_client_id  | Service Principal 的应用程序（客户端）ID。                   |
| azure.adls1.oauth2_credential | Service Principal 的客户端密钥的值。                         |
| azure.adls1.oauth2_endpoint   | Service Principal 的 OAuth 2.0 令牌端点 (v1)。               |

#### 示例

##### External Catalog

在 [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential` 和 `azure.adls1.oauth2_endpoint`：

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

##### 文件外部表

在 [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint` 和文件路径 (`path`)：

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

在 [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint` 和文件路径 (`DATA INFILE`)：

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
