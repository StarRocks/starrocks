---
displayed_sidebar: "Chinese"
---

# 配置 Microsoft Azure Storage 认证信息

StarRocks 从 3.0 版本起支持在以下场景中集成 Microsoft Azure Storage（Azure Blob Storage 或 Azure Data Lake Storage）：

- 从 Azure Storage 批量导入数据。
- 从 Azure Storage 备份数据、或把数据恢复到 Azure Storage。
- 查询 Azure Storage 中的 Parquet 或 ORC 格式的数据文件。
- 查询 Azure Storage 中的 [Hive](../data_source/catalog/hive_catalog.md)、[Iceberg](../data_source/catalog/iceberg_catalog.md)、[Hudi](../data_source/catalog/hudi_catalog.md)、或 [Delta Lake](../data_source/catalog/deltalake_catalog.md) 表。

StarRocks 支持通过以下类型的 Azure 存储账号来访问 Azure Storage：

- Azure Blob Storage
- Azure Data Lake Storage Gen1
- Azure Data Lake Storage Gen2

本文档以 Hive catalog、文件外部表和 Broker Load 为例，介绍 StarRocks 在各应用场景下如何通过不同类型的存储账号来访问 Azure Storage。有关下面示例中出现的参数详解，参见 [Hive catalog](../data_source/catalog/hive_catalog.md)、[文件外部表](../data_source/file_external_table.md)和 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

## Blob Storage

StarRocks 支持通过以下认证方式来访问 Blob Storage：

- Shared Key
- SAS Token

> **说明**
>
> 从 Blob Storage 导入数据或直接查询 Blob Storage 中的数据文件时，需要使用 wasb 或 wasbs 作为文件协议访问目标数据：
>
> - 如果您的存储账号支持通过 HTTP 协议进行访问，请使用 wasb 文件协议，文件路径格式为 `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/`。
> - 如果您的存储账号支持通过 HTTPS 协议进行访问，请使用 wasbs 文件协议，文件路径格式为 `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/`。

### 基于 Shared Key 认证鉴权

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.blob.storage_account` 和 `azure.blob.shared_key`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.blob.storage_account`、`azure.blob.shared_key` 和文件路径 (`path`)：

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

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置  `azure.blob.storage_account`、`azure.blob.shared_key` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
);
```

### 基于 SAS Token 认证鉴权

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.blob.account_name`、`azure.blob.container_name` 和 `azure.blob.sas_token`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.blob.account_name" = "<blob_storage_account_name>",
    "azure.blob.container_name" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.blob.account_name`、`azure.blob.container_name`、`azure.blob.sas_token` 和文件路径 (`path`)：

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
    "azure.blob.account_name" = "<blob_storage_account_name>",
    "azure.blob.container_name" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

#### Broker load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.blob.account_name`、`azure.blob.container_name`、`azure.blob.sas_token` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.blob.account_name" = "<blob_storage_account_name>",
    "azure.blob.container_name" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

## Data Lake Storage Gen1

StarRocks 支持通过以下认证方式来访问 Data Lake Storage Gen1：

- Managed Service Identity
- Service Principal

> **说明**
>
> 从 Azure Data Lake Storage Gen1 导入数据或直接查询 Azure Data Lake Storage Gen1 中的数据文件时，需要使用 adl 作为文件协议访问目标数据，文件路径格式为 `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`。

### 基于 Managed Service Identity 认证鉴权

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity` 和文件路径 (`path`)：

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

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls1.use_managed_service_identity` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls1.use_managed_service_identity" = "true"
);
```

### 基于 Service Principal 认证鉴权

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential` 和 `azure.adls1.oauth2_endpoint`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls1.oauth2_client_id" = "<application_client_id>",
    "azure.adls1.oauth2_credential" = "<application_client_credential>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint` 和文件路径 (`path`)：

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
    "azure.adls1.oauth2_client_id" = "<application_client_id>",
    "azure.adls1.oauth2_credential" = "<application_client_credential>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
);
```

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls1.oauth2_client_id" = "<application_client_id>",
    "azure.adls1.oauth2_credential" = "<application_client_credential>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
);
```

## Data Lake Storage Gen2

StarRocks 支持通过以下认证方式来访问 Data Lake Storage Gen2：

- Managed Identity
- Shared Key
- Service Principal

> **说明**
>
> 从 Data Lake Storage Gen2 导入数据或直接查询 Azure Data Lake Storage Gen2 中的数据文件时，需要使用 abfs 或 abfss 作为文件协议访问目标数据：
>
> - 如果您的存储账号支持通过 HTTP 协议进行访问，请使用 abfs 文件协议，文件路径格式为 `abfs://<container>@<storage_account>.dfs.core.windows.net/<file_name>`。
> - 如果您的存储账号支持通过 HTTPS 协议进行访问，请使用 abfss 文件协议，文件路径格式为 `abfss://<container>@<storage_account>.dfs.core.windows.net/<file_name>`。

### 基于 Managed Identity 认证鉴权

如果选择 Managed Identity 鉴权方式，您必须提前完成如下准备工作：

- 根据认证要求，对 StarRocks 部署所在的 VM 进行编辑。
- 在这些 VM 上添加 Managed Identity。
- 确保添加的 Managed Identity 绑定了 **Storage Blob Data Reader** 角色（该角色拥有读取存储账号内数据的权限）。

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id` 和 `azure.adls2.oauth2_client_id`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`path`)：

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>",
    "format" = "ORC",
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

### 基于 Shared Key 认证鉴权

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.storage_account` 和 `azure.adls2.shared_key`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.storage_account`、`azure.adls2.shared_key` 和文件路径 (`path`)：

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>",
    "format" = "ORC",
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.storage_account`、`azure.adls2.shared_key` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.storage_account" = "<storage_account_name>",
    "azure.adls2.shared_key" = "<shared_key>"
);
```

### 基于 Service Principal 认证鉴权

如果选择 Service Principal 认证方式，您需要提前创建一个 Service Principal，然后创建一个角色分配条件 (Role Assignment)、并把该角色分配条件添加到存储账号，这样可以确保您通过创建的 Service Principal 能够正常访问存储账号内的数据。

#### External Catalog

在 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret` 和 `azure.adls2.oauth2_client_endpoint`：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls2.oauth2_client_id" = "<service_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

#### 文件外部表

在 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint` 和文件路径 (`path`)：

```SQL
CREATE EXTERNAL TABLE external_table_azure
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>",
    "format" = "ORC",
    "azure.adls2.oauth2_client_id" = "<service_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

#### Broker Load

在 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句中，按如下配置 `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint` 和文件路径 (`DATA INFILE`)：

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.oauth2_client_id" = "<service_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```
