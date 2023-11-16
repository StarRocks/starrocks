---
displayed_sidebar: "English"
---

# Authenticate to Microsoft Azure Storage

From v3.0 onwards, StarRocks can integrate with Microsoft Azure Storage (Azure Blob Storage or Azure Data Lake Storage) in the following scenarios:

- Batch load data from Azure Storage.
- Back up data from and restore data to Azure Storage.
- Query Parquet and ORC files in Azure Storage.
- Query [Hive](../data_source/catalog/hive_catalog.md), [Iceberg](../data_source/catalog/iceberg_catalog.md), [Hudi](../data_source/catalog/hudi_catalog.md), and [Delta Lake](../data_source/catalog/deltalake_catalog.md) tables in Azure Storage.

StarRocks supports the following types of Azure Storage accounts:

- Azure Blob Storage
- Azure Data Lake Storage Gen1
- Azure Data Lake Storagee Gen2

In this topic, Hive catalog, file external table, and Broker Load are used as examples to show how StarRocks integrates with Azure Storage by using these types of Azure Storage accounts. For information about the parameters in the examples, see [Hive catalog](../data_source/catalog/hive_catalog.md), [File external table](../data_source/file_external_table.md), and [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md).

## Blob Storage

StarRocks supports using one of the following authentication methods to access Blob Storage:

- Shared Key
- SAS Token

> **NOTE**
>
> When you load data or directly query files from Blob Storage, you must use the wasb or wasbs protocol to access your data:
>
> - If your storage account allows access over HTTP, use the wasb protocol and write the file path as `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*`.
> - If your storage account allows access over HTTPS, use the wasbs protocol and write the file path as `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>/*`.

### Shared Key

#### External catalog

Configure `azure.blob.storage_account` and `azure.blob.shared_key` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.blob.storage_account`, `azure.blob.shared_key`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.blob.storage_account`, `azure.blob.shared_key`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

### SAS Token

#### External catalog

Configure `azure.blob.account_name`, `azure.blob.container_name`, and `azure.blob.sas_token` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.blob.account_name`, `azure.blob.container_name`, `azure.blob.sas_token`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.blob.account_name`, `azure.blob.container_name`, `azure.blob.sas_token`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

StarRocks supports using one of the following authentication methods to access Data Lake Storage Gen1:

- Managed Service Identity
- Service Principal

> **NOTE**
>
> When you load data or query files from Data Lake Storage Gen1, you must use the adl protocol to access your data and write the file path as `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`.

### Managed Service Identity

#### External catalog

Configure `azure.adls1.use_managed_service_identity` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://10.1.0.18:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

#### File external table

Configure `azure.adls1.use_managed_service_identity` and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.adls1.use_managed_service_identity` and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

### Service Principal

#### External catalog

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, and `azure.adls1.oauth2_endpoint` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, `azure.adls1.oauth2_endpoint`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.adls1.oauth2_client_id`, `azure.adls1.oauth2_credential`, `azure.adls1.oauth2_endpoint`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

StarRocks supports using one of the following authentication methods to access Data Lake Storage Gen2:

- Managed Identity
- Shared Key
- Service Principal

> **NOTE**
>
> When you load data or query files from Data Lake Storage Gen2, you must use the abfs or abfss protocol to access your data:
>
> - If your storage account allows access over HTTP, use the abfs protocol and write the file path as `abfs://<container>@<storage_account>.dfs.core.windows.net/<file_name>`.
> - If your storage account allows access over HTTPS, use the abfss protocol and write the file path as `abfss://<container>@<storage_account>.dfs.core.windows.net/<file_name>`.

### Managed Identity

Before you start, you need to make the following preparations:

- Edit the virtual machines (VMs) on which your StarRocks cluster is deployed.
- Add the managed identities to these VMs.
- Make sure that the managed identities are associated with the role (**Storage Blob Data Reader**) authorized to read data in your storage account.

#### External catalog

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, and `azure.adls2.oauth2_client_id` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.adls2.oauth2_use_managed_identity`, `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

### Shared Key

#### External catalog

Configure `azure.adls2.storage_account` and `azure.adls2.shared_key` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.adls2.storage_account`, `azure.adls2.shared_key`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.adls2.storage_account`, `azure.adls2.shared_key`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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

### Service Principal

Before you start, you need to create a service principal, create a role assignment to assign a role to the service principal, and then add the role assignment to your storage account. As such, you can make sure that this service principal can successfully access the data in your storage account.

#### External catalog

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, and `azure.adls2.oauth2_client_endpoint` as follows in the [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) statement:

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

#### File external table

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, `azure.adls2.oauth2_client_endpoint`, and the file path (`path`) as follows in the [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) statement:

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

Configure `azure.adls2.oauth2_client_id`, `azure.adls2.oauth2_client_secret`, `azure.adls2.oauth2_client_endpoint`, and the file path (`DATA INFILE`) as follows in the [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) statement:

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
