---
description: このトピックでは、Azure で認証する方法について説明します。
displayed_sidebar: docs
---

# Microsoft Azure Storage への認証

v3.0 以降、StarRocks は以下のシナリオで Microsoft Azure Storage (Azure Blob Storage または Azure Data Lake Storage) と統合できます。

- Azure Storage からのバッチデータのロード。
- Azure Storage からのデータのバックアップと、Azure Storage へのデータの復元。
- Azure Storage 内の Parquet および ORC ファイルのクエリ。
- Azure Storage 内の [Hive](../data_source/catalog/hive_catalog.md)、[Iceberg](../data_source/catalog/iceberg_catalog.md)、[Hudi](../data_source/catalog/hudi_catalog.md)、および [Delta Lake](../data_source/catalog/deltalake_catalog.md) テーブルのクエリ。

StarRocks は以下の種類の Azure Storage アカウントをサポートしています。

- Azure Blob Storage
- Azure Data Lake Storage Gen1
- Azure Data Lake Storage Gen2

このトピックでは、Hive catalog、ファイル外部テーブル、および Broker Load を例として、これらの種類の Azure Storage アカウントを使用して StarRocks が Azure Storage と統合する方法を示します。例のパラメータについては、[Hive catalog](../data_source/catalog/hive_catalog.md)、[File external table](../data_source/file_external_table.md)、および [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

## Blob Storage

StarRocks は、Blob Storage にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- Shared Key
- SAS Token

> **NOTE**
>
> Blob Storage からデータをロードしたり、ファイルを直接クエリしたりする場合、データにアクセスするために wasb または wasbs プロトコルを使用する必要があります。
>
> - ストレージ アカウントが HTTP 経由でのアクセスを許可している場合は、wasb プロトコルを使用し、ファイル パスを `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>` として記述します。
> - ストレージ アカウントが HTTPS 経由でのアクセスを許可している場合は、wasbs プロトコルを使用し、ファイル パスを `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>` として記述します。

### Shared Key

#### External catalog

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.blob.storage_account` と `azure.blob.shared_key` を次のように設定します。

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

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.blob.storage_account`、`azure.blob.shared_key`、およびファイルパス (`path`) を次のように設定します。

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

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.blob.storage_account`、`azure.blob.shared_key`、およびファイルパス (`DATA INFILE`) を次のように設定します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>")
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

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.blob.storage_account`、`azure.blob.container`、および `azure.blob.sas_token` を次のように設定します。

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

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token`、およびファイルパス (`path`) を次のように設定します。

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

#### Broker load

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token`、およびファイルパス (`DATA INFILE`) を次のように設定します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.blob.storage_account" = "<blob_storage_account_name>",
    "azure.blob.container" = "<blob_container_name>",
    "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
);
```

## Data Lake Storage Gen1

StarRocks は、Data Lake Storage Gen1 にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- Managed Service Identity
- Service Principal

> **NOTE**
>
> Data Lake Storage Gen1 からデータをロードしたり、ファイルをクエリしたりする場合、データにアクセスするために adl プロトコルを使用し、ファイルパスを `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>` として記述する必要があります。

### Managed Service Identity

#### External catalog

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.adls1.use_managed_service_identity` を次のように設定します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.adls1.use_managed_service_identity` およびファイルパス (`path`) を次のように設定します。

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

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.adls1.use_managed_service_identity` およびファイルパス (`DATA INFILE`) を次のように設定します。

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

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、および `azure.adls1.oauth2_endpoint` を次のように設定します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.oauth2_client_id" = "<application_client_id>",
    "azure.adls1.oauth2_credential" = "<application_client_credential>",
    "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
);
```

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint`、およびファイルパス (`path`) を次のように設定します。

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

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint`、およびファイルパス (`DATA INFILE`) を次のように設定します。

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

StarRocks は、Data Lake Storage Gen2 にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- Managed Identity
- Shared Key
- Service Principal

> **NOTE**
>
> Data Lake Storage Gen2 からデータをロードしたり、ファイルをクエリしたりする場合、データにアクセスするために abfs または abfss プロトコルを使用する必要があります。
>
> - ストレージ アカウントが HTTP 経由でのアクセスを許可している場合は、abfs プロトコルを使用し、ファイル パスを `abfs://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>` として記述します。
> - ストレージ アカウントが HTTPS 経由でのアクセスを許可している場合は、abfss プロトコルを使用し、ファイル パスを `abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>` として記述します。

### Managed Identity

開始する前に、次の準備を行う必要があります。

- StarRocks クラスターがデプロイされている仮想マシン (VM) を編集します。
- これらの VM にマネージド ID を追加します。
- マネージド ID がストレージ アカウント内のデータを読み取る権限を持つ役割 (**Storage Blob Data Reader**) に関連付けられていることを確認します。

#### External catalog

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、および `azure.adls2.oauth2_client_id` を次のように設定します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.oauth2_use_managed_identity" = "true",
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`path`) を次のように設定します。

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
    "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
    "azure.adls2.oauth2_client_id" = "<service_client_id>"
);
```

#### Broker Load

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`DATA INFILE`) を次のように設定します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>")
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

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.adls2.storage_account` および `azure.adls2.shared_key` を次のように設定します。

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

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.adls2.storage_account`、`azure.adls2.shared_key`、およびファイルパス (`path`) を次のように設定します。

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

#### Broker Load

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.adls2.storage_account`、`azure.adls2.shared_key`、およびファイルパス (`DATA INFILE`) を次のように設定します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>")
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

開始する前に、サービス プリンシパルを作成し、サービス プリンシパルに役割を割り当てる役割の割り当てを作成し、次にその役割の割り当てをストレージ アカウントに追加する必要があります。これにより、このサービス プリンシパルがストレージ アカウント内のデータに正常にアクセスできることを確認できます。

#### External catalog

[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、および `azure.adls2.oauth2_client_endpoint` を次のように設定します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls2.oauth2_client_id" = "<service_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

#### File external table

[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint`、およびファイルパス (`path`) を次のように設定します。

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
    "azure.adls2.oauth2_client_id" = "<service_client_id>",
    "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
    "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
);
```

#### Broker Load

[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで `azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint`、およびファイルパス (`DATA INFILE`) を次のように設定します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("adfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>")
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