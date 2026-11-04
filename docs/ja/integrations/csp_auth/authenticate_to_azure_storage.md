---
sidebar_position: 20
description: Azure Blob Storage および Azure Data Lake Storage で認証するように StarRocks を設定する方法について説明します。
displayed_sidebar: docs
---

# Microsoft Azure Storage への認証

バージョン 3.0 以降、StarRocks は以下のシナリオで Microsoft Azure Storage (Azure Blob Storage または Azure Data Lake Storage) と統合できます。

- Azure Storage 内の [Hive](../../data_source/catalog/hive_catalog.md)、[Iceberg](../../data_source/catalog/iceberg/iceberg.md)、[Hudi](../../data_source/catalog/hudi_catalog.md)、および [Delta Lake](../../data_source/catalog/deltalake_catalog.md) テーブルのクエリ。
- Azure Storage からのバッチデータのロード。
- Azure Storage からのデータのバックアップと Azure Storage へのデータの復元。
- Azure Storage 内の Parquet および ORC ファイルのクエリ。

次の表は、Azure Storage の種類ごとに StarRocks がサポートする認証方法を示しています。

| **Azure Storage の種類**                 | **認証方法**                                                 |
| ---------------------------------------- | ------------------------------------------------------------ |
| Azure Blob Storage                       | <ul><li>共有キー (ストレージアカウントのアクセスキー)</li><li>SAS トークン</li></ul> |
| Azure Data Lake Storage Gen2 (ADLS Gen2) | <ul><li>マネージド ID</li><li>共有キー (ストレージアカウントのアクセスキー)</li><li>サービスプリンシパル</li></ul> |
| Azure Data Lake Storage Gen1 (ADLS Gen1)、廃止済み | <ul><li>マネージドサービス ID</li><li>サービスプリンシパル</li></ul> |

v3.5.10 以降、ADLS Gen2 は [ワークロード ID](#ワークロード-id) もサポートしています。

マネージド ID 認証方法では、クラスターマネージド ID を使用します。このトピックでは、クラスターマネージド ID とは、StarRocks クラスターがデプロイされている仮想マシン (VM) にアタッチされたユーザー割り当てマネージド ID を指します。

External catalog の場合は、外部データソースのデータを格納しているストレージアカウントの認証情報を使用します。

このトピックでは、Hive catalog、ファイル外部テーブル、および Broker Load を例として使用します。Broker Load の例では、データファイル `file1.parquet` をデータベース `mydatabase` のテーブル `table1` にロードします。`file1.parquet` と `table1` のスキーマは同じです。例のパラメータについては、[Hive catalog](../../data_source/catalog/hive_catalog.md)、[File external table](../../data_source/file_external_table.md)、および [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

## Blob Storage

StarRocks は、Blob Storage にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- 共有キー
- SAS トークン

> **注意**
>
> Blob Storage からデータをロードしたり、ファイルを直接クエリしたりする場合、データにアクセスするために wasb または wasbs プロトコルを使用する必要があります。
>
> - ストレージアカウントが HTTP 経由でのアクセスを許可する場合、wasb プロトコルを使用し、ファイルパスを `wasb://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>` として記述します。
> - ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、wasbs プロトコルを使用し、ファイルパスを `wasbs://<container>@<storage_account>.blob.core.windows.net/<path>/<file_name>` として記述します。

### 共有キー (ストレージアカウントのアクセスキー)

アクセスキーは、StarRocks クラスターがストレージアカウントに送信するリクエストを認証します。アクセスキーの詳細については、[Manage account access keys - Azure Storage](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) を参照してください。

#### パラメータ

| **パラメータ**             | **説明**                                                  |
| -------------------------- | --------------------------------------------------------- |
| azure.blob.storage_account | Blob Storage のストレージアカウントの名前。               |
| azure.blob.shared_key      | Blob Storage のストレージアカウントの共有キー (アクセスキー)。 |

Blob Storage のストレージアカウントの共有キーを取得するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** ページに移動します。
2. ストレージアカウントを見つけて、その名前をクリックします。
3. 左側のナビゲーションペインで、**Access keys** を選択します。
4. 使用するキーの横にある **Show** をクリックし、コピーアイコンをクリックしてキーをコピーします。

#### 例

##### External catalog

`azure.blob.storage_account` と `azure.blob.shared_key` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.blob.storage_account`、`azure.blob.shared_key`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.blob.storage_account`、`azure.blob.shared_key`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

### SAS トークン

共有アクセス署名 (SAS) は、ストレージアカウントへの制限付きアクセスを許可するトークンです。SAS トークンの詳細については、[Delegate access by using a shared access signature - Azure Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature) を参照してください。

#### パラメータ

| **パラメータ**             | **説明**                                                     |
| -------------------------- | ------------------------------------------------------------ |
| azure.blob.storage_account | Blob Storage のストレージアカウントの名前。                  |
| azure.blob.container       | Blob Storage のストレージアカウント内でデータを格納している Blob コンテナの名前。 |
| azure.blob.sas_token       | Blob Storage のストレージアカウントへのアクセスに使用する SAS トークン。 |

Blob Storage のストレージアカウントの SAS トークンを取得するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** ページに移動します。
2. ストレージアカウントを見つけて、その名前をクリックします。
3. 左側のナビゲーションペインで、**Shared access signature** を選択します。
4. **Allowed services**、**Allowed resource types**、および **Allowed permissions** セクションの設定を確認します。次に、**Signing key** ドロップダウンリストからキーを選択し、**Generate SAS and connection string** をクリックします。
5. コピーアイコンをクリックして、生成された SAS トークンをコピーします。

#### 例

##### External catalog

`azure.blob.storage_account`、`azure.blob.container`、および `azure.blob.sas_token` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.blob.storage_account`、`azure.blob.container`、`azure.blob.sas_token`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

StarRocks は、Data Lake Storage Gen2 にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- マネージド ID
- 共有キー
- サービスプリンシパル
- ワークロード ID (v3.5.10 以降)

> **注意**
>
> Data Lake Storage Gen2 からデータをロードしたり、ファイルをクエリしたりする場合、データにアクセスするために abfs または abfss プロトコルを使用する必要があります。
>
> - ストレージアカウントが HTTP 経由でのアクセスを許可する場合、abfs プロトコルを使用し、ファイルパスを `abfs://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>` として記述します。
> - ストレージアカウントが HTTPS 経由でのアクセスを許可する場合、abfss プロトコルを使用し、ファイルパスを `abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/<file_name>` として記述します。

### マネージド ID

ユーザー割り当てマネージド ID を使用すると、VM などの Azure リソースは、認証情報を保存することなく他の Azure サービスに対して認証できます。ユーザー割り当てマネージド ID は、スタンドアロンの Azure リソースです。1 台の VM で複数のユーザー割り当てマネージド ID を使用でき、1 つのユーザー割り当てマネージド ID を複数の VM で共有できます。

この方法では、StarRocks はクラスターマネージド ID を使用してストレージアカウントにアクセスします。

#### 準備

この認証方法を使用する前に、クラスターマネージド ID が存在し、ストレージアカウントに対して必要な権限を持っていることを確認してください。

まず、StarRocks クラスターがデプロイされている VM にユーザー割り当てマネージド ID がアタッチされていることを確認し、その名前を控えておきます。アタッチされていない場合は、ユーザー割り当てマネージド ID を作成してこれらの VM に追加します。詳細については、[Configure managed identities on Azure virtual machines](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-configure-managed-identities) を参照してください。

次に、クラスターマネージド ID がストレージアカウントに対して必要な権限を持っていることを確認します。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** ページに移動します。
2. ストレージアカウントを見つけて、その名前をクリックします。
3. 左側のナビゲーションペインで、**Access control (IAM)** を選択します。
4. **Role assignments** タブで、既存のロールの割り当てを確認します。
5. クラスターマネージド ID がストレージアカウントに対して必要な権限を持っていない場合は、**Add** をクリックして **Add role assignment** を選択します。次に、ロールの割り当てを追加します。

   a. **Job function roles** タブでロールを選択し、**Next** をクリックします。たとえば、**Storage Blob Data Reader** は読み取りアクセスを付与し、**Storage Blob Data Contributor** は読み取りと書き込みのアクセスを付与します。

   b. **Members** タブで、**Assign access to** に **Managed identity** を選択します。

   c. **Members** で **Select members** をクリックします。表示されたペインでサブスクリプションを選択し、**Managed identity** ドロップダウンリストから **User-assigned managed identity** を選択し、クラスターマネージド ID を選択して、**Select** をクリックします。

   d. **Review + assign** をクリックします。情報を確認し、もう一度 **Review + assign** をクリックしてロールの割り当てを追加します。

#### パラメータ

| **パラメータ**                          | **説明**                                                     |
| --------------------------------------- | ------------------------------------------------------------ |
| azure.adls2.oauth2_use_managed_identity | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
| azure.adls2.oauth2_tenant_id            | ADLS Gen2 ストレージアカウントのテナントの ID。              |
| azure.adls2.oauth2_client_id            | クラスターマネージド ID のクライアント ID。                  |

テナント ID を取得するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Tenant properties](https://portal.azure.com/#view/Microsoft_AAD_IAM/TenantProperties.ReactView)** ページに移動します。
2. **Tenant ID** を見つけ、コピーアイコンをクリックしてコピーします。

クラスターマネージド ID のクライアント ID を取得するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Managed Identities](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.ManagedIdentity%2FuserAssignedIdentities)** ページに移動します。
2. クラスターマネージド ID を見つけて、その名前をクリックします。
3. **Overview** ページで **Client ID** を見つけ、コピーアイコンをクリックしてコピーします。

#### 例

##### External catalog

`azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、および `azure.adls2.oauth2_client_id` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls2.oauth2_use_managed_identity`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

### 共有キー (ストレージアカウントのアクセスキー)

共有キーを使用して ADLS Gen2 にアクセスする方法は、共有キーを使用して Blob Storage にアクセスする方法と同じです。共有キーの取得方法については、[Blob Storage > 共有キー](#共有キー-ストレージアカウントのアクセスキー) を参照してください。

#### パラメータ

| **パラメータ**              | **説明**                                                     |
| --------------------------- | ------------------------------------------------------------ |
| azure.adls2.storage_account | ADLS Gen2 ストレージアカウントの名前。                       |
| azure.adls2.shared_key      | ADLS Gen2 ストレージアカウントの共有キー (アクセスキー)。    |

#### 例

##### External catalog

`azure.adls2.storage_account` と `azure.adls2.shared_key` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.adls2.storage_account`、`azure.adls2.shared_key`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls2.storage_account`、`azure.adls2.shared_key`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

### サービスプリンシパル

#### 準備

この認証方法を使用する前に、サービスプリンシパルがあり、そのサービスプリンシパルがストレージアカウントに対して必要な権限を持っていることを確認してください。サービスプリンシパルとクライアントシークレットの作成方法については、[Register a Microsoft Entra app and create a service principal](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal) を参照してください。

サービスプリンシパルの権限を確認するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[Storage accounts](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Storage%2FStorageAccounts)** ページに移動します。
2. ADLS Gen2 ストレージアカウントを見つけて、その名前をクリックします。
3. 左側のナビゲーションペインで、**Access control (IAM)** を選択します。
4. **Role assignments** タブで、既存のロールの割り当てを確認します。
5. サービスプリンシパルがストレージアカウントに対して必要な権限を持っていない場合は、**Add** をクリックして **Add role assignment** を選択します。次に、ロールの割り当てを追加します。

   a. **Job function roles** タブでロールを選択し、**Next** をクリックします。たとえば、**Storage Blob Data Reader** は読み取りアクセスを付与し、**Storage Blob Data Contributor** は読み取りと書き込みのアクセスを付与します。

   b. **Members** タブで、**Assign access to** に **User, group, or service principal** を選択します。

   c. **Members** で **Select members** をクリックします。表示されたペインで、登録したアプリケーションを検索して選択し、**Select** をクリックします。

   d. **Review + assign** をクリックします。情報を確認し、もう一度 **Review + assign** をクリックしてロールの割り当てを追加します。

#### パラメータ

| **パラメータ**                     | **説明**                                                    |
| ---------------------------------- | ----------------------------------------------------------- |
| azure.adls2.oauth2_client_id       | サービスプリンシパルのアプリケーション (クライアント) ID。  |
| azure.adls2.oauth2_client_secret   | サービスプリンシパルのクライアントシークレットの値。        |
| azure.adls2.oauth2_client_endpoint | サービスプリンシパルの OAuth 2.0 トークンエンドポイント (v1)。 |

> **注意**
>
> Azure は、クライアントシークレットの値を作成直後に一度だけ表示します。ページを離れる前に値を保存してください。

サービスプリンシパルのアプリケーション (クライアント) ID と OAuth 2.0 トークンエンドポイント (v1) を取得するには、次の手順に従います。

1. [Azure portal](https://portal.azure.com/#home) にサインインし、**[App registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)** ページに移動します。
2. 登録したアプリケーションを見つけて、その名前をクリックします。
3. **Overview** ページで **Application (client) ID** を見つけ、コピーアイコンをクリックしてコピーします。
4. **Overview** ページの上部にある **Endpoints** をクリックします。
5. **Endpoints** ペインで **OAuth 2.0 token endpoint (v1)** を見つけ、コピーアイコンをクリックしてコピーします。

#### 例

##### External catalog

`azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、および `azure.adls2.oauth2_client_endpoint` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls2.oauth2_client_id`、`azure.adls2.oauth2_client_secret`、`azure.adls2.oauth2_client_endpoint`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

### ワークロード ID

v3.5.10 以降、StarRocks は Azure Data Lake Storage Gen2 へのアクセスにおける認証方法として Azure ワークロード ID (Workload Identity) をサポートしています。この認証方法は、Azure 内で実行されるワークロード（AKS ポッドなど）向けに設計されており、プロジェクションされたトークンファイルを介してコンピューティング ID を Azure AD アプリケーションとフェデレーションすることで、長期有効な認証情報の保存を不要にします。

ワークロード ID 認証を使用するには、続行する前に以下の Azure 側の設定が必要です。

- AKS クラスター（または同等のもの）で Azure ワークロード ID ウェブフックを有効にします。
- Kubernetes サービスアカウントを Azure AD アプリケーションまたはユーザー割り当てマネージド ID にリンクするフェデレーション ID 資格情報を作成します。
- Webhook がトークンプロジェクションを注入できるように、Kubernetes ポッド/サービスアカウントにアノテーションを付与します。トークンファイルは、`azure.adls2.oauth2_token_file` で設定されたパス（通常は `/var/run/secrets/azure/tokens/azure-identity-token`）に生成されます。
- ストレージアカウントに対して、Azure AD ID に必要な RBAC ロール（例: Storage Blob Data Reader または Storage Blob Data Contributor）を付与します。

#### External catalog

`azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id`、および `azure.adls2.oauth2_client_id` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

#### ファイル外部テーブル

`azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls2.oauth2_token_file`、`azure.adls2.oauth2_tenant_id`、`azure.adls2.oauth2_client_id`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

> **注意**
>
> Microsoft は 2024 年 2 月 29 日に Azure Data Lake Storage Gen1 を廃止しました。このトピックの Gen1 に関するセクションは、参考のために残しています。

StarRocks は、Data Lake Storage Gen1 にアクセスするために次の認証方法のいずれかを使用することをサポートしています。

- マネージドサービス ID
- サービスプリンシパル

> **注意**
>
> Data Lake Storage Gen1 からデータをロードしたり、ファイルをクエリしたりする場合、データにアクセスするために adl プロトコルを使用し、ファイルパスを `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>` として記述する必要があります。

### マネージドサービス ID

#### パラメータ

| **パラメータ**                           | **説明**                                                     |
| ---------------------------------------- | ------------------------------------------------------------ |
| azure.adls1.use_managed_service_identity | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

#### 例

##### External catalog

`azure.adls1.use_managed_service_identity` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_azure
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "azure.adls1.use_managed_service_identity" = "true"
);
```

##### ファイル外部テーブル

`azure.adls1.use_managed_service_identity` およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls1.use_managed_service_identity` およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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

### サービスプリンシパル

サービスプリンシパルを作成し、そのアプリケーション (クライアント) ID、クライアントシークレット、および OAuth 2.0 トークンエンドポイント (v1) を取得する方法については、[Data Lake Storage Gen2 > サービスプリンシパル](#サービスプリンシパル) を参照してください。

#### パラメータ

| **パラメータ**                | **説明**                                                     |
| ----------------------------- | ------------------------------------------------------------ |
| azure.adls1.oauth2_client_id  | サービスプリンシパルのアプリケーション (クライアント) ID。   |
| azure.adls1.oauth2_credential | サービスプリンシパルのクライアントシークレットの値。         |
| azure.adls1.oauth2_endpoint   | サービスプリンシパルの OAuth 2.0 トークンエンドポイント (v1)。 |

#### 例

##### External catalog

`azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、および `azure.adls1.oauth2_endpoint` を [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントで次のように設定します。

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

##### ファイル外部テーブル

`azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint`、およびファイルパス (`path`) を [CREATE EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントで次のように設定します。

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

`azure.adls1.oauth2_client_id`、`azure.adls1.oauth2_credential`、`azure.adls1.oauth2_endpoint`、およびファイルパス (`DATA INFILE`) を [LOAD LABEL](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントで次のように設定します。

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
