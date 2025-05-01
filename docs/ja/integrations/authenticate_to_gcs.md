---
description: GCS で認証する方法について説明します
displayed_sidebar: docs
---

# Google Cloud Storage への認証

## 認証方法

v3.0 以降、StarRocks は Google Cloud Storage (GCS) にアクセスするために以下の認証方法をサポートしています。

- VM ベースの認証

  Google Cloud Compute Engine にアタッチされたクレデンシャルを使用して GCS を認証します。

- サービスアカウントベースの認証

  サービスアカウントを使用して GCS を認証します。

- インパーソネーションベースの認証

  サービスアカウントまたは仮想マシン (VM) インスタンスを他のサービスアカウントにインパーソネートさせます。

## シナリオ

StarRocks は以下のシナリオで GCS に認証できます。

- GCS からデータをバッチロードする。
- GCS からデータをバックアップし、GCS にデータを復元する。
- GCS 内の Parquet および ORC ファイルをクエリする。
- GCS 内の [Hive](../data_source/catalog/hive_catalog.md), [Iceberg](../data_source/catalog/iceberg_catalog.md), [Hudi](../data_source/catalog/hudi_catalog.md), および [Delta Lake](../data_source/catalog/deltalake_catalog.md) テーブルをクエリする。

このトピックでは、[Hive catalog](../data_source/catalog/hive_catalog.md), [file external table](../data_source/file_external_table.md), および [Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を例として使用し、StarRocks が異なるシナリオで GCS と統合する方法を示します。例の `StorageCredentialParams` に関する情報は、このトピックの「[Parameters](../integrations/authenticate_to_gcs.md#parameters)」セクションを参照してください。

> **NOTE**
>
> StarRocks は、GCS からデータをロードしたり、ファイルを直接クエリしたりするのを gs プロトコルに従ってのみサポートします。したがって、GCS からデータをロードしたりファイルをクエリしたりする場合、ファイルパスに `gs` をプレフィックスとして含める必要があります。

### External catalog

GCS からファイルをクエリするために、[CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/Catalog/CREATE_EXTERNAL_CATALOG.md) ステートメントを使用して `hive_catalog_gcs` という名前の Hive catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG hive_catalog_gcs
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    StorageCredentialParams
);
```

### File external table

メタストアなしで GCS から `test_file_external_tbl` という名前のデータファイルをクエリするために、[CREATE EXTERNAL TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) ステートメントを使用して `external_table_gcs` という名前のファイル外部テーブルを作成します。

```SQL
CREATE EXTERNAL TABLE external_table_gcs
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "gs:////test-gcs/test_file_external_tbl",
    "format" = "ORC",
    StorageCredentialParams
);
```

### Broker load

GCS から StarRocks テーブル `target_table` にデータをバッチロードするために、[LOAD LABEL](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) ステートメントを使用して `test_db.label000` というラベルの Broker Load ジョブを作成します。

```SQL
LOAD LABEL test_db.label000
(
    DATA INFILE("gs://bucket_gcs/test_brokerload_ingestion/*")
    INTO TABLE target_table
    FORMAT AS "parquet"
)
WITH BROKER
(
    StorageCredentialParams
);
```

## Parameters

`StorageCredentialParams` は、異なる認証方法で GCS に認証する方法を説明するパラメータセットを表します。

### VM ベースの認証

StarRocks クラスターが Google Cloud Platform (GCP) 上でホストされている VM インスタンスにデプロイされており、その VM インスタンスを使用して GCS を認証したい場合、`StorageCredentialParams` を次のように設定します。

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true"
```

以下の表は、`StorageCredentialParams` に設定する必要があるパラメータを説明しています。

| **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
| ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
| gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |

### サービスアカウントベースの認証

サービスアカウントを直接使用して GCS を認証する場合、`StorageCredentialParams` を次のように設定します。

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
"gcp.gcs.service_account_private_key" = "<google_service_private_key>"
```

以下の表は、`StorageCredentialParams` に設定する必要があるパラメータを説明しています。

| **Parameter**                          | **Default value** | **Value** **example**                                       | **Description**                                              |
| -------------------------------------- | ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""                | "`user@hello.iam.gserviceaccount.com`"                        | サービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
| gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
| gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | サービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |

### インパーソネーションベースの認証

#### VM インスタンスをサービスアカウントにインパーソネートさせる

StarRocks クラスターが GCP 上でホストされている VM インスタンスにデプロイされており、その VM インスタンスをサービスアカウントにインパーソネートさせ、StarRocks がサービスアカウントからの権限を継承して GCS にアクセスできるようにしたい場合、`StorageCredentialParams` を次のように設定します。

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true",
"gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
```

以下の表は、`StorageCredentialParams` に設定する必要があるパラメータを説明しています。

| **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
| ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
| gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |
| gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウント。                    |

#### サービスアカウントを他のサービスアカウントにインパーソネートさせる

サービスアカウント（仮にメタサービスアカウントと呼ぶ）を他のサービスアカウント（仮にデータサービスアカウントと呼ぶ）にインパーソネートさせ、StarRocks がデータサービスアカウントからの権限を継承して GCS にアクセスできるようにしたい場合、`StorageCredentialParams` を次のように設定します。

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
"gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
"gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
```

以下の表は、`StorageCredentialParams` に設定する必要があるパラメータを説明しています。

| **Parameter**                          | **Default value** | **Value** **example**                                       | **Description**                                              |
| -------------------------------------- | ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""                | "`user@hello.iam.gserviceaccount.com`"                        | メタサービスアカウントの作成時に生成された JSON ファイル内のメールアドレス。 |
| gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー ID。 |
| gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | メタサービスアカウントの作成時に生成された JSON ファイル内のプライベートキー。 |
| gcp.gcs.impersonation_service_account  | ""                | "hello"                                                     | インパーソネートしたいデータサービスアカウント。               |