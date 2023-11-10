# 配置 GCS 认证信息

## 认证方式介绍

StarRocks 从 3.0 版本起支持通过以下认证方式访问 Google Cloud Storage（简称 GCS）：

- 基于 VM 认证鉴权

  使用 Google Cloud Compute Engine 上绑定的凭证对 GCS 进行认证和鉴权。

- 基于 Service Account 认证鉴权

  使用 Service Account 对 GCS 进行认证和鉴权。

- 基于 Impersonation 认证鉴权

  使用一个 Service Account 或 VM 实例模拟另一个 Service Account，从实现对 GCS 的认证和鉴权。

## 应用场景

StarRocks 支持在以下场景中集成 GCS：

- 从 GCS 批量导入数据。
- 从 GCS 备份数据、或把数据恢复到 GCS。
- 查询 GCS 中的 Parquet 或 ORC 格式的数据文件。
- 查询 GCS 中的 [Hive](../data_source/catalog/hive_catalog.md)、[Iceberg](../data_source/catalog/iceberg_catalog.md)、[Hudi](../data_source/catalog/hudi_catalog.md)、或 [Delta Lake](../data_source/catalog/deltalake_catalog.md) 表。

本文档以 [Hive catalog](../data_source/catalog/hive_catalog.md)、[文件外部表](../data_source/file_external_table.md) 和 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 为例，介绍 StarRocks 在各应用场景下如何集成 GCS。有关下面示例中出现的 `StorageCredentialParams` 详解，参见本文档“[参数配置](../integrations/authenticate_to_gcs.md#参数配置)”小节。

> **说明**
>
> 由于 Broker Load 只支持通过 gs 协议访问 Google GCS，因此当从 Google GCS 导入数据时，必须确保文件路径中传入的目标文件的 GCS URI 使用 `gs://` 作为前缀。

### External Catalog

通过 [CREATE EXTERNAL CATALOG](../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md) 语句创建一个 Hive Catalog `hive_catalog_gcs`，用以查询 GCS 中的数据文件：

```SQL
CREATE EXTERNAL CATALOG hive_catalog_gcs
PROPERTIES
(
    "type" = "hive", 
    "hive.metastore.uris" = "thrift://34.132.15.127:9083",
    StorageCredentialParams
);
```

### 文件外部表

通过 [CREATE EXTERNAL TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 语句创建一个文件外部表 `external_table_gcs`，用以查询 GCS 中的数据文件 `test_file_external_tbl`（GCS 未配置元数据服务）：

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

通过 [LOAD LABEL](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 语句创建一个 Broker Load 作业，作业标签为 `test_db.label000`，用以把 GCS 中的数据批量导入 StarRocks 表 `target_table`：

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

## 参数配置

`StorageCredentialParams` 用于指定 StarRocks 访问 GCS 的相关参数配置。具体包含哪些参数，需要根据所使用的认证方式来确定。

### 基于 VM 认证鉴权

如果您将 StarRocks 部署在 Google Cloud Platform（简称 GCP）上的 VM 实例的同时，基于 VM 对 GCS 进行认证和鉴权，请按如下配置 `StorageCredentialParams`：

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true"
```

`StorageCredentialParams` 包含如下参数。

| **参数**                                   | **默认值** | **取值样例** | **说明**                                                 |
| ------------------------------------------ | ---------- | ------------ | -------------------------------------------------------- |
| gcp.gcs.use_compute_engine_service_account | false      | true         | 是否直接使用 Compute Engine 上面绑定的 Service Account。 |

### 基于 Service Account 认证鉴权

如果您使用某个 Service Account 对 GCS 进行认证和鉴权，请按如下配置 `StorageCredentialParams`：

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
"gcp.gcs.service_account_private_key" = "<google_service_private_key>"
```

`StorageCredentialParams` 包含如下参数。

| **参数**                               | **默认值** | **取值样例**                                                | **说明**                                                     |
| -------------------------------------- | ---------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""         | "`user@hello.iam.gserviceaccount.com`"                        | 创建 Service Account 时生成的 JSON 文件中的 Email。          |
| gcp.gcs.service_account_private_key_id | ""         | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | 创建 Service Account 时生成的 JSON 文件中的 Private Key ID。 |
| gcp.gcs.service_account_private_key    | ""         | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | 创建 Service Account 时生成的 JSON 文件中的 Private Key。    |

### 基于 Impersonation 认证鉴权

#### 使用一个 VM 实例模拟一个 Service Account

如果您将 StarRocks 部署在 GCP 上的 VM 实例的同时，使用该 VM 实例模拟一个 Service Account，从而使 StarRocks 继承该 Service Account 访问 GCS 的权限，请按如下配置 `StorageCredentialParams`：

```Plain
"gcp.gcs.use_compute_engine_service_account" = "true",
"gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
```

`StorageCredentialParams` 包含如下参数。

| **参数**                                   | **默认值** | **取值样例** | **说明**                                                 |
| ------------------------------------------ | ---------- | ------------ | -------------------------------------------------------- |
| gcp.gcs.use_compute_engine_service_account | false      | true         | 是否直接使用 Compute Engine 上面绑定的 Service Account。 |
| gcp.gcs.impersonation_service_account      | ""         | "hello"      | 需要模拟的目标 Service Account。                         |

#### 使用一个 Service Account 模拟另外一个 Service Account

如果您使用一个 Service Account（暂时命名为“Meta Service Account”）模拟另外一个 Service Account（暂时命名为“Data Service Account”），从而使 StarRocks 继承该 Data Service Account 访问 GCS 的权限，请按如下配置 `StorageCredentialParams`：

```Plain
"gcp.gcs.service_account_email" = "<google_service_account_email>",
"gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
"gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
"gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
```

`StorageCredentialParams` 包含如下参数。

| **参数**                               | **默认值** | **取值样例**                                                | **说明**                                                     |
| -------------------------------------- | ---------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| gcp.gcs.service_account_email          | ""         | "`user@hello.iam.gserviceaccount.com`"                        | 创建 Meta Service Account 时生成的 JSON 文件中的 Email。     |
| gcp.gcs.service_account_private_key_id | ""         | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                  | 创建 Meta Service Account 时生成的 JSON 文件中的 Private Key ID。 |
| gcp.gcs.service_account_private_key    | ""         | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | 创建 Meta Service Account 时生成的 JSON 文件中的 Private Key。 |
| gcp.gcs.impersonation_service_account  | ""         | "hello"                                                     | 需要模拟的目标 Data Service Account。                        |
