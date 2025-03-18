---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# Unified catalog

Unified Catalog 是一种 External Catalog，自 3.2 版本起支持。通过 Unified Catalog，您可以把 Apache Hive™、Apache Iceberg、Apache Hudi、 Delta Lake 和 Apache Kudu 数据源作为一个融合的数据源，不需要执行导入就可以直接操作其中的表数据，包括：

- 无需手动建表，通过 Unified Catalog 直接查询 Hive、Iceberg、Hudi、Delta Lake 和 Kudu 数据源里的数据。
- 通过 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 或异步物化视图（2.5 版本及以上）将 Hive、Iceberg、Hudi、Delta Lake 和 Kudu 数据源里的数据进行加工建模，并导入至 StarRocks。
- 在 StarRocks 侧创建或删除 Hive、Iceberg 库表。

为保证正常访问融合数据源内的数据，StarRocks 集群必须能够访问融合数据源的存储系统和元数据服务。目前 StarRocks 支持以下存储系统和元数据服务：

- 分布式文件系统 (HDFS) 或对象存储。当前支持的对象存储包括：AWS S3、Microsoft Azure Storage、Google GCS、其他兼容 S3 协议的对象存储（如阿里云 OSS、MinIO）。

- 元数据服务。当前支持的元数据服务包括：Hive Metastore（以下简称 HMS）、AWS Glue。

  > **说明**
  >
  > 如果选择 AWS S3 作为存储系统，您可以选择 HMS 或 AWS Glue 作为元数据服务。如果选择其他存储系统，则只能选择 HMS 作为元数据服务。

## 使用限制

一个 Unified Catalog 当前只支持对接一个存储系统和一个元数据服务。因此，您需要确保您通过 Unified Catalog 访问的所有数据源使用同一个存储系统和同一个元数据服务。

## 使用说明

- 有关 Unified Catalog 支持的文件格式和数据类型，请参见 [Hive catalog](../catalog/hive_catalog.md)、[Iceberg catalog](./iceberg/iceberg_catalog.md)、[Hudi catalog](../catalog/hudi_catalog.md)、[Delta Lake catalog](../catalog/deltalake_catalog.md) 和 [Kudu catalog](../catalog/kudu_catalog.md) 文档中“使用说明”部分。

- 部分操作只能用于特定的表格式。例如，[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 和 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 当前只支持 Hive 和 Iceberg 表，[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) 只支持 Hive 和 Hudi 表。

  当您通过 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 在 Unified Catalog 中创建表时，必须通过 `ENGINE` 参数来指定表格式（Hive 或 Iceberg）。

## 准备工作

在创建 Unified Catalog 之前，请确保 StarRocks 集群能够正常访问您所使用的文件存储及元数据服务。

### AWS IAM

如果您使用 AWS S3 作为文件存储或使用 AWS Glue 作为元数据服务，您需要选择一种合适的认证鉴权方案，确保 StarRocks 集群可以访问相关的 AWS 云资源。有关 StarRocks 访问 AWS 认证鉴权的详细内容，参见[配置 AWS 认证方式 - 准备工作](../../integrations/authenticate_to_aws_resources.md#准备工作)。

### HDFS

如果使用 HDFS 作为文件存储，则需要在 StarRocks 集群中做如下配置：

- （可选）设置用于访问 HDFS 集群和 HMS 的用户名。 您可以在每个 FE 的 **fe/conf/hadoop_env.sh** 文件、以及每个 BE **be/conf/hadoop_env.sh** 文件（或每个 CN 的 **cn/conf/hadoop_env.sh** 文件）最开头增加 `export HADOOP_USER_NAME="<user_name>"` 来设置该用户名。配置完成后，需重启各个 FE 和 BE（或 CN）使配置生效。如果不设置该用户名，则默认使用 FE 和 BE（或 CN）进程的用户名进行访问。每个 StarRocks 集群仅支持配置一个用户名。
- 查询数据时，StarRocks 集群的 FE 和 BE（或 CN）会通过 HDFS 客户端访问 HDFS 集群。一般情况下，StarRocks 会按照默认配置来启动 HDFS 客户端，无需手动配置。但在以下场景中，需要进行手动配置：
  - 如果 HDFS 集群开启了高可用（High Availability，简称为“HA”）模式，则需要将 HDFS 集群中的 **hdfs-site.xml** 文件放到每个 FE 的 **$FE_HOME/conf** 路径下、以及每个 BE 的 **$BE_HOME/conf** 路径（或每个 CN 的 **$CN_HOME/conf** 路径）下。
  - 如果 HDFS 集群配置了 ViewFs，则需要将 HDFS 集群中的 **core-site.xml** 文件放到每个 FE 的 **$FE_HOME/conf** 路径下、以及每个 BE 的 **$BE_HOME/conf** 路径（或每个 CN 的 **$CN_HOME/conf** 路径）下。

> **注意**
>
> 如果查询时因为域名无法识别 (Unknown Host) 而发生访问失败，您需要将 HDFS 集群中各节点的主机名及 IP 地址之间的映射关系配置到 **/etc/hosts** 路径中。

### Kerberos 认证

如果 HDFS 集群或 HMS 开启了 Kerberos 认证，则需要在 StarRocks 集群中做如下配置：

- 在每个 FE 和 每个 BE（或 CN）上执行 `kinit -kt keytab_path principal` 命令，从 Key Distribution Center (KDC) 获取到 Ticket Granting Ticket (TGT)。执行命令的用户必须拥有访问 HMS 和 HDFS 的权限。注意，使用该命令访问 KDC 具有时效性，因此需要使用 cron 定期执行该命令。
- 在每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 的 **$BE_HOME/conf/be.conf** 文件（或每个 CN 的 **$CN_HOME/conf/cn.conf** 文件）中添加 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"`。其中，`/etc/krb5.conf` 是 krb5.conf 文件的路径，可以根据文件的实际路径进行修改。

## 创建 Unified Catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "unified",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams,
    KuduCatalogParams
)
```

### 参数说明

#### catalog_name

Unified Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### comment

Unified  Catalog 的描述。此参数为可选。

#### type

数据源的类型。设置为 `unified` 。

#### MetastoreParams

StarRocks 访问元数据服务的相关参数配置。

##### Hive metastore

如果选择 HMS 作为元数据服务，请按如下配置 `MetastoreParams`：

```SQL
"unified.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **说明**
>
> 在查询数据之前，必须将所有 HMS 节点的主机名及 IP 地址之间的映射关系添加到 **/etc/hosts** 路径。否则，发起查询时，StarRocks 可能无法访问 HMS。

`MetastoreParams` 包含如下参数。

| 参数                   | 是否必须 | 说明                                                         |
| ---------------------- | -------- | ------------------------------------------------------------ |
| unified.metastore.type | 是       | 元数据服务的类型。设置为 `hive`。                            |
| hive.metastore.uris    | 是       | HMS 的 URI。格式：`thrift://<HMS IP 地址>:<HMS 端口号>`。 如果您的 HMS 开启了高可用模式，此处可以填写多个 HMS 地址并用逗号 (`,`) 分隔，例如：`"thrift://<HMS IP 地址 1>:<HMS 端口号 1>,thrift://<HMS IP 地址 2>:<HMS 端口号 2>,thrift://<HMS IP 地址 3>:<HMS 端口号 3>"`。 |

##### AWS Glue

如果选择 AWS Glue 作为元数据服务（只有使用 AWS S3 作为存储系统时支持），请按如下配置 `MetastoreParams`：

- 基于 Instance Profile 进行认证和鉴权

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 基于 Assumed Role 进行认证和鉴权

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 基于 IAM User 进行认证和鉴权

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

`MetastoreParams` 包含如下参数。

| 参数                          | 是否必须 | 说明                                                         |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| unified.metastore.type        | 是       | 元数据服务的类型。设置为 `glue`。                            |
| aws.glue.use_instance_profile | 是       | 指定是否开启 Instance Profile 和 Assumed Role 两种鉴权方式。取值范围：`true` 和 `false`。默认值：`false`。 |
| aws.glue.iam_role_arn         | 否       | 有权限访问 AWS Glue Data Catalog 的 IAM Role 的 ARN。采用 Assumed Role 鉴权方式访问 AWS Glue 时，必须指定此参数。 |
| aws.glue.region               | 是       | AWS Glue Data Catalog 所在的地域。示例：`us-west-1`。        |
| aws.glue.access_key           | 否       | IAM User 的 Access Key。采用 IAM User 鉴权方式访问 AWS Glue 时，必须指定此参数。 |
| aws.glue.secret_key           | 否       | IAM User 的 Secret Key。采用 IAM User 鉴权方式访问 AWS Glue 时，必须指定此参数。 |

有关如何选择用于访问 AWS Glue 的鉴权方式、以及如何在 AWS IAM 控制台配置访问控制策略，参见[访问 AWS Glue 的认证参数](../../integrations/authenticate_to_aws_resources.md#访问-aws-glue-的认证参数)。

#### StorageCredentialParams

StarRocks 访问文件存储的相关参数配置。

如果您使用 HDFS 作为存储系统，则不需要配置 `StorageCredentialParams`。

如果您使用 AWS S3、阿里云 OSS、其他兼容 S3 协议的对象存储、Microsoft Azure Storage、 或 GCS，则必须配置 `StorageCredentialParams`。

##### AWS S3

如果选择 AWS S3 作为文件存储，请按如下配置 `StorageCredentialParams`：

- 基于 Instance Profile 进行认证和鉴权

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 基于 Assumed Role 进行认证和鉴权

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 基于 IAM User 进行认证和鉴权

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

`StorageCredentialParams` 包含如下参数。

| 参数                        | 是否必须   | 说明                                                         |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | 是       | 指定是否开启 Instance Profile 和 Assumed Role 两种鉴权方式。取值范围：`true` 和 `false`。默认值：`false`。 |
| aws.s3.iam_role_arn         | 否       | 有权限访问 AWS S3 Bucket 的 IAM Role 的 ARN。采用 Assumed Role 鉴权方式访问 AWS S3 时，必须指定此参数。 |
| aws.s3.region               | 是       | AWS S3 Bucket 所在的地域。示例：`us-west-1`。                |
| aws.s3.access_key           | 否       | IAM User 的 Access Key。采用 IAM User 鉴权方式访问 AWS S3 时，必须指定此参数。 |
| aws.s3.secret_key           | 否       | IAM User 的 Secret Key。采用 IAM User 鉴权方式访问 AWS S3 时，必须指定此参数。 |

有关如何选择用于访问 AWS S3 的鉴权方式、以及如何在 AWS IAM 控制台配置访问控制策略，参见[访问 AWS S3 的认证参数](../../integrations/authenticate_to_aws_resources.md#访问-aws-s3-的认证参数)。

##### 阿里云 OSS

如果选择阿里云 OSS 作为文件存储，需要在 `StorageCredentialParams` 中配置如下认证参数：

```SQL
"aliyun.oss.access_key" = "<user_access_key>",
"aliyun.oss.secret_key" = "<user_secret_key>",
"aliyun.oss.endpoint" = "<oss_endpoint>" 
```

| 参数                            | 是否必须 | 说明                                                         |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aliyun.oss.endpoint             | 是      | 阿里云 OSS Endpoint, 如 `oss-cn-beijing.aliyuncs.com`，您可根据 Endpoint 与地域的对应关系进行查找，请参见 [访问域名和数据中心](https://help.aliyun.com/document_detail/31837.html)。    |
| aliyun.oss.access_key           | 是      | 指定阿里云账号或 RAM 用户的 AccessKey ID，获取方式，请参见 [获取 AccessKey](https://help.aliyun.com/document_detail/53045.html)。                                     |
| aliyun.oss.secret_key           | 是      | 指定阿里云账号或 RAM 用户的 AccessKey Secret，获取方式，请参见 [获取 AccessKey](https://help.aliyun.com/document_detail/53045.html)。      |

##### 兼容 S3 协议的对象存储

如果选择兼容 S3 协议的对象存储（如 MinIO）作为文件存储，请按如下配置 `StorageCredentialParams`：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` 包含如下参数。

| 参数                             | 是否必须   | 说明                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | 是否开启 SSL 连接。<br />取值范围：`true` 和 `false`。默认值：`true`。 |
| aws.s3.enable_path_style_access  | Yes      | 是否开启路径类型访问 (Path-Style Access)。<br />取值范围：`true` 和 `false`。默认值：`false`。对于 MinIO，必须设置为 `true`。<br />路径类型 URL 使用如下格式：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例如，如果您在美国西部（俄勒冈）区域中创建一个名为 `DOC-EXAMPLE-BUCKET1` 的存储桶，并希望访问该存储桶中的 `alice.jpg` 对象，则可使用以下路径类型 URL：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | Yes      | 用于访问兼容 S3 协议的对象存储的 Endpoint。 |
| aws.s3.access_key                | Yes      | IAM User 的 Access Key。 |
| aws.s3.secret_key                | Yes      | IAM User 的 Secret Key。 |

##### Microsoft Azure Storage

###### Azure Blob Storage

如果选择 Blob Storage 作为文件存储，请按如下配置 `StorageCredentialParams`：

- 基于 Shared Key 进行认证和鉴权

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                   | **是否必须** | **说明**                         |
  | -------------------------- | ------------ | -------------------------------- |
  | azure.blob.storage_account | 是           | Blob Storage 账号的用户名。      |
  | azure.blob.shared_key      | 是           | Blob Storage 账号的 Shared Key。 |

- 基于 SAS Token 进行认证和鉴权

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                  | **是否必须** | **说明**                                 |
  | ------------------------- | ------------ | ---------------------------------------- |
  | azure.blob.storage_account| 是           | Blob Storage 账号的用户名。              |
  | azure.blob.container      | 是           | 数据所在 Blob 容器的名称。               |
  | azure.blob.sas_token      | 是           | 用于访问 Blob Storage 账号的 SAS Token。 |

###### Azure Data Lake Storage Gen2

如果选择 Data Lake Storage Gen2 作为文件存储，请按如下配置 `StorageCredentialParams`：

- 基于 Managed Identity 进行认证和鉴权

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                                | **是否必须** | **说明**                                                |
  | --------------------------------------- | ------------ | ------------------------------------------------------- |
  | azure.adls2.oauth2_use_managed_identity | 是           | 指定是否开启 Managed Identity 鉴权方式。设置为 `true`。 |
  | azure.adls2.oauth2_tenant_id            | 是           | 数据所属 Tenant 的 ID。                                 |
  | azure.adls2.oauth2_client_id            | 是           | Managed Identity 的 Client (Application) ID。           |

- 基于 Shared Key 进行认证和鉴权

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                    | **是否必须** | **说明**                                   |
  | --------------------------- | ------------ | ------------------------------------------ |
  | azure.adls2.storage_account | 是           | Data Lake Storage Gen2 账号的用户名。      |
  | azure.adls2.shared_key      | 是           | Data Lake Storage Gen2 账号的 Shared Key。 |

- 基于 Service Principal 进行认证和鉴权

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                           | **是否必须** | **说明**                                                     |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | 是           | Service Principal 的 Client (Application) ID。               |
  | azure.adls2.oauth2_client_secret   | 是           | 新建的 Client (Application) Secret。                         |
  | azure.adls2.oauth2_client_endpoint | 是           | Service Principal 或 Application 的 OAuth 2.0 Token Endpoint (v1)。 |

###### Azure Data Lake Storage Gen1

如果选择 Data Lake Storage Gen1 作为文件存储，请按如下配置 `StorageCredentialParams`：

- 基于 Managed Service Identity 进行认证和鉴权

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                                 | **是否必须** | **说明**                                                     |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | 是           | 指定是否开启 Managed Service Identity 鉴权方式。设置为 `true`。 |

- 基于 Service Principal 进行认证和鉴权

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **Parameter**                 | **Required** | **Description**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | 是           | Service Principal 的 Client (Application) ID。               |
  | azure.adls1.oauth2_credential | 是           | 新建的 Client (Application) Secret。                         |
  | azure.adls1.oauth2_endpoint   | 是           | Service Principal 或 Application 的 OAuth 2.0 Token Endpoint (v1)。 |

##### Google GCS

如果选择 Google GCS 作为文件存储，请按如下配置 `StorageCredentialParams`：

- 基于 VM 进行认证和鉴权

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                                   | **默认值** | **取值样例** | **说明**                                                 |
  | ------------------------------------------ | ---------- | ------------ | -------------------------------------------------------- |
  | gcp.gcs.use_compute_engine_service_account | false      | true         | 是否直接使用 Compute Engine 上面绑定的 Service Account。 |

- 基于 Service Account 进行认证和鉴权

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` 包含如下参数。

  | **参数**                               | **默认值** | **取值样例**                                                 | **说明**                                                     |
  | -------------------------------------- | ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""         | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 创建 Service Account 时生成的 JSON 文件中的 Email。          |
  | gcp.gcs.service_account_private_key_id | ""         | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 创建 Service Account 时生成的 JSON 文件中的 Private Key ID。 |
  | gcp.gcs.service_account_private_key    | ""         | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 创建 Service Account 时生成的 JSON 文件中的 Private Key。    |

- 基于 Impersonation 进行认证和鉴权

  - 使用 VM 实例模拟 Service Account

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` 包含如下参数。

    | **参数**                                   | **默认值** | **取值样例** | **说明**                                                     |
    | ------------------------------------------ | ---------- | ------------ | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false      | true         | 是否直接使用 Compute Engine 上面绑定的 Service Account。     |
    | gcp.gcs.impersonation_service_account      | ""         | "hello"      | 需要模拟的目标 Service Account。 |

  - 使用一个 Service Account（暂时命名为“Meta Service Account”）模拟另一个 Service Account（暂时命名为“Data Service Account”）

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` 包含如下参数。

    | **参数**                               | **默认值** | **取值样例**                                                 | **说明**                                                     |
    | -------------------------------------- | ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""         | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 创建 Meta Service Account 时生成的 JSON 文件中的 Email。     |
    | gcp.gcs.service_account_private_key_id | ""         | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 创建 Meta Service Account 时生成的 JSON 文件中的 Private Key ID。 |
    | gcp.gcs.service_account_private_key    | ""         | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 创建 Meta Service Account 时生成的 JSON 文件中的 Private Key。 |
    | gcp.gcs.impersonation_service_account  | ""         | "hello"                                                      | 需要模拟的目标 Data Service Account。 |

#### MetadataUpdateParams

指定缓存元数据更新策略的一组参数。此组参数为可选。StarRocks 根据该策略更新缓存的 Hive、Hudi 和 Delta Lake 元数据。有关 Hive、Hudi、和 Delta Lake 元数据缓存更新的详细介绍，参见 [Hive catalog](../../data_source/catalog/hive_catalog.md)、[Hudi catalog](../../data_source/catalog/hudi_catalog.md) 和 [Delta Lake catalog](../../data_source/catalog/deltalake_catalog.md)。

StarRocks 默认采用自动异步更新策略，开箱即用。因此，一般情况下，您可以忽略 `MetadataUpdateParams`，无需对其中的策略参数进行调优。

如果 Hive、Hudi、或 Delta Lake 数据更新频率较高，那么您可以对这些参数进行调优，从而优化自动异步更新策略的性能。

| 参数                                   | 是否必须 | 说明                                                         |
| -------------------------------------- | -------- | ------------------------------------------------------------ |
| enable_metastore_cache                 | 否       | 指定 StarRocks 是否缓存 Hive、Hudi、或 Delta Lake 表的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。 |
| enable_remote_file_cache               | 否       | 指定 StarRocks 是否缓存 Hive、Hudi、或 Delta Lake 表或分区的数据文件的元数据。取值范围：`true` 和 `false`。默认值：`true`。取值为 `true` 表示开启缓存，取值为 `false` 表示关闭缓存。 |
| metastore_cache_refresh_interval_sec   | 否       | StarRocks 异步更新缓存的 Hive、Hudi、或 Delta Lake 表或分区的元数据的时间间隔。单位：秒。默认值：`7200`，即 2 小时。 |
| remote_file_cache_refresh_interval_sec | 否       | StarRocks 异步更新缓存的 Hive、Hudi、或 Delta Lake 表或分区的数据文件的元数据的时间间隔。单位：秒。默认值：`60`。 |
| metastore_cache_ttl_sec                | 否       | StarRocks 自动淘汰缓存的 Hive、Hudi、或 Delta Lake 表或分区的元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。 |
| remote_file_cache_ttl_sec              | 否       | StarRocks 自动淘汰缓存的 Hive、Hudi、或 Delta Lake 表或分区的数据文件的元数据的时间间隔。单位：秒。默认值：`129600`，即 36 小时。 |

#### KuduCatalogParams

指定 Kudu Catalog 连接的一组参数。此组参数为可选。

| 参数                                   | 是否必须 | 说明                                                                                     |
| -------------------------------------- | -------- |----------------------------------------------------------------------------------------|
| kudu.master                 | 否       | 指定 `Kudu Master` 连接地址，默认为：`localhost:7051`。                                            |
| kudu.schema-emulation.enabled               | 否       | 是否启用模拟 `schema` 功能，默认处于关闭状态（`false`），即所有表都属于 `default` `schema`。                       |
| kudu.schema-emulation.prefix   | 否       | 仅在 `kudu.schema-emulation.enabled` = `true` 即启用模拟 `schema` 功能时，需设置匹配前缀，默认采用前缀空字符串：` `。 |

### 示例

以下示例创建了一个名为 `unified_catalog_hms` 或 `unified_catalog_glue` 的 Unified Catalog，用于查询融合数据源里的数据。

#### HDFS

使用 HDFS 作为存储时，可以按如下创建 Unified Catalog：

```SQL
CREATE EXTERNAL CATALOG unified_catalog_hms
PROPERTIES
(
    "type" = "unified",
    "unified.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### 如果基于 Instance Profile 进行鉴权和认证

- 如果使用 HMS 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果 Amazon EMR 使用 AWS Glue 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果基于 Assumed Role 进行鉴权和认证

- 如果使用 HMS 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果 Amazon EMR 使用 AWS Glue 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果基于 IAM User 进行鉴权和认证

- 如果使用 HMS 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果 Amazon EMR 使用 AWS Glue 作为元数据服务，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_glue
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

#### 兼容 S3 协议的对象存储

以 MinIO 为例，可以按如下创建 Unified Catalog：

```SQL
CREATE EXTERNAL CATALOG unified_catalog_hms
PROPERTIES
(
    "type" = "unified",
    "unified.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

#### Microsoft Azure Storage

##### Azure Blob Storage

- 如果基于 Shared Key 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.enable_ssl" = "true",
      "aws.s3.enable_path_style_access" = "true",
      "aws.s3.endpoint" = "<s3_endpoint>",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>"
  );
  ```

- 如果基于 SAS Token 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- 如果基于 Managed Service Identity 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- 如果基于 Service Principal 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- 如果基于 Managed Identity 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 如果基于 Shared Key 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- 如果基于 Service Principal 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>" 
  );
  ```

#### Google GCS

- 如果基于 VM 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- 如果基于 Service Account 进行认证和鉴权，可以按如下创建 Unified Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- 如果基于 Impersonation 进行认证和鉴权

  - 使用 VM 实例模拟 Service Account，可以按如下创建 Unified Catalog：

    ```SQL
    CREATE EXTERNAL CATALOG unified_catalog_hms
    PROPERTIES
    (
        "type" = "unified",
        "unified.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - 使用一个 Service Account 模拟另一个 Service Account，可以按如下创建 Unified Catalog：

    ```SQL
    CREATE EXTERNAL CATALOG unified_catalog_hms
    PROPERTIES
    (
        "type" = "unified",
        "unified.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## 查看 Unified Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前所在 StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询某个 External Catalog 的创建语句。例如，通过如下命令查询 Unified Catalog `unified_catalog_glue` 的创建语句：

```SQL
SHOW CREATE CATALOG unified_catalog_glue;
```

## 切换 Unified Catalog 和数据库

您可以通过如下方法切换至目标 Unified Catalog 和数据库：

- 先通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 指定当前会话生效的 Unified Catalog，然后再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定数据库：

  ```SQL
  -- 切换当前会话生效的 Catalog：
  SET CATALOG <catalog_name>
  -- 指定当前会话生效的数据库：
  USE <db_name>
  ```

- 通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标 Unified Catalog 下的指定数据库：

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## 删除 Unified Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除某个 External Catalog。

例如，通过如下命令删除 Unified Catalog `unified_catalog_glue`：

```SQL
DROP CATALOG unified_catalog_glue;
```

## 查看 Unified Catalog 内的表结构

您可以通过如下方法查看 Hive 表的表结构：

- 查看表结构

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- 从 CREATE 命令查看表结构和表文件存放位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## 查询 Unified Catalog 内的表数据

 您可以通过如下操作查询 Unified Catalog 内的数据：

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看指定 Unified Catalog 所属的数据源中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [切换至目标 Unified Catalog 和数据库](#切换-unified-catalog-和数据库)。

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询目标数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## 从 Hive、Iceberg、Hudi、Delta Lake 或 Kudu 导入数据

您可以通过 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将 Hive、Iceberg、Hudi、Delta Lake 或 Kudu 表中的数据导入 StarRocks 中 Unified Catalog 下的表。

例如，通过如下命令将 Hive 表 `hive_table` 的数据导入到 StarRocks 中 Unified Catalog `unified_catalog` 下数据库`test_database` 里的表 `test_table`：

```SQL
INSERT INTO unified_catalog.test_database.test_table SELECT * FROM hive_table
```

## 在 Unified Catalog 内创建数据库

同 StarRocks 内部数据目录 (Internal Catalog) 一致，如果您拥有 Unified Catalog 的 [CREATE DATABASE](../../administration/user_privs/privilege_item.md#数据目录权限-catalog) 权限，那么您可以使用 [CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) 在该 Unified Catalog 内创建数据库。

> **说明**
>
> 您可以通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

注意当前仅支持创建 Hive 数据库和 Iceberg 数据库。

[切换至目标 Unified Catalog](#切换-unified-catalog-和数据库)，然后通过如下语句创建数据库：

```SQL
CREATE DATABASE <database_name>
[properties ("location" = "<prefix>://<path_to_database>/<database_name.db>")]
```

`location` 参数用于指定数据库所在的文件路径，支持 HDFS 和对象存储：

- 选择 HMS 作为元数据服务时，如果您在创建数据库时不指定 `location`，那么系统会使用 HMS 默认的 `<warehouse_location>/<database_name.db>` 作为文件路径。
- 选择 AWS Glue 作为元数据服务时，`location` 参数没有默认值，因此您在创建数据库时必须指定该参数。

`prefix` 根据存储系统的不同而不同：

| **存储系统**                           | **`Prefix`** **取值**                                        |
| -------------------------------------- | ------------------------------------------------------------ |
| HDFS                                   | `hdfs`                                                       |
| Google GCS                             | `gs`                                                         |
| Azure Blob Storage                     | <ul><li>如果您的存储账号支持通过 HTTP 协议进行访问，`prefix` 为 `wasb`。</li><li>如果您的存储账号支持通过 HTTPS 协议进行访问，`prefix` 为 `wasbs`。</li></ul> |
| Azure Data Lake Storage Gen1           | `adl`                                                        |
| Azure Data Lake Storage Gen2           | <ul><li>如果您的存储账号支持通过 HTTP 协议进行访问，`prefix` 为 `abfs`。</li><li>如果您的存储账号支持通过 HTTPS 协议进行访问，`prefix` 为 `abfss`。</li></ul> |
| 阿里云 OSS                             | `oss`                                                        |
| 腾讯云 COS                             | `cosn`                                                       |
| 华为云 OBS                             | `obs`                                                        |
| AWS S3 及其他兼容 S3 的存储（如 MinIO）  | `s3`                                                         |

## 从 Unified Catalog 内删除数据库

同 StarRocks 内部数据库一致，如果您拥有 Unified Catalog 内数据库的 [DROP](../../administration/user_privs/privilege_item.md#数据库权限-database) 权限，那么您可以使用 [DROP DATABASE](../../sql-reference/sql-statements/Database/DROP_DATABASE.md) 来删除该数据库。仅支持删除空数据库。

> **说明**
>
> 您可以通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

注意当前仅支持删除 Hive 数据库和 Iceberg 数据库。

删除数据库操作并不会将 HDFS 或对象存储上的对应文件路径删除。

[切换至目标 Unified Catalog](#切换-unified-catalog-和数据库)，然后通过如下语句删除数据库：

```SQL
DROP DATABASE <database_name>
```

## 在 Unified Catalog 内创建表

同 StarRocks 内部数据库一致，如果您拥有 Unified Catalog 内数据库的 [CREATE TABLE](../../administration/user_privs/privilege_item.md#数据库权限-database) 权限，那么您可以使用 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 或 [CREATE TABLE AS SELECT (CTAS)](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 在该数据库下创建表。

> **说明**
>
> 您可以通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

注意当前仅支持创建 Hive 表和 Iceberg 表。

[切换至目标 Unified Catalog 和数据库](#切换-unified-catalog-和数据库)，然后通过 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 创建 Hive 表或 Iceberg 表：

```SQL
CREATE TABLE <table_name>
(column_definition1[, column_definition2, ...]
ENGINE = {|hive|iceberg}
[partition_desc]
```

有关创建 Hive 表和 Iceberg 表的详细信息，请参见[创建 Hive 表](../catalog/hive_catalog.md#创建-hive-表)和[创建 Iceberg 表](./iceberg/iceberg_catalog.md#创建-iceberg-表)。

例如，通过如下语句，创建一张 Hive 表 `hive_table`：

```SQL
CREATE TABLE hive_table
(
    action varchar(65533),
    id int,
    dt date
)
ENGINE = hive
PARTITION BY (id,dt);
```

## 向 Unified Catalog 内的表中插入数据

同 StarRocks 内表一致，如果您拥有 Unified Catalog 内表的 [INSERT](../../administration/user_privs/privilege_item.md#表权限-table) 权限，那么您可以使用 [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将 StarRocks 表数据写入到该表（当前仅支持写入到 Parquet 格式的 Unified Catalog 表）。

> **说明**
>
> 您可以通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

注意当前仅支持向 Hive 表和 Iceberg 表中插入数据。

[切换至目标 Unified Catalog 和数据库](#切换-unified-catalog-和数据库)，然后通过 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 向 Hive 表或 Iceberg 表中插入数据：

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 向指定分区写入数据。
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

有关向 Hive 表和 Iceberg 表中插入数据的详细信息，请参见[向 Hive 表中插入数据](../catalog/hive_catalog.md#向-hive-表中插入数据)和[向 Iceberg 表中插入数据](./iceberg/iceberg_catalog.md#向-iceberg-表中插入数据)。

例如，通过如下语句，向 Hive 表 `hive_table` 中写入如下数据：

```SQL
INSERT INTO hive_table
VALUES
    ("buy", 1, "2023-09-01"),
    ("sell", 2, "2023-09-02"),
    ("buy", 3, "2023-09-03");
```

## 从 Unified Catalog 内删除表

同 StarRocks 内表一致，如果您拥有 Unified Catalog 内表的 [DROP](../../administration/user_privs/privilege_item.md#表权限-table) 权限，那么您可以使用 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 来删除该表。

> **说明**
>
> 您可以通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

注意当前仅支持删除 Hive 表和 Iceberg 表。

[切换至目标 Unified Catalog 和数据库](#切换-unified-catalog-和数据库)，然后通过 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 删除 Hive 表或 Iceberg 表。

```SQL
DROP TABLE <table_name>
```

有关删除 Hive 表和 Iceberg 表的详细信息，请参见[删除 Hive 表](../catalog/hive_catalog.md#删除-hive-表)和[删除 Iceberg 表](./iceberg/iceberg_catalog.md#删除-iceberg-表)。

例如，通过如下语句，删除 Hive 表 `hive_table`：

```SQL
DROP TABLE hive_table FORCE
```
