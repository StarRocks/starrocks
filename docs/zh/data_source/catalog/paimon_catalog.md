---
sidebar_position: 100
displayed_sidebar: docs
description: "StarRocks 从 v3.1 起支持 Paimon catalog，无导入直接查询 Apache Paimon 数据及转换导入。"
toc_max_heading_level: 5
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Paimon catalog

<Beta />

StarRocks 从 v3.1 开始支持 Paimon catalog。

Paimon catalog 是一种 external catalog，可以让您在不进行数据导入的情况下查询 Apache Paimon 的数据。

此外，您还可以基于 Paimon catalog 使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 直接转换和导入 Paimon 的数据。

为了确保在您的 Paimon 集群上成功执行 SQL 工作负载，您的 StarRocks 集群必须能够访问 Paimon 集群的存储系统和元存储。StarRocks 支持以下存储系统和元存储：

- 分布式文件系统（HDFS）或对象存储，如 AWS S3、Microsoft Azure Storage、Google GCS 或其他兼容 S3 的存储系统（例如 MinIO）
- 元存储，如您的文件系统或 Hive 元存储

## 使用注意事项

除查询外，当前 Paimon 集成还实现了 `CREATE/DROP DATABASE`、`CREATE/DROP TABLE`、批量 `INSERT INTO`、`INSERT OVERWRITE`、`CREATE TABLE AS SELECT`（CTAS）以及同一 Catalog 内的 `CREATE TABLE LIKE`。不支持行级 `UPDATE` 和 `DELETE`。只能删除空数据库。

:::caution
DDL 和批量写入仍处于集成阶段。下文的建表和写入说明描述当前实现，不代表已通过运行时验证的版本可用性。示例和端到端回读行为仍需运行时验证。
:::

## Paimon to StarRocks data types

| Paimon Type           | StarRocks Type              |
|-----------------------|-----------------------------|
| `BINARY`              | `VARBINARY`                 |
| `VARBINARY`           | `VARBINARY`                 |
| `CHAR`                | `CHAR(length)`              |
| `VARCHAR`             | `VARCHAR`                   |
| `BOOLEAN`             | `BOOLEAN`                   |
| `DECIMAL`             | `DECIMAL(precision, scale)` |
| `TINYINT`             | `TINYINT`                   |
| `SMALLINT`            | `SMALLINT`                  |
| `INT`                 | `INT`                       |
| `BIGINT`              | `BIGINT`                    |
| `FLOAT`               | `FLOAT`                     |
| `DOUBLE`              | `DOUBLE`                    |
| `DATE`                | `DATE`                      |
| `TIME`                | `TIME`                      |
| `TIMESTAMP`           | `DATETIME`                  |
| `LocalZonedTimestamp` | `DATETIME`                  |
| `ARRAY`               | `ARRAY<element_type>`       |
| `MAP`                 | `MAP<key_type, value_type>` |
| `ROW/STRUCT`          | `STRUCT<field1:type1, ...>` |
| `VARIANT`             | `VARIANT`                   |

:::note
`VARIANT` 从 v4.2 版本起支持，且仅支持由 Native Reader 读取的 split（Append-Only 表以及 Primary Key 表中已完成 Compaction 的数据）。从尚未 Compaction 的 Primary Key 数据中读取 `VARIANT` 需要使用 JNI Reader，目前尚不支持，此类查询会在 Plan 阶段报错并被拒绝执行。
:::

## 集成准备

在创建 Paimon catalog 之前，请确保您的 StarRocks 集群可以与 Paimon 集群的存储系统和元存储集成。

### AWS IAM

如果您的 Paimon 集群使用 AWS S3 作为存储，请选择合适的身份验证方法并进行必要的准备，以确保您的 StarRocks 集群可以访问相关的 AWS 云资源。

推荐以下身份验证方法：

- 实例配置文件（推荐）
- 假设角色
- IAM 用户

在上述三种身份验证方法中，实例配置文件是最广泛使用的。

有关更多信息，请参见 [AWS IAM 中的身份验证准备](../../integrations/csp_auth/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication)。

### HDFS

如果您选择 HDFS 作为存储，请按以下方式配置您的 StarRocks 集群：

- （可选）设置用于访问您的 HDFS 集群和 Hive 元存储的用户名。默认情况下，StarRocks 使用 FE 和 BE 或 CN 进程的用户名访问您的 HDFS 集群和 Hive 元存储。您还可以通过在每个 FE 的 **fe/conf/hadoop_env.sh** 文件的开头和每个 BE 或 CN 的 **be/conf/hadoop_env.sh** 文件的开头添加 `export HADOOP_USER_NAME="<user_name>"` 来设置用户名。在这些文件中设置用户名后，重启每个 FE 和每个 BE 或 CN 以使参数设置生效。您只能为每个 StarRocks 集群设置一个用户名。
- 当您查询 Paimon 数据时，您的 StarRocks 集群的 FEs 和 BEs 或 CNs 使用 HDFS 客户端访问您的 HDFS 集群。在大多数情况下，您无需配置您的 StarRocks 集群即可实现此目的，StarRocks 使用默认配置启动 HDFS 客户端。您仅需在以下情况下配置您的 StarRocks 集群：
  - 如果您的 HDFS 集群启用了高可用性（HA）：将您的 HDFS 集群的 **hdfs-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 或 CN 的 **$BE_HOME/conf** 路径。
  - 如果您的 HDFS 集群启用了 View File System (ViewFs)：将您的 HDFS 集群的 **core-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 或 CN 的 **$BE_HOME/conf** 路径。

> **注意**
>
> 如果在发送查询时返回未知主机的错误，您必须将您的 HDFS 集群节点的主机名和 IP 地址的映射添加到 **/etc/hosts** 路径。

### Kerberos 身份验证

如果您的 HDFS 集群或 Hive 元存储启用了 Kerberos 身份验证，请按以下方式配置您的 StarRocks 集群：

- 在每个 FE 和每个 BE 或 CN 上运行 `kinit -kt keytab_path principal` 命令，从密钥分发中心 (KDC) 获取票证授予票证 (TGT)。要运行此命令，您必须具有访问您的 HDFS 集群和 Hive 元存储的权限。请注意，使用此命令访问 KDC 是时间敏感的。因此，您需要使用 cron 定期运行此命令。
- 将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 或 CN 的 **$BE_HOME/conf/be.conf** 文件中。在此示例中，`/etc/krb5.conf` 是 **krb5.conf** 文件的保存路径。您可以根据需要修改路径。

## 创建 Paimon catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "paimon",
    CatalogParams,
    StorageCredentialParams,
)
```

### 参数

#### catalog_name

Paimon catalog 的名称。命名规则如下：

- 名称可以包含字母、数字 (0-9) 和下划线 (_)。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### comment

Paimon catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `paimon`。

#### CatalogParams

关于 StarRocks 如何访问 Paimon 集群元数据的一组参数。

下表描述了您需要在 `CatalogParams` 中配置的参数。

| 参数                      | 是否必需 | 描述                                                         |
| ------------------------ | -------- | ------------------------------------------------------------ |
| paimon.catalog.type      | 是       | 您用于 Paimon 集群的元存储类型。将此参数设置为 `filesystem` 或 `hive`。 |
| paimon.catalog.warehouse | 是       | 您的 Paimon 数据的仓库存储路径。                             |
| hive.metastore.uris      | 否       | 您的 Hive 元存储的 URI。格式：`thrift://<metastore_IP_address>:<metastore_port>`。如果您的 Hive 元存储启用了高可用性（HA），您可以指定多个元存储 URI，并用逗号（`,`）分隔，例如，`"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

> **注意**
>
> 如果您使用 Hive 元存储，您必须在查询 Paimon 数据之前将您的 Hive 元存储节点的主机名和 IP 地址的映射添加到 `/etc/hosts` 路径。否则，当您启动查询时，StarRocks 可能无法访问您的 Hive 元存储。

#### StorageCredentialParams

关于 StarRocks 如何与您的存储系统集成的一组参数。此参数集是可选的。

如果您使用 HDFS 作为存储，则无需配置 `StorageCredentialParams`。

如果您使用 AWS S3、其他兼容 S3 的存储系统、Microsoft Azure Storage 或 Google GCS 作为存储，则必须配置 `StorageCredentialParams`。

##### AWS S3

如果您选择 AWS S3 作为 Paimon 集群的存储，请采取以下操作之一：

- 要选择基于实例配置文件的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- 要选择基于假设角色的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.endpoint" = "<aws_s3_endpoint>"
  ```

下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                           | 是否必需 | 描述                                                         |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | 是       | 指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。 |
| aws.s3.iam_role_arn         | 否       | 在您的 AWS S3 存储桶上具有权限的 IAM 角色的 ARN。如果您使用基于假设角色的身份验证方法访问 AWS S3，您必须指定此参数。 |
| aws.s3.endpoint             | 是       | 用于连接到您的 AWS S3 存储桶的端点。例如，`https://s3.us-west-2.amazonaws.com`。 |
| aws.s3.access_key           | 否       | 您的 IAM 用户的访问密钥。如果您使用基于 IAM 用户的身份验证方法访问 AWS S3，您必须指定此参数。 |
| aws.s3.secret_key           | 否       | 您的 IAM 用户的秘密密钥。如果您使用基于 IAM 用户的身份验证方法访问 AWS S3，您必须指定此参数。 |

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS S3 的身份验证参数](../../integrations/csp_auth/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

##### 兼容 S3 的存储系统

如果您选择兼容 S3 的存储系统，例如 MinIO，作为 Paimon 集群的存储，请按如下方式配置 `StorageCredentialParams` 以确保成功集成：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                           | 是否必需 | 描述                                                         |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl               | 是       | 指定是否启用 SSL 连接。<br />有效值：`true` 和 `false`。默认值：`true`。 |
| aws.s3.enable_path_style_access | 是       | 指定是否启用路径样式访问。<br />有效值：`true` 和 `false`。默认值：`false`。对于 MinIO，您必须将值设置为 `true`。<br />路径样式 URL 使用以下格式：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例如，如果您在美国西部（俄勒冈）区域创建了一个名为 `DOC-EXAMPLE-BUCKET1` 的存储桶，并且您想要访问该存储桶中的 `alice.jpg` 对象，您可以使用以下路径样式 URL：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                 | 是       | 用于连接到您的兼容 S3 的存储系统而不是 AWS S3 的端点。       |
| aws.s3.access_key               | 是       | 您的 IAM 用户的访问密钥。                                     |
| aws.s3.secret_key               | 是       | 您的 IAM 用户的秘密密钥。                                     |

##### Microsoft Azure Storage

###### Azure Blob Storage

如果您选择 Blob Storage 作为 Paimon 集群的存储，请采取以下操作之一：

- 要选择共享密钥身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                      | 是否必需 | 描述                                                         |
  | -------------------------- | -------- | ------------------------------------------------------------ |
  | azure.blob.storage_account | 是       | 您的 Blob Storage 账户的用户名。                             |
  | azure.blob.shared_key      | 是       | 您的 Blob Storage 账户的共享密钥。                           |

- 要选择 SAS 令牌身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                      | 是否必需 | 描述                                                         |
  | ------------------------- | -------- | ------------------------------------------------------------ |
  | azure.blob.storage_account| 是       | 您的 Blob Storage 账户的用户名。                             |
  | azure.blob.container      | 是       | 存储您数据的 blob 容器的名称。                               |
  | azure.blob.sas_token      | 是       | 用于访问您的 Blob Storage 账户的 SAS 令牌。                  |

###### Azure Data Lake Storage Gen2

如果您选择 Data Lake Storage Gen2 作为 Paimon 集群的存储，请采取以下操作之一：

- 要选择托管身份身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                               | 是否必需 | 描述                                                         |
  | --------------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | 是       | 指定是否启用托管身份身份验证方法。将值设置为 `true`。         |
  | azure.adls2.oauth2_tenant_id            | 是       | 您要访问的数据的租户 ID。                                     |
  | azure.adls2.oauth2_client_id            | 是       | 托管身份的客户端（应用程序）ID。                              |

- 要选择共享密钥身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                   | 是否必需 | 描述                                                         |
  | --------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.storage_account | 是       | 您的 Data Lake Storage Gen2 存储账户的用户名。               |
  | azure.adls2.shared_key      | 是       | 您的 Data Lake Storage Gen2 存储账户的共享密钥。             |

- 要选择服务主体身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                          | 是否必需 | 描述                                                         |
  | ---------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | 是       | 服务主体的客户端（应用程序）ID。                              |
  | azure.adls2.oauth2_client_secret   | 是       | 创建的新客户端（应用程序）密钥的值。                          |
  | azure.adls2.oauth2_client_endpoint | 是       | 服务主体或应用程序的 OAuth 2.0 令牌端点（v1）。               |

- 要选择 Workload Identity 验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_token_file" = "<path_to_token>",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                               | **必需** | **描述**                                              |
  | ------------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_token_file         | 是       | Azure Workload Identity Webhook 投射到 Pod 中的 OAuth2 令牌文件的绝对文件路径。 |
  | azure.adls2.oauth2_tenant_id          | 是       | 您要访问数据的租户的 ID。                             |
  | azure.adls2.oauth2_client_id          | 是       | 与 Workload Identity 关联的 Azure AD 应用程序（用户分配的托管身份或应用程序注册）的客户端 ID（应用程序 ID）。 |

###### Azure Data Lake Storage Gen1

如果您选择 Data Lake Storage Gen1 作为 Paimon 集群的存储，请采取以下操作之一：

- 要选择托管服务身份身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                                | 是否必需 | 描述                                                         |
  | ---------------------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | 是       | 指定是否启用托管服务身份身份验证方法。将值设置为 `true`。     |

- 要选择服务主体身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                     | 是否必需 | 描述                                                         |
  | ----------------------------- | -------- | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | 是       | 服务主体的客户端（应用程序）ID。                              |
  | azure.adls1.oauth2_credential | 是       | 创建的新客户端（应用程序）密钥的值。                          |
  | azure.adls1.oauth2_endpoint   | 是       | 服务主体或应用程序的 OAuth 2.0 令牌端点（v1）。               |

##### Google GCS

如果您选择 Google GCS 作为 Paimon 集群的存储，请采取以下操作之一：

- 要选择基于 VM 的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                                  | 默认值         | 示例值       | 描述                                                         |
  | ------------------------------------------ | ------------- | ------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | FALSE         | TRUE          | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。         |

- 要选择基于服务账户的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | 参数                              | 默认值         | 示例值                                                      | 描述                                                         |
  | -------------------------------------- | ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""            | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建服务账户时生成的 JSON 文件中的电子邮件地址。            |
  | gcp.gcs.service_account_private_key_id | ""            | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建服务账户时生成的 JSON 文件中的私钥 ID。                |
  | gcp.gcs.service_account_private_key    | ""            | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 在创建服务账户时生成的 JSON 文件中的私钥。                   |

- 要选择基于模拟的身份验证方法，请按如下方式配置 `StorageCredentialParams`：

  - 使 VM 实例模拟服务账户：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | 参数                                  | 默认值         | 示例值       | 描述                                                         |
    | ------------------------------------------ | ------------- | ------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | FALSE         | TRUE          | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。         |
    | gcp.gcs.impersonation_service_account      | ""            | "hello"       | 您要模拟的服务账户。                                         |

  - 使服务账户（暂时命名为元服务账户）模拟另一个服务账户（暂时命名为数据服务账户）：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | 参数                              | 默认值         | 示例值                                                      | 描述                                                         |
    | -------------------------------------- | ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""            | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建元服务账户时生成的 JSON 文件中的电子邮件地址。          |
    | gcp.gcs.service_account_private_key_id | ""            | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建元服务账户时生成的 JSON 文件中的私钥 ID。              |
    | gcp.gcs.service_account_private_key    | ""            | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 在创建元服务账户时生成的 JSON 文件中的私钥。                 |
    | gcp.gcs.impersonation_service_account  | ""            | "hello"                                                      | 您要模拟的数据服务账户。                                     |

#### MetadataCacheParams（可选）

StarRocks 会缓存 Paimon 元数据并在后台刷新，以下可选属性用于调整该缓存。

| 参数                                     | 默认值   | 说明                                                         |
| ---------------------------------------- | -------- | ------------------------------------------------------------ |
| paimon_meta_cache_ttl_sec                | 86400    | 缓存条目的存活时间，单位为秒。 |
| paimon_table_cache_refresh_interval_sec  | 60       | 同一张表由查询触发的两次刷新之间的最小间隔，单位为秒。当查询读到比上次刷新更新的 Snapshot 时触发，刷新在后台执行，查询不会等待。设置为 `0` 表示只依赖周期性后台刷新。自 v4.2.0 起支持。 |

### 示例

以下示例创建一个名为 `paimon_catalog_fs` 的 Paimon catalog，其元存储类型 `paimon.catalog.type` 设置为 `filesystem`，用于从您的 Paimon 集群查询数据。

#### AWS S3

- 如果您选择基于实例配置文件的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- 如果您选择基于假设角色的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

- 如果您选择基于 IAM 用户的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<s3_paimon_warehouse_path>",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.endpoint" = "<s3_endpoint>"
  );
  ```

#### 兼容 S3 的存储系统

以 MinIO 为例。运行如下命令：

```SQL
CREATE EXTERNAL CATALOG paimon_catalog_fs
PROPERTIES
(
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "paimon.catalog.warehouse" = "<paimon_warehouse_path>",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "<s3_endpoint>",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>"
);
```

#### Microsoft Azure Storage

##### Azure Blob Storage

- 如果您选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- 如果您选择 SAS 令牌身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<blob_paimon_warehouse_path>",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- 如果您选择托管服务身份身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.use_managed_service_identity" = "true"
  );
  ```

- 如果您选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls1_paimon_warehouse_path>",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- 如果您选择托管身份身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 如果您选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"
  );
  ```

- 如果您选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

- 如果您选择 Workload Identity 身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<adls2_paimon_warehouse_path>",
      "azure.adls2.oauth2_token_file" = "/var/run/secrets/azure/tokens/azure-identity-token",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

#### Google GCS

- 如果您选择基于 VM 的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.use_compute_engine_service_account" = "true"
  );
  ```

- 如果您选择基于服务账户的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG paimon_catalog_fs
  PROPERTIES
  (
      "type" = "paimon",
      "paimon.catalog.type" = "filesystem",
      "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  );
  ```

- 如果您选择基于模拟的身份验证方法：

  - 如果您使 VM 实例模拟服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    );
    ```

  - 如果您使服务账户模拟另一个服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG paimon_catalog_fs
    PROPERTIES
    (
        "type" = "paimon",
        "paimon.catalog.type" = "filesystem",
        "paimon.catalog.warehouse" = "<gcs_paimon_warehouse_path>",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    );
    ```

## 查看 Paimon catalog

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前 StarRocks 集群中的所有 catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询 external catalog 的创建语句。以下示例查询名为 `paimon_catalog_fs` 的 Paimon catalog 的创建语句：

```SQL
SHOW CREATE CATALOG paimon_catalog_fs;
```

## 删除 Paimon catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除 external catalog。

以下示例删除名为 `paimon_catalog_fs` 的 Paimon catalog：

```SQL
DROP Catalog paimon_catalog_fs;
```

## 查看 Paimon 表的 schema

您可以使用以下语法之一查看 Paimon 表的 schema：

- 查看 schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- 从 CREATE 语句中查看 schema 和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## 查询 Paimon 表

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看您的 Paimon 集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 在当前会话中切换到目标 catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   然后，使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话中的活动数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，您可以使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接指定目标 catalog 中的活动数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 从 Paimon 导入数据

假设您有一个名为 `olap_tbl` 的 OLAP 表，您可以按如下方式转换和导入数据：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM paimon_table;
```

## 创建和写入 Paimon 表

以下内容描述仍处于集成阶段的 DDL 和批量写入实现。您可以复用已有 Paimon Catalog 及其数据库，或按下例创建数据库。Warehouse、元存储和存储凭据仍在 Catalog 的 `PROPERTIES` 中配置，不要写入表属性。`CREATE DATABASE` 不接受 Catalog 或位置覆盖配置。

### 创建数据库和表

如果 `write_db` 已存在，请跳过 `CREATE DATABASE`，直接使用该数据库。表位置由 Catalog 决定。`ENGINE` 根据目标 Catalog 自动推断为 `paimon`。

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS write_db;
USE write_db;

CREATE TABLE orders (
    id BIGINT NOT NULL,
    dt DATE NOT NULL,
    amount DECIMAL(12, 2)
)
PRIMARY KEY (id, dt)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "4",
    "file.format" = "parquet",
    "merge-engine" = "deduplicate"
);

CREATE TABLE event_log (
    id BIGINT,
    dt DATE,
    event_time DATETIME,
    tags ARRAY<STRING>,
    attributes MAP<STRING, STRING>,
    details STRUCT<source STRING, attempt INT>,
    payload VARIANT
)
PARTITION BY (dt)
PROPERTIES (
    "bucket" = "-1",
    "file.format" = "parquet"
);
```

`PRIMARY KEY` 定义的是 Paimon 原生主键，而不是 OLAP 表模型；主键列不允许为 NULL。对于可写主键表，必须显式设置 `bucket > 0`。不含主键的表为 Append 表。分区使用普通列列表 `PARTITION BY (column, ...)`，不支持分区定义或表达式。原生分桶通过 `PROPERTIES` 中的 `bucket` 及按需指定的 `bucket-key` 配置，而不是 `DISTRIBUTED BY`。不支持 OLAP 的 `ORDER BY`、`ROLLUP`、聚合列、生成列和 `AUTO_INCREMENT`。

### 写入类型和默认值

- 新表支持可表示的标量类型：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INT`、`BIGINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、`CHAR`、`VARCHAR`/`STRING`、`VARBINARY`、`DATE` 和 `DATETIME`。还支持 `ARRAY`、`MAP`、`STRUCT` 和 `VARIANT`，以及由支持类型组成的嵌套结构。
- 新建的 `DATETIME` 列映射为不带时区的 Paimon `TIMESTAMP(6)`。写入已有表时使用原生时间戳类型，但公共 C++ 0.3 Writer 仅支持精度 `0`、`3`、`6` 和 `9`；精度 `1`、`2`、`4`、`5`、`7` 和 `8` 会在能力检查时被拒绝。对于带时区的目标类型，源 `DATETIME` 按会话时区解释。如果低于 `6` 的目标精度无法精确表示源值的微秒部分，Writer 会拒绝该值，而不是静默截断。这不会增加源值超过微秒的精度，也不会改变读取侧的时间戳映射和限制。
- 新建嵌套子字段允许为 NULL。SQL `CREATE TABLE` 无法声明嵌套 `NOT NULL`。写入时会检查已有原生可空性约束；`CREATE TABLE LIKE` 会保留这些约束。
- 普通 `LARGEINT` 列以及其他不支持或无法表示的类型会被明确拒绝，而不是静默缩小范围。支持 `VARIANT` 不代表已验证与公共 Paimon Reader 之间完整的有符号 128 位整数互操作性；该互操作性尚未验证。
- 可以不声明 `DEFAULT`，或仅为可空列指定 `DEFAULT NULL`。不支持非 NULL 字面量或表达式默认值。向具有原生非 NULL 默认值的已有表插入数据时，必须显式提供值，不能依赖省略列或 `DEFAULT`。必填列和主键列必须提供值。

### 批量 INSERT 和 CTAS

```sql
INSERT INTO paimon_catalog_fs.write_db.orders
VALUES (1, '2026-01-01', 12.34);

CREATE TABLE paimon_catalog_fs.write_db.orders_copy
PROPERTIES ("bucket" = "-1", "file.format" = "parquet")
AS SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders;
```

[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 接受值列表或查询结果。对于主键目标表，使用原生 `deduplicate` Merge Engine，而不是 StarRocks OLAP 部分列更新语义。[CTAS](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 先创建原生新表，再写入查询结果，不会克隆源表的主键或表属性。上例创建 Append 表。若要创建主键目标表，请声明 `PRIMARY KEY` 并显式设置 `bucket > 0`。

CTAS 不保证建表和数据发布之间的原子性。如果写入失败，当前实现会保守地保留已创建的表，因为公共 Catalog 无法根据表身份执行原子条件删除。手动清理或重试前，请检查并核对目标表。发布结果未知的表绝不能自动删除。

### CREATE TABLE LIKE

源表和目标表必须属于同一 Paimon Catalog，但可以位于不同数据库：

```sql
SET CATALOG paimon_catalog_fs;
CREATE DATABASE IF NOT EXISTS copy_db;
CREATE TABLE paimon_catalog_fs.copy_db.orders_like
LIKE paimon_catalog_fs.write_db.orders;
```

`LIKE` 复制原生 Schema，包括主键和分区键、原生类型、时间戳类型和精度、顶层及嵌套 `NOT NULL` 约束，以及可复用的表属性。目标表获得新的身份和存储位置，不复制源数据文件或快照，也不复制源表专属位置和凭据。通过 `LIKE` 建表不会绕过下述写入能力检查。

### 写入限制

- 写入目标必须是主分支上的原生 File-Store 表，并使用单条自动提交语句。不支持显式多语句事务，也不保证跨 FE 重启的 Exactly-Once 恢复。
- 数据文件仅支持 Parquet，包括按层指定文件格式的配置。此 Writer 不支持外部数据路径（`data-file.external-paths`）、替代数据目录（`data-file.path-directory`）或带有附属文件的 Committable。
- 主键表要求固定分桶（`bucket > 0`）和 `merge-engine = deduplicate`。Append 表支持 Unaware 分桶（`bucket = -1`）或固定分桶（`bucket > 0`，需配置原生要求的 `bucket-key`）。
- 不支持主键表动态分桶（`bucket = -1`）、Postpone 分桶（`bucket = -2`）、全局索引、Data Evolution、Row Tracking 和 `FILEBLOB`。不支持跨分区主键更新：所有分区列都必须包含在主键中。
- 分区值目前仅支持 `BOOLEAN`、整数类型、`CHAR`/`VARCHAR` 和 `DATE`；数据列支持时间戳和小数并不代表分区列也支持。可空分区列使用原生 NULL 标记。由于公共 SDK 会将空字符串或全空白字符串与 NULL 混淆，此类字符串分区值会被明确拒绝；保留的默认分区名称同样会被拒绝。
- 数据写入仅通过隔离的 `libstarrocks_paimon.so` Shim 使用原生 C++ Writer。写入库缺失或不兼容时，操作失败，不提供 JNI Writer 或 JNI 回退。FE 的 Java Catalog 和快照提交协议与数据 Writer 相互独立，不通过 JNI 执行。公共 Paimon C++ 0.3 和 Java 2.0 SDK 基线保持不变。

### INSERT OVERWRITE

Paimon 通过发布快照实现覆盖写入，不使用 StarRocks 临时分区交换。不接受分区名称或临时分区；静态分区使用列值对，必须指定所有分区列。

| 模式 | 选择方式 | 空输入的效果 |
| --- | --- | --- |
| 全表 | 未指定 `PARTITION` 且未启用 Dynamic Overwrite；非分区表也使用此模式。替换整表数据。 | 清空整表。 |
| 静态 | `PARTITION (column = value, ...)`。替换指定分区。 | 清空指定分区。 |
| 动态 | 无静态分区子句、目标为分区表且 `dynamic_overwrite = true`。仅替换输入行涉及的分区，保留其他分区。 | 不涉及任何分区，已有数据保持不变。 |

```sql
INSERT OVERWRITE paimon_catalog_fs.write_db.orders
PARTITION (dt = '2026-01-01')
SELECT id, amount FROM paimon_catalog_fs.write_db.orders_copy
WHERE dt = '2026-01-01';

INSERT /*+ SET_VAR(dynamic_overwrite = true) */ OVERWRITE paimon_catalog_fs.write_db.orders
SELECT id, dt, amount FROM paimon_catalog_fs.write_db.orders_copy;
```

空输入的 `INSERT INTO` 同样不修改数据。全表和静态覆盖在输入为空时仍执行清空操作。

### 提交结果和回读

非空写入，以及输入为空的全表或静态覆盖，只有在 FE 确认 Paimon 快照发布后才报告成功。仅 BE 完成文件写入不代表成功。空输入的追加和动态覆盖会确认为无数据变更，不需要新快照。

:::warning
如果错误包含 `Paimon commit outcome is unknown for operation ...; preserve its files and reconcile the snapshot before retrying`，发布结果可能已经持久化。请保留文件及任何 CTAS 目标表，记录操作标识，并核对 Paimon 快照后再决定是否重试。即使客户端收到失败或取消结果，也不要盲目重跑语句或删除输出。
:::

写入不会消除已有的读取限制，包括上文的 `VARIANT` 边界及时间戳和时区映射。本功能的回读验证范围仅为原生 C++ Reader 和 StarRocks 原生 Parquet Reader，不包含 JNI 互操作验证。主键表遵循原生合并语义，结果不应直接按物理输入行数理解。Reader 选择、Compaction、时间戳及时区行为以及原生回读仍需端到端运行时验证。
