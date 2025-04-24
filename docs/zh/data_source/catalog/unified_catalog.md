---
displayed_sidebar: docs
toc_max_heading_level: 5
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Unified catalog

<Beta />

统一 catalog 是一种外部 catalog，由 StarRocks 从 v3.2 开始提供，用于处理来自 Apache Hive™、Apache Iceberg、Apache Hudi、Delta Lake 和 Apache Kudu 数据源的表，作为统一的数据源而无需导入。通过统一 catalog，您可以：

- 直接查询存储在 Hive、Iceberg、Hudi、Delta Lake、Paimon 和 Kudu 中的数据，无需手动创建表。
- 使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 或异步物化视图（从 v2.5 开始支持）处理存储在 Hive、Iceberg、Hudi、Delta Lake、Paimon 和 Kudu 中的数据，并将数据导入 StarRocks。
- 在 StarRocks 上执行操作以创建或删除 Hive 和 Iceberg 数据库和表。

为了确保在统一数据源上成功执行 SQL 工作负载，您的 StarRocks 集群必须能够访问统一数据源的存储系统和元数据存储。StarRocks 支持以下存储系统和元数据存储：

- 分布式文件系统（HDFS）或对象存储，如 AWS S3、Microsoft Azure Storage、Google GCS 或其他兼容 S3 的存储系统（例如 MinIO）

- 元数据存储如 Hive metastore 或 AWS Glue

  > **注意**
  >
  > 如果选择 AWS S3 作为存储，可以使用 HMS 或 AWS Glue 作为元数据存储。如果选择其他存储系统，则只能使用 HMS 作为元数据存储。

## 限制

一个统一 catalog 仅支持与单一存储系统和单一元数据存储服务的集成。因此，请确保您希望与 StarRocks 集成为统一数据源的所有数据源使用相同的存储系统和元数据存储服务。

## 使用注意事项

- 请参阅 [Hive catalog](../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](./iceberg/iceberg_catalog.md)、[Hudi catalog](../../data_source/catalog/hudi_catalog.md)、[Delta Lake catalog](../../data_source/catalog/deltalake_catalog.md)、[Paimon catalog](../catalog/paimon_catalog.md) 和 [Kudu catalog](../../data_source/catalog/kudu_catalog.md) 中的“使用注意事项”部分，以了解支持的文件格式和数据类型。

- 格式特定的操作仅支持特定的表格式。例如，[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 和 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 仅支持 Hive 和 Iceberg，[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) 仅支持 Hive 和 Hudi。

  当您使用 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 语句在统一 catalog 中创建表时，使用 `ENGINE` 参数指定表格式（Hive 或 Iceberg）。

## 集成准备

在创建统一 catalog 之前，请确保您的 StarRocks 集群可以与统一数据源的存储系统和元数据存储集成。

### AWS IAM

如果您使用 AWS S3 作为存储或 AWS Glue 作为元数据存储，请选择合适的身份验证方法并进行必要的准备，以确保您的 StarRocks 集群可以访问相关的 AWS 云资源。有关更多信息，请参阅 [Authenticate to AWS resources - Preparations](../../integrations/authenticate_to_aws_resources.md#preparations)。

### HDFS

如果选择 HDFS 作为存储，请按以下步骤配置您的 StarRocks 集群：

- （可选）设置用于访问 HDFS 集群和 Hive metastore 的用户名。默认情况下，StarRocks 使用 FE 和 BE 或 CN 进程的用户名访问 HDFS 集群和 Hive metastore。您还可以通过在每个 FE 的 **fe/conf/hadoop_env.sh** 文件的开头和每个 BE 的 **be/conf/hadoop_env.sh** 文件的开头或每个 CN 的 **cn/conf/hadoop_env.sh** 文件的开头添加 `export HADOOP_USER_NAME="<user_name>"` 来设置用户名。设置用户名后，重启每个 FE 和每个 BE 或 CN 以使参数设置生效。每个 StarRocks 集群只能设置一个用户名。
- 当您查询数据时，StarRocks 集群的 FEs 和 BEs 或 CNs 使用 HDFS 客户端访问 HDFS 集群。在大多数情况下，您不需要为此目的配置 StarRocks 集群，StarRocks 使用默认配置启动 HDFS 客户端。您仅在以下情况下需要配置 StarRocks 集群：
  - 启用了 HDFS 集群的高可用性（HA）：将 HDFS 集群的 **hdfs-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 的 **$BE_HOME/conf** 路径或每个 CN 的 **$CN_HOME/conf** 路径。
  - 启用了 HDFS 集群的 View File System (ViewFs)：将 HDFS 集群的 **core-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 的 **$BE_HOME/conf** 路径或每个 CN 的 **$CN_HOME/conf** 路径。

> **注意**
>
> 如果在发送查询时返回未知主机错误，您必须将 HDFS 集群节点的主机名和 IP 地址的映射添加到 **/etc/hosts** 路径。

### Kerberos 认证

如果 HDFS 集群或 Hive metastore 启用了 Kerberos 认证，请按以下步骤配置您的 StarRocks 集群：

- 在每个 FE 和每个 BE 或 CN 上运行 `kinit -kt keytab_path principal` 命令，从密钥分发中心 (KDC) 获取票证授予票证 (TGT)。要运行此命令，您必须具有访问 HDFS 集群和 Hive metastore 的权限。请注意，使用此命令访问 KDC 是时间敏感的。因此，您需要使用 cron 定期运行此命令。
- 将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 的 **$BE_HOME/conf/be.conf** 文件或每个 CN 的 **$CN_HOME/conf/cn.conf** 文件中。在此示例中，`/etc/krb5.conf` 是 krb5.conf 文件的保存路径。您可以根据需要修改路径。

## 创建统一 catalog

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
    PaimonCatalogParams,
    KuduCatalogParams
)
```

### 参数

#### catalog_name

统一 catalog 的名称。命名约定如下：

- 名称可以包含字母、数字（0-9）和下划线（_）。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### comment

统一 catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `unified`。

#### MetastoreParams

关于 StarRocks 如何与您的元数据存储集成的一组参数。

##### Hive metastore

如果选择 Hive metastore 作为统一数据源的元数据存储，请按以下方式配置 `MetastoreParams`：

```SQL
"unified.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

> **注意**
>
> 在查询数据之前，您必须将 Hive metastore 节点的主机名和 IP 地址的映射添加到 **/etc/hosts** 路径。否则，StarRocks 在启动查询时可能无法访问 Hive metastore。

下表描述了您需要在 `MetastoreParams` 中配置的参数。

| 参数                      | 必需 | 描述                                                  |
| ------------------------- | ---- | ----------------------------------------------------- |
| unified.metastore.type    | 是   | 您用于统一数据源的元数据存储类型。将值设置为 `hive`。 |
| hive.metastore.uris       | 是   | Hive metastore 的 URI。格式：`thrift://<metastore_IP_address>:<metastore_port>`。如果启用了 Hive metastore 的高可用性（HA），您可以指定多个 metastore URI，并用逗号（`,`）分隔，例如 `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

如果选择 AWS Glue 作为数据源的元数据存储，仅在选择 AWS S3 作为存储时支持，请采取以下操作之一：

- 要选择基于实例配置文件的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "unified.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

下表描述了您需要在 `MetastoreParams` 中配置的参数。

| 参数                             | 必需 | 描述                                                  |
| -------------------------------- | ---- | ----------------------------------------------------- |
| unified.metastore.type           | 是   | 您用于统一数据源的元数据存储类型。将值设置为 `glue`。 |
| aws.glue.use_instance_profile    | 是   | 指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证。有效值：`true` 和 `false`。默认值：`false`。 |
| aws.glue.iam_role_arn            | 否   | 在 AWS Glue Data Catalog 上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS Glue，您必须指定此参数。 |
| aws.glue.region                  | 是   | 您的 AWS Glue Data Catalog 所在的区域。例如：`us-west-1`。 |
| aws.glue.access_key              | 否   | 您的 AWS IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，您必须指定此参数。 |
| aws.glue.secret_key              | 否   | 您的 AWS IAM 用户的密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，您必须指定此参数。 |

有关如何选择访问 AWS Glue 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参阅 [Authentication parameters for accessing AWS Glue](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)。

#### StorageCredentialParams

关于 StarRocks 如何与您的存储系统集成的一组参数。此参数集是可选的。

如果使用 HDFS 作为存储，则无需配置 `StorageCredentialParams`。

如果使用 AWS S3、其他兼容 S3 的存储系统、Microsoft Azure Storage 或 Google GCS 作为存储，则必须配置 `StorageCredentialParams`。

##### AWS S3

如果选择 AWS S3 作为存储，请采取以下操作之一：

- 要选择基于实例配置文件的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                           | 必需 | 描述                                                  |
| ------------------------------ | ---- | ----------------------------------------------------- |
| aws.s3.use_instance_profile    | 是   | 指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。 |
| aws.s3.iam_role_arn            | 否   | 在 AWS S3 存储桶上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS S3，您必须指定此参数。 |
| aws.s3.region                  | 是   | 您的 AWS S3 存储桶所在的区域。例如：`us-west-1`。 |
| aws.s3.access_key              | 否   | 您的 IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，您必须指定此参数。 |
| aws.s3.secret_key              | 否   | 您的 IAM 用户的密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，您必须指定此参数。 |

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参阅 [Authentication parameters for accessing AWS S3](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

##### 兼容 S3 的存储系统

如果选择兼容 S3 的存储系统，例如 MinIO，作为存储，请按以下方式配置 `StorageCredentialParams` 以确保成功集成：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                             | 必需 | 描述                                                  |
| -------------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.enable_ssl                | 是   | 指定是否启用 SSL 连接。<br />有效值：`true` 和 `false`。默认值：`true`。 |
| aws.s3.enable_path_style_access  | 是   | 指定是否启用路径样式访问。<br />有效值：`true` 和 `false`。默认值：`false`。对于 MinIO，您必须将值设置为 `true`。<br />路径样式 URL 使用以下格式：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例如，如果您在 US West (Oregon) 区域创建了一个名为 `DOC-EXAMPLE-BUCKET1` 的存储桶，并且您想要访问该存储桶中的 `alice.jpg` 对象，您可以使用以下路径样式 URL：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                  | 是   | 用于连接到兼容 S3 的存储系统而不是 AWS S3 的端点。 |
| aws.s3.access_key                | 是   | 您的 IAM 用户的访问密钥。 |
| aws.s3.secret_key                | 是   | 您的 IAM 用户的密钥。 |

##### Microsoft Azure Storage

###### Azure Blob Storage

如果选择 Blob Storage 作为存储，请采取以下操作之一：

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                    | **必需** | **描述**                              |
  | --------------------------- | -------- | ------------------------------------- |
  | azure.blob.storage_account  | 是       | 您的 Blob Storage 帐户的用户名。      |
  | azure.blob.shared_key       | 是       | 您的 Blob Storage 帐户的共享密钥。    |

- 要选择 SAS Token 身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                    | **必需** | **描述**                                              |
  | --------------------------- | -------- | ----------------------------------------------------- |
  | azure.blob.storage_account  | 是       | 您的 Blob Storage 帐户的用户名。                      |
  | azure.blob.container        | 是       | 存储数据的 blob 容器的名称。                          |
  | azure.blob.sas_token        | 是       | 用于访问您的 Blob Storage 帐户的 SAS 令牌。           |

###### Azure Data Lake Storage Gen2

如果选择 Data Lake Storage Gen2 作为存储，请采取以下操作之一：

- 要选择托管身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                             | **必需** | **描述**                                              |
  | ------------------------------------ | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_use_managed_identity | 是       | 指定是否启用托管身份验证方法。将值设置为 `true`。     |
  | azure.adls2.oauth2_tenant_id         | 是       | 您要访问的数据的租户 ID。                             |
  | azure.adls2.oauth2_client_id         | 是       | 托管身份的客户端（应用程序）ID。                      |

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                   | **必需** | **描述**                                              |
  | -------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.storage_account | 是       | 您的 Data Lake Storage Gen2 存储帐户的用户名。        |
  | azure.adls2.shared_key     | 是       | 您的 Data Lake Storage Gen2 存储帐户的共享密钥。      |

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                          | **必需** | **描述**                                              |
  | --------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_client_id      | 是       | 服务主体的客户端（应用程序）ID。                      |
  | azure.adls2.oauth2_client_secret  | 是       | 创建的新客户端（应用程序）密钥的值。                  |
  | azure.adls2.oauth2_client_endpoint | 是       | 服务主体或应用程序的 OAuth 2.0 令牌端点（v1）。       |

###### Azure Data Lake Storage Gen1

如果选择 Data Lake Storage Gen1 作为存储，请采取以下操作之一：

- 要选择托管服务身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                            | **必需** | **描述**                                              |
  | ----------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls1.use_managed_service_identity | 是     | 指定是否启用托管服务身份验证方法。将值设置为 `true`。 |

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                     | **必需** | **描述**                                              |
  | ---------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls1.oauth2_client_id | 是       | 服务主体的客户端（应用程序）ID。                      |
  | azure.adls1.oauth2_credential | 是      | 创建的新客户端（应用程序）密钥的值。                  |
  | azure.adls1.oauth2_endpoint  | 是       | 服务主体或应用程序的 OAuth 2.0 令牌端点（v1）。       |

##### Google GCS

如果选择 Google GCS 作为存储，请采取以下操作之一：

- 要选择基于 VM 的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                              | **默认值** | **值示例** | **描述**                                              |
  | ------------------------------------- | ---------- | ---------- | ----------------------------------------------------- |
  | gcp.gcs.use_compute_engine_service_account | false    | true       | 指定是否直接使用绑定到您的 Compute Engine 的服务帐户。 |

- 要选择基于服务帐户的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
  | --------------------------------- | ---------- | ------------------------------------------------- | ----------------------------------------------------- |
  | gcp.gcs.service_account_email     | ""         | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建服务帐户时生成的 JSON 文件中的电子邮件地址。     |
  | gcp.gcs.service_account_private_key_id | ""      | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"        | 在创建服务帐户时生成的 JSON 文件中的私钥 ID。          |
  | gcp.gcs.service_account_private_key | ""       | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | 在创建服务帐户时生成的 JSON 文件中的私钥。             |

- 要选择基于模拟的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  - 使 VM 实例模拟服务帐户：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                              | **默认值** | **值示例** | **描述**                                              |
    | ------------------------------------- | ---------- | ---------- | ----------------------------------------------------- |
    | gcp.gcs.use_compute_engine_service_account | false    | true       | 指定是否直接使用绑定到您的 Compute Engine 的服务帐户。 |
    | gcp.gcs.impersonation_service_account | ""         | "hello"    | 您要模拟的服务帐户。                                  |

  - 使服务帐户（暂时命名为元服务帐户）模拟另一个服务帐户（暂时命名为数据服务帐户）：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    下表描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
    | --------------------------------- | ---------- | ------------------------------------------------- | ----------------------------------------------------- |
    | gcp.gcs.service_account_email     | ""         | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建元服务帐户时生成的 JSON 文件中的电子邮件地址。   |
    | gcp.gcs.service_account_private_key_id | ""      | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"        | 在创建元服务帐户时生成的 JSON 文件中的私钥 ID。        |
    | gcp.gcs.service_account_private_key | ""       | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n" | 在创建元服务帐户时生成的 JSON 文件中的私钥。           |
    | gcp.gcs.impersonation_service_account | ""      | "hello"                                           | 您要模拟的数据服务帐户。                               |

#### MetadataUpdateParams

关于 StarRocks 如何更新 Hive、Hudi 和 Delta Lake 的缓存元数据的一组参数。此参数集是可选的。有关从 Hive、Hudi 和 Delta Lake 更新缓存元数据的策略的更多信息，请参阅 [Hive catalog](../../data_source/catalog/hive_catalog.md)、[Hudi catalog](../../data_source/catalog/hudi_catalog.md) 和 [Delta Lake catalog](../../data_source/catalog/deltalake_catalog.md)。

在大多数情况下，您可以忽略 `MetadataUpdateParams`，无需调整其中的策略参数，因为这些参数的默认值已经为您提供了开箱即用的性能。

然而，如果 Hive、Hudi 或 Delta Lake 中的数据更新频率较高，您可以调整这些参数以进一步优化自动异步更新的性能。

| 参数                                  | 必需 | 描述                                                  |
| ------------------------------------- | ---- | ----------------------------------------------------- |
| enable_metastore_cache                | 否   | 指定 StarRocks 是否缓存 Hive、Hudi 或 Delta Lake 表的元数据。有效值：`true` 和 `false`。默认值：`true`。值 `true` 启用缓存，值 `false` 禁用缓存。 |
| enable_remote_file_cache              | 否   | 指定 StarRocks 是否缓存 Hive、Hudi 或 Delta Lake 表或分区的底层数据文件的元数据。有效值：`true` 和 `false`。默认值：`true`。值 `true` 启用缓存，值 `false` 禁用缓存。 |
| metastore_cache_refresh_interval_sec  | 否   | StarRocks 异步更新缓存的 Hive、Hudi 或 Delta Lake 表或分区元数据的时间间隔。单位：秒。默认值：`7200`，即 2 小时。 |
| remote_file_cache_refresh_interval_sec | 否  | StarRocks 异步更新缓存的 Hive、Hudi 或 Delta Lake 表或分区的底层数据文件元数据的时间间隔。单位：秒。默认值：`60`。 |
| metastore_cache_ttl_sec               | 否   | StarRocks 自动丢弃缓存的 Hive、Hudi 或 Delta Lake 表或分区元数据的时间间隔。单位：秒。默认值：`86400`，即 24 小时。 |
| remote_file_cache_ttl_sec             | 否   | StarRocks 自动丢弃缓存的 Hive、Hudi 或 Delta Lake 表或分区的底层数据文件元数据的时间间隔。单位：秒。默认值：`129600`，即 36 小时。 |

#### PaimonCatalogParams

关于如何连接 Paimon Catalog 的一组参数。此参数集是可选的。

| 参数                        | 必需 | 描述                                     |
|-----------------------------|------|------------------------------------------|
| paimon.catalog.warehouse    | 否   | 您的 Paimon 数据的仓库存储路径。         |

#### KuduCatalogParams

关于如何连接 Kudu Catalog 的一组参数。此参数集是可选的。

| 参数                              | 必需 | 描述                                                  |
| --------------------------------- | ---- | ----------------------------------------------------- |
| kudu.master                       | 否   | 指定 Kudu Master 地址，默认为 `localhost:7051`。      |
| kudu.schema-emulation.enabled     | 否   | 选项以启用或禁用 `schema` 仿真。默认情况下，它是关闭的（false），这意味着所有表都属于 `default` `schema`。 |
| kudu.schema-emulation.prefix      | 否   | `schema` 仿真的前缀应仅在 `kudu.schema-emulation.enabled` = `true` 时设置。默认前缀为空字符串：` `。 |

### 示例

以下示例创建一个名为 `unified_catalog_hms` 或 `unified_catalog_glue` 的统一 catalog，具体取决于您使用的元数据存储类型，以从您的统一数据源查询数据。

#### HDFS

如果使用 HDFS 作为存储，请运行如下命令：

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

##### 基于实例配置文件的身份验证

- 如果使用 Hive metastore，请运行如下命令：

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

- 如果使用 AWS Glue 和 Amazon EMR，请运行如下命令：

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

##### 基于假设角色的身份验证

- 如果使用 Hive metastore，请运行如下命令：

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

- 如果使用 AWS Glue 和 Amazon EMR，请运行如下命令：

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

##### 基于 IAM 用户的身份验证

- 如果使用 Hive metastore，请运行如下命令：

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

- 如果使用 AWS Glue 和 Amazon EMR，请运行如下命令：

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

#### 兼容 S3 的存储系统

以 MinIO 为例。运行如下命令：

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

- 如果选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG unified_catalog_hms
  PROPERTIES
  (
      "type" = "unified",
      "unified.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- 如果选择 SAS Token 身份验证方法，请运行如下命令：

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

- 如果选择托管服务身份验证方法，请运行如下命令：

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

- 如果选择服务主体身份验证方法，请运行如下命令：

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

- 如果选择托管身份验证方法，请运行如下命令：

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

- 如果选择共享密钥身份验证方法，请运行如下命令：

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

- 如果选择服务主体身份验证方法，请运行如下命令：

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

- 如果选择基于 VM 的身份验证方法，请运行如下命令：

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

- 如果选择基于服务帐户的身份验证方法，请运行如下命令：

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

- 如果选择基于模拟的身份验证方法：

  - 如果使 VM 实例模拟服务帐户，请运行如下命令：

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

  - 如果使服务帐户模拟另一个服务帐户，请运行如下命令：

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

## 查看统一 catalog

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前 StarRocks 集群中的所有 catalog：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询外部 catalog 的创建语句。以下示例查询名为 `unified_catalog_glue` 的统一 catalog 的创建语句：

```SQL
SHOW CREATE CATALOG unified_catalog_glue;
```

## 切换到统一 catalog 和其中的数据库

您可以使用以下方法之一切换到统一 catalog 和其中的数据库：

- 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 在当前会话中指定一个统一 catalog，然后使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定一个活动数据库：

  ```SQL
  -- 在当前会话中切换到指定的 catalog：
  SET CATALOG <catalog_name>
  -- 在当前会话中指定活动数据库：
  USE <db_name>
  ```

- 直接使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 切换到统一 catalog 和其中的数据库：

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## 删除统一 catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除外部 catalog。

以下示例删除名为 `unified_catalog_glue` 的统一 catalog：

```SQL
DROP CATALOG unified_catalog_glue;
```

## 查看统一 catalog 中表的模式

您可以使用以下语法之一查看统一 catalog 中表的模式：

- 查看模式

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- 从 CREATE 语句中查看模式和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## 从统一 catalog 查询数据

要从统一 catalog 查询数据，请按照以下步骤操作：

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看与统一 catalog 关联的统一数据源中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [切换到 Hive Catalog 和其中的数据库](#switch-to-a-unified-catalog-and-a-database-in-it)。

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## 从 Hive、Iceberg、Hudi、Delta Lake 或 Kudu 导入数据

您可以使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将 Hive、Iceberg、Hudi、Delta Lake 或 Kudu 表的数据导入到在统一 catalog 中创建的 StarRocks 表中。

以下示例将 Hive 表 `hive_table` 的数据导入到在统一 catalog `unified_catalog` 所属的数据库 `test_database` 中创建的 StarRocks 表 `test_tbl` 中：

```SQL
INSERT INTO unified_catalog.test_database.test_table SELECT * FROM hive_table
```

## 在统一 catalog 中创建数据库

与 StarRocks 的内部 catalog 类似，如果您在统一 catalog 上具有 CREATE DATABASE 权限，您可以使用 CREATE DATABASE 语句在该 catalog 中创建数据库。

> **注意**
>
> 您可以使用 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 授予和撤销权限。

StarRocks 仅支持在统一 catalog 中创建 Hive 和 Iceberg 数据库。

[切换到统一 catalog](#switch-to-a-unified-catalog-and-a-database-in-it)，然后使用以下语句在该 catalog 中创建数据库：

```SQL
CREATE DATABASE <database_name>
[properties ("location" = "<prefix>://<path_to_database>/<database_name.db>")]
```

`location` 参数指定您要创建数据库的文件路径，可以在 HDFS 或云存储中。

- 当您使用 Hive metastore 作为数据源的元数据存储时，如果在创建数据库时未指定该参数，`location` 参数默认为 `<warehouse_location>/<database_name.db>`，这是 Hive metastore 支持的。
- 当您使用 AWS Glue 作为数据源的元数据存储时，`location` 参数没有默认值，因此您必须在创建数据库时指定该参数。

`prefix` 根据您使用的存储系统而有所不同：

| **存储系统**                                             | **`Prefix`** **值**                                       |
| -------------------------------------------------------- | --------------------------------------------------------- |
| HDFS                                                     | `hdfs`                                                    |
| Google GCS                                               | `gs`                                                      |
| Azure Blob Storage                                       | <ul><li>如果您的存储帐户允许通过 HTTP 访问，`prefix` 为 `wasb`。</li><li>如果您的存储帐户允许通过 HTTPS 访问，`prefix` 为 `wasbs`。</li></ul> |
| Azure Data Lake Storage Gen1                             | `adl`                                                     |
| Azure Data Lake Storage Gen2                             | <ul><li>如果您的存储帐户允许通过 HTTP 访问，`prefix` 为 `abfs`。</li><li>如果您的存储帐户允许通过 HTTPS 访问，`prefix` 为 `abfss`。</li></ul> |
| AWS S3 或其他兼容 S3 的存储（例如 MinIO）                | `s3`                                                      |

## 从统一 catalog 中删除数据库

与 StarRocks 的内部数据库类似，如果您在统一 catalog 中创建的数据库上具有 [DROP](../../administration/user_privs/user_privs.md#database) 权限，您可以使用 [DROP DATABASE](../../sql-reference/sql-statements/Database/DROP_DATABASE.md) 语句删除该数据库。您只能删除空数据库。

> **注意**
>
> 您可以使用 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 授予和撤销权限。

StarRocks 仅支持从统一 catalog 中删除 Hive 和 Iceberg 数据库。

当您从统一 catalog 中删除数据库时，数据库在您的 HDFS 集群或云存储上的文件路径不会随数据库一起删除。

[切换到统一 catalog](#switch-to-a-unified-catalog-and-a-database-in-it)，然后使用以下语句在该 catalog 中删除数据库：

```SQL
DROP DATABASE <database_name>
```

## 在统一 catalog 中创建表

与 StarRocks 的内部数据库类似，如果您在统一 catalog 中创建的数据库上具有 [CREATE TABLE](../../administration/user_privs/user_privs.md#database) 权限，您可以使用 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 或 [CREATE TABLE AS SELECT ../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.mdELECT.md) 语句在该数据库中创建表。

> **注意**
>
> 您可以使用 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 授予和撤销权限。

StarRocks 仅支持在统一 catalog 中创建 Hive 和 Iceberg 表。

[切换到 Hive Catalog 和其中的数据库](#switch-to-a-unified-catalog-and-a-database-in-it)。然后，使用 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) 在该数据库中创建 Hive 或 Iceberg 表：

```SQL
CREATE TABLE <table_name>
(column_definition1[, column_definition2, ...]
ENGINE = {|hive|iceberg}
[partition_desc]
```

有关更多信息，请参阅 [Create a Hive table](../catalog/hive_catalog.md#create-a-hive-table) 和 [Create an Iceberg table](./iceberg/iceberg_catalog.md#create-an-iceberg-table)。

以下示例创建一个名为 `hive_table` 的 Hive 表。该表由三列 `action`、`id` 和 `dt` 组成，其中 `id` 和 `dt` 是分区列。

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

## 将数据下沉到统一 catalog 中的表

与 StarRocks 的内部表类似，如果您在统一 catalog 中创建的表上具有 [INSERT](../../administration/user_privs/user_privs.md#table) 权限，您可以使用 [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 语句将 StarRocks 表的数据下沉到该统一 catalog 表（目前仅支持 Parquet 格式的统一 catalog 表）。

> **注意**
>
> 您可以使用 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 授予和撤销权限。

StarRocks 仅支持将数据下沉到统一 catalog 中的 Hive 和 Iceberg 表。

[切换到 Hive Catalog 和其中的数据库](#switch-to-a-unified-catalog-and-a-database-in-it)。然后，使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将数据插入到该数据库中的 Hive 或 Iceberg 表：

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 如果要将数据下沉到指定分区，请使用以下语法：
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

有关更多信息，请参阅 [Sink data to a Hive table ](../catalog/hive_catalog.md#sink-data-to-a-hive-table) 和 [Sink data to an Iceberg table](./iceberg/iceberg_catalog.md#sink-data-to-an-iceberg-table)。

以下示例将三行数据插入到名为 `hive_table` 的 Hive 表中：

```SQL
INSERT INTO hive_table
VALUES
    ("buy", 1, "2023-09-01"),
    ("sell", 2, "2023-09-02"),
    ("buy", 3, "2023-09-03");
```

## 从统一 catalog 中删除表

与 StarRocks 的内部表类似，如果您在统一 catalog 中创建的表上具有 [DROP](../../administration/user_privs/user_privs.md#table) 权限，您可以使用 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 语句删除该表。

> **注意**
>
> 您可以使用 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md) 授予和撤销权限。

StarRocks 仅支持从统一 catalog 中删除 Hive 和 Iceberg 表。

[切换到 Hive Catalog 和其中的数据库](#switch-to-a-unified-catalog-and-a-database-in-it)。然后，使用 [DROP TABLE](../../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md) 删除该数据库中的 Hive 或 Iceberg 表：

```SQL
DROP TABLE <table_name>
```

有关更多信息，请参阅 [Drop a Hive table](../catalog/hive_catalog.md#drop-a-hive-table) 和 [Drop an Iceberg table](./iceberg/iceberg_catalog.md#drop-an-iceberg-table)。

以下示例删除名为 `hive_table` 的 Hive 表：

```SQL
DROP TABLE hive_table FORCE
```