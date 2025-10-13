---
displayed_sidebar: docs
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

您只能使用 Paimon catalog 查询数据。您不能使用 Paimon catalog 删除、删除或插入数据到您的 Paimon 集群中。

## Paimon to StarRocks data types

| Paimon Type         | StarRocks Type            |
|---------------------|---------------------------|
| BINARY	            | VARBINARY                 |
| VARBINARY	          | VARBINARY                 |
| CHAR	              | CHAR(length)              |
| VARCHAR	            | VARCHAR                   |
| BOOLEAN	            | BOOLEAN                   |
| DECIMAL	            | DECIMAL(precision, scale) |
| TINYINT	            | TINYINT                   |
| SMALLINT	          | SMALLINT                  |
| INT	                | INT                       |
| BIGINT	            | BIGINT                    |
| FLOAT	              | FLOAT                     |
| DOUBLE	            | DOUBLE                    |
| DATE	              | DATE                      |
| TIME	              | TIME                      |
| TIMESTAMP	          | DATETIME                  |
| LocalZonedTimestamp	| DATETIME                  |
| ARRAY	              | ARRAY<element_type>       |
| MAP	                | MAP<key_type, value_type> |
| ROW/STRUCT	        | STRUCT<field1:type1, ...> |

## 集成准备

在创建 Paimon catalog 之前，请确保您的 StarRocks 集群可以与 Paimon 集群的存储系统和元存储集成。

### AWS IAM

如果您的 Paimon 集群使用 AWS S3 作为存储，请选择合适的身份验证方法并进行必要的准备，以确保您的 StarRocks 集群可以访问相关的 AWS 云资源。

推荐以下身份验证方法：

- 实例配置文件（推荐）
- 假设角色
- IAM 用户

在上述三种身份验证方法中，实例配置文件是最广泛使用的。

有关更多信息，请参见 [AWS IAM 中的身份验证准备](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication)。

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

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS S3 的身份验证参数](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

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

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## 从 Paimon 导入数据

假设您有一个名为 `olap_tbl` 的 OLAP 表，您可以按如下方式转换和导入数据：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM paimon_table;
```