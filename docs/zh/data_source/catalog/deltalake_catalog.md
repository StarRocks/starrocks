---
description: 直接从 Delta Lake 查询数据
displayed_sidebar: docs
---
import Intro from '../../_assets/catalog/_deltalake_intro.mdx'
import DatabricksParams from '../../_assets/catalog/_databricks_params.mdx'

# Delta Lake catalog

<Intro />

## 使用说明

- StarRocks 支持的 Delta Lake 文件格式是 Parquet。Parquet 文件支持以下压缩格式：SNAPPY、LZ4、ZSTD、GZIP 和 NO_COMPRESSION。
- StarRocks 不支持 Delta Lake 的数据类型为 MAP 和 STRUCT。

## 集成准备

在创建 Delta Lake catalog 之前，请确保您的 StarRocks 集群可以与 Delta Lake 集群的存储系统和元存储集成。

### AWS IAM

如果您的 Delta Lake 集群使用 AWS S3 作为存储或 AWS Glue 作为元存储，请选择适合的身份验证方法并进行必要的准备，以确保您的 StarRocks 集群可以访问相关的 AWS 云资源。

推荐以下身份验证方法：

- 实例配置文件
- 假设角色
- IAM 用户

在上述三种身份验证方法中，实例配置文件是最广泛使用的。

有关更多信息，请参见 [AWS IAM 中的身份验证准备](../../integrations/authenticate_to_aws_resources.md#preparation-for-iam-user-based-authentication)。

### HDFS

如果选择 HDFS 作为存储，请按以下步骤配置您的 StarRocks 集群：

- （可选）设置用于访问 HDFS 集群和 Hive 元存储的用户名。默认情况下，StarRocks 使用 FE 和 BE 或 CN 进程的用户名访问 HDFS 集群和 Hive 元存储。您也可以通过在每个 FE 的 **fe/conf/hadoop_env.sh** 文件和每个 BE 或 CN 的 **be/conf/hadoop_env.sh** 或 **cn/conf/hadoop_env.sh** 文件开头添加 `export HADOOP_USER_NAME="<user_name>"` 来设置用户名。设置完成后，重启每个 FE 和每个 BE 或 CN 以使参数设置生效。每个 StarRocks 集群只能设置一个用户名。
- 当您查询 Delta Lake 数据时，StarRocks 集群的 FEs 和 BEs 或 CNs 使用 HDFS 客户端访问 HDFS 集群。在大多数情况下，您无需配置 StarRocks 集群即可实现此目的，StarRocks 使用默认配置启动 HDFS 客户端。仅在以下情况下需要配置 StarRocks 集群：

  - 启用了 HDFS 集群的高可用性 (HA)：将 HDFS 集群的 **hdfs-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 或 CN 的 **$BE_HOME/conf** 或 **$CN_HOME/conf** 路径。
  - 启用了 HDFS 集群的 View File System (ViewFs)：将 HDFS 集群的 **core-site.xml** 文件添加到每个 FE 的 **$FE_HOME/conf** 路径和每个 BE 或 CN 的 **$BE_HOME/conf** 或 **$CN_HOME/conf** 路径。

  :::note
  如果在发送查询时返回未知主机错误，您必须将 HDFS 集群节点的主机名和 IP 地址映射添加到 **/etc/hosts** 路径。
  :::

### Kerberos 认证

如果您的 HDFS 集群或 Hive 元存储启用了 Kerberos 认证，请按以下步骤配置您的 StarRocks 集群：

- 在每个 FE 和每个 BE 或 CN 上运行 `kinit -kt keytab_path principal` 命令，从密钥分发中心 (KDC) 获取票证授予票证 (TGT)。要运行此命令，您必须具有访问 HDFS 集群和 Hive 元存储的权限。请注意，使用此命令访问 KDC 是时间敏感的。因此，您需要使用 cron 定期运行此命令。
- 将 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` 添加到每个 FE 的 **$FE_HOME/conf/fe.conf** 文件和每个 BE 或 CN 的 **$BE_HOME/conf/be.conf** 或 **$CN_HOME/conf/cn.conf** 文件中。在此示例中，`/etc/krb5.conf` 是 **krb5.conf** 文件的保存路径。您可以根据需要修改路径。

## 创建 Delta Lake catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "deltalake",
    MetastoreParams,
    StorageCredentialParams,
    MetadataUpdateParams
)
```

### 参数

#### catalog_name

Delta Lake catalog 的名称。命名规则如下：

- 名称可以包含字母、数字 (0-9) 和下划线 (_)。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### comment

Delta Lake catalog 的描述。此参数是可选的。

#### type

数据源的类型。将值设置为 `deltalake`。

#### MetastoreParams

关于 StarRocks 如何与数据源的元存储集成的一组参数。

##### Hive metastore

如果选择 Hive metastore 作为数据源的元存储，请按以下方式配置 `MetastoreParams`：

```SQL
"hive.metastore.type" = "hive",
"hive.metastore.uris" = "<hive_metastore_uri>"
```

:::note
在查询 Delta Lake 数据之前，必须将 Hive 元存储节点的主机名和 IP 地址映射添加到 `/etc/hosts` 路径。否则，StarRocks 在启动查询时可能无法访问 Hive 元存储。
:::

以下表格描述了需要在 `MetastoreParams` 中配置的参数。

| 参数                  | 必需 | 描述                                                  |
| ------------------- | ---- | ----------------------------------------------------- |
| hive.metastore.type | 是   | 您用于 Delta Lake 集群的元存储类型。将值设置为 `hive`。 |
| hive.metastore.uris | 是   | 您的 Hive 元存储的 URI。格式：`thrift://<metastore_IP_address>:<metastore_port>`。<br />如果启用了 Hive 元存储的高可用性 (HA)，可以指定多个元存储 URI，并用逗号 (`,`) 分隔，例如 `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`。 |

##### AWS Glue

如果选择 AWS Glue 作为数据源的元存储，仅在选择 AWS S3 作为存储时支持，请采取以下操作之一：

- 要选择基于实例配置文件的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "true",
  "aws.glue.iam_role_arn" = "<iam_role_arn>",
  "aws.glue.region" = "<aws_glue_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按以下方式配置 `MetastoreParams`：

  ```SQL
  "hive.metastore.type" = "glue",
  "aws.glue.use_instance_profile" = "false",
  "aws.glue.access_key" = "<iam_user_access_key>",
  "aws.glue.secret_key" = "<iam_user_secret_key>",
  "aws.glue.region" = "<aws_s3_region>"
  ```

以下表格描述了需要在 `MetastoreParams` 中配置的参数。

| 参数                           | 必需 | 描述                                                  |
| ----------------------------- | ---- | ----------------------------------------------------- |
| hive.metastore.type           | 是   | 您用于 Delta Lake 集群的元存储类型。将值设置为 `glue`。 |
| aws.glue.use_instance_profile | 是   | 指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。 |
| aws.glue.iam_role_arn         | 否   | 在您的 AWS Glue Data Catalog 上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS Glue，必须指定此参数。 |
| aws.glue.region               | 是   | 您的 AWS Glue Data Catalog 所在的区域。例如：`us-west-1`。 |
| aws.glue.access_key           | 否   | 您的 AWS IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，必须指定此参数。 |
| aws.glue.secret_key           | 否   | 您的 AWS IAM 用户的密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS Glue，必须指定此参数。 |

有关如何选择访问 AWS Glue 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS Glue 的身份验证参数](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-glue)。

<DatabricksParams />

#### StorageCredentialParams

关于 StarRocks 如何与您的存储系统集成的一组参数。此参数集是可选的。

如果使用 HDFS 作为存储，则无需配置 `StorageCredentialParams`。

如果使用 AWS S3、其他 S3 兼容存储系统、Microsoft Azure Storage 或 Google GCS 作为存储，则必须配置 `StorageCredentialParams`。

##### AWS S3

如果选择 AWS S3 作为 Delta Lake 集群的存储，请采取以下操作之一：

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

以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

| 参数                         | 必需 | 描述                                                  |
| --------------------------- | ---- | ----------------------------------------------------- |
| aws.s3.use_instance_profile | 是   | 指定是否启用基于实例配置文件的身份验证方法和基于假设角色的身份验证方法。有效值：`true` 和 `false`。默认值：`false`。 |
| aws.s3.iam_role_arn         | 否   | 在您的 AWS S3 存储桶上具有权限的 IAM 角色的 ARN。如果使用基于假设角色的身份验证方法访问 AWS S3，必须指定此参数。 |
| aws.s3.region               | 是   | 您的 AWS S3 存储桶所在的区域。例如：`us-west-1`。 |
| aws.s3.access_key           | 否   | 您的 IAM 用户的访问密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，必须指定此参数。 |
| aws.s3.secret_key           | 否   | 您的 IAM 用户的密钥。如果使用基于 IAM 用户的身份验证方法访问 AWS S3，必须指定此参数。 |

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS S3 的身份验证参数](../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

##### S3 兼容存储系统

从 v2.5 开始，Delta Lake catalogs 支持 S3 兼容存储系统。

如果选择 S3 兼容存储系统（如 MinIO）作为 Delta Lake 集群的存储，请按以下方式配置 `StorageCredentialParams` 以确保成功集成：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

| 参数                            | 必需 | 描述                                                  |
| ------------------------------ | ---- | ----------------------------------------------------- |
| aws.s3.enable_ssl              | 是   | 指定是否启用 SSL 连接。<br />有效值：`true` 和 `false`。默认值：`true`。 |
| aws.s3.enable_path_style_access| 是   | 指定是否启用路径样式访问。<br />有效值：`true` 和 `false`。默认值：`false`。对于 MinIO，必须将值设置为 `true`。<br />路径样式 URL 使用以下格式：`https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`。例如，如果在美国西部（俄勒冈）区域创建了一个名为 `DOC-EXAMPLE-BUCKET1` 的存储桶，并且要访问该存储桶中的 `alice.jpg` 对象，可以使用以下路径样式 URL：`https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`。 |
| aws.s3.endpoint                | 是   | 用于连接到您的 S3 兼容存储系统而不是 AWS S3 的端点。 |
| aws.s3.access_key              | 是   | 您的 IAM 用户的访问密钥。 |
| aws.s3.secret_key              | 是   | 您的 IAM 用户的密钥。 |

##### Microsoft Azure Storage

从 v3.0 开始，Delta Lake catalogs 支持 Microsoft Azure Storage。

###### Azure Blob Storage

如果选择 Blob Storage 作为 Delta Lake 集群的存储，请采取以下操作之一：

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                    | **必需** | **描述**                              |
  | -------------------------- | -------- | ------------------------------------- |
  | azure.blob.storage_account | 是       | 您的 Blob Storage 账户的用户名。      |
  | azure.blob.shared_key      | 是       | 您的 Blob Storage 账户的共享密钥。    |

- 要选择 SAS 令牌身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                   | **必需** | **描述**                                              |
  | ------------------------- | -------- | ----------------------------------------------------- |
  | azure.blob.storage_account| 是       | 您的 Blob Storage 账户的用户名。                      |
  | azure.blob.container      | 是       | 存储数据的 blob 容器的名称。                          |
  | azure.blob.sas_token      | 是       | 用于访问您的 Blob Storage 账户的 SAS 令牌。           |

###### Azure Data Lake Storage Gen2

如果选择 Data Lake Storage Gen2 作为 Delta Lake 集群的存储，请采取以下操作之一：

- 要选择托管身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                               | **必需** | **描述**                                              |
  | ------------------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.oauth2_use_managed_identity | 是       | 指定是否启用托管身份验证方法。将值设置为 `true`。     |
  | azure.adls2.oauth2_tenant_id          | 是       | 您要访问数据的租户的 ID。                             |
  | azure.adls2.oauth2_client_id          | 是       | 托管身份的客户端（应用程序）ID。                      |

- 要选择共享密钥身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                   | **必需** | **描述**                                              |
  | ------------------------- | -------- | ----------------------------------------------------- |
  | azure.adls2.storage_account | 是       | 您的 Data Lake Storage Gen2 存储账户的用户名。        |
  | azure.adls2.shared_key      | 是       | 您的 Data Lake Storage Gen2 存储账户的共享密钥。      |

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                      | **必需** | **描述**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | 是          | 服务主体的客户端（应用程序）ID。        |
  | azure.adls2.oauth2_client_secret   | 是          | 新创建的客户端（应用程序）密钥的值。    |
  | azure.adls2.oauth2_client_endpoint | 是          | 服务主体或应用程序的 OAuth 2.0 令牌端点 (v1)。 |

###### Azure Data Lake Storage Gen1

如果选择 Data Lake Storage Gen1 作为 Delta Lake 集群的存储，请采取以下操作之一：

- 要选择托管服务身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                            | **必需** | **描述**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | 是          | 指定是否启用托管服务身份验证方法。将值设置为 `true`。 |

- 要选择服务主体身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                 | **必需** | **描述**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | 是          | 服务主体的客户端（应用程序）ID。        |
  | azure.adls1.oauth2_credential | 是          | 新创建的客户端（应用程序）密钥的值。    |
  | azure.adls1.oauth2_endpoint   | 是          | 服务主体或应用程序的 OAuth 2.0 令牌端点 (v1)。 |

##### Google GCS

从 v3.0 开始，Delta Lake catalogs 支持 Google GCS。

如果选择 Google GCS 作为 Delta Lake 集群的存储，请采取以下操作之一：

- 要选择基于 VM 的身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                              | **默认值** | **值示例** | **描述**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。 |

- 要选择服务账户身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>",
  ```

  以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建服务账户时生成的 JSON 文件中的电子邮件地址。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建服务账户时生成的 JSON 文件中的私钥 ID。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 在创建服务账户时生成的 JSON 文件中的私钥。 |

- 要选择模拟身份验证方法，请按以下方式配置 `StorageCredentialParams`：

  - 使 VM 实例模拟服务账户：
  
    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                              | **默认值** | **值示例** | **描述**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | 您要模拟的服务账户。            |

  - 使服务账户（暂时命名为元服务账户）模拟另一个服务账户（暂时命名为数据服务账户）：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下表格描述了需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | "[user@hello.iam.gserviceaccount.com](mailto:user@hello.iam.gserviceaccount.com)" | 在创建元服务账户时生成的 JSON 文件中的电子邮件地址。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建元服务账户时生成的 JSON 文件中的私钥 ID。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 在创建元服务账户时生成的 JSON 文件中的私钥。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | 您要模拟的数据服务账户。       |

#### MetadataUpdateParams

关于 StarRocks 如何更新 Delta Lake 缓存元数据的一组参数。此参数集是可选的。

从 v3.3.3 开始，Delta Lake Catalog 支持 [元数据本地缓存和检索](#appendix-metadata-local-cache-and-retrieval)。在大多数情况下，您可以忽略 `MetadataUpdateParams`，不需要调整其中的策略参数，因为这些参数的默认值已经为您提供了开箱即用的性能。

然而，如果 Delta Lake 中的数据更新频率较高，您可以调整这些参数以进一步优化自动异步更新的性能。

:::note
在大多数情况下，如果 Delta Lake 数据的更新粒度为 1 小时或更短，则数据更新频率被视为较高。
:::

| **参数**                                      | **单位** | **默认值** | **描述**                                |
|----------------------------------------------------| -------- | -------------|----------------------------------------------- |
| enable_deltalake_table_cache                       | -        | true         | 是否在 Delta Lake 的元数据缓存中启用表缓存。 |
| enable_deltalake_json_meta_cache                   | -        | true         | 是否为 Delta Log JSON 文件启用缓存。 |
| deltalake_json_meta_cache_ttl_sec                  | 秒       | 48 * 60 * 60 | Delta Log JSON 文件缓存的生存时间 (TTL)。 |
| deltalake_json_meta_cache_memory_usage_ratio       | -        | 0.1          | Delta Log JSON 文件缓存占用的 JVM 堆内存的最大比例。 |
| enable_deltalake_checkpoint_meta_cache             | -        | true         | 是否为 Delta Log Checkpoint 文件启用缓存。 |
| deltalake_checkpoint_meta_cache_ttl_sec            | 秒       | 48 * 60 * 60 | Delta Log Checkpoint 文件缓存的生存时间 (TTL)。  |
| deltalake_checkpoint_meta_cache_memory_usage_ratio | -        | 0.1          | Delta Log Checkpoint 文件缓存占用的 JVM 堆内存的最大比例。 |

### 示例

以下示例创建一个名为 `deltalake_catalog_hms` 或 `deltalake_catalog_glue` 的 Delta Lake catalog，具体取决于您使用的元存储类型，以查询 Delta Lake 集群中的数据。

#### HDFS

如果使用 HDFS 作为存储，请运行如下命令：

```SQL
CREATE EXTERNAL CATALOG deltalake_catalog_hms
PROPERTIES
(
    "type" = "deltalake",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

#### AWS S3

##### 如果选择基于实例配置文件的凭证

- 如果在 Delta Lake 集群中使用 Hive 元存储，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Delta Lake 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果选择基于假设角色的凭证

- 如果在 Delta Lake 集群中使用 Hive 元存储，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Delta Lake 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2"
  );
  ```

##### 如果选择基于 IAM 用户的凭证

- 如果在 Delta Lake 集群中使用 Hive 元存储，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2"
  );
  ```

- 如果在 Amazon EMR Delta Lake 集群中使用 AWS Glue，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_glue
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "glue",
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

#### S3 兼容存储系统

以 MinIO 为例。运行如下命令：

```SQL
CREATE EXTERNAL CATALOG deltalake_catalog_hms
PROPERTIES
(
    "type" = "deltalake",
    "hive.metastore.type" = "hive",
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
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.shared_key" = "<blob_storage_account_shared_key>"
  );
  ```

- 如果选择 SAS 令牌身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.blob.storage_account" = "<blob_storage_account_name>",
      "azure.blob.container" = "<blob_container_name>",
      "azure.blob.sas_token" = "<blob_storage_account_SAS_token>"
  );
  ```

##### Azure Data Lake Storage Gen1

- 如果选择托管服务身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.use_managed_service_identity" = "true"    
  );
  ```

- 如果选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls1.oauth2_client_id" = "<application_client_id>",
      "azure.adls1.oauth2_credential" = "<application_client_credential>",
      "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  );
  ```

##### Azure Data Lake Storage Gen2

- 如果选择托管身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_use_managed_identity" = "true",
      "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
      "azure.adls2.oauth2_client_id" = "<service_client_id>"
  );
  ```

- 如果选择共享密钥身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.storage_account" = "<storage_account_name>",
      "azure.adls2.shared_key" = "<shared_key>"     
  );
  ```

- 如果选择服务主体身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "azure.adls2.oauth2_client_id" = "<service_client_id>",
      "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
      "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  );
  ```

#### Google GCS

- 如果选择基于 VM 的身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.use_compute_engine_service_account" = "true"    
  );
  ```

- 如果选择服务账户身份验证方法，请运行如下命令：

  ```SQL
  CREATE EXTERNAL CATALOG deltalake_catalog_hms
  PROPERTIES
  (
      "type" = "deltalake",
      "hive.metastore.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "gcp.gcs.service_account_email" = "<google_service_account_email>",
      "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
      "gcp.gcs.service_account_private_key" = "<google_service_private_key>"    
  );
  ```

- 如果选择模拟身份验证方法：

  - 如果让 VM 实例模拟服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG deltalake_catalog_hms
    PROPERTIES
    (
        "type" = "deltalake",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.use_compute_engine_service_account" = "true",
        "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"    
    );
    ```

  - 如果让服务账户模拟另一个服务账户，请运行如下命令：

    ```SQL
    CREATE EXTERNAL CATALOG deltalake_catalog_hms
    PROPERTIES
    (
        "type" = "deltalake",
        "hive.metastore.type" = "hive",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
        "gcp.gcs.service_account_email" = "<google_service_account_email>",
        "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
        "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
        "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"    
    );
    ```

## 查看 Delta Lake catalogs

您可以使用 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前 StarRocks 集群中的所有 catalogs：

```SQL
SHOW CATALOGS;
```

您还可以使用 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询外部 catalog 的创建语句。以下示例查询名为 `deltalake_catalog_glue` 的 Delta Lake catalog 的创建语句：

```SQL
SHOW CREATE CATALOG deltalake_catalog_glue;
```

## 切换到 Delta Lake Catalog 及其中的数据库

您可以使用以下方法之一切换到 Delta Lake catalog 及其中的数据库：

- 使用 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 指定当前会话中的 Delta Lake catalog，然后使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定活动数据库：

  ```SQL
  -- 切换到当前会话中的指定 catalog：
  SET CATALOG <catalog_name>
  -- 指定当前会话中的活动数据库：
  USE <db_name>
  ```

- 直接使用 [USE](../../sql-reference/sql-statements/Database/USE.md) 切换到 Delta Lake catalog 及其中的数据库：

  ```SQL
  USE <catalog_name>.<db_name>
  ```

## 删除 Delta Lake catalog

您可以使用 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除外部 catalog。

以下示例删除名为 `deltalake_catalog_glue` 的 Delta Lake catalog：

```SQL
DROP Catalog deltalake_catalog_glue;
```

## 查看 Delta Lake 表的模式

您可以使用以下语法之一查看 Delta Lake 表的模式：

- 查看模式

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>
  ```

- 从 CREATE 语句中查看模式和位置

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>
  ```

## 查询 Delta Lake 表

1. 使用 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看 Delta Lake 集群中的数据库：

   ```SQL
   SHOW DATABASES FROM <catalog_name>
   ```

2. [切换到 Delta Lake Catalog 及其中的数据库](#switch-to-a-delta-lake-catalog-and-a-database-in-it)。

3. 使用 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) 查询指定数据库中的目标表：

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10
   ```

## 从 Delta Lake 导入数据

假设您有一个名为 `olap_tbl` 的 OLAP 表，可以按如下方式转换和导入数据：

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM deltalake_table
```

## 配置元数据缓存和更新策略

从 v3.3.3 开始，Delta Lake Catalog 支持 [元数据本地缓存和检索](#appendix-metadata-local-cache-and-retrieval)。

您可以通过以下 FE 参数配置 Delta Lake 元数据缓存刷新：

| **配置项**                                       | **默认值** | **描述**                                               |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------- |
| enable_background_refresh_connector_metadata                 | `true`      | 是否启用周期性的 Delta Lake 元数据缓存刷新。启用后，StarRocks 会轮询 Delta Lake 集群的元存储（Hive Metastore 或 AWS Glue），并刷新频繁访问的 Delta Lake catalogs 的缓存元数据，以感知数据变化。`true` 表示启用 Delta Lake 元数据缓存刷新，`false` 表示禁用。 |
| background_refresh_metadata_interval_millis                  | `600000`    | 两次连续 Delta Lake 元数据缓存刷新之间的间隔。单位：毫秒。 |
| background_refresh_metadata_time_secs_since_last_access_secs | `86400`     | Delta Lake 元数据缓存刷新任务的过期时间。对于已访问的 Delta Lake catalog，如果超过指定时间未访问，StarRocks 将停止刷新其缓存元数据。对于未访问的 Delta Lake catalog，StarRocks 不会刷新其缓存元数据。单位：秒。 |

## 附录：元数据本地缓存和检索

由于元数据文件的重复解压和解析可能引入不必要的延迟，StarRocks 采用了一种新的元数据缓存策略，将反序列化的内存对象缓存起来。通过将这些反序列化的文件存储在内存中，系统可以绕过后续查询的解压和解析阶段。这种缓存机制允许直接访问所需的元数据，显著减少检索时间。因此，系统变得更加响应，能够更好地满足高查询需求和物化视图重写需求。

您可以通过 Catalog 属性 [MetadataUpdateParams](#metadataupdateparams) 和 [相关配置项](#configure-metadata-cache-and-update-strategy) 配置此行为。

## 功能支持

目前，Delta Lake catalogs 支持以下表功能：

- V2 Checkpoint（从 v3.3.0 开始）
- 无时区的时间戳（从 v3.3.1 开始）
- 列映射（从 v3.3.6 开始）
- Deletion Vector（从 v3.4.1 开始）