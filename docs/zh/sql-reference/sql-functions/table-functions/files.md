---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# `FILES`

定义远端存储中的数据文件，用于数据导入和导出：

- [从远端存储系统导入或查询数据](#files-for-loading)
- [将数据导出到远端存储系统](#files-for-unloading)

`FILES()` 支持以下数据源和文件格式：

- **数据源：**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - 其他 S3 兼容存储系统
  - Microsoft Azure Blob Storage
  - NFS(NAS)
- **文件格式：**
  - Parquet
  - ORC (从 v3.3 开始支持)
  - CSV (从 v3.3 开始支持)
  - Avro (从 v3.4.4 开始支持，仅用于导入)

从 v3.2 开始，FILES() 进一步支持复杂数据类型，包括 `ARRAY`、`JSON`、`MAP` 和 `STRUCT`，以及基本数据类型。

## `FILES()` 用于导入

从 v3.1.0 开始，StarRocks 支持使用表函数 `FILES()` 定义远端存储中的只读文件。它可以通过文件的路径相关属性访问远端存储，推断文件中的表结构，并返回数据行。您可以直接使用 [`SELECT`](../../sql-statements/table_bucket_part_index/SELECT.md) 查询数据行，使用 [`INSERT`](../../sql-statements/loading_unloading/INSERT.md) 将数据行导入到现有表中，或使用 [`CREATE TABLE AS SELECT`](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 创建新表并将数据行导入其中。从 v3.3.4 开始，您还可以使用 `FILES()` 和 [`DESC`](../../sql-statements/table_bucket_part_index/DESCRIBE.md) 查看数据文件的结构。

### 语法

```SQL
FILES( data_location , [data_format] [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] [, list_files_only ] [, list_recursively])
```

### 参数

所有参数均为 `"key" = "value"` 对。

#### `data_location`

用于访问文件的 URI。

您可以指定路径或文件。例如，您可以将此参数指定为 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"`，以从 HDFS 服务器上的路径 `/user/data/tablename` 加载名为 `20210411` 的数据文件。

您还可以使用通配符 `?`、`*`、`[]`、`{}` 或 `^` 指定多个数据文件的保存路径。例如，您可以将此参数指定为 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` 或 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"`，以从 HDFS 服务器上的路径 `/user/data/tablename` 加载所有分区或仅 `202104` 分区的数据文件。

:::note

通配符也可以用于指定中间路径。

:::

- 要访问 HDFS，您需要将此参数指定为：

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- 示例: "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- 要访问 AWS S3：

  - 如果您使用 S3 协议，您需要将此参数指定为：

    ```SQL
    "path" = "s3://<s3_path>"
    -- 示例: "path" = "s3://path/file.parquet"
    ```

  - 如果您使用 S3A 协议，您需要将此参数指定为：

    ```SQL
    "path" = "s3a://<s3_path>"
    -- 示例: "path" = "s3a://path/file.parquet"
    ```

- 要访问 Google Cloud Storage，您需要将此参数指定为：

  ```SQL
  "path" = "s3a://<gcs_path>"
  -- 示例: "path" = "s3a://path/file.parquet"
  ```

- 要访问 Azure Blob Storage：

  - 如果您的存储账户允许通过 HTTP 访问，您需要将此参数指定为：

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 示例: "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```
  
  - 如果您的存储账户允许通过 HTTPS 访问，您需要将此参数指定为：

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 示例: "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

- 要访问 NFS(NAS)：

  ```SQL
  "path" = "file:///<absolute_path>"
  -- 示例: "path" = "file:///home/ubuntu/parquetfile/file.parquet"
  ```

  :::note

  要通过 `file://` 协议访问 NFS 中的文件，您需要将 NAS 设备挂载为 NFS，并放置在每个 BE 或 CN 节点的相同目录下。

  :::

#### `data_format`

数据文件的格式。有效值：
- `parquet`
- `orc` (从 v3.3 开始支持)
- `csv` (从 v3.3 开始支持)
- `avro` (从 v3.4.4 开始支持，仅用于导入)

您必须为特定数据文件格式设置详细选项。

当 `list_files_only` 设置为 `true` 时，您无需指定 `data_format`。

##### Parquet

Parquet 格式示例：

```SQL
"format"="parquet",
"parquet.use_legacy_encoding" = "true",   -- 仅用于导出
"parquet.version" = "2.6"                 -- 仅用于导出
```

###### `parquet.use_legacy_encoding`

控制用于 DATETIME 和 DECIMAL 数据类型的编码技术。有效值：`true` 和 `false`（默认）。此属性仅支持数据导出。

如果此项设置为 `true`：

- 对于 DATETIME 类型，系统使用 `INT96` 编码。
- 对于 DECIMAL 类型，系统使用 `fixed_len_byte_array` 编码。

如果此项设置为 `false`：

- 对于 DATETIME 类型，系统使用 `INT64` 编码。
- 对于 DECIMAL 类型，系统使用 `INT32` 或 `INT64` 编码。

:::note

对于 DECIMAL 128 数据类型，仅支持 `fixed_len_byte_array` 编码。`parquet.use_legacy_encoding` 不生效。

:::

###### `parquet.version`

控制系统导出数据的 Parquet 版本。从 v3.4.6 开始支持。有效值：`1.0`、`2.4` 和 `2.6`（默认）。此属性仅支持数据导出。

##### CSV

CSV 格式示例：

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

###### `csv.column_separator`

指定数据文件为 CSV 格式时使用的列分隔符。如果您未指定此参数，则默认为 `\\t`，表示制表符。您通过此参数指定的列分隔符必须与数据文件中实际使用的列分隔符相同。否则，由于数据质量不足，导入作业将失败。

使用 Files() 的任务是根据 MySQL 协议提交的。StarRocks 和 MySQL 都会在导入请求中转义字符。因此，如果列分隔符是不可见字符，如制表符，您必须在列分隔符前加上反斜杠 (`\`)。例如，如果列分隔符是 `\t`，您必须输入 `\\t`；如果列分隔符是 `\n`，您必须输入 `\\n`。Apache Hive™ 文件使用 `\x01` 作为列分隔符，因此如果数据文件来自 Hive，您必须输入 `\\x01`。

:::note
- 对于 CSV 数据，您可以使用 UTF-8 字符串，如逗号 (,) 、制表符或管道符 (|)，其长度不超过 50 字节，作为文本分隔符。
> - 空值使用 `\N` 表示。例如，一个数据文件由三列组成，其中一条记录在第一列和第三列中有数据，但在第二列中没有数据。在这种情况下，您需要在第二列中使用 `\N` 表示空值。这意味着记录必须编写为 `a,\N,b` 而不是 `a,,b`。`a,,b` 表示记录的第二列包含一个空字符串。
:::

###### `csv.enclose`

指定当数据文件为 CSV 格式时，根据 RFC4180 用于包裹字段值的字符。类型：单字节字符。默认值：`NONE`。最常见的字符是单引号 (`'`) 和双引号 (`"`)。

所有由 `enclose` 指定字符包裹的特殊字符（包括行分隔符和列分隔符）都被视为普通符号。StarRocks 可以超越 RFC4180，因为它允许您指定任何单字节字符作为 `enclose` 指定字符。

如果字段值包含 `enclose` 指定字符，您可以使用相同的字符来转义该 `enclose` 指定字符。例如，您将 `enclose` 设置为 `"`, 而字段值是 `a "quoted" c`。在这种情况下，您可以将字段值输入为 `"a ""quoted"" c"` 到数据文件中。

###### `csv.skip_header`

指定要跳过的 CSV 格式数据中的标题行数。类型：`INTEGER`。默认值：`0`。

在某些 CSV 格式的数据文件中，若干标题行用于定义元数据，如列名和列数据类型。通过设置 `skip_header` 参数，您可以使 StarRocks 跳过这些标题行。例如，如果您将此参数设置为 `1`，StarRocks 在数据导入期间会跳过数据文件的第一行。

数据文件中的标题行必须使用您在导入语句中指定的行分隔符分隔。

###### `csv.escape`

指定用于转义各种特殊字符的字符，例如行分隔符、列分隔符、转义字符和 `enclose` 指定字符，这些字符随后被 StarRocks 视为普通字符，并作为它们所在字段值的一部分进行解析。类型：单字节字符。默认值：`NONE`。最常见的字符是斜杠 (`\`)，在 SQL 语句中必须写为双斜杠 (`\\`)。

:::note
 `escape` 指定的字符适用于每对 `enclose` 指定字符的内部和外部。
> 以下是两个示例：
> - 当您将 `enclose` 设置为 `"` 并将 `escape` 设置为 `\` 时，StarRocks 将 `"say \"Hello world\""` 解析为 `say "Hello world"`。
> - 假设列分隔符是逗号 (`,`)。当您将 `escape` 设置为 `\` 时，StarRocks 将 `a, b\, c` 解析为两个独立的字段值：`a` 和 `b, c`。
:::

#### `schema_detect`

从 v3.2 开始，`FILES()` 支持自动结构检测和同一批数据文件的联合化。StarRocks 首先通过对批中随机数据文件的某些数据行进行采样来检测数据的结构。然后，StarRocks 将批中所有数据文件的列联合化。

您可以使用以下参数配置采样规则：

- `auto_detect_sample_files`：每批中要采样的随机数据文件数量。默认情况下，选择第一个和最后一个文件。范围：`[0, + ∞]`。默认值：`2`。
- `auto_detect_sample_rows`：每个采样数据文件中要扫描的数据行数。范围：`[0, + ∞]`。默认值：`500`。

采样后，StarRocks 根据以下规则联合化所有数据文件的列：

- 对于具有不同列名或索引的列，每个列被识别为一个独立的列，最终返回所有独立列的联合。
- 对于具有相同列名但不同数据类型的列，它们被识别为同一列，但具有相对较细粒度的通用数据类型。例如，如果文件 A 中的列 `col1` 是 `INT`，但在文件 B 中是 `DECIMAL`，则返回的列中使用 `DOUBLE`。
  - 所有整数列将被联合化为一个整体较粗粒度的整数类型。
  - 整数列与 `FLOAT` 类型列一起将被联合化为 DECIMAL 类型。
  - 字符串类型用于联合化其他类型。
- 通常，`STRING` 类型可以用于联合化所有数据类型。

您可以参考示例 5。

如果 StarRocks 无法联合化所有列，它会生成一个包含错误信息和所有文件结构的结构错误报告。

:::important
单批中的所有数据文件必须具有相同的文件格式。
:::

##### 推送目标表结构检查

从 v3.4.0 开始，系统支持将目标表结构检查推送到 `FILES()` 的扫描阶段。

`FILES()` 的结构检测并不完全严格。例如，CSV 文件中的任何整数列在函数读取文件时被推断和检查为 BIGINT 类型。在这种情况下，如果目标表中的相应列是 `TINYINT` 类型，CSV 数据记录中超过 BIGINT 类型的数据将不会被过滤。相反，它们将被隐式填充为 `NULL`。

为了解决此问题，系统引入了动态 FE 配置项 `files_enable_insert_push_down_schema`，用于控制是否将目标表结构检查推送到 `FILES()` 的扫描阶段。通过将 `files_enable_insert_push_down_schema` 设置为 `true`，系统将在文件读取时过滤掉未通过目标表结构检查的数据记录。

##### 联合化具有不同结构的文件

从 v3.4.0 开始，系统支持联合化具有不同结构的文件，默认情况下，如果存在不存在的列，将返回错误。通过将属性 `fill_mismatch_column_with` 设置为 `null`，您可以允许系统为不存在的列分配 `NULL` 值，而不是返回错误。

`fill_mismatch_column_with`：在联合化具有不同结构的文件时检测到不存在的列后，系统的行为。有效值：
- `none`：如果检测到不存在的列，将返回错误。
- `null`：将为不存在的列分配 NULL 值。

例如，要读取的文件来自 Hive 表的不同分区，并且在较新的分区上执行了 Schema Change。在读取新旧分区时，您可以将 `fill_mismatch_column_with` 设置为 `null`，系统将联合化新旧分区文件的结构，并为不存在的列分配 NULL 值。

系统根据列名联合化 Parquet 和 ORC 文件的结构，根据列的位置（顺序）联合化 CSV 文件的结构。

##### 从 Parquet 推断 STRUCT 类型

从 v3.4.0 开始，`FILES()` 支持从 Parquet 文件推断 `STRUCT` 类型数据。

#### `StorageCredentialParams`

StarRocks 用于访问您的存储系统的身份验证信息。

StarRocks 目前支持使用简单身份验证访问 HDFS，使用基于 IAM 用户的身份验证访问 AWS S3 和 GCS，以及使用共享密钥、SAS 令牌、托管身份和服务主体访问 Azure Blob Storage。

##### HDFS

- 使用简单身份验证访问 HDFS：

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **Key**                        | **Required** | **Description**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | `hadoop.security.authentication` | No           | 身份验证方法。有效值：`simple`（默认）。`simple` 表示简单身份验证，意味着无需身份验证。 |
  | `username`                      | Yes          | 您要用于访问 HDFS 集群的 NameNode 的账户用户名。 |
  | `password`                       | Yes          | 您要用于访问 HDFS 集群的 NameNode 的账户密码。 |

- 使用 Kerberos 身份验证访问 HDFS：

  目前，FILES() 仅通过放置在 **`fe/conf`**、**`be/conf`** 和 **`cn/conf`** 目录下的配置文件 **`hdfs-site.xml`** 支持与 HDFS 的 Kerberos 身份验证。

  此外，您需要在每个 FE 配置文件 **`fe.conf`**、BE 配置文件 **`be.conf`** 和 CN 配置文件 **`cn.conf`** 中的配置项 `JAVA_OPTS` 中附加以下选项：

  ```Plain
  # 指定 Kerberos 配置文件存储的本地路径。
  -Djava.security.krb5.conf=<path_to_kerberos_conf_file>
  ```

  示例：

  ```Properties
  JAVA_OPTS="-Xlog:gc*:${LOG_DIR}/be.gc.log.$DATE:time -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log -Djava.security.krb5.conf=/etc/krb5.conf"
  ```

  您还需要在每个 FE、BE 和 CN 节点上运行 `kinit` 命令，以从密钥分发中心 (KDC) 获取票证授予票证 (TGT)。

  ```Bash
  kinit -kt <path_to_keytab_file> <principal>
  ```

  要运行此命令，您使用的主体必须具有对 HDFS 集群的写访问权限。此外，您需要为该命令设置一个 crontab，以特定间隔调度任务，从而防止身份验证过期。

  示例：

```Bash
  # 每 6 小时更新一次 TGT。
  0 */6 * * * kinit -kt sr.keytab sr/test.starrocks.com@STARROCKS.COM > /tmp/kinit.log
  ```

- 访问启用 HA 模式的 HDFS：

  目前，`FILES()` 仅通过放置在 **`fe/conf`**、**`be/conf`** 和 **`cn/conf`** 目录下的配置文件 **`hdfs-site.xml`** 支持访问启用 HA 模式的 HDFS。

##### AWS S3

如果您选择 AWS S3 作为您的存储系统，请采取以下操作之一：

- 要选择基于实例配置文件的身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于假设角色的身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- 要选择基于 IAM 用户的身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                           | 是否必需 | 描述                                                      |
| --------------------------- | -------- | ------------------------------------------------------------ |
| `aws.s3.use_instance_profile` | 是      | 指定是否启用凭证方法实例配置文件和假设角色。有效值：`true` 和 `false`。默认值：`false`。 |
| `aws.s3.iam_role_arn`         | 否       | 在您的 AWS S3 存储桶上具有权限的 IAM 角色的 ARN。如果您选择假设角色作为访问 AWS S3 的凭证方法，您必须指定此参数。 |
| `aws.s3.region`               | 是      | 您的 AWS S3 存储桶所在的区域。示例：`us-west-1`。 |
| `aws.s3.access_key`           | 否       | 您的 IAM 用户的访问密钥。如果您选择 IAM 用户作为访问 AWS S3 的凭证方法，您必须指定此参数。 |
| `aws.s3.secret_key`           | 否       | 您的 IAM 用户的密钥。如果您选择 IAM 用户作为访问 AWS S3 的凭证方法，您必须指定此参数。 |

有关如何选择访问 AWS S3 的身份验证方法以及如何在 AWS IAM 控制台中配置访问控制策略的信息，请参见 [访问 AWS S3 的身份验证参数](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)。

###### AWS STS 区域终端节点

[AWS 安全令牌服务](https://docs.aws.amazon.com/sdkref/latest/guide/feature-sts-regionalized-endpoints.html) (AWS STS) 可作为全球和区域服务使用。

| 参数                   | 是否必需 | 描述                                                              |
| --------------------- | -------- | ------------------------------------------------------------------------ |
| `aws.s3.sts.region`   | 否       | 要访问的 AWS 安全令牌服务的区域。                  |
| `aws.s3.sts.endpoint` | 否       | 用于覆盖 AWS 安全令牌服务的默认终端节点。 |

  :::important
  当使用 AWS STS 终端节点进行身份验证并访问 S3 之外的 S3 兼容存储中的数据时，您必须将 `aws.s3.use_instance_profile` 设置为 `false`。
  ::: 

##### Google GCS

如果您选择 Google GCS 作为您的存储系统，请采取以下操作之一：

- 要选择基于 VM 的身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                              | **默认值** | **值示例** | **描述**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | `gcp.gcs.use_compute_engine_service_account` | false             | true                  | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。 |

- 要选择基于服务账户的身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | `gcp.gcs.service_account_email`          | ""                | `"user@hello.iam.gserviceaccount.com"` | 在创建服务账户时生成的 JSON 文件中的电子邮件地址。 |
  | `gcp.gcs.service_account_private_key_id` | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建服务账户时生成的 JSON 文件中的私钥 ID。 |
  | `gcp.gcs.service_account_private_key`    | ""                | "`-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`"  | 在创建服务账户时生成的 JSON 文件中的私钥。 |

- 要选择基于模拟的身份验证方法，请按如下配置 `StorageCredentialParams`：

  - 使 VM 实例模拟服务账户：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                              | **默认值** | **值示例** | **描述**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | `gcp.gcs.use_compute_engine_service_account` | false             | true                  | 指定是否直接使用绑定到您的 Compute Engine 的服务账户。 |
    | `gcp.gcs.impersonation_service_account`      | ""                | "hello"               | 您要模拟的服务账户。            |

  - 使服务账户（称为元服务账户）模拟另一个服务账户（称为数据服务账户）：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

    | **参数**                          | **默认值** | **值示例**                                        | **描述**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | `gcp.gcs.service_account_email`          | ""                | `"user@hello.iam.gserviceaccount.com"` | 在创建元服务账户时生成的 JSON 文件中的电子邮件地址。 |
    | `gcp.gcs.service_account_private_key_id` | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | 在创建元服务账户时生成的 JSON 文件中的私钥 ID。 |
    | `gcp.gcs.service_account_private_key`    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | 在创建元服务账户时生成的 JSON 文件中的私钥。 |
    | `gcp.gcs.impersonation_service_account`  | ""                | "hello"                                                      | 您要模拟的数据服务账户。       |

##### Azure Blob Storage

- 使用共享密钥访问 Azure Blob Storage：

  ```SQL
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.blob.shared_key`      | Yes          | 您可以用来访问 Azure Blob Storage 账户的共享密钥。 |

- 使用 SAS 令牌访问 Azure Blob Storage：

  ```SQL
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.blob.sas_token`       | Yes          | 您可以用来访问 Azure Blob Storage 账户的 SAS 令牌。 |

- 使用托管身份访问 Azure Blob Storage（从 v3.4.4 开始支持）：

  :::note
  - 仅支持具有客户端 ID 凭据的用户分配托管身份。
  - FE 动态配置 `azure_use_native_sdk`（默认：`true`）控制是否允许系统使用托管身份和服务主体进行身份验证。
  :::

  ```SQL
  "azure.blob.oauth2_use_managed_identity" = "true",
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.blob.oauth2_use_managed_identity` | Yes          | 是否使用托管身份访问 Azure Blob Storage 账户。将其设置为 `true`。                  |
  | `azure.blob.oauth2_client_id`            | Yes          | 您可以用来访问 Azure Blob Storage 账户的托管身份的客户端 ID。                |

- 使用服务主体访问 Azure Blob Storage（从 v3.4.4 开始支持）：

  :::note
  - 仅支持客户端密钥凭据。
  - FE 动态配置 `azure_use_native_sdk`（默认：`true`）控制是否允许系统使用托管身份和服务主体进行身份验证。
  :::

  ```SQL
  "azure.blob.oauth2_client_id" = "<oauth2_client_id>",
  "azure.blob.oauth2_client_secret" = "<oauth2_client_secret>",
  "azure.blob.oauth2_tenant_id" = "<oauth2_tenant_id>"
  ```

  | **Key**                                | **Required** | **Description**                                              |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.blob.oauth2_client_id`            | Yes          | 您可以用来访问 Azure Blob Storage 账户的服务主体的客户端 ID。                    |
  | `azure.blob.oauth2_client_secret`        | Yes          | 您可以用来访问 Azure Blob Storage 账户的服务主体的客户端密钥。          |
  | `azure.blob.oauth2_tenant_id`            | Yes          | 您可以用来访问 Azure Blob Storage 账户的服务主体的租户 ID。                |

##### Azure Data Lake Storage Gen2

如果您选择 Data Lake Storage Gen2 作为您的存储系统，请采取以下操作之一：

- 要选择托管身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                           | **是否必需** | **描述**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.adls2.oauth2_use_managed_identity` | 是          | 指定是否启用托管身份验证方法。将值设置为 `true`。 |
  | `azure.adls2.oauth2_tenant_id`            | 是          | 您要访问的数据的租户 ID。          |
  | `azure.adls2.oauth2_client_id`            | 是          | 托管身份的客户端（应用程序）ID。         |

- 要选择共享密钥身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**               | **是否必需** | **描述**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.adls2.storage_account` | 是          | 您的 Data Lake Storage Gen2 存储账户的用户名。 |
  | `azure.adls2.shared_key`      | 是          | 您的 Data Lake Storage Gen2 存储账户的共享密钥。 |

- 要选择服务主体身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                      | **是否必需** | **描述**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.adls2.oauth2_client_id`       | 是          | 服务主体的客户端（应用程序）ID。        |
  | `azure.adls2.oauth2_client_secret`   | 是          | 创建的新客户端（应用程序）密钥的值。    |
  | `azure.adls2.oauth2_client_endpoint` | 是          | 服务主体或应用程序的 OAuth 2.0 令牌终端（v1）。 |

##### Azure Data Lake Storage Gen1

如果您选择 Data Lake Storage Gen1 作为您的存储系统，请采取以下操作之一：

- 要选择托管服务身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                            | **是否必需** | **描述**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.adls1.use_managed_service_identity` | 是          | 指定是否启用托管服务身份验证方法。将值设置为 `true`。 |

- 要选择服务主体身份验证方法，请按如下配置 `StorageCredentialParams`：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

  | **参数**                 | **是否必需** | **描述**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | `azure.adls1.oauth2_client_id`  | 是          | 客户端（应用程序）的 ID。                         |
  | `azure.adls1.oauth2_credential` | 是          | 创建的新客户端（应用程序）密钥的值。    |
  | `azure.adls1.oauth2_endpoint`   | 是          | 服务主体或应用程序的 OAuth 2.0 令牌终端（v1）。 |

##### 其他 S3 兼容存储系统

如果您选择其他 S3 兼容存储系统，例如 MinIO，请按如下配置 `StorageCredentialParams`：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

以下表格描述了您需要在 `StorageCredentialParams` 中配置的参数。

| 参数                        | 是否必需 | 描述                                                      |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| `aws.s3.enable_ssl`                | 是      | 指定是否启用 SSL 连接。有效值：`true` 和 `false`。默认值：`true`。 |
| `aws.s3.enable_path_style_access`  | 是      | 指定是否启用路径样式 URL 访问。有效值：`true` 和 `false`。默认值：`false`。对于 MinIO，您必须将值设置为 `true`。 |
| `aws.s3.endpoint`                  | 是      | 用于连接到您的 S3 兼容存储系统而不是 AWS S3 的终端。 |
| `aws.s3.access_key`                | 是      | 您的 IAM 用户的访问密钥。 |
| `aws.s3.secret_key`                | 是      | 您的 IAM 用户的密钥。 |

#### `columns_from_path`

从 v3.2 开始，StarRocks 可以从文件路径中提取键/值对的值作为列的值。

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

假设数据文件 **file1** 存储在格式为 `/geo/country=US/city=LA/` 的路径下。您可以将 `columns_from_path` 参数指定为 `"columns_from_path" = "country, city"`，以提取文件路径中的地理信息作为返回列的值。有关进一步说明，请参见示例 4。

#### `list_files_only`

从 v3.4.0 开始，`FILES()` 支持在读取文件时仅列出文件。

```SQL
"list_files_only" = "true"
```

请注意，当 `list_files_only` 设置为 `true` 时，您无需指定 `data_format`。

有关更多信息，请参见 [Return](#return)。

#### `list_recursively`

StarRocks 进一步支持 `list_recursively` 以递归列出文件和目录。`list_recursively` 仅在 `list_files_only` 设置为 `true` 时生效。默认值为 `false`。

```SQL
"list_files_only" = "true",
"list_recursively" = "true"
```

当 `list_files_only` 和 `list_recursively` 都设置为 `true` 时，StarRocks 将执行以下操作：

- 如果指定的 `path` 是一个文件（无论是具体指定还是由通配符表示），StarRocks 将显示该文件的信息。
- 如果指定的 `path` 是一个目录（无论是具体指定还是由通配符表示，无论是否以 `/` 结尾），StarRocks 将显示该目录下的所有文件和子目录。

有关更多信息，请参见 [Return](#return)。

### 返回

#### `SELECT FROM FILES()`

与 SELECT 一起使用时，FILES() 将文件中的数据作为表返回。

- 查询 CSV 文件时，您可以在 SELECT 语句中使用 `$1`、`$2` 等表示每一列，或指定 `*` 以获取所有列的数据。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE $1 > 5;
  +------+---------+------------+
  | $1   | $2      | $3         |
  +------+---------+------------+
  |    6 | 0.34413 | 2017-11-25 |
  |    7 | 0.40055 | 2017-11-26 |
  |    8 | 0.42437 | 2017-11-27 |
  |    9 | 0.67935 | 2017-11-27 |
  |   10 | 0.22783 | 2017-11-29 |
  +------+---------+------------+
  5 rows in set (0.30 sec)

  SELECT $1, $2 FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------+
  | $1   | $2      |
  +------+---------+
  |    1 | 0.71173 |
  |    2 | 0.16145 |
  |    3 | 0.80524 |
  |    4 | 0.91852 |
  |    5 | 0.37766 |
  |    6 | 0.34413 |
  |    7 | 0.40055 |
  |    8 | 0.42437 |
  |    9 | 0.67935 |
  |   10 | 0.22783 |
  +------+---------+
  10 rows in set (0.38 sec)
  ```

- 查询 Parquet 或 ORC 文件时，您可以在 SELECT 语句中直接指定所需列的名称，或指定 `*` 以获取所有列的数据。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

- 当您查询文件时，`list_files_only` 设置为 `true`，系统将返回 `PATH`、`SIZE`、`IS_DIR`（给定路径是否为目录）和 `MODIFICATION_TIME`。

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://bucket/*.parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true"
  );
  +-----------------------+------+--------+---------------------+
  | PATH                  | SIZE | IS_DIR | MODIFICATION_TIME   |
  +-----------------------+------+--------+---------------------+
  | s3://bucket/1.parquet | 5221 |      0 | 2024-08-15 20:47:02 |
  | s3://bucket/2.parquet | 5222 |      0 | 2024-08-15 20:54:57 |
  | s3://bucket/3.parquet | 5223 |      0 | 2024-08-20 15:21:00 |
  | s3://bucket/4.parquet | 5224 |      0 | 2024-08-15 11:32:14 |
  +-----------------------+------+--------+---------------------+
  4 rows in set (0.03 sec)
  ```

- 当您查询文件时，`list_files_only` 和 `list_recursively` 设置为 `true`，系统将递归列出文件和目录。

  假设路径 `s3://bucket/list/` 包含以下文件和子目录：

  ```Plain
  s3://bucket/list/
  ├── basic1.csv
  ├── basic2.csv
  ├── orc0
  │   └── orc1
  │       └── basic_type.orc
  ├── orc1
  │   └── basic_type.orc
  └── parquet
      └── basic_type.parquet
  ```

  递归列出文件和目录：

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "list_files_only" = "true", 
      "list_recursively" = "true"
  );
  +---------------------------------------------+------+--------+---------------------+
  | PATH                                        | SIZE | IS_DIR | MODIFICATION_TIME   |
  +---------------------------------------------+------+--------+---------------------+
  | s3://bucket/list                            |    0 |      1 | 2024-12-24 22:15:59 |
  | s3://bucket/list/basic1.csv                 |   52 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/basic2.csv                 |   34 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0                       |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1                  |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc0/orc1/basic_type.orc   | 1027 |      0 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1                       |    0 |      1 | 2024-12-24 22:16:00 |
  | s3://bucket/list/orc1/basic_type.orc        | 1027 |      0 | 2024-12-24 22:16:00 |
  | s3://bucket/list/parquet                    |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/parquet/basic_type.parquet | 2281 |      0 | 2024-12-24 11:35:53 |
  +---------------------------------------------+------+--------+---------------------+
  10 rows in set (0.04 sec)
  ```

  以非递归方式列出此路径中匹配 `orc*` 的文件和目录：

  ```Plain
  SELECT * FROM FILES(
      "path"="s3://bucket/list/orc*", 
      "list_files_only" = "true", 
      "list_recursively" = "false"
  );
  +--------------------------------------+------+--------+---------------------+
  | PATH                                 | SIZE | IS_DIR | MODIFICATION_TIME   |
  +--------------------------------------+------+--------+---------------------+
  | s3://bucket/list/orc0/orc1           |    0 |      1 | 2024-12-24 11:35:53 |
  | s3://bucket/list/orc1/basic_type.orc | 1027 |      0 | 2024-12-24 22:16:00 |
  +--------------------------------------+------+--------+---------------------+
  2 rows in set (0.03 sec)
  ```

#### `DESC FILES()`

与 `DESC` 一起使用时，`FILES()` 返回文件的结构。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

当您查看文件时，`list_files_only` 设置为 `true`，系统将返回 `PATH`、`SIZE`、`IS_DIR`（给定路径是否为目录）和 `MODIFICATION_TIME` 的 `Type` 和 `Null` 属性。

```Plain
DESC FILES(
    "path" = "s3://bucket/*.parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "list_files_only" = "true"
);
+-------------------+------------------+------+
| Field             | Type             | Null |
+-------------------+------------------+------+
| PATH              | varchar(1048576) | YES  |
| SIZE              | bigint           | YES  |
| IS_DIR            | boolean          | YES  |
| MODIFICATION_TIME | datetime         | YES  |
+-------------------+------------------+------+
4 rows in set (0.00 sec)
```

## `FILES()` 用于导出

从 v3.2.0 开始，`FILES()` 支持将数据写入远端存储中的文件。您可以使用 `INSERT INTO FILES()` 将数据从 StarRocks 导出到远端存储。

### 语法

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
```

### 参数

所有参数均为 `"key" = "value"` 对。

#### data_location

参见 [`FILES()` 用于导入 - 参数 - data_location](#data_location)。

#### data_format

参见 [`FILES()` 用于导入 - 参数 - data_format](#data_format)。

#### StorageCredentialParams

参见 [`FILES()` 用于导入 - 参数 - StorageCredentialParams](#storagecredentialparams)。

#### `unload_data_param`

```sql
unload_data_param ::=
    "compression" = { "uncompressed" | "gzip" | "snappy" | "zstd | "lz4" },
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **Key**          | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| `compression`      | Yes          | 导出数据时使用的压缩方法。有效值：<ul><li>`uncompressed`：不使用压缩算法。</li><li>`gzip`：使用 gzip 压缩算法。</li><li>`snappy`：使用 SNAPPY 压缩算法。</li><li>`zstd`：使用 Zstd 压缩算法。</li><li>`lz4`：使用 LZ4 压缩算法。</li></ul>**注意**<br />导出到 CSV 文件不支持数据压缩。您必须将此项设置为 `uncompressed`。                  |
| `partition_by`     | No           | 用于将数据文件分区到不同存储路径的列列表。多个列用逗号（,）分隔。`FILES()` 提取指定列的键/值信息，并将数据文件存储在具有提取键/值对的存储路径下。有关进一步说明，请参见示例 7。 |
| `single`           | No           | 是否将数据导出到单个文件。有效值：<ul><li>`true`：数据存储在单个数据文件中。</li><li>`false`（默认）：如果导出数据量超过 512 MB，数据将存储在多个文件中。</li></ul>                  |
| `target_max_file_size` | No           | 要导出的批次中每个文件的最大努力大小。单位：字节。默认值：1073741824（1 GB）。当要导出的数据大小超过此值时，数据将被分割成多个文件，并且每个文件的大小不会显著超过此值。引入于 v3.2.7。 |

## 示例

#### 示例 1：查询文件中的数据

查询 AWS S3 存储桶 `inserttest` 中 Parquet 文件 **parquet/par-dup.parquet** 中的数据：

```SQL
SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
     "aws.s3.region" = "us-west-2"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

查询 NFS(NAS) 中的 Parquet 文件中的数据：

```SQL
SELECT * FROM FILES(
  'path' = 'file:///home/ubuntu/parquetfile/*.parquet', 
  'format' = 'parquet'
);
```

#### 示例 2：从文件中插入数据行

将 AWS S3 存储桶 `inserttest` 中 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据行插入到表 `insert_wiki_edit` 中：

```SQL
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

将 NFS(NAS) 中的 CSV 文件中的数据行插入到表 `insert_wiki_edit` 中：

```SQL
INSERT INTO insert_wiki_edit
  SELECT * FROM FILES(
    'path' = 'file:///home/ubuntu/csvfile/*.csv', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimiter' = '\n'
  );
```

#### 示例 3：使用文件中的数据行进行 CTAS

创建名为 `ctas_wiki_edit` 的表，并将 AWS S3 存储桶 `inserttest` 中 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据行插入到该表中：

```Plain
CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```

#### 示例 4：查询文件中的数据并提取其路径中的键/值信息

查询 HDFS 中 Parquet 文件 **/geo/country=US/city=LA/file1.parquet**（仅包含两列 -`id` 和 `user`）中的数据，并提取其路径中的键/值信息作为返回列。

```Plain
SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/geo/country=US/city=LA/file1.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "columns_from_path" = "country, city"
);
+------+---------+---------+------+
| id   | user    | country | city |
+------+---------+---------+------+
|    1 | richard | US      | LA   |
|    2 | amber   | US      | LA   |
+------+---------+---------+------+
2 rows in set (3.84 sec)
```

#### 示例 5：自动结构检测和联合化

以下示例基于 S3 存储桶中的两个 Parquet 文件：

- 文件 1 包含三列 - INT 列 `c1`、FLOAT 列 `c2` 和 DATE 列 `c3`。

```Plain
c1,c2,c3
1,0.71173,2017-11-20
2,0.16145,2017-11-21
3,0.80524,2017-11-22
4,0.91852,2017-11-23
5,0.37766,2017-11-24
6,0.34413,2017-11-25
7,0.40055,2017-11-26
8,0.42437,2017-11-27
9,0.67935,2017-11-27
10,0.22783,2017-11-29
```

- 文件 2 包含三列 - INT 列 `c1`、INT 列 `c2` 和 DATETIME 列 `c3`。

```Plain
c1,c2,c3
101,9,2018-05-15T18:30:00
102,3,2018-05-15T18:30:00
103,2,2018-05-15T18:30:00
104,3,2018-05-15T18:30:00
105,6,2018-05-15T18:30:00
106,1,2018-05-15T18:30:00
107,8,2018-05-15T18:30:00
108,5,2018-05-15T18:30:00
109,6,2018-05-15T18:30:00
110,8,2018-05-15T18:30:00
```

使用 CTAS 语句创建名为 `test_ctas_parquet` 的表，并将两个 Parquet 文件中的数据行插入到该表中：

```SQL
CREATE TABLE test_ctas_parquet AS
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/*",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

查看 `test_ctas_parquet` 的表结构：

```SQL
SHOW CREATE TABLE test_ctas_parquet\G
```

```Plain
*************************** 1. row ***************************
       Table: test_ctas_parquet
Create Table: CREATE TABLE `test_ctas_parquet` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
```

结果显示，包含 FLOAT 和 INT 数据的 `c2` 列被合并为 DECIMAL 列，而包含 DATE 和 DATETIME 数据的 `c3` 列被合并为 VARCHAR 列。

当 Parquet 文件更改为包含相同数据的 CSV 文件时，上述结果保持不变：

```SQL
CREATE TABLE test_ctas_csv AS
  SELECT * FROM FILES(
    "path" = "s3://inserttest/csv/*",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
Query OK, 0 rows affected (30.90 sec)

SHOW CREATE TABLE test_ctas_csv\G
*************************** 1. row ***************************
       Table: test_ctas_csv
Create Table: CREATE TABLE `test_ctas_csv` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
1 row in set (0.27 sec)
```

- 联合化 Parquet 文件的结构，并通过将 `fill_mismatch_column_with` 设置为 `null` 允许系统为不存在的列分配 NULL 值：

```SQL
SELECT * FROM FILES(
  "path" = "s3://inserttest/basic_type.parquet,s3://inserttest/basic_type_k2k5k7.parquet",
  "format" = "parquet",
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "us-west-2",
  "fill_mismatch_column_with" = "null"
);
+------+------+------+-------+------------+---------------------+------+------+
| k1   | k2   | k3   | k4    | k5         | k6                  | k7   | k8   |
+------+------+------+-------+------------+---------------------+------+------+
| NULL |   21 | NULL |  NULL | 2024-10-03 | NULL                | c    | NULL |
|    0 |    1 |    2 |  3.20 | 2024-10-01 | 2024-10-01 12:12:12 | a    |  4.3 |
|    1 |   11 |   12 | 13.20 | 2024-10-02 | 2024-10-02 13:13:13 | b    | 14.3 |
+------+------+------+-------+------------+---------------------+------+------+
3 rows in set (0.03 sec)
```

#### 示例 6：查看文件的结构

使用 DESC 查看存储在 AWS S3 中的 Parquet 文件 `lineorder` 的结构。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

#### 示例 7：数据导出

将 `sales_records` 中的所有数据行作为多个 Parquet 文件导出到 HDFS 集群中的路径 **/unload/partitioned/** 下。这些文件存储在由列 `sales_time` 的值区分的不同子路径中。

```SQL
INSERT INTO FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "partition_by" = "sales_time"
)
SELECT * FROM sales_records;
```

将查询结果导出到 NFS(NAS) 中的 CSV 和 Parquet 文件：

```SQL
-- CSV
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/csvfile/', 
    'format' = 'csv', 
    'csv.column_separator' = ',', 
    'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Parquet
INSERT INTO FILES(
    'path' = 'file:///home/ubuntu/parquetfile/',
    'format' = 'parquet'
)
SELECT * FROM sales_records;
```

#### 示例 8：Avro 文件

加载 Avro 文件：

```SQL
INSERT INTO avro_tbl
  SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xx.x:yyyy/avro/primitive.avro", 
    "format" = "avro"
);
```

查询 Avro 文件中的数据：

```SQL
SELECT * FROM FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/complex.avro", "format" = "avro")\G
*************************** 1. row ***************************
record_field: {"id":1,"name":"avro"}
  enum_field: HEARTS
 array_field: ["one","two","three"]
   map_field: {"a":1,"b":2}
 union_field: 100
 fixed_field: 0x61626162616261626162616261626162
1 row in set (0.05 sec)
```

查看 Avro 文件的结构：

```SQL
DESC FILES("path" = "hdfs://xxx.xx.xx.x:yyyy/avro/logical.avro", "format" = "avro");
+------------------------+------------------+------+
| Field                  | Type             | Null |
+------------------------+------------------+------+
| decimal_bytes          | decimal(10,2)    | YES  |
| decimal_fixed          | decimal(10,2)    | YES  |
| uuid_string            | varchar(1048576) | YES  |
| date                   | date             | YES  |
| time_millis            | int              | YES  |
| time_micros            | bigint           | YES  |
| timestamp_millis       | datetime         | YES  |
| timestamp_micros       | datetime         | YES  |
| local_timestamp_millis | bigint           | YES  |
| local_timestamp_micros | bigint           | YES  |
| duration               | varbinary(12)    | YES  |
+------------------------+------------------+------+
```

#### 示例 9：使用托管身份和服务主体访问 Azure Blob Storage

```SQL
-- 托管身份
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_use_managed_identity" = "true",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-aaaaaaaaaaaa"
);
-- 服务主体
SELECT * FROM FILES(
    "path" = "wasbs://storage-container@storage-account.blob.core.windows.net/ssb_1g/customer/*",
    "format" = "parquet",
    "azure.blob.oauth2_client_id" = "1d6bfdec-dd34-4260-b8fd-bbbbbbbbbbbb",
    "azure.blob.oauth2_client_secret" = "C2M8Q~ZXXXXXX_5XsbDCeL2dqP7hIR60xxxxxxxx",
    "azure.blob.oauth2_tenant_id" = "540e19cc-386b-4a44-a7b8-cccccccccccc"
);
```

#### 示例 10：CSV 文件

查询 CSV 文件中的数据：

```SQL
SELECT * FROM FILES(                                                                                                                                                     "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
+------+---------+--------------+
| $1   | $2      | $3           |
+------+---------+--------------+
|    1 | 0.71173 | 2017-11-20   |
|    2 | 0.16145 | 2017-11-21   |
|    3 | 0.80524 | 2017-11-22   |
|    4 | 0.91852 | 2017-11-23   |
|    5 | 0.37766 | 2017-11-24   |
|    6 | 0.34413 | 2017-11-25   |
|    7 | 0.40055 | 2017-11-26   |
|    8 | 0.42437 | 2017-11-27   |
|    9 | 0.67935 | 2017-11-27   |
|   10 | 0.22783 | 2017-11-29   |
+------+---------+--------------+
10 rows in set (0.33 sec)
```

加载 CSV 文件：

```SQL
INSERT INTO csv_tbl
  SELECT * FROM FILES(
    "path" = "s3://test-bucket/file1.csv",
    "format" = "csv",
    "csv.column_separator"=",",
    "csv.row_delimiter"="\r\n",
    "csv.enclose"='"',
    "csv.skip_header"="1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

#### 示例 11：使用 AWS STS 区域终端节点

这里展示了两种情况：

1. 在 AWS 环境之外使用 STS 区域终端。
2. 在 AWS 环境内使用 STS（例如，EC2）。

##### 在 AWS 环境之外

:::important
在 AWS 环境之外工作并使用区域 STS 需要设置 `"aws.s3.use_instance_profile" = "false"`。
:::

```sql
SELECT COUNT(*)
FROM FILES("path" = "s3://aws-bucket/path/file.csv.gz",
    "format" = "csv",
    "compression" = "gzip",
    "aws.s3.endpoint"="https://s3.us-east-1.amazonaws.com",
    "aws.s3.region"="us-east-1",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
--highlight-start
    "aws.s3.use_instance_profile" = "false",
--highlight-end
    "aws.s3.access_key" = "****",
    "aws.s3.secret_key" = "****",
    "aws.s3.iam_role_arn"="arn:aws:iam::1234567890:role/access-role",
--highlight-start
    "aws.s3.sts.region" = "{sts_region}",
    "aws.s3.sts.endpoint" = "{sts_endpoint}"
--highlight-end
);
```

##### 在 AWS 环境内

```sql
SELECT COUNT(*)
FROM FILES("path" = "s3://aws-bucket/path/file.csv.gz",
    "format" = "csv",
    "compression" = "gzip",
    "aws.s3.endpoint"="https://s3.us-east-1.amazonaws.com",
    "aws.s3.region"="us-east-1",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
--highlight-start
    "aws.s3.use_instance_profile" = "true",
--highlight-end
    "aws.s3.access_key" = "****",
    "aws.s3.secret_key" = "****",
    "aws.s3.iam_role_arn"="arn:aws:iam::1234567890:role/access-role",
--highlight-start
    "aws.s3.sts.region" = "{sts_region}",
    "aws.s3.sts.endpoint" = "{sts_endpoint}"
--highlight-end
);
```