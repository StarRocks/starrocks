# 配置 AWS 认证信息

## 认证方式介绍

### 基于 Instance Profile 认证鉴权

通过 Instance Profile，您可以让 StarRocks 集群直接从 AWS EC2 虚拟机继承 Instance Profile 的权限。在该模式下，任何能够登录到集群的用户都可以根据 AWS IAM 策略配置，对相关的 AWS 资源执行授权范围内允许的操作（例如 AWS S3 指定 Bucket 的读写操作）。

### 基于 Assumed Role 认证鉴权

与 Instance Profile 的模式不同，Assumed Role 是一种 Catalog 级的数据源访问控制解决方案，支持通过担任 AWS IAM Role 来实现认证。参见 AWS 官网文档“[代入角色](https://docs.aws.amazon.com/zh_cn/awscloudtrail/latest/userguide/cloudtrail-sharing-logs-assume-role.html)”。 具体来说，您可以创建多个不同的 Catalog，并为每个 Catalog 配置特定的 Assume Role 来进行鉴权，从而拥有特定 AWS 资源（例如 S3 Bucket）的访问权限。作为管理员，您还可以向不同的用户授予不同 Catalog 的操作权限，从而实现同一集群内不同用户对不同外部数据的访问控制。

### 基于 IAM User 认证鉴权

IAM User 也是一种 Catalog 级的数据源访问控制解决方案，支持通过 IAM User 来实现认证和鉴权。您可以在不同 Catalog 里指定不同的 IAM User 的 Access Key 和 Secret Key，从而实现同一集群内不同用户对不同外部数据的访问控制。

## 准备工作

### 基于 Instance Profile 认证鉴权

创建如下 IAM 策略用以授予特定 AWS 资源的访问权限。然后，将该策略添加到 EC2 实例相关联的 IAM Role。

#### 访问 AWS S3

如果希望 StarRocks 能够访问特定的 S3 Bucket，您需要创建下述 IAM 策略：

> **注意**
>
> 您需要将 `bucket_name` 替换成您希望访问的 Bucket 名称。

```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "s3",
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::<bucket_name>/*"]
        },
        {
            "Sid": "s3list",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::<bucket_name>"]
        }
        ]
}
```

#### 访问 AWS Glue

如果希望 StarRocks 访问 AWS Glue，您需要创建下述 IAM 策略：

```JSON
{
     "Version": "2012-10-17",
     "Statement": [
         {
             "Effect": "Allow",
             "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:GetTable",
                "glue:GetTableVersions",
                "glue:GetTables",
                "glue:GetConnection",
                "glue:GetConnections",
                "glue:GetDevEndpoint",
                "glue:GetDevEndpoints",
                "glue:BatchGetPartition"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```

### 基于 Assumed Role 认证鉴权

您需要创建一个 Assumed Role（例如，命名为 `s3_role_test`），并将本文“[访问 AWS S3](../integrations/authenticate_to_aws_resources.md#访问-aws-s3)”小节所述的 IAM 策略添加到该角色，保证该 Assumed Role 可以访问 AWS S3 资源。接下来，与 EC2 实例相关联的 IAM Role 可以通过担任该 Assumed Role，从而获得访问对应 AWS S3 资源的权限。

同样，如果您希望 StarRocks 访问您的 AWS Glue 资源，可以创建另外一个 Assumed Role（例如，命名为`glue_role_test`），后续流程同理。

完成上述操作后，还需要在创建好的 Assumed Role、以及与 EC2 实例相关联的 IAM Role 之间配置信任关系。具体操作步骤如下：

#### 配置信任关系

首先，创建如下 IAM 策略，并添加到创建好的 Assumed Role，例如 `s3_role_test`：

> **注意**
>
> 您需要将 `cluster_EC2_iam_role_ARN` 替换成与 EC2 实例相关联的 IAM Role 的 ARN。该策略将会允许 EC2 实例所关联的 Role 来代入 Assumed Role。

```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<cluster_EC2_iam_role_ARN>"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

然后，创建如下 IAM 策略，并添加到与 EC2 实例相关联的 IAM Role：

> **注意**
>
> 您需要在 `Resource` 中填入 Assumed Role `s3_role_test` 的 ARN。另外，只有在选择了 AWS Glue 作为元数据服务并为 AWS Glue 创建了另一个 Assumed Role `glue_role_test` 时，才需要填入该 Assumed Role `glue_role_test` 的 ARN。

```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["sts:AssumeRole"],
            "Resource": [
                "<ARN of s3_role_test>",
                "<ARN of glue_role_test>"
            ]
        }
    ]
}
```

### 基于 IAM User 认证鉴权

创建一个 IAM User，并将本文“[访问 AWS S3](../integrations/authenticate_to_aws_resources.md#访问-aws-s3)”或“[访问 AWS Glue](../integrations/authenticate_to_aws_resources.md#访问-aws-glue)”小节所述的 IAM 策略添加到该 IAM User。并准备好该 IAM 用户的 Access Key 和 Secret Key。

## 原理图

StarRocks 中 Instance Profile、Assumed Role、以及 IAM User 三种认证方式的原理和差异如下图所示。

![Credentials](../assets/authenticate_s3_credential_methods.png)

## 相关参数配置

### 访问 AWS S3 的认证参数

在 StarRocks 需要与 AWS S3 进行集成的各类场景下，例如在创建 External Catalog 或文件外部表、以及从 AWS S3 导入、备份或恢复数据时，AWS S3 的认证参数都需要参考下述进行配置：

- 如果基于 Instance Profile 进行认证和鉴权，则需要把 `aws.s3.use_instance_profile` 设置为 `true`。
- 如果基于 Assumed Role 进行认证和鉴权，则需要把 `aws.s3.use_instance_profile` 设置为 `true`，并在 `aws.s3.iam_role_arn` 中填入用于访问 AWS S3 的 Assumed Role 的 ARN。
- 如果基于 IAM User 进行认证和鉴权，则需要把 `aws.s3.use_instance_profile` 设置为 `false`，并在 `aws.s3.access_key` 和 `aws.s3.secret_key` 中分别填入 AWS IAM User 的 Access Key 和 Secret Key。

### 访问 AWS Glue 的认证参数

在 StarRocks 需要与 AWS Glue 进行集成的各类场景下，例如在创建 External Catalog 时，AWS Glue 的认证参数都需要参考下述进行配置：

- 如果基于 Instance Profile 进行认证和鉴权，则需要把 `aws.glue.use_instance_profile` 设置为 `true`。
- 如果基于 Assumed Role 进行认证和鉴权，则需要把 `aws.glue.use_instance_profile` 设置为 `true`，并在 `aws.glue.iam_role_arn` 中填入用于访问 AWS Glue 的 Assumed Role 的 ARN。
- 如果基于 IAM User 进行认证和鉴权，则需要把 `aws.glue.use_instance_profile` 设置为 `false`，并在 `aws.glue.access_key` 和 `aws.glue.secret_key` 中分别填入 AWS IAM User 的 Access Key 和 Secret Key。

## 集成示例

### External Catalog

在 StarRocks 集群中创建 External Catalog 之前，您必须集成数据湖的两个关键组件：

- 分布式文件存储，如 AWS S3，用于存储表文件。
- 元数据服务，如 Hive Metastore（以下简称 HMS）或 AWS Glue，用于存储表文件的元数据和位置信息。

StarRocks 支持以下类型的 External Catalog：

- [Hive catalog](../data_source/catalog/hive_catalog.md)
- [Iceberg catalog](../data_source/catalog/iceberg_catalog.md)
- [Hudi catalog](../data_source/catalog/hudi_catalog.md)
- [Delta Lake catalog](../data_source/catalog/deltalake_catalog.md)

以下示例创建了一个名为 `hive_catalog_hms` 或 `hive_catalog_glue` 的 Hive Catalog，用于查询 Hive 集群里的数据。有关详细的语法和参数说明，参见 [Hive catalog](../data_source/catalog/hive_catalog.md)。

#### 基于 Instance Profile 鉴权认证

- 如果 Hive 集群使用 HMS 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083"
  );
  ```

- 如果 Amazon EMR Hive 集群使用 AWS Glue 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.region" = "us-west-2"
  );
  ```

#### 基于 Assumed Role 鉴权认证

- 如果 Hive 集群使用 HMS 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083"
  );
  ```

- 如果 Amazon EMR Hive 集群使用 AWS Glue 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "true",
      "aws.glue.iam_role_arn" = "arn:aws:iam::081976408565:role/test_glue_role",
      "aws.glue.region" = "us-west-2"
  );
  ```

#### 基于 IAM User 鉴权认证

- 如果 Hive 集群使用 HMS 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_hms
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_access_key>",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.uris" = "thrift://xx.xx.xx:9083"
  );
  ```

- 如果 Amazon EMR Hive 集群使用 AWS Glue 作为元数据服务，您可以这样创建 Hive Catalog：

  ```SQL
  CREATE EXTERNAL CATALOG hive_catalog_glue
  PROPERTIES
  (
      "type" = "hive",
      "aws.s3.use_instance_profile" = "false",
      "aws.s3.access_key" = "<iam_user_access_key>",
      "aws.s3.secret_key" = "<iam_user_secret_key>",
      "aws.s3.region" = "us-west-2",
      "hive.metastore.type" = "glue",
      "aws.glue.use_instance_profile" = "false",
      "aws.glue.access_key" = "<iam_user_access_key>",
      "aws.glue.secret_key" = "<iam_user_secret_key>",
      "aws.glue.region" = "us-west-2"
  );
  ```

### 文件外部表

必须在 Internal Catalog `default_catalog` 中创建文件外部表。以下示例在现有数据库 `test_s3_db` 上创建了一个名为 `file_table` 的文件外部表。有关详细的语法和参数说明，参见 [文件外部表](../data_source/file_external_table.md)。

#### 基于 Instance Profile 鉴权认证

您可以这样创建文件外部表：

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile"="true",
    "aws.s3.region"="us-west-2"
);
```

#### 基于 Assumed Role 鉴权认证

您可以这样创建文件外部表：

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile"="true",
    "aws.s3.iam_role_arn"="arn:aws:iam::081976408565:role/test_s3_role",
    "aws.s3.region"="us-west-2"
);
```

#### 基于 IAM User 鉴权认证

您可以这样创建文件外部表：

```SQL
CREATE EXTERNAL TABLE test_s3_db.file_table
(
    id varchar(65500),
    attributes map<varchar(100), varchar(2000)>
) 
ENGINE=FILE
PROPERTIES
(
    "path" = "s3://starrocks-test/",
    "format" = "ORC",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>",
    "aws.s3.region"="us-west-2"
);
```

### 数据导入

您可以从 AWS S3 导入数据。 以下示例将存储在 `s3a://test-bucket/test_brokerload_ingestion` 路径下的所有 Parquet 格式数据文件都导入到了现有数据库 `test_s3_db` 中一个名为 `test_ingestion_2` 的表中。有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md)。

#### 基于 Instance Profile 鉴权认证

您可以这样导入数据：

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile"= "true",
    "aws.s3.region"="us-west-1"
)
PROPERTIES
(
    "timeout"="1200"
);
```

#### 基于 Assumed Role 鉴权认证

您可以这样导入数据：

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile"= "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::081976408565:role/test_s3_role",
    "aws.s3.region"="us-west-1"
)
PROPERTIES
(
    "timeout"="1200"
);
```

#### 基于 IAM User 鉴权认证

您可以这样导入数据：

```SQL
LOAD LABEL test_s3_db.test_credential_instanceprofile_7
(
    DATA INFILE("s3a://test-bucket/test_brokerload_ingestion/*")
    INTO TABLE test_ingestion_2
    FORMAT AS "parquet"
)
WITH BROKER
(
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_secret_key>",
    "aws.s3.region"="us-west-1"
)
PROPERTIES
(
    "timeout"="1200"
);
```
