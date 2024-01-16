---
displayed_sidebar: "Chinese"
---

# CREATE REPOSITORY

## 功能

基于远端存储系统创建用于存储数据快照的仓库。仓库用于 [备份和恢复](../../../administration/Backup_and_restore.md) 数据库数据。

> **注意**
>
> 仅拥有 ADMIN 权限的用户可以创建仓库。

删除仓库操作请参考 [DROP REPOSITORY](../data-definition/DROP_REPOSITORY.md) 章节。

## 语法

```SQL
CREATE [READ ONLY] REPOSITORY <repository_name>
WITH BROKER
ON LOCATION "<repository_location>"
PROPERTIES ("key"="value", ...)
```

## 参数说明

| **参数**            | **说明**                                                     |
| ------------------- | ------------------------------------------------------------ |
| READ ONLY           | 创建只读仓库。请注意只读仓库只可进行恢复操作。当为两个集群创建相同仓库，用以迁移数据时，可以为新集群创建只读仓库，仅赋予其恢复的权限。|
| repository_name     | 仓库名。                                                     |
| repository_location | 远端存储系统路径。                                           |
| PROPERTIES          | 访问远端存储系统的节点及密钥或用户名及密码。具体使用方式见以下说明。 |

**PROPERTIES**：

StarRocks 支持在 HDFS、AWS S3、Google GCS、阿里云 OSS 以及腾讯云 COS 中创建仓库。

- HDFS：
  - "username"：用于访问 HDFS 集群中 NameNode 节点的用户名。
  - "password"：用于访问 HDFS 集群中 NameNode 节点的密码。

- S3：
  - "aws.s3.use_instance_profile"：是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 AWS S3。默认值：`false`。

    - 如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 AWS S3，则无需指定该参数，并指定 "aws.s3.access_key"、"aws.s3.secret_key" 以及 "aws.s3.endpoint"。
    - 如果您使用 Instance Profile 访问 AWS S3，则需将该参数设置为 `true`，并指定 "aws.s3.region"。
    - 如果您使用 Assumed Role 访问 AWS S3，则需将该参数设置为 `true`，并指定 "aws.s3.iam_role_arn" 和 "aws.s3.region"。

  - "aws.s3.access_key"：访问 AWS S3 存储空间的 Access Key。
  - "aws.s3.secret_key"：访问 AWS S3 存储空间的 Secret Key。
  - "aws.s3.endpoint"：访问 AWS S3 存储空间的连接地址。
  - "aws.s3.iam_role_arn"：有访问 AWS S3 存储空间权限 IAM Role 的 ARN。如使用 Instance Profile 或 Assumed Role 作为安全凭证访问 AWS S3，则必须指定该参数。
  - "aws.s3.region"：需访问的 AWS S3 存储空间的地区，如 `us-west-1`。

> **说明**
>
> StarRocks 仅支持通过 S3A 协议在 AWS S3 中创建仓库。因此，当您在 AWS S3 中创建仓库时，必须在 `ON LOCATION` 参数下将 S3 URI 中的 `s3://` 替换为 `s3a://`。

- GCS：
  - "fs.s3a.access.key"：访问 Google GCS 存储空间的 Access Key。
  - "fs.s3a.secret.key"：访问 Google GCS 存储空间的 Secret Key。
  - "fs.s3a.endpoint"：访问 Google GCS 存储空间的连接地址。

> **说明**
>
> StarRocks 仅支持通过 S3A 协议在 Google GCS 中创建仓库。 因此，当您在 Google GCS 中创建仓库时，必须在 `ON LOCATION` 参数下将 GCS URI 的前缀替换为 `s3a://`。

- OSS：
  - "fs.oss.accessKeyId"：访问阿里云 OSS 存储空间的 AccessKey ID，用于标识用户。
  - "fs.oss.accessKeySecret"：访问阿里云 OSS 存储空间的 AccessKey Secret，是用于加密签名字符串和 OSS 用来验证签名字符串的密钥。
  - "fs.oss.endpoint"：访问阿里云 OSS 存储空间的连接地址。

- COS：
  - "fs.cosn.userinfo.secretId"：访问腾讯云 COS 存储空间的 SecretId，用于标识 API 调用者的身份。
  - "fs.cosn.userinfo.secretKey"：访问腾讯云 COS 存储空间的 SecretKey，是用于加密签名字符串和服务端验证签名字符串的密钥。
  - "fs.cosn.bucket.endpoint_suffix"：访问腾讯云 COS 存储空间的连接地址。

## 示例

示例一：在 Apache™ Hadoop® 集群中创建名为 `hdfs_repo` 的仓库。

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER
ON LOCATION "hdfs://x.x.x.x:yyyy/repo_dir/backup"
PROPERTIES(
    "username" = "xxxxxxxx",
    "password" = "yyyyyyyy"
);
```

示例二：在 Amazon S3 存储桶 `bucket_s3` 中创建名为 `s3_repo` 的只读仓库。

```SQL
CREATE READ ONLY REPOSITORY s3_repo
WITH BROKER
ON LOCATION "s3a://bucket_s3/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.endpoint" = "s3.us-east-1.amazonaws.com"
);
```

示例三：在 Google GCS 存储桶 `bucket_gcs` 中创建一个名为 `gcs_repo` 的仓库。

```SQL
CREATE REPOSITORY gcs_repo
WITH BROKER
ON LOCATION "s3a://bucket_gcs/backup"
PROPERTIES(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "storage.googleapis.com"
);
```
