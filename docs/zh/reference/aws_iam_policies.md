---
displayed_sidebar: "Chinese"
---

# AWS IAM 策略

IAM 策略用于声明特定 AWS 资源的一组访问权限。创建 IAM 策略后，您需要将创建好的策略添加到某个 IAM 用户或角色，从而使该 IAM 用户或角色拥有该策略中所声明的访问特定 AWS 资源的权限。

在 StarRocks 中，不同的操作涉及的 AWS 资源也不同。因此，您需要根据要访问的 AWS 资源来创建 IAM 策略。

本文介绍在不同场景下为确保 StarRocks 能够正确访问所涉及的 AWS 资源需要配置哪些 IAM 策略。

## 从 AWS S3 批量导入数据

如果您需要从 S3 Bucket 批量导入数据，请按如下配置 IAM 策略：

> **注意**
>
> 您需要将下面策略中的 `<bucket_name>` 替换为数据所在的 S3 Bucket 的名称。

```SQL
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Sid": "s3",
          "Effect": "Allow",
          "Action": [
              "s3:GetObject"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>/*"
          ]
      },
      {
          "Sid": "s3list",
          "Effect": "Allow",
          "Action": [
              "s3:ListBucket"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>"
          ]
      }
  ]
}
```

## 从 AWS S3 读写数据

如果您需要从 S3 Bucket 查询数据，请按如下配置 IAM 策略：

> **注意**
>
> 您需要将下面策略中的 `<bucket_name>` 替换为数据所在的 S3 Bucket 的名称。

```SQL
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Sid": "s3",
          "Effect": "Allow",
          "Action": [
              "s3:GetObject", 
              "s3:PutObject"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>/*"
          ]
      },
      {
          "Sid": "s3list",
          "Effect": "Allow",
          "Action": [
              "s3:ListBucket"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>"
          ]
      }
  ]
}
```

## 对接 AWS Glue

如果您需要对接 AWS Glue，请按如下配置 IAM 策略：

```SQL
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
