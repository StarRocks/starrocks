# Create Iceberg REST Catalog for AWS S3

本文介绍如何在 StarRocks 中创建 Iceberg REST Catalog，以通过 AWS Glue Iceberg REST 端点访问 AWS S3 中的数据。

AWS Glue Iceberg REST 端点实现了 [Iceberg REST Catalog Open API 规范](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)，提供了一个标准化接口用于与 Iceberg 表进行交互。要使用此端点访问 S3 中的表，您需要通过结合 IAM 策略和 AWS Lake Formation 授权来配置凭证。以下部分将指导您完成访问权限设置，包括定义所需的策略、在数据库和表级别建立 Lake Formation 权限，以及使用 StarRocks 创建一个 Iceberg REST catalog 以访问 S3 表。

## （可选）创建一个表存储桶

如果您已经有一个用于 Iceberg 表的表存储桶，可以跳过此步骤。

1. 以具有管理员权限的用户身份登录 [Amazon S3 控制台](https://console.aws.amazon.com/s3)。
2. 在页面的右上角，选择您的 AWS 区域。
3. 在左侧导航窗格中，从导航面板中选择 **Table buckets**。
4. 点击 **Create table bucket** 创建一个表存储桶。
5. 创建表存储桶后，选择它并点击 **Create table with Athena**。
6. 创建一个命名空间。
7. 创建命名空间后，再次点击 **Create table with Athena** 创建一个表。

> **注意**
> 
> 您可以使用 Athena 创建数据库和表，然后使用 StarRocks 查询它们。或者，您也可以只创建一个表存储桶，然后使用 StarRocks 创建数据库和表。

## 创建 IAM 策略

要通过 AWS Glue 端点访问表，请创建一个具有 AWS Glue 和 Lake Formation 操作权限的 IAM 策略：

1. 以具有管理员权限的用户身份登录 [Amazon IAM 控制台](https://console.aws.amazon.com/iam)。
2. 在页面的右上角，选择您的 AWS 区域。
3. 在左侧导航窗格中，从导航面板中选择 **Policies**。
4. 选择 **Create a policy**，在策略编辑器中选择 **JSON**。
5. 添加以下策略以授予对 AWS Glue 和 Lake Formation 操作的访问权限。

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:GetCatalog",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:CreateTable",
                "glue:UpdateTable"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<account-id>:catalog",
                "arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog",
                "arn:aws:glue:<region>:<account-id>:catalog/s3tablescatalog/<s3_table_bucket_name>",
                "arn:aws:glue:<region>:<account-id>:table/s3tablescatalog/<s3_table_bucket_name>/<namespace>/*",
                "arn:aws:glue:<region>:<account-id>:database/s3tablescatalog/<s3_table_bucket_name>/<namespace>"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": "*"
        }
    ]
}
```

创建 IAM 策略后，将其附加到目标 IAM 用户：

1. 从导航面板中选择 **Users**。
2. 选择需要 S3 表访问权限的用户。
3. 点击 **Add permissions** 并选择 **Attach policies directly**。
4. 附加新创建的策略。

## 通过 Lake Formation 管理权限

要访问 S3 表，StarRocks 需要 Lake Formation 首先设置允许第三方查询引擎访问 S3 表的权限。

1. 以具有管理员权限的用户身份登录 [Lake Formation 控制台](https://console.aws.amazon.com/lakeformation)。
2. 在页面的右上角，选择您的 AWS 区域。
3. 在左侧导航窗格中，从导航面板中选择 **Application integration settings**。
4. 选择 **Allow external engines to access data in Amazon S3 locations with full table access.**

接下来，在 Lake Formation 中授予上述 IAM 用户访问权限。

1. 在 [Lake Formation 控制台](https://console.aws.amazon.com/lakeformation) 的左侧导航窗格中，从导航面板中选择 **Data permissions**。
2. 点击 **Grant**。
3. 在 **Principals** 部分，选择 **IAM users and roles**，并从 **IAM users and roles** 下拉列表中选择授权的 IAM 用户。
4. 在 **LF-Tags or catalog resources** 部分，选择 **Named Data Catalog resources**，并从 **Catalogs** 下拉列表中选择您创建的表存储桶。
5. 在 **Catalog permissions** 部分，为 **Catalog permissions** 选择 **Super**。
6. 点击 **Grant**。

> **注意**
>
> 这里授予了超级权限以便于测试。您需要根据实际需求在生产环境中分配适当的权限。

## 创建 Iceberg REST Catalog

在 StarRocks 中创建一个 Iceberg REST catalog：

```SQL
CREATE EXTERNAL CATALOG starrocks_lakehouse_s3tables PROPERTIES(
  "type"="iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri"  = "https://glue.<region>.amazonaws.com/iceberg",
  "iceberg.catalog.rest.sigv4-enabled" = "true",
  "iceberg.catalog.rest.signing-name" = "glue",
  "iceberg.catalog.rest.access-key-id" = "<iam_user_access_key>",
  "iceberg.catalog.rest.secret-access-key" = "<iam_user_secret_key>",
  "iceberg.catalog.warehouse" = "<accountid>:s3tablescatalog/<table-bucket-name>",
  "aws.s3.region" = "<region>"
);
```

然后，您可以创建数据库和表并在其中运行查询。

示例：

```SQL
-- 切换 catalog
StarRocks> SET CATALOG starrocks_lakehouse_s3tables;
-- 创建数据库
StarRocks> CREATE DATABASE s3table_db;
Query OK, 0 rows affected

-- 切换数据库
StarRocks> USE s3table_db;
Database changed

-- 创建表
StarRocks> CREATE TABLE taxis (
    trip_id BIGINT,
    trip_distance FLOAT,
    fare_amount DOUBLE,
    store_and_fwd_flag STRING,
    vendor_id BIGINT
) PARTITION BY (vendor_id);
Query OK, 0 rows affected 

-- 插入数据
StarRocks> INSERT INTO taxis 
VALUES (1000371, 1.8, 15.32, 'N', 1), 
       (1000372, 2.5, 22.15, 'N', 2),
       (1000373, 0.9, 9.01, 'N', 2),
       (1000374, 8.4, 42.13, 'Y', 1);
Query OK, 4 rows affected

-- 查询数据
StarRocks> SELECT * FROM taxis;
+---------+---------------+-------------+--------------------+-----------+
| trip_id | trip_distance | fare_amount | store_and_fwd_flag | vendor_id |
+---------+---------------+-------------+--------------------+-----------+
| 1000372 |           2.5 |       22.15 | N                  |         2 |
| 1000373 |           0.9 |        9.01 | N                  |         2 |
| 1000371 |           1.8 |       15.32 | N                  |         1 |
| 1000374 |           8.4 |       42.13 | Y                  |         1 |
+---------+---------------+-------------+--------------------+-----------+
4 rows in set
```
