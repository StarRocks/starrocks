---
displayed_sidebar: docs
---

# StarRocks + Amazon S3 Tables
本文介绍StarRocks如何通过 AWS Glue Iceberg REST endpoint 访问 Amazon S3 tables

AWS Glue Iceberg REST endpoint 实现了[Iceberg REST Catalog Open API 规范](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)，该规范为与 Iceberg 表交互提供了标准化的接口,
要使用该端点访问S3表，你需要通过结合 IAM 策略和 AWS Lake Formation授权来配置权限。以下部分将逐步设置访问权限, 包括定义所需的IAM Policy 以及通过 Lake Formation 进行权限管理，并使用 StarRocks 
创建Iceberg REST catalog来访问S3 Tables.

## 创建Table bucket
1. 登录到 [Amazon S3 控制台](https://console.aws.amazon.com/s3)，并从导航面板中选择“Table buckets”。
2. 点击**Create table bucket**创建一个Table bucket。
3. 创建好 Table bucket之后，点击创建好的Table bucket，然后点击**Create table with Athena**
4. 创建一个 namespace
5. 创建好 namespace 后，点击 **Create table with Athena**后创建一个table
> **说明**
>
> 可以使用Athena创建好Database,Table, 之后使用StarRocks查询, 也可以只创建好Table bucket, 之后使用StarRocks创建Database, Table.

## 创建 IAM Policy
要通过 AWS Glue 端点访问表，您需要创建一个具有访问 AWS Glue 和 Lake Formation 操作权限的 IAM Policy。
1. 打开 IAM 控制台：https://console.aws.amazon.com/iam/
2. 在左侧导航栏中，选择**Policies**。
3. 选择**Create a policy**，并在策略编辑器中选择**JSON**。
4. 添加以下策略，该策略授予对AWS Glue和Lake Formation操作的访问权限：
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow"
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
        }
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
创建好 IAM Policy 之后，将此Policy添加到IAM User的Permissions, 此IAM User将拥有访问S3 Tables的权限
1. 在左侧导航栏中，选择**Users**。
2. 选择后续访问S3 Tables的User
3. 点击 **Add permissions**，并选择**Attach policies directly**
4. 选择上面创建好的IAM Policy, 添加权限给该User

## 使用Lake Formation进行权限管理
StarRocks要想访问S3 Tables, Lake Formation需要首先设置允许第三方查询引擎访问 S3 Tables。
1. 打开 [Lake Formation 控制台](https://console.aws.amazon.com/lakeformation)
2. 点击 **Application integration settings**
3. 选择 **Allow external engines to access data in Amazon S3 locations with full table access.**

接下来，在Lake Formation中对上面IAM User授予访问的权限
1. 在[Lake Formation 控制台](https://console.aws.amazon.com/lakeformation)页面点击**Data permissions**，然后点击**Grant**
2. 在Principals部分，点击**IAM users and roles**, 并选择之前授权的IAM User
3. 在LF-Tags or catalog resources部分选择**Named Data Catalog resources**
4. 在catalogs选择你的之前创建的Table bucket
5. 在catalog permissions选择**Super**
6. 点击**Grant**

> **注意**
>
> 上面为了方便测试，我们授予了Super权限，用户应该根据实际需要授予合理的权限

## 创建Iceberg REST Catalog
使用StarRocks创建Iceberg REST Catalog

```SQL
create external catalog starrocks_lakehouse_s3tables properties(
  "type"="iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri"  = "https://glue.<region>.amazonaws.com/iceberg",
  "iceberg.catalog.rest.sigv4-enabled" = "true",
  "iceberg.catalog.rest.signing-name" = "glue",
  "iceberg.catalog.rest.access-key-id" = <iam_user_access_key>,
  "iceberg.catalog.rest.secret-access-key" = <iam_user_secret_key>,
  "iceberg.catalog.warehouse" = "<accountid>:s3tablescatalog/<table-bucket-name>",
  "aws.s3.region" = <region>
);
```

创建db和table, 并进行查询
```SQL
-- Create database
StarRocks>create database s3table_db;
Query OK, 0 rows affected
      
-- Switch database
StarRocks>use s3table_db;
Database changed
         
-- Create table
StarRocks>CREATE TABLE taxis
    -> (
    ->   trip_id bigint,
    ->   trip_distance float,
    ->   fare_amount double,
    ->   store_and_fwd_flag string,
    ->   vendor_id bigint
    -> )
    -> PARTITION BY (vendor_id);
Query OK, 0 rows affected 

-- Insert data
StarRocks>INSERT INTO taxis
    -> VALUES (1000371, 1.8, 15.32, 'N', 1), (1000372, 2.5, 22.15, 'N', 2), (1000373, 0.9, 9.01, 'N', 2), (1000374, 8.4, 42.13, 'Y', 1);
Query OK, 4 rows affected

-- Query data
StarRocks>select * from taxis;
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
