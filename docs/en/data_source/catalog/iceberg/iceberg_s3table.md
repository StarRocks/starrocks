---
displayed_sidebar: docs
---

# StarRocks + Amazon S3 Tables
This article explains how StarRocks accesses Amazon S3 tables through the AWS Glue Iceberg REST endpoint.

The AWS Glue Iceberg REST endpoint implements the [Iceberg REST Catalog Open API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), which provides a standardized interface for interacting with Iceberg tables. To access S3 tables using this endpoint, you need to configure permissions by combining IAM policies and AWS Lake Formation authorization. The following sections will guide you through setting up access permissions, including defining required policies, establishing Lake Formation permissions at database and table levels, and using StarRocks to create an Iceberg REST catalog for accessing S3 tables.

## Create Table Bucket
1. Log in to the [Amazon S3 Console](https://console.aws.amazon.com/s3) and select "Table buckets" from the navigation panel.
2. Click **Create table bucket** to create a table bucket.
3. After creating the table bucket, select it and click **Create table with Athena**.
4. Create a namespace.
5. After creating the namespace, click **Create table with Athena** to create a table.

> **NOTE**
> 
> You can create a Database and Table using Athena, and then query them using StarRocks. Alternatively, you can just create a Table bucket, and then use StarRocks to create the Database and Table.

## Create IAM Policy
To access tables via the AWS Glue endpoint, create an IAM Policy with permissions for AWS Glue and Lake Formation operations:
1. Open the IAM Console: https://console.aws.amazon.com/iam/
2. Select **Policies** from the left navigation bar.
3. Choose **Create a policy**, select **JSON** in the policy editor.
4. Add the following policy granting access to AWS Glue and Lake Formation actions:
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
After creating the IAM policy, attach it to the target IAM user:

1. Select **Users** from the left navigation bar.
2. Choose the user requiring S3 table access.
3. Click **Add permissions** and select **Attach policies directly**.
4. Attach the newly created policy.

## Manage Permissions via Lake Formation
To access S3 tables, StarRocks requires Lake Formation to first set up permissions that allow third-party query engines to access S3 tables.

1. Open the [Lake Formation Console](https://console.aws.amazon.com/lakeformation).
2. Navigate to **Application integration settings**.
3. Enable **Allow external engines to access data in Amazon S3 locations with full table access.**

Next, grant the above IAM User access permissions in Lake Formation.
1. In [Lake Formation Console](https://console.aws.amazon.com/lakeformation), go to **Data permissions** > **Grant**.
2. Under Principals, select **IAM users and roles** and choose the authorized IAM user.
3. Under LF-Tags or catalog resources, select **Named Data Catalog resources**.
4. Choose the previously created table bucket in the catalogs.
5. Select **Super** under catalog permissions.
6. Click **Grant**.

> **NOTE**
>
> Super permissions are granted here for testing convenience. Users should assign appropriate permissions based on actual requirements.

Create Iceberg REST Catalog
Use StarRocks to create an Iceberg REST catalog:

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

Create Database/Table and Query
```SQL
-- Create database
StarRocks> CREATE DATABASE s3table_db;
Query OK, 0 rows affected

-- Switch database
StarRocks> USE s3table_db;
Database changed

-- Create table
StarRocks> CREATE TABLE taxis (
    trip_id BIGINT,
    trip_distance FLOAT,
    fare_amount DOUBLE,
    store_and_fwd_flag STRING,
    vendor_id BIGINT
) PARTITION BY (vendor_id);
Query OK, 0 rows affected 

-- Insert data
StarRocks> INSERT INTO taxis 
VALUES (1000371, 1.8, 15.32, 'N', 1), 
       (1000372, 2.5, 22.15, 'N', 2),
       (1000373, 0.9, 9.01, 'N', 2),
       (1000374, 8.4, 42.13, 'Y', 1);
Query OK, 4 rows affected

-- Query data
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