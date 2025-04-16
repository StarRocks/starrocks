# Create Iceberg REST Catalog for AWS S3

This article explains how to create Iceberg REST Catalog in StarRocks for access to data in AWS S3 through the AWS Glue Iceberg REST endpoint.

The AWS Glue Iceberg REST endpoint implements the [Iceberg REST Catalog Open API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml), which provides a standardized interface for interacting with Iceberg tables. To access tables in S3 using this endpoint, you need to configure credentials by combining IAM policies and AWS Lake Formation authorization. The following sections will guide you through the access permission setup, including defining required policies, establishing Lake Formation permissions at database and table levels, and using StarRocks to create an Iceberg REST catalog for accessing S3 tables.

## (Optional) Create a table bucket

You can skip this step if you already have a table bucket for Iceberg tables.

1. Sign in to the [Amazon S3 Console](https://console.aws.amazon.com/s3) as a user with administrator privileges.
2. In the upper-right corner of the page, select your AWS region.
3. In the left-side navigation pane, choose **Table buckets** from the navigation panel.
4. Click **Create table bucket** to create a table bucket.
5. After creating the table bucket, select it and click **Create table with Athena**.
6. Create a namespace.
7. After creating the namespace, click **Create table with Athena** again to create a table.

> **NOTE**
> 
> You can create a Database and Table using Athena, and then query them using StarRocks. Alternatively, you can just create a Table bucket, and then use StarRocks to create the Database and Table.
## Create IAM Policy

To access tables via the AWS Glue endpoint, create an IAM Policy with permissions for AWS Glue and Lake Formation operations:

1. Sign in to the [Amazon IAM Console](https://console.aws.amazon.com/iam) as a user with administrator privileges.
2. In the upper-right corner of the page, select your AWS region.
3. In the left-side navigation pane, choose **Policies** from the navigation panel.
4. Choose **Create a policy**, select **JSON** in the policy editor.
5. Add the following policy to grant access to AWS Glue and Lake Formation actions.

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

1. Choose **Users** from the navigation panel.
2. Select the user requiring S3 table access.
3. Click **Add permissions** and select **Attach policies directly**.
4. Attach the newly created policy.

## Manage Permissions via Lake Formation

To access S3 tables, StarRocks requires Lake Formation to first set up permissions that allow third-party query engines to access S3 tables.

1. Sign in to the [Lake Formation Console](https://console.aws.amazon.com/lakeformation) as a user with administrator privileges.
2. In the upper-right corner of the page, select your AWS region.
3. In the left-side navigation pane, choose **Application integration settings** from the navigation panel.
4. Choose **Allow external engines to access data in Amazon S3 locations with full table access.**

Next, grant the above IAM User access permissions in Lake Formation.

1. In the left-side navigation pane of the [Lake Formation Console](https://console.aws.amazon.com/lakeformation), choose **Data permissions** from the navigation panel.
2. Click **Grant**.
3. In the **Principals** section, choose **IAM users and roles**, and select the authorized IAM user from the **IAM users and roles** drop-down list.
4. In the **LF-Tags or catalog resources** section, choose **Named Data Catalog resources**, and select your table bucket you created in the **Catalogs** drop-down list.
5. In the **Catalog permissions** section, choose **Super** for **Catalog permissions**.
6. Click **Grant**.

> **NOTE**
>
> Here, Super permissions are granted for testing convenience. You need to assign appropriate permissions based on actual requirements in a production environment.
## Create Iceberg REST Catalog

Create an Iceberg REST catalog in StarRocks:

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

You can then create databases and tables run queries in it.

Example:

```SQL
-- Switch to the catalog
StarRocks> SET CATALOG starrocks_lakehouse_s3tables;
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
