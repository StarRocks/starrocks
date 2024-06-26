---
displayed_sidebar: "English"
---

# AWS IAM policies

A policy in AWS IAM declares a set of permissions on a specific AWS resource. After creating a policy, you need to attach it to an IAM role or user. As such, the IAM role or user is assigned the permissions declared in the policy to access the specified AWS resource.

Different operations in StarRocks require you to have access permissions on different AWS resources, and therefore you need to configure different policies.

This topic provides the IAM policies that you need to configure for StarRocks to integrate with different AWS resources in various business scenarios when you choose [the instance profile, assumed role, or IAM user-based authentication method](../integrations/authenticate_to_aws_resources.md#preparations).

## Batch load data from AWS S3

If you want to load data from your S3 bucket, configure the following IAM policy:

> **NOTICE**
>
> Replace `<bucket_name>` in the following JSON policy template with the name of your S3 bucket that stores your data files.

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

## Read/write AWS S3

If you want to query data from your S3 bucket, configure the following IAM policy:

> **NOTICE**
>
> Replace `<bucket_name>` in the following JSON policy template with the name of your S3 bucket that stores your data files.

```SQL
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Sid": "s3",
          "Effect": "Allow",
          "Action": [
              "s3:PutObject",
              "s3:GetObject",
              "s3:DeleteObject"
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

## Integrate with AWS Glue

If you want to integrate with your AWS Glue Data Catalog, configure the following IAM policy:

```SQL
{
    "Version": "2012-10-17",
    "Statement": [
      {
          "Effect": "Allow",
          "Action": [
                "glue:BatchCreatePartition",
                "glue:UpdateDatabase",
                "glue:GetConnections",
                "glue:CreateTable",
                "glue:DeleteDatabase",
                "glue:BatchUpdatePartition",
                "glue:GetTables",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:UpdateTable",
                "glue:BatchGetPartition",
                "glue:DeleteTable",
                "glue:GetDatabases",
                "glue:GetDevEndpoint",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:GetDevEndpoints",
                "glue:GetConnection",
                "glue:CreateDatabase",
                "glue:CreatePartition",
                "glue:DeletePartition",
                "glue:UpdatePartition"
          ],
          "Resource": [
              "*"
            ]
        }
    ]
}
```
