---
displayed_sidebar: docs
---

# CREATE REPOSITORY

## Description

Creates a repository in a remote storage system that is used to store data snapshots for backing up and restoring data.

> **CAUTION**
>
> Only users with the ADMIN privilege can create a repository.

For detailed instructions on deleting a repository, see [DROP REPOSITORY](./DROP_REPOSITORY.md).

## Syntax

```SQL
CREATE [READ ONLY] REPOSITORY <repository_name>
WITH BROKER
ON LOCATION "<repository_location>"
PROPERTIES ("key"="value", ...)
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| READ ONLY           | Create a read-only repository. Note that you can only restore data from a read-only repository. When creating the same repository for two clusters to migrate data, you can create a read-only repository for the new cluster and only grant it RESTORE permissions.|
| repository_name     | Repository name. For the naming conventions, see [System limits](../../System_limit.md).                           |
| repository_location | Location of the repository in the remote storage system.     |
| PROPERTIES          |The credential method for accessing the remote storage system. |

**PROPERTIES**:

StarRocks supports creating repositories in HDFS, AWS S3, and Google GCS.

- For HDFS:
  - "username": The username of the account that you want to use to access the NameNode of the HDFS cluster.
  - "password": The password of the account that you want to use to access the NameNode of the HDFS cluster.

- For AWS S3:
  - "aws.s3.use_instance_profile": Whether or not to allow instance profile and assumed role as credential methods for accessing AWS S3. Default: `false`.

    - If you use IAM user-based credential (Access Key and Secret Key) to access AWS S3, you don't need to specify this parameter, and you need to specify "aws.s3.access_key", "aws.s3.secret_key", and "aws.s3.endpoint".
    - If you use Instance Profile to access AWS S3, you need to set this parameter to `true` and specify "aws.s3.region".
    - If you use Assumed Role to access AWS S3, you need to set this parameter to `true` and specify "aws.s3.iam_role_arn" and "aws.s3.region".
  
  - "aws.s3.access_key": The Access Key ID that you can use to access the Amazon S3 bucket.
  - "aws.s3.secret_key": The Secret Access Key that you can use to access the Amazon S3 bucket.
  - "aws.s3.endpoint": The endpoint that you can use to access the Amazon S3 bucket.
  - "aws.s3.iam_role_arn": The ARN of the IAM role that has privileges on the AWS S3 bucket in which your data files are stored. If you want to use assumed role as the credential method for accessing AWS S3, you must specify this parameter. Then, StarRocks will assume this role when it analyzes your Hive data by using a Hive catalog.
  - "aws.s3.region": The region in which your AWS S3 bucket resides. Example: `us-west-1`.

> **NOTE**
>
> StarRocks supports creating repositories in AWS S3 only according to the S3A protocol. Therefore, when you create repositories in AWS S3, you must replace `s3://` in the S3 URI you pass as a repository location in `ON LOCATION` with `s3a://`.

- For Google GCS:
  - "fs.s3a.access.key": The Access Key that you can use to access the Google GCS bucket.
  - "fs.s3a.secret.key": The Secret Key that you can use to access the Google GCS bucket.
  - "fs.s3a.endpoint": The endpoint that you can use to access the Google GCS bucket.

> **NOTE**
>
> StarRocks supports creating repositories in Google GCS only according to the S3A protocol. Therefore, when you create repositories in Google GCS, you must replace the prefix in the GCS URI you pass as a repository location in `ON LOCATION` with `s3a://`.

## Examples

Example 1: Create a repository named `hdfs_repo` in an Apache™ Hadoop® cluster.

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER
ON LOCATION "hdfs://x.x.x.x:yyyy/repo_dir/backup"
PROPERTIES(
    "username" = "xxxxxxxx",
    "password" = "yyyyyyyy"
);
```

Example 2: Create a read-only repository named `s3_repo` in the Amazon S3 bucket `bucket_s3`.

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

Example 3: Create a repository named `gcs_repo` in the Google GCS bucket `bucket_gcs`.

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
