# CREATE REPOSITORY

## Description

Creates a repository in a remote storage system that is used to store data snapshots for [Backup and restore data](../../../administration/Backup_and_restore.md).

> **CAUTION**
>
> Only root user and superuser can create a repository.

For detailed instructions on deleting a repository, see [DROP REPOSITORY](../data-definition/DROP%20REPOSITORY.md).

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
| READ ONLY           | Create a read-only repository. Note that you can only restore data from a read-only repository. When creating the same repository for two clusters to migrate data, you can create a read-only warehouse for the new cluster and only grant it RESTORE permissions.|
| repository_name     | Repository name.                                             |
| repository_location | Location of the repository in the remote storage system.     |
| PROPERTIES          | Username/password or credential/endpoint to the remote storage system. See [Examples](#examples) for more instructions. |

**PROPERTIES**:

StarRocks Supports creating repository in HDFS, AWS S3, and Google GCS.

- For HDFS:
  - "username": The username of the account that you want to use to access the NameNode of the HDFS cluster.
  - "password": The password of the account that you want to use to access the NameNode of the HDFS cluster.

- For AWS S3:
  - "fs.s3a.access.key": The Access Key ID that you can use to access the Amazon S3 bucket.
  - "fs.s3a.secret.key": The Secret Access Key that you can use to access the Amazon S3 bucket.
  - "fs.s3a.endpoint": The endpoint that you can use to access the Amazon S3 bucket.

- For Google GCS:
  - "fs.s3a.access.key": The Access Key that you can use to access the Google GCS bucket.
  - "fs.s3a.secret.key": The Secret Key that you can use to access the Google GCS bucket.
  - "fs.s3a.endpoint": The endpoint that you can use to access the Google GCS bucket.

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
