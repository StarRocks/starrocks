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
  - "username": Username used to log in HDFS.
  - "password": Password used to log in HDFS.

- For AWS S3:
  - "fs.s3a.access.key": Access Key used to log in S3.
  - "fs.s3a.secret.key": Secret Key used to log in S3.
  - "fs.s3a.endpoint": Endpoint of the S3 storage.

- For Google GCS:
  - "fs.s3a.access.key": Access Key used to log in S3.
  - "fs.s3a.secret.key": Secret Key used to log in S3.
  - "fs.s3a.endpoint": Endpoint of the S3 storage.

## Examples

Example 1: Create a repository named `hdfs_repo` in an Apache™ Hadoop® cluster.

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER
ON LOCATION "hdfs://<hdfs_host>:<hdfs_port>/repo_dir/backup"
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
