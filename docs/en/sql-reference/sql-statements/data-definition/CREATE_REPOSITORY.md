# CREATE REPOSITORY

## Description

Creates a repository in a remote storage system that is used to store data snapshots for [Backup and restore data](../../../administration/Backup_and_restore.md).

> **CAUTION**
>
> Only root user and superuser can create a repository.

For detailed instructions on deleting a repository, see [DROP REPOSITORY](../data-definition/DROP_REPOSITORY.md).

## Syntax

```SQL
CREATE [READ ONLY] REPOSITORY <repository_name>
WITH BROKER <broker_name>
ON LOCATION "<repository_location>"
PROPERTIES ("key"="value", ...)
```

## Parameters

| **Parameter**       | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| READ ONLY           | Create a read-only repository. Note that you can only restore data from a read-only repository. When creating the same repository for two clusters to migrate data, you can create a read-only warehouse for the new cluster and only grant it RESTORE permissions.|
| broker_name         | The name of the broker used to create the repository.
| repository_name     | Repository name.                                             |
| repository_location | Location of the repository in the remote storage system.     |
| PROPERTIES          | Username/password or access key/endpoint to the remote storage system. See [Examples](#examples) for more instructions. |

**PROPERTIES**:

StarRocks Supports creating repository in HDFS, S3, OSS, and COS.

- HDFS:
  - "username": Username used to log in HDFS.
  - "password": Password used to log in HDFS.

- S3:
  - "fs.s3a.access.key": Access Key used to log in S3.
  - "fs.s3a.secret.key": Secret Key used to log in S3.
  - "fs.s3a.endpoint": Endpoint of the S3 storage.

- For OSS:
  - "fs.oss.accessKeyId": Access Key ID used to log in OSS.
  - "fs.oss.accessKeySecret": Access Key Secret used to log in OSS.
  - "fs.oss.endpoint": Endpoint of the OSS storage.

- For COS:
  - "fs.cosn.userinfo.secretId": Secret ID used to log in COS.
  - "fs.cosn.userinfo.secretKey": Secret Key used to log in COS.
  - "fs.cosn.bucket.endpoint_suffix": COS endpoint suffix.

## Examples

Example 1: creates a repository named `oss_repo` using the remote storage directory `oss://starRocks_backup`.

```SQL
CREATE REPOSITORY oss_repo
WITH BROKER oss_broker
ON LOCATION "oss://starRocks_backup"
PROPERTIES
(
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
);
```

Example 2: creates a read-only repository named `oss_repo` using the remote storage directory `oss://starRocks_backup`.

```SQL
CREATE READ ONLY REPOSITORY oss_repo
WITH BROKER oss_broker
ON LOCATION "oss://starRocks_backup"
PROPERTIES
(
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
);
```

Example 3: creates a repository named `hdfs_repo` using the remote storage directory `hdfs://hadoop-name-node:xxxxx/path/to/repo/`.

```SQL
CREATE REPOSITORY hdfs_repo
WITH BROKER hdfs_broker
ON LOCATION "hdfs://hadoop-name-node:xxxxx/path/to/repo/"
PROPERTIES
(
    "username" = "xxxx",
    "password" = "yyyy"
);
```
