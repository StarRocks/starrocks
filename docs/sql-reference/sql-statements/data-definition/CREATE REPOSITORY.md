# CREATE REPOSITORY

## Description

Creates a repository in an remote storage system that is used to store data snapshots for [data backup and restoration](../../../administration/Backup_and_restore.md).

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
| READ ONLY           | Create a read-only repository. Note that you can only restore data from a read-only repository. |
| repository_name     | Repository name.                                             |
| repository_location | Location of the repository in the remote storage system.     |
| PROPERTIES          | Username/password or access key/endpoint to the remote storage system. See [Examples](#examples) for more instructions. |

## Examples

Example 1: creates a repository named `oss_repo` using the remote storage directory `oss://starRocks_backup`.

```SQL
CREATE REPOSITORY `oss_repo`
WITH BROKER
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
CREATE READ ONLY REPOSITORY `oss_repo`
WITH BROKER
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
CREATE REPOSITORY `hdfs_repo`
WITH BROKER
ON LOCATION "hdfs://hadoop-name-node:xxxxx/path/to/repo/"
PROPERTIES
(
    "username" = "xxxx",
    "password" = "yyyy"
);
```
