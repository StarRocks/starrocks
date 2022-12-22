# CREATE REPOSITORY

## 功能

基于远端存储系统创建用于存储数据快照的仓库。仓库用于 [备份和恢复](../../../administration/Backup_and_restore.md) 数据库数据。

> **注意**
>
> 仅 root 或 superuser 用户可以创建仓库。

删除仓库操作请参考 [DROP REPOSITORY](../data-definition/DROP%20REPOSITORY.md) 章节。

## 语法

```SQL
CREATE [READ ONLY] REPOSITORY <repository_name>
WITH BROKER
ON LOCATION "<repository_location>"
PROPERTIES ("key"="value", ...)
```

## 参数说明

| **参数**            | **说明**                                                     |
| ------------------- | ------------------------------------------------------------ |
| READ ONLY           | 创建只读仓库。请注意只读仓库只可进行恢复操作。当为两个集群创建相同仓库，用以迁移数据时，可以为新集群创建只读仓库，仅赋予其恢复的权限。|
| repository_name     | 仓库名。                                                     |
| repository_location | 远端存储系统路径。                                           |
| PROPERTIES          | 访问远端存储系统的节点及密钥或用户名及密码。具体使用方式见以下说明。 |

**PROPERTIES**:

StarRocks 支持在 HDFS、S3、OSS 以及 COS 中创建仓库。

- HDFS：
  - "username"：登陆 HDFS 的用户名。
  - "password"：登陆 HDFS 的密码。

- S3：
  - "fs.s3a.access.key"：登陆 S3 的 Access Key。
  - "fs.s3a.secret.key"：登陆 S3 的 Secret Key。
  - "fs.s3a.endpoint"：S3 存储端点。

- For OSS：
  - "fs.oss.accessKeyId"：登陆 OSS 的 Access Key ID。
  - "fs.oss.accessKeySecret"：登陆 OSS 的 Access Key Secret。
  - "fs.oss.endpoint"：OSS 存储端点。

- For COS：
  - "fs.cosn.userinfo.secretId"：登陆 COS 的 Secret ID。
  - "fs.cosn.userinfo.secretKey"：登陆 COS 的 Secret Key。
  - "fs.cosn.bucket.endpoint_suffix"：COS 存储端点后缀。

## 示例

示例一：基于数据根目录 `oss://starRocks_backup` 创建名为 `oss_repo` 的仓库。

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

示例二：基于数据根目录 `oss://starRocks_backup` 创建名为 `oss_repo` 的只读仓库。

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

示例三：基于数据根目录 `hdfs://hadoop-name-node:54310/path/to/repo/` 创建名为 `hdfs_repo` 的仓库。

```SQL
CREATE REPOSITORY `hdfs_repo`
WITH BROKER
ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
PROPERTIES
(
"username" = "xxxx",
"password" = "yyyy"
);
```
