# CREATE REPOSITORY

## 功能

该语句用于创建仓库。仓库用于 [备份恢复](/administration/Backup_and_restore.md) 数据库数据。仅 root 或 superuser 用户可以创建仓库。

删除 REPOSITORY 操作请参考 [DROP REPOSITORY](../data-definition/DROP_REPOSITORY.md) 章节。

## 语法

```sql
CREATE [READ ONLY] REPOSITORY <repo_name>
WITH BROKER <broker_name>
ON LOCATION <repo_location>
PROPERTIES ("key"="value", ...);
```

说明：

1. 仓库的创建，依赖于已存在的 broker。broker 的详细介绍及部署方法请参考 [BROKER LOAD](/sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 章节。
2. 如果是只读仓库，则只能在仓库上进行恢复。如果不是，则可以进行备份和恢复操作。当为两个集群创建相同仓库，用以迁移数据时，可以为新集群创建只读仓库，仅赋予其恢复的权限。
3. 根据 broker 的不同类型，PROPERTIES 有所不同，具体见示例。

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

1. 创建名为 oss_repo 的仓库，依赖 `broker "oss_broker"`，数据根目录为：`oss://starRocks_backup`。

    ```sql
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

2. 创建和示例 1 相同的仓库，但属性为只读。

    ```sql
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

3. 创建名为 hdfs_repo 的仓库，依赖 broker `hdfs_broker`，数据根目录为：`hdfs://hadoop-name-node:54310/path/to/repo/`。

    ```sql
    CREATE REPOSITORY hdfs_repo
    WITH BROKER hdfs_broker
    ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
    PROPERTIES
    (
        "username" = "user",
        "password" = "password"
    );
    ```
