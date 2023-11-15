# CREATE REPOSITORY

## 功能

该语句用于创建仓库。仓库用于 [备份恢复](/administration/Backup_and_restore.md) 数据库数据。仅 root 或 superuser 用户可以创建仓库。

删除 REPOSITORY 操作请参考 [DROP REPOSITORY](../data-definition/DROP_REPOSITORY.md) 章节。

## 语法

```sql
CREATE [READ ONLY] REPOSITORY `repo_name`
WITH BROKER `broker_name`
ON LOCATION `repo_location`
PROPERTIES ("key"="value", ...);
```

注：方括号 [] 中内容可省略不写。

说明：

1. 仓库的创建，依赖于已存在的 broker, broker 的详细介绍及部署方法请参考 [BROKER LOAD](/sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 章节。
2. 如果是只读仓库，则只能在仓库上进行恢复。如果不是，则可以进行备份和恢复操作。
3. 根据 broker 的不同类型，PROPERTIES 有所不同，具体见示例。

## 示例

1. 创建名为 bos_repo 的仓库，依赖 `BOS broker "bos_broker"`，数据根目录为：`bos://starRocks_backup`。

    ```sql
    CREATE REPOSITORY `bos_repo`
    WITH BROKER `bos_broker`
    ON LOCATION "bos://starRocks_backup"
    PROPERTIES
    (
        "bos_endpoint" = "http://gz.bcebos.com",
        "bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
        "bos_secret_accesskey"="70999999999999de274d59eaa980a"
    );
    ```

2. 创建和示例 1 相同的仓库，但属性为只读。

    ```sql
    CREATE READ ONLY REPOSITORY `bos_repo`
    WITH BROKER `bos_broker`
    ON LOCATION "bos://starRocks_backup"
    PROPERTIES
    (
        "bos_endpoint" = "http://gz.bcebos.com",
        "bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
        "bos_secret_accesskey"="70999999999999de274d59eaa980a"
    );
    ```

3. 创建名为 hdfs_repo 的仓库，依赖 `Baidu hdfs broker "hdfs_broker"`，数据根目录为：`hdfs://hadoop-name-node:54310/path/to/repo/`。

    ```sql
    CREATE REPOSITORY `hdfs_repo`
    WITH BROKER `hdfs_broker`
    ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
    PROPERTIES
    (
        "username" = "user",
        "password" = "password"
    );
    ```

## 关键字(keywords)

CREATE REPOSITORY
