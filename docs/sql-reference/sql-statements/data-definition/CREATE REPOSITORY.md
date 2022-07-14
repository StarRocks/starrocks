# CREATE REPOSITORY

## description

This statement is used to create the repository. The repository is used for backup or restore. Only root or superuser users can create repository.

Syntax:

```sql
CREATE [READ ONLY] REPOSITORY `repo_name`
WITH BROKER `broker_name`
ON LOCATION `repo_location`
PROPERTIES ("key"="value", ...);
```

Note:

1. The creation of repositories depends on existing brokers.
2. If it is a read-only repository, it can only be restored on the repository. If not, it can be backed up and restored.
3. PROPERTIES vary depending on broker of different types.

## example

1. Create a repository named bos_repo based on BOS broker "bos_broker", and the data root directory is: bos://palo_backup.

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

2. Create a same repository as in example 1, but with read-only attribute:

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

3. Create a repository named hdfs_repo based on Baidu HDFS broker "hdfs_broker", and the data root directory is: hdfs://hadoop-name-node:54310/path/to/repo./

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

## keyword

CREATE REPOSITORY
