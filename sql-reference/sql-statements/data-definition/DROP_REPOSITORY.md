# DROP REPOSITORY

## 功能

该语句用于删除一个已创建的仓库。仅 root 或 superuser 用户可以删除仓库。

创建 REPOSITORY 操作请参考 [CREATE REPOSITORY](../data-definition/CREATE_REPOSITORY.md) 章节。

## 语法

```sql
DROP REPOSITORY `repo_name`;
```

说明：

删除仓库，仅仅是 **删除该仓库在 StarRocks 中的映射**，不会删除实际的仓库数据。删除后，可以再次通过指定相同的 broker 和 LOCATION 映射到该仓库。

## 示例

1. 删除名为 oss_repo 的仓库。

    ```sql
    DROP REPOSITORY `oss_repo`;
    ```
