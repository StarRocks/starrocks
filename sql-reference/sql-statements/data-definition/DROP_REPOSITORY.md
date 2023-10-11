# DROP REPOSITORY

## 功能

删除一个仓库。仓库用于 [备份和恢复](../../../administration/Backup_and_restore.md) 数据库数据。

> **注意**
>
> - 只有拥有 System 级 REPOSITORY 权限的用户才可以删除仓库。
> - 该操作仅删除仓库在 StarRocks 中的映射，不会删除实际的仓库数据。如需删除备份数据，您需要手动删除备份在远端存储系统的快照路径。删除后，您可以再次通过指定相同的远端存储系统路径映射到该仓库。

## 语法

```SQL
DROP REPOSITORY <repository_name>
```

## 参数说明

| **参数**        | **说明**         |
| --------------- | ---------------- |
| repository_name | 要删除的仓库名。 |

## 示例

示例一：删除名为 `oss_repo` 的仓库。

```SQL
DROP REPOSITORY `oss_repo`;
```
