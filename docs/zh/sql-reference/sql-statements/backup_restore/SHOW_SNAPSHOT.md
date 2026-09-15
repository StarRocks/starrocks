---
displayed_sidebar: docs
description: "查看指定仓库中的数据快照备份。"
---

# SHOW SNAPSHOT

## 功能

查看指定仓库中的备份。更多信息，请见 [备份和恢复](../../../administration/management/Backup_and_restore.md)。

## 语法

```SQL
SHOW SNAPSHOT ON <repo_name>
[WHERE SNAPSHOT = <snapshot_name> [AND TIMESTAMP = <backup_timestamp>]]
```

## 参数说明

| **参数**         | **说明**         |
| ---------------- | ---------------- |
| repo_name        | 备份所属仓库名。 |
| snapshot_nam     | 备份名。         |
| backup_timestamp | 备份时间戳。     |

## 返回

| **返回**  | **说明**                                    |
| --------- | ------------------------------------------- |
| Snapshot  | 备份名。                                    |
| Timestamp | 备份时间戳。                                |
| Status    | 如果备份正常，则显示 OK，否则显示错误信息。 |
| Database  | 备份所属数据库名。                          |
| Details   | 备份的数据目录及文件结构。JSON 格式。       |
| ClusterId | 创建该备份的集群 ID。自 v4.2.0 起支持。     |
| FinishTime | 备份完成的时间。自 v4.2.0 起支持。         |
| TTL       | 备份的保留时长，由 [BACKUP](./BACKUP.md) 的 `ttl` 属性指定。自 v4.2.0 起支持。 |
| ExpireTime | 备份的到期时间，到期后可被自动清理。自 v4.2.0 起支持。 |

`ClusterId`、`FinishTime`、`TTL` 和 `ExpireTime` 四列读自备份自身。当备份元数据不可读时四列均为 `NULL`，包括 v4.2.0 之前创建的备份、备份仍在进行中，以及备份中断的情况。永久保留的备份，其 `TTL` 与 `ExpireTime` 也为 `NULL`。

`Timestamp` 是备份开始的时间，也是快照的命名依据；`FinishTime` 是备份收尾的时间，`ExpireTime` 即由它算得。

## 示例

示例一：查看仓库 `example_repo` 中已有的备份。

```SQL
SHOW SNAPSHOT ON example_repo;
```

示例二：查看仓库 `example_repo` 中名为 `backup1` 的备份。

```SQL
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1";
```

示例三：查看仓库 `example_repo` 中名为 `backup1` 、时间戳为 `2018-05-05-15-34-26` 的备份。

```SQL
SHOW SNAPSHOT ON example_repo 
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
```
