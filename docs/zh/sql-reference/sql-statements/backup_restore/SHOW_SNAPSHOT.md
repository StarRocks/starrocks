---
displayed_sidebar: docs
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
