---
displayed_sidebar: docs
keywords: ['beifen']
---

# CANCEL BACKUP

## 功能

取消指定数据库中一个正在进行的备份任务。更多信息，请见 备份和恢复。

## 语法

```SQL
CANCEL BACKUP FROM <db_name>
```

## 参数说明

| **参数** | **说明**               |
| -------- | ---------------------- |
| db_name  | 备份任务所属数据库名。 |

## 示例

示例一：取消 `example_db` 数据库下的备份任务。

```SQL
CANCEL BACKUP FROM example_db;
```
