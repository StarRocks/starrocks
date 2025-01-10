---
displayed_sidebar: docs
keywords: ['beifen']
---

# CANCEL BACKUP

取消指定数据库中一个正在进行的备份任务。

## 语法

```SQL
CANCEL BACKUP { FROM <db_name> | FOR EXTERNAL CATALOG }
```

## 参数说明

| **参数** | **说明**               |
| -------- | ---------------------- |
| db_name  | 备份任务所属数据库名。 |
| FOR EXTERNAL CATALOG | 取消正在进行的 External Catalog 元数据备份任务。 |

## 示例

示例一：取消 `example_db` 数据库下的备份任务。

```SQL
CANCEL BACKUP FROM example_db;
```
