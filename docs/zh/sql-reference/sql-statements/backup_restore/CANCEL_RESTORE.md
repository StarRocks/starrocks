---
displayed_sidebar: docs
keywords: ['beifen']
---

# CANCEL RESTORE

取消指定数据库中一个正在进行的恢复任务。

> **注意**
>
> 如果恢复作业在 COMMIT 阶段被取消，已恢复的数据将会损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。

## 语法

```SQL
CANCEL RESTORE { FROM <db_name> | FOR EXTERNAL CATALOG }
```

## 参数说明

| **参数** | **说明**               |
| -------- | ---------------------- |
| db_name  | 恢复任务所属数据库名。 |
| FOR EXTERNAL CATALOG | 取消正在进行的 External Catalog 元数据恢复任务。 |

## 示例

示例一：取消 `example_db` 数据库下的恢复任务。

```SQL
CANCEL RESTORE FROM example_db;
```
