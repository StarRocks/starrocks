---
displayed_sidebar: "Chinese"
---

# CANCEL EXPORT

## 功能

取消指定的数据导出作业，状态为 `CANCELLED` 或 `FINISHED` 的导出作业不能取消。CANCEL EXPORT 是一个异步操作，执行后可使用 [SHOW EXPORT](./SHOW_EXPORT.md) 语句查看是否取消成功。当状态 (`State`) 为 `CANCELLED` 时，代表成功取消了导入作业。

如使用该语句，用户需要有指定导出作业所在数据库的任意一种以下权限：`SELECT_PRIV`、`LOAD_PRIV`、`ALTER_PRIV`、`CREATE_PRIV`、`DROP_PRIV`、`USAGE_PRIV`。有关权限的说明，请参见 [GRANT](../account-management/GRANT.md)。

## 语法

```SQL
CANCEL EXPORT
[FROM db_name]
WHERE QUERYID = "query_id"
```

## 参数说明

| **参数** | **必选** | **说明**                                                     |
| -------- | -------- | ------------------------------------------------------------ |
| db_name  | 否       | 数据库名称。如不指定，则默认取消当前数据库中的指定导出作业。 |
| query_id | 是       | 导出作业的查询 ID，可使用 [LAST_QUERY_ID()](../../sql-functions/utility-functions/last_query_id.md) 函数获取。注意使用该函数只能获取到最近一次的查询 ID。 |

## 示例

示例一：取消当前数据库中，查询 ID 为 `921d8f80-7c9d-11eb-9342-acde48001121` 的导出作业。

```SQL
CANCEL EXPORT
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001121";
```

示例二：取消数据库 `example_db` 中，查询 ID 为 `921d8f80-7c9d-11eb-9342-acde48001122` 的导出作业。

```SQL
CANCEL EXPORT 
FROM example_db 
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
```
