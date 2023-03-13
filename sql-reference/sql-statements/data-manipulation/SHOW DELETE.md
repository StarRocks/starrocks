# SHOW DELETE

## 功能

该语句用于展示在当前数据库下，所有在明细模型表 (Duplicate Key) 上已执行成功的历史 DELETE 任务。

## 语法

```sql
SHOW DELETE [FROM <db_name>]
```

## 示例

展示数据库 `database` 下的所有历史 DELETE 任务。

```sql
SHOW DELETE FROM database;
```
