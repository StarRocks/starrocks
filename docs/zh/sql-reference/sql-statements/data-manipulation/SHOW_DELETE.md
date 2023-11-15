# SHOW DELETE

## 功能

该语句用于展示在当前数据库下，所有在明细模型表 (Duplicate Key) 上成功执行的历史删除 (DELETE) 任务。

有关删除操作的更多信息，参见 [DELETE](DELETE.md)。

## 语法

```sql
SHOW DELETE [FROM <db_name>]
```

`db_name`: 数据库名称，可选。如果不指定，默认为当前数据库。

命令返回字段：

- TableName: 表名称，即从哪些表中删除了数据。
- PartitionName：表的分区名称，即从哪些分区中删除了数据。如果表是**非分区表**，则显示为 `*`。
- CreateTime: DELETE 任务的创建时间。
- DeleteCondition: 指定的删除条件。
- State: DELETE 任务的状态。

## 示例

展示数据库 `database` 下的所有历史 DELETE 任务。

```sql
SHOW DELETE FROM database;

+------------+---------------+---------------------+-----------------+----------+
| TableName  | PartitionName | CreateTime          | DeleteCondition | State    |
+------------+---------------+---------------------+-----------------+----------+
| mail_merge | *             | 2023-03-14 10:39:03 | name EQ "Peter" | FINISHED |
+------------+---------------+---------------------+-----------------+----------+
```
