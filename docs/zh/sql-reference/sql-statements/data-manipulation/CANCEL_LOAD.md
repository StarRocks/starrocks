# CANCEL LOAD

## 功能

取消指定的 Broker Load、Spark Load 或 INSERT 导入作业，以下情况的导入作业不能取消：

- 状态为 `CANCELLED` 或 `FINISHED` 的导入作业。
- 事务已提交的导入作业。

CANCEL LOAD 是一个异步操作，执行后可使用 [SHOW LOAD](../data-manipulation/SHOW_LOAD.md) 语句查看是否取消成功。当状态 (`State`) 为 `CANCELLED` 且导入作业失败原因 (`ErrorMsg`) 中的失败类型 (`type`) 为 `USER_CANCEL` 时，代表成功取消了导入作业。

## 语法

```SQL
CANCEL LOAD
[FROM db_name]
WHERE LABEL = "label_name"
```

## 参数说明

| **参数**   | **必选** | **说明**                                       |
| ---------- | -------- | ---------------------------------------------- |
| db_name    | 否       | 导入作业所在的数据库的名称。默认为当前数据库。 |
| label_name | 是       | 导入作业的标签。                               |

## 示例

示例一：取消当前数据库中标签为`example_label`的导入作业。

```SQL
CANCEL LOAD
WHERE LABEL = "example_label";
```

示例二：取消数据库`example_db`中标签为`example_label`的导入作业。

```SQL
CANCEL LOAD
FROM example_db
WHERE LABEL = "example_label";
```
