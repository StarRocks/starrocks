---
displayed_sidebar: "Chinese"
---

# SHOW ALTER MATERIALIZED VIEW

## 功能

查看同步物化视图构建状态。

## 语法

```SQL
SHOW ALTER MATERIALIZED VIEW [ { FROM | IN } db_name]
```

## 参数

| **参数** | **必选** | **说明**                                                     |
| -------- | -------- | ------------------------------------------------------------ |
| db_name  | 否       | 待查看的数据库名称。如果不指定该参数，则默认使用当前数据库。 |

## 返回

| **返回**        | **说明**             |
| --------------- | -------------------- |
| JobId           | 作业 ID。            |
| TableName       | 表名。               |
| CreateTime      | 作业创建时间。       |
| FinishedTime    | 作业结束时间。       |
| BaseIndexName   | 基表名称。           |
| RollupIndexName | 物化视图名称。       |
| RollupId        | 物化视图 Rollup ID。 |
| TransactionId   | 等待的事务 ID。      |
| State           | 创建任务状态。       |
| Msg             | 错误信息。           |
| Progress        | 创建任务进度。       |
| Timeout         | 超时时间长度。       |

## 示例

### 示例一：查看同步物化视图刷新任务

```Plain
MySQL > SHOW ALTER MATERIALIZED VIEW\G
*************************** 1. row ***************************
          JobId: 475991
      TableName: lineorder
     CreateTime: 2022-08-24 19:46:53
   FinishedTime: 2022-08-24 19:47:15
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_1
       RollupId: 475992
  TransactionId: 33067
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
*************************** 2. row ***************************
          JobId: 477337
      TableName: lineorder
     CreateTime: 2022-08-24 19:47:25
   FinishedTime: 2022-08-24 19:47:45
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_2
       RollupId: 477338
  TransactionId: 33068
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
2 rows in set (0.00 sec)
```
