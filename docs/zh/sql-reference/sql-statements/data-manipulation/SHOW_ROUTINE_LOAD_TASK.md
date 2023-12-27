---
displayed_sidebar: "Chinese"
---

# SHOW ROUTINE LOAD TASK

## 功能

查看 Routine Load 导入作业下属导入任务的执行情况。

:::note

Routine Load 中导入作业和导入任务的关系，参见[使用 Routine Load 导入数据](../../../loading/RoutineLoad.md#基本原理)。

:::

## 语法

```SQL
SHOW ROUTINE LOAD TASK
    [FROM <db_name>]
    [ WHERE JobName = <job_name> ]
```

:::note

返回结果中的字段较多，可使用 `\G` 分行，如 `SHOW ROUTINE LOAD TASK FOR JobName = <job_name> \G`。

:::

## 参数说明

| **参数** | **必选** | **说明**                              |
| -------- | -------- | ------------------------------------- |
| db_name  | 否       | Routine Load 导入作业所属数据库名称。 |
| JobName  | 否       | Routine Load 导入作业名称。           |

## 返回结果说明

| 字段                 | 说明                                                         |
| -------------------- | ------------------------------------------------------------ |
| TaskId               | 导入任务的全局唯一 ID，由 StarRocks 自动生成。               |
| TxnId                | 该导入任务所属事务 ID。                                      |
| TxnStatus            | 该导入任务所属事务状态。`UNKOWN` 表示事务没有开始，可能是导入任务还没有下发，或者没有实际执行。 |
| JobId                | 导入作业 ID。                                                |
| CreateTime           | 该导入任务创建时间。                                         |
| LastScheduledTime    | 上一次调度该导入任务的时间。                                 |
| ExecuteStartTime     | 该导入任务执行时间。                                         |
| Timeout              | 导入任务超时时间，由 FE 参数 [`routine_load_task_timeout_second`](../../../administration/Configuration.md#routine_load_task_timeout_second) 和 Routine Load 导入作业的 [job_properties](./CREATE_ROUTINE_LOAD.md#job_properties) 中的参数 `task_timeout_second` 控制。 |
| BeId                 | 执行该导入任务的 BE 节点 ID。                                |
| DataSourceProperties | 该导入任务消费分区数据的进度。                               |
| Message              | 导入任务的返回信息，包含任务错误信息。                       |

## 示例

查看 Routine Load  导入作业 `example_tbl_ordertest` 下的所有导入任务。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "example_tbl_ordertest";  
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| TaskId                               | TxnId | TxnStatus | JobId | CreateTime          | LastScheduledTime   | ExecuteStartTime | Timeout | BeId | DataSourceProperties               | Message                                                                     |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| abde6998-c19a-43d6-b48c-6ca7e14144a3 | -1    | UNKNOWN   | 10208 | 2023-12-22 12:46:10 | 2023-12-22 12:47:00 | NULL             | 60      | -1   | Progress:{"0":6},LatestOffset:null | there is no new data in kafka/pulsar, wait for 10 seconds to schedule again |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
