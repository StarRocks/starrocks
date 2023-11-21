---
displayed_sidebar: "Chinese"
---

# REFRESH MATERIALIZED VIEW

## 功能

手动刷新指定异步物化视图或其中部分分区。

> **注意**
>
> 您只能通过该命令手动刷新刷新方式为 ASYNC 或 MANUAL 的异步物化视图。您可以通过 [SHOW MATERIALIZED VIEWS](../data-manipulation/SHOW_MATERIALIZED_VIEW.md) 查看物化视图的刷新方式。

## 语法

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
[PARTITION START ("<partition_start_date>") END ("<partition_end_date>")]
[FORCE]
[WITH { SYNC | ASYNC } MODE]
```

## 参数

| **参数**                   | **必选**     | **说明**                     |
| ------------------------- | ------------ | ---------------------------- |
| mv_name                   | 是           | 待手动刷新的异步物化视图名称。    |
| PARTITION START () END () | 否           | 手动刷新该时间区间内的分区。      |
| partition_start_date      | 否           | 待手动刷新的分区开始时间。       |
| partition_end_date        | 否           | 待手动刷新的分区结束时间。       |
| FORCE                     | 否           | 如果指定该参数，StarRocks 将强制刷新相应的物化视图或分区。如果不指定该参数，StarRocks 会自动判断数据是否被更新过，只在需要时刷新分区。|
| WITH ... MODE             | 否           | 同步或异步调用刷新任务。`SYNC` 指同步调用刷新任务，执行 SQL 语句后，StarRocks 将在刷新任务成功或失败后返回结果。`ASYNC` 指异步调用刷新任务，执行 SQL 语句后，StarRocks 将在刷新任务提交后立即返回成功，实际刷新任务会异步在后台运行。您可以通过查询 StarRocks 的 Information Schema 中的 `tasks` 和 `task_runs` 元数据表来查看异步物化视图的刷新状态。详细信息，请参考[查看异步物化视图的执行状态](../../../using_starrocks/Materialized_view.md#查看异步物化视图的执行状态)。默认值：`ASYNC`。自 v3.1.0 起支持。 |

> **注意**
>
> 刷新基于外部数据目录（External Catalog）创建的异步物化视图时，StarRocks 会刷新所有分区。

## 示例

示例一：异步调用任务手动刷新指定物化视图。

```SQL
REFRESH MATERIALIZED VIEW lo_mv1;

REFRESH MATERIALIZED VIEW lo_mv1 WITH ASYNC MODE;
```

示例二：手动刷新物化视图指定分区。

```SQL
REFRESH MATERIALIZED VIEW lo_mv1 
PARTITION START ("2020-02-01") END ("2020-03-01");
```

示例三：强制手动刷新物化视图指定分区。

```SQL
REFRESH MATERIALIZED VIEW lo_mv1 
PARTITION START ("2020-02-01") END ("2020-03-01") FORCE;
```

示例四：同步调用任务手动刷新指定物化视图。

```SQL
REFRESH MATERIALIZED VIEW lo_mv1 WITH SYNC MODE;
```
