# REFRESH MATERIALIZED VIEW

## 功能

手动刷新指定异步物化视图或其中部分分区。

> **注意**
>
> 您只能通过该命令手动刷新刷新方式为 ASYNC 或 MANUAL 的异步物化视图。您可以通过 [SHOW MATERIALIZED VIEWS](../data-manipulation/SHOW%20MATERIALIZED%20VIEW.md) 查看物化视图的刷新方式。

## 语法

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
[PARTITION START ("<partition_start_date>") END ("<partition_end_date>")] [FORCE]
```

## 参数

| **参数** | **必选** | **说明**                     |
| -------- | -------- | ---------------------------- |
| mv_name                   | 是          | 待手动刷新的异步物化视图名称 |
| PARTITION START () END () | 否           | 手动刷新该时间区间内的分区。 |
| partition_start_date      | 否           | 待手动刷新的分区开始时间。  |
| partition_end_date        | 否           | 待手动刷新的分区结束时间。  |
| FORCE                     | 否           | 如果指定该参数，StarRocks 将强制刷新相应的物化视图或分区。如果不指定该参数，StarRocks 会自动判断分区是否被更新过，只在需要时刷新分区。|

> **注意**
>
> 刷新基于外部数据目录（External Catalog）创建的异步物化视图时，StarRocks 会刷新所有分区。

## 示例

示例一：手动刷新指定物化视图。

```SQL
REFRESH MATERIALIZED VIEW lo_mv1;
```

示例二：手动刷新物化视图指定分区。

```SQL
REFRESH MATERIALIZED VIEW mv 
PARTITION START ("2020-02-01") END ("2020-03-01");
```

示例三：强制手动刷新物化视图指定分区。

```SQL
REFRESH MATERIALIZED VIEW mv 
PARTITION START ("2020-02-01") END ("2020-03-01") FORCE;
```
