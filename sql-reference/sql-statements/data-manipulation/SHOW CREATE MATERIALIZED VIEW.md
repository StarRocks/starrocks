# SHOW CREATE MATERIALIZED VIEW

## 功能

查看指定物化视图定义。

## 语法

```SQL
SHOW CREATE MATERIALIZED VIEW [database.]mv_name
```

注：方括号 [] 中内容可省略不写。

## 参数

| **参数** | **必选** | **说明**                     |
| -------- | -------- | ---------------------------- |
| mv_name  | 是       | 待查看定义的物化视图的名称。 |

## 返回

| **返回**                 | **说明**         |
| ------------------------ | ---------------- |
| Materialized View        | 物化视图的名称。 |
| Create Materialized View | 物化视图的定义。 |

## 示例

### 示例一：查看指定物化视图定义

```Plain
MySQL > SHOW CREATE MATERIALIZED VIEW lo_mv1\G
*************************** 1. row ***************************
       Materialized View: lo_mv1
Create Materialized View: CREATE MATERIALIZED VIEW `lo_mv1`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10 
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
1 row in set (0.01 sec)
```
