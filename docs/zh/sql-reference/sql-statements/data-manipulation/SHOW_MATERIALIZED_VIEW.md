---
displayed_sidebar: "Chinese"
---

# SHOW MATERIALIZED VIEW

## 功能

展示所有或指定异步物化视图信息。

> **注意**
>
> 该命令当前仅针对异步物化视图生效。针对同步物化视图您可以通过 [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) 命令查看当前数据库中同步物化视图的构建状态。

## 语法

```SQL
SHOW MATERIALIZED VIEW
[FROM db_name]
[
WHERE
[NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

## 参数

| **参数**        | **必选** | **说明**                                                     |
| --------------- | -------- | ------------------------------------------------------------ |
| db_name         | 否       | 物化视图所属的数据库名称。如果不指定该参数，则默认使用当前数据库。 |
| mv_name         | 否       | 用于精确匹配的物化视图名称。                                 |
| mv_name_matcher | 否       | 用于模糊匹配的物化视图名称 matcher。                         |

## 返回

| **返回**                   | **说明**                                                    |
| -------------------------- | --------------------------------------------------------- |
| id                         | 物化视图 ID。                                               |
| database_name              | 物化视图所属的数据库名称。                                     |
| name                       | 物化视图名称。                                               |
| rows                       | 物化视图中数据行数。                                           |
| text                       | 创建物化视图的查询语句。                                        |

## 示例

```Plain
mysql> SHOW MATERIALIZED VIEW\G
*************************** 1. row ***************************
           id: 10099
         name: order_mv
database_name: test
         text: CREATE MATERIALIZED VIEW `order_mv` (`order_id`, `total`)
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC START("2022-09-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `order_list`.`order_id`, sum(`goods`.`price`) AS `total`
FROM `wlc`.`order_list` INNER JOIN `wlc`.`goods` ON `goods`.`item_id1` = `order_list`.`item_id2`
GROUP BY `order_list`.`order_id`;
         rows: 3
1 row in set (0.02 sec)
```
