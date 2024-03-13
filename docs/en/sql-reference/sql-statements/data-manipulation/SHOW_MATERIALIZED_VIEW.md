---
displayed_sidebar: "English"
---

# SHOW MATERIALIZED VIEW

## Description

Shows all or one specific asynchronous materialized view.

## Syntax

```SQL
SHOW MATERIALIZED VIEW
[FROM db_name]
[
WHERE
[NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter**   | **Required** | **Description**                                              |
| --------------- | ------------ | ------------------------------------------------------------ |
| db_name         | no           | The name of the database to which the materialized view resides. If this parameter is not specified, the current database is used by default. |
| mv_name         | no           | The name of the materialized view to show.                   |
| mv_name_matcher | no           | The matcher used to filter materialized views.               |

## Returns

| **Return**                 | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| id                         | The ID of the materialized view.                             |
| database_name              | The name of the database in which the materialized view resides. |
| name                       | The name of the materialized view.                           |
| rows                       | The number of data rows in the materialized view.            |
| text                       | The statement used to create the materialized view.          |

## Examples

```Plain
MySQL > SHOW MATERIALIZED VIEW\G
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
