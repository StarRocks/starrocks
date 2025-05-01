---
displayed_sidebar: docs
---

# SHOW MATERIALIZED VIEW

## Description

すべてまたは特定の非同期マテリアライズドビューを表示します。

## Syntax

```SQL
SHOW MATERIALIZED VIEW
[FROM db_name]
[
WHERE
[NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

角括弧 [] 内のパラメータはオプションです。

## Parameters

| **Parameter**   | **Required** | **Description**                                              |
| --------------- | ------------ | ------------------------------------------------------------ |
| db_name         | no           | マテリアライズドビューが存在するデータベースの名前。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。 |
| mv_name         | no           | 表示するマテリアライズドビューの名前。                       |
| mv_name_matcher | no           | マテリアライズドビューをフィルタリングするために使用されるマッチャー。 |

## Returns

| **Return**                 | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| id                         | マテリアライズドビューのID。                                 |
| database_name              | マテリアライズドビューが存在するデータベースの名前。         |
| name                       | マテリアライズドビューの名前。                               |
| rows                       | マテリアライズドビュー内のデータ行数。                       |
| text                       | マテリアライズドビューを作成するために使用されたステートメント。 |

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