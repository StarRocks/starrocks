---
displayed_sidebar: docs
---

# DROP MATERIALIZED VIEW

## 説明

マテリアライズドビューを削除します。

このコマンドでは、作成中の同期マテリアライズドビューを削除することはできません。作成中の同期マテリアライズドビューを削除するには、[同期マテリアライズドビュー - 未完成のマテリアライズドビューを削除する](../../../using_starrocks/Materialized_view-single_table.md#drop-an-unfinished-synchronous-materialized-view)を参照してください。

:::tip

この操作には、対象のマテリアライズドビューに対する DROP 権限が必要です。

:::

## 構文

```SQL
DROP MATERIALIZED VIEW [IF EXISTS] [database.]mv_name
```

角括弧 [] 内のパラメータはオプションです。

## パラメータ

| **パラメータ** | **必須** | **説明** |
| ------------- | -------- | -------- |
| IF EXISTS     | いいえ   | このパラメータが指定されている場合、存在しないマテリアライズドビューを削除しても StarRocks は例外をスローしません。このパラメータが指定されていない場合、存在しないマテリアライズドビューを削除するとシステムは例外をスローします。 |
| mv_name       | はい     | 削除するマテリアライズドビューの名前。 |

## 例

例 1: 既存のマテリアライズドビューを削除する

1. データベース内のすべての既存のマテリアライズドビューを表示します。

  ```Plain
  MySQL > SHOW MATERIALIZED VIEWS\G
  *************************** 1. row ***************************
              id: 470740
          name: order_mv1
  database_name: default_cluster:sr_hub
        text: SELECT `sr_hub`.`orders`.`dt` AS `dt`, `sr_hub`.`orders`.`order_id` AS `order_id`, `sr_hub`.`orders`.`user_id` AS `user_id`, sum(`sr_hub`.`orders`.`cnt`) AS `total_cnt`, sum(`sr_hub`.`orders`.`revenue`) AS `total_revenue`, count(`sr_hub`.`orders`.`state`) AS `state_count` FROM `sr_hub`.`orders` GROUP BY `sr_hub`.`orders`.`dt`, `sr_hub`.`orders`.`order_id`, `sr_hub`.`orders`.`user_id`
          rows: 0
  1 rows in set (0.00 sec)
  ```

2. マテリアライズドビュー `order_mv1` を削除します。

  ```SQL
  DROP MATERIALIZED VIEW order_mv1;
  ```

3. 削除されたマテリアライズドビューが存在するかどうかを確認します。

  ```Plain
  MySQL > SHOW MATERIALIZED VIEWS;
  Empty set (0.01 sec)
  ```

例 2: 存在しないマテリアライズドビューを削除する

- パラメータ `IF EXISTS` が指定されている場合、存在しないマテリアライズドビューを削除しても StarRocks は例外をスローしません。

```Plain
MySQL > DROP MATERIALIZED VIEW IF EXISTS k1_k2;
Query OK, 0 rows affected (0.00 sec)
```

- パラメータ `IF EXISTS` が指定されていない場合、存在しないマテリアライズドビューを削除するとシステムは例外をスローします。

```Plain
MySQL > DROP MATERIALIZED VIEW k1_k2;
ERROR 1064 (HY000): Materialized view k1_k2 is not find
```