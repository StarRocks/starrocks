---
displayed_sidebar: docs
---

# SHOW CREATE MATERIALIZED VIEW

## 説明

特定の非同期マテリアライズドビューの定義を表示します。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
SHOW CREATE MATERIALIZED VIEW [database.]<mv_name>
```

角括弧 [] 内のパラメータはオプションです。

## パラメータ

| **パラメータ** | **必須** | **説明**                                      |
| -------------- | -------- | ---------------------------------------------- |
| mv_name        | yes      | 表示するマテリアライズドビューの名前。          |

## 戻り値

| **戻り値**               | **説明**                                      |
| ------------------------ | ---------------------------------------------- |
| Materialized View        | マテリアライズドビューの名前。                |
| Create Materialized View | マテリアライズドビューの定義。                |

## 例

例 1: 特定のマテリアライズドビューの定義を表示する

```Plain
MySQL > SHOW CREATE MATERIALIZED VIEW lo_mv1\G
*************************** 1. row ***************************
       Materialized View: lo_mv1
Create Materialized View: CREATE MATERIALIZED VIEW `lo_mv1`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`lo_orderkey`) 
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
1 row in set (0.01 sec)
```