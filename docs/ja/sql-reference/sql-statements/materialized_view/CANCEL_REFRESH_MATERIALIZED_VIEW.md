---
displayed_sidebar: docs
---

# CANCEL REFRESH MATERIALIZED VIEW

## 説明

非同期マテリアライズドビューのリフレッシュタスクをキャンセルします。

:::tip

この操作には、対象のマテリアライズドビューに対する REFRESH 権限が必要です。

:::

## 構文

```SQL
CANCEL REFRESH MATERIALIZED VIEW [<database_name>.]<materialized_view_name>
```

## パラメーター

| **パラメーター**       | **必須**     | **説明**                                                     |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| database_name          | いいえ       | マテリアライズドビューが存在するデータベースの名前。このパラメーターが指定されていない場合、現在のデータベースが使用されます。 |
| materialized_view_name | はい         | マテリアライズドビューの名前。                               |

## 例

例 1: 非同期リフレッシュマテリアライズドビュー `lo_mv1` のリフレッシュタスクをキャンセルします。

```SQL
CANCEL REFRESH MATERIALIZED VIEW lo_mv1;
```