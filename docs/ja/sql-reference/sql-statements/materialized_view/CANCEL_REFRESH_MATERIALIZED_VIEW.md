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
CANCEL REFRESH MATERIALIZED VIEW [<database_name>.]<materialized_view_name> [FORCE]
```

## パラメータ

| **パラメータ**         | **必須**     | **説明**                                                     |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| database_name          | いいえ       | マテリアライズドビューが存在するデータベースの名前。このパラメータが指定されていない場合、現在のデータベースが使用されます。 |
| materialized_view_name | はい         | マテリアライズドビューの名前。                               |
| FORCE                  | いいえ       | 実行中のマテリアライズドビューのリフレッシュタスクを強制的にキャンセルします。 |

## 例

例 1: 非同期リフレッシュマテリアライズドビュー `lo_mv1` のリフレッシュタスクをキャンセルします。

```SQL
CANCEL REFRESH MATERIALIZED VIEW lo_mv1;
CANCEL REFRESH MATERIALIZED VIEW lo_mv1 FORCE;
```