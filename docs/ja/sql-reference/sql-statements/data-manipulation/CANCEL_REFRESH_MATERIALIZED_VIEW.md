---
displayed_sidebar: docs
---

# CANCEL REFRESH MATERIALIZED VIEW

## Description

非同期マテリアライズドビューのリフレッシュタスクをキャンセルします。

## Syntax

```SQL
CANCEL REFRESH MATERIALIZED VIEW [<database_name>.]<materialized_view_name>
```

## Parameters

| **Parameter**          | **Required** | **Description**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| database_name          | No           | マテリアライズドビューが存在するデータベースの名前。このパラメータが指定されていない場合、現在のデータベースが使用されます。 |
| materialized_view_name | Yes          | マテリアライズドビューの名前。                               |

## Examples

Example 1: 非同期リフレッシュマテリアライズドビュー `lo_mv1` のリフレッシュタスクをキャンセルします。

```SQL
CANCEL REFRESH MATERIALIZED VIEW lo_mv1;
```