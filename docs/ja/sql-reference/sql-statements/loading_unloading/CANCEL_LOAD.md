---
displayed_sidebar: docs
---

# CANCEL LOAD

## 説明

指定されたロードジョブをキャンセルします: [Broker Load](BROKER_LOAD.md), [Spark Load](SPARK_LOAD.md), または [INSERT](INSERT.md)。`PREPARED`、`CANCELLED`、または `FINISHED` 状態のロードジョブはキャンセルできません。

ロードジョブのキャンセルは非同期プロセスです。[SHOW LOAD](SHOW_LOAD.md) ステートメントを使用して、ロードジョブが正常にキャンセルされたかどうかを確認できます。`State` の値が `CANCELLED` で、`type` の値（`ErrorMsg` に表示される）が `USER_CANCEL` の場合、ロードジョブは正常にキャンセルされています。

## 構文

```SQL
CANCEL LOAD
[FROM db_name]
WHERE LABEL = "label_name"
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | いいえ           | ロードジョブが属するデータベースの名前。このパラメータが指定されていない場合、現在のデータベース内のロードジョブがデフォルトでキャンセルされます。 |
| label_name    | はい          | ロードジョブのラベル。                                   |

## 例

例 1: 現在のデータベースでラベルが `example_label` のロードジョブをキャンセルします。

```SQL
CANCEL LOAD
WHERE LABEL = "example_label";
```

例 2: `example_db` データベースでラベルが `example_label` のロードジョブをキャンセルします。

```SQL
CANCEL LOAD
FROM example_db
WHERE LABEL = "example_label";
```