---
displayed_sidebar: docs
---

# ALTER LOAD

## 説明

**QUEUEING** または **LOADING** 状態にある Broker Load ジョブの優先度を変更します。このステートメントは v2.5 からサポートされています。

> **注意**
>
> **LOADING** 状態にある Broker Load ジョブの優先度を変更しても、ジョブの実行には影響しません。

## 構文

```SQL
ALTER LOAD FOR <label_name>
PROPERTIES
(
    'priority'='{LOWEST | LOW | NORMAL | HIGH | HIGHEST}'
)
```

## パラメータ

| **パラメータ** | **必須** | 説明                                                  |
| ------------- | ------------ | ------------------------------------------------------------ |
| label_name    | はい          | ロードジョブのラベル。形式: `[<database_name>.]<label_name>`。詳細は [BROKER LOAD](BROKER_LOAD.md#database_name-and-label_name) を参照してください。 |
| priority      | はい          | ロードジョブに指定したい新しい優先度。有効な値: `LOWEST`、`LOW`、`NORMAL`、`HIGH`、`HIGHEST`。詳細は [BROKER LOAD](BROKER_LOAD.md) を参照してください。 |

## 例

`test_db.label1` というラベルの Broker Load ジョブが **QUEUEING** 状態にあるとします。ジョブをできるだけ早く実行したい場合、以下のコマンドを実行してジョブの優先度を `HIGHEST` に変更できます。

```SQL
ALTER LOAD FOR test_db.label1
PROPERTIES
(
    'priority'='HIGHEST'
);
```