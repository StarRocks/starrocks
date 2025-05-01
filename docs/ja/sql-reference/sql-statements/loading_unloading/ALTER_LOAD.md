---
displayed_sidebar: docs
---

# ALTER LOAD

## Description

**QUEUEING** または **LOADING** 状態にある Broker Load ジョブの優先度を変更します。このステートメントは v2.5 からサポートされています。

> **NOTE**
>
> **LOADING** 状態にある Broker Load ジョブの優先度を変更しても、ジョブの実行には影響しません。

## Syntax

```SQL
ALTER LOAD FOR <label_name>
PROPERTIES
(
    'priority'='{LOWEST | LOW | NORMAL | HIGH | HIGHEST}'
)
```

## Parameters

| **Parameter** | **Required** | Description                                                  |
| ------------- | ------------ | ------------------------------------------------------------ |
| label_name    | Yes          | ロードジョブのラベル。フォーマット: `[<database_name>.]<label_name>`。詳細は [BROKER LOAD](BROKER_LOAD.md#database_name-and-label_name) を参照してください。 |
| priority      | Yes          | ロードジョブに指定したい新しい優先度。有効な値: `LOWEST`、`LOW`、`NORMAL`、`HIGH`、`HIGHEST`。詳細は [BROKER LOAD](BROKER_LOAD.md) を参照してください。 |

## Examples

ラベルが `test_db.label1` で、ジョブが **QUEUEING** 状態にある Broker Load ジョブがあるとします。ジョブをできるだけ早く実行したい場合、次のコマンドを実行してジョブの優先度を `HIGHEST` に変更できます。

```SQL
ALTER LOAD FOR test_db.label1
PROPERTIES
(
    'priority'='HIGHEST'
);
```