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
| label_name    | Yes          | ロードジョブのラベルです。形式: `[<database_name>.]<label_name>`。 [BROKER LOAD](BROKER_LOAD.md#database_name-and-label_name) を参照してください。 |
| priority      | Yes          | ロードジョブに指定したい新しい優先度です。有効な値: `LOWEST`, `LOW`, `NORMAL`, `HIGH`, および `HIGHEST`。 [BROKER LOAD](BROKER_LOAD.md) を参照してください。 |

## Examples

ラベルが `test_db.label1` で、ジョブが **QUEUEING** 状態にある Broker Load ジョブがあるとします。ジョブをできるだけ早く実行したい場合、以下のコマンドを実行してジョブの優先度を `HIGHEST` に変更できます。

```SQL
ALTER LOAD FOR test_db.label1
PROPERTIES
(
    'priority'='HIGHEST'
);
```