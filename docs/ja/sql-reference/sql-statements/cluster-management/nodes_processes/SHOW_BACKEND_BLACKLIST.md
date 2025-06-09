---
displayed_sidebar: docs
---

# SHOW BACKEND BLACKLIST

## 説明

BE Blacklist にある BE ノードを表示します。

この機能は v3.3.0 以降でサポートされています。詳細は [Manage BE Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を行うには、SYSTEM レベルの BLACKLIST 権限を持つユーザーである必要があります。

:::

## 構文

```SQL
SHOW BACKEND BLACKLIST
```

## 戻り値

| **Return**                   | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | BE ノードがブラックリストに追加された方法を示します。`MANUAL` はユーザーによって手動でブラックリストに追加されたことを示します。`AUTO` は StarRocks によって自動的にブラックリストに追加されたことを示します。 |
| LostConnectionTime           | `MANUAL` タイプの場合、BE ノードが手動でブラックリストに追加された時刻を示します。<br />`AUTO` タイプの場合、最後に正常に接続された時刻を示します。 |
| LostConnectionNumberInPeriod | `CheckTimePeriod(s)` 内で検出された切断の回数を示します。 |
| CheckTimePeriod(s)           | StarRocks がブラックリストに登録された BE ノードの接続状態を確認する間隔を示します。この値は、FE の設定項目 `black_host_history_sec` に指定した値に評価されます。単位: 秒。 |

## 例

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

## 関連 SQL

- [ADD BACKEND BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [DELETE BACKEND BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)