---
displayed_sidebar: docs
---

# SHOW BACKEND/COMPUTE NODE BLACKLIST

BE および CN ブラックリストにある BE/CN ノードを表示します。

BE ブラックリストは v3.3.0 以降でサポートされ、CN ブラックリストは v4.0 以降でサポートされています。詳細については、[Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を実行できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。

:::

## Syntax

```SQL
SHOW { BACKEND | COMPUTE NODE } BLACKLIST
```

## Return value

| **Return**                   | **Description**                                              |
| ---------------------------- | ------------------------------------------------------------ |
| AddBlackListType             | BE/CN ノードがブラックリストに追加された方法。`MANUAL` はユーザーによって手動でブラックリストに追加されたことを示します。`AUTO` は StarRocks によって自動的にブラックリストに追加されたことを示します。 |
| LostConnectionTime           | `MANUAL` タイプの場合、BE/CN ノードが手動でブラックリストに追加された時間を示します。<br />`AUTO` タイプの場合、最後に成功した接続が確立された時間を示します。 |
| LostConnectionNumberInPeriod | `CheckTimePeriod(s)` 内で検出された切断の回数。 |
| CheckTimePeriod(s)           | StarRocks がブラックリストに登録された BE/CN ノードの接続状態を確認する間隔。この値は、FE 設定項目 `black_host_history_sec` に指定した値に評価されます。単位: 秒。 |

## Examples

```SQL
-- BE ブラックリストを表示します。
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+

-- CN ブラックリストを表示します。
SHOW COMPUTE NODE BLACKLIST;
+---------------+------------------+---------------------+------------------------------+--------------------+
| ComputeNodeId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+---------------+------------------+---------------------+------------------------------+--------------------+
| 10005         | MANUAL           | 2025-08-18 10:47:51 | 0                            | 5                  |
+---------------+------------------+---------------------+------------------------------+--------------------+
```

## Relevant SQLs

- [ ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [ DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [ SHOW BACKENDS](SHOW_BACKENDS.md)
- [ SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)