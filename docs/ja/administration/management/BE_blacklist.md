---
displayed_sidebar: docs
---

# Manage BE Blacklist

このトピックでは、BE Blacklist の管理方法について説明します。

v3.3.0 以降、StarRocks は BE Blacklist 機能をサポートしており、特定の BE ノードのクエリ実行を禁止することで、BE ノードへの接続失敗による頻繁なクエリ失敗やその他の予期しない動作を回避できます。

デフォルトでは、StarRocks は BE Blacklist を自動的に管理し、接続が失われた BE ノードをブラックリストに追加し、接続が再確立されたときにブラックリストから削除します。ただし、ノードが手動でブラックリストに追加された場合、StarRocks はそのノードをブラックリストから削除しません。

:::note

- SYSTEM レベルの BLACKLIST 権限を持つユーザーのみがこの機能を使用できます。
- 各 FE ノードは独自の BE Blacklist を保持し、他の FE ノードと共有しません。

:::

## BE をブラックリストに追加する

[ADD BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md) を使用して、BE ノードを手動で BE Blacklist に追加できます。このステートメントでは、ブラックリストに追加する BE ノードの ID を指定する必要があります。BE ID は [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) を実行して取得できます。

例:

```SQL
-- BE ID を取得します。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- BE をブラックリストに追加します。
ADD BACKEND BLACKLIST 10001;
```

## ブラックリストから BE を削除する

[DELETE BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md) を使用して、BE ノードを手動で BE Blacklist から削除できます。このステートメントでも、BE ノードの ID を指定する必要があります。

例:

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## BE Blacklist を表示する

[SHOW BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md) を使用して、BE Blacklist にある BE ノードを表示できます。

例:

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

返されるフィールドは次のとおりです:

- `AddBlackListType`: BE ノードがブラックリストに追加された方法です。`MANUAL` はユーザーによって手動でブラックリストに追加されたことを示します。`AUTO` は StarRocks によって自動的にブラックリストに追加されたことを示します。
- `LostConnectionTime`:
  - `MANUAL` タイプの場合、BE ノードが手動でブラックリストに追加された時間を示します。
  - `AUTO` タイプの場合、最後に成功した接続が確立された時間を示します。
- `LostConnectionNumberInPeriod`: `CheckTimePeriod(s)` 内で検出された切断の数であり、StarRocks がブラックリストにある BE ノードの接続状態をチェックする間隔です。
- `CheckTimePeriod(s)`: ブラックリストにある BE ノードの接続状態を StarRocks がチェックする間隔です。その値は FE 設定項目 `black_host_history_sec` に指定した値に評価されます。単位: 秒。

## BE Blacklist の自動管理を設定する

BE ノードが FE ノードへの接続を失うたびに、または BE ノードでタイムアウトが原因でクエリが失敗するたびに、FE ノードはその BE ノードを BE Blacklist に追加します。FE ノードは、一定期間内の接続失敗をカウントすることで、ブラックリストにある BE ノードの接続性を常に評価します。StarRocks は、接続失敗の数が事前に指定されたしきい値を下回った場合にのみ、ブラックリストにある BE ノードを削除します。

次の [FE 設定](./FE_configuration.md) を使用して、BE Blacklist の自動管理を設定できます:

- `black_host_history_sec`: BE Blacklist にある BE ノードの過去の接続失敗を保持する期間。
- `black_host_connect_failures_within_time`: ブラックリストにある BE ノードに許可される接続失敗のしきい値。

BE ノードが自動的に BE Blacklist に追加された場合、StarRocks はその接続性を評価し、BE Blacklist から削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストにある BE ノードが `black_host_connect_failures_within_time` に設定されたしきい値よりも少ない接続失敗を持つ場合にのみ、BE Blacklist から削除できます。