---
displayed_sidebar: docs
---

# BE ブラックリストの管理

このトピックでは、BE ブラックリストの管理方法について説明します。

バージョン v3.3.0 以降、StarRocks は BE ブラックリスト機能をサポートしています。この機能により、特定の BE ノードのクエリ実行での使用を禁止することができ、BE ノードへの接続が失敗することによる頻繁なクエリの失敗やその他の予期しない動作を回避できます。

デフォルトでは、StarRocks は BE ブラックリストを自動的に管理し、接続が失われた BE ノードをブラックリストに追加し、接続が再確立されたときにブラックリストから削除します。ただし、手動でブラックリストに追加された BE ノードは、StarRocks によってブラックリストから削除されません。

:::note

- SYSTEM レベルの BLACKLIST 権限を持つユーザーのみがこの機能を使用できます。
- 各 FE ノードは独自の BE ブラックリストを保持し、他の FE ノードと共有しません。

:::

## BE をブラックリストに追加する

[ADD BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md) を使用して、BE ノードを手動で BE ブラックリストに追加できます。このステートメントでは、ブラックリストに追加する BE ノードの ID を指定する必要があります。[SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) を実行して BE ID を取得できます。

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

[DELETE BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md) を使用して、BE ノードを手動で BE ブラックリストから削除できます。このステートメントでも、BE ノードの ID を指定する必要があります。

例:

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## BE ブラックリストを表示する

[SHOW BACKEND BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md) を使用して、BE ブラックリストにある BE ノードを表示できます。

例:

```SQL
SHOW BACKEND BLACKLIST;
+-----------+------------------+---------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime  | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+---------------------+------------------------------+--------------------+
| 10001     | MANUAL           | 2024-04-28 11:52:09 | 0                            | 5                  |
+-----------+------------------+---------------------+------------------------------+--------------------+
```

次のフィールドが返されます:

- `AddBlackListType`: BE ノードがブラックリストに追加された方法を示します。`MANUAL` はユーザーによって手動でブラックリストに追加されたことを示し、`AUTO` は StarRocks によって自動的にブラックリストに追加されたことを示します。
- `LostConnectionTime`:
  - `MANUAL` タイプの場合、BE ノードが手動でブラックリストに追加された時間を示します。
  - `AUTO` タイプの場合、最後に成功した接続が確立された時間を示します。
- `LostConnectionNumberInPeriod`: `CheckTimePeriod(s)` 内で検出された切断の数であり、StarRocks がブラックリストにある BE ノードの接続状態を確認する間隔です。
- `CheckTimePeriod(s)`: ブラックリストにある BE ノードの接続状態を StarRocks が確認する間隔です。この値は FE 設定項目 `black_host_history_sec` に指定された値に評価されます。単位: 秒。

## BE ブラックリストの自動管理を設定する

BE ノードが FE ノードへの接続を失うたびに、または BE ノードでタイムアウトが原因でクエリが失敗するたびに、FE ノードは BE ノードを BE ブラックリストに追加します。FE ノードは、ブラックリストにある BE ノードの接続失敗を一定期間内にカウントすることで、その接続性を常に評価します。StarRocks は、接続失敗の数が事前に指定された閾値を下回った場合にのみ、ブラックリストにある BE ノードを削除します。

次の [FE 設定](./FE_configuration.md) を使用して、BE ブラックリストの自動管理を設定できます:

- `black_host_history_sec`: BE ブラックリストにある BE ノードの過去の接続失敗を保持する期間。
- `black_host_connect_failures_within_time`: ブラックリストにある BE ノードに許容される接続失敗の閾値。

BE ノードが自動的に BE ブラックリストに追加された場合、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストにある BE ノードが `black_host_connect_failures_within_time` に設定された閾値よりも少ない接続失敗を持つ場合にのみ、BE ブラックリストから削除できます。