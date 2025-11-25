---
displayed_sidebar: docs
---

# BE と CN のブラックリストを管理する

このトピックでは、BE と CN のブラックリストの管理方法について説明します。

バージョン v3.3.0 以降、StarRocks は BE ブラックリスト機能をサポートしており、特定の BE ノードのクエリ実行を禁止することで、BE ノードへの接続失敗による頻繁なクエリ失敗やその他の予期しない動作を回避できます。

バージョン v4.0 以降、StarRocks は COMPUTE NODE (CN) をブラックリストに追加することをサポートしています。

デフォルトでは、StarRocks は自動的に BE と CN のブラックリストを管理し、接続が失われた BE または CN ノードをブラックリストに追加し、接続が再確立されたときにブラックリストから削除します。ただし、手動でブラックリストに追加されたノードは、StarRocks によってブラックリストから削除されません。

:::note

- この機能を使用できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。
- 各 FE ノードは独自の BE と CN のブラックリストを保持し、他の FE ノードと共有しません。

:::

## BE/CN をブラックリストに追加する

[ADD BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/ADD_BACKEND_BLACKLIST.md) を使用して、手動で BE/CN ノードをブラックリストに追加できます。このステートメントでは、ブラックリストに追加する BE/CN ノードの ID を指定する必要があります。BE ID は [SHOW BACKENDS](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) を実行して取得し、CN ID は [SHOW COMPUTE NODES](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_COMPUTE_NODES.md) を実行して取得できます。

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

-- CN ID を取得します。
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- CN をブラックリストに追加します。
ADD COMPUTE NODE BLACKLIST 10005;
```

## ブラックリストから BE/CN を削除する

[DELETE BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/DELETE_BACKEND_BLACKLIST.md) を使用して、手動で BE/CN ノードをブラックリストから削除できます。このステートメントでも、BE/CN ノードの ID を指定する必要があります。

例:

```SQL
-- BE をブラックリストから削除します。
DELETE BACKEND BLACKLIST 10001;

-- CN をブラックリストから削除します。
DELETE COMPUTE NODE BLACKLIST 10005;
```

## BE/CN ブラックリストを表示する

[SHOW BACKEND/COMPUTE NODE BLACKLIST](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKEND_BLACKLIST.md) を使用して、ブラックリストにある BE/CN ノードを表示できます。

例:

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

次のフィールドが返されます:

- `AddBlackListType`: BE/CN ノードがブラックリストに追加された方法。`MANUAL` はユーザーによって手動でブラックリストに追加されたことを示します。`AUTO` は StarRocks によって自動的にブラックリストに追加されたことを示します。
- `LostConnectionTime`:
  - `MANUAL` タイプの場合、BE/CN ノードが手動でブラックリストに追加された時間を示します。
  - `AUTO` タイプの場合、最後に成功した接続が確立された時間を示します。
- `LostConnectionNumberInPeriod`: `CheckTimePeriod(s)` 内で検出された切断の数で、これは StarRocks がブラックリスト内の BE/CN ノードの接続状態を確認する間隔です。
- `CheckTimePeriod(s)`: StarRocks がブラックリストにある BE/CN ノードの接続状態を確認する間隔。この値は FE 設定項目 `black_host_history_sec` に指定した値に評価されます。単位: 秒。

## BE/CN ブラックリストの自動管理を設定する

BE/CN ノードが FE ノードへの接続を失うたびに、または BE/CN ノードでタイムアウトによるクエリが失敗するたびに、FE ノードは BE/CN ノードをその BE と CN ブラックリストに追加します。FE ノードは、一定期間内の接続失敗をカウントすることで、ブラックリスト内の BE/CN ノードの接続性を常に評価します。StarRocks は、接続失敗の数が事前に指定された閾値を下回った場合にのみ、ブラックリストにある BE/CN ノードを削除します。

次の [FE 設定](./FE_configuration.md) を使用して、BE と CN ブラックリストの自動管理を設定できます:

- `black_host_history_sec`: ブラックリスト内の BE/CN ノードの接続失敗の履歴を保持する期間。
- `black_host_connect_failures_within_time`: ブラックリストにある BE/CN ノードに許可される接続失敗の閾値。

BE/CN ノードが自動的にブラックリストに追加された場合、StarRocks はその接続性を評価し、ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストにある BE/CN ノードが `black_host_connect_failures_within_time` に設定された閾値よりも少ない接続失敗を持つ場合にのみ、ブラックリストから削除されます。
