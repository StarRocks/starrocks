---
displayed_sidebar: docs
description: "SHOW PROCESSLIST は、サーバー内で実行中のスレッドが現在実行している操作を一覧表示します。"
---

# SHOW PROCESSLIST

SHOW PROCESSLIST は、サーバー内で実行中のスレッドが現在実行している操作を一覧表示します。現在のバージョンの StarRocks はクエリの一覧表示のみをサポートしています。

:::tip

この操作には権限は必要ありません。

:::

## 構文

```SQL
SHOW [FULL] PROCESSLIST
```

## パラメータ

| パラメータ | 必須 | 説明 |
| --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| FULL      | いいえ   | このパラメータを指定すると、完全な SQL ステートメントが表示されます。指定しない場合、ステートメントの最初の 100 文字のみが表示されます。 |

## 戻り値

| 戻り値              | 説明                                                  |
| ------------------- | ------------------------------------------------------------ |
| Server              | サーバー ID。                                                   |
| Id                  | 接続 ID。                                               |
| User                | 操作を実行するユーザーの名前。                 |
| Host                | 操作を実行するクライアントのホスト名。         |
| Db                  | 操作が実行されるデータベースの名前。    |
| Command             | コマンドの種類。                                     |
| ConnectionStartTime | 接続が開始された時刻。                             |
| Time                | 操作が現在の状態に入ってからの時間（秒単位）。 |
| State               | 操作の状態。                                  |
| Info                | 操作が実行しているコマンド。                 |
| IsPending           | クエリがキューで保留中かどうか。有効な値: `true` および `false`。 |
| Warehouse           | クエリが実行されるウェアハウスの名前。       |
| CNGroup             | クエリが実行されるコンピュートノードグループの名前。   |
| Catalog             | カタログの名前。                                     |
| QueryId             | Command が "Query" の場合のクエリ ID。                       |

## 使用上の注意

現在のユーザーが `root` の場合、このステートメントはクラスター内のすべてのユーザーの操作を一覧表示します。それ以外の場合は、現在のユーザーの操作のみが一覧表示されます。

`IsPending`、`Warehouse`、および `CNGroup` フィールドは、ウェアハウス環境でのクエリ実行に関する追加情報を提供します。

- `IsPending`: クエリがキューで待機中（`true`）か、アクティブに実行中（`false`）かを示します
- `Warehouse`: クエリが実行されているウェアハウス名を表示します
- `CNGroup`: クエリの実行を担当するコンピュートノードグループ名を示します

## 例

例 1: ユーザー `root` を使用して操作の状態を一覧表示します。

```SQL
SHOW PROCESSLIST\G

*************************** 6. row ***************************
         ServerName: starrocks-fe_9010_1782850099498
                 Id: 33560554
               User: root
               Host: 172.18.0.4:54818
                 Db:
            Command: Query
ConnectionStartTime: 2026-07-02 01:21:14
               Time: 0
              State: OK
               Info: show processlist
          IsPending: false
          Warehouse: default_warehouse
            CNGroup:
            Catalog: default_catalog
            QueryId: 019f1eb3-246c-7899-ab3e-40a018645bba
```
