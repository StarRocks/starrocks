---
displayed_sidebar: docs
---

# SHOW FRONTENDS

SHOW FRONTENDS は、IP アドレス、ポート、ロール、ステータスなど、クラスター内のすべての FE ノードの情報を表示します。

:::tip

この操作を実行できるのは、SYSTEM レベルの OPERATE 権限を持つユーザーまたは `cluster_admin` ロールを持つユーザーのみです。

:::

## 構文

```sql
SHOW FRONTENDS
```

## 戻り値

```sql
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
| Id   | Name                         | IP        | EditLogPort | HttpPort | QueryPort | RpcPort | Role   | ClusterId | Join | Alive | ReplayedJournalId | LastHeartbeat       | IsHelper | ErrMsg | StartTime           | Version       |
+------+------------------------------+-----------+-------------+----------+-----------+---------+--------+-----------+------+-------+-------------------+---------------------+----------+--------+---------------------+---------------+
```

以下の表は、このステートメントによって返されるパラメーターについて説明しています。

| **パラメーター**       | **説明**                                                          |
| -------------------- | ----------------------------------------------------------------- |
| Id                   | FE ノードの一意の ID。                                                |
| Name                 | BDBJE 内の FE ノードの名前（通常は IP_EditLogPort_Timestamp）。         |
| IP                   | FE ノードの IP アドレス。                                            |
| EditLogPort          | BDBJE 通信に使用されるポート（デフォルト: 9010）。                       |
| HttpPort             | FE ノードの HTTP サーバーポート（デフォルト: 8030）。FE Web UI へのアクセスに使用されます。 |
| QueryPort            | MySQL プロトコルポート（デフォルト: 9030）。MySQL クライアントが StarRocks に接続するために使用します。 |
| RpcPort              | Thrift サーバーポート（デフォルト: 9020）。内部 RPC 通信に使用されます。    |
| Role                 | FE ノードのロール。 <ul><li>LEADER: マスターノード。メタデータの書き込みを処理します。</li><li>FOLLOWER: 選挙およびメタデータの同期に参加します。</li><li>OBSERVER: メタデータを同期しますが、選挙には参加しません。</li></ul> |
| ClusterId            | クラスターの一意の ID。同じクラスター内のすべてのノードは同じ ClusterId を持つ必要があります。 |
| Join                 | ノードが BDBJE グループに参加しているかどうか。 <ul><li>true: 参加済み。</li><li>false: 未参加（注: ノードが削除されていても一時的に true と表示される場合があります）。</li></ul> |
| Alive                | ノードが生存しており応答しているかどうか。 <ul><li>true: 生存。</li><li>false: 非生存。</li></ul> |
| ReplayedJournalId    | ノードが現在リプレイしたメタデータジャーナルの最大 ID。ノードがリーダーと同期しているかを確認するために使用されます。 |
| LastHeartbeat        | 最後にハートビートを受信したタイムスタンプ。                              |
| IsHelper             | Helper ノードかどうか（クラスター起動時に他のノードの参加を助けるために指定されます）。 |
| ErrMsg               | ハートビートが失敗した場合に表示されるエラーメッセージ。                    |
| StartTime            | この FE プロセスが開始された時間。                                     |
| Version              | この FE ノードで実行されている StarRocks のバージョン                   |

## 例

クラスター内のすべての FE ノードの情報を表示します。

```Plain
mysql> SHOW FRONTENDS\G;
*************************** 1. row ***************************
               Id: 1
             Name: 127.0.0.1_9010_1766983864084
               IP: 127.0.0.1
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 391198626
             Join: true
            Alive: true
ReplayedJournalId: 59720
    LastHeartbeat: 2026-01-21 13:24:20
         IsHelper: true
           ErrMsg:
        StartTime: 2026-01-20 12:58:48
          Version: 4.0.2-1f1aa9c
1 row in set (0.01 sec)
```
