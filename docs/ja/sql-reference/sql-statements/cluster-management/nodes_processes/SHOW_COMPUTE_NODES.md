---
displayed_sidebar: docs
---

# SHOW COMPUTE NODES

## 説明

クラスタ内のすべての CN ノードの情報を表示します。

:::tip

この操作を実行できるのは、SYSTEM レベルの OPERATE 権限を持つユーザーまたは `cluster_admin` ロールを持つユーザーのみです。

:::

## 構文

```SQL
SHOW COMPUTE NODES
```

## 戻り値

```SQL
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
| ComputeNodeId | IP           | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | ErrMsg | Version            | CpuCores | NumRunningQueries | MemUsedPct | CpuUsedPct | HasStoragePath | StarletPort | WorkerId | WarehouseName     | TabletNum |
+---------------+--------------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+--------+--------------------+----------+-------------------+------------+------------+----------------+-------------+----------+-------------------+-----------+
```

このステートメントによって返されるパラメータを次の表に示します。

| **パラメータ**         | **説明**                                                             |
| -------------------- | ----------------------------------------------------------------- |
| ComputeNodeId        | CN ノードの ID。                                                   |
| IP                   | CN ノードの IP アドレス。                                          |
| HeartbeatPort        | CN ノードのハートビートポート。FE ノードからのハートビートを受信するために使用されます。 |
| BePort               | CN ノードの Thrift サーバーポート。FE ノードからのリクエストを受信するために使用されます。 |
| HttpPort             | CN ノードの HTTP サーバーポート。ウェブページを介して CN ノードにアクセスするために使用されます。 |
| BrpcPort             | CN ノードの bRPC ポート。CN ノード間の通信に使用されます。          |
| LastStartTime        | CN ノードが最後に起動した時刻。                                     |
| LastHeartbeat        | CN ノードが最後にハートビートを送信した時刻。                       |
| Alive                | CN ノードが生存しているかどうか。<ul><li>`true`: CN ノードは生存しています。</li><li>`false`: CN ノードは生存していません。</li></ul> |
| SystemDecommissioned | パラメータの値が `true` の場合、CN ノードは StarRocks クラスタから削除されます。 |
| ClusterDecommissioned | このパラメータはシステムの互換性のために使用されます。              |
| ErrMsg               | CN ノードがハートビートの送信に失敗した場合のエラーメッセージ。      |
| Version              | CN ノードの StarRocks バージョン。                                  |
| CpuCores             | CN ノードの CPU コア数。                                            |
| NumRunningQueries    | CN ノードで実行中のクエリの数。                                     |
| MemUsedPct           | 使用されているメモリの割合。                                        |
| CpuUsedPct           | 使用されている CPU コアの割合。                                     |
| HasStoragePath       | CN ノードにストレージパスが設定されているかどうか。                  |
| StarletPort          | CN ノードの `starlet_port`。これは追加のエージェントサービスポートです。 |
| WorkerId             | 内部スケジューリング用の CN ノードの ID。                            |
| WarehouseName        | CN ノードが属するウェアハウスの名前。値は常に `default_warehouse` です。 |
| TabletNum            | CN ノード上のタブレット（キャッシュされたデータ）の数。               |

## 例

クラスタ内のすべての CN ノードの情報を表示します。

```Plain
MySQL > SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10001
                   IP: x.x.x.x
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2024-05-14 15:45:34
        LastHeartbeat: 2024-05-14 15:47:59
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
               ErrMsg: 
              Version: 3.3.0-rc01-3b8cb0c
             CpuCores: 4
    NumRunningQueries: 0
           MemUsedPct: 1.95 %
           CpuUsedPct: 0.0 %
       HasStoragePath: true
          StarletPort: 8167
             WorkerId: 1
        WarehouseName: default_warehouse
            TabletNum: 58
1 row in set (0.00 sec)
```