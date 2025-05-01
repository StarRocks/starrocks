---
displayed_sidebar: docs
unlisted: True
---

# Deploy StarRocks with Docker

このクイックスタートチュートリアルでは、Docker を使用してローカルマシンに StarRocks をデプロイする手順を案内します。始める前に、[StarRocks Architecture](../introduction/Architecture.md) を読んで、概念的な詳細を確認することができます。

これらの手順に従うことで、**1 つの FE ノード**と **1 つの BE ノード**を持つシンプルな StarRocks クラスターをデプロイできます。これにより、[テーブルの作成](../quick_start/Create_table.md) や [データのロードとクエリ](../quick_start/Import_and_query.md) に関する今後のクイックスタートチュートリアルを完了し、StarRocks の基本操作に慣れることができます。

> **注意**
>
> このチュートリアルで使用される Docker イメージを使用して StarRocks をデプロイするのは、小さなデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストや本番環境には推奨されません。高可用性の StarRocks クラスターをデプロイするには、[Deployment overview](../deployment/deployment_overview.md) を参照して、シナリオに適した他のオプションを確認してください。

## 前提条件

Docker で StarRocks をデプロイする前に、以下の要件を満たしていることを確認してください。

- **ハードウェア**

  StarRocks を 8 CPU コアと 16 GB 以上のメモリを持つマシンにデプロイすることを推奨します。

- **ソフトウェア**

  マシンに以下のソフトウェアがインストールされている必要があります。

  - [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以降)
  - MySQL クライアント (5.5 以降)

## ステップ 1: StarRocks Docker イメージをダウンロード

[StarRocks Docker Hub](https://hub.docker.com/r/starrocks/allin1-ubuntu/tags) から StarRocks Docker イメージをダウンロードします。イメージのタグに基づいて特定のバージョンを選択できます。

```Bash
sudo docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \
    -itd registry.starrocks.io/starrocks/allin1-ubuntu
```

> **トラブルシューティング**
>
> ホストマシン上の上記のポートのいずれかが占有されている場合、システムは "docker: Error response from daemon: driver failed programming external connectivity on endpoint tender_torvalds (): Bind for 0.0.0.0:xxxx failed: port is already allocated." と表示します。コマンド内のコロン (:) の前のポートを変更することで、ホストマシン上の利用可能なポートを割り当てることができます。

次のコマンドを実行して、コンテナが正しく作成され、実行されているかどうかを確認できます。

```Bash
sudo docker ps
```

以下に示すように、StarRocks コンテナの `STATUS` が `Up` であれば、Docker コンテナ内に StarRocks を正常にデプロイしたことになります。

```Plain
CONTAINER ID   IMAGE                                          COMMAND                  CREATED         STATUS                 PORTS                                                                                                                             NAMES
8962368f9208   starrocks/allin1-ubuntu:branch-3.0-0afb97bbf   "/bin/sh -c ./start_…"   4 minutes ago   Up 4 minutes           0.0.0.0:8037->8030/tcp, :::8037->8030/tcp, 0.0.0.0:8047->8040/tcp, :::8047->8040/tcp, 0.0.0.0:9037->9030/tcp, :::9037->9030/tcp   xxxxx
```

## ステップ 2: StarRocks に接続

StarRocks が正常にデプロイされた後、MySQL クライアントを介して接続できます。

```Bash
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

> **注意**
>
> `docker run` コマンドで `9030` に異なるポートを割り当てた場合は、上記のコマンド内の `9030` を割り当てたポートに置き換える必要があります。

次の SQL を実行して FE ノードのステータスを確認できます。

```SQL
SHOW PROC '/frontends'\G
```

例:

```Plain
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: 8962368f9208_9010_1681370634632
               IP: 8962368f9208
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 555505802
             Join: true
            Alive: true
ReplayedJournalId: 99
    LastHeartbeat: 2023-04-13 07:28:50
         IsHelper: true
           ErrMsg: 
        StartTime: 2023-04-13 07:24:11
          Version: BRANCH-3.0-0afb97bbf
1 row in set (0.02 sec)
```

- フィールド `Alive` が `true` の場合、この FE ノードは正常に起動され、クラスターに追加されています。
- フィールド `Role` が `FOLLOWER` の場合、この FE ノードは Leader FE ノードとして選出される資格があります。
- フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

次の SQL を実行して BE ノードのステータスを確認できます。

```SQL
SHOW PROC '/backends'\G
```

例:

```Plain
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: 8962368f9208
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-13 07:24:25
        LastHeartbeat: 2023-04-13 07:29:05
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 30
     DataUsedCapacity: 0.000 
        AvailCapacity: 527.437 GB
        TotalCapacity: 1.968 TB
              UsedPct: 73.83 %
       MaxDiskUsedPct: 73.83 %
               ErrMsg: 
              Version: BRANCH-3.0-0afb97bbf
               Status: {"lastSuccessReportTabletsTime":"2023-04-13 07:28:26"}
    DataTotalCapacity: 527.437 GB
          DataUsedPct: 0.00 %
             CpuCores: 16
    NumRunningQueries: 0
           MemUsedPct: 0.02 %
           CpuUsedPct: 0.1 %
1 row in set (0.00 sec)
```

フィールド `Alive` が `true` の場合、この BE ノードは正常に起動され、クラスターに追加されています。

## Docker コンテナの停止と削除

クイックスタートチュートリアル全体を完了した後、コンテナ ID を使用して StarRocks クラスターをホストするコンテナを停止し、削除できます。

> **注意**
>
> `sudo docker ps` を実行して Docker コンテナの `container_id` を取得できます。

次のコマンドを実行してコンテナを停止します。

```Bash
# <container_id> を StarRocks クラスターのコンテナ ID に置き換えてください。
sudo docker stop <container_id>
```

コンテナが不要な場合は、次のコマンドを実行して削除できます。

```Bash
# <container_id> を StarRocks クラスターのコンテナ ID に置き換えてください。
sudo docker rm <container_id>
```

> **注意**
>
> コンテナの削除は不可逆です。削除する前に、コンテナ内の重要なデータのバックアップを確保してください。

## 次のステップ

StarRocks をデプロイした後、[テーブルの作成](../quick_start/Create_table.md) や [データのロードとクエリ](../quick_start/Import_and_query.md) に関するクイックスタートチュートリアルを続けることができます。

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=f5ae0b2c-3578-4a40-9056-178e9837cfe0" />