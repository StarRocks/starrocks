---
displayed_sidebar: docs
---

# クイックスタート: Docker で StarRocks をデプロイ

このクイックスタートでは、以下のガイドを提供します。

- [ Docker](https://docs.docker.com/engine/install/) を使用して、1 つの FE と 1 つの BE で StarRocks をデプロイします。
- MySQL クライアントを使用して StarRocks に接続します。
- テーブルを作成し、データを挿入し、そのデータをクエリします。

## 前提条件

1. Docker
2. MySQL クライアント

## ステップ 1: デプロイ

StarRocks のバージョンを選択するには、[ StarRocks Dockerhub リポジトリ](https://hub.docker.com/r/starrocks/allin1-ubuntu/tags) にアクセスし、バージョンタグに基づいてバージョンを選択します。

例えば、StarRocks v2.5.4 をデプロイするには、以下のコマンドを実行します。

```sh
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd starrocks/allin1-ubuntu:2.5.4
```

その後、以下のコマンドを使用してコンテナの状態を確認できます。

```sh
docker ps
```

## ステップ 2: StarRocks に接続

StarRocks が準備完了するまでに少し時間がかかります。接続する前に少なくとも 30 秒待つことをお勧めします。

```sh
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

## ステップ 3: StarRocks を使用

以下のコマンドを使用して、FE と BE の状態を確認します。FE と BE の両方で `Alive` が `true` と表示されていれば、StarRocks は正常で準備完了です。

FE

```SQL
SHOW PROC '/frontends'\G
```

```plaintext
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: be552e5f0de9_9010_1680659932444
               IP: be552e5f0de9
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 1556630880
             Join: true
            Alive: true
ReplayedJournalId: 944
    LastHeartbeat: 2023-04-05 02:49:36
         IsHelper: true
           ErrMsg: 
        StartTime: 2023-04-05 01:59:00
          Version: 2.5.4-1021a9299
1 row in set (0.05 sec)

```

BE

```SQL
SHOW PROC '/backends'\G
```

```plaintext
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: be552e5f0de9
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-05 01:59:13
        LastHeartbeat: 2023-04-05 02:50:06
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 32
     DataUsedCapacity: 4.474 KB
        AvailCapacity: 197.683 GB
        TotalCapacity: 235.983 GB
              UsedPct: 16.23 %
       MaxDiskUsedPct: 16.23 %
               ErrMsg: 
              Version: 2.5.4-1021a9299
               Status: {"lastSuccessReportTabletsTime":"2023-04-05 02:49:14"}
    DataTotalCapacity: 197.683 GB
          DataUsedPct: 0.00 %
             CpuCores: 7
    NumRunningQueries: 0
           MemUsedPct: 0.24 %
           CpuUsedPct: 0.5 %
1 row in set (0.03 sec)
```

これでテーブルを作成し、データを挿入できます。

**_注意:_** このクイックスタートでは 1 つの BE をデプロイします。CREATE TABLE 句に `properties ("replication_num" = "1")` を追加する必要があります。これにより、データのレプリカが BE に 1 つだけ保存されます。

```SQL
CREATE DATABASE test;

USE test;

CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");

INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
```

データをクエリします。

```plaintext
StarRocks > SELECT * FROM tbl;
+------+------+
| c1   | c2   |
+------+------+
|    3 |    3 |
|    1 |    1 |
|    2 |    2 |
+------+------+
3 rows in set (0.03 sec)
```