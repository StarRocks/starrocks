---
displayed_sidebar: docs
---

# 高可用性を備えた FE クラスターのデプロイ

このトピックでは、StarRocks の FE ノードの高可用性 (HA) デプロイメントについて紹介します。

## FE HA クラスターの理解

StarRocks の FE 高可用性クラスターは、プライマリ・セカンダリレプリケーションアーキテクチャを使用して、単一障害点を回避します。FE は、リーダー選出、ログレプリケーション、およびフェイルオーバーを実現するために、Raft に似た BDB JE プロトコルを使用します。

### FE の役割の理解

FE クラスターでは、ノードは次の 2 つの役割に分けられます。

- **FE Follower**

FE Follower はレプリケーションプロトコルの投票メンバーであり、Leader FE の選出に参加し、ログを提出します。FE Follower の数は奇数 (2n+1) です。過半数 (n+1) の確認が必要であり、少数 (n) の障害を許容します。

- **FE Observer**

FE Observer は非投票メンバーであり、レプリケーションログを非同期で購読するために使用されます。クラスター内では、FE Observer の状態は Follower より遅れています。他のレプリケーションプロトコルにおけるリーナーの役割に似ています。

FE クラスターは、FE Follower から自動的に Leader FE を選出します。Leader FE はすべての状態変更を実行します。変更は非リーダー FE ノードから開始され、実行のために Leader FE に転送されます。非リーダー FE ノードは、レプリケーションログの最新の変更の LSN を記録します。読み取り操作は非リーダーノードで直接実行できますが、非リーダー FE ノードの状態が最後の操作の LSN と同期されるまで待機します。Observer ノードは、FE クラスターの読み取り操作のロード容量を増やすことができます。緊急性の低いユーザーは、Observer ノードを読み取ることができます。

## FE HA クラスターのデプロイ

高可用性を備えた FE クラスターをデプロイするには、次の要件を満たす必要があります。

- FE ノード間の時計の差は 5 秒を超えてはなりません。NTP プロトコルを使用して時間を調整してください。
- 1 台のマシンに 1 つの FE ノードのみをデプロイできます。
- すべての FE ノードに同じ HTTP ポートを割り当てる必要があります。

上記の要件がすべて満たされたら、次の手順に従って FE インスタンスを 1 つずつ StarRocks クラスターに追加し、FE ノードの HA デプロイメントを有効にします。

1. バイナリおよび構成ファイルを配布します (単一インスタンスと同じ)。
2. MySQL クライアントを既存の FE に接続し、新しいインスタンスの情報を追加します。役割、IP、ポートを含みます。

   ```sql
   mysql> ALTER SYSTEM ADD FOLLOWER "host:port";
   ```

   または

   ```sql
   mysql> ALTER SYSTEM ADD OBSERVER "host:port";
   ```

   ホストはマシンの IP です。マシンに複数の IP がある場合は、priority_networks の IP を選択します。たとえば、`priority_networks=192.168.1.0/24` を設定して、通信にサブネット `192.168.1.x` を使用できます。ポートは `edit_log_port` で、デフォルトは `9010` です。

   > 注意: セキュリティ上の考慮から、StarRocks の FE と BE は通信のために 1 つの IP のみをリッスンできます。マシンに複数のネットワークカードがある場合、StarRocks は正しい IP を自動的に見つけられないことがあります。たとえば、`ifconfig` コマンドを実行して `eth0 IP` が `192.168.1.1`、`docker0 : 172.17.0.1` であることを確認します。ネットワーク `192.168.1.0/24` を設定して、eth0 を通信 IP として指定できます。ここでは、[CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) 表記を使用して、IP が存在するサブネット範囲を指定し、すべての BE および FE で使用できるようにします。`priority_networks` は `fe.conf` と `be.conf` の両方に記述されます。この属性は、FE または BE が起動されたときに使用する IP を示します。例は次のとおりです: `priority_networks=10.1.3.0/24`。

   エラーが発生した場合は、次のコマンドを使用して FE を削除します。

   ```sql
   alter system drop follower "fe_host:edit_log_port";
   alter system drop observer "fe_host:edit_log_port";
   ```

3. FE ノードは、マスター選択、投票、ログ提出、およびレプリケーションを完了するためにペアで相互接続する必要があります。FE ノードが最初に起動されるとき、既存のクラスター内のノードをヘルパーとして指定する必要があります。ヘルパーノードは、クラスター内のすべての FE ノードの構成情報を取得して接続を確立します。したがって、起動時に `--helper` パラメータを指定します。

   ```shell
   ./bin/start_fe.sh --helper host:port --daemon
   ```

   ホストはヘルパーノードの IP です。複数の IP がある場合は、`priority_networks` の IP を選択します。ポートは `edit_log_port` で、デフォルトは `9010` です。

   将来の起動時には `--helper` パラメータを指定する必要はありません。FE は他の FE の構成情報をローカルディレクトリに保存します。直接起動するには:

   ```shell
   ./bin/start_fe.sh --daemon
   ```

4. クラスターの状態を確認し、デプロイが成功したことを確認します。

  ```Plain Text
  mysql> SHOW PROC '/frontends'\G

  ***1\. row***

  Name: 172.26.x.x_9010_1584965098874

  IP: 172.26.x.x

  HostName: sr-test1

  ......

  Role: FOLLOWER

  IsMaster: true

  ......

  Alive: true

  ......

  ***2\. row***

  Name: 172.26.x.x_9010_1584965098874

  IP: 172.26.x.x

  HostName: sr-test2

  ......

  Role: FOLLOWER

  IsMaster: false

  ......

  Alive: true

  ......

  ***3\. row***

  Name: 172.26.x.x_9010_1584965098874

  IP: 172.26.x.x

  HostName: sr-test3

  ......

  Role: FOLLOWER

  IsMaster: false

  ......

  Alive: true

  ......

  3 rows in set (0.05 sec)
  ```

`Alive` が `true` の場合、ノードはクラスターに正常に追加されています。上記の例では、`172.26.x.x_9010_1584965098874` が Leader FE ノードです。