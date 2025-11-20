---
displayed_sidebar: docs
---

# スケールインとスケールアウト

このトピックでは、StarRocks のノードをスケールインおよびスケールアウトする方法について説明します。

## FE のスケールインとスケールアウト

StarRocks には 2 種類の FE ノードがあります: Follower と Observer です。Follower は選挙投票と書き込みに関与し、Observer はログの同期と読み取り性能の向上にのみ使用されます。

> * Follower FE (リーダーを含む) の数は奇数でなければならず、高可用性 (HA) モードを形成するために 3 つのデプロイを推奨します。
> * FE が高可用性デプロイメント (1 リーダー、2 フォロワー) にある場合、読み取り性能を向上させるために Observer FE を追加することをお勧めします。

### FE のスケールアウト

FE ノードをデプロイしてサービスを開始した後、次のコマンドを実行して FE をスケールアウトします。

~~~sql
alter system add follower "fe_host:edit_log_port";
alter system add observer "fe_host:edit_log_port";
~~~

### FE のスケールイン

FE のスケールインはスケールアウトと似ています。次のコマンドを実行して FE をスケールインします。

~~~sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
~~~

拡張と縮小の後、`show proc '/frontends';` を実行してノード情報を表示できます。

## BE のスケールインとスケールアウト

StarRocks は、BE のスケールインまたはスケールアウト後に、全体のパフォーマンスに影響を与えることなく、自動的に負荷分散を実行します。

新しい BE ノードを追加すると、システムのタブレット スケジューラが新しいノードとその低負荷を検出します。そして、高負荷の BE ノードから新しい低負荷の BE ノードへタブレットを移動し始め、クラスター全体でデータと負荷が均等に分散されるようにします。

負荷分散プロセスは、ディスク使用率とレプリカ数の両方を考慮した、各 BE ごとに計算された loadScore に基づいています。システムは、loadScore の高いノードから loadScore の低いノードへタブレットを移動することを目指します。

FE 構成パラメータ `tablet_sched_disable_balance` を確認することで、自動負荷分散が無効になっていないことを確認できます（このパラメータはデフォルトで false に設定されており、タブレット負荷分散がデフォルトで有効になっていることを意味します）。詳細は、[レプリカ管理ドキュメント](./resource_management/Replica.md) をご覧ください。

### BE のスケールアウト

次のコマンドを実行して BE をスケールアウトします。

~~~sql
alter system add backend 'be_host:be_heartbeat_service_port';
~~~

次のコマンドを実行して BE のステータスを確認します。

~~~sql
show proc '/backends';
~~~

### BE のスケールイン

BE ノードをスケールインする方法は 2 つあります - `DROP` と `DECOMMISSION` です。

`DROP` は BE ノードを即座に削除し、失われた複製は FE のスケジューリングによって補われます。`DECOMMISSION` は最初に複製を補ってから BE ノードを削除します。`DECOMMISSION` の方が少し親切で、BE のスケールインには推奨されます。

両方の方法のコマンドは似ています:

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system drop backend "be_host:be_heartbeat_service_port";`

バックエンドの削除は危険な操作なので、実行する前に 2 回確認する必要があります

* `alter system drop backend "be_host:be_heartbeat_service_port";`

## CN のスケールインとスケールアウト

### CN のスケールアウト

次のコマンドを実行して CN をスケールアウトします。

~~~sql
ALTER SYSTEM ADD COMPUTE NODE "cn_host:cn_heartbeat_service_port";
~~~

次のコマンドを実行して CN のステータスを確認します。

~~~sql
SHOW PROC '/compute_nodes';
~~~

### CN のスケールイン

CN のスケールインはスケールアウトと似ています。次のコマンドを実行して CN をスケールインします。

~~~sql
ALTER SYSTEM DROP COMPUTE NODE "cn_host:cn_heartbeat_service_port";
~~~

`SHOW PROC '/compute_nodes';` を実行してノード情報を表示できます。
