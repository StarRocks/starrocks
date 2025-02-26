---
displayed_sidebar: docs
---

# スケールインとスケールアウト

このトピックでは、StarRocks のノードをスケールインおよびスケールアウトする方法について説明します。

## FE のスケールインとスケールアウト

StarRocks には 2 種類の FE ノードがあります: Follower と Observer です。Follower は選挙投票と書き込みに関与し、Observer はログの同期と読み取り性能の向上にのみ使用されます。

> * Follower FE (リーダーを含む) の数は奇数でなければならず、高可用性 (HA) モードを形成するために 3 つのデプロイを推奨します。
> * FE が高可用性デプロイメント (1 リーダー、2 フォロワー) にある場合、読み取り性能を向上させるために Observer FE を追加することをお勧めします。* 通常、1 つの FE ノードは 10-20 の BE ノードと連携できます。FE ノードの総数は 10 未満であることが推奨されます。ほとんどの場合、3 つで十分です。

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

BE がスケールインまたはスケールアウトされた後、StarRocks は自動的にロードバランシングを行い、全体の性能に影響を与えません。

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