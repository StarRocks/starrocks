---
displayed_sidebar: docs
---

# Scale in and out

このトピックでは、StarRocks のノードをスケールインおよびスケールアウトする方法について説明します。

## Scale FE in and out

StarRocks には 2 種類の FE ノードがあります: Follower と Observer です。Follower は選挙投票と書き込みに関与します。Observer はログの同期と読み取り性能の拡張にのみ使用されます。

> * Follower FE（リーダーを含む）の数は奇数でなければならず、3 つをデプロイして高可用性 (HA) モードを形成することをお勧めします。
> * FE が高可用性デプロイメント（1 リーダー、2 フォロワー）にある場合、読み取り性能を向上させるために Observer FE を追加することをお勧めします。* 通常、1 つの FE ノードは 10-20 の BE ノードと連携できます。FE ノードの総数は 10 未満であることが推奨されます。ほとんどの場合、3 つで十分です。

### Scale FE out

FE ノードをデプロイしてサービスを開始した後、次のコマンドを実行して FE をスケールアウトします。

~~~sql
alter system add follower "fe_host:edit_log_port";
alter system add observer "fe_host:edit_log_port";
~~~

### Scale FE in

FE のスケールインはスケールアウトと似ています。次のコマンドを実行して FE をスケールインします。

~~~sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
~~~

拡張と縮小の後、`show proc '/frontends';` を実行してノード情報を表示できます。

## Scale BE in and out

BE がスケールインまたはスケールアウトされた後、StarRocks は自動的にロードバランシングを行い、全体的な性能に影響を与えません。

### Scale BE out

次のコマンドを実行して BE をスケールアウトします。

~~~sql
alter system add backend 'be_host:be_heartbeat_service_port';
~~~

次のコマンドを実行して BE のステータスを確認します。

~~~sql
show proc '/backends';
~~~

### Scale BE in

BE ノードをスケールインする方法は 2 つあります – `DROP` と `DECOMMISSION` です。

`DROP` は BE ノードを即座に削除し、失われた複製は FE のスケジューリングによって補われます。`DECOMMISSION` は最初に複製が補われることを確認し、その後 BE ノードを削除します。`DECOMMISSION` の方が少しフレンドリーで、BE のスケールインには推奨されます。

両方の方法のコマンドは似ています:

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system drop backend "be_host:be_heartbeat_service_port";`

バックエンドの削除は危険な操作であるため、実行する前に 2 回確認する必要があります

* `alter system drop backend "be_host:be_heartbeat_service_port";`