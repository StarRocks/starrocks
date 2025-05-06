---
displayed_sidebar: docs
---

# StarRocks のダウングレード

このトピックでは、StarRocks クラスターをダウングレードする方法について説明します。

StarRocks クラスターをアップグレードした後に例外が発生した場合、以前のバージョンにダウングレードしてクラスターを迅速に復旧することができます。

## 概要

ダウングレードする前に、このセクションの情報を確認してください。推奨されるアクションを実行してください。

### ダウングレードパス

- **パッチバージョンのダウングレードの場合**

  StarRocks クラスターをパッチバージョン間でダウングレードできます。例えば、v2.2.11 から直接 v2.2.6 へのダウングレードが可能です。

- **マイナーバージョンのダウングレードの場合**

  互換性と安全性の理由から、StarRocks クラスターを**マイナーバージョンごとに連続して**ダウングレードすることを強くお勧めします。例えば、StarRocks v2.5 クラスターを v2.2 にダウングレードするには、次の順序でダウングレードする必要があります: v2.5.x --> v2.4.x --> v2.3.x --> v2.2.x。

- **メジャーバージョンのダウングレードの場合**

  StarRocks v3.0 クラスターを v2.5.3 以降のバージョンにのみダウングレードできます。

  - StarRocks は v3.0 で BDB ライブラリをアップグレードします。ただし、BDBJE はロールバックできません。ダウングレード後は v3.0 の BDB ライブラリを使用する必要があります。
  - 新しい RBAC 権限システムは、v3.0 にアップグレードした後にデフォルトで使用されます。ダウングレード後も RBAC 権限システムのみ使用できます。

### ダウングレード手順

StarRocks のダウングレード手順は、[アップグレード手順](../deployment/upgrade.md#upgrade-procedure)の逆順です。したがって、**まず FEs をダウングレードし、その後 BEs と CNs をダウングレード**する必要があります。順序を誤ると、FEs と BEs/CNs 間の互換性が失われ、サービスがクラッシュする可能性があります。FE ノードの場合、まずすべての Follower FE ノードをダウングレードしてから Leader FE ノードをダウングレードする必要があります。

## 始める前に

準備中に、マイナーまたはメジャーバージョンのダウングレードを行う場合は、互換性の設定を行う必要があります。また、クラスター内のすべてのノードをダウングレードする前に、FEs または BEs のいずれかでダウングレードの可用性テストを実行する必要があります。

### 互換性設定を実行する

StarRocks クラスターを以前のマイナーまたはメジャーバージョンにダウングレードする場合、互換性設定を実行する必要があります。一般的な互換性設定に加えて、詳細な設定はダウングレード元の StarRocks クラスターのバージョンによって異なります。

- **一般的な互換性設定**

StarRocks クラスターをダウングレードする前に、tablet クローンを無効にする必要があります。バランサーを無効にしている場合は、このステップをスキップできます。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

ダウングレード後、すべての BE ノードのステータスが `Alive` になった場合、tablet クローンを再度有効にできます。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **v2.2 以降のバージョンからダウングレードする場合**

FE 設定項目 `ignore_unknown_log_id` を `true` に設定します。これは静的パラメータであるため、FE 設定ファイル **fe.conf** で変更し、ノードを再起動して変更を有効にする必要があります。ダウングレードと最初のチェックポイントが完了した後、これを `false` にリセットし、ノードを再起動できます。

- **FQDN アクセスを有効にしている場合**

FQDN アクセスを有効にしている（v2.4 以降でサポート）場合、v2.4 より前のバージョンにダウングレードする必要がある場合は、ダウングレード前に IP アドレスアクセスに切り替える必要があります。詳細な手順については、[Rollback FQDN](../administration/management/enable_fqdn.md#rollback) を参照してください。

## FE のダウングレード

互換性設定と可用性テストの後、FE ノードをダウングレードできます。まず Follower FE ノードをダウングレードし、その後 Leader FE ノードをダウングレードする必要があります。

1. メタデータスナップショットを作成します。

   a. [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) を実行してメタデータスナップショットを作成します。

   b. Leader FE のログファイル **fe.log** を確認することで、イメージファイルが同期されたかどうかを確認できます。"push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" というログの記録があれば、イメージファイルが正常に同期されたことを示しています。

   > **注意**
   >
   > ALTER SYSTEM CREATE IMAGE ステートメントは v2.5.3 以降でサポートされています。以前のバージョンでは、Leader FE を再起動してメタデータスナップショットを作成する必要があります。

2. FE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <fe_dir> を FE ノードのデプロイメントディレクトリに置き換えてください。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

3. **bin**、**lib**、および **spark-dpp** の下の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

   > **注意**
   >
   > StarRocks v3.0 を v2.5 にダウングレードする場合、デプロイメントファイルを置き換えた後に次の手順を実行する必要があります:
   >
   > 1. v3.0 デプロイメントのファイル **fe/lib/starrocks-bdb-je-18.3.13.jar** を v2.5 デプロイメントのディレクトリ **fe/lib** にコピーします。
   > 2. ファイル **fe/lib/je-7.\*.jar** を削除します。

4. FE ノードを起動します。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

5. FE ノードが正常に起動したかどうかを確認します。

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. 上記の手順 2 から手順 5 を繰り返して、他の Follower FE ノードをダウングレードし、最後に Leader FE ノードをダウングレードします。

   > **注意**
   >
   > 失敗したアップグレード後にクラスターをダウングレードし、再度クラスターをアップグレードしたい場合、例えば 2.5->3.0->2.5->3.0 のように、Follower FEs のメタデータアップグレードの失敗を防ぐために、手順 1 を繰り返して新しいスナップショットをトリガーしてください。

## BE のダウングレード

FE ノードをダウングレードした後、クラスター内の BE ノードをダウングレードできます。

1. BE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <be_dir> を BE ノードのデプロイメントディレクトリに置き換えてください。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. **bin** および **lib** の下の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. BE ノードを起動します。

   ```Bash
   sh bin/start_be.sh --daemon
   ```

4. BE ノードが正常に起動したかどうかを確認します。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 上記の手順を繰り返して、他の BE ノードをダウングレードします。

## CN のダウングレード

1. CN ノードの作業ディレクトリに移動し、ノードを優雅に停止します。

   ```Bash
   # <cn_dir> を CN ノードのデプロイメントディレクトリに置き換えてください。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. **bin** および **lib** の下の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. CN ノードを起動します。

   ```Bash
   sh bin/start_cn.sh --daemon
   ```

4. CN ノードが正常に起動したかどうかを確認します。

   ```Bash
   ps aux | grep  starrocks_be
   ```

5. 上記の手順を繰り返して、他の CN ノードをダウングレードします。