---
displayed_sidebar: docs
---

# Downgrade StarRocks

このトピックでは、StarRocks クラスターをダウングレードする方法について説明します。

StarRocks クラスターをアップグレードした後に例外が発生した場合、以前のバージョンにダウングレードしてクラスターを迅速に復旧できます。

## 概要

ダウングレードする前に、このセクションの情報を確認してください。推奨されるアクションを実行してください。

### ダウングレードパス

- **パッチバージョンのダウングレードの場合**

  StarRocks クラスターをパッチバージョン間でダウングレードできます。例えば、v2.2.11 から直接 v2.2.6 へ。

- **マイナーバージョンのダウングレードの場合**

  互換性と安全性の理由から、StarRocks クラスターを**一つのマイナーバージョンから次のバージョンへ順次**ダウングレードすることを強くお勧めします。例えば、StarRocks v2.5 クラスターを v2.2 にダウングレードするには、次の順序でダウングレードする必要があります: v2.5.x --> v2.4.x --> v2.3.x --> v2.2.x。

  :::warning

  StarRocks を v3.3 にアップグレードした後、v3.2.0、v3.2.1、または v3.2.2 に直接ダウングレードしないでください。そうしないとメタデータが失われます。問題を防ぐために、クラスターを v3.2.3 以降にダウングレードする必要があります。

  :::

- **メジャーバージョンのダウングレードの場合**

  StarRocks v3.0 クラスターを v2.5.3 以降のバージョンにのみダウングレードできます。

  - StarRocks は v3.0 で BDB ライブラリをアップグレードします。ただし、BDBJE はロールバックできません。ダウングレード後も v3.0 の BDB ライブラリを使用する必要があります。
  - 新しい RBAC 特権システムは、v3.0 にアップグレードした後にデフォルトで使用されます。ダウングレード後も RBAC 特権システムのみ使用できます。

### ダウングレード手順

StarRocks のダウングレード手順は、[アップグレード手順](../deployment/upgrade.md#upgrade-procedure)の逆順です。したがって、**最初に FEs をダウングレードし、その後 BEs と CNs をダウングレード**する必要があります。順序を間違えると、FEs と BEs/CNs 間の互換性が失われ、サービスがクラッシュする可能性があります。FE ノードの場合、Leader FE ノードをダウングレードする前に、まずすべての Follower FE ノードをダウングレードする必要があります。

## 始める前に

準備中に、マイナーまたはメジャーバージョンのダウングレードを行う場合は、互換性のある設定を行う必要があります。また、クラスター内のすべてのノードをダウングレードする前に、FEs または BEs のいずれかでダウングレードの可用性テストを実行する必要があります。

### 互換性のある設定を行う

StarRocks クラスターを以前のマイナーまたはメジャーバージョンにダウングレードする場合は、互換性のある設定を行う必要があります。一般的な互換性のある設定に加えて、ダウングレードする StarRocks クラスターのバージョンに応じて詳細な設定が異なります。

- **一般的な互換性のある設定**

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

FE 設定項目 `ignore_unknown_log_id` を `true` に設定します。これは静的パラメータであるため、FE 設定ファイル **fe.conf** で変更し、ノードを再起動して変更を有効にする必要があります。ダウングレードと最初のチェックポイントが完了した後、`false` にリセットしてノードを再起動できます。

- **FQDN アクセスを有効にしている場合**

FQDN アクセスを有効にしている場合（v2.4 以降でサポート）で、v2.4 より前のバージョンにダウングレードする必要がある場合は、ダウングレード前に IP アドレスアクセスに切り替える必要があります。詳細な手順については、[Rollback FQDN](../administration/management/enable_fqdn.md#rollback) を参照してください。

## FE のダウングレード

:::note

v3.3.0 以降から v3.2 にクラスターをダウングレードするには、ダウングレード前に次の手順を実行してください。

1. v3.3 クラスターで開始されたすべての ALTER TABLE SCHEMA CHANGE トランザクションが完了またはキャンセルされていることを確認します。
2. 次のコマンドを実行して、すべてのトランザクション履歴をクリアします。

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. 次のコマンドを実行して、残りの履歴レコードがないことを確認します。

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```
:::

互換性のある設定と可用性テストの後、FE ノードをダウングレードできます。まず Follower FE ノードをダウングレードし、その後 Leader FE ノードをダウングレードする必要があります。

1. メタデータスナップショットを作成します。

   a. [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) を実行してメタデータスナップショットを作成します。

   b. Leader FE のログファイル **fe.log** を確認して、イメージファイルが同期されているかどうかを確認できます。「push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes」というログの記録があれば、イメージファイルが正常に同期されたことを示しています。

   > **CAUTION**
   >
   > ALTER SYSTEM CREATE IMAGE ステートメントは v2.5.3 以降でサポートされています。以前のバージョンでは、Leader FE を再起動してメタデータスナップショットを作成する必要があります。

2. FE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <fe_dir> を FE ノードのデプロイメントディレクトリに置き換えます。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

3. **bin**、**lib**、および **spark-dpp** の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

   > **CAUTION**
   >
   > StarRocks v3.0 を v2.5 にダウングレードする場合、デプロイメントファイルを置き換えた後、次の手順に従う必要があります。
   >
   > 1. v3.0 デプロイメントの **fe/lib/starrocks-bdb-je-18.3.13.jar** ファイルを v2.5 デプロイメントの **fe/lib** ディレクトリにコピーします。
   > 2. **fe/lib/je-7.\*.jar** ファイルを削除します。

4. FE ノードを起動します。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

5. FE ノードが正常に起動したかどうかを確認します。

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. 上記の手順 2 から手順 5 を繰り返して、他の Follower FE ノードをダウングレードし、最後に Leader FE ノードをダウングレードします。

   > **CAUTION**
   >
   > アップグレードに失敗した後にクラスターをダウングレードし、再度クラスターをアップグレードしたい場合、例えば、2.5->3.0->2.5->3.0 のように、Follower FEs のメタデータアップグレードの失敗を防ぐために、手順 1 を繰り返して新しいスナップショットをトリガーします。

## BE のダウングレード

FE ノードをダウングレードした後、クラスター内の BE ノードをダウングレードできます。

1. BE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <be_dir> を BE ノードのデプロイメントディレクトリに置き換えます。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. **bin** および **lib** の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

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

1. CN ノードの作業ディレクトリに移動し、ノードを正常に停止します。

   ```Bash
   # <cn_dir> を CN ノードのデプロイメントディレクトリに置き換えます。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. **bin** および **lib** の元のデプロイメントファイルを以前のバージョンのものに置き換えます。

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