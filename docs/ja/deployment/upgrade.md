---
displayed_sidebar: docs
---

# StarRocks のアップグレード

このトピックでは、StarRocks クラスタをアップグレードする方法について説明します。

## 概要

アップグレードする前にこのセクションの情報を確認し、推奨されるアクションを実行してください。

### StarRocks のバージョン

StarRocks のバージョンは、**Major.Minor.Patch** の形式で表され、例えば `2.5.4` です。最初の数字は StarRocks のメジャーバージョンを表し、2 番目の数字はマイナーバージョンを、3 番目の数字はパッチバージョンを表します。

> **注意**
>
> 既存の共有なしクラスタを共有データクラスタにアップグレードすることはできませんし、その逆もできません。新しい共有データクラスタをデプロイする必要があります。

### アップグレードパス

- **パッチバージョンのアップグレードの場合**

  StarRocks クラスタをパッチバージョン間でアップグレードできます。例えば、v2.2.6 から直接 v2.2.11 へアップグレードできます。

- **マイナーバージョンのアップグレードの場合**

  StarRocks v2.0 以降では、StarRocks クラスタをマイナーバージョン間でアップグレードできます。例えば、v2.2.x から直接 v2.5.x へアップグレードできます。ただし、互換性と安全性の理由から、StarRocks クラスタを**一つのマイナーバージョンから次のバージョンへ順次アップグレード**することを強くお勧めします。例えば、StarRocks v2.2 クラスタを v2.5 にアップグレードするには、次の順序でアップグレードする必要があります: v2.2.x --> v2.3.x --> v2.4.x --> v2.5.x。

- **メジャーバージョンのアップグレードの場合**

  StarRocks クラスタを v3.0 にアップグレードするには、まず v2.5 にアップグレードする必要があります。

> **注意**
>
> 例えば、2.4->2.5->3.0->3.1->3.2 のように連続したマイナーバージョンのアップグレードを行う必要がある場合、またはアップグレードが失敗した後にクラスタをダウングレードし、再度アップグレードを行いたい場合、例えば 2.5->3.0->2.5->3.0 のように、いくつかの Follower FE のメタデータアップグレードの失敗を防ぐために、2 つの連続したアップグレードの間、またはダウングレード後の 2 回目のアップグレードの試行前に次の手順を実行してください:
>
> 1. [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) を実行して新しいイメージを作成します。
> 2. 新しいイメージがすべての Follower FE に同期されるのを待ちます。
>
> イメージファイルが同期されたかどうかは、Leader FE のログファイル **fe.log** を確認することで確認できます。"push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" のようなログ記録があれば、イメージファイルが正常に同期されたことを示しています。

### アップグレード手順

StarRocks は**ローリングアップグレード**をサポートしており、サービスを停止せずにクラスタをアップグレードできます。設計上、BEs と CNs は FEs と後方互換性があります。そのため、クラスタを適切に動作させながらアップグレードするために、**まず BEs と CNs をアップグレードし、その後 FEs をアップグレード**する必要があります。逆の順序でアップグレードすると、FEs と BEs/CNs 間の互換性が失われ、サービスがクラッシュする可能性があります。FE ノードの場合、まずすべての Follower FE ノードをアップグレードしてから Leader FE ノードをアップグレードする必要があります。

## 始める前に

準備中に、マイナーまたはメジャーバージョンのアップグレードを行う場合は、互換性のある設定を行う必要があります。また、クラスタ内のすべてのノードをアップグレードする前に、FEs と BEs のいずれかでアップグレードの可用性テストを実行する必要があります。

### 互換性設定を行う

StarRocks クラスタを後のマイナーまたはメジャーバージョンにアップグレードする場合、互換性設定を行う必要があります。一般的な互換性設定に加えて、アップグレード元の StarRocks クラスタのバージョンに応じて詳細な設定が異なります。

- **一般的な互換性設定**

StarRocks クラスタをアップグレードする前に、tablet クローンを無効にする必要があります。バランサーを無効にしている場合は、この手順をスキップできます。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

アップグレード後、すべての BE ノードのステータスが `Alive` になったら、tablet クローンを再度有効にできます。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **v2.0 から後のバージョンにアップグレードする場合**

StarRocks v2.0 クラスタをアップグレードする前に、次の BE 設定とシステム変数を設定する必要があります。

1. BE 設定項目 `vector_chunk_size` を変更している場合、アップグレード前に `4096` に設定する必要があります。これは静的パラメータであるため、BE 設定ファイル **be.conf** で変更し、ノードを再起動して変更を有効にする必要があります。
2. システム変数 `batch_size` をグローバルに `4096` 以下に設定します。

   ```SQL
   SET GLOBAL batch_size = 4096;
   ```

## BE のアップグレード

アップグレードの可用性テストに合格したら、クラスタ内の BE ノードを最初にアップグレードできます。

1. BE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <be_dir> を BE ノードのデプロイメントディレクトリに置き換えます。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. **bin** および **lib** の下の元のデプロイメントファイルを新しいバージョンのものに置き換えます。

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

5. 他の BE ノードをアップグレードするために、上記の手順を繰り返します。

## CN のアップグレード

1. CN ノードの作業ディレクトリに移動し、ノードを正常に停止します。

   ```Bash
   # <cn_dir> を CN ノードのデプロイメントディレクトリに置き換えます。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. **bin** および **lib** の下の元のデプロイメントファイルを新しいバージョンのものに置き換えます。

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
   ps aux | grep starrocks_be
   ```

5. 他の CN ノードをアップグレードするために、上記の手順を繰り返します。

## FE のアップグレード

すべての BE および CN ノードをアップグレードした後、FE ノードをアップグレードできます。まず Follower FE ノードをアップグレードし、その後 Leader FE ノードをアップグレードする必要があります。

1. FE ノードの作業ディレクトリに移動し、ノードを停止します。

   ```Bash
   # <fe_dir> を FE ノードのデプロイメントディレクトリに置き換えます。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. **bin**、**lib**、および **spark-dpp** の下の元のデプロイメントファイルを新しいバージョンのものに置き換えます。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

3. FE ノードを起動します。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

4. FE ノードが正常に起動したかどうかを確認します。

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. 他の Follower FE ノードをアップグレードし、最後に Leader FE ノードをアップグレードするために、上記の手順を繰り返します。