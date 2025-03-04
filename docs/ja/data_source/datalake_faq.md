---
displayed_sidebar: docs
---

# データレイク FAQ

このトピックでは、データレイクに関するよくある質問 (FAQ) を説明し、これらの問題に対する解決策を提供します。このトピックで言及されているいくつかのメトリクスは、SQL クエリのプロファイルからのみ取得できます。SQL クエリのプロファイルを取得するには、`set enable_profile=true` を指定する必要があります。

## 遅い HDFS DataNode

### 問題の説明

HDFS クラスターに保存されているデータファイルにアクセスする際、実行した SQL クエリのプロファイルから `__MAX_OF_FSIOTime` と `__MIN_OF_FSIOTime` メトリクスの値に大きな差があることに気付くかもしれません。これは、HDFS クラスター内の一部の DataNode が遅いことを示しています。以下は、遅い HDFS DataNode の問題を示す典型的なプロファイルの例です。

```plaintext
 - InputStream: 0
   - AppIOBytesRead: 22.72 GB
     - __MAX_OF_AppIOBytesRead: 187.99 MB
     - __MIN_OF_AppIOBytesRead: 64.00 KB
   - AppIOCounter: 964.862K (964862)
     - __MAX_OF_AppIOCounter: 7.795K (7795)
     - __MIN_OF_AppIOCounter: 1
   - AppIOTime: 1s372ms
     - __MAX_OF_AppIOTime: 4s358ms
     - __MIN_OF_AppIOTime: 1.539ms
   - FSBytesRead: 15.40 GB
     - __MAX_OF_FSBytesRead: 127.41 MB
     - __MIN_OF_FSBytesRead: 64.00 KB
   - FSIOCounter: 1.637K (1637)
     - __MAX_OF_FSIOCounter: 12
     - __MIN_OF_FSIOCounter: 1
   - FSIOTime: 9s357ms
     - __MAX_OF_FSIOTime: 60s335ms
     - __MIN_OF_FSIOTime: 1.536ms
```

### 解決策

この問題を解決するために、次のいずれかの解決策を使用できます。

- **[推奨]** [Data Cache](../data_source/data_cache.md) 機能を有効にします。これにより、外部ストレージシステムから StarRocks クラスターの BEs または CNs にデータを自動的にキャッシュすることで、遅い HDFS DataNode がクエリに与える影響を排除します。
- **[推奨]** HDFS クライアントと DataNode 間のタイムアウト時間を短縮します。この解決策は、Data Cache が遅い HDFS DataNode の問題を解決できない場合に適しています。
- [Hedged Read](https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/release/2.4.0/RELEASENOTES.2.4.0.html) 機能を有効にします。この機能を有効にすると、ブロックからの読み取りが遅い場合、StarRocks は新しい読み取りを開始し、元の読み取りと並行して実行し、異なるブロックレプリカに対して読み取ります。2 つの読み取りのいずれかが返された場合、もう一方の読み取りはキャンセルされます。**Hedged Read 機能は読み取りを高速化するのに役立ちますが、Java 仮想マシン (JVM) のヒープメモリ消費量を大幅に増加させます。そのため、物理マシンのメモリ容量が小さい場合は、Hedged Read 機能を有効にしないことをお勧めします。**

#### [推奨] Data Cache

[Data Cache](../data_source/data_cache.md) を参照してください。

#### [推奨] HDFS クライアントと DataNode 間のタイムアウト時間を短縮

`hdfs-site.xml` ファイルで `dfs.client.socket-timeout` プロパティを設定して、HDFS クライアントと DataNode 間のタイムアウト時間を短縮します。(デフォルトのタイムアウト時間は 60 秒で、少し長めです。) これにより、StarRocks が遅い DataNode に遭遇した場合、接続要求が非常に短時間でタイムアウトし、別の DataNode に転送されることができます。以下の例では、5 秒のタイムアウト時間を設定しています。

```xml
<configuration>
  <property>
      <name>dfs.client.socket-timeout</name>
      <value>5000</value>
   </property>
</configuration>
```

#### Hedged Read

HDFS クラスターで Hedged Read 機能を有効にして設定するには、BE または CN の設定ファイル `be.conf` で次のパラメータを使用します (v3.0 以降でサポートされています)。

| パラメータ                                | デフォルト値 | 説明                                                         |
| ---------------------------------------- | ------------- | ------------------------------------------------------------------- |
| hdfs_client_enable_hedged_read           | false         | Hedged Read 機能を有効にするかどうかを指定します。                                    |
| hdfs_client_hedged_read_threadpool_size  | 128           | HDFS クライアントでの Hedged Read スレッドプールのサイズを指定します。スレッドプールのサイズは、HDFS クライアントでの Hedged Read の実行に専念するスレッドの数を制限します。このパラメータは、HDFS クラスターの `hdfs-site.xml` ファイル内の `dfs.client.hedged.read.threadpool.size` パラメータに相当します。 |
| hdfs_client_hedged_read_threshold_millis | 2500          | Hedged Read を開始するまでの待機時間をミリ秒単位で指定します。たとえば、このパラメータを `30` に設定した場合、ブロックからの読み取りが 30 ミリ秒以内に返されない場合、HDFS クライアントは直ちに異なるブロックレプリカに対して Hedged Read を開始します。このパラメータは、HDFS クラスターの `hdfs-site.xml` ファイル内の `dfs.client.hedged.read.threshold.millis` パラメータに相当します。 |

クエリプロファイル内の次のメトリクスのいずれかの値が `0` を超える場合、Hedged Read 機能が有効です。

| メトリクス                         | 説明                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| TotalHedgedReadOps             | 開始された Hedged Read の数。                 |
| TotalHedgedReadOpsInCurThread  | Hedged Read スレッドプールが `hdfs_client_hedged_read_threadpool_size` パラメータで指定された最大サイズに達したため、StarRocks が新しいスレッドではなく現在のスレッドで Hedged Read を開始しなければならなかった回数。 |
| TotalHedgedReadOpsWin          | Hedged Read が元の読み取りに勝った回数。 |