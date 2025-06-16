---
displayed_sidebar: docs
---

# Data Cache FAQ

このトピックでは、Data Cache に関するよくある質問 (FAQ) と一般的な問題について説明し、これらの問題に対するトラブルシューティング手順と解決策を提供します。

## Data Cache の有効化

### Data Cache が正常に有効化されているかどうかを確認するにはどうすればよいですか？

ほとんどの場合、次のいずれかの方法で Data Cache が正常に有効化されているかどうかを確認できます。

- SQL クライアントから `SHOW BACKENDS` (または `SHOW COMPUTE NODES`) を実行し、`DataCacheMetrics` の値を確認します。ディスクまたはメモリキャッシュのクォータが 0 より大きい場合、Data Cache が有効化されていることを確認できます。

```SQL
mysql> show backends \G
*************************** 1. row ***************************
            BackendId: 89041
                   IP: X.X.X.X
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2025-05-29 14:45:37
        LastHeartbeat: 2025-05-29 19:20:32
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 10
     DataUsedCapacity: 0.000 B
        AvailCapacity: 1.438 TB
        TotalCapacity: 1.718 TB
              UsedPct: 16.27 %
       MaxDiskUsedPct: 16.27 %
               ErrMsg:
              Version: main-c15b412
               Status: {"lastSuccessReportTabletsTime":"2025-05-29 19:20:30"}
    DataTotalCapacity: 1.438 TB
          DataUsedPct: 0.00 %
             CpuCores: 8
             MemLimit: 50.559GB
    NumRunningQueries: 0
           MemUsedPct: 0.50 %
           CpuUsedPct: 0.2 %
     DataCacheMetrics: Status: Normal, DiskUsage: 44MB/1TB, MemUsage: 0B/0B
             Location:
           StatusCode: OK
1 row in set (0.00 sec)
```

上記の例では、Data Cache のディスクキャッシュクォータは 1TB で、現在 44MB が使用されています。一方、メモリキャッシュクォータは 0B であるため、メモリキャッシュは有効化されていません。

- BE Web コンソール (`http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat`) にアクセスして、現在の Data Cache クォータ、ヒット率、およびその他のメトリクスを確認できます。`disk_quota_bytes` または `mem_quota_bytes` が 0 より大きい場合、Data Cache が有効化されていることを確認できます。

![Data Cache FAQ - Web Console](../_assets/data_cache_be_web_console.png)

### なぜ Data Cache はデフォルトで有効化されていないのですか？

v3.3 以降、BE は起動時に Data Cache を有効化しようとします。ただし、現在のディスクに十分な空きスペースがない場合、Data Cache は自動的に有効化されません。

これは次の状況によって引き起こされる可能性があります。

- **使用率**: 現在のディスク使用率が高い。

- **残りスペース**: 残りのディスクスペースのサイズが比較的少ない。

したがって、Data Cache がデフォルトで有効化されていない場合、まず現在のディスク使用率を確認し、必要に応じてディスク容量を増やすことができます。

または、現在の利用可能なディスクスペースに基づいてキャッシュクォータを設定することで、手動で Data Cache を有効化することもできます。

```
# Data Cache 自動調整を無効化
datacache_auto_adjust_enable = false
# Data Cache ディスククォータを手動で設定
datacache_disk_size = 1T
```

## Data Cache の使用

### Data Cache はどのカタログタイプをサポートしていますか？

Data Cache は現在、StarRocks Native File Reader (Parquet/ORC/CSV Reader など) を使用する external catalog タイプをサポートしており、Hive、Iceberg、Hudi、Delta Lake、Paimon などが含まれます。JNI に基づいてデータにアクセスするカタログ (JDBC Catalog など) はまだサポートされていません。

:::note
一部のカタログは、特定の条件 (ファイルタイプやデータの状態など) に基づいて異なるデータアクセス方法を使用する場合があります。たとえば、Paimon catalog では、StarRocks は現在のデータの compaction 状態に基づいて、Native File Reader または JNI を使用してデータにアクセスするかどうかを自動的に選択する場合があります。Paimon データに JNI を使用してアクセスする場合、Data Cache のアクセラレーションはサポートされません。
:::

### クエリがキャッシュにヒットしたことを知るにはどうすればよいですか？

対応する Query Profile で Data Cache に関連するメトリクスを確認できます。`DataCacheReadBytes` と `DataCacheReadCounter` のメトリクスは、ローカルキャッシュのヒット状況を示します。

```
 - DataCacheReadBytes: 518.73 MB
   - __MAX_OF_DataCacheReadBytes: 4.73 MB
   - __MIN_OF_DataCacheReadBytes: 16.00 KB
 - DataCacheReadCounter: 684
   - __MAX_OF_DataCacheReadCounter: 4
   - __MIN_OF_DataCacheReadCounter: 0
 - DataCacheReadTimer: 737.357us
 - DataCacheWriteBytes: 7.65 GB
   - __MAX_OF_DataCacheWriteBytes: 64.39 MB
   - __MIN_OF_DataCacheWriteBytes: 0.00
 - DataCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_DataCacheWriteCounter: 65
   - __MIN_OF_DataCacheWriteCounter: 0
 - DataCacheWriteTimer: 23.467ms
   - __MAX_OF_DataCacheWriteTimer: 62.280ms
   - __MIN_OF_DataCacheWriteTimer: 0ns
```

### Data Cache が有効化されているのに、なぜクエリがキャッシュにヒットしないのですか？

トラブルシューティングの手順は次のとおりです。

1. Data Cache が現在のカタログタイプをサポートしているかどうかを確認します。
2. クエリステートメントがキャッシュのポピュレーション条件を満たしているかどうかを確認します。特定のケースでは、Data Cache は一部のクエリに対してキャッシュのポピュレーションを拒否します。詳細については、[Data Cache Population Rules](./data_cache.md#ポピュレーションルール) を参照してください。

`EXPLAIN VERBOSE` コマンドを使用して、クエリがキャッシュのポピュレーションをトリガーするかどうかを確認できます。
例:

```sql
mysql> EXPLAIN VERBOSE SELECT col1 FROM hudi_table;
|   0:HudiScanNode                        |
|      TABLE: hudi_table                  |
|      partitions=3/3                     |
|      cardinality=9084                   |
|      avgRowSize=2.0                     |
|      dataCacheOptions={populate: false} |
|      cardinality: 9084                  |
+-----------------------------------------+
```

上記の例では、`dataCacheOptions` セクションの `populate` フィールドが `false` であることは、クエリに対してキャッシュがポピュレートされないことを示しています。

このようなクエリに対して Data Cache を有効化するには、システム変数 `populate_datacache_mode` を `always` に設定して、デフォルトのポピュレーション動作を変更できます。

## Data Cache ヒット

### なぜ同じクエリを複数回実行しないとキャッシュに完全にヒットしないのですか？

現在のバージョンでは、Data Cache はクエリパフォーマンスへの影響を軽減するためにデフォルトで非同期ポピュレーションを使用します。非同期ポピュレーションを使用する場合、システムは読み取りパフォーマンスにできるだけ影響を与えずに、バックグラウンドでアクセスされたデータをキャッシュしようとします。したがって、クエリを一度実行するだけでは、クエリに必要なデータの一部しかキャッシュできません。クエリを複数回実行して、クエリに必要なすべてのデータをキャッシュする必要があります。

`enable_datacache_async_populate_mode=false` を設定して同期キャッシュポピュレーションを使用するか、`CACHE SELECT` を使用して対象データを事前にウォームアップすることもできます。

### 現在のクエリのすべてのデータがキャッシュされているのに、なぜ一部のデータがリモートでアクセスされるのですか？

現在のバージョンでは、ディスク I/O 負荷が高い場合にキャッシュパフォーマンスを最適化するために、I/O 適応がデフォルトで有効になっています。これにより、一部のケースでは少数のリクエストが直接リモートストレージにアクセスすることがあります。

`enable_datacache_io-adapter` を `false` に設定して I/O 適応機能を無効にすることができます。

## その他

### キャッシュされたデータをクリアするにはどうすればよいですか？

現在、Data Cache はキャッシュされたデータを直接クリアするインターフェースを提供していませんが、次の方法のいずれかを選択してクリアできます。

- BE/CN ノード上の `datacache` ディレクトリ内のすべてのデータ (ブロックファイルやメタディレクトリを含む) を削除し、ノードを再起動することでキャッシュされたデータをクリーンアップできます。(推奨)

- BE/CN ノードを再起動したくない場合、実行時にキャッシュクォータを縮小することで間接的にキャッシュされたデータをクリーンアップすることもできます。たとえば、以前にディスクキャッシュクォータを 2TB に設定した場合、まずそれを 0 に縮小することができます (システムは自動的にキャッシュされたデータをクリーンアップします)、その後再度 2TB に設定します。

例:

```SQL
UPDATE be_configs SET VALUE="0" WHERE NAME="datacache_disk_size" and BE_ID=10005;
UPDATE be_configs SET VALUE="2T" WHERE NAME="datacache_disk_size" and BE_ID=10005;
```

:::note
実行時にキャッシュされたデータをクリーンアップする際は、ステートメントの `WHERE` 条件に注意して、他の無関係なパラメータやノードを誤って損傷しないようにしてください。
:::

### Data Cache のパフォーマンスを向上させるにはどうすればよいですか？

Data Cache を使用すると、StarRocks はリモートストレージの代わりにローカルメモリまたはディスクにアクセスします。したがって、パフォーマンスはローカルキャッシュメディアに直接関連しています。ディスク負荷が高いためにキャッシュアクセスのレイテンシーが高い場合は、ローカルキャッシュメディアのパフォーマンスを向上させることを検討できます。

- キャッシュディスクとして高性能な NVME ディスクを優先的に使用します。

- 高性能ディスクが利用できない場合は、ディスクの数を増やして I/O 圧力を分散させることもできます。

- BE/CN ノードのサーバーメモリを増やし (Data Cache メモリクォータではなく)、オペレーティングシステムの Page Cache を使用して直接ディスクアクセスの回数とディスク I/O 圧力を減らします。
