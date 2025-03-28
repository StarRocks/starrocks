---
displayed_sidebar: docs
toc_max_heading_level: 2
sidebar_position: 20
---

# Data Cache

v3.1.7 および v3.2.3 以降、StarRocks は共有データクラスタにおけるクエリを高速化するために Data Cache を導入し、以前のバージョンでの File Cache を置き換えました。Data Cache は必要に応じてリモートストレージからデータをブロック単位（MB 単位）でロードしますが、File Cache は必要なデータ行数に関係なく、毎回バックグラウンドでデータファイル全体をロードします。

File Cache と比較して、Data Cache には以下の利点があります：

- オブジェクトストレージからの読み取りが少なく、オブジェクトストレージへのアクセスコストが削減されます（オブジェクトストレージがアクセス頻度に基づいて課金される場合）。
- ローカルディスクへの書き込み圧力と CPU 使用率が低下し、他のロードやクエリタスクへの影響が少なくなります（バックグラウンドのロードスレッドが不要になるため）。
- キャッシュの効果が最適化されます（File Cache がファイル内のあまり使用されないデータをロードする可能性があるため）。
- キャッシュされたデータの制御が最適化され、File Cache によって削除されなかった過剰なデータによってローカルディスクが圧迫されることを回避します。

## Data Cache の有効化

v3.4.0 以降、StarRocks は共有データクラスタ内の external catalog およびクラウドネイティブテーブルに対するクエリに対して統一された Data Cache インスタンスを使用します。

## Data Cache の設定

以下の CN(BE) 設定項目を使用して Data Cache を設定できます：

### キャッシュディレクトリ

- [storage_root_path](../../administration/management/BE_configuration.md#storage_root_path) （共有データクラスタでは、この項目はキャッシュされたデータが保存されるルートパスを指定するために使用されます。）

### キャッシュディスクサイズ

- [datacache_disk_size](../../administration/management/BE_configuration.md#datacache_disk_size)
- [starlet_star_cache_disk_size_percent](../../administration/management/BE_configuration.md#starlet_star_cache_disk_size_percent)

共有データクラスタ内のキャッシュのディスクサイズは、`datacache_disk_size` と `starlet_star_cache_disk_size_percent` のうち大きい方の値を取ります。

## Data Cache の状態を確認

- キャッシュされたデータを保存するルートパスを確認するには、次のステートメントを実行します：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%storage_root_path%";
  ```

  通常、キャッシュされたデータは `storage_root_path` のサブパス `datacache/` に保存されます。

- Data Cache が使用できるストレージの最大割合を確認するには、次のステートメントを実行します：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_star_cache_disk_size_percent% or %datacache_disk_size%";
  ```

## Data Cache の監視

StarRocks は Data Cache を監視するためのさまざまなメトリクスを提供します。

### ダッシュボードテンプレート

StarRocks 環境に基づいて、以下の Grafana ダッシュボードテンプレートをダウンロードできます：

- [仮想マシン上の StarRocks 共有データクラスタ用ダッシュボードテンプレート](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-vm.json)
- [Kubernetes 上の StarRocks 共有データクラスタ用ダッシュボードテンプレート](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-k8s.json)

### 重要なメトリクス

#### fslib read io_latency

Data Cache の読み取りレイテンシーを記録します。

#### fslib write io_latency

Data Cache の書き込みレイテンシーを記録します。

#### fslib star cache meta memory size

Data Cache の推定メモリ使用量を記録します。

#### fslib star cache data disk size

Data Cache の実際のディスク使用量を記録します。

## Data Cache の無効化

Data Cache を無効にするには、CN 設定ファイル **cn.conf** に次の設定を追加し、CN ノードを再起動します：

```Properties
datacache_enable = false
storage_root_path =
```

## キャッシュされたデータのクリア

緊急時にはキャッシュされたデータをクリアできます。これにより、リモートストレージ内の元のデータには影響しません。

CN ノード上でキャッシュされたデータをクリアする手順は次のとおりです：

1. データを保存するサブディレクトリを削除します。

   例：

   ```Bash
   # Suppose `storage_root_path = /data/disk1;/data/disk2`
   rm -rf /data/disk1/datacache/
   rm -rf /data/disk2/datacache/
   ```

2. CN ノードを再起動します。

## 使用上の注意

- クラウドネイティブテーブルに対して `datacache.enable` プロパティが `false` に設定されている場合、そのテーブルに対して Data Cache は有効になりません。
- `datacache.partition_duration` プロパティが特定の時間範囲に設定されている場合、その時間範囲を超えたデータはキャッシュされません。
- 共有データクラスタを v3.3 から v3.2.8 以前にダウングレードした後、Data Cache のキャッシュされたデータを再利用する場合、ディレクトリ `starlet_cache` 内の Blockfile のファイル名形式を `blockfile_{n}.{version}` から `blockfile_{n}` に手動で変更し、バージョン情報のサフィックスを削除する必要があります。v3.2.9 以降のバージョンは v3.3 のファイル名形式と互換性があるため、この操作を手動で行う必要はありません。次のシェルスクリプトを実行して名前を変更できます：

```Bash
#!/bin/bash

# Replace <starlet_cache_path> with the directory of Data Cache of your cluster, for example, /usr/be/storage/starlet_cache.
starlet_cache_path="<starlet_cache_path>"

for blockfile in ${starlet_cache_path}/blockfile_*; do
    if [ -f "$blockfile" ]; then
        new_blockfile=$(echo "$blockfile" | cut -d'.' -f1)
        mv "$blockfile" "$new_blockfile"
    fi
done
```

## 既知の問題

### 高メモリ使用量

- 説明：クラスタが低負荷条件下で動作している間、CN ノードの総メモリ使用量が各モジュールのメモリ使用量の合計をはるかに超えています。
- 識別：`fslib star cache meta memory size` と `fslib star cache data memory size` の合計が CN ノードの総メモリ使用量のかなりの割合を占めている場合、この問題を示している可能性があります。
- 影響を受けるバージョン：v3.1.8 およびそれ以前のパッチバージョン、v3.2.3 およびそれ以前のパッチバージョン
- 修正バージョン：v3.1.9、v3.2.4
- 解決策：
  - クラスタを修正バージョンにアップグレードします。
  - クラスタをアップグレードしたくない場合は、CN ノードのディレクトリ `${storage_root_path}/starlet_cache/star_cache/meta` をクリアし、ノードを再起動します。

## Q&A

### Q1: なぜ Data Cache ディレクトリはキャッシュされたデータの実際のサイズよりもはるかに大きなストレージスペースを占有しているのですか？

Data Cache によって占有されるディスクスペースは、過去のピーク使用量を表しており、現在の実際のキャッシュデータサイズとは無関係です。例えば、100 GB のデータがキャッシュされている場合、データサイズはコンパクション後に 200 GB になります。その後、ガーベジコレクション（GC）によってデータサイズは 100 GB に減少しました。しかし、Data Cache によって占有されるディスクスペースは、実際のキャッシュデータが 100 GB であっても、ピークの 200 GB のままです。

### Q2: Data Cache は自動的にデータを削除しますか？

いいえ。Data Cache はディスク使用量の制限（デフォルトでディスクスペースの `80%`）に達したときにのみデータを削除します。削除プロセスはデータを削除するのではなく、古いキャッシュを保存しているディスクスペースを空きとしてマークします。その後、新しいキャッシュが古いキャッシュを上書きします。したがって、削除が発生してもディスク使用量は減少せず、実際の使用には影響しません。

### Q3: なぜディスク使用量は設定された最大レベルのままで減少しないのですか？

Q2 を参照してください。Data Cache の削除メカニズムはキャッシュされたデータを削除するのではなく、古いデータを上書き可能としてマークします。したがって、ディスク使用量は減少しません。

### Q4: テーブルを削除し、リモートストレージからテーブルファイルが削除された後でも、なぜ Data Cache のディスク使用量は同じレベルのままですか？

テーブルを削除しても、Data Cache 内のデータ削除はトリガーされません。削除されたテーブルのキャッシュは、Data Cache の LRU（Least Recently Used）ロジックに基づいて時間の経過とともに徐々に削除され、実際の使用には影響しません。

### Q5: 80% のディスク容量が設定されているにもかかわらず、なぜ実際のディスク使用量が 90% 以上に達するのですか？

Data Cache によるディスク使用量は正確であり、設定された制限を超えることはありません。過剰なディスク使用量は以下の原因で発生する可能性があります：

- 実行時に生成されたログファイル。
- CN のクラッシュによって生成されたコアファイル。
- 主キーテーブルの永続性インデックス（`${storage_root_path}/persist/` に保存されます）。
- BE/CN/FE インスタンスの混在デプロイメントが同じディスクを共有している。
- ディスクの問題、例えば ext3 ファイルシステムが使用されている。

ディスクのルートディレクトリまたはサブディレクトリで `du -h . -d 1` を実行して、特定のスペースを占有しているディレクトリを確認し、予期しない部分を削除できます。また、`starlet_star_cache_disk_size_percent` を設定して Data Cache のディスク容量制限を減らすこともできます。

### Q6: なぜノード間で Data Cache のディスク使用量に大きな違いがあるのですか？

単一ノードキャッシングの固有の制限により、ノード間で一貫したキャッシュ使用量を確保することは不可能です。クエリのレイテンシーに影響がない限り、キャッシュ使用量の違いは許容されます。この違いは以下の原因で発生する可能性があります：

- ノードがクラスタに追加された時点の違い。
- 異なるノードが所有するタブレットの数の違い。
- 異なるタブレットが所有するデータのサイズの違い。
- ノード間でのコンパクションと GC の状況の違い。
- ノードがクラッシュまたは Out-Of-Memory（OOM）問題を経験している。

要するに、キャッシュ使用量の違いは複数の要因によって影響を受けます。