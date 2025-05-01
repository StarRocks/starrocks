---
displayed_sidebar: docs
toc_max_heading_level: 2
sidebar_position: 20
---

# Data Cache

v3.1.7 および v3.2.3 以降、StarRocks は共有データクラスタでのクエリを高速化するために Data Cache を導入し、以前のバージョンでの File Cache を置き換えました。Data Cache は必要に応じてリモートストレージからデータをブロック単位（MB オーダー）でロードしますが、File Cache は必要なデータ行の数に関係なく、バックグラウンドでデータファイル全体をロードします。

File Cache と比較して、Data Cache には次のような利点があります：

- オブジェクトストレージからの読み取りが少なくなり、オブジェクトストレージへのアクセスコストが削減されます（オブジェクトストレージがアクセス頻度に基づいて課金される場合）。
- ローカルディスクへの書き込み圧力と CPU 使用率が減少し、他のロードやクエリタスクへの影響が少なくなります（バックグラウンドロードスレッドが不要になるため）。
- キャッシュの効果が最適化されます（File Cache はファイル内のあまり使用されないデータをロードする可能性があります）。
- キャッシュされたデータの制御が最適化され、File Cache によって削除されなかった過剰なデータによってローカルディスクが圧迫されるのを防ぎます。

## Data Cache を有効にする

v3.2.3 以降、Data Cache はデフォルトで有効になっています。v3.1 クラスタで Data Cache を使用したい場合、または以前にこの機能を手動で無効にした場合は、次の手順を実行して有効にする必要があります。

実行時に Data Cache を動的に有効にするには、次のステートメントを実行します：

```SQL
UPDATE information_schema.be_configs SET VALUE = 1 
WHERE name = "starlet_use_star_cache";
```

動的な構成は、CN ノードが再起動されると無効になります。

Data Cache を永続的に有効にするには、CN 構成ファイル **cn.conf** に次の構成を追加し、CN ノードを再起動する必要があります：

```Properties
starlet_use_star_cache = true
```

## Data Cache を構成する

次の CN(BE) 構成項目を使用して Data Cache を構成できます：

- [storage_root_path](../../administration/management/BE_configuration.md#storage_root_path)（共有データクラスタでは、キャッシュされたデータが保存されるルートパスを指定するために使用されます。）
- [starlet_use_star_cache](../../administration/management/BE_configuration.md#starlet_use_star_cache)
- [starlet_star_cache_disk_size_percent](../../administration/management/BE_configuration.md#starlet_star_cache_disk_size_percent)

:::note

StarRocks クラスタを v3.1.7、v3.2.3、またはそれ以降のバージョンにアップグレードする前に File Cache を有効にしていた場合、構成項目 `starlet_cache_evict_high_water` を変更したかどうかを確認してください。この項目のデフォルト値は `0.2` で、File Cache がストレージスペースの 80% を使用してキャッシュデータファイルを保存することを示しています。この項目を変更した場合、アップグレード時に `starlet_star_cache_disk_size_percent` を適切に構成する必要があります。例えば、以前に `starlet_cache_evict_high_water` を `0.3` に設定していた場合、StarRocks クラスタをアップグレードする際に `starlet_star_cache_disk_size_percent` を `70` に設定して、Data Cache が File Cache に設定したディスク容量と同じ割合を使用するようにします。

:::

## Data Cache のステータスを確認する

- Data Cache が有効かどうかを確認するには、次のステートメントを実行します：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_use_star_cache%";
  ```

- キャッシュされたデータが保存されるルートパスを表示するには、次のステートメントを実行します：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%storage_root_path%";
  ```

  キャッシュされたデータは、`storage_root_path` のサブパス `starlet_cache/star_cache` に保存されます。

- Data Cache が使用できるストレージの最大割合を表示するには、次のステートメントを実行します：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_star_cache_disk_size_percent%";
  ```

## Data Cache を監視する

StarRocks は Data Cache を監視するためのさまざまなメトリクスを提供します。

### ダッシュボードテンプレート

StarRocks 環境に基づいて次の Grafana ダッシュボードテンプレートをダウンロードできます：

- [仮想マシン上の StarRocks 共有データクラスタ用ダッシュボードテンプレート](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-vm.json)
- [Kubernetes 上の StarRocks 共有データクラスタ用ダッシュボードテンプレート](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-k8s.json)

### 重要なメトリクス

#### fslib read io_latency

Data Cache の読み取り遅延を記録します。

#### fslib write io_latency

Data Cache の書き込み遅延を記録します。

#### fslib star cache meta memory size

Data Cache の推定メモリ使用量を記録します。

#### fslib star cache data disk size

Data Cache の実際のディスク使用量を記録します。

## Data Cache を無効にする

実行時に Data Cache を動的に無効にするには、次のステートメントを実行します：

```SQL
UPDATE information_schema.be_configs SET VALUE = 0 
WHERE name = "starlet_use_star_cache";
```

動的な構成は、CN ノードが再起動されると無効になります。

Data Cache を永続的に無効にするには、CN 構成ファイル **cn.conf** に次の構成を追加し、CN ノードを再起動する必要があります：

```Properties
starlet_use_star_cache = false
```

## キャッシュされたデータをクリアする

緊急時にはキャッシュされたデータをクリアできます。これにより、リモートストレージ内の元のデータには影響しません。

CN ノードでキャッシュされたデータをクリアする手順は次のとおりです：

1. データを保存しているサブディレクトリを削除します。つまり、`${storage_root_path}/starlet_cache` です。

   例：

   ```Bash
   # `storage_root_path = /data/disk1;/data/disk2` と仮定します
   rm -rf /data/disk1/starlet_cache/
   rm -rf /data/disk2/starlet_cache/
   ```

2. CN ノードを再起動します。

## 使用上の注意

- クラウドネイティブテーブルに対して `datacache.enable` プロパティが `false` に設定されている場合、そのテーブルには Data Cache は有効になりません。
- `datacache.partition_duration` プロパティが特定の時間範囲に設定されている場合、その時間範囲を超えるデータはキャッシュされません。
- 共有データクラスタを v3.3 から v3.2.8 以前にダウングレードした後、Data Cache のキャッシュデータを再利用したい場合、ディレクトリ `starlet_cache` 内の Blockfile のファイル名形式を `blockfile_{n}.{version}` から `blockfile_{n}` に変更し、バージョン情報のサフィックスを削除する必要があります。v3.2.9 以降のバージョンは v3.3 のファイル名形式と互換性があるため、この操作を手動で行う必要はありません。次のシェルスクリプトを実行して名前を変更できます：

```Bash
#!/bin/bash

# Data Cache のディレクトリを <starlet_cache_path> に置き換えます。例：/usr/be/storage/starlet_cache。
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

- 説明：クラスタが低負荷条件で動作している間、CN ノードの総メモリ使用量が各モジュールのメモリ使用量の合計をはるかに超えています。
- 識別：`fslib star cache meta memory size` と `fslib star cache data memory size` の合計が CN ノードの総メモリ使用量の大部分を占めている場合、この問題を示している可能性があります。
- 影響を受けるバージョン：v3.1.8 およびそれ以前のパッチバージョン、v3.2.3 およびそれ以前のパッチバージョン
- 修正バージョン：v3.1.9、v3.2.4
- 解決策：
  - クラスタを修正されたバージョンにアップグレードします。
  - クラスタをアップグレードしたくない場合は、CN ノードのディレクトリ `${storage_root_path}/starlet_cache/star_cache/meta` をクリアし、ノードを再起動します。

## Q&A

### Q1: Data Cache ディレクトリがキャッシュされたデータの実際のサイズよりもはるかに大きなストレージスペースを占有するのはなぜですか？

Data Cache によって占有されるディスクスペースは、過去のピーク使用量を表しており、現在の実際のキャッシュデータサイズとは無関係です。たとえば、100 GB のデータがキャッシュされた場合、データサイズはコンパクション後に 200 GB になります。その後、ガーベジコレクション（GC）後にデータサイズは 100 GB に減少しました。しかし、Data Cache によって占有されるディスクスペースは、実際のキャッシュデータが 100 GB であっても、ピークの 200 GB のままです。

### Q2: Data Cache は自動的にデータを削除しますか？

いいえ。Data Cache はディスク使用量の制限（デフォルトでディスクスペースの `80%`）に達したときのみデータを削除します。削除プロセスはデータを削除するのではなく、古いキャッシュを保存しているディスクスペースを空としてマークします。その後、新しいキャッシュが古いキャッシュを上書きします。したがって、削除が発生してもディスク使用量は減少せず、実際の使用には影響しません。

### Q3: ディスク使用量が設定された最大レベルにとどまり、減少しないのはなぜですか？

Q2 を参照してください。Data Cache の削除メカニズムはキャッシュデータを削除せず、古いデータを上書き可能としてマークします。したがって、ディスク使用量は減少しません。

### Q4: テーブルを削除し、テーブルファイルがリモートストレージから削除された後も、Data Cache のディスク使用量が同じレベルにとどまるのはなぜですか？

テーブルを削除しても、Data Cache 内のデータの削除はトリガーされません。削除されたテーブルのキャッシュは、Data Cache の LRU（Least Recently Used）ロジックに基づいて時間とともに徐々に削除され、実際の使用には影響しません。

### Q5: 実際のディスク使用量が 90% 以上に達しているのに、80% のディスク容量が設定されているのはなぜですか？

Data Cache によるディスク使用量は正確であり、設定された制限を超えることはありません。過剰なディスク使用量は、次の原因による可能性があります：

- 実行時に生成されたログファイル。
- CN のクラッシュによって生成されたコアファイル。
- 主キーテーブルの永続性インデックス（`${storage_root_path}/persist/` に保存されます）。
- 同じディスクを共有する BE/CN/FE インスタンスの混在デプロイメント。
- 外部テーブルキャッシュ（`${STARROCKS_HOME}/block_cache/` に保存されます）。
- ディスクの問題、たとえば ext3 ファイルシステムが使用されている場合。

ディスクのルートディレクトリまたはサブディレクトリで `du -h . -d 1` を実行して、特定のスペースを占有しているディレクトリを確認し、予期しない部分を削除できます。また、`starlet_star_cache_disk_size_percent` を構成して Data Cache のディスク容量制限を減らすこともできます。

### Q6: ノード間で Data Cache のディスク使用量に大きな違いがあるのはなぜですか？

単一ノードキャッシングの固有の制限により、ノード間で一貫したキャッシュ使用量を確保することは不可能です。クエリの遅延に影響を与えない限り、キャッシュ使用量の違いは許容されます。この差異は次の原因による可能性があります：

- ノードがクラスタに追加された時点の違い。
- 異なるノードが所有するタブレットの数の違い。
- 異なるタブレットが所有するデータのサイズの違い。
- ノード間でのコンパクションと GC の状況の違い。
- ノードがクラッシュまたはメモリ不足（OOM）問題を経験している。

要するに、キャッシュ使用量の違いは複数の要因によって影響を受けます。