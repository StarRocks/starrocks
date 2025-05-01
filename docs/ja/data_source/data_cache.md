---
displayed_sidebar: docs
---

# Data Cache

このトピックでは、Data Cache の動作原理と、外部データのクエリパフォーマンスを向上させるために Data Cache を有効にする方法について説明します。

データレイク分析において、StarRocks は OLAP エンジンとして、HDFS や Amazon S3 などの外部ストレージシステムに保存されたデータファイルをスキャンします。スキャンするファイル数が増えると、I/O のオーバーヘッドが増加します。さらに、一部のアドホックなシナリオでは、同じデータへの頻繁なアクセスが I/O のオーバーヘッドを倍増させます。

これらのシナリオでクエリパフォーマンスを最適化するために、StarRocks 2.5 は Data Cache 機能を提供します。この機能は、外部ストレージシステム内のデータを事前定義されたポリシーに基づいて複数のブロックに分割し、StarRocks のバックエンド (BEs) にキャッシュします。これにより、各アクセス要求ごとに外部システムからデータを取得する必要がなくなり、ホットデータのクエリと分析が高速化されます。Data Cache は、外部カタログや外部テーブルを使用して外部ストレージシステムからデータをクエリする場合にのみ機能します (JDBC 互換データベース用の外部テーブルを除く)。StarRocks の内部テーブルをクエリする場合には機能しません。

## 動作原理

StarRocks は、外部ストレージシステム内のデータを同じサイズの複数のブロックに分割し、BEs にキャッシュします。ブロックはデータキャッシュの最小単位であり、設定可能です。

例えば、ブロックサイズを 1 MB に設定し、Amazon S3 から 128 MB の Parquet ファイルをクエリしたい場合、StarRocks はファイルを 128 ブロックに分割します。ブロックは [0, 1 MB), [1 MB, 2 MB), [2 MB, 3 MB) ... [127 MB, 128 MB) となります。StarRocks は各ブロックにグローバルに一意の ID を割り当て、これをキャッシュキーと呼びます。キャッシュキーは次の3つの部分で構成されます。

```Plain
hash(filename) + fileModificationTime + blockId
```

次の表は、各部分の説明を示しています。

| **コンポーネント項目** | **説明**                                              |
| ------------------ | ------------------------------------------------------------ |
| filename           | データファイルの名前。                                   |
| fileModificationTime | データファイルの最終変更時刻。                  |
| blockId            | データファイルを分割する際に StarRocks がブロックに割り当てる ID。同じデータファイル内では一意ですが、StarRocks クラスター内では一意ではありません。 |

クエリが [1 MB, 2 MB) ブロックにヒットした場合、StarRocks は次の操作を行います。

1. ブロックがキャッシュ内に存在するかどうかを確認します。
2. ブロックが存在する場合、StarRocks はキャッシュからブロックを読み取ります。ブロックが存在しない場合、StarRocks は Amazon S3 からブロックを読み取り、BE にキャッシュします。

Data Cache が有効になると、StarRocks は外部ストレージシステムから読み取ったデータブロックをキャッシュします。このようなデータブロックをキャッシュしたくない場合は、次のコマンドを実行します。

```SQL
SET enable_populate_block_cache = false;
```

`enable_populate_block_cache` についての詳細は、 [System variables](../sql-reference/System_variable.md) を参照してください。

## ブロックのストレージメディア

StarRocks は、BE マシンのメモリとディスクを使用してブロックをキャッシュします。メモリのみ、またはメモリとディスクの両方でキャッシュをサポートします。

ディスクをストレージメディアとして使用する場合、キャッシュ速度はディスクの性能に直接影響されます。そのため、データキャッシュには NVMe ディスクなどの高性能ディスクを使用することをお勧めします。高性能ディスクがない場合は、ディスク I/O の負荷を軽減するためにディスクを追加することができます。

## キャッシュ置換ポリシー

StarRocks は [least recently used](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) (LRU) ポリシーを使用してデータをキャッシュおよび破棄します。

- StarRocks は最初にメモリからデータを読み取ります。メモリにデータが見つからない場合、StarRocks はディスクからデータを読み取り、ディスクから読み取ったデータをメモリにロードしようとします。
- メモリから破棄されたデータはディスクに書き込まれます。ディスクから破棄されたデータは削除されます。

## Data Cache の有効化

Data Cache はデフォルトで無効になっています。この機能を有効にするには、StarRocks クラスター内の FEs と BEs を設定します。

### FEs の設定

FEs に対して Data Cache を有効にするには、次のいずれかの方法を使用します。

- 必要に応じて、特定のセッションに対して Data Cache を有効にします。

  ```SQL
  SET enable_scan_block_cache = true;
  ```

- すべてのアクティブなセッションに対して Data Cache を有効にします。

  ```SQL
  SET GLOBAL enable_scan_block_cache = true;
  ```

### BEs の設定

各 BE の **conf/be.conf** ファイルに次のパラメータを追加します。その後、各 BE を再起動して設定を有効にします。

| **パラメータ**          | **説明**                                              | **デフォルト値** |
| ---------------------- | ------------------------------------------------------------ | -------------------|
| block_cache_enable     | Data Cache を有効にするかどうか。<ul><li>`true`: Data Cache が有効です。</li><li>`false`: Data Cache が無効です。</li></ul> | false |
| block_cache_disk_path  | ディスクのパス。複数のディスクを設定でき、ディスクパスをセミコロン (;) で区切ります。設定したパスの数は、BE マシンのディスク数と同じであることをお勧めします。BE が起動すると、StarRocks は自動的にディスクキャッシュディレクトリを作成します (親ディレクトリが存在しない場合は作成に失敗します)。 | `${STARROCKS_HOME}/block_cache` |
| block_cache_meta_path  | ブロックメタデータのストレージパス。このパラメータは指定しなくても構いません。 | `${STARROCKS_HOME}/block_cache` |
| block_cache_mem_size   | メモリにキャッシュできるデータの最大量。単位: バイト。このパラメータの値を少なくとも 20 GB に設定することをお勧めします。Data Cache が有効になった後、StarRocks がディスクから大量のデータを読み取る場合は、値を増やすことを検討してください。 | `2147483648`, これは 2 GB です。  |
| block_cache_disk_size  | 単一のディスクにキャッシュできるデータの最大量。単位: バイト。例えば、`block_cache_disk_path` パラメータに 2 つのディスクパスを設定し、`block_cache_disk_size` パラメータの値を `21474836480` (20 GB) に設定した場合、これらの 2 つのディスクに最大 40 GB のデータをキャッシュできます。  | `0`, これはメモリのみを使用してデータをキャッシュすることを示します。  |

これらのパラメータを設定する例。

```Plain

# Data Cache を有効にします。
block_cache_enable = true  

# ディスクパスを設定します。BE マシンに 2 つのディスクが装備されていると仮定します。
block_cache_disk_path = /home/disk1/sr/dla_cache_data/;/home/disk2/sr/dla_cache_data/

# block_cache_mem_size を 2 GB に設定します。
block_cache_mem_size = 2147483648

# block_cache_disk_size を 1.2 TB に設定します。
block_cache_disk_size = 1288490188800
```

## クエリがデータキャッシュにヒットしたかどうかを確認する

クエリがデータキャッシュにヒットしたかどうかを確認するには、クエリプロファイルの次のメトリクスを分析します。

- `BlockCacheReadBytes`: StarRocks がメモリとディスクから直接読み取ったデータの量。
- `BlockCacheWriteBytes`: 外部ストレージシステムから StarRocks のメモリとディスクにロードされたデータの量。
- `BytesRead`: 読み取られたデータの総量。StarRocks が外部ストレージシステム、メモリ、およびディスクから読み取ったデータを含みます。

例 1: この例では、StarRocks は外部ストレージシステムから大量のデータ (7.65 GB) を読み取り、メモリとディスクからはわずかなデータ (518.73 MB) を読み取ります。これは、データキャッシュがほとんどヒットしなかったことを意味します。

```Plain
 - Table: lineorder
 - BlockCacheReadBytes: 518.73 MB
   - __MAX_OF_BlockCacheReadBytes: 4.73 MB
   - __MIN_OF_BlockCacheReadBytes: 16.00 KB
 - BlockCacheReadCounter: 684
   - __MAX_OF_BlockCacheReadCounter: 4
   - __MIN_OF_BlockCacheReadCounter: 0
 - BlockCacheReadTimer: 737.357us
 - BlockCacheWriteBytes: 7.65 GB
   - __MAX_OF_BlockCacheWriteBytes: 64.39 MB
   - __MIN_OF_BlockCacheWriteBytes: 0.00 
 - BlockCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_BlockCacheWriteCounter: 65
   - __MIN_OF_BlockCacheWriteCounter: 0
 - BlockCacheWriteTimer: 23.467ms
   - __MAX_OF_BlockCacheWriteTimer: 62.280ms
   - __MIN_OF_BlockCacheWriteTimer: 0ns
 - BufferUnplugCount: 15
   - __MAX_OF_BufferUnplugCount: 2
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 7.65 GB
   - __MAX_OF_BytesRead: 64.39 MB
   - __MIN_OF_BytesRead: 0.00
```

例 2: この例では、StarRocks はデータキャッシュから大量のデータ (46.08 GB) を読み取り、外部ストレージシステムからはデータを読み取りません。これは、StarRocks がデータキャッシュからのみデータを読み取ったことを意味します。

```Plain
Table: lineitem
- BlockCacheReadBytes: 46.08 GB
 - __MAX_OF_BlockCacheReadBytes: 194.99 MB
 - __MIN_OF_BlockCacheReadBytes: 81.25 MB
- BlockCacheReadCounter: 72.237K (72237)
 - __MAX_OF_BlockCacheReadCounter: 299
 - __MIN_OF_BlockCacheReadCounter: 118
- BlockCacheReadTimer: 856.481ms
 - __MAX_OF_BlockCacheReadTimer: 1s547ms
 - __MIN_OF_BlockCacheReadTimer: 261.824ms
- BlockCacheWriteBytes: 0.00 
- BlockCacheWriteCounter: 0
- BlockCacheWriteTimer: 0ns
- BufferUnplugCount: 1.231K (1231)
 - __MAX_OF_BufferUnplugCount: 81
 - __MIN_OF_BufferUnplugCount: 35
- BytesRead: 46.08 GB
 - __MAX_OF_BytesRead: 194.99 MB
 - __MIN_OF_BytesRead: 81.25 MB
```