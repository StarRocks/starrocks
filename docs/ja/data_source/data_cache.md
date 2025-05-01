---
displayed_sidebar: docs
---

# Data Cache

このトピックでは、Data Cache の動作原理と、外部データのクエリパフォーマンスを向上させるために Data Cache を有効にする方法について説明します。

データレイク分析において、StarRocks は OLAP エンジンとして、HDFS や Amazon S3 などの外部ストレージシステムに保存されたデータファイルをスキャンします。スキャンするファイル数が増えると、I/O オーバーヘッドも増加します。さらに、いくつかのアドホックなシナリオでは、同じデータへの頻繁なアクセスが I/O オーバーヘッドを倍増させます。

これらのシナリオでクエリパフォーマンスを最適化するために、StarRocks 2.5 は Data Cache 機能を提供します。この機能は、外部ストレージシステムのデータを事前定義されたポリシーに基づいて複数のブロックに分割し、StarRocks のバックエンド (BEs) にデータをキャッシュします。これにより、各アクセス要求ごとに外部システムからデータを取得する必要がなくなり、ホットデータのクエリと分析が高速化されます。Data Cache は、外部カタログや外部テーブル (JDBC 互換データベースの外部テーブルを除く) を使用して外部ストレージシステムからデータをクエリする場合にのみ機能します。StarRocks の内部テーブルをクエリする場合には機能しません。

## 動作原理

StarRocks は、外部ストレージシステムのデータを同じサイズの複数のブロック (デフォルトでは 1 MB) に分割し、BEs にデータをキャッシュします。ブロックはデータキャッシュの最小単位です。

たとえば、Amazon S3 から 128 MB の Parquet ファイルをクエリする場合、StarRocks はファイルを 128 ブロック (各ブロック 1 MB、推奨設定) に分割します。ブロックは [0, 1 MB), [1 MB, 2 MB), [2 MB, 3 MB) ... [127 MB, 128 MB) です。StarRocks は各ブロックにグローバルに一意の ID を割り当て、これをキャッシュキーと呼びます。キャッシュキーは次の 3 つの部分で構成されます。

```Plain
hash(filename) + filesize + blockId
```

次の表は、各部分の説明を示しています。

| **コンポーネント項目** | **説明**                                              |
| ------------------ | ------------------------------------------------------------ |
| filename           | データファイルの名前。                                   |
| filesize           | データファイルのサイズ。デフォルトは 1 MB。                  |
| blockId            | データファイルを分割する際に StarRocks がブロックに割り当てる ID。同じデータファイル内では一意ですが、StarRocks クラスター内では一意ではありません。 |

クエリが [1 MB, 2 MB) ブロックにヒットした場合、StarRocks は次の操作を行います。

1. ブロックがキャッシュに存在するかどうかを確認します。
2. ブロックが存在する場合、StarRocks はキャッシュからブロックを読み取ります。ブロックが存在しない場合、StarRocks は Amazon S3 からブロックを読み取り、BE にキャッシュします。

Data Cache が有効になると、StarRocks は外部ストレージシステムから読み取ったデータブロックをキャッシュします。これらのデータブロックをキャッシュしたくない場合は、次のコマンドを実行します。

```SQL
SET enable_populate_block_cache = false;
```

`enable_populate_block_cache` の詳細については、[System variables](../reference/System_variable.md) を参照してください。

## ブロックのストレージメディア

デフォルトでは、StarRocks は BE マシンのメモリを使用してブロックをキャッシュします。また、メモリとディスクの両方をブロックのハイブリッドストレージメディアとして使用することもサポートしています。BE マシンに NVMe ドライブや SSD などのディスクが装備されている場合、メモリとディスクの両方を使用してブロックをキャッシュできます。BE マシンに Amazon EBS などのクラウドストレージを使用する場合は、メモリのみを使用してブロックをキャッシュすることをお勧めします。

## キャッシュ置換ポリシー

StarRocks は [least recently used](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) (LRU) ポリシーを使用してデータをキャッシュおよび破棄します。

- StarRocks はメモリからデータを読み取り、メモリにデータが見つからない場合はディスクから読み取ります。ディスクから読み取ったデータはメモリにロードされます。
- メモリから削除されたデータはディスクに書き込まれます。ディスクから削除されたデータは破棄されます。

## Data Cache を有効にする

Data Cache はデフォルトでは無効になっています。この機能を有効にするには、StarRocks クラスター内の FEs と BEs を設定します。

### FEs の設定

FEs に対して Data Cache を有効にするには、次のいずれかの方法を使用します。

- 要件に基づいて特定のセッションに対して Data Cache を有効にします。

  ```SQL
  SET enable_scan_block_cache = true;
  ```

- すべてのアクティブなセッションに対して Data Cache を有効にします。

  ```SQL
  SET GLOBAL enable_scan_block_cache = true;
  ```

### BEs の設定

各 BE の **conf/be.conf** ファイルに次のパラメータを追加します。その後、各 BE を再起動して設定を有効にします。

| **パラメータ**          | **説明**                                              |
| ---------------------- | ------------------------------------------------------------ |
| block_cache_enable     | Data Cache が有効かどうか。<ul><li>`true`: Data Cache が有効です。</li><li>`false`: Data Cache が無効です。このパラメータのデフォルト値は `false` です。</li></ul>Data Cache を有効にするには、このパラメータの値を `true` に設定します。 |
| block_cache_disk_path  | ディスクのパス。このパラメータに設定するパスの数は、BE マシンのディスクの数と同じであることをお勧めします。複数のパスはセミコロン (;) で区切る必要があります。このパラメータを追加すると、StarRocks は自動的に **cachelib_data** という名前のファイルを作成してブロックをキャッシュします。 |
| block_cache_meta_path  | ブロックメタデータのストレージパス。ストレージパスをカスタマイズできます。メタデータを **$STARROCKS_HOME** パスの下に保存することをお勧めします。 |
| block_cache_mem_size   | メモリにキャッシュできるデータの最大量。単位: バイト。デフォルト値は `2147483648` (2 GB) です。このパラメータの値を少なくとも 20 GB に設定することをお勧めします。Data Cache が有効になった後にディスクから大量のデータを読み取る場合は、値を増やすことを検討してください。 |
| block_cache_disk_size  | 単一のディスクにキャッシュできるデータの最大量。たとえば、`block_cache_disk_path` パラメータに 2 つのディスクパスを設定し、`block_cache_disk_size` パラメータの値を `21474836480` (20 GB) に設定した場合、これらの 2 つのディスクに最大 40 GB のデータをキャッシュできます。デフォルト値は `0` で、メモリのみがデータのキャッシュに使用されることを示します。単位: バイト。 |

これらのパラメータを設定する例。

```Plain

# Data Cache を有効にします。
block_cache_enable = true  

# ディスクパスを設定します。BE マシンに 2 つのディスクが装備されていると仮定します。
block_cache_disk_path = /home/disk1/sr/dla_cache_data/;/home/disk2/sr/dla_cache_data/

# メタデータのストレージパスを設定します。
block_cache_meta_path = /home/disk1/sr/dla_cache_meta/ 

# block_cache_mem_size を 2 GB に設定します。
block_cache_mem_size = 2147483648

# block_cache_disk_size を 1.2 TB に設定します。
block_cache_disk_size = 1288490188800
```

## クエリがデータキャッシュにヒットしたかどうかを確認する

クエリがデータキャッシュにヒットしたかどうかを確認するには、クエリプロファイルの次のメトリクスを分析します。

- `BlockCacheReadBytes`: StarRocks がメモリやディスクから直接読み取ったデータの量。
- `BlockCacheWriteBytes`: 外部ストレージシステムから StarRocks のメモリやディスクにロードされたデータの量。
- `BytesRead`: 読み取られたデータの総量。StarRocks が外部ストレージシステム、メモリ、ディスクから読み取ったデータを含みます。

例 1: この例では、StarRocks は外部ストレージシステムから大量のデータ (7.65 GB) を読み取り、メモリやディスクからはわずかなデータ (518.73 MB) を読み取っています。これは、データキャッシュがほとんどヒットしなかったことを意味します。

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

例 2: この例では、StarRocks はデータキャッシュから大量のデータ (46.08 GB) を読み取り、外部ストレージシステムからはデータを読み取っていません。これは、StarRocks がデータキャッシュからのみデータを読み取っていることを意味します。

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