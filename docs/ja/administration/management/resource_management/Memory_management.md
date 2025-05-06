---
displayed_sidebar: docs
sidebar_position: 40
---

# メモリ管理

このセクションでは、メモリの分類と StarRocks のメモリ管理方法について簡単に紹介します。

## メモリの分類

説明:

|   Metric  | Name | Description |
| --- | --- | --- |
|  process   |  Total memory used of BE  | |
|  query\_pool   |   Memory used by data querying  | 実行レイヤーとストレージレイヤーの2つの部分から構成されます。|
|  load   |  Memory used by data loading    | 一般的に MemTable|
|  table_meta   |   Metadata memory | S Schema, Tablet metadata, RowSet metadata, Column metadata, ColumnReader, IndexReader |
|  compaction   |   Multi-version memory compaction  | データインポート完了後に発生する compaction |
|  snapshot  |   Snapshot memory  | 一般的にクローン用で、メモリ使用量は少ない |
|  column_pool   |    Column pool memory   | カラムキャッシュの解放を要求してカラムを加速 |
|  page_cache   |   BE's own PageCache   | デフォルトはオフで、ユーザーは BE ファイルを変更してオンにできます |

## メモリ関連の設定

* **BE 設定**

| Name | Default| Description|
| --- | --- | --- |
| vector_chunk_size | 4096 | チャンク行数 |
| mem_limit | 90% | BE プロセスメモリの上限。パーセンテージ（"80%"）または物理的な制限（"100G"）として設定できます。デフォルトのハードリミットはサーバーメモリサイズの90%、ソフトリミットは80%です。同じサーバーで他のメモリ集約型サービスと一緒に StarRocks をデプロイしたい場合、このパラメータを設定する必要があります。 |
| disable_storage_page_cache | false | PageCache を無効にするかどうかを制御するブール値。PageCache が有効な場合、StarRocks は最近スキャンされたデータをキャッシュします。PageCache は、類似のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。`true` は PageCache を無効にすることを示します。この項目は `storage_page_cache_limit` と一緒に使用し、十分なメモリリソースと多くのデータスキャンがあるシナリオでクエリパフォーマンスを加速できます。この項目のデフォルト値は StarRocks v2.4 以降 `true` から `false` に変更されました。 |
| write_buffer_size | 104857600 | 単一の MemTable の容量制限で、これを超えるとディスクスワイプが行われます。 |
| load_process_max_memory_limit_bytes | 107374182400 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの上限。その値は `mem_limit * load_process_max_memory_limit_percent / 100` と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。 |
| load_process_max_memory_limit_percent | 30 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの最大パーセンテージ。その値は `mem_limit * load_process_max_memory_limit_percent / 100` と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。 |
| default_load_mem_limit | 2147483648 | 単一のインポートインスタンスの受信側でメモリ制限に達した場合、ディスクスワイプがトリガーされます。これを有効にするには、セッション変数 `load_mem_limit` を使用して変更する必要があります。このパラメータは、イベントベースの Compaction フレームワークが有効な場合に変更可能です。|
| max_compaction_concurrency | -1 | Compaction（Base Compaction と Cumulative Compaction の両方）の最大同時実行数。値が -1 の場合、同時実行数に制限はありません。 |
| cumulative_compaction_check_interval_seconds | 1 | Compaction チェックの間隔|

* **セッション変数**

| Name| Default| Description|
| --- | --- | --- |
| query_mem_limit| 0| 各 BE ノードでのクエリのメモリ制限 |
| load_mem_limit | 0| 単一のインポートタスクのメモリ制限。値が 0 の場合、`exec_mem_limit` が適用されます|

## メモリ使用量の確認

* **mem\_tracker**

~~~ bash
//全体のメモリ統計を表示
<http://be_ip:be_http_port/mem_tracker>

// 詳細なメモリ統計を表示
<http://be_ip:be_http_port/mem_tracker?type=query_pool&upper_level=3>
~~~

* **tcmalloc**

~~~ bash
<http://be_ip:be_http_port/memz>
~~~

~~~plain text
------------------------------------------------
MALLOC:      777276768 (  741.3 MiB) Bytes in use by application
MALLOC: +   8851890176 ( 8441.8 MiB) Bytes in page heap freelist
MALLOC: +    143722232 (  137.1 MiB) Bytes in central cache freelist
MALLOC: +     21869824 (   20.9 MiB) Bytes in transfer cache freelist
MALLOC: +    832509608 (  793.9 MiB) Bytes in thread cache freelists
MALLOC: +     58195968 (   55.5 MiB) Bytes in malloc metadata
MALLOC:   ------------
MALLOC: =  10685464576 (10190.5 MiB) Actual memory used (physical + swap)
MALLOC: +  25231564800 (24062.7 MiB) Bytes released to OS (aka unmapped)
MALLOC:   ------------
MALLOC: =  35917029376 (34253.1 MiB) Virtual address space used
MALLOC:
MALLOC:         112388              Spans in use
MALLOC:            335              Thread heaps in use
MALLOC:           8192              Tcmalloc page size
------------------------------------------------
Call ReleaseFreeMemory() to release freelist memory to the OS (via madvise()).
Bytes released to the OS take up virtual address space but no physical memory.
~~~

この方法でクエリされたメモリは正確です。ただし、StarRocks では予約されているが使用されていないメモリもあります。TcMalloc は予約されたメモリをカウントし、使用されたメモリではありません。

ここで `Bytes in use by application` は現在使用中のメモリを指します。

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
curl -XGET http://be_ip:be_http_port/metrics | grep 'column_pool'
~~~

metrics の値は10秒ごとに更新されます。古いバージョンで一部のメモリ統計を監視することが可能です。