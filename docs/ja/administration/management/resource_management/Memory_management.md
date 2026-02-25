---
displayed_sidebar: docs
sidebar_position: 40
---

# メモリ管理

このセクションでは、メモリの分類と StarRocks におけるメモリ管理の方法を簡単に紹介します。

## メモリの分類

説明:

|   Metric  | Name | Description |
| --- | --- | --- |
|  process   |  BE の使用メモリ合計  | |
|  query\_pool   |   データクエリによるメモリ使用量  | 実行レイヤーとストレージレイヤーの2つの部分から成ります。|
|  load   |  データロードによるメモリ使用量    | 一般的に MemTable|
|  table_meta   |   メタデータメモリ | S Schema、Tablet メタデータ、RowSet メタデータ、Column メタデータ、ColumnReader、IndexReader |
|  compaction   |   マルチバージョンメモリコンパクション  |  データインポート完了後に発生するコンパクション |
|  snapshot  |   スナップショットメモリ  | 一般的にクローンに使用され、メモリ使用量は少ない |
|  column_pool   |    カラムプールメモリ   | カラムキャッシュを解放してカラムを加速するためのリクエスト |
|  page_cache   |   BE の独自の PageCache   | デフォルトはオフで、ユーザーは BE ファイルを変更してオンにできます |

## メモリ関連の設定

* **BE 設定**

| Name | Default| Description|
| --- | --- | --- |
| vector_chunk_size | 4096 | チャンク行数 |
| mem_limit | 90% | BE プロセスメモリの上限。パーセンテージ（"80%"）または物理的な制限（"100G"）として設定できます。デフォルトのハードリミットはサーバーメモリサイズの90%、ソフトリミットは80%です。同じサーバーで他のメモリ集約型サービスと一緒に StarRocks をデプロイしたい場合、このパラメータを設定する必要があります。 |
| disable_storage_page_cache | false | PageCache を無効にするかどうかを制御するブール値。PageCache が有効な場合、StarRocks は最近スキャンされたデータをキャッシュします。PageCache は、類似のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。`true` は PageCache を無効にすることを示します。この項目は `storage_page_cache_limit` と一緒に使用し、十分なメモリリソースと多くのデータスキャンがあるシナリオでクエリパフォーマンスを加速できます。この項目のデフォルト値は StarRocks v2.4 以降 `true` から `false` に変更されました。 |
| write_buffer_size | 104857600 |  単一の MemTable の容量制限で、これを超えるとディスクスワイプが行われます。 |
| load_process_max_memory_limit_bytes | 107374182400 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの上限。その値は `mem_limit * load_process_max_memory_limit_percent / 100` と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。  |
| load_process_max_memory_limit_percent | 30 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの最大パーセンテージ。その値は `mem_limit * load_process_max_memory_limit_percent / 100` と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。 |
| default_load_mem_limit | 2147483648 | 単一のインポートインスタンスの受信側でメモリ制限に達した場合、ディスクスワイプがトリガーされます。これを有効にするには、セッション変数 `load_mem_limit` と一緒に変更する必要があります。このパラメータは、イベントベースのコンパクションフレームワークが有効な場合に変更可能です。|
| max_compaction_concurrency | -1 | コンパクション（Base Compaction と Cumulative Compaction の両方）の最大同時実行数。値が -1 の場合、同時実行数に制限はありません。 |
| cumulative_compaction_check_interval_seconds | 1 | コンパクションチェックの間隔|

* **セッション変数**

| Name| Default| Description|
| --- | --- | --- |
| query_mem_limit| 0| 各 BE ノードでのクエリのメモリ制限 |
| load_mem_limit | 0| 単一のインポートタスクのメモリ制限。値が 0 の場合、`exec_mem_limit` が使用されます|

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
MALLOC:      777276768 (  741.3 MiB) アプリケーションによって使用されているバイト数
MALLOC: +   8851890176 ( 8441.8 MiB) ページヒープフリーリストのバイト数
MALLOC: +    143722232 (  137.1 MiB) セントラルキャッシュフリーリストのバイト数
MALLOC: +     21869824 (   20.9 MiB) トランスファーキャッシュフリーリストのバイト数
MALLOC: +    832509608 (  793.9 MiB) スレッドキャッシュフリーリストのバイト数
MALLOC: +     58195968 (   55.5 MiB) malloc メタデータのバイト数
MALLOC:   ------------
MALLOC: =  10685464576 (10190.5 MiB) 実際に使用されているメモリ（物理 + スワップ）
MALLOC: +  25231564800 (24062.7 MiB) OS に解放されたバイト数（アンマップされたもの）
MALLOC:   ------------
MALLOC: =  35917029376 (34253.1 MiB) 使用されている仮想アドレス空間
MALLOC:
MALLOC:         112388              使用中のスパン数
MALLOC:            335              使用中のスレッドヒープ数
MALLOC:           8192              Tcmalloc ページサイズ
------------------------------------------------
ReleaseFreeMemory() を呼び出して、フリーリストメモリを OS に解放します（madvise() 経由）。
OS に解放されたバイト数は仮想アドレス空間を占有しますが、物理メモリは占有しません。
~~~

この方法でクエリされたメモリは正確です。ただし、StarRocks の一部のメモリは予約されているが使用されていない場合があります。TcMalloc は予約されたメモリをカウントし、使用されているメモリをカウントしません。

ここで `Bytes in use by application` は現在使用されているメモリを指します。

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
curl -XGET http://be_ip:be_http_port/metrics | grep 'column_pool'
~~~

metrics の値は10秒ごとに更新されます。古いバージョンで一部のメモリ統計を監視することが可能です。