---
displayed_sidebar: docs
description: "StarRocks のメモリ分類、管理戦略、jemalloc 統計、メモリ制限設定について説明します。"
sidebar_position: 40
products: [oss, enterprise, anywhere]
---

# メモリ管理

このセクションでは、メモリの分類と StarRocks におけるメモリ管理の方法を簡単に紹介します。

## メモリの分類

説明:

|   Metric  | Name | Description |
| --- | --- | --- |
|  process   |  BE の使用メモリ合計  | |
|  `query_pool`   |   データクエリによるメモリ使用量  | 実行レイヤーとストレージレイヤーの2つの部分から成ります。|
|  load   |  データロードによるメモリ使用量    | 一般的に MemTable|
|  metadata   |   メタデータメモリ | S Schema、Tablet メタデータ、RowSet メタデータ、Column メタデータ、ColumnReader、IndexReader |
|  compaction   |   マルチバージョンメモリコンパクション  |  データインポート完了後に発生するコンパクション |
|  clone  |   クローンメモリ  | Tablet クローンタスクが使用するメモリ |
|  page_cache   |   BE の独自の PageCache   | v2.4 以降、デフォルトで有効です。BE 設定ファイルで `disable_storage_page_cache` を `true` に設定すると無効にできます |

## メモリ関連の設定

* **BE 設定**

| Name | Default| Description|
| --- | --- | --- |
| vector_chunk_size | 4096 | チャンク行数 |
| mem_limit | 90% | BE プロセスメモリの上限。パーセンテージ（"80%"）または物理的な制限（"100G"）として設定できます。デフォルト値はサーバーメモリサイズの90%です。BE は `mem_limit` の90%をプロセスメモリトラッカーのメモリ制限として使用するため、デフォルトではサーバーメモリサイズの81%になります。同じサーバーで他のメモリ集約型サービスと一緒に StarRocks をデプロイしたい場合、このパラメータを設定する必要があります。 |
| disable_storage_page_cache | false | PageCache を無効にするかどうかを制御するブール値。PageCache が有効な場合、StarRocks は最近スキャンされたデータをキャッシュします。PageCache は、類似のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。`true` は PageCache を無効にすることを示します。この項目は `storage_page_cache_limit` と一緒に使用し、十分なメモリリソースと多くのデータスキャンがあるシナリオでクエリパフォーマンスを加速できます。この項目のデフォルト値は StarRocks v2.4 以降 `true` から `false` に変更されました。 |
| write_buffer_size | 104857600 |  単一の MemTable の容量制限で、これを超えるとディスクスワイプが行われます。 |
| load_process_max_memory_limit_bytes | 107374182400 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの上限。その値は `BE プロセスのメモリ上限 * load_process_max_memory_limit_percent / 100`（BE プロセスのメモリ上限は `mem_limit` の 90%）と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。  |
| load_process_max_memory_limit_percent | 30 | BE ノード上のすべてのロードプロセスが占有できるメモリリソースの最大パーセンテージ。その値は `BE プロセスのメモリ上限 * load_process_max_memory_limit_percent / 100`（BE プロセスのメモリ上限は `mem_limit` の 90%）と `load_process_max_memory_limit_bytes` の小さい方です。このしきい値を超えると、フラッシュとバックプレッシャーがトリガーされます。 |
| max_compaction_concurrency | -1 | コンパクション（Base Compaction と Cumulative Compaction の両方）の最大同時実行数。値が -1 の場合、同時実行数に制限はありません。 |
| cumulative_compaction_check_interval_seconds | 1 | コンパクションチェックの間隔|

* **セッション変数**

| Name| Default| Description|
| --- | --- | --- |
| query_mem_limit| 0| 各 BE ノードでのクエリのメモリ制限 |
| load_mem_limit | 0| 各 BE ノードでの単一のインポートタスクのメモリ制限。値が 0 の場合、タスクごとの制限は設定されず、BE ノード上のすべてのロードプロセスのメモリ制限（`load_process_max_memory_limit_percent` および `load_process_max_memory_limit_bytes` を参照）のみが適用されます。|

## メモリ使用量の確認

* **`mem_tracker`**

~~~ bash
//全体のメモリ統計を表示
<http://be_ip:be_http_port/mem_tracker>

// 詳細なメモリ統計を表示
<http://be_ip:be_http_port/mem_tracker?type=query_pool&upper_level=3>
~~~

* **jemalloc**

~~~ bash
<http://be_ip:be_http_port/memz>
~~~

`/memz` ページには、BE プロセスのメモリ制限、現在のプロセスメモリ使用量、jemalloc アロケータ統計、および Update Manager のメモリ統計が表示されます。jemalloc セクションの `allocated`、`active`、`resident`、`mapped`、`retained`、`metadata` などの項目は、オブジェクトが実際に使用しているメモリとアロケータが保持しているメモリを区別するために役立ちます。

アロケータが保持しているメモリは、オブジェクトが現在使用しているメモリより大きくなる場合があります。`/mem_tracker` と `/memz` を併用して、StarRocks のメモリ Tracker とプロセスレベルのアロケータ統計を比較してください。

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
~~~

metrics の値は10秒ごとに更新されます。古いバージョンで一部のメモリ統計を監視することが可能です。
