---
displayed_sidebar: docs
description: "StarRocks のメモリ分類、管理戦略、jemalloc 統計、メモリ制限設定について説明します。"
sidebar_position: 40
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
curl -XGET http://be_ip:be_http_port/metrics | grep 'column_pool'
~~~

metrics の値は10秒ごとに更新されます。古いバージョンで一部のメモリ統計を監視することが可能です。

## Lake メタデータメモリ {#lake-metadata-memory}

共有データクラスタの `lake_metadata_cache_limit` が制限するのは、各 CN のメタデータ LRU キャッシュだけです。`metadata` トラッカーが報告する総メモリの上限ではありません。クエリとコンパクションは、LRU から削除された Segment を保持したり、キャッシュに追加せずにメタデータをロードしたりできます。オブジェクトは最後の所有者が解放するまで存続するため、キャッシュ上限を下げても実行中のタスクが保持するメモリは制限できません。

CN の `/metrics` は `dump_metrics_with_bvar` に依存せず、次の Gauge を公開します。

- `starrocks_be_lake_metacache_usage_bytes`: LRU キーとハンドルを含むキャッシュ計上量。
- `starrocks_be_lake_metacache_capacity_bytes`: 現在の実効キャッシュ容量。
- `starrocks_be_lake_metacache_payload_bytes{type="segment"}`: キャッシュ内 Segment のペイロード計上量。他の `type` 値は `tablet_metadata`, `txn_log`, `schema`, `delvec`, `combined_txn_log`, `bundled_metadata_marker` です。

`/mem_tracker`（`?type=metadata` を含む）は、同じ値を独立した診断テーブルに表示します。これらは重複する観測値であり、総メモリに加算するトラッカーの子ではありません。

CN ごとに、キャッシュ内 Segment で説明できない `metadata` の部分を次の式で推定できます。

```promql
clamp_min(
  starrocks_be_metadata_mem_bytes
  - ignoring(type) starrocks_be_lake_metacache_payload_bytes{type="segment"},
  0
)
```

これは削除済み Segment の正確なバイト数ではありません。他のメタデータ、計上範囲の違い、Segment のキャッシュ計上量をバッチ更新する際の一時的な差も含み、各 Gauge のサンプリングも独立しています。アイドル時の基準値と比較し、クエリやコンパクションの実行中と終了後の変化を確認してください。キャッシュが容量に達している間も推定値が増える場合、Segment キャッシュ外のメタデータが増加しています。入力 Segment を保持するコンパクションについては、`starrocks_be_lake_compaction_held_segment_bytes` も参照できます。metacache の総量には異なる範囲のオブジェクトやオーバーヘッドも含まれるため、それを `metadata` から直接差し引かないでください。

クエリが Segment を開く際の割り当ては allocation hook によって instance/query トラッカーにも計上されます。そのため `metadata` と `query_pool` は重複し、**加算できません**。process トラッカーは同じ割り当てを 1 回だけ数えます。metadata が独立した診断ビューであることは、これらの割り当てがクエリのメモリ制限やアドミッションの計上から除外されることを意味しません。
