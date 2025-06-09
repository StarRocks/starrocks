---
displayed_sidebar: docs
---

# StarRocks version 3.2

## 3.2.4

リリース日: 2024年3月12日

### 新機能

- クラウドネイティブの主キーテーブルが共有データクラスタでサイズ階層型の Compaction をサポートし、書き込み I/O の増幅を削減します。 [#41034](https://github.com/StarRocks/starrocks/pull/41034)
- 日付関数 `milliseconds_diff` を追加しました。 [#38171](https://github.com/StarRocks/starrocks/pull/38171)
- セッションが属する catalog を指定するセッション変数 `catalog` を追加しました。 [#41329](https://github.com/StarRocks/starrocks/pull/41329)
- [ヒントでのユーザー定義変数の設定](https://docs.starrocks.io/docs/administration/Query_planning/#user-defined-variable-hint) をサポートします。 [#40746](https://github.com/StarRocks/starrocks/pull/40746)
- Hive catalogs での CREATE TABLE LIKE をサポートします。 [#37685](https://github.com/StarRocks/starrocks/pull/37685)
- パーティションの詳細なメタデータを記録するビュー `information_schema.partitions_meta` を追加しました。 [#39265](https://github.com/StarRocks/starrocks/pull/39265)
- StarRocks のメモリ使用量を記録するビュー `sys.fe_memory_usage` を追加しました。 [#40464](https://github.com/StarRocks/starrocks/pull/40464)

### 動作の変更

- `cbo_decimal_cast_string_strict` は、CBO が DECIMAL 型から STRING 型にデータを変換する方法を制御します。デフォルト値 `true` は、v2.5.x 以降のバージョンで組み込まれたロジックが優先され、システムが厳密な変換を実施することを示します（つまり、生成された文字列を切り捨て、スケール長に基づいて 0 を埋めます）。DECIMAL 型は以前のバージョンでは厳密に埋められておらず、DECIMAL 型と STRING 型を比較すると異なる結果を引き起こす可能性があります。 [#40619](https://github.com/StarRocks/starrocks/pull/40619)
- Iceberg Catalog パラメータ `enable_iceberg_metadata_cache` のデフォルト値が `false` に変更されました。v3.2.1 から v3.2.3 までは、このパラメータは使用されるメタストアサービスに関係なくデフォルトで `true` に設定されていました。v3.2.4 以降では、Iceberg クラスタが AWS Glue をメタストアとして使用する場合、このパラメータは引き続きデフォルトで `true` です。しかし、Iceberg クラスタが Hive メタストアなどの他のメタストアサービスを使用する場合、このパラメータはデフォルトで `false` です。 [#41826](https://github.com/StarRocks/starrocks/pull/41826)
- マテリアライズドビューをリフレッシュできるユーザーが `root` ユーザーからマテリアライズドビューを作成したユーザーに変更されました。この変更は既存のマテリアライズドビューには影響しません。 [#40670](https://github.com/StarRocks/starrocks/pull/40670)
- デフォルトでは、定数型と文字列型の列を比較する際、StarRocks はそれらを文字列として比較します。ユーザーはセッション変数 `cbo_eq_base_type` を使用して比較に使用するルールを調整できます。たとえば、ユーザーが `cbo_eq_base_type` を `decimal` に設定すると、StarRocks は列を数値として比較します。 [#40619](https://github.com/StarRocks/starrocks/pull/40619)

### 改善点

- 共有データ StarRocks クラスタは、S3 互換オブジェクトストレージシステムのためのパーティション化されたプレフィックス機能をサポートします。この機能が有効になると、StarRocks はデータをバケットの下にある複数の均一にプレフィックスされたパーティション（サブパス）に保存します。これにより、S3 互換オブジェクトストレージでのデータファイルの読み書き効率が向上します。 [#41627](https://github.com/StarRocks/starrocks/pull/41627)
- StarRocks は、パラメータ `s3_compatible_fs_list` を使用して AWS SDK 経由でアクセスできる S3 互換オブジェクトストレージを指定し、パラメータ `fallback_to_hadoop_fs_list` を使用して HDFS スキーマ経由でアクセスが必要な非 S3 互換オブジェクトストレージを指定することをサポートします（この方法にはベンダー提供の JAR パッケージの使用が必要です）。 [#41123](https://github.com/StarRocks/starrocks/pull/41123)
- Trino との互換性を最適化しました。次の Trino 関数の構文変換をサポートします: current_catalog, current_schema, to_char, from_hex, to_date, to_timestamp, および index。 [#41217](https://github.com/StarRocks/starrocks/pull/41217) [#41319](https://github.com/StarRocks/starrocks/pull/41319) [#40803](https://github.com/StarRocks/starrocks/pull/40803)
- マテリアライズドビューのクエリの書き換えロジックを最適化しました。StarRocks は、ビューに基づいて作成されたマテリアライズドビューを使用してクエリを書き換えることができます。 [#42173](https://github.com/StarRocks/starrocks/pull/42173)
- STRING 型から DATETIME 型への変換効率を 35% から 40% 向上させました。 [#41464](https://github.com/StarRocks/starrocks/pull/41464)
- 集計テーブルの BITMAP 型の列の `agg_type` を `replace_if_not_null` に設定することで、テーブルの一部の列のみを更新することをサポートします。 [#42034](https://github.com/StarRocks/starrocks/pull/42034)
- 小さな ORC ファイルをロードする際の Broker Load のパフォーマンスを向上させました。 [#41765](https://github.com/StarRocks/starrocks/pull/41765)
- 行と列のハイブリッドストレージを持つテーブルが Schema Change をサポートします。 [#40851](https://github.com/StarRocks/starrocks/pull/40851)
- 行と列のハイブリッドストレージを持つテーブルが BITMAP、HLL、JSON、ARRAY、MAP、STRUCT などの複雑な型をサポートします。 [#41476](https://github.com/StarRocks/starrocks/pull/41476)
- 統計およびマテリアライズドビューに関連するログデータを記録する新しい内部 SQL ログファイルを追加しました。 [#40453](https://github.com/StarRocks/starrocks/pull/40453)

### バグ修正

次の問題を修正しました:

- Hive ビューの作成時にクエリされたテーブルまたはビューの名前やエイリアスに一貫性のない文字ケースが割り当てられると "Analyze Error" が発生します。 [#40921](https://github.com/StarRocks/starrocks/pull/40921)
- 主キーテーブルに永続性インデックスが作成されると、I/O 使用量が上限に達します。 [#39959](https://github.com/StarRocks/starrocks/pull/39959)
- 共有データクラスタでは、主キーインデックスディレクトリが5時間ごとに削除されます。 [#40745](https://github.com/StarRocks/starrocks/pull/40745)
- ユーザーが手動で ALTER TABLE COMPACT を実行した後、Compaction 操作のメモリ使用量統計が異常になります。 [#41150](https://github.com/StarRocks/starrocks/pull/41150)
- 主キーテーブルの Publish フェーズのリトライがハングする可能性があります。 [#39890](https://github.com/StarRocks/starrocks/pull/39890)

## 3.2.3

リリース日: 2024年2月8日

### 新機能

- [プレビュー] テーブルの行と列のハイブリッドストレージをサポートします。これにより、主キーテーブルに対する高コンカレンシー、低レイテンシーのポイントルックアップや部分データ更新のパフォーマンスが向上します。現在、この機能は ALTER TABLE による変更、ソートキーの変更、列モードでの部分更新をサポートしていません。
- 非同期マテリアライズドビューのバックアップと復元をサポートします。
- Broker Load が JSON 型データのロードをサポートします。
- ビューに基づいて作成された非同期マテリアライズドビューを使用したクエリの書き換えをサポートします。ビューに対するクエリは、そのビューに基づいて作成されたマテリアライズドビューに基づいて書き換えられます。
- CREATE OR REPLACE PIPE をサポートします。 [#37658](https://github.com/StarRocks/starrocks/pull/37658)

### 動作の変更

- セッション変数 `enable_strict_order_by` を追加しました。この変数がデフォルト値 `TRUE` に設定されている場合、次のようなクエリパターンに対してエラーが報告されます: クエリの異なる式で重複したエイリアスが使用され、このエイリアスが ORDER BY のソートフィールドでもある場合、例えば `select distinct t1.* from tbl1 t1 order by t1.k1;`。このロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、これらのクエリは有効な SQL クエリとして処理されます。 [#37910](https://github.com/StarRocks/starrocks/pull/37910)
- セッション変数 `enable_materialized_view_for_insert` を追加しました。この変数は、マテリアライズドビューが INSERT INTO SELECT ステートメントでクエリを書き換えるかどうかを制御します。デフォルト値は `false` です。 [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- 単一のクエリが Pipeline フレームワーク内で実行される場合、そのメモリ制限は `exec_mem_limit` ではなく変数 `query_mem_limit` によって制約されます。`query_mem_limit` の値を `0` に設定すると、制限がないことを示します。 [#34120](https://github.com/StarRocks/starrocks/pull/34120)

### パラメータの変更

- HTTP サーバーが HTTP リクエストを処理するためのスレッド数を指定する FE 設定項目 `http_worker_threads_num` を追加しました。デフォルト値は `0` です。このパラメータの値が負の値または `0` に設定されている場合、実際のスレッド数は CPU コア数の 2 倍です。 [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- 共有データ StarRocks クラスタ内の主キーテーブルの Compaction タスクで許可される最大入力 rowset 数を制御する BE 設定項目 `lake_pk_compaction_max_input_rowsets` を追加しました。これにより、Compaction タスクのリソース消費が最適化されます。 [#39611](https://github.com/StarRocks/starrocks/pull/39611)
- Hive テーブルまたは Iceberg テーブルにデータを書き込む、または Files() を使用してデータをエクスポートする際に使用される圧縮アルゴリズムを指定するセッション変数 `connector_sink_compression_codec` を追加しました。有効なアルゴリズムには GZIP、BROTLI、ZSTD、LZ4 があります。 [#37912](https://github.com/StarRocks/starrocks/pull/37912)
- FE 設定項目 `routine_load_unstable_threshold_second` を追加しました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- ディスク上の Compaction の最大同時実行数を設定する BE 設定項目 `pindex_major_compaction_limit_per_disk` を追加しました。これにより、Compaction によるディスク間の I/O の不均一性の問題が解決されます。この問題は、特定のディスクの I/O が過度に高くなる原因となる可能性があります。デフォルト値は `1` です。 [#36681](https://github.com/StarRocks/starrocks/pull/36681)
- BE 設定項目 `enable_lazy_delta_column_compaction` を追加しました。デフォルト値は `true` で、StarRocks がデルタ列に対して頻繁な Compaction 操作を行わないことを示します。 [#36654](https://github.com/StarRocks/starrocks/pull/36654)
- マテリアライズドビューが作成された後に即座にリフレッシュするかどうかを指定する FE 設定項目 `default_mv_refresh_immediate` を追加しました。デフォルト値は `true` です。 [#37093](https://github.com/StarRocks/starrocks/pull/37093)
- FE 設定項目 `default_mv_refresh_partition_num` のデフォルト値を `1` に変更しました。これは、マテリアライズドビューのリフレッシュ中に複数のパーティションを更新する必要がある場合、タスクがバッチで分割され、1 回に 1 つのパーティションのみがリフレッシュされることを示します。これにより、各リフレッシュ中のリソース消費が削減されます。 [#36560](https://github.com/StarRocks/starrocks/pull/36560)
- BE/CN 設定項目 `starlet_use_star_cache` のデフォルト値を `true` に変更しました。これは、共有データクラスタでデータキャッシュがデフォルトで有効になっていることを示します。アップグレード前に BE/CN 設定項目 `starlet_cache_evict_high_water` を手動で `X` に設定している場合、BE/CN 設定項目 `starlet_star_cache_disk_size_percent` を `(1.0 - X) * 100` に設定する必要があります。たとえば、アップグレード前に `starlet_cache_evict_high_water` を `0.3` に設定している場合、`starlet_star_cache_disk_size_percent` を `70` に設定する必要があります。これにより、ファイルデータキャッシュとデータキャッシュがディスク容量制限を超えないようにします。 [#38200](https://github.com/StarRocks/starrocks/pull/38200)

### 改善点

- Apache Iceberg テーブルの TIMESTAMP パーティションフィールドをサポートするために、日付形式 `yyyy-MM-ddTHH:mm` および `yyyy-MM-dd HH:mm` を追加しました。 [#39986](https://github.com/StarRocks/starrocks/pull/39986)
- モニタリング API にデータキャッシュ関連のメトリクスを追加しました。 [#40375](https://github.com/StarRocks/starrocks/pull/40375)
- BE ログの印刷を最適化し、不要なログが多すぎるのを防ぎました。 [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- ビュー `information_schema.be_tablets` にフィールド `storage_medium` を追加しました。 [#37070](https://github.com/StarRocks/starrocks/pull/37070)
- 複数のサブクエリで `SET_VAR` をサポートします。 [#36871](https://github.com/StarRocks/starrocks/pull/36871)
- SHOW ROUTINE LOAD の返り値に新しいフィールド `LatestSourcePosition` を追加し、Kafka トピックの各パーティション内の最新メッセージの位置を記録し、データロードの遅延を確認するのに役立ちます。 [#38298](https://github.com/StarRocks/starrocks/pull/38298)
- WHERE 句内の LIKE 演算子の右側の文字列に `%` または `_` が含まれていない場合、LIKE 演算子は `=` 演算子に変換されます。 [#37515](https://github.com/StarRocks/starrocks/pull/37515)
- ゴミ箱ファイルのデフォルトの保持期間が元の 3 日から 1 日に変更されました。 [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- パーティション変換を使用した Iceberg テーブルからの統計収集をサポートします。 [#39907](https://github.com/StarRocks/starrocks/pull/39907)
- Routine Load のスケジューリングポリシーが最適化され、遅いタスクが他の通常のタスクの実行を妨げないようにしました。 [#37638](https://github.com/StarRocks/starrocks/pull/37638)

### バグ修正

次の問題を修正しました:

- ANALYZE TABLE の実行が時折スタックします。 [#36836](https://github.com/StarRocks/starrocks/pull/36836)
- 特定の状況で、PageCache によるメモリ消費が BE 動的パラメータ `storage_page_cache_limit` で指定されたしきい値を超えます。 [#37740](https://github.com/StarRocks/starrocks/pull/37740)
- Hive catalogs の Hive メタデータが、Hive テーブルに新しいフィールドが追加されたときに自動的に更新されません。 [#37549](https://github.com/StarRocks/starrocks/pull/37549)
- 一部のケースで、`bitmap_to_string` がデータ型のオーバーフローにより不正確な結果を返すことがあります。 [#37405](https://github.com/StarRocks/starrocks/pull/37405)
- `SELECT ... FROM ... INTO OUTFILE` を実行してデータを CSV ファイルにエクスポートすると、FROM 句に複数の定数が含まれている場合、「列の数が一致しない」というエラーが報告されます。 [#38045](https://github.com/StarRocks/starrocks/pull/38045)
- 一部のケースで、テーブル内の半構造化データをクエリすると、BEs がクラッシュすることがあります。 [#40208](https://github.com/StarRocks/starrocks/pull/40208)

## 3.2.2

リリース日: 2023年12月30日

### バグ修正

次の問題を修正しました:

- StarRocks を v3.1.2 以前から v3.2 にアップグレードすると、FEs が再起動に失敗することがあります。 [#38172](https://github.com/StarRocks/starrocks/pull/38172)

## 3.2.1

リリース日: 2023年12月21日

### 新機能

#### データレイク分析

- Java Native Interface (JNI) を通じて、Avro、SequenceFile、および RCFile 形式の Hive Catalog テーブルとファイル外部テーブルの読み取りをサポートします。

#### マテリアライズドビュー

- データベース `sys` にビュー `object_dependencies` を追加しました。非同期マテリアライズドビューの系統情報を含みます。 [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- WHERE 句を使用した同期マテリアライズドビューの作成をサポートします。
- Iceberg catalogs に基づいて作成された非同期マテリアライズドビューのパーティションレベルの増分リフレッシュをサポートします。
- [プレビュー] Paimon catalog のテーブルに基づいて作成された非同期マテリアライズドビューのパーティションレベルのリフレッシュをサポートします。

#### クエリと SQL 関数

- プリペアドステートメントをサポートします。これにより、高コンカレンシーのポイントルックアップクエリの処理パフォーマンスが向上します。また、SQL インジェクションを効果的に防ぎます。
- 次の Bitmap 関数をサポートします: [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/)、[bitmap_from_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/)、および [bitmap_to_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。
- Array 関数 [array_unique_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_unique_agg/) をサポートします。

#### モニタリングとアラート

- 許可される rowset の最大数を設定するための新しいメトリック `max_tablet_rowset_num` を追加しました。このメトリックは、可能な Compaction の問題を検出し、「バージョンが多すぎる」というエラーの発生を減少させます。 [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### パラメータの変更

- 新しい BE 設定項目 `enable_stream_load_verbose_log` を追加しました。デフォルト値は `false` です。このパラメータを `true` に設定すると、StarRocks は Stream Load ジョブの HTTP リクエストとレスポンスを記録し、トラブルシューティングを容易にします。 [#36113](https://github.com/StarRocks/starrocks/pull/36113)

### 改善点

- JDK8 のデフォルト GC アルゴリズムを G1 にアップグレードしました。 [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- セッション変数 [sql_mode](https://docs.starrocks.io/docs/reference/System_variable/#sql_mode) に新しい値オプション `GROUP_CONCAT_LEGACY` を追加し、v2.5 より前のバージョンでの [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数の実装ロジックとの互換性を提供します。 [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- [AWS S3 における Broker Load ジョブ](https://docs.starrocks.io/docs/loading/s3/) の認証情報 `aws.s3.access_key` および `aws.s3.access_secret` が監査ログで非表示になります。 [#36571](https://github.com/StarRocks/starrocks/pull/36571)
- `information_schema` データベースの `be_tablets` ビューに新しいフィールド `INDEX_DISK` を追加し、永続性インデックスのディスク使用量（バイト単位）を記録します。 [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントの返り値に新しいフィールド `OtherMsg` を追加し、最後に失敗したタスクに関する情報を表示します。 [#35806](https://github.com/StarRocks/starrocks/pull/35806)

### バグ修正

次の問題を修正しました:

- データ破損が発生した場合にユーザーが永続性インデックスを作成すると、BEs がクラッシュします。 [#30841](https://github.com/StarRocks/starrocks/pull/30841)
- [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) 関数が時折 BEs をクラッシュさせます。 [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- DISTINCT ウィンドウ演算子のプッシュダウン機能が有効になっている場合、ウィンドウ関数によって計算された列の複雑な式に対して SELECT DISTINCT 操作が実行されるとエラーが報告されます。 [#36357](https://github.com/StarRocks/starrocks/pull/36357)
- 一部の S3 互換オブジェクトストレージが重複したファイルを返し、BEs がクラッシュします。 [#36103](https://github.com/StarRocks/starrocks/pull/36103)

## 3.2.0

リリース日: 2023年12月1日

### 新機能

#### 共有データクラスタ

- [主キーテーブル](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/) のインデックスをローカルディスクに永続化することをサポートします。
- 複数のローカルディスク間でのデータキャッシュの均等な分散をサポートします。

#### マテリアライズドビュー

**非同期マテリアライズドビュー**

- クエリダンプファイルに非同期マテリアライズドビューの情報を含めることができます。
- 非同期マテリアライズドビューのリフレッシュタスクに対して、デフォルトでディスクへのスピル機能が有効になっており、メモリ消費を削減します。

#### データレイク分析

- [Hive catalogs](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) でのデータベースおよび管理テーブルの作成と削除をサポートし、INSERT または INSERT OVERWRITE を使用して Hive の管理テーブルにデータをエクスポートすることをサポートします。
- [Unified Catalog](https://docs.starrocks.io/docs/data_source/catalog/unified_catalog/) をサポートし、ユーザーが共通のメタストア（Hive メタストアや AWS Glue など）を共有する異なるテーブル形式（Hive、Iceberg、Hudi、Delta Lake）にアクセスできるようにします。
- ANALYZE TABLE を使用して Hive および Iceberg テーブルの統計を収集し、StarRocks に統計を保存することで、クエリプランの最適化を促進し、後続のクエリを加速します。
- 外部テーブルのための Information Schema をサポートし、外部システム（BI ツールなど）と StarRocks の間のインタラクションをより便利にします。

#### ストレージエンジン、データ取り込み、およびエクスポート

- テーブル関数 [FILES()](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/) を使用したロードの次の機能を追加しました:
  - Azure または GCP からの Parquet および ORC 形式データのロード。
  - ファイルパスからキー/値ペアの値を抽出し、列の値として使用するためのパラメータ `columns_from_path` の使用。
  - ARRAY、JSON、MAP、STRUCT などの複雑なデータ型のロード。
- StarRocks から AWS S3 または HDFS に保存された Parquet 形式のファイルにデータをアンロードすることをサポートします。詳細な手順については、[INSERT INTO FILES を使用したデータのアンロード](https://docs.starrocks.io/docs/unloading/unload_using_insert_into_files/) を参照してください。
- 既存のテーブルで使用されるテーブル構造とデータ分布戦略の[手動最適化](https://docs.starrocks.io/docs/table_design/Data_distribution#optimize-data-distribution-after-table-creation-since-32) をサポートし、クエリとロードのパフォーマンスを最適化します。新しいバケットキー、バケット数、またはソートキーをテーブルに設定できます。特定のパーティションに対して異なるバケット数を設定することもできます。
- [AWS S3](https://docs.starrocks.io/docs/loading/s3/#use-pipe) または [HDFS](https://docs.starrocks.io/docs/loading/hdfs_load/#use-pipe) を使用した PIPE メソッドによる継続的なデータロードをサポートします。
  - PIPE がリモートストレージディレクトリで新しいデータまたは変更を検出すると、変更されたデータを StarRocks の宛先テーブルに自動的にロードできます。データをロードする際、PIPE は大きなロードタスクを小さなシリアル化されたタスクに自動的に分割し、大規模なデータ取り込みシナリオでの安定性を向上させ、エラーリトライのコストを削減します。

#### クエリ

- [HTTP SQL API](https://docs.starrocks.io/docs/reference/HTTP_API/SQL/) をサポートし、ユーザーが HTTP 経由で StarRocks データにアクセスし、SELECT、SHOW、EXPLAIN、または KILL 操作を実行できるようにします。
- Runtime Profile およびテキストベースの Profile 分析コマンド（SHOW PROFILELIST、ANALYZE PROFILE、EXPLAIN ANALYZE）をサポートし、ユーザーが MySQL クライアントを介して直接プロファイルを分析できるようにし、ボトルネックの特定と最適化の機会の発見を促進します。

#### SQL リファレンス

次の関数を追加しました:

- 文字列関数: substring_index, url_extract_parameter, url_encode, url_decode, および translate
- 日付関数: dayofweek_iso, week_iso, quarters_add, quarters_sub, milliseconds_add, milliseconds_sub, date_diff, jodatime_format, str_to_jodatime, to_iso8601, to_tera_date, および to_tera_timestamp
- パターンマッチング関数: regexp_extract_all
- ハッシュ関数: xx_hash3_64
- 集計関数: approx_top_k
- ウィンドウ関数: cume_dist, percent_rank および session_number
- ユーティリティ関数: get_query_profile および is_role_in_session

#### 特権とセキュリティ

StarRocks は [Apache Ranger](https://docs.starrocks.io/docs/administration/ranger_plugin/) を通じてアクセス制御をサポートし、より高いレベルのデータセキュリティを提供し、外部データソースの既存のサービスの再利用を可能にします。Apache Ranger と統合することで、StarRocks は次のアクセス制御方法を有効にします:

- StarRocks 内の内部テーブル、外部テーブル、またはその他のオブジェクトにアクセスする際、Ranger で StarRocks サービスに対して設定されたアクセスポリシーに基づいてアクセス制御を実施できます。
- 外部 catalog にアクセスする際、元のデータソース（Hive サービスなど）の対応する Ranger サービスを利用してアクセス制御を行うこともできます（現在、Hive へのデータエクスポートのアクセス制御はまだサポートされていません）。

詳細については、[Apache Ranger を使用した権限管理](https://docs.starrocks.io/docs/administration/ranger_plugin/) を参照してください。

### 改善点

#### データレイク分析

- ORC リーダーを最適化しました:
  - ORC カラムリーダーを最適化し、VARCHAR および CHAR データの読み取りパフォーマンスがほぼ 2 倍に向上しました。
  - Zlib 圧縮形式の ORC ファイルの解凍パフォーマンスを最適化しました。
- Parquet リーダーを最適化しました:
  - 適応型 I/O マージをサポートし、フィルタリング効果に基づいて述語のあるカラムとないカラムの適応的なマージを可能にし、I/O を削減します。
  - より高速な述語書き換えのために Dict フィルタを最適化しました。STRUCT サブカラムをサポートし、必要に応じて辞書カラムのデコードを行います。
  - Dict デコードのパフォーマンスを最適化しました。
  - 後期実体化のパフォーマンスを最適化しました。
  - ファイルフッターのキャッシュをサポートし、繰り返しの計算オーバーヘッドを回避します。
  - lzo 圧縮形式の Parquet ファイルの解凍をサポートします。
- CSV リーダーを最適化しました:
  - リーダーのパフォーマンスを最適化しました。
  - Snappy および lzo 圧縮形式の CSV ファイルの解凍をサポートします。
- カウント計算のパフォーマンスを最適化しました。
- Iceberg Catalog 機能を最適化しました:
  - クエリを加速するために、Manifest ファイルからカラム統計を収集することをサポートします。
  - クエリを加速するために、Puffin ファイルから NDV（異なる値の数）を収集することをサポートします。
  - プルーニングをサポートします。
  - 大量のメタデータや高いクエリコンカレンシーのシナリオでの安定性を向上させるために、Iceberg メタデータのメモリ消費を削減しました。

#### マテリアライズドビュー

**非同期マテリアライズドビュー**

- ビューまたはマテリアライズドビューに基づいて作成された非同期マテリアライズドビューのスキーマ変更が発生した場合、自動リフレッシュをサポートします。
- データの一貫性:
  - 非同期マテリアライズドビュー作成のためのプロパティ `query_rewrite_consistency` を追加しました。このプロパティは、一貫性チェックに基づいたクエリの書き換えルールを定義します。
  - 外部 catalog に基づいて作成された非同期マテリアライズドビューのためのプロパティ `force_external_table_query_rewrite` を追加しました。このプロパティは、外部 catalog に基づいて作成された非同期マテリアライズドビューのための強制クエリ書き換えを許可するかどうかを定義します。
  - 詳細情報については、[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/) を参照してください。
- マテリアライズドビューのパーティションキーの一貫性チェックを追加しました。
  - ユーザーが PARTITION BY 式を含むウィンドウ関数を使用して非同期マテリアライズドビューを作成する場合、ウィンドウ関数のパーティション列はマテリアライズドビューのパーティション列と一致する必要があります。

#### ストレージエンジン、データ取り込み、およびエクスポート

- 主キーテーブルの永続性インデックスを最適化し、メモリ使用ロジックを改善し、I/O の読み取りおよび書き込みの増幅を削減しました。 [#24875](https://github.com/StarRocks/starrocks/pull/24875)  [#27577](https://github.com/StarRocks/starrocks/pull/27577)  [#28769](https://github.com/StarRocks/starrocks/pull/28769)
- 主キーテーブルのローカルディスク間でのデータ再配布をサポートします。
- パーティションテーブルは、パーティションの時間範囲とクールダウン時間に基づいて自動クールダウンをサポートします。元のクールダウンロジックと比較して、パーティションレベルでのホットおよびコールドデータ管理がより便利になります。詳細については、[初期記憶媒体、自動記憶クールダウン時間、レプリカ数を指定する](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE#specify-initial-storage-medium-automatic-storage-cooldown-time-replica-number) を参照してください。
- 主キーテーブルにデータを書き込むロードジョブの Publish フェーズが非同期モードから同期モードに変更されました。そのため、ロードジョブが終了した直後にデータをクエリできます。詳細については、[enable_sync_publish](https://docs.starrocks.io/docs/administration/FE_configuration#enable_sync_publish) を参照してください。
- 高速スキーマ進化をサポートし、テーブルプロパティ [`fast_schema_evolution`](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE#set-fast-schema-evolution) によって制御されます。この機能が有効になると、カラムの追加または削除の実行効率が大幅に向上します。このモードはデフォルトで無効になっています（デフォルト値は `false`）。ALTER TABLE を使用して既存のテーブルのこのプロパティを変更することはできません。
- [ランダムバケット法を使用して作成された Duplicate Key テーブル](https://docs.starrocks.io/docs/table_design/Data_distribution#set-the-number-of-buckets) のタブレットの作成数をクラスタ情報とデータのサイズに応じて動的に調整することをサポートします。

#### クエリ

- StarRocks の Metabase および Superset との互換性を最適化しました。外部 catalog との統合をサポートします。

#### SQL リファレンス

- [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/) がキーワード DISTINCT をサポートします。
- INSERT、UPDATE、および DELETE 操作が `SET_VAR` をサポートします。 [#35283](https://github.com/StarRocks/starrocks/pull/35283)

#### その他

- DECIMAL 型のオーバーフローを処理するルールを設定するためのセッション変数 `large_decimal_underlying_type = "panic"|"double"|"decimal"` を追加しました。`panic` は即座にエラーを返すことを示し、`double` はデータを DOUBLE 型に変換することを示し、`decimal` はデータを DECIMAL(38,s) に変換することを示します。

### 開発者ツール

- 非同期マテリアライズドビューのための Trace Query Profile をサポートし、その透明な書き換えを分析するのに使用できます。

### 動作の変更

更新予定です。

### パラメータの変更

#### FE パラメータ

- 次の FE 設定項目を追加しました:
  - `catalog_metadata_cache_size`
  - `enable_backup_materialized_view`
  - `enable_colocate_mv_index`
  - `enable_fast_schema_evolution`
  - `json_file_size_limit`
  - `lake_enable_ingest_slowdown`
  - `lake_ingest_slowdown_threshold`
  - `lake_ingest_slowdown_ratio`
  - `lake_compaction_score_upper_bound`
  - `mv_auto_analyze_async`
  - `primary_key_disk_schedule_time`
  - `statistic_auto_collect_small_table_rows`
  - `stream_load_task_keep_max_num`
  - `stream_load_task_keep_max_second`
- FE 設定項目 `enable_pipeline_load` を削除しました。
- デフォルト値の変更:
  - `enable_sync_publish` のデフォルト値を `false` から `true` に変更しました。
  - `enable_persistent_index_by_default` のデフォルト値を `false` から `true` に変更しました。

#### BE パラメータ

- データキャッシュ関連の設定変更。
  - `block_cache_enable` を置き換える `datacache_enable` を追加しました。
  - `block_cache_mem_size` を置き換える `datacache_mem_size` を追加しました。
  - `block_cache_disk_size` を置き換える `datacache_disk_size` を追加しました。
  - `block_cache_disk_path` を置き換える `datacache_disk_path` を追加しました。
  - `block_cache_meta_path` を置き換える `datacache_meta_path` を追加しました。
  - `block_cache_block_size` を置き換える `datacache_block_size` を追加しました。
  - `block_cache_checksum_enable` を置き換える `datacache_checksum_enable` を追加しました。
  - `block_cache_direct_io_enable` を置き換える `datacache_direct_io_enable` を追加しました。
  - `block_cache_max_concurrent_inserts` を置き換える `datacache_max_concurrent_inserts` を追加しました。
  - `datacache_max_flying_memory_mb` を追加しました。
  - `block_cache_engine` を置き換える `datacache_engine` を追加しました。
  - `block_cache_max_parcel_memory_mb` を削除しました。
  - `block_cache_report_stats` を削除しました。
  - `block_cache_lru_insertion_point` を削除しました。

  Block Cache を Data Cache に名前を変更した後、StarRocks は `datacache` プレフィックスを持つ新しい BE パラメータセットを導入し、元の `block_cache` プレフィックスを持つパラメータを置き換えました。v3.2 にアップグレードした後、元のパラメータは引き続き有効です。一度有効にすると、新しいパラメータが元のパラメータを上書きします。新旧のパラメータの混在使用はサポートされていません。将来的に、StarRocks は `block_cache` プレフィックスを持つ元のパラメータを廃止する予定であるため、`datacache` プレフィックスを持つ新しいパラメータを使用することをお勧めします。

- 次の BE 設定項目を追加しました:
  - `spill_max_dir_bytes_ratio`
  - `streaming_agg_limited_memory_size`
  - `streaming_agg_chunk_buffer_size`
- 次の BE 設定項目を削除しました:
  - 動的パラメータ `tc_use_memory_min`
  - 動的パラメータ `tc_free_memory_rate`
  - 動的パラメータ `tc_gc_period`
  - 静的パラメータ `tc_max_total_thread_cache_byte`
- デフォルト値の変更:
  - `disable_column_pool` のデフォルト値を `false` から `true` に変更しました。
  - `thrift_port` のデフォルト値を `9060` から `0` に変更しました。
  - `enable_load_colocate_mv` のデフォルト値を `false` から `true` に変更しました。
  - `enable_pindex_minor_compaction` のデフォルト値を `false` から `true` に変更しました。

#### システム変数

- 次のセッション変数を追加しました:
  - `enable_per_bucket_optimize`
  - `enable_write_hive_external_table`
  - `hive_temp_staging_dir`
  - `spill_revocable_max_bytes`
  - `thrift_plan_protocol`
- 次のセッション変数を削除しました:
  - `enable_pipeline_query_statistic`
  - `enable_deliver_batch_fragments`
- 次のセッション変数をリネームしました:
  - `enable_scan_block_cache` は `enable_scan_datacache` にリネームされました。
  - `enable_populate_block_cache` は `enable_populate_datacache` にリネームされました。

#### 予約キーワード

予約キーワード `OPTIMIZE` および `PREPARE` を追加しました。

### バグ修正

次の問題を修正しました:

- libcurl が呼び出されると BEs がクラッシュします。 [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 指定されたタブレットバージョンがガーベジコレクションによって処理されるため、Schema Change が非常に長い時間がかかると失敗することがあります。 [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- ファイル外部テーブルを介して MinIO の Parquet ファイルにアクセスできませんでした。 [#29873](https://github.com/StarRocks/starrocks/pull/29873)
- `information_schema.columns` に ARRAY、MAP、および STRUCT 型のカラムが正しく表示されません。 [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- Broker Load を介してデータをロードする際に特定のパス形式が使用されるとエラーが報告されます: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`。 [#32720](https://github.com/StarRocks/starrocks/pull/32720)
- `information_schema.columns` ビューで BINARY または VARBINARY データ型の `DATA_TYPE` および `COLUMN_TYPE` が `unknown` と表示されます。 [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- 多くのユニオン、式、および SELECT カラムを含む複雑なクエリが、FE ノード内の帯域幅または CPU 使用率の急激な増加を引き起こす可能性があります。
- 非同期マテリアライズドビューのリフレッシュが時折デッドロックに遭遇することがあります。 [#35736](https://github.com/StarRocks/starrocks/pull/35736)

### アップグレードノート

- **ランダムバケット法** の最適化はデフォルトで無効になっています。有効にするには、テーブル作成時にプロパティ `bucket_size` を追加する必要があります。これにより、クラスタ情報とロードされたデータのサイズに基づいて、システムがタブレットの数を動的に調整できるようになります。この最適化を有効にした場合、クラスタを v3.1 以前にロールバックする必要がある場合は、この最適化を有効にしたテーブルを削除し、メタデータチェックポイントを手動で実行する必要があります（`ALTER SYSTEM CREATE IMAGE` を実行）。そうしないと、ロールバックが失敗します。
- v3.2.0 以降、StarRocks は非パイプラインクエリを無効にしました。そのため、クラスタを v3.2 にアップグレードする前に、FE 設定ファイル **fe.conf** に `enable_pipeline_engine=true` という設定を追加して、パイプラインエンジンをグローバルに有効にする必要があります。そうしないと、非パイプラインクエリに対してエラーが発生します。