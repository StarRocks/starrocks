---
displayed_sidebar: docs
---

# StarRocks バージョン 3.0

## 3.0.9

リリース日: 2024年1月2日

### 新機能

- [percentile_disc](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_disc/) 関数を追加しました。 [#36352](https://github.com/StarRocks/starrocks/pull/36352)
- `max_tablet_rowset_num` という新しいメトリックを追加し、許可される最大の rowset 数を設定できるようにしました。このメトリックは、可能な Compaction の問題を検出し、「バージョンが多すぎる」というエラーの発生を減少させます。 [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### 改善点

- セッション変数 [sql_mode](https://docs.starrocks.io/docs/sql-reference/System_variable/#sql_mode) に新しい値オプション `GROUP_CONCAT_LEGACY` を追加し、v2.5 より前のバージョンでの [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数の実装ロジックとの互換性を提供します。 [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- JDK を使用する場合、デフォルトの GC アルゴリズムは G1 です。 [#37386](https://github.com/StarRocks/starrocks/pull/37386)
- `information_schema` データベースの `be_tablets` ビューに新しいフィールド `INDEX_DISK` を追加し、永続性インデックスのディスク使用量（バイト単位）を記録します。 [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- MySQL 外部テーブルおよび JDBC カタログ内の外部テーブルに対するクエリで、WHERE 句にキーワードを含めることができます。 [#35917](https://github.com/StarRocks/starrocks/pull/35917)
- 自動パーティション化されたテーブルの指定されたパーティションへの更新をサポートします。指定されたパーティションが存在しない場合、エラーが返されます。 [#34777](https://github.com/StarRocks/starrocks/pull/34777)
- [SHOW DATA](https://docs.starrocks.io/docs/sql-reference/sql-statements/Database/SHOW_DATA/) ステートメントで返される主キーテーブルのサイズには、**.cols** ファイル（部分列の更新および生成列に関連するファイル）および永続性インデックスファイルのサイズが含まれます。 [#34898](https://github.com/StarRocks/starrocks/pull/34898)
- 主キーテーブルのすべての rowset に対して Compaction が実行されるときに、永続性インデックスの更新パフォーマンスを最適化し、ディスク読み取り I/O を削減しました。 [#36819](https://github.com/StarRocks/starrocks/pull/36819)
- WHERE 句内の LIKE 演算子の右側の文字列に `%` または `_` が含まれていない場合、LIKE 演算子は `=` 演算子に変換されます。 [#37515](https://github.com/StarRocks/starrocks/pull/37515)
- 主キーテーブルの Compaction スコアを計算するためのロジックを最適化し、他の3つのテーブルタイプとより一貫した範囲で主キーテーブルの Compaction スコアを整合させました。 [#36534](https://github.com/StarRocks/starrocks/pull/36534)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK/) ステートメントで返される結果には、各パーティションからの消費メッセージのタイムスタンプが含まれるようになりました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- ビットマップ関連の操作のパフォーマンスを最適化しました。以下を含みます:
  - ネストされたループジョインを最適化しました。 [#340804](https://github.com/StarRocks/starrocks/pull/34804) [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - `bitmap_xor` 関数を最適化しました。 [#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - ビットマップのパフォーマンスを最適化し、メモリ消費を削減するために Copy on Write をサポートします。 [#34047](https://github.com/StarRocks/starrocks/pull/34047)

### 動作変更

- セッション変数 `enable_materialized_view_for_insert` を追加し、INSERT INTO SELECT ステートメントでマテリアライズドビューがクエリを書き換えるかどうかを制御します。デフォルト値は `false` です。 [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- FE の設定項目 `enable_new_publish_mechanism` を動的パラメータから静的パラメータに変更しました。パラメータ設定を変更した後、FE を再起動する必要があります。 [#35338](https://github.com/StarRocks/starrocks/pull/35338)
- ゴミファイルのデフォルトの保持期間を元の3日から1日に変更しました。 [#37113](https://github.com/StarRocks/starrocks/pull/37113)

### パラメータ変更

#### セッション変数

- セッション変数 `cbo_decimal_cast_string_strict` を追加し、CBO が DECIMAL 型から STRING 型にデータを変換する方法を制御します。この変数が `true` に設定されている場合、v2.5.x 以降のバージョンで組み込まれたロジックが優先され、システムは厳密な変換を実施します（つまり、生成された文字列を切り捨て、スケール長に基づいて 0 を埋めます）。この変数が `false` に設定されている場合、v2.5.x より前のバージョンで組み込まれたロジックが優先され、システムはすべての有効な数字を処理して文字列を生成します。デフォルト値は `true` です。 [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- セッション変数 `transaction_read_only` と `tx_read_only` を追加し、トランザクションアクセスモードを指定します。これらは MySQL バージョン 5.7.20 以降と互換性があります。 [#37249](https://github.com/StarRocks/starrocks/pull/37249)

#### FE パラメータ

- FE の設定項目 `routine_load_unstable_threshold_second` を追加しました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- FE の設定項目 `http_worker_threads_num` を追加し、HTTP サーバーが HTTP リクエストを処理するためのスレッド数を指定します。デフォルト値は `0` です。このパラメータの値が負の値または 0 に設定されている場合、実際のスレッド数は CPU コア数の2倍です。 [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- FE の設定項目 `default_mv_refresh_immediate` を追加し、マテリアライズドビューが作成された後にすぐにリフレッシュするかどうかを指定します。デフォルト値は `true` です。 [#37093](https://github.com/StarRocks/starrocks/pull/37093)

#### BE パラメータ

- BE の設定項目 `enable_stream_load_verbose_log` を追加しました。デフォルト値は `false` です。このパラメータが `true` に設定されている場合、StarRocks は Stream Load ジョブの HTTP リクエストとレスポンスを記録し、トラブルシューティングを容易にします。 [#36113](https://github.com/StarRocks/starrocks/pull/36113)
- BE の設定項目 `pindex_major_compaction_limit_per_disk` を追加し、ディスク上の Compaction の最大並行性を設定します。これは、Compaction によるディスク間の不均一な I/O の問題に対処します。この問題は、特定のディスクに対して過度に高い I/O を引き起こす可能性があります。デフォルト値は `1` です。 [#37694](https://github.com/StarRocks/starrocks/pull/37694)
- オブジェクトストレージへの接続のタイムアウト期間を指定するための BE 設定項目を追加しました:
  - `object_storage_connect_timeout_ms`: オブジェクトストレージとのソケット接続を確立するためのタイムアウト期間。デフォルト値は `-1` で、SDK 設定のデフォルトのタイムアウト期間を使用することを意味します。
  - `object_storage_request_timeout_ms`: オブジェクトストレージとの HTTP 接続を確立するためのタイムアウト期間。デフォルト値は `-1` で、SDK 設定のデフォルトのタイムアウト期間を使用することを意味します。

### バグ修正

以下の問題を修正しました:

- 一部のケースで、Catalog を使用して ORC 外部テーブルを読み取るときに BEs がクラッシュする可能性があります。 [#27971](https://github.com/StarRocks/starrocks/pull/27971)
- データ破損が発生した場合に永続性インデックスを作成すると BEs がクラッシュします。 [#30841](https://github.com/StarRocks/starrocks/pull/30841)
- ビットマップインデックスが追加された後、BEs が時折クラッシュします。 [#26463](https://github.com/StarRocks/starrocks/pull/26463)
- レプリカ操作の再生に失敗すると FEs がクラッシュする可能性があります。 [#32295](https://github.com/StarRocks/starrocks/pull/32295)
- FE パラメータ `recover_with_empty_tablet` を `true` に設定すると FEs がクラッシュする可能性があります。 [#33071](https://github.com/StarRocks/starrocks/pull/33071)
- ハッシュジョイン中にクエリが失敗し、BEs がクラッシュします。 [#32219](https://github.com/StarRocks/starrocks/pull/32219)
- StarRocks 共有なしクラスタで、Iceberg または Hive テーブルに対するクエリが BEs をクラッシュさせる可能性があります。 [#34682](https://github.com/StarRocks/starrocks/pull/34682)
- クエリに対して「get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction」というエラーが返されます。 [#33246](https://github.com/StarRocks/starrocks/pull/33246)
- `show proc '/statistic'` を実行するとデッドロックが発生する可能性があります。 [#34237](https://github.com/StarRocks/starrocks/pull/34237/files)
- FE 設定項目 `enable_collect_query_detail_info` を `true` に設定すると FE のパフォーマンスが急落します。 [#35945](https://github.com/StarRocks/starrocks/pull/35945)
- 大量のデータが永続性インデックスが有効な主キーテーブルにロードされるとエラーが発生する可能性があります。 [#34352](https://github.com/StarRocks/starrocks/pull/34352)
- StarRocks が v2.4 以前から後のバージョンにアップグレードされると、Compaction スコアが予期せず上昇することがあります。 [#34618](https://github.com/StarRocks/starrocks/pull/34618)
- MariaDB ODBC データベースドライバを使用して `INFORMATION_SCHEMA` をクエリすると、`schemata` ビューで返される `CATALOG_NAME` 列には `null` 値しか含まれません。 [#34627](https://github.com/StarRocks/starrocks/pull/34627)
- 異常なデータがロードされると FEs がクラッシュし、再起動できません。 [#34590](https://github.com/StarRocks/starrocks/pull/34590)
- Stream Load ジョブが **PREPARED** 状態のときにスキーマ変更が実行されると、ジョブによってロードされるソースデータの一部が失われます。 [#34381](https://github.com/StarRocks/starrocks/pull/34381)
- HDFS ストレージパスの末尾に2つ以上のスラッシュ（`/`）を含めると、HDFS からのデータのバックアップと復元が失敗します。 [#34601](https://github.com/StarRocks/starrocks/pull/34601)
- ALTER TABLE ステートメントを使用して追加された `partition_live_number` プロパティが効果を発揮しません。 [#34842](https://github.com/StarRocks/starrocks/pull/34842)
- [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) 関数が時折 BEs をクラッシュさせることがあります。 [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- マテリアライズドビューをリフレッシュするとデッドロックが発生する可能性があります。 [#35736](https://github.com/StarRocks/starrocks/pull/35736)
- グローバルランタイムフィルタが特定のシナリオで BEs をクラッシュさせる可能性があります。 [#35776](https://github.com/StarRocks/starrocks/pull/35776)
- 一部のケースで、`bitmap_to_string` がデータ型のオーバーフローにより誤った結果を返すことがあります。 [#37405](https://github.com/StarRocks/starrocks/pull/37405)

## 3.0.8

リリース日: 2023年11月17日

### 改善点

- システムデータベース `INFORMATION_SCHEMA` の `COLUMNS` ビューで ARRAY、MAP、STRUCT 列を表示できるようになりました。 [#33431](https://github.com/StarRocks/starrocks/pull/33431)

### バグ修正

以下の問題を修正しました:

- `show proc '/current_queries';` が実行されている間にクエリが開始されると、BEs がクラッシュすることがあります。 [#34316](https://github.com/StarRocks/starrocks/pull/34316)
- ソートキーが指定された主キーテーブルに高頻度でデータが連続してロードされると、Compaction が失敗することがあります。 [#26486](https://github.com/StarRocks/starrocks/pull/26486)
- Broker Load ジョブでフィルタリング条件が指定されている場合、特定の状況でデータロード中に BEs がクラッシュすることがあります。 [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- SHOW GRANTS を実行すると不明なエラーが報告されます。 [#30100](https://github.com/StarRocks/starrocks/pull/30100)
- `cast()` 関数で指定されたターゲットデータ型が元のデータ型と同じ場合、特定のデータ型で BE がクラッシュすることがあります。 [#31465](https://github.com/StarRocks/starrocks/pull/31465)
- BINARY または VARBINARY データ型の `DATA_TYPE` および `COLUMN_TYPE` が `information_schema.columns` ビューで `unknown` と表示されます。 [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- 永続性インデックスが有効な主キーテーブルに長時間、大量のデータをロードすると BEs がクラッシュすることがあります。 [#33220](https://github.com/StarRocks/starrocks/pull/33220)
- Query Cache が有効な場合、クエリ結果が正しくありません。 [#32778](https://github.com/StarRocks/starrocks/pull/32778)
- クラスタが再起動された後、復元されたテーブルのデータがバックアップ前のデータと一致しないことがあります。 [#33567](https://github.com/StarRocks/starrocks/pull/33567)
- RESTORE が実行され、同時に Compaction が行われると、BEs がクラッシュすることがあります。 [#32902](https://github.com/StarRocks/starrocks/pull/32902)

## 3.0.7

リリース日: 2023年10月18日

### 改善点

- ウィンドウ関数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP が ORDER BY 句とウィンドウ句をサポートするようになりました。 [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- 主キーテーブルにデータを書き込むロードジョブの公開フェーズが非同期モードから同期モードに変更されました。これにより、ロードジョブが終了した直後にロードされたデータをクエリできます。 [#27055](https://github.com/StarRocks/starrocks/pull/27055)
- DECIMAL 型データに対するクエリ中に小数点オーバーフローが発生した場合、NULL ではなくエラーが返されます。 [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- 無効なコメントを含む SQL コマンドを実行すると、MySQL と一致する結果が返されます。 [#30210](https://github.com/StarRocks/starrocks/pull/30210)
- パーティション列が1つだけのレンジパーティション化または式に基づくパーティション化を使用する StarRocks テーブルの場合、パーティション列の式を含む SQL 述語もパーティションプルーニングに使用できます。 [#30421](https://github.com/StarRocks/starrocks/pull/30421)

### バグ修正

以下の問題を修正しました:

- データベースやテーブルを同時に作成および削除すると、特定のケースでテーブルが見つからず、そのテーブルへのデータロードが失敗することがあります。 [#28985](https://github.com/StarRocks/starrocks/pull/28985)
- UDF を使用すると、特定のケースでメモリリークが発生することがあります。 [#29467](https://github.com/StarRocks/starrocks/pull/29467) [#29465](https://github.com/StarRocks/starrocks/pull/29465)
- ORDER BY 句に集計関数が含まれている場合、エラー "java.lang.IllegalStateException: null" が返されます。 [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- Tencent COS に保存されたデータに対して Hive カタログを使用してクエリを実行すると、クエリ結果が正しくありません。 [#30363](https://github.com/StarRocks/starrocks/pull/30363)
- `ARRAY<STRUCT>` 型データの STRUCT の一部のサブフィールドが欠落している場合、クエリ中に欠落したサブフィールドにデフォルト値が埋め込まれるとデータ長が不正確になり、BEs がクラッシュします。
- セキュリティ脆弱性を回避するために Berkeley DB Java Edition のバージョンがアップグレードされました。[#30029](https://github.com/StarRocks/starrocks/pull/30029)
- 主キーテーブルにデータをロードし、同時にトランケート操作とクエリを実行すると、特定のケースでエラー "java.lang.NullPointerException" がスローされます。 [#30573](https://github.com/StarRocks/starrocks/pull/30573)
- スキーマ変更の実行時間が長すぎる場合、指定されたバージョンのタブレットがガベージコレクションされるため、失敗することがあります。 [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- CloudCanal を使用して `NOT NULL` に設定されているがデフォルト値が指定されていないテーブル列にデータをロードすると、エラー "Unsupported dataFormat value is : \N" がスローされます。 [#30799](https://github.com/StarRocks/starrocks/pull/30799)
- StarRocks 共有データクラスタでは、テーブルキーの情報が `information_schema.COLUMNS` に記録されません。その結果、Flink Connector を使用してデータをロードする際に DELETE 操作を実行できません。 [#31458](https://github.com/StarRocks/starrocks/pull/31458)
- アップグレード中に、特定の列の型もアップグレードされる場合（例えば、Decimal 型から Decimal v3 型への変更）、特定の特性を持つテーブルでの Compaction が BEs をクラッシュさせることがあります。 [#31626](https://github.com/StarRocks/starrocks/pull/31626)
- Flink Connector を使用してデータをロードする際に、高度に並行したロードジョブが存在し、HTTP スレッド数とスキャンスレッド数が上限に達している場合、ロードジョブが予期せず中断されます。 [#32251](https://github.com/StarRocks/starrocks/pull/32251)
- libcurl が呼び出されると BEs がクラッシュします。 [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- BITMAP 型の列が主キーテーブルに追加されるとエラーが発生します。 [#31763](https://github.com/StarRocks/starrocks/pull/31763)

## 3.0.6

リリース日: 2023年9月12日

### 動作変更

- [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数を使用する場合、セパレータを宣言するために SEPARATOR キーワードを使用する必要があります。

### 新機能

- 集計関数 [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) が DISTINCT キーワードと ORDER BY 句をサポートするようになりました。 [#28778](https://github.com/StarRocks/starrocks/pull/28778)
- パーティション内のデータを時間の経過とともに自動的にクールダウンできるようになりました。（この機能は [リストパーティション化](https://docs.starrocks.io/docs/table_design/list_partitioning/) ではサポートされていません。） [#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### 改善点

- すべての複合述語および WHERE 句内のすべての式に対して暗黙の変換をサポートします。暗黙の変換を有効または無効にするには、[セッション変数](https://docs.starrocks.io/docs/sql-reference/System_variable/) `enable_strict_type` を使用します。このセッション変数のデフォルト値は `false` です。 [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- 文字列を整数に変換する際の FE と BE のロジックを統一しました。 [#29969](https://github.com/StarRocks/starrocks/pull/29969)

### バグ修正

- `enable_orc_late_materialization` が `true` に設定されている場合、Hive カタログを使用して ORC ファイル内の STRUCT 型データをクエリすると予期しない結果が返されます。 [#27971](https://github.com/StarRocks/starrocks/pull/27971)
- Hive カタログを通じてデータをクエリする際、WHERE 句にパーティション列と OR 演算子が指定されている場合、クエリ結果が正しくありません。 [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- クラウドネイティブテーブルに対して RESTful API アクション `show_data` で返される値が正しくありません。 [#29473](https://github.com/StarRocks/starrocks/pull/29473)
- [共有データクラスタ](https://docs.starrocks.io/docs/deployment/shared_data/azure/) が Azure Blob Storage にデータを保存し、テーブルが作成されると、クラスタがバージョン 3.0 にロールバックされた後、FE が起動に失敗します。 [#29433](https://github.com/StarRocks/starrocks/pull/29433)
- Iceberg カタログ内のテーブルをクエリする際、ユーザーにそのテーブルへの権限が付与されていても、ユーザーに権限がありません。 [#29173](https://github.com/StarRocks/starrocks/pull/29173)
- [SHOW FULL COLUMNS](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SHOW_FULL_COLUMNS/) ステートメントで返される [BITMAP](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/BITMAP/) または [HLL](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/HLL/) データ型の列の `Default` フィールド値が正しくありません。 [#29510](https://github.com/StarRocks/starrocks/pull/29510)
- `ADMIN SET FRONTEND CONFIG` コマンドを使用して FE 動的パラメータ `max_broker_load_job_concurrency` を変更しても効果がありません。
- マテリアライズドビューがリフレッシュされている間にそのリフレッシュ戦略が変更されると、FE が起動に失敗することがあります。 [#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- `select count(distinct(int+double)) from table_name` を実行すると、エラー `unknown error` が返されます。 [#29691](https://github.com/StarRocks/starrocks/pull/29691)
- 主キーテーブルが復元された後、BE が再起動されるとメタデータエラーが発生し、メタデータの不整合が発生します。 [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 3.0.5

リリース日: 2023年8月16日

### 新機能

- 集計関数 [COVAR_SAMP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/)、[COVAR_POP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/)、および [CORR](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/corr/) をサポートします。
- 次の [ウィンドウ関数](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/) をサポートします: COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP。

### 改善点

- エラーメッセージ `xxx too many versions xxx` により多くのプロンプトを追加しました。 [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- 動的パーティション化でパーティション単位を年にすることをさらにサポートします。 [#28386](https://github.com/StarRocks/starrocks/pull/28386)
- テーブル作成時に式に基づくパーティション化が使用され、[INSERT OVERWRITE を使用して特定のパーティションのデータを上書きする](https://docs.starrocks.io/docs/table_design/expression_partitioning#load-data-into-partitions) 場合、パーティションフィールドは大文字小文字を区別しません。 [#28309](https://github.com/StarRocks/starrocks/pull/28309)

### バグ修正

以下の問題を修正しました:

- FE の不正確なテーブルレベルのスキャン統計がテーブルクエリとロードのメトリックを不正確にします。 [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- パーティションテーブルのソートキーが変更されると、クエリ結果が安定しません。 [#27850](https://github.com/StarRocks/starrocks/pull/27850)
- タブレットのバージョン番号がデータが復元された後、BE と FE の間で一致しません。 [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- Colocation テーブルを作成する際にバケット数が指定されていない場合、数は0と推測され、新しいパーティションの追加が失敗します。 [#27086](https://github.com/StarRocks/starrocks/pull/27086)
- INSERT INTO SELECT の SELECT 結果セットが空の場合、SHOW LOAD で返されるロードジョブのステータスは `CANCELED` です。 [#26913](https://github.com/StarRocks/starrocks/pull/26913)
- sub_bitmap 関数の入力値が BITMAP 型でない場合、BEs がクラッシュすることがあります。 [#27982](https://github.com/StarRocks/starrocks/pull/27982)
- AUTO_INCREMENT 列が更新されているときに BEs がクラッシュすることがあります。 [#27199](https://github.com/StarRocks/starrocks/pull/27199)
- マテリアライズドビューの外部結合と反結合の書き換えエラー。 [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- 平均行サイズの不正確な推定が、主キー部分更新が過度に大きなメモリを占有する原因となります。 [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- 非アクティブなマテリアライズドビューをアクティブ化すると FE がクラッシュすることがあります。 [#27959](https://github.com/StarRocks/starrocks/pull/27959)
- 外部テーブルに基づいて作成されたマテリアライズドビューにクエリを再書き込みできません。 [#28023](https://github.com/StarRocks/starrocks/pull/28023)
- Hive テーブルが削除され、メタデータキャッシュが手動で更新された後でも、Hive テーブルのデータをクエリできます。 [#28223](https://github.com/StarRocks/starrocks/pull/28223)
- 同期呼び出しを介して非同期マテリアライズドビューを手動でリフレッシュすると、`information_schema.task_runs` テーブルに複数の INSERT OVERWRITE レコードが生成されます。 [#28060](https://github.com/StarRocks/starrocks/pull/28060)
- LabelCleaner スレッドがブロックされることによる FE メモリリーク。 [#28311](https://github.com/StarRocks/starrocks/pull/28311)

## 3.0.4

リリース日: 2023年7月18日

### 新機能

クエリがマテリアライズドビューと異なるタイプのジョインを含んでいる場合でも、クエリを再書き込みできます。 [#25099](https://github.com/StarRocks/starrocks/pull/25099)

### 改善点

- 非同期マテリアライズドビューの手動リフレッシュを最適化しました。REFRESH MATERIALIZED VIEW WITH SYNC MODE 構文を使用して、マテリアライズドビューのリフレッシュタスクを同期的に呼び出すことをサポートします。 [#25910](https://github.com/StarRocks/starrocks/pull/25910)
- クエリされたフィールドがマテリアライズドビューの出力列に含まれていないが、マテリアライズドビューの述語に含まれている場合でも、クエリを再書き込みしてマテリアライズドビューの利点を得ることができます。 [#23028](https://github.com/StarRocks/starrocks/issues/23028)
- [SQL 方言 (`sql_dialect`) が `trino` に設定されている場合](https://docs.starrocks.io/docs/sql-reference/System_variable/)、テーブルエイリアスは大文字小文字を区別しません。 [#26094](https://github.com/StarRocks/starrocks/pull/26094) [#25282](https://github.com/StarRocks/starrocks/pull/25282)
- テーブル `Information_schema.tables_config` に新しいフィールド `table_id` を追加しました。`Information_schema` データベースのテーブル `tables_config` とテーブル `be_tablets` を列 `table_id` で結合して、タブレットが属するデータベースとテーブルの名前をクエリできます。 [#24061](https://github.com/StarRocks/starrocks/pull/24061)

### バグ修正

以下の問題を修正しました:

- sum 集計関数を含むクエリが単一テーブルのマテリアライズドビューから直接クエリ結果を取得するように書き換えられる場合、型推論の問題により sum() フィールドの値が不正確になることがあります。 [#25512](https://github.com/StarRocks/starrocks/pull/25512)
- StarRocks 共有データクラスタ内のタブレット情報を表示するために SHOW PROC を使用するとエラーが発生します。
- 挿入される STRUCT 内の CHAR データの長さが最大長を超えると、INSERT 操作がハングします。 [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- INSERT INTO SELECT with FULL JOIN の一部のデータ行が返されません。 [#26603](https://github.com/StarRocks/starrocks/pull/26603)
- ALTER TABLE ステートメントを使用してテーブルのプロパティ `default.storage_medium` を変更すると、エラー `ERROR xxx: Unknown table property xxx` が発生します。 [#25870](https://github.com/StarRocks/starrocks/issues/25870)
- Broker Load を使用して空のファイルをロードするとエラーが発生します。 [#26212](https://github.com/StarRocks/starrocks/pull/26212)
- BE の退役が時々ハングします。 [#26509](https://github.com/StarRocks/starrocks/pull/26509)

## 3.0.3

リリース日: 2023年6月28日

### 改善点

- StarRocks 外部テーブルのメタデータ同期がデータロード中に行われるように変更されました。 [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- 自動的に作成されたパーティションを持つテーブルに対して INSERT OVERWRITE を実行する際にパーティションを指定できます。詳細については、[自動パーティション化](https://docs.starrocks.io/docs/table_design/expression_partitioning/) を参照してください。 [#25005](https://github.com/StarRocks/starrocks/pull/25005)
- 非パーティション化テーブルにパーティションを追加する際に報告されるエラーメッセージを最適化しました。 [#25266](https://github.com/StarRocks/starrocks/pull/25266)

### バグ修正

以下の問題を修正しました:

- Parquet ファイルに複雑なデータ型が含まれている場合、min/max フィルタが誤った Parquet フィールドを取得します。 [#23976](https://github.com/StarRocks/starrocks/pull/23976)
- 関連するデータベースまたはテーブルが削除された場合でも、ロードタスクがキューに残ります。 [#24801](https://github.com/StarRocks/starrocks/pull/24801)
- FE の再起動が BE をクラッシュさせる可能性が低い確率であります。 [#25037](https://github.com/StarRocks/starrocks/pull/25037)
- 変数 `enable_profile` が `true` に設定されている場合、ロードおよびクエリジョブが時々フリーズします。 [#25060](https://github.com/StarRocks/starrocks/pull/25060)
- 3つ未満の生存 BE を持つクラスタで INSERT OVERWRITE が実行されると、不正確なエラーメッセージが表示されます。 [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 3.0.2

リリース日: 2023年6月13日

### 改善点

- 非同期マテリアライズドビューによってクエリが書き換えられた後、UNION クエリ内の述語をプッシュダウンできます。 [#23312](https://github.com/StarRocks/starrocks/pull/23312)
- テーブルの自動タブレット分配ポリシーを最適化しました。 [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- システムクロックに依存しない NetworkTime を削除し、サーバー間でシステムクロックが不一致である場合に発生する不正確な NetworkTime を修正しました。 [#24858](https://github.com/StarRocks/starrocks/pull/24858)

### バグ修正

以下の問題を修正しました:

- スキーマ変更と同時にデータロードが発生すると、スキーマ変更が時々ハングすることがあります。 [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- セッション変数 `pipeline_profile_level` が `0` に設定されている場合、クエリでエラーが発生します。 [#23873](https://github.com/StarRocks/starrocks/pull/23873)
- CREATE TABLE で `cloud_native_storage_type` が `S3` に設定されているとエラーが発生します。
- パスワードを使用しなくても LDAP 認証が成功します。 [#24862](https://github.com/StarRocks/starrocks/pull/24862)
- 関連するテーブルが存在しない場合、CANCEL LOAD が失敗します。 [#24922](https://github.com/StarRocks/starrocks/pull/24922)

### アップグレードノート

システムに `starrocks` という名前のデータベースがある場合、アップグレード前に ALTER DATABASE RENAME を使用して別の名前に変更してください。これは、`starrocks` が特権情報を格納するデフォルトのシステムデータベースの名前であるためです。

## 3.0.1

リリース日: 2023年6月1日

### 新機能

- [プレビュー] 大規模なオペレーターの中間計算結果をディスクにスピルして、大規模なオペレーターのメモリ消費を削減することをサポートします。詳細については、[ディスクへのスピル](https://docs.starrocks.io/docs/administration/spill_to_disk/) を参照してください。
- [Routine Load](https://docs.starrocks.io/docs/loading/RoutineLoad#load-avro-format-data) が Avro データのロードをサポートします。
- [Microsoft Azure Storage](https://docs.starrocks.io/docs/integrations/authenticate_to_azure_storage/)（Azure Blob Storage および Azure Data Lake Storage を含む）をサポートします。

### 改善点

- 共有データクラスタが StarRocks 外部テーブルを使用して別の StarRocks クラスタとデータを同期することをサポートします。
- 最近のロードエラーを記録するために [Information Schema](https://docs.starrocks.io/docs/sql-reference/information_schema/load_tracking_logs/) に `load_tracking_logs` を追加しました。
- CREATE TABLE ステートメント内の特殊文字を無視します。 [#23885](https://github.com/StarRocks/starrocks/pull/23885)

### バグ修正

以下の問題を修正しました:

- 主キーテーブルに対して SHOW CREATE TABLE で返される情報が正しくありません。 [#24237](https://github.com/StarRocks/starrocks/issues/24237)
- Routine Load ジョブ中に BEs がクラッシュすることがあります。 [#20677](https://github.com/StarRocks/starrocks/issues/20677)
- パーティションテーブルを作成する際にサポートされていないプロパティを指定すると、Null ポインタ例外 (NPE) が発生します。 [#21374](https://github.com/StarRocks/starrocks/issues/21374)
- SHOW TABLE STATUS で返される情報が不完全です。 [#24279](https://github.com/StarRocks/starrocks/issues/24279)

### アップグレードノート

システムに `starrocks` という名前のデータベースがある場合、アップグレード前に ALTER DATABASE RENAME を使用して別の名前に変更してください。これは、`starrocks` が特権情報を格納するデフォルトのシステムデータベースの名前であるためです。

## 3.0.0

リリース日: 2023年4月28日

### 新機能

#### システムアーキテクチャ

- **ストレージとコンピュートの分離。** StarRocks は、S3 互換のオブジェクトストレージへのデータ永続化をサポートし、リソースの分離を強化し、ストレージコストを削減し、コンピュートリソースをよりスケーラブルにします。ローカルディスクはクエリパフォーマンスを向上させるためのホットデータキャッシュとして使用されます。新しい共有データアーキテクチャのクエリパフォーマンスは、ローカルディスクキャッシュがヒットした場合、クラシックアーキテクチャ（共有なし）と同等です。詳細については、[共有データ StarRocks のデプロイと使用](https://docs.starrocks.io/docs/deployment/shared_data/s3/) を参照してください。

#### ストレージエンジンとデータ取り込み

- [AUTO_INCREMENT](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/auto_increment/) 属性がサポートされ、グローバルに一意の ID を提供し、データ管理を簡素化します。
- 自動パーティション化とパーティション化式がサポートされ、パーティション作成がより使いやすく柔軟になります。
- 主キーテーブルが、CTE の使用や複数のテーブルへの参照を含む、より完全な [UPDATE](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/UPDATE/) および [DELETE](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/#primary-key-tables) 構文をサポートします。
- Broker Load および INSERT INTO ジョブのロードプロファイルを追加しました。ロードプロファイルをクエリすることで、ロードジョブの詳細を表示できます。使用方法は [クエリプロファイルの分析](https://docs.starrocks.io/docs/administration/query_profile_overview/) と同じです。

#### データレイク分析

- [プレビュー] Presto/Trino 互換の方言をサポートします。Presto/Trino の SQL は自動的に StarRocks の SQL パターンに書き換えられます。詳細については、[システム変数](https://docs.starrocks.io/docs/reference/System_variable/) `sql_dialect` を参照してください。
- [プレビュー] [JDBC カタログ](https://docs.starrocks.io/docs/data_source/catalog/jdbc_catalog/) をサポートします。
- 現在のセッションでカタログ間を手動で切り替えるために [SET CATALOG](https://docs.starrocks.io/docs/sql-reference/sql-statements/Catalog/SET_CATALOG/) を使用することをサポートします。

#### 特権とセキュリティ

- 完全な RBAC 機能を備えた新しい特権システムを提供し、ロールの継承とデフォルトロールをサポートします。詳細については、[特権の概要](https://docs.starrocks.io/docs/administration/privilege_overview/) を参照してください。
- より多くの特権管理オブジェクトとより細かい特権を提供します。詳細については、[StarRocks がサポートする特権](https://docs.starrocks.io/docs/administration/privilege_item/) を参照してください。

#### クエリエンジン

- 結合されたテーブルに対するより多くのクエリが [クエリキャッシュ](https://docs.starrocks.io/docs/using_starrocks/query_cache/) の利点を享受できるようになりました。たとえば、クエリキャッシュは現在、Broadcast Join および Bucket Shuffle Join をサポートしています。
- [グローバル UDF](https://docs.starrocks.io/docs/sql-reference/sql-functions/JAVA_UDF/) をサポートします。
- 動的適応並行性: StarRocks はクエリの並行性のために `pipeline_dop` パラメータを自動的に調整できます。

#### SQL リファレンス

- 次の特権関連の SQL ステートメントを追加しました: [SET DEFAULT ROLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE/)、[SET ROLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SET_ROLE/)、[SHOW ROLES](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_ROLES/)、および [SHOW USERS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_USERS/)。
- 次の半構造化データ分析関数を追加しました: [map_apply](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_apply/)、[map_from_arrays](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_from_arrays/)。
- [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/) が ORDER BY をサポートします。
- ウィンドウ関数 [lead](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lead) および [lag](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lag) が IGNORE NULLS をサポートします。
- 文字列関数 [replace](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/replace/)、[hex_decode_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/hex_decode_binary/)、および [hex_decode_string()](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/hex_decode_string/) を追加しました。
- 暗号化関数 [base64_decode_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_binary/) および [base64_decode_string](https://docs.starrocks.io/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_string/) を追加しました。
- 数学関数 [sinh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/sinh/)、[cosh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cosh/)、および [tanh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/tanh/) を追加しました。
- ユーティリティ関数 [current_role](https://docs.starrocks.io/docs/sql-reference/sql-functions/utility-functions/current_role/) を追加しました。

### 改善点

#### デプロイメント

- バージョン 3.0 用の Docker イメージと関連する [Docker デプロイメントドキュメント](https://docs.starrocks.io/docs/quick_start/deploy_with_docker/) を更新しました。 [#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021)

#### ストレージエンジンとデータ取り込み

- データ取り込みのために、SKIP_HEADER、TRIM_SPACE、ENCLOSE、および ESCAPE を含むより多くの CSV パラメータをサポートします。[STREAM LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/STREAM_LOAD/)、[BROKER LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/BROKER_LOAD/)、および [ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD/) を参照してください。
- 主キーとソートキーが [主キーテーブル](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/) で分離され、テーブル作成時に `ORDER BY` でソートキーを個別に指定できます。
- 大量の取り込み、部分更新、および永続的な主キーインデックスなどのシナリオでの主キーテーブルへのデータ取り込みのメモリ使用量を最適化しました。
- 非同期 INSERT タスクの作成をサポートします。詳細については、[INSERT](https://docs.starrocks.io/docs/loading/InsertInto#load-data-asynchronously-using-insert) および [SUBMIT TASK](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK/) を参照してください。 [#20609](https://github.com/StarRocks/starrocks/issues/20609)

#### マテリアライズドビュー

- [マテリアライズドビュー](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/) の書き換え機能を最適化しました。以下を含みます:
  - View Delta Join、Outer Join、Cross Join の書き換えをサポートします。
  - パーティションを持つ Union の SQL 書き換えを最適化しました。
- マテリアライズドビューの構築機能を改善しました。CTE、select *、および Union をサポートします。
- [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW/) で返される情報を最適化しました。
- マテリアライズドビューの構築中にパーティション追加の効率を向上させるために、MV パーティションをバッチで追加することをサポートします。 [#21167](https://github.com/StarRocks/starrocks/pull/21167)

#### クエリエンジン

- すべてのオペレーターがパイプラインエンジンでサポートされています。非パイプラインコードは後のバージョンで削除されます。
- [大規模クエリの位置付け](https://docs.starrocks.io/docs/administration/monitor_manage_big_queries/) を改善し、大規模クエリログを追加しました。[SHOW PROCESSLIST](https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST/) で CPU とメモリ情報を表示できます。
- 外部結合の再順序を最適化しました。
- SQL パース段階でのエラーメッセージを最適化し、より正確なエラーポジショニングと明確なエラーメッセージを提供します。

#### データレイク分析

- メタデータ統計収集を最適化しました。
- [SHOW CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE/) を使用して、Apache Hive™、Apache Iceberg、Apache Hudi、または Delta Lake に格納されている外部カタログによって管理されるテーブルの作成ステートメントを表示することをサポートします。

### バグ修正

- StarRocks のソースファイルのライセンスヘッダー内の一部の URL にアクセスできません。 [#2224](https://github.com/StarRocks/starrocks/issues/2224)
- SELECT クエリ中に不明なエラーが返されます。 [#19731](https://github.com/StarRocks/starrocks/issues/19731)
- SHOW/SET CHARACTER をサポートします。 [#17480](https://github.com/StarRocks/starrocks/issues/17480)
- ロードされたデータが StarRocks がサポートするフィールド長を超える場合、返されるエラーメッセージが正しくありません。 [#14](https://github.com/StarRocks/DataX/issues/14)
- `show full fields from 'table'` をサポートします。 [#17233](https://github.com/StarRocks/starrocks/issues/17233)
- パーティションプルーニングが MV の書き換えを失敗させます。 [#14641](https://github.com/StarRocks/starrocks/issues/14641)
- CREATE MATERIALIZED VIEW ステートメントに `count(distinct)` が含まれており、`count(distinct)` が DISTRIBUTED BY 列に適用される場合、MV の書き換えが失敗します。 [#16558](https://github.com/StarRocks/starrocks/issues/16558)
- VARCHAR 列がマテリアライズドビューのパーティション列として使用されている場合、FEs が起動に失敗します。 [#19366](https://github.com/StarRocks/starrocks/issues/19366)
- ウィンドウ関数 [LEAD](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lead) および [LAG](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lag) が IGNORE NULLS を正しく処理しません。 [#21001](https://github.com/StarRocks/starrocks/pull/21001)
- 一時パーティションの追加が自動パーティション作成と競合します。 [#21222](https://github.com/StarRocks/starrocks/issues/21222)

### 動作変更

- 新しいロールベースのアクセス制御 (RBAC) システムは、以前の特権とロールをサポートします。ただし、[GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/) や [REVOKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/REVOKE/) などの関連ステートメントの構文が変更されました。
- SHOW MATERIALIZED VIEW を [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW/) に名前を変更しました。
- 次の [予約キーワード](https://docs.starrocks.io/docs/sql-reference/sql-statements/keywords/) を追加しました: AUTO_INCREMENT、CURRENT_ROLE、DEFERRED、ENCLOSE、ESCAPE、IMMEDIATE、PRIVILEGES、SKIP_HEADER、TRIM_SPACE、VARBINARY。

### アップグレードノート

v2.5 から v3.0 へのアップグレードまたは v3.0 から v2.5 へのダウングレードが可能です。

> 理論上、v2.5 より前のバージョンからのアップグレードもサポートされています。システムの可用性を確保するために、まずクラスタを v2.5 にアップグレードし、その後 v3.0 にアップグレードすることをお勧めします。

v3.0 から v2.5 へのダウングレードを行う際には、以下の点に注意してください。

#### BDBJE

StarRocks は v3.0 で BDB ライブラリをアップグレードします。ただし、BDBJE はロールバックできません。ダウングレード後も v3.0 の BDB ライブラリを使用する必要があります。次の手順を実行してください:

1. FE パッケージを v2.5 パッケージに置き換えた後、v3.0 の `fe/lib/starrocks-bdb-je-18.3.13.jar` を v2.5 の `fe/lib` ディレクトリにコピーします。

2. `fe/lib/je-7.*.jar` を削除します。

#### 特権システム

新しい RBAC 特権システムは、v3.0 にアップグレードした後にデフォルトで使用されます。v2.5 にのみダウングレードできます。

ダウングレード後、[ALTER SYSTEM CREATE IMAGE](https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM/) を実行して新しいイメージを作成し、新しいイメージがすべてのフォロワー FE に同期されるのを待ちます。このコマンドを実行しないと、一部のダウングレード操作が失敗する可能性があります。このコマンドは 2.5.3 以降でサポートされています。

v2.5 と v3.0 の特権システムの違いについては、[StarRocks がサポートする特権](https://docs.starrocks.io/docs/administration/privilege_item/) の「アップグレードノート」を参照してください。