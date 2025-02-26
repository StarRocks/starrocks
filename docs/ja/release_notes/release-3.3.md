---
displayed_sidebar: docs
---

# StarRocks バージョン 3.3

:::warning

StarRocks を v3.3 にアップグレードした後、直接 v3.2.0、v3.2.1、または v3.2.2 にダウングレードしないでください。そうしないとメタデータが失われます。問題を防ぐために、クラスタを v3.2.3 以降にダウングレードする必要があります。

:::

## 3.3.10

リリース日: 2025年2月21日

### 新機能

- ウィンドウ関数が `max_by` と `min_by` をサポートしました。 [#54961](https://github.com/StarRocks/starrocks/pull/54961)

### 改善点

- テーブル関数における複雑な型のサブフィールドのプッシュダウンをサポートしました。 [#55425](https://github.com/StarRocks/starrocks/pull/55425)
- MariaDB クライアントのための LDAP ログインをサポートしました。 [#55720](https://github.com/StarRocks/starrocks/pull/55720)
- Paimon バージョンを 1.0.1 にアップグレードしました。 [#54796](https://github.com/StarRocks/starrocks/pull/54796) [#55760](https://github.com/StarRocks/starrocks/pull/55760)
- クエリ実行中の不要な `unnest` 計算を排除し、オーバーヘッドを削減しました。 [#55431](https://github.com/StarRocks/starrocks/pull/55431)
- 共有データモードのソースクラスタに対する Compaction の有効化を、クロスクラスタ同期中にサポートしました。 [#54787](https://github.com/StarRocks/starrocks/pull/54787)
- DECIMAL 除算のような高コストの操作を topN 計算で前倒しし、オーバーヘッドを削減しました。 [#55417](https://github.com/StarRocks/starrocks/pull/55417)
- ARM アーキテクチャでのパフォーマンスを最適化しました。 [#55072](https://github.com/StarRocks/starrocks/pull/55072) [#55510](https://github.com/StarRocks/starrocks/pull/55510)
- Hive テーブルベースのマテリアライズドビューにおいて、ベーステーブルが削除され再作成された場合、StarRocks は更新されたパーティションのみをチェックし、リフレッシュします。 [#45118](https://github.com/StarRocks/starrocks/pull/45118)
- DELETE 操作がパーティションプルーニングをサポートしました。 [#55400](https://github.com/StarRocks/starrocks/pull/55400)
- 内部テーブルの統計収集の優先順位戦略を最適化し、テーブルが多すぎる場合の効率を向上させました。 [#55446](https://github.com/StarRocks/starrocks/pull/55446)
- データロードが複数のパーティションを含む場合、StarRocks はトランザクションログをマージしてロードパフォーマンスを向上させます。 [#55143](https://github.com/StarRocks/starrocks/pull/55143)
- SQL 翻訳のエラーメッセージを最適化しました。 [#55327](https://github.com/StarRocks/starrocks/pull/55327)
- 並列マージ動作を制御するためのセッション変数 `parallel_merge_late_materialization_mode` を追加しました。 [#55082](https://github.com/StarRocks/starrocks/pull/55082)
- 生成列のエラーメッセージを最適化しました。 [#54949](https://github.com/StarRocks/starrocks/pull/54949)
- `SHOW MATERIALIZED VIEWS` のパフォーマンスを最適化しました。 [#54374](https://github.com/StarRocks/starrocks/pull/54374)

### バグ修正

以下の問題を修正しました:

- CTE 内で LIMIT を述語の前にプッシュダウンすることによって引き起こされるエラー。 [#55768](https://github.com/StarRocks/starrocks/pull/55768)
- Stream Load におけるテーブルスキーマ変更によって引き起こされるエラー。 [#55773](https://github.com/StarRocks/starrocks/pull/55773)
- DELETE 文の実行計画に SELECT を含むことによる権限の問題。 [#55695](https://github.com/StarRocks/starrocks/pull/55695)
- CN をシャットダウンする際に compaction タスクを中止しないことによって引き起こされる問題。 [#55503](https://github.com/StarRocks/starrocks/pull/55503)
- Follower FE ノードが更新されたロード統計を取得できない。 [#55758](https://github.com/StarRocks/starrocks/pull/55758)
- スピルディレクトリの容量統計が正しくない。 [#55703](https://github.com/StarRocks/starrocks/pull/55703)
- リストパーティションを持つベーステーブルに対する十分なパーティションチェックがないため、マテリアライズドビューの作成に失敗する。 [#55673](https://github.com/StarRocks/starrocks/pull/55673)
- ALTER TABLE におけるメタデータロックの欠如によって引き起こされる問題。 [#55605](https://github.com/StarRocks/starrocks/pull/55605)
- 制約によって引き起こされる `SHOW CREATE TABLE` のエラー。 [#55592](https://github.com/StarRocks/starrocks/pull/55592)
- Nestloop Join における大きな ARRAY による OOM。 [#55603](https://github.com/StarRocks/starrocks/pull/55603)
- DROP PARTITION におけるロックの問題。 [#55549](https://github.com/StarRocks/starrocks/pull/55549)
- 文字列型をサポートしないことによる min/max ウィンドウ関数の問題。 [#55537](https://github.com/StarRocks/starrocks/pull/55537)
- パーサーのパフォーマンスが低下する。 [#54830](https://github.com/StarRocks/starrocks/pull/54830)
- 部分更新中の列名の大文字小文字の区別の問題。 [#55442](https://github.com/StarRocks/starrocks/pull/55442)
- Stream Load が "Alive" 状態が false のノードでスケジュールされるとインポートに失敗する。 [#55371](https://github.com/StarRocks/starrocks/pull/55371)
- ORDER BY を含むマテリアライズドビューでの出力列の順序が正しくない。 [#55355](https://github.com/StarRocks/starrocks/pull/55355)
- ディスク障害による BE のクラッシュ。 [#55042](https://github.com/StarRocks/starrocks/pull/55042)
- Query Cache によって引き起こされるクエリ結果の誤り。 [#55287](https://github.com/StarRocks/starrocks/pull/55287)
- Parquet Writer がタイムゾーンを持つ TIMESTAMP 型を書き込む際にタイムゾーンを変換できない。 [#55194](https://github.com/StarRocks/starrocks/pull/55194)
- ALTER ジョブのタイムアウトによるロードタスクのハング。 [#55207](https://github.com/StarRocks/starrocks/pull/55207)
- 入力がミリ秒単位の場合の `date_format` 関数によって引き起こされるエラー。 [#54854](https://github.com/StarRocks/starrocks/pull/54854)
- Partition Key が DATE 型であることによって引き起こされるマテリアライズドビューの書き換え失敗。 [#54804](https://github.com/StarRocks/starrocks/pull/54804)

### 動作の変更

- Iceberg の UUID 型が BINARY にマッピングされるようになりました。 [#54978](https://github.com/StarRocks/starrocks/pull/54978)
- 統計を再収集する必要があるかどうかを判断するために、パーティションの可視時間の代わりに変更された行数を使用します。 [#55373](https://github.com/StarRocks/starrocks/pull/55373)

## 3.3.9

リリース日: 2025年1月12日

### 新機能

- Trino SQL を StarRocks SQL に変換することをサポートしました。 [#54185](https://github.com/StarRocks/starrocks/pull/54185)

### 改善点

- `bdbje_reset_election_group` で始まる FE ノード名を修正し、明確さを向上させました。 [#54399](https://github.com/StarRocks/starrocks/pull/54399)
- ARM アーキテクチャでの `IF` 関数のベクトル化を実装しました。 [#53093](https://github.com/StarRocks/starrocks/pull/53093)
- `ALTER SYSTEM CREATE IMAGE` が StarManager のイメージ作成をサポートしました。 [#54370](https://github.com/StarRocks/starrocks/pull/54370)
- 共有データクラスタ内の主キーテーブルのクラウドネイティブインデックスの削除をサポートしました。 [#53971](https://github.com/StarRocks/starrocks/pull/53971)
- `FORCE` キーワードが指定された場合、マテリアライズドビューのリフレッシュを強制しました。 [#52081](https://github.com/StarRocks/starrocks/pull/52081)
- `CACHE SELECT` にヒントを指定することをサポートしました。 [#54697](https://github.com/StarRocks/starrocks/pull/54697)
- `FILES()` 関数を使用して圧縮された CSV ファイルをロードすることをサポートしました。サポートされる圧縮形式には、gzip、bz2、lz4、deflate、zstd が含まれます。 [#54626](https://github.com/StarRocks/starrocks/pull/54626)
- `UPDATE` 文で同じ列に複数の値を割り当てることをサポートしました。 [#54534](https://github.com/StarRocks/starrocks/pull/54534)

### バグ修正

以下の問題を修正しました:

- JDBC カタログに基づいて構築されたマテリアライズドビューのリフレッシュ時に予期しないエラーが発生する。 [#54487](https://github.com/StarRocks/starrocks/pull/54487)
- Delta Lake テーブルが自身とジョインする際の結果の不安定性。 [#54473](https://github.com/StarRocks/starrocks/pull/54473)
- HDFS にデータをバックアップする際のアップロード再試行の失敗。 [#53679](https://github.com/StarRocks/starrocks/pull/53679)
- aarch64 アーキテクチャでの BFD 初期化エラー。 [#54372](https://github.com/StarRocks/starrocks/pull/54372)
- BE ログに記録された機密情報。 [#54677](https://github.com/StarRocks/starrocks/pull/54677)
- Compaction 関連のメトリクスにおけるエラー。 [#54678](https://github.com/StarRocks/starrocks/pull/54678)
- ネストされた `TIME` 型を持つテーブルの作成による BE のクラッシュ。 [#54601](https://github.com/StarRocks/starrocks/pull/54601)
- サブクエリ TOP-N を持つ `LIMIT` クエリのクエリプランエラー。 [#54507](https://github.com/StarRocks/starrocks/pull/54507)

### ダウングレードノート

- クラスタは v3.3.9 から v3.2.11 以降にのみダウングレードできます。

## 3.3.8

リリース日: 2025年1月3日

### 改善点

- クラスタの状態を判断するためのクラスタアイドル API を追加しました。 [#53850](https://github.com/StarRocks/starrocks/pull/53850)
- JSON メトリクスにノード情報とヒストグラムメトリクスを含めました。 [#53735](https://github.com/StarRocks/starrocks/pull/53735)
- 共有データクラスタ内の主キーテーブルの MemTable を最適化しました。 [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- 共有データクラスタ内の主キーテーブルのメモリ使用量と統計を最適化しました。 [#54358](https://github.com/StarRocks/starrocks/pull/54358)
- フルテーブルまたは大規模なパーティションスキャンを必要とするクエリのために、ノードごとにスキャンされるパーティションの数に制限を設け、個々の BE または CN ノードへのスキャン圧力を軽減することでシステムの安定性を向上させました。 [#53747](https://github.com/StarRocks/starrocks/pull/53747)
- Paimon テーブルの統計収集をサポートしました。 [#52858](https://github.com/StarRocks/starrocks/pull/52858)
- 共有データクラスタのための S3 クライアントリクエストタイムアウトの設定をサポートしました。 [#54211](https://github.com/StarRocks/starrocks/pull/54211)

### バグ修正

以下の問題を修正しました:

- 主キーテーブルの DelVec の不整合によって引き起こされる BE のクラッシュ。 [#53460](https://github.com/StarRocks/starrocks/pull/53460)
- 共有データクラスタ内の主キーテーブルのロック解除の問題。 [#53878](https://github.com/StarRocks/starrocks/pull/53878)
- 関数にネストされた UDF のエラーがクエリ失敗時に返されない。 [#44297](https://github.com/StarRocks/starrocks/pull/44297)
- トランザクションが元のレプリカに依存しているため、Decommission フェーズでブロックされる。 [#49349](https://github.com/StarRocks/starrocks/pull/49349)
- Delta Lake テーブルに対するクエリがファイル取得にファイル名ではなく相対パスを使用する。 [#53949](https://github.com/StarRocks/starrocks/pull/53949)
- Delta Lake Shallow Clone テーブルに対するクエリでエラーが返される。 [#54044](https://github.com/StarRocks/starrocks/pull/54044)
- JNI を使用して Paimon を読み取る際の大文字小文字の区別の問題。 [#54041](https://github.com/StarRocks/starrocks/pull/54041)
- Hive で作成された Hive テーブルに対する `INSERT OVERWRITE` 操作でエラーが返される。 [#53792](https://github.com/StarRocks/starrocks/pull/53792)
- `SHOW TABLE STATUS` コマンドがビュー権限を検証しない。 [#53811](https://github.com/StarRocks/starrocks/pull/53811)
- FE メトリクスが欠落している。 [#53058](https://github.com/StarRocks/starrocks/pull/53058)
- `INSERT` タスクでのメモリリーク。 [#53809](https://github.com/StarRocks/starrocks/pull/53809)
- レプリケーションタスクでの書き込みロックの欠如による並行性の問題。 [#54061](https://github.com/StarRocks/starrocks/pull/54061)
- `statistics` データベース内のテーブルの `partition_ttl` が効果を発揮しない。 [#54398](https://github.com/StarRocks/starrocks/pull/54398)
- Query Cache 関連の問題:
  - Query Cache がグループ実行と共に有効になっているときのクラッシュ。 [#54363](https://github.com/StarRocks/starrocks/pull/54363)
  - ランタイムフィルターのクラッシュ。 [#54305](https://github.com/StarRocks/starrocks/pull/54305)
- マテリアライズドビューの Union Rewrite の問題。 [#54293](https://github.com/StarRocks/starrocks/pull/54293)
- 主キーテーブルの部分更新における文字列更新のパディングの欠如。 [#54182](https://github.com/StarRocks/starrocks/pull/54182)
- 低基数最適化が有効な場合の `max(count(distinct))` の実行計画の誤り。 [#53403](https://github.com/StarRocks/starrocks/pull/53403)
- マテリアライズドビューの `excluded_refresh_tables` パラメータの変更に関する問題。 [#53394](https://github.com/StarRocks/starrocks/pull/53394)

### 動作の変更

- 共有データクラスタ内の主キーテーブルの `persistent_index_type` のデフォルト値を `CLOUD_NATIVE` に変更しました。つまり、デフォルトで永続性インデックスが有効になっています。 [#52209](https://github.com/StarRocks/starrocks/pull/52209)

## 3.3.7

リリース日: 2024年11月29日

### 新機能

- 新しいマテリアライズドビューのパラメータ `excluded_refresh_tables` を追加し、リフレッシュが必要なテーブルを除外します。 [#50926](https://github.com/StarRocks/starrocks/pull/50926)

### 改善点

- `unnest(bitmap_to_array)` を `unnest_bitmap` に書き換え、パフォーマンスを向上させました。 [#52870](https://github.com/StarRocks/starrocks/pull/52870)
- Txn ログの書き込みと削除操作を削減しました。 [#42542](https://github.com/StarRocks/starrocks/pull/42542)

### バグ修正

以下の問題を修正しました:

- 外部テーブルへの Power BI の接続に失敗する。 [#52977](https://github.com/StarRocks/starrocks/pull/52977)
- ログにおける誤解を招く FE Thrift RPC 失敗メッセージ。 [#52706](https://github.com/StarRocks/starrocks/pull/52706)
- ルーチンロードタスクが期限切れのトランザクションのためにキャンセルされる（現在はデータベースまたはテーブルが存在しない場合にのみタスクがキャンセルされます）。 [#50334](https://github.com/StarRocks/starrocks/pull/50334)
- HTTP 1.0 を使用して送信された場合の Stream Load の失敗。 [#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- パーティション ID の整数オーバーフロー。 [#52965](https://github.com/StarRocks/starrocks/pull/52965)
- Hive Text Reader が最後の空の要素を認識できない。 [#52990](https://github.com/StarRocks/starrocks/pull/52990)
- Join 条件での `array_map` によって引き起こされる問題。 [#52911](https://github.com/StarRocks/starrocks/pull/52911)
- 高並行性シナリオでのメタデータキャッシュの問題。 [#52968](https://github.com/StarRocks/starrocks/pull/52968)
- ベーステーブルからパーティションが削除されたときに、マテリアライズドビュー全体がリフレッシュされる。 [#52740](https://github.com/StarRocks/starrocks/pull/52740)

## 3.3.6

リリース日: 2024年11月18日

### 改善点

- 主キーテーブルの内部修復ロジックを最適化しました。 [#52707](https://github.com/StarRocks/starrocks/pull/52707)
- 統計のヒストグラムの内部実装を最適化しました。 [#52400](https://github.com/StarRocks/starrocks/pull/52400)
- FE 設定項目 `sys_log_warn_modules` を介してログレベルを調整し、Hudi Catalog のログを削減することをサポートしました。 [#52709](https://github.com/StarRocks/starrocks/pull/52709)
- `yearweek` 関数での定数畳み込みをサポートしました。 [#52714](https://github.com/StarRocks/starrocks/pull/52714)
- ラムダ関数のプッシュダウンを回避しました。 [#52655](https://github.com/StarRocks/starrocks/pull/52655)
- クエリエラーメトリクスを内部エラー率、解析エラー率、タイムアウト率の3つに分割しました。 [#52646](https://github.com/StarRocks/starrocks/pull/52646)
- `array_map` 内で定数式が共通式として抽出されるのを回避しました。 [#52541](https://github.com/StarRocks/starrocks/pull/52541)
- マテリアライズドビューのテキストベースの書き換えを最適化しました。 [#52498](https://github.com/StarRocks/starrocks/pull/52498)

### バグ修正

以下の問題を修正しました:

- 共有データクラスタ内のクラウドネイティブテーブルの `SHOW CREATE TABLE` における `unique_constraints` および `foreign_constraints` パラメータが不完全である。 [#52804](https://github.com/StarRocks/starrocks/pull/52804)
- `enable_mv_automatic_active_check` が `false` に設定されている場合でも、一部のマテリアライズドビューがアクティブ化される。 [#52799](https://github.com/StarRocks/starrocks/pull/52799)
- 古いメモリフラッシュ後にメモリ使用量が減少しない。 [#52613](https://github.com/StarRocks/starrocks/pull/52613)
- Hudi ファイルシステムビューによるリソースリーク。 [#52738](https://github.com/StarRocks/starrocks/pull/52738)
- 主キーテーブルでの同時発行および更新操作が問題を引き起こす可能性がある。 [#52687](https://github.com/StarRocks/starrocks/pull/52687)
- クライアントでのクエリ終了の失敗。 [#52185](https://github.com/StarRocks/starrocks/pull/52185)
- 複数列のリストパーティションがプッシュダウンできない。 [#51036](https://github.com/StarRocks/starrocks/pull/51036)
- ORC ファイルに `hasnull` プロパティがないために誤った結果が返される。 [#52555](https://github.com/StarRocks/starrocks/pull/52555)
- テーブル作成時に ORDER BY で大文字の列名を使用することによって引き起こされる問題。 [#52513](https://github.com/StarRocks/starrocks/pull/52513)
- `ALTER TABLE PARTITION (*) SET ("storage_cooldown_ttl" = "xxx")` を実行した後にエラーが返される。 [#52482](https://github.com/StarRocks/starrocks/pull/52482)

### 動作の変更

- 以前のバージョンでは、`_statistics_` データベース内のビューに対して十分なレプリカがない場合、スケールイン操作が失敗していました。v3.3.6 以降、ノードが 3 以上にスケールインされる場合、ビューのレプリカは 3 に設定され、スケールイン後にノードが 1 つしかない場合、ビューのレプリカは 1 に設定され、スケールインが成功するようになりました。 [#51799](https://github.com/StarRocks/starrocks/pull/51799)

  影響を受けるビューには以下が含まれます:

  - `column_statistics`
  - `histogram_statistics`
  - `table_statistic_v1`
  - `external_column_statistics`
  - `external_histogram_statistics`
  - `pipe_file_list`
  - `loads_history`
  - `task_run_history`

- 新しい主キーテーブルでは、`allow_system_reserved_names` が `true` に設定されていても、`__op` を列名として使用することはできません。既存のテーブルには影響しません。 [#52621](https://github.com/StarRocks/starrocks/pull/52621)
- 式でパーティション化されたテーブルのパーティション名を変更することはできません。 [#52557](https://github.com/StarRocks/starrocks/pull/52557)
- FE パラメータ `heartbeat_mgr_blocking_queue_size` および `profile_process_threads_num` を非推奨にしました。 [#52236](https://github.com/StarRocks/starrocks/pull/52236)
- 共有データクラスタ内の主キーテーブルに対して、オブジェクトストレージ上での永続性インデックスをデフォルトで有効にしました。 [#52209](https://github.com/StarRocks/starrocks/pull/52209)
- ランダムバケット法を使用するテーブルのバケット法を手動で変更することを許可しません。 [#52120](https://github.com/StarRocks/starrocks/pull/52120)
- バックアップとリストアに関連するパラメータの変更: [#52111](https://github.com/StarRocks/starrocks/pull/52111)
  - `make_snapshot_worker_count` が動的設定をサポート。
  - `release_snapshot_worker_count` が動的設定をサポート。
  - `upload_worker_count` が動的設定をサポート。デフォルト値は `1` から BE が存在するマシンの CPU コア数に変更されました。
  - `download_worker_count` が動的設定をサポート。デフォルト値は `1` から BE が存在するマシンの CPU コア数に変更されました。
- `SELECT @@autocommit` の戻り値の型が BOOLEAN から BIGINT に変更されました。 [#51946](https://github.com/StarRocks/starrocks/pull/51946)
- パーティションごとの最大バケット数を制御する新しい FE 設定項目 `max_bucket_number_per_partition` を追加しました。 [#47852](https://github.com/StarRocks/starrocks/pull/47852)
- 主キーテーブルのメモリ使用量チェックをデフォルトで有効にしました。 [#52393](https://github.com/StarRocks/starrocks/pull/52393)
- Compaction タスクが時間内に完了できない場合にロード速度を低下させるようにロード戦略を最適化しました。 [#52269](https://github.com/StarRocks/starrocks/pull/52269)

## 3.3.5

リリース日: 2024年10月23日

### 新機能

- DATETIME 型でミリ秒およびマイクロ秒の精度をサポートしました。
- リソースグループが CPU のハードアイソレーションをサポートしました。

### 改善点

- Flat JSON のパフォーマンスと抽出戦略を最適化しました。 [#50696](https://github.com/StarRocks/starrocks/pull/50696)
- 次の ARRAY 関数のメモリ使用量を削減しました:
  - array_contains/array_position [#50912](https://github.com/StarRocks/starrocks/pull/50912)
  - array_filter [#51363](https://github.com/StarRocks/starrocks/pull/51363)
  - array_match [#51377](https://github.com/StarRocks/starrocks/pull/51377)
  - array_map [#51244](https://github.com/StarRocks/starrocks/pull/51244)
- `Null` 値を `Not Null` 属性を持つリストパーティションキーにロードする際のエラーメッセージを最適化しました。 [#51086](https://github.com/StarRocks/starrocks/pull/51086)
- Files() 関数で認証が失敗した場合のエラーメッセージを最適化しました。 [#51697](https://github.com/StarRocks/starrocks/pull/51697)
- `INSERT OVERWRITE` の内部統計を最適化しました。 [#50417](https://github.com/StarRocks/starrocks/pull/50417)
- 共有データクラスタが永続性インデックスファイルのガーベジコレクション (GC) をサポートしました。 [#51684](https://github.com/StarRocks/starrocks/pull/51684)
- FE のメモリ不足 (OOM) 問題の診断を支援するために FE ログを追加しました。 [#51528](https://github.com/StarRocks/starrocks/pull/51528)
- FE のメタデータディレクトリからメタデータを復元することをサポートしました。 [#51040](https://github.com/StarRocks/starrocks/pull/51040)

### バグ修正

以下の問題を修正しました:

- PIPE 例外によって引き起こされるデッドロックの問題。 [#50841](https://github.com/StarRocks/starrocks/pull/50841)
- 動的パーティション作成の失敗が後続のパーティション作成をブロックする。 [#51440](https://github.com/StarRocks/starrocks/pull/51440)
- `UNION ALL` クエリで `ORDER BY` を使用するとエラーが返される。 [#51647](https://github.com/StarRocks/starrocks/pull/51647)
- UPDATE 文内の CTE がヒントを無視する。 [#51458](https://github.com/StarRocks/starrocks/pull/51458)
- システム定義ビュー `statistics.loads_history` の `load_finish_time` フィールドがロードタスク完了後に期待通りに更新されない。 [#51174](https://github.com/StarRocks/starrocks/pull/51174)
- UDTF がマルチバイト UTF-8 文字を誤って処理する。 [#51232](https://github.com/StarRocks/starrocks/pull/51232)

### 動作の変更

- `EXPLAIN` 文の戻り内容を変更しました。変更後、戻り内容は `EXPLAIN COST` と同等です。`EXPLAIN` によって返される詳細レベルを動的 FE パラメータ `query_detail_explain_level` を使用して設定できます。デフォルト値は `COSTS` で、他の有効な値は `NORMAL` と `VERBOSE` です。 [#51439](https://github.com/StarRocks/starrocks/pull/51439)

## 3.3.4

リリース日: 2024年9月30日

### 新機能

- リストパーティションテーブルで非同期マテリアライズドビューの作成をサポートしました。 [#46680](https://github.com/StarRocks/starrocks/pull/46680) [#46808](https://github.com/StarRocks/starrocks/pull/46808/files)
- リストパーティションテーブルが Nullable パーティション列をサポートしました。 [#47797](https://github.com/StarRocks/starrocks/pull/47797)
- `DESC FILES()` を使用して外部ファイルのスキーマ情報を表示することをサポートしました。 [#50527](https://github.com/StarRocks/starrocks/pull/50527)
- `SHOW PROC '/replications'` を介してレプリケーションタスクメトリクスを表示することをサポートしました。 [#50483](https://github.com/StarRocks/starrocks/pull/50483)

### 改善点

- 共有データクラスタでの `TRUNCATE TABLE` のデータリサイクルパフォーマンスを最適化しました。 [#49975](https://github.com/StarRocks/starrocks/pull/49975)
- CTE オペレーターの中間結果のスピルをサポートしました。 [#47982](https://github.com/StarRocks/starrocks/pull/47982)
- 複雑なクエリによる OOM 問題を軽減するために適応的段階的スケジューリングをサポートしました。 [#47868](https://github.com/StarRocks/starrocks/pull/47868)
- 特定のシナリオで STRING 型の日付または日時列の述語プッシュダウンをサポートしました。 [#50643](https://github.com/StarRocks/starrocks/pull/50643)
- 定数の半構造化データに対する COUNT DISTINCT 計算をサポートしました。 [#48273](https://github.com/StarRocks/starrocks/pull/48273)
- 共有データクラスタ内のテーブルのタブレットバランスを有効にするための新しい FE パラメータ `lake_enable_balance_tablets_between_workers` を追加しました。 [#50843](https://github.com/StarRocks/starrocks/pull/50843)
- 生成列のクエリ書き換え機能を強化しました。 [#50398](https://github.com/StarRocks/starrocks/pull/50398)
- 部分更新が `CURRENT_TIMESTAMP` のデフォルト値で列を自動的に埋めることをサポートしました。 [#50287](https://github.com/StarRocks/starrocks/pull/50287)

### バグ修正

以下の問題を修正しました:

- タブレットクローン中に FE 側で無限ループが発生し、「バージョンがコンパクトされました」というエラーが発生する。 [#50561](https://github.com/StarRocks/starrocks/pull/50561)
- ISO 形式の DATETIME 型がプッシュダウンできない。 [#49358](https://github.com/StarRocks/starrocks/pull/49358)
- 同時シナリオでタブレットが削除された後もデータが存在する。 [#50382](https://github.com/StarRocks/starrocks/pull/50382)
- `yearweek` 関数によって返される結果が正しくない。 [#51065](https://github.com/StarRocks/starrocks/pull/51065)
- CTE クエリ中の ARRAY 内の低基数辞書の問題。 [#51148](https://github.com/StarRocks/starrocks/pull/51148)
- FE 再起動後、マテリアライズドビューのパーティション TTL 関連のパラメータが失われる。 [#51028](https://github.com/StarRocks/starrocks/pull/51028)
- アップグレード後に `CURRENT_TIMESTAMP` で定義された列のデータが失われる。 [#50911](https://github.com/StarRocks/starrocks/pull/50911)
- `array_distinct` 関数によるスタックオーバーフロー。 [#51017](https://github.com/StarRocks/starrocks/pull/51017)
- デフォルトのフィールド長の変更によるアップグレード後のマテリアライズドビューのアクティベーション失敗。`enable_active_materialized_view_schema_strict_check` を `false` に設定することでこの問題を回避できます。 [#50869](https://github.com/StarRocks/starrocks/pull/50869)
- リソースグループプロパティ `cpu_weight` が負の値に設定できる。 [#51005](https://github.com/StarRocks/starrocks/pull/51005)
- ディスク容量情報の統計が正しくない。 [#50669](https://github.com/StarRocks/starrocks/pull/50669)
- `replace` 関数での定数畳み込み。 [#50828](https://github.com/StarRocks/starrocks/pull/50828)

### 動作の変更

- 外部カタログベースのマテリアライズドビューのデフォルトレプリカ数を `1` から FE パラメータ `default_replication_num` の値（デフォルト値: `3`）に変更しました。 [#50931](https://github.com/StarRocks/starrocks/pull/50931)

## 3.3.3

リリース日: 2024年9月5日

### 新機能

- ユーザーレベルの変数をサポートしました。 [#48477](https://github.com/StarRocks/starrocks/pull/48477)
- Delta Lake Catalog メタデータキャッシュを手動および定期的なリフレッシュ戦略でサポートしました。 [#46526](https://github.com/StarRocks/starrocks/pull/46526) [#49069](https://github.com/StarRocks/starrocks/pull/49069)
- Parquet ファイルから JSON 型をロードすることをサポートしました。 [#49385](https://github.com/StarRocks/starrocks/pull/49385)
- JDBC SQL Server Catalog が LIMIT を持つクエリをサポートしました。 [#48248](https://github.com/StarRocks/starrocks/pull/48248)
- 共有データクラスタが INSERT INTO を使用した部分更新をサポートしました。 [#49336](https://github.com/StarRocks/starrocks/pull/49336)

### 改善点

- ロード時のエラーメッセージを最適化しました:
  - ロード中にメモリ制限に達した場合、対応する BE ノードの IP が返され、トラブルシューティングが容易になります。 [#49335](https://github.com/StarRocks/starrocks/pull/49335)
  - CSV データがターゲットテーブルの列にロードされる際に、列が十分に長くない場合に詳細なメッセージが提供されます。 [#49713](https://github.com/StarRocks/starrocks/pull/49713)
  - Broker Load で Kerberos 認証が失敗した場合に特定のノード情報が提供されます。 [#46085](https://github.com/StarRocks/starrocks/pull/46085)
- データロード中のパーティションメカニズムを最適化し、初期段階でのメモリ使用量を削減しました。 [#47976](https://github.com/StarRocks/starrocks/pull/47976)
- 共有なしクラスタのメモリ使用量を最適化し、メタデータメモリ使用量を制限して、タブレットやセグメントファイルが多すぎる場合の問題を回避しました。 [#49170](https://github.com/StarRocks/starrocks/pull/49170)
- `max(partition_column)` を使用したクエリのパフォーマンスを最適化しました。 [#49391](https://github.com/StarRocks/starrocks/pull/49391)
- パーティション列が生成列（テーブル内の内部列に基づいて計算される列）であり、クエリ述語フィルター条件が内部列を含む場合、クエリパフォーマンスを最適化するためにパーティションプルーニングを使用します。 [#48692](https://github.com/StarRocks/starrocks/pull/48692)
- Files() および PIPE の認証情報のマスキングをサポートしました。 [#47629](https://github.com/StarRocks/starrocks/pull/47629)
- すべての FE ノードで実行中のクエリを表示するための新しいステートメント `show proc '/global_current_queries'` を導入しました。`show proc '/current_queries'` は現在の FE ノードで実行中のクエリのみを表示します。 [#49826](https://github.com/StarRocks/starrocks/pull/49826)

### バグ修正

以下の問題を修正しました:

- StarRocks 外部テーブルを介してデータを宛先クラスタにエクスポートする際に、ソースクラスタの BE ノードが誤って現在のクラスタに追加される。 [#49323](https://github.com/StarRocks/starrocks/pull/49323)
- TINYINT データ型が aarch64 マシンにデプロイされたクラスタから `select * from files` を使用して ORC ファイルを読み取る際に NULL を返す。 [#49517](https://github.com/StarRocks/starrocks/pull/49517)
- JSON ファイルに大きな整数型が含まれている場合、Stream Load が失敗する。 [#49927](https://github.com/StarRocks/starrocks/pull/49927)
- Files() を使用して CSV ファイルをロードする際に、不可視文字の不適切な処理によって誤ったスキーマが返される。 [#49718](https://github.com/StarRocks/starrocks/pull/49718)
- 複数のパーティション列を持つテーブルでの一時パーティション置換の問題。 [#49764](https://github.com/StarRocks/starrocks/pull/49764)

### 動作の変更

- クラウドオブジェクトストレージを使用したバックアップシナリオにより適応するために、新しいパラメータ `object_storage_rename_file_request_timeout_ms` を導入しました。このパラメータはバックアップタイムアウトとして使用され、デフォルト値は 30 秒です。 [#49706](https://github.com/StarRocks/starrocks/pull/49706)
- `to_json`、`CAST(AS MAP)`、および `STRUCT AS JSON` は、変換が失敗した場合にデフォルトで NULL を返します。システム変数 `sql_mode` を `ALLOW_THROW_EXCEPTION` に設定することで、エラーを許可できます。 [#50157](https://github.com/StarRocks/starrocks/pull/50157)

## 3.3.2

リリース日: 2024年8月8日

### 新機能

- StarRocks 内部テーブル内の列の名前変更をサポートしました。 [#47851](https://github.com/StarRocks/starrocks/pull/47851)
- Iceberg ビューの読み取りをサポートしました。現在、StarRocks を通じて作成された Iceberg ビューのみがサポートされています。 [#46273](https://github.com/StarRocks/starrocks/issues/46273)
- [実験的] STRUCT 型データのフィールドの追加および削除をサポートしました。 [#46452](https://github.com/StarRocks/starrocks/issues/46452)
- テーブル作成時に ZSTD 圧縮形式の圧縮レベルを指定することをサポートしました。 [#46839](https://github.com/StarRocks/starrocks/issues/46839)
- テーブルの境界を制限するための以下の FE 動的パラメータを追加しました。 [#47896](https://github.com/StarRocks/starrocks/pull/47869)

  含まれるもの:

  - `auto_partition_max_creation_number_per_load`
  - `max_partition_number_per_table`
  - `max_bucket_number_per_partition`
  - `max_column_number_per_table`

- テーブルデータの分布のランタイム最適化をサポートし、最適化タスクがテーブル上の DML 操作と競合しないようにします。 [#43747](https://github.com/StarRocks/starrocks/pull/43747)
- Data Cache のグローバルヒット率のための観測インターフェースを追加しました。 [#48450](https://github.com/StarRocks/starrocks/pull/48450)
- SQL 関数 array_repeat を追加しました。 [#47862](https://github.com/StarRocks/starrocks/pull/47862)

### 改善点

- Kafka 認証失敗によるルーチンロード失敗のエラーメッセージを最適化しました。 [#46136](https://github.com/StarRocks/starrocks/pull/46136) [#47649](https://github.com/StarRocks/starrocks/pull/47649)
- Stream Load が行および列の区切り文字として `\t` および `\n` を使用することをサポートしました。ユーザーはそれらを 16 進 ASCII コードに変換する必要はありません。 [#47302](https://github.com/StarRocks/starrocks/pull/47302)
- 多くのインポートタスクがある場合のレイテンシーの増加の問題に対処するために、書き込みオペレーターの非同期統計収集方法を最適化しました。 [#48162](https://github.com/StarRocks/starrocks/pull/48162)
- ロード中のリソースハードリミットを制御するための以下の BE 動的パラメータを追加し、多くのタブレットを書き込む際の BE の安定性への影響を軽減しました。 [#48495](https://github.com/StarRocks/starrocks/pull/48495)

  含まれるもの:

  - `load_process_max_memory_hard_limit_ratio`
  - `enable_new_load_on_memory_limit_exceeded`

- Compaction エラーを防ぐために、同じテーブル内の列 ID の一貫性チェックを追加しました。 [#48498](https://github.com/StarRocks/starrocks/pull/48628)
- FE 再起動によるメタデータ損失を防ぐために、PIPE メタデータの永続化をサポートしました。 [#48852](https://github.com/StarRocks/starrocks/pull/48852)

### バグ修正

以下の問題を修正しました:

- FE Follower から辞書を作成する際にプロセスが終了しない。 [#47802](https://github.com/StarRocks/starrocks/pull/47802)
- 共有データクラスタと共有なしクラスタで `SHOW PARTITIONS` コマンドによって返される情報が一致しない。 [#48647](https://github.com/StarRocks/starrocks/pull/48647)
- JSON フィールドから `ARRAY<BOOLEAN>` 列にデータをロードする際の型処理の誤りによるデータエラー。 [#48387](https://github.com/StarRocks/starrocks/pull/48387)
- `information_schema.task_runs` の `query_id` 列がクエリできない。 [#48876](https://github.com/StarRocks/starrocks/pull/48879)
- バックアップ中に、同じ操作に対して複数のリクエストが異なる Brokers に送信され、リクエストエラーが発生する。 [#48856](https://github.com/StarRocks/starrocks/pull/48856)
- v3.1.11 または v3.2.4 より前のバージョンにダウングレードすると、主キーテーブルのインデックス解凍が失敗し、クエリエラーが発生する。 [#48659](https://github.com/StarRocks/starrocks/pull/48659)

### ダウングレードノート

列の名前変更機能を使用した場合、クラスタを以前のバージョンにダウングレードする前に、列を元の名前に戻す必要があります。アップグレード後にクラスタの監査ログを確認し、`ALTER TABLE RENAME COLUMN` 操作と列の元の名前を特定できます。

## 3.3.1 (取り下げ)

リリース日: 2024年7月18日

:::tip

このバージョンは、主キーテーブルでの互換性の問題のためにオフラインにされました。

- **問題**: クラスタが v3.1.11 および v3.2.4 より前のバージョンから v3.3.1 にアップグレードされた後、インデックス解凍の失敗により、主キーテーブルに対するクエリが失敗します。

- **影響範囲**: この問題は主キーテーブルに対するクエリにのみ影響します。

- **一時的な回避策**: この問題を回避するために、クラスタを v3.3.0 またはそれ以前にダウングレードできます。この問題は v3.3.2 で修正されます。

:::

### 新機能

- [プレビュー] 一時テーブルをサポートしました。
- [プレビュー] JDBC Catalog が Oracle および SQL Server をサポートしました。
- [プレビュー] Unified Catalog が Kudu をサポートしました。
- 主キーテーブルでの INSERT INTO が列リストを指定することで部分更新をサポートしました。
- ユーザー定義変数が ARRAY 型をサポートしました。 [#42631](https://github.com/StarRocks/starrocks/pull/42613)
- Stream Load が JSON 型データを変換し、STRUCT/MAP/ARRAY 型の列にロードすることをサポートしました。 [#45406](https://github.com/StarRocks/starrocks/pull/45406)
- グローバル辞書キャッシュをサポートしました。
- パーティションのバッチ削除をサポートしました。 [#44744](https://github.com/StarRocks/starrocks/issues/44744)
- Apache Ranger での列レベルの権限管理をサポートしました。（マテリアライズドビューおよびビューの列レベルの権限はテーブルオブジェクトの下で設定する必要があります。） [#47702](https://github.com/StarRocks/starrocks/pull/47702)
- 共有データクラスタ内の主キーテーブルでの列モードでの部分更新をサポートしました。 [#46516](https://github.com/StarRocks/starrocks/issues/46516)
- Stream Load が伝送中のデータ圧縮をサポートし、ネットワーク帯域幅のオーバーヘッドを削減します。ユーザーは `compression` および `Content-Encoding` パラメータを使用して異なる圧縮アルゴリズムを指定できます。サポートされる圧縮アルゴリズムには GZIP、BZIP2、LZ4_FRAME、および ZSTD が含まれます。 [#43732](https://github.com/StarRocks/starrocks/pull/43732)

### 改善点

- IdChain ハッシュコードの実装を最適化し、FE の再起動時間を短縮しました。 [#47599](https://github.com/StarRocks/starrocks/pull/47599)
- FILES() 関数の `csv.trim_space` パラメータのエラーメッセージを改善し、不正な文字をチェックし、合理的なプロンプトを提供しました。 [#44740](https://github.com/StarRocks/starrocks/pull/44740)
- Stream Load が行および列の区切り文字として `\t` および `\n` を使用することをサポートしました。ユーザーはそれらを 16 進 ASCII コードに変換する必要はありません。 [#47302](https://github.com/StarRocks/starrocks/pull/47302)

### バグ修正

以下の問題を修正しました:

- スキーマ変更プロセス中にタブレットの移動によって引き起こされるスキーマ変更の失敗。 [#45517](https://github.com/StarRocks/starrocks/pull/45517)
- クロスクラスタデータ移行ツールがフィールドのデフォルト値に `\`、`\r` などの制御文字が含まれているため、ターゲットクラスタでテーブルを作成できない。 [#47861](https://github.com/StarRocks/starrocks/pull/47861)
- BE の再起動後の永続的な bRPC 失敗。 [#40229](https://github.com/StarRocks/starrocks/pull/40229)
- `user_admin` ロールが ALTER USER コマンドを使用して root パスワードを変更できる。 [#47801](https://github.com/StarRocks/starrocks/pull/47801)
- 主キーインデックスの書き込み失敗がデータ書き込みエラーを引き起こす。 [#48045](https://github.com/StarRocks/starrocks/pull/48045)

### 動作の変更

- Hive および Iceberg にデータをシンクする際に、中間結果のスピルがデフォルトで有効になっています。 [#47118](https://github.com/StarRocks/starrocks/pull/47118)
- BE 設定項目 `max_cumulative_compaction_num_singleton_deltas` のデフォルト値を `500` に変更しました。 [#47621](https://github.com/StarRocks/starrocks/pull/47621)
- ユーザーがパーティションテーブルを作成する際にバケット数を指定しない場合、パーティション数が 5 を超えると、バケット数の設定ルールが `max(2*BE または CN 数, 最大の履歴パーティションデータ量に基づいて計算されたバケット数)` に変更されます。以前のルールは最大の履歴パーティションデータ量に基づいてバケット数を計算するものでした。 [#47949](https://github.com/StarRocks/starrocks/pull/47949)
- 主キーテーブルの INSERT INTO 文で列リストを指定する場合、以前のバージョンではフルアップサートが行われていましたが、部分更新が行われるようになりました。

### ダウングレードノート

v3.3.1 以降から v3.2 にクラスタをダウングレードするには、次の手順に従う必要があります:

1. クラスタで新しい一時テーブルの作成を許可しないようにします:

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_experimental_temporary_table"="false"); 
   ```

2. クラスタに一時テーブルがあるかどうかを確認します:

   ```SQL
   SELECT * FROM information_schema.temp_tables;
   ```

3. システムに一時テーブルがある場合、次のコマンドを使用してそれらをクリーンアップします（システムレベルの操作権限が必要です）:

   ```SQL
   CLEAN TEMPORARY TABLE ON SESSION 'session';
   ```

## 3.3.0

リリース日: 2024年6月21日

### 新機能と改善点

#### 共有データクラスタ

- 共有データクラスタでのスキーマ進化のパフォーマンスを最適化し、DDL 変更の時間消費をサブ秒レベルに削減しました。詳細については、[スキーマ進化](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#set-fast-schema-evolution) を参照してください。
- 共有なしクラスタから共有データクラスタへのデータ移行の要件を満たすために、コミュニティは公式に [StarRocks データ移行ツール](https://docs.starrocks.io/docs/administration/data_migration_tool/) をリリースしました。これは、共有なしクラスタ間のデータ同期および災害復旧にも使用できます。
- [プレビュー] AWS Express One Zone Storage をストレージボリュームとして使用でき、読み書きパフォーマンスを大幅に向上させます。詳細については、[CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME/#properties) を参照してください。
- 共有データクラスタでのガーベジコレクション (GC) メカニズムを最適化しました。オブジェクトストレージ内のデータに対する手動コンパクションをサポートします。詳細については、[手動コンパクション](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#manual-compaction-from-31) を参照してください。
- 共有データクラスタ内の主キーテーブルの Compaction トランザクションの公開実行を最適化し、主キーインデックスの読み取りを回避することで I/O およびメモリオーバーヘッドを削減しました。
- タブレット内での内部並列スキャンをサポートします。これにより、テーブル内のバケットが非常に少ないシナリオでのクエリパフォーマンスが最適化され、クエリの並列性がタブレットの数に制限されます。ユーザーは次のシステム変数を設定することで並列スキャン機能を有効にできます:

  ```SQL
  SET GLOBAL enable_lake_tablet_internal_parallel = true;
  SET GLOBAL tablet_internal_parallel_mode = "force_split";
  ```

#### データレイク分析

- **Data Cache の強化**
  - ホットスポットデータをデータレイクから取得するための [Data Cache Warmup](https://docs.starrocks.io/docs/data_source/data_cache_warmup/) コマンド CACHE SELECT を追加し、クエリを高速化し、リソース使用量を最小化します。CACHE SELECT は SUBMIT TASK と組み合わせて定期的なキャッシュウォームアップを実現できます。この機能は、外部カタログ内のテーブルと共有データクラスタ内の内部テーブルの両方をサポートします。
  - [Data Cache の観測性](https://docs.starrocks.io/docs/data_source/data_cache_observe/) を強化するためにメトリクスと監視方法を追加しました。
- **Parquet リーダーパフォーマンスの強化**
  - ページインデックスを最適化し、データスキャンサイズを大幅に削減しました。
  - ページインデックスが使用される場合に不要なページを読み取る頻度を減らしました。
  - データ行が空であるかどうかを判断する計算を SIMD を使用して高速化しました。
- **ORC リーダーパフォーマンスの強化**
  - スキーマ変更後に ORC ファイルを読み取るために述語プッシュダウンに列 ID を使用します。
  - ORC 小さなストライプの処理ロジックを最適化しました。
- **Iceberg テーブル形式の強化**
  - Iceberg Catalog のメタデータアクセスパフォーマンスを大幅に向上させ、並列スキャンロジックをリファクタリングしました。大量のメタデータファイルを処理する際のネイティブ Iceberg SDK の単一スレッド I/O ボトルネックを解決しました。その結果、メタデータボトルネックを持つクエリは 10 倍以上のパフォーマンス向上を達成しました。
  - Parquet 形式の Iceberg v2 テーブルに対するクエリが [equality deletes](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#usage-notes) をサポートします。
- **[実験的] Paimon Catalog の強化**
  - Paimon 外部テーブルに基づいて作成されたマテリアライズドビューが自動クエリ書き換えをサポートします。
  - Paimon Catalog に対するクエリのスキャン範囲スケジューリングを最適化し、I/O 並行性を向上させました。
  - Paimon システムテーブルのクエリをサポートします。
  - Paimon 外部テーブルが DELETE Vectors をサポートし、更新および削除シナリオでのクエリ効率を向上させます。
- **[外部テーブル統計の収集の強化](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#collect-statistics-of-hiveiceberghudi-tables)**
  - ANALYZE TABLE を使用して外部テーブルのヒストグラムを収集でき、データスキューを防ぎます。
  - STRUCT サブフィールドの統計収集をサポートします。
- **テーブルシンクの強化**
  - Sink オペレーターのパフォーマンスが Trino と比較して倍増しました。
  - [Hive カタログ](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) および HDFS や AWS S3 などのクラウドストレージでの Textfile および ORC 形式のテーブルにデータをシンクできます。
- [プレビュー] Alibaba Cloud [MaxCompute カタログ](https://docs.starrocks.io/docs/data_source/catalog/maxcompute_catalog/) をサポートし、MaxCompute からのデータをインジェストせずにクエリし、INSERT INTO を使用して MaxCompute からデータを直接変換およびロードできます。
- [実験的] ClickHouse Catalog をサポートします。
- [実験的] [Kudu Catalog](https://docs.starrocks.io/docs/data_source/catalog/kudu_catalog/) をサポートします。

#### パフォーマンス改善とクエリ最適化

- **ARM 上でのパフォーマンスを最適化しました。**
  - ARM アーキテクチャ命令セットのパフォーマンスを大幅に最適化しました。AWS Graviton インスタンスでのパフォーマンステストでは、SSB 100G テストで ARM アーキテクチャが x86 アーキテクチャよりも 11% 高速であり、Clickbench テストで 39% 高速、TPC-H 100G テストで 13% 高速、TPC-DS 100G テストで 35% 高速でした。
- **ディスクへのスピルが GA になりました。** 複雑なクエリのメモリ使用量を最適化し、スピルスケジューリングを改善して、大規模なクエリが OOM なしで安定して実行できるようにしました。
- [プレビュー] [中間結果をオブジェクトストレージにスピルすることをサポートします](https://docs.starrocks.io/docs/administration/management/resource_management/spill_to_disk/#preview-spill-intermediate-result-to-object-storage)。
- **より多くのインデックスをサポートします。**
  - [プレビュー] [全文逆インデックス](https://docs.starrocks.io/docs/table_design/indexes/inverted_index/) をサポートし、全文検索を加速します。
  - [プレビュー] [N-Gram ブルームフィルターインデックス](https://docs.starrocks.io/docs/table_design/indexes/Ngram_Bloom_Filter_Index/) をサポートし、`LIKE` クエリや `ngram_search` および `ngram_search_case_insensitive` 関数の計算速度を向上させます。
- Bitmap 関数のパフォーマンスとメモリ使用量を改善しました。[Hive Bitmap UDFs](https://docs.starrocks.io/docs/sql-reference/sql-functions/hive_bitmap_udf/) を使用して Bitmap データを Hive にエクスポートする機能を追加しました。
- **[プレビュー] [Flat JSON](https://docs.starrocks.io/docs/using_starrocks/Flat_json/) をサポートします。** この機能は、データロード中に JSON データを自動的に検出し、JSON データから共通フィールドを抽出して、これらのフィールドをカラム形式で保存します。これにより、JSON クエリパフォーマンスが向上し、STRUCT データのクエリに匹敵するようになります。
- **[プレビュー] グローバル辞書を最適化しました。** 辞書テーブルからのキーと値のペアのマッピングを BE メモリ内に保存する辞書オブジェクトを提供します。新しい `dictionary_get()` 関数を使用して、BE メモリ内の辞書オブジェクトを直接クエリし、辞書テーブルをクエリする速度を `dict_mapping()` 関数を使用する場合と比較して加速します。さらに、辞書オブジェクトはディメンションテーブルとしても機能します。`dictionary_get()` を使用して辞書オブジェクトを直接クエリすることでディメンション値を取得でき、ディメンションテーブルでの JOIN 操作を行ってディメンション値を取得する従来の方法よりも高速なクエリ速度を実現します。
- [プレビュー] Colocate Group Execution をサポートします。Colocate テーブルでの Join および Agg オペレーターの実行におけるメモリ使用量を大幅に削減し、大規模なクエリをより安定して実行できるようにします。
- CodeGen のパフォーマンスを最適化しました。JIT がデフォルトで有効になっており、複雑な式計算で 5 倍のパフォーマンス向上を達成します。
- ベクトル化技術を使用して正規表現マッチングを実装し、`regexp_replace` 関数の CPU 消費を削減します。
- Broadcast Join を最適化し、右テーブルが空の場合に Broadcast Join 操作を事前に終了できるようにしました。
- データスキューのシナリオで Shuffle Join を最適化し、OOM を防ぎます。
- 集計クエリに `Limit` が含まれる場合、複数のパイプラインスレッドが `Limit` 条件を共有して計算リソースの消費を防ぎます。

#### ストレージ最適化とクラスタ管理

- **[レンジパーティション化の柔軟性を強化しました](https://docs.starrocks.io/docs/table_design/Data_distribution/#range-partitioning)。** 3 つの時間関数をパーティション列として使用できます。これらの関数は、パーティション列内のタイムスタンプまたは文字列を日付値に変換し、変換された日付値に基づいてデータをパーティション化します。
- **FE メモリの観測性。** FE 内の各モジュールの詳細なメモリ使用量メトリクスを提供し、リソースをより適切に管理します。
- **[FE 内のメタデータロックを最適化しました](https://docs.starrocks.io/docs/administration/management/FE_configuration/#lock_manager_enabled)。** FE 内のメタデータロックを集中管理するためのロックマネージャーを提供します。たとえば、メタデータロックの粒度をデータベースレベルからテーブルレベルに細分化し、ロードおよびクエリの並行性を向上させます。小規模データセットでの 100 の同時ロードジョブのシナリオでは、ロード時間を 35% 削減できます。
- **[BE にラベルを追加することをサポートします](https://docs.starrocks.io/docs/administration/management/resource_management/be_label/)。** BE が配置されているラックやデータセンターなどの情報に基づいて BE にラベルを追加することをサポートします。これにより、ラックやデータセンター間でのデータ分布が均等になり、特定のラックでの停電やデータセンターでの障害が発生した場合の災害復旧が容易になります。
- **[ソートキーを最適化しました](https://docs.starrocks.io/docs/table_design/indexes/Prefix_index_sort_key/#usage-notes)。** 重複キーテーブル、集計テーブル、およびユニークキーテーブルはすべて、`ORDER BY` 句を使用してソートキーを指定することをサポートします。
- **[実験的] 非文字列スカラー データのストレージ効率を最適化しました。** このタイプのデータは辞書エンコーディングをサポートし、ストレージスペースの使用量を 12% 削減します。
- **主キーテーブルのサイズ階層型コンパクションをサポートします。** コンパクション中の書き込み I/O およびメモリオーバーヘッドを削減します。この改善は、共有データクラスタと共有なしクラスタの両方でサポートされています。BE 設定項目 `enable_pk_size_tiered_compaction_strategy` を使用して、この機能を有効にするかどうかを制御できます（デフォルトで有効）。
- **主キーテーブルの永続性インデックスの読み取り I/O を最適化しました。** 永続性インデックスをより小さな粒度（ページ）で読み取ることをサポートし、永続性インデックスのブルームフィルターを改善します。この改善は、共有データクラスタと共有なしクラスタの両方でサポートされています。
- IPv6 をサポートします。StarRocks は、IPv6 ネットワークでのデプロイをサポートします。

#### マテリアライズドビュー

- **ビューに基づくクエリ書き換えをサポートします。** この機能が有効になっている場合、ビューに対するクエリは、そのビューに基づいて作成されたマテリアライズドビューに書き換えられます。詳細については、[ビューに基づくマテリアライズドビューの書き換え](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#view-based-materialized-view-rewrite) を参照してください。
- **テキストベースのクエリ書き換えをサポートします。** この機能が有効になっている場合、マテリアライズドビューと同じ抽象構文木 (AST) を持つクエリ（またはそのサブクエリ）は透過的に書き換えられます。詳細については、[テキストベースのマテリアライズドビューの書き換え](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#text-based-materialized-view-rewrite) を参照してください。
- **[プレビュー] マテリアライズドビューに直接対するクエリの透過的な書き換えモードを設定することをサポートします。** `transparent_mv_rewrite_mode` プロパティが有効になっている場合、StarRocks はクエリをマテリアライズドビューに自動的に書き換えます。リフレッシュされたマテリアライズドビューのパーティションからのデータを、リフレッシュされていないパーティションに対応する生データと自動的に UNION 操作を使用してマージします。このモードは、データの一貫性を維持しながら、リフレッシュ頻度を制御し、リフレッシュコストを削減することを目的としたモデリングシナリオに適しています。詳細については、[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/#parameters-1) を参照してください。
- マテリアライズドビューのクエリ書き換えのための集計プッシュダウンをサポートします: `enable_materialized_view_agg_pushdown_rewrite` 変数が有効になっている場合、ユーザーは [Aggregation Rollup](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#aggregation-rollup-rewrite) を使用して、マルチテーブルジョインシナリオを加速するために単一テーブルの非同期マテリアライズドビューを使用できます。集計関数はクエリ実行中にスキャンオペレーターにプッシュダウンされ、ジョインオペレーターが実行される前にマテリアライズドビューによって書き換えられ、クエリ効率が大幅に向上します。詳細については、[集計プッシュダウン](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/#aggregation-pushdown) を参照してください。
- **マテリアライズドビューの書き換えを制御する新しいプロパティをサポートします。** ユーザーは `enable_query_rewrite` プロパティを `false` に設定して、特定のマテリアライズドビューに基づくクエリ書き換えを無効にし、クエリ書き換えのオーバーヘッドを削減できます。モデリング後に直接クエリのためにのみ使用され、クエリ書き換えのために使用されないマテリアライズドビューの場合、ユーザーはこのマテリアライズドビューのクエリ書き換えを無効にできます。詳細については、[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/#parameters-1) を参照してください。
- **マテリアライズドビューの書き換えのコストを最適化しました。** 候補マテリアライズドビューの数を指定することをサポートし、フィルタリングアルゴリズムを強化しました。クエリ書き換えフェーズでのオプティマイザーの時間消費を削減するために、マテリアライズドビュープランキャッシュを導入しました。詳細については、`cbo_materialized_view_rewrite_related_mvs_limit` を参照してください。
- **Iceberg カタログに基づいて作成されたマテリアライズドビューを最適化しました。** Iceberg カタログに基づくマテリアライズドビューは、パーティション更新によってトリガーされるインクリメンタルリフレッシュと、パーティション変換を使用した Iceberg テーブルのパーティション整合性をサポートします。詳細については、[マテリアライズドビューによるデータレイククエリアクセラレーション](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/data_lake_query_acceleration_with_materialized_views/#choose-a-suitable-refresh-strategy) を参照してください。
- **マテリアライズドビューの観測性を強化しました。** マテリアライズドビューの監視と管理を改善し、システムの洞察を向上させました。詳細については、[非同期マテリアライズドビューのメトリクス](https://docs.starrocks.io/docs/administration/management/monitoring/metrics/#metrics-for-asynchronous-materialized-views) を参照してください。
- **大規模なマテリアライズドビューのリフレッシュ効率を改善しました。** グローバル FIFO スケジューリングをサポートし、ネストされたマテリアライズドビューのカスケードリフレッシュ戦略を最適化し、高頻度リフレッシュシナリオで発生するいくつかの問題を修正しました。
- **複数のファクトテーブルによってトリガーされるリフレッシュをサポートします。** 複数のファクトテーブルに基づいて作成されたマテリアライズドビューは、ファクトテーブルのいずれかのデータが更新された場合にパーティションレベルのインクリメンタルリフレッシュをサポートし、データ管理の柔軟性を向上させます。詳細については、[複数のベーステーブルとのパーティション整合性](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/create_partitioned_materialized_view/#align-partitions-with-multiple-base-tables) を参照してください。

#### SQL 関数

- DATETIME フィールドがマイクロ秒精度をサポートします。新しい時間単位は関連する時間関数およびデータロード中にサポートされます。
- 次の関数を追加しました:
  - [文字列関数](https://docs.starrocks.io/docs/category/string-1/): crc32, url_extract_host, ngram_search
  - 配列関数: [array_contains_seq](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_contains_seq/)
  - 日付と時間関数: [yearweek](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/yearweek/)
  - 数学関数: [cbrt](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cbrt/)

#### エコシステムサポート

- [実験的] [ClickHouse SQL Rewriter](https://github.com/StarRocks/SQLTransformer) を提供し、ClickHouse の構文を StarRocks の構文に変換するための新しいツールです。
- StarRocks が提供する Flink コネクタ v1.2.9 は Flink CDC 3.0 フレームワークと統合されており、CDC データソースから StarRocks へのストリーミング ELT パイプラインを構築できます。このパイプラインは、ソースの全データベース、シャーディングテーブル、およびスキーマ変更を StarRocks に同期できます。詳細については、[Flink CDC 3.0 を使用したデータの同期（スキーマ変更対応）](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#synchronize-data-with-flink-cdc-30-with-schema-change-supported) を参照してください。

### 動作とパラメータの変更

#### テーブル作成とデータ分布

- ユーザーは CTAS を使用してコロケートテーブルを作成する際に Distribution Key を指定する必要があります。 [#45537](https://github.com/StarRocks/starrocks/pull/45537)
- ユーザーが非パーティションテーブルを作成する際にバケット数を指定しない場合、システムがテーブルに設定する最小バケット数は `16`（`2*BE または CN 数` に基づく）です。ユーザーが小さなテーブルを作成する際に小さなバケット数を設定したい場合は、明示的に設定する必要があります。 [#47005](https://github.com/StarRocks/starrocks/pull/47005)

#### ロードとアンロード

- `__op` は StarRocks によって特別な目的で予約されており、デフォルトでは `__op` で始まる名前の列を作成することは禁じられています。FE 設定 `allow_system_reserved_names` を `true` に設定することで、このような名前形式を許可できます。ただし、主キーテーブルでこのような列を作成すると、未定義の動作が発生する可能性があります。 [#46239](https://github.com/StarRocks/starrocks/pull/46239)
- Routine Load ジョブ中に、StarRocks がデータを消費できない時間が FE 設定 `routine_load_unstable_threshold_second`（デフォルト値は `3600`、つまり1時間）で指定されたしきい値を超えると、ジョブのステータスは `UNSTABLE` になりますが、ジョブは続行されます。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- FE 設定 `enable_automatic_bucket` のデフォルト値が `false` から `true` に変更されました。この項目が `true` に設定されている場合、システムは新しく作成されたテーブルに対して自動的に `bucket_size` を設定し、自動バケッティングを有効にします。ただし、v3.2 では、`enable_automatic_bucket` を `true` に設定しても効果はありません。代わりに、システムは `bucket_size` が指定された場合にのみ自動バケッティングを有効にします。これにより、ユーザーが v3.3 から v3.2 に StarRocks をダウングレードする際のリスクを防ぎます。

#### クエリと半構造化データ

- 単一のクエリがパイプラインフレームワーク内で実行される場合、メモリ制限は `exec_mem_limit` ではなく `query_mem_limit` のみで制限されます。`query_mem_limit` の値が `0` の場合、制限はありません。 [#34120](https://github.com/StarRocks/starrocks/pull/34120)
- JSON 内の NULL 値は、IS NULL および IS NOT NULL 演算子によって実行されるときに SQL NULL 値として扱われます。たとえば、`parse_json('{"a": null}') -> 'a' IS NULL` は `1` を返し、`parse_json('{"a": null}') -> 'a' IS NOT NULL` は `0` を返します。 [#42765](https://github.com/StarRocks/starrocks/pull/42765) [#42909](https://github.com/StarRocks/starrocks/pull/42909)
- CBO が DECIMAL 型から STRING 型にデータを変換する方法を制御するために、新しいセッション変数 `cbo_decimal_cast_string_strict` が追加されました。この変数が `true` に設定されている場合、v2.5.x 以降のバージョンで組み込まれたロジックが優先され、システムは厳密な変換を実行します（つまり、生成された文字列を切り捨て、スケール長に基づいて 0 を埋めます）。この変数が `false` に設定されている場合、v2.5.x より前のバージョンで組み込まれたロジックが優先され、システムはすべての有効な桁を処理して文字列を生成します。デフォルト値は `true` です。 [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- `cbo_eq_base_type` のデフォルト値が `varchar` から `decimal` に変更され、システムは DECIMAL 型のデータを文字列としてではなく数値として比較します。 [#43443](https://github.com/StarRocks/starrocks/pull/43443)

#### その他

- マテリアライズドビューのプロパティ `partition_refresh_num` のデフォルト値が `-1` から `1` に変更されました。パーティション化されたマテリアライズドビューをリフレッシュする必要がある場合、元の動作ではすべてのパーティションを単一のタスクでリフレッシュしていましたが、新しい動作では 1 つのパーティションを一度にインクリメンタルにリフレッシュします。この変更は、元の動作によって引き起こされる過剰なリソース消費を防ぐことを目的としています。デフォルトの動作は、FE 設定 `default_mv_partition_refresh_number` を使用して調整できます。
- 元々、データベース整合性チェッカーは GMT+8 タイムゾーンに基づいてスケジュールされていました。データベース整合性チェッカーは現在、ローカルタイムゾーンに基づいてスケジュールされています。 [#45748](https://github.com/StarRocks/starrocks/issues/45748)
- データレイククエリを加速するために、デフォルトで Data Cache が有効になっています。ユーザーは `SET enable_scan_datacache = false` を実行して手動で無効にできます。
- 共有データクラスタを v3.3 から v3.2.8 およびそれ以前にダウングレードした後に Data Cache 内のキャッシュデータを再利用したい場合、**starlet_cache** ディレクトリ内の Blockfile のファイル名形式を `blockfile_{n}.{version}` から `blockfile_{n}` に変更し、バージョン情報のサフィックスを削除する必要があります。詳細については、[Data Cache 使用上の注意](https://docs.starrocks.io/docs/using_starrocks/caching/block_cache/#usage-notes) を参照してください。v3.2.9 およびそれ以降のバージョンは v3.3 のファイル名形式と互換性があるため、ユーザーはこの操作を手動で行う必要はありません。
- FE パラメータ `sys_log_level` を動的に変更することをサポートします。 [#45062](https://github.com/StarRocks/starrocks/issues/45062)
- Hive Catalog プロパティ `metastore_cache_refresh_interval_sec` のデフォルト値が `7200`（2 時間）から `60`（1 分）に変更されました。 [#46681](https://github.com/StarRocks/starrocks/pull/46681)

### バグ修正

以下の問題を修正しました:

- UNION ALL を使用して作成されたマテリアライズドビューにクエリが書き換えられると、クエリ結果が正しくありません。 [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- クエリ実行中に述語を持つクエリがマテリアライズドビューに書き換えられると、余分な列が読み取られます。 [#45272](https://github.com/StarRocks/starrocks/issues/45272)
- `next_day` および `previous_day` 関数の結果が正しくありません。 [#45343](https://github.com/StarRocks/starrocks/issues/45343)
- レプリカの移動によるスキーマ変更の失敗。 [#45384](https://github.com/StarRocks/starrocks/issues/45384)
- フルテキスト逆インデックスを持つテーブルを復元すると、BE がクラッシュします。 [#45010](https://github.com/StarRocks/starrocks/issues/45010)
- Iceberg カタログを使用してデータをクエリすると、重複したデータ行が返されます。 [#44753](https://github.com/StarRocks/starrocks/issues/44753)
- 低基数辞書の最適化が集計テーブルの `ARRAY<VARCHAR>` 型の列に対して効果を発揮しません。 [#44702](https://github.com/StarRocks/starrocks/issues/44702)
- UNION ALL を使用して作成されたマテリアライズドビューにクエリが書き換えられると、クエリ結果が正しくありません。 [#42949](https://github.com/StarRocks/starrocks/issues/42949)
- BE が ASAN でコンパイルされている場合、クラスタが起動されると BE がクラッシュし、`be.warning` ログに `dict_func_expr == nullptr` が表示されます。 [#44551](https://github.com/StarRocks/starrocks/issues/44551)
- 単一レプリカテーブルで集計クエリが実行されると、クエリ結果が正しくありません。 [#43223](https://github.com/StarRocks/starrocks/issues/43223)
- View Delta Join の書き換えが失敗します。 [#43788](https://github.com/StarRocks/starrocks/issues/43788)
- 列の型が VARCHAR から DECIMAL に変更された後、BE がクラッシュします。 [#44406](https://github.com/StarRocks/starrocks/issues/44406)
- リストパーティション化されたテーブルが not-equal 演算子を使用してクエリされると、パーティションが誤ってプルーニングされ、誤ったクエリ結果が返されます。 [#42907](https://github.com/StarRocks/starrocks/issues/42907)
- Leader FE のヒープサイズが、非トランザクションインターフェースを使用する多くの Stream Load ジョブが終了すると急速に増加します。 [#43715](https://github.com/StarRocks/starrocks/issues/43715)

### ダウングレードノート

v3.3.0 以降から v3.2 にクラスタをダウングレードするには、次の手順に従う必要があります:

1. v3.3 クラスタで開始されたすべての ALTER TABLE SCHEMA CHANGE トランザクションが完了またはキャンセルされていることを確認します。
2. 次のコマンドを実行してすべてのトランザクション履歴をクリアします:

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. 次のコマンドを実行して、残りの履歴レコードがないことを確認します:

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```

4. クラスタを v3.2.8 より前のパッチバージョンまたは v3.1.14 にダウングレードする場合、`PROPERTIES('compression' = 'lz4')` を使用して作成されたすべての非同期マテリアライズドビューを削除する必要があります。

5. 次のコマンドを実行してメタデータのイメージファイルを作成します:

   ```sql
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 新しいイメージファイルがすべての FE ノードの **meta/image** ディレクトリに転送された後、まず Follower FE ノードをダウングレードします。エラーが返されない場合、クラスタ内の他のノードをダウングレードできます。