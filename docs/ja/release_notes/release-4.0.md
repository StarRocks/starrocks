---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**ダウングレードに関する注意事項**

- StarRocks を v4.0 にアップグレードした後、v3.5.0 および v3.5.1 にダウングレードしないでください。そうするとメタデータの非互換性が発生し、FE がクラッシュする可能性があります。これらの問題を回避するには、クラスタを v3.5.2 以降にダウングレードする必要があります。

- v4.0.2 から v4.0.1、v4.0.0、および v3.5.2～v3.5.10 へのクラスタのダウングレード前に、次のステートメントを実行してください：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  クラスタをv4.0.2以降にアップグレードした後、次のステートメントを実行してください：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.5

リリース日：2026年2月3日

### 改善点

- Paimon のバージョンを 1.3.1 に更新しました。[#67098](https://github.com/StarRocks/starrocks/pull/67098)
- DP 統計情報推定における欠落していた最適化を復元し、冗長な計算を削減しました。[#67852](https://github.com/StarRocks/starrocks/pull/67852)
- DP Join 並べ替えにおけるプルーニングを改善し、高コストな候補プランを早期にスキップできるようにしました。[#67828](https://github.com/StarRocks/starrocks/pull/67828)
- JoinReorderDP のパーティション列挙を最適化し、オブジェクト割り当てを削減するとともに、アトム数の上限（≤ 62）を追加しました。[#67643](https://github.com/StarRocks/starrocks/pull/67643)
- DP Join 並べ替えのプルーニングを最適化し、BitSet にチェックを追加してストリーム処理のオーバーヘッドを削減しました。[#67644](https://github.com/StarRocks/starrocks/pull/67644)
- DP 統計情報推定時に述語列の統計情報収集をスキップし、CPU オーバーヘッドを削減しました。[#67663](https://github.com/StarRocks/starrocks/pull/67663)
- 相関 Join の行数推定を最適化し、`Statistics` オブジェクトの再生成を回避しました。[#67773](https://github.com/StarRocks/starrocks/pull/67773)
- `Statistics.getUsedColumns` におけるメモリ割り当てを削減しました。[#67786](https://github.com/StarRocks/starrocks/pull/67786)
- 行数のみを更新する場合に、`Statistics` マップの不要なコピーを回避しました。[#67777](https://github.com/StarRocks/starrocks/pull/67777)
- クエリに集約が存在しない場合、集約プッシュダウン処理をスキップしてオーバーヘッドを削減しました。[#67603](https://github.com/StarRocks/starrocks/pull/67603)
- ウィンドウ関数における COUNT DISTINCT を改善し、複数 DISTINCT 集約の融合に対応するとともに、CTE 生成を最適化しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Trino 方言で `map_agg` 関数をサポートしました。[#66673](https://github.com/StarRocks/starrocks/pull/66673)
- 物理プランニング時に LakeTablet のロケーション情報をバッチ取得できるようにし、共有データクラスタでの RPC 呼び出しを削減しました。[#67325](https://github.com/StarRocks/starrocks/pull/67325)
- shared-nothing クラスタにおいて Publish Version トランザクション用のスレッドプールを追加し、並行性を向上させました。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- LocalMetastore のロック粒度を最適化し、データベースレベルのロックをテーブルレベルのロックに置き換えました。[#67658](https://github.com/StarRocks/starrocks/pull/67658)
- MergeCommitTask のライフサイクル管理をリファクタリングし、タスクキャンセルをサポートしました。[#67425](https://github.com/StarRocks/starrocks/pull/67425)
- 自動クラスタスナップショットに対して実行間隔の設定をサポートしました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- MemTrackerManager において、未使用の `mem_pool` エントリを自動的にクリーンアップするようにしました。[#67347](https://github.com/StarRocks/starrocks/pull/67347)
- ウェアハウスのアイドルチェック時に `information_schema` クエリを無視するようにしました。[#67958](https://github.com/StarRocks/starrocks/pull/67958)
- データ分布に応じて、Iceberg テーブルの書き込み時にグローバルシャッフルを動的に有効化できるようにしました。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- Connector Sink モジュール向けに Profile メトリクスを追加しました。[#67761](https://github.com/StarRocks/starrocks/pull/67761)
- Profile におけるロードスピルメトリクスの収集および表示を改善し、ローカル I/O とリモート I/O を区別しました。[#67527](https://github.com/StarRocks/starrocks/pull/67527)
- Async-Profiler のログレベルを Error に変更し、警告ログの繰り返し出力を防止しました。[#67297](https://github.com/StarRocks/starrocks/pull/67297)
- BE シャットダウン時に Starlet へ通知し、StarMgr に SHUTDOWN ステータスを報告するようにしました。[#67461](https://github.com/StarRocks/starrocks/pull/67461)

### バグ修正

以下の問題を修正しました：

- ハイフン（`-`）を含む正当なシンプルパスがサポートされていませんでした。[#67988](https://github.com/StarRocks/starrocks/pull/67988)
- JSON 型を含むグループキーに対して集約プッシュダウンが行われた場合に実行時エラーが発生する問題。[#68142](https://github.com/StarRocks/starrocks/pull/68142)
- JSON パス書き換えルールにより、パーティション述語で参照されているパーティション列が誤ってプルーニングされる問題。[#67986](https://github.com/StarRocks/starrocks/pull/67986)
- 統計情報を用いたシンプル集約の書き換え時に型不一致が発生する問題。[#67829](https://github.com/StarRocks/starrocks/pull/67829)
- パーティション Join におけるヒープバッファオーバーフローの潜在的な問題。[#67435](https://github.com/StarRocks/starrocks/pull/67435)
- 重い式をプッシュダウンする際に `slot_ids` が重複して生成される問題。[#67477](https://github.com/StarRocks/starrocks/pull/67477)
- 前提条件チェック不足により、ExecutionDAG の Fragment 接続でゼロ除算が発生する問題。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- 単一 BE 環境での Fragment 並列準備に起因する潜在的な問題。[#67798](https://github.com/StarRocks/starrocks/pull/67798)
- RawValuesSourceOperator に `set_finished` メソッドが存在せず、オペレーターが正しく終了しない問題。[#67609](https://github.com/StarRocks/starrocks/pull/67609)
- 列アグリゲータで DECIMAL256 型（精度 > 38）がサポートされておらず、BE がクラッシュする問題。[#68134](https://github.com/StarRocks/starrocks/pull/68134)
- DELETE 操作時にリクエストへ `schema_key` を含めていなかったため、共有データクラスタで Fast Schema Evolution v2 がサポートされていなかった問題。[#67456](https://github.com/StarRocks/starrocks/pull/67456)
- 同期マテリアライズドビューおよび従来のスキーマ変更において、共有データクラスタで Fast Schema Evolution v2 がサポートされていなかった問題。[#67443](https://github.com/StarRocks/starrocks/pull/67443)
- FE のダウングレード時にファイルバンドルが無効化されている場合、Vacuum が誤ってファイルを削除する可能性がある問題。[#67849](https://github.com/StarRocks/starrocks/pull/67849)
- MySQLReadListener における正常終了処理が正しくない問題。[#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

リリース日：2026年1月16日

## 改善点

- クエリスケジューリング性能を向上させるため、Operator および Driver の並列 Prepare と、単一ノードでの Fragment 一括デプロイをサポートしました。 [#63956](https://github.com/StarRocks/starrocks/pull/63956)
- 大規模パーティションテーブルに対する `deltaRows` の計算を遅延評価（Lazy Evaluation）方式に最適化しました。 [#66381](https://github.com/StarRocks/starrocks/pull/66381)
- Flat JSON の処理を最適化し、逐次イテレーション方式の採用およびパス導出ロジックを改善しました。 [#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- Group Execution におけるメモリ使用量を削減するため、Spill Operator のメモリを早期に解放できるようにしました。 [#66669](https://github.com/StarRocks/starrocks/pull/66669)
- 文字列比較処理のオーバーヘッドを削減するロジックを最適化しました。 [#66570](https://github.com/StarRocks/starrocks/pull/66570)
- `GroupByCountDistinctDataSkewEliminateRule` および `SkewJoinOptimizeRule` におけるデータスキュー検出を強化し、ヒストグラムおよび NULL ベースの戦略をサポートしました。 [#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- Chunk 内の Column 所有権管理を Move セマンティクスで強化し、Copy-On-Write のオーバーヘッドを削減しました。 [#66805](https://github.com/StarRocks/starrocks/pull/66805)
- Shared-data クラスタ向けに FE の `TableSchemaService` を追加し、`MetaScanNode` を更新して Fast Schema Evolution v2 のスキーマ取得をサポートしました。 [#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- マルチ Warehouse 環境における Backend リソース統計および並列度（DOP）の計算をサポートし、リソース分離を強化しました。 [#66632](https://github.com/StarRocks/starrocks/pull/66632)
- StarRocks セッション変数 `connector_huge_file_size` により Iceberg の Split サイズを設定できるようになりました。 [#67044](https://github.com/StarRocks/starrocks/pull/67044)
- `QueryDumpDeserializer` において、ラベル形式（Label-formatted）の統計情報をサポートしました。 [#66656](https://github.com/StarRocks/starrocks/pull/66656)
- Shared-data クラスタで Full Vacuum を無効化するための FE 設定 `lake_enable_fullvacuum`（デフォルト：`false`）を追加しました。 [#63859](https://github.com/StarRocks/starrocks/pull/63859)
- lz4 依存関係を v1.10.0 にアップグレードしました。 [#67045](https://github.com/StarRocks/starrocks/pull/67045)
- 行数が 0 の場合に、サンプリングベースのカーディナリティ推定に対するフォールバックロジックを追加しました。 [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- `array_sort` における Lambda Comparator の Strict Weak Ordering 特性を検証しました。 [#66951](https://github.com/StarRocks/starrocks/pull/66951)
- 外部テーブル（Delta / Hive / Hudi / Iceberg）のメタデータ取得に失敗した場合のエラーメッセージを改善し、根本原因を表示するようにしました。 [#66916](https://github.com/StarRocks/starrocks/pull/66916)
- クエリタイムアウト時に Pipeline の状態を Dump し、FE 側で `TIMEOUT` 状態としてクエリをキャンセルできるようにしました。 [#66540](https://github.com/StarRocks/starrocks/pull/66540)
- SQL ブラックリストのエラーメッセージに、マッチしたルールのインデックスを表示するようにしました。 [#66618](https://github.com/StarRocks/starrocks/pull/66618)
- `EXPLAIN` 出力に列統計情報のラベルを追加しました。 [#65899](https://github.com/StarRocks/starrocks/pull/65899)
- 正常終了（例：LIMIT 到達）時の「cancel fragment」ログを除外しました。 [#66506](https://github.com/StarRocks/starrocks/pull/66506)
- Warehouse がサスペンドされている場合の Backend ハートビート失敗ログを削減しました。 [#66733](https://github.com/StarRocks/starrocks/pull/66733)
- `ALTER STORAGE VOLUME` 構文で `IF EXISTS` をサポートしました。 [#66691](https://github.com/StarRocks/starrocks/pull/66691)

## バグ修正

以下の問題を修正しました：

- Low Cardinality 最適化下で `withLocalShuffle` が不足していたことにより、`DISTINCT` および `GROUP BY` の結果が不正になる問題を修正しました。 [#66768](https://github.com/StarRocks/starrocks/pull/66768)
- Lambda 式を含む JSON v2 関数におけるリライトエラーを修正しました。 [#66550](https://github.com/StarRocks/starrocks/pull/66550)
- 相関サブクエリ内の Null-aware Left Anti Join において、Partition Join が誤って適用される問題を修正しました。 [#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Meta Scan のリライトルールにおける行数計算の誤りを修正しました。 [#66852](https://github.com/StarRocks/starrocks/pull/66852)
- 統計情報に基づく Meta Scan のリライト時に、Union Node の Nullable 属性が不一致となる問題を修正しました。 [#67051](https://github.com/StarRocks/starrocks/pull/67051)
- `PARTITION BY` および `ORDER BY` が指定されていない Ranking ウィンドウ関数において、最適化ロジックが原因で BE がクラッシュする問題を修正しました。 [#67094](https://github.com/StarRocks/starrocks/pull/67094)
- ウィンドウ関数と組み合わせた Group Execution Join において、誤った結果が返される可能性がある問題を修正しました。 [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- 特定のフィルタ条件下で `PartitionColumnMinMaxRewriteRule` が誤った結果を生成する問題を修正しました。 [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- 集約後の Union 処理において Nullable 属性の推論が誤っていた問題を修正しました。 [#65429](https://github.com/StarRocks/starrocks/pull/65429)
- 圧縮パラメータ処理時に `percentile_approx_weighted` がクラッシュする問題を修正しました。 [#64838](https://github.com/StarRocks/starrocks/pull/64838)
- 大きな文字列エンコーディングを伴う Spill 処理中にクラッシュが発生する問題を修正しました。 [#61495](https://github.com/StarRocks/starrocks/pull/61495)
- ローカル TopN のプッシュダウン時に `set_collector` が複数回呼び出され、クラッシュが発生する問題を修正しました。 [#66199](https://github.com/StarRocks/starrocks/pull/66199)
- LowCardinality リライトロジックにおける依存関係推論エラーを修正しました。 [#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset のコミット失敗時に Rowset ID がリークする問題を修正しました。 [#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache におけるロック競合の問題を修正しました。 [#66637](https://github.com/StarRocks/starrocks/pull/66637)
- 条件付き更新と列モード部分更新を併用した場合に、インジェストが失敗する問題を修正しました。 [#66139](https://github.com/StarRocks/starrocks/pull/66139)
- ALTER 操作中に Tablet が削除されることで、並行インポートが失敗する問題を修正しました。 [#65396](https://github.com/StarRocks/starrocks/pull/65396)
- RocksDB のイテレーションタイムアウトにより Tablet メタデータのロードが失敗する問題を修正しました。 [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- Shared-data クラスタにおいて、テーブル作成および Schema Change 時に圧縮設定が適用されない問題を修正しました。 [#65673](https://github.com/StarRocks/starrocks/pull/65673)
- アップグレード時の Delete Vector における CRC32 互換性問題を修正しました。 [#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Clone タスク失敗後のファイルクリーンアップ処理におけるステータスチェックロジックの誤りを修正しました。 [#65709](https://github.com/StarRocks/starrocks/pull/65709)
- `INSERT OVERWRITE` 実行後の統計情報収集ロジックが異常となる問題を修正しました。 [#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- FE 再起動後に外部キー制約が失われる問題を修正しました。 [#66474](https://github.com/StarRocks/starrocks/pull/66474)
- Warehouse 削除後にメタデータ取得が失敗する問題を修正しました。 [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- 高い選択度のフィルタ条件下で、監査ログのスキャン統計が不正確になる問題を修正しました。 [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- クエリエラー率メトリクスの計算ロジックが誤っていた問題を修正しました。 [#65891](https://github.com/StarRocks/starrocks/pull/65891)
- タスク終了時に MySQL 接続がリークする可能性がある問題を修正しました。 [#66829](https://github.com/StarRocks/starrocks/pull/66829)
- SIGSEGV クラッシュ発生時に BE ステータスが即時更新されない問題を修正しました。 [#66212](https://github.com/StarRocks/starrocks/pull/66212)
- LDAP ユーザーのログイン処理中に NPE が発生する問題を修正しました。 [#65843](https://github.com/StarRocks/starrocks/pull/65843)
- HTTP SQL リクエストでユーザー切り替えを行った際のエラーログが不正確な問題を修正しました。 [#65371](https://github.com/StarRocks/starrocks/pull/65371)
- TCP 接続再利用時に HTTP コンテキストがリークする問題を修正しました。 [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Follower から転送されたクエリにおいて、Profile ログに QueryDetail が欠落する問題を修正しました。 [#64395](https://github.com/StarRocks/starrocks/pull/64395)
- 監査ログに Prepare / Execute の詳細が記録されない問題を修正しました。 [#65448](https://github.com/StarRocks/starrocks/pull/65448)
- HyperLogLog のメモリ割り当て失敗によりクラッシュする問題を修正しました。 [#66747](https://github.com/StarRocks/starrocks/pull/66747)
- `trim` 関数のメモリ予約処理に関する問題を修正しました。 [#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- CVE-2025-66566 および CVE-2025-12183 に対応しました。 [#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053)
- Exec Group Driver のサブミッション処理における競合状態を修正しました。 [#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Pipeline のカウントダウン処理における use-after-free のリスクを修正しました。 [#65940](https://github.com/StarRocks/starrocks/pull/65940)
- キューがクローズされた際に `MemoryScratchSinkOperator` がハングする問題を修正しました。 [#66041](https://github.com/StarRocks/starrocks/pull/66041)
- ファイルシステムキャッシュのキー衝突問題を修正しました。 [#65823](https://github.com/StarRocks/starrocks/pull/65823)
- `SHOW PROC '/compactions'` におけるサブタスク数の表示誤りを修正しました。 [#67209](https://github.com/StarRocks/starrocks/pull/67209)
- Query Profile API が統一された JSON 形式を返さない問題を修正しました。 [#67077](https://github.com/StarRocks/starrocks/pull/67077)
- `getTable` の例外処理が不適切で、マテリアライズドビューのチェックに影響する問題を修正しました。 [#67224](https://github.com/StarRocks/starrocks/pull/67224)
- ネイティブテーブルとクラウドネイティブテーブルで `DESC` 文の `Extra` 列の出力が不一致となる問題を修正しました。 [#67238](https://github.com/StarRocks/starrocks/pull/67238)
- 単一ノード構成における競合状態の問題を修正しました。 [#67215](https://github.com/StarRocks/starrocks/pull/67215)
- サードパーティライブラリからのログ漏洩を修正しました。 [#67129](https://github.com/StarRocks/starrocks/pull/67129)
- REST Catalog の認証ロジック不備により認証に失敗する問題を修正しました。 [#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

リリース日：2025 年 12 月 25 日

### 改善点

- STRUCT データ型に対する `ORDER BY` 句をサポートしました。[#66035](https://github.com/StarRocks/starrocks/pull/66035)
- プロパティ付き Iceberg ビューの作成をサポートし、`SHOW CREATE VIEW` の出力にプロパティを表示できるようになりました。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- `ALTER TABLE ADD/DROP PARTITION COLUMN` による Iceberg テーブルのパーティション Spec の変更をサポートしました。[#65922](https://github.com/StarRocks/starrocks/pull/65922)
- フレーム付きウィンドウ（例：`ORDER BY` / `PARTITION BY`）上での `COUNT/SUM/AVG(DISTINCT)` 集約をサポートし、最適化オプションを追加しました。[#65815](https://github.com/StarRocks/starrocks/pull/65815)
- 単一文字区切り文字に `memchr` を使用することで、CSV パース性能を最適化しました。[#63715](https://github.com/StarRocks/starrocks/pull/63715)
- ネットワークオーバーヘッド削減のため、Partial TopN を事前集約（Pre-Aggregation）フェーズにプッシュダウンするオプティマイザルールを追加しました。[#61497](https://github.com/StarRocks/starrocks/pull/61497)
- Data Cache の監視機能を強化しました：
  - メモリ／ディスクのクォータおよび使用量に関する新しいメトリクスを追加しました。[#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - `api/datacache/stat` HTTP エンドポイントに Page Cache の統計情報を追加しました。[#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - ネイティブテーブルのヒット率統計を追加しました。[#66198](https://github.com/StarRocks/starrocks/pull/66198)
- OOM 発生時に迅速にメモリを解放できるよう、Sort および Aggregation オペレーターを最適化しました。[#66157](https://github.com/StarRocks/starrocks/pull/66157)
- 共有データクラスターにおいて、CN が必要なスキーマをオンデマンドで取得できるよう、FE に `TableSchemaService` を追加しました。[#66142](https://github.com/StarRocks/starrocks/pull/66142)
- すべての依存する取り込みジョブが完了するまで履歴スキーマを保持するよう、Fast Schema Evolution を最適化しました。[#65799](https://github.com/StarRocks/starrocks/pull/65799)
- `filterPartitionsByTTL` を強化し、NULL パーティション値を正しく処理することで、全パーティションが誤って除外される問題を防止しました。[#65923](https://github.com/StarRocks/starrocks/pull/65923)
- `FusedMultiDistinctState` を最適化し、リセット時に関連する MemPool を解放するようにしました。[#66073](https://github.com/StarRocks/starrocks/pull/66073)
- Iceberg REST Catalog において、`ICEBERG_CATALOG_SECURITY` プロパティのチェックを大文字・小文字を区別しないようにしました。[#66028](https://github.com/StarRocks/starrocks/pull/66028)
- 共有データクラスター向けに、StarOS Service ID を取得する HTTP エンドポイント `GET /service_id` を追加しました。[#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Kafka コンシューマー設定において、非推奨の `metadata.broker.list` を `bootstrap.servers` に置き換えました。[#65437](https://github.com/StarRocks/starrocks/pull/65437)
- Full Vacuum Daemon を無効化できる FE 設定項目 `lake_enable_fullvacuum`（デフォルト：false）を追加しました。[#66685](https://github.com/StarRocks/starrocks/pull/66685)
- lz4 ライブラリを v1.10.0 に更新しました。[#67080](https://github.com/StarRocks/starrocks/pull/67080)

### バグ修正

以下の問題を修正しました：

- `latest_cached_tablet_metadata` により、バッチ Publish 中にバージョンが誤ってスキップされる可能性がありました。[#66558](https://github.com/StarRocks/starrocks/pull/66558)
- 共有なしクラスター実行時に、`CatalogRecycleBin` 内の `ClusterSnapshot` の相対チェックが引き起こす可能性のある問題。[#66501](https://github.com/StarRocks/starrocks/pull/66501)
- Spill 処理中に、複雑なデータ型（ARRAY / MAP / STRUCT）を Iceberg テーブルへ書き込む際に BE がクラッシュする問題。[#66209](https://github.com/StarRocks/starrocks/pull/66209)
- writer の初期化または初回書き込みが失敗した場合に、Connector Chunk Sink がハングする可能性がある問題。[#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink において、`PartitionChunkWriter` の初期化失敗により、クローズ時に Null Pointer 参照が発生する問題。[#66097](https://github.com/StarRocks/starrocks/pull/66097)
- 存在しないシステム変数を設定した際に、エラーが返されず成功してしまう問題。[#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Data Cache が破損している場合に、Bundle メタデータの解析が失敗する問題。[#66021](https://github.com/StarRocks/starrocks/pull/66021)
- 結果が空の場合に、MetaScan が count 列に対して 0 ではなく NULL を返す問題。[#66010](https://github.com/StarRocks/starrocks/pull/66010)
- 旧バージョンで作成されたリソースグループに対し、`SHOW VERBOSE RESOURCE GROUP ALL` が `default_mem_pool` ではなく NULL を表示する問題。[#65982](https://github.com/StarRocks/starrocks/pull/65982)
- `flat_json` テーブル設定を無効化した後、クエリ実行中に `RuntimeException` が発生する問題。[#65921](https://github.com/StarRocks/starrocks/pull/65921)
- 共有データクラスターにおいて、Schema Change 後に `min` / `max` 統計を MetaScan に書き換える際に発生する型不一致の問題。[#65911](https://github.com/StarRocks/starrocks/pull/65911)
- `PARTITION BY` および `ORDER BY` が指定されていない場合に、ランキングウィンドウ最適化によって BE がクラッシュする問題。[#67093](https://github.com/StarRocks/starrocks/pull/67093)
- 実行時フィルター統合時の `can_use_bf` 判定が不正確で、誤った結果やクラッシュを引き起こす可能性がある問題。[#67062](https://github.com/StarRocks/starrocks/pull/67062)
- 実行時 bitset フィルターをネストした OR 述語にプッシュダウンした際に、結果が不正になる問題。[#67061](https://github.com/StarRocks/starrocks/pull/67061)
- DeltaWriter 完了後の書き込みや flush 処理により、データ競合やデータ損失が発生する可能性がある問題。[#66966](https://github.com/StarRocks/starrocks/pull/66966)
- 単純集約を MetaScan に書き換える際、nullable 属性の不一致により実行エラーが発生する問題。[#67068](https://github.com/StarRocks/starrocks/pull/67068)
- MetaScan 書き換えルールにおける行数計算が正しくない問題。[#66967](https://github.com/StarRocks/starrocks/pull/66967)
- Tablet メタデータキャッシュの不整合により、バッチ Publish 中にバージョンが誤ってスキップされる可能性がある問題。[#66575](https://github.com/StarRocks/starrocks/pull/66575)
- HyperLogLog 処理において、メモリ割り当て失敗時のエラーハンドリングが不適切な問題。[#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

リリース日：2025年12月4日

### 新機能

- 新しいリソースグループ属性 `mem_pool` を追加しました。複数のリソースグループが同じメモリプールを共有し、そのプールに対して共同のメモリ上限を適用できます。本機能は後方互換性があります。`mem_pool` が指定されていない場合は `default_mem_pool` が使用されます。[#64112](https://github.com/StarRocks/starrocks/pull/64112)

### 改善点

- File Bundling 有効時の Vacuum におけるリモートストレージアクセスを削減しました。[#65793](https://github.com/StarRocks/starrocks/pull/65793)
- File Bundling 機能が最新のタブレットメタデータをキャッシュするようになりました。[#65640](https://github.com/StarRocks/starrocks/pull/65640)
- 長い文字列を扱うシナリオでの安全性と安定性を向上しました。[#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)
- `SplitTopNAggregateRule` のロジックを最適化し、性能劣化を回避しました。[#65478](https://github.com/StarRocks/starrocks/pull/65478)
- Iceberg/DeltaLake と同様のテーブル統計収集戦略を他の外部データソースに適用し、単一テーブルの場合に不要な統計収集を行わないよう改善しました。[#65430](https://github.com/StarRocks/starrocks/pull/65430)
- Data Cache HTTP API `api/datacache/app_stat` に Page Cache 指標を追加しました。[#65341](https://github.com/StarRocks/starrocks/pull/65341)
- ORC ファイルの分割をサポートし、大規模 ORC ファイルの並列スキャンが可能になりました。[#65188](https://github.com/StarRocks/starrocks/pull/65188)
- 最適化エンジンに IF 述語の選択率推定を追加しました。[#64962](https://github.com/StarRocks/starrocks/pull/64962)
- FE が `DATE` および `DATETIME` 型に対する `hour`、`minute`、`second` の定数評価をサポートしました。[#64953](https://github.com/StarRocks/starrocks/pull/64953)
- 単純な集計を MetaScan に書き換える機能をデフォルトで有効化しました。[#64698](https://github.com/StarRocks/starrocks/pull/64698)
- 共有データクラスタにおける複数レプリカ割り当ての処理を改善し、信頼性を強化しました。[#64245](https://github.com/StarRocks/starrocks/pull/64245)
- 監査ログおよびメトリクスでキャッシュヒット率を公開しました。[#63964](https://github.com/StarRocks/starrocks/pull/63964)
- HyperLogLog またはサンプリングにより、ヒストグラムのバケットごとの重複除外数（NDV）推定を実施し、述語や JOIN に対してより正確な NDV を提供します。[#58516](https://github.com/StarRocks/starrocks/pull/58516)
- SQL 標準セマンティクスに準拠した FULL OUTER JOIN USING をサポートしました。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- オプティマイザのタイムアウト時にメモリ情報を出力して診断を支援します。[#65206](https://github.com/StarRocks/starrocks/pull/65206)

### バグ修正

以下の問題を修正しました：

- DECIMAL56 の `mod` 演算に関する問題。[#65795](https://github.com/StarRocks/starrocks/pull/65795)
- Iceberg のスキャンレンジ処理に関する問題。[#65658](https://github.com/StarRocks/starrocks/pull/65658)
- 一時パーティションおよびランダム bucket における MetaScan 書き換え問題。[#65617](https://github.com/StarRocks/starrocks/pull/65617)
- 透明なマテリアライズドビュー書き換え後に `JsonPathRewriteRule` が誤ったテーブルを参照する問題。[#65597](https://github.com/StarRocks/starrocks/pull/65597)
- `partition_retention_condition` が生成列を参照している場合のマテリアライズドビューのリフレッシュ失敗。[#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg の min/max 値の型に関する問題。[#65551](https://github.com/StarRocks/starrocks/pull/65551)
- `enable_evaluate_schema_scan_rule=true` の場合に異なるデータベースを跨いで `information_schema.tables` および `views` をクエリする際の問題。[#65533](https://github.com/StarRocks/starrocks/pull/65533)
- JSON 配列比較における整数オーバーフロー。[#64981](https://github.com/StarRocks/starrocks/pull/64981)
- MySQL Reader が SSL をサポートしていない問題。[#65291](https://github.com/StarRocks/starrocks/pull/65291)
- SVE ビルド非互換性による ARM ビルド問題。[#65268](https://github.com/StarRocks/starrocks/pull/65268)
- bucket-aware 実行に基づくクエリが bucketed Iceberg テーブルでハングする可能性がある問題。[#65261](https://github.com/StarRocks/starrocks/pull/65261)
- OLAP テーブルスキャンでメモリ制限チェックが不足していたことによるエラー伝播およびメモリ安全性の問題。[#65131](https://github.com/StarRocks/starrocks/pull/65131)

### 動作の変更

- マテリアライズドビューが非アクティブ化されると、その依存マテリアライズドビューも再帰的に非アクティブ化されます。[#65317](https://github.com/StarRocks/starrocks/pull/65317)
- SHOW CREATE の生成時に、コメントやフォーマットを含む元のマテリアライズドビュー定義 SQL を使用します。[#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

リリース日：2025年11月17日

### 改善点

- TaskRun セッション変数の処理を最適化し、既知の変数のみを処理するようにしました。 [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- デフォルトで Iceberg および Delta Lake テーブルの統計情報をメタデータから収集できるようになりました。 [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- bucket および truncate パーティション変換を使用する Iceberg テーブルの統計情報収集をサポートしました。 [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- デバッグのために FE `/proc` プロファイルの確認をサポートしました。 [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- Iceberg REST カタログでの OAuth2 および JWT 認証サポートを強化しました。 [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- バンドルタブレットのメタデータ検証およびリカバリ処理を改善しました。 [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- スキャン範囲のメモリ見積もりロジックを改善しました。 [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### バグ修正

以下の問題を修正しました：

- バンドルタブレットを公開する際にトランザクションログが削除される問題を修正しました。 [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- join 後にソートプロパティがリセットされないため、join アルゴリズムがソート特性を保持できない問題を修正しました。 [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- 透過的なマテリアライズドビューのリライトに関連する問題を修正しました。 [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### 動作変更

- Iceberg カタログに `enable_iceberg_table_cache` プロパティを追加し、Iceberg テーブルキャッシュを無効化して常に最新データを読み込むように設定可能にしました。 [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- `INSERT ... SELECT` 実行前に外部テーブルをリフレッシュし、最新のメタデータを読み取るようにしました。 [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- ロックテーブルスロット数を 256 に増加し、スロー・ロックログに `rid` フィールドを追加しました。 [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- イベントベースのスケジューリングとの非互換性により、`shared_scan` を一時的に無効化しました。 [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- Hive カタログのキャッシュ TTL のデフォルト値を 24 時間に変更し、未使用のパラメータを削除しました。 [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- セッション変数および挿入列数に基づいて Partial Update モードを自動判定するようにしました。 [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

リリース日：2025年10月17日

### データレイク分析

- BE メタデータ用の Page Cache と Data Cache を統合し、スケーリングに適応的な戦略を採用。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Iceberg 統計情報のメタデータファイル解析を最適化し、繰り返し解析を回避。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Iceberg メタデータに対する COUNT/MIN/MAX クエリを最適化し、データファイルスキャンを効率的にスキップすることで、大規模パーティションテーブルの集約クエリ性能を大幅に向上させ、リソース消費を削減。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- プロシージャ `rewrite_data_files` により Iceberg テーブルの Compaction をサポート。
- 隠しパーティション（Hidden Partition）を持つ Iceberg テーブルをサポート（作成、書き込み、読み取りを含む）。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Iceberg テーブル作成時のソートキー設定をサポートします。
- Iceberg テーブル向けのシンクパフォーマンスを最適化します。
  - Iceberg シンクは、メモリ使用量の最適化と小ファイル問題の解決のため、大規模オペレータのスパイリング、グローバルシャッフル、ローカルソートをサポートします。[#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg シンクは、Spill Partition Writer に基づくローカルソートを最適化し、書き込み効率を向上させます。[#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg シンクはパーティションのグローバルシャッフルをサポートし、小ファイルをさらに削減します。[#62123](https://github.com/StarRocks/starrocks/pull/62123)
- Iceberg テーブルのバケット対応実行を強化し、バケット化テーブルの並行処理能力と分散処理能力を向上させました。[#61756](https://github.com/StarRocks/starrocks/pull/61756)
- Paimon カタログで TIME データ型をサポート。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Iceberg バージョンを 1.10.0 にアップグレード。[#63667](https://github.com/StarRocks/starrocks/pull/63667)

### セキュリティと認証

- JWT 認証と Iceberg REST Catalog を利用するシナリオで、StarRocks は REST Session Catalog を介してユーザーログイン情報を Iceberg に透過し、その後のデータアクセス認証をサポート。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg カタログ用の Vended Credential をサポート。
- Group Provider 経由で取得した外部グループへの StarRocks 内部ロールの付与をサポート。[#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- 外部テーブルのリフレッシュ権限を制御するため、外部テーブルに REFRESH 権限を追加しました。[#63385](https://github.com/StarRocks/starrocks/pull/62636)

<!--
- StarRocks FE 側で証明書を設定することで HTTPS をサポートし、クラウドやイントラネットでの暗号化通信要件を満たす安全なシステムアクセスを実現。[#56394](https://github.com/StarRocks/starrocks/pull/56394)
- BE ノード間の HTTPS 通信をサポートし、データ伝送の暗号化と完全性を保証。内部データ漏洩や中間者攻撃を防止。[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### ストレージ最適化とクラスタ管理

- 共有データクラスタのクラウドネイティブテーブルにファイルバンドル（File Bundling）最適化を導入。ロード、Compaction、Publish 操作によって生成されるデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減。ファイルバンドリングは、v4.0 以降で作成されたテーブルに対してデフォルトで有効化されています。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- 複数テーブル間の Write-Write トランザクション（Multi-Table Write-Write Transaction）をサポートし、INSERT、UPDATE、DELETE 操作のアトミックコミットを制御可能。Stream Load および INSERT INTO インターフェイスをサポートし、ETL やリアルタイム書き込みシナリオにおけるクロステーブルの一貫性を保証。[#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Routine Load で Kafka 4.0 をサポート。
- 共有なしクラスタの主キーテーブルに対する全文インバーテッドインデックスをサポート。
- 集約テーブルの集約キーの変更をサポート。[#62253](https://github.com/StarRocks/starrocks/issues/62253)s
- カタログ、データベース、テーブル、ビュー、マテリアライズドビューの名前に対して大文字小文字を区別しない処理を有効化可能。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 共有データクラスタにおける Compute  Node のブラックリスト化をサポート。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- グローバル接続 ID をサポート。[#57256](https://github.com/StarRocks/starrocks/pull/57276)
- 復元可能な削除済みメタデータを表示するため、Information Schema に `recyclebin_catalogs` メタデータビューを追加しました。[#51007](https://github.com/StarRocks/starrocks/pull/51007)

### クエリと性能改善

- DECIMAL256 データ型をサポートし、精度の上限を 38 ビットから 76 ビットに拡張。256 ビットのストレージにより、高精度が求められる金融や科学計算シナリオに柔軟に対応し、大規模集約や高次演算での DECIMAL128 の精度オーバーフロー問題を効果的に緩和。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- 基本演算子のパフォーマンスを改善しました。[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- JOIN および AGG 演算子の性能を最適化。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] SQL Plan Manager を導入し、クエリとクエリプランをバインド可能に。これにより、システム状態（データ更新や統計更新）の変化によるクエリプランの変動を防止し、クエリ性能を安定化。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Partition-wise Spillable Aggregate/Distinct 演算子を導入し、従来のソートベース集約による Spill 実装を置き換え。複雑かつ高カーディナリティの GROUP BY シナリオで集約性能を大幅に改善し、読み書き負荷を削減。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - テーブルレベルで Flat JSON を設定可能。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - JSON カラム型ストレージを強化：V1 メカニズムを維持しつつ、Page レベルおよび Segment レベルのインデックス（ZoneMap、ブルームフィルター）、遅延マテリアライゼーションを伴う述語プッシュダウン、辞書エンコーディング、低カーディナリティのグローバル辞書の統合を追加し、実行効率を大幅に向上。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- STRING データ型向けに適応型 ZoneMap インデックス作成戦略をサポート。[#61960](https://github.com/StarRocks/starrocks/issues/61960)
- クエリ可視性の強化:
  - EXPLAIN ANALYZEの出力を最適化し、実行メトリクスをグループ別および演算子別に表示することで可読性を向上。[#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` および `QueryProfileActionV2` が JSON 形式をサポートし、クロス FE クエリ機能が強化されました。[#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - 全 FE にわたるクエリプロファイル情報の取得をサポート。[#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST 文でカタログ、クエリ ID などの情報を表示。[#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - クエリキューとプロセス監視を強化し、Running/Pending のステータス表示をサポート。[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- マテリアライズドビューの再作成では、元のテーブルの分散キーとソートキーを考慮し、最適なマテリアライズドビューの選択を改善。[#62830](https://github.com/StarRocks/starrocks/pull/62830)

### 関数と SQL 構文

- 以下の関数を追加：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` および `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` および `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- 以下の構文拡張を提供：
  - CREATE ANALYZE FULL TABLE で IF NOT EXISTS キーワードをサポート。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT で EXCLUDE 句をサポート。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 集約関数で FILTER 句をサポートし、条件付き集約の可読性と実行効率を向上。[#58937](https://github.com/StarRocks/starrocks/pull/58937)

### 動作変更

- マテリアライズドビューのパラメータ `auto_partition_refresh_number` のロジックを調整し、自動リフレッシュか手動リフレッシュかにかかわらず、リフレッシュ対象のパーティション数を制限します。[#62301](https://github.com/StarRocks/starrocks/pull/62301)
- Flat JSON がデフォルトで有効化されました。[#62097](https://github.com/StarRocks/starrocks/pull/62097)
- システム変数 `enable_materialized_view_agg_pushdown_rewrite` のデフォルト値が `true` に設定され、マテリアライズドビューのクエリ書き換えにおける集計プッシュダウンがデフォルトで有効化されました。[#60976](https://github.com/StarRocks/starrocks/pull/60976)
- `information_schema.materialized_views` 内のいくつかの列の型を変更し、対応するデータとの整合性を高めました。[#60054](https://github.com/StarRocks/starrocks/pull/60054)
- `split_part` 関数は区切り文字が一致しない場合に NULL を返すように変更。[#56967](https://github.com/StarRocks/starrocks/pull/56967)
- CTAS/CREATE MATERIALIZED VIEW で固定長 CHAR を STRING に置き換え、誤った列長推論によるマテリアライズドビュー更新失敗を回避。[#63114](https://github.com/StarRocks/starrocks/pull/63114) [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- データキャッシュ関連の構成が簡素化されました。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` および `datacache_disk_size` が有効になりました。
  - `storage_page_cache_limit`、`block_cache_mem_size`、`block_cache_disk_size` は非推奨となりました。
- Hive および Iceberg のメタデータキャッシュに使用するメモリリソースを制限する新しいカタログプロパティを追加（Hive 用 `remote_file_cache_memory_ratio`、Iceberg 用 `iceberg_data_file_cache_memory_usage_ratio` および `iceberg_delete_file_cache_memory_usage_ratio`）。デフォルト値を `0.1`（10%）に設定。メタデータキャッシュの TTL を24時間に調整。[#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION は、同じバケットシーケンス番号を持つすべてのマテリアライズドインデックスの統計情報を統合しなくなりました。マテリアライズドインデックスレベルでのデータ分散のみを表示します。[#59656](https://github.com/StarRocks/starrocks/pull/59656)
- 自動バケットテーブルのデフォルトバケットサイズを4GBから1GBに変更し、パフォーマンスとリソース利用率を改善しました。[#63168](https://github.com/StarRocks/starrocks/pull/63168)
- システムは対応するセッション変数と INSERT 文の列数に基づいて部分更新モードを決定します。[#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Information Schema 内の `fe_tablet_schedules` ビューを最適化しました。[#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - `TABLET_STATUS` 列を `SCHEDULE_REASON` に、`CLONE_SRC` 列を `SRC_BE_ID` に、`CLONE_DEST` 列を `DEST_BE_ID` に名称変更しました。
  - `CREATE_TIME`、`SCHEDULE_TIME` および `FINISH_TIME` 列のデータ型を `DOUBLE` から `DATETIME` に変更しました。
- 一部の FE メトリクスに `is_leader` ラベルを追加しました。[#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Microsoft Azure Blob Storage および Data Lake Storage Gen 2 をオブジェクト ストレージとして使用する共有データクラスターは、v4.0 へのアップグレード後にデータキャッシュの障害が発生します。システムは自動的にキャッシュを再読み込みします。
