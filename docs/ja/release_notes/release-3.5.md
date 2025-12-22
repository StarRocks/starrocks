---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

- StarRocks を v3.5 にアップグレードした後、直接 v3.4.0 ~ v3.4.4 にダウングレードしないでください。そうするとメタデータの非互換性を引き起こす。問題を防ぐために、クラスタを v3.4.5 以降にダウングレードする必要があります。
- StarRocks を v3.5.2 以降にアップグレードした後、v3.5.0 および v3.5.1 にダウングレードしないでください。そうすると FE がクラッシュします。

:::

## 3.5.10

リリース日：2025 年 12 月 15 日

### 改善点

- BE のクラッシュログに実行計画ノード ID を出力できるようになり、問題のあるオペレーターの特定が高速化されました。 [#66454](https://github.com/StarRocks/starrocks/pull/66454)
- `information_schema` 内のシステムビューに対するスキャンを最適化し、オーバーヘッドを削減しました。 [#66200](https://github.com/StarRocks/starrocks/pull/66200)
- スローロックの可観測性を向上させるため、2 つのヒストグラムメトリクス（`slow_lock_held_time_ms`、`slow_lock_wait_time_ms`）を追加し、長時間保持されているロックと高いロック競合を区別できるようになりました。 [#66027](https://github.com/StarRocks/starrocks/pull/66027)
- Tablet レポートおよびクローン処理において、レプリカロックの粒度をデータベースレベルからテーブルレベルに変更し、ロック競合を削減してスケジューリング効率を向上させました。 [#61939](https://github.com/StarRocks/starrocks/pull/61939)
- BE ストレージ層で不要なカラムを出力しないようにし、述語計算を BE ストレージ層へプッシュダウンしました。 [#60462](https://github.com/StarRocks/starrocks/pull/60462)
- バックグラウンドスレッドで scan range をデプロイする際のクエリ Profile の精度を向上させました。 [#62223](https://github.com/StarRocks/starrocks/pull/62223)
- 追加タスクのデプロイ時における Profile の集計ロジックを改善し、CPU 時間が重複して計上されないようにしました。 [#62186](https://github.com/StarRocks/starrocks/pull/62186)
- 参照されたパーティションが存在しない場合に、より詳細なエラーメッセージを追加し、障害の切り分けを容易にしました。 [#65674](https://github.com/StarRocks/starrocks/pull/65674)
- サンプリング型のカーディナリティ推定をコーナーケースでより堅牢にし、行数推定の精度を向上させました。 [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- 統計情報のロード時にパーティションフィルタを追加し、INSERT OVERWRITE が古いパーティション統計を読み込まないようにしました。 [#65578](https://github.com/StarRocks/starrocks/pull/65578)
- Pipeline CPU の `execution_time` メトリクスをクエリ用とロード用に分離し、ワークロード種別ごとの可観測性を向上させました。 [#65535](https://github.com/StarRocks/starrocks/pull/65535)
- `enable_statistic_collect_on_first_load` をテーブル単位で設定可能にし、初回ロード時の統計収集をより細かく制御できるようにしました。 [#65463](https://github.com/StarRocks/starrocks/pull/65463)
- S3 に依存するユニットテストの名前を `PocoClientTest` から、依存関係と目的を明確に表す S3 専用の名前に変更しました。 [#65524](https://github.com/StarRocks/starrocks/pull/65524)

### バグ修正

以下の問題を修正しました：

- 非互換な JDK で StarRocks を起動した際に libhdfs がクラッシュする問題。 [#65882](https://github.com/StarRocks/starrocks/pull/65882)
- `PartitionColumnMinMaxRewriteRule` による誤ったクエリ結果。 [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- AST キーによる物化ビュー解決時に、メタデータが更新されずリライトに失敗する問題。 [#66472](https://github.com/StarRocks/starrocks/pull/66472)
- 特定の Unicode 空白文字をトリムする際に、trim 関数がクラッシュまたは誤った結果を返す問題。 [#66428](https://github.com/StarRocks/starrocks/pull/66428), [#66477](https://github.com/StarRocks/starrocks/pull/66477)
- 削除済み Warehouse を参照し続けることで発生するロードメタデータおよび SQL 実行エラー。 [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- グループ実行 Join とウィンドウ関数を組み合わせた場合の誤った結果。 [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- `resetDecommStatForSingleReplicaTabletUnlocked` における FE の NullPointerException の可能性。 [#66034](https://github.com/StarRocks/starrocks/pull/66034)
- 共有データクラスタにおいて、`LakeDataSource` で Join ランタイムフィルタのプッシュダウン最適化が欠落していた問題。 [#66354](https://github.com/StarRocks/starrocks/pull/66354)
- ランタイムフィルタ送信オプション（タイムアウト、HTTP RPC 制限など）が受信側に転送されず、パラメータが不整合になる問題。 [#66393](https://github.com/StarRocks/starrocks/pull/66393)
- 既存のパーティション値がある場合に自動パーティション作成が失敗する問題。 [#66167](https://github.com/StarRocks/starrocks/pull/66167)
- 高い選択性を持つ述語がある場合、監査ログ内のスキャン統計が不正確になる問題。 [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- 非決定性関数がオペレーターの下にプッシュダウンされることで発生する誤ったクエリ結果。 [#66323](https://github.com/StarRocks/starrocks/pull/66323)
- CASE WHEN による式数の指数的増加。 [#66324](https://github.com/StarRocks/starrocks/pull/66324)
- 同一クエリ内で同じテーブルが異なるパーティション述語で複数回参照された場合の物化ビュー補償の不具合。 [#66369](https://github.com/StarRocks/starrocks/pull/66369)
- サブプロセスで fork を使用した際に BE が応答しなくなる問題。 [#66334](https://github.com/StarRocks/starrocks/pull/66334)
- CVE-2025-66566 および CVE-2025-12183 への対応。 [#66453](https://github.com/StarRocks/starrocks/pull/66453), [#66362](https://github.com/StarRocks/starrocks/pull/66362)
- ネストされた CTE の再利用によるエラー。 [#65800](https://github.com/StarRocks/starrocks/pull/65800)
- スキーマ変更句の競合に対する検証不足による問題。 [#66208](https://github.com/StarRocks/starrocks/pull/66208)
- Rowset コミット失敗時の不適切な Rowset GC 動作。 [#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Pipeline カウントダウン時の use-after-free の可能性。 [#65940](https://github.com/StarRocks/starrocks/pull/65940)
- Stream Load において `information_schema.loads` の `Warehouse` フィールドが NULL になる問題。 [#66202](https://github.com/StarRocks/starrocks/pull/66202)
- 参照先ビューがベーステーブルと同名の場合の物化ビュー作成不具合。 [#66274](https://github.com/StarRocks/starrocks/pull/66274)
- グローバル辞書が特定条件下で正しく更新されない問題。 [#66194](https://github.com/StarRocks/starrocks/pull/66194)
- Follower ノードから転送されたクエリの Profile ログが不正確な問題。 [#64395](https://github.com/StarRocks/starrocks/pull/64395)
- SELECT 結果のキャッシュおよびスキーマ再順序時の BE クラッシュ。 [#65850](https://github.com/StarRocks/starrocks/pull/65850)
- 式によるパーティション削除時にシャドウパーティションが削除される問題。 [#66171](https://github.com/StarRocks/starrocks/pull/66171)
- 同一 Tablet に CLONE タスクが存在する場合でも DROP タスクが実行される問題。 [#65780](https://github.com/StarRocks/starrocks/pull/65780)
- RocksDB のログファイル設定不備による安定性および可観測性の問題。 [#66166](https://github.com/StarRocks/starrocks/pull/66166)
- NULL 結果を返す可能性のある誤った物化ビュー補償。 [#66216](https://github.com/StarRocks/starrocks/pull/66216)
- BE が `SIGSEGV` を受信後も生存状態として報告される問題。 [#66212](https://github.com/StarRocks/starrocks/pull/66212)
- Iceberg スキャンに関する不具合。 [#65658](https://github.com/StarRocks/starrocks/pull/65658)
- Iceberg ビュー SQL テストの回帰カバレッジおよび安定性問題。 [#66126](https://github.com/StarRocks/starrocks/pull/66126)
- `set_collector` が繰り返し呼び出されることによる想定外の動作。 [#66199](https://github.com/StarRocks/starrocks/pull/66199)
- カラムモード部分更新と条件付き更新を併用した場合の取り込み失敗。 [#66139](https://github.com/StarRocks/starrocks/pull/66139)
- 同時トランザクション下での一時パーティション値競合。 [#66025](https://github.com/StarRocks/starrocks/pull/66025)
- Iceberg テーブルキャッシュにおいて、`cache.size() == 0` の場合でも Guava LocalCache が古いエントリを保持し、更新が無効化される問題。 [#65917](https://github.com/StarRocks/starrocks/pull/65917)
- `LargeInPredicateException` のフォーマット指定子が誤っており、エラーメッセージ内の件数が不正確になる問題。 [#66152](https://github.com/StarRocks/starrocks/pull/66152)
- connectContext が NULL の場合に `ConnectScheduler` のタイムアウトチェッカーで NullPointerException が発生する問題。 [#66136](https://github.com/StarRocks/starrocks/pull/66136)
- スレッドプールタスクから投げられた例外が処理されず、クラッシュにつながる問題。 [#66114](https://github.com/StarRocks/starrocks/pull/66114)
- 特定の実行計画において DISTINCT LIMIT のプッシュダウンによりデータが欠落する問題。 [#66109](https://github.com/StarRocks/starrocks/pull/66109)
- `multi_distinct_count` が二段階 HashSet へ変換後に `distinct_size` を更新せず、誤った DISTINCT 件数となる問題。 [#65916](https://github.com/StarRocks/starrocks/pull/65916)
- Exec Group が次の Driver をサブミットする際の競合状態により、`Check failed: !driver->is_in_blocked()` が発生し BE が終了する可能性。 [#66099](https://github.com/StarRocks/starrocks/pull/66099)
- デフォルト値付きの ALTER TABLE ADD COLUMN と INSERT を同時実行した際、新しいカラムのデフォルト式型不一致により INSERT が失敗する問題。 [#65968](https://github.com/StarRocks/starrocks/pull/65968)
- SparkSQL が早期終了した際、`RecordBatchQueue` シャットダウン後に `MemoryScratchSinkOperator` が `pending_finish` のままとなり、Pipeline がハングする問題。 [#66041](https://github.com/StarRocks/starrocks/pull/66041)
- 空の Row Group を含む Parquet ファイル読み取り時のコアダンプ。 [#65928](https://github.com/StarRocks/starrocks/pull/65928)
- 高 DOP 環境において、イベントスケジューラの準備判定が複雑なため再帰呼び出しやスタックオーバーフローが発生する問題。 [#66016](https://github.com/StarRocks/starrocks/pull/66016)
- Iceberg のベーステーブルに期限切れスナップショットが存在する場合、非同期物化ビュー更新がスキップされる問題。 [#65969](https://github.com/StarRocks/starrocks/pull/65969)
- オプティマイザが述語の差異判定に hashCode のみを使用することで発生する述語再利用および書き換えの問題。 [#65999](https://github.com/StarRocks/starrocks/pull/65999)
- 多段階パーティションを持つベーステーブルの非同期物化ビュー更新において、親パーティションのみがチェックされ、子パーティション更新がスキップされる問題。 [#65596](https://github.com/StarRocks/starrocks/pull/65596)
- 統計収集において、空の結果セットに対する AVG(ARRAY_LENGTH(...)) が NULL を返す問題。 [#65788](https://github.com/StarRocks/starrocks/pull/65788)
- BE および FE におけるランタイム Profile カウンタの増分更新時に min/max 値が正しく更新・クリアされない問題。 [#65869](https://github.com/StarRocks/starrocks/pull/65869)
- クラスタスナップショット作成時に image journal ID を取得するロジックが不正確な問題。 [#65970](https://github.com/StarRocks/starrocks/pull/65970)
- ファイルクリーンアップ失敗時の状態判定ロジック誤りにより、結果が誤って報告される問題。 [#65709](https://github.com/StarRocks/starrocks/pull/65709)
- DictMappingOperator の `isVariable()` が特定条件で無限ループに陥る問題。 [#65743](https://github.com/StarRocks/starrocks/pull/65743)
- scan range デプロイスレッドに ConnectContext が渡されないことによる監査ログおよび Profile データ欠落。 [#63544](https://github.com/StarRocks/starrocks/pull/63544)
- ストレージエンジン停止時のローカル主キーインデックスマネージャにおける use-after-free。 [#65534](https://github.com/StarRocks/starrocks/pull/65534)
- 動的上書きモードの INSERT OVERWRITE における統計収集の問題。 [#65657](https://github.com/StarRocks/starrocks/pull/65657)
- DiskAndTabletLoadReBalancer の粗粒度ロックによる並行性問題。 [#65557](https://github.com/StarRocks/starrocks/pull/65557)
- 重要なロックに対するスローロック検出がなく、スローロックが正しく検出・報告されない問題。 [#65559](https://github.com/StarRocks/starrocks/pull/65559)
- 対象データベース削除後に upsert トランザクション状態をリプレイする際の NullPointerException。 [#65595](https://github.com/StarRocks/starrocks/pull/65595)
- INSERT OVERWRITE による統計収集後、古いパーティション統計が削除されず、古い統計が使用される問題。 [#65586](https://github.com/StarRocks/starrocks/pull/65586)
- パーティション ID 割り当てにおけるデータ競合により、並行環境で ID 衝突が発生する問題。 [#65608](https://github.com/StarRocks/starrocks/pull/65608)
- 初期 Tablet メタデータ取得時に一部 Tablet ID が欠落する問題。 [#65550](https://github.com/StarRocks/starrocks/pull/65550)
- PREPARE / EXECUTE 文に関する監査ログおよび Profile 記録情報が不正確な問題。 [#65448](https://github.com/StarRocks/starrocks/pull/65448)
- スレッドセーフでない `has_output` 関数が複数スレッドから呼び出されることによるクラッシュの可能性。 [#65514](https://github.com/StarRocks/starrocks/pull/65514)
- `memtable_finalize_task_total` メトリクスが存在しないため、MemTable finalize タスクが正しく追跡できない問題。 [#65548](https://github.com/StarRocks/starrocks/pull/65548)
- Arrow Flight におけるクエリ ID の衝突により、複数クエリが同一クエリ ID を共有できない問題。 [#65558](https://github.com/StarRocks/starrocks/pull/65558)
- `TabletChecker.doCheck()` と他の操作間のロック競合。 [#65237](https://github.com/StarRocks/starrocks/pull/65237)
- 共有データクラスタと共有なしクラスタでスキャン挙動が一致せず、クエリの意味が異なる問題。 [#61100](https://github.com/StarRocks/starrocks/pull/61100)

## 3.5.9

リリース日：2025年11月26日

### 改善点

- トランザクション各ステージのタイミングを観測するため、FE にトランザクションレイテンシ指標を追加しました。[#64948](https://github.com/StarRocks/starrocks/pull/64948)
- データレイク環境でのテーブル全体の再書き込みを簡素化するため、S3 上の非パーティション Hive テーブルへの上書きに対応しました。[#65340](https://github.com/StarRocks/starrocks/pull/65340)
- Tablet メタデータキャッシュをより細かく制御するため、CacheOptions を導入しました。[#65222](https://github.com/StarRocks/starrocks/pull/65222)
- 最新データに整合した統計情報を維持するため、INSERT OVERWRITE のサンプル統計収集をサポートしました。[#65363](https://github.com/StarRocks/starrocks/pull/65363)
- Tablet の非同期レポートによる統計情報の欠落や誤りを避けるため、INSERT OVERWRITE 後の統計収集戦略を最適化しました。[#65327](https://github.com/StarRocks/starrocks/pull/65327)
- INSERT OVERWRITE またはマテリアライズドビューのリフレッシュ操作で削除または置き換えられたパーティションに保持期間を導入し、回収可能性を向上させるため一定期間リサイクルビンに残すようにしました。[#64779](https://github.com/StarRocks/starrocks/pull/64779)

### バグ修正

次の問題を修正しました：

- `LocalMetastore.truncateTable()` に関連するロック競合および並行性の問題。[#65191](https://github.com/StarRocks/starrocks/pull/65191)
- TabletChecker に関連するロック競合およびレプリカチェック性能の問題。[#65312](https://github.com/StarRocks/starrocks/pull/65312)
- HTTP SQL によるユーザー切り替え時の誤ったエラーロギング。[#65371](https://github.com/StarRocks/starrocks/pull/65371)
- DelVec CRC32 アップグレード互換性問題によって発生するチェックサム失敗。[#65442](https://github.com/StarRocks/starrocks/pull/65442)
- RocksDB のイテレーションタイムアウトによる Tablet メタデータ読み込み失敗。[#65146](https://github.com/StarRocks/starrocks/pull/65146)
- JSON ハイパーパスが `$` またはすべてスキップされた場合、内部 `flat_path` が空になり、`substr` 呼び出しが例外を投げ BE がクラッシュする問題。[#65260](https://github.com/StarRocks/starrocks/pull/65260)
- フラグメント実行において PREPARED フラグが正しく設定されない問題。[#65423](https://github.com/StarRocks/starrocks/pull/65423)
- 重複したロードプロファイルカウンタにより、書き込みおよびフラッシュ指標が不正確になる問題。[#65252](https://github.com/StarRocks/starrocks/pull/65252)
- 複数の HTTP リクエストが同じ TCP 接続を再利用する際、ExecuteSQL リクエストの後に非 ExecuteSQL リクエストが到着すると、チャネルクローズ時に `HttpConnectContext` が登録解除されず、HTTP コンテキストリークが発生する問題。[#65203](https://github.com/StarRocks/starrocks/pull/65203)
- MySQL 8.0 のスキーマイントロスペクションエラー（`default_authentication_plugin` と `authentication_policy` のセッション変数追加で修正）。[#65330](https://github.com/StarRocks/starrocks/pull/65330)
- パーティション上書き後に作成される一時パーティションに対して不要な統計収集が行われることで、SHOW ANALYZE STATUS がエラーになる問題。[#65298](https://github.com/StarRocks/starrocks/pull/65298)
- Event Scheduler における Global Runtime Filter の競合問題。[#65200](https://github.com/StarRocks/starrocks/pull/65200)
- Data Cache の最小ディスクサイズ制約が大きすぎるため、Data Cache が過度に無効化される問題。[#64909](https://github.com/StarRocks/starrocks/pull/64909)
- `gold` リンカの自動フォールバックに関連する aarch64 のビルド問題。[#65156](https://github.com/StarRocks/starrocks/pull/65156)

## 3.5.8

リリース日：2025年11月10日

### 改善点

- Arrow を 19.0.1 にアップグレードし、Parquet のレガシー LIST エンコーディングをサポートして、ネストされた複雑なファイルを処理可能にしました。 [#64238](https://github.com/StarRocks/starrocks/pull/64238)
- FILES() が Parquet のレガシー LIST エンコーディングをサポートしました。 [#64160](https://github.com/StarRocks/starrocks/pull/64160)
- セッション変数と挿入列数に基づいて Partial Update モードを自動的に判定するようにしました。 [#62091](https://github.com/StarRocks/starrocks/pull/62091)
- テーブル関数上の分析演算子に低カーディナリティ最適化を適用しました。 [#63378](https://github.com/StarRocks/starrocks/pull/63378)
- ブロッキングを回避するため、`finishTransaction` に設定可能なテーブルロックタイムアウトを追加しました。 [#63981](https://github.com/StarRocks/starrocks/pull/63981)
- 共有データクラスターでテーブルレベルのスキャンメトリクスの帰属をサポートしました。 [#62832](https://github.com/StarRocks/starrocks/pull/62832)
- ウィンドウ関数 LEAD/LAG/FIRST_VALUE/LAST_VALUE が ARRAY 型引数をサポートしました。 [#63547](https://github.com/StarRocks/starrocks/pull/63547)
- 複数の配列関数で定数畳み込みをサポートし、述語プッシュダウンおよび結合の単純化を改善しました。 [#63692](https://github.com/StarRocks/starrocks/pull/63692)
- `SHOW PROC /backends/{id}` 経由で指定ノードの `tabletNum` を取得するためのバッチ API を最適化しました。FE 設定項目 `enable_collect_tablet_num_in_show_proc_backend_disk_path`（デフォルト：`true`）を追加。 [#64013](https://github.com/StarRocks/starrocks/pull/64013)
- `INSERT ... SELECT` が計画前に外部テーブルを更新し、最新のメタデータを参照するようにしました。 [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- テーブル関数、NL Join Probe、Hash Join Probe に `capacity_limit_reached` チェックを追加し、過剰列の構築を防止しました。 [#64009](https://github.com/StarRocks/starrocks/pull/64009)
- 外部テーブルの統計収集タスク数の上限を設定する FE 設定項目 `collect_stats_io_tasks_per_connector_operator`（デフォルト：`4`）を追加しました。 [#64016](https://github.com/StarRocks/starrocks/pull/64016)
- サンプル収集のデフォルトパーティションサイズを 1000 から 300 に変更しました。 [#64022](https://github.com/StarRocks/starrocks/pull/64022)
- ロックテーブルスロットを 256 に増加し、スロー・ロック・ログに `rid` を追加しました。 [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- レガシーデータを扱う際の Gson デシリアライズの堅牢性を改善しました。 [#63555](https://github.com/StarRocks/starrocks/pull/63555)
- FILES() スキーマプッシュダウンにおけるメタデータロック範囲を縮小し、ロック競合と計画遅延を削減しました。 [#63796](https://github.com/StarRocks/starrocks/pull/63796)
- FE 設定項目 `task_runs_timeout_second` を導入し、Task Run の実行タイムアウト検知と期限切れタスクのキャンセルロジックを改善しました。 [#63842](https://github.com/StarRocks/starrocks/pull/63842)
- `REFRESH MATERIALIZED VIEW ... FORCE` が常に対象パーティションをリフレッシュするようにしました（不整合や破損がある場合でも）。 [#63844](https://github.com/StarRocks/starrocks/pull/63844)

### バグ修正

次の問題を修正しました：

- ClickHouse の Nullable (Decimal) 型解析時の例外。 [#64195](https://github.com/StarRocks/starrocks/pull/64195)
- タブレット移行とプライマリキー索引検索の同時実行問題。 [#64164](https://github.com/StarRocks/starrocks/pull/64164)
- マテリアライズドビュー更新で FINISHED 状態が欠落していた問題。 [#64191](https://github.com/StarRocks/starrocks/pull/64191)
- 共有データクラスターで Schema Change Publish が再試行されない問題。 [#64093](https://github.com/StarRocks/starrocks/pull/64093)
- データレイクのプライマリキー表で行数統計が誤っていた問題。 [#64007](https://github.com/StarRocks/starrocks/pull/64007)
- タブレット作成タイムアウト時にノード情報が返されない問題。 [#63963](https://github.com/StarRocks/starrocks/pull/63963)
- 破損した Lake DataCache がクリアできない問題。 [#63182](https://github.com/StarRocks/starrocks/pull/63182)
- IGNORE NULLS フラグ付きウィンドウ関数が対応する非 IGNORE NULLS 関数と統合できない問題。 [#63958](https://github.com/StarRocks/starrocks/pull/63958)
- FE 再起動後、以前中止されたコンパクションが再スケジュールされない問題。 [#63881](https://github.com/StarRocks/starrocks/pull/63881)
- FE の頻繁な再起動でタスクがスケジュールできない問題。 [#63966](https://github.com/StarRocks/starrocks/pull/63966)
- GCS のエラーコード処理に関する問題。 [#64066](https://github.com/StarRocks/starrocks/pull/64066)
- StarMgr gRPC エグゼキュータの不安定性。 [#63828](https://github.com/StarRocks/starrocks/pull/63828)
- 排他ワークグループ作成時のデッドロック。 [#63893](https://github.com/StarRocks/starrocks/pull/63893)
- Iceberg テーブルキャッシュが正しく無効化されない問題。 [#63971](https://github.com/StarRocks/starrocks/pull/63971)
- 共有データクラスターでソート集約の結果が誤っていた問題。 [#63849](https://github.com/StarRocks/starrocks/pull/63849)
- `PartitionedSpillerWriter::_remove_partition` における ASAN エラー。 [#63903](https://github.com/StarRocks/starrocks/pull/63903)
- モーセルキューからスプリットを取得できなかった場合に BE がクラッシュする問題。 [#62753](https://github.com/StarRocks/starrocks/pull/62753)
- マテリアライズドビュー書き換え時の集約プッシュダウン型キャストバグ。 [#63875](https://github.com/StarRocks/starrocks/pull/63875)
- FE で期限切れロードジョブを削除する際の NPE。 [#63820](https://github.com/StarRocks/starrocks/pull/63820)
- パーティション削除時の Partitioned Spill クラッシュ。 [#63825](https://github.com/StarRocks/starrocks/pull/63825)
- 特定のプランでマテリアライズドビュー書き換えが `IllegalStateException` をスローする問題。 [#63655](https://github.com/StarRocks/starrocks/pull/63655)
- パーティション付きマテリアライズドビュー作成時の NPE。 [#63830](https://github.com/StarRocks/starrocks/pull/63830)

## 3.5.7

リリース日：2025年10月21日

### 改善点

- メモリ競合が激しいシナリオでリトライバックオフを導入し、スキャン演算子のメモリ統計の精度を向上させました。[#63788](https://github.com/StarRocks/starrocks/pull/63788)
- 既存のタブレット分布を活用して、マテリアライズドビューのバケット推論を最適化し、過剰なバケット作成を防ぎました。[#63367](https://github.com/StarRocks/starrocks/pull/63367)
- Iceberg テーブルのキャッシュメカニズムを改訂し、一貫性を向上させ、頻繁なメタデータ更新時のキャッシュ無効化リスクを減少させました。[#63388](https://github.com/StarRocks/starrocks/pull/63388)
- `QueryDetail` と `AuditEvent` に `querySource` フィールドを追加し、API およびスケジューラー間でクエリの起源をよりよく追跡できるようにしました。[#63480](https://github.com/StarRocks/starrocks/pull/63480)
- MemTable 書き込み時に重複キーが検出された場合に、詳細なコンテキストを表示することで、永続的インデックスの診断機能を強化しました。[#63560](https://github.com/StarRocks/starrocks/pull/63560)
- ロック粒度を洗練し、並行シナリオでのシーケンシングを改善することで、マテリアライズドビュー操作におけるロック競合を減少させました。[#63481](https://github.com/StarRocks/starrocks/pull/63481)

### バグ修正

以下の問題を修正しました：

- 型の不一致により、マテリアライズドビューの再書き込みに失敗する問題。[#63659](https://github.com/StarRocks/starrocks/pull/63659)
- `regexp_extract_all` が誤った動作をし、`pos=0` をサポートしていない問題。[#63626](https://github.com/StarRocks/starrocks/pull/63626)
- 複雑な関数を含む CASE WHEN の無駄な簡略化により、スキャンパフォーマンスが低下する問題。[#63732](https://github.com/StarRocks/starrocks/pull/63732)
- 列モードから行モードへの部分更新切替時に、DCG データの読み取りが不正になる問題。[#61529](https://github.com/StarRocks/starrocks/pull/61529)
- `ExceptionStackContext` の初期化時にデッドロックが発生する可能性のある問題。[#63776](https://github.com/StarRocks/starrocks/pull/63776)
- ARMアーキテクチャのマシンで、Parquet の数値変換がクラッシュする問題。[#63294](https://github.com/StarRocks/starrocks/pull/63294)
- 集約中間型が `ARRAY<NULL_TYPE>` を使用している問題。[#63371](https://github.com/StarRocks/starrocks/pull/63371)
- LARGEINT を DECIMAL128 にキャストする際、符号の端点でのオーバーフロー検出が誤って行われることによる安定性の問題。[#63559](https://github.com/StarRocks/starrocks/pull/63559)
- LZ4 の圧縮および解凍エラーが認識できない問題。[#63629](https://github.com/StarRocks/starrocks/pull/63629)
- `FROM_UNIXTIME` でパーティションされたテーブルのクエリ実行時に `ClassCastException` が発生する問題。[#63684](https://github.com/StarRocks/starrocks/pull/63684)
- `DECOMMISSION` としてマークされた唯一の有効なソースレプリカで、バランス駆動の移行後にタブレットが修復できない問題。[#62942](https://github.com/StarRocks/starrocks/pull/62942)
- `PREPARE` ステートメントを使用した場合、SQL ステートメントとプランナートレースが失われる問題。[#63519](https://github.com/StarRocks/starrocks/pull/63519)
- `extract_number`、`extract_bool`、`extract_string` 関数が例外に安全でない問題。[#63575](https://github.com/StarRocks/starrocks/pull/63575)
- シャットダウンされたタブレットが適切にガーベジコレクトされない問題。[#63595](https://github.com/StarRocks/starrocks/pull/63595)
- `PREPARE`/`EXECUTE` ステートメントの返却結果がプロファイルで`omit`として表示される問題。[#62988](https://github.com/StarRocks/starrocks/pull/62988)
- 組み合わせた述語による `date_trunc` パーティションプルーニングが誤って EMPTYSET を生成する問題。[#63464](https://github.com/StarRocks/starrocks/pull/63464)
- `NullableColumn` での CHECK によりリリースビルドでクラッシュが発生する問題。[#63553](https://github.com/StarRocks/starrocks/pull/63553)

## 3.5.6

リリース日: 2025年9月22日

### 改善点

- 廃止済み BE のすべてのタブレットがリサイクルビンにある場合、その BE を強制的に削除し、タブレットによる廃止処理のブロックを回避します。 [#62781](https://github.com/StarRocks/starrocks/pull/62781)
- Vacuum 成功時に Vacuum メトリクスを更新します。 [#62540](https://github.com/StarRocks/starrocks/pull/62540)
- フラグメントインスタンス実行状態レポートにスレッドプールのメトリクスを追加しました。アクティブスレッド数、キュー数、実行中スレッド数を含みます。 [#63067](https://github.com/StarRocks/starrocks/pull/63067)
- 共有データクラスターで S3 パススタイルアクセスをサポートし、MinIO などの S3 互換ストレージとの互換性を向上しました。ストレージボリューム作成時に `aws.s3.enable_path_style_access` を `true` に設定して有効化できます。 [#62591](https://github.com/StarRocks/starrocks/pull/62591)
- `ALTER TABLE <table_name> AUTO_INCREMENT = 10000;` による AUTO_INCREMENT 値の開始位置リセットをサポートしました。 [#62767](https://github.com/StarRocks/starrocks/pull/62767)
- グループプロバイダーで Distinguished Name (DN) を使用したグループマッチングをサポートし、LDAP/Microsoft Active Directory 環境でのグループ解決を改善しました。 [#62711](https://github.com/StarRocks/starrocks/pull/62711)
- Azure Data Lake Storage Gen2 に対して Azure Workload Identity 認証をサポートしました。 [#62754](https://github.com/StarRocks/starrocks/pull/62754)
- `information_schema.loads` ビューにトランザクションエラーメッセージを追加し、障害診断を容易にしました。 [#61364](https://github.com/StarRocks/starrocks/pull/61364)
- 複雑な CASE WHEN 式を含む Scan プレディケートで共通式の再利用をサポートし、重複計算を削減しました。 [#62779](https://github.com/StarRocks/starrocks/pull/62779)
- マテリアライズドビューの REFRESH 実行に ALTER 権限ではなく REFRESH 権限を使用するように変更しました。 [#62636](https://github.com/StarRocks/starrocks/pull/62636)
- 潜在的な問題を避けるため、Lake テーブルでの低カーディナリティ最適化をデフォルトで無効化しました。 [#62586](https://github.com/StarRocks/starrocks/pull/62586)
- 共有データクラスターでタブレットのワーカー間バランシングをデフォルトで有効化しました。 [#62661](https://github.com/StarRocks/starrocks/pull/62661)
- 外部結合の WHERE プレディケートでの式再利用をサポートし、重複計算を削減しました。 [#62139](https://github.com/StarRocks/starrocks/pull/62139)
- FE に Clone メトリクスを追加しました。 [#62421](https://github.com/StarRocks/starrocks/pull/62421)
- BE に Clone メトリクスを追加しました。 [#62479](https://github.com/StarRocks/starrocks/pull/62479)
- 統計キャッシュの遅延更新をデフォルトで無効にする FE 設定項目 `enable_statistic_cache_refresh_after_write` を追加しました。 [#62518](https://github.com/StarRocks/starrocks/pull/62518)
- セキュリティ向上のため、SUBMIT TASK で資格情報をマスクしました。 [#62311](https://github.com/StarRocks/starrocks/pull/62311)
- Trino 方言における `json_extract` が JSON 型を返すようになりました。 [#59718](https://github.com/StarRocks/starrocks/pull/59718)
- `null_or_empty` で ARRAY 型をサポートしました。 [#62207](https://github.com/StarRocks/starrocks/pull/62207)
- Iceberg マニフェストキャッシュのサイズ制限を調整しました。 [#61966](https://github.com/StarRocks/starrocks/pull/61966)
- Hive にリモートファイルキャッシュの制限を追加しました。 [#62288](https://github.com/StarRocks/starrocks/pull/62288)

### バグ修正

以下の問題を修正しました：

- 負のタイムアウト値によりタイムスタンプ比較が誤り、セカンダリレプリカが無期限にハングする問題。 [#62805](https://github.com/StarRocks/starrocks/pull/62805)
- TransactionState が REPLICATION の場合、PublishTask がブロックされる可能性。 [#61664](https://github.com/StarRocks/starrocks/pull/61664)
- 物化ビューリフレッシュ中に Hive テーブルが削除され再作成された際の修復メカニズムの誤り。 [#63072](https://github.com/StarRocks/starrocks/pull/63072)
- 物化ビュー集約プッシュダウン改写後に誤った実行計画が生成される問題。 [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- PlanTuningGuide がクエリプロファイルに認識できない文字列（null explainString）を生成し、ANALYZE PROFILE が失敗する問題。 [#63024](https://github.com/StarRocks/starrocks/pull/63024)
- `hour_from_unixtime` の戻り値型が不適切で、`CAST` の改写ルールも誤り。 [#63006](https://github.com/StarRocks/starrocks/pull/63006)
- Iceberg マニフェストキャッシュでデータ競合下に NPE が発生。 [#63043](https://github.com/StarRocks/starrocks/pull/63043)
- 共有データクラスターで物化ビューの Colocation がサポートされていない。 [#62941](https://github.com/StarRocks/starrocks/pull/62941)
- Scan Range デプロイ時に Iceberg テーブルスキャン例外が発生。 [#62994](https://github.com/StarRocks/starrocks/pull/62994)
- ビューに基づく改写で誤った実行計画が生成される。 [#62918](https://github.com/StarRocks/starrocks/pull/62918)
- Compute Node が終了時に正常にシャットダウンされず、エラーやタスク中断を引き起こす。 [#62916](https://github.com/StarRocks/starrocks/pull/62916)
- Stream Load 実行状態更新時に NPE が発生。 [#62921](https://github.com/StarRocks/starrocks/pull/62921)
- 列名と PARTITION BY 句内の名前の大文字小文字が異なる場合、統計が誤る。 [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- `LEAST` 関数を述語として使用した際に誤った結果が返る。 [#62826](https://github.com/StarRocks/starrocks/pull/62826)
- テーブルプルーニング境界の CTEConsumer 上の ProjectOperator が無効。 [#62914](https://github.com/StarRocks/starrocks/pull/62914)
- Clone 後に冗長なレプリカ処理が発生。 [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- Stream Load プロファイルを収集できない。 [#62802](https://github.com/StarRocks/starrocks/pull/62802)
- 不適切な BE 選択によりディスク再バランスが無効化。 [#62776](https://github.com/StarRocks/starrocks/pull/62776)
- `tablet_id` が欠落して delta writer が null になる場合、LocalTabletsChannel で NPE によるクラッシュが発生。 [#62861](https://github.com/StarRocks/starrocks/pull/62861)
- KILL ANALYZE が効果を持たない。 [#62842](https://github.com/StarRocks/starrocks/pull/62842)
- MCV 値にシングルクォートが含まれる場合、ヒストグラム統計で SQL 構文エラーが発生。 [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- Prometheus 向けメトリクスの出力形式が誤り。 [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- データベース削除後に `information_schema.analyze_status` を照会すると NPE が発生。 [#62796](https://github.com/StarRocks/starrocks/pull/62796)
- CVE-2025-58056。 [#62801](https://github.com/StarRocks/starrocks/pull/62801)
- SHOW CREATE ROUTINE LOAD 実行時、データベースが指定されない場合に null と見なされ誤った結果が返る。 [#62745](https://github.com/StarRocks/starrocks/pull/62745)
- `files()` で CSV ヘッダを誤ってスキップし、データ損失が発生。 [#62719](https://github.com/StarRocks/starrocks/pull/62719)
- バッチトランザクション upsert のリプレイ時に NPE が発生。 [#62715](https://github.com/StarRocks/starrocks/pull/62715)
- 共有ナッシングクラスターでの優雅なシャットダウン中に Publish が誤って成功と報告される。 [#62417](https://github.com/StarRocks/starrocks/pull/62417)
- 非同期 delta writer でヌルポインタによりクラッシュが発生。 [#62626](https://github.com/StarRocks/starrocks/pull/62626)
- リストアジョブ失敗後に物化ビューのバージョンマップがクリアされず、物化ビューリフレッシュがスキップされる。 [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- 物化ビューアナライザーでの大文字小文字区別のパーティション列検証が原因で問題が発生。 [#62598](https://github.com/StarRocks/starrocks/pull/62598)
- 構文エラーを含む文に重複した ID が生成される。 [#62258](https://github.com/StarRocks/starrocks/pull/62258)
- CancelableAnalyzeTask 内の冗長な状態割り当てにより StatisticsExecutor ステータスが上書きされる。 [#62538](https://github.com/StarRocks/starrocks/pull/62538)
- 統計情報収集により誤ったエラーメッセージが生成される。 [#62533](https://github.com/StarRocks/starrocks/pull/62533)
- 外部ユーザーのデフォルト最大接続数不足により、スロットリングが早期に発生。 [#62523](https://github.com/StarRocks/starrocks/pull/62523)
- 物化ビューのバックアップとリストア操作で NPE が発生する可能性。 [#62514](https://github.com/StarRocks/starrocks/pull/62514)
- `http_workers_num` メトリクスが不正確。 [#62457](https://github.com/StarRocks/starrocks/pull/62457)
- 実行時フィルタ構築時に対応する実行グループを特定できない。 [#62465](https://github.com/StarRocks/starrocks/pull/62465)
- 複雑な関数を含む CASE WHEN の簡略化により、Scan ノードで冗長な結果が発生。 [#62505](https://github.com/StarRocks/starrocks/pull/62505)
- `gmtime` がスレッドセーフではない。 [#60483](https://github.com/StarRocks/starrocks/pull/60483)
- Hive パーティション取得時にエスケープ文字列の扱いに問題。 [#59032](https://github.com/StarRocks/starrocks/pull/59032)

## 3.5.5

リリース日: 2025年9月5日

### 改善点

- 新しいシステム変数 `enable_drop_table_check_mv_dependency`（デフォルト: `false`）を追加しました。`true` に設定すると、削除対象のオブジェクトが下流のマテリアライズドビューに依存されている場合、システムは `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW` の実行を阻止します。エラーメッセージには依存関係のあるマテリアライズドビューが表示され、詳細は `sys.object_dependencies` ビューで確認するよう案内されます。 [#61584](https://github.com/StarRocks/starrocks/pull/61584)
- ログにビルドの Linux ディストリビューションおよび CPU アーキテクチャ情報を追加し、問題の再現やトラブルシューティングを容易にしました。ログ形式: `... build <hash> distro <id> arch <arch>`。 [#62017](https://github.com/StarRocks/starrocks/pull/62017)
- 各 Tablet ごとにインデックスおよびインクリメンタルカラムグループのファイルサイズをキャッシュに永続化することで、オンデマンドのディレクトリスキャンを置き換えました。これにより、BE の Tablet ステータス報告を高速化し、高 I/O シナリオでのレイテンシを削減します。 [#61901](https://github.com/StarRocks/starrocks/pull/61901)
- FE/BE ログ内の高頻度な INFO ログを VLOG に格下げし、タスク送信ログを集約することで、ストレージ関連の冗長ログや高負荷時のログ量を大幅に削減しました。 [#62121](https://github.com/StarRocks/starrocks/pull/62121)
- `information_schema` データベースを通じて External Catalog メタデータを照会する際、`getTable` 呼び出し前にテーブルフィルタをプッシュダウンするように改善し、テーブルごとの RPC を回避して性能を向上しました。 [#62404](https://github.com/StarRocks/starrocks/pull/62404)

### バグ修正

次の問題が修正されました：

- Plan 段階でパーティションレベルのカラム統計を取得する際、欠損により NullPointerException が発生する問題。 [#61935](https://github.com/StarRocks/starrocks/pull/61935)
- Parquet 書き込み時、NULL 配列が非ゼロサイズの場合に発生する問題を修正し、`SPLIT(NULL, …)` の挙動を常に NULL を返すように修正しました。これにより、データ破損や実行時エラーを防ぎます。 [#61999](https://github.com/StarRocks/starrocks/pull/61999)
- `CASE WHEN` 式を使用したマテリアライズドビュー作成時、VARCHAR 型の非互換な戻り値により失敗する問題を修正しました（修正後はリフレッシュ前後の一貫性を確保し、FE 設定 `transform_type_prefer_string_for_varchar` を追加。STRING を優先することで長さ不一致を回避）。 [#61996](https://github.com/StarRocks/starrocks/pull/61996)
- `enable_rbo_table_prune` が `false` の場合、テーブルプルーニング時にメモ外でネストした CTE の統計を計算できない問題。 [#62070](https://github.com/StarRocks/starrocks/pull/62070)
- Audit Log において、INSERT INTO SELECT 文の Scan Rows 結果が不正確である問題。 [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- 初期化段階で ExceptionInInitializerError/NullPointerException が発生し、Query Queue v2 有効時に FE が起動に失敗する問題。 [#62161](https://github.com/StarRocks/starrocks/pull/62161)
- BE が `LakePersistentIndex` の初期化失敗時に `_memtable` のクリーンアップでクラッシュする問題。 [#62279](https://github.com/StarRocks/starrocks/pull/62279)
- マテリアライズドビューのリフレッシュ時、作成者の全ロールが有効化されずに発生する権限問題を修正しました（修正後、新しい FE 設定 `mv_use_creator_based_authorization` を追加。`false` に設定すると root 権限でリフレッシュを実行し、LDAP 認証方式のクラスタに対応）。 [#62396](https://github.com/StarRocks/starrocks/pull/62396)
- List パーティションテーブル名が大文字小文字のみ異なる場合にマテリアライズドビューのリフレッシュが失敗する問題（修正後、パーティション名に対して大文字小文字を区別しない一意性チェックを実施し、OLAP テーブルのセマンティクスと一致させました）。 [#62389](https://github.com/StarRocks/starrocks/pull/62389)

## 3.5.4

リリース日: 2025年8月22日

### 改善点

- タブレットを修復できない理由を明確にするログを追加。 [#61959](https://github.com/StarRocks/starrocks/pull/61959)
- ログ内の DROP PARTITION 情報を最適化。 [#61787](https://github.com/StarRocks/starrocks/pull/61787)
- 統計情報が不明なテーブルに対して、大きな（ただし設定可能な）行数を割り当て。 [#61332](https://github.com/StarRocks/starrocks/pull/61332)
- ラベル位置に基づくバランス統計を追加。 [#61905](https://github.com/StarRocks/starrocks/pull/61905)
- クラスター監視を改善するためにコロケーショングループのバランス統計を追加。 [#61736](https://github.com/StarRocks/starrocks/pull/61736)
- 正常なレプリカ数がデフォルトのレプリカ数を超える場合、Publish の待機フェーズをスキップ。 [#61820](https://github.com/StarRocks/starrocks/pull/61820)
- タブレットレポートにタブレット情報収集時間を追加。 [#61643](https://github.com/StarRocks/starrocks/pull/61643)
- タグ付き Starlet ファイルの書き込みをサポート。 [#61605](https://github.com/StarRocks/starrocks/pull/61605)
- クラスターのバランス統計の表示をサポート。 [#61578](https://github.com/StarRocks/starrocks/pull/61578)
- librdkafka を 2.11.0 にアップグレードし、Kafka 4.0 をサポート。非推奨設定を削除。 [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Stream Load トランザクションインターフェイスに `prepared_timeout` 設定を追加。 [#61539](https://github.com/StarRocks/starrocks/pull/61539)
- StarOS を v3.5-rc3 にアップグレード。 [#61685](https://github.com/StarRocks/starrocks/pull/61685)

### バグ修正

次の問題が修正されました：

- ランダム分布テーブルにおける Dict バージョンの誤り。 [#61933](https://github.com/StarRocks/starrocks/pull/61933)
- コンテキスト条件でのクエリコンテキスト誤り。 [#61929](https://github.com/StarRocks/starrocks/pull/61929)
- ALTER 操作中、シャドウタブレットに対する同期 Publish による Publish 失敗。 [#61887](https://github.com/StarRocks/starrocks/pull/61887)
- CVE-2025-55163 問題を修正。 [#62041](https://github.com/StarRocks/starrocks/pull/62041)
- Apache Kafka からのリアルタイムデータ取り込み時のメモリリーク。 [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- レイク永続インデックスにおけるリビルドファイル数の誤り。 [#61859](https://github.com/StarRocks/starrocks/pull/61859)
- 生成式カラムの統計収集によりクロスデータベースクエリが失敗。 [#61829](https://github.com/StarRocks/starrocks/pull/61829)
- Query Cache が共有ナッシング型クラスターで不整合を引き起こす。 [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- CatalogRecycleBin に削除済みパーティション情報が保持され、高メモリ使用を引き起こす。 [#61582](https://github.com/StarRocks/starrocks/pull/61582)
- SQL Server JDBC 接続が 65,535 ミリ秒を超えるタイムアウトで失敗。 [#61719](https://github.com/StarRocks/starrocks/pull/61719)
- セキュリティ統合がパスワードを暗号化できず、機密情報が漏洩。 [#60666](https://github.com/StarRocks/starrocks/pull/60666)
- Iceberg パーティション列の `MIN()` および `MAX()` が NULL を返す問題。 [#61858](https://github.com/StarRocks/starrocks/pull/61858)
- 非プッシュダウンサブフィールドを含む Join の述語が誤って書き換えられる。 [#61868](https://github.com/StarRocks/starrocks/pull/61868)
- QueryContext のキャンセルにより use-after-free が発生する可能性。 [#61897](https://github.com/StarRocks/starrocks/pull/61897)
- CBO のテーブルプルーニングが他の述語を見落とす。 [#61881](https://github.com/StarRocks/starrocks/pull/61881)
- `COLUMN_UPSERT_MODE` における部分更新が、自動増分列をゼロで上書きする可能性。 [#61341](https://github.com/StarRocks/starrocks/pull/61341)
- JDBC TIME 型変換で誤ったタイムゾーンオフセットを使用し、不正な時刻値となる問題。 [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- Routine Load ジョブで `max_filter_ratio` がシリアライズされない。 [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Stream Load の `now(precision)` 関数における精度損失。 [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- クエリキャンセル時に「query id not found」エラーが発生する可能性。 [#61667](https://github.com/StarRocks/starrocks/pull/61667)
- LDAP 認証が検索中に PartialResultException を見逃す可能性。 [#60667](https://github.com/StarRocks/starrocks/pull/60667)
- Paimon Timestamp のタイムゾーン変換が DATETIME 条件を含む場合に誤る。 [#60473](https://github.com/StarRocks/starrocks/pull/60473)

## 3.5.3

リリース日: 2025年8月11日

### 改善点

- Lake Compaction にセグメント書き込み時間統計情報を追加。 [#60891](https://github.com/StarRocks/starrocks/pull/60891)
- パフォーマンス低下を避けるため、Data Cache 書き込みの Inline モードを無効化。 [#60530](https://github.com/StarRocks/starrocks/pull/60530)
- Iceberg メタデータスキャンで共有ファイル I/O をサポート。 [#61012](https://github.com/StarRocks/starrocks/pull/61012)
- すべての PENDING 状態の ANALYZE タスクの終了をサポート。 [#61118](https://github.com/StarRocks/starrocks/pull/61118)
- CTE ノードが多すぎる場合、最適化時間が長くならないように強制的に再利用。 [#60983](https://github.com/StarRocks/starrocks/pull/60983)
- クラスターバランス結果に `BALANCE` タイプを追加。 [#61081](https://github.com/StarRocks/starrocks/pull/61081)
- 外部テーブルを含む物化ビューの書き換えを最適化。 [#61037](https://github.com/StarRocks/starrocks/pull/61037)
- システム変数 `enable_materialized_view_agg_pushdown_rewrite` のデフォルト値を `true` に変更し、マテリアライズドビューに対する集計関数のプッシュダウンをデフォルトで有効化。 [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- パーティション統計のロック競合を最適化。 [#61041](https://github.com/StarRocks/starrocks/pull/61041)

### バグ修正

次の問題が修正されました：

- 列の切り取り後、チャンク列のサイズが不一致。 [#61271](https://github.com/StarRocks/starrocks/pull/61271)
- 非同期実行のパーティション統計ロードでデッドロックが発生する可能性。 [#61300](https://github.com/StarRocks/starrocks/pull/61300)
- `array_map` が定数配列列を処理する際にクラッシュ。 [#61309](https://github.com/StarRocks/starrocks/pull/61309)
- 自動増分列を NULL に設定すると、システムエラーが発生し、同一チャンク内の有効データが拒否される。 [#61255](https://github.com/StarRocks/starrocks/pull/61255)
- 実際の JDBC 接続数が `jdbc_connection_pool_size` 制限を超える可能性。 [#61038](https://github.com/StarRocks/starrocks/pull/61038)
- FQDN モードで IP アドレスがキャッシュキーとして使用されない。 [#61203](https://github.com/StarRocks/starrocks/pull/61203)
- 配列比較中に配列列のクローンエラー。 [#61036](https://github.com/StarRocks/starrocks/pull/61036)
- シリアライズされたスレッドプールをデプロイする際にブロックが発生し、クエリのパフォーマンスが低下しています。 [#61150](https://github.com/StarRocks/starrocks/pull/61150)
- heartbeatRetryTimes カウンターのリセット後、OK hbResponse が同期されない。 [#61249](https://github.com/StarRocks/starrocks/pull/61249)
- `hour_from_unixtime` 関数の結果が誤っている。 [#61206](https://github.com/StarRocks/starrocks/pull/61206)
- ALTER TABLE タスクとパーティション作成の競合。 [#60890](https://github.com/StarRocks/starrocks/pull/60890)
- v3.3 から v3.4 以降にアップグレード後、キャッシュが効かない。 [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- ベクトルインデックス指標 `hit_count` が設定されていない。 [#61102](https://github.com/StarRocks/starrocks/pull/61102)
- Stream Load トランザクションがコーディネータノードを見つけられない。 [#60154](https://github.com/StarRocks/starrocks/pull/60154)
- OOM パーティションを読み込む際にBEがクラッシュ。 [#60778](https://github.com/StarRocks/starrocks/pull/60778)
- 手動作成したパーティションで INSERT OVERWRITE 実行時に失敗。 [#60858](https://github.com/StarRocks/starrocks/pull/60858)
- パーティション値が異なっても名前が大文字小文字を区別しない場合にパーティション作成が失敗。 [#60909](https://github.com/StarRocks/starrocks/pull/60909)
- PostgreSQL UUID 型がサポートされていない。 [#61021](https://github.com/StarRocks/starrocks/pull/61021)
- `FILES()` 経由で Parquet データをインポート時、列名の大文字小文字の問題。 [#61059](https://github.com/StarRocks/starrocks/pull/61059)

## 3.5.2

リリース日: 2025年7月18日

### 改善点

- ARRAY 列に対する NDV 統計の収集を追加し、クエリプランの精度を向上。[#60623](https://github.com/StarRocks/starrocks/pull/60623)
- 共有データクラスタで、Colocate テーブルのレプリカ均衡とタブレットスケジューリングを無効化し、不要なログ出力を抑制。[#60737](https://github.com/StarRocks/starrocks/pull/60737)
- FE 起動時、外部データソースへのアクセスを非同期かつ遅延させるように最適化し、外部サービスの利用不可による起動停止を防止。[#60614](https://github.com/StarRocks/starrocks/pull/60614)
- プレディケートプッシュダウンを制御するセッション変数 `enable_predicate_expr_reuse` を追加。[#60603](https://github.com/StarRocks/starrocks/pull/60603)
- Kafka Partition 情報の取得失敗時に自動リトライを実装。[#60513](https://github.com/StarRocks/starrocks/pull/60513)
- マテリアライズドビューとベーステーブル間のパーティション列の 1 対 1 制約を撤廃。[#60565](https://github.com/StarRocks/starrocks/pull/60565)
- 集約フェーズでのデータフィルタリングを通じてパフォーマンスを向上させる Runtime In-Filter をサポート。[#59288](https://github.com/StarRocks/starrocks/pull/59288)

### バグ修正

以下の問題を修正しました：

- 低カーディナリティ最適化が原因で、複数列の COUNT DISTINCT クエリがクラッシュする問題を修正。[#60664](https://github.com/StarRocks/starrocks/pull/60664)
- 同名のグローバル UDF が複数存在する場合に関数が誤ってマッチする問題を修正。[#60550](https://github.com/StarRocks/starrocks/pull/60550)
- Stream Load によるインポート時の NPE を修正。[#60755](https://github.com/StarRocks/starrocks/pull/60755)
- クラスタスナップショットからの復旧時に FE が NPE で起動に失敗する問題を修正。[#60604](https://github.com/StarRocks/starrocks/pull/60604)
- 無順序値列のショートサーキットクエリ処理時、列モード不一致により BE がクラッシュする問題を修正。[#60466](https://github.com/StarRocks/starrocks/pull/60466)
- SUBMIT TASK ステートメントで PROPERTIES を使って設定したセッション変数が無効だった問題を修正。[#60584](https://github.com/StarRocks/starrocks/pull/60584)
- 特定条件下で `SELECT min/max` クエリが誤った結果を返す問題を修正。[#60601](https://github.com/StarRocks/starrocks/pull/60601)
- プレディケートの左辺が関数である場合、誤ったバケットが使用され、クエリ結果が間違ってしまう問題を修正。[#60467](https://github.com/StarRocks/starrocks/pull/60467)
- Arrow Flight SQL プロトコルで存在しない `query_id` をクエリした際にクラッシュする問題を修正。[#60497](https://github.com/StarRocks/starrocks/pull/60497)

### 動作の変更

- `lake_compaction_allow_partial_success` のデフォルト値を `true` に変更。Compaction タスクが一部のみ成功しても成功とみなされ、後続のタスクがブロックされるのを回避可能に。[#60643](https://github.com/StarRocks/starrocks/pull/60643)

## 3.5.1

リリース日：2025年7月1日

### 新機能

- [実験的] バージョン 3.5.1 以降、StarRocks は Apache Arrow Flight SQL プロトコルに基づく高性能なデータ転送パイプラインを導入しました。これにより、データ読み取り経路を全面的に最適化し、転送効率を大幅に向上させます。このソリューションは、StarRocks のカラムナー実行エンジンからクライアントまで、エンドツーエンドのカラムナーデータ転送を実現し、従来の JDBC や ODBC インターフェースで発生する頻繁な行列変換やシリアライズのオーバーヘッドを回避します。真のゼロコピー、低レイテンシー、高スループットのデータ転送を可能にします。 [#57956](https://github.com/StarRocks/starrocks/pull/57956)
- Java Scalar UDF（ユーザー定義関数）の入力パラメータとして ARRAY 型および MAP 型をサポートしました。 [#55356](https://github.com/StarRocks/starrocks/pull/55356)
- **ノード間キャッシュ共有機能**：コンピュートノード間でリモートデータレイク上の外部テーブルデータのキャッシュをネットワーク経由で共有できます。ローカルキャッシュがヒットしない場合、同一クラスタ内の他のノードのキャッシュから優先的にデータを取得し、全ノードのキャッシュがヒットしない場合のみリモートストレージから再取得します。この機能により、スケールイン/アウト時のキャッシュ無効化による性能の揺らぎを抑え、クエリ性能の安定性を確保します。新しいFE設定パラメータ `enable_trace_historical_node` でこの挙動を制御できます（デフォルト：`false`）。 [#57083](https://github.com/StarRocks/starrocks/pull/57083)
- **Storage Volume が Google Cloud Storage (GCS) をネイティブサポート**：GCS をバックエンドストレージとして利用でき、ネイティブ SDK で GCS リソースの管理・アクセスが可能です。 [#58815](https://github.com/StarRocks/starrocks/pull/58815)

### 改善点

- Hive 外部テーブル作成失敗時のエラーメッセージを改善。 [#60076](https://github.com/StarRocks/starrocks/pull/60076)
- Iceberg メタデータの `file_record_count` を利用し、`count(1)` クエリの性能を最適化。 [#60022](https://github.com/StarRocks/starrocks/pull/60022)
- Compaction スケジューリングロジックを改善し、全てのサブタスク成功時の遅延スケジューリングを防止。 [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- BE/CN ノードを JDK17 にアップグレード後、`JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED"` を追加。 [#59947](https://github.com/StarRocks/starrocks/pull/59947)
- Kafka Broker のエンドポイント変更時、ALTER ROUTINE LOAD で `kafka_broker_list` プロパティを修正可能に。 [#59787](https://github.com/StarRocks/starrocks/pull/59787)
- Docker Base Image ビルド時の依存関係をパラメータで簡略化可能に。 [#59772](https://github.com/StarRocks/starrocks/pull/59772)
- Managed Identity 認証による Azure アクセスをサポート。 [#59657](https://github.com/StarRocks/starrocks/pull/59657)
- `Files()` 関数で外部データをクエリする際のパス列の重複エラーを改善。 [#59597](https://github.com/StarRocks/starrocks/pull/59597)
- LIMIT プッシュダウンロジックを最適化。 [#59265](https://github.com/StarRocks/starrocks/pull/59265)

### バグ修正

以下の問題を修正しました：

- クエリが Max/Min 集計と空パーティションを含む場合のパーティションプルーニングの問題。 [#60162](https://github.com/StarRocks/starrocks/pull/60162)
- マテリアライズドビューでクエリを書き換えた際、NULL パーティションが欠落し正しい結果が得られない問題。 [#60087](https://github.com/StarRocks/starrocks/pull/60087)
- Iceberg 外部テーブルで `str2date` を用いたパーティション式が原因でリフレッシュが失敗する問題。 [#60089](https://github.com/StarRocks/starrocks/pull/60089)
- START END 方式で作成した一時パーティションの範囲が正しくない問題。 [#60014](https://github.com/StarRocks/starrocks/pull/60014)
- 非リーダー FE ノードで Routine Load メトリクスが正しく表示されない問題。 [#59985](https://github.com/StarRocks/starrocks/pull/59985)
- `COUNT(*)` ウィンドウ関数を含むクエリで BE/CN がクラッシュする問題。 [#60003](https://github.com/StarRocks/starrocks/pull/60003)
- Stream Load でテーブル名に中国語が含まれるとインポートに失敗する問題。 [#59722](https://github.com/StarRocks/starrocks/pull/59722)
- 3 レプリカテーブルへのインポート時、一部セカンダリレプリカの失敗により全体が失敗する問題。 [#59762](https://github.com/StarRocks/starrocks/pull/59762)
- SHOW CREATE VIEW でパラメータが欠落する問題。 [#59714](https://github.com/StarRocks/starrocks/pull/59714)

### 動作の変更

- 一部の FE メトリクスに `is_leader` ラベルを追加。 [#59883](https://github.com/StarRocks/starrocks/pull/59883)

## 3.5.0

リリース日： 2025 年 6 月 13 日

### アップグレードに関する注意

- StarRocks v3.5.0 以降は JDK 17 以上が必要です。
  - v3.4 以前からのアップグレードでは、StarRocks が依存する JDK  バージョンを 17 以上に更新し、FE の構成ファイル **fe.conf** の `JAVA_OPTS` にある JDK 17 と互換性のないオプション（CMS や GC 関連など）を削除する必要があります。v3.5 設定ファイルの`JAVA_OPTS`のデフォルト値を推奨する。
  - 外部カタログを使用するクラスタでは、BE の構成ファイル **be.conf** の`JAVA_OPTS` 設定項目に `--add-opens=java.base/java.util=ALL-UNNAMED` を追加する必要がある。
  - Java UDF を使用するクラスタでは、BE の構成ファイル **be.conf** の`JAVA_OPTS` 設定項目に `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED` を追加する必要がある。
  - また、v3.5.0 以降、StarRocks は特定の JDK バージョン向けの JVM 構成を提供しません。すべての JDK バージョンに対して共通の `JAVA_OPTS` を使用します。

### 共有データクラスタ機能強化

- 共有データクラスタで生成列（Generated Column）をサポートしました。[#53526](https://github.com/StarRocks/starrocks/pull/53526)
- 共有データクラスタ内のクラウドネイティブな主キーテーブルで、特定インデックスの再構築をサポートし、インデックスの性能も最適化しました。[#53971](https://github.com/StarRocks/starrocks/pull/53971) [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- 大規模なデータロード操作の実行ロジックを最適化し、メモリ制限による Rowset の小ファイルの大量生成を回避。ロード中、システムは一時的なデータブロックをマージして小さなファイルの生成を減らし、ロード後のクエリパフォーマンスを向上させるとともに、その後のコンパクション操作を減らしてシステムリソースの使用率を向上させます。[#53954](https://github.com/StarRocks/starrocks/issues/53954)

### データレイク分析

- **[Beta]** Hive Metastore 統合による Iceberg Catalog での Iceberg ビューの作成サポートしました。また、ALTER VIEW 文を使って Iceberg ビューの SQL 方言の追加・変更が可能になり、外部システムとの互換性が向上しました。[#56120](https://github.com/StarRocks/starrocks/pull/56120)
- [Iceberg REST Catalog](https://docs.starrocks.io/ja/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) におけるネストされた名前空間をサポートしました。[#58016](https://github.com/StarRocks/starrocks/pull/58016)
- [Iceberg REST Catalog](https://docs.starrocks.io/ja/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) にて、`IcebergAwsClientFactory` を使用して AWS クライアントを作成できる、Vended Credential をサポートしました。[#58296](https://github.com/StarRocks/starrocks/pull/58296)
- Parquet Reader が Bloom Filter を使用したデータフィルタリングをサポートしました。[#56445](https://github.com/StarRocks/starrocks/pull/56445)
- Parquet 形式の Hive/Iceberg テーブルに対し、クエリ実行時に低カーディナリティ列のグローバル辞書を自動生成する機能をサポートしました。[#55167](https://github.com/StarRocks/starrocks/pull/55167)

### パフォーマンスとクエリ最適化

- 統計情報の最適化：
  - Table Sample をサポート。物理ファイルのデータブロックをサンプリングすることで、統計の精度とクエリ性能を改善。[#52787](https://github.com/StarRocks/starrocks/issues/52787)
  - [クエリで使用される述語列を記録し](https://docs.starrocks.io/ja/docs/using_starrocks/Cost_based_optimizer/#predicate-column)、対象列に対して効率的な統計情報収集を可能に。[#53204](https://github.com/StarRocks/starrocks/issues/53204)
  - パーティション単位でのカーディナリティ推定をサポート。システムビュー `_statistics_.column_statistics` を使用して各パーティションの NDV を記録。[#51513](https://github.com/StarRocks/starrocks/pull/51513)
  - [複数列に対する Joint NDV の収集](https://docs.starrocks.io/ja/docs/using_starrocks/Cost_based_optimizer/#%E8%A4%87%E6%95%B0%E5%88%97%E3%81%AE%E5%85%B1%E5%90%8C%E7%B5%B1%E8%A8%88)をサポートし、列間に相関がある場合の CBO 最適化精度を向上。[#56481](https://github.com/StarRocks/starrocks/pull/56481) [#56715](https://github.com/StarRocks/starrocks/pull/56715) [#56766](https://github.com/StarRocks/starrocks/pull/56766) [#56836](https://github.com/StarRocks/starrocks/pull/56836)
  - ヒストグラムを使用した Join ノードのカーディナリティと in_predicate の選択性推定をサポートし、データ偏り時の精度を改善。[#57874](https://github.com/StarRocks/starrocks/pull/57874)
  - [Query Feedback](https://docs.starrocks.io/ja/docs/using_starrocks/query_feedback/) を最適化。同一構造で異なるパラメータ値を持つクエリを同一グループに分類し、実行計画の最適化に役立つ情報を共有。[#58306](https://github.com/StarRocks/starrocks/pull/58306)
- 特定のシナリオで Bloom Filter に代わる最適化手段として Runtime Bitset Filter をサポート。[#57157](https://github.com/StarRocks/starrocks/pull/57157)
- Join Runtime Filter のストレージレイヤへのプッシュダウンをサポート。[#55124](https://github.com/StarRocks/starrocks/pull/55124)
- Pipeline Event Scheduler をサポート。[#54259](https://github.com/StarRocks/starrocks/pull/54259)

### パーティション管理

- [時間関数に基づいた式パーティションのマージ](https://docs.starrocks.io/ja/docs/table_design/data_distribution/expression_partitioning/#%E5%BC%8F%E3%83%91%E3%83%BC%E3%83%86%E3%82%A3%E3%82%B7%E3%83%A7%E3%83%B3%E3%81%AE%E3%83%9E%E3%83%BC%E3%82%B8)を ALTER TABLE で実行可能にし、ストレージ効率とクエリ性能を改善。[#56840](https://github.com/StarRocks/starrocks/pull/56840)
- List パーティションテーブルおよびマテリアライズドビューに対するパーティションの TTL をサポートし、`partition_retention_condition` プロパティを設定することで柔軟なパーティション削除ポリシーを実現。[#53117](https://github.com/StarRocks/starrocks/issues/53117)
- [一般的なパーティション式に基づいた複数パーティションの削除](https://docs.starrocks.io/ja/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#%E3%83%91%E3%83%BC%E3%83%86%E3%82%A3%E3%82%B7%E3%83%A7%E3%83%B3%E3%81%AE%E5%89%8A%E9%99%A4)を ALTER TABLE で実行可能に。[#53118](https://github.com/StarRocks/starrocks/pull/53118)

### クラスタ管理

- FE の Java コンパイルターゲットを Java 11 から Java 17 にアップグレードし、システムの安定性と性能を改善。[#53617](https://github.com/StarRocks/starrocks/pull/53617)

### セキュリティと認証

- MySQL プロトコルに基づいた [SSL 暗号化接続](https://docs.starrocks.io/ja/docs/administration/user_privs/ssl_authentication/)をサポート。[#54877](https://github.com/StarRocks/starrocks/pull/54877)
- 外部認証との統合強化：
  - [OAuth 2.0](https://docs.starrocks.io/ja/docs/administration/user_privs/authentication/oauth2_authentication/) と [JSON Web Token（JWT）](https://docs.starrocks.io/ja/docs/administration/user_privs/authentication/jwt_authentication/)を使用して StarRocks ユーザーを作成可能に。
  - [Security Integration](https://docs.starrocks.io/ja/docs/administration/user_privs/authentication/security_integration/) 機能を導入し、LDAP、OAuth 2.0、JWT との認証統合を簡素化。[#55846](https://github.com/StarRocks/starrocks/pull/55846)
- Group Provider をサポート。LDAP、OS、ファイルからユーザーグループ情報を取得し、認証・認可に利用可能。関数 `current_group()` を使って所属グループを確認できます。[#56670](https://github.com/StarRocks/starrocks/pull/56670)

### マテリアライズドビュー

- 複数のパーティション列を持つマテリアライズドビューの作成をサポートし、より柔軟なパーティショニング戦略を構成可能に。[#52576](https://github.com/StarRocks/starrocks/issues/52576)
- `query_rewrite_consistency` を `force_mv` に設定することで、クエリリライト時にマテリアライズドビューの使用を強制可能に。これにより、ある程度のデータ鮮度を犠牲にしてパフォーマンスの安定性を確保。[#53819](https://github.com/StarRocks/starrocks/pull/53819)

### ロードとアンロード

- JSON 解析エラーが発生した場合、`pause_on_json_parse_error` プロパティを `true` に設定することで Routine Load ジョブを一時停止可能に。[#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[Beta]** [複数の SQL 文を含むトランザクション](https://docs.starrocks.io/ja/docs/loading/SQL_transaction/)（現時点では INSERT のみ対応）をサポート。トランザクションの開始・適用・取り消しにより、複数のロード操作に ACID 特性を提供。[#53978](https://github.com/StarRocks/starrocks/issues/53978)

### 関数

- セッションおよびグローバルレベルでシステム変数 `lower_upper_support_utf8` を導入し、`upper()` や `lower()` などの大文字・小文字変換関数が UTF-8（特に非 ASCII 文字）をより適切に扱えるように改善。[#56192](https://github.com/StarRocks/starrocks/pull/56192)
- 新しい関数を追加：
  - [`field()`](https://docs.starrocks.io/ja/docs/sql-reference/sql-functions/string-functions/field/) [#55331](https://github.com/StarRocks/starrocks/pull/55331)
  - [`ds_theta_count_distinct()`](https://docs.starrocks.io/ja/docs/sql-reference/sql-functions/aggregate-functions/ds_theta_count_distinct/) [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - [`array_flatten()`](https://docs.starrocks.io/ja/docs/sql-reference/sql-functions/array-functions/array_flatten/) [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - [`inet_aton()`](https://docs.starrocks.io/ja/docs/sql-reference/sql-functions/string-functions/inet_aton/) [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - [`percentile_approx_weight()`](https://docs.starrocks.io/ja/docs/sql-reference/sql-functions/aggregate-functions/percentile_approx_weight/) [#57410](https://github.com/StarRocks/starrocks/pull/57410)
