---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

StarRocks を v3.5 にアップグレードした後、直接 v3.4.0 ~ v3.4.4 にダウングレードしないでください。そうしないとメタデータの非互換性を引き起こす。問題を防ぐために、クラスタを v3.4.5 以降にダウングレードする必要があります。

:::

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
