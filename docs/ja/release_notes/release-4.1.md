---
displayed_sidebar: docs
description: "StarRocks 4.1リリースノート：マルチテナント範囲ベースのタブレット自動分割、大容量タブレットサポート（100 GBターゲット）、Fast Schema Evolution V2など..."
---

# StarRocks バージョン 4.1

:::danger

**コンテナイメージの問題 (v4.1.0)**

v4.1.0 コンテナイメージにおけるロード順序の不安定な問題により、コンテナ環境で BE プロセスが正常に起動しない場合があります。**コンテナ環境のユーザーは v4.1.0 にアップグレードしないでください。**修正を含む v4.1.1 をお待ちください（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**ダウングレードに関する注意事項**

- StarRocks を v4.1 にアップグレードした後、v4.0.6 未満の v4.0 バージョンにダウングレードしないでください。

  v4.1 で導入されたデータレイアウトの内部変更（タブレット分割および分散メカニズムに関連）により、v4.1 にアップグレードされたクラスターは、以前のバージョンと完全には互換性のないメタデータおよびストレージ構造を生成する場合があります。そのため、v4.1 からのダウングレードは v4.0.6 以降のみサポートされています。v4.0.6 より前のバージョンへのダウングレードはサポートされていません。この制限は、以前のバージョンがタブレットレイアウトおよび分散メタデータを解釈する方法における後方互換性の制約によるものです。

:::

## 4.1.3

リリース日: 2026年7月14日

### 動作の変更

- `CTAS` は、明示的に宣言された `VARCHAR(N)` 列の長さを `VARCHAR(MAX)` に拡張する代わりに、そのまま保持するようになりました。既存のテーブルには影響しません。`CTAS` で作成された新しいテーブルは、以降の書き込みで宣言された長さを適用します。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- `sys.fe_memory_usage` または `sys.fe_locks` を `OPERATE ON SYSTEM` 権限なしでクエリすると、誤解を招くノードルックアップ失敗ではなく、明確なアクセス拒否エラーが返されるようになりました。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` およびブローカー/ストリームロードは、`isAdjustedToUTC=false` で書き込まれた `INT64` Parquet タイムスタンプにセッションタイムゾーンのシフトを適用しなくなりました。これらのタイムスタンプはウォールクロック値として扱われ、そのままロードされます。v4.1.3 より前にそのようなファイルからロードされたデータは、アップグレード後にロードされたデータと異なる場合があります。一貫性が必要な場合は再ロードしてください。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 正常にコミットされたマルチテーブルトランザクションストリームロードジョブは、`PREPARING` のまま停止し続けるのではなく、`information_schema.loads` および `SHOW STREAM LOAD` で `VISIBLE` として正しく表示されるようになりました。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- コネクターの増分スキャン範囲スケジューリングは、デプロイされたフラグメントのドライバーレイアウトを一貫して再利用するようになり、スキャン範囲が存在しないドライバーに誤って割り当てられることを防ぎます。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` の定数畳み込みは MySQL 8 のバックスラッシュエスケープセマンティクスに一致するようになり、`'a\\\\b'` などのパターンが以前とは逆の結果を返していたケースを修正しました。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load は `property.kafka_partition_discovery` プロパティをサポートするようになりました。このプロパティにより、`kafka_partitions` および `kafka_offsets` が正確な開始オフセットを指定するために設定されている場合でも、パーティションの自動検出を継続できます。`property.kafka_default_offsets` が設定されていない場合、ジョブがすでに消費進捗を持った後に検出されたパーティションのデフォルト開始オフセットは `OFFSET_END` から `OFFSET_BEGINNING` に変更されます。これは**すべての**自動検出ジョブに適用され、新しいプロパティを使用するジョブだけでなく、すべてのジョブに適用されます。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非グループ化集計は、マージ前に `UNION ALL` ブランチを通じてプッシュダウンされるようになり、ユニオン上で集計するクエリのネットワーク転送とメモリ使用量を削減します。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM メンテナンスクエリは、`CREATE` 時に保存された固定クエリテキストを使用する代わりに、各リフレッシュ時に現在のビュー定義から再導出されるようになりました。既存の MV は再作成することなく、リライターのバグ修正の恩恵を自動的に受けます。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- サンプルベースのタブレット事前分割は、事前分割シャードをソースタブレットのワーカーにまとめる（`PACK` 配置）のではなく、すべてのコンピュートノードに分散する（`SPREAD` 配置）ようになり、ロードの並列性が向上します。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 列コメントのみを変更する `ALTER TABLE ... MODIFY COLUMN` は、フルスキーマ変更ジョブを生成する代わりに、軽量なメタデータのみのパスを使用するようになりました。また、これはプライマリキー列でも機能するようになりました。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` と `CEIL` は非予約キーワードとして扱われるようになり、クォートなしでカラム名として使用できます。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` の出力には、UDF および UDAF の Properties カラムに `isolation` プロパティ（`shared` または `isolated`）が常に含まれるようになりました。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` のデフォルト値が 100 から 200 に引き上げられ、`DeleteObjects` リクエストごとにより多くの古いファイルの削除をバッチ処理することで、オブジェクトストレージの vacuum スループットが向上しました。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- ベンドクレデンシャルを使用する Iceberg REST カタログテーブルがキャッシュされ、バックグラウンドでクレデンシャルが更新されるようになりました。これにより、AWS Lake Formation のレート制限を引き起こしていた `getTable()` ごとの `GetDataAccess` 呼び出しが不要になりました。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM の `bitmap_union`、`hll_union`、および `percentile_union` 集計状態は、マテリアライズドビューに 2 回（可視カラム + 隠し `__AGG_STATE_` カラム）ではなく 1 回だけ保存されるようになり、これらのスケッチタイプのストレージが半減しました。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- インクリメンタルマテリアライズドビューが `bitmap_agg`、`hll_union`、`percentile_union`、および `bitmap_union` 集計関数をサポートするようになり、正確な distinct カウントおよびスケッチベースの集計をインクリメンタルに維持できるようになりました。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- サンプルベースのタブレット事前分割のタブレット数は、均等な分散のためにアクティブなコンピュートノード数の最も近い倍数に切り上げられるようになり、小規模なロードでの過度な断片化を避けるために最小タブレットサイズで下限が設けられました。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改善点

- `ngram_search` 関数が非定数の needle 引数を受け付けるようになりました。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- `enable_http_auth` FE 設定で制御される HTTP 認証フレームワークが追加され、すべての外部 HTTP エンドポイントで認証および RBAC の適用が制御されます。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- リフレッシュおよび配置の可観測性カラム（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）が `information_schema.materialized_views` に追加されました。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- ジャーナルリプレイ時に外部統計キャッシュのオプトイン遅延リフレッシュが追加されました。新しい FE 設定で制御され、低速または停止した外部メタストアが FE ジャーナルリプレイや起動を停滞させるのを防ぎます。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- `VARCHAR` の長さの増加は、データの書き直しなしにファストスキーマエボリューションを通じて、範囲分散（共有データ）ソートキーカラムで許可されるようになりました。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 共有データのトランザクションログ書き込みが設定可能なしきい値を超えた場合にスタックトレースダンプが追加され、低速な `put_txn_log` / `put_combined_txn_log` 呼び出しの診断が容易になりました。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- タブレット事前分割のメタ層フッターリーダーが `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR`、および ORC の `TIMESTAMP` ソートキーをサポートするようになり、データ層サンプリングにフォールバックしなければならないロードの数が減少しました。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- サンプルベースのタブレット事前分割が `INSERT INTO ... SELECT ... FROM <OLAP table>` ロードにも適用されるようになり、すべてのソートキー列を含む列リスト `INSERT` ステートメントにも適用されるようになりました。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 共有データのタブレットメタデータおよびトランザクションログファイルに Adler-32 チェックサム保護を追加し、読み取り時にサイレント破損を検出できるようになりました。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- データベースごとに `txn_max_committed_pending_publish_ms` FE メトリクスを追加しました。これはコミット済みだが未公開のトランザクションの最古のものの経過時間を報告し、バージョン公開の停滞を検出するのに役立ちます。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- タブレットの分割/マージがパブリッシュバージョンのレスポンスからリアルタイムでトリガーされるようになり、ロードの完了から自動分割/マージが開始されるまでのラグが短縮されました。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- SST なしの条件マージタスクを `pk_index_execution` スレッドプールにルーティングすることで、レイクプライマリキーテーブルの条件更新比較フェーズを最適化しました。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- レイクのスキーマ変更およびロールアップジョブのロックをデータベース全体ではなくテーブルレベルにスコープし、同じデータベース内の他のテーブルに対する並行操作のロック競合を軽減しました。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- シェアードナッシングモードにおいて、複数のデータベースレベルの書き込みロックをテーブルスコープの集中書き込みロックに絞り込み、BE レポートコールバックおよびクールダウン操作中のロック競合を軽減しました。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load がネイティブの `MAP` および `STRUCT` ターゲット列をサポートするようになりました。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- 範囲コロケートタブレットの安定性ゲーティングが、グループを安定とマークする前に StarOS の配置収束を待機するようになり、コロケートジョインがホストローカル実行を達成できるようになりました。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 外部テーブルの CBO 統計を改善しました。オプティマイザは完全なファイル列挙なしに Iceberg マニフェストから行数を推定し、Parquet/ORC 圧縮に対する Hive/Hudi の行数過小評価を修正し、JDBC コネクタの非同期行数統計を追加し、Puffin 統計が利用できない場合の Iceberg および外部コネクタ向けの NDV 推定フォールバックを提供します。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg マニフェストの列統計がクラスタ化列のみに選択的にキャッシュされるようになり、多数のデータファイルを持つ幅広テーブルの FE ヒープ消費を削減しました。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部テーブルの統計収集が FE の再起動および HA フェイルオーバーをまたいだ永続的な述語列トラッキングをサポートするようになり、auto-ANALYZE が正しい列をターゲットにできるようになりました。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- スケジューリングから実行までの外部テーブル統計収集のライフサイクル全体をカバーする構造化 `[ExternalStats]` ログ行を追加しました。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` が外部テーブル統計ジョブの Properties 列にパーティション、列、およびスナップショットメタデータを含めるようになりました。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 各外部テーブルの統計ソース（`TABLE_METADATA`、`ANALYZE`、または `NONE`）がクエリランタイムプロファイルに公開されるようになりました。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- IcebergおよびDelta Lake外部テーブルのパーティションフィルター要件とパーティション数制限のサポートを追加しました（以前はHive、Hudi、Paimonのみで利用可能でした）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` およびヒストグラム `ANALYZE` で1%未満のサンプリング比率をサポートし、計算された比率がゼロに切り捨てられる大きなテーブルでの失敗を修正しました。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- `jemalloc_conf` BEの設定項目を追加し、jemallocのランタイムオプションを `information_schema.be_configs` 経由で確認できるようにしました。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- `compaction_chunk_reset_memory_tracker_threshold_percent` BEの設定を追加し、保持されたチャンク容量を解放することで、shared-nothingモードにおけるPrimary Keyコンパクション時のメモリ使用量を削減します。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- starosをv4.1.1にアップグレードしました。再起動をまたいだ `datacache.enable` の永続化、ワーカーグループごとのシャードウォームアップタイムアウトの上書き、S3リトライジッターの改善が含まれます。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- SQL文字列に認証情報マーカーが存在しない場合に正規表現スキャンをスキップすることで、監査ホットパスにおけるSQLクレデンシャルの難読化処理を最適化しました。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquetスキャナーにおける式駆動のオンデマンド遅延カラムロードにより、複数分岐の `OR` クエリでの不要なI/Oを削減します。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` は、順序依存のHIPエスティメーターの代わりに複合エスティメーターを使用することで、安定したカーディナリティ推定を生成するようになりました。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### セキュリティ

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNIハンドラーのヒープ枯渇（DoS）、IPv6サブネットフィルターバイパス、およびDNSキャッシュポイズニングを修正するため、Nettyを4.1.135.Finalにアップグレードしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 2つのデシリアライゼーション脆弱性を修正するため、`jackson-databind` を2.21.4にアップグレードしました。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 認証なしのTelnetサーバーDoS脆弱性を修正するため、Hadoopの推移的依存関係から `org.jline:jline-remote-telnet` を除外しました。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] pprofバイナリの脆弱性を修正するため、pprofのビルド済みバイナリを更新しました。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 述語値内のシングルクォートがリテラル境界をエスケープする可能性があった `information_schema.task_runs` のSQLインジェクションを修正しました。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key`、および `iceberg.catalog.jdbc.password` は、`SHOW CREATE CATALOG` の出力でマスクされるようになりました。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 入力がパーセントエスケープシーケンスの途中で終わる場合に発生する `url_decode` の範囲外読み取りを修正しました。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- `HyperLogLog::deserialize` が範囲外の `SPARSE` レジスタインデックスを受け入れる問題を修正しました。この問題により、不正な入力でヒープメモリが破損し、BEがクラッシュする可能性がありました。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- `bar()` が負の幅の値を拒否する問題を修正しました。以前はこれにより、文字列の無制限な増大とBEのメモリ枯渇が発生する可能性がありました。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### バグ修正

以下の問題が修正されました：

- `add_files` が論理型の値ではなくParquetの物理エンコーディングバイトでIcebergファイルの境界を設定していたため、ファイルレベルの最小/最大プルーニングが不正確になっていました（例：`DECIMAL` カラムの場合）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` は、入力リストがイミュータブルな `List.of(...)` として構築されたプランノードを走査する際に `UnsupportedOperationException` をスローしました。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` の二段階再プランにより、最初のプランニングセッションから古いラムダ引数列参照IDが生成され、`expr_type does not match slot_type` エラーが発生する可能性がありました。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- GIN（転置）インデックスを持つテーブルへの部分更新において、GINインデックス付き列が更新から省略された場合、クエリが無期限にハングするか失敗する問題がありました。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- Lake PCU（部分列更新）は、行セットスキーマとタブレットスキーマの間でスキーマドリフトが発生した場合に、クラッシュするかデータをサイレントに破損させていました。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 複数のスキーマ句を持つ外部Icebergテーブルに対する `ALTER TABLE` の組み合わせが、句のディスパッチごとに以前にキューに入れられたすべてのアクションを誤って再実行していました。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- `PartitionedSpillerWriter` は、パーティションフラッシュとリソースグループキャンセルのインターリーブ中に `num_rows` スナップショットが実際のチャンク行数を超えた場合、`SIGSEGV` でクラッシュしました。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- BEシグナル初期化でSIGPIPEが無視されていなかったため、BEプロセスが起動時（通常はデプロイ直後）に予期せず終了する可能性がありました。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 行範囲フィルタリングによってstruct VARCHARサブフィールドの埋め込みがスキップされた場合、Parquetの一時dictコード列が上位レイヤーにリークし、型の不一致が発生していました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` は、実際にエクスポートされた行数の代わりに `ReturnRows=0` を監査ログに記録していました。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- `TabletChecker.doCheck()` は、ロックタイプの不一致により `blockingAddTabletCtxToScheduler` 内で `IllegalMonitorStateException` をスローし、チェッカーラウンド全体がサイレントにアボートする原因となっていました。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` は `DATETIME_PRECISION` に対して常に `NULL` を返しており、そのフィールドから列サイズを導出するMySQLプロトコルクライアントが正常に動作しない問題がありました。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- クエリが異なるデータベースまたはカタログをまたいで同じ非修飾名を持つ2つのテーブルを結合した場合、MVリフレッシュが `Duplicate key` で失敗していました。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- スピル可能なハッシュジョインのプローブが特定の条件下でクラッシュしていました。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- Icebergの `truncate` および `bucket` 変換関数は、幅またはバケット数の引数がゼロの場合に `SIGFPE` でBEをクラッシュさせていました。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- `mod()` および `pmod()` は、被除数が `TYPE_MIN` で除数が `-1` の場合に `SIGFPE` でBEをクラッシュさせていました。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- `histogram()` は、`bucket_num` がゼロまたは負の場合に `SIGFPE` でBEをクラッシュさせていました。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- `encode_fingerprint_sha256` は、すべての入力行が `NULL` の場合に `SIGSEGV` でクラッシュしていました。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 単一文字ワイルドカード `_` を含む `LIKE` パターンは、GIN転置インデックスを介して評価された場合に誤った結果を返していました。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- GIN転置インデックスに対するANDのみの `MATCH` クエリは、対象セグメントが空の場合に誤ったエラーを返していました。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene の `match_all` クエリが誤った結果を返していました。CLucene の依存関係をアップグレードすることで解決しました。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- ベクターインデックスの書き換えが、共有テーブルスキーマに合成距離カラムを直接登録したため、同じテーブルに対する無関係な並行クエリで `Multiple entries with same key` エラーが発生していました。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- 結合の並べ替えプルーニングが、スキャン述語でまだ参照されているカラムをプルーニングする可能性があり、統計推定で `missing statistic of col` がスローされていました。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` が sum/count マテリアライズドビューを通じて誤って書き換えられ、`DISTINCT` が暗黙的に削除され、重複が存在する場合に誤った結果が返されていました。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` は、クリーンなセマンティックエラーではなく、内部的な `NullPointerException` をスローしていました。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `SHOW CREATE ROUTINE LOAD` は、`COLUMNS TERMINATED BY` 句を持たないジョブの最初のロード説明句の前に、不要な先頭カンマを出力していました。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- CTE を含む `SECURITY INVOKER` ビューは、CTE 名が実際のテーブル参照と誤認された場合に、NPE により権限チェックが失敗することがありました。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- `ReduceCastRule` は、日付/日時の境界リテラルのシフトが表現可能な範囲をオーバーフローする場合（例: `<= '9999-12-31'`）に、`SemanticException` でクエリ計画を中断していました。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- `SplitJoinORToUnionRule` は、結合条件で null セーフ等価（`<=>`）の論理和が使用された場合に重複行を出力していました。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- マルチテーブル外部クエリのメタデータ準備スレッドを並列で共有する `Tracers` が、`enable_profile=true` の下で `IllegalStateException` を引き起こしていました。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` のパーティションコンシューマエラーが暗黙的に破棄され、パーティション化された TopN がエラーを表面化させることなく部分的または誤った結果を返す可能性がありました。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE のバキュームタスクが FE 呼び出し元のタイムアウト経過後もゾンビとして実行し続け、`RELEASE_SNAPSHOT` スレッドプールを枯渇させ、バキュームのスループットを低下させていました。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- 自動バキュームの競合により、進行中のトランザクションより 1 大きい `minActiveTxnId` が一時的に計算され、BE がまだ必要な結合トランザクションログを削除し、パブリッシュが永続的にブロックされる可能性がありました。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- FE の EOS キャンセルと BE のステージ 2 デプロイ間の競合により、クエリが正常に完了した後にキャンセル済みとして誤ってマークされていました。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 集計 TopN のランタイムフィルタのビルドキーが `ConstColumn` の場合、BE が `AggTopNRuntimeFilterUpdaterImpl` 内で `SIGSEGV` によりクラッシュしていました。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- `array_map` / `transform` は、null でない配列がすべて空の場合に `NULL` 行を暗黙的に削除し、誤った行数を返していました。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- JIT コンパイルされた式において、2^64 を超える `LARGEINT` / `DECIMAL128` リテラルが暗黙的に 64 ビットに切り捨てられていました。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- UTF-8 文字列関数（`split`、`split_part`、`str_to_map`）は、最後の文字が切り捨てられた、または無効なマルチバイトリードバイトを持ち、区切り文字が空の場合に文字列の末尾を超えて読み取っていました。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- `parse_json()` は、`ALLOW_THROW_EXCEPTION` SQL モードであっても、不正な JSON に対してクエリを失敗させる代わりに暗黙的に `NULL` を返していました。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 厳格モードの数値ナローイングキャストが、スロットデータが未定義の `NULL` 行に対して誤ってオーバーフローエラーを発生させていました。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 1970年以前のParquet `INT64` タイムスタンプは、負の切り捨て除算の余りにより、サブ秒部分がゼロでない場合にゴミ値にデコードされていました。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970年以前のORC `TIMESTAMP` 値は、ロード時にサブ秒コンポーネントが削除されていました。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORCストライプの最小/最大タイムスタンプ統計が、1970年以前およびサブ秒の境界に対して誤ってデコードされ、データファイルが誤ってプルーニングされていました。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- `ARRAY`、`MAP`、または `STRUCT` カラム内にネストされた `INT96` Parquetタイムスタンプは、ロード時にセッションタイムゾーンオフセットが1つ失われていました。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquetの `UINT_32` 値を `BIGINT` カラムにロードする際、ゼロ拡張ではなく符号拡張が行われ、上位ビットが立った符号なし整数に対して負の値がサイレントに格納されていました。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` デストラクタは、`_scanner_ctx`（それらのノードを参照する述語を保持）より先に `_pool`（およびその `Expr` ノード）を破棄することで、ヒープ解放後使用を引き起こしていました。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- OpenX SerDeを使用したgzip圧縮JSONのHive外部テーブルの読み取りは、マルチバイトUTF-8文字が8MBの解凍バッファ境界をまたいだ場合に `UTF8_ERROR` で失敗していました。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2の `ListPaths` は、クライアントが無条件にアクセスするJSONフィールドが欠落しているため、非HNSアカウントで `SIGSEGV` とともにクラッシュしていました。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- `unnest` は、複数の `UNNEST` 演算子が同じ入力配列カラムを共有し、異なるサブフィールドを消費した場合に、クラッシュまたは誤った結果を返していました。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- `query_mem_limit` は `unnest` の実行中に適用されず、大きな配列に対する `unnest` がクエリを失敗させる代わりにBEをOOMキルしていました。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- `RANK` 境界を持つ `TopN` は、ランク制限がチャンク境界に正確に一致した場合に1行を削除していました。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` 後のカラムプルーニングにより、空の分析（ウィンドウ）演算子が生成され、プランニングまたは実行エラーが発生することがありました。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` はスキャン演算子にのみ行制限を設定し、グローバル制限を設定しなかったため、制限付きサブクエリに対する `COUNT(*)` が並行処理下で期待以上の行数を返すことがありました。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- レイクの主キー永続インデックスの再構築は、セグメント範囲モードで誤ったセグメントイテレータ位置を使用し、キー範囲フィルタの適用が誤っていました。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` はテーブルロックなしに `rebuildPindexVersion` を変更しました。`RestoreJob` はリストア後にDBのREADロックのみでMVベーステーブル情報を変更しました。`FinalizeCreateTableAction` はイテレータ作成をまたいでDBレベルのロックを渡していました。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- `dumpImage` は、ループ中にデータベースごとのロックの取得が例外をスローした場合、グローバルメタロックを無期限に保持し続ける可能性がありました。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- マルチステートメントのストリームロードは、トランザクションごとに `TxnStateCallbackFactory` エントリを1つリークし、際限なく増加して最終的にFEヒープを枯渇させていました。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- ヒストグラム統計の `information_schema.task_runs` 行数は、カタログ、データベース、テーブル、またはパーティション名が長い場合に `primary_key_limit_size`（128バイト）をオーバーフローする可能性がありました。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVMメトリクスが無効なPrometheus `# TYPE` 行（メトリクス名内のラベルセット）を出力し、Prometheusがスクレイプ全体を中断する原因となっていました。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` および `information_schema.partitions_meta` は、共有データテーブルにおいて各パーティションの実際のバケット数ではなく、テーブルレベルのデフォルト値としてすべての物理パーティションのバケット数を報告していました。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` は、共有データ（`CLOUD_NATIVE`）テーブル上のすべてのロールアップインデックスに対してベーステーブルのスキーマを返していました。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` のno-op句が誤って軽量コメントパスにルーティングされ、バッチ `ALTER TABLE` ステートメントで `MODIFY COLUMN COMMENT can not be combined with other alter operations` エラーが発生していました。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` は、`isKey` / `aggregationType` の正規化が不正確なため、キー列や集計列をコメントのみの変更と誤って識別する場合がありました。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` は循環ビュー定義をコミットする可能性があり、その後の `SELECT` が `StackOverflowError` をスローする原因となっていました。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `OrderedPartitionExchanger` は、下流のコンシューマが前のチャンクを変更した際に `accept()` がそのポインタを保持したままとなり、ヒープ解放後使用（heap-use-after-free）を引き起こしていました。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- NLJoinは、ビルド側のスロットディスクリプタがnon-nullableであるにもかかわらずランタイム状態がnullableであった場合にクラッシュしていました。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- `CAST(json/variant AS struct)` は、構造体フィールド名が解析可能なJSONパスでない場合、フラグメントのprepare時にBEをクラッシュさせていました。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- ネストされた辞書式表現に対するDict-decodeは、プロデューサーフラグメントとコンシューマフラグメント間で互換性のない辞書変換を生成し、実行時に `Dict Decode failed` エラーを引き起こす場合がありました。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- スキーマ変更は、`get_rowset_by_version` が `nullptr` を返し、`gtid` の比較がnullチェックより前に置かれた場合に、ヌルポインタ参照によりクラッシュしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- 共有データクラスターのスナップショットは、スナップショットマネージャーが親タブレットのメタデータを回収するかどうかを判断する際にリシャードジョブを考慮しなかったため、タブレットの分割/マージ後に復元不能になっていました。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- ファイルバンドリングバキュームは、兄弟タブレットのゼロ行バンドルセグメントを誤って非共有としてフラグを立て、他のタブレットがまだ参照しているにもかかわらずバンドルファイルを削除していました。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 共有データテーブルのコンパクションパブリッシュは、コンパクショントランザクション開始後に可視化されたロールアップ/同期MVインデックスを削除し、バンドルファイルにそれらのインデックスが含まれない状態にしていました。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- 共有データの永続インデックスコンパクションは、タブレットリシャード中にコンパクションが破棄された際に、パススルー再利用されたSSTファイルを誤って削除していました。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` からnullable フラットJSONカラムへのスキーマ進化により、コンパクション読み取りパスで `CHECK` クラッシュが発生していました。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- nullable カラムに対する `count_combine` は、ストリーミング事前集計パススルーパスで `SIGSEGV` によりBEをクラッシュさせていました。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- Java UDFは、JDK 21で削除されたリフレクティブな `DirectByteBuffer` コンストラクタルックアップのため、JDK 21以降でのロードに失敗していました。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- Unifiedカタログ（Hiveメタストア）への `CTAS` は、パーサーが `CREATE TABLE AS SELECT` 内の `ENGINE` 句をサポートしていなかったため、常に失敗していました。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` フィードバック駆動型ジョイン再構築が `predicateCommonOperators` を失い、共通部分式の再利用を含むプランで `InputDependenciesChecker` 検証エラーが発生していました。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- クエリキャッシュの正規化は、空のサブパーティションがバージョンリストの構築前に刈り込まれたサブパーティションを持つテーブルで `Preconditions.checkState` によりクラッシュしていました。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` は、レガシーエイリアスで保存されたセッション変数を暗黙的にスキップしており、クエリダンプの再生がデフォルト値にフォールバックする原因となっていました。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- Iceberg の `_row_id` 仮想列は、行グループ開始オフセットの二重カウントにより、複数の Parquet 行グループを持つデータファイルで誤った値を返していました。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg の DELETE/UPDATE プランナーは、物理テーブルの識別子ではなく合成テーブル ID で照合していたため、ターゲットスキャンノードを特定できず、ベーススナップショット ID と競合検出フィルターが失われていました。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `FragmentContext::set_final_status` は、`PipelineExecutorSet::start()` が呼び出される前に `cancel_plan_fragment` RPC が到着した場合に `SIGSEGV` でクラッシュしていました。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `QueryContext` は、`FragmentExecutor` がフラグメントをまだ破棄中に再利用される可能性があり、`ResGuard::reset()` でヒープ解放後使用が発生していました。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` が未初期化であったため、デフォルトコンストラクトされた `search()` が未初期化ポインターを逆参照する可能性がありました。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` のマイクロ秒は JVM のデフォルトロケールの数字セットを使用してレンダリングされており、アラビア語やペルシャ語などのロケールで非 ASCII 数字が生成され、タブレットの事前分割における境界値の解析が壊れていました。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- パーティションの行数は、タブレット統計が更新される前に `INSERT OVERWRITE` の後で `_statistics_.column_statistics` にゼロとして書き込まれる可能性があり、オプティマイザーがパーティションのカーディナリティ推定を折りたたむ原因となっていました。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- `enable_statistic_collect_on_first_load` のテーブルレベルのオーバーライドは、グローバル設定で無効になっている場合に初回ロード統計収集を有効にできませんでした。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- `PushDownNonGroupedAggregateBelowUnion` は、ユニオンブランチに入力行がない場合に非 NULL 宣言型で NULL 許容の出力を生成し、BE の `CHECK` 失敗を引き起こしていました。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2

リリース日: 2026年6月18日

### 動作変更

- ユーザーが権限を持たないデータベースへの接続は、ERROR 2013 で接続を切断する代わりに、適切な MySQL エラーパケットを返すようになりました。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- `SHOW FUNCTIONS` は、関数レベルの権限を通じて関数を参照できるが create-function スコープ権限を持たないユーザーに対して、UDF ファイルおよびオブジェクトファイルのパスを `***` としてマスクするようになりました。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- Ranger の行フィルターおよび列マスキングポリシーは、外部カタログから Hive ビューをクエリする際に正しく適用されるようになりました。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` は `current_timestamp` ジェネレーター式を正しく保持するようになりました。`DESCRIBE` および `information_schema` は、バックフィル時のリテラルではなく式を反映するようになりました。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` のロード時フィルタリングは、セッションタイムゾーンが UTC+8 と異なるクラスターでフィルター境界をシフトしなくなりました。ロード時刻は FE–BE 境界を越えて UTC エポックミリ秒として交換されるようになりました。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` セッション変数は、常にデフォルト値を使用する代わりに、Paimon スキャン分割計算に正しく適用されるようになりました。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` はデフォルトで有効になりました。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hiveパーティション統計は、タイマーによってキーごとに自動的に更新されなくなりました。パーティション統計は、明示的な`refreshTable()`呼び出し時にのみ更新されるようになり、大規模なパーティションテーブルでのHMSの負荷が軽減されます。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- IcebergまたはExternal Catalogのベーステーブルでスキーマドリフト（列タイプの変更、列の削除、またはテーブルの削除）が発生した場合、依存するマテリアライズドビューは、NULL行を無音で生成したり不透明なエラーを出したりする代わりに、次回の更新時に非アクティブとしてマークされるようになりました。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Icebergコネクターは、`AND`複合述語の変換可能な側のみが変換可能な場合に述語全体を破棄する代わりに、変換可能な側を部分的にプッシュダウンするようになり、パーティションプルーニングとデータスキッピングが改善されました。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 明示的トランザクションの`COMMIT`は、データベースの書き込みロックを待機する際に、ミリ秒ではなく最大`query_timeout`秒まで正しく待機するようになり、短時間の同時書き込みアクティビティ中の誤ったロックタイムアウト障害を防ぎます。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVMの更新は、フィルタリングされた行を無音でドロップする代わりに、厳格なロードフィルターエラーを呼び出し元に通知するようになりました。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)`はNULL行を正しく除外するようになり、`COUNT(col)`のセマンティクスと一致します。`COUNT(<nullable column>)`に基づくインクリメンタルMVは、以前は膨らんだカウントをマテリアライズしていました。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN`は、クラウドネイティブ（共有データ）テーブルの`file_bundling`や`enable_persistent_index`などのプロパティに対して`ALTER TABLE ... SET (...)`によってトリガーされた非同期メタデータのみの変更ジョブも表示するようになりました。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 集計関数を参照する`HAVING`句を含むインクリメンタルMVの作成は、最初の更新時に内部プランエラーを生成する代わりに、`CREATE`時に明確なエラーで失敗するようになりました。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVMは、インクリメンタルマテリアライズドビューで`MIN`/`MAX(DECIMAL)`集計関数をサポートするようになりました。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVMアダプティブリフレッシュは、最初のデルタトレイトがすでに`mv_max_rows_per_refresh`を超えている場合にデルタウィンドウを正しく制限するようになり、バックログ全体が1回のタスク実行で更新されることを防ぎます。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- GROUP-BYのみのインクリメンタルMV（例：`SELECT k FROM t GROUP BY k`）は、`__ROW_ID__`をVARCHARとして正しくエンコードするようになり、2回目の更新時のクラッシュが修正されました。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 改善点

- Paimonビューをサポートし、`CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`、およびExternal CatalogからのPaimonビューのクエリが含まれます。Paimonビュー内のテーブル参照は、`default_catalog`の代わりにPaimonカタログに対して解決されるようになりました。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- スキーマドリフトや複雑なネスト型を持つファイルを読み取る際の安定したスキーマ制御のために、`FILES()`で明示的な`schema`パラメーターをサポートします。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()`は、接続されているFEのみではなく、すべてのFEノードにわたってクエリプロファイル情報を取得するようになりました。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 現在実行中のクエリのUUIDを返す組み込み関数`query_id()`が追加されました。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 共有データモードの`CREATE`/`ALTER STORAGE VOLUME`は、メタデータを永続化する前にストレージロケーションのアクセシビリティ（認証情報とエンドポイント）を検証するようになり、設定ミスの場合に早期に失敗します。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- BEでAWS S3認証情報の`WebIdentity`トークンプロバイダーサポートが追加され、`AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE`の既存のFEサポートと一致するようになりました。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 欠落した`txnlog`、失われたセグメント、または低速なリモートI/Oによってパブリッシュが永続的にブロックされている場合に、共有データテーブルでスタックした`COMMITTED`トランザクションのブロックを解除する`ADMIN SKIP COMMITTED TRANSACTION`コマンドが追加されました。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config`は`table_name`述語をFEにプッシュダウンするようになり、単一テーブルのルックアップのオーバーヘッドが大幅に削減されます。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- BI ツールおよび接続イントロスペクション時に MySQL 8 スキーマを検査する JDBC ドライバとの互換性を向上させるため、`information_schema` テーブルに不足していた MySQL 8 のカラムを追加しました。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- `enable_pipeline_event_scheduler` BE 設定をクラスター全体のキルスイッチとして追加しました。`false` に設定すると、セッションごとの変数を上書きします。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 複数のワイド文字列カラムを持つテーブルで統計を収集する際のクエリごとのメモリピークを削減するため、統計収集においてワイド文字列カラムの分離をオプトインで有効化できるようにしました。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- スローロックロギングがイベントごとのレート制限と設定可能なスタックキャプチャ制御をサポートするようになり、高いロック競合下での JVM セーフポイントストールを防止します。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- MV リフレッシュのログエントリのプレフィックスにデータベース名が含まれるようになり、同じ MV 名が複数のスキーマに存在するマルチテナント環境でログ行を区別しやすくなりました。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 設定がミュータブルになり、FE を再起動せずに `ADMIN SET FRONTEND CONFIG` を使用してランタイムで切り替えられるようになりました。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- ロードプロファイル（ストリームロード、ルーティンロード、ブローカーロード、マージコミットロード）を `fe.profile.log` に書き込むための `enable_print_load_profile_to_log` FE 設定（デフォルト `false`）を追加しました。これにより、クエリプロファイルのバーストによってインメモリストアが退避された場合でもプロファイルが保持されます。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` が Java オブジェクト参照の代わりに `JobProperties` でカラムマッピングを正しくレンダリングするようになりました。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` がカタログレベルのロックではなくテーブルレベルのロックを使用するようになり、多数のテーブルが同時にアクティブなカタログでのリフレッシュのシリアライゼーション遅延が軽減されます。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- メタスキャン（バックグラウンド統計収集）が、変更後のセグメントファイルで見つからないエラーで失敗する代わりに、`ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN`、および `REORDER COLUMN` のスキーマ変更を適切に処理するようになりました。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- サンプルベースのタブレット事前分割が、マルチパーティションのレンジ分散テーブルおよびブローカーロードに対応するようになり、既存のデータ層ベースラインなしで初回ロードの並列処理が可能になりました。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL の結果シリアライゼーションが行ごとの仮想ディスパッチを使用しなくなりました。型付きカラムライターがチャンクごとに一度構築されるため、幅広いまたは大きな結果セットのシリアライゼーションオーバーヘッドが削減されます。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` から文字列へのキャストが出力バッファに直接書き込まれるようになり、行ごとのヒープアロケーションが不要になりました。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- クエリ統計のマージパスで `SpinLock` がロックフリーの並列マップに置き換えられ、ワーカーが中間または最終統計を送信する際の大規模クラスターでの CPU 使用率が削減されます。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 集計ハッシュマップおよびハッシュセットのプリフェッチが L2 キャッシュの常駐状態に基づいてゲートされるようになり、バケット配列が L2 に収まる場合の 4〜9% の性能低下を回避します。プリフェッチ距離も設定可能になりました。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 共有データのプライマリキーテーブルにおけるライトコンパクションのパブリッシュ時に、セグメントごとの `.lcrm` 読み取りをパイプライン化することで、オブジェクトストレージへの逐次ラウンドトリップを削減します。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 共有データモードでのコールド PK インデックス再構築スキャンがセグメント間で並列化されるようになり、セグメント読み取りがリモート I/O バウンドの場合の再構築時間が短縮されます。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部クエリ（統計収集、タスク実行、MV リフレッシュ）が `SHOW PROC '/current_queries'` で表示されるようになり、`KILL QUERY` でキャンセルできるようになりました。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- S3 スロットリングの監視と `lake_vacuum_min_batch_delete_size` のチューニングのために、レイクバキュームのバッチサイズおよびリトライカウントの bvar メトリクスを追加しました。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- `CatalogRecycleBin` サイズゲージメトリクスを追加し、リサイクルビンの増加が FE ヒープに影響を与える前に把握できるようにしました。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` でパーティション分割されたテーブルは、範囲パーティションテーブル向けに設計された最新 N ヒューリスティックを適用する代わりに、`OlapTableSink` ですべてのパーティションを開くようになり、インクリメンタルオープン RPC のオーバーヘッドが削減されました。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- `LARGE_LIST` および `FIXED_SIZE_LIST` Arrow 型を、`FILES()` または Broker Load 経由で JSON カラムにロードすることをサポートします。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 共有データテーブルへのマージコミット（`FRONTEND_STREAMING`）ロードに対して、トランザクションログとファイルバンドルの組み合わせをサポートし、他のロードタイプと整合性を持たせました。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- FE を再起動せずにレイクパブリッシュフェーズ分解警告しきい値を制御するための、ミュータブル FE 設定 `slow_publish_partition_log_threshold_ms`（デフォルト 3000 ms）を追加しました。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### セキュリティ

- [CVE-2026-43869] 不正な証明書ホスト検証に対処するため、`libthrift` を 0.23.0 にバージョンアップしました。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] HTTP/2 リクエストヘッダー検証に対処するため、Apache Tomcat を 9.0.118 にバージョンアップしました。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラーのヒープ枯渇（DoS）、IPv6 サブネットフィルターバイパス、および DNS キャッシュポイズニングに対処するため、Netty を 4.1.135.Final にバージョンアップしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- Go 標準ライブラリのセキュリティ修正を含めるため、pprof プリビルドバイナリを Go 1.25.11 にアップグレードしました。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### バグ修正

以下の問題が修正されました：

- `parse_url()` は、URL が `host:port` パターン外に `:` を含む場合に誤ったホストを返していました。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 辞書変換された式は、これが成立しない式（例：`IF(col = '1', NULL, 'ok')`）に対して誤って `f(null) = null` を仮定していました。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- トランザクションストリームロードは、ユーザー指定のタイムアウトではなくデフォルトの RPC タイムアウトを使用していたため、早期タイムアウトが発生していました。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- ID カラムに NULL 値を持つ Iceberg 等値削除ファイルは、結合述語で `NULL = NULL` が UNKNOWN と評価されるため、一致する行の削除に失敗していました。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- `INJECTED` パーティションプロジェクションカラムを持つテーブルのエラーメッセージが、問題の原因となっているカラムを示すより詳細な内容になりました。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- 挿入専用 ACID Hive テーブルへのクエリは、insert-overwrite 操作が認識されなかったため、期待より多くの行を返していました。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 同時読み取り中に Iceberg メタデータエントリがピン留めされた場合、ディスクキャッシュが設定容量を超過していました。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 外部カタログをクエリする際、Paimon 主キーカラムが誤って非 NULL 可能としてマークされていました。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 複数の `ARRAY_AGG(DISTINCT <const>)` 入力を持つクエリで `MultiDistinctByMultiFuncRewriter` が同じルールを繰り返し適用した場合、オプティマイザがタイムアウトしていました。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Oracle JDBC の日付述語が `DATE`/`TIMESTAMP` キーワードなしにプッシュダウンされると、NLS フォーマットエラーが発生していました。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- Partition TopN が子オペレーターから必要な出力列を失う場合がありました。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- パーティション進化を持つ Iceberg テーブルに対して、非パーティション MV を作成できませんでした。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- `information_schema.be_cloud_native_compactions` の並列サブタスクにおいて、コンパクションタスクの統計が上書きされ失われていました。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同期 MV の `SHOW CREATE MATERIALIZED VIEW` が「Table is not found」エラーで失敗していました。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- Lake のマルチステートメントトランザクションのパブリッシュ中、ステートメント `.log` ファイルが 4 セグメントパスに配置された際にスキーマ変更でデッドロックが発生していました。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- ソートマージプロバイダーのエラーがフラグメントコンテキストに伝播されず、クエリが無音で失敗していました。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- `ConnectorTableId` が長時間稼働しているフォロワー FE で `int` からオーバーフローして負の値になり、Iceberg および Hive クエリが誤解を招く「Invalid table type」エラーで失敗していました。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 空の optimize 句（分散またはパーティション仕様なし）を持つ `ALTER TABLE` が誤ってパースされ、FE リプレイ時にテーブルのデフォルト分散を破損する可能性がありました。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- `AZURE_PATH_KEY` が有効な `StorageVolumeMgr` パラメーターとして認識されなかったため、ADLS2 共有データのディザスタリカバリ中に FE の起動が失敗していました。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- オプティマイザーがネストされた型の一部を `UNKNOWN_TYPE` に刈り込んだ場合、またはヌル許容の配列・マップ・構造体スキーマが使用された場合に、Avro 複合型のデコードが失敗していました。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- COW 列ミューテーション最適化により、2 つの `NullableColumn` が同じ `NullColumn` オブジェクトを共有していたため、`map_apply` などの関数でクラッシュが発生していました。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- カスタム `LocationProvider` を持つ Iceberg テーブルで、プロバイダーが FE 上で先行してインスタンス化されたため、`ClassNotFoundException` を使用した `SELECT` クエリが失敗していました。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` がキャッシュミスのたびに余分な `getTableComment()` ラウンドトリップを実行し、プランニングフェーズの集中的なロック保持時間が延長されて並行 DDL がブロックされていました。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- ネストされた MV のリフレッシュで、ネストされた MV が `FULL` または `UNKNOWN` の鮮度を返した際に `NullPointerException` がスローされていました。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE ワーカーが低速な MySQL クライアントへのクエリ結果送信で無期限にブロックされていました。結果送信パスに書き込みタイムアウトが適用されるようになりました。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- V1 エンコード（共有なし）から V2 エンコード（共有データ）クラスター、または 2 つの共有データクラスター間でプライマリキーテーブルをレプリケートする際に、PK `.del` ファイルがトランスコードされていませんでした。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 古いレプリカ参照がライブのものを追加する前に削除されなかったため、`VERSION_INCOMPLETE` リカバリ中に `TabletInvertedIndex` に重複レプリカが蓄積されていました。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- `REPLICATE_SNAPSHOT` タスクとファイルごとのコピーサブタスクが同じスレッドプールを共有していたため、共有データ Lake レプリケーションのファイルコピーが CN をクラッシュさせていました。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- `RuntimeProfileParser` は、BEが `.000` 小数点サフィックスでユニットカウンターをフォーマットした際に `NumberFormatException` をスローしました。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 共有データPKタブレット分割における共有セグメントの物理rowidエンコーディングが誤っており、不正な `rss_rowid` エントリが発生していました。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- 混合レンジコロケート × ハッシュ分散 `JOIN` クエリが有効な結果の代わりに `Unknown error` を返していました。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` は固定のUTC+8フォーマッターを使用していましたが、出力はセッションの `time_zone` を尊重するようになりました。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- すべての値が `NULL` であり、列がnullable単項関数パスを通過した場合、Decimal型の列はスケールを失い、下流の結果型が破損していました。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- ネストされた型に対するJSONの部分追記がASANクラッシュを引き起こしていました。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` ロールの権限キャッシュが `GRANT`/`REVOKE` 時に無効化されず、有効期限まで古い権限が残存していました。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- `FlatJson` はサブライターに追記がない場合にクラッシュしていました。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- MVに `HAVING` 述語が含まれている場合、集計MVの書き換えが誤って適用され、不完全な結果が返される可能性がありました。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 並列マージモードへの移行時にスピルライターの `auto_flush` フラグでデータ競合が発生し、ARM上で意図しないセグメントフラッシュが引き起こされていました。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- ルーティングロードスケジューラーは、KafkaまたはPulsarのパーティションメタデータを取得するためにブロッキングBE RPCを実行している間、ジョブごとの書き込みロックを保持しており、最大33.6秒のロック保持時間が発生していました。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- `tablet_sched_disable_colocate_balance` が有効な場合、停止したBE上のコロケートタブレットが誤って `HEALTHY` として報告されていました。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- `ADMIN SHOW REPLICA STATUS` は、MISSING（ファントム）レプリカ行が存在する場合にMySQLの結果ストリームを非同期化し、クライアントのハングまたは切断を引き起こしていました。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 共有データモードでは、パーティションごとのコーディネータークレームがすべての送信者の `open` RPC上で再記録されておらず、コーディネーター選出で一部の送信者が見落とされ、`combined_txn_log` ファイルが書き込まれないままになっていました。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_.pipe_file_list` 内部テーブルは、`_statistics_` データベースまたはテーブルが削除された後に再作成されていませんでした。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- `TaskCleaner` によって強制終了されたタスク実行はアーカイブされず、`information_schema.task_runs` から痕跡なく消えていました。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` および `SWAP TABLE`/`SWAP MATERIALIZED VIEW` はデータベース書き込みロックではなく集中テーブルロックのみを保持しており、同時読み取り側が中途半端な名前からテーブルへのマッピングを観測できる状態でした。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PKインデックスコンパクションの出力sstableがタブレットメタデータなしで開かれ、永続的な `metadata is null when loading delvec` 障害が発生していました。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 同一トランザクション内ですでに変更されたテーブルを対象とした明示的トランザクション内の部分更新 `INSERT` が、`COMMIT` でデータを静かに破損させていました。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- レンジ分散と互換性のない `ALTER TABLE` 操作（スキーマ変更、ソートキー変更）は、メタデータを静かに破損させる代わりに、実行可能なエラーで拒否されるようになりました。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- オプティマイザにおける型不一致の子を持つ集計関数が、誤ったクエリ結果を引き起こしていました。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 予約済みキーワードのテーブル名を持つ `ALTER ROUTINE LOAD` が解析不能な `origStmt` を書き込み、FE 再起動後にカラムマッピングが失われる原因となっていました。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM の `state_union` 互換性チェックがネストされた型（例：`ARRAY<VARCHAR>`）に再帰しなかったため、`ARRAY_AGG` IMV に対して `CREATE MATERIALIZED VIEW` が失敗していました。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- スキャン範囲が完全にフィルタリングされた場合、Parquet の一時辞書コードカラムが上位レイヤーにリークし、下流での型不一致を引き起こしていました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- 浮動小数点と整数が混在する `WHEN` および結果型を持つ `CASE WHEN` が無効な JIT IR を生成し、誤った結果またはクラッシュを引き起こしていました。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT コンパイルの失敗により `LLVMContext` の解放後使用が発生し、SIGSEGV が引き起こされていました。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- バックグラウンド統計タスクがセッションの `WAREHOUSE` 設定を上書きし、同じ接続コンテキスト上の後続のユーザークエリに影響を与えていました。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- `CatalogRecycleBin` はクラスタースナップショットが一度も正常に完了していない場合にエントリの削除を停止し、大量の `INSERT OVERWRITE` ワークロード下で FE メモリが無制限に増大する原因となっていました。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- 非 PK レプリカのバージョンホールが FE によって検出されず、クエリが凍結した `max_version` を持つホールのあるレプリカに永続的にルーティングされていました。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（共有読み取りロック下で変更される `HashSet`）のデータ競合により、エントリの消失またはセットの破損が発生する可能性がありました。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 保持境界メタデータがすでにバキュームされている場合、バキュームウォーターマークが正しく報告されず、`file_bundling` のスイッチバージョンクリーンアップが停止していました。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake バキュームの再試行は決定論的な指数バックオフを使用していましたが、S3 スロットリング下で CN 間に再試行を分散させるために、非相関ジッターが追加されました。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- `OlapTableSink` のメモリ計上が過大になっていました。これは、クエリメモリプールから割り当てられた RPC リクエストがプロセスコンテキストで解放されていたためです。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 自動パーティション作成が `_incremental_open_node_channel` を同時にトリガーした際の `TabletSinkSender::_send_chunk_by_node` における競合状態。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 上流の変更のバックポートによって作成された UDAF コンテキストが、`unique_ptr::release` を介してメモリリークを引き起こしていました。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- `append_selective` の不正確なメモリ計上によって引き起こされた、パーティション化されたジョインプローブにおける潜在的な境界外アクセス。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 設定フィールドの名前にタイポがありました。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` が range-colocate PACK シャードグループを孤立として誤って回収し、共有データモードでアクティブなシャードを永続的に削除していました。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- colocate タブレット分割のソートキーのアリティが、PRIMARY KEY テーブルおよび明示的な `ORDER BY` を持たないテーブルに対して、マテリアライズされたスキーマではなくベーススキーマから解決されていたため、分割ジョブがタブレットサイズを縮小せずに完了していました。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 自動マージデーモンが、パーティションデータがマージしきい値を下回った場合に分割前のタブレットを再結合し、サンプルベースの事前分割による並列性の恩恵を無効にしていました。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- db レベルの UDF は、関数の `FunctionName.db` がソースデータベースを指したままだったため、`RESTORE ... AS <new_db>` 後にフォロワー FE で欠落していました。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- ウェアハウスに複数の CN グループがある場合、共有データ `DISTRIBUTED BY RANDOM` CTAS/INSERT のイミュータブルパーティションタブレットロケーションに誤った CN グループが割り当てられていました。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 統計推定中にパーティションが同時に削除された場合、`StatisticsCalcUtils` で `NullPointerException` が発生していました。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` および `FrontendServiceImpl` のメタデータ RPC ハンドラーがデータベース全体の READ ロックを保持し、無関係なテーブルへの DDL をブロックしていました。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- パイプラインオペレーターが共有コンテキストオブザーバーに通知せずに終了状態を切り替えると、イベントスケジューラ下でピアドライバーが停止する可能性がありました。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 述語プッシュダウンにおける非ルートの複合述語が、スキャンレベルの EOF の代わりに `NotPushDown` を返し、UNION 配下に不可能なネスト AND ブランチが存在する場合に `OlapScanNode` が行を出力しない問題がありました。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` は単一ストレージメディアを持つ BE に対してコストの高いレプリカごとのスキャンを実行していました。均質ディスク BE に対するチェックは O(1) になりました。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- データディレクトリロードスレッドでのスレッド名設定の競合により、BE 起動のたびに `failed to set thread name` 警告が大量に出力されていました。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- タスクマネージャーが不正な `RUNNING→RUNNING` 編集ログを書き込み、タスク実行が実行中マップに無期限に停止しているように見える問題がありました。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK マルチステートメントバッチトランザクションが複合ローセット全体で `num_rows`、`data_size`、および `num_dels` を累積しなかったため、共有データ Primary Key テーブルの行数統計が不正確になっていました。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake ロードスピルのクリーンアップに txn-id ベースのバキューム駆動回収が使用されるようになり、BE クラッシュや OOM 後に孤立したスピルファイルが残る問題を防ぎます。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- 年 `0000` から始まる PostgreSQL JDBC の時刻値が、誤った型マッピング結果を引き起こしていました。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- スキーマ変更中にローセットから `gtid` を読み取る前の null チェックが欠落しており、NPE クラッシュが発生していました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1

リリース日: 2026年5月29日

### 動作変更

- Hive コネクターは、デフォルトで JNI Avro スキャナーの代わりにネイティブ C++ Avro スキャナーを使用するようになりました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- INCREMENTAL/AUTO マテリアライズドビューに対するクエリリライトが無効化され、INCREMENTAL/AUTO マテリアライズドビューに対する FORCE リフレッシュおよびパーティションリフレッシュが拒否されるようになりました。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改善

- Java UDF/UDAF/UDTF で、UDAF/UDTF の STRUCT 引数および戻り値、ネストされた ARRAY/MAP 型、DATE/DATETIME、DECIMAL、可変長引数など、より多くの型がサポートされるようになりました。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- スカラー UDF が STRUCT 引数をサポートするようになりました。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF がネストされた ARRAY/MAP 型をサポートするようになりました。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF がクエリをまたいで一度ロード・初期化され再利用されるようになり、クエリごとのオーバーヘッドが削減されました。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Hive コネクター向けに JNI Avro スキャナーをネイティブ C++ スキャナーに置き換え、直接バイナリデコードおよび `avro.schema.literal` と `avro.schema.url` のサポートを追加しました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS ステートメントで Trino の `WITH` 句をサポートします。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- シンクパスにおける Iceberg の `timestamptz` パーティション変換サポートを完成させました。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Iceberg テーブル集計に対する TopN ランタイムフィルタープッシュダウンを有効化しました。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Iceberg の datetime min/max 最適化をサポートします。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 複数の HDFS クラスターへのアクセスをサポートするため、Catalog および BE での HDFS HA 設定のパススルーを許可します。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 外部テーブルクエリに対するパーティションスキャン数の上限を追加しました。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- サポートされていない Iceberg V3 機能に対してフェイルファストします。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- INSERT INTO FILES による CSV エクスポートで `csv.enclose` および `csv.escape` をサポートします。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- `files()` へのフルスキーマプッシュダウン用に `enable_push_down_schema` INSERT プロパティを追加しました。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load ジョブが再試行不可能なエラー（例：主キーサイズ超過）で一時停止されるようになりました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 2 つの子からの複雑な式に対するジョイン並び替えをサポートします。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- CBO 統計推定を改善しました。`date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION、および定数に対する MCV/null 割合の伝播を含みます。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- スキュージョイン検出の改善: スキューはすべてのジョインキーがスキューしている場合にのみ検出され、スキュールールを強制するための `force_group_by_skew_eliminate_when_skewed` スイッチが追加されました。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- FE における `regexp_replace` の定数畳み込みをサポートします。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 定数パーティション値を持つ日付パーティション列に対する MIN/MAX を最適化しました。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- マテリアライズドビューのリフレッシュにおいて、`ASYNC` の同義語として `SCHEDULE` キーワードを導入しました。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 共有データモードの Lake テーブルに対するタブレット作成リトライをサポートします。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Lake カラムモードの部分更新に対する条件付き更新をサポートします。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 部分更新のパブリッシュ、永続インデックスの初期化、および SSTable のオープンを並列化し、インジェストスループットを向上させました。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- shared-nothing から shared-data へのレプリケーション中の DCG ファイル同期をサポートします。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- キー列および非キー列の両方で VARCHAR 長を拡張するスキーマ進化をサポートします。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- クラスタースナップショットの整合性チェック用に `snapshot_meta.json` マーカーを追加しました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- DN パターンによる LDAP ダイレクトバインド認証をサポートします。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- クエリのトラブルシューティングを容易にするために `get_query_dump_from_query_id` メタ関数を追加しました。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 監査ログにおけるクエリされたリレーションの監査をサポートします。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- MySQL バイナリ結果エンコーディング用のセッション変数を追加しました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 可観測性向上のためのメトリクスを追加しました。共有データクラスター向けの `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`、および Iceberg メタデータテーブルクエリメトリクスが含まれます。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- FE設定 `deploy_serialization_min_thread_pool_size` を追加しました。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- MergeTabletJobの作成を無効にする `tablet_reshard_enable_tablet_merge` 設定を追加しました。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- `SO_REUSEPORT` を使用してHTTPサーバーのaccept thundering-herdを排除しました。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- `CREATE FUNCTION ... AS <sql_body>` を使用したSQL UDFの作成をサポートしました。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- S3からのUDFの読み込みをサポートしました。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 時間順のUUID v7値を生成する `uuid_v7` 関数を追加しました。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- 外部カタログの可観測性のために、カタログタイプごとのクエリメトリクスを追加しました。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューされたパーティションキーを持つウィンドウ関数をUNIONに分割することで自動的に最適化します。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### セキュリティ

- [CVE] NettyをVersion 4.1.133.Finalにアップグレードしました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] pgjdbcを42.7.11（クライアント側のDoS：無制限のSCRAM PBKDF2イテレーション数）およびBouncyCastleを1.84（FrodoKEM秘密鍵漏洩）にバージョンアップしました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] Golang CVEを排除するためにgo1.25.9でpprofをビルドしました。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- jetty-httpを9.4.58.v20250814にアップグレードしました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- BrokerのCVE依存関係をクリーンアップし、`wildfly-openssl` を削除しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- INSERT INTO FILESのエラーメッセージ内の認証情報をマスクしました。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### バグ修正

以下の問題が修正されました：

- `hash_util` の静的初期化順序によって引き起こされる起動時のCNセグメンテーションフォルト。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 物理分割が有効な状態で空のタブレットをスキャンする際のCNクラッシュ。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- `information_schema.warehouse_queries` のクエリ時に BE がクラッシュする。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- rowset `num_rows` がゼロの場合、Lake コンパクション中に SIGFPE が発生する。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG フラグメント接続でゼロ除算が発生する。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer でグレースフル終了クラッシュが発生する。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- スピル可能なハッシュ結合プローブがクラッシュする。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 一時的な `std::string` へのフォーマット時にスタックバッファオーバーフローが発生する。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` でクラッシュが発生する。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 一時的な `shared_ptr` の破棄によって `LoadChannel::get_load_replica_status` で解放後使用が発生する。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- スレッド作成失敗時に `ThreadPool::do_submit` で解放後使用が発生する。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- フラグメント破棄をまたいで Hive パーティションディスクリプタの解放後使用が発生する。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- インフォメーションスキーマシンクで解放後使用が発生する。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- HttpClient インスタンスの再利用による FE のファイルディスクリプタリークが発生する。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` で JNI ローカル参照リークが発生する。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- MV プランコンテキストのキャッシュ時にメモリリークが発生する。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- ローカルエクスチェンジで予期しないメモリ過剰使用が発生する。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` の `response->tablet_metas` でレースが発生する。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` で `SegmentFlushTask` の同時実行レースが発生する。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- シリアライズ中に `RuntimeProfile` の min/max でレースが発生する。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- クエリコンテキスト破棄中に `PipelineTimerTask` でレース条件が発生する。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` でレース条件が発生する。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- 共有された `NullColumn` の問題（`map_apply` および `array_length`）。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- パーティションバージョンのギャップによって引き起こされたバッチパブリッシュのデッドロック。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- shared-nothing モードでの行セットメタデータの LRU キャッシュウォームアップ時のデッドロック。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` のロールバックが例外安全でなく、アンロック順序が正しくない。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 読み取り専用パスおよびメタデータパスにおける複数の DB ロックによって引き起こされた DDL と StarOS RPC のロック競合。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- プロジェクトノードの欠落による不正なシャッフル分散。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- クラッシュおよび誤った結果を引き起こす AGG TopN ランタイムフィルターの `exprOrder` ミスマッチ。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY による誤った結果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- クエリキャッシュとローカルシャッフル集計の競合。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- フラット JSON におけるグローバル辞書生成の不整合。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- フラット JSON マージの空の不整合。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 明示的なキー/値の型が宣言されている場合のマップリテラルにおける型の不一致。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- JOIN USING トランスフォーマーで COALESCE の子が共通型にキャストされない。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- グローバル変数を使用した reduce-cast 後に VARCHAR の長さが保持されない。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- MySQL 結果セットのネストされた型内で VARBINARY が誤ってエンコードされる。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 小さな LIMIT で集計スピルを無効にした場合の having 句チェックの問題。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日付解析前にクォートが除去されない問題、および PostgreSQL の日付/時刻のバグ。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- データファイルの shared フラグの消失により、バキュームが兄弟スプリットタブレットによってまだ参照されているファイルを削除してしまう問題。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→mergeシーケンスにおけるタブレットマージの正確性。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- タブレット分割中のクロスパブリッシュされたtxnログのnum_rows/data_sizeの膨張。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一パブリッシュバッチ内のwrite-before-compactionによって引き起こされるDelvecの孤立エントリ。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- StarMgrジャーナルリプレイを同期することによるフォロワーFEでの「no queryable replica」。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 通常のrowsetコミットを適用する際にmerge_conditionが保持されない問題。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- 不正なスナップショットIDとフィルターを使用したIceberg DELETEの競合検出。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 無効なIcebergトランスフォーム引数によるNPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- プランナーによって挿入された余分な列によりスキップされるIcebergのmin/max最適化。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- IcebergベーステーブルでのAggregate-join-pushdown MVの書き換え。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITEコミット前のHiveパーティションディレクトリの欠落。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNIスキャナーにAWS assume-roleが適用されない問題。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- プルーニングされた子とネストされたnullableスキーマに対するAvro複合型のデコード。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Loadエラーにファイル/列/行のコンテキストが欠落している問題。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- ParquetスキャナーでのArrow辞書値のサポート不足。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- PaimonテーブルのプライマリキーがSHOW CREATEに表示されず、DESCの返却にも表示されない問題。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBCの型互換性と末尾スラッシュを含むJDBC URL構築。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- JDBCカタログ内のSQL Serverテーブルでのマテリアライズドビューのリフレッシュ問題。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 外部結合上のマテリアライズドビューにおけるレイジーマテリアライゼーションスロットのnullability問題。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTOおよびINCREMENTALマテリアライズドビューのパーティションリフレッシュが拒否される問題。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- マテリアライズドビューが非アクティブになった後も、マテリアライズドビュースケジューラが停止されない。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- MySQLクライアント互換性のための`SHOW GRANTS FOR CURRENT_USER()`のサポートが不足している。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW文は明示的なトランザクション内では使用できない。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flightが空の結果セットに対してカラム名`r`を返す。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDFコードにJNI例外処理チェックが不足している。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query`関数の登録に関する問題。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- `enable_load_profile`使用時のStream LoadプロファイルコレクションのIssue。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- プロファイルのSTART_TIME/END_TIMEがセッションのタイムゾーンで表示されない。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec`がランタイムで変更可能でない。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables`が等値述語内の特殊文字をエスケープしない。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- エラーパスでの並列セグメント/ローセットロード中のuse-after-free。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 集計スピル`set_finishing`におけるハッシュテーブルデータ損失の可能性。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- ディスク再マイグレーション（A→B→A）中のGCレースによるPKタブレットのrowsetメタ損失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr`におけるDB読み取りロックのリーク。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVMリフレッシュ記録でPCTパーティションメタデータが不完全。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 参照カラムが欠落している場合、Stream Load/Broker LoadでのGeneratedカラム解析時にNPEが発生する。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- ショートサーキットポイントルックアップでパーティション述語が欠落している。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0

リリース日：2026年4月13日

### 共有データアーキテクチャ

- **新しいマルチテナントデータ管理**

  共有データクラスターは、範囲ベースのデータ分散およびタブレットの自動分割・マージをサポートするようになりました。タブレットは、スキーマ変更、SQL変更、またはデータの再取り込みを必要とせず、サイズが過大になったりホットスポットになった場合に自動的に分割できます。この機能により、使いやすさが大幅に向上し、マルチテナントワークロードにおけるデータスキューおよびホットスポットの問題に直接対処できます。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量タブレットサポート（フェーズ1）**

  共有データクラスターが1タブレットあたり大幅に多くのデータをホストできるようにし、長期的な目標は1タブレットあたり100 GBです。フェーズ1では、インジェスト、主キー更新、およびCompactionパイプライン全体にわたるタブレット内並列処理を導入し、単一のLakeタブレットが成長するにつれてシングルスレッドのボトルネックになることがなくなります。改善点には、単一タブレット内の並列Compaction（セグメントレベルの分割を含む）、Lake読み込み用の並列MemTableファイナライズ、フラッシュ、およびマージ（ロードスピルパスを含む）、主キーテーブル用のタブレット内部並列パブリッシュおよび並列条件更新、リモートストレージマッパーファイルサポートを備えたクラウドネイティブ主キーインデックス用のレンジ分割／並列／サイズ階層型Compactionが含まれます。これらの変更により、大容量タブレットワークロードにおけるインジェストメモリオーバーヘッド、Compaction増幅、およびFEメタデータ負荷が大幅に軽減されます。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **Fast Schema Evolution V2**

  共有データクラスターでFast Schema Evolution V2がサポートされるようになりました。これにより、スキーマ操作に対して秒単位のDDL実行が可能になり、さらにマテリアライズドビューへのサポートも拡張されます。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[ベータ] 共有データ上の転置インデックス**

  共有データクラスター向けに組み込みの転置インデックスを有効にし、テキストフィルタリングおよび全文検索ワークロードを高速化します。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **キャッシュの可観測性**

  クエリレベルのキャッシュヒット率が監査ログおよび監視システムに公開され、キャッシュの透明性とレイテンシ診断が向上しました。追加のデータキャッシュメトリクスには、メモリおよびディスククォータ使用量、ページキャッシュ統計が含まれます。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Lakeテーブルにセグメントメタデータフィルターを追加し、スキャン中にソートキー範囲に基づいて無関係なセグメントをスキップすることで、範囲述語クエリのI/Oを削減します。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- Lake DeltaWriterの高速キャンセルをサポートし、共有データクラスターでのキャンセルされた取り込みジョブのレイテンシを削減します。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 自動クラスタースナップショットのインターバルベーススケジューリングのサポートを追加しました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- MemTableのフラッシュとマージのパイプライン実行をサポートし、共有データクラスターのクラウドネイティブテーブルの取り込みスループットを向上させます。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- クラウドネイティブテーブルの修復のための`dry_run`モードをサポートし、実行前に修復アクションをプレビューできるようにします。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 共有なしクラスターのパブリッシュトランザクション用スレッドプールを追加し、パブリッシュスループットを向上させます。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### データレイク分析

- **Iceberg DELETEサポート**

  IcebergテーブルのポジションデリートファイルへのWriteをサポートし、StarRocksから直接IcebergテーブルへのDELETE操作を可能にします。Plan、Sink、Commit、Auditの全パイプラインをカバーします。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **HiveおよびIcebergテーブルのTRUNCATE**

  外部HiveおよびIcebergテーブルでのTRUNCATE TABLEをサポートします。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Icebergの増分マテリアライズドビュー**

  増分マテリアライズドビューのリフレッシュサポートをIcebergの追記専用テーブルに拡張し、テーブル全体のリフレッシュなしにクエリの高速化を実現します。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **IcebergにおけるセミStructuredデータのVARIANT型**

  Iceberg CatalogでVARIANTデータ型をサポートし、セミStructuredデータの柔軟なスキーマオンリードストレージとクエリを実現します。読み取り、書き込み、型キャスト、Parquet統合をサポートします。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3サポート**

  Iceberg v3のデフォルト値機能と行リネージのサポートを追加しました。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Icebergテーブルメンテナンスプロシージャ**

  `rewrite_manifests`プロシージャのサポートを追加し、`expire_snapshots`および`remove_orphan_files`プロシージャをより細かいテーブルメンテナンスのための追加引数で拡張しました。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- Icebergテーブルからファイルパスおよび行位置メタデータカラムの読み取りをサポートします。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- Iceberg v3テーブルから`_row_id`の読み取りをサポートし、Iceberg v3のグローバル遅延マテリアライゼーションをサポートします。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- カスタムプロパティを持つIcebergビューの作成をサポートし、SHOW CREATE VIEWの出力にプロパティを表示します。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 特定のブランチ、タグ、バージョン、またはタイムスタンプを指定したPaimonテーブルのクエリをサポートします。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- Paimonテーブルの複合型（ARRAY、MAP、STRUCT）をサポートします。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- Icebergテーブル作成時に括弧構文を使用したPartition Transformsをサポートします。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- データ整理の向上のため、Transform PartitionベースのIcebergグローバルシャッフルをサポートします。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- Icebergテーブルシンクのグローバルシャッフルを動的に有効化することをサポートします。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- Icebergテーブルシンクの同時Commitの競合を回避するためのCommitキューを導入しました。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- データ整理と読み取りパフォーマンスを向上させるため、Icebergテーブルシンクにホストレベルのソートを追加しました。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- ETL実行モードでデフォルトで追加の最適化を有効にし、明示的な設定なしにINSERT INTO SELECT、CREATE TABLE AS SELECTおよび類似のバッチ操作のパフォーマンスを向上させます。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- IcebergテーブルのINSERTおよびDELETE操作にコミット監査情報を追加しました。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- Iceberg REST Catalogでビューエンドポイント操作の有効化または無効化をサポートします。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- CachingIcebergCatalogのキャッシュルックアップ効率を最適化しました。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- さまざまなIcebergカタログタイプでのEXPLAINをサポートします。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- AWS Glue Catalogテーブルのパーティションプロジェクションをサポートします。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- AWS Glue `GetDatabases` APIのリソース共有タイプサポートを追加しました。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- エンドポイントインジェクション（`azblob`/`adls2`）を使用したAzure ABFS/WASBパスマッピングをサポートします。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- リモートRPCのオーバーヘッドおよび外部システム障害の影響を軽減するため、JDBCカタログにデータベースメタデータキャッシュを追加しました。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- `information_schema`でのPostgreSQLテーブルの列コメントをサポートします。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- OracleおよびPostgreSQLのJDBCタイプマッピングを改善しました。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### クエリエンジン

- **再帰CTE**

  階層トラバーサル、グラフクエリ、反復SQL計算のための再帰共通テーブル式（CTE）をサポートします。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 統計ベースのスキュー検出、ヒストグラムサポート、NULLスキュー認識を備えた改善されたSkew Join v2の書き換え。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- ウィンドウ関数上のCOUNT DISTINCTを改善し、融合マルチディスティンクト集計のサポートを追加しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューされたパーティションキーを持つウィンドウ関数をUNIONに分割することで自動最適化します。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- Trino ParserのINSERT文に対するEXPLAINおよびEXPLAIN ANALYZEをサポートします。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- クエリキューの可視性のためのEXPLAINをサポートします。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 関数とSQL構文

- 以下の関数を追加しました：
  - `array_top_n`：配列から値でランク付けされた上位N要素を返します。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：複数の配列を要素ごとに構造体の配列に結合します。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：JSON文字列をインデント付きでフォーマットします。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：JSON文字列内の指定されたパスに値を設定します。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：各単語の最初の文字を大文字に変換します。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：同じキーを持つ行のMAP値を合計します。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：現在のセッションタイムゾーンを返します。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：現在のウェアハウスの名前を返します。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：秒数をTIME値に変換します。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：推論ワークロードのためにSQLから外部AIモデルを呼び出します。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：SQL式でユーザー定義エラーを発生させます。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 以下の関数または構文拡張を提供します：
  - カスタムソート順のために `array_sort` でラムダコンパレータをサポートします。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - SQL標準のセマンティクスを持つFULL OUTER JOINのUSING句をサポートします。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - ORDER BY/PARTITION BYを使用したフレームウィンドウ関数に対するDISTINCT集計をサポートします。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`/`lag`/`first_value`/`last_value` ウィンドウ関数でARRAY型をサポートします。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - count distinct系の集計関数でVARBINARYをサポートします。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - IN式での日付および文字列型のキャストをサポートします。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - BEGIN/START TRANSACTIONのWITH LABEL構文をサポートします。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - SHOW文でのWHERE/ORDER/LIMIT句をサポートします。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - タスク管理のための `ALTER TASK` 文をサポートします。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - CSVファイルエクスポートで複数の圧縮形式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）をサポートします。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 名前ベースの構造体フィールドマッチングのための `STRUCT_CAST_BY_NAME` SQLモードをサポートします。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理と可観測性

- マルチウェアハウスのCPUリソース分離を改善するために、リソースグループの `warehouses`、`cpu_weight_percent`、および `exclusive_cpu_weight` 属性をサポートします。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FEスレッドの状態を検査するための `information_schema.fe_threads` システムビューを導入します。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスターレベルで特定のクエリパターンをブロックするSQLダイジェストブラックリストをサポートします。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワークトポロジーの制約によりアクセスできないノードからのArrow Flightデータ取得をサポートします。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続なしに既存の接続へグローバル変数の変更を伝播するREFRESH CONNECTIONSコマンドを導入します。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイルの分析やフォーマットされたSQLの表示を行う組み込みUI機能を追加し、クエリチューニングをより手軽にしました。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスター概要を提供する `ClusterSummaryActionV2` APIエンドポイントを実装します。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 現在のクラスター実行モード（shared-data または shared-nothing）を照会するためのグローバル読み取り専用システム変数 `@@run_mode` を追加しました。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- クエリキュー管理を改善するために、`query_queue_v2` をデフォルトで有効にしました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Stream Load および Merge Commit 操作に対するユーザーレベルのデフォルトウェアハウスをサポートします。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 必要に応じてバックエンドブラックリスト検証をバイパスするための `skip_black_list` セッション変数を追加しました。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- メトリクス API に `enable_table_metrics_collect` オプションを追加しました。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- クエリ詳細 HTTP API にユーザーなりすましサポートを追加しました。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- テーブルレベルのプロパティとして `table_query_timeout` を追加しました。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- FE オブザーバーノードの追加をサポートします。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- ロードジョブの可視性向上のために、`information_schema.loads` での Merge Commit 情報の表示をサポートします。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- トラブルシューティングを改善するために、クラウドネイティブテーブルでのタブレットステータスの表示をサポートします。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### セキュリティ

- [CVE-2026-33870] [CVE-2026-33871] AWS バンドルを置き換え、Netty を 4.1.132.Final にバージョンアップしました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Hadoop を v3.4.2 にアップグレードしました。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] `spark-core_2.12` を 3.5.7 にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### バグ修正

以下の問題が修正されました：

- 範囲分散タブレットのデータファイル削除をスキップすることで、タブレット分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 複合型における `DefaultValueColumnIterator` のメモリリークを修正しました。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- `BatchUnit` と `FetchTaskContext` 間の `shared_ptr` サイクルによって引き起こされるメモリリークを修正しました。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 同時 getline アクセスによる SystemMetrics のダブルフリークラッシュを修正しました。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- eager merge がすべてのブロックを消費した際の SpillMemTableSink のクラッシュを修正しました。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- TTLクリーナーによって自動作成されたパーティションが削除された際のNPEを修正しました。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- スナップショットが期限切れになった際の`IcebergCatalog.getPartitionLastUpdatedTime`におけるNPEを修正しました。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 定数側の列参照を持つ外部結合に対する誤った述語の書き換えを修正しました。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 共有データでCHAR列の長さを変更した後の誤ったクエリ結果を修正しました。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 複数テーブルの場合のMVリフレッシュのバグを修正しました。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 強制リフレッシュ時のMVリサイクル時間が不正確になる問題を修正しました。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 同期MVにおける全null値の処理バグを修正しました。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 高速スキーマ変更のADD COLUMN後にMVをクエリした際の重複列IDエラーを修正しました。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 共有DecodeInfoによって引き起こされる低カーディナリティ書き換えのNPEを修正しました。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 低カーディナリティの結合述語の型不一致を修正しました。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- `null_counts`が空の場合のParquet Page Index FilterにおけるSegfaultを修正しました。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 同一パス上でのJSONフラット化配列とオブジェクトの競合を修正しました。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Icebergキャッシュウェイヤーの不正確さを修正しました。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Icebergテーブルキャッシュのメモリ制限を修正しました。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Icebergの削除列のnull許容性の問題を修正しました。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- コンテナを含めるようにAzure ABFS/WASB FileSystemのキャッシュキーを修正しました。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- HMS接続プールが満杯の場合のデッドロックを修正しました。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Paimon CatalogにおけるVARCHARフィールドタイプの誤った長さを修正しました。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- ObjectTableでのClassCastExceptionによるPaimonカタログリフレッシュのクラッシュを修正しました。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 定数サブクエリを使用したFULL OUTER JOIN USINGを修正しました。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- CTEスコープのJON ON句のバグを修正しました。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- bindScope()パターンを使用してConnectContextのメモリリークを修正しました。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- shared-nothingクラスターにおける`CatalogRecycleBin.asyncDeleteForTables`のメモリリークを修正しました。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 例外が発生した際にThriftのacceptスレッドが終了する問題を修正しました。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- ルーティングロードのカラムマッピングにおけるUDF解決を修正しました。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- `DROP FUNCTION IF EXISTS`が`ifExists`フラグを無視する問題を修正しました。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- dictページが大きすぎる場合のスキャン結果エラーを修正しました。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 範囲パーティションの重複を修正しました。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- クエリキューの割り当て時間と保留タイムアウトを修正しました。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- nullリテラル配列を処理する際の`array_map`クラッシュを修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64`のスタックオーバーフローを修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- LDAP認証におけるユーザー名の大文字小文字を区別しない正規化を修正しました。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- API `proc_file`のSSRFリスクを軽減しました。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 監査およびSQL難読化においてユーザー認証文字列をマスクしました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 動作の変更

- ETL実行モードの最適化がデフォルトで有効になりました。これにより、明示的な設定変更なしに、INSERT INTO SELECT、CREATE TABLE AS SELECT、および類似のバッチワークロードが恩恵を受けます。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead`ウィンドウ関数の第3引数が、定数値に加えてカラム参照もサポートするようになりました。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USINGがSQL標準のセマンティクスに従うようになりました。USINGカラムは出力に2回ではなく1回だけ表示されます。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2`がデフォルトで有効になりました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQLトランザクションは、デフォルトでセッション変数`enable_sql_transaction`によって制御されます。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
