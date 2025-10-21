---
displayed_sidebar: docs
---

# StarRocks version 4.0

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

- 共有データクラスタのクラウドネイティブテーブルにファイルバンドル（File Bundling）最適化を導入。ロード、Compaction、Publish 操作によって生成されるデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
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
