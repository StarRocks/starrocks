---
displayed_sidebar: docs
---

# StarRocks version 4.0

## 4.0.0-RC02

リリース日: 2025年9月29日

### 新機能

- Iceberg テーブル作成時にソートキーを設定可能。
- マルチテーブル Write-Write トランザクションに対応し、`INSERT`、`UPDATE`、`DELETE` 操作のアトミックコミットを実現。Stream Load と `INSERT INTO` インターフェースの両方をサポートし、ETL やリアルタイム書き込みシナリオにおけるクロステーブル一貫性を保証。
- 集計テーブルの集計キーを変更可能に。

### 機能改善

- Delta Lake Catalog キャッシュ設定を最適化：`DELTA_LAKE_JSON_META_CACHE_TTL` と `DELTA_LAKE_CHECKPOINT_META_CACHE_TTL` のデフォルト値を24時間に変更し、Parquet handler のロジックを簡素化。 [#63441](https://github.com/StarRocks/starrocks/pull/63441)
- Delta Lake Catalog のエラーログの形式と内容を改善し、デバッグ体験を向上。 [#63389](https://github.com/StarRocks/starrocks/pull/63389)
- 外部グループ（例: LDAP Group）がロールの付与・剥奪・表示に対応し、SQL 構文とテストケースを強化して権限管理を改善。 [#63385](https://github.com/StarRocks/starrocks/pull/63385)
- Stream Load のパラメータ一貫性チェックを強化し、パラメータドリフトによるリスクを軽減。 [#63347](https://github.com/StarRocks/starrocks/pull/63347)
- Stream Load の Label 伝達メカニズムを最適化し依存関係を削減。 [#63334](https://github.com/StarRocks/starrocks/pull/63334)
- `ANALYZE PROFILE` の形式を改善し、ExplainAnalyzer がオペレーターごとにメトリクスをグループ表示可能に。 [#63326](https://github.com/StarRocks/starrocks/pull/63326)
- `QueryDetailActionV2` と `QueryProfileActionV2` API を改善し、JSON 形式で結果を返却。 [#63235](https://github.com/StarRocks/starrocks/pull/63235)
- 大量の CompoundPredicates を含むシナリオでの述語解析を改善。 [#63139](https://github.com/StarRocks/starrocks/pull/63139)
- 一部の FE メトリクスを leader 認識型に変更。 [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- `SHOW PROCESS LIST` を改善し、Catalog と Query ID 情報を追加。 [#62552](https://github.com/StarRocks/starrocks/pull/62552)
- BE JVM メモリ監視メトリクスを改善。 [#62210](https://github.com/StarRocks/starrocks/pull/62210)
- マテリアライズドビューのリライトロジックとログ出力を最適化。 [#62985](https://github.com/StarRocks/starrocks/pull/62985)
- ランダムバケット戦略を最適化。 [#63168](https://github.com/StarRocks/starrocks/pull/63168)
- `ALTER TABLE <table_name> AUTO_INCREMENT = 10000;` で AUTO_INCREMENT 値の開始点をリセット可能に。 [#62767](https://github.com/StarRocks/starrocks/pull/62767)
- Group Provider が DN による Group マッチングをサポート。 [#62711](https://github.com/StarRocks/starrocks/pull/62711)

### バグ修正

以下の問題を修正しました：

- ARRAY 低カーディナリティ最適化により `Left Join` 結果が不完全になる問題。 [#63419](https://github.com/StarRocks/starrocks/pull/63419)
- マテリアライズドビュー集計プッシュダウンリライト後に誤った実行計画が生成される問題。 [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- JSON フィールドの剪定シナリオで Schema にフィールドが見つからない場合、不要な Warning ログが出力される問題。 [#63414](https://github.com/StarRocks/starrocks/pull/63414)
- ARM 環境で DECIMAL256 データ挿入時に SIMD Batch パラメータ誤りで無限ループが発生する問題。 [#63406](https://github.com/StarRocks/starrocks/pull/63406)
- ストレージ関連の3つの問題: [#63398](https://github.com/StarRocks/starrocks/pull/63398)
  - ディスクパスが空の際にキャッシュ例外が発生。
  - Azure キャッシュ Key プレフィックスの誤り。
  - S3 マルチパートアップロードの異常。
- Fast Schema Evolution による CHAR → VARCHAR スキーマ変更後、ZoneMap フィルタが無効になる問題。 [#63377](https://github.com/StarRocks/starrocks/pull/63377)
- 中間型 `ARRAY<NULL_TYPE>` による ARRAY 集計型解析エラー。 [#63371](https://github.com/StarRocks/starrocks/pull/63371)
- 自動インクリメント列に基づく Partial Update 時のメタデータ不整合問題。 [#63370](https://github.com/StarRocks/starrocks/pull/63370)
- Tablet 削除やクエリ同時実行時のメタ情報不整合問題。 [#63291](https://github.com/StarRocks/starrocks/pull/63291)
- Iceberg テーブル書き込み時に `spill` ディレクトリ作成が失敗する問題。 [#63278](https://github.com/StarRocks/starrocks/pull/63278)
- Ranger Hive Service 権限変更が反映されない問題。 [#63251](https://github.com/StarRocks/starrocks/pull/63251)
- Group Provider が `IF NOT EXISTS` と `IF EXISTS` 句をサポートしていない問題。 [#63248](https://github.com/StarRocks/starrocks/pull/63248)
- Iceberg パーティションに予約語を使用した際の異常。 [#63243](https://github.com/StarRocks/starrocks/pull/63243)
- Prometheus メトリクス形式の問題。 [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- Compaction 有効化時にレプリケーショントランザクションを開始するとバージョン検証が失敗する問題。 [#62663](https://github.com/StarRocks/starrocks/pull/62663)
- File Bunding 有効化後に Compaction Profile が欠落する問題。 [#62638](https://github.com/StarRocks/starrocks/pull/62638)
- Clone 後の冗長レプリカ処理問題。 [#62542](https://github.com/StarRocks/starrocks/pull/62542)
- Delta Lake テーブルでパーティション列が見つからない問題。 [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- 共有データクラスタにおけるマテリアライズドビューが Colocation をサポートしない問題。 [#62941](https://github.com/StarRocks/starrocks/pull/62941)
- Iceberg テーブル NULL パーティション読み取りの問題。 [#62934](https://github.com/StarRocks/starrocks/pull/62934)
- Histogram 統計の MCV (Most Common Values) にシングルクォートが含まれると SQL 構文エラーになる問題。 [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- `KILL ANALYZE` コマンドが無効になる問題。 [#62842](https://github.com/StarRocks/starrocks/pull/62842)
- Stream Load Profile の収集に失敗する問題。 [#62802](https://github.com/StarRocks/starrocks/pull/62802)
- CTE Reuse 計画抽出の誤り。 [#62784](https://github.com/StarRocks/starrocks/pull/62784)
- BE 選択の異常により Rebalance が失敗する問題。 [#62776](https://github.com/StarRocks/starrocks/pull/62776)
- `User Property` の優先度が `Session Variable` より低い問題。 [#63173](https://github.com/StarRocks/starrocks/pull/63173)

## 4.0.0-RC

リリース日：2025年9月9日

### データレイク分析

- BE メタデータ用の Page Cache と Data Cache を統合し、スケーリングに適応的な戦略を採用。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Iceberg 統計情報のメタデータファイル解析を最適化し、繰り返し解析を回避。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Iceberg メタデータに対する COUNT/MIN/MAX クエリを最適化し、データファイルスキャンを効率的にスキップすることで、大規模パーティションテーブルの集約クエリ性能を大幅に向上させ、リソース消費を削減。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- プロシージャ `rewrite_data_files` により Iceberg テーブルの Compaction をサポート。
- 隠しパーティション（Hidden Partition）を持つ Iceberg テーブルをサポート（作成、書き込み、読み取りを含む）。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Paimon カタログで TIME データ型をサポート。[#58292](https://github.com/StarRocks/starrocks/pull/58292)

<!--
- Iceberg テーブルのソートを最適化。
-->

### セキュリティと認証

- JWT 認証と Iceberg REST Catalog を利用するシナリオで、StarRocks は REST Session Catalog を介してユーザーログイン情報を Iceberg に透過し、その後のデータアクセス認証をサポート。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg カタログ用の Vended Credential をサポート。

<!--
- StarRocks FE 側で証明書を設定することで HTTPS をサポートし、クラウドやイントラネットでの暗号化通信要件を満たす安全なシステムアクセスを実現。[#56394](https://github.com/StarRocks/starrocks/pull/56394)
- BE ノード間の HTTPS 通信をサポートし、データ伝送の暗号化と完全性を保証。内部データ漏洩や中間者攻撃を防止。[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### ストレージ最適化とクラスタ管理

- 共有データクラスタのクラウドネイティブテーブルにファイルバンドル（File Bundling）最適化を導入。ロード、Compaction、Publish 操作によって生成されるデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Routine Load で Kafka 4.0 をサポート。
- 共有なしクラスタの主キーテーブルに対する全文インバーテッドインデックスをサポート。
- カタログ、データベース、テーブル、ビュー、マテリアライズドビューの名前に対して大文字小文字を区別しない処理を有効化可能。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 共有データクラスタにおける Compute  Node のブラックリスト化をサポート。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- グローバル接続 ID をサポート。[#57256](https://github.com/StarRocks/starrocks/pull/57276)

<!--
- 複数テーブル間の Write-Write トランザクション（Multi-Table Write-Write Transaction）をサポートし、INSERT、UPDATE、DELETE 操作のアトミックコミットを制御可能。Stream Load および INSERT INTO インターフェイスをサポートし、ETL やリアルタイム書き込みシナリオにおけるクロステーブルの一貫性を保証。
- 集約テーブルの集約キーの変更をサポート。
-->

### クエリと性能改善

- DECIMAL256 データ型をサポートし、精度の上限を 38 ビットから 76 ビットに拡張。256 ビットのストレージにより、高精度が求められる金融や科学計算シナリオに柔軟に対応し、大規模集約や高次演算での DECIMAL128 の精度オーバーフロー問題を効果的に緩和。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- JOIN および AGG 演算子の性能を最適化。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] SQL Plan Manager を導入し、クエリとクエリプランをバインド可能に。これにより、システム状態（データ更新や統計更新）の変化によるクエリプランの変動を防止し、クエリ性能を安定化。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Partition-wise Spillable Aggregate/Distinct 演算子を導入し、従来のソートベース集約による Spill 実装を置き換え。複雑かつ高カーディナリティの GROUP BY シナリオで集約性能を大幅に改善し、読み書き負荷を削減。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - テーブルレベルで Flat JSON を設定可能。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - JSON カラム型ストレージを強化：V1 メカニズムを維持しつつ、Page レベルおよび Segment レベルのインデックス（ZoneMap、ブルームフィルター）、遅延マテリアライゼーションを伴う述語プッシュダウン、辞書エンコーディング、低カーディナリティのグローバル辞書の統合を追加し、実行効率を大幅に向上。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- STRING データ型向けに適応型 ZoneMap インデックス作成戦略をサポート。[#61960](https://github.com/StarRocks/starrocks/issues/61960)

### 関数と SQL 構文

- 以下の関数を追加：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` および `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
- 以下の構文拡張を提供：
  - CREATE ANALYZE FULL TABLE で IF NOT EXISTS キーワードをサポート。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT で EXCLUDE 句をサポート。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 集約関数で FILTER 句をサポートし、条件付き集約の可読性と実行効率を向上。[#58937](https://github.com/StarRocks/starrocks/pull/58937)
