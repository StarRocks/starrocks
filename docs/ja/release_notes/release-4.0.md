---
displayed_sidebar: docs
---

# StarRocks version 4.0

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
