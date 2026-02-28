---
displayed_sidebar: docs
---

# StarRocks version 4.1

## 4.1.0-RC

リリース日：2026年2月28日

### 共有データアーキテクチャ

- **新しいマルチテナントデータ管理**
  共有データクラスタで、レンジベースのデータ分散および Tablet の自動分割をサポートしました。Tablet が肥大化した場合やホットスポットが発生した場合でも、スキーマ変更や SQL 修正、データ再投入なしで自動分割が可能です。マルチテナント環境におけるデータスキューやホットスポット問題を直接的に緩和します。 [#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743)
- **大容量 Tablet サポート（フェーズ1）**
  共有データクラスタで、Tablet あたりのデータ容量を大幅に拡張しました（長期目標：100GB/Tablet）。フェーズ1では、単一 Lake Tablet 内での並列 Compaction と並列 MemTable Finalize を実装し、Tablet サイズ増加時の取り込みおよび Compaction のオーバーヘッドを削減します。 [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#68677](https://github.com/StarRocks/starrocks/pull/68677)
- **Fast Schema Evolution V2**
  秒単位で DDL を実行可能な Fast Schema Evolution V2 を共有データクラスタでサポートし、マテリアライズドビューにも対応しました。 [#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)
- **[Beta] 共有データ向け転置インデックス**
  共有データクラスタで組み込み転置インデックスを利用可能にし、テキストフィルタや全文検索ワークロードを高速化します。 [#66541](https://github.com/StarRocks/starrocks/pull/66541)
- **キャッシュ可観測性の向上**
  監査ログおよび監視システムにキャッシュヒット率を公開し、キャッシュの可視性とレイテンシ予測性を向上しました。メモリ／ディスククォータ、ページキャッシュ統計、テーブル別ヒット率などの詳細な Data Cache 指標を提供します。 [#63964](https://github.com/StarRocks/starrocks/pull/63964)
- Lake テーブルでソートキー範囲に基づく Segment メタデータフィルタを追加し、レンジ条件クエリ時の I/O を削減。 [#68124](https://github.com/StarRocks/starrocks/pull/68124)
- Lake DeltaWriter の高速キャンセルをサポートし、キャンセルされた取り込みジョブの遅延を削減。 [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 自動クラスタスナップショットの間隔ベーススケジューリングをサポート。 [#67525](https://github.com/StarRocks/starrocks/pull/67525)

### データレイク分析

- **Iceberg DELETE サポート**
  Iceberg テーブルに対して position delete ファイルを書き込み可能にし、StarRocks から直接 DELETE を実行可能にしました。Plan、Sink、Commit、Audit の全パイプラインに対応します。 [#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)
- **Hive および Iceberg テーブルの TRUNCATE**
  外部 Hive および Iceberg テーブルに対する TRUNCATE TABLE をサポート。 [#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)
- **Iceberg および Paimon に対する増分マテリアライズドビュー**
  Iceberg append-only テーブルおよび Paimon テーブルで増分リフレッシュをサポートし、フルリフレッシュなしでクエリ高速化を実現。 [#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)
- Iceberg テーブルのファイルパスおよび行位置メタデータ列を読み取り可能。 [#67003](https://github.com/StarRocks/starrocks/pull/67003)
- Iceberg v3 テーブルの `_row_id` をサポートし、global late materialization に対応。 [#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- カスタムプロパティ付き Iceberg ビューの作成をサポートし、SHOW CREATE VIEW に表示。 [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Paimon テーブルを branch、tag、version、timestamp 指定でクエリ可能。 [#63316](https://github.com/StarRocks/starrocks/pull/63316)
- ETL 実行モード最適化をデフォルト有効化し、INSERT INTO SELECT や CTAS の性能を向上。 [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- Iceberg テーブルの INSERT／DELETE にコミット監査情報を追加。 [#69198](https://github.com/StarRocks/starrocks/pull/69198)
- Iceberg REST Catalog の view endpoint 操作の有効／無効を切替可能。 [#66083](https://github.com/StarRocks/starrocks/pull/66083)
- CachingIcebergCatalog のキャッシュ検索効率を改善。 [#66388](https://github.com/StarRocks/starrocks/pull/66388)
- 各種 Iceberg catalog タイプに対する EXPLAIN をサポート。 [#66563](https://github.com/StarRocks/starrocks/pull/66563)

### クエリエンジン

- **ASOF JOIN**
  時系列およびイベント相関クエリ向けに ASOF JOIN を導入。時間または順序キーに基づき、2つのデータセット間で最も近いレコードを効率的にマッチングします。 [#63070](https://github.com/StarRocks/starrocks/pull/63070) [#63236](https://github.com/StarRocks/starrocks/pull/63236)
- **半構造化データ向け VARIANT 型**
  Schema-on-read に対応した VARIANT 型を導入。読み書き、型変換、Parquet 統合をサポート。 [#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)
- **再帰 CTE**
  階層探索、グラフクエリ、反復 SQL 計算向けに再帰 Common Table Expression をサポート。 [#65932](https://github.com/StarRocks/starrocks/pull/65932)
- Skew Join v2 を改善し、統計ベースのスキュー検出、ヒストグラム対応、NULL スキュー認識を実装。 [#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- ウィンドウ関数での COUNT DISTINCT を改善し、複数 DISTINCT 集約の融合をサポート。 [#67453](https://github.com/StarRocks/starrocks/pull/67453)

### 関数および SQL 構文

- 新規関数：
  - `array_top_n`：値順に並べた配列から上位 N 要素を返します。 [#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：複数配列を要素単位で結合し、構造体配列を生成します。 [#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：JSON 文字列を整形してインデントを付与します。 [#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：JSON 文字列内の指定パスに値を設定します。 [#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：各単語の先頭文字を大文字に変換します。 [#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：同一キーを持つ MAP 値を行間で合計します。 [#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：現在のセッションのタイムゾーンを返します。 [#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：現在の Warehouse 名を返します。 [#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：秒数を TIME 型に変換します。 [#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：SQL から外部 AI モデルを呼び出して推論を実行します。 [#61583](https://github.com/StarRocks/starrocks/pull/61583)
- 構文拡張：
  - `array_sort` で lambda 比較関数をサポート。 [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - FULL OUTER JOIN USING が SQL 標準セマンティクスに準拠。 [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - ORDER BY／PARTITION BY 付きウィンドウで DISTINCT 集約をサポート。 [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`／`lag`／`first_value`／`last_value` で ARRAY 型をサポート。 [#63547](https://github.com/StarRocks/starrocks/pull/63547)

### 管理と可観測性

- リソースグループで `warehouses`、`cpu_weight_percent`、`exclusive_cpu_weight` をサポートし、CPU 分離を強化。 [#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FE スレッド状態を確認できる `information_schema.fe_threads` システムビューを追加。 [#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスタレベルで特定クエリパターンをブロックする SQL Digest ブラックリストをサポートしました。 [#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワーク制約で直接アクセスできないノードから Arrow Flight によるデータ取得をサポートしました。 [#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続なしで既存接続へグローバル変数変更を反映する REFRESH CONNECTIONS コマンドを導入しました。 [#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイル分析および整形済み SQL 表示を行う組み込み UI 機能を追加しました。 [#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスタ概要を提供する `ClusterSummaryActionV2` API エンドポイントを実装しました。 [#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 現在のクラスタ実行モード（shared-data または shared-nothing）を確認できる読み取り専用システム変数 `@@run_mode` を追加しました。 [#69247](https://github.com/StarRocks/starrocks/pull/69247)

## 動作変更

- ETL 実行モード最適化をデフォルト有効化し、INSERT INTO SELECT や CTAS の性能を向上。 [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`／`lead` の第3引数で列参照をサポート。 [#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING が SQL 標準仕様に準拠（列は1回のみ出力）。 [#65122](https://github.com/StarRocks/starrocks/pull/65122)
