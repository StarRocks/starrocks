---
displayed_sidebar: docs
---

# StarRocks バージョン 3.5

## v3.5.0-RC01

### 共有データクラスタ機能強化

- 共有データクラスタで生成列（Generated Column）をサポートしました。[#53526](https://github.com/StarRocks/starrocks/pull/53526)
- 共有データクラスタ内のクラウドネイティブな主キーテーブルで、特定インデックスの再構築をサポートし、インデックスの性能も最適化しました。[#53971](https://github.com/StarRocks/starrocks/pull/53971) [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- 大規模なデータロード操作の実行ロジックを最適化し、メモリ制限による Rowset の小ファイルの大量生成を回避。ロード中、システムは一時的なデータブロックをマージして小さなファイルの生成を減らし、ロード後のクエリパフォーマンスを向上させるとともに、その後のコンパクション操作を減らしてシステムリソースの使用率を向上させます。[#53954](https://github.com/StarRocks/starrocks/issues/53954)

### データレイク分析

- **[Beta]** Hive Metastore 統合による Iceberg Catalog での Iceberg ビューの作成サポートしました。また、ALTER VIEW 文を使って Iceberg ビューの SQL 方言の追加・変更が可能になり、外部システムとの互換性が向上しました。[#56120](https://github.com/StarRocks/starrocks/pull/56120)
- Iceberg REST Catalog におけるネストされた名前空間をサポートしました。[#58016](https://github.com/StarRocks/starrocks/pull/58016)
- Iceberg REST Catalog にて、`IcebergAwsClientFactory` を使用して AWS クライアントを作成できる、Vended Credential をサポートしました。[#58296](https://github.com/StarRocks/starrocks/pull/58296)
- Parquet Reader が Bloom Filter を使用したデータフィルタリングをサポートしました。[#56445](https://github.com/StarRocks/starrocks/pull/56445)
- Parquet 形式の Hive/Iceberg テーブルに対し、クエリ実行時に低カーディナリティ列のグローバル辞書を自動生成する機能をサポートしました。[#55167](https://github.com/StarRocks/starrocks/pull/55167)

### パフォーマンスとクエリ最適化

- 統計情報の最適化：
  - Table Sample をサポート。物理ファイルのデータブロックをサンプリングすることで、統計の精度とクエリ性能を改善。[#52787](https://github.com/StarRocks/starrocks/issues/52787)
  - クエリで使用される述語列を記録し、対象列に対して効率的な統計情報収集を可能に。[#53204](https://github.com/StarRocks/starrocks/issues/53204)
  - パーティション単位でのカーディナリティ推定をサポート。システムビュー `_statistics_.column_statistics` を使用して各パーティションの NDV を記録。[#51513](https://github.com/StarRocks/starrocks/pull/51513)
  - 複数列に対する Joint NDV の収集をサポートし、列間に相関がある場合の CBO 最適化精度を向上。[#56481](https://github.com/StarRocks/starrocks/pull/56481) [#56715](https://github.com/StarRocks/starrocks/pull/56715) [#56766](https://github.com/StarRocks/starrocks/pull/56766) [#56836](https://github.com/StarRocks/starrocks/pull/56836)
  - ヒストグラムを使用した Join ノードのカーディナリティと in_predicate の選択性推定をサポートし、データ偏り時の精度を改善。[#57874](https://github.com/StarRocks/starrocks/pull/57874)
  - Query Feedback をサポート。同一構造で異なるパラメータを持つクエリを同一グループに分類し、実行計画の最適化に役立つ情報を共有。[#58306](https://github.com/StarRocks/starrocks/pull/58306)
- 特定のシナリオで Bloom Filter に代わる最適化手段として Runtime Bitset Filter をサポート。[#57157](https://github.com/StarRocks/starrocks/pull/57157)
- Join Runtime Filter のストレージレイヤへのプッシュダウンをサポート。[#55124](https://github.com/StarRocks/starrocks/pull/55124)
- Pipeline Event Scheduler をサポート。[#54259](https://github.com/StarRocks/starrocks/pull/54259)

### パーティション管理

- 時間関数に基づいた式パーティションのマージを ALTER TABLE で実行可能にし、ストレージ効率とクエリ性能を改善。[#56840](https://github.com/StarRocks/starrocks/pull/56840)
- List パーティションテーブルおよびマテリアライズドビューに対するパーティションの TTL をサポートし、`partition_retention_condition` プロパティを設定することで柔軟なパーティション削除ポリシーを実現。[#53117](https://github.com/StarRocks/starrocks/issues/53117)
- 一般的なパーティション式に基づいた複数パーティションの削除を ALTER TABLE で実行可能に。[#53118](https://github.com/StarRocks/starrocks/pull/53118)

### クラスタ管理

- FE の Java コンパイルターゲットを Java 11 から Java 17 にアップグレードし、システムの安定性と性能を改善。[#53617](https://github.com/StarRocks/starrocks/pull/53617)

### セキュリティと認証

- MySQL プロトコルに基づいた SSL 暗号化接続をサポート。[#54877](https://github.com/StarRocks/starrocks/pull/54877)
- 外部認証との統合強化：
  - OAuth 2.0 と JSON Web Token（JWT）を使用して StarRocks ユーザーを作成可能に。
  - Security Integration 機能を導入し、LDAP、OAuth 2.0、JWT との認証統合を簡素化。[#55846](https://github.com/StarRocks/starrocks/pull/55846)
- Group Provider をサポート。LDAP、OS、ファイルからユーザーグループ情報を取得し、認証・認可に利用可能。関数 `current_group()` を使って所属グループを確認できます。[#56670](https://github.com/StarRocks/starrocks/pull/56670)

### マテリアライズドビュー

- 複数のパーティション列または式の指定をサポートし、より柔軟なパーティショニング戦略を構成可能に。[#52576](https://github.com/StarRocks/starrocks/issues/52576)
- `query_rewrite_consistency` を `force_mv` に設定することで、クエリリライト時にマテリアライズドビューの使用を強制可能に。これにより、ある程度のデータ鮮度を犠牲にしてパフォーマンスの安定性を確保。[#53819](https://github.com/StarRocks/starrocks/pull/53819)

### ロードとアンロード

- JSON 解析エラーが発生した場合、`pause_on_json_parse_error` プロパティを `true` に設定することで Routine Load ジョブを一時停止可能に。[#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[Experimental]** 複数の SQL 文を含むトランザクション（現時点では INSERT のみ対応）をサポート。トランザクションの開始・適用・取り消しにより、複数のロード操作に ACID 特性を提供。[#53978](https://github.com/StarRocks/starrocks/issues/53978)

### 関数

- セッションおよびグローバルレベルでシステム変数 `lower_upper_support_utf8` を導入し、`upper()` や `lower()` などの大文字・小文字変換関数が UTF-8（特に非 ASCII 文字）をより適切に扱えるように改善。[#56192](https://github.com/StarRocks/starrocks/pull/56192)
- 新しい関数を追加：
  - `field()` [#55331](https://github.com/StarRocks/starrocks/pull/55331)
  - `ds_theta_count_distinct()` [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - `array_flatten()` [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - `inet_aton()` [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - `percentile_approx_weight()` [#57410](https://github.com/StarRocks/starrocks/pull/57410)

### アップグレードに関する注意

- StarRocks v3.5.0 以降は JDK 17 以上が必要です。v3.4 以前からのアップグレードでは、StarRocks が依存する JDK  バージョンを 17 以上に更新し、FE の構成ファイル **fe.conf** の `JAVA_OPTS` にある JDK 17 と互換性のないオプション（CMS や GC 関連など）を削除する必要があります。また、v3.5.0 以降、StarRocks は特定の JDK バージョン向けの JVM 構成を提供しません。すべての JDK バージョンに対して共通の `JAVA_OPTS` を使用します。

