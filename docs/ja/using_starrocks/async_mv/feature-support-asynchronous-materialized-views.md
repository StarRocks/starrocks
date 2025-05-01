---
displayed_sidebar: docs
sidebar_position: 50
sidebar_label: Feature Support
---

# 機能サポート: 非同期マテリアライズドビュー

非同期マテリアライズドビューは StarRocks v2.4 以降でサポートされています。非同期マテリアライズドビューは、StarRocks 内やデータレイク上の大規模なテーブルに対するジョインや集計を含む複雑なクエリを高速化するために設計されています。クエリが頻繁に実行される場合や十分に複雑な場合、パフォーマンスの差は顕著になることがあります。さらに、非同期マテリアライズドビューは、データウェアハウス上で数学モデルを構築する際にも特に有用です。

このドキュメントでは、非同期マテリアライズドビューの能力の範囲と、関連する機能のサポートされているバージョンについて説明します。

## DDL 機能

| 機能                       | 説明                                              | サポートされているバージョン |
| :-------------------------------- | :----------------------------------------------------------- | :----------------------- |
| Auto Analyze                      | マテリアライズドビューが作成された後に統計を自動的に収集し、書き換えの失敗を回避します。 | v3.0+                           |
| Random Bucketing                  | マテリアライズドビューに対してデフォルトでランダムバケット法を有効にします。 | v3.1+                           |
| Deferred Refresh                  | CREATE MATERIALIZED VIEW で DEFERRED または IMMEDIATE を使用して、マテリアライズドビューを作成後にすぐにリフレッシュするかどうかを指定できます。 | v3.0+                           |
| Order By                          | ORDER BY を使用してマテリアライズドビューのソートキーを指定できます。 | v3.1+                           |
| Window/CTE/Union/Subquery         | マテリアライズドビューでウィンドウ関数、CTE、Union、およびサブクエリを使用できます。 | v2.5+                           |
| ALTER ACTIVE                      | ベーステーブルのスキーマ変更後に、ALTER MATERIALIZED VIEW の ACTIVE キーワードを使用して無効なマテリアライズドビューをアクティブ化します。 | v2.5.7+<br />v3.0.1+<br />v3.1+ |
| REFRESH SYNC MODE                 | REFRESH MATERIALIZED VIEW で WITH SYNC MODE キーワードを使用して、マテリアライズドビューのリフレッシュタスクを同期的に実行できます。 | v2.5.8+<br />v3.0.4+<br />v3.1+ |
| Intermediate Result Spilling      | マテリアライズドビューの構築中に OOM を回避するために `enable_spill` プロパティを使用して Intermediate Result Spilling を有効にできます。 | v3.1+                           |
| Resource Group                    | リソース分離を実現するために、マテリアライズドビューの構築に `resource_group` プロパティを指定できます。 | v3.1+                           |
| Materialized View on View         | ビューに基づいてマテリアライズドビューを作成できます。 | v3.1+                           |
| Swap Materialized View            | ALTER MATERIALIZED VIEW で SWAP WITH キーワードを使用して、マテリアライズドビューを原子的に置き換えることができます。 | v3.1+                           |
| CREATE INDEX ON Materialized View | マテリアライズドビューにインデックスを作成してポイントクエリを高速化できます。 | v3.0.7+<br />v3.1.4+<br />v3.2+ |
| AUTO ACTIVE                       | 背景で指数関数的バックオフを使用して無効なマテリアライズドビューを自動的にアクティブ化し、間隔が60分に達すると停止します。 | v3.1.4+<br />v3.2+              |
| Backup and Restore                | マテリアライズドビューのバックアップとリストアをサポートします。          | v3.2+                           |
| Object Dependencies               | マテリアライズドビューとベーステーブル間の依存関係を明確にするためのシステム定義ビュー `sys.object_dependencies` を提供します。 | v3.2+                           |

## 変数

| 変数                                    | 説明                                              | デフォルト | サポートされているバージョン |
| :---------------------------------------------- | :----------------------------------------------------------- | :---------- | :------------------------ |
| enable_materialized_view_rewrite                | マテリアライズドビューのクエリの書き換えを有効にするかどうか。           | true        | v2.5+                                                        |
| enable_materialized_view_for_insert             | INSERT ステートメントに対するマテリアライズドビューのクエリの書き換えを有効にするかどうか。 | false       | v2.5.18+<br />v3.0.9+<br />v3.1.7+<br />v3.2.2+              |
| materialized_view_rewrite_mode                  | マテリアライズドビューのクエリの書き換えモード。                     | DEFAULT     | v3.2+                                                        |
| optimizer_materialized_view_timelimit           | マテリアライズドビューのクエリの書き換えに使用できる最大時間。この時間を超えると、クエリの書き換えは中止され、オプティマイザープロセスが続行されます。 | 1000        | v3.1.9+<br />v3.2.5+                                         |
| analyze_mv                                      | マテリアライズドビューがリフレッシュされた後に統計を収集する方法。 | SAMPLE      | v3.0+                                                        |
| enable_materialized_view_plan_cache             | マテリアライズドビューのプランキャッシュを有効にするかどうか。デフォルトでは、1000 のマテリアライズドビュープランがキャッシュされます。 | TRUE        | v2.5.13+<br />v3.0.7+<br />v3.1.4+<br />v3.2.0+<br />v3.3.0+ |
| query_including_mv_names                        | クエリの書き換えに使用できるマテリアライズドビューのホワイトリスト。 |             | v3.1.11+<br />v3.2.5+                                        |
| query_excluding_mv_names                        | クエリの書き換えに使用できるマテリアライズドビューのブラックリスト。 |             | v3.1.11+<br />v3.2.5+                                        |
| cbo_materialized_view_rewrite_related_mvs_limit | プランステージでの候補マテリアライズドビューの最大数。 | 64          | v3.1.9+<br /> v3.2.5+                                        |

## プロパティ

| プロパティ                       | 説明                                              | サポートされているバージョン |
| :--------------------------------- | :----------------------------------------------------------- | :----------------------- |
| `session.<property_name>`          | マテリアライズドビューの構築に使用されるセッション変数のプレフィックス。例: `session.query_timeout` や `session.query_mem_limit`。 | v3.0+                    |
| auto_refresh_partitions_limit      | 自動リフレッシュがトリガーされるたびにリフレッシュされるマテリアライズドビューパーティションの最大数。 | v2.5+                    |
| excluded_trigger_tables            | マテリアライズドビューの自動リフレッシュをトリガーしないベーステーブル。 | v2.5+                    |
| partition_refresh_number           | リフレッシュタスクがバッチで実行されるときにリフレッシュされるパーティションの数。 | v2.5+                    |
| partition_ttl_number               | 保持する最新のマテリアライズドビューパーティションの数。 | v2.5+                    |
| partition_ttl                      | マテリアライズドビューパーティションの有効期限 (TTL)。このプロパティは `partition_ttl_number` より推奨されます。 | v3.1.4+<br />v3.2+       |
| force_external_table_query_rewrite | 外部カタログベースのマテリアライズドビューに対するクエリの書き換えを有効にするかどうか。 | v2.5+                    |
| query_rewrite_consistency          | 内部テーブルに基づいて構築されたマテリアライズドビューのクエリの書き換えルール。 | v3.0.5+<br />v3.1+       |
| resource_group                     | マテリアライズドビューのリフレッシュタスクが属するリソースグループ。 | v3.1+                    |
| colocate_with                      | マテリアライズドビューのコロケーショングループ。               | v3.1+                    |
| foreign_key_constraints            | View Delta Join シナリオでクエリの書き換えのためにマテリアライズドビューを作成する際の外部キー制約。 | v2.5.4+<br />v3.0+       |
| unique_constraints                 | View Delta Join シナリオでクエリの書き換えのためにマテリアライズドビューを作成する際のユニークキー制約。 | v2.5.4+<br />v3.0+       |
| mv_rewrite_staleness_second        | クエリの書き換え中のマテリアライズドビューデータの陳腐化許容度。 | v3.1+                    |
| enable_query_rewrite               | マテリアライズドビューがクエリの書き換えに使用できるかどうか。 | v3.3+                    |

## パーティショニング

| アライメント                                                | ユースケース                                                 | サポートされているバージョン |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------- |
| [Align partitions one-to-one (Date types)](use_cases/create_partitioned_materialized_view.md#align-partitions-one-to-one)                 | 同じパーティションキーを使用して、ベーステーブルのパーティションに一対一で対応するマテリアライズドビューを作成します。パーティションキーは DATE または DATETIME 型でなければなりません。 | v2.5+                    |
| [Align partitions one-to-one (STRING type)](use_cases/create_partitioned_materialized_view.md#align-partitions-one-to-one)                | 同じパーティションキーを使用して、ベーステーブルのパーティションに一対一で対応するマテリアライズドビューを作成します。パーティションキーは STRING 型でなければなりません。 | v3.1.4+<br />v3.2+       |
| [Align partitions with time granularity rollup (Date types)](use_cases/create_partitioned_materialized_view.md#align-partitions-with-time-granularity-rollup) | パーティションキーに `date_trunc` 関数を使用して、ベーステーブルよりも大きなパーティショニング粒度を持つマテリアライズドビューを作成します。パーティションキーは DATE または DATETIME 型でなければなりません。 | v2.5+                    |
| [Align partitions with time granularity rollup (STRING type)](use_cases/create_partitioned_materialized_view.md#align-partitions-with-time-granularity-rollup) | パーティションキーに `date_trunc` 関数を使用して、ベーステーブルよりも大きなパーティショニング粒度を持つマテリアライズドビューを作成します。パーティションキーは STRING 型でなければなりません。 | v3.1.4+<br />v3.2+       |
| [Align partitions at a customized time granularity](use_cases/create_partitioned_materialized_view.md#align-partitions-at-a-customized-time-granularity)        | `date_trunc` 関数と `time_slice` または `date_slice` 関数を使用して、パーティションの時間粒度をカスタマイズしたマテリアライズドビューを作成します。 | v3.2+                    |
| [Align partitions with multiple base tables](use_cases/create_partitioned_materialized_view.md#align-partitions-with-multiple-base-tables)               | 複数のベーステーブルのパーティションと整合するマテリアライズドビューを作成します。ベーステーブルが同じタイプのパーティションキーを使用している限り可能です。 | v3.3+                    |

**異なるジョイン方法**

- **Single Fact Table (v2.4+)**: マテリアライズドビューとファクトテーブル間のパーティションマッピングを確立することで、ファクトテーブルが更新されたときにマテリアライズドビューパーティションが自動的にリフレッシュされることを保証します。
- **Multiple Fact Tables (v3.3+)**: マテリアライズドビューと、同じ時間粒度でジョイン/ユニオンされる複数のファクトテーブル間のパーティションマッピングを確立することで、いずれかのファクトテーブルが更新されたときにマテリアライズドビューパーティションが自動的にリフレッシュされることを保証します。
- **Temporal Dimension Table (v3.3+)**: 次のような場合を考えます。ディメンジョンテーブルが履歴バージョンデータを保存し、特定の時間粒度でパーティション化されており、ファクトテーブルが同じ時間粒度でディメンジョンテーブルとジョインします。マテリアライズドビューとファクトテーブルおよびディメンジョンテーブルの両方との間にパーティションマッピングを確立することで、いずれかのテーブルが更新されたときにマテリアライズドビューパーティションが自動的にリフレッシュされることを保証します。

## 外部カタログ上のマテリアライズドビュー

| 外部データソース | サポートされているシナリオとバージョン                        | 安定版バージョン |
| :----------------------- | :----------------------------------------------------------- | :-------------------- |
| Hive                         | <ul><li>非パーティションテーブル: v2.5.4 & v3.0+</li><li>DATE および DATETIME 型パーティション: v2.5.4 & v3.0+</li><li>STRING 型パーティションキーを DATE 型に変換: v3.1.4 & v3.2+</li><li>Hive View 上のマテリアライズドビュー: サポート予定</li><li>多層パーティショニング: サポート予定</li></ul> | v2.5.13+<br />v3.0.6+<br />v3.1.5+<br />v3.2+ |
| Iceberg                      | <ul><li>非パーティションテーブル: v3.0+</li><li>DATE および DATETIME 型パーティション: v3.1.4 & v3.2+</li><li>STRING 型パーティションキーを DATE 型に変換: v3.1.4 & v3.2+</li><li>Iceberg View 上のマテリアライズドビュー: サポート予定</li><li>パーティション変換: v3.2.3</li><li>パーティションレベルのリフレッシュ: v3.1.7 & v3.2.3</li><li>多層パーティショニング: サポート予定</li></ul> | v3.1.5+<br />v3.2+                            |
| Hudi                         | <ul><li>非パーティションテーブル: v3.2+</li><li>DATE および DATETIME 型パーティション: v3.2+</li><li>多層パーティショニング: サポート予定</li></ul> | 安定版ではない                                    |
| Paimon                       | <ul><li>非パーティションテーブル: v2.5.4 & v3.0+</li><li>DATE および DATETIME 型パーティション: サポート予定</li><li>多層パーティショニング: サポート予定</li></ul> | 安定版ではない                                    |
| DeltaLake                    | <ul><li>非パーティションテーブル: v3.2+</li><li>パーティションテーブル: サポート予定</li><li>多層パーティショニング: サポート予定</li></ul> | 安定版ではない                                    |
| JDBC                         | <ul><li>非パーティションテーブル: v3.0+</li><li>パーティションテーブル: MySQL RangeColumn Partition v3.1.4</li></ul> | 安定版ではない                                    |

## クエリの書き換え

| 機能                         | 説明                                              | サポートされているバージョン        |
| :---------------------------------- | :----------------------------------------------------------- | :------------------------------ |
| Single Table Rewrite                | 単一の内部テーブルに基づいて構築されたマテリアライズドビューを使用したクエリの書き換え。 | v2.5+                           |
| Inner Join Rewrite                  | 内部テーブルに対する INNER/CROSS JOIN のクエリの書き換え。       | v2.5+                           |
| Aggregate Rewrite                   | 基本的な集計を伴うジョインのクエリの書き換え。             | v2.5+                           |
| UNION Rewrite                       | 内部テーブルに対する述語 UNION 補償書き換えとパーティション UNION 補償書き換え。 | v2.5+                           |
| Nested Materialized View Rewrite    | 内部テーブルに対するネストされたマテリアライズドビューを使用したクエリの書き換え。 | v2.5+                           |
| Count Distinct Rewrite (bitmap/hll) | COUNT DISTINCT 計算をビットマップまたは HLL ベースの計算に書き換え。 | v2.5.6+<br />v3.0+              |
| View Delta Join Rewrite             | マテリアライズドビューがジョインするテーブルのサブセットをジョインするクエリの書き換え。 | v2.5.4+<br />v3.0+              |
| Join Derivability Rewrite           | 異なるジョインタイプ間のクエリの書き換え。                  | v2.5.8+<br />v3.0.4+<br />v3.1+ |
| Full Outer Join and Other Joins     | Full Outer Join、Semi Join、Anti Join のクエリの書き換え。 | v3.1+                           |
| Avg to Sum/Count Rewrite            | avg() を sum() / count() に書き換えるクエリ。                   | v3.1+                           |
| View-based Rewrite                  | ビューに基づいて構築されたマテリアライズドビューを使用したクエリの書き換え。ビューに対するクエリをビューのベーステーブルに対するクエリに書き換えずに行います。 | v3.2.2+                         |
| Count Distinct Rewrite (ArrayAgg)   | COUNT DISTINCT 計算を `array_agg_distinct` 関数を使用した計算に書き換え。 | v3.2.5+<br />v3.3+              |
| Text-based Query Rewrite            | マテリアライズドビューの定義と同一の抽象構文ツリーを持つクエリを再書き換え。 | v3.3+                           |

## 診断機能

| 機能        | 使用シナリオ                                           | サポートされているバージョン         |
| :----------------- | :----------------------------------------------------------- | :------------------------------- |
| TRACE REWRITE      | TRACE REWRITE ステートメントを使用して書き換えの問題を診断します。  | v2.5.10+<br />v3.0.5+<br />v3.1+ |
| Query Dump         | マテリアライズドビューがクエリされた際にその情報をダンプします。 | v3.1+                            |
| Refresh Audit Log  | マテリアライズドビューがリフレッシュされた際に Audit Log に実行された SQL を記録します。 | v2.5.8+<br />v3.0.3+<br />v3.1+  |
| Hit Audit Log      | クエリがマテリアライズドビューに書き換えられた際に、ヒットしたマテリアライズドビューと候補マテリアライズドビューを Audit Log に記録します。 | v3.1.4+<br />v3.2+               |
| Monitoring Metrics | マテリアライズドビュー専用の監視メトリクス。         | v3.1.4+<br />v3.2+               |