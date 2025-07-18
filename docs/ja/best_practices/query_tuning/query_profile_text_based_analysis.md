---
displayed_sidebar: docs
sidebar_position: 60
---

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
# テキストベースの Query Profile の可視化分析

MySQL クライアントを通じてテキストベースの Query Profile を取得し、分析する方法。
=======
# Explain Analyze

このドキュメントでは、StarRocks でテキストベースの Query Profile を取得し、分析する方法を説明します。これにより、クエリパフォーマンスを理解し、SQL クエリを最適化する方法を見つける手助けをします。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

## ANALYZE PROFILE を使用して既存クエリのプロファイルを分析する

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
クラスター内の既存（履歴または実行中）のクエリのテキストベースのプロファイルを分析するには、まず [SHOW PROFILELIST](../sql-reference/sql-statements/cluster-management/plan_profile/SHOW_PROFILELIST.md) ステートメントを使用してクエリの概要を取得する必要があります。このコマンドは、正常に終了したクエリ、エラーで失敗したクエリ、およびまだ終了していない（10 秒以上実行中の）クエリを一覧表示します。このステートメントを通じて、後続の分析のための対応する Query ID を取得できます。構文は以下の通りです。
=======
クラスター内の既存の（履歴または実行中の）クエリのテキストベースのプロファイルを分析するには、まず [SHOW PROFILELIST](../../sql-reference/sql-statements/cluster-management/plan_profile/SHOW_PROFILELIST.md) ステートメントを使用してクエリの概要を取得する必要があります。このコマンドは、正常に終了したクエリ、エラーで失敗したクエリ、およびまだ実行中のクエリ（10 秒以上経過しても終了していないもの）を一覧表示します。このステートメントを通じて、後続の分析のための対応する Query ID を取得できます。構文は次のとおりです。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

```SQL
SHOW PROFILELIST [LIMIT <num>];
```

例:

```SQL
SHOW PROFILELIST;
SHOW PROFILELIST LIMIT 5;
```

出力:

```plaintext
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
| QueryId                              | StartTime           | Time  | State    | Statement                                                                                                                         |
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
| a40456b2-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:18 | 21ms  | Finished | SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES\n    WHERE ROUTINE_TYPE="FUNCTION" AND ROUTINE_SCHEMA = "None"               |
| a3fc4060-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 39ms  | Finished | select TABLE_NAME, COLUMN_NAME from information_schema.columns\n                                    where table_schema = 'Non ... |
| a3f7d38d-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 15ms  | Finished | select connection_id()                                                                                                            |
| a3efbd3b-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:17 | 16ms  | Finished | select connection_id()                                                                                                            |
| a26ec286-8428-11ee-8d02-6a32f8c68848 | 2023-11-16 10:34:15 | 269ms | Error    | EXPLAIN ANALYZE  SELECT c_nation, s_nation, year(lo_orderdate) AS year , SUM(lo_revenue) AS revenue FROM lineorder_flat WHERE ...  |
+--------------------------------------+---------------------+-------+----------+-----------------------------------------------------------------------------------------------------------------------------------+
```

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
Query ID を取得したら、[ANALYZE PROFILE](../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) ステートメントを使用して Query Profile の分析を進めることができます。構文は以下の通りです。
=======
Query ID を取得したら、[ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) ステートメントを使用して Query Profile の分析を進めることができます。構文は次のとおりです。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

```SQL
ANALYZE PROFILE FROM '<Query_ID>' [, <Node_ID> [, ...] ]
```

- `Query_ID`: `SHOW PROFILELIST` ステートメントから取得したクエリに対応する ID。
<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
- `Node_ID`: プロファイルノード ID。指定された ID のノードについては、StarRocks は詳細なメトリック情報を返します。指定されていないノードについては、StarRocks は要約情報のみを返します。
=======
- `Node_ID`: プロファイルノード ID。ID が指定されたノードについては、StarRocks はそのノードの詳細なメトリック情報を返します。ID が指定されていないノードについては、StarRocks は要約情報のみを返します。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

プロファイルには以下のセクションが含まれます。

- Summary: プロファイルの要約情報。
  - QueryID
  - バージョン情報
<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
  - クエリのステータス（`Finished`、`Error`、`Running` を含む）
  - クエリの総時間
  - メモリ使用量
  - CPU 使用率が最も高い上位 10 ノード
  - メモリ使用率が最も高い上位 10 ノード
  - デフォルト値と異なる値を持つセッション変数
- Fragments: 各フラグメント内の各ノードのメトリクスを表示。
  - 各ノードの時間、メモリ使用量、コスト推定情報、出力行数。
  - 時間使用率が 30% を超えるノードは赤で強調表示。
  - 時間使用率が 15% を超え 30% 未満のノードはピンクで強調表示。
=======
  - クエリの状態（`Finished`、`Error`、`Running` を含む）
  - 総クエリ時間
  - メモリ使用量
  - CPU 使用率が最も高いトップ 10 ノード
  - メモリ使用率が最も高いトップ 10 ノード
  - デフォルト値と異なる値を持つセッション変数
- Fragments: 各フラグメント内の各ノードのメトリクスを表示。
  - 各ノードの時間、メモリ使用量、コスト見積もり情報、および出力行。
  - 時間使用率が 30% を超えるノードは赤で強調表示されます。
  - 時間使用率が 15% を超え、30% 未満のノードはピンクで強調表示されます。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

例 1: ノード ID を指定せずに Query Profile をクエリ。

![img](../../_assets/Profile/text_based_profile_without_node_id.jpeg)

例 2: Query Profile をクエリし、ノード ID を `0` と指定。StarRocks はノード ID `0` のすべての詳細なメトリクスを返し、問題の特定を容易にするために高使用率のメトリクスを強調表示します。

![img](../../_assets/Profile/text_based_profile_with_node_id.jpeg)

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
さらに、上記の方法は実行中のクエリのために生成された Runtime Query Profile の表示と分析もサポートしています。Query Profile 機能が有効になっている場合、この方法を使用して 10 秒以上実行中のクエリのプロファイルを取得できます。
=======
さらに、上記の方法は、実行中のクエリに対して生成されたプロファイルである Runtime Query Profile の表示と分析もサポートしています。Query Profile 機能が有効になっている場合、この方法を使用して 10 秒以上実行中のクエリのプロファイルを取得できます。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

終了したクエリのものと比較して、実行中のクエリのテキストベースの Query Profile には以下の情報が含まれます。

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
- Operator のステータス:
  - ⏳: 開始されていない Operator。これらの Operator は依存関係のために実行が開始されていない可能性があります。
  - 🚀: 実行中の Operator。
  - ✅: 実行が終了した Operator。

- 全体の進捗状況: `実行が終了した Operator の数 / Operator の総数` に基づいて計算されます。データ行の詳細情報が不足しているため、この値は若干歪む可能性があります。

- Operator の進捗状況: `処理された行数 / 行の総数` に基づいて計算されます。行の総数を計算できない場合、進捗は `?` と表示されます。
=======
- Operator の状態:
  - ⏳: 開始されていない Operator。これらの Operator は依存関係のために実行を開始していない可能性があります。
  - 🚀: 実行中の Operator。
  - ✅: 実行を完了した Operator。

- 全体の進捗状況: `実行を完了した Operator の数 / 総 Operator 数` に基づいて計算されます。データ行の詳細情報が不足しているため、この値は若干歪む可能性があります。

- Operator の進捗状況: `処理された行数 / 総行数` に基づいて計算されます。総行数が計算できない場合、進捗は `?` と表示されます。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

例:

![img](../../_assets/Profile/text_based_runtime_profile.jpeg)

## EXPLAIN ANALYZE を使用してプロファイル分析のためにクエリをシミュレートする

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
StarRocks は、クエリのプロファイルを直接シミュレートして分析するための [EXPLAIN ANALYZE](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE.md) ステートメントを提供します。構文は以下の通りです。
=======
StarRocks は [EXPLAIN ANALYZE](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE.md) ステートメントを提供しており、クエリのプロファイルを直接シミュレートして分析することができます。構文は次のとおりです。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

```SQL
EXPLAIN ANALYZE <sql_statement>
```

`EXPLAIN ANALYZE` を実行すると、StarRocks はデフォルトで現在のセッションに対して Query Profile 機能を有効にします。

<<<<<<< HEAD:docs/ja/administration/query_profile_text_based_analysis.md
現在、`EXPLAIN ANALYZE` は 2 種類の SQL ステートメントをサポートしています: SELECT ステートメントと INSERT INTO ステートメント。StarRocks のデフォルトカタログ内の内部テーブルに対してのみ、INSERT INTO ステートメントの Query Profile をシミュレートして分析できます。INSERT INTO ステートメントの Query Profile をシミュレートして分析する際、実際のデータはロードされません。デフォルトで、インポートトランザクションは中止され、分析中にデータに意図しない変更が加えられないようにします。
=======
現在、`EXPLAIN ANALYZE` は 2 種類の SQL ステートメントをサポートしています: SELECT ステートメントと INSERT INTO ステートメント。StarRocks のデフォルトカタログ内の内部テーブルに対してのみ、INSERT INTO ステートメントの Query Profile をシミュレートして分析できます。INSERT INTO ステートメントの Query Profile をシミュレートして分析する際には、実際のデータはロードされません。デフォルトでは、分析中にデータに意図しない変更が加えられないように、インポートトランザクションは中止されます。
>>>>>>> 65a3c16e86 ([Doc] refactor query tuning best practice (#60935)):docs/ja/best_practices/query_tuning/query_profile_text_based_analysis.md

例 1: SELECT ステートメントをシミュレートして分析。クエリ結果は破棄されます。

![img](../../_assets/Profile/text_based_explain_analyze_select.jpeg)

例 2: INSERT INTO ステートメントをシミュレートして分析。ロードトランザクションは中止されます。

![img](../../_assets/Profile/text_based_explain_analyze_insert.jpeg)

## 制限事項

- `EXPLAIN ANALYZE INSERT INTO` ステートメントはデフォルトカタログ内のテーブルに対してのみサポートされています。
- より良い視覚効果を得るために、出力テキストには色、ハイライト、その他の機能を提供するために ANSI 文字が含まれています。MyCLI クライアントの使用を推奨します。ANSI 機能をサポートしていないクライアント（MySQL クライアントなど）では、若干の表示の乱れが生じる可能性がありますが、通常、使用には影響しません。例えば:

![img](../../_assets/Profile/text_based_profile_not_aligned.jpeg)