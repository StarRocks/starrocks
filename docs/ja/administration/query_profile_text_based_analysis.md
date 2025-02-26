---
displayed_sidebar: docs
---

# テキストベースの Query Profile の可視化分析

このトピックでは、MySQL クライアントを使用してテキストベースの Query Profile を取得し、分析する方法を紹介します。

## ANALYZE PROFILE を使用して既存のクエリのプロファイルを分析する

クラスター内の既存の（履歴または実行中の）クエリのテキストベースのプロファイルを分析するには、まず [SHOW PROFILELIST](../sql-reference/sql-statements/cluster-management/plan_profile/SHOW_PROFILELIST.md) ステートメントを使用してクエリの概要を取得する必要があります。このコマンドは、正常に終了したクエリ、エラーで失敗したクエリ、および（10 秒以上実行中でまだ終了していない）クエリをすべてリストします。このステートメントを通じて、後続の分析のための対応する Query ID を取得できます。構文は次のとおりです。

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

Query ID を取得したら、[ANALYZE PROFILE](../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) ステートメントを使用して Query Profile の分析を進めることができます。構文は次のとおりです。

```SQL
ANALYZE PROFILE FROM '<Query_ID>' [, <Node_ID> [, ...] ]
```

- `Query_ID`: `SHOW PROFILELIST` ステートメントから取得したクエリに対応する ID。
- `Node_ID`: プロファイルノード ID。ID が指定されたノードについては、StarRocks はそのノードの詳細なメトリック情報を返します。ID が指定されていないノードについては、StarRocks は概要情報のみを返します。

プロファイルには次のセクションが含まれます。

- Summary: プロファイルの概要情報。
  - QueryID
  - バージョン情報
  - クエリの状態（`Finished`、`Error`、`Running` を含む）
  - クエリの総時間
  - メモリ使用量
  - CPU 使用率が最も高いトップ 10 ノード
  - メモリ使用率が最も高いトップ 10 ノード
  - デフォルト値とは異なる値を持つセッション変数
- Fragments: 各フラグメント内の各ノードのメトリクスを表示。
  - 各ノードの時間、メモリ使用量、コスト推定情報、出力行数。
  - 時間使用率が 30% を超えるノードは赤で強調表示。
  - 時間使用率が 15% を超え、30% 未満のノードはピンクで強調表示。

例 1: ノード ID を指定せずに Query Profile をクエリ。

![img](../_assets/Profile/text_based_profile_without_node_id.jpeg)

例 2: ノード ID を `0` と指定して Query Profile をクエリ。StarRocks はノード ID `0` のすべての詳細なメトリクスを返し、問題の特定を容易にするために高使用率のメトリクスを強調表示します。

![img](../_assets/Profile/text_based_profile_with_node_id.jpeg)

さらに、上記の方法は、実行中のクエリのために生成されたプロファイルである Runtime Query Profile の表示と分析もサポートしています。Query Profile 機能が有効になっている場合、この方法を使用して 10 秒以上実行中のクエリのプロファイルを取得できます。

完了したクエリのものと比較して、実行中のクエリのテキストベースの Query Profile には次の情報が含まれます。

- オペレーターの状態:
  - ⏳: オペレーターが開始されていません。これらのオペレーターは依存関係のために実行が開始されていない可能性があります。
  - 🚀: 実行中のオペレーター。
  - ✅: 実行が完了したオペレーター。

- 全体の進捗: `実行が完了したオペレーターの数 / オペレーターの総数` に基づいて計算されます。データ行の詳細情報が不足しているため、この値は若干歪む可能性があります。

- オペレーターの進捗: `処理された行数 / 行の総数` に基づいて計算されます。行の総数を計算できない場合、進捗は `?` と表示されます。

例:

![img](../_assets/Profile/text_based_runtime_profile.jpeg)

## EXPLAIN ANALYZE を使用してプロファイル分析のためのクエリをシミュレートする

StarRocks は [EXPLAIN ANALYZE](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE.md) ステートメントを提供しており、クエリのプロファイルを直接シミュレートして分析することができます。構文は次のとおりです。

```SQL
EXPLAIN ANALYZE <sql_statement>
```

`EXPLAIN ANALYZE` を実行すると、StarRocks はデフォルトで現在のセッションに対して Query Profile 機能を有効にします。

現在、`EXPLAIN ANALYZE` は 2 種類の SQL ステートメントをサポートしています: SELECT ステートメントと INSERT INTO ステートメント。StarRocks のデフォルトカタログ内の内部テーブルに対する INSERT INTO ステートメントの Query Profile のシミュレーションと分析のみが可能です。INSERT INTO ステートメントの Query Profile をシミュレーションおよび分析する際、実際のデータはロードされません。デフォルトで、インポートトランザクションは中止され、分析中にデータに意図しない変更が加えられないようにします。

例 1: SELECT ステートメントをシミュレートして分析。クエリ結果は破棄されます。

![img](../_assets/Profile/text_based_explain_analyze_select.jpeg)

例 2: INSERT INTO ステートメントをシミュレートして分析。ロードトランザクションは中止されます。

![img](../_assets/Profile/text_based_explain_analyze_insert.jpeg)

## 制限事項

- `EXPLAIN ANALYZE INSERT INTO` ステートメントはデフォルトカタログ内のテーブルに対してのみサポートされています。
- より良い視覚効果を得るために、出力テキストには色付け、ハイライト、その他の機能を提供するために ANSI 文字が含まれています。MyCLI クライアントの使用を推奨します。ANSI 機能をサポートしていないクライアント（MySQL クライアントなど）では、表示が若干乱れることがありますが、通常は使用に影響しません。例えば:

![img](../_assets/Profile/text_based_profile_not_aligned.jpeg)