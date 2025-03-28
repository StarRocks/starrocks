---
displayed_sidebar: docs
sidebar_position: 11
---

# クエリフィードバック

このトピックでは、クエリフィードバック機能、その適用シナリオ、および Query Plan Advisor を使用して実行統計からのフィードバックに基づいてクエリプランを最適化する方法を紹介します。

StarRocks は v3.4.0 以降でクエリフィードバック機能をサポートしています。

## 概要

クエリフィードバックは、コストベースオプティマイザ (CBO) のフレームワークであり、重要なコンポーネントです。クエリ実行中に実行統計を記録し、類似のクエリプランを持つ後続のクエリで再利用して、CBO が最適化されたクエリプランを生成するのを助けます。CBO は推定統計に基づいてクエリプランを最適化しますが、統計情報が古いまたは不正確な場合、大きなテーブルをブロードキャストしたり、左と右のテーブルの順序を誤ったりするなど、非効率的なクエリプラン（悪いプラン）を選択する可能性があります。これらの悪いプランは、クエリ実行のタイムアウト、過剰なリソース消費、さらにはシステムクラッシュを引き起こす可能性があります。

## ワークフロー

クエリフィードバックに基づくプラン最適化のワークフローは、次の3つのステージで構成されます。

1. **観察**: BE または CN は、各クエリプランの PlanNode の主要なメトリクス（`InputRows` と `OutputRows` を含む）を記録します。
2. **分析**: 設定されたしきい値を超える遅いクエリや手動で分析対象とされたクエリについて、システムはクエリが終了し結果が返される前に、現在のクエリプランの最適化機会を特定するために重要なノードでの実行詳細を分析します。FE はクエリプランを実行統計と比較し、異常なクエリプランによって引き起こされた遅いクエリであるかどうかを確認します。FE が不正確な統計を分析する際、各クエリに対する SQL チューニングガイドを生成し、CBO にクエリを動的に最適化するよう指示し、パフォーマンスを向上させるための戦略を推奨します。
3. **最適化**: CBO が物理プランを生成した後、そのプランに適用される既存のチューニングガイドを検索します。もしあれば、CBO はガイドと戦略に従ってプランを動的に最適化し、問題のあるセクションを修正し、悪いクエリプランの繰り返し使用によるクエリパフォーマンスへの影響を排除します。最適化されたプランの実行時間は、チューニングの効果を評価するために元のプランと比較されます。

## 使用法

システム変数 `enable_plan_advisor`（デフォルト: `true`）によって制御される Query Plan Advisor は、FE 設定項目 `slow_query_analyze_threshold`（デフォルト: `5` 秒）で定義されたしきい値を超える実行時間を持つ遅いクエリに対してデフォルトで有効になっています。

さらに、特定のクエリを手動で分析するか、実行されたすべてのクエリに対して自動分析を有効にすることができます。

### 特定のクエリを手動で分析する

`slow_query_analyze_threshold` を超えない実行時間を持つ特定のクエリステートメントを手動で分析することができます。

```SQL
ALTER PLAN ADVISOR ADD <query_statement>
```

例:

```SQL
ALTER PLAN ADVISOR ADD SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
```

### すべてのクエリに対して自動分析を有効にする

すべてのクエリに対して自動分析を有効にするには、システム変数 `enable_plan_analyzer`（デフォルト: `false`）を `true` に設定する必要があります。

```SQL
SET enable_plan_analyzer = true;
```

### 現在の FE でのチューニングガイドを表示する

各 FE は独自のチューニングガイドの記録を保持しています。以下のステートメントを使用して、現在の FE に対して生成されたクエリのチューニングガイドを表示できます。

```SQL
SHOW PLAN ADVISOR
```

### チューニングガイドが効果を発揮しているか確認する

クエリステートメントに対して [EXPLAIN](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) を実行します。EXPLAIN 文字列に `Plan had been tuned by Plan Advisor` というメッセージが表示されている場合、対応するクエリにチューニングガイドが適用されたことを示しています。

例:

```SQL
EXPLAIN SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
+-----------------------------------------------------------------------------------------------+
| Explain String                                                                                |
+-----------------------------------------------------------------------------------------------+
| Plan had been tuned by Plan Advisor.                                                          |
| Original query id:8e010cf4-b178-11ef-8aa4-8a5075cec65e                                        |
| Original time cost: 148 ms                                                                    |
| 1: LeftChildEstimationErrorTuningGuide                                                        |
| Reason: left child statistics of JoinNode 5 had been overestimated.                           |
| Advice: Adjust the distribution join execution type and join plan to improve the performance. |
|                                                                                               |
| PLAN FRAGMENT 0                                                                               |
|  OUTPUT EXPRS:9: count                                                                        |
|   PARTITION: UNPARTITIONED                                           
```

### 特定のクエリのチューニングガイドを削除する

`SHOW PLAN ADVISOR` から返されたクエリ ID に基づいて、特定のクエリのチューニングガイドを削除することができます。

```SQL
ALTER PLAN ADVISOR DROP <query_id>
```

例:

```SQL
ALTER PLAN ADVISOR DROP "8e010cf4-b178-11ef-8aa4-8a5075cec65e";
```

### 現在の FE 上のすべてのチューニングガイドをクリアする

現在の FE 上のすべてのチューニングガイドをクリアするには、次のステートメントを実行します。

```SQL
TRUNCATE PLAN ADVISOR
```

## ユースケース

現在、クエリフィードバックは主に次のシナリオを最適化するために使用されています。

- ローカル Join ノードの左側と右側の順序を最適化する
- ローカル Join ノードの実行方法を最適化する（例: Broadcast から Shuffle への切り替え）
- 集約の可能性が大きいケースでは、`pre_aggregation` モードを強制して、最初のフェーズでのデータ集約を最大化する

チューニングガイドは主に `Runtime Exec Node Input/Output Rows` と FE の `statistics estimated rows` に基づいています。現在のチューニングしきい値は比較的保守的であるため、Query Profile や EXPLAIN 文字列で問題が観察された場合、クエリフィードバックを活用して潜在的なパフォーマンス向上を確認することをお勧めします。

以下に3つの一般的なユーザーケースを示します。

### ケース 1: 誤った Join 順序

元の悪いプラン:

```SQL
small left table inner join large table (broadcast)
```

最適化されたプラン:

```SQL
large left table inner join small right table (broadcast)
```

**原因** 問題は、古いまたは欠落した統計によって引き起こされ、コストベースオプティマイザ (CBO) が不正確なデータに基づいて誤ったプランを生成することが原因かもしれません。

**解決策** クエリ実行中に、システムは Left Child と Right Child の `input/output rows` と `statistics estimated rows` を比較し、チューニングガイドを生成します。再実行時に、システムは自動的に Join 順序を調整します。

### ケース 2: 誤った Join 実行方法

元の悪いプラン:

```SQL
large left table1 inner join large right table2 (broadcast)
```

最適化されたプラン:

```SQL
large left table1 (shuffle) inner join large right table2 (shuffle)
```

**原因** 問題はデータスキューに起因する可能性があります。右テーブルに多くのパーティションがあり、そのうちの1つが不均衡に大量のデータを含んでいる場合、システムは述語が適用された後の右テーブルの行数を誤って推定することがあります。

**解決策** クエリ実行中に、システムは Left Child と Right Child の `input/output rows` と `statistics estimated rows` を比較し、チューニングガイドを生成します。最適化後、Join 方法は Broadcast Join から Shuffle Join に調整されます。

### ケース 3: 非効率的な第一フェーズの事前集計モード

**症状** 集約の可能性が高いデータに対して、第一フェーズ集約の `auto` モードは、ローカルデータのわずかな集約しか行わず、パフォーマンス向上の機会を逃す可能性があります。

**解決策** クエリ実行中に、システムはローカルおよびグローバル集約の `Input/Output Rows` を収集します。履歴データに基づいて、集約列の可能性を評価します。可能性が高い場合、システムはローカル集約で `pre_aggregation` モードの使用を強制し、第一フェーズでのデータ集約を最大化し、全体的なクエリパフォーマンスを向上させます。

## 制限事項

- チューニングガイドは、それが生成された正確なクエリにのみ使用できます。同じパターンでも異なるパラメータを持つクエリには適用されません。
- 各 FE は独立して Query Plan Advisor を管理し、FE ノード間の同期はサポートされていません。同じクエリが異なる FE ノードに送信された場合、チューニング結果が異なる可能性があります。
- Query Plan Advisor はインメモリキャッシュ構造を使用します:
  - チューニングガイドの数が制限を超えると、期限切れのチューニングガイドが自動的に削除されます。
  - チューニングガイドのデフォルトの制限は 300 であり、履歴チューニングガイドの永続性はサポートされていません。